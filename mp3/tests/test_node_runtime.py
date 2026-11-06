"""Local integration tests for the combined MP3 node runtime and CLI."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from mp3.membership import GossipEnvelope, Member, MemberState, MembershipTable
from mp3.coordinator import (
    OwnerDispatchError, OwnerOutcome, RpcCompletion, Schema2Activation,
)
from mp3.attempts import AttemptLease
from mp3.models import ConsistencyMode, Context, LocalStatus, Version
from mp3.node import NodeRuntime, main as node_main
from mp3.ring import Endpoint, Manifest
from mp3.rpc import (ReplicaClient, RpcOperation, RpcOutcomeKind, RpcStatus,
                     new_operation_id, new_request, receive_response, send_request)
from mp3.storage import StorageError, open_store


ROOT = Path(__file__).resolve().parents[2]
MAX_VALUE_BYTES = 1_048_576


def _free_port(reserved: set[int]) -> int:
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in reserved:
            reserved.add(port)
            return port


def local_manifest() -> Manifest:
    """Build ten valid loopback identities; no VM/network is contacted."""
    reserved: set[int] = set()
    endpoints = {}
    for number in range(1, 11):
        vm_id = f"vm{number:02d}"
        ports = [_free_port(reserved) for _ in range(4)]
        endpoints[vm_id] = {
            "kv": Endpoint("127.0.0.1", ports[0]),
            "membership": Endpoint("127.0.0.1", ports[1]),
            "log_query": Endpoint("127.0.0.1", ports[2]),
            "control": Endpoint("127.0.0.1", ports[3]),
        }
    return Manifest.from_endpoints(endpoints)


def replace_row(rows: list[Member], member: Member) -> None:
    index = next(i for i, row in enumerate(rows) if row.vm_id == member.vm_id)
    rows[index] = member


def wait_until(predicate, timeout: float = 8.0, description: str = "condition") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.025)
    raise AssertionError(f"timed out waiting for {description}")


class NodeRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = local_manifest()
        self.manifest_path = self.root / "manifest.json"
        self.manifest_path.write_text(self.manifest.to_json(), encoding="utf-8")

    def node(self, vm_id: str, *, bootstrap: bool = True,
             mode: ConsistencyMode | None = None, **kwargs) -> NodeRuntime:
        return NodeRuntime(
            manifest=self.manifest, vm_id=vm_id,
            database_path=self.root / f"{vm_id}.sqlite", bootstrap=bootstrap,
            bootstrap_mode=mode, **kwargs)

    @staticmethod
    def _member(node: NodeRuntime, vm_id: str) -> Member:
        return next(row for row in node.membership if row.vm_id == vm_id)

    def _wait_known_generations(self, nodes: tuple[NodeRuntime, ...], ids: tuple[str, ...]):
        wait_until(
            lambda: all(
                node.ready and all(self._member(node, peer_id).restart_generation >= 1
                                   for peer_id in ids if peer_id != node.vm_id)
                for node in nodes),
            description="membership generation exchange")

    def test_explicit_bootstrap_defaults_strong_and_stops_without_deleting_data(self):
        node = self.node("vm04", gossip_background=False).start()
        self.assertTrue(node.ready)
        self.assertEqual(node.context, Context(1, ConsistencyMode.STRONG))
        self.assertEqual(node.generation, 1)
        self.assertEqual(node.membership_runtime.table.local_generation, 1)
        self.assertEqual(len(node.owners("photo")), 5)
        self.assertEqual(node.replica_service.membership, node.membership_runtime.table)
        self.assertEqual(node.rpc_client.membership, node.membership_runtime.table)
        report = node.stop(timeout=3)
        self.assertTrue(report.clean, report)
        self.assertTrue(report.store_closed)

        # Reopening the file confirms it was released and its persisted context is intact.
        with open_store(self.root / "vm04.sqlite", vm_id="vm04") as store:
            self.assertEqual(store.current_context(), Context(1, ConsistencyMode.STRONG))
            self.assertEqual(store.start_incarnation(), 2)

    def test_explicit_disposable_eventual_bootstrap_is_supported(self):
        node = self.node("vm02", mode=ConsistencyMode.EVENTUAL_RR,
                         gossip_background=False).start()
        self.assertEqual(node.context, Context(1, ConsistencyMode.EVENTUAL_RR))
        self.assertTrue(node.stop(timeout=3).clean)

    def test_schema2_dispatch_reaches_remote_store_after_membership_converges(self):
        inventory = {
            f"vm{index:02d}": {"release_id": "local-test-build", "schema_version": 2}
            for index in range(1, 11)
        }
        activation = Schema2Activation.from_inventory(self.manifest, inventory)
        key = "schema-two-key"
        target_id = next(owner.vm_id for owner in self.manifest.owners(key)
                         if owner.vm_id != "vm01")
        first = self.node("vm01", protocol_activation=activation).start()
        second = self.node(target_id, protocol_activation=activation).start()
        try:
            self._wait_known_generations((first, second), ("vm01", target_id))
            operation_id = new_operation_id()
            version = first.store.allocate_strong_version(
                context=first.context, observed_max=None)
            write = first.dispatch_owner(
                target_id, RpcOperation.INSTALL_IF_NEWER, key,
                deadline=time.monotonic() + 3, phase="strong_write",
                operation_id=operation_id, version=version, value=b"remote bytes")
            written = write.future.result(timeout=4)
            self.assertIsInstance(written, RpcCompletion)
            self.assertEqual(written.outcome.kind, RpcOutcomeKind.RESPONSE)
            self.assertEqual((written.outcome.response.status,
                              written.outcome.response.stored_version,
                              written.outcome.response.durable),
                             (RpcStatus.INSTALLED, version, True))
            read = first.dispatch_owner(
                target_id, RpcOperation.READ_LOCAL, key,
                deadline=time.monotonic() + 3, phase="strong_query",
                operation_id=operation_id)
            result = read.future.result(timeout=4)
            self.assertEqual((result.outcome.response.status,
                              result.outcome.response.version,
                              result.outcome.response.value),
                             (RpcStatus.FOUND, version, b"remote bytes"))
            self.assertEqual(second.store.read_local(
                key, context=second.context).record.value, b"remote bytes")
        finally:
            self.assertTrue(second.stop(timeout=4).clean)
            self.assertTrue(first.stop(timeout=4).clean)

    def test_schema2_nonowner_dispatch_is_rejected_before_client_call(self):
        inventory = {
            f"vm{index:02d}": {"release_id": "local-test-build", "schema_version": 2}
            for index in range(1, 11)
        }
        activation = Schema2Activation.from_inventory(self.manifest, inventory)
        first = self.node("vm01", protocol_activation=activation).start()
        try:
            key = "nonowner-preflight"
            owner_ids = {owner.vm_id for owner in first.owners(key)}
            nonowner_id = next(f"vm{index:02d}" for index in range(1, 11)
                               if f"vm{index:02d}" not in owner_ids)
            with mock.patch.object(first.rpc_client, "call_replica",
                                   wraps=first.rpc_client.call_replica) as rpc_call:
                with self.assertRaises(OwnerDispatchError) as rejected:
                    first.dispatch_owner(
                        nonowner_id, RpcOperation.INSTALL_IF_NEWER, key,
                        deadline=time.monotonic() + 2, phase="strong_write",
                        operation_id=new_operation_id(),
                        version=Version("strong", 3, "vm01/1"), value=b"must not send")
            self.assertEqual(rejected.exception.failure.outcome,
                             OwnerOutcome.NOT_PRESCRIBED_OWNER)
            rpc_call.assert_not_called()
        finally:
            self.assertTrue(first.stop(timeout=4).clean)

    def test_schema2_suspected_owner_remains_designated_and_attemptable(self):
        inventory = {
            f"vm{index:02d}": {"release_id": "local-test-build", "schema_version": 2}
            for index in range(1, 11)
        }
        activation = Schema2Activation.from_inventory(self.manifest, inventory)
        key = "suspected-owner-key"
        target_id = next(owner.vm_id for owner in self.manifest.owners(key)
                         if owner.vm_id != "vm01")
        first = self.node("vm01", protocol_activation=activation).start()
        second = self.node(target_id, protocol_activation=activation).start()
        try:
            self._wait_known_generations((first, second), ("vm01", target_id))
            table = first.membership_runtime.table
            rows = list(table.snapshot())
            target_row = next(row for row in rows if row.vm_id == target_id)
            replace_row(rows, Member(target_id, target_row.restart_generation,
                                     target_row.incarnation, target_row.heartbeat,
                                     MemberState.SUSPECTED))
            sender_id = next(f"vm{index:02d}" for index in range(1, 11)
                             if f"vm{index:02d}" not in {"vm01", target_id})
            sender_row = next(row for row in rows if row.vm_id == sender_id)
            replace_row(rows, Member(sender_id, 1, sender_row.incarnation,
                                     sender_row.heartbeat, MemberState.ALIVE))
            table.merge_gossip(GossipEnvelope(
                self.manifest.fingerprint, sender_id, 1, tuple(rows)))
            self.assertEqual(self._member(first, target_id).status, MemberState.SUSPECTED)
            self.assertIn(target_id, {owner.vm_id for owner in first.owners(key)})

            call = first.dispatch_owner(
                target_id, RpcOperation.READ_LOCAL, key,
                deadline=time.monotonic() + 3, phase="strong_query",
                operation_id=new_operation_id())
            result = call.future.result(timeout=4)
            self.assertEqual(result.outcome.kind, RpcOutcomeKind.RESPONSE)
            self.assertEqual(result.outcome.response.status, RpcStatus.NOT_FOUND)

            self.assertTrue(second.stop(timeout=4).clean)
            failed_owner_call = first.dispatch_owner(
                target_id, RpcOperation.READ_LOCAL, key,
                deadline=time.monotonic() + 3, phase="strong_query",
                operation_id=new_operation_id())
            failed_owner = failed_owner_call.future.result(timeout=4)
            self.assertEqual(failed_owner.outcome.kind, RpcOutcomeKind.UNREACHABLE)
            self.assertIn(target_id, {owner.vm_id for owner in first.owners(key)})
        finally:
            self.assertTrue(second.stop(timeout=4).clean)
            self.assertTrue(first.stop(timeout=4).clean)

    def test_admitted_coordinator_can_dispatch_second_phase_during_graceful_stop(self):
        inventory = {
            f"vm{index:02d}": {"release_id": "local-test-build", "schema_version": 2}
            for index in range(1, 11)
        }
        activation = Schema2Activation.from_inventory(self.manifest, inventory)
        key = "shutdown-between-phases"
        target_id = next(owner.vm_id for owner in self.manifest.owners(key)
                         if owner.vm_id != "vm01")
        first = self.node("vm01", protocol_activation=activation).start()
        second = self.node(target_id, protocol_activation=activation).start()
        try:
            self._wait_known_generations((first, second), ("vm01", target_id))
            phase_one_done = threading.Event()
            allow_phase_two = threading.Event()
            admission_box = []

            def two_phase_harness(admission):
                admission_box.append(admission)
                forged = type(admission)(admission.deadline, admission.context,
                                          admission.sender_generation,
                                          admission.operation_id)
                with self.assertRaises(RuntimeError):
                    first.dispatch_owner(
                        target_id, RpcOperation.READ_LOCAL, key,
                        deadline=admission.deadline, phase="strong_query",
                        operation_id=admission.operation_id, admission=forged)
                query = first.dispatch_owner(
                    target_id, RpcOperation.READ_LOCAL, key,
                    deadline=admission.deadline, phase="strong_query",
                    operation_id=admission.operation_id, admission=admission)
                queried = query.future.result(timeout=4)
                self.assertEqual(queried.outcome.response.status, RpcStatus.NOT_FOUND)
                phase_one_done.set()
                if not allow_phase_two.wait(4):
                    raise TimeoutError("test did not release phase two")
                version = first.store.allocate_strong_version(
                    context=admission.context, observed_max=None)
                write = first.dispatch_owner(
                    target_id, RpcOperation.INSTALL_IF_NEWER, key,
                    deadline=admission.deadline, phase="strong_write",
                    operation_id=admission.operation_id, version=version,
                    value=b"phase two survived shutdown admission closure",
                    admission=admission)
                return write.future.result(timeout=4)

            operation = first.submit_coordinator(
                two_phase_harness, deadline=time.monotonic() + 8)
            self.assertIsNotNone(operation)
            self.assertTrue(phase_one_done.wait(4), "phase one did not finish")

            stop_started = threading.Event()
            stopped = []

            def stop_first():
                stop_started.set()
                stopped.append(first.stop(timeout=7))

            stopper = threading.Thread(target=stop_first, daemon=True)
            stopper.start()
            self.assertTrue(stop_started.wait(1))
            wait_until(lambda: not first.ready, description="shutdown admission closure")
            with self.assertRaises(RuntimeError):
                first.submit_coordinator(lambda _admission: None,
                                         deadline=time.monotonic() + 2)
            allow_phase_two.set()

            second_phase = operation.result(timeout=6)
            self.assertEqual(second_phase.outcome.kind, RpcOutcomeKind.RESPONSE)
            self.assertEqual(second_phase.outcome.response.status, RpcStatus.INSTALLED)
            stopper.join(timeout=7)
            self.assertFalse(stopper.is_alive())
            self.assertTrue(stopped[0].clean, stopped[0])
            self.assertTrue(stopped[0].store_closed)

            admission = admission_box[0]
            with self.assertRaises(RuntimeError):
                first.dispatch_owner(
                    target_id, RpcOperation.READ_LOCAL, key,
                    deadline=admission.deadline, phase="strong_query",
                    operation_id=admission.operation_id, admission=admission)
        finally:
            if "allow_phase_two" in locals():
                allow_phase_two.set()
            if "stopper" in locals():
                stopper.join(timeout=8)
            self.assertTrue(second.stop(timeout=4).clean)
            if (first.store is not None and
                    (first._shutdown_report is None or not first._shutdown_report.clean)):
                first.stop(timeout=4)

    def test_eventual_schema2_dispatch_uses_attempt_lease_over_loopback(self):
        inventory = {
            f"vm{index:02d}": {"release_id": "local-test-build", "schema_version": 2}
            for index in range(1, 11)
        }
        activation = Schema2Activation.from_inventory(self.manifest, inventory)
        first = self.node("vm03", mode=ConsistencyMode.EVENTUAL_NO_RR,
                          protocol_activation=activation).start()
        second = self.node("vm04", mode=ConsistencyMode.EVENTUAL_NO_RR,
                           protocol_activation=activation).start()
        try:
            self._wait_known_generations((first, second), ("vm03", "vm04"))
            operation_id = new_operation_id()
            version = first.store.allocate_eventual_version(context=first.context)
            lease = AttemptLease()
            call = first.dispatch_owner(
                "vm04", RpcOperation.INSTALL_IF_NEWER, "leased-write",
                deadline=lease.hard_deadline, phase="eventual_put",
                operation_id=operation_id, version=version, value=b"lease bytes",
                attempt_lease=lease)
            result = call.future.result(timeout=4)
            self.assertEqual(result.outcome.kind, RpcOutcomeKind.RESPONSE)
            self.assertEqual((result.outcome.response.status,
                              result.outcome.response.stored_version,
                              result.outcome.response.durable),
                             (RpcStatus.INSTALLED, version, True))
            self.assertEqual(second.store.read_local(
                "leased-write", context=second.context).record.value, b"lease bytes")
        finally:
            self.assertTrue(second.stop(timeout=4).clean)
            self.assertTrue(first.stop(timeout=4).clean)

    def test_identity_manifest_and_bootstrap_recovery_inputs_fail_closed(self):
        with self.assertRaises(ValueError):
            NodeRuntime(manifest=self.manifest, vm_id="vm11",
                        database_path=self.root / "bad.sqlite", bootstrap=True)
        with self.assertRaises(ValueError):
            NodeRuntime(manifest=self.manifest, vm_id="vm01",
                        database_path=self.root / "bad.sqlite", bootstrap=False,
                        bootstrap_mode=ConsistencyMode.STRONG)
        with self.assertRaises(StorageError):
            self.node("vm01", bootstrap=False).start()
        with self.assertRaises(SystemExit) as invalid_mode:
            node_main(["--vm-id", "vm01", "--manifest", str(self.manifest_path),
                       "--db", str(self.root / "cli.sqlite"), "--recover",
                       "--bootstrap-mode", "strong"])
        self.assertEqual(invalid_mode.exception.code, 2)

        corrupt = self.root / "corrupt.sqlite"
        corrupt.write_bytes(b"not a database")
        with self.assertRaises(StorageError):
            NodeRuntime(manifest=self.manifest, vm_id="vm01", database_path=corrupt,
                        bootstrap=False).start()
        invalid_manifest = json.loads(self.manifest.to_json())
        invalid_manifest["nodes"][0]["vm_id"] = "vm99"
        invalid_path = self.root / "invalid-manifest.json"
        invalid_path.write_text(json.dumps(invalid_manifest), encoding="utf-8")
        with self.assertRaises(ValueError):
            NodeRuntime.from_files(vm_id="vm01", manifest_path=invalid_path,
                                   database_path=self.root / "bad-manifest.sqlite",
                                   bootstrap=True)
        self.assertFalse(any(thread.name.startswith("mp3-membership")
                             for thread in threading.enumerate()))

    def test_sqlite_database_lock_fails_startup_within_store_budget(self):
        path = self.root / "locked.sqlite"
        with open_store(path, bootstrap=Context(1, ConsistencyMode.STRONG), vm_id="vm01"):
            pass
        blocker = sqlite3.connect(path, isolation_level=None)
        blocker.execute("BEGIN IMMEDIATE")
        node = NodeRuntime(manifest=self.manifest, vm_id="vm01", database_path=path,
                           bootstrap=False, store_timeout=0.1)
        started = time.monotonic()
        try:
            with self.assertRaises(StorageError):
                node.start()
        finally:
            elapsed = time.monotonic() - started
            blocker.execute("ROLLBACK")
            blocker.close()
        self.assertLess(elapsed, 1.0)
        self.assertFalse(node.ready)
        with open_store(path, vm_id="vm01") as recovered:
            self.assertEqual(recovered.current_context(), Context(1, ConsistencyMode.STRONG))

    def test_existing_database_is_never_implicitly_bootstrapped_or_reidentified(self):
        path = self.root / "identity.sqlite"
        with open_store(path, bootstrap=Context(1, ConsistencyMode.STRONG), vm_id="vm01"):
            pass
        with self.assertRaises(StorageError):
            NodeRuntime(manifest=self.manifest, vm_id="vm01", database_path=path,
                        bootstrap=True).start()
        with self.assertRaises(StorageError):
            NodeRuntime(manifest=self.manifest, vm_id="vm02", database_path=path,
                        bootstrap=False).start()
        # Constructor rejection releases the process lock for later legitimate recovery.
        with open_store(path, vm_id="vm01") as recovered:
            self.assertEqual(recovered.current_context(), Context(1, ConsistencyMode.STRONG))

    def test_failure_after_membership_start_releases_listener_and_store_lock(self):
        occupied = socket.socket()
        occupied.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        occupied.bind(("127.0.0.1", self.manifest.node("vm04").kv.port))
        occupied.listen()
        node = self.node("vm04", gossip_background=False)
        try:
            with self.assertRaises(OSError):
                node.start()
            self.assertFalse(node.ready)
            self.assertTrue(node._shutdown_report.store_closed)
        finally:
            occupied.close()

        endpoint = self.manifest.node("vm04").membership
        with socket.socket() as probe:
            probe.bind((endpoint.host, endpoint.port))
        self.assertFalse(any(
            thread.name.startswith(("mp3-membership-accept", "mp3-membership-gossip-loop",
                                    "mp3-kv-accept", "mp3-kv-handler"))
            for thread in threading.enumerate()))
        with open_store(self.root / "vm04.sqlite", vm_id="vm04") as recovered:
            self.assertEqual(recovered.current_context(), Context(1, ConsistencyMode.STRONG))

    def test_membership_port_conflict_closes_database_without_starting_kv(self):
        endpoint = self.manifest.node("vm03").membership
        occupied = socket.socket()
        occupied.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        occupied.bind((endpoint.host, endpoint.port))
        occupied.listen()
        node = self.node("vm03", gossip_background=False)
        try:
            with self.assertRaises(OSError):
                node.start()
        finally:
            occupied.close()
        self.assertFalse(node.ready)
        with socket.socket() as probe:
            probe.bind((self.manifest.node("vm03").kv.host,
                        self.manifest.node("vm03").kv.port))
        self.assertTrue(node._shutdown_report.store_closed)

    def test_gossip_thread_start_failure_rolls_back_membership_listener_and_store(self):
        node = self.node("vm08", gossip_background=True)
        original_start = threading.Thread.start

        def fail_gossip_start(thread):
            if thread.name == "mp3-membership-gossip-loop":
                raise RuntimeError("injected gossip start failure")
            return original_start(thread)

        with mock.patch.object(threading.Thread, "start", fail_gossip_start):
            with self.assertRaisesRegex(RuntimeError, "injected gossip start failure"):
                node.start()

        self.assertFalse(node.ready)
        self.assertTrue(node._shutdown_report.membership_stopped)
        self.assertTrue(node._shutdown_report.store_closed)
        endpoint = self.manifest.node("vm08").membership
        with socket.socket() as probe:
            probe.bind((endpoint.host, endpoint.port))
        with open_store(self.root / "vm08.sqlite", vm_id="vm08") as recovered:
            self.assertEqual(recovered.current_context(), Context(1, ConsistencyMode.STRONG))

    def test_kv_accept_thread_start_failure_rolls_back_workers_listener_and_store(self):
        node = self.node("vm09", gossip_background=False, replica_workers=2)
        original_start = threading.Thread.start

        def fail_accept_start(thread):
            if thread.name == "mp3-kv-accept-vm09":
                raise RuntimeError("injected KV accept start failure")
            return original_start(thread)

        with mock.patch.object(threading.Thread, "start", fail_accept_start):
            with self.assertRaisesRegex(RuntimeError, "injected KV accept start failure"):
                node.start()

        self.assertFalse(node.ready)
        self.assertTrue(node._shutdown_report.membership_stopped)
        self.assertTrue(node._shutdown_report.store_closed)
        self.assertTrue(node._shutdown_report.replica.workers_joined)
        for endpoint in (self.manifest.node("vm09").membership,
                         self.manifest.node("vm09").kv):
            with socket.socket() as probe:
                probe.bind((endpoint.host, endpoint.port))
        with open_store(self.root / "vm09.sqlite", vm_id="vm09") as recovered:
            self.assertEqual(recovered.current_context(), Context(1, ConsistencyMode.STRONG))

    def test_forced_node_shutdown_reports_incomplete_active_and_queued_requests(self):
        node = self.node("vm05", gossip_background=False,
                         replica_workers=1, replica_queue=1).start()
        active = threading.Event()
        release = threading.Event()

        def blocked_handler(_connection, _deadline):
            active.set()
            release.wait(4)

        node.replica_service._handle = blocked_handler
        first = socket.create_connection((self.manifest.node("vm05").kv.host,
                                          self.manifest.node("vm05").kv.port), timeout=1)
        second = None
        worker = node.replica_service._workers[0]
        try:
            self.assertTrue(active.wait(1))
            second = socket.create_connection((self.manifest.node("vm05").kv.host,
                                               self.manifest.node("vm05").kv.port), timeout=1)
            wait_until(lambda: node.replica_service._outstanding == 2,
                       description="active and queued requests admitted")
            report = node.stop(timeout=0.05)
            self.assertFalse(report.clean)
            self.assertEqual(report.incomplete_requests, 2)
            self.assertFalse(report.replica.workers_joined)
            self.assertFalse(report.store_closed)
        finally:
            release.set()
            worker.join(timeout=2)
            first.close()
            if second is not None:
                second.close()
            if worker.is_alive():
                self.fail("forced test worker remained alive after its gate was released")
        completed = node.stop(timeout=3)
        self.assertTrue(completed.clean, completed)
        self.assertTrue(completed.store_closed)
        with open_store(self.root / "vm05.sqlite", vm_id="vm05") as reopened:
            self.assertEqual(reopened.current_context(), Context(1, ConsistencyMode.STRONG))

    def test_admitted_call_keeps_context_and_generation_through_shutdown(self):
        node = self.node("vm01", gossip_background=False).start()
        self.addCleanup(node.stop)
        constructing = threading.Event()
        allow_construction = threading.Event()
        shutdown_entered = threading.Event()
        draining_call = threading.Event()
        call_finished = threading.Event()
        stop_finished = threading.Event()
        captured = []
        rpc_result = object()
        call_errors = []
        stop_results = []
        real_new_request = new_request
        real_service_stop = node.replica_service.stop
        real_condition_wait = node._calls.wait

        def gated_new_request(**kwargs):
            constructing.set()
            if not allow_construction.wait(3):
                raise AssertionError("request construction gate was not released")
            return real_new_request(**kwargs)

        def fake_call(target, request, *, deadline):
            captured.append((target.vm_id, request, deadline))
            return rpc_result

        def observed_service_stop(*, timeout=None):
            shutdown_entered.set()
            return real_service_stop(timeout=timeout)

        def observed_condition_wait(timeout=None):
            draining_call.set()
            return real_condition_wait(timeout)

        node.rpc_client.call_replica = fake_call
        node.replica_service.stop = observed_service_stop
        node._calls.wait = observed_condition_wait
        request_patch = mock.patch("mp3.node.new_request", side_effect=gated_new_request)
        request_patch.start()
        self.addCleanup(request_patch.stop)

        def invoke():
            try:
                result = node.call_replica(
                    "vm02", RpcOperation.INSTALL_IF_NEWER, "admitted",
                    deadline=time.monotonic() + 4,
                    version=Version("strong", 9, "vm01/1"), value=b"captured")
                self.assertIs(result, rpc_result)
            except BaseException as error:
                call_errors.append(error)
            finally:
                call_finished.set()

        def stop_node():
            try:
                stop_results.append(node.stop(timeout=3))
            except BaseException as error:
                call_errors.append(error)
            finally:
                stop_finished.set()

        caller = threading.Thread(target=invoke)
        stopper = threading.Thread(target=stop_node)
        caller.start()
        try:
            self.assertTrue(constructing.wait(2), "call did not reach request construction")
            self.assertEqual(node._active_calls, 1)
            stopper.start()
            self.assertTrue(shutdown_entered.wait(2), "shutdown did not close admission")
            self.assertFalse(node.ready)
            self.assertTrue(draining_call.wait(3), "shutdown did not wait for admitted call")
            self.assertFalse(stop_finished.is_set(), "shutdown skipped an admitted call")
            allow_construction.set()
            self.assertTrue(call_finished.wait(2), "admitted RPC did not finish")
            self.assertTrue(stop_finished.wait(4), "shutdown did not finish after RPC")
            stopper.join(timeout=1)
        finally:
            allow_construction.set()
            caller.join(timeout=2)
            if stopper.ident is not None:
                stopper.join(timeout=4)

        self.assertFalse(caller.is_alive())
        self.assertEqual(call_errors, [])
        self.assertEqual(len(captured), 1)
        target_id, request, _deadline = captured[0]
        self.assertEqual(target_id, "vm02")
        self.assertEqual(request.sender_generation, 1)
        self.assertEqual((request.epoch, request.mode), (1, ConsistencyMode.STRONG))
        self.assertEqual((request.op, request.version, request.value),
                         (RpcOperation.INSTALL_IF_NEWER,
                          Version("strong", 9, "vm01/1"), b"captured"))
        self.assertEqual(node._active_calls, 0)
        self.assertTrue(stop_results[0].clean, stop_results[0])
        self.assertTrue(stop_results[0].store_closed)

    def test_shutdown_closes_call_admission_before_later_rpc_attempt(self):
        node = self.node("vm02", gossip_background=False).start()
        self.addCleanup(node.stop)
        service_stop_entered = threading.Event()
        release_service_stop = threading.Event()
        stop_finished = threading.Event()
        stop_results = []
        stop_errors = []
        real_service_stop = node.replica_service.stop

        def gated_service_stop(*, timeout=None):
            service_stop_entered.set()
            if not release_service_stop.wait(3):
                raise AssertionError("service shutdown gate was not released")
            return real_service_stop(timeout=timeout)

        node.replica_service.stop = gated_service_stop

        def stop_node():
            try:
                stop_results.append(node.stop(timeout=4))
            except BaseException as error:
                stop_errors.append(error)
            finally:
                stop_finished.set()

        stopper = threading.Thread(target=stop_node)
        stopper.start()
        try:
            self.assertTrue(service_stop_entered.wait(2), "shutdown did not begin")
            self.assertFalse(node.ready)
            with self.assertRaisesRegex(RuntimeError, "not accepting RPC calls"):
                node.call_replica("vm03", RpcOperation.READ_LOCAL, "too-late",
                                  deadline=time.monotonic() + 2)
            self.assertEqual(node._active_calls, 0)
        finally:
            release_service_stop.set()
            stopper.join(timeout=5)
        self.assertFalse(stopper.is_alive())
        self.assertEqual(stop_errors, [])
        self.assertTrue(stop_finished.is_set())
        self.assertTrue(stop_results[0].clean, stop_results[0])

    def test_forced_node_stop_reports_admitted_rpc_and_closes_store_only_after_retry(self):
        node = self.node("vm03", gossip_background=False).start()
        self.addCleanup(node.stop)
        entered_rpc = threading.Event()
        release_rpc = threading.Event()
        call_finished = threading.Event()
        call_errors = []
        rpc_result = object()

        def blocked_rpc(_target, _request, *, deadline):
            entered_rpc.set()
            if not release_rpc.wait(4):
                raise AssertionError("RPC release gate was not signaled")
            return rpc_result

        node.rpc_client.call_replica = blocked_rpc

        def invoke():
            try:
                self.assertIs(node.call_replica(
                    "vm04", RpcOperation.READ_LOCAL, "forced",
                    deadline=time.monotonic() + 5), rpc_result)
            except BaseException as error:
                call_errors.append(error)
            finally:
                call_finished.set()

        caller = threading.Thread(target=invoke)
        caller.start()
        try:
            self.assertTrue(entered_rpc.wait(2), "RPC was not admitted")
            report = node.stop(timeout=0.05)
            self.assertFalse(report.clean)
            self.assertEqual(report.outstanding_rpc_calls, 1)
            self.assertFalse(report.store_closed)
            self.assertFalse(node.store._closed)
            self.assertEqual(node._active_calls, 1)
        finally:
            release_rpc.set()
            caller.join(timeout=2)

        self.assertFalse(caller.is_alive())
        self.assertTrue(call_finished.is_set())
        self.assertEqual(call_errors, [])
        self.assertEqual(node._active_calls, 0)
        completed = node.stop(timeout=3)
        self.assertTrue(completed.clean, completed)
        self.assertTrue(completed.store_closed)
        with open_store(self.root / "vm03.sqlite", vm_id="vm03") as reopened:
            self.assertEqual(reopened.current_context(), Context(1, ConsistencyMode.STRONG))

    def test_real_three_node_gossip_rpc_binary_values_and_concurrency(self):
        nodes = tuple(self.node(f"vm{number:02d}", gossip_background=True,
                               replica_workers=6, replica_queue=12).start()
                      for number in (1, 2, 3))
        for node in nodes:
            self.addCleanup(node.stop)
        self._wait_known_generations(nodes, ("vm01", "vm02", "vm03"))
        owners = tuple(tuple(node.owners("photo")) for node in nodes)
        self.assertEqual(owners[0], owners[1])
        self.assertEqual(owners[1], owners[2])

        node1, node2, node3 = nodes
        missing = node1.call_replica("vm02", RpcOperation.READ_LOCAL, "empty-or-missing",
                                     deadline=time.monotonic() + 3)
        self.assertEqual(missing.response.status, RpcStatus.NOT_FOUND)
        empty_write = node3.call_replica(
            "vm01", RpcOperation.INSTALL_IF_NEWER, "empty-or-missing",
            deadline=time.monotonic() + 3,
            version=Version("strong", 1, "vm03/1"), value=b"")
        self.assertEqual(empty_write.response.status, RpcStatus.INSTALLED)
        empty_read = node2.call_replica("vm01", RpcOperation.READ_LOCAL, "empty-or-missing",
                                        deadline=time.monotonic() + 3)
        self.assertEqual((empty_read.response.status, empty_read.response.value),
                         (RpcStatus.FOUND, b""))

        max_value = bytes(range(256)) * (MAX_VALUE_BYTES // 256)
        installed = node1.call_replica(
            "vm02", RpcOperation.INSTALL_IF_NEWER, "maximum-binary",
            deadline=time.monotonic() + 5,
            version=Version("strong", 5, "vm01/1"), value=max_value)
        self.assertEqual((installed.response.status, installed.response.durable),
                         (RpcStatus.INSTALLED, True))
        readback = node2.call_replica("vm02", RpcOperation.READ_LOCAL, "maximum-binary",
                                      deadline=time.monotonic() + 5)
        self.assertEqual(readback.response.value, max_value)
        stored = node2.store.read_local("maximum-binary", context=node2.context)
        self.assertEqual((stored.status, stored.record.version.number, stored.record.value),
                         (LocalStatus.FOUND, 5, max_value))

        # Two independent requesters concurrently install on one replica while the
        # background membership loops keep exchanging real TCP snapshots.
        def write(index: int):
            source = node1 if index % 2 == 0 else node3
            return source.call_replica(
                "vm02", RpcOperation.INSTALL_IF_NEWER, "racing",
                deadline=time.monotonic() + 4,
                version=Version("strong", index + 10, f"{source.vm_id}/{source.generation}"),
                value=str(index).encode())

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(write, range(12)))
        self.assertTrue(all(result.kind == RpcOutcomeKind.RESPONSE for result in outcomes))
        self.assertEqual(node2.store.read_local("racing", context=node2.context).record.version.number,
                         21)
        self.assertTrue(any(self._member(node, "vm02").heartbeat > 0 for node in nodes))

    def _start_cli(self, vm_id: str, db: Path, *, bootstrap: bool) -> subprocess.Popen:
        command = [sys.executable, "-B", "-m", "mp3.node", "--vm-id", vm_id,
                   "--manifest", str(self.manifest_path), "--db", str(db),
                   "--bootstrap" if bootstrap else "--recover"]
        environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        process = subprocess.Popen(command, cwd=ROOT, env=environment,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, bufsize=1)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        try:
            events = selector.select(timeout=10)
            if not events:
                process.kill()
                stdout, stderr = process.communicate(timeout=5)
                self.fail(f"node CLI did not become ready; stdout={stdout!r}; stderr={stderr!r}")
            line = process.stdout.readline().strip()
        finally:
            selector.close()
        if not line.startswith(f"READY {vm_id} "):
            process.kill()
            stdout, stderr = process.communicate(timeout=5)
            self.fail(f"node CLI failed startup: {line!r}; stdout={stdout!r}; stderr={stderr!r}")
        process.ready_line = line
        return process

    @staticmethod
    def _stop_cli(process: subprocess.Popen, *, signal_number=signal.SIGTERM):
        if process.poll() is None:
            process.send_signal(signal_number)
        remaining, stderr = process.communicate(timeout=8)
        return process.returncode, remaining, stderr

    def test_subprocess_sigkill_recovery_gossip_and_stale_evidence(self):
        requester = self.node("vm01", gossip_background=True).start()
        self.addCleanup(requester.stop)
        peer = None
        remote = None
        db = self.root / "vm06.sqlite"
        try:
            peer = self._start_cli("vm02", self.root / "vm02.sqlite", bootstrap=True)
            remote = self._start_cli("vm06", db, bootstrap=True)
            self.assertIn("generation=1", remote.ready_line)
            wait_until(lambda: (self._member(requester, "vm06").restart_generation == 1
                                and self._member(requester, "vm02").restart_generation == 1),
                       description="requester learns initial VM6 generation")
            wait_until(lambda: self._target_accepts_requester(requester, "vm06"),
                       description="target learns requester's generation")
            storage_key = next(
                f"crash-owner-{index}" for index in range(1000)
                if "vm06" in {node.vm_id for node in self.manifest.owners(f"crash-owner-{index}")})
            version = Version("strong", 77, "vm01/1")
            outcome = requester.call_replica(
                "vm06", RpcOperation.INSTALL_IF_NEWER, storage_key,
                deadline=time.monotonic() + 4, version=version, value=b"durable bytes")
            self.assertEqual((outcome.kind, outcome.response.status),
                             (RpcOutcomeKind.RESPONSE, RpcStatus.INSTALLED))
            owners_before = tuple(node.vm_id for node in requester.owners(storage_key))
            self.assertIn("vm06", owners_before)

            stale_table = MembershipTable(self.manifest, "vm01")
            stale_table.set_local_generation(requester.generation)
            stale_rows = list(stale_table.snapshot())
            replace_row(stale_rows, Member("vm01", requester.generation, 0, 0,
                                           MemberState.ALIVE))
            replace_row(stale_rows, Member("vm02", 1, 0, 1, MemberState.ALIVE))
            replace_row(stale_rows, Member("vm06", 1, 0, 1, MemberState.ALIVE))
            stale_table.merge_gossip(GossipEnvelope(
                self.manifest.fingerprint, "vm02", 1, tuple(stale_rows)))
            stale_client = ReplicaClient(self.manifest, stale_table)

            remote.send_signal(signal.SIGKILL)
            remote.wait(timeout=5)
            remote.stdout.close()
            remote.stderr.close()
            self.assertEqual(tuple(node.vm_id for node in requester.owners(storage_key)),
                             owners_before)

            remote = self._start_cli("vm06", db, bootstrap=False)
            self.assertIn("generation=2", remote.ready_line)
            wait_until(lambda: self._member(requester, "vm06").restart_generation == 2,
                       description="peer learns VM6 recovered generation")
            wait_until(lambda: self._target_accepts_requester(requester, "vm06"),
                       description="recovered service learns requester's generation")

            # A caller holding old target-generation evidence must reject VM6's
            # generation-2 response rather than count it as a current reply.
            stale_request = new_request(
                manifest=self.manifest, sender_id="vm01", sender_generation=1,
                target_id="vm06", epoch=1, mode=ConsistencyMode.STRONG,
                op=RpcOperation.READ_LOCAL, key=storage_key)
            stale_outcome = stale_client.call_replica(
                self.manifest.node("vm06"), stale_request,
                deadline=time.monotonic() + 3)
            self.assertEqual(stale_outcome.kind, RpcOutcomeKind.INVALID_RESPONSE)

            # A request naming the old requester generation is rejected by the
            # recovered replica; it cannot fabricate absence or mutate storage.
            stale_sender = new_request(
                manifest=self.manifest, sender_id="vm01", sender_generation=0,
                target_id="vm06", epoch=1, mode=ConsistencyMode.STRONG,
                op=RpcOperation.INSTALL_IF_NEWER, key="stale-sender",
                version=Version("strong", 80, "vm01/0"), value=b"must reject")
            with socket.create_connection((self.manifest.node("vm06").kv.host,
                                           self.manifest.node("vm06").kv.port), timeout=1) as sock:
                from mp3.rpc import send_request
                deadline = time.monotonic() + 3
                send_request(sock, stale_sender, self.manifest, deadline=deadline)
                sock.shutdown(socket.SHUT_WR)
                denied = receive_response(sock, self.manifest, deadline=deadline)
            self.assertEqual((denied.status, denied.error), (RpcStatus.ERROR, "INVALID_REQUEST"))

            current = requester.call_replica(
                "vm06", RpcOperation.READ_LOCAL, storage_key,
                deadline=time.monotonic() + 4)
            self.assertEqual((current.kind, current.response.status, current.response.value,
                              current.response.version),
                             (RpcOutcomeKind.RESPONSE, RpcStatus.FOUND, b"durable bytes", version))
            self.assertEqual(tuple(node.vm_id for node in requester.owners(storage_key)),
                             owners_before)

            old_rows = list(requester.membership_runtime.table.snapshot())
            replace_row(old_rows, Member("vm02", 1, 0, 1, MemberState.ALIVE))
            replace_row(old_rows, Member("vm06", 1, 0, 999, MemberState.ALIVE))
            old_gossip = GossipEnvelope(self.manifest.fingerprint, "vm02", 1,
                                        tuple(old_rows))
            requester.membership_runtime.table.merge_gossip(old_gossip)
            self.assertEqual(self._member(requester, "vm06").restart_generation, 2)
        finally:
            if remote is not None and remote.poll() is None:
                status, output, stderr = self._stop_cli(remote)
                self.assertEqual(status, 0, (output, stderr))
            if peer is not None and peer.poll() is None:
                status, output, stderr = self._stop_cli(peer)
                self.assertEqual(status, 0, (output, stderr))

    def _target_accepts_requester(self, requester: NodeRuntime, target_id: str) -> bool:
        """A valid FOUND reply proves the remote membership accepts this generation."""
        result = requester.call_replica(
            target_id, RpcOperation.READ_LOCAL, "crash-owner-0",
            deadline=time.monotonic() + 2)
        return (result.kind == RpcOutcomeKind.RESPONSE
                and result.response is not None
                and result.response.status in (RpcStatus.FOUND, RpcStatus.NOT_FOUND))

    def test_sigterm_shutdown_reports_success_and_recovery_keeps_context(self):
        db = self.root / "vm08.sqlite"
        process = self._start_cli("vm08", db, bootstrap=True)
        self.assertIn("context=strong@1", process.ready_line)
        status, output, stderr = self._stop_cli(process)
        self.assertEqual(status, 0, (output, stderr))
        self.assertIn("STOPPED clean=true", output)

        recovered = self._start_cli("vm08", db, bootstrap=False)
        self.assertIn("generation=2", recovered.ready_line)
        self.assertIn("context=strong@1", recovered.ready_line)
        status, output, stderr = self._stop_cli(recovered)
        self.assertEqual(status, 0, (output, stderr))
        self.assertIn("STOPPED clean=true", output)


if __name__ == "__main__":
    unittest.main()
