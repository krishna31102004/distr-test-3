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
from mp3.models import ConsistencyMode, Context, LocalStatus, Version
from mp3.node import NodeRuntime, main as node_main
from mp3.ring import Endpoint, Manifest
from mp3.rpc import (ReplicaClient, RpcOperation, RpcOutcomeKind, RpcStatus,
                     new_request, receive_response, send_request)
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
        ports = [_free_port(reserved) for _ in range(3)]
        endpoints[vm_id] = {
            "kv": Endpoint("127.0.0.1", ports[0]),
            "membership": Endpoint("127.0.0.1", ports[1]),
            "log_query": Endpoint("127.0.0.1", ports[2]),
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
