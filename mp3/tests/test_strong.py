"""Local TCP integration tests for the Strong ABD coordinator."""

from concurrent.futures import ThreadPoolExecutor
import json
import random
from pathlib import Path
import selectors
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from mp3.coordinator import OperationStatus, RpcCompletion, Schema2Activation
from mp3.models import ConsistencyMode, Record, Version
from mp3.node import NodeRuntime
from mp3.ring import Endpoint, Manifest
from mp3.rpc import (
    RpcOperation, RpcOutcome, RpcOutcomeKind, RpcStatus, new_operation_id,
    new_request, receive_response, send_request,
)
from mp3.tests.history_checker import HistoryEntry, check_register_history


ROOT = Path(__file__).resolve().parents[2]
MAX_VALUE_BYTES = 1_048_576
NODE_PROCESS_SOURCE = r'''
import json, sys
from mp3.models import ConsistencyMode
from mp3.node import NodeRuntime

vm_id, manifest, database, inventory = sys.argv[1:]
node = NodeRuntime.from_files(
    vm_id=vm_id, manifest_path=manifest, database_path=database,
    bootstrap=True, schema2_inventory_path=inventory).start()
print(json.dumps({"event": "ready", "vm_id": vm_id,
                  "generation": node.generation}), flush=True)
for line in sys.stdin:
    command = line.strip()
    if command == "snapshot":
        rows = node.membership_runtime.table.snapshot()
        print(json.dumps({"event": "snapshot", "members": {
            row.vm_id: row.restart_generation for row in rows}}), flush=True)
    elif command == "stop":
        report = node.stop(timeout=5)
        print(json.dumps({"event": "stopped", "clean": report.clean}), flush=True)
        break
'''


def _free_port(reserved: set[int]) -> int:
    while True:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        if port not in reserved:
            reserved.add(port)
            return port


def _manifest() -> Manifest:
    reserved: set[int] = set()
    endpoints = {}
    for index in range(1, 11):
        vm_id = f"vm{index:02d}"
        ports = [_free_port(reserved) for _ in range(3)]
        endpoints[vm_id] = {
            "kv": Endpoint("127.0.0.1", ports[0]),
            "membership": Endpoint("127.0.0.1", ports[1]),
            "log_query": Endpoint("127.0.0.1", ports[2]),
        }
    return Manifest.from_endpoints(endpoints)


class NodeProcess:
    """Independent loopback node process with a test-only membership snapshot pipe."""

    def __init__(self, vm_id: str, manifest_path: Path, database_path: Path,
                 inventory_path: Path, *, bootstrap: bool):
        source = NODE_PROCESS_SOURCE.replace(
            "bootstrap=True, schema2_inventory_path=inventory",
            f"bootstrap={bootstrap!r}, schema2_inventory_path=inventory")
        self.process = subprocess.Popen(
            [sys.executable, "-B", "-c", source, vm_id, str(manifest_path),
             str(database_path), str(inventory_path)],
            cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1)
        ready = self._read_line(12)
        if ready.get("event") != "ready":
            raise AssertionError(f"node subprocess failed to start: {ready}")
        self.generation = ready["generation"]

    def _read_line(self, timeout: float) -> dict:
        if self.process.stdout is None:
            raise AssertionError("node subprocess stdout is unavailable")
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            if not selector.select(timeout):
                stderr = "" if self.process.stderr is None else self.process.stderr.read()
                raise AssertionError(f"node subprocess timed out; stderr={stderr}")
        line = self.process.stdout.readline()
        if not line:
            stderr = "" if self.process.stderr is None else self.process.stderr.read()
            raise AssertionError(f"node subprocess exited {self.process.poll()}; stderr={stderr}")
        return json.loads(line)

    def snapshot(self) -> dict[str, int]:
        assert self.process.stdin is not None
        self.process.stdin.write("snapshot\n")
        self.process.stdin.flush()
        reply = self._read_line(3)
        if reply.get("event") != "snapshot":
            raise AssertionError(f"unexpected node subprocess reply: {reply}")
        return reply["members"]

    def kill(self) -> int:
        if self.process.poll() is None:
            self.process.kill()
        result = self.process.wait(timeout=5)
        self._close_pipes()
        return result

    def stop(self) -> bool:
        if self.process.poll() is not None:
            return False
        assert self.process.stdin is not None
        self.process.stdin.write("stop\n")
        self.process.stdin.flush()
        reply = self._read_line(8)
        self.process.wait(timeout=5)
        self._close_pipes()
        return reply.get("event") == "stopped" and reply.get("clean") is True

    def _close_pipes(self) -> None:
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if stream is not None and not stream.closed:
                stream.close()


class StrongIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.manifest = _manifest()
        self.manifest_path = self.root / "manifest.json"
        self.manifest_path.write_text(self.manifest.to_json(), encoding="utf-8")
        self.inventory_path = self.root / "schema2.json"
        self.inventory_path.write_text(json.dumps({
            "manifest_id": self.manifest.fingerprint,
            "nodes": {
                f"vm{index:02d}": {"release_id": "test-build", "schema_version": 2}
                for index in range(1, 11)
            },
        }), encoding="utf-8")
        self.activation = Schema2Activation.from_file(
            self.manifest, self.inventory_path)
        self.nodes: dict[str, NodeRuntime] = {}
        self.processes: list[NodeProcess] = []
        self.addCleanup(self.close_cluster)

    def node(self, vm_id: str) -> NodeRuntime:
        return NodeRuntime(
            manifest=self.manifest, vm_id=vm_id,
            database_path=self.root / f"{vm_id}.sqlite", bootstrap=True,
            protocol_activation=self.activation, replica_workers=4, replica_queue=16,
            coordinator_workers=4, coordinator_queue=8,
            attempt_workers=8, attempt_queue=24)

    def start_nodes(self, vm_ids):
        for vm_id in vm_ids:
            if vm_id not in self.nodes:
                self.nodes[vm_id] = self.node(vm_id).start()
        self.wait_membership(set(vm_ids))

    def start_process(self, vm_id: str, *, bootstrap=True) -> NodeProcess:
        child = NodeProcess(vm_id, self.manifest_path,
                            self.root / f"{vm_id}.sqlite",
                            self.inventory_path, bootstrap=bootstrap)
        self.processes.append(child)
        return child

    def wait_membership(self, participant_ids: set[str], timeout=12):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all(all(
                    next(row for row in node.membership if row.vm_id == peer).restart_generation > 0
                    for peer in participant_ids if peer != vm_id)
                   for vm_id, node in self.nodes.items()
                   if vm_id in participant_ids and node.ready):
                # If there is a subprocess, also require that it learned every live
                # in-process identity. This makes request-generation checks deterministic.
                if all(all(snapshot.get(peer, 0) > 0 for peer in participant_ids
                           if peer != process_id)
                       for process_id, child in self._process_by_id().items()
                       for snapshot in (child.snapshot(),)):
                    return
            time.sleep(0.025)
        state = {vm_id: {row.vm_id: row.restart_generation for row in node.membership}
                 for vm_id, node in self.nodes.items() if node.ready}
        raise AssertionError(f"membership did not converge for {participant_ids}: {state}")

    def _process_by_id(self) -> dict[str, NodeProcess]:
        result = {}
        for child in self.processes:
            if child.process.poll() is None:
                # Stable identity is included in the ready line; discover it from the
                # database filename, whose stem is the VM identity.
                vm_id = Path(child.process.args[-2]).stem
                result[vm_id] = child
        return result

    def close_cluster(self):
        for child in reversed(self.processes):
            if child.process.poll() is None:
                try:
                    child.stop()
                except Exception:
                    child.kill()
        for node in reversed(tuple(self.nodes.values())):
            if node.store is not None and (node._shutdown_report is None
                                           or not node._shutdown_report.clean):
                try:
                    node.stop(timeout=6)
                except Exception:
                    pass
        self.temp.cleanup()

    def participants_for(self, key: str, *, coordinators: tuple[str, ...] = ()):
        owners = self.manifest.owners(key)
        ids = tuple(node.vm_id for node in owners)
        participant_ids = set(ids) | set(coordinators)
        self.start_nodes(participant_ids)
        return owners, ids

    def test_nonowner_coordinator_empty_binary_replacement_and_one_mib_value(self):
        key = "unicode\0雪"
        owners = self.manifest.owners(key)
        owner_ids = tuple(node.vm_id for node in owners)
        nonowner = next(f"vm{index:02d}" for index in range(1, 11)
                        if f"vm{index:02d}" not in owner_ids)
        self.start_nodes(set(owner_ids) | {nonowner})
        coordinator = self.nodes[nonowner]
        self.assertNotIn(nonowner, owner_ids)

        absent = coordinator.strong_get(key, deadline=time.monotonic() + 5)
        self.assertEqual(absent.status, OperationStatus.NOT_FOUND, absent)

        empty_put = coordinator.strong_put(key, b"", deadline=time.monotonic() + 6)
        self.assertEqual(empty_put.status, OperationStatus.OK, empty_put)
        self.assertIsNone(empty_put.value)
        self.assertEqual(len(empty_put.qualifying_owners), 3)
        empty_get = self.nodes[owner_ids[0]].strong_get(
            key, deadline=time.monotonic() + 6)
        self.assertEqual((empty_get.status, empty_get.value),
                         (OperationStatus.OK, b""), empty_get)

        replacement = b"replaced"
        replaced = coordinator.strong_put(
            key, replacement, deadline=time.monotonic() + 6)
        self.assertEqual(replaced.status, OperationStatus.OK, replaced)
        self.assertGreater(replaced.version.compare(empty_put.version), 0)
        self.assertEqual(self.nodes[owner_ids[1]].strong_get(
            key, deadline=time.monotonic() + 6).value, replacement)

        full_key = "full-binary-value"
        full_owner_ids = {node.vm_id for node in self.manifest.owners(full_key)}
        self.start_nodes(full_owner_ids | {nonowner})
        raw = bytes((index * 73) % 256 for index in range(MAX_VALUE_BYTES))
        full_put = coordinator.strong_put(
            full_key, raw, deadline=time.monotonic() + 6)
        self.assertEqual(full_put.status, OperationStatus.OK, full_put)
        full_get = self.nodes[owner_ids[2]].strong_get(
            full_key, deadline=time.monotonic() + 6)
        self.assertEqual((full_get.status, full_get.value),
                         (OperationStatus.OK, raw), full_get)
        self.assertIsNotNone(full_get.version)

        invalid = coordinator.strong_put("bad", None, deadline=time.monotonic() + 2)
        self.assertEqual(invalid.status, OperationStatus.ERROR)
        oversized = coordinator.strong_put(
            "too-large", b"x" * (MAX_VALUE_BYTES + 1), deadline=time.monotonic() + 2)
        self.assertEqual(oversized.status, OperationStatus.ERROR)

    def test_quorum_succeeds_with_two_unavailable_and_never_fakes_absence(self):
        key = "quorum-availability"
        owners, owner_ids = self.participants_for(key)
        coordinator_id = owner_ids[0]
        coordinator = self.nodes[coordinator_id]
        original_owners = tuple(node.vm_id for node in coordinator.owners(key))

        down = owner_ids[1:3]
        for vm_id in down:
            self.assertTrue(self.nodes[vm_id].stop(timeout=4).clean)
        put = coordinator.strong_put(key, b"three copies", deadline=time.monotonic() + 6)
        self.assertEqual(put.status, OperationStatus.OK, put)
        self.assertEqual(set(put.qualifying_owners), set(owner_ids) - set(down))
        self.assertEqual(tuple(node.vm_id for node in coordinator.owners(key)), original_owners)

        third_down = owner_ids[3]
        self.assertTrue(self.nodes[third_down].stop(timeout=4).clean)
        failed_get = coordinator.strong_get(key, deadline=time.monotonic() + 5)
        self.assertNotEqual(failed_get.status, OperationStatus.NOT_FOUND, failed_get)
        self.assertIn(failed_get.status,
                      (OperationStatus.ERROR, OperationStatus.TIMEOUT), failed_get)

    def test_failed_put_with_lost_acks_is_unknown_and_get_reads_back_partial_write(self):
        key = "partial-strong-write"
        owners, owner_ids = self.participants_for(key)
        coordinator = self.nodes[owner_ids[0]]
        commit_ids = set(owner_ids[:3])
        client = coordinator.rpc_client
        actual_call = client.call_replica
        injected_timeouts = []
        timeout_lock = threading.Lock()
        all_commit_acks_lost = threading.Event()

        def lose_phase_two_acks(target, request, **kwargs):
            if request.phase != "strong_write":
                return actual_call(target, request, **kwargs)
            if target.vm_id in commit_ids:
                outcome = actual_call(target, request, **kwargs)
                self.assertEqual(outcome.kind, RpcOutcomeKind.RESPONSE)
                self.assertIn(outcome.response.status,
                              (RpcStatus.INSTALLED, RpcStatus.UNCHANGED))
                with timeout_lock:
                    injected_timeouts.append(target.vm_id)
                    if set(injected_timeouts) == commit_ids:
                        all_commit_acks_lost.set()
                return RpcOutcome(RpcOutcomeKind.TIMEOUT, error="injected lost ack",
                                  completion_unknown=True)
            if not all_commit_acks_lost.wait(4):
                return RpcOutcome(RpcOutcomeKind.ERROR,
                                  error="test did not observe all committed writes")
            return RpcOutcome(RpcOutcomeKind.UNREACHABLE,
                              error="injected owner unavailable")

        with mock.patch.object(client, "call_replica", side_effect=lose_phase_two_acks):
            failed_invoked = time.monotonic()
            failed = coordinator.strong_put(
                key, b"partially committed", deadline=time.monotonic() + 6)
            failed_responded = time.monotonic()
        self.assertEqual(failed.status, OperationStatus.ERROR, failed)
        self.assertTrue(failed.completion_unknown, failed)
        self.assertEqual(set(injected_timeouts), commit_ids)
        partial_records = [self.nodes[vm_id].store.read_local(
            key, context=self.nodes[vm_id].context).record for vm_id in commit_ids]
        self.assertTrue(all(record is not None for record in partial_records))
        self.assertEqual({record.value for record in partial_records},
                         {b"partially committed"})
        self.assertEqual(len({record.version for record in partial_records}), 1)

        readback_invoked = time.monotonic()
        readback = coordinator.strong_get(key, deadline=time.monotonic() + 6)
        readback_responded = time.monotonic()
        self.assertEqual((readback.status, readback.value),
                         (OperationStatus.OK, b"partially committed"), readback)
        copies = [self.nodes[vm_id].store.read_local(
            key, context=self.nodes[vm_id].context).record for vm_id in owner_ids]
        self.assertGreaterEqual(sum(
            record is not None and record.version == readback.version
            and record.value == b"partially committed" for record in copies), 3)
        partial_history = [
            HistoryEntry(failed.operation_id, coordinator.vm_id, key, "put",
                         b"partially committed", failed_invoked, failed_responded, failed),
            HistoryEntry(readback.operation_id, coordinator.vm_id, key, "get", None,
                         readback_invoked, readback_responded, readback),
        ]
        self.assertTrue(check_register_history(partial_history), partial_history)

    def test_equal_version_conflicting_bytes_fails_before_readback(self):
        key = "contradictory-records"
        owners, owner_ids = self.participants_for(key)
        coordinator = self.nodes[owner_ids[0]]
        conflict = Version("strong", 10**12, "vm10/999")
        values = (b"left", b"left", b"right", b"right", b"third")
        for vm_id, value in zip(owner_ids, values):
            store = self.nodes[vm_id].store
            result = store.install_if_newer(
                key, Record(conflict, value), context=self.nodes[vm_id].context)
            self.assertEqual(result.status.value, "INSTALLED")

        phases = []
        real_collect = __import__("mp3.strong", fromlist=["collect_distinct"]).collect_distinct

        def record_collection(*args, **kwargs):
            phase = kwargs["phase"]
            phases.append(phase)
            return real_collect(*args, **kwargs)

        with mock.patch("mp3.strong.collect_distinct", side_effect=record_collection):
            result = coordinator.strong_get(key, deadline=time.monotonic() + 5)
        self.assertEqual(result.status, OperationStatus.ERROR, result)
        self.assertEqual(phases, ["strong_query"])

    def test_two_writers_same_observation_order_tags_and_remain_linearizable(self):
        key = "multi-writer-abd"
        _, owner_ids = self.participants_for(key, coordinators=("vm01", "vm02"))
        writer_a, writer_b = self.nodes["vm01"], self.nodes["vm02"]
        a_entered_phase_two, release_a = threading.Event(), threading.Event()
        a_operation_id = []
        original_dispatch = writer_a.dispatch_owner

        def gate_a_phase_two(target_id, op, item_key, **kwargs):
            if kwargs["phase"] == "strong_write":
                admission = kwargs["admission"]
                if not a_operation_id:
                    a_operation_id.append(admission.operation_id)
                    a_entered_phase_two.set()
                if admission.operation_id == a_operation_id[0]:
                    self.assertTrue(release_a.wait(8), "writer A phase 2 gate timed out")
            return original_dispatch(target_id, op, item_key, **kwargs)

        collections = {}
        real_collect = __import__("mp3.strong", fromlist=["collect_distinct"]).collect_distinct

        def capture_collection(*args, **kwargs):
            if (kwargs["phase"] == "strong_write"
                    and args[0][0].request.sender_id == "vm02"):
                # Let this completed higher-tag PUT finish its already-dispatched
                # one-shot calls before releasing the lower writer's phase two.
                for call in args[0]:
                    call.future.result(timeout=5)
            collection = real_collect(*args, **kwargs)
            collections[(kwargs["operation_id"], kwargs["phase"])] = collection
            return collection

        history = []
        with mock.patch.object(writer_a, "dispatch_owner", side_effect=gate_a_phase_two), \
                mock.patch("mp3.strong.collect_distinct", side_effect=capture_collection), \
                ThreadPoolExecutor(max_workers=4) as pool:
            a_invoked = time.monotonic()
            future_a = pool.submit(
                writer_a.strong_put, key, b"writer A", deadline=time.monotonic() + 8)
            self.assertTrue(a_entered_phase_two.wait(5), "writer A did not finish query")

            b_invoked = time.monotonic()
            result_b = writer_b.strong_put(
                key, b"writer B", deadline=time.monotonic() + 8)
            b_returned = time.monotonic()
            self.assertEqual(result_b.status, OperationStatus.OK, result_b)
            release_a.set()
            result_a = future_a.result(timeout=8)
            a_returned = time.monotonic()
            self.assertEqual(result_a.status, OperationStatus.OK, result_a)

            a_collection = collections[(result_a.operation_id, "strong_write")]
            self.assertGreaterEqual(sum(
                reply.response.status == RpcStatus.SUPERSEDED
                for reply in a_collection.qualifying), 3)
            self.assertLess(result_a.version.compare(result_b.version), 0)

            get_invoked = time.monotonic()
            get_result = writer_a.strong_get(key, deadline=time.monotonic() + 8)
            get_returned = time.monotonic()
            self.assertEqual((get_result.status, get_result.value),
                             (OperationStatus.OK, b"writer B"), get_result)
            history.extend((
                HistoryEntry(result_a.operation_id, "vm01", key, "put", b"writer A",
                             a_invoked, a_returned, result_a),
                HistoryEntry(result_b.operation_id, "vm02", key, "put", b"writer B",
                             b_invoked, b_returned, result_b),
                HistoryEntry(get_result.operation_id, "vm01", key, "get", None,
                             get_invoked, get_returned, get_result),
            ))

            # Two concurrent writes from one VM must get distinct durable tags even
            # when their query phases overlap and observe the same prior maximum.
            gate = threading.Barrier(2)

            def same_writer(value):
                gate.wait(timeout=3)
                invoked = time.monotonic()
                result = writer_a.strong_put(
                    key, value, deadline=time.monotonic() + 8)
                returned = time.monotonic()
                return HistoryEntry(result.operation_id, "vm01", key, "put", value,
                                    invoked, returned, result)

            same_writer_rows = list(pool.map(same_writer, (b"same-A", b"same-B")))
            self.assertTrue(all(row.result.status == OperationStatus.OK
                                for row in same_writer_rows), same_writer_rows)
            self.assertNotEqual(same_writer_rows[0].result.version,
                                same_writer_rows[1].result.version)
            history.extend(same_writer_rows)
            final_get = writer_b.strong_get(key, deadline=time.monotonic() + 8)
            final_started = time.monotonic() - 0.001
            final_returned = time.monotonic()
            self.assertEqual(final_get.status, OperationStatus.OK, final_get)
            history.append(HistoryEntry(final_get.operation_id, "vm02", key, "get", None,
                                        final_started, final_returned, final_get))

        self.assertTrue(check_register_history(history),
                        f"gated two-writer ABD history is not linearizable: {history!r}")

    def test_admitted_put_can_dispatch_phase_two_after_shutdown_closes_admission(self):
        key = "shutdown-between-abd-phases"
        _, owner_ids = self.participants_for(key)
        coordinator = self.nodes[owner_ids[0]]
        at_write_phase = threading.Event()
        allow_write_phase = threading.Event()
        original_dispatch = coordinator.dispatch_owner

        def gate_before_write(target_id, op, item_key, **kwargs):
            if kwargs["phase"] == "strong_write":
                at_write_phase.set()
                if not allow_write_phase.wait(5):
                    raise TimeoutError("test gate expired")
            return original_dispatch(target_id, op, item_key, **kwargs)

        with mock.patch.object(coordinator, "dispatch_owner", side_effect=gate_before_write), \
                ThreadPoolExecutor(max_workers=2) as pool:
            invoked = time.monotonic()
            operation = pool.submit(
                coordinator.strong_put, key, b"accepted before stop",
                deadline=time.monotonic() + 8)
            self.assertTrue(at_write_phase.wait(5), "operation did not enter phase two")

            stopping = pool.submit(coordinator.stop, timeout=7)
            shutdown_deadline = time.monotonic() + 3
            while coordinator.ready and time.monotonic() < shutdown_deadline:
                time.sleep(0.005)
            self.assertFalse(coordinator.ready, "shutdown did not close readiness")
            allow_write_phase.set()

            result = operation.result(timeout=5)
            responded = time.monotonic()
            report = stopping.result(timeout=7)
        self.assertEqual(result.status, OperationStatus.OK, result)
        self.assertTrue(report.clean, report)
        history = HistoryEntry(result.operation_id, coordinator.vm_id, key, "put",
                               b"accepted before stop", invoked, responded, result)
        self.assertTrue(check_register_history([history]))

    def test_admission_closed_before_new_strong_operation_is_rejected(self):
        key = "closed-admission"
        owners, owner_ids = self.participants_for(key)
        coordinator = self.nodes[owner_ids[0]]
        self.assertTrue(coordinator.stop(timeout=5).clean)
        result = coordinator.strong_put(key, b"reject", deadline=time.monotonic() + 3)
        self.assertEqual(result.status, OperationStatus.ERROR, result)
        self.assertIn("not ready", result.error or "")
        for owner_id in owner_ids:
            if not self.nodes[owner_id].ready:
                continue
            read = self.nodes[owner_id].store.read_local(
                key, context=self.nodes[owner_id].context)
            self.assertEqual(read.status.value, "NOT_FOUND")

    def test_delayed_query_and_install_responses_arrive_after_quorum_in_order(self):
        key = "delayed-strong-replies"
        _, owner_ids = self.participants_for(key)
        coordinator = self.nodes[owner_ids[0]]
        query_started = threading.Event()
        query_quorum_collected = threading.Event()
        query_done = threading.Event()
        write_started = threading.Event()
        write_quorum_collected = threading.Event()
        write_done = threading.Event()
        threshold_times = {}
        completion_times = {}
        real_call = coordinator.rpc_client.call_replica
        real_collect = __import__("mp3.strong", fromlist=["collect_distinct"]).collect_distinct
        delayed_query_owner, delayed_write_owner = owner_ids[-1], owner_ids[-2]

        def controlled_call(target, request, **kwargs):
            if request.phase == "strong_query":
                delayed = target.vm_id == delayed_query_owner
                started = query_started
                gate, done = query_quorum_collected, query_done
            elif request.phase == "strong_write":
                delayed = target.vm_id == delayed_write_owner
                started = write_started
                gate, done = write_quorum_collected, write_done
            else:
                return real_call(target, request, **kwargs)
            if delayed:
                started.set()
                if not gate.wait(5):
                    return RpcOutcome(RpcOutcomeKind.TIMEOUT,
                                      error="deterministic delayed-response gate expired")
                try:
                    outcome = real_call(target, request, **kwargs)
                    completion_times[request.phase] = time.monotonic()
                    return outcome
                finally:
                    done.set()
            if not started.wait(5):
                return RpcOutcome(RpcOutcomeKind.TIMEOUT,
                                  error="delayed owner was not dispatched")
            return real_call(target, request, **kwargs)

        def collect_then_release(*args, **kwargs):
            collection = real_collect(*args, **kwargs)
            phase = kwargs["phase"]
            if phase in ("strong_query", "strong_write"):
                threshold_times[phase] = time.monotonic()
            return collection

        try:
            with mock.patch.object(coordinator.rpc_client, "call_replica",
                                   side_effect=controlled_call), \
                    mock.patch("mp3.strong.collect_distinct",
                               side_effect=collect_then_release):
                result = coordinator.strong_put(
                    key, b"quorum precedes slow replies", deadline=time.monotonic() + 7)
                # The coordinator must return after Q=3 without waiting for either
                # delayed prescribed owner response.
                self.assertTrue(query_started.is_set())
                self.assertTrue(write_started.is_set())
                self.assertEqual(result.status, OperationStatus.OK, result)
        finally:
            query_quorum_collected.set()
            write_quorum_collected.set()
        self.assertTrue(query_done.wait(3), "late query RPC did not release")
        self.assertTrue(write_done.wait(3), "late write RPC did not release")
        self.assertGreater(completion_times["strong_query"],
                           threshold_times["strong_query"])
        self.assertGreater(completion_times["strong_write"],
                           threshold_times["strong_write"])
        late_record = self.nodes[delayed_write_owner].store.read_local(
            key, context=self.nodes[delayed_write_owner].context).record
        self.assertEqual((late_record.version, late_record.value),
                         (result.version, b"quorum precedes slow replies"))

    def test_randomized_concurrent_histories_are_linearizable_for_recorded_seeds(self):
        key = "seeded-histories"
        _, _ = self.participants_for(key, coordinators=("vm01", "vm02"))
        coordinators = (self.nodes["vm01"], self.nodes["vm02"])
        all_history = []
        seeds = (17, 425, 20261102)
        for seed in seeds:
            rng = random.Random(seed)
            actions = [
                ("put", coordinators[0], f"{seed}-a".encode()),
                ("put", coordinators[1], f"{seed}-b".encode()),
                ("get", coordinators[0], None),
                ("get", coordinators[1], None),
            ]
            rng.shuffle(actions)
            gate = threading.Barrier(len(actions))

            def execute(action):
                operation, runtime, value = action
                gate.wait(timeout=4)
                invoked = time.monotonic()
                if operation == "put":
                    result = runtime.strong_put(
                        key, value, deadline=time.monotonic() + 8)
                else:
                    result = runtime.strong_get(key, deadline=time.monotonic() + 8)
                responded = time.monotonic()
                return HistoryEntry(result.operation_id, runtime.vm_id, key,
                                    operation, value, invoked, responded, result)

            with ThreadPoolExecutor(max_workers=4) as pool:
                rows = list(pool.map(execute, actions))
            self.assertTrue(all(row.result.status in
                                (OperationStatus.OK, OperationStatus.NOT_FOUND)
                                for row in rows), f"seed={seed}: {rows!r}")
            all_history.extend(rows)
            self.assertTrue(check_register_history(all_history),
                            f"seed={seed} failed; replay this exact history: {all_history!r}")

    def test_sigkill_after_commit_lost_ack_recovery_and_generation_fencing(self):
        key = "crash-commit-before-client-ack"
        owners = self.manifest.owners(key)
        owner_ids = tuple(node.vm_id for node in owners)
        crashed_owner = owner_ids[0]
        coordinator_id = next(f"vm{index:02d}" for index in range(1, 11)
                              if f"vm{index:02d}" != crashed_owner)
        in_process_owners = set(owner_ids) - {crashed_owner}
        parent_ids = in_process_owners | {coordinator_id}
        self.start_nodes(parent_ids)
        child = self.start_process(crashed_owner)
        participants = parent_ids | {crashed_owner}
        self.wait_membership(participants)

        coordinator = self.nodes[coordinator_id]
        client = coordinator.rpc_client
        actual_call = client.call_replica
        commit_ids = set(owner_ids[:3])
        self.assertIn(crashed_owner, commit_ids)

        def commit_then_lose_ack(target, request, **kwargs):
            if request.phase != "strong_write":
                return actual_call(target, request, **kwargs)
            if target.vm_id not in commit_ids:
                return RpcOutcome(RpcOutcomeKind.UNREACHABLE,
                                  error="injected unavailable prescribed owner")
            outcome = actual_call(target, request, **kwargs)
            self.assertEqual(outcome.kind, RpcOutcomeKind.RESPONSE)
            self.assertIn(outcome.response.status,
                          (RpcStatus.INSTALLED, RpcStatus.UNCHANGED))
            if target.vm_id == crashed_owner:
                self.assertEqual(child.kill(), -9)
            return RpcOutcome(RpcOutcomeKind.TIMEOUT,
                              error="commit durable but acknowledgment dropped",
                              completion_unknown=True)

        crashed_put_invoked = time.monotonic()
        with mock.patch.object(client, "call_replica", side_effect=commit_then_lose_ack):
            failed = coordinator.strong_put(
                key, b"durable but unacknowledged", deadline=time.monotonic() + 8)
        crashed_put_responded = time.monotonic()
        self.assertIn(failed.status, (OperationStatus.ERROR, OperationStatus.TIMEOUT), failed)
        self.assertTrue(failed.completion_unknown, failed)

        child = self.start_process(crashed_owner, bootstrap=False)
        self.wait_membership(participants, timeout=14)
        snapshots = [row for row in coordinator.membership if row.vm_id == crashed_owner]
        self.assertEqual(snapshots[0].restart_generation, 2)
        self.assertEqual(child.generation, 2)

        recovered_read_invoked = time.monotonic()
        recovered_read = coordinator.strong_get(
            key, deadline=time.monotonic() + 8)
        recovered_read_responded = time.monotonic()
        self.assertEqual((recovered_read.status, recovered_read.value),
                         (OperationStatus.OK, b"durable but unacknowledged"), recovered_read)
        crash_history = [
            HistoryEntry(failed.operation_id, coordinator.vm_id, key, "put",
                         b"durable but unacknowledged", crashed_put_invoked,
                         crashed_put_responded, failed),
            HistoryEntry(recovered_read.operation_id, coordinator.vm_id, key, "get", None,
                         recovered_read_invoked, recovered_read_responded, recovered_read),
        ]
        self.assertTrue(check_register_history(crash_history), crash_history)

        # The old target generation must be rejected as response evidence even
        # though the restarted process serves the same stable VM identity.
        old_generation_reply = coordinator.call_replica(
            crashed_owner, RpcOperation.READ_LOCAL, key,
            deadline=time.monotonic() + 3, phase="strong_query", schema=2,
            operation_id=new_operation_id(), expected_target_generation=1)
        self.assertEqual(old_generation_reply.kind, RpcOutcomeKind.INVALID_RESPONSE)

        # A request claiming the crashed process's stale sender generation is
        # rejected by an actual receiving replica before any storage mutation.
        target = self.nodes[coordinator_id]
        stale_key = "old-generation-must-not-write"
        stale_request = new_request(
            manifest=self.manifest, sender_id=crashed_owner, sender_generation=1,
            target_id=coordinator_id, epoch=1, mode=ConsistencyMode.STRONG,
            op=RpcOperation.INSTALL_IF_NEWER, key=stale_key,
            version=Version("strong", 999_999, f"{crashed_owner}/1"),
            value=b"reject", phase="strong_write", schema=2,
            operation_id=new_operation_id())
        with socket.create_connection(target.replica_service.address, timeout=2) as sock:
            send_request(sock, stale_request, self.manifest,
                         deadline=time.monotonic() + 2)
            sock.shutdown(socket.SHUT_WR)
            stale_response = receive_response(sock, self.manifest,
                                              deadline=time.monotonic() + 2)
        self.assertEqual(stale_response.status, RpcStatus.ERROR)
        self.assertEqual(stale_response.status, RpcStatus.ERROR)
        self.assertIn(stale_response.error, {"STALE_GENERATION", "INVALID_REQUEST"})
        self.assertEqual(target.store.read_local(
            stale_key, context=target.context).status.value, "NOT_FOUND")


if __name__ == "__main__":
    unittest.main()
