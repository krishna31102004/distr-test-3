"""Cross-protocol adversarial integration checks over real loopback nodes.

Existing protocol suites cover their detailed per-mode fault matrices. This file
adds multi-coordinator history and shared coordinator-capacity stress using fresh
clusters for each consistency mode.
"""

from concurrent.futures import ThreadPoolExecutor
import io
from pathlib import Path
import tempfile
import random
import threading
import time
import unittest
from unittest import mock

from mp3.coordinator import OperationStatus, Schema2Activation
from mp3.models import ConsistencyMode
from mp3.node import NodeRuntime
from mp3.tests.history_checker import HistoryEntry, check_register_history
from mp3.tests.test_strong import _manifest


class ProtocolAdversarialTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = _manifest()
        self.inventory = Schema2Activation.from_inventory(self.manifest, {
            node.vm_id: {"release_id": "adversarial-local", "schema_version": 2}
            for node in self.manifest.nodes
        })
        self.nodes: dict[tuple[ConsistencyMode, str], NodeRuntime] = {}
        self.addCleanup(self._stop_nodes)

    def _node(self, vm_id: str, mode: ConsistencyMode, **limits) -> NodeRuntime:
        return NodeRuntime(
            manifest=self.manifest, vm_id=vm_id,
            database_path=self.root / f"{mode.value}-{vm_id}.sqlite",
            bootstrap=True, bootstrap_mode=mode,
            protocol_activation=self.inventory,
            event_stream=io.StringIO(),
            replica_workers=4, replica_queue=16,
            coordinator_workers=limits.get("coordinator_workers", 4),
            coordinator_queue=limits.get("coordinator_queue", 8),
            attempt_workers=8, attempt_queue=24)

    def _start(self, vm_ids, mode: ConsistencyMode, *, limits=None):
        limits = limits or {}
        for vm_id in sorted(vm_ids):
            key = (mode, vm_id)
            if key not in self.nodes:
                self.nodes[key] = self._node(vm_id, mode, **limits).start()
        expected = set(vm_ids)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if all(
                all(any(row.vm_id == peer and row.restart_generation > 0
                    for row in self.nodes[(mode, vm_id)].membership)
                    for peer in expected if peer != vm_id)
                for vm_id in expected
            ):
                return
            time.sleep(0.02)
        snapshots = {
            vm_id: {row.vm_id: row.restart_generation
                    for row in self.nodes[(mode, vm_id)].membership}
            for vm_id in expected
        }
        self.fail(f"membership did not converge for {expected}: {snapshots}")

    def _stop_nodes(self):
        for node in reversed(tuple(self.nodes.values())):
            if node.store is None:
                continue
            report = node._shutdown_report
            if report is None or not report.clean:
                try:
                    node.stop(timeout=8)
                except Exception:
                    pass

    def test_three_coordinator_strong_history_is_linearizable(self):
        seed = 42520261102
        key = "adversarial-three-coordinator-key"
        owners = tuple(node.vm_id for node in self.manifest.owners(key))
        coordinators = ("vm01", "vm02", "vm03")
        participants = set(owners) | set(coordinators)
        self._start(participants, ConsistencyMode.STRONG)

        rng = random.Random(seed)
        actions = [
            ("put", coordinators[0], b"writer-a"),
            ("put", coordinators[1], b"writer-b"),
            ("put", coordinators[2], b"writer-c"),
            ("get", coordinators[0], None),
            ("get", coordinators[1], None),
            ("get", coordinators[2], None),
        ]
        rng.shuffle(actions)
        gate = threading.Barrier(len(actions))

        def execute(action):
            operation, vm_id, value = action
            gate.wait(timeout=5)
            invoked = time.monotonic()
            runtime = self.nodes[(ConsistencyMode.STRONG, vm_id)]
            if operation == "put":
                result = runtime.strong_put(key, value, deadline=time.monotonic() + 9)
            else:
                result = runtime.strong_get(key, deadline=time.monotonic() + 9)
            responded = time.monotonic()
            return HistoryEntry(result.operation_id, vm_id, key, operation, value,
                                invoked, responded, result)

        with ThreadPoolExecutor(max_workers=len(actions)) as pool:
            history = list(pool.map(execute, actions))

        trace = {
            "seed": seed, "mode": "strong", "key": key,
            "coordinators": coordinators, "owners": owners,
            "history": [
                {"op_id": row.operation_id, "coordinator": row.coordinator_id,
                 "operation": row.operation, "input": row.input_value,
                 "invoked": row.invoked_at, "responded": row.responded_at,
                 "status": row.result.status.value, "value": row.result.value,
                 "version": row.result.version}
                for row in history
            ],
        }
        self.assertTrue(all(
            row.result.status == OperationStatus.OK if row.operation == "put"
            else row.result.status in (OperationStatus.OK, OperationStatus.NOT_FOUND)
            for row in history), trace)
        self.assertTrue(check_register_history(history), trace)
        self.assertGreater(min(row.responded_at for row in history),
                           max(row.invoked_at for row in history), trace)
        self.assertEqual({row.coordinator_id for row in history}, set(coordinators), trace)

    def _assert_bounded_coordinator_capacity(self, mode):
        key = f"adversarial-capacity-{mode.value}"
        owner_ids = {node.vm_id for node in self.manifest.owners(key)}
        coordinator_id = next(f"vm{index:02d}" for index in range(1, 11)
                              if f"vm{index:02d}" not in owner_ids)
        self._start(owner_ids | {coordinator_id}, mode,
                    limits={"coordinator_workers": 1, "coordinator_queue": 1})
        coordinator = self.nodes[(mode, coordinator_id)]
        entered = threading.Event()
        release = threading.Event()
        original_reserve = coordinator.reserve_attempts
        first = True
        first_lock = threading.Lock()

        def gated_reservation(*args, **kwargs):
            nonlocal first
            with first_lock:
                should_gate = first
                first = False
            if should_gate:
                entered.set()
                if not release.wait(5):
                    raise TimeoutError("test reservation gate was not released")
            return original_reserve(*args, **kwargs)

        def invoke(index):
            value = f"{mode.value}-{index}".encode()
            if mode == ConsistencyMode.STRONG:
                return coordinator.strong_put(
                    key, value, deadline=time.monotonic() +
                    (0.15 if index == 3 else 8))
            if index == 3:
                from mp3.eventual import EventualNoRRCoordinator
                return EventualNoRRCoordinator(
                    coordinator, foreground_seconds=0.15).eventual_put(key, value)
            return coordinator.eventual_put(key, value)

        results = []
        try:
            with mock.patch.object(
                    coordinator, "reserve_attempts", side_effect=gated_reservation):
                with ThreadPoolExecutor(max_workers=3) as pool:
                    first_future = pool.submit(invoke, 1)
                    self.assertTrue(entered.wait(3), mode.value)
                    second_future = pool.submit(invoke, 2)
                    deadline = time.monotonic() + 2
                    while time.monotonic() < deadline:
                        active, queued, _ = coordinator.coordinator_resources.coordinators.counts
                        if active == 1 and queued == 1:
                            break
                        time.sleep(0.005)
                    self.assertEqual(
                        coordinator.coordinator_resources.coordinators.counts[:2],
                        (1, 1), mode.value)
                    active, queued, _ = coordinator.coordinator_resources.coordinators.counts
                    self.assertLessEqual(active, 1, mode.value)
                    self.assertLessEqual(queued, 1, mode.value)
                    overloaded = invoke(3)
                    self.assertEqual(overloaded.status, OperationStatus.OVERLOADED,
                                     (mode.value, overloaded))
                    release.set()
                    results = [first_future.result(timeout=12),
                               second_future.result(timeout=12)]
        finally:
            release.set()

        self.assertTrue(all(result.status == OperationStatus.OK
                            for result in results), (mode.value, results))
        if mode == ConsistencyMode.EVENTUAL_NO_RR:
            for result in results:
                propagation = result.propagation.wait(5)
                self.assertTrue(propagation.terminal, (result, propagation))
                self.assertEqual(
                    {row.outcome.value for row in propagation.owners},
                    {"DURABLE_ACK"}, (result, propagation))
        counts = coordinator.coordinator_resources.coordinators.counts
        self.assertEqual(counts, (0, 0, 0), (mode.value, counts))
        # Strong may return as soon as its third durable acknowledgment arrives
        # while the two non-quorum owner RPCs are still completing. Wait for their
        # bounded terminal cleanup before asserting no reservation remains.
        attempt_deadline = time.monotonic() + 4
        while (coordinator.coordinator_resources.attempts.counts != (0, 0, 0)
               and time.monotonic() < attempt_deadline):
            time.sleep(0.01)
        attempt_counts = coordinator.coordinator_resources.attempts.counts
        self.assertEqual(attempt_counts, (0, 0, 0), (mode.value, attempt_counts))

    def test_bounded_coordinator_admission_in_strong_cluster(self):
        self._assert_bounded_coordinator_capacity(ConsistencyMode.STRONG)

    def test_bounded_coordinator_admission_in_eventual_cluster(self):
        self._assert_bounded_coordinator_capacity(ConsistencyMode.EVENTUAL_NO_RR)


if __name__ == "__main__":
    unittest.main()
