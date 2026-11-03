"""Local loopback acceptance tests for Eventual-noRR (KD-12)."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock

from mp3.coordinator import OperationStatus
from mp3.eventual import EventualNoRRCoordinator
from mp3.models import ConsistencyMode, Record, Version
from mp3.node import NodeRuntime
from mp3.rpc import (ReplicaResponse, RpcOperation, RpcOutcome, RpcOutcomeKind,
                     RpcStatus)
from mp3.tests.test_strong import _manifest
from mp3.coordinator import Schema2Activation


ROOT = Path(__file__).resolve().parents[1]


class EventualIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.manifest = _manifest()
        self.inventory = Schema2Activation.from_inventory(self.manifest, {
            node.vm_id: {"release_id": "eventual-test", "schema_version": 2}
            for node in self.manifest.nodes
        })
        self.nodes = {}
        self.addCleanup(self.close_nodes)

    def node(self, vm_id, *, attempt_workers=8, attempt_queue=24):
        return NodeRuntime(
            manifest=self.manifest, vm_id=vm_id,
            database_path=self.root / f"{vm_id}.sqlite", bootstrap=True,
            bootstrap_mode=ConsistencyMode.EVENTUAL_NO_RR,
            protocol_activation=self.inventory, replica_workers=4,
            replica_queue=16, coordinator_workers=4, coordinator_queue=8,
            attempt_workers=attempt_workers, attempt_queue=attempt_queue)

    def start(self, vm_ids, *, attempt_workers=8, attempt_queue=24):
        for vm_id in vm_ids:
            if vm_id not in self.nodes:
                self.nodes[vm_id] = self.node(
                    vm_id, attempt_workers=attempt_workers,
                    attempt_queue=attempt_queue).start()
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if all(all(any(row.vm_id == peer and row.restart_generation > 0
                           for row in self.nodes[vm_id].membership)
                           for peer in vm_ids if peer != vm_id)
                   for vm_id in vm_ids):
                return
            time.sleep(0.02)
        self.fail(f"membership did not learn generations for {vm_ids}")

    def for_key(self, key):
        owners = self.manifest.owners(key)
        owner_ids = tuple(row.vm_id for row in owners)
        coordinator_id = next(f"vm{i:02d}" for i in range(1, 11)
                              if f"vm{i:02d}" not in owner_ids)
        self.start(set(owner_ids) | {coordinator_id})
        return owners, owner_ids, self.nodes[coordinator_id]

    def close_nodes(self):
        for node in reversed(tuple(self.nodes.values())):
            try:
                if node.store is not None and (node._shutdown_report is None
                        or not node._shutdown_report.clean):
                    node.stop(timeout=8)
            except Exception:
                pass
        self.temp.cleanup()

    @staticmethod
    def _read_response(target, request, expected_generation, *, status, version=None,
                       value=b""):
        response = ReplicaResponse(
            request_id=request.request_id, manifest_id=request.manifest_id,
            sender_id=target.vm_id, sender_generation=expected_generation,
            target_id=request.sender_id, epoch=request.epoch, mode=request.mode,
            op=request.op, phase=request.phase, status=status,
            present=(status == RpcStatus.FOUND), version=version, value=value,
            schema=2, operation_id=request.operation_id)
        return RpcOutcome(RpcOutcomeKind.RESPONSE, response)

    def test_put_reserves_five_and_propagates_after_w_without_blocking_client(self):
        key = "eventual-background-fanout"
        _, owner_ids, coordinator = self.for_key(key)
        client = coordinator.rpc_client
        actual = client.call_replica
        release = threading.Event()
        blocked = set(owner_ids[2:])
        counts = {owner: 0 for owner in owner_ids}
        lock = threading.Lock()

        def delayed(target, request, **kwargs):
            with lock:
                counts[target.vm_id] += 1
            if request.op == RpcOperation.INSTALL_IF_NEWER and target.vm_id in blocked:
                release.wait(5)
            return actual(target, request, **kwargs)

        try:
            with mock.patch.object(client, "call_replica", side_effect=delayed):
                result = coordinator.eventual_put(key, b"eventual-value")
                self.assertEqual(result.status, OperationStatus.OK, result)
                self.assertEqual(len(result.qualifying_owners), 2)
                snap = result.propagation.snapshot()
                self.assertFalse(snap.terminal)
                self.assertEqual(sum(row.outcome is not None for row in snap.owners), 2)
                self.assertEqual(set(counts), set(owner_ids))
                self.assertTrue(all(count == 1 for count in counts.values()))
        finally:
            release.set()
        final = result.propagation.wait(5)
        self.assertTrue(final.terminal, final)
        self.assertEqual(sum(row.outcome.value == "DURABLE_ACK" for row in final.owners), 5,
                         final)
        self.assertTrue(all(self.nodes[vm_id].store.read_local(
            key, context=self.nodes[vm_id].context).record.value == b"eventual-value"
            for vm_id in owner_ids))

    def test_get_from_a_different_nonowner_coordinator(self):
        key = "eventual-cross-coordinator"
        _, owner_ids, writer = self.for_key(key)
        reader_id = next(f"vm{i:02d}" for i in range(1, 11)
                         if f"vm{i:02d}" not in set(owner_ids) | {writer.vm_id})
        self.start(set(owner_ids) | {writer.vm_id, reader_id})
        put = writer.eventual_put(key, b"written by one coordinator")
        self.assertEqual(put.status, OperationStatus.OK, put)
        self.assertTrue(put.propagation.wait(5).terminal)
        get = self.nodes[reader_id].eventual_get(
            key, deadline=time.monotonic() + 4, read_quorum=2)
        self.assertEqual((get.status, get.value),
                         (OperationStatus.OK, b"written by one coordinator"), get)

    def test_owner_can_also_coordinate_its_local_replica_rpc(self):
        key = "eventual-owner-coordinator"
        owners = self.manifest.owners(key)
        owner_ids = tuple(row.vm_id for row in owners)
        self.start(set(owner_ids))
        coordinator = self.nodes[owner_ids[0]]
        put = coordinator.eventual_put(key, b"local owner through TCP")
        self.assertEqual(put.status, OperationStatus.OK, put)
        self.assertTrue(put.propagation.wait(5).terminal)
        local = coordinator.store.read_local(key, context=coordinator.context)
        self.assertEqual(local.record.value, b"local owner through TCP")
        get = coordinator.eventual_get(key, deadline=time.monotonic() + 4)
        self.assertEqual((get.status, get.value),
                         (OperationStatus.OK, b"local owner through TCP"), get)

    def test_queued_owner_starts_after_client_ack_with_remaining_lease(self):
        key = "eventual-queued-owner"
        owners = self.manifest.owners(key)
        owner_ids = tuple(row.vm_id for row in owners)
        coordinator_id = next(f"vm{i:02d}" for i in range(1, 11)
                              if f"vm{i:02d}" not in owner_ids)
        participant_ids = set(owner_ids) | {coordinator_id}
        self.start(participant_ids, attempt_workers=2, attempt_queue=3)
        coordinator = self.nodes[coordinator_id]
        client, actual = coordinator.rpc_client, coordinator.rpc_client.call_replica
        release_blocked = threading.Event()
        blocked_started = threading.Event()
        calls = []
        blocked_count = [0]
        calls_lock = threading.Lock()

        def delay_after_first_owner(target, request, **kwargs):
            with calls_lock:
                calls.append(target.vm_id)
                if (request.op == RpcOperation.INSTALL_IF_NEWER
                        and target.vm_id != owner_ids[0]):
                    blocked_count[0] += 1
                    if blocked_count[0] >= 2:
                        blocked_started.set()
            if request.op == RpcOperation.INSTALL_IF_NEWER and target.vm_id != owner_ids[0]:
                release_blocked.wait(timeout=6)
            return actual(target, request, **kwargs)

        try:
            with mock.patch.object(client, "call_replica", side_effect=delay_after_first_owner):
                result = coordinator.eventual_put(key, b"queued owner eventually receives it",
                                                  write_quorum=1)
                self.assertEqual(result.status, OperationStatus.OK, result)
                blocked_started.wait(timeout=2)
                self.assertEqual(len(calls), 3)
                self.assertFalse(result.propagation.snapshot().terminal)
                release_blocked.set()
                final = result.propagation.wait(6)
        finally:
            release_blocked.set()
        self.assertTrue(final.terminal, final)
        self.assertEqual(len(calls), 5)
        self.assertEqual(set(calls), set(owner_ids))
        self.assertEqual(sum(row.outcome.value == "DURABLE_ACK" for row in final.owners), 5,
                         final)

    def test_graceful_shutdown_after_w_drains_background_fanout(self):
        key = "eventual-shutdown-after-w"
        _, owner_ids, coordinator = self.for_key(key)
        client, actual = coordinator.rpc_client, coordinator.rpc_client.call_replica
        release = threading.Event()
        blocked_started = threading.Event()
        blocked_count = [0]
        lock = threading.Lock()

        def hold_remaining(target, request, **kwargs):
            if request.op == RpcOperation.INSTALL_IF_NEWER and target.vm_id != owner_ids[0]:
                with lock:
                    blocked_count[0] += 1
                    if blocked_count[0] >= 2:
                        blocked_started.set()
                release.wait(5)
            return actual(target, request, **kwargs)

        stopped = threading.Event()
        report = []
        try:
            with mock.patch.object(client, "call_replica", side_effect=hold_remaining):
                result = coordinator.eventual_put(key, b"drain after foreground", write_quorum=1)
                self.assertEqual(result.status, OperationStatus.OK, result)
                self.assertTrue(blocked_started.wait(2))

                def stop_node():
                    stopped.set()
                    report.append(coordinator.stop(timeout=5))

                stopper = threading.Thread(target=stop_node)
                stopper.start()
                self.assertTrue(stopped.wait(1))
                deadline = time.monotonic() + 2
                while coordinator.ready and time.monotonic() < deadline:
                    time.sleep(0.005)
                self.assertFalse(coordinator.ready, "shutdown did not close admission")
                release.set()
                stopper.join(timeout=6)
                self.assertFalse(stopper.is_alive())
        finally:
            release.set()
        self.assertTrue(report and report[0].clean, report)
        final = result.propagation.wait(2)
        self.assertTrue(final.terminal, final)
        self.assertTrue(all(row.outcome.value == "DURABLE_ACK" for row in final.owners))
        self.assertTrue(report[0].store_closed)

    def test_read_returns_max_of_first_r_and_never_repairs(self):
        key = "first-r-selection"
        _, owner_ids, coordinator = self.for_key(key)
        versions = [Version("eventual", n, f"{owner_id}/1")
                    for n, owner_id in enumerate(owner_ids, start=1)]
        for index, owner_id in enumerate(owner_ids):
            self.nodes[owner_id].store.install_if_newer(
                key, Record(versions[index], f"v{index + 1}".encode()),
                context=self.nodes[owner_id].context)
        client, actual = coordinator.rpc_client, coordinator.rpc_client.call_replica
        release = threading.Event()
        operations = []

        def ordered(target, request, **kwargs):
            operations.append(request.op)
            if request.phase == "eventual_read" and target.vm_id not in owner_ids[:2]:
                release.wait(3)
            return actual(target, request, **kwargs)

        try:
            with mock.patch.object(client, "call_replica", side_effect=ordered):
                result = EventualNoRRCoordinator(
                    coordinator, read_quorum=2, write_quorum=1).eventual_get(
                        key, deadline=time.monotonic() + 4)
            self.assertEqual((result.status, result.value), (OperationStatus.OK, b"v2"), result)
        finally:
            release.set()
        self.assertEqual(operations, [RpcOperation.READ_LOCAL] * len(operations))
        self.assertNotIn(RpcOperation.INSTALL_IF_NEWER, operations)

    def test_rw_matrix_and_invalid_configurations(self):
        key = "rw-config-matrix"
        _, owner_ids, coordinator = self.for_key(key)
        for read, write in ((1, 1), (2, 2), (3, 3), (5, 5), (1, 3), (4, 2)):
            api = EventualNoRRCoordinator(coordinator, read_quorum=read,
                                          write_quorum=write)
            put = api.eventual_put(key, f"{read}/{write}".encode())
            self.assertEqual(put.status, OperationStatus.OK, (read, write, put))
            self.assertEqual(len(put.qualifying_owners), write)
            put.propagation.wait(4)
            get = api.eventual_get(key, deadline=time.monotonic() + 4)
            self.assertEqual(get.status, OperationStatus.OK, (read, write, get))
            self.assertEqual(len(get.qualifying_owners), read)
        for value in (0, -1, 6, True, "2"):
            with self.assertRaises(ValueError):
                EventualNoRRCoordinator(coordinator, read_quorum=value)
            with self.assertRaises(ValueError):
                EventualNoRRCoordinator(coordinator, write_quorum=value)

    def test_empty_value_and_explicit_absence(self):
        key = "eventual-empty"
        _, _, coordinator = self.for_key(key)
        self.assertEqual(coordinator.eventual_get(
            key, deadline=time.monotonic() + 3).status, OperationStatus.NOT_FOUND)
        put = coordinator.eventual_put(key, b"")
        self.assertEqual(put.status, OperationStatus.OK, put)
        put.propagation.wait(4)
        got = coordinator.eventual_get(key, deadline=time.monotonic() + 3)
        self.assertEqual((got.status, got.value), (OperationStatus.OK, b""), got)

        maximum = bytes(range(256)) * 4096
        large = coordinator.eventual_put(key, maximum)
        self.assertEqual(large.status, OperationStatus.OK, large)
        large.propagation.wait(5)
        got_large = coordinator.eventual_get(key, deadline=time.monotonic() + 4)
        self.assertEqual((got_large.status, got_large.value),
                         (OperationStatus.OK, maximum))

    def test_equal_version_conflict_fails_closed(self):
        key = "eventual-equal-version-conflict"
        owners, _, coordinator = self.for_key(key)
        shared = Version("eventual", 77, "vm09/1")
        for owner, value in zip(owners[:2], (b"left", b"right")):
            self.nodes[owner.vm_id].store.install_if_newer(
                key, Record(shared, value), context=self.nodes[owner.vm_id].context)
        result = EventualNoRRCoordinator(
            coordinator, read_quorum=5, write_quorum=1).eventual_get(
                key, deadline=time.monotonic() + 3)
        self.assertEqual(result.status, OperationStatus.ERROR)
        self.assertIn("VERSION_CONFLICT", result.error)

    def test_unicode_and_nul_key_round_trip(self):
        key = "café\x00🧪"
        _, _, coordinator = self.for_key(key)
        put = coordinator.eventual_put(key, b"key bytes remain metadata-safe")
        self.assertEqual(put.status, OperationStatus.OK, put)
        put.propagation.wait(5)
        got = coordinator.eventual_get(key, deadline=time.monotonic() + 4)
        self.assertEqual((got.status, got.value),
                         (OperationStatus.OK, b"key bytes remain metadata-safe"))

    def test_concurrent_coordinators_allocate_ordered_eventual_versions(self):
        key = "eventual-concurrent-coordinators"
        owners, owner_ids, first = self.for_key(key)
        second_id = next(f"vm{i:02d}" for i in range(1, 11)
                         if f"vm{i:02d}" not in set(owner_ids) | {first.vm_id})
        self.start(set(owner_ids) | {first.vm_id, second_id})
        second = self.nodes[second_id]
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(first.eventual_put, key, b"from-first"),
                       pool.submit(second.eventual_put, key, b"from-second")]
            results = [future.result(timeout=8) for future in futures]
        self.assertTrue(all(result.status in (OperationStatus.OK, OperationStatus.ERROR,
                                               OperationStatus.TIMEOUT)
                            for result in results), results)
        successful_versions = [result.version for result in results
                               if result.status == OperationStatus.OK]
        self.assertEqual(len(successful_versions), len(set(successful_versions)))
        reports = []
        for result in results:
            if result.propagation is not None:
                report = result.propagation.wait(10)
                self.assertTrue(report.terminal, report)
                reports.append(report)
        records = [self.nodes[owner.vm_id].store.read_local(
            key, context=self.nodes[owner.vm_id].context).record for owner in owners]
        present = [record for record in records if record is not None]
        self.assertTrue(present)
        maximum = present[0]
        for record in present[1:]:
            if record.version.compare(maximum.version) > 0:
                maximum = record
        self.assertTrue(all(row.version == maximum.version and row.value == maximum.value
                            for row in present),
                        {"records": [(row.version, row.value) for row in present],
                         "results": [(row.status, row.version, row.error) for row in results],
                         "propagation": reports})

    def test_w2_with_three_unavailable_owners_fails_without_owner_substitution(self):
        key = "eventual-two-live-only"
        owners, owner_ids, coordinator = self.for_key(key)
        designated = tuple(node.vm_id for node in coordinator.owners(key))
        for vm_id in owner_ids[:3]:
            self.assertTrue(self.nodes[vm_id].stop(timeout=5).clean)
        result = coordinator.eventual_put(key, b"two reachable copies satisfy W=2")
        # Exactly two reachable designated owners are enough at default W=2.
        self.assertEqual(result.status, OperationStatus.OK, result)
        self.assertEqual(set(result.qualifying_owners), set(owner_ids[3:]))
        self.assertEqual(tuple(node.vm_id for node in coordinator.owners(key)), designated)
        final = result.propagation.wait(4)
        self.assertTrue(final.terminal, final)
        self.assertEqual({row.owner_id for row in final.owners}, set(owner_ids))

    def test_one_and_two_down_owners_remain_fixed_and_w2_succeeds(self):
        key = "eventual-one-and-two-down"
        _, owner_ids, coordinator = self.for_key(key)
        prescribed = tuple(row.vm_id for row in coordinator.owners(key))
        self.assertTrue(self.nodes[owner_ids[0]].stop(timeout=5).clean)
        one_down = coordinator.eventual_put(key, b"one owner down")
        self.assertEqual(one_down.status, OperationStatus.OK, one_down)
        self.assertNotIn(owner_ids[0], one_down.qualifying_owners)
        self.assertTrue(one_down.propagation.wait(5).terminal)

        self.assertTrue(self.nodes[owner_ids[1]].stop(timeout=5).clean)
        two_down = coordinator.eventual_put(key, b"two owners down")
        self.assertEqual(two_down.status, OperationStatus.OK, two_down)
        self.assertEqual(len(two_down.qualifying_owners), 2)
        self.assertTrue(set(two_down.qualifying_owners) <= set(owner_ids[2:]))
        final = two_down.propagation.wait(5)
        self.assertTrue(final.terminal, final)
        self.assertEqual(tuple(row.vm_id for row in coordinator.owners(key)), prescribed)

    def test_atomic_reservation_overload_sends_no_install(self):
        key = "eventual-reservation-overload"
        _, _, coordinator = self.for_key(key)
        with mock.patch.object(coordinator, "reserve_attempts", return_value=None), \
                mock.patch.object(coordinator, "dispatch_owner") as dispatch:
            result = coordinator.eventual_put(key, b"must not be sent")
        self.assertEqual(result.status, OperationStatus.OVERLOADED)
        self.assertFalse(result.completion_unknown)
        self.assertEqual(dispatch.call_count, 0)
        final = result.propagation.snapshot()
        self.assertTrue(final.terminal)
        self.assertEqual({row.outcome.value for row in final.owners}, {"OVERLOADED"})

    def test_one_synchronous_owner_dispatch_failure_does_not_stop_other_owners(self):
        client_cases = (("eventual-preflight-w2", 2, OperationStatus.OK),
                        ("eventual-preflight-w5", 5, OperationStatus.ERROR))
        for key, write_quorum, expected_status in client_cases:
            with self.subTest(write_quorum=write_quorum):
                _, owner_ids, coordinator = self.for_key(key)
                failed_owner = owner_ids[0]
                original_dispatch = coordinator.dispatch_owner
                attempted = []

                def fail_one(target_id, *args, **kwargs):
                    attempted.append(target_id)
                    if target_id == failed_owner:
                        raise RuntimeError("injected local generation preflight failure")
                    return original_dispatch(target_id, *args, **kwargs)

                with mock.patch.object(coordinator, "dispatch_owner", side_effect=fail_one):
                    result = coordinator.eventual_put(
                        key, b"other prescribed owners remain eligible",
                        write_quorum=write_quorum)
                self.assertEqual(result.status, expected_status, result)
                self.assertEqual(set(attempted), set(owner_ids))
                final = result.propagation.wait(5)
                self.assertTrue(final.terminal, final)
                self.assertEqual({row.owner_id for row in final.owners}, set(owner_ids))
                failed = next(row for row in final.owners if row.owner_id == failed_owner)
                self.assertEqual(failed.outcome.value, "ERROR")
                self.assertFalse(failed.completion_unknown)
                self.assertEqual(sum(row.outcome.value == "DURABLE_ACK"
                                     for row in final.owners), 4)
                if write_quorum == 2:
                    self.assertEqual(len(result.qualifying_owners), 2)
                    self.assertNotIn(failed_owner, result.qualifying_owners)
                else:
                    self.assertEqual(result.qualifying_owners, ())

    def test_get_dispatch_failure_still_allows_r2_and_selects_first_r_maximum(self):
        key = "eventual-get-preflight-r2"
        _, owner_ids, coordinator = self.for_key(key)
        failed_owner = owner_ids[0]
        slow_release = threading.Event()
        attempted = []
        operations = []
        original_dispatch = coordinator.dispatch_owner
        versions = {
            owner_ids[1]: Version("eventual", 8, f"{owner_ids[1]}/1"),
            owner_ids[2]: Version("eventual", 7, f"{owner_ids[2]}/1"),
            owner_ids[3]: Version("eventual", 100, f"{owner_ids[3]}/1"),
            owner_ids[4]: Version("eventual", 101, f"{owner_ids[4]}/1"),
        }

        def dispatch(target_id, *args, **kwargs):
            attempted.append(target_id)
            if target_id == failed_owner:
                raise RuntimeError("injected synchronous dispatch failure")
            return original_dispatch(target_id, *args, **kwargs)

        def answer(target, request, **kwargs):
            operations.append(request.op)
            if target.vm_id in owner_ids[3:]:
                slow_release.wait(3)
            return self._read_response(
                target, request, kwargs["expected_target_generation"],
                status=RpcStatus.FOUND, version=versions[target.vm_id],
                value=target.vm_id.encode())

        try:
            with mock.patch.object(coordinator, "dispatch_owner", side_effect=dispatch), \
                    mock.patch.object(coordinator.rpc_client, "call_replica", side_effect=answer):
                result = EventualNoRRCoordinator(
                    coordinator, read_quorum=2).eventual_get(
                        key, deadline=time.monotonic() + 5)
            self.assertEqual(result.status, OperationStatus.OK, result)
            self.assertEqual(result.version, versions[owner_ids[1]])
            self.assertEqual(result.value, owner_ids[1].encode())
            self.assertEqual(set(attempted), set(owner_ids))
            self.assertEqual(len(attempted), 5)
            self.assertEqual(operations, [RpcOperation.READ_LOCAL] * len(operations))
            self.assertNotIn(RpcOperation.INSTALL_IF_NEWER, operations)
        finally:
            slow_release.set()

    def test_get_dispatch_failure_makes_r5_impossible_without_waiting_for_deadline(self):
        key = "eventual-get-preflight-r5"
        _, owner_ids, coordinator = self.for_key(key)
        failed_owner = owner_ids[0]
        attempted = []
        replied = set()
        original_dispatch = coordinator.dispatch_owner
        def dispatch(target_id, *args, **kwargs):
            attempted.append(target_id)
            if target_id == failed_owner:
                raise RuntimeError("injected synchronous dispatch failure")
            call = original_dispatch(target_id, *args, **kwargs)
            completion = call.future.result(timeout=2)
            self.assertEqual(completion.outcome.kind, RpcOutcomeKind.RESPONSE)
            self.assertEqual(completion.outcome.response.status, RpcStatus.NOT_FOUND)
            replied.add(target_id)
            return call

        def answer(target, request, **kwargs):
            return self._read_response(
                target, request, kwargs["expected_target_generation"],
                status=RpcStatus.NOT_FOUND)

        started = time.monotonic()
        with mock.patch.object(coordinator, "dispatch_owner", side_effect=dispatch), \
                mock.patch.object(coordinator.rpc_client, "call_replica", side_effect=answer):
            result = EventualNoRRCoordinator(
                coordinator, read_quorum=5).eventual_get(
                    key, deadline=time.monotonic() + 10)
        elapsed = time.monotonic() - started
        self.assertEqual(result.status, OperationStatus.ERROR, result)
        self.assertIsNone(result.value)
        self.assertEqual(set(attempted), set(owner_ids))
        self.assertEqual(len(attempted), 5)
        self.assertEqual(replied, set(owner_ids[1:]))
        self.assertLess(elapsed, 2.0, "impossible R=5 waited on the operation deadline")

    def test_get_all_synchronous_dispatch_failures_are_explicit_and_release_resources(self):
        key = "eventual-get-preflight-all"
        _, owner_ids, coordinator = self.for_key(key)
        attempted = []

        def fail_all(target_id, *args, **kwargs):
            attempted.append(target_id)
            raise RuntimeError("injected local dispatch failure")

        with mock.patch.object(coordinator, "dispatch_owner", side_effect=fail_all), \
                mock.patch.object(coordinator.rpc_client, "call_replica") as rpc_call:
            result = EventualNoRRCoordinator(
                coordinator, read_quorum=2).eventual_get(
                    key, deadline=time.monotonic() + 5)
        self.assertEqual(result.status, OperationStatus.ERROR, result)
        self.assertIsNone(result.value)
        self.assertFalse(result.completion_unknown)
        self.assertEqual(set(attempted), set(owner_ids))
        self.assertEqual(len(attempted), 5)
        rpc_call.assert_not_called()
        report = coordinator.stop(timeout=5)
        self.assertTrue(report.clean, report)

    def test_get_dispatch_failure_distinguishes_empty_value_from_not_found(self):
        key = "eventual-get-preflight-empty"
        _, owner_ids, coordinator = self.for_key(key)
        failed_owner = owner_ids[0]
        release = threading.Event()
        operations = []
        original_dispatch = coordinator.dispatch_owner

        def dispatch(target_id, *args, **kwargs):
            if target_id == failed_owner:
                raise RuntimeError("injected synchronous dispatch failure")
            return original_dispatch(target_id, *args, **kwargs)

        def answer(target, request, **kwargs):
            operations.append(request.op)
            if target.vm_id in owner_ids[3:]:
                release.wait(3)
            if target.vm_id == owner_ids[1]:
                return self._read_response(
                    target, request, kwargs["expected_target_generation"],
                    status=RpcStatus.FOUND,
                    version=Version("eventual", 9, f"{owner_ids[1]}/1"), value=b"")
            return self._read_response(
                target, request, kwargs["expected_target_generation"],
                status=RpcStatus.NOT_FOUND)

        try:
            with mock.patch.object(coordinator, "dispatch_owner", side_effect=dispatch), \
                    mock.patch.object(coordinator.rpc_client, "call_replica", side_effect=answer):
                result = EventualNoRRCoordinator(
                    coordinator, read_quorum=2).eventual_get(
                        key, deadline=time.monotonic() + 5)
            self.assertEqual(result.status, OperationStatus.OK, result)
            self.assertEqual(result.value, b"")
            self.assertEqual(result.version, Version("eventual", 9, f"{owner_ids[1]}/1"))
            self.assertEqual(operations, [RpcOperation.READ_LOCAL] * len(operations))
            self.assertNotIn(RpcOperation.INSTALL_IF_NEWER, operations)
        finally:
            release.set()

    def test_lost_ack_after_commit_is_uncertain_and_not_quorum_evidence(self):
        key = "eventual-lost-ack"
        owners, owner_ids, coordinator = self.for_key(key)
        committed_owner = owner_ids[0]
        client = coordinator.rpc_client
        actual = client.call_replica

        def lose_ack(target, request, **kwargs):
            if target.vm_id == committed_owner:
                outcome = actual(target, request, **kwargs)
                self.assertEqual(outcome.kind, RpcOutcomeKind.RESPONSE)
                return RpcOutcome(RpcOutcomeKind.TIMEOUT,
                                  error="injected lost acknowledgment",
                                  completion_unknown=True)
            return RpcOutcome(RpcOutcomeKind.UNREACHABLE,
                              error="injected unavailable owner")

        with mock.patch.object(client, "call_replica", side_effect=lose_ack):
            result = coordinator.eventual_put(key, b"committed but unacknowledged")
        self.assertEqual(result.status, OperationStatus.ERROR)
        self.assertTrue(result.completion_unknown)
        final = result.propagation.wait(4)
        self.assertTrue(final.terminal, final)
        lost = next(row for row in final.owners if row.owner_id == committed_owner)
        self.assertEqual(lost.outcome.value, "TIMEOUT")
        self.assertTrue(lost.completion_unknown)
        record = self.nodes[committed_owner].store.read_local(
            key, context=self.nodes[committed_owner].context).record
        self.assertIsNotNone(record)
        self.assertEqual(record.value, b"committed but unacknowledged")

    def test_stale_target_generation_is_not_counted_for_w(self):
        key = "eventual-stale-target-generation"
        _, owner_ids, coordinator = self.for_key(key)
        client, actual = coordinator.rpc_client, coordinator.rpc_client.call_replica
        stale_owner = owner_ids[0]

        def stale_one_response(target, request, **kwargs):
            outcome = actual(target, request, **kwargs)
            if target.vm_id == stale_owner and outcome.response is not None:
                stale_response = replace(
                    outcome.response,
                    sender_generation=outcome.response.sender_generation + 1)
                outcome = replace(outcome, response=stale_response)
            return outcome

        with mock.patch.object(client, "call_replica", side_effect=stale_one_response):
            result = coordinator.eventual_put(key, b"other owners still satisfy W")
        self.assertEqual(result.status, OperationStatus.OK, result)
        self.assertNotIn(stale_owner, result.qualifying_owners)
        final = result.propagation.wait(5)
        stale = next(row for row in final.owners if row.owner_id == stale_owner)
        self.assertEqual(stale.outcome.value, "INVALID_RESPONSE")
        self.assertEqual(sum(row.outcome.value == "DURABLE_ACK" for row in final.owners), 4)

    def test_wrong_mode_rejects_before_protocol_dispatch(self):
        node = NodeRuntime(
            manifest=self.manifest, vm_id="vm01",
            database_path=self.root / "strong.sqlite", bootstrap=True,
            bootstrap_mode=ConsistencyMode.STRONG, protocol_activation=self.inventory)
        self.nodes["vm01"] = node.start()
        with mock.patch.object(node, "dispatch_owner") as dispatch:
            result = node.eventual_put("wrong-mode", b"no")
        self.assertEqual(result.status, OperationStatus.ERROR)
        self.assertIn("requires eventual-noRR", result.error)
        dispatch.assert_not_called()

    def test_foreground_deadline_includes_coordinator_queue_delay(self):
        key = "eventual-queued-deadline"
        _, _, coordinator = self.for_key(key)
        barrier = threading.Barrier(5)
        release = threading.Event()

        def occupy(_admission):
            barrier.wait(timeout=2)
            release.wait(timeout=3)

        queued = [coordinator.submit_coordinator(
            occupy, deadline=time.monotonic() + 4) for _ in range(4)]
        barrier.wait(timeout=2)
        try:
            api = EventualNoRRCoordinator(coordinator, foreground_seconds=0.1)
            with mock.patch.object(coordinator, "dispatch_owner") as dispatch:
                start = time.monotonic()
                result = api.eventual_put(key, b"queued past deadline")
                elapsed = time.monotonic() - start
            self.assertEqual(result.status, OperationStatus.TIMEOUT)
            self.assertFalse(result.completion_unknown)
            self.assertLess(elapsed, 0.8)
            dispatch.assert_not_called()
        finally:
            release.set()
        for future in queued:
            future.result(timeout=2)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline and coordinator.coordinator_resources.coordinators.counts[1]:
            time.sleep(0.01)
        self.assertEqual(coordinator.coordinator_resources.coordinators.counts[1], 0)
        propagation = result.propagation.snapshot()
        self.assertTrue(propagation.terminal, propagation)
        self.assertEqual({row.outcome.value for row in propagation.owners},
                         {"NOT_STARTED_EXPIRED"})

    def test_timeout_during_version_allocation_prevents_late_first_dispatch(self):
        key = "eventual-timeout-before-first-send"
        _, _, coordinator = self.for_key(key)
        allocation_started = threading.Event()
        release_allocation = threading.Event()
        original_allocate = coordinator.store.allocate_eventual_version

        def delayed_allocate(*, context):
            allocation_started.set()
            release_allocation.wait(3)
            return original_allocate(context=context)

        api = EventualNoRRCoordinator(coordinator, foreground_seconds=0.1)
        try:
            with mock.patch.object(coordinator.store, "allocate_eventual_version",
                                   side_effect=delayed_allocate), \
                    mock.patch.object(coordinator, "dispatch_owner") as dispatch:
                with ThreadPoolExecutor(max_workers=1) as pool:
                    pending = pool.submit(api.eventual_put, key, b"deadline wins")
                    self.assertTrue(allocation_started.wait(2))
                    result = pending.result(timeout=1)
                    self.assertEqual(result.status, OperationStatus.TIMEOUT)
                    self.assertFalse(result.completion_unknown)
                    release_allocation.set()
                    deadline = time.monotonic() + 2
                    final = result.propagation.snapshot()
                    while not final.terminal and time.monotonic() < deadline:
                        time.sleep(0.01)
                        final = result.propagation.snapshot()
                    self.assertEqual(dispatch.call_count, 0)
                    self.assertTrue(final.terminal, final)
                    self.assertEqual({row.outcome.value for row in final.owners},
                                     {"NOT_STARTED_EXPIRED"})
        finally:
            release_allocation.set()

    def test_subprocess_sigkill_after_w_keeps_commits_and_does_not_repair_missed_owners(self):
        key = "eventual-process-crash"
        owners = self.manifest.owners(key)
        owner_ids = tuple(node.vm_id for node in owners)
        self.start(set(owner_ids))
        coordinator_id = next(f"vm{i:02d}" for i in range(1, 11)
                              if f"vm{i:02d}" not in owner_ids)
        from mp3.tests.test_strong import NodeProcess
        manifest_path = self.root / "manifest.json"
        manifest_path.write_text(self.manifest.to_json(), encoding="utf-8")
        inventory_path = self.root / "schema2.json"
        inventory_path.write_text(json.dumps({
            "manifest_id": self.manifest.fingerprint,
            "nodes": {node.vm_id: {"release_id": "eventual-test", "schema_version": 2}
                      for node in self.manifest.nodes},
        }), encoding="utf-8")
        child = NodeProcess(coordinator_id, manifest_path,
                            self.root / f"{coordinator_id}.sqlite",
                            inventory_path, bootstrap=True, bootstrap_mode="eventual")
        self.addCleanup(lambda: child.kill() if child.process.poll() is None else None)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            snapshot = child.snapshot()
            peers_know_child = all(any(
                row.vm_id == coordinator_id and row.restart_generation == child.generation
                for row in self.nodes[vm_id].membership) for vm_id in owner_ids)
            if (all(snapshot.get(vm_id, 0) > 0 for vm_id in owner_ids)
                    and peers_know_child):
                break
            time.sleep(0.02)
        else:
            self.fail("Eventual coordinator did not learn owner generations")

        response = child.start_eventual_put(key, "committed-before-crash")
        self.assertEqual(response["status"], "OK", response)
        self.assertEqual(len(response["owners"]), 2)
        committed = set(response["owners"])
        self.assertEqual(child.kill(), -9)
        stored = {
            vm_id: self.nodes[vm_id].store.read_local(
                key, context=self.nodes[vm_id].context).record for vm_id in owner_ids
        }
        self.assertEqual({vm_id for vm_id, record in stored.items() if record is not None},
                         committed)
        self.assertTrue(all(stored[vm_id].value == b"committed-before-crash"
                            for vm_id in committed))

        restarted = NodeProcess(coordinator_id, manifest_path,
                               self.root / f"{coordinator_id}.sqlite",
                               inventory_path, bootstrap=False, bootstrap_mode="eventual")
        self.addCleanup(lambda: restarted.kill() if restarted.process.poll() is None else None)
        self.assertEqual(restarted.generation, 2)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            snapshot = restarted.snapshot()
            peers_know_restarted = all(any(
                row.vm_id == coordinator_id and row.restart_generation == 2
                for row in self.nodes[vm_id].membership) for vm_id in owner_ids)
            if all(snapshot.get(vm_id, 0) > 0 for vm_id in owner_ids) and peers_know_restarted:
                break
            time.sleep(0.02)
        else:
            self.fail("owners did not learn the restarted coordinator generation")

        # A GET observes only the existing copies; Eventual-noRR does not repair
        # the owners that missed the first write after the coordinator was killed.
        get = self.nodes[owner_ids[0]].eventual_get(
            key, deadline=time.monotonic() + 4, read_quorum=5)
        self.assertEqual((get.status, get.value),
                         (OperationStatus.OK, b"committed-before-crash"), get)
        for vm_id in set(owner_ids) - committed:
            self.assertIsNone(self.nodes[vm_id].store.read_local(
                key, context=self.nodes[vm_id].context).record)

        later_key = key
        second = restarted.start_eventual_put(later_key, "after-restart")
        self.assertEqual(second["status"], "OK", second)
        self.assertGreater(second["version"]["number"], response["version"]["number"])
        restarted.kill()


if __name__ == "__main__":
    unittest.main()
