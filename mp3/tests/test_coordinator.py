"""Focused tests for shared Step 3 coordinator primitives."""
from concurrent.futures import Future
import threading
import time
import unittest

from mp3.attempts import AttemptLease
from mp3.coordinator import (
    AttemptLedger, AttemptState, BoundedExecutor, OwnerCall, OwnerDispatchError,
    OwnerOutcome, RpcCompletion, collect_distinct, dispatch_owner,
)
from mp3.models import ConsistencyMode, Context, Version
from mp3.ring import Endpoint, Manifest
from mp3.rpc import (PROTOCOL_SCHEMA_VERSION, ReplicaResponse, RpcOutcome,
                     RpcOutcomeKind, RpcOperation, RpcStatus, new_operation_id,
                     new_request)


def manifest_fixture():
    endpoints = {}
    for index in range(1, 11):
        vm_id = f"vm{index:02d}"
        port = 52000 + index * 3
        endpoints[vm_id] = {
            "kv": Endpoint("127.0.0.1", port),
            "membership": Endpoint("127.0.0.1", port + 1),
            "log_query": Endpoint("127.0.0.1", port + 2),
        }
    return Manifest.from_endpoints(endpoints)


def call_fixture(manifest, owner, operation_id, *, status=RpcStatus.FOUND,
                 generation=3, version=None, phase="strong_query"):
    mode = ConsistencyMode.STRONG
    operation = (RpcOperation.READ_LOCAL if phase == "strong_query"
                 else RpcOperation.INSTALL_IF_NEWER)
    requested = None if phase == "strong_query" else version
    body = b"" if phase == "strong_query" else b"value"
    request = new_request(
        manifest=manifest, sender_id="vm01", sender_generation=2,
        target_id=owner.vm_id, epoch=1, mode=mode, op=operation, key="alpha",
        version=requested, value=body, phase=phase, schema=PROTOCOL_SCHEMA_VERSION,
        operation_id=operation_id)
    response_fields = dict(
        request_id=request.request_id, manifest_id=manifest.fingerprint,
        sender_id=owner.vm_id, sender_generation=generation, target_id="vm01",
        epoch=1, mode=mode, op=operation, phase=phase, status=status,
        schema=PROTOCOL_SCHEMA_VERSION, operation_id=operation_id)
    if status == RpcStatus.ERROR:
        outcome = RpcOutcome(RpcOutcomeKind.ERROR, error="injected network failure")
        future = Future()
        future.set_result(RpcCompletion(outcome, time.monotonic()))
        return OwnerCall(owner, request, generation, future)
    if status == RpcStatus.NOT_FOUND:
        response = ReplicaResponse(**response_fields, present=False)
    elif phase == "strong_query":
        response = ReplicaResponse(
            **response_fields, present=True, version=version or Version("strong", 8, "vm02/1"),
            value=b"value")
    else:
        response = ReplicaResponse(
            **response_fields, present=False, stored_version=version, durable=True)
    future = Future()
    future.set_result(RpcCompletion(RpcOutcome(RpcOutcomeKind.RESPONSE, response), time.monotonic()))
    return OwnerCall(owner, request, generation, future)


class AttemptLeaseTests(unittest.TestCase):
    def test_five_second_foreground_and_bounded_continuation(self):
        lease = AttemptLease(admitted_at=0)
        self.assertEqual(lease.foreground_deadline, 5)
        self.assertEqual(lease.finish_foreground(4.9), 14.9)
        early = AttemptLease(admitted_at=0)
        self.assertEqual(early.finish_foreground(1), 11)
        timeout = AttemptLease(admitted_at=0)
        self.assertEqual(timeout.finish_foreground(5), 15)

    def test_queued_attempt_can_use_continuation_after_client_completion(self):
        now = [8.0]
        lease = AttemptLease(admitted_at=0, clock=lambda: now[0])
        lease.finish_foreground(4.9)
        self.assertAlmostEqual(lease.check(), 6.9)

    def test_cancel_is_idempotent_and_stops_lease(self):
        lease = AttemptLease()
        self.assertTrue(lease.cancel())
        self.assertFalse(lease.cancel())
        with self.assertRaises(Exception):
            lease.check()


class BoundedExecutorTests(unittest.TestCase):
    def test_reserved_capacity_accepts_burst_and_drains(self):
        executor = BoundedExecutor(workers=2, max_pending=3, name="reserve-test")
        reservation = executor.reserve(5, deadline=time.monotonic() + 1)
        self.assertIsNotNone(reservation)
        release = threading.Event()
        futures = [reservation.submit(release.wait, 2) for _ in range(5)]
        reservation.close()
        release.set()
        self.assertTrue(all(future.result(timeout=2) for future in futures))
        result = executor.shutdown(timeout=2)
        self.assertTrue(result.clean)
        self.assertEqual(result.accepted, 5)

    def test_capacity_exhaustion_does_not_partially_reserve(self):
        executor = BoundedExecutor(workers=1, max_pending=1, name="capacity-test")
        started, release = threading.Event(), threading.Event()
        reservation = executor.reserve(1, deadline=time.monotonic() + 1)
        future = reservation.submit(lambda: (started.set(), release.wait(timeout=2)))
        reservation.close()
        self.assertTrue(started.wait(2))
        queued = executor.submit(lambda: None, deadline=time.monotonic() + 1)
        self.assertIsNotNone(queued)
        self.assertIsNone(executor.reserve(1, deadline=time.monotonic()))
        release.set()
        future.result(timeout=2)
        queued.result(timeout=2)
        self.assertTrue(executor.shutdown(timeout=2).clean)


class AttemptLedgerTests(unittest.TestCase):
    def test_durable_ack_is_terminal_and_releases_once(self):
        manifest = manifest_fixture()
        released = []
        ledger = AttemptLedger(manifest.owners("ledger-key"), release=released.append)
        owner = manifest.owners("ledger-key")[0].vm_id
        ledger.transition(owner, AttemptState.QUEUED)
        ledger.transition(owner, AttemptState.IN_FLIGHT)
        ledger.finish(owner, OwnerOutcome.DURABLE_ACK)
        report = ledger.report()
        self.assertFalse(report.terminal)
        self.assertEqual(dict((vm_id, (outcome, unknown))
                              for vm_id, outcome, unknown in report.outcomes)[owner],
                         (OwnerOutcome.DURABLE_ACK, False))
        with self.assertRaises(RuntimeError):
            ledger.finish(owner, OwnerOutcome.DURABLE_ACK)
        self.assertEqual(released, [owner])

    def test_canceled_and_completed_attempts_cannot_release_twice(self):
        manifest = manifest_fixture()
        released = []
        owners = manifest.owners("ledger-key")
        ledger = AttemptLedger(owners, release=released.append)
        ledger.transition(owners[0].vm_id, AttemptState.QUEUED)
        ledger.finish(owners[0].vm_id, OwnerOutcome.CANCELLED)
        ledger.finish(owners[1].vm_id, OwnerOutcome.NOT_STARTED_SHUTDOWN)
        for owner in owners[2:]:
            ledger.finish(owner.vm_id, OwnerOutcome.ERROR)
        self.assertTrue(ledger.report().terminal)
        self.assertEqual(set(released), {owner.vm_id for owner in owners})
        self.assertEqual(len(released), 5)
        with self.assertRaises(RuntimeError):
            ledger.finish(owners[0].vm_id, OwnerOutcome.CANCELLED)
        self.assertEqual(len(released), 5)

    def test_forced_shutdown_reports_active_and_cancels_queued(self):
        executor = BoundedExecutor(workers=1, max_pending=2, name="force-test")
        started, release = threading.Event(), threading.Event()

        def active():
            started.set()
            release.wait(timeout=2)

        first = executor.submit(active, deadline=time.monotonic() + 1)
        self.assertTrue(started.wait(2))
        queued = executor.submit(lambda: None, deadline=time.monotonic() + 1)
        report = executor.shutdown(timeout=0, force=True)
        self.assertEqual(report.incomplete, 1)
        self.assertFalse(report.clean)
        self.assertTrue(queued.cancelled())
        release.set()
        first.result(timeout=2)
        self.assertTrue(executor.shutdown(timeout=2).clean)


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.manifest = manifest_fixture()
        self.owners = self.manifest.owners("alpha")
        self.operation_id = new_operation_id()

    def test_shared_dispatch_rejects_nonowner_before_queueing(self):
        key = "shared-nonowner"
        owner_ids = {owner.vm_id for owner in self.manifest.owners(key)}
        nonowner = next(node for node in self.manifest.nodes if node.vm_id not in owner_ids)
        request = new_request(
            manifest=self.manifest, sender_id="vm01", sender_generation=1,
            target_id=nonowner.vm_id, epoch=1, mode=ConsistencyMode.STRONG,
            op=RpcOperation.READ_LOCAL, key=key, phase="strong_query", schema=2,
            operation_id=self.operation_id)
        executor = BoundedExecutor(workers=1, max_pending=1, name="owner-preflight")
        try:
            with self.assertRaises(OwnerDispatchError) as rejected:
                dispatch_owner(
                    object(), nonowner, request, manifest=self.manifest,
                    deadline=time.monotonic() + 2, expected_generation=1,
                    executor=executor)
            self.assertEqual(rejected.exception.failure.owner_id, nonowner.vm_id)
            self.assertEqual(rejected.exception.failure.outcome,
                             OwnerOutcome.NOT_PRESCRIBED_OWNER)
            self.assertEqual(executor.counts, (0, 0, 0))
        finally:
            self.assertTrue(executor.shutdown(timeout=2).clean)

    def collect(self, calls, phase="strong_query", required=3):
        return collect_distinct(
            calls, owners=self.owners, manifest=self.manifest, required=required,
            operation_id=self.operation_id, phase=phase,
            context=Context(1, ConsistencyMode.STRONG), deadline=time.monotonic() + 2)

    def test_only_three_distinct_prescribed_strong_replies_reach_threshold(self):
        calls = [call_fixture(
            self.manifest, owner, self.operation_id,
            version=Version("strong", i + 1, "vm02/1"))
            for i, owner in enumerate(self.owners)]
        result = self.collect(calls)
        self.assertTrue(result.reached)
        self.assertEqual(len(result.qualifying), 3)
        self.assertEqual(len({row.owner_id for row in result.qualifying}), 3)

    def test_explicit_not_found_qualifies_but_transport_error_does_not(self):
        calls = [call_fixture(
            self.manifest, owner, self.operation_id,
            status=RpcStatus.NOT_FOUND if index < 3 else RpcStatus.ERROR)
            for index, owner in enumerate(self.owners)]
        result = self.collect(calls)
        self.assertTrue(result.reached)
        self.assertEqual({row.response.status for row in result.qualifying}, {RpcStatus.NOT_FOUND})

    def test_extra_duplicate_owner_does_not_add_quorum_weight(self):
        calls = [call_fixture(self.manifest, owner, self.operation_id) for owner in self.owners]
        calls.append(call_fixture(self.manifest, self.owners[0], self.operation_id))
        result = self.collect(calls)
        self.assertTrue(result.reached)
        self.assertEqual(len({row.owner_id for row in result.qualifying}), len(result.qualifying))
        self.assertIn(OwnerOutcome.DUPLICATE_OWNER, {row.outcome for row in result.failures})

    def test_dispatch_generation_is_pinned_to_original_request(self):
        call = call_fixture(self.manifest, self.owners[0], self.operation_id, generation=3)
        result = self.collect([call] + [call_fixture(
            self.manifest, owner, self.operation_id, status=RpcStatus.ERROR)
            for owner in self.owners[1:]])
        self.assertEqual(result.qualifying[0].response.sender_generation, 3)

    def test_eventual_superseded_is_not_exact_version_write_evidence(self):
        expected = Version("eventual", 10, "vm01/1")
        calls = []
        for index, owner in enumerate(self.owners):
            requested = new_request(
                manifest=self.manifest, sender_id="vm01", sender_generation=2,
                target_id=owner.vm_id, epoch=1, mode=ConsistencyMode.EVENTUAL_NO_RR,
                op=RpcOperation.INSTALL_IF_NEWER, key="alpha", version=expected,
                value=b"value", phase="eventual_put", schema=2,
                operation_id=self.operation_id)
            status = RpcStatus.SUPERSEDED if index == 0 else RpcStatus.INSTALLED
            stored = Version("eventual", 11, "vm02/1") if index == 0 else expected
            response = ReplicaResponse(
                request_id=requested.request_id, manifest_id=self.manifest.fingerprint,
                sender_id=owner.vm_id, sender_generation=3, target_id="vm01",
                epoch=1, mode=ConsistencyMode.EVENTUAL_NO_RR,
                op=requested.op, phase="eventual_put", status=status,
                present=False, stored_version=stored, durable=True, schema=2,
                operation_id=self.operation_id)
            future = Future()
            future.set_result(RpcCompletion(RpcOutcome(RpcOutcomeKind.RESPONSE, response), time.monotonic()))
            calls.append(OwnerCall(owner, requested, 3, future))
        result = collect_distinct(
            calls, owners=self.owners, manifest=self.manifest, required=2,
            operation_id=self.operation_id, phase="eventual_put",
            context=Context(1, ConsistencyMode.EVENTUAL_NO_RR), deadline=time.monotonic() + 2)
        self.assertTrue(result.reached)
        self.assertEqual(len(result.qualifying), 2)
        self.assertIn(OwnerOutcome.SUPERSEDED, {row.outcome for row in result.failures})

    def test_eventual_w_keeps_all_reserved_owner_attempts_running(self):
        expected = Version("eventual", 10, "vm01/1")
        calls = []
        for index, owner in enumerate(self.owners):
            request = new_request(
                manifest=self.manifest, sender_id="vm01", sender_generation=2,
                target_id=owner.vm_id, epoch=1, mode=ConsistencyMode.EVENTUAL_NO_RR,
                op=RpcOperation.INSTALL_IF_NEWER, key="alpha", version=expected,
                value=b"value", phase="eventual_put", schema=2,
                operation_id=self.operation_id)
            future = Future()
            if index == 0:
                response = ReplicaResponse(
                    request_id=request.request_id, manifest_id=self.manifest.fingerprint,
                    sender_id=owner.vm_id, sender_generation=3, target_id="vm01",
                    epoch=1, mode=ConsistencyMode.EVENTUAL_NO_RR,
                    op=request.op, phase="eventual_put", status=RpcStatus.INSTALLED,
                    present=False, stored_version=expected, durable=True, schema=2,
                    operation_id=self.operation_id)
                future.set_result(RpcCompletion(
                    RpcOutcome(RpcOutcomeKind.RESPONSE, response), time.monotonic()))
            calls.append(OwnerCall(owner, request, 3, future))
        result = collect_distinct(
            calls, owners=self.owners, manifest=self.manifest, required=1,
            operation_id=self.operation_id, phase="eventual_put",
            context=Context(1, ConsistencyMode.EVENTUAL_NO_RR),
            deadline=time.monotonic() + 2)
        self.assertTrue(result.reached)
        self.assertEqual(len(result.qualifying), 1)
        self.assertEqual({call.node.vm_id for call in result.continuing},
                         {call.node.vm_id for call in calls[1:]})
        self.assertTrue(all(not call.future.cancelled() for call in result.continuing))

    def test_eventual_fanout_continues_when_w_becomes_impossible(self):
        expected = Version("eventual", 12, "vm01/1")
        calls = []
        for index, owner in enumerate(self.owners):
            request = new_request(
                manifest=self.manifest, sender_id="vm01", sender_generation=2,
                target_id=owner.vm_id, epoch=1, mode=ConsistencyMode.EVENTUAL_NO_RR,
                op=RpcOperation.INSTALL_IF_NEWER, key="alpha", version=expected,
                value=b"value", phase="eventual_put", schema=2,
                operation_id=self.operation_id)
            future = Future()
            if index < 2:
                future.set_result(RpcCompletion(
                    RpcOutcome(RpcOutcomeKind.UNREACHABLE, error="injected down"),
                    time.monotonic()))
            calls.append(OwnerCall(owner, request, 3, future))
        result = collect_distinct(
            calls, owners=self.owners, manifest=self.manifest, required=4,
            operation_id=self.operation_id, phase="eventual_put",
            context=Context(1, ConsistencyMode.EVENTUAL_NO_RR),
            deadline=time.monotonic() + 0.02)
        self.assertFalse(result.reached)
        self.assertEqual(len(result.failures), 2)
        self.assertEqual(len(result.continuing), 3)
        self.assertTrue(all(not call.future.cancelled() for call in result.continuing))

    def test_impossible_required_four_stops_after_two_failures(self):
        calls = [call_fixture(self.manifest, owner, self.operation_id,
                              status=RpcStatus.ERROR)
                 for owner in self.owners[:2]]
        for owner in self.owners[2:]:
            completed = call_fixture(
                self.manifest, owner, self.operation_id,
                version=Version("strong", 5, "vm02/1"))
            calls.append(OwnerCall(completed.node, completed.request,
                                   completed.expected_generation, Future()))
        started = threading.Event()
        result_box = []

        def collect():
            started.set()
            result_box.append(collect_distinct(
                calls, owners=self.owners, manifest=self.manifest, required=4,
                operation_id=self.operation_id, phase="strong_query",
                context=Context(1, ConsistencyMode.STRONG),
                deadline=time.monotonic() + 60))

        thread = threading.Thread(target=collect, daemon=True)
        thread.start()
        self.assertTrue(started.wait(1))
        thread.join(timeout=1)
        self.assertFalse(thread.is_alive(), "impossible quorum waited for the long deadline")
        result = result_box[0]
        self.assertFalse(result.reached)
        self.assertEqual(len(result.failures), 5)
        self.assertEqual(len(result.continuing), 0)
        self.assertTrue(all(call.future.cancelled() for call in calls[2:]))

    def test_required_three_remains_possible_after_two_failures(self):
        calls = [call_fixture(
            self.manifest, owner, self.operation_id,
            status=RpcStatus.ERROR if index < 2 else RpcStatus.FOUND,
            version=Version("strong", index + 5, "vm02/1"))
            for index, owner in enumerate(self.owners)]
        result = collect_distinct(
            calls, owners=self.owners, manifest=self.manifest, required=3,
            operation_id=self.operation_id, phase="strong_query",
            context=Context(1, ConsistencyMode.STRONG), deadline=time.monotonic() + 2)
        self.assertTrue(result.reached)
        self.assertEqual(len(result.qualifying), 3)


if __name__ == "__main__":
    unittest.main()
