"""Eventual-noRR client coordination on the shared bounded MP3 runtime (KD-12)."""

from __future__ import annotations

from concurrent.futures import Future, wait, FIRST_COMPLETED
from dataclasses import dataclass
import math
import threading
import time
from typing import TYPE_CHECKING

from .attempts import AttemptLease
from .config import MAX_VALUE_BYTES
from .coordinator import (
    OperationStatus, OwnerCall, OwnerFailure, OwnerOutcome, ProtocolResult, RpcCompletion,
    collect_distinct,
)
from .models import ConsistencyMode, Record
from .rpc import RpcOperation, RpcOutcomeKind, RpcStatus, new_operation_id

if TYPE_CHECKING:
    from .node import CoordinatorAdmission, NodeRuntime


@dataclass(frozen=True)
class OwnerPropagation:
    owner_id: str
    outcome: OwnerOutcome | None
    completion_unknown: bool = False
    error: str | None = None


@dataclass(frozen=True)
class PropagationSnapshot:
    operation_id: str
    terminal: bool
    owners: tuple[OwnerPropagation, ...]

    @property
    def completion_unknown(self) -> bool:
        return any(owner.completion_unknown for owner in self.owners)


class _ForegroundTransition:
    """Serialize the one transition from client foreground to lease continuation."""

    def __init__(self, lease):
        self.lease = lease
        self._lock = threading.Lock()
        self._deadline: float | None = None
        self._dispatch_started = False

    def finish(self, completed_at: float) -> float:
        with self._lock:
            if self._deadline is None:
                self._deadline = self.lease.finish_foreground(completed_at)
            return self._deadline

    def begin_dispatch(self) -> bool:
        """Atomically fence the first fanout against a foreground timeout."""
        with self._lock:
            if self._deadline is not None and not self._dispatch_started:
                return False
            self._dispatch_started = True
            return True

    @property
    def may_have_dispatched(self) -> bool:
        with self._lock:
            return self._dispatch_started


class PropagationHandle:
    """Bounded, callback-updated view of one operation's five reserved attempts."""

    def __init__(self, operation_id: str, owners, manifest):
        self.operation_id = operation_id
        self._manifest = manifest
        self._lock = threading.Lock()
        self._rows = {owner.vm_id: OwnerPropagation(owner.vm_id, None) for owner in owners}
        self._futures = {}
        self._calls = {}
        self._dispatching = set()

    def mark_dispatching(self, owner_id: str) -> None:
        """Conservatively fence timeout uncertainty before executor submission."""
        with self._lock:
            if owner_id not in self._rows or owner_id in self._futures:
                raise RuntimeError("invalid propagation dispatch transition")
            self._dispatching.add(owner_id)

    def attach(self, call: OwnerCall) -> None:
        owner_id = call.node.vm_id
        with self._lock:
            if owner_id not in self._rows or owner_id in self._futures:
                raise RuntimeError("propagation owner attached more than once")
            self._futures[owner_id] = call.future
            self._calls[owner_id] = call
        call.future.add_done_callback(lambda future, owner=owner_id: self._finish(owner, future))

    def finish_without_dispatch(self, owner_id: str, outcome: OwnerOutcome,
                                error: str | None = None) -> None:
        with self._lock:
            if owner_id not in self._rows or owner_id in self._futures:
                raise RuntimeError("invalid terminal propagation owner")
            self._dispatching.discard(owner_id)
            self._rows[owner_id] = OwnerPropagation(owner_id, outcome, False, error)

    def finish_unsubmitted(self, outcome: OwnerOutcome, error: str) -> None:
        with self._lock:
            for owner_id, row in self._rows.items():
                if owner_id not in self._futures and row.outcome is None:
                    self._dispatching.discard(owner_id)
                    self._rows[owner_id] = OwnerPropagation(owner_id, outcome, False, error)

    def _finish(self, owner_id: str, future: Future) -> None:
        row = self._classify(owner_id, future)
        with self._lock:
            if self._rows[owner_id].outcome is None:
                self._rows[owner_id] = row
            self._dispatching.discard(owner_id)

    def _classify(self, owner_id: str, future: Future) -> OwnerPropagation:
        call = self._calls[owner_id]
        if future.cancelled():
            return OwnerPropagation(owner_id, OwnerOutcome.CANCELLED,
                                    error="owner attempt canceled before completion")
        try:
            completion = future.result()
            if not isinstance(completion, RpcCompletion):
                raise TypeError("owner future returned an invalid completion")
        except BaseException as error:
            return OwnerPropagation(owner_id, OwnerOutcome.ERROR, error=str(error))
        outcome = completion.outcome
        if outcome.kind != RpcOutcomeKind.RESPONSE or outcome.response is None:
            mapping = {
                RpcOutcomeKind.TIMEOUT: OwnerOutcome.TIMEOUT,
                RpcOutcomeKind.UNREACHABLE: OwnerOutcome.UNREACHABLE,
                RpcOutcomeKind.INVALID_RESPONSE: OwnerOutcome.INVALID_RESPONSE,
                RpcOutcomeKind.OVERLOADED: OwnerOutcome.OVERLOADED,
                RpcOutcomeKind.CANCELLED: OwnerOutcome.CANCELLED,
                RpcOutcomeKind.ERROR: OwnerOutcome.ERROR,
            }
            return OwnerPropagation(owner_id, mapping.get(outcome.kind, OwnerOutcome.ERROR),
                                    outcome.completion_unknown, outcome.error)
        response = outcome.response
        try:
            response.validate_for(call.request, call.node, self._manifest,
                                  call.expected_generation)
        except (TypeError, ValueError) as error:
            return OwnerPropagation(owner_id, OwnerOutcome.INVALID_RESPONSE,
                                    outcome.completion_unknown, str(error))
        if (response.operation_id != call.request.operation_id
                or response.phase != "eventual_put"):
            return OwnerPropagation(owner_id, OwnerOutcome.INVALID_RESPONSE,
                                    outcome.completion_unknown,
                                    "operation or phase did not match")
        if (response.status in (RpcStatus.INSTALLED, RpcStatus.UNCHANGED)
                and response.durable and response.stored_version == call.request.version):
            return OwnerPropagation(owner_id, OwnerOutcome.DURABLE_ACK)
        mapping = {
            RpcStatus.SUPERSEDED: OwnerOutcome.SUPERSEDED,
            RpcStatus.VERSION_CONFLICT: OwnerOutcome.VERSION_CONFLICT,
            RpcStatus.STALE_EPOCH: OwnerOutcome.STALE_EPOCH,
            RpcStatus.ERROR: OwnerOutcome.ERROR,
        }
        return OwnerPropagation(owner_id, mapping.get(response.status, OwnerOutcome.INVALID_RESPONSE),
                                outcome.completion_unknown, response.error)

    def snapshot(self) -> PropagationSnapshot:
        # Poll completed Futures to cover the callback-registration race without
        # a monitor thread. A snapshot never waits for unfinished owner work.
        with self._lock:
            futures = tuple(self._futures.items())
        for owner_id, future in futures:
            if future.done():
                self._finish(owner_id, future)
        with self._lock:
            rows = tuple(self._rows.values())
        return PropagationSnapshot(self.operation_id,
                                   all(row.outcome is not None for row in rows), rows)

    @property
    def any_dispatched(self) -> bool:
        with self._lock:
            return bool(self._futures or self._dispatching)

    def wait(self, timeout: float | None = None) -> PropagationSnapshot:
        if timeout is not None and (type(timeout) not in (int, float)
                                    or not math.isfinite(timeout) or timeout < 0):
            raise ValueError("timeout must be finite and nonnegative")
        with self._lock:
            futures = tuple(self._futures.values())
        if futures:
            wait(futures, timeout=timeout)
        return self.snapshot()


class EventualNoRRCoordinator:
    """Run first-R Eventual reads and bounded five-owner Eventual writes."""

    def __init__(self, runtime: "NodeRuntime", *, read_quorum: int = 2,
                 write_quorum: int = 2, foreground_seconds: float = 5.0):
        self.runtime = runtime
        self.read_quorum = self._quorum(read_quorum, "read_quorum")
        self.write_quorum = self._quorum(write_quorum, "write_quorum")
        if (type(foreground_seconds) not in (int, float)
                or not math.isfinite(foreground_seconds) or foreground_seconds <= 0):
            raise ValueError("foreground_seconds must be positive and finite")
        self.foreground_seconds = float(foreground_seconds)

    @staticmethod
    def _quorum(value: int, name: str) -> int:
        if type(value) is not int or not 1 <= value <= 5:
            raise ValueError(f"{name} must be an integer in 1..5")
        return value

    def eventual_put(self, key: str, value: bytes) -> ProtocolResult:
        operation_id = new_operation_id()
        if not isinstance(value, bytes) or len(value) > MAX_VALUE_BYTES:
            return self._result(OperationStatus.ERROR, operation_id,
                                error=f"value must be bytes of size 0..{MAX_VALUE_BYTES}")
        invalid = self._validate_key(key, operation_id)
        if invalid:
            return invalid
        admitted_at = time.monotonic()
        deadline = admitted_at + self.foreground_seconds
        lease = AttemptLease(admitted_at=admitted_at,
                             foreground_seconds=self.foreground_seconds)
        transition = _ForegroundTransition(lease)
        owners = self.runtime.manifest.owners(key)
        propagation = PropagationHandle(operation_id, owners, self.runtime.manifest)
        failure = self._external_gate(operation_id)
        if failure:
            transition.finish(min(time.monotonic(), lease.foreground_deadline))
            return failure
        try:
            future = self.runtime.submit_coordinator(
                self._run_put, key, value, admitted_at, lease, transition, propagation,
                self.write_quorum,
                deadline=deadline,
                operation_id=operation_id)
        except (RuntimeError, ValueError) as error:
            transition.finish(min(time.monotonic(), lease.foreground_deadline))
            return self._result(OperationStatus.ERROR, operation_id, error=str(error))
        if future is None:
            transition.finish(min(time.monotonic(), lease.foreground_deadline))
            propagation.finish_unsubmitted(
                OwnerOutcome.NOT_STARTED_EXPIRED, "coordinator task was not admitted")
            return self._result(OperationStatus.OVERLOADED, operation_id,
                                propagation=propagation,
                                error="coordinator capacity unavailable")

        def record_cancelled(_future):
            if _future.cancelled():
                propagation.finish_unsubmitted(
                    OwnerOutcome.NOT_STARTED_SHUTDOWN,
                    "coordinator canceled before dispatch")

        future.add_done_callback(record_cancelled)
        try:
            return future.result(timeout=max(0.0, deadline - time.monotonic()))
        except TimeoutError:
            transition.finish(deadline)
            return self._result(OperationStatus.TIMEOUT, operation_id,
                                completion_unknown=(transition.may_have_dispatched
                                                    or propagation.any_dispatched),
                                propagation=propagation,
                                error="foreground deadline expired while queued or running")
        except Exception as error:
            transition.finish(min(time.monotonic(), lease.foreground_deadline))
            return self._result(OperationStatus.ERROR, operation_id, error=str(error))

    def eventual_get(self, key: str, *, deadline: float) -> ProtocolResult:
        operation_id = new_operation_id()
        invalid = self._validate_key(key, operation_id)
        if invalid:
            return invalid
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)):
            return self._result(OperationStatus.ERROR, operation_id,
                                error="deadline must be a finite monotonic timestamp")
        failure = self._external_gate(operation_id)
        if failure:
            return failure
        try:
            future = self.runtime.submit_coordinator(
                self._run_get, key, deadline, self.read_quorum, deadline=deadline,
                operation_id=operation_id)
        except (RuntimeError, ValueError) as error:
            return self._result(OperationStatus.ERROR, operation_id, error=str(error))
        if future is None:
            return self._result(OperationStatus.OVERLOADED, operation_id,
                                error="coordinator capacity unavailable")
        try:
            return future.result(timeout=max(0.0, deadline - time.monotonic()))
        except TimeoutError:
            return self._result(OperationStatus.TIMEOUT, operation_id,
                                error="Eventual GET deadline expired")
        except Exception as error:
            return self._result(OperationStatus.ERROR, operation_id, error=str(error))

    def _validate_key(self, key: str, operation_id: str) -> ProtocolResult | None:
        if not isinstance(key, str):
            return self._result(OperationStatus.ERROR, operation_id, error="key must be text")
        try:
            key.encode("utf-8", errors="strict")
        except UnicodeError as error:
            return self._result(OperationStatus.ERROR, operation_id, error=str(error))
        return None

    def _external_gate(self, operation_id: str) -> ProtocolResult | None:
        if not self.runtime.ready:
            return self._result(OperationStatus.ERROR, operation_id, error="node is not ready")
        activation = self.runtime.protocol_activation
        if activation is None or not activation.enabled_for(self.runtime.manifest):
            return self._result(OperationStatus.ERROR, operation_id, error="SCHEMA2_DISABLED")
        if self.runtime.context.mode != ConsistencyMode.EVENTUAL_NO_RR:
            return self._result(OperationStatus.ERROR, operation_id,
                                error="Eventual-noRR operation requires eventual-noRR mode")
        return None

    def _run_put(self, admission: "CoordinatorAdmission", key: str,
                 value: bytes, admitted_at: float, lease: AttemptLease,
                 transition: _ForegroundTransition,
                 propagation: PropagationHandle, required: int) -> ProtocolResult:
        operation_id = admission.operation_id
        deadline = admission.deadline
        context = admission.context
        if context.mode != ConsistencyMode.EVENTUAL_NO_RR:
            transition.finish(min(time.monotonic(), lease.foreground_deadline))
            return self._result(OperationStatus.ERROR, operation_id,
                                error="Eventual-noRR operation requires eventual-noRR mode")
        owners = self.runtime.manifest.owners(key)
        if time.monotonic() >= deadline:
            transition.finish(deadline)
            propagation.finish_unsubmitted(
                OwnerOutcome.NOT_STARTED_EXPIRED,
                "coordinator began after the foreground deadline")
            return self._result(OperationStatus.TIMEOUT, operation_id,
                                propagation=propagation,
                                error="foreground deadline expired before dispatch")
        reservation = self.runtime.reserve_attempts(
            5, deadline=deadline, queue_deadline=lease.hard_deadline,
            admission=admission, attempt_lease=lease)
        if reservation is None:
            transition.finish(min(time.monotonic(), lease.foreground_deadline))
            propagation.finish_unsubmitted(
                OwnerOutcome.OVERLOADED,
                "five-owner reservation unavailable; no replica request was dispatched")
            return self._result(OperationStatus.OVERLOADED, operation_id,
                                propagation=propagation,
                                error="cannot atomically reserve all five owner attempts")
        dispatched = False
        preflight_failures = []
        try:
            version = self.runtime.store.allocate_eventual_version(context=context)
            if time.monotonic() >= lease.foreground_deadline:
                transition.finish(lease.foreground_deadline)
                propagation.finish_unsubmitted(
                    OwnerOutcome.NOT_STARTED_EXPIRED,
                    "foreground expired before dispatch; no owner write was sent")
                return self._result(
                    OperationStatus.TIMEOUT, operation_id, propagation=propagation,
                    error="foreground deadline expired before owner dispatch")
            if not transition.begin_dispatch():
                propagation.finish_unsubmitted(
                    OwnerOutcome.NOT_STARTED_EXPIRED,
                    "foreground closed before first dispatch; no owner write was sent")
                return self._result(
                    OperationStatus.TIMEOUT, operation_id, propagation=propagation,
                    error="foreground deadline expired before owner dispatch")
            calls = []
            for owner in owners:
                try:
                    propagation.mark_dispatching(owner.vm_id)
                    call = self.runtime.dispatch_owner(
                        owner.vm_id, RpcOperation.INSTALL_IF_NEWER, key,
                        deadline=lease.hard_deadline, reservation=reservation,
                        phase="eventual_put", operation_id=operation_id,
                        version=version, value=value, attempt_lease=lease,
                        admission=admission)
                    dispatched = True
                    propagation.attach(call)
                    calls.append(call)
                except Exception as error:
                    failure = getattr(error, "failure", None)
                    if not isinstance(failure, OwnerFailure):
                        failure = OwnerFailure(owner.vm_id, OwnerOutcome.ERROR, str(error))
                    propagation.finish_without_dispatch(
                        owner.vm_id, failure.outcome, failure.error)
                    preflight_failures.append(failure)
                    continue
            if not calls:
                transition.finish(min(time.monotonic(), lease.foreground_deadline))
                return self._result(
                    OperationStatus.ERROR, operation_id,
                    propagation=propagation,
                    error="no prescribed owner attempt could be dispatched")
            calls = tuple(calls)
            result = collect_distinct(
                calls, owners=owners, manifest=self.runtime.manifest,
                required=required, operation_id=operation_id,
                phase="eventual_put", context=context, deadline=deadline,
                preflight_failures=preflight_failures)
            completed_at = min(time.monotonic(), lease.foreground_deadline)
            transition.finish(completed_at)
            status = OperationStatus.OK if result.reached else (
                OperationStatus.TIMEOUT if result.deadline_expired
                else OperationStatus.ERROR)
            unknown = (not result.reached and (
                result.completion_unknown or bool(result.qualifying)
                or bool(result.continuing)))
            return self._result(
                status, operation_id, version=version if result.reached else None,
                owners=tuple(reply.owner_id for reply in result.qualifying),
                completion_unknown=unknown, propagation=propagation,
                error=None if result.reached else self._failure_text(result, "eventual_put"))
        except Exception as error:
            if not dispatched:
                propagation.finish_unsubmitted(OwnerOutcome.ERROR, str(error))
            transition.finish(min(time.monotonic(), lease.foreground_deadline))
            return self._result(OperationStatus.ERROR, operation_id,
                                completion_unknown=dispatched, propagation=propagation,
                                error=str(error))
        finally:
            reservation.close()

    def _run_get(self, admission: "CoordinatorAdmission", key: str,
                 deadline: float, required: int) -> ProtocolResult:
        operation_id, context = admission.operation_id, admission.context
        if context.mode != ConsistencyMode.EVENTUAL_NO_RR:
            return self._result(OperationStatus.ERROR, operation_id,
                                error="Eventual-noRR operation requires eventual-noRR mode")
        owners = self.runtime.manifest.owners(key)
        reservation = self.runtime.reserve_attempts(5, deadline=deadline,
                                                    admission=admission)
        if reservation is None:
            return self._result(OperationStatus.OVERLOADED, operation_id,
                                error="cannot reserve read attempts")
        try:
            calls = self._dispatch(admission, owners, reservation, key,
                                   RpcOperation.READ_LOCAL, "eventual_read", None, b"")
            collection = collect_distinct(
                calls, owners=owners, manifest=self.runtime.manifest,
                required=required, operation_id=operation_id,
                phase="eventual_read", context=context, deadline=deadline)
            if not collection.reached:
                return self._result(
                    self._failure_status(collection), operation_id,
                    error=self._failure_text(collection, "eventual_read"))
            records = [Record(reply.response.version, reply.response.value)
                       for reply in collection.qualifying
                       if reply.response.status == RpcStatus.FOUND]
            if not records:
                return self._result(OperationStatus.NOT_FOUND, operation_id)
            maximum = records[0]
            for record in records[1:]:
                order = record.version.compare(maximum.version)
                if order == 0 and record.value != maximum.value:
                    return self._result(OperationStatus.ERROR, operation_id,
                                        error="equal Eventual version has conflicting bytes")
                if order > 0:
                    maximum = record
            return self._result(OperationStatus.OK, operation_id,
                                value=maximum.value, version=maximum.version,
                                owners=tuple(reply.owner_id for reply in collection.qualifying))
        finally:
            reservation.close()

    def _dispatch(self, admission, owners, reservation, key, op, phase, version, value):
        calls = []
        for owner in owners:
            calls.append(self.runtime.dispatch_owner(
                owner.vm_id, op, key, deadline=admission.deadline,
                reservation=reservation, phase=phase,
                operation_id=admission.operation_id, version=version,
                value=value, admission=admission))
        return tuple(calls)

    @staticmethod
    def _failure_status(collection) -> OperationStatus:
        if collection.deadline_expired:
            return OperationStatus.TIMEOUT
        if collection.failures and all(row.outcome == OwnerOutcome.OVERLOADED
                                       for row in collection.failures):
            return OperationStatus.OVERLOADED
        return OperationStatus.ERROR

    @staticmethod
    def _failure_text(collection, phase: str) -> str:
        details = ", ".join(f"{row.owner_id}:{row.outcome.value}" for row in collection.failures)
        return f"{phase} got {len(collection.qualifying)} qualifying replies" + (
            f"; failures={details}" if details else "")

    @staticmethod
    def _result(status, operation_id, *, value=None, version=None, owners=(),
                completion_unknown=False, propagation=None, error=None):
        return ProtocolResult(status, value, version, operation_id,
                              tuple(dict.fromkeys(owners)), completion_unknown,
                              propagation, error)
