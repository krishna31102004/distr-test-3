"""Eventual-noRR and Eventual-RR client coordination on the bounded MP3 runtime (KD-12/13)."""

from __future__ import annotations

from concurrent.futures import Future, InvalidStateError, wait
from dataclasses import dataclass
from enum import Enum
import hashlib
import math
import threading
import time
from typing import TYPE_CHECKING

from .attempts import AttemptLease
from .config import MAX_VALUE_BYTES
from .coordinator import (
    OperationStatus, OwnerCall, OwnerDispatchError, OwnerFailure, OwnerOutcome,
    ProtocolResult, RpcCompletion,
    collect_distinct,
)
from .eventlog import log_read_repair
from .models import ConsistencyMode, Record, Version
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
    """Run first-R Eventual reads and bounded five-owner Eventual writes.

    `EventualRRCoordinator` subclasses this: PUT is identical, only the mode, the
    schema activation it needs and the GET path differ.
    """

    MODE = ConsistencyMode.EVENTUAL_NO_RR
    MODE_ERROR = "Eventual-noRR operation requires eventual-noRR mode"
    GATE_ERROR = "SCHEMA2_DISABLED"

    def _activation(self):
        return self.runtime.protocol_activation

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
        activation = self._activation()
        if activation is None or not activation.enabled_for(self.runtime.manifest):
            return self._result(OperationStatus.ERROR, operation_id, error=self.GATE_ERROR)
        if self.runtime.context.mode != self.MODE:
            return self._result(OperationStatus.ERROR, operation_id, error=self.MODE_ERROR)
        return None

    def _run_put(self, admission: "CoordinatorAdmission", key: str,
                 value: bytes, admitted_at: float, lease: AttemptLease,
                 transition: _ForegroundTransition,
                 propagation: PropagationHandle, required: int) -> ProtocolResult:
        operation_id = admission.operation_id
        deadline = admission.deadline
        context = admission.context
        if context.mode != self.MODE:
            transition.finish(min(time.monotonic(), lease.foreground_deadline))
            return self._result(OperationStatus.ERROR, operation_id, error=self.MODE_ERROR)
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
        if context.mode != self.MODE:
            return self._result(OperationStatus.ERROR, operation_id, error=self.MODE_ERROR)
        owners = self.runtime.manifest.owners(key)
        reservation = self.runtime.reserve_attempts(5, deadline=deadline,
                                                    admission=admission)
        if reservation is None:
            return self._result(OperationStatus.OVERLOADED, operation_id,
                                error="cannot reserve read attempts")
        try:
            calls, preflight_failures = self._dispatch(
                admission, owners, reservation, key,
                RpcOperation.READ_LOCAL, "eventual_read", None, b"")
            collection = collect_distinct(
                calls, owners=owners, manifest=self.runtime.manifest,
                required=required, operation_id=operation_id,
                phase="eventual_read", context=context, deadline=deadline,
                preflight_failures=preflight_failures, key=key)
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
        preflight_failures = []
        for owner in owners:
            try:
                calls.append(self.runtime.dispatch_owner(
                    owner.vm_id, op, key, deadline=admission.deadline,
                    reservation=reservation, phase=phase,
                    operation_id=admission.operation_id, version=version,
                    value=value, admission=admission))
            except Exception as error:
                if isinstance(error, OwnerDispatchError):
                    failure = error.failure
                    if failure.owner_id != owner.vm_id:
                        failure = OwnerFailure(
                            owner.vm_id, OwnerOutcome.ERROR,
                            "dispatch failure identified a different owner")
                else:
                    outcome = (OwnerOutcome.TIMEOUT if isinstance(error, TimeoutError)
                               else OwnerOutcome.ERROR)
                    failure = OwnerFailure(owner.vm_id, outcome, str(error))
                preflight_failures.append(failure)
        return tuple(calls), tuple(preflight_failures)

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


# ---------------------------------------------------------------------------
# Eventual-RR (KD-13): first-R client reply, bounded comparison, one repair pass
# ---------------------------------------------------------------------------

RR_READ_PHASE = "eventual_rr_read"
RR_REPAIR_PHASE = "eventual_rr_repair"
RR_READ_LIFETIME_SECONDS = 15.0       # read lease hard cap from admission
RR_READ_CONTINUATION_SECONDS = 10.0   # read window that follows the client reply
RR_REPAIR_SECONDS = 5.0               # repair pass budget
RR_TOTAL_LIFETIME_SECONDS = 20.0      # nothing runs past admission + 20 s
RR_SETTLE_SECONDS = 1.0               # grace for lease-aware workers to exit


@dataclass(frozen=True)
class ReadEvidence:
    """One owner's read: a valid reply (outcome None) or why none is usable."""

    owner_id: str
    generation: int | None
    outcome: OwnerOutcome | None
    status: RpcStatus | None = None
    version: Version | None = None
    value_digest: str | None = None
    error: str | None = None

    @property
    def valid(self) -> bool:
        return self.outcome is None


class RepairStatus(str, Enum):
    REPAIRED = "REPAIRED"
    SUPERSEDED_NEWER = "SUPERSEDED_NEWER"
    FAILED = "FAILED"
    NOT_ATTEMPTED = "NOT_ATTEMPTED"


@dataclass(frozen=True)
class RepairResult:
    owner_id: str
    status: RepairStatus
    requested_version: Version | None
    stored_version: Version | None = None
    error: str | None = None


@dataclass(frozen=True)
class ReadRepairReport:
    """Final evidence for one Eventual-RR GET; carries digests, never value bytes."""

    operation_id: str
    returned_status: OperationStatus | None
    returned_version: Version | None
    replies: tuple[ReadEvidence, ...]
    maximum_version: Version | None
    repairs: tuple[RepairResult, ...]
    conflict: bool = False
    incomplete_reason: str | None = None

    @property
    def reachable(self) -> tuple[str, ...]:
        return tuple(reply.owner_id for reply in self.replies if reply.valid)


class ReadRepairHandle:
    """Completion view of one RR GET's background comparison and repair."""

    def __init__(self, operation_id: str):
        self.operation_id = operation_id
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._claimed = False
        self._report: ReadRepairReport | None = None

    def claim(self) -> bool:
        """Take the single right to log and finish this operation's report."""
        with self._lock:
            if self._claimed:
                return False
            self._claimed = True
            return True

    def finish(self, report: ReadRepairReport) -> None:
        with self._lock:
            self._report = report
        self._done.set()

    @property
    def terminal(self) -> bool:
        return self._done.is_set()

    def wait(self, timeout: float | None = None) -> ReadRepairReport | None:
        """Return the final report, or None if it is not ready within `timeout`."""
        if timeout is not None and (type(timeout) not in (int, float)
                                    or not math.isfinite(timeout) or timeout < 0):
            raise ValueError("timeout must be finite and nonnegative")
        if not self._done.wait(timeout):
            return None
        return self._report


def _version_text(version: Version | None) -> str | None:
    return None if version is None else f"{version.kind}:{version.number}:{version.writer_id}"


class EventualRRCoordinator(EventualNoRRCoordinator):
    """Eventual-RR: PUT is the Eventual PUT; GET replies after R then repairs once."""

    MODE = ConsistencyMode.EVENTUAL_RR
    MODE_ERROR = "Eventual-RR operation requires eventual-RR mode"
    GATE_ERROR = "SCHEMA3_DISABLED"

    def _activation(self):
        return self.runtime.rr_activation

    def eventual_get(self, key: str, *, deadline: float) -> ProtocolResult:
        """Return the newest of the first R replies; compare and repair in the background.

        The result's `propagation` is a `ReadRepairHandle` whose `wait()` gives the
        final report. The client result is frozen when R replies arrive.
        """
        operation_id = new_operation_id()
        invalid = self._validate_key(key, operation_id)
        if invalid:
            return invalid
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            return self._result(OperationStatus.ERROR, operation_id,
                                error="deadline must be a finite monotonic timestamp")
        failure = self._external_gate(operation_id)
        if failure:
            return failure
        handle = ReadRepairHandle(operation_id)
        foreground: Future = Future()
        admitted_at = time.monotonic()  # every RR time limit counts from admission
        try:
            future = self.runtime.submit_coordinator(
                self._run_rr_get, key, deadline, self.read_quorum, foreground, handle,
                admitted_at, deadline=deadline, operation_id=operation_id)
        except (RuntimeError, ValueError) as error:
            return self._result(OperationStatus.ERROR, operation_id, error=str(error))
        if future is None:
            return self._result(OperationStatus.OVERLOADED, operation_id,
                                error="coordinator capacity unavailable")

        def settle(done: Future) -> None:
            # The task normally settles the client result itself; this covers a task
            # that was cancelled or died before it could.
            if foreground.done():
                return
            reason = "coordinator canceled" if done.cancelled() else (
                f"coordinator failed: {type(done.exception()).__name__}")
            try:
                foreground.set_result(self._result(OperationStatus.ERROR, operation_id,
                                                   error=reason))
            except InvalidStateError:
                pass
            self._finish_report(handle, key, ReadRepairReport(
                operation_id, OperationStatus.ERROR, None, (), None, (),
                incomplete_reason=reason))

        future.add_done_callback(settle)
        try:
            return foreground.result(timeout=max(0.0, deadline - time.monotonic()))
        except TimeoutError:
            return self._result(OperationStatus.TIMEOUT, operation_id, propagation=handle,
                                error="Eventual-RR GET deadline expired")

    # -- coordinator task ---------------------------------------------------

    def _run_rr_get(self, admission: "CoordinatorAdmission", key: str, deadline: float,
                    required: int, foreground: Future, handle: ReadRepairHandle,
                    admitted_at: float) -> None:
        """Foreground half: five reads, reply after R. The rest runs on the background pool."""
        operation_id, context = admission.operation_id, admission.context
        started = time.monotonic()

        def settle(result: ProtocolResult) -> None:
            try:
                foreground.set_result(result)
            except InvalidStateError:
                pass

        def give_up(status: OperationStatus, reason: str, replies=(), *, error=None) -> None:
            settle(self._result(status, operation_id, propagation=handle,
                                error=error or reason))
            self._finish_report(handle, key, ReadRepairReport(
                operation_id, status, None, tuple(replies), None, (),
                incomplete_reason=reason))

        if context.mode != self.MODE:
            give_up(OperationStatus.ERROR, self.MODE_ERROR)
            return
        if deadline - started <= 0:
            give_up(OperationStatus.TIMEOUT, "deadline expired before dispatch")
            return
        owners = self.runtime.manifest.owners(key)
        lease = AttemptLease(
            admitted_at=admitted_at,
            foreground_seconds=min(deadline - admitted_at, RR_READ_LIFETIME_SECONDS),
            continuation_seconds=RR_READ_CONTINUATION_SECONDS,
            max_lifetime=RR_READ_LIFETIME_SECONDS)
        try:
            reservation = self.runtime.reserve_attempts(
                5, deadline=admission.deadline, queue_deadline=lease.hard_deadline,
                admission=admission, attempt_lease=lease)
        except (RuntimeError, ValueError) as error:
            give_up(OperationStatus.ERROR, "cannot reserve read attempts",
                    error=f"cannot reserve read attempts: {error}")
            return
        if reservation is None:
            give_up(OperationStatus.OVERLOADED,
                    "cannot reserve read attempts; no replica request was sent",
                    error="cannot reserve read attempts")
            return
        calls: list[OwnerCall] = []
        preflight: list[OwnerFailure] = []
        client = None
        try:
            for owner in owners:
                try:
                    calls.append(self.runtime.dispatch_owner(
                        owner.vm_id, RpcOperation.READ_LOCAL, key,
                        deadline=lease.hard_deadline, reservation=reservation,
                        phase=RR_READ_PHASE, operation_id=operation_id,
                        attempt_lease=lease, admission=admission))
                except Exception as error:
                    failure = getattr(error, "failure", None)
                    if not isinstance(failure, OwnerFailure):
                        failure = OwnerFailure(owner.vm_id, OwnerOutcome.ERROR, str(error))
                    preflight.append(failure)
            if not calls:
                give_up(OperationStatus.ERROR, "no prescribed owner read could be dispatched",
                        [self._failed_read(f) for f in preflight])
                return
            collection = collect_distinct(
                tuple(calls), owners=owners, manifest=self.runtime.manifest,
                required=required, operation_id=operation_id, phase=RR_READ_PHASE,
                context=context, deadline=min(deadline, lease.foreground_deadline),
                preflight_failures=preflight, key=key)
            if not collection.reached:
                # R is out of reach: stop the unused reads and schedule no repair.
                self._stop_reads(lease, calls)
                give_up(self._failure_status(collection),
                        "R replies were not reached; no repair scheduled",
                        self._gather_reads(calls, preflight),
                        error=self._failure_text(collection, RR_READ_PHASE))
                return
            returned = self._returned_record(collection)
            if returned is False:
                self._stop_reads(lease, calls)
                give_up(OperationStatus.ERROR, "equal version with conflicting bytes",
                        self._gather_reads(calls, preflight),
                        error="equal Eventual version has conflicting bytes")
                return
            owner_ids = tuple(reply.owner_id for reply in collection.qualifying)
            if returned is None:
                outcome = self._result(OperationStatus.NOT_FOUND, operation_id,
                                       owners=owner_ids, propagation=handle)
            else:
                outcome = self._result(OperationStatus.OK, operation_id,
                                       value=returned.value, version=returned.version,
                                       owners=owner_ids, propagation=handle)
            lease.finish_foreground(min(time.monotonic(), lease.foreground_deadline))
            settle(outcome)  # the client result is frozen from here on
            client = outcome
        except Exception as error:
            give_up(OperationStatus.ERROR, f"internal error: {type(error).__name__}",
                    error=f"Eventual-RR GET failed: {type(error).__name__}")
        finally:
            reservation.close()
        if client is not None:
            self._hand_off(context, key, owners, calls, preflight, lease, client, handle,
                           admitted_at)

    def _hand_off(self, context, key, owners, calls, preflight, lease, client, handle,
                  admitted_at) -> None:
        """Give the comparison and repair to the bounded background pool, not a worker."""
        resources = self.runtime.coordinator_resources
        future = None
        try:
            if resources is not None:
                future = resources.background.submit(
                    self._background, context, key, owners, calls, preflight, lease,
                    client, handle, admitted_at, deadline=time.monotonic() + 1.0)
        except (RuntimeError, ValueError):
            future = None
        if future is None:
            self._stop_reads(lease, calls)
            self._finish_report(handle, key, ReadRepairReport(
                handle.operation_id, client.status, client.version,
                tuple(self._gather_reads(calls, preflight)), None, (),
                incomplete_reason="background capacity unavailable; no repair"))
            return

        def abandoned(done: Future) -> None:
            # A queued comparison dropped at shutdown must still finish its report.
            if done.cancelled():
                self._stop_reads(lease, calls)
                self._finish_report(handle, key, ReadRepairReport(
                    handle.operation_id, client.status, client.version, (), None, (),
                    incomplete_reason="shut down before the comparison started; no repair"))

        future.add_done_callback(abandoned)

    def _background(self, context, key, owners, calls, preflight, lease, client, handle,
                    admitted_at) -> None:
        try:
            self._compare_and_repair(context, key, owners, calls, preflight, lease,
                                     client, handle, admitted_at)
        except Exception as error:
            self._finish_report(handle, key, ReadRepairReport(
                handle.operation_id, client.status, client.version, (), None, (),
                incomplete_reason=f"internal error: {type(error).__name__}"))

    def _stop_reads(self, lease: AttemptLease, calls) -> None:
        lease.cancel()
        for call in calls:
            call.future.cancel()

    @staticmethod
    def _returned_record(collection):
        """Maximum FOUND record among the first R replies; None if all absent; False on conflict."""
        records = [Record(reply.response.version, reply.response.value)
                   for reply in collection.qualifying
                   if reply.response.status == RpcStatus.FOUND]
        if not records:
            return None
        maximum = records[0]
        for record in records[1:]:
            order = record.version.compare(maximum.version)
            if order == 0 and record.value != maximum.value:
                return False
            if order > 0:
                maximum = record
        return maximum

    # -- evidence -----------------------------------------------------------

    @staticmethod
    def _failed_read(failure: OwnerFailure) -> ReadEvidence:
        return ReadEvidence(failure.owner_id, None, failure.outcome, error=failure.error)

    def _read_evidence(self, call: OwnerCall) -> tuple[ReadEvidence, bytes]:
        owner_id, generation = call.node.vm_id, call.expected_generation

        def failed(outcome, error=None):
            return ReadEvidence(owner_id, generation, outcome, error=error), b""

        future = call.future
        if future.cancelled():
            return failed(OwnerOutcome.CANCELLED, "read canceled before it started")
        if not future.done():
            return failed(OwnerOutcome.TIMEOUT, "read did not finish in the comparison window")
        try:
            completion = future.result()
            if not isinstance(completion, RpcCompletion):
                raise TypeError("invalid completion")
        except BaseException as error:
            return failed(OwnerOutcome.ERROR, type(error).__name__)
        outcome = completion.outcome
        if outcome.kind != RpcOutcomeKind.RESPONSE or outcome.response is None:
            names = {RpcOutcomeKind.TIMEOUT: OwnerOutcome.TIMEOUT,
                     RpcOutcomeKind.UNREACHABLE: OwnerOutcome.UNREACHABLE,
                     RpcOutcomeKind.INVALID_RESPONSE: OwnerOutcome.INVALID_RESPONSE,
                     RpcOutcomeKind.OVERLOADED: OwnerOutcome.OVERLOADED,
                     RpcOutcomeKind.CANCELLED: OwnerOutcome.CANCELLED}
            return failed(names.get(outcome.kind, OwnerOutcome.ERROR), outcome.error)
        response = outcome.response
        try:
            response.validate_for(call.request, call.node, self.runtime.manifest, generation)
        except (TypeError, ValueError) as error:
            return failed(OwnerOutcome.INVALID_RESPONSE, str(error))
        if (response.operation_id != call.request.operation_id
                or response.phase != RR_READ_PHASE):
            return failed(OwnerOutcome.INVALID_RESPONSE, "operation or phase did not match")
        if response.status == RpcStatus.FOUND:
            digest = hashlib.sha256(response.value).hexdigest()
            return ReadEvidence(owner_id, generation, None, RpcStatus.FOUND,
                                response.version, digest), response.value
        if response.status == RpcStatus.NOT_FOUND:
            return ReadEvidence(owner_id, generation, None, RpcStatus.NOT_FOUND), b""
        if response.status == RpcStatus.STALE_EPOCH:
            return failed(OwnerOutcome.STALE_EPOCH, response.error)
        return failed(OwnerOutcome.ERROR, response.error or response.status.value)

    def _gather_reads(self, calls, preflight) -> list[ReadEvidence]:
        rows = [self._read_evidence(call)[0] for call in calls]
        rows.extend(self._failed_read(failure) for failure in preflight)
        return rows

    # -- comparison and the single repair pass ------------------------------

    def _compare_and_repair(self, context, key, owners, calls, preflight, lease, client,
                            handle, admitted_at) -> None:
        operation_id = handle.operation_id
        futures = [call.future for call in calls]
        # Window: until every read is terminal or the read cutoff (<= admission + 15 s).
        wait(futures, timeout=max(0.0, lease.current_deadline() - time.monotonic()))
        for future in futures:
            future.cancel()  # a read still queued at the cutoff never starts
        wait(futures, timeout=RR_SETTLE_SECONDS)
        gathered = [self._read_evidence(call) for call in calls]
        replies = [row for row, _value in gathered]
        replies.extend(self._failed_read(failure) for failure in preflight)

        found = [(row, value) for row, value in gathered
                 if row.valid and row.status == RpcStatus.FOUND]
        conflict = False
        best_row = best_value = None
        for row, value in found:
            if best_row is None:
                best_row, best_value = row, value
                continue
            order = row.version.compare(best_row.version)
            if order == 0 and row.value_digest != best_row.value_digest:
                conflict = True
            elif order > 0:
                best_row, best_value = row, value
        maximum = None if best_row is None else best_row.version
        if conflict:
            # Two different values share one version: which is right is unknowable.
            self._finish_report(handle, key, ReadRepairReport(
                operation_id, client.status, client.version, tuple(replies), maximum, (),
                conflict=True, incomplete_reason="conflicting bytes for one version; no repair"))
            return
        targets = []
        if maximum is not None:
            for row, _value in gathered:
                if not row.valid:
                    continue
                if row.status == RpcStatus.NOT_FOUND or (
                        row.status == RpcStatus.FOUND and row.version.compare(maximum) < 0):
                    targets.append(row.owner_id)
        repairs = self._repair(context, operation_id, key, targets, maximum, best_value,
                               admitted_at)
        self._finish_report(handle, key, ReadRepairReport(
            operation_id, client.status, client.version, tuple(replies), maximum,
            tuple(repairs)))

    def _repair(self, context, operation_id, key, targets, version, value, admitted_at):
        """Send one conditional install to each stale or missing reachable owner."""
        if not targets:
            return []

        def skipped(reason):
            return [RepairResult(owner, RepairStatus.NOT_ATTEMPTED, version, error=reason)
                    for owner in targets]

        try:
            current = self.runtime.context
        except RuntimeError:
            return skipped("node is not accepting work")
        if current != context:
            return skipped("mode or epoch changed after the read")
        # Everything, including the settle wait below, ends by admission + 20 s.
        window = min(RR_REPAIR_SECONDS, admitted_at + RR_TOTAL_LIFETIME_SECONDS
                     - time.monotonic() - RR_SETTLE_SECONDS)
        if window <= 0.05:
            return skipped("20-second repair limit reached")
        lease = AttemptLease(admitted_at=time.monotonic(), foreground_seconds=window,
                             continuation_seconds=window, max_lifetime=window)
        reserve_deadline = time.monotonic() + 1.0
        try:
            reservation = self.runtime.reserve_attempts(
                len(targets), deadline=reserve_deadline,
                queue_deadline=max(lease.hard_deadline, reserve_deadline))
        except (RuntimeError, ValueError) as error:
            return skipped(f"cannot reserve repair attempts: {type(error).__name__}")
        if reservation is None:
            return skipped("repair capacity unavailable")
        results: dict[str, RepairResult] = {}
        calls: list[OwnerCall] = []
        try:
            for owner in targets:
                try:
                    # No coordinator admission is held here; the lease bounds the call.
                    calls.append(self.runtime.dispatch_owner(
                        owner, RpcOperation.INSTALL_IF_NEWER, key,
                        deadline=lease.hard_deadline, reservation=reservation,
                        phase=RR_REPAIR_PHASE, operation_id=operation_id,
                        version=version, value=value, attempt_lease=lease))
                except Exception as error:
                    failure = getattr(error, "failure", None)
                    results[owner] = RepairResult(
                        owner, RepairStatus.NOT_ATTEMPTED, version,
                        error=(failure.error if failure is not None else type(error).__name__))
            futures = [call.future for call in calls]
            wait(futures, timeout=max(0.0, lease.hard_deadline - time.monotonic()))
            lease.cancel()
            wait(futures, timeout=RR_SETTLE_SECONDS)
            for call in calls:
                results[call.node.vm_id] = self._repair_result(call, version)
        finally:
            reservation.close()
        return [results[owner] for owner in targets]

    def _repair_result(self, call: OwnerCall, version: Version) -> RepairResult:
        owner = call.node.vm_id

        def failed(text):
            return RepairResult(owner, RepairStatus.FAILED, version, error=text)

        future = call.future
        if future.cancelled():
            return failed("repair canceled before it started")
        if not future.done():
            return failed("repair did not finish in its window")
        try:
            outcome = future.result().outcome
        except BaseException as error:
            return failed(type(error).__name__)
        if outcome.kind != RpcOutcomeKind.RESPONSE or outcome.response is None:
            return failed(outcome.kind.value + (f": {outcome.error}" if outcome.error else ""))
        response = outcome.response
        try:
            response.validate_for(call.request, call.node, self.runtime.manifest,
                                  call.expected_generation)
        except (TypeError, ValueError) as error:
            return failed(f"invalid response: {error}")
        if (response.operation_id != call.request.operation_id
                or response.phase != RR_REPAIR_PHASE):
            return failed("operation or phase did not match")
        if (response.status in (RpcStatus.INSTALLED, RpcStatus.UNCHANGED)
                and response.durable and response.stored_version == version):
            return RepairResult(owner, RepairStatus.REPAIRED, version, response.stored_version)
        if (response.status == RpcStatus.SUPERSEDED and response.durable
                and response.stored_version is not None
                and response.stored_version.compare(version) > 0):
            # A newer write won. Its bytes are not in this reply, so nothing is propagated.
            return RepairResult(owner, RepairStatus.SUPERSEDED_NEWER, version,
                                response.stored_version)
        return failed(response.error or response.status.value)

    def _finish_report(self, handle: ReadRepairHandle, key: str,
                       report: ReadRepairReport) -> None:
        if not handle.claim():
            return
        try:
            self._log_report(key, report)  # logged before waiters are released
        finally:
            handle.finish(report)

    def _log_report(self, key: str, report: ReadRepairReport) -> None:
        log_read_repair(getattr(self.runtime, "logger", None), node_id=self.runtime.vm_id,
                        operation_id=report.operation_id, fields={
                            "operation": "get", "mode": "eventual-RR", "key": key,
                            "returned_status": (report.returned_status.value
                                                if report.returned_status else None),
                            "returned_version": _version_text(report.returned_version),
                            "reachable": list(report.reachable),
                            "responses": [{
                                "owner": row.owner_id,
                                "result": (row.status.value if row.valid
                                           else row.outcome.value),
                                "version": _version_text(row.version),
                                "digest": row.value_digest, "error": row.error,
                            } for row in report.replies],
                            "maximum_version": _version_text(report.maximum_version),
                            "repairs": [{
                                "owner": repair.owner_id, "status": repair.status.value,
                                "requested_version": _version_text(repair.requested_version),
                                "stored_version": _version_text(repair.stored_version),
                                "error": repair.error,
                            } for repair in report.repairs],
                            "conflict": report.conflict,
                            "incomplete_reason": report.incomplete_reason})
