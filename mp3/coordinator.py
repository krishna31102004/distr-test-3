"""Shared coordinator evidence, bounded dispatch and schema-2 activation (KD-08/11/12/14/15).

This module deliberately contains no Strong ABD or Eventual coordinator. It provides
the bounded, version-aware primitives those future algorithms will use.
"""

from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
from enum import Enum
import json
import math
from pathlib import Path
import queue
import threading
import time
from typing import Callable, Iterable, Mapping
import uuid

from .attempts import AttemptLease
from .models import ConsistencyMode, Context, Version
from .ring import Manifest, Node
from .rpc import (
    PROTOCOL_SCHEMA_VERSION, ReplicaRequest, ReplicaResponse, RpcOutcome,
    RpcOutcomeKind, RpcOperation, RpcProtocolError, RpcStatus,
)


DEFAULT_COORDINATOR_WORKERS = 16
DEFAULT_COORDINATOR_QUEUE = 32
DEFAULT_ATTEMPT_WORKERS = 16
DEFAULT_ATTEMPT_QUEUE = 64
DEFAULT_ADMISSION_WAIT_SECONDS = 5.0


class OperationStatus(str, Enum):
    OK = "OK"
    NOT_FOUND = "NOT_FOUND"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    OVERLOADED = "OVERLOADED"


class OwnerOutcome(str, Enum):
    DURABLE_ACK = "DURABLE_ACK"
    SUPERSEDED = "SUPERSEDED"
    NOT_PRESCRIBED_OWNER = "NOT_PRESCRIBED_OWNER"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    STALE_EPOCH = "STALE_EPOCH"
    UNREACHABLE = "UNREACHABLE"
    TIMEOUT = "TIMEOUT"
    INVALID_RESPONSE = "INVALID_RESPONSE"
    OVERLOADED = "OVERLOADED"
    ERROR = "ERROR"
    CANCELLED = "CANCELLED"
    DUPLICATE_OWNER = "DUPLICATE_OWNER"
    LATE = "LATE"
    NOT_STARTED_EXPIRED = "NOT_STARTED_EXPIRED"
    NOT_STARTED_SHUTDOWN = "NOT_STARTED_SHUTDOWN"


class AttemptState(str, Enum):
    RESERVED = "RESERVED"
    QUEUED = "QUEUED"
    IN_FLIGHT = "IN_FLIGHT"
    TERMINAL = "TERMINAL"


@dataclass(frozen=True)
class ProtocolResult:
    status: OperationStatus
    value: bytes | None
    version: Version | None
    operation_id: str
    qualifying_owners: tuple[str, ...] = ()
    completion_unknown: bool = False
    propagation: object | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.status, OperationStatus):
            raise ValueError("status must be an OperationStatus")
        if self.value is not None and not isinstance(self.value, bytes):
            raise ValueError("value must be bytes or None")
        if self.status != OperationStatus.OK and (self.value is not None or self.version is not None):
            raise ValueError("non-OK results cannot carry a value or version")
        if type(self.completion_unknown) is not bool:
            raise ValueError("completion_unknown must be a boolean")
        if not isinstance(self.operation_id, str):
            raise ValueError("operation_id must be text")
        try:
            parsed = uuid.UUID(self.operation_id)
        except (ValueError, AttributeError, TypeError) as error:
            raise ValueError("operation_id must be a canonical lowercase UUID4") from error
        if str(parsed) != self.operation_id or parsed.version != 4:
            raise ValueError("operation_id must be a canonical lowercase UUID4")


@dataclass(frozen=True)
class RpcCompletion:
    outcome: RpcOutcome
    completed_at: float

    def __post_init__(self) -> None:
        if not isinstance(self.outcome, RpcOutcome):
            raise ValueError("outcome must be an RpcOutcome")
        if (type(self.completed_at) not in (int, float)
                or not math.isfinite(self.completed_at)):
            raise ValueError("completed_at must be a finite monotonic timestamp")


@dataclass(frozen=True)
class QualifiedReply:
    owner_id: str
    request: ReplicaRequest
    response: ReplicaResponse
    completed_at: float


@dataclass(frozen=True)
class OwnerFailure:
    owner_id: str
    outcome: OwnerOutcome
    error: str | None = None
    completion_unknown: bool = False


class OwnerDispatchError(ValueError):
    """A local preflight rejection with explicit failure evidence and no RPC sent."""

    def __init__(self, failure: OwnerFailure):
        self.failure = failure
        super().__init__(failure.error or failure.outcome.value)


@dataclass(frozen=True)
class QuorumCollection:
    qualifying: tuple[QualifiedReply, ...]
    failures: tuple[OwnerFailure, ...]
    reached: bool
    deadline_expired: bool
    fatal_error: str | None = None
    continuing: tuple[OwnerCall, ...] = ()

    @property
    def completion_unknown(self) -> bool:
        return any(item.completion_unknown for item in self.failures)


@dataclass(frozen=True)
class OwnerCall:
    node: Node
    request: ReplicaRequest
    expected_generation: int
    future: Future


@dataclass(frozen=True)
class AttemptReport:
    terminal: bool
    outcomes: tuple[tuple[str, OwnerOutcome | None, bool], ...]


class AttemptLedger:
    """Track exactly one attempt for each of the five stable prescribed owners."""

    def __init__(self, owners: Iterable[Node], *, release: Callable[[str], None] | None = None):
        rows = tuple(owners)
        ids = tuple(node.vm_id for node in rows)
        if len(ids) != 5 or len(set(ids)) != 5:
            raise ValueError("attempt ledger requires five distinct prescribed owners")
        self._lock = threading.Lock()
        self._states = {vm_id: AttemptState.RESERVED for vm_id in ids}
        self._outcomes: dict[str, tuple[OwnerOutcome | None, bool]] = {
            vm_id: (None, False) for vm_id in ids
        }
        self._release = release

    def transition(self, owner_id: str, state: AttemptState) -> None:
        with self._lock:
            current = self._states.get(owner_id)
            allowed = {
                AttemptState.RESERVED: {AttemptState.QUEUED, AttemptState.TERMINAL},
                AttemptState.QUEUED: {AttemptState.IN_FLIGHT, AttemptState.TERMINAL},
                AttemptState.IN_FLIGHT: {AttemptState.TERMINAL},
                AttemptState.TERMINAL: set(),
            }
            if current is None or state not in allowed[current]:
                raise RuntimeError(f"invalid owner attempt transition: {owner_id} {current}->{state}")
            self._states[owner_id] = state

    def finish(self, owner_id: str, outcome: OwnerOutcome, *,
               completion_unknown: bool = False) -> None:
        if not isinstance(outcome, OwnerOutcome):
            raise TypeError("outcome must be an OwnerOutcome")
        if type(completion_unknown) is not bool:
            raise TypeError("completion_unknown must be a boolean")
        if outcome == OwnerOutcome.DURABLE_ACK and completion_unknown:
            raise ValueError("a confirmed durable acknowledgment cannot be uncertain")
        with self._lock:
            current = self._states.get(owner_id)
            if current not in (AttemptState.RESERVED, AttemptState.QUEUED,
                               AttemptState.IN_FLIGHT):
                raise RuntimeError("owner attempt is already terminal or unknown")
            self._states[owner_id] = AttemptState.TERMINAL
            self._outcomes[owner_id] = (outcome, bool(completion_unknown))
        if self._release is not None:
            self._release(owner_id)

    def report(self) -> AttemptReport:
        with self._lock:
            outcomes = tuple((owner, *self._outcomes[owner]) for owner in self._states)
            terminal = all(state == AttemptState.TERMINAL for state in self._states.values())
            return AttemptReport(terminal, outcomes)


@dataclass(frozen=True)
class Schema2Activation:
    """Explicit operator activation based on a complete ten-identity release inventory.

    The inventory is an assertion from deployment tooling, not a live health check or
    proof that university machines actually run that release.
    """

    manifest_id: str
    release_id: str
    compatible_nodes: tuple[str, ...]
    schema_version: int = PROTOCOL_SCHEMA_VERSION

    @classmethod
    def from_inventory(cls, manifest: Manifest,
                       inventory: Mapping[str, Mapping[str, object]]) -> "Schema2Activation":
        if not isinstance(manifest, Manifest) or not isinstance(inventory, Mapping):
            raise TypeError("manifest and compatibility inventory are required")
        expected = {node.vm_id for node in manifest.nodes}
        if set(inventory) != expected:
            raise ValueError("schema-2 activation requires a complete ten-node inventory")
        release_ids = set()
        for vm_id, record in inventory.items():
            if not isinstance(record, Mapping) or set(record) != {"release_id", "schema_version"}:
                raise ValueError(f"invalid release record for {vm_id}")
            release_id, schema_version = record["release_id"], record["schema_version"]
            if not isinstance(release_id, str) or not release_id.strip():
                raise ValueError(f"invalid release ID for {vm_id}")
            if type(schema_version) is not int or schema_version < PROTOCOL_SCHEMA_VERSION:
                raise ValueError(f"{vm_id} does not advertise schema 2")
            release_ids.add(release_id)
        if len(release_ids) != 1:
            raise ValueError("all ten inventory entries must identify the same compatible release")
        return cls(manifest.fingerprint, next(iter(release_ids)),
                   tuple(node.vm_id for node in manifest.nodes))

    @classmethod
    def from_file(cls, manifest: Manifest, path: str | Path) -> "Schema2Activation":
        def unique_object(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError(f"duplicate inventory field: {key}")
                result[key] = value
            return result

        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"),
                              object_pairs_hook=unique_object)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise ValueError(f"cannot read schema-2 inventory: {error}") from error
        if (not isinstance(data, dict) or set(data) != {"manifest_id", "nodes"}
                or data["manifest_id"] != manifest.fingerprint
                or not isinstance(data["nodes"], dict)):
            raise ValueError("inventory has wrong shape or manifest fingerprint")
        return cls.from_inventory(manifest, data["nodes"])

    def enabled_for(self, manifest: Manifest) -> bool:
        return (isinstance(manifest, Manifest)
                and manifest.fingerprint == self.manifest_id
                and self.schema_version == PROTOCOL_SCHEMA_VERSION
                and set(self.compatible_nodes) == {node.vm_id for node in manifest.nodes})


@dataclass
class _Job:
    future: Future
    function: Callable
    args: tuple
    kwargs: dict
    lease: AttemptLease | None


class CapacityReservation:
    """Atomic reservation for one or more bounded worker slots."""

    def __init__(self, executor: "BoundedExecutor", count: int, deadline: float):
        self._executor = executor
        self._remaining = count
        self._closed = False
        self._deadline = deadline

    @property
    def remaining(self) -> int:
        with self._executor._condition:
            return self._remaining

    def submit(self, function: Callable, *args, lease: AttemptLease | None = None,
               **kwargs) -> Future:
        return self._executor._submit_reserved(self, function, args, kwargs, lease)

    def close(self) -> None:
        self._executor._release_unused(self)


@dataclass(frozen=True)
class WorkerShutdownReport:
    accepted: int
    incomplete: int
    workers_joined: bool

    @property
    def clean(self) -> bool:
        return self.incomplete == 0 and self.workers_joined


class BoundedExecutor:
    """Fixed workers and bounded queue with atomic active+queued reservations."""

    def __init__(self, *, workers: int, max_pending: int, name: str):
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be positive")
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be positive")
        self.workers = workers
        self.max_pending = max_pending
        self.capacity = workers + max_pending
        self.name = name
        self._queue: queue.Queue[_Job] = queue.Queue(maxsize=max_pending)
        self._condition = threading.Condition()
        self._used = 0
        self._unsubmitted = 0
        self._closed = False
        self._active: dict[Future, AttemptLease | None] = {}
        self._reservations: set[CapacityReservation] = set()
        self._threads = [
            threading.Thread(target=self._worker, name=f"{name}-{index + 1}", daemon=True)
            for index in range(workers)
        ]
        self._accepted = 0
        for thread in self._threads:
            thread.start()

    @property
    def counts(self) -> tuple[int, int, int]:
        with self._condition:
            return len(self._active), self._queue.qsize(), self._unsubmitted

    def reserve(self, count: int, *, deadline: float | None = None,
                queue_deadline: float | None = None) -> CapacityReservation | None:
        if type(count) is not int or count < 1 or count > self.capacity:
            raise ValueError("reservation count exceeds bounded capacity")
        if deadline is None:
            deadline = time.monotonic() + DEFAULT_ADMISSION_WAIT_SECONDS
        elif type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("admission deadline must be a finite monotonic timestamp")
        if queue_deadline is None:
            queue_deadline = deadline
        elif (type(queue_deadline) not in (int, float)
              or not math.isfinite(queue_deadline) or queue_deadline < deadline):
            raise ValueError("queue deadline must be finite and no earlier than admission")
        with self._condition:
            if deadline <= time.monotonic():
                return None
            while self._used + count > self.capacity and not self._closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._condition.wait(min(remaining, 0.05))
            if self._closed:
                return None
            self._used += count
            self._unsubmitted += count
            reservation = CapacityReservation(self, count, queue_deadline)
            self._reservations.add(reservation)
            return reservation

    def submit(self, function: Callable, *args, deadline: float | None = None,
               lease: AttemptLease | None = None, **kwargs) -> Future | None:
        reservation = self.reserve(1, deadline=deadline)
        if reservation is None:
            return None
        try:
            return reservation.submit(function, *args, lease=lease, **kwargs)
        finally:
            reservation.close()

    def _submit_reserved(self, reservation: CapacityReservation, function: Callable,
                         args: tuple, kwargs: dict,
                         lease: AttemptLease | None) -> Future:
        if not callable(function):
            raise TypeError("worker function must be callable")
        with self._condition:
            if (reservation._executor is not self or reservation._closed
                    or self._closed or reservation._remaining <= 0):
                raise RuntimeError("reservation is closed or exhausted")
            future = Future()
            job = _Job(future, function, args, kwargs, lease)
            # A reservation accounts for active+queued+unsubmitted capacity, but
            # a burst of submissions can transiently fill the physical queue
            # before idle workers dequeue. Wait on that already-reserved slot;
            # a worker notifies after taking a job. This preserves the guarantee
            # made by reserve() without making the queue itself unbounded.
            while self._queue.full():
                if self._closed or reservation._closed:
                    raise RuntimeError("executor closed before reserved submission")
                remaining = reservation._deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("reserved submission deadline expired")
                self._condition.wait(min(remaining, 0.05))
            if self._closed or reservation._closed:
                raise RuntimeError("executor closed before reserved submission")
            self._queue.put_nowait(job)
            reservation._remaining -= 1
            self._unsubmitted -= 1
            self._accepted += 1
            self._condition.notify_all()
            return future

    def _release_unused(self, reservation: CapacityReservation) -> None:
        with self._condition:
            if reservation._executor is not self or reservation._closed:
                return
            reservation._closed = True
            unused = reservation._remaining
            reservation._remaining = 0
            self._unsubmitted -= unused
            self._used -= unused
            self._reservations.discard(reservation)
            self._condition.notify_all()

    def _worker(self) -> None:
        while True:
            try:
                job = self._queue.get(timeout=0.05)
            except queue.Empty:
                with self._condition:
                    if self._closed and self._used == 0:
                        return
                continue
            future = job.future
            if not future.set_running_or_notify_cancel():
                self._finish_job(future)
                self._queue.task_done()
                continue
            with self._condition:
                self._active[future] = job.lease
                self._condition.notify_all()
            try:
                result = job.function(*job.args, **job.kwargs)
            except BaseException as error:
                future.set_exception(error)
            else:
                future.set_result(result)
            finally:
                self._finish_job(future)
                self._queue.task_done()

    def _finish_job(self, future: Future) -> None:
        with self._condition:
            self._active.pop(future, None)
            self._used -= 1
            self._condition.notify_all()

    def shutdown(self, *, timeout: float, force: bool = False) -> WorkerShutdownReport:
        if (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout < 0):
            raise ValueError("timeout must be finite and nonnegative")
        deadline = time.monotonic() + timeout
        with self._condition:
            self._closed = True
            reservations = tuple(self._reservations)
        for reservation in reservations:
            reservation.close()
        if force:
            self._cancel_queued()
            with self._condition:
                leases = tuple(self._active.values())
            for lease in leases:
                if lease is not None:
                    lease.cancel()
        with self._condition:
            while self._used and not force:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(min(remaining, 0.05))
            if self._used and not force:
                force = True
        if force:
            self._cancel_queued()
            with self._condition:
                leases = tuple(self._active.values())
            for lease in leases:
                if lease is not None:
                    lease.cancel()
        # The workers poll the queue and exit after accepted work reaches zero.
        for thread in self._threads:
            remaining = max(0.0, deadline - time.monotonic())
            thread.join(remaining)
        with self._condition:
            incomplete = self._used
            joined = all(not thread.is_alive() for thread in self._threads)
            return WorkerShutdownReport(self._accepted, incomplete, joined)

    def _cancel_queued(self) -> None:
        while True:
            try:
                job = self._queue.get_nowait()
            except queue.Empty:
                break
            if job.lease is not None:
                job.lease.cancel()
            job.future.cancel()
            self._finish_job(job.future)
            self._queue.task_done()


@dataclass(frozen=True)
class CoordinatorShutdownReport:
    coordinators: WorkerShutdownReport
    attempts: WorkerShutdownReport

    @property
    def clean(self) -> bool:
        return self.coordinators.clean and self.attempts.clean


class CoordinatorResources:
    """Node-owned bounded operation and outbound-attempt workers."""

    def __init__(self, *, coordinator_workers: int = DEFAULT_COORDINATOR_WORKERS,
                 coordinator_queue: int = DEFAULT_COORDINATOR_QUEUE,
                 attempt_workers: int = DEFAULT_ATTEMPT_WORKERS,
                 attempt_queue: int = DEFAULT_ATTEMPT_QUEUE):
        self.coordinators = BoundedExecutor(
            workers=coordinator_workers, max_pending=coordinator_queue, name="mp3-coordinator")
        try:
            self.attempts = BoundedExecutor(
                workers=attempt_workers, max_pending=attempt_queue, name="mp3-attempt")
        except BaseException:
            self.coordinators.shutdown(timeout=1.0, force=True)
            raise

    def reserve_attempts(self, count: int, *, deadline: float | None = None,
                         queue_deadline: float | None = None):
        return self.attempts.reserve(count, deadline=deadline,
                                     queue_deadline=queue_deadline)

    def shutdown(self, *, timeout: float, force: bool = False) -> CoordinatorShutdownReport:
        # Close operation admission first. Existing operation tasks can still use
        # already-reserved outbound slots until they end; then outbound admission closes.
        deadline = time.monotonic() + timeout
        coordinator_report = self.coordinators.shutdown(
            timeout=max(0.0, deadline - time.monotonic()), force=force)
        attempt_report = self.attempts.shutdown(
            timeout=max(0.0, deadline - time.monotonic()),
            force=force or not coordinator_report.clean)
        return CoordinatorShutdownReport(coordinator_report, attempt_report)


def dispatch_owner(client, node: Node, request: ReplicaRequest, *, manifest: Manifest,
                   deadline: float,
                   expected_generation: int, executor: BoundedExecutor,
                   reservation: CapacityReservation | None = None,
                   attempt_lease: AttemptLease | None = None) -> OwnerCall:
    """Dispatch exactly one RPC with dispatch-time generation evidence."""
    if not isinstance(manifest, Manifest):
        raise TypeError("dispatch requires the validated owner manifest")
    if (not isinstance(node, Node) or request.target_id != node.vm_id
            or type(expected_generation) is not int or expected_generation <= 0):
        raise ValueError("owner call target and expected generation are invalid")
    if request.schema == PROTOCOL_SCHEMA_VERSION:
        prescribed_ids = {owner.vm_id for owner in manifest.owners(request.key)}
        if node.vm_id not in prescribed_ids:
            raise OwnerDispatchError(OwnerFailure(
                node.vm_id, OwnerOutcome.NOT_PRESCRIBED_OWNER,
                f"{node.vm_id} is not a prescribed owner for key {request.key!r}"))
    if request.manifest_id != manifest.fingerprint:
        raise ValueError("owner request belongs to a different manifest")

    def invoke() -> RpcCompletion:
        outcome = client.call_replica(
            node, request, deadline=deadline,
            expected_target_generation=expected_generation,
            attempt_lease=attempt_lease)
        return RpcCompletion(outcome, time.monotonic())

    if reservation is None:
        future = executor.submit(invoke, deadline=deadline, lease=attempt_lease)
        if future is None:
            future = Future()
            future.set_result(RpcCompletion(
                RpcOutcome(RpcOutcomeKind.OVERLOADED, error="outbound attempt capacity exhausted"),
                time.monotonic()))
    else:
        try:
            future = reservation.submit(invoke, lease=attempt_lease)
        except (RuntimeError, ValueError) as error:
            future = Future()
            future.set_result(RpcCompletion(
                RpcOutcome(RpcOutcomeKind.OVERLOADED, error=str(error)), time.monotonic()))
    return OwnerCall(node, request, expected_generation, future)


def _failure_from_outcome(owner_id: str, outcome: RpcOutcome) -> OwnerFailure:
    mapping = {
        RpcOutcomeKind.TIMEOUT: OwnerOutcome.TIMEOUT,
        RpcOutcomeKind.UNREACHABLE: OwnerOutcome.UNREACHABLE,
        RpcOutcomeKind.INVALID_RESPONSE: OwnerOutcome.INVALID_RESPONSE,
        RpcOutcomeKind.OVERLOADED: OwnerOutcome.OVERLOADED,
        RpcOutcomeKind.CANCELLED: OwnerOutcome.CANCELLED,
        RpcOutcomeKind.ERROR: OwnerOutcome.ERROR,
    }
    return OwnerFailure(owner_id, mapping.get(outcome.kind, OwnerOutcome.ERROR),
                        outcome.error, outcome.completion_unknown)


def _qualifies(phase: str, request: ReplicaRequest,
               response: ReplicaResponse) -> bool:
    if phase in ("strong_query", "eventual_read"):
        return response.status in (RpcStatus.FOUND, RpcStatus.NOT_FOUND)
    if phase in ("strong_write", "strong_readback"):
        if response.status in (RpcStatus.INSTALLED, RpcStatus.UNCHANGED):
            return (response.durable and response.stored_version == request.version)
        return (response.status == RpcStatus.SUPERSEDED and response.durable
                and response.stored_version is not None
                and request.version is not None
                and response.stored_version.compare(request.version) > 0)
    if phase == "eventual_put":
        return (response.status in (RpcStatus.INSTALLED, RpcStatus.UNCHANGED)
                and response.durable and response.stored_version == request.version)
    return False


def collect_distinct(calls: Iterable[OwnerCall], *, owners: Iterable[Node],
                     manifest: Manifest, required: int, operation_id: str,
                     phase: str, context: Context, deadline: float,
                     preflight_failures: Iterable[OwnerFailure] = (),
                     key: str | None = None,
                     clock=time.monotonic) -> QuorumCollection:
    """Collect phase-qualified evidence once per prescribed stable VM identity."""
    owner_rows = tuple(owners)
    owner_ids = tuple(node.vm_id for node in owner_rows)
    if (not isinstance(manifest, Manifest) or len(owner_ids) != 5
            or len(set(owner_ids)) != 5
            or any(node != manifest.node(node.vm_id) for node in owner_rows)):
        raise ValueError("collector requires five distinct nodes from the manifest")
    if type(required) is not int or not 1 <= required <= 5:
        raise ValueError("required threshold must be in 1..5")
    if phase not in {"strong_query", "strong_write", "strong_readback",
                     "eventual_read", "eventual_put"}:
        raise ValueError("unsupported protocol phase")
    if type(deadline) not in (int, float) or not math.isfinite(deadline):
        raise ValueError("deadline must be a finite monotonic timestamp")
    rows = tuple(calls)
    local_failures = tuple(preflight_failures)
    local_failure_by_owner = {}
    for failure in local_failures:
        if (not isinstance(failure, OwnerFailure) or failure.owner_id not in owner_ids
                or failure.owner_id in local_failure_by_owner):
            raise ValueError("preflight failures must identify distinct prescribed owners")
        local_failure_by_owner[failure.owner_id] = failure
    if rows:
        first_key = rows[0].request.key
        if key is not None and key != first_key:
            raise ValueError("explicit key does not match dispatched owner requests")
    elif isinstance(key, str):
        # A complete set of local preflight failures is terminal evidence too;
        # no synthetic network call should be fabricated just to collect it.
        first_key = key
    else:
        raise ValueError("collector requires calls or an explicit key with failures")
    prescribed = tuple(node.vm_id for node in manifest.owners(first_key))
    if set(owner_ids) != set(prescribed):
        raise ValueError("collector owners must be the five prescribed ring owners")
    primary: dict[str, OwnerCall] = {}
    valid_owner_call: dict[str, bool] = {}
    failures: list[OwnerFailure] = list(local_failures)
    for call in rows:
        owner_id = call.node.vm_id
        if owner_id not in owner_ids:
            failures.append(OwnerFailure(owner_id, OwnerOutcome.INVALID_RESPONSE,
                                         "call target is not one of the five owners"))
            call.future.cancel()
            continue
        if owner_id in primary:
            failures.append(OwnerFailure(owner_id, OwnerOutcome.DUPLICATE_OWNER,
                                         "duplicate owner attempt cannot add quorum weight"))
            call.future.cancel()
            continue
        primary[owner_id] = call
        request = call.request
        if (request.target_id != owner_id or request.operation_id != operation_id
                or request.phase != phase or request.schema != PROTOCOL_SCHEMA_VERSION
                or request.manifest_id != manifest.fingerprint
                or request.key != first_key
                or (request.epoch, request.mode) != (context.epoch, context.mode)
                or call.expected_generation <= 0):
            failures.append(OwnerFailure(owner_id, OwnerOutcome.INVALID_RESPONSE,
                                         "request is not bound to this operation/phase/context"))
            call.future.cancel()
            valid_owner_call[owner_id] = False
        else:
            valid_owner_call[owner_id] = True
    if set(primary).intersection(local_failure_by_owner):
        raise ValueError("an owner cannot have both a dispatched call and a preflight failure")
    if set(primary).union(local_failure_by_owner) != set(owner_ids):
        raise ValueError("collector requires one call or preflight failure per prescribed owner")

    pending = {call.future: call for owner_id, call in primary.items()
               if valid_owner_call[owner_id]}
    qualifying: list[QualifiedReply] = []
    seen_versions: dict[Version, bytes] = {}
    fatal_error = None
    deadline_expired = False
    threshold_impossible = len(qualifying) + len(pending) < required

    def impossible_with_remaining() -> bool:
        return len(qualifying) + len(pending) < required

    while pending and len(qualifying) < required and not threshold_impossible:
        remaining = deadline - clock()
        if remaining <= 0:
            deadline_expired = True
            break
        completed, _ = wait(tuple(pending), timeout=min(remaining, 0.05),
                            return_when=FIRST_COMPLETED)
        if not completed:
            continue
        def completion_order(future):
            try:
                completed_at = future.result().completed_at
            except BaseException:
                completed_at = float("inf")
            return completed_at, pending[future].node.vm_id

        completed_ordered = sorted(completed, key=completion_order)
        # wait(FIRST_COMPLETED) may return several futures at once. Process them
        # in completion order and stop exactly at the threshold. Leave remaining
        # futures pending so Eventual PUT's reserved fanout is never discarded.
        for index, future in enumerate(completed_ordered):
            call = pending.pop(future)
            owner_id = call.node.vm_id
            try:
                completion = future.result()
                if not isinstance(completion, RpcCompletion):
                    raise TypeError("future must return RpcCompletion")
            except BaseException as error:
                failures.append(OwnerFailure(owner_id, OwnerOutcome.ERROR, str(error)))
                threshold_impossible = impossible_with_remaining()
                if threshold_impossible:
                    break
                continue
            outcome = completion.outcome
            if completion.completed_at > deadline:
                failures.append(OwnerFailure(owner_id, OwnerOutcome.LATE,
                                             "response completed after foreground deadline",
                                             outcome.completion_unknown))
                threshold_impossible = impossible_with_remaining()
                if threshold_impossible:
                    break
                continue
            if outcome.kind != RpcOutcomeKind.RESPONSE or outcome.response is None:
                failures.append(_failure_from_outcome(owner_id, outcome))
                threshold_impossible = impossible_with_remaining()
                if threshold_impossible:
                    break
                continue
            response = outcome.response
            try:
                response.validate_for(call.request, call.node, manifest,
                                      call.expected_generation)
            except (RpcProtocolError, ValueError, TypeError) as error:
                failures.append(OwnerFailure(owner_id, OwnerOutcome.INVALID_RESPONSE,
                                             str(error), outcome.completion_unknown))
                threshold_impossible = impossible_with_remaining()
                if threshold_impossible:
                    break
                continue
            if response.operation_id != operation_id or response.phase != phase:
                failures.append(OwnerFailure(owner_id, OwnerOutcome.INVALID_RESPONSE,
                                             "operation ID or phase mismatch"))
                threshold_impossible = impossible_with_remaining()
                if threshold_impossible:
                    break
                continue
            if response.status == RpcStatus.VERSION_CONFLICT:
                failures.append(OwnerFailure(owner_id, OwnerOutcome.VERSION_CONFLICT,
                                             "equal-tag bytes conflict"))
                fatal_error = "VERSION_CONFLICT"
                break
            if response.status == RpcStatus.STALE_EPOCH:
                failures.append(OwnerFailure(owner_id, OwnerOutcome.STALE_EPOCH,
                                             response.error))
                threshold_impossible = impossible_with_remaining()
                if threshold_impossible:
                    break
                continue
            if response.status == RpcStatus.SUPERSEDED and phase == "eventual_put":
                failures.append(OwnerFailure(owner_id, OwnerOutcome.SUPERSEDED,
                                             "higher version does not acknowledge this Eventual PUT"))
                threshold_impossible = impossible_with_remaining()
                if threshold_impossible:
                    break
                continue
            if not _qualifies(phase, call.request, response):
                outcome_map = {
                    RpcStatus.ERROR: OwnerOutcome.ERROR,
                    RpcStatus.SUPERSEDED: OwnerOutcome.SUPERSEDED,
                }
                if (response.status == RpcStatus.ERROR
                        and response.error == "NOT_PRESCRIBED_OWNER"):
                    outcome_map[RpcStatus.ERROR] = OwnerOutcome.NOT_PRESCRIBED_OWNER
                failures.append(OwnerFailure(
                    owner_id, outcome_map.get(response.status, OwnerOutcome.ERROR),
                    response.error or f"{response.status.value} is not qualifying evidence",
                    outcome.completion_unknown))
                threshold_impossible = impossible_with_remaining()
                if threshold_impossible:
                    break
                continue
            if response.status == RpcStatus.FOUND:
                assert response.version is not None
                previous = seen_versions.get(response.version)
                if previous is not None and previous != response.value:
                    fatal_error = "VERSION_CONFLICT"
                    failures.append(OwnerFailure(owner_id, OwnerOutcome.VERSION_CONFLICT,
                                                 "same version has conflicting bytes"))
                    break
                seen_versions[response.version] = response.value
            qualifying.append(QualifiedReply(
                owner_id, call.request, response, completion.completed_at))
            if len(qualifying) >= required:
                break
            if impossible_with_remaining():
                threshold_impossible = True
                break
        if fatal_error:
            break
        if threshold_impossible:
            break

    if threshold_impossible and phase != "eventual_put":
        # A bounded phase may stop as soon as terminal outcomes prove the
        # threshold impossible; no replacement owner is submitted.
        for future, call in tuple(pending.items()):
            if future.cancel():
                failures.append(OwnerFailure(call.node.vm_id, OwnerOutcome.CANCELLED,
                                             "threshold is no longer reachable"))
                pending.pop(future, None)
    if len(qualifying) < required and clock() >= deadline:
        deadline_expired = True
    continuing = []
    for future, call in tuple(pending.items()):
        # After W, Eventual PUT must preserve every reserved one-shot owner
        # attempt through its lease/shutdown boundary. For read/Strong phases,
        # cancel work that has not started; an in-flight RPC cannot be forcefully
        # canceled by Future.cancel(), so expose it to the caller as continuing.
        if phase != "eventual_put" and future.cancel():
            failures.append(OwnerFailure(call.node.vm_id, OwnerOutcome.CANCELLED,
                                         "collection completed before this attempt started"))
        else:
            continuing.append(call)
    return QuorumCollection(tuple(qualifying), tuple(failures),
                            len(qualifying) >= required, deadline_expired, fatal_error,
                            tuple(continuing))
