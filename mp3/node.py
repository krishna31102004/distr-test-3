from __future__ import annotations

import argparse
from dataclasses import dataclass
import logging
import math
from pathlib import Path
import signal
import sys
import threading
import time
import uuid
from typing import TextIO

from .eventlog import configure_logging
from .attempts import AttemptLease
from .config import DEFAULT_R, DEFAULT_W, validate_quorums
from .control import ControlService
from .coordinator import (
    DEFAULT_ATTEMPT_QUEUE, DEFAULT_ATTEMPT_WORKERS,
    DEFAULT_BACKGROUND_QUEUE, DEFAULT_BACKGROUND_WORKERS,
    DEFAULT_COORDINATOR_QUEUE, DEFAULT_COORDINATOR_WORKERS,
    CoordinatorResources, CoordinatorShutdownReport, OwnerCall, OwnerDispatchError,
    OwnerFailure, OwnerOutcome, Schema2Activation, Schema3Activation,
    dispatch_owner as _dispatch_owner,
)
from .membership import Member, MembershipRuntime
from .models import ConsistencyMode, Context, Version
from .replica import DEFAULT_REPLICA_QUEUE, DEFAULT_REPLICA_WORKERS, ReplicaService, ShutdownReport
from .ring import Manifest, Node, load_manifest
from .rpc import (PROTOCOL_SCHEMA_VERSION, RR_SCHEMA_VERSION, ReplicaClient,
                  RpcOperation, RpcOutcome, new_operation_id, new_request)
from .storage import Store, open_store
from .multiput import MultiputManager
from .transition import TransitionManager


DEFAULT_NODE_SHUTDOWN_TIMEOUT = 12.0


@dataclass(frozen=True)
class NodeShutdownReport:
    replica: ShutdownReport | None
    outstanding_rpc_calls: int
    membership_stopped: bool
    store_closed: bool
    coordinator: CoordinatorShutdownReport | None = None
    control: ShutdownReport | None = None

    @property
    def clean(self) -> bool:
        return (
            self.outstanding_rpc_calls == 0
            and self.membership_stopped
            and self.store_closed
            and self.incomplete_requests == 0
            and (self.replica is None or self.replica.workers_joined)
            and (self.coordinator is None or self.coordinator.clean)
            and (self.control is None or (
                self.control.workers_joined and self.control.incomplete_requests == 0))
        )

    @property
    def incomplete_requests(self) -> int:
        return 0 if self.replica is None else self.replica.incomplete_requests


@dataclass(frozen=True, eq=False)
class CoordinatorAdmission:
    deadline: float
    context: Context
    sender_generation: int
    operation_id: str


class NodeRuntime:
    def __init__(self, *, manifest: Manifest, vm_id: str, database_path: str | Path,
                 bootstrap: bool, bootstrap_mode: ConsistencyMode | None = None,
                 log_path: str | Path | None = None, event_stream: TextIO | None = None,
                 store_timeout: float = 5.0, store_capacity: int = 8,
                 replica_workers: int = DEFAULT_REPLICA_WORKERS,
                 replica_queue: int = DEFAULT_REPLICA_QUEUE,
                 coordinator_workers: int = DEFAULT_COORDINATOR_WORKERS,
                 coordinator_queue: int = DEFAULT_COORDINATOR_QUEUE,
                 attempt_workers: int = DEFAULT_ATTEMPT_WORKERS,
                 attempt_queue: int = DEFAULT_ATTEMPT_QUEUE,
                 background_workers: int = DEFAULT_BACKGROUND_WORKERS,
                 background_queue: int = DEFAULT_BACKGROUND_QUEUE,
                 read_quorum: int = DEFAULT_R, write_quorum: int = DEFAULT_W,
                 multiput_file: str | Path | None = None,
                 protocol_activation: Schema2Activation | None = None,
                 rr_activation: Schema3Activation | None = None,
                 gossip_background: bool = True):
        if not isinstance(manifest, Manifest):
            raise TypeError("manifest must be a validated Manifest")
        manifest.node(vm_id)
        if type(bootstrap) is not bool:
            raise ValueError("choose bootstrap=True or bootstrap=False recovery explicitly")
        if bootstrap and bootstrap_mode is not None and not isinstance(
                bootstrap_mode, ConsistencyMode):
            raise ValueError("bootstrap_mode must be a ConsistencyMode")
        if not bootstrap and bootstrap_mode is not None:
            raise ValueError("recovery uses the persisted mode; do not supply bootstrap_mode")
        self.manifest = manifest
        self.vm_id = vm_id
        self.database_path = Path(database_path)
        self.bootstrap = bootstrap
        self.bootstrap_mode = bootstrap_mode or ConsistencyMode.STRONG if bootstrap else None
        self.log_path = None if log_path is None else Path(log_path)
        self.event_stream = event_stream
        self.store_timeout = store_timeout
        self.store_capacity = store_capacity
        self.replica_workers = replica_workers
        self.replica_queue = replica_queue
        self.coordinator_workers = coordinator_workers
        self.coordinator_queue = coordinator_queue
        self.attempt_workers = attempt_workers
        self.attempt_queue = attempt_queue
        if protocol_activation is not None and not protocol_activation.enabled_for(manifest):
            raise ValueError("schema-2 activation does not match this manifest")
        self.protocol_activation = protocol_activation
        if rr_activation is not None and not rr_activation.enabled_for(manifest):
            raise ValueError("schema-3 activation does not match this manifest")
        self.rr_activation = rr_activation
        self.background_workers = background_workers
        self.background_queue = background_queue
        validate_quorums(read_quorum, write_quorum)
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum
        self.multiput_file = None if multiput_file is None else Path(multiput_file)
        self.gossip_background = gossip_background

        self.store: Store | None = None
        self.membership_runtime: MembershipRuntime | None = None
        self.replica_service: ReplicaService | None = None
        self.rpc_client: ReplicaClient | None = None
        self.coordinator_resources: CoordinatorResources | None = None
        self.control_service: ControlService | None = None
        self.transition: TransitionManager | None = None
        self.multiput: MultiputManager | None = None
        self.logger: logging.Logger | None = None
        self._ready = False
        self._stopping = False
        self._started = False
        self._shutdown_report: NodeShutdownReport | None = None
        self._calls = threading.Condition()
        self._accepting_calls = False
        self._active_calls = 0
        self._admitted_coordinators: dict[int, CoordinatorAdmission] = {}

    @classmethod
    def from_files(cls, *, vm_id: str, manifest_path: str | Path,
                   database_path: str | Path, bootstrap: bool,
                   bootstrap_mode: ConsistencyMode | None = None,
                   schema2_inventory_path: str | Path | None = None,
                   **kwargs) -> "NodeRuntime":
        manifest = load_manifest(manifest_path)
        activation = (None if schema2_inventory_path is None else
                      Schema2Activation.from_file(manifest, schema2_inventory_path))
        rr_activation = None
        if schema2_inventory_path is not None:
            try:
                rr_activation = Schema3Activation.from_file(manifest, schema2_inventory_path)
            except ValueError:
                rr_activation = None
        return cls(manifest=manifest, vm_id=vm_id, database_path=database_path,
                   bootstrap=bootstrap, bootstrap_mode=bootstrap_mode,
                   protocol_activation=activation, rr_activation=rr_activation, **kwargs)

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def context(self) -> Context:
        if not self._ready or self.store is None:
            raise RuntimeError("node is not ready")
        return self.store.current_context()

    @property
    def generation(self) -> int:
        if not self._ready or self.membership_runtime is None:
            raise RuntimeError("node is not ready")
        return self.membership_runtime.generation

    @property
    def membership(self) -> tuple[Member, ...]:
        if not self._ready or self.membership_runtime is None:
            raise RuntimeError("node is not ready")
        return self.membership_runtime.table.snapshot()

    def list_store(self, after: str | None = None):
        if not self._ready or self.store is None:
            raise RuntimeError("node is not ready")
        return self.store.list_keys(after=after)

    def transition_phase(self) -> str:
        if not self._ready or self.store is None:
            return "ACTIVE"
        return self.store.local_phase()

    def owners(self, key: str) -> tuple[Node, ...]:
        return self.manifest.owners(key)

    def list_mem_ids(self):
        if not self._ready or self.membership_runtime is None:
            raise RuntimeError("node is not ready")
        return self.membership_runtime.table.list_mem_ids()

    def strong_put(self, key: str, value: bytes, *, deadline: float):
        from .strong import StrongCoordinator
        return StrongCoordinator(self).strong_put(key, value, deadline=deadline)

    def strong_get(self, key: str, *, deadline: float):
        from .strong import StrongCoordinator
        return StrongCoordinator(self).strong_get(key, deadline=deadline)

    def eventual_put(self, key: str, value: bytes, *, read_quorum: int | None = None,
                     write_quorum: int | None = None):
        from .eventual import EventualNoRRCoordinator
        return EventualNoRRCoordinator(
            self, read_quorum=self.read_quorum if read_quorum is None else read_quorum,
            write_quorum=self.write_quorum if write_quorum is None else write_quorum,
        ).eventual_put(key, value)

    def eventual_get(self, key: str, *, deadline: float, read_quorum: int | None = None,
                     write_quorum: int | None = None):
        from .eventual import EventualNoRRCoordinator
        return EventualNoRRCoordinator(
            self, read_quorum=self.read_quorum if read_quorum is None else read_quorum,
            write_quorum=self.write_quorum if write_quorum is None else write_quorum,
        ).eventual_get(key, deadline=deadline)

    def eventual_rr_put(self, key: str, value: bytes, *, read_quorum: int | None = None,
                        write_quorum: int | None = None):
        from .eventual import EventualRRCoordinator
        return EventualRRCoordinator(
            self, read_quorum=self.read_quorum if read_quorum is None else read_quorum,
            write_quorum=self.write_quorum if write_quorum is None else write_quorum,
        ).eventual_put(key, value)

    def eventual_rr_get(self, key: str, *, deadline: float, read_quorum: int | None = None,
                        write_quorum: int | None = None):
        from .eventual import EventualRRCoordinator
        return EventualRRCoordinator(
            self, read_quorum=self.read_quorum if read_quorum is None else read_quorum,
            write_quorum=self.write_quorum if write_quorum is None else write_quorum,
        ).eventual_get(key, deadline=deadline)

    def start(self) -> "NodeRuntime":
        if self._started:
            raise RuntimeError("node runtime can be started only once")
        self._started = True
        try:
            bootstrap_context = (Context(1, self.bootstrap_mode)
                                 if self.bootstrap else None)
            self.store = open_store(
                self.database_path, bootstrap=bootstrap_context, vm_id=self.vm_id,
                timeout=self.store_timeout, capacity=self.store_capacity)
            self.store.current_context()
            if self.log_path is not None:
                self.logger = logging.getLogger(f"mp3.node.{self.vm_id}.{id(self)}")
                configure_logging(self.logger, self.log_path, stream=sys.stderr)

            self.membership_runtime = MembershipRuntime(self.store, self.manifest)
            self.membership_runtime.start(background=self.gossip_background, listen=True)

            self.replica_service = ReplicaService(
                self.store, self.manifest, self.membership_runtime.table,
                workers=self.replica_workers, max_pending=self.replica_queue,
                logger=self.logger, event_stream=self.event_stream,
                protocol_activation=self.protocol_activation,
                rr_activation=self.rr_activation)
            self.replica_service.start()

            self.rpc_client = ReplicaClient(self.manifest,
                                            self.membership_runtime.table,
                                            protocol_activation=self.protocol_activation,
                                            rr_activation=self.rr_activation)
            self.coordinator_resources = CoordinatorResources(
                coordinator_workers=self.coordinator_workers,
                coordinator_queue=self.coordinator_queue,
                attempt_workers=self.attempt_workers,
                attempt_queue=self.attempt_queue,
                background_workers=self.background_workers,
                background_queue=self.background_queue)
            with self._calls:
                self._ready = True
                self._accepting_calls = True
            self.multiput = MultiputManager(self)
            self.control_service = ControlService(self)
            self.control_service.start()
            self.transition = TransitionManager(self)
            self.transition.start_recovery()
            return self
        except BaseException:
            self._ready = False
            self._stopping = True
            self._cleanup_failed_start()
            raise

    def _cleanup_failed_start(self) -> None:
        control_report = None
        if self.multiput is not None:
            self.multiput.stop()
        if self.transition is not None:
            self.transition.stop(timeout=1.0)
        if self.control_service is not None:
            try:
                control_report = self.control_service.stop(timeout=2.0)
            except BaseException:
                pass
        coordinator_report = None
        if self.coordinator_resources is not None:
            try:
                coordinator_report = self.coordinator_resources.shutdown(
                    timeout=2.0, force=True)
            except BaseException:
                pass
        replica_report = None
        if self.replica_service is not None:
            try:
                replica_report = self.replica_service.stop()
            except BaseException:
                pass
        membership_stopped = True
        if self.membership_runtime is not None:
            try:
                membership_stopped = self.membership_runtime.stop()
            except BaseException:
                membership_stopped = False
        safe_to_close_store = (
            (replica_report is None or replica_report.workers_joined)
            and (control_report is None or control_report.workers_joined)
            and (coordinator_report is None or coordinator_report.clean))
        store_closed = False
        if self.store is not None and safe_to_close_store and membership_stopped:
            try:
                self.store.close()
                store_closed = True
            except BaseException:
                pass
        self._close_logger()
        self._shutdown_report = NodeShutdownReport(
            replica_report, 0, membership_stopped, store_closed, coordinator_report,
            control_report)

    def call_replica(self, target_id: str, op: RpcOperation, key: str, *,
                     deadline: float, version: Version | None = None,
                     value: bytes = b"", phase: str = "local",
                     schema: int = 1, operation_id: str | None = None,
                     expected_target_generation: int | None = None,
                     attempt_lease: AttemptLease | None = None) -> RpcOutcome:
        with self._calls:
            if not self._ready or not self._accepting_calls or self.rpc_client is None:
                raise RuntimeError("node is not accepting RPC calls")
            context = self.context
            generation = self.generation
            client = self.rpc_client
            self._active_calls += 1
        try:
            request = new_request(
                manifest=self.manifest, sender_id=self.vm_id,
                sender_generation=generation, target_id=target_id,
                epoch=context.epoch, mode=context.mode, op=op, key=key,
                version=version, value=value, phase=phase, schema=schema,
                operation_id=operation_id)
            call_options = {}
            if expected_target_generation is not None:
                call_options["expected_target_generation"] = expected_target_generation
            if attempt_lease is not None:
                call_options["attempt_lease"] = attempt_lease
            return client.call_replica(
                self.manifest.node(target_id), request, deadline=deadline,
                **call_options)
        finally:
            with self._calls:
                self._active_calls -= 1
                self._calls.notify_all()

    def reserve_attempts(self, count: int, *, deadline: float,
                         admission: CoordinatorAdmission | None = None,
                         attempt_lease: AttemptLease | None = None,
                         queue_deadline: float | None = None):
        return self._reserve_attempts(count, deadline=deadline, admission=admission,
                                      attempt_lease=attempt_lease,
                                      queue_deadline=queue_deadline)

    def _reserve_attempts(self, count: int, *, deadline: float,
                          admission: CoordinatorAdmission | None,
                          attempt_lease: AttemptLease | None = None,
                          queue_deadline: float | None = None):
        with self._calls:
            external = self._ready and self._accepting_calls
            authorized = self._admission_is_live_locked(admission, deadline)
            if attempt_lease is not None:
                authorized = (isinstance(admission, CoordinatorAdmission)
                              and self._admitted_coordinators.get(id(admission)) is admission
                              and deadline == admission.deadline
                              and queue_deadline == attempt_lease.hard_deadline
                              and not attempt_lease.cancelled
                              and time.monotonic() < attempt_lease.current_deadline())
            if (not external and not authorized) or self.coordinator_resources is None:
                raise RuntimeError("node is not accepting coordinator work")
            if admission is not None and not authorized:
                raise RuntimeError("coordinator admission is invalid, expired, or completed")
            resources = self.coordinator_resources
        return resources.reserve_attempts(count, deadline=deadline,
                                          queue_deadline=queue_deadline)

    def submit_coordinator(self, function, *args, deadline: float,
                           operation_id: str | None = None):
        if not callable(function):
            raise TypeError("coordinator function must be callable")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise ValueError("coordinator deadline must be a finite monotonic timestamp")
        if operation_id is not None:
            try:
                parsed = uuid.UUID(operation_id)
            except (ValueError, TypeError, AttributeError) as error:
                raise ValueError("operation_id must be a canonical lowercase UUID4") from error
            if str(parsed) != operation_id or parsed.version != 4:
                raise ValueError("operation_id must be a canonical lowercase UUID4")
        with self._calls:
            if (not self._ready or not self._accepting_calls
                    or self.coordinator_resources is None or self.store is None
                    or self.membership_runtime is None):
                raise RuntimeError("node is not accepting coordinator work")
            resources = self.coordinator_resources
            admission = CoordinatorAdmission(
                deadline, self.store.current_context(),
                self.membership_runtime.generation,
                operation_id or new_operation_id())
            self._admitted_coordinators[id(admission)] = admission
            self._active_calls += 1
        try:
            future = resources.coordinators.submit(
                function, admission, *args, deadline=deadline)
            if future is None:
                self._finish_coordinator_admission(admission)
                return None
            future.add_done_callback(lambda _future: self._finish_coordinator_admission(admission))
            return future
        except BaseException:
            self._finish_coordinator_admission(admission)
            raise

    def _admission_is_live_locked(self, admission, deadline: float) -> bool:
        if not isinstance(admission, CoordinatorAdmission):
            return False
        if self._admitted_coordinators.get(id(admission)) is not admission:
            return False
        if (type(deadline) not in (int, float) or not math.isfinite(deadline)
                or deadline > admission.deadline or time.monotonic() >= deadline):
            return False
        return True

    def _finish_coordinator_admission(self, admission: CoordinatorAdmission) -> None:
        with self._calls:
            if self._admitted_coordinators.pop(id(admission), None) is admission:
                self._active_calls -= 1
                self._calls.notify_all()

    def dispatch_owner(self, target_id: str, op: RpcOperation, key: str, *,
                       deadline: float, reservation=None, phase: str,
                       operation_id: str, version: Version | None = None,
                       value: bytes = b"", attempt_lease: AttemptLease | None = None,
                       admission: CoordinatorAdmission | None = None,
                       ) -> OwnerCall:
        with self._calls:
            external = self._ready and self._accepting_calls
            authorization_deadline = deadline
            authorized = self._admission_is_live_locked(admission, authorization_deadline)
            if attempt_lease is not None:
                authorized = (isinstance(admission, CoordinatorAdmission)
                              and self._admitted_coordinators.get(id(admission)) is admission
                              and operation_id == admission.operation_id
                              and deadline == attempt_lease.hard_deadline
                              and not attempt_lease.cancelled
                              and time.monotonic() < attempt_lease.current_deadline())
            if ((not external and not authorized) or self.rpc_client is None
                    or self.membership_runtime is None or self.coordinator_resources is None):
                raise RuntimeError("node is not accepting RPC calls")
            if admission is not None and not authorized:
                raise RuntimeError("coordinator admission is invalid, expired, or completed")
            if self.protocol_activation is None and self.rr_activation is None:
                raise RuntimeError("schema-2 protocol is not activated")
            if admission is not None and operation_id != admission.operation_id:
                raise RuntimeError("RPC operation ID does not match its coordinator admission")
            try:
                target_node = self.manifest.node(target_id)
            except ValueError:
                raise OwnerDispatchError(OwnerFailure(
                    target_id, OwnerOutcome.NOT_PRESCRIBED_OWNER,
                    f"{target_id!r} is not a manifest owner for key {key!r}")) from None
            if target_id not in {owner.vm_id for owner in self.manifest.owners(key)}:
                raise OwnerDispatchError(OwnerFailure(
                    target_id, OwnerOutcome.NOT_PRESCRIBED_OWNER,
                    f"{target_id} is not a prescribed owner for key {key!r}"))
            if admission is None:
                context = self.store.current_context()
                sender_generation = self.membership_runtime.generation
            else:
                context = admission.context
                sender_generation = admission.sender_generation
            schema = (RR_SCHEMA_VERSION if context.mode == ConsistencyMode.EVENTUAL_RR
                      else PROTOCOL_SCHEMA_VERSION)
            activation = (self.rr_activation if schema == RR_SCHEMA_VERSION
                          else self.protocol_activation)
            if activation is None:
                raise RuntimeError(f"schema-{schema} protocol is not activated")
            client = self.rpc_client
            membership = self.membership_runtime.table.snapshot()
            target_row = next(row for row in membership if row.vm_id == target_id)
            expected_generation = target_row.restart_generation
            if expected_generation <= 0:
                raise RuntimeError("target generation is unknown")
            self._active_calls += 1
        try:
            request = new_request(
                manifest=self.manifest, sender_id=self.vm_id,
                sender_generation=sender_generation, target_id=target_id,
                epoch=context.epoch, mode=context.mode, op=op, key=key,
                version=version, value=value, phase=phase, schema=schema,
                operation_id=operation_id)
            owner_call = _dispatch_owner(
                client, target_node, request, manifest=self.manifest, deadline=deadline,
                expected_generation=expected_generation,
                executor=self.coordinator_resources.attempts,
                reservation=reservation, attempt_lease=attempt_lease)
            owner_call.future.add_done_callback(self._finish_admitted_call)
            return owner_call
        except BaseException:
            self._finish_admitted_call(None)
            raise

    def _finish_admitted_call(self, _future) -> None:
        with self._calls:
            self._active_calls -= 1
            self._calls.notify_all()

    def stop(self, *, timeout: float = DEFAULT_NODE_SHUTDOWN_TIMEOUT) -> NodeShutdownReport:
        if self._shutdown_report is not None and self._shutdown_report.clean:
            return self._shutdown_report
        try:
            valid_timeout = (type(timeout) in (int, float)
                             and math.isfinite(timeout) and timeout >= 0)
        except OverflowError:
            valid_timeout = False
        if not valid_timeout:
            raise ValueError("shutdown timeout must be nonnegative and finite")
        deadline = time.monotonic() + timeout
        with self._calls:
            self._ready = False
            self._stopping = True
            self._accepting_calls = False

        replica_report = None
        coordinator_report = None
        control_report = None
        if self.multiput is not None:
            self.multiput.stop()
        if self.transition is not None:
            self.transition.stop(timeout=min(2.0, max(0.0, deadline - time.monotonic())))
        if self.control_service is not None:
            control_report = self.control_service.stop(
                timeout=max(0.0, (deadline - time.monotonic()) / 2))
        if self.coordinator_resources is not None:
            coordinator_report = self.coordinator_resources.shutdown(
                timeout=max(0.0, deadline - time.monotonic()))
        if self.replica_service is not None:
            replica_report = self.replica_service.stop(
                timeout=max(0.0, deadline - time.monotonic()))

        membership_stopped = True
        if self.membership_runtime is not None:
            membership_stopped = bool(self.membership_runtime.stop())

        with self._calls:
            while self._active_calls and time.monotonic() < deadline:
                self._calls.wait(max(0.0, deadline - time.monotonic()))
            outstanding_calls = self._active_calls

        safe_to_close_store = (
            (replica_report is None or replica_report.workers_joined)
            and (coordinator_report is None or coordinator_report.clean)
            and (control_report is None or control_report.workers_joined)
            and membership_stopped and outstanding_calls == 0)
        store_closed = False
        if self.store is not None and safe_to_close_store:
            self.store.close()
            store_closed = True
        self._close_logger()
        self._shutdown_report = NodeShutdownReport(
            replica_report, outstanding_calls, membership_stopped, store_closed,
            coordinator_report, control_report)
        return self._shutdown_report

    def _close_logger(self) -> None:
        if self.logger is None:
            return
        for handler in tuple(self.logger.handlers):
            self.logger.removeHandler(handler)
            handler.close()
        self.logger = None

    def __enter__(self) -> "NodeRuntime":
        return self.start()

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        report = self.stop()
        if not report.clean and exc_type is None:
            raise RuntimeError(f"node shutdown incomplete: {report}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python3 -m mp3.node",
        description="Run one MP3 node's membership and local replica RPC services.")
    parser.add_argument("--vm-id", required=True, help="stable identity vm01 through vm10")
    parser.add_argument("--manifest", required=True, help="path to the validated ten-node JSON manifest")
    parser.add_argument("--db", required=True, help="local SQLite database path")
    lifecycle = parser.add_mutually_exclusive_group(required=True)
    lifecycle.add_argument("--bootstrap", action="store_true",
                           help="create a new database (must not already exist)")
    lifecycle.add_argument("--recover", action="store_true",
                           help="open an existing database and preserve its persisted context/data")
    parser.add_argument("--bootstrap-mode", choices=[mode.value for mode in ConsistencyMode],
                        default=None,
                        help="initial mode for --bootstrap only; default: strong epoch 1")
    parser.add_argument("--read-quorum", type=int, default=DEFAULT_R,
                        help="Eventual R: replies a GET waits for (1..5, default 2)")
    parser.add_argument("--write-quorum", type=int, default=DEFAULT_W,
                        help="Eventual W: durable acks a PUT waits for (1..5, default 2)")
    parser.add_argument("--multiput-file",
                        help="file whose bytes this VM puts in a multiput (default: 1 KiB of "
                             "random bytes)")
    parser.add_argument("--schema2-inventory",
                        help="explicit complete ten-node compatible-release inventory")
    parser.add_argument("--log", help="optional append-only JSON-lines log path; use "
                        "machine.<vm number>.log in the MP1 server's directory to query it")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    bootstrap = args.bootstrap
    if not bootstrap and args.bootstrap_mode is not None:
        _parser().error("--bootstrap-mode is valid only with --bootstrap")
    mode = (ConsistencyMode(args.bootstrap_mode or ConsistencyMode.STRONG.value)
            if bootstrap else None)
    try:
        node = NodeRuntime.from_files(
            vm_id=args.vm_id, manifest_path=args.manifest, database_path=args.db,
            bootstrap=bootstrap, bootstrap_mode=mode, log_path=args.log,
            read_quorum=args.read_quorum, write_quorum=args.write_quorum,
            multiput_file=args.multiput_file,
            schema2_inventory_path=args.schema2_inventory,
            event_stream=sys.stdout).start()
    except Exception as error:
        print(f"START_FAILED {type(error).__name__}: {error}", file=sys.stderr, flush=True)
        return 2

    stop_requested = threading.Event()
    previous_handlers = {}

    def request_stop(_signum, _frame):
        stop_requested.set()

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, request_stop)
        node_config = node.manifest.node(node.vm_id)
        print(
            f"READY {node.vm_id} generation={node.generation} "
            f"context={node.context.mode.value}@{node.context.epoch} "
            f"kv={node_config.kv.host}:{node_config.kv.port} "
            f"membership={node_config.membership.host}:{node_config.membership.port} "
            f"control={node_config.control.host}:{node_config.control.port}",
            flush=True)
        while not stop_requested.wait(0.25):
            pass
    except KeyboardInterrupt:
        stop_requested.set()
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)
    report = node.stop()
    print(
        f"STOPPED clean={str(report.clean).lower()} "
        f"incomplete_requests={report.incomplete_requests} "
        f"outstanding_rpc_calls={report.outstanding_rpc_calls} "
        f"workers_joined={str(report.replica is None or report.replica.workers_joined).lower()} "
        f"membership_stopped={str(report.membership_stopped).lower()} "
        f"store_closed={str(report.store_closed).lower()}",
        flush=True)
    return 0 if report.clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
