"""Executable lifecycle wrapper for one MP3 VM (no distributed coordinator)."""

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
from typing import TextIO

from .eventlog import configure_logging
from .membership import Member, MembershipRuntime
from .models import ConsistencyMode, Context, Version
from .replica import DEFAULT_REPLICA_QUEUE, DEFAULT_REPLICA_WORKERS, ReplicaService, ShutdownReport
from .ring import Manifest, Node, load_manifest
from .rpc import ReplicaClient, ReplicaRequest, RpcOperation, RpcOutcome, new_request
from .storage import Store, open_store


DEFAULT_NODE_SHUTDOWN_TIMEOUT = 12.0


@dataclass(frozen=True)
class NodeShutdownReport:
    """Evidence about completion of coordinated local shutdown."""

    replica: ShutdownReport | None
    outstanding_rpc_calls: int
    membership_stopped: bool
    store_closed: bool

    @property
    def clean(self) -> bool:
        return (
            self.outstanding_rpc_calls == 0
            and self.membership_stopped
            and self.store_closed
            and self.incomplete_requests == 0
            and (self.replica is None or self.replica.workers_joined)
        )

    @property
    def incomplete_requests(self) -> int:
        return 0 if self.replica is None else self.replica.incomplete_requests


class NodeRuntime:
    """Own one VM's Store, membership/gossip, KV replica service, and RPC client.

    This is only a local node lifecycle and single-replica RPC wrapper. It does not
    coordinate multi-owner reads or writes and does not implement a consistency mode.
    """

    def __init__(self, *, manifest: Manifest, vm_id: str, database_path: str | Path,
                 bootstrap: bool, bootstrap_mode: ConsistencyMode | None = None,
                 log_path: str | Path | None = None, event_stream: TextIO | None = None,
                 store_timeout: float = 5.0, store_capacity: int = 8,
                 replica_workers: int = DEFAULT_REPLICA_WORKERS,
                 replica_queue: int = DEFAULT_REPLICA_QUEUE,
                 gossip_background: bool = True):
        if not isinstance(manifest, Manifest):
            raise TypeError("manifest must be a validated Manifest")
        manifest.node(vm_id)  # exact identity and configured endpoints must exist
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
        self.gossip_background = gossip_background

        self.store: Store | None = None
        self.membership_runtime: MembershipRuntime | None = None
        self.replica_service: ReplicaService | None = None
        self.rpc_client: ReplicaClient | None = None
        self.logger: logging.Logger | None = None
        self._ready = False
        self._stopping = False
        self._started = False
        self._shutdown_report: NodeShutdownReport | None = None
        self._calls = threading.Condition()
        self._accepting_calls = False
        self._active_calls = 0

    @classmethod
    def from_files(cls, *, vm_id: str, manifest_path: str | Path,
                   database_path: str | Path, bootstrap: bool,
                   bootstrap_mode: ConsistencyMode | None = None,
                   **kwargs) -> "NodeRuntime":
        """Load the explicit node identity and validated ten-node manifest."""
        manifest = load_manifest(manifest_path)
        return cls(manifest=manifest, vm_id=vm_id, database_path=database_path,
                   bootstrap=bootstrap, bootstrap_mode=bootstrap_mode, **kwargs)

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

    def owners(self, key: str) -> tuple[Node, ...]:
        """Return the fixed five prescribed owners, regardless of liveness."""
        return self.manifest.owners(key)

    def list_mem_ids(self):
        """Expose diagnostic membership rows, including suspected members."""
        if not self._ready or self.membership_runtime is None:
            raise RuntimeError("node is not ready")
        return self.membership_runtime.table.list_mem_ids()

    def start(self) -> "NodeRuntime":
        """Open explicit bootstrap/recovery state, then start services in order."""
        if self._started:
            raise RuntimeError("node runtime can be started only once")
        self._started = True
        try:
            bootstrap_context = (Context(1, self.bootstrap_mode)
                                 if self.bootstrap else None)
            self.store = open_store(
                self.database_path, bootstrap=bootstrap_context, vm_id=self.vm_id,
                timeout=self.store_timeout, capacity=self.store_capacity)
            context = self.store.current_context()
            if self.log_path is not None:
                self.logger = logging.getLogger(f"mp3.node.{self.vm_id}.{id(self)}")
                configure_logging(self.logger, self.log_path, stream=sys.stderr)

            # MembershipRuntime is the only path that reserves a process generation.
            # It commits generation before opening the membership listener or gossip.
            self.membership_runtime = MembershipRuntime(self.store, self.manifest)
            self.membership_runtime.start(background=self.gossip_background, listen=True)

            # The KV listener is admitted only after membership has installed the
            # durable generation. Its store context came from explicit bootstrap or
            # validated recovery above; it is never copied from a peer.
            self.replica_service = ReplicaService(
                self.store, self.manifest, self.membership_runtime.table,
                workers=self.replica_workers, max_pending=self.replica_queue,
                logger=self.logger, event_stream=self.event_stream)
            self.replica_service.start()

            # Endpoint resolution is part of constructing the existing single-call
            # client. Only after all local services exist do we report local readiness.
            self.rpc_client = ReplicaClient(self.manifest,
                                            self.membership_runtime.table)
            self._ready = True
            with self._calls:
                self._accepting_calls = True
            return self
        except BaseException:
            self._ready = False
            self._stopping = True
            self._cleanup_failed_start()
            raise

    def _cleanup_failed_start(self) -> None:
        """Release initialized resources while preserving the original exception."""
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
        safe_to_close_store = (replica_report is None or replica_report.workers_joined)
        store_closed = False
        if self.store is not None and safe_to_close_store and membership_stopped:
            try:
                self.store.close()
                store_closed = True
            except BaseException:
                pass
        self._close_logger()
        self._shutdown_report = NodeShutdownReport(
            replica_report, 0, membership_stopped, store_closed)

    def call_replica(self, target_id: str, op: RpcOperation, key: str, *,
                     deadline: float, version: Version | None = None,
                     value: bytes = b"") -> RpcOutcome:
        """Send exactly one local replica RPC using this process's current identity."""
        with self._calls:
            if not self._ready or not self._accepting_calls or self.rpc_client is None:
                raise RuntimeError("node is not accepting RPC calls")
            self._active_calls += 1
        try:
            context = self.context
            request = new_request(
                manifest=self.manifest, sender_id=self.vm_id,
                sender_generation=self.generation, target_id=target_id,
                epoch=context.epoch, mode=context.mode, op=op, key=key,
                version=version, value=value)
            return self.rpc_client.call_replica(
                self.manifest.node(target_id), request, deadline=deadline)
        finally:
            with self._calls:
                self._active_calls -= 1
                self._calls.notify_all()

    def stop(self, *, timeout: float = DEFAULT_NODE_SHUTDOWN_TIMEOUT) -> NodeShutdownReport:
        """Stop accepting calls, drain bounded work, stop services, then close Store."""
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
        self._ready = False
        self._stopping = True
        with self._calls:
            self._accepting_calls = False

        replica_report = None
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
            and membership_stopped and outstanding_calls == 0)
        store_closed = False
        if self.store is not None and safe_to_close_store:
            self.store.close()
            store_closed = True
        self._close_logger()
        self._shutdown_report = NodeShutdownReport(
            replica_report, outstanding_calls, membership_stopped, store_closed)
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
    parser.add_argument("--log", help="optional append-only JSON-lines local log path")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Start one node until SIGINT/SIGTERM, then report bounded shutdown status."""
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
            f"membership={node_config.membership.host}:{node_config.membership.port}",
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
