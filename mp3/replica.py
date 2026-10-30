"""Bounded TCP service for one local read or conditional replica installation."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import math
import queue
import socket
import threading
import time

from .eventlog import log_replica_event
from .membership import MembershipTable
from .models import Context, InstallResult, LocalStatus, ReadResult, Record
from .ring import Manifest
from .rpc import (
    ReplicaRequest, ReplicaResponse, RpcOperation, RpcProtocolError,
    RpcStatus, receive_request, send_response,
)


DEFAULT_REPLICA_WORKERS = 32
DEFAULT_REPLICA_QUEUE = 64
DEFAULT_REPLICA_REQUEST_TIMEOUT = 5.0
DEFAULT_OVERLOAD_REPLY_TIMEOUT = 0.25


@dataclass(frozen=True)
class ShutdownReport:
    """Summary of the bounded service shutdown and any incomplete requests."""

    incomplete_requests: int
    workers_joined: bool


class ReplicaService:
    """Serve validated READ_LOCAL and INSTALL_IF_NEWER RPCs over TCP.

    The fixed worker count and bounded queue limit active/queued request handlers.
    No consistency coordinator, quorum, repair, or client-facing CLI logic lives here.
    """

    def __init__(self, store, manifest: Manifest, membership: MembershipTable, *,
                 workers: int = DEFAULT_REPLICA_WORKERS,
                 max_pending: int = DEFAULT_REPLICA_QUEUE,
                 request_timeout: float = DEFAULT_REPLICA_REQUEST_TIMEOUT,
                 overload_reply_timeout: float = DEFAULT_OVERLOAD_REPLY_TIMEOUT,
                 logger: logging.Logger | None = None, event_stream=None):
        if not isinstance(manifest, Manifest) or not isinstance(membership, MembershipTable):
            raise TypeError("manifest and MembershipTable are required")
        if manifest.fingerprint != membership.manifest.fingerprint:
            raise ValueError("membership and replica manifest differ")
        if getattr(store, "vm_id", None) != membership.self_id:
            raise ValueError("store identity and membership self identity differ")
        manifest.node(membership.self_id)
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be a positive integer")
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be a positive bounded integer")
        for value, label in ((request_timeout, "request_timeout"),
                             (overload_reply_timeout, "overload_reply_timeout")):
            try:
                valid = type(value) in (int, float) and math.isfinite(value) and value > 0
            except OverflowError:
                valid = False
            if not valid:
                raise ValueError(f"{label} must be positive and finite")

        self.store = store
        self.manifest = manifest
        self.membership = membership
        self.endpoint = manifest.node(membership.self_id).kv
        self.workers = workers
        self.max_pending = max_pending
        self.request_timeout = float(request_timeout)
        self.overload_reply_timeout = float(overload_reply_timeout)
        self.logger = logger
        self.event_stream = event_stream

        self._queue: queue.Queue = queue.Queue(maxsize=max_pending)
        self._stop = threading.Event()
        self._listener: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._workers: list[threading.Thread] = []
        self._condition = threading.Condition()
        self._connections: set[socket.socket] = set()
        self._outstanding = 0
        self._started = False
        self._stopped = False
        self._last_shutdown = ShutdownReport(0, True)

    @property
    def address(self) -> tuple[str, int]:
        """Return the bound address; useful when a test binds port zero."""
        if self._listener is None:
            raise RuntimeError("replica service has not started")
        address = self._listener.getsockname()
        return address[0], address[1]

    def start(self) -> None:
        """Bind the manifest KV endpoint only after restart generation is ready."""
        if self._started or self._stopped:
            raise RuntimeError("replica service may start once")
        self.membership.local_generation
        listener = socket.socket(socket.AF_INET6 if ":" in self.endpoint.host else socket.AF_INET,
                                 socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((self.endpoint.host, self.endpoint.port))
            listener.listen(self.workers + self.max_pending)
            listener.settimeout(0.2)
        except OSError:
            listener.close()
            raise
        self._listener = listener
        try:
            self._workers = [threading.Thread(target=self._worker,
                                              name=f"mp3-kv-handler-{self.membership.self_id}-{i}",
                                              daemon=True)
                             for i in range(self.workers)]
            for worker in self._workers:
                worker.start()
            self._accept_thread = threading.Thread(target=self._accept_loop,
                                                   name=f"mp3-kv-accept-{self.membership.self_id}",
                                                   daemon=True)
            self._accept_thread.start()
            self._started = True
        except Exception:
            self._stop.set()
            listener.close()
            for worker in self._workers:
                worker.join(timeout=0.5)
            raise

    def _accept_loop(self) -> None:
        listener = self._listener
        assert listener is not None
        while not self._stop.is_set():
            try:
                connection, _address = listener.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stop.is_set():
                    return
                continue
            accepted_at = time.monotonic()
            request_deadline = accepted_at + self.request_timeout
            with self._condition:
                if self._stop.is_set():
                    connection.close()
                    continue
                self._connections.add(connection)
                self._outstanding += 1
            try:
                self._queue.put_nowait((connection, request_deadline))
            except queue.Full:
                # A bounded fast rejection path preserves an explicit OVERLOADED
                # result for well-formed callers without creating another worker pool.
                try:
                    self._reply_overloaded(connection,
                                           min(request_deadline,
                                               time.monotonic() + self.overload_reply_timeout))
                finally:
                    self._finish(connection)

    def _worker(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.1)
            except queue.Empty:
                # Shutdown is signaled out of band so every idle worker can wake
                # even when the bounded queue is smaller than the worker pool.
                if self._stop.is_set():
                    return
                continue
            if item is None:
                self._queue.task_done()
                return
            connection, deadline = item
            try:
                self._handle(connection, deadline)
            except Exception:
                # A bug in one request must not silently shrink the fixed pool.
                # The accepted connection and queue accounting are still released.
                if self.logger is not None:
                    try:
                        self.logger.exception(
                            "replica request handler failed",
                            extra={"node_id": self.membership.self_id,
                                   "request_id": None})
                    except Exception:
                        pass
            finally:
                try:
                    self._finish(connection)
                finally:
                    self._queue.task_done()

    def _finish(self, connection: socket.socket) -> None:
        try:
            connection.close()
        except OSError:
            # Closing a reset peer can report a socket error; it must not leak
            # admission accounting or terminate a fixed worker.
            pass
        finally:
            with self._condition:
                self._connections.discard(connection)
                self._outstanding -= 1
                self._condition.notify_all()

    def _error_response(self, request: ReplicaRequest, code: str, *,
                        context: Context | None = None) -> ReplicaResponse:
        if context is None:
            epoch, mode = request.epoch, request.mode
        else:
            epoch, mode = context.epoch, context.mode
        return ReplicaResponse(
            request_id=request.request_id, manifest_id=self.manifest.fingerprint,
            sender_id=self.membership.self_id,
            sender_generation=self.membership.local_generation,
            target_id=request.sender_id, epoch=epoch, mode=mode,
            op=request.op, phase=request.phase, status=RpcStatus.ERROR,
            present=False, durable=False, error=code,
        )

    def _reply_overloaded(self, connection: socket.socket, deadline: float) -> None:
        try:
            request = receive_request(connection, self.manifest, deadline=deadline)
            response = self._error_response(request, "OVERLOADED")
            send_response(connection, response, self.manifest, deadline=deadline)
        except (OSError, TimeoutError, ValueError, TypeError, RuntimeError):
            # A malformed/slow overload request has no trustworthy reply identity.
            return

    def _validate_identity(self, request: ReplicaRequest) -> str | None:
        if request.manifest_id != self.manifest.fingerprint:
            return "INVALID_REQUEST"
        if request.target_id != self.membership.self_id:
            return "INVALID_REQUEST"
        try:
            self.manifest.node(request.sender_id)
        except ValueError:
            return "INVALID_REQUEST"
        sender = next((member for member in self.membership.snapshot()
                       if member.vm_id == request.sender_id), None)
        if (sender is None or sender.restart_generation == 0
                or request.sender_generation != sender.restart_generation):
            return "INVALID_REQUEST"
        # SUSPECTED is not an identity change: a matching generation remains valid.
        return None

    def _emit(self, request: ReplicaRequest, phase: str) -> None:
        operation = "get" if request.op == RpcOperation.READ_LOCAL else "put"
        log_replica_event(self.logger, phase, operation, request.key,
                          node_id=self.membership.self_id,
                          request_id=request.request_id, stream=self.event_stream)

    def _process(self, request: ReplicaRequest, deadline: float) -> ReplicaResponse:
        error = self._validate_identity(request)
        if error is not None:
            return self._error_response(request, error)
        try:
            context = Context(request.epoch, request.mode)
        except (ValueError, TypeError):
            try:
                actual = self.store.current_context(deadline=deadline)
            except Exception:
                return self._error_response(request, "STORAGE_ERROR")
            return ReplicaResponse(
                request_id=request.request_id, manifest_id=self.manifest.fingerprint,
                sender_id=self.membership.self_id,
                sender_generation=self.membership.local_generation,
                target_id=request.sender_id, epoch=actual.epoch, mode=actual.mode,
                op=request.op, phase=request.phase, status=RpcStatus.STALE_EPOCH,
                present=False, durable=False, error="STALE_EPOCH",
            )

        if request.op == RpcOperation.READ_LOCAL:
            result: ReadResult = self.store.read_local(request.key, context=context,
                                                       deadline=deadline)
            if result.status == LocalStatus.FOUND:
                assert result.record is not None
                return ReplicaResponse(
                    request_id=request.request_id, manifest_id=self.manifest.fingerprint,
                    sender_id=self.membership.self_id,
                    sender_generation=self.membership.local_generation,
                    target_id=request.sender_id, epoch=result.context.epoch, mode=result.context.mode,
                    op=request.op, phase=request.phase, status=RpcStatus.FOUND,
                    present=True, version=result.record.version, value=result.record.value,
                )
            if result.status == LocalStatus.NOT_FOUND:
                return ReplicaResponse(
                    request_id=request.request_id, manifest_id=self.manifest.fingerprint,
                    sender_id=self.membership.self_id,
                    sender_generation=self.membership.local_generation,
                    target_id=request.sender_id, epoch=result.context.epoch, mode=result.context.mode,
                    op=request.op, phase=request.phase, status=RpcStatus.NOT_FOUND,
                    present=False,
                )
            if result.status == LocalStatus.STALE_EPOCH:
                return ReplicaResponse(
                    request_id=request.request_id, manifest_id=self.manifest.fingerprint,
                    sender_id=self.membership.self_id,
                    sender_generation=self.membership.local_generation,
                    target_id=request.sender_id, epoch=result.context.epoch, mode=result.context.mode,
                    op=request.op, phase=request.phase, status=RpcStatus.STALE_EPOCH,
                    present=False, durable=False, error="STALE_EPOCH",
                )
            return self._error_response(request, "STORAGE_ERROR")

        record = Record(request.version, request.value)
        result: InstallResult = self.store.install_if_newer(request.key, record,
                                                            context=context,
                                                            deadline=deadline)
        if result.status == LocalStatus.STALE_EPOCH:
            return ReplicaResponse(
                request_id=request.request_id, manifest_id=self.manifest.fingerprint,
                sender_id=self.membership.self_id,
                sender_generation=self.membership.local_generation,
                target_id=request.sender_id, epoch=result.context.epoch, mode=result.context.mode,
                op=request.op, phase=request.phase, status=RpcStatus.STALE_EPOCH,
                present=False, durable=False, error="STALE_EPOCH",
            )
        status_map = {
            LocalStatus.INSTALLED: RpcStatus.INSTALLED,
            LocalStatus.UNCHANGED: RpcStatus.UNCHANGED,
            LocalStatus.SUPERSEDED: RpcStatus.SUPERSEDED,
            LocalStatus.VERSION_CONFLICT: RpcStatus.VERSION_CONFLICT,
        }
        if result.status not in status_map:
            return self._error_response(request, "STORAGE_ERROR")
        return ReplicaResponse(
            request_id=request.request_id, manifest_id=self.manifest.fingerprint,
            sender_id=self.membership.self_id,
            sender_generation=self.membership.local_generation,
            target_id=request.sender_id, epoch=result.context.epoch, mode=result.context.mode,
            op=request.op, phase=request.phase, status=status_map[result.status],
            present=False, stored_version=result.stored_version, durable=result.durable,
        )

    def _handle(self, connection: socket.socket, deadline: float) -> None:
        try:
            request = receive_request(connection, self.manifest, deadline=deadline)
            error = self._validate_identity(request)
            if error is not None:
                response = self._error_response(request, error)
                send_response(connection, response, self.manifest, deadline=deadline)
                return
            if time.monotonic() >= deadline:
                return
            self._emit(request, "received")
            response = self._process(request, deadline)
            if time.monotonic() < deadline:
                send_response(connection, response, self.manifest, deadline=deadline)
                self._emit(request, "responded")
        except (OSError, TimeoutError, RpcProtocolError, ValueError, TypeError, RuntimeError):
            # Invalid framing/metadata is closed; no operation is dispatched until
            # complete request validation and exact-body checks have succeeded.
            return

    def stop(self, *, timeout: float | None = None) -> ShutdownReport:
        """Stop admission and drain accepted work; bound forced shutdown explicitly."""
        if self._stopped:
            return self._last_shutdown
        budget = (self.request_timeout + 2 * float(getattr(self.store, "timeout", 5.0)) + 1.0
                  if timeout is None else timeout)
        if type(budget) not in (int, float) or not math.isfinite(budget) or budget < 0:
            raise ValueError("shutdown timeout must be finite and nonnegative")
        self._stopped = True
        self._stop.set()
        listener = self._listener
        self._listener = None
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        shutdown_deadline = time.monotonic() + budget
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=max(0.0, shutdown_deadline - time.monotonic()))
            self._accept_thread = None

        with self._condition:
            while self._outstanding and time.monotonic() < shutdown_deadline:
                self._condition.wait(shutdown_deadline - time.monotonic())
            incomplete = self._outstanding

        if incomplete:
            # Drop only accepted-but-unstarted work after an explicit forced stop;
            # every dropped request is counted in the report.
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is not None:
                    connection, _deadline = item
                    self._finish(connection)
                self._queue.task_done()

            # Queued sockets were closed by _finish above. Shutdown the remaining
            # active sockets to interrupt bounded network IO; each worker remains
            # their sole owner for final close/accounting in its finally block.
            with self._condition:
                active_connections = tuple(self._connections)
            for connection in active_connections:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

        for worker in self._workers:
            worker.join(timeout=max(0.0, shutdown_deadline - time.monotonic()))
        joined = all(not worker.is_alive() for worker in self._workers)
        if joined:
            self._workers.clear()
        self._last_shutdown = ShutdownReport(incomplete, joined)
        if incomplete and self.logger is not None:
            self.logger.warning("replica shutdown left requests incomplete",
                                extra={"node_id": self.membership.self_id,
                                       "request_id": None})
        return self._last_shutdown
