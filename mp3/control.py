from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, replace
from enum import Enum
import hashlib
import json
import math
import queue
import re
import socket
import threading
import time

from .config import MAX_VALUE_BYTES
from .coordinator import OperationStatus
from .framing import HEADER, send_all_deadline
from .models import ConsistencyMode, Version
from .replica import ShutdownReport
from .ring import Manifest, key_position, ring_id_hex
from .rpc import (
    MAX_ERROR_BYTES, RpcOperation, RpcOutcomeKind, RpcProtocolError, RpcStatus,
    _encode_payload, _recv_payload, _require_eof, _validate_request_id, _validate_vm_id,
    _version_from_wire, _version_to_wire, new_operation_id,
)
from .storage import TransitioningError


CONTROL_SCHEMA = 1
COMMANDS = frozenset({"put", "get", "status", "mode", "tx_prepare", "tx_commit",
                      "tx_activate", "tx_abort", "tx_status", "liststore", "list_mem_ids",
                      "read_replica", "multiput", "mp_arm", "mp_go", "mp_abort", "mp_status"})
PAYLOAD_COMMANDS = frozenset({"read_replica", "multiput", "mp_arm", "mp_go", "mp_abort",
                              "mp_status"})
MULTIPUT_COMMANDS = frozenset({"mp_arm", "mp_go", "mp_abort", "mp_status"})
ADMIN_COMMANDS = frozenset({"liststore", "list_mem_ids", "read_replica", "multiput"}
                           | MULTIPUT_COMMANDS)
TRANSITION_COMMANDS = frozenset({"tx_prepare", "tx_commit", "tx_activate", "tx_abort",
                                 "tx_status"})
MAX_PAYLOAD_BYTES = 2048
MAX_DETAIL_BYTES = 8192
MAX_CURSOR_BYTES = 65536
MULTIPUT_MIN_BUDGET = 6.0
MAX_KEY_BYTES = 4096
DEFAULT_CONTROL_WORKERS = 8
DEFAULT_CONTROL_QUEUE = 16
DEFAULT_CONTROL_REQUEST_TIMEOUT = 50.0
DEFAULT_COMMAND_TIMEOUT = 10.0
DEFAULT_CLIENT_TIMEOUT = 25.0
MODE_CLIENT_TIMEOUT = 55.0
DEFAULT_OVERLOAD_REPLY_TIMEOUT = 0.25
MAX_REMEMBERED_COMMANDS = 256
MAX_CONCURRENT_LONG_REQUESTS = 3
MAX_CONCURRENT_REPLICA_READS = 3
REPLY_MARGIN_SECONDS = 1.0
EVENTUAL_PUT_BUDGET_SECONDS = 5.5
NOT_STARTED_ERROR = "request time ran out before the command started"
COMPLETION_UNKNOWN_SUFFIX = " (completion unknown)"
MAX_RESPONSE_ERROR_CHARS = MAX_ERROR_BYTES // 4
_KEY_ID = re.compile(r"[0-9a-f]{64}\Z")


class ControlError(RuntimeError):
    pass


class ControlStatus(str, Enum):
    OK = "OK"
    NOT_FOUND = "NOT_FOUND"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    OVERLOADED = "OVERLOADED"
    NO_CHANGE = "NO_CHANGE"
    BUSY = "BUSY"
    PENDING = "PENDING"


_STATUS_MAP = {
    OperationStatus.OK: ControlStatus.OK,
    OperationStatus.NOT_FOUND: ControlStatus.NOT_FOUND,
    OperationStatus.TIMEOUT: ControlStatus.TIMEOUT,
    OperationStatus.ERROR: ControlStatus.ERROR,
    OperationStatus.OVERLOADED: ControlStatus.OVERLOADED,
}


def _validate_key(key: object) -> str:
    if not isinstance(key, str):
        raise RpcProtocolError("key must be a string")
    try:
        encoded = key.encode("utf-8", "strict")
    except UnicodeEncodeError as error:
        raise RpcProtocolError("key must be strictly UTF-8 encodable") from error
    if len(encoded) > MAX_KEY_BYTES:
        raise RpcProtocolError(f"key exceeds {MAX_KEY_BYTES} bytes")
    return key


def _exact_fields(metadata: object, required: set[str], what: str) -> dict:
    if not isinstance(metadata, dict) or set(metadata) != required:
        raise RpcProtocolError(f"{what} has missing or unexpected fields")
    return metadata


@dataclass(frozen=True)
class ControlRequest:
    command_id: str
    manifest_id: str
    target_id: str
    command: str
    key: str = ""
    value: bytes = b""

    def __post_init__(self) -> None:
        _validate_request_id(self.command_id)
        if not isinstance(self.manifest_id, str) or re.fullmatch(
                r"[0-9a-f]{64}", self.manifest_id) is None:
            raise RpcProtocolError("manifest_id must be lowercase SHA-256 hex")
        _validate_vm_id(self.target_id, "target_id")
        if self.command not in COMMANDS:
            raise RpcProtocolError("unsupported control command")
        _validate_key(self.key)
        if not isinstance(self.value, bytes) or len(self.value) > MAX_VALUE_BYTES:
            raise RpcProtocolError("value must be bytes no larger than 1 MiB")
        if self.command == "status":
            if self.key or self.value:
                raise RpcProtocolError("status takes no key or value")
        elif self.command == "mode":
            try:
                ConsistencyMode(self.key)
            except ValueError as error:
                raise RpcProtocolError("mode needs strong, eventual-noRR or eventual-RR") from error
        elif self.command == "list_mem_ids":
            if self.key or self.value:
                raise RpcProtocolError("list_mem_ids takes no key or value")
        elif self.command == "liststore":
            if self.key:
                raise RpcProtocolError("liststore sends its cursor in the body, not the key")
            if len(self.value) > MAX_CURSOR_BYTES:
                raise RpcProtocolError("liststore cursor is too large")
        elif self.command in PAYLOAD_COMMANDS:
            if not self.key:
                raise RpcProtocolError(f"{self.command} requires a nonempty key")
            if not self.value or len(self.value) > MAX_PAYLOAD_BYTES:
                raise RpcProtocolError(f"{self.command} requires a JSON payload of at most "
                                       f"{MAX_PAYLOAD_BYTES} bytes")
        elif self.command in TRANSITION_COMMANDS:
            _validate_request_id(self.key)
            if (self.command == "tx_prepare") != bool(self.value):
                raise RpcProtocolError("only tx_prepare carries a payload, and it requires one")
            if len(self.value) > MAX_PAYLOAD_BYTES:
                raise RpcProtocolError("transition payload is too large")
        elif not self.key:
            raise RpcProtocolError("put and get require a nonempty key")
        if self.command not in (PAYLOAD_COMMANDS | {"put", "tx_prepare", "liststore"}) and self.value:
            raise RpcProtocolError("this command carries no value")

    def to_metadata(self) -> dict:
        return {"schema": CONTROL_SCHEMA, "command_id": self.command_id,
                "manifest_id": self.manifest_id, "target_id": self.target_id,
                "command": self.command, "key": self.key, "value_len": len(self.value)}

    @classmethod
    def from_metadata(cls, metadata: dict, value: bytes, manifest: Manifest) -> "ControlRequest":
        _exact_fields(metadata, {"schema", "command_id", "manifest_id", "target_id",
                                 "command", "key", "value_len"}, "control request")
        if type(metadata["schema"]) is not int or metadata["schema"] != CONTROL_SCHEMA:
            raise RpcProtocolError("unsupported control schema")
        if metadata["manifest_id"] != manifest.fingerprint:
            raise RpcProtocolError("control manifest fingerprint mismatch")
        if metadata["value_len"] != len(value):
            raise RpcProtocolError("control value_len is inconsistent with body")
        try:
            return cls(command_id=metadata["command_id"], manifest_id=metadata["manifest_id"],
                       target_id=metadata["target_id"], command=metadata["command"],
                       key=metadata["key"], value=value)
        except (ValueError, TypeError) as error:
            if isinstance(error, RpcProtocolError):
                raise
            raise RpcProtocolError(f"invalid control request: {error}") from error


@dataclass(frozen=True)
class ControlResponse:
    command_id: str
    manifest_id: str
    node_id: str
    command: str
    status: ControlStatus
    mode: ConsistencyMode
    epoch: int
    version: Version | None = None
    owners: tuple[str, ...] = ()
    key_id: str | None = None
    error: str | None = None
    value: bytes = b""
    detail: str = ""

    def __post_init__(self) -> None:
        _validate_request_id(self.command_id)
        _validate_vm_id(self.node_id, "node_id")
        if (not isinstance(self.detail, str) or "\x00" in self.detail
                or len(self.detail.encode("utf-8", "replace")) > MAX_DETAIL_BYTES):
            raise RpcProtocolError("detail must be NUL-free text of at most 4096 bytes")
        if self.command not in COMMANDS or not isinstance(self.status, ControlStatus):
            raise RpcProtocolError("invalid control response command or status")
        if not isinstance(self.mode, ConsistencyMode) or type(self.epoch) is not int or self.epoch < 1:
            raise RpcProtocolError("invalid control response context")
        if self.key_id is not None and (
                not isinstance(self.key_id, str) or _KEY_ID.fullmatch(self.key_id) is None):
            raise RpcProtocolError("key_id must be 64 lowercase hex digits")
        if len(self.owners) > 5 or len(set(self.owners)) != len(self.owners):
            raise RpcProtocolError("owners must be at most five distinct VMs")
        for owner in self.owners:
            _validate_vm_id(owner, "owner")
        if not isinstance(self.value, bytes) or len(self.value) > MAX_VALUE_BYTES:
            raise RpcProtocolError("response body must be bytes no larger than 1 MiB")
        if self.value and not (self.command in ("get", "read_replica", "liststore")
                               and self.status == ControlStatus.OK):
            raise RpcProtocolError("only a successful get, read_replica or liststore "
                                   "carries a body")
        if self.version is not None and (
                self.status != ControlStatus.OK or self.command == "status"):
            raise RpcProtocolError("only successful put/get carry a version")
        if self.error is not None:
            if not isinstance(self.error, str) or "\x00" in self.error:
                raise RpcProtocolError("error must be text without NUL")
            if len(self.error.encode("utf-8", "replace")) > MAX_ERROR_BYTES:
                raise RpcProtocolError("error text exceeds the safe field size")

    def to_metadata(self) -> dict:
        return {"schema": CONTROL_SCHEMA, "command_id": self.command_id,
                "manifest_id": self.manifest_id, "node_id": self.node_id,
                "command": self.command, "status": self.status.value,
                "mode": self.mode.value, "epoch": self.epoch,
                "version": _version_to_wire(self.version), "owners": list(self.owners),
                "key_id": self.key_id, "error": self.error, "detail": self.detail,
                "value_len": len(self.value)}

    @classmethod
    def from_metadata(cls, metadata: dict, value: bytes, manifest: Manifest) -> "ControlResponse":
        _exact_fields(metadata, {"schema", "command_id", "manifest_id", "node_id", "command",
                                 "status", "mode", "epoch", "version", "owners", "key_id",
                                 "error", "detail", "value_len"}, "control response")
        if type(metadata["schema"]) is not int or metadata["schema"] != CONTROL_SCHEMA:
            raise RpcProtocolError("unsupported control schema")
        if metadata["manifest_id"] != manifest.fingerprint:
            raise RpcProtocolError("control manifest fingerprint mismatch")
        if metadata["value_len"] != len(value):
            raise RpcProtocolError("control value_len is inconsistent with body")
        owners = metadata["owners"]
        if not isinstance(owners, list):
            raise RpcProtocolError("owners must be a list")
        try:
            return cls(command_id=metadata["command_id"], manifest_id=metadata["manifest_id"],
                       node_id=metadata["node_id"], command=metadata["command"],
                       status=ControlStatus(metadata["status"]),
                       mode=ConsistencyMode(metadata["mode"]), epoch=metadata["epoch"],
                       version=_version_from_wire(metadata["version"], manifest),
                       owners=tuple(owners), key_id=metadata["key_id"],
                       error=metadata["error"], value=value, detail=metadata["detail"])
        except (ValueError, TypeError) as error:
            if isinstance(error, RpcProtocolError):
                raise
            raise RpcProtocolError(f"invalid control response: {error}") from error


def _send_message(sock: socket.socket, metadata: dict, value: bytes, *, deadline: float) -> None:
    payload = _encode_payload(metadata, value)
    send_all_deadline(sock, HEADER.pack(len(payload)), deadline=deadline)
    send_all_deadline(sock, payload, deadline=deadline)


class ControlService:
    def __init__(self, runtime, *, workers: int = DEFAULT_CONTROL_WORKERS,
                 max_pending: int = DEFAULT_CONTROL_QUEUE,
                 request_timeout: float = DEFAULT_CONTROL_REQUEST_TIMEOUT,
                 command_timeout: float = DEFAULT_COMMAND_TIMEOUT):
        if type(workers) is not int or workers < 1:
            raise ValueError("workers must be a positive integer")
        if type(max_pending) is not int or max_pending < 1:
            raise ValueError("max_pending must be a positive bounded integer")
        for value, label in ((request_timeout, "request_timeout"),
                             (command_timeout, "command_timeout")):
            try:
                valid = type(value) in (int, float) and math.isfinite(value) and value > 0
            except OverflowError:
                valid = False
            if not valid:
                raise ValueError(f"{label} must be positive and finite")
        if command_timeout >= request_timeout:
            raise ValueError("command_timeout must leave time to reply within request_timeout")
        self.runtime = runtime
        self.manifest: Manifest = runtime.manifest
        self.vm_id: str = runtime.vm_id
        self.endpoint = self.manifest.node(self.vm_id).control
        self.workers = workers
        self.max_pending = max_pending
        self.request_timeout = float(request_timeout)
        self.command_timeout = float(command_timeout)

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
        self._long_slots = threading.BoundedSemaphore(MAX_CONCURRENT_LONG_REQUESTS)
        self._replica_slots = threading.BoundedSemaphore(MAX_CONCURRENT_REPLICA_READS)
        self._memory = threading.Lock()
        self._inflight: dict[str, tuple] = {}
        self._finished: OrderedDict[str, tuple[tuple, ControlResponse]] = OrderedDict()

    @property
    def address(self) -> tuple[str, int]:
        if self._listener is None:
            raise RuntimeError("control service has not started")
        address = self._listener.getsockname()
        return address[0], address[1]

    def start(self) -> None:
        if self._started or self._stopped:
            raise RuntimeError("control service may start once")
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
                                              name=f"mp3-control-handler-{self.vm_id}-{i}",
                                              daemon=True)
                             for i in range(self.workers)]
            for worker in self._workers:
                worker.start()
            self._accept_thread = threading.Thread(target=self._accept_loop,
                                                   name=f"mp3-control-accept-{self.vm_id}",
                                                   daemon=True)
            self._accept_thread.start()
            self._started = True
        except Exception:
            self._stop.set()
            listener.close()
            for worker in self._workers:
                if worker.ident is not None:
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
            request_deadline = time.monotonic() + self.request_timeout
            with self._condition:
                if self._stop.is_set():
                    connection.close()
                    continue
                self._connections.add(connection)
                self._outstanding += 1
            try:
                self._queue.put_nowait((connection, request_deadline))
            except queue.Full:
                try:
                    self._reply_overloaded(connection, min(
                        request_deadline, time.monotonic() + DEFAULT_OVERLOAD_REPLY_TIMEOUT))
                finally:
                    self._finish(connection)

    def _worker(self) -> None:
        while True:
            try:
                item = self._queue.get(timeout=0.1)
            except queue.Empty:
                if self._stop.is_set():
                    return
                continue
            connection, deadline = item
            try:
                self._handle(connection, deadline)
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
            pass
        finally:
            with self._condition:
                self._connections.discard(connection)
                self._outstanding -= 1
                self._condition.notify_all()

    def _context(self):
        try:
            return self.runtime.context
        except RuntimeError:
            return None

    def _reply(self, request: ControlRequest, status: ControlStatus, *, error: str | None = None,
               **fields) -> ControlResponse:
        context = self._context()
        mode = context.mode if context is not None else ConsistencyMode.STRONG
        epoch = context.epoch if context is not None else 1
        return ControlResponse(
            command_id=request.command_id, manifest_id=self.manifest.fingerprint,
            node_id=self.vm_id, command=request.command, status=status, mode=mode,
            epoch=epoch, error=error, **fields)

    def _reply_overloaded(self, connection: socket.socket, deadline: float) -> None:
        try:
            metadata, value = _recv_payload(connection, deadline=deadline)
            request = ControlRequest.from_metadata(metadata, value, self.manifest)
            response = self._reply(request, ControlStatus.OVERLOADED, error="OVERLOADED")
            _send_message(connection, response.to_metadata(), b"", deadline=deadline)
        except (OSError, TimeoutError, ValueError, TypeError, RuntimeError):
            return

    def _handle(self, connection: socket.socket, deadline: float) -> None:
        try:
            metadata, value = _recv_payload(connection, deadline=deadline)
            request = ControlRequest.from_metadata(metadata, value, self.manifest)
            _require_eof(connection, deadline=deadline)
            response = self._execute(request, deadline)
            if time.monotonic() < deadline:
                _send_message(connection, response.to_metadata(), response.value,
                              deadline=deadline)
        except (OSError, TimeoutError, RpcProtocolError, ValueError, TypeError, RuntimeError):
            return

    @staticmethod
    def _fingerprint(request: ControlRequest) -> tuple:
        return (request.command, request.key, hashlib.sha256(request.value).digest())

    def _execute(self, request: ControlRequest, request_deadline: float) -> ControlResponse:
        if request.target_id != self.vm_id:
            return self._reply(request, ControlStatus.ERROR, error="WRONG_NODE")
        fingerprint = self._fingerprint(request)
        refusal = None
        with self._memory:
            running = self._inflight.get(request.command_id)
            remembered = self._finished.get(request.command_id)
            if running is not None:
                refusal = ("DUPLICATE_COMMAND_IN_PROGRESS" if running == fingerprint
                           else "COMMAND_ID_REUSED")
            elif remembered is not None:
                earlier, earlier_response = remembered
                if earlier != fingerprint:
                    refusal = "COMMAND_ID_REUSED"
                elif request.command == "put":
                    return earlier_response
                else:
                    refusal = "DUPLICATE_COMMAND"
            else:
                self._inflight[request.command_id] = fingerprint
        if refusal is not None:
            return self._reply(request, ControlStatus.ERROR, error=refusal)
        response = None
        try:
            response = self._run(request, request_deadline)
        except Exception as error:
            suffix = COMPLETION_UNKNOWN_SUFFIX if request.command == "put" else ""
            response = self._reply(request, ControlStatus.ERROR,
                                   error=f"INTERNAL_ERROR {type(error).__name__}{suffix}")
        finally:
            with self._memory:
                self._inflight.pop(request.command_id, None)
                if (response is not None and request.command in ("put", "get")
                        and response.error != NOT_STARTED_ERROR):
                    self._finished[request.command_id] = (
                        fingerprint, replace(response, value=b""))
                    while len(self._finished) > MAX_REMEMBERED_COMMANDS:
                        self._finished.popitem(last=False)
        return response

    def _run_transition(self, request: ControlRequest) -> ControlResponse:
        manager = getattr(self.runtime, "transition", None)
        if manager is None:
            return self._reply(request, ControlStatus.ERROR, error="mode changes are not available")
        if request.command == "mode":
            if not self._long_slots.acquire(blocking=False):
                return self._reply(request, ControlStatus.BUSY,
                                   error="too many long-running requests are waiting on this node")
            try:
                outcome = manager.request_mode(ConsistencyMode(request.key), request.command_id)
            finally:
                self._long_slots.release()
            error = outcome.error[:MAX_RESPONSE_ERROR_CHARS] if outcome.error else None
            return self._reply(request, outcome.status, error=error,
                               detail=json.dumps(outcome.detail, sort_keys=True))
        status, detail, error = manager.participant_message(
            request.command, request.key, request.value)
        return self._reply(request, status, error=error,
                           detail=json.dumps(detail, sort_keys=True))

    def _run_admin(self, request: ControlRequest, request_deadline: float) -> ControlResponse:
        command = request.command
        if command == "list_mem_ids":
            rows = [{"vm": vm, "ring_id": ring_id_hex(ring), "status": status.value,
                     "generation": generation, "incarnation": incarnation,
                     "heartbeat": heartbeat}
                    for vm, generation, incarnation, heartbeat, status, ring
                    in self.runtime.list_mem_ids()]
            node_ring = ring_id_hex(self.manifest.node(self.vm_id).ring_id)
            return self._reply(request, ControlStatus.OK, detail=json.dumps(
                {"node_ring_id": node_ring, "rows": rows}, sort_keys=True))
        if command == "liststore":
            after = None
            if request.value:
                try:
                    after = json.loads(request.value.decode("utf-8"))["after"]
                    if not isinstance(after, str):
                        raise TypeError("cursor must be text")
                except (UnicodeError, ValueError, KeyError, TypeError):
                    return self._reply(request, ControlStatus.ERROR, error="bad liststore cursor")
            try:
                keys, following = self.runtime.list_store(after=after)
            except (ValueError, TypeError):
                return self._reply(request, ControlStatus.ERROR, error="bad liststore cursor")
            except TransitioningError:
                return self._reply(request, ControlStatus.ERROR,
                                   error="TRANSITIONING: the cluster is changing consistency mode")
            body = json.dumps({"keys": keys, "next": following}).encode("utf-8")
            node_ring = ring_id_hex(self.manifest.node(self.vm_id).ring_id)
            return self._reply(request, ControlStatus.OK, value=body,
                               detail=json.dumps({"node_ring_id": node_ring}))
        if command == "read_replica":
            if not self._replica_slots.acquire(blocking=False):
                return self._reply(request, ControlStatus.BUSY,
                                   error="too many replica reads are waiting on this node")
            try:
                return self._read_replica(request, request_deadline)
            finally:
                self._replica_slots.release()
        manager = getattr(self.runtime, "multiput", None)
        if manager is None:
            return self._reply(request, ControlStatus.ERROR, error="multiput is not available")
        if command in MULTIPUT_COMMANDS:
            status, detail, error = manager.participant_message(
                command, request.key, request.value)
            return self._reply(request, status, error=error, detail=json.dumps(detail))
        try:
            vms = json.loads(request.value.decode("utf-8")).get("vms")
            if not isinstance(vms, list) or not all(isinstance(vm, str) for vm in vms):
                raise ValueError("vms must be a list of VM names")
        except (UnicodeError, ValueError, AttributeError) as error:
            return self._reply(request, ControlStatus.ERROR, error=f"bad multiput payload: {error}")
        available = request_deadline - REPLY_MARGIN_SECONDS - time.monotonic()
        if available < MULTIPUT_MIN_BUDGET:
            return self._reply(request, ControlStatus.TIMEOUT, error=NOT_STARTED_ERROR)
        if not self._long_slots.acquire(blocking=False):
            return self._reply(request, ControlStatus.BUSY,
                               error="too many long-running requests are waiting on this node")
        try:
            status, detail, error = manager.coordinate(request.key, vms, budget=available)
        finally:
            self._long_slots.release()
        return self._reply(request, status, error=error[:MAX_RESPONSE_ERROR_CHARS] if error else None,
                           key_id=ring_id_hex(key_position(request.key)),
                           detail=json.dumps(detail, sort_keys=True))

    def _read_replica(self, request: ControlRequest, request_deadline: float) -> ControlResponse:
        key_id = ring_id_hex(key_position(request.key))
        try:
            target = json.loads(request.value.decode("utf-8")).get("target")
            self.manifest.node(target)
        except (UnicodeError, ValueError, AttributeError):
            return self._reply(request, ControlStatus.ERROR, key_id=key_id,
                               error="the target must be a VM in the manifest")
        available = request_deadline - REPLY_MARGIN_SECONDS - time.monotonic()
        if available < REPLY_MARGIN_SECONDS:
            return self._reply(request, ControlStatus.TIMEOUT, key_id=key_id,
                               error=NOT_STARTED_ERROR)
        deadline = time.monotonic() + min(self.command_timeout, available)
        outcome = self.runtime.call_replica(target, RpcOperation.READ_LOCAL, request.key,
                                            deadline=deadline, phase="local", schema=1)
        if outcome.kind != RpcOutcomeKind.RESPONSE or outcome.response is None:
            status = (ControlStatus.TIMEOUT if outcome.kind == RpcOutcomeKind.TIMEOUT
                      else ControlStatus.ERROR)
            return self._reply(request, status, key_id=key_id, owners=(target,),
                               error=(outcome.error or outcome.kind.value)[:MAX_RESPONSE_ERROR_CHARS])
        response = outcome.response
        if response.status == RpcStatus.FOUND:
            return self._reply(request, ControlStatus.OK, key_id=key_id, owners=(target,),
                               version=response.version, value=response.value)
        if response.status == RpcStatus.NOT_FOUND:
            return self._reply(request, ControlStatus.NOT_FOUND, key_id=key_id,
                               owners=(target,))
        reason = response.error or response.status.value
        return self._reply(request, ControlStatus.ERROR, key_id=key_id, owners=(target,),
                           error=f"{target} refused the read: {reason}"[:MAX_RESPONSE_ERROR_CHARS])

    def _run(self, request: ControlRequest, request_deadline: float) -> ControlResponse:
        if request.command == "mode" or request.command in TRANSITION_COMMANDS:
            return self._run_transition(request)
        if request.command in ADMIN_COMMANDS:
            return self._run_admin(request, request_deadline)
        context = self.runtime.context
        if request.command == "status":
            return self._reply(
                request, ControlStatus.OK,
                detail=json.dumps({"read_quorum": self.runtime.read_quorum,
                                   "write_quorum": self.runtime.write_quorum,
                                   "generation": self.runtime.generation},
                                  sort_keys=True))
        key_id = ring_id_hex(key_position(request.key))
        phase = getattr(self.runtime, "transition_phase", None)
        if callable(phase) and phase() != "ACTIVE":
            return self._reply(request, ControlStatus.ERROR, key_id=key_id,
                               error="TRANSITIONING: the cluster is changing consistency "
                                     "mode; retry shortly")
        now = time.monotonic()
        available = request_deadline - REPLY_MARGIN_SECONDS - now
        deadline = now + min(self.command_timeout, available)
        mode = context.mode
        if mode in (ConsistencyMode.EVENTUAL_NO_RR, ConsistencyMode.EVENTUAL_RR) \
                and request.command == "put":
            enough = available >= EVENTUAL_PUT_BUDGET_SECONDS
        else:
            enough = deadline - now >= REPLY_MARGIN_SECONDS
        if not enough:
            return self._reply(request, ControlStatus.TIMEOUT, key_id=key_id,
                               error=NOT_STARTED_ERROR)
        if mode == ConsistencyMode.STRONG:
            result = (self.runtime.strong_put(request.key, request.value, deadline=deadline)
                      if request.command == "put"
                      else self.runtime.strong_get(request.key, deadline=deadline))
        elif mode == ConsistencyMode.EVENTUAL_NO_RR:
            result = (self.runtime.eventual_put(request.key, request.value)
                      if request.command == "put"
                      else self.runtime.eventual_get(request.key, deadline=deadline))
        else:
            result = (self.runtime.eventual_rr_put(request.key, request.value)
                      if request.command == "put"
                      else self.runtime.eventual_rr_get(request.key, deadline=deadline))
        error = result.error
        if result.completion_unknown:
            base = (error or "operation incomplete")[
                :MAX_RESPONSE_ERROR_CHARS - len(COMPLETION_UNKNOWN_SUFFIX)]
            error = base + COMPLETION_UNKNOWN_SUFFIX
        elif error is not None:
            error = error[:MAX_RESPONSE_ERROR_CHARS]
        return self._reply(
            request, _STATUS_MAP[result.status], error=error, key_id=key_id,
            version=result.version, owners=tuple(result.qualifying_owners),
            value=result.value if request.command == "get" and result.value is not None else b"")

    def stop(self, *, timeout: float | None = None) -> ShutdownReport:
        if (self._stopped and self._last_shutdown.workers_joined
                and self._last_shutdown.incomplete_requests == 0):
            return self._last_shutdown
        budget = self.request_timeout + 1.0 if timeout is None else timeout
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
        accept_joined = True
        if self._accept_thread is not None and self._accept_thread.ident is not None:
            self._accept_thread.join(timeout=max(0.0, shutdown_deadline - time.monotonic()))
            accept_joined = not self._accept_thread.is_alive()
        with self._condition:
            while self._outstanding and time.monotonic() < shutdown_deadline:
                self._condition.wait(shutdown_deadline - time.monotonic())
            incomplete = self._outstanding
        if incomplete:
            while True:
                try:
                    connection, _deadline = self._queue.get_nowait()
                except queue.Empty:
                    break
                self._finish(connection)
                self._queue.task_done()
            with self._condition:
                active = tuple(self._connections)
            for connection in active:
                try:
                    connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        for worker in self._workers:
            if worker.ident is not None:
                worker.join(timeout=max(0.0, shutdown_deadline - time.monotonic()))
        joined = accept_joined and all(not worker.is_alive() for worker in self._workers)
        if joined:
            self._workers.clear()
        self._last_shutdown = ShutdownReport(incomplete, joined)
        return self._last_shutdown


def send_command(manifest: Manifest, vm_id: str, command: str, key: str = "",
                 value: bytes = b"", *, timeout: float = DEFAULT_CLIENT_TIMEOUT,
                 command_id: str | None = None) -> ControlResponse:
    node = manifest.node(vm_id)
    deadline = time.monotonic() + timeout
    request = ControlRequest(command_id or new_operation_id(), manifest.fingerprint, vm_id,
                             command, key, value)
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("client timeout expired")
        with socket.create_connection((node.control.host, node.control.port),
                                      timeout=remaining) as sock:
            _send_message(sock, request.to_metadata(), request.value, deadline=deadline)
            sock.shutdown(socket.SHUT_WR)
            metadata, body = _recv_payload(sock, deadline=deadline)
            response = ControlResponse.from_metadata(metadata, body, manifest)
            _require_eof(sock, deadline=deadline)
    except (OSError, TimeoutError, RpcProtocolError, ValueError, TypeError) as error:
        raise ControlError(f"control call to {vm_id} failed: {error}") from error
    if (response.command_id != request.command_id or response.node_id != vm_id
            or response.command != request.command):
        raise ControlError(f"control reply from {vm_id} does not match the request")
    return response
