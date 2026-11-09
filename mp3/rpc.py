from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import json
import math
import re
import errno
import selectors
import socket
import time
import uuid

from .config import MAX_VALUE_BYTES
from .attempts import AttemptCancelled, AttemptLease
from .framing import HEADER, recv_exact_deadline, send_all_deadline
from .membership import MembershipTable
from .models import ConsistencyMode, Version
from .ring import Manifest, Node


SCHEMA_VERSION = 1
PROTOCOL_SCHEMA_VERSION = 2
RR_SCHEMA_VERSION = 3
MAX_JSON_BYTES = 16_384
MAX_RPC_PAYLOAD = 4 + MAX_JSON_BYTES + MAX_VALUE_BYTES
MAX_ERROR_BYTES = 1_024
MAX_SOCKET_TIMEOUT_SECONDS = 2**31 - 1
_VM_ID = re.compile(r"vm(?:0[1-9]|10)\Z")
_MANIFEST_ID = re.compile(r"[0-9a-f]{64}\Z")
_DECIMAL = re.compile(r"0|[1-9][0-9]*\Z")
_WRITER_ID = re.compile(r"(vm(?:0[1-9]|10))/(0|[1-9][0-9]*)\Z")


class RpcProtocolError(ValueError):
    pass


class RpcOperation(str, Enum):
    READ_LOCAL = "READ_LOCAL"
    INSTALL_IF_NEWER = "INSTALL_IF_NEWER"


class RpcStatus(str, Enum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    INSTALLED = "INSTALLED"
    UNCHANGED = "UNCHANGED"
    SUPERSEDED = "SUPERSEDED"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    STALE_EPOCH = "STALE_EPOCH"
    ERROR = "ERROR"


class RpcOutcomeKind(str, Enum):
    RESPONSE = "RESPONSE"
    TIMEOUT = "TIMEOUT"
    UNREACHABLE = "UNREACHABLE"
    INVALID_RESPONSE = "INVALID_RESPONSE"
    OVERLOADED = "OVERLOADED"
    CANCELLED = "CANCELLED"
    ERROR = "ERROR"


PROTOCOL_PHASES = frozenset({"strong_query", "strong_write", "strong_readback",
                             "eventual_read", "eventual_put"})
RR_PHASES = frozenset({"eventual_put", "eventual_rr_read", "eventual_rr_repair"})
SUPPORTED_SCHEMAS = (SCHEMA_VERSION, PROTOCOL_SCHEMA_VERSION, RR_SCHEMA_VERSION)


def _validate_phase(op: RpcOperation, mode: ConsistencyMode, phase: str,
                    schema: int, operation_id: str | None) -> None:
    if schema == SCHEMA_VERSION:
        if phase != "local" or operation_id is not None:
            raise RpcProtocolError("schema 1 supports only phase=local without operation_id")
        return
    if schema == RR_SCHEMA_VERSION:
        if phase not in RR_PHASES:
            raise RpcProtocolError("unsupported schema-3 protocol phase")
        if operation_id is None:
            raise RpcProtocolError("schema 3 requires operation_id")
        _validate_request_id(operation_id)
        expected_op = (RpcOperation.READ_LOCAL if phase == "eventual_rr_read"
                       else RpcOperation.INSTALL_IF_NEWER)
        if op != expected_op:
            raise RpcProtocolError("operation does not match protocol phase")
        if mode != ConsistencyMode.EVENTUAL_RR:
            raise RpcProtocolError("Eventual-RR protocol phase requires Eventual-RR mode")
        return
    if schema != PROTOCOL_SCHEMA_VERSION or phase not in PROTOCOL_PHASES:
        raise RpcProtocolError("unsupported schema or protocol phase")
    if operation_id is None:
        raise RpcProtocolError("schema 2 requires operation_id")
    _validate_request_id(operation_id)
    expected_op = (RpcOperation.READ_LOCAL if phase in ("strong_query", "eventual_read")
                   else RpcOperation.INSTALL_IF_NEWER)
    if op != expected_op:
        raise RpcProtocolError("operation does not match protocol phase")
    if phase.startswith("strong_") and mode != ConsistencyMode.STRONG:
        raise RpcProtocolError("Strong protocol phase requires Strong mode")
    if phase.startswith("eventual_") and mode != ConsistencyMode.EVENTUAL_NO_RR:
        raise RpcProtocolError("Eventual-noRR protocol phase requires Eventual-noRR mode")


def _is_int(value: object) -> bool:
    return type(value) is int


def _validate_request_id(value: object) -> str:
    if not isinstance(value, str):
        raise RpcProtocolError("request_id must be a UUID4 string")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as error:
        raise RpcProtocolError("request_id must be a canonical lowercase UUID4") from error
    if (str(parsed) != value or parsed.version != 4
            or parsed.variant != uuid.RFC_4122):
        raise RpcProtocolError("request_id must be a canonical lowercase UUID4")
    return value


def _validate_vm_id(value: object, name: str) -> str:
    if not isinstance(value, str) or _VM_ID.fullmatch(value) is None:
        raise RpcProtocolError(f"{name} must be vm01 through vm10")
    return value


def _parse_decimal(value: object, name: str) -> int:
    if not isinstance(value, str) or _DECIMAL.fullmatch(value) is None:
        raise RpcProtocolError(f"{name} must be a canonical nonnegative decimal string")
    number = 0
    for start in range(0, len(value), 9):
        chunk = value[start:start + 9]
        number = number * (10 ** len(chunk)) + int(chunk)
    return number


def _format_decimal(value: int) -> str:
    if not _is_int(value) or value < 0:
        raise RpcProtocolError("decimal value must be a nonnegative integer")
    pieces = []
    while value >= 1_000_000_000:
        value, rest = divmod(value, 1_000_000_000)
        pieces.append(f"{rest:09d}")
    return str(value) + "".join(reversed(pieces))


def _version_from_wire(raw: object, manifest: Manifest) -> Version | None:
    if raw is None:
        return None
    if not isinstance(raw, dict) or set(raw) != {"kind", "number", "writer_id"}:
        raise RpcProtocolError("version must contain exactly kind, number, writer_id")
    kind = raw["kind"]
    writer = raw["writer_id"]
    if kind not in ("strong", "eventual") or not isinstance(writer, str):
        raise RpcProtocolError("invalid version kind or writer_id")
    match = _WRITER_ID.fullmatch(writer)
    if match is None:
        raise RpcProtocolError("writer_id must be vmNN/generation with canonical generation")
    try:
        manifest.node(match.group(1))
        number = _parse_decimal(raw["number"], "version.number")
        return Version(kind, number, writer)
    except (ValueError, TypeError) as error:
        raise RpcProtocolError(f"invalid version: {error}") from error


def _version_to_wire(value: Version | None) -> dict[str, str] | None:
    if value is None:
        return None
    return {"kind": value.kind, "number": _format_decimal(value.number),
            "writer_id": value.writer_id}


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise RpcProtocolError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


def _reject_json_constant(value: str):
    raise RpcProtocolError(f"invalid JSON constant: {value}")


def _decode_json(encoded: bytes) -> dict:
    try:
        text = encoded.decode("utf-8", "strict")
        data = json.loads(text, object_pairs_hook=_unique_object,
                          parse_constant=_reject_json_constant)
    except RpcProtocolError:
        raise
    except (UnicodeError, json.JSONDecodeError, RecursionError) as error:
        raise RpcProtocolError(f"invalid UTF-8 JSON metadata: {error}") from error
    if not isinstance(data, dict):
        raise RpcProtocolError("metadata must be a JSON object")
    return data


def _encode_json(data: dict) -> bytes:
    try:
        encoded = json.dumps(data, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8", "strict")
    except (TypeError, ValueError, UnicodeError) as error:
        raise RpcProtocolError(f"metadata is not strict UTF-8 JSON: {error}") from error
    if len(encoded) > MAX_JSON_BYTES:
        raise RpcProtocolError("JSON metadata exceeds 16384 bytes")
    return encoded


def _mode(value: object) -> ConsistencyMode:
    try:
        return ConsistencyMode(value)
    except (ValueError, TypeError) as error:
        raise RpcProtocolError("invalid consistency mode") from error


def _operation(value: object) -> RpcOperation:
    try:
        return RpcOperation(value)
    except (ValueError, TypeError) as error:
        raise RpcProtocolError("unsupported operation") from error


def _status(value: object) -> RpcStatus:
    try:
        return RpcStatus(value)
    except (ValueError, TypeError) as error:
        raise RpcProtocolError("unsupported response status") from error


@dataclass(frozen=True)
class ReplicaRequest:
    request_id: str
    manifest_id: str
    sender_id: str
    sender_generation: int
    target_id: str
    epoch: int
    mode: ConsistencyMode
    op: RpcOperation
    phase: str
    key: str
    version: Version | None = None
    value: bytes = b""
    schema: int = SCHEMA_VERSION
    operation_id: str | None = None

    def __post_init__(self) -> None:
        _validate_request_id(self.request_id)
        if not isinstance(self.manifest_id, str) or _MANIFEST_ID.fullmatch(self.manifest_id) is None:
            raise RpcProtocolError("manifest_id must be lowercase SHA-256 hex")
        _validate_vm_id(self.sender_id, "sender_id")
        _validate_vm_id(self.target_id, "target_id")
        if not _is_int(self.schema) or self.schema not in SUPPORTED_SCHEMAS:
            raise RpcProtocolError("unsupported RPC schema")
        minimum = 0 if self.schema == SCHEMA_VERSION else 1
        if not _is_int(self.sender_generation) or self.sender_generation < minimum:
            raise RpcProtocolError("invalid sender_generation for RPC schema")
        if not _is_int(self.epoch) or self.epoch < minimum:
            raise RpcProtocolError("invalid epoch for RPC schema")
        if not isinstance(self.mode, ConsistencyMode):
            raise RpcProtocolError("mode must be a ConsistencyMode")
        if not isinstance(self.op, RpcOperation):
            raise RpcProtocolError("op must be a supported RpcOperation")
        _validate_phase(self.op, self.mode, self.phase, self.schema, self.operation_id)
        if not isinstance(self.key, str):
            raise RpcProtocolError("key must be a string")
        try:
            self.key.encode("utf-8", "strict")
        except UnicodeEncodeError as error:
            raise RpcProtocolError("key must be strictly UTF-8 encodable") from error
        if not isinstance(self.value, bytes) or len(self.value) > MAX_VALUE_BYTES:
            raise RpcProtocolError("value must be bytes no larger than 1 MiB")
        if self.op == RpcOperation.READ_LOCAL:
            if self.version is not None or self.value:
                raise RpcProtocolError("READ_LOCAL requires null version and an empty body")
        elif self.version is None:
            raise RpcProtocolError("INSTALL_IF_NEWER requires a version")
        if self.version is not None:
            self._validate_version_kind(self.version)
        if self.schema in (PROTOCOL_SCHEMA_VERSION, RR_SCHEMA_VERSION):
            if self.phase in ("strong_query", "eventual_read", "eventual_rr_read"):
                if self.version is not None or self.value:
                    raise RpcProtocolError("protocol read requires null version and empty body")
            elif self.version is None:
                raise RpcProtocolError("protocol install requires a version")

    @property
    def value_len(self) -> int:
        return len(self.value)

    def _validate_version_kind(self, version: Version) -> None:
        expected = "strong" if self.mode == ConsistencyMode.STRONG else "eventual"
        if version.kind != expected:
            raise RpcProtocolError("version kind does not match request mode")

    def validate_manifest(self, manifest: Manifest) -> None:
        if not isinstance(manifest, Manifest) or self.manifest_id != manifest.fingerprint:
            raise RpcProtocolError("request manifest fingerprint mismatch")
        manifest.node(self.sender_id)
        manifest.node(self.target_id)
        if self.version is not None:
            match = _WRITER_ID.fullmatch(self.version.writer_id)
            if match is None:
                raise RpcProtocolError("invalid version writer identity")
            manifest.node(match.group(1))

    def to_metadata(self) -> dict:
        metadata = {
            "schema": self.schema,
            "request_id": self.request_id,
            "manifest_id": self.manifest_id,
            "sender_id": self.sender_id,
            "sender_generation": _format_decimal(self.sender_generation),
            "target_id": self.target_id,
            "epoch": _format_decimal(self.epoch),
            "mode": self.mode.value,
            "op": self.op.value,
            "phase": self.phase,
            "key": self.key,
            "version": _version_to_wire(self.version),
            "value_len": self.value_len,
        }
        if self.schema != SCHEMA_VERSION:
            metadata["operation_id"] = self.operation_id
        return metadata

    @classmethod
    def from_metadata(cls, metadata: dict, value: bytes, manifest: Manifest) -> "ReplicaRequest":
        if not isinstance(metadata, dict) or "schema" not in metadata:
            raise RpcProtocolError("request schema is missing")
        schema = metadata["schema"]
        if not _is_int(schema) or schema not in SUPPORTED_SCHEMAS:
            raise RpcProtocolError("unsupported RPC schema")
        required = {"schema", "request_id", "manifest_id", "sender_id", "sender_generation",
                    "target_id", "epoch", "mode", "op", "phase", "key", "version", "value_len"}
        if schema != SCHEMA_VERSION:
            required.add("operation_id")
        if set(metadata) != required:
            raise RpcProtocolError("request has missing or unexpected fields")
        value_len = metadata["value_len"]
        if not _is_int(value_len) or not 0 <= value_len <= MAX_VALUE_BYTES or value_len != len(value):
            raise RpcProtocolError("request value_len is invalid or inconsistent with body")
        version = _version_from_wire(metadata["version"], manifest)
        try:
            request = cls(
                request_id=metadata["request_id"], manifest_id=metadata["manifest_id"],
                sender_id=metadata["sender_id"],
                sender_generation=_parse_decimal(metadata["sender_generation"], "sender_generation"),
                target_id=metadata["target_id"], epoch=_parse_decimal(metadata["epoch"], "epoch"),
                mode=_mode(metadata["mode"]), op=_operation(metadata["op"]),
                phase=metadata["phase"], key=metadata["key"], version=version, value=value,
                schema=metadata["schema"],
                operation_id=metadata.get("operation_id"),
            )
            request.validate_manifest(manifest)
            return request
        except (ValueError, TypeError) as error:
            if isinstance(error, RpcProtocolError):
                raise
            raise RpcProtocolError(f"invalid request metadata: {error}") from error


@dataclass(frozen=True)
class ReplicaResponse:
    request_id: str
    manifest_id: str
    sender_id: str
    sender_generation: int
    target_id: str
    epoch: int
    mode: ConsistencyMode
    op: RpcOperation
    phase: str
    status: RpcStatus
    present: bool
    version: Version | None = None
    stored_version: Version | None = None
    value: bytes = b""
    durable: bool = False
    error: str | None = None
    schema: int = SCHEMA_VERSION
    operation_id: str | None = None

    def __post_init__(self) -> None:
        _validate_request_id(self.request_id)
        if not isinstance(self.manifest_id, str) or _MANIFEST_ID.fullmatch(self.manifest_id) is None:
            raise RpcProtocolError("manifest_id must be lowercase SHA-256 hex")
        _validate_vm_id(self.sender_id, "sender_id")
        _validate_vm_id(self.target_id, "target_id")
        if not _is_int(self.schema) or self.schema not in SUPPORTED_SCHEMAS:
            raise RpcProtocolError("unsupported RPC schema")
        minimum = 0 if self.schema == SCHEMA_VERSION else 1
        if not _is_int(self.sender_generation) or self.sender_generation < minimum:
            raise RpcProtocolError("invalid sender_generation for RPC schema")
        if not _is_int(self.epoch) or self.epoch < minimum:
            raise RpcProtocolError("invalid epoch for RPC schema")
        if not isinstance(self.mode, ConsistencyMode):
            raise RpcProtocolError("mode must be a ConsistencyMode")
        if not isinstance(self.op, RpcOperation):
            raise RpcProtocolError("unsupported operation")
        _validate_phase(self.op, self.mode, self.phase, self.schema, self.operation_id)
        if not isinstance(self.status, RpcStatus):
            raise RpcProtocolError("status must be a RpcStatus")
        if type(self.present) is not bool or type(self.durable) is not bool:
            raise RpcProtocolError("present and durable must be booleans")
        if not isinstance(self.value, bytes) or len(self.value) > MAX_VALUE_BYTES:
            raise RpcProtocolError("response body must be bytes no larger than 1 MiB")
        if self.error is not None:
            if not isinstance(self.error, str):
                raise RpcProtocolError("error must be text or null")
            try:
                error_bytes = self.error.encode("utf-8", "strict")
            except UnicodeEncodeError as error:
                raise RpcProtocolError("error must be UTF-8 encodable") from error
            if len(error_bytes) > MAX_ERROR_BYTES or "\x00" in self.error:
                raise RpcProtocolError("error text exceeds the safe 1024-byte field")
        for version in (self.version, self.stored_version):
            if version is not None and not isinstance(version, Version):
                raise RpcProtocolError("version fields must be Version or null")
        expected_kind = "strong" if self.mode == ConsistencyMode.STRONG else "eventual"
        for version in (self.version, self.stored_version):
            if version is not None and version.kind != expected_kind:
                raise RpcProtocolError("response version kind does not match response mode")

        empty_payload_shape = not self.present and self.version is None and not self.value
        no_write_evidence = empty_payload_shape and not self.durable
        if self.status == RpcStatus.FOUND:
            if (self.op != RpcOperation.READ_LOCAL or not self.present or self.version is None
                    or self.stored_version is not None or self.durable or self.error is not None):
                raise RpcProtocolError("contradictory FOUND response")
        elif self.status == RpcStatus.NOT_FOUND:
            if (self.op != RpcOperation.READ_LOCAL or not no_write_evidence
                    or self.stored_version is not None or self.error is not None):
                raise RpcProtocolError("contradictory NOT_FOUND response")
        elif self.status in (RpcStatus.INSTALLED, RpcStatus.UNCHANGED, RpcStatus.SUPERSEDED):
            if (self.op != RpcOperation.INSTALL_IF_NEWER or not empty_payload_shape
                    or self.stored_version is None or not self.durable or self.error is not None):
                raise RpcProtocolError(f"contradictory {self.status.value} response")
        elif self.status == RpcStatus.VERSION_CONFLICT:
            if (self.op != RpcOperation.INSTALL_IF_NEWER or not no_write_evidence
                    or self.stored_version is None or self.error is not None):
                raise RpcProtocolError("contradictory VERSION_CONFLICT response")
        elif self.status == RpcStatus.STALE_EPOCH:
            if (not no_write_evidence or self.stored_version is not None
                    or self.error != "STALE_EPOCH"):
                raise RpcProtocolError("contradictory STALE_EPOCH response")
        elif self.status == RpcStatus.ERROR:
            if (not no_write_evidence or self.stored_version is not None or self.error is None):
                raise RpcProtocolError("contradictory ERROR response")

    @property
    def value_len(self) -> int:
        return len(self.value)

    def validate_for(self, request: ReplicaRequest, target: Node, manifest: Manifest,
                     expected_generation: int) -> None:
        if (self.schema != request.schema or self.request_id != request.request_id
                or self.operation_id != request.operation_id):
            raise RpcProtocolError("response schema or request_id mismatch")
        if (self.manifest_id != manifest.fingerprint or request.manifest_id != manifest.fingerprint
                or target != manifest.node(request.target_id)):
            raise RpcProtocolError("response manifest or target binding mismatch")
        if (self.sender_id != target.vm_id or self.target_id != request.sender_id
                or self.sender_generation != expected_generation):
            raise RpcProtocolError("response replica identity or generation mismatch")
        if self.op != request.op or self.phase != request.phase:
            raise RpcProtocolError("response operation or phase mismatch")
        if self.status == RpcStatus.STALE_EPOCH:
            if (self.epoch, self.mode) == (request.epoch, request.mode):
                raise RpcProtocolError("STALE_EPOCH must report a different current context")
            return
        if (self.epoch, self.mode) != (request.epoch, request.mode):
            raise RpcProtocolError("response context mismatch")
        if request.op == RpcOperation.READ_LOCAL:
            if self.version is not None:
                request._validate_version_kind(self.version)
        elif self.status in (RpcStatus.INSTALLED, RpcStatus.UNCHANGED,
                             RpcStatus.SUPERSEDED, RpcStatus.VERSION_CONFLICT):
            requested = request.version
            assert requested is not None and self.stored_version is not None
            comparison = self.stored_version.compare(requested)
            if self.status in (RpcStatus.INSTALLED, RpcStatus.UNCHANGED) and comparison != 0:
                raise RpcProtocolError("installed response stored_version differs from request")
            if self.status == RpcStatus.SUPERSEDED and comparison <= 0:
                raise RpcProtocolError("SUPERSEDED must report a strictly newer stored_version")
            if self.status == RpcStatus.VERSION_CONFLICT and comparison != 0:
                raise RpcProtocolError("VERSION_CONFLICT must report the equal stored version")

    def to_metadata(self) -> dict:
        metadata = {
            "schema": self.schema,
            "request_id": self.request_id,
            "manifest_id": self.manifest_id,
            "sender_id": self.sender_id,
            "sender_generation": _format_decimal(self.sender_generation),
            "target_id": self.target_id,
            "epoch": _format_decimal(self.epoch),
            "mode": self.mode.value,
            "op": self.op.value,
            "phase": self.phase,
            "status": self.status.value,
            "present": self.present,
            "version": _version_to_wire(self.version),
            "stored_version": _version_to_wire(self.stored_version),
            "value_len": self.value_len,
            "durable": self.durable,
            "error": self.error,
        }
        if self.schema != SCHEMA_VERSION:
            metadata["operation_id"] = self.operation_id
        return metadata

    @classmethod
    def from_metadata(cls, metadata: dict, value: bytes, manifest: Manifest) -> "ReplicaResponse":
        if not isinstance(metadata, dict) or "schema" not in metadata:
            raise RpcProtocolError("response schema is missing")
        schema = metadata["schema"]
        if not _is_int(schema) or schema not in SUPPORTED_SCHEMAS:
            raise RpcProtocolError("unsupported RPC schema")
        required = {"schema", "request_id", "manifest_id", "sender_id", "sender_generation",
                    "target_id", "epoch", "mode", "op", "phase", "status", "present",
                    "version", "stored_version", "value_len", "durable", "error"}
        if schema != SCHEMA_VERSION:
            required.add("operation_id")
        if set(metadata) != required:
            raise RpcProtocolError("response has missing or unexpected fields")
        value_len = metadata["value_len"]
        if not _is_int(value_len) or not 0 <= value_len <= MAX_VALUE_BYTES or value_len != len(value):
            raise RpcProtocolError("response value_len is invalid or inconsistent with body")
        try:
            response = cls(
                request_id=metadata["request_id"], manifest_id=metadata["manifest_id"],
                sender_id=metadata["sender_id"],
                sender_generation=_parse_decimal(metadata["sender_generation"], "sender_generation"),
                target_id=metadata["target_id"], epoch=_parse_decimal(metadata["epoch"], "epoch"),
                mode=_mode(metadata["mode"]), op=_operation(metadata["op"]),
                phase=metadata["phase"], status=_status(metadata["status"]),
                present=metadata["present"],
                version=_version_from_wire(metadata["version"], manifest),
                stored_version=_version_from_wire(metadata["stored_version"], manifest),
                value=value, durable=metadata["durable"], error=metadata["error"],
                schema=metadata["schema"],
                operation_id=metadata.get("operation_id"),
            )
            manifest.node(response.sender_id)
            manifest.node(response.target_id)
            return response
        except (ValueError, TypeError) as error:
            if isinstance(error, RpcProtocolError):
                raise
            raise RpcProtocolError(f"invalid response metadata: {error}") from error


@dataclass(frozen=True)
class RpcOutcome:
    kind: RpcOutcomeKind
    response: ReplicaResponse | None = None
    error: str | None = None
    completion_unknown: bool = False


def new_operation_id() -> str:
    return str(uuid.uuid4())


def new_request(*, manifest: Manifest, sender_id: str, sender_generation: int,
                target_id: str, epoch: int, mode: ConsistencyMode, op: RpcOperation,
                key: str, version: Version | None = None, value: bytes = b"",
                phase: str = "local", schema: int = SCHEMA_VERSION,
                operation_id: str | None = None) -> ReplicaRequest:
    if schema != SCHEMA_VERSION and operation_id is None:
        operation_id = new_operation_id()
    request = ReplicaRequest(
        request_id=str(uuid.uuid4()), manifest_id=manifest.fingerprint,
        sender_id=sender_id, sender_generation=sender_generation,
        target_id=target_id, epoch=epoch, mode=mode, op=op, phase=phase,
        key=key, version=version, value=value, schema=schema,
        operation_id=operation_id)
    request.validate_manifest(manifest)
    return request


def _encode_payload(metadata: dict, value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) > MAX_VALUE_BYTES:
        raise RpcProtocolError("raw value body must be bytes of at most 1 MiB")
    encoded = _encode_json(metadata)
    payload = HEADER.pack(len(encoded)) + encoded + value
    if len(payload) > MAX_RPC_PAYLOAD:
        raise RpcProtocolError("outer RPC payload exceeds 1064964 bytes")
    return payload


def encode_request(request: ReplicaRequest, manifest: Manifest) -> bytes:
    request.validate_manifest(manifest)
    return _encode_payload(request.to_metadata(), request.value)


def encode_response(response: ReplicaResponse, manifest: Manifest) -> bytes:
    if response.manifest_id != manifest.fingerprint:
        raise RpcProtocolError("response manifest fingerprint mismatch")
    manifest.node(response.sender_id)
    manifest.node(response.target_id)
    return _encode_payload(response.to_metadata(), response.value)


def _recv_payload(sock: socket.socket, *, deadline: float) -> tuple[dict, bytes]:
    outer_header = recv_exact_deadline(sock, HEADER.size, deadline=deadline)
    (outer_len,) = HEADER.unpack(outer_header)
    if outer_len < HEADER.size or outer_len > MAX_RPC_PAYLOAD:
        raise RpcProtocolError("outer payload length is out of bounds")
    json_header = recv_exact_deadline(sock, HEADER.size, deadline=deadline)
    (json_len,) = HEADER.unpack(json_header)
    if json_len > MAX_JSON_BYTES:
        raise RpcProtocolError("JSON metadata length exceeds 16384 bytes")
    if HEADER.size + json_len > outer_len:
        raise RpcProtocolError("JSON length exceeds declared outer payload")
    metadata = _decode_json(recv_exact_deadline(sock, json_len, deadline=deadline))
    value_len = metadata.get("value_len")
    if not _is_int(value_len) or not 0 <= value_len <= MAX_VALUE_BYTES:
        raise RpcProtocolError("value_len must be an integer within the 1 MiB limit")
    if HEADER.size + json_len + value_len != outer_len:
        raise RpcProtocolError("declared lengths do not exactly match the outer payload")
    value = recv_exact_deadline(sock, value_len, deadline=deadline)
    return metadata, value


def _require_eof(sock: socket.socket, *, deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("operation deadline expired while checking connection end")
    sock.settimeout(remaining)
    if sock.recv(1):
        raise RpcProtocolError("unexpected bytes after the single RPC message")


def receive_request(sock: socket.socket, manifest: Manifest, *, deadline: float,
                    require_eof: bool = True) -> ReplicaRequest:
    metadata, value = _recv_payload(sock, deadline=deadline)
    request = ReplicaRequest.from_metadata(metadata, value, manifest)
    if require_eof:
        _require_eof(sock, deadline=deadline)
    return request


def receive_response(sock: socket.socket, manifest: Manifest, *, deadline: float,
                     require_eof: bool = True) -> ReplicaResponse:
    metadata, value = _recv_payload(sock, deadline=deadline)
    response = ReplicaResponse.from_metadata(metadata, value, manifest)
    if require_eof:
        _require_eof(sock, deadline=deadline)
    return response


def send_request(sock: socket.socket, request: ReplicaRequest, manifest: Manifest,
                 *, deadline: float) -> None:
    payload = encode_request(request, manifest)
    send_all_deadline(sock, HEADER.pack(len(payload)), deadline=deadline)
    send_all_deadline(sock, payload, deadline=deadline)


def send_response(sock: socket.socket, response: ReplicaResponse, manifest: Manifest,
                  *, deadline: float) -> None:
    payload = encode_response(response, manifest)
    send_all_deadline(sock, HEADER.pack(len(payload)), deadline=deadline)
    send_all_deadline(sock, payload, deadline=deadline)


def _request_error(kind: RpcOutcomeKind, error: BaseException | str, *,
                   completion_unknown: bool = False) -> RpcOutcome:
    return RpcOutcome(kind, None, str(error)[:MAX_ERROR_BYTES], completion_unknown)


def _lease_wait(sock: socket.socket, event: int, lease: AttemptLease) -> None:
    with selectors.DefaultSelector() as selector:
        selector.register(sock, event)
        while True:
            remaining = lease.check()
            ready = selector.select(min(0.05, remaining))
            if ready:
                lease.check()
                return


def _lease_connect(addresses, lease: AttemptLease) -> socket.socket:
    last_error = None
    connecting = {errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY, errno.EINTR}
    for family, socktype, proto, sockaddr in addresses:
        lease.check()
        connection = socket.socket(family, socktype, proto)
        connection.setblocking(False)
        try:
            result = connection.connect_ex(sockaddr)
            if result not in (0, errno.EISCONN):
                if result not in connecting:
                    raise OSError(result, "nonblocking connect failed")
                _lease_wait(connection, selectors.EVENT_WRITE, lease)
                error = connection.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                if error:
                    raise OSError(error, "nonblocking connect failed")
            lease.check()
            return connection
        except (AttemptCancelled, TimeoutError):
            connection.close()
            raise
        except OSError as error:
            connection.close()
            last_error = error
    if last_error is not None:
        raise last_error
    raise OSError("no usable cached address")


def _lease_send_all(sock: socket.socket, data: bytes, lease: AttemptLease,
                    before_first_send=None) -> None:
    offset = 0
    while offset < len(data):
        lease.check()
        _lease_wait(sock, selectors.EVENT_WRITE, lease)
        if offset == 0 and before_first_send is not None:
            before_first_send()
        sent = sock.send(data[offset:])
        if sent <= 0:
            raise ConnectionError("socket closed during request send")
        offset += sent
    lease.check()


def _lease_recv_exact(sock: socket.socket, size: int, lease: AttemptLease) -> bytes:
    if type(size) is not int or size < 0:
        raise ValueError("size must be a nonnegative integer")
    data = bytearray()
    while len(data) < size:
        lease.check()
        _lease_wait(sock, selectors.EVENT_READ, lease)
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("socket closed before expected data arrived")
        data.extend(chunk)
    lease.check()
    return bytes(data)


def _lease_receive_response(sock: socket.socket, manifest: Manifest,
                            lease: AttemptLease) -> ReplicaResponse:
    (outer_len,) = HEADER.unpack(_lease_recv_exact(sock, HEADER.size, lease))
    if outer_len < HEADER.size or outer_len > MAX_RPC_PAYLOAD:
        raise RpcProtocolError("outer RPC payload length is out of bounds")
    (json_len,) = HEADER.unpack(_lease_recv_exact(sock, HEADER.size, lease))
    if json_len > MAX_JSON_BYTES or HEADER.size + json_len > outer_len:
        raise RpcProtocolError("JSON length is outside the declared RPC payload")
    metadata = _decode_json(_lease_recv_exact(sock, json_len, lease))
    value_len = metadata.get("value_len")
    if not _is_int(value_len) or not 0 <= value_len <= MAX_VALUE_BYTES:
        raise RpcProtocolError("value_len must be within the 1 MiB limit")
    if HEADER.size + json_len + value_len != outer_len:
        raise RpcProtocolError("declared RPC lengths do not match exactly")
    value = _lease_recv_exact(sock, value_len, lease)
    response = ReplicaResponse.from_metadata(metadata, value, manifest)
    lease.check()
    _lease_wait(sock, selectors.EVENT_READ, lease)
    if sock.recv(1):
        raise RpcProtocolError("unexpected bytes after the single RPC message")
    lease.check()
    return response


class ReplicaClient:
    def __init__(self, manifest: Manifest, membership: MembershipTable, *,
                 protocol_activation=None, rr_activation=None):
        if not isinstance(manifest, Manifest) or not isinstance(membership, MembershipTable):
            raise TypeError("manifest and MembershipTable are required")
        if manifest.fingerprint != membership.manifest.fingerprint:
            raise ValueError("membership and RPC manifest differ")
        self.manifest = manifest
        self.membership = membership
        self.protocol_activation = protocol_activation
        self.rr_activation = rr_activation
        self._addresses = {}
        for node in manifest.nodes:
            try:
                self._addresses[node.vm_id] = tuple(
                    (family, socktype, proto, sockaddr)
                    for family, socktype, proto, _canonname, sockaddr in
                    socket.getaddrinfo(node.kv.host, node.kv.port, type=socket.SOCK_STREAM)
                )[:8]
            except OSError as error:
                self._addresses[node.vm_id] = error

    def _connect(self, target: Node, *, deadline: float) -> socket.socket:
        addresses = self._addresses[target.vm_id]
        if isinstance(addresses, OSError):
            raise OSError(f"could not resolve KV endpoint for {target.vm_id}: {addresses}")
        last_error = None
        for family, socktype, proto, sockaddr in addresses:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("operation deadline expired during connection")
            connection = socket.socket(family, socktype, proto)
            try:
                connection.settimeout(remaining)
                connection.connect(sockaddr)
                return connection
            except socket.timeout:
                connection.close()
                raise
            except OSError as error:
                connection.close()
                last_error = error
        if last_error is not None:
            raise last_error
        raise OSError(f"no usable KV address for {target.vm_id}")

    def _call_replica_with_lease(self, target: Node, request: ReplicaRequest,
                                 payload: bytes, expected_generation: int,
                                 lease: AttemptLease) -> RpcOutcome:
        addresses = self._addresses[target.vm_id]
        if isinstance(addresses, OSError):
            return _request_error(RpcOutcomeKind.UNREACHABLE, addresses)
        connection = None
        connected = False
        install_may_have_been_sent = False
        try:
            connection = _lease_connect(addresses, lease)
            connected = True

            def mark_possible_install_transmission() -> None:
                nonlocal install_may_have_been_sent
                install_may_have_been_sent = True

            frame = HEADER.pack(len(payload)) + payload
            _lease_send_all(connection, frame, lease, mark_possible_install_transmission)
            connection.shutdown(socket.SHUT_WR)
            response = _lease_receive_response(connection, self.manifest, lease)
            response.validate_for(request, target, self.manifest, expected_generation)
            lease.check()
            if response.status == RpcStatus.ERROR and response.error.startswith("OVERLOADED"):
                return RpcOutcome(RpcOutcomeKind.OVERLOADED, response, response.error)
            completion_unknown = (
                request.op == RpcOperation.INSTALL_IF_NEWER
                and response.status == RpcStatus.ERROR
                and response.error == "STORAGE_ERROR"
            )
            return RpcOutcome(RpcOutcomeKind.RESPONSE, response,
                              completion_unknown=completion_unknown)
        except AttemptCancelled as error:
            return _request_error(RpcOutcomeKind.CANCELLED, error,
                                  completion_unknown=install_may_have_been_sent)
        except TimeoutError as error:
            return _request_error(RpcOutcomeKind.TIMEOUT, error,
                                  completion_unknown=install_may_have_been_sent)
        except RpcProtocolError as error:
            return _request_error(RpcOutcomeKind.INVALID_RESPONSE, error,
                                  completion_unknown=install_may_have_been_sent)
        except (ConnectionRefusedError, socket.gaierror, OSError) as error:
            kind = RpcOutcomeKind.ERROR if connected else RpcOutcomeKind.UNREACHABLE
            return _request_error(kind, error,
                                  completion_unknown=install_may_have_been_sent)
        except (ValueError, TypeError, OverflowError) as error:
            return _request_error(RpcOutcomeKind.INVALID_RESPONSE, error,
                                  completion_unknown=install_may_have_been_sent)
        finally:
            if connection is not None:
                try:
                    connection.close()
                except OSError:
                    pass

    def call_replica(self, target: Node, request: ReplicaRequest, *, deadline: float,
                     expected_target_generation: int | None = None,
                     attempt_lease: AttemptLease | None = None) -> RpcOutcome:
        if type(deadline) not in (int, float):
            return _request_error(RpcOutcomeKind.ERROR,
                                  "deadline must be a finite monotonic timestamp")
        try:
            finite_deadline = math.isfinite(deadline)
        except OverflowError:
            finite_deadline = False
        if not finite_deadline:
            return _request_error(RpcOutcomeKind.ERROR,
                                  "deadline must be a finite monotonic timestamp")
        if attempt_lease is None and deadline <= time.monotonic():
            return _request_error(RpcOutcomeKind.TIMEOUT, "operation deadline expired")
        if attempt_lease is not None:
            schema2_put = (request.schema == PROTOCOL_SCHEMA_VERSION
                           and request.phase == "eventual_put"
                           and request.op == RpcOperation.INSTALL_IF_NEWER)
            schema3_rr = (request.schema == RR_SCHEMA_VERSION
                          and request.phase in RR_PHASES)
            if (not isinstance(attempt_lease, AttemptLease)
                    or not (schema2_put or schema3_rr)
                    or deadline != attempt_lease.hard_deadline):
                return _request_error(
                    RpcOutcomeKind.ERROR,
                    "attempt lease is only valid for schema-2 Eventual PUT "
                    "or schema-3 Eventual-RR phases")
            try:
                attempt_lease.check()
            except AttemptCancelled as error:
                return _request_error(RpcOutcomeKind.CANCELLED, error)
            except TimeoutError as error:
                return _request_error(RpcOutcomeKind.TIMEOUT, error)
        if attempt_lease is None and deadline - time.monotonic() > MAX_SOCKET_TIMEOUT_SECONDS:
            return _request_error(RpcOutcomeKind.ERROR,
                                  "deadline exceeds the supported socket timeout range")
        try:
            if not isinstance(target, Node) or target != self.manifest.node(target.vm_id):
                raise RpcProtocolError("target must be a node from this manifest")
            request.validate_manifest(self.manifest)
            if request.target_id != target.vm_id:
                raise RpcProtocolError("request target_id does not match target endpoint")
            if request.sender_id != self.membership.self_id:
                raise RpcProtocolError("request sender_id does not match local membership identity")
            if request.sender_generation != self.membership.local_generation:
                raise RpcProtocolError("request sender_generation does not match local generation")
            if (request.schema == PROTOCOL_SCHEMA_VERSION
                    and (self.protocol_activation is None
                         or not self.protocol_activation.enabled_for(self.manifest))):
                raise RpcProtocolError("SCHEMA2_DISABLED")
            if (request.schema == RR_SCHEMA_VERSION
                    and (self.rr_activation is None
                         or not self.rr_activation.enabled_for(self.manifest))):
                raise RpcProtocolError("SCHEMA3_DISABLED")
            row = next(member for member in self.membership.snapshot()
                       if member.vm_id == target.vm_id)
            if expected_target_generation is None:
                expected_generation = row.restart_generation
            else:
                if type(expected_target_generation) is not int or expected_target_generation <= 0:
                    raise RpcProtocolError("expected target generation must be positive")
                expected_generation = expected_target_generation
            if expected_generation <= 0:
                raise RpcProtocolError("target generation is unknown; wait for membership recovery")
            payload = encode_request(request, self.manifest)
            if attempt_lease is None and time.monotonic() >= deadline:
                return _request_error(RpcOutcomeKind.TIMEOUT, "operation deadline expired")
        except (ValueError, TypeError, RuntimeError) as error:
            return _request_error(RpcOutcomeKind.ERROR, error)
        if attempt_lease is not None:
            return self._call_replica_with_lease(
                target, request, payload, expected_generation, attempt_lease)

        connected = False
        install_may_have_been_sent = False
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return _request_error(RpcOutcomeKind.TIMEOUT, "operation deadline expired")
            with self._connect(target, deadline=deadline) as sock:
                connected = True
                def note_possible_transmission() -> None:
                    nonlocal install_may_have_been_sent
                    if request.op == RpcOperation.INSTALL_IF_NEWER:
                        install_may_have_been_sent = True

                send_all_deadline(sock, HEADER.pack(len(payload)), deadline=deadline,
                                  before_send=note_possible_transmission)
                send_all_deadline(sock, payload, deadline=deadline,
                                  before_send=note_possible_transmission)
                sock.shutdown(socket.SHUT_WR)
                response = receive_response(sock, self.manifest, deadline=deadline)
            response.validate_for(request, target, self.manifest, expected_generation)
            if time.monotonic() >= deadline:
                return _request_error(
                    RpcOutcomeKind.TIMEOUT,
                    "operation deadline expired during response validation",
                    completion_unknown=install_may_have_been_sent,
                )
            if response.status == RpcStatus.ERROR and response.error.startswith("OVERLOADED"):
                return RpcOutcome(RpcOutcomeKind.OVERLOADED, response, response.error)
            completion_unknown = (
                request.op == RpcOperation.INSTALL_IF_NEWER
                and response.status == RpcStatus.ERROR
                and response.error == "STORAGE_ERROR"
            )
            return RpcOutcome(RpcOutcomeKind.RESPONSE, response,
                              completion_unknown=completion_unknown)
        except (socket.timeout, TimeoutError) as error:
            return RpcOutcome(RpcOutcomeKind.TIMEOUT, None,
                              str(error or "operation deadline expired")[:MAX_ERROR_BYTES],
                              install_may_have_been_sent)
        except RpcProtocolError as error:
            return RpcOutcome(RpcOutcomeKind.INVALID_RESPONSE, None,
                              str(error)[:MAX_ERROR_BYTES], install_may_have_been_sent)
        except (ConnectionRefusedError, socket.gaierror, OSError) as error:
            kind = RpcOutcomeKind.ERROR if connected else RpcOutcomeKind.UNREACHABLE
            return RpcOutcome(kind, None, str(error)[:MAX_ERROR_BYTES],
                              install_may_have_been_sent)
        except OverflowError as error:
            return RpcOutcome(RpcOutcomeKind.ERROR, None,
                              str(error)[:MAX_ERROR_BYTES], install_may_have_been_sent)
        except (ValueError, TypeError) as error:
            return RpcOutcome(RpcOutcomeKind.INVALID_RESPONSE, None,
                              str(error)[:MAX_ERROR_BYTES], install_may_have_been_sent)
