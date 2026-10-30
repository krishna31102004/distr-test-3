"""Local command/result types; these are not a network schema (KD-06/10)."""

from dataclasses import dataclass
from enum import Enum


class ConsistencyMode(str, Enum):
    STRONG = "strong"
    EVENTUAL_NO_RR = "eventual-noRR"
    EVENTUAL_RR = "eventual-RR"


class Status(str, Enum):
    OK = "OK"
    NOT_FOUND = "NOT_FOUND"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"


@dataclass(frozen=True)
class Command:
    name: str
    args: tuple[str, ...]


@dataclass(frozen=True)
class OperationResult:
    """Empty bytes are a successful value, never a missing-value sentinel.

    Final RPC responses will also require request/replica/version/epoch metadata;
    this deliberately small local result does not define their encoding.
    """

    status: Status
    message: str = ""
    value: bytes | None = None

    def __post_init__(self) -> None:
        """Reject ambiguous values on failures and nonbinary successful values."""
        if not isinstance(self.status, Status):
            raise ValueError("status must be a Status")
        if self.value is not None and not isinstance(self.value, bytes):
            raise TypeError("value must be bytes or None")
        if self.status != Status.OK and self.value is not None:
            raise ValueError("only OK results may carry a value")


@dataclass(frozen=True)
class Context:
    epoch: int
    mode: ConsistencyMode

    def __post_init__(self):
        if type(self.epoch) is not int or self.epoch < 1:
            raise ValueError("epoch must be a positive integer")
        if not isinstance(self.mode, ConsistencyMode):
            raise ValueError("mode must be a ConsistencyMode")


@dataclass(frozen=True)
class Version:
    kind: str
    number: int
    writer_id: str

    def __post_init__(self):
        import re
        if self.kind not in ("strong", "eventual"):
            raise ValueError("invalid version kind")
        if type(self.number) is not int or self.number < 0:
            raise ValueError("version number must be a nonnegative integer")
        if not isinstance(self.writer_id, str) or not re.fullmatch(
                r"vm(?:0[1-9]|10)/(?:0|[1-9][0-9]*)", self.writer_id):
            raise ValueError("invalid writer identity")

    def compare(self, other: "Version") -> int:
        """Compare within one kind; callers separately validate mode/epoch."""
        if not isinstance(other, Version) or self.kind != other.kind:
            raise ValueError("cannot compare different version kinds")
        left, right = (self.number, self.writer_id), (other.number, other.writer_id)
        return (left > right) - (left < right)


@dataclass(frozen=True)
class Record:
    version: Version
    value: bytes

    def __post_init__(self):
        from .config import MAX_VALUE_BYTES
        if not isinstance(self.version, Version):
            raise ValueError("record requires a Version")
        if not isinstance(self.value, bytes) or len(self.value) > MAX_VALUE_BYTES:
            raise ValueError("value must be bytes of size 0..1 MiB")


class LocalStatus(str, Enum):
    FOUND = "FOUND"
    NOT_FOUND = "NOT_FOUND"
    INSTALLED = "INSTALLED"
    UNCHANGED = "UNCHANGED"
    SUPERSEDED = "SUPERSEDED"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    STALE_EPOCH = "STALE_EPOCH"
    ERROR = "ERROR"


@dataclass(frozen=True)
class ReadResult:
    status: LocalStatus
    record: Record | None
    context: Context
    error: str | None = None


@dataclass(frozen=True)
class InstallResult:
    status: LocalStatus
    stored_version: Version | None
    durable: bool
    context: Context
    error: str | None = None
