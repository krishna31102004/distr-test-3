from dataclasses import dataclass
from enum import Enum


class ConsistencyMode(str, Enum):
    STRONG = "strong"
    EVENTUAL_NO_RR = "eventual-noRR"
    EVENTUAL_RR = "eventual-RR"


@dataclass(frozen=True)
class Command:
    name: str
    args: tuple[str, ...]


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
    TRANSITIONING = "TRANSITIONING"
    ERROR = "ERROR"


def mode_change_clears_data(source: "ConsistencyMode", target: "ConsistencyMode") -> bool:
    return (source == ConsistencyMode.STRONG) != (target == ConsistencyMode.STRONG)


@dataclass(frozen=True)
class TransitionInfo:
    transition_id: str
    coordinator: str
    from_context: "Context"
    target_context: "Context"
    phase: str = "PREPARED"

    def __post_init__(self) -> None:
        import re
        if not isinstance(self.transition_id, str) or not re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                self.transition_id):
            raise ValueError("transition_id must be a canonical lowercase UUID4")
        if self.coordinator != "vm01":
            raise ValueError("vm01 is the only transition coordinator")
        if not isinstance(self.from_context, Context) or not isinstance(
                self.target_context, Context):
            raise ValueError("transition contexts must be Context values")
        if self.target_context.epoch <= self.from_context.epoch:
            raise ValueError("the target epoch must be above the current epoch")
        if self.target_context.mode == self.from_context.mode:
            raise ValueError("a transition must change the mode")
        if self.phase not in ("PREPARED", "COMMITTED", "ACTIVE", "ABORTED"):
            raise ValueError("invalid transition phase")


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
