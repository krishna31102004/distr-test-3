"""Version-stable deterministic benchmark workload generation."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib

VALUE_BYTES = 1024
DEFAULT_KEY_COUNT = 100_000
HOT_KEY_COUNT = 10


def _u64(seed: int, stream: str, index: int, retry: int = 0) -> int:
    payload = f"mp3-bench-v1\0{seed}\0{stream}\0{index}\0{retry}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def _below(seed: int, stream: str, index: int, limit: int) -> int:
    """Unbiased deterministic integer in [0, limit), independent of Python random."""
    if type(limit) is not int or limit < 1:
        raise ValueError("limit must be a positive integer")
    ceiling = (1 << 64) - ((1 << 64) % limit)
    retry = 0
    while True:
        value = _u64(seed, stream, index, retry)
        if value < ceiling:
            return value % limit
        retry += 1


@dataclass(frozen=True)
class Workload:
    seed: int
    key_count: int = DEFAULT_KEY_COUNT
    distribution: str = "uniform"
    hot_key_count: int = HOT_KEY_COUNT
    hot_percent: int = 99

    def __post_init__(self) -> None:
        if type(self.seed) is not int or self.seed < 0:
            raise ValueError("seed must be a nonnegative integer")
        if type(self.key_count) is not int or self.key_count < 1:
            raise ValueError("key_count must be positive")
        if self.distribution not in {"uniform", "hot99"}:
            raise ValueError("distribution must be uniform or hot99")
        if (self.distribution == "hot99"
                and (type(self.hot_key_count) is not int
                     or not 1 <= self.hot_key_count < self.key_count
                     or type(self.hot_percent) is not int or self.hot_percent != 99)):
            raise ValueError("hot99 requires 99 percent hot keys and a smaller hot set")

    def operation(self, client_id: str, operation_index: int) -> tuple[str, str]:
        """Return an alternating GET/PUT and deterministic key for one client slot."""
        if not isinstance(client_id, str) or not client_id:
            raise ValueError("client_id is required")
        if type(operation_index) is not int or operation_index < 0:
            raise ValueError("operation_index must be nonnegative")
        command = "get" if operation_index % 2 == 0 else "put"
        if self.distribution == "uniform":
            key_index = _below(self.seed, f"uniform:{client_id}", operation_index,
                               self.key_count)
        else:
            command_ordinal = operation_index // 2
            if command_ordinal % 100 < self.hot_percent:
                key_index = _below(self.seed, f"hot:{client_id}:{command}",
                                   command_ordinal, self.hot_key_count)
            else:
                cold_count = self.key_count - self.hot_key_count
                key_index = self.hot_key_count + _below(
                    self.seed, f"cold:{client_id}:{command}", command_ordinal, cold_count)
        return command, f"bench-key-{key_index:06d}"


def unique_value(run_id: str, client_id: str, operation_index: int,
                 *, size: int = VALUE_BYTES) -> bytes:
    """Create an opaque fixed-size PUT value with an inspectable unique prefix."""
    if type(size) is not int or size < 0:
        raise ValueError("size must be nonnegative")
    prefix = f"mp3bench:{run_id}:{client_id}:{operation_index}:".encode("utf-8")
    if len(prefix) > size:
        raise ValueError("run/client identifiers do not fit in the requested value size")
    output = bytearray(prefix)
    block = 0
    while len(output) < size:
        output.extend(hashlib.sha256(prefix + block.to_bytes(8, "big")).digest())
        block += 1
    return bytes(output[:size])


def preload_key(index: int) -> str:
    if type(index) is not int or index < 0:
        raise ValueError("index must be nonnegative")
    return f"bench-key-{index:06d}"


def preload_value(run_id: str, index: int, *, size: int = VALUE_BYTES) -> bytes:
    return unique_value(run_id, "preload", index, size=size)
