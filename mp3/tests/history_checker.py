"""Small backtracking checker for per-key register histories used by Strong tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from mp3.coordinator import OperationStatus, ProtocolResult


@dataclass(frozen=True)
class HistoryEntry:
    operation_id: str
    coordinator_id: str
    key: str
    operation: str
    input_value: bytes | None
    invoked_at: float
    responded_at: float
    result: ProtocolResult


def check_register_history(entries: Iterable[HistoryEntry]) -> bool:
    """Return whether each key admits a real-time-respecting sequential register.

    A non-OK PUT with ``completion_unknown`` is modeled both as omitted and as a
    completed write of its input value. Its result has no version by contract, so
    the checker treats that optional write's version as unknown. Other failed
    operations are no-ops. This is a bounded history checker, not a general proof.
    """
    rows = tuple(entries)
    if len({row.operation_id for row in rows}) != len(rows):
        return False
    by_key: dict[str, list[HistoryEntry]] = {}
    for row in rows:
        if (row.operation not in {"put", "get"}
                or row.invoked_at > row.responded_at
                or row.result.operation_id != row.operation_id):
            return False
        if row.operation == "put" and not isinstance(row.input_value, bytes):
            return False
        by_key.setdefault(row.key, []).append(row)
    return all(_check_one_key(key_rows) for key_rows in by_key.values())


def _check_one_key(rows: list[HistoryEntry]) -> bool:
    size = len(rows)
    predecessors = [set() for _ in rows]
    for left, earlier in enumerate(rows):
        for right, later in enumerate(rows):
            if left != right and earlier.responded_at <= later.invoked_at:
                predecessors[right].add(left)

    # State is absent or (bytes, version). An uncertain write may be omitted or
    # included at one legal point in its invocation/response interval.
    def visit(done: frozenset[int], state: tuple[bytes, object | None] | None) -> bool:
        if len(done) == size:
            return True
        ready = [index for index in range(size)
                 if index not in done and predecessors[index] <= done]
        for index in ready:
            row = rows[index]
            status = row.result.status
            next_done = done | {index}

            if row.operation == "put":
                if status == OperationStatus.OK:
                    if row.result.version is None:
                        continue
                    if state is not None and state[1] is not None:
                        order = row.result.version.compare(state[1])
                        if order < 0 or (order == 0 and row.input_value != state[0]):
                            continue
                    if visit(next_done, (row.input_value, row.result.version)):
                        return True
                elif row.result.completion_unknown:
                    if visit(next_done, state):
                        return True
                    if visit(next_done, (row.input_value, None)):
                        return True
                elif visit(next_done, state):
                    return True
                continue

            if status == OperationStatus.OK:
                if state is None or row.result.value != state[0]:
                    continue
                if state[1] is not None and row.result.version != state[1]:
                    continue
            elif status == OperationStatus.NOT_FOUND:
                if state is not None:
                    continue
            if visit(next_done, state):
                return True
        return False

    return visit(frozenset(), None)
