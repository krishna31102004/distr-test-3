"""Bounded lifetime primitives shared by future Eventual PUT attempts (KD-08/12/14)."""

from __future__ import annotations

import math
import threading
import time


class AttemptCancelled(Exception):
    """A lease was explicitly canceled before its current deadline."""


class AttemptLease:
    """One operation's mutable attempt deadline with a fixed hard lifetime.

    The client foreground collector uses ``foreground_deadline``. Network attempts
    use ``current_deadline`` so accepted work can continue briefly after the client
    result without resetting or exceeding the admission-based hard deadline.
    """

    def __init__(self, *, admitted_at: float | None = None,
                 foreground_seconds: float = 5.0,
                 continuation_seconds: float = 10.0,
                 max_lifetime: float = 15.0,
                 clock=time.monotonic):
        for value, label in ((foreground_seconds, "foreground_seconds"),
                             (continuation_seconds, "continuation_seconds"),
                             (max_lifetime, "max_lifetime")):
            if (type(value) not in (int, float) or not math.isfinite(value)
                    or value <= 0):
                raise ValueError(f"{label} must be positive and finite")
        if not callable(clock):
            raise ValueError("clock must be callable")
        now = clock() if admitted_at is None else admitted_at
        if type(now) not in (int, float) or not math.isfinite(now):
            raise ValueError("admitted_at must be a finite monotonic timestamp")
        if foreground_seconds > max_lifetime:
            raise ValueError("foreground deadline cannot exceed hard lifetime")
        self.admitted_at = float(now)
        self.foreground_deadline = self.admitted_at + float(foreground_seconds)
        self.hard_deadline = self.admitted_at + float(max_lifetime)
        self.continuation_seconds = float(continuation_seconds)
        self._clock = clock
        self._lock = threading.Lock()
        self._continuation_deadline: float | None = None
        self._foreground_finished = False
        self._cancelled = False

    def finish_foreground(self, completed_at: float | None = None) -> float:
        """Begin the one permitted continuation window; reject repeated calls."""
        when = self._clock() if completed_at is None else completed_at
        if type(when) not in (int, float) or not math.isfinite(when):
            raise ValueError("completed_at must be a finite monotonic timestamp")
        if when < self.admitted_at:
            raise ValueError("foreground completion precedes admission")
        with self._lock:
            if self._foreground_finished:
                raise RuntimeError("foreground completion may be recorded only once")
            self._foreground_finished = True
            self._continuation_deadline = min(
                float(when) + self.continuation_seconds, self.hard_deadline)
            return self._continuation_deadline

    def current_deadline(self) -> float:
        """Return the current socket/attempt deadline under the lease lock."""
        with self._lock:
            if self._continuation_deadline is None:
                return self.hard_deadline
            return min(self._continuation_deadline, self.hard_deadline)

    def check(self) -> float:
        """Return remaining seconds or raise cancellation/timeout."""
        with self._lock:
            if self._cancelled:
                raise AttemptCancelled("attempt lease canceled")
            deadline = (self.hard_deadline if self._continuation_deadline is None
                        else min(self._continuation_deadline, self.hard_deadline))
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise TimeoutError("attempt lease expired")
        return remaining

    @property
    def cancelled(self) -> bool:
        with self._lock:
            return self._cancelled

    @property
    def foreground_finished(self) -> bool:
        with self._lock:
            return self._foreground_finished

    def cancel(self) -> bool:
        """Cancel accepted attempts; return False if already canceled/expired."""
        with self._lock:
            if self._cancelled or self._clock() >= (
                    self.hard_deadline if self._continuation_deadline is None
                    else min(self._continuation_deadline, self.hard_deadline)):
                return False
            self._cancelled = True
            return True
