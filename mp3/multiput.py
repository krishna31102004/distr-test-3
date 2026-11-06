"""`multiput`: puts to one key from several VMs at the same moment (KD-14).

The VM that receives the command coordinates. It sends ARM to every selected VM; each
VM loads its own value (a local file or random bytes) and answers READY. Only when all
are READY does it send GO to all of them concurrently, and each VM then runs an ordinary
put in the cluster's current mode. If any VM is not ready, ABORT goes to all of them and
no put starts on a VM that has not already accepted GO.

Honest limits: GO is concurrent but not atomic, and a missing reply never proves a put
did not start, so each VM is reported as NOT_STARTED (confirmed), STARTED/DONE, PENDING
or UNKNOWN. Deduplication is in memory only (the approved KD-14 choice): a node that
crashes after accepting GO forgets it, nothing is replayed, and the outcome is UNKNOWN.
Timings are local durations; clocks of different VMs are never compared.
"""

from __future__ import annotations

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import hashlib
import json
import os
import threading
import time
import uuid

from .control import ControlError, ControlStatus, send_command
from .models import ConsistencyMode

MULTIPUT_BUDGET_SECONDS = 25.0
ARM_TIMEOUT_SECONDS = 5.0
GO_TIMEOUT_SECONDS = 5.0
ABORT_TIMEOUT_SECONDS = 5.0
POLL_SECONDS = 0.2
ARM_LEASE_SECONDS = 15.0       # receipt-local: GO is accepted only this soon after ARM
PUT_TIMEOUT_SECONDS = 10.0
RANDOM_VALUE_BYTES = 1024
MAX_REMEMBERED = 64
MAX_ARMED = 8                  # live armed values held at once (each may be up to 1 MiB)
MAX_PUT_WORKERS = 4
MAX_VMS = 10
ERROR_CHARS = 60
# What a node reports about one multiput.
NOT_STARTED, ARMED, STARTED, DONE, UNKNOWN, PENDING = (
    "NOT_STARTED", "ARMED", "STARTED", "DONE", "UNKNOWN", "PENDING")


def _multiput_id(payload: dict) -> str:
    value = payload.get("multiput_id")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError, TypeError) as error:
        raise ValueError("multiput_id must be a canonical lowercase UUID4") from error
    if str(parsed) != value or parsed.version != 4:
        raise ValueError("multiput_id must be a canonical lowercase UUID4")
    return value


def _payload(raw: bytes) -> dict:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("payload is not valid JSON") from error
    if not isinstance(data, dict):
        raise ValueError("payload must be a JSON object")
    return data


@dataclass
class _Entry:
    key: str
    state: str
    value: bytes = b""
    nbytes: int = 0
    digest: str = ""
    armed_at: float = 0.0
    started_at: float = 0.0
    result: dict | None = None


class MultiputManager:
    """Participant behavior on every node and the coordinator on the initiating node."""

    def __init__(self, runtime):
        self.runtime = runtime
        self.manifest = runtime.manifest
        self.vm_id = runtime.vm_id
        self._lock = threading.Lock()
        self._entries: OrderedDict[str, _Entry] = OrderedDict()
        self._pool = ThreadPoolExecutor(max_workers=MAX_PUT_WORKERS,
                                        thread_name_prefix=f"mp3-multiput-{self.vm_id}")

    def stop(self) -> None:
        self._pool.shutdown(wait=False, cancel_futures=True)

    # -- participant --------------------------------------------------------

    def participant_message(self, command: str, key: str,
                            raw: bytes) -> tuple[ControlStatus, dict, str | None]:
        try:
            multiput_id = _multiput_id(_payload(raw))
        except ValueError as error:
            return ControlStatus.ERROR, {}, str(error)
        if command == "mp_arm":
            return self._arm(multiput_id, key)
        if command == "mp_go":
            return self._go(multiput_id, key)
        if command == "mp_abort":
            return self._abort(multiput_id, key)
        return self._status(multiput_id)

    def _choose_value(self) -> bytes:
        path = getattr(self.runtime, "multiput_file", None)
        if path is None:
            return os.urandom(RANDOM_VALUE_BYTES)
        from .commands import read_value_file
        return read_value_file(str(path))

    @staticmethod
    def _view(entry: _Entry) -> dict:
        names = {"ABORTED": NOT_STARTED, "EXPIRED": NOT_STARTED, "GO_ACCEPTED": STARTED}
        detail = {"state": names.get(entry.state, entry.state), "bytes": entry.nbytes,
                  "sha256": entry.digest}
        if entry.result is not None:
            detail["result"] = entry.result
        return detail

    def _expire_locked(self) -> None:
        """An ARM whose lease ran out can never start; drop its value so memory is bounded."""
        now = time.monotonic()
        for entry in self._entries.values():
            if entry.state == "ARMED" and now - entry.armed_at > ARM_LEASE_SECONDS:
                entry.state, entry.value = "EXPIRED", b""

    def _remember(self, multiput_id: str, entry: _Entry) -> None:
        self._entries[multiput_id] = entry
        while len(self._entries) > MAX_REMEMBERED:
            # Forget the oldest finished entry first; never one that may still be running.
            for old_id, old in self._entries.items():
                if old.state in ("DONE", "ABORTED", "EXPIRED"):
                    del self._entries[old_id]
                    break
            else:
                break

    def _arm(self, multiput_id: str, key: str):
        with self._lock:
            self._expire_locked()
            existing = self._entries.get(multiput_id)
            if existing is not None:
                if existing.key != key:
                    return ControlStatus.ERROR, {}, "multiput ID was reused for another key"
                return ControlStatus.OK, self._view(existing), None  # idempotent
            if sum(1 for e in self._entries.values() if e.state == "ARMED") >= MAX_ARMED:
                return ControlStatus.ERROR, {"state": NOT_STARTED}, "too many multiputs are armed here"
        try:
            value = self._choose_value()
        except (OSError, ValueError) as error:
            return ControlStatus.ERROR, {"state": NOT_STARTED}, f"no value to put: {error}"[:200]
        entry = _Entry(key, "ARMED", value, len(value),
                       hashlib.sha256(value).hexdigest()[:16], time.monotonic())
        with self._lock:
            if multiput_id in self._entries:  # a concurrent duplicate ARM won
                return ControlStatus.OK, self._view(self._entries[multiput_id]), None
            if sum(1 for e in self._entries.values() if e.state == "ARMED") >= MAX_ARMED:
                return ControlStatus.ERROR, {"state": NOT_STARTED}, "too many multiputs are armed here"
            self._remember(multiput_id, entry)
            return ControlStatus.OK, self._view(entry), None

    def _go(self, multiput_id: str, key: str):
        with self._lock:
            entry = self._entries.get(multiput_id)
            if entry is None:
                return ControlStatus.ERROR, {"state": UNKNOWN}, "unknown multiput; not started"
            if entry.key != key:
                return ControlStatus.ERROR, {}, "multiput ID was reused for another key"
            if entry.state == "ARMED":
                if time.monotonic() - entry.armed_at > ARM_LEASE_SECONDS:
                    entry.state = "EXPIRED"
                    entry.value = b""
                    return ControlStatus.ERROR, self._view(entry), "the ARM lease expired"
                entry.state = "GO_ACCEPTED"
                entry.started_at = time.monotonic()
                try:
                    self._pool.submit(self._run_put, multiput_id)
                except RuntimeError:
                    entry.state = "DONE"
                    entry.result = {"status": "ERROR", "error": "node is stopping"}
                return ControlStatus.OK, self._view(entry), None
            if entry.state in ("ABORTED", "EXPIRED"):
                return ControlStatus.ERROR, self._view(entry), "multiput was aborted; not started"
            return ControlStatus.OK, self._view(entry), None  # duplicate GO: never a second put

    def _abort(self, multiput_id: str, key: str):
        with self._lock:
            entry = self._entries.get(multiput_id)
            if entry is None:
                # A tombstone, so a GO that arrives after this ABORT is refused.
                entry = _Entry(key, "ABORTED")
                self._remember(multiput_id, entry)
            elif entry.state == "ARMED":
                entry.state, entry.value = "ABORTED", b""
            return ControlStatus.OK, self._view(entry), None  # a started put cannot be undone

    def _status(self, multiput_id: str):
        with self._lock:
            self._expire_locked()
            entry = self._entries.get(multiput_id)
            if entry is None:
                return ControlStatus.OK, {"state": UNKNOWN}, None
            return ControlStatus.OK, self._view(entry), None

    def _run_put(self, multiput_id: str) -> None:
        with self._lock:
            entry = self._entries[multiput_id]
            key, value = entry.key, entry.value
        began = time.monotonic()
        result: dict
        try:
            phase = self.runtime.transition_phase()
            if phase != "ACTIVE":
                # The put never ran: report it as not started, with the reason.
                result = {"status": "ERROR", "error": "TRANSITIONING", "put_ms": 0.0}
                with self._lock:
                    entry.result, entry.state, entry.value = result, "ABORTED", b""
                return
            else:
                outcome = self._put(key, value)
                version = outcome.version
                result = {"status": outcome.status.value,
                          "version": (None if version is None else
                                      f"{version.kind}:{version.number}:{version.writer_id}"),
                          "acks": list(outcome.qualifying_owners)}
                if outcome.error:
                    result["error"] = outcome.error[:ERROR_CHARS]
        except Exception as error:
            # The put may have reached replicas before the failure.
            result = {"status": "ERROR",
                      "error": f"{type(error).__name__} (completion unknown)"}
        result["put_ms"] = round((time.monotonic() - began) * 1000, 1)
        with self._lock:
            entry.result, entry.state, entry.value = result, "DONE", b""

    def _put(self, key: str, value: bytes):
        mode = self.runtime.context.mode
        if mode == ConsistencyMode.STRONG:
            return self.runtime.strong_put(key, value,
                                           deadline=time.monotonic() + PUT_TIMEOUT_SECONDS)
        if mode == ConsistencyMode.EVENTUAL_NO_RR:
            return self.runtime.eventual_put(key, value)
        return self.runtime.eventual_rr_put(key, value)

    # -- coordinator --------------------------------------------------------

    def _fanout(self, command: str, key: str, vms: list[str], raw: bytes,
                timeout: float) -> dict:
        """Send `command` to every VM at once: vm -> (ok, detail, error, local ms)."""
        def send(vm_id: str):
            began = time.monotonic()
            try:
                reply = send_command(self.manifest, vm_id, command, key, raw, timeout=timeout)
                detail = json.loads(reply.detail or "{}")
                ok, error = reply.status == ControlStatus.OK, reply.error
            except (ControlError, ValueError) as failure:
                detail, ok, error = {}, False, str(failure)[:ERROR_CHARS]
            return ok, detail, error, round((time.monotonic() - began) * 1000, 1)

        with ThreadPoolExecutor(max_workers=len(vms)) as pool:
            futures = {vm: pool.submit(send, vm) for vm in vms}
            return {vm: future.result() for vm, future in futures.items()}

    @staticmethod
    def _absorb(row: dict, detail: dict) -> None:
        """Copy a node's finished-put result into its report row."""
        result = detail.get("result")
        if isinstance(result, dict):
            row.update({k: result[k] for k in ("status", "version", "acks", "put_ms", "error")
                        if k in result})

    def coordinate(self, key: str, vms: list[str], *, budget: float | None = None
                   ) -> tuple[ControlStatus, dict, str | None]:
        """Run one multiput; returns (status, detail with one row per VM, error text).

        `budget` (seconds) is clamped to the 25-second maximum so a command can never
        outlive the request that carries it."""
        if not 1 <= len(vms) <= MAX_VMS or len(set(vms)) != len(vms):
            return ControlStatus.ERROR, {}, "choose 1 to 10 distinct VMs"
        for vm in vms:
            try:
                self.manifest.node(vm)
            except ValueError:
                return ControlStatus.ERROR, {}, f"{vm!r} is not a VM in the manifest"
        limit = MULTIPUT_BUDGET_SECONDS if budget is None else min(budget, MULTIPUT_BUDGET_SECONDS)
        deadline = time.monotonic() + limit
        multiput_id = str(uuid.uuid4())
        raw = json.dumps({"multiput_id": multiput_id}).encode("utf-8")
        rows = {vm: {"vm": vm, "state": UNKNOWN} for vm in vms}

        def remaining(limit: float) -> float:
            return max(0.2, min(limit, deadline - time.monotonic()))

        arm = self._fanout("mp_arm", key, vms, raw, remaining(ARM_TIMEOUT_SECONDS))
        for vm, (ok, detail, error, _ms) in arm.items():
            rows[vm].update({k: detail[k] for k in ("bytes", "sha256") if k in detail})
            rows[vm]["state"] = ARMED if ok else UNKNOWN
            if not ok:
                rows[vm]["error"] = error
        if any(not arm[vm][0] for vm in vms):
            # Not everyone is ready: cancel everywhere. A VM that confirms the abort is
            # NOT_STARTED; one that does not answer stays UNKNOWN.
            abort = self._fanout("mp_abort", key, vms, raw, remaining(ABORT_TIMEOUT_SECONDS))
            for vm, (ok, detail, error, _ms) in abort.items():
                state = detail.get("state") if ok else None
                rows[vm]["state"] = state if state in (NOT_STARTED, STARTED, DONE) else UNKNOWN
            return (ControlStatus.ERROR, self._detail(multiput_id, key, rows),
                    "not every selected VM was ready; no put was started where aborted")

        go = self._fanout("mp_go", key, vms, raw, remaining(GO_TIMEOUT_SECONDS))
        for vm, (ok, detail, error, ms) in go.items():
            row = rows[vm]
            row["go_ms"] = ms
            state = detail.get("state")
            if ok:
                row["state"] = state or UNKNOWN
                self._absorb(row, detail)
            elif state in (UNKNOWN, NOT_STARTED):
                # A real reply that refused GO (it lost its ARM, or the lease ran out):
                # this VM never started its put.
                row["state"], row["error"] = NOT_STARTED, error
            else:
                row["state"], row["error"] = PENDING, error
        retry = [vm for vm in vms if rows[vm]["state"] == PENDING]
        if retry:  # one idempotent resend: a duplicate GO never starts a second put
            again = self._fanout("mp_go", key, retry, raw, remaining(GO_TIMEOUT_SECONDS))
            for vm, (ok, detail, error, _ms) in again.items():
                state = detail.get("state")
                if ok:
                    rows[vm]["state"] = state or UNKNOWN
                    rows[vm].pop("error", None)
                    self._absorb(rows[vm], detail)
                elif state == NOT_STARTED:
                    rows[vm]["state"] = NOT_STARTED
                elif state == UNKNOWN:  # the first GO may have been accepted before a restart
                    rows[vm]["state"] = UNKNOWN

        while time.monotonic() < deadline:
            waiting = [vm for vm in vms if rows[vm]["state"] in (STARTED, PENDING)]
            if not waiting:
                break
            polled = self._fanout("mp_status", key, waiting, raw, remaining(2.0))
            resend = []
            for vm, (ok, detail, _error, _ms) in polled.items():
                state = detail.get("state") if ok else None
                if state in (STARTED, DONE, NOT_STARTED):
                    rows[vm]["state"] = state
                    self._absorb(rows[vm], detail)
                elif state == UNKNOWN:
                    rows[vm]["state"] = UNKNOWN
                    rows[vm]["error"] = "this VM has no record of the multiput; outcome unknown"
                elif state == ARMED:
                    resend.append(vm)  # our GO never arrived; the lease still allows it
            if resend:
                again = self._fanout("mp_go", key, resend, raw, remaining(GO_TIMEOUT_SECONDS))
                for vm, (ok, detail, _error, _ms) in again.items():
                    if ok:
                        rows[vm]["state"] = detail.get("state") or UNKNOWN
                        self._absorb(rows[vm], detail)
            if not any(rows[vm]["state"] in (STARTED, PENDING) for vm in vms):
                break
            time.sleep(POLL_SECONDS)

        done_ok = all(rows[vm]["state"] == DONE and rows[vm].get("status") == "OK"
                      for vm in vms)
        unresolved = [vm for vm in vms if rows[vm]["state"] in (STARTED, PENDING)]
        unknown = [vm for vm in vms if rows[vm]["state"] == UNKNOWN]
        detail = self._detail(multiput_id, key, rows)
        if done_ok:
            return ControlStatus.OK, detail, None
        if unresolved:
            return (ControlStatus.PENDING, detail,
                    "some puts were started but not confirmed on: " + ", ".join(unresolved))
        if unknown:
            return (ControlStatus.ERROR, detail,
                    "outcome UNKNOWN on: " + ", ".join(unknown)
                    + " (a restarted VM forgets; nothing is replayed)")
        return ControlStatus.ERROR, detail, "at least one put did not succeed"

    @staticmethod
    def _detail(multiput_id: str, key: str, rows: dict) -> dict:
        return {"multiput_id": multiput_id, "nodes": [rows[vm] for vm in rows]}
