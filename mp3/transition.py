"""Cluster-wide consistency-mode transitions: vm01 coordinates ten nodes (KD-16).

Protocol, in order. vm01 allocates an epoch (burned even if the change aborts), writes
PREPARING to its journal, and sends PREPARE to all ten nodes. Each node durably fences
itself (PREPARED) before it answers. Only when all ten answered does vm01 durably record
COMMIT_DECIDED, the point after which the change can never abort. COMMIT makes every
node apply, in one SQLite transaction, the clear-or-retain action and the new
mode/epoch (COMMITTED, still fenced). When all ten committed, vm01 records GLOBAL_ACTIVE
and sends ACTIVATE; a node reopens only after it verifies that decision with vm01. The
command succeeds only after all ten acknowledged ACTIVE.

Crashes and lost replies are handled by the journals, never by guessing: vm01 replays
its unfinished journal at start-up, and a node that restarts fenced asks vm01 for the
durable decision. There is no leader election: if vm01 is down no new change can start
and fenced nodes stay fenced. Local deadlines are monotonic; messages carry remaining
durations, never clock values.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import json
import threading
import time
import uuid
from typing import Callable

from .control import ControlError, ControlStatus, TRANSITION_COMMANDS, send_command
from .models import ConsistencyMode, Context, TransitionInfo
from .storage import (
    JournalEntry, StaleEpochError, StorageError, TransitionRefused,
)

COORDINATOR_ID = "vm01"
TRANSITION_BUDGET_SECONDS = 30.0
STATUS_TIMEOUT_SECONDS = 5.0
FORWARD_TIMEOUT_SECONDS = TRANSITION_BUDGET_SECONDS + 10.0
ABORT_NOTIFY_SECONDS = 5.0
RECOVERY_IDLE_SECONDS = 1.0
RECOVERY_BUSY_SECONDS = 0.5
# A status probe that names no transition: it just reports a node's phase and context.
PROBE_ID = "00000000-0000-4000-8000-000000000000"
_PAYLOAD_KEYS = {"coordinator", "from_epoch", "from_mode", "target_epoch", "target_mode",
                 "budget"}
_ACTIVATING_DECISIONS = ("GLOBAL_ACTIVE", "DONE")
_COMMITTING_DECISIONS = ("COMMIT_DECIDED", "GLOBAL_ACTIVE", "DONE")
# What a node must report back for vm01 to count a message as acknowledged.
_EXPECTED_PHASES = {"tx_prepare": {"PREPARED"}, "tx_commit": {"COMMITTED", "ACTIVE"},
                    "tx_activate": {"ACTIVE"}, "tx_abort": {"ABORTED"}}


class TransitionFault(Exception):
    """Raised by a test hook to stop the driver exactly where a crash would."""


@dataclass
class ModeOutcome:
    status: ControlStatus
    error: str | None = None
    detail: dict = field(default_factory=dict)


@dataclass
class _Run:
    target: ConsistencyMode
    done: threading.Event = field(default_factory=threading.Event)
    outcome: ModeOutcome | None = None


def _entry_detail(entry: JournalEntry, state: str | None = None) -> dict:
    return {"transition_id": entry.transition_id, "state": state or entry.state,
            "from_mode": entry.from_context.mode.value,
            "from_epoch": entry.from_context.epoch,
            "target_mode": entry.target_context.mode.value,
            "target_epoch": entry.target_context.epoch}


class TransitionManager:
    """Participant behavior on every node; coordinator behavior on vm01."""

    def __init__(self, runtime, *, budget: float = TRANSITION_BUDGET_SECONDS):
        self.runtime = runtime
        self.manifest = runtime.manifest
        self.vm_id = runtime.vm_id
        self.budget = float(budget)
        self.fault_hook: Callable[[str], None] | None = None  # tests only
        self._stop = threading.Event()
        self._recovery_paused = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._drive_lock = threading.Lock()
        self._current: _Run | None = None

    @property
    def store(self):
        return self.runtime.store

    def _hook(self, stage: str) -> None:
        if self.fault_hook is not None:
            self.fault_hook(stage)

    # -- participant side ---------------------------------------------------

    def participant_message(self, command: str, transition_id: str,
                            payload: bytes) -> tuple[ControlStatus, dict, str | None]:
        """Run one transition message on this node: (status, detail, refusal text)."""
        if command not in TRANSITION_COMMANDS:
            return ControlStatus.ERROR, {}, "unknown transition command"
        store = self.store
        try:
            if command == "tx_status":
                return ControlStatus.OK, self._status_detail(transition_id), None
            if command == "tx_prepare":
                info = self._parse_prepare(transition_id, payload)
                phase = store.prepare_transition(info, deadline=self._deadline(payload))
            elif command == "tx_commit":
                # Never clear data on an unverified message: vm01 must have decided COMMIT.
                if not self._decision_is(transition_id, _COMMITTING_DECISIONS):
                    return ControlStatus.ERROR, {}, "the COMMIT decision is not verified"
                phase = store.commit_transition(transition_id, deadline=self._deadline(b""))
            elif command == "tx_activate":
                if not self._decision_is(transition_id, _ACTIVATING_DECISIONS):
                    return ControlStatus.ERROR, {}, "the global ACTIVE decision is not verified"
                phase = store.activate_transition(transition_id, deadline=self._deadline(b""))
            else:  # tx_abort
                phase = store.abort_transition(transition_id, deadline=self._deadline(b""))
            return ControlStatus.OK, {"phase": phase}, None
        except (TransitionRefused, StaleEpochError, ValueError) as error:
            return ControlStatus.ERROR, {}, str(error)[:300]
        except StorageError as error:
            return ControlStatus.ERROR, {}, f"storage error: {str(error)[:200]}"

    @staticmethod
    def _deadline(payload: bytes) -> float:
        """Local storage deadline: 10 s, or the budget the coordinator still had if shorter."""
        budget = 10.0
        try:
            sent = json.loads(payload.decode("utf-8")).get("budget") if payload else None
            if type(sent) in (int, float) and 0 < sent < budget:
                budget = max(1.0, float(sent))
        except (UnicodeError, ValueError, AttributeError):
            pass
        return time.monotonic() + budget

    def _parse_prepare(self, transition_id: str, payload: bytes) -> TransitionInfo:
        try:
            data = json.loads(payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as error:
            raise ValueError("transition payload is not valid JSON") from error
        if not isinstance(data, dict) or set(data) != _PAYLOAD_KEYS:
            raise ValueError("transition payload has missing or unexpected fields")
        if data["coordinator"] != COORDINATOR_ID:
            raise ValueError("only vm01 may coordinate a mode change")
        for name in ("from_epoch", "target_epoch"):
            if type(data[name]) is not int or data[name] < 1:
                raise ValueError(f"{name} must be a positive integer")
        info = TransitionInfo(
            transition_id, data["coordinator"],
            Context(data["from_epoch"], ConsistencyMode(data["from_mode"])),
            Context(data["target_epoch"], ConsistencyMode(data["target_mode"])))
        # A node that cannot speak the target mode's wire schema must not agree to it.
        target = info.target_context.mode
        if target == ConsistencyMode.EVENTUAL_RR:
            activation = self.runtime.rr_activation
        else:
            activation = self.runtime.protocol_activation
        if activation is None or not activation.enabled_for(self.manifest):
            raise ValueError(f"{self.vm_id} is not activated for {target.value}")
        return info

    def _status_detail(self, transition_id: str) -> dict:
        store = self.store
        detail = {"local_phase": store.local_phase()}
        if transition_id != PROBE_ID:
            known = store.transition_state(transition_id)
            detail["phase"] = "NONE" if known is None else known.phase
            if self.vm_id == COORDINATOR_ID:
                entry = store.journal_get(transition_id)
                detail["decision"] = "UNKNOWN" if entry is None else entry.state
        return detail

    def _decision_is(self, transition_id: str, states: tuple) -> bool:
        """Verify with vm01's durable journal that this transition has reached `states`."""
        if self.vm_id == COORDINATOR_ID:
            entry = self.store.journal_get(transition_id)
            return entry is not None and entry.state in states
        try:
            reply = send_command(self.manifest, COORDINATOR_ID, "tx_status",
                                 transition_id, timeout=STATUS_TIMEOUT_SECONDS)
            detail = json.loads(reply.detail or "{}")
        except (ControlError, ValueError):
            return False
        return reply.status == ControlStatus.OK and detail.get("decision") in states

    # -- coordinator side (vm01) --------------------------------------------

    def request_mode(self, target: ConsistencyMode, request_id: str) -> ModeOutcome:
        """Change every node to `target`. On non-coordinators this forwards to vm01."""
        if self.vm_id != COORDINATOR_ID:
            try:
                reply = send_command(self.manifest, COORDINATOR_ID, "mode", target.value,
                                     timeout=FORWARD_TIMEOUT_SECONDS, command_id=request_id)
            except ControlError as error:
                return ModeOutcome(ControlStatus.ERROR,
                                   f"cannot reach the transition coordinator vm01: {error}")
            try:
                detail = json.loads(reply.detail or "{}")
            except ValueError:
                detail = {}
            return ModeOutcome(reply.status, reply.error, detail)
        deadline = time.monotonic() + self.budget
        with self._state_lock:
            run = self._current
            if run is not None and not run.done.is_set():
                if run.target != target:
                    return ModeOutcome(ControlStatus.BUSY,
                                       "another mode change is in progress",
                                       {"target_mode": run.target.value})
                joining = run
            else:
                pending = self.store.journal_open()
                if pending is not None:
                    if pending.target_context.mode != target:
                        return ModeOutcome(ControlStatus.BUSY,
                                           "another mode change is being completed",
                                           _entry_detail(pending))
                    joining = None
                    run = None
                else:
                    joining = None
                    run = self._current = _Run(target)
        if joining is not None:
            joining.done.wait(max(0.0, deadline - time.monotonic()))
            return joining.outcome or ModeOutcome(ControlStatus.PENDING,
                                                  "the running mode change has not finished")
        if run is None:
            return self._wait_for_journal(pending.transition_id, deadline)
        outcome = ModeOutcome(ControlStatus.ERROR, "the mode change was interrupted")
        try:
            outcome = self._start_mode_change(target, request_id, deadline)
        except TransitionFault:
            outcome = ModeOutcome(ControlStatus.ERROR, "interrupted by a test fault")
        except Exception as error:
            outcome = ModeOutcome(ControlStatus.ERROR,
                                  f"mode change failed: {type(error).__name__}")
        finally:
            # Even for a BaseException the run is closed, so recovery is never blocked.
            run.outcome = outcome
            run.done.set()
        return outcome

    def _wait_for_journal(self, transition_id: str, deadline: float) -> ModeOutcome:
        while time.monotonic() < deadline:
            entry = self.store.journal_get(transition_id)
            if entry is not None and entry.state in ("DONE", "ABORTED"):
                return self._outcome_for(entry)
            time.sleep(0.2)
        entry = self.store.journal_get(transition_id)
        return ModeOutcome(ControlStatus.PENDING, "the mode change is still completing",
                           {} if entry is None else _entry_detail(entry))

    @staticmethod
    def _outcome_for(entry: JournalEntry) -> ModeOutcome:
        if entry.state == "DONE":
            return ModeOutcome(ControlStatus.OK, None, _entry_detail(entry))
        return ModeOutcome(ControlStatus.ERROR, "the mode change was aborted; nothing changed",
                           _entry_detail(entry))

    def _start_mode_change(self, target: ConsistencyMode, request_id: str,
                           deadline: float) -> ModeOutcome:
        cluster = self._poll_cluster(deadline)
        down = sorted(vm for vm, row in cluster.items() if row is None)
        if down:
            return ModeOutcome(ControlStatus.TIMEOUT,
                               "cannot verify the cluster; unreachable: " + ", ".join(down))
        contexts = {row[1] for row in cluster.values()}
        phases = {row[0] for row in cluster.values()}
        if phases != {"ACTIVE"} or len(contexts) != 1:
            return ModeOutcome(ControlStatus.ERROR,
                               "the cluster is not in one uniform ACTIVE state yet; retry shortly",
                               {"phases": sorted(phases)})
        current = next(iter(contexts))
        if current.mode == target:
            return ModeOutcome(ControlStatus.NO_CHANGE, None,
                               {"mode": target.value, "epoch": current.epoch})
        epoch = self.store.allocate_transition_epoch()  # burned even if this aborts
        transition_id = str(uuid.uuid4())
        self.store.journal_start(transition_id, request_id, current, Context(epoch, target))
        return self._drive(self.store.journal_get(transition_id), deadline)

    def _poll_cluster(self, deadline: float) -> dict:
        results = {}

        def probe(vm_id: str):
            if vm_id == self.vm_id:
                _, detail, _ = self.participant_message("tx_status", PROBE_ID, b"")
                context = self.store.current_context()
                return detail["local_phase"], context
            reply = send_command(self.manifest, vm_id, "tx_status", PROBE_ID,
                                 timeout=min(STATUS_TIMEOUT_SECONDS,
                                             max(0.1, deadline - time.monotonic())))
            if reply.status != ControlStatus.OK:
                return None
            return json.loads(reply.detail)["local_phase"], Context(reply.epoch, reply.mode)

        with ThreadPoolExecutor(max_workers=len(self.manifest.nodes)) as pool:
            futures = {node.vm_id: pool.submit(probe, node.vm_id) for node in self.manifest.nodes}
            for vm_id, future in futures.items():
                try:
                    results[vm_id] = future.result()
                except Exception:
                    results[vm_id] = None
        return results

    def _phase(self, command: str, entry: JournalEntry, deadline: float) -> tuple[bool, dict]:
        """Send one message to all ten nodes; (all acknowledged, per-node failures)."""
        payload = b""
        if command == "tx_prepare":
            payload = json.dumps({
                "coordinator": COORDINATOR_ID,
                "from_epoch": entry.from_context.epoch, "from_mode": entry.from_context.mode.value,
                "target_epoch": entry.target_context.epoch,
                "target_mode": entry.target_context.mode.value,
                "budget": max(0.0, deadline - time.monotonic())},
                sort_keys=True, separators=(",", ":")).encode("utf-8")

        expected = _EXPECTED_PHASES[command]

        def acknowledged(status, detail, error):
            if status != ControlStatus.OK:
                return False, error
            phase = detail.get("phase")
            if phase not in expected:
                return False, f"unexpected phase {phase!r}"
            return True, None

        def send(vm_id: str):
            if vm_id == self.vm_id:
                return acknowledged(*self.participant_message(
                    command, entry.transition_id, payload))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False, "time budget exhausted"
            reply = send_command(self.manifest, vm_id, command, entry.transition_id, payload,
                                 timeout=remaining)
            try:
                detail = json.loads(reply.detail or "{}")
            except ValueError:
                detail = {}
            return acknowledged(reply.status, detail, reply.error)

        failures = {}
        with ThreadPoolExecutor(max_workers=len(self.manifest.nodes)) as pool:
            futures = {node.vm_id: pool.submit(send, node.vm_id) for node in self.manifest.nodes}
            for vm_id, future in futures.items():
                try:
                    ok, error = future.result()
                except ControlError as error_:
                    ok, error = False, f"unreachable: {str(error_)[:120]}"
                except Exception as error_:
                    ok, error = False, type(error_).__name__
                if not ok:
                    failures[vm_id] = error or "refused"
        return not failures, failures

    def _drive(self, entry: JournalEntry, deadline: float) -> ModeOutcome:
        """Advance one journal entry as far as it will go; every step is durable first."""
        store = self.store
        with self._drive_lock:
            while True:
                if self._stop.is_set():
                    return ModeOutcome(ControlStatus.PENDING, "this node is stopping",
                                       _entry_detail(entry))
                entry = store.journal_get(entry.transition_id)
                state = entry.state
                if state in ("DONE", "ABORTED"):
                    return self._outcome_for(entry)
                if state == "PREPARING":
                    self._hook("before_prepare")
                    ok, failures = self._phase("tx_prepare", entry, deadline)
                    self._hook("after_prepare")
                    if ok and time.monotonic() < deadline:
                        store.journal_advance(entry.transition_id, "COMMIT_DECIDED")
                        self._hook("after_commit_decided")
                    else:
                        store.journal_advance(entry.transition_id, "ABORT_DECIDED")
                        reason = failures or {"all": "time budget exhausted"}
                        outcome = self._abort(entry)
                        outcome.error = f"aborted before commit: {reason}"
                        return outcome
                elif state == "ABORT_DECIDED":
                    return self._abort(entry)
                elif state == "COMMIT_DECIDED":
                    ok, failures = self._phase("tx_commit", entry, deadline)
                    self._hook("after_commit")
                    if not ok:
                        return self._pending(entry, "commit", failures)
                    store.journal_advance(entry.transition_id, "GLOBAL_ACTIVE")
                    self._hook("after_global_active")
                elif state == "GLOBAL_ACTIVE":
                    ok, failures = self._phase("tx_activate", entry, deadline)
                    self._hook("after_activate")
                    if not ok:
                        return self._pending(entry, "activation", failures)
                    store.journal_advance(entry.transition_id, "DONE")

    def _abort(self, entry: JournalEntry) -> ModeOutcome:
        """Tell every node to resume the old mode (best effort), then close the journal."""
        self._phase("tx_abort", entry, time.monotonic() + ABORT_NOTIFY_SECONDS)
        self.store.journal_advance(entry.transition_id, "ABORTED")
        return ModeOutcome(ControlStatus.ERROR, "the mode change was aborted; nothing changed",
                           _entry_detail(entry, "ABORTED"))

    @staticmethod
    def _pending(entry: JournalEntry, step: str, failures: dict) -> ModeOutcome:
        return ModeOutcome(
            ControlStatus.PENDING,
            f"the change is decided but {step} is not confirmed on: " + ", ".join(sorted(failures))
            + "; vm01 keeps retrying", _entry_detail(entry))

    # -- recovery -----------------------------------------------------------

    def start_recovery(self) -> None:
        """Start the thread that finishes a decided change after a crash or lost message."""
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._recovery_loop, name=f"mp3-transition-recovery-{self.vm_id}",
            daemon=True)
        self._thread.start()

    def pause_recovery(self) -> None:
        """Stop the background recovery from acting (it keeps running, idle).

        Used to simulate a coordinator or participant that cannot recover yet; requests
        still work. `resume_recovery` lets it act again."""
        self._recovery_paused.set()

    def resume_recovery(self) -> None:
        self._recovery_paused.clear()

    def stop(self, *, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout)

    def _recovery_loop(self) -> None:
        while not self._stop.is_set():
            busy = False
            try:
                if self._recovery_paused.is_set():
                    pass
                elif self.vm_id == COORDINATOR_ID:
                    busy = self._recover_coordinator()
                else:
                    busy = self._recover_participant()
            except Exception:
                busy = True  # try again; recovery must never kill the node
            self._stop.wait(RECOVERY_BUSY_SECONDS if busy else RECOVERY_IDLE_SECONDS)

    def _recover_coordinator(self) -> bool:
        entry = self.store.journal_open()
        run = self._current
        driving = (run is not None and not run.done.is_set()) or self._drive_lock.locked()
        if entry is None:
            # vm01 is a participant too: a failed local abort or commit is retried here.
            return False if driving else self._recover_own_row()
        if driving:
            return True  # a live request is already driving it
        if entry.state == "PREPARING":
            # No COMMIT was ever decided, so the only safe resolution is to abort.
            self.store.journal_advance(entry.transition_id, "ABORT_DECIDED")
        self._drive(entry, time.monotonic() + self.budget)
        return True

    def _recover_own_row(self) -> bool:
        """vm01 settles its own fenced row from its own journal."""
        info = self.store.open_transition()
        if info is None:
            return False
        entry = self.store.journal_get(info.transition_id)
        self._apply_decision(info, "UNKNOWN" if entry is None else entry.state)
        return True

    def _recover_participant(self) -> bool:
        info = self.store.open_transition()
        if info is None:
            return False
        try:
            reply = send_command(self.manifest, COORDINATOR_ID, "tx_status",
                                 info.transition_id, timeout=STATUS_TIMEOUT_SECONDS)
            decision = json.loads(reply.detail or "{}").get("decision")
        except (ControlError, ValueError):
            return True  # vm01 unreachable: stay fenced and retry; never guess
        self._apply_decision(info, decision)
        return True

    def _apply_decision(self, info: TransitionInfo, decision) -> None:
        """Apply vm01's durable decision to this node's open transition row."""
        try:
            if info.phase == "PREPARED" and decision in (
                    "ABORT_DECIDED", "ABORTED", "UNKNOWN"):
                # UNKNOWN is safe to abort: vm01 journals PREPARING before it sends any
                # PREPARE, so an ID it has never heard of can never have been committed.
                self.store.abort_transition(info.transition_id)
            elif decision in _COMMITTING_DECISIONS:
                if info.phase == "PREPARED":
                    self.store.commit_transition(info.transition_id)
                if decision in _ACTIVATING_DECISIONS:
                    self.store.activate_transition(info.transition_id)
        except (TransitionRefused, StorageError):
            pass
