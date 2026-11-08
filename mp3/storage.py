"""Local SQLite store: values with versions, version allocation, and mode-transition state.

One VM's durable data (WAL, synchronous=FULL). It does no replication or networking; the
coordinators and the replica service build on it. Each operation owns a connection and a
bounded admission slot. The process lock protects the data path's lifetime, not
distributed GET/PUT operations.
"""
from contextlib import contextmanager
import fcntl
import math
from pathlib import Path
import re
import sqlite3
import threading
import time

from dataclasses import dataclass

from .models import (ConsistencyMode, Context, Version, Record, ReadResult,
                     InstallResult, LocalStatus as S, TransitionInfo,
                     mode_change_clears_data)


class StorageError(RuntimeError):
    """Storage could not establish or preserve the promised local guarantees."""


class StaleEpochError(StorageError):
    def __init__(self, context):
        super().__init__("context does not match persisted epoch/mode")
        self.context = context


class TransitioningError(StorageError):
    """The node is between PREPARED and ACTIVE; normal storage is fenced (KD-16)."""

    def __init__(self, context, phase):
        super().__init__("node is transitioning between consistency modes")
        self.context = context
        self.phase = phase


class TransitionRefused(StorageError):
    """A transition request does not fit this node's durable state."""


@dataclass(frozen=True)
class JournalEntry:
    """vm01's durable record of one cluster transition it is driving."""

    transition_id: str
    request_id: str
    from_context: Context
    target_context: Context
    state: str


JOURNAL_OPEN_STATES = ("PREPARING", "COMMIT_DECIDED", "GLOBAL_ACTIVE")
# COMMIT_DECIDED can never move to an abort state: that is the irrevocability point.
JOURNAL_MOVES = {
    "PREPARING": {"COMMIT_DECIDED", "ABORT_DECIDED"},
    "COMMIT_DECIDED": {"GLOBAL_ACTIVE"},
    "GLOBAL_ACTIVE": {"DONE"},
    "ABORT_DECIDED": {"ABORTED"},
}
TRANSITION_HISTORY = 64
_TRANSITION_COLUMNS = ("transition_id,coordinator,from_epoch,from_mode,"
                       "target_epoch,target_mode,phase")
_JOURNAL_COLUMNS = "transition_id,request_id,from_epoch,from_mode,target_epoch,target_mode,state"


def _natural(text):
    """Decode canonical decimal metadata; never silently coerce corrupt state."""
    if not isinstance(text, str) or not re.fullmatch(r"0|[1-9][0-9]*", text):
        raise StorageError("invalid canonical decimal metadata")
    result = 0
    for start in range(0, len(text), 9):
        chunk = text[start:start + 9]
        result = result * (10 ** len(chunk)) + int(chunk)
    return result


def _decimal(number):
    """Encode nonnegative integers without changing Python's global digit limits."""
    chunks = []
    while number >= 10**9:
        number, remainder = divmod(number, 10**9)
        chunks.append(f"{remainder:09d}")
    return str(number) + "".join(reversed(chunks))


def _key(key):
    if not isinstance(key, str):
        raise ValueError("key must be a string")
    key.encode("utf-8", errors="strict")


def _kind(context):
    return "strong" if context.mode == ConsistencyMode.STRONG else "eventual"


class Store:
    def __init__(self, path, *, bootstrap=None, vm_id="vm01", timeout=5.0, capacity=8):
        """Open/recover explicitly; raise StorageError on invalid or locked state.

        vm_id is persisted on bootstrap and checked on reopen. No existing file
        is overwritten or initialized implicitly. Parent directory must exist.
        """
        if not re.fullmatch(r"vm(?:0[1-9]|10)", vm_id):
            raise ValueError("invalid VM identity")
        try:
            valid_timeout = type(timeout) in (int, float) and math.isfinite(timeout) and timeout > 0
        except OverflowError:
            valid_timeout = False
        if not valid_timeout or type(capacity) is not int or capacity < 1:
            raise ValueError("positive finite timeout and capacity required")
        if bootstrap is not None and (not isinstance(bootstrap, Context) or bootstrap.epoch != 1):
            raise ValueError("bootstrap requires explicit epoch 1 Context")
        self.path = Path(path).resolve()
        self.vm_id, self.timeout, self.capacity = vm_id, timeout, capacity
        self._condition = threading.Condition()
        self._active = 0
        self._closed = False
        self._lockfile = None
        self._context = bootstrap
        try:
            self._lockfile = open(str(self.path) + ".lock", "a+b")
            fcntl.flock(self._lockfile, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if bootstrap is not None:
                # Exclusive creation also rejects empty/corrupt pre-existing files.
                with self.path.open("xb"):
                    pass
            elif not self.path.is_file():
                raise StorageError("existing database required; bootstrap explicitly")
            # Recovery validates every stored record. A populated benchmark DB
            # can take longer than the 5-second per-operation deadline.
            with self._transaction(write=True, startup=True) as db:
                if bootstrap is not None:
                    db.execute("CREATE TABLE meta (name TEXT PRIMARY KEY, value TEXT NOT NULL)")
                    db.execute("CREATE TABLE records (key TEXT PRIMARY KEY, kind TEXT NOT NULL, number TEXT NOT NULL, writer TEXT NOT NULL, value BLOB NOT NULL)")
                    db.executemany("INSERT INTO meta VALUES (?,?)", [
                        ("schema", "2"), ("vm_id", vm_id), ("epoch", "1"),
                        ("mode", bootstrap.mode.value), ("generation", "0"),
                        ("last_timestamp", "0"), ("strong_counter", "0")])
                if db.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise StorageError("database integrity check failed")
                self._ensure_transition_tables(db)  # additive: old databases upgrade in place
                metadata = dict(db.execute("SELECT name,value FROM meta"))
                schema1 = {"schema", "vm_id", "epoch", "mode", "generation", "last_timestamp"}
                schema2 = schema1 | {"strong_counter"}
                if metadata.get("vm_id") != vm_id:
                    raise StorageError("invalid schema/identity metadata")
                if metadata.get("schema") == "1" and set(metadata) == schema1:
                    # Validate all old state before changing metadata. SQLite makes
                    # the data-preserving schema bump and Strong high-water insert
                    # one atomic transaction; a killed migration rolls back wholly.
                    self._context = self._read_context(db)
                    _natural(metadata["generation"])
                    _natural(metadata["last_timestamp"])
                    strong_high_water = 0
                    for key, *row in db.execute("SELECT key,kind,number,writer,value FROM records"):
                        _key(key)
                        record = self._decode(row)
                        if record.version.kind != _kind(self._context):
                            raise StorageError("stored version kind disagrees with mode")
                        if record.version.kind == "strong":
                            strong_high_water = max(strong_high_water, record.version.number)
                    db.execute("INSERT INTO meta(name,value) VALUES ('strong_counter',?)",
                               (_decimal(strong_high_water),))
                    db.execute("UPDATE meta SET value='2' WHERE name='schema'")
                    metadata = dict(db.execute("SELECT name,value FROM meta"))
                if set(metadata) != schema2 or metadata.get("schema") != "2":
                    raise StorageError("invalid schema/identity metadata")
                self._context = self._read_context(db)
                _natural(metadata["generation"])
                _natural(metadata["last_timestamp"])
                strong_high_water = _natural(metadata["strong_counter"])
                record_high_water = 0
                for key, *row in db.execute("SELECT key,kind,number,writer,value FROM records"):
                    _key(key)
                    record = self._decode(row)
                    if record.version.kind != _kind(self._context):
                        raise StorageError("stored version kind disagrees with mode")
                    if record.version.kind == "strong":
                        record_high_water = max(record_high_water, record.version.number)
                if strong_high_water < record_high_water:
                    raise StorageError("Strong counter metadata is below a stored Strong version")
        except Exception as error:
            if self._lockfile is not None:
                self._lockfile.close()
            self._closed = True
            raise StorageError(f"cannot open store: {error}") from error

    @contextmanager
    def _transaction(self, *, write=False, deadline=None, startup=False):
        """Bound admission and SQLite waits; rollback any unsuccessful operation.

        Startup performs full-database integrity and record validation, so give
        that one-time scan a longer deadline without relaxing GET/PUT deadlines.
        """
        local_deadline = time.monotonic() + (
            max(self.timeout, 120.0) if startup else self.timeout)
        if deadline is not None:
            try:
                valid_deadline = (type(deadline) in (int, float)
                                  and math.isfinite(deadline))
                if not valid_deadline:
                    raise ValueError("deadline must be a finite monotonic timestamp")
                local_deadline = min(local_deadline, deadline)
            except OverflowError as error:
                raise ValueError("deadline is outside the supported numeric range") from error
        deadline = local_deadline
        with self._condition:
            while self._active >= self.capacity and not self._closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise StorageError("storage admission timed out")
                self._condition.wait(remaining)
            if self._closed:
                raise StorageError("store closed")
            self._active += 1
        db = None
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise StorageError("storage deadline expired")
            db = sqlite3.connect(self.path.as_uri() + "?mode=rw", uri=True,
                                 timeout=min(remaining, self.timeout), isolation_level=None)
            db.execute(f"PRAGMA busy_timeout={max(1, int(min(remaining, self.timeout) * 1000))}")
            if db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise StorageError("WAL unavailable")
            db.execute("PRAGMA synchronous=FULL")
            if db.execute("PRAGMA synchronous").fetchone()[0] != 2:
                raise StorageError("FULL unavailable")
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 100)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise StorageError("storage deadline expired")
            db.execute(f"PRAGMA busy_timeout={max(1, int(min(remaining, self.timeout) * 1000))}")
            db.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield db
            if time.monotonic() >= deadline:
                raise StorageError("storage deadline expired before commit")
            db.execute("COMMIT")
        finally:
            if db is not None:
                db.close()  # Rolls back any still-open transaction, including exceptions.
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    @staticmethod
    def _read_context(db):
        data = dict(db.execute("SELECT name,value FROM meta WHERE name IN ('epoch','mode')"))
        return Context(_natural(data["epoch"]), ConsistencyMode(data["mode"]))

    @staticmethod
    def _decode(row):
        return Record(Version(row[0], _natural(row[1]), row[2]), row[3])

    def _check_context(self, db, context):
        actual = self._read_context(db)
        if not isinstance(context, Context):
            raise ValueError("context must be a Context")
        # The fence and the context check share this transaction, so a request can
        # never slip between a transition's PREPARED record and its commit.
        open_transition = self._open_transition(db)
        if open_transition is not None:
            raise TransitioningError(actual, open_transition[6])
        if actual != context:
            raise StaleEpochError(actual)
        return actual

    def current_context(self, *, deadline=None):
        with self._transaction(deadline=deadline) as db:
            return self._read_context(db)

    def read_local(self, key, *, context, deadline=None):
        try:
            _key(key)
            with self._transaction(deadline=deadline) as db:
                actual = self._check_context(db, context)
                row = db.execute("SELECT kind,number,writer,value FROM records WHERE key=?", (key,)).fetchone()
                record = None if row is None else self._decode(row)
                if record is not None and record.version.kind != _kind(actual):
                    raise StorageError("stored version kind disagrees with mode")
            return ReadResult(S.NOT_FOUND if record is None else S.FOUND, record, actual)
        except StaleEpochError as error:
            return ReadResult(S.STALE_EPOCH, None, error.context, str(error))
        except TransitioningError as error:
            return ReadResult(S.TRANSITIONING, None, error.context, "TRANSITIONING")
        except (sqlite3.Error, StorageError, ValueError, TypeError, KeyError) as error:
            return ReadResult(S.ERROR, None, self._context, str(error))

    def install_if_newer(self, key, record, *, context, deadline=None):
        try:
            _key(key)
            if not isinstance(record, Record):
                raise ValueError("record required")
            with self._transaction(write=True, deadline=deadline) as db:
                actual = self._check_context(db, context)
                if record.version.kind != _kind(actual):
                    raise ValueError("version kind disagrees with mode")
                row = db.execute("SELECT kind,number,writer,value FROM records WHERE key=?", (key,)).fetchone()
                previous = None if row is None else self._decode(row)
                order = 1 if previous is None else record.version.compare(previous.version)
                status = S.INSTALLED
                if order > 0:
                    db.execute("INSERT OR REPLACE INTO records VALUES (?,?,?,?,?)", (key, record.version.kind,
                               _decimal(record.version.number), record.version.writer_id, record.value))
                    stored = record.version
                else:
                    stored = previous.version
                    status = S.SUPERSEDED if order < 0 else (S.UNCHANGED if previous.value == record.value else S.VERSION_CONFLICT)
                if record.version.kind == "strong":
                    high_water = _natural(db.execute(
                        "SELECT value FROM meta WHERE name='strong_counter'").fetchone()[0])
                    committed_number = max(record.version.number,
                                           0 if previous is None else previous.version.number)
                    if committed_number > high_water:
                        db.execute("UPDATE meta SET value=? WHERE name='strong_counter'",
                                   (_decimal(committed_number),))
            return InstallResult(status, stored, status != S.VERSION_CONFLICT, actual)
        except StaleEpochError as error:
            return InstallResult(S.STALE_EPOCH, None, False, error.context, str(error))
        except TransitioningError as error:
            return InstallResult(S.TRANSITIONING, None, False, error.context, "TRANSITIONING")
        except (sqlite3.Error, StorageError, ValueError, TypeError, KeyError) as error:
            return InstallResult(S.ERROR, None, False, self._context, str(error))

    def list_keys(self, *, after=None, max_keys=2000, max_bytes=128 * 1024, deadline=None):
        """One bounded, ordered page of stored key names for `liststore`.

        Returns (keys, next_cursor): `next_cursor` is the last key of this page when more
        remain, else None; pass it back as `after`. Keys sort bytewise by UTF-8. The page
        is one short read transaction, so printing never holds the database. A node that
        is fenced by a mode change refuses (TransitioningError) instead of listing data
        that is about to be cleared."""
        if after is not None:
            _key(after)
        if type(max_keys) is not int or max_keys < 1 or type(max_bytes) is not int or max_bytes < 1:
            raise ValueError("page limits must be positive integers")
        with self._transaction(deadline=deadline) as db:
            open_transition = self._open_transition(db)
            if open_transition is not None:
                raise TransitioningError(self._read_context(db), open_transition[6])
            rows = db.execute(
                "SELECT key FROM records WHERE (?1 IS NULL OR key > ?1) ORDER BY key LIMIT ?2",
                (after, max_keys + 1)).fetchall()
        keys, size = [], 0
        for (key,) in rows:
            weight = len(key.encode("utf-8")) + 8
            if len(keys) >= max_keys or (keys and size + weight > max_bytes):
                return keys, keys[-1]
            keys.append(key)
            size += weight
        return keys, None

    def start_incarnation(self):
        """Reserve a durable generation; caller invokes once before serving a boot."""
        try:
            with self._transaction(write=True) as db:
                generation = _natural(db.execute("SELECT value FROM meta WHERE name='generation'").fetchone()[0]) + 1
                db.execute("UPDATE meta SET value=? WHERE name='generation'", (_decimal(generation),))
            return generation
        except (sqlite3.Error, ValueError, TypeError) as error:
            raise StorageError(str(error)) from error

    def allocate_eventual_version(self, *, context):
        """Reserve timestamp before publishing; epoch validation shares transaction."""
        try:
            with self._transaction(write=True) as db:
                actual = self._check_context(db, context)
                if _kind(actual) != "eventual":
                    raise StorageError("eventual allocation requires an Eventual mode")
                generation = _natural(db.execute("SELECT value FROM meta WHERE name='generation'").fetchone()[0])
                if generation == 0:
                    raise StorageError("start_incarnation required before allocation")
                last = _natural(db.execute("SELECT value FROM meta WHERE name='last_timestamp'").fetchone()[0])
                number = max(time.time_ns(), last + 1)
                db.execute("UPDATE meta SET value=? WHERE name='last_timestamp'", (_decimal(number),))
            return Version("eventual", number, f"{self.vm_id}/{_decimal(generation)}")
        except (sqlite3.Error, ValueError, TypeError) as error:
            raise StorageError(str(error)) from error

    def allocate_strong_version(self, *, context, observed_max, deadline=None):
        """Durably allocate a unique local Strong tag above query/local maxima.

        The short write transaction serializes simultaneous local allocations. It
        does not span network I/O. The committed counter survives process crashes.
        """
        try:
            if observed_max is not None and (
                    not isinstance(observed_max, Version) or observed_max.kind != "strong"):
                raise ValueError("observed_max must be a Strong Version or None")
            with self._transaction(write=True, deadline=deadline) as db:
                actual = self._check_context(db, context)
                if actual.mode != ConsistencyMode.STRONG:
                    raise StorageError("Strong allocation requires Strong context")
                generation = _natural(db.execute(
                    "SELECT value FROM meta WHERE name='generation'").fetchone()[0])
                if generation == 0:
                    raise StorageError("start_incarnation required before allocation")
                local_high_water = _natural(db.execute(
                    "SELECT value FROM meta WHERE name='strong_counter'").fetchone()[0])
                observed = 0 if observed_max is None else observed_max.number
                number = max(local_high_water, observed) + 1
                db.execute("UPDATE meta SET value=? WHERE name='strong_counter'",
                           (_decimal(number),))
            return Version("strong", number, f"{self.vm_id}/{_decimal(generation)}")
        except StaleEpochError:
            raise
        except (sqlite3.Error, StorageError, ValueError, TypeError) as error:
            raise StorageError(str(error)) from error

    # -- cluster mode transitions (KD-16) ------------------------------------

    @staticmethod
    def _ensure_transition_tables(db):
        db.execute("CREATE TABLE IF NOT EXISTS transition (seq INTEGER PRIMARY KEY AUTOINCREMENT,"
                   " transition_id TEXT NOT NULL UNIQUE, coordinator TEXT NOT NULL,"
                   " from_epoch TEXT NOT NULL, from_mode TEXT NOT NULL,"
                   " target_epoch TEXT NOT NULL, target_mode TEXT NOT NULL, phase TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS authority (name TEXT PRIMARY KEY, value TEXT NOT NULL)")
        db.execute("CREATE TABLE IF NOT EXISTS controller_journal ("
                   " seq INTEGER PRIMARY KEY AUTOINCREMENT, transition_id TEXT NOT NULL UNIQUE,"
                   " request_id TEXT NOT NULL, from_epoch TEXT NOT NULL, from_mode TEXT NOT NULL,"
                   " target_epoch TEXT NOT NULL, target_mode TEXT NOT NULL, state TEXT NOT NULL)")

    @staticmethod
    def _open_transition(db):
        return db.execute(
            f"SELECT {_TRANSITION_COLUMNS} FROM transition "
            "WHERE phase IN ('PREPARED','COMMITTED') ORDER BY seq DESC LIMIT 1").fetchone()

    @staticmethod
    def _transition_info(row):
        return TransitionInfo(
            row[0], row[1],
            Context(_natural(row[2]), ConsistencyMode(row[3])),
            Context(_natural(row[4]), ConsistencyMode(row[5])), row[6])

    def local_phase(self, *, deadline=None):
        """ACTIVE when no transition is open here, else PREPARED or COMMITTED."""
        with self._transaction(deadline=deadline) as db:
            row = self._open_transition(db)
            return "ACTIVE" if row is None else row[6]

    def open_transition(self, *, deadline=None):
        """The transition currently fencing this node (PREPARED or COMMITTED), or None."""
        with self._transaction(deadline=deadline) as db:
            row = self._open_transition(db)
            return None if row is None else self._transition_info(row)

    def transition_state(self, transition_id=None, *, deadline=None):
        """Return the named transition (or the latest one) as TransitionInfo, or None."""
        with self._transaction(deadline=deadline) as db:
            if transition_id is None:
                row = db.execute(f"SELECT {_TRANSITION_COLUMNS} FROM transition "
                                 "ORDER BY seq DESC LIMIT 1").fetchone()
            else:
                row = db.execute(f"SELECT {_TRANSITION_COLUMNS} FROM transition "
                                 "WHERE transition_id=?", (transition_id,)).fetchone()
            return None if row is None else self._transition_info(row)

    def _trim_transitions(self, db):
        db.execute("DELETE FROM transition WHERE phase IN ('ACTIVE','ABORTED') AND seq <= "
                   "(SELECT COALESCE(MAX(seq),0) FROM transition) - ?", (TRANSITION_HISTORY,))

    def prepare_transition(self, info, *, deadline=None):
        """Durably fence this node for `info`; idempotent for the same transition."""
        if not isinstance(info, TransitionInfo):
            raise ValueError("TransitionInfo required")
        with self._transaction(write=True, deadline=deadline) as db:
            row = db.execute(f"SELECT {_TRANSITION_COLUMNS} FROM transition WHERE transition_id=?",
                             (info.transition_id,)).fetchone()
            if row is not None:
                known = self._transition_info(row)
                if known.phase == "ABORTED":
                    raise TransitionRefused("transition was aborted")
                if (known.from_context, known.target_context, known.coordinator) != (
                        info.from_context, info.target_context, info.coordinator):
                    raise TransitionRefused("transition ID was reused with a different payload")
                return known.phase
            actual = self._read_context(db)
            if actual != info.from_context:
                raise TransitionRefused(
                    f"node is at epoch {actual.epoch} {actual.mode.value}, not the expected "
                    f"epoch {info.from_context.epoch} {info.from_context.mode.value}")
            if self._open_transition(db) is not None:
                raise TransitionRefused("another transition is already in progress here")
            db.execute("INSERT INTO transition(transition_id,coordinator,from_epoch,from_mode,"
                       "target_epoch,target_mode,phase) VALUES (?,?,?,?,?,?,'PREPARED')",
                       (info.transition_id, info.coordinator,
                        _decimal(info.from_context.epoch), info.from_context.mode.value,
                        _decimal(info.target_context.epoch), info.target_context.mode.value))
            self._trim_transitions(db)
        return "PREPARED"

    def commit_transition(self, transition_id, *, deadline=None):
        """Apply the clear-or-retain action and the new context in ONE transaction."""
        with self._transaction(write=True, deadline=deadline) as db:
            row = db.execute(f"SELECT {_TRANSITION_COLUMNS} FROM transition WHERE transition_id=?",
                             (transition_id,)).fetchone()
            if row is None:
                raise TransitionRefused("unknown transition")
            info = self._transition_info(row)
            if info.phase in ("COMMITTED", "ACTIVE"):
                return info.phase
            if info.phase == "ABORTED":
                raise TransitionRefused("transition was aborted")
            if self._read_context(db) != info.from_context:
                raise TransitionRefused("context no longer matches the prepared transition")
            if mode_change_clears_data(info.from_context.mode, info.target_context.mode):
                db.execute("DELETE FROM records")
                db.execute("UPDATE meta SET value='0' "
                           "WHERE name IN ('strong_counter','last_timestamp')")
            db.execute("UPDATE meta SET value=? WHERE name='epoch'",
                       (_decimal(info.target_context.epoch),))
            db.execute("UPDATE meta SET value=? WHERE name='mode'",
                       (info.target_context.mode.value,))
            db.execute("UPDATE transition SET phase='COMMITTED' WHERE transition_id=?",
                       (transition_id,))
        self._context = info.target_context
        return "COMMITTED"

    def activate_transition(self, transition_id, *, deadline=None):
        """Reopen normal storage once the matching global decision has been verified."""
        with self._transaction(write=True, deadline=deadline) as db:
            row = db.execute(f"SELECT {_TRANSITION_COLUMNS} FROM transition WHERE transition_id=?",
                             (transition_id,)).fetchone()
            if row is None:
                raise TransitionRefused("unknown transition")
            phase = row[6]
            if phase == "ACTIVE":
                return phase
            if phase != "COMMITTED":
                raise TransitionRefused(f"cannot activate a transition that is {phase}")
            db.execute("UPDATE transition SET phase='ACTIVE' WHERE transition_id=?",
                       (transition_id,))
            self._trim_transitions(db)
        return "ACTIVE"

    def abort_transition(self, transition_id, *, deadline=None):
        """Resume the old mode; only a PREPARED (or unknown) transition can abort."""
        with self._transaction(write=True, deadline=deadline) as db:
            row = db.execute(f"SELECT {_TRANSITION_COLUMNS} FROM transition WHERE transition_id=?",
                             (transition_id,)).fetchone()
            if row is None:
                if self._open_transition(db) is not None:
                    # A stray abort must not shadow the transition that fences this node.
                    raise TransitionRefused("another transition is open here; "
                                            "cannot record an unrelated abort")
                # A tombstone, so a delayed PREPARE for this ID is refused later.
                actual = self._read_context(db)
                other = (ConsistencyMode.EVENTUAL_NO_RR if actual.mode == ConsistencyMode.STRONG
                         else ConsistencyMode.STRONG)
                db.execute("INSERT INTO transition(transition_id,coordinator,from_epoch,from_mode,"
                           "target_epoch,target_mode,phase) VALUES (?,?,?,?,?,?,'ABORTED')",
                           (transition_id, "vm01", _decimal(actual.epoch), actual.mode.value,
                            _decimal(actual.epoch + 1), other.value))
                self._trim_transitions(db)
                return "ABORTED"
            phase = row[6]
            if phase == "ABORTED":
                return phase
            if phase != "PREPARED":
                raise TransitionRefused(f"cannot abort a transition that is {phase}")
            db.execute("UPDATE transition SET phase='ABORTED' WHERE transition_id=?",
                       (transition_id,))
            self._trim_transitions(db)
        return "ABORTED"

    def allocate_transition_epoch(self, *, deadline=None):
        """Burn and return an epoch above the durable high-water mark and the active epoch."""
        with self._transaction(write=True, deadline=deadline) as db:
            actual = self._read_context(db)
            row = db.execute("SELECT value FROM authority WHERE name='epoch_high_water'").fetchone()
            high_water = 0 if row is None else _natural(row[0])
            epoch = max(high_water, actual.epoch) + 1
            db.execute("INSERT OR REPLACE INTO authority(name,value) VALUES ('epoch_high_water',?)",
                       (_decimal(epoch),))
        return epoch

    @staticmethod
    def _journal_entry(row):
        return JournalEntry(row[0], row[1],
                            Context(_natural(row[2]), ConsistencyMode(row[3])),
                            Context(_natural(row[4]), ConsistencyMode(row[5])), row[6])

    def journal_open(self, *, deadline=None):
        """vm01's unfinished transition, if any."""
        marks = ",".join("?" * len(JOURNAL_OPEN_STATES))
        with self._transaction(deadline=deadline) as db:
            row = db.execute(f"SELECT {_JOURNAL_COLUMNS} FROM controller_journal "
                             f"WHERE state IN ({marks}) ORDER BY seq DESC LIMIT 1",
                             JOURNAL_OPEN_STATES).fetchone()
            return None if row is None else self._journal_entry(row)

    def journal_get(self, transition_id, *, deadline=None):
        with self._transaction(deadline=deadline) as db:
            row = db.execute(f"SELECT {_JOURNAL_COLUMNS} FROM controller_journal "
                             "WHERE transition_id=?", (transition_id,)).fetchone()
            return None if row is None else self._journal_entry(row)

    def journal_start(self, transition_id, request_id, from_context, target_context, *,
                      deadline=None):
        """Record a new PREPARING transition; only one may be open at a time."""
        marks = ",".join("?" * len(JOURNAL_OPEN_STATES))
        with self._transaction(write=True, deadline=deadline) as db:
            if db.execute(f"SELECT 1 FROM controller_journal WHERE state IN ({marks})",
                          JOURNAL_OPEN_STATES).fetchone() is not None:
                raise TransitionRefused("a transition is already open")
            db.execute("INSERT INTO controller_journal(transition_id,request_id,from_epoch,"
                       "from_mode,target_epoch,target_mode,state) VALUES (?,?,?,?,?,?,'PREPARING')",
                       (transition_id, request_id, _decimal(from_context.epoch),
                        from_context.mode.value, _decimal(target_context.epoch),
                        target_context.mode.value))
            db.execute("DELETE FROM controller_journal WHERE state IN ('DONE','ABORTED') AND seq <= "
                       "(SELECT MAX(seq) FROM controller_journal) - ?", (TRANSITION_HISTORY,))

    def journal_advance(self, transition_id, new_state, *, deadline=None):
        """Move the journal forward along the only legal path; repeating a state is a no-op."""
        with self._transaction(write=True, deadline=deadline) as db:
            row = db.execute("SELECT state FROM controller_journal WHERE transition_id=?",
                             (transition_id,)).fetchone()
            if row is None:
                raise TransitionRefused("unknown journal transition")
            if row[0] == new_state:
                return new_state
            if new_state not in JOURNAL_MOVES.get(row[0], ()):
                raise TransitionRefused(f"illegal journal move {row[0]} -> {new_state}")
            db.execute("UPDATE controller_journal SET state=? WHERE transition_id=?",
                       (new_state, transition_id))
        return new_state

    def close(self):
        """Stop admission, wait for bounded accepted operations, release path lock."""
        with self._condition:
            self._closed = True
            self._condition.notify_all()
            while self._active:
                self._condition.wait()
            if self._lockfile is not None:
                self._lockfile.close()
                self._lockfile = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def open_store(path, *, bootstrap=None, vm_id="vm01", timeout=5.0, capacity=8):
    """Explicit local store factory; additional identity/budget args are local settings."""
    return Store(path, bootstrap=bootstrap, vm_id=vm_id, timeout=timeout, capacity=capacity)
