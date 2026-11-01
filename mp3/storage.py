"""Batch 1 local SQLite foundation. No replication, networking or mode switching.

Each operation owns a connection and a bounded admission slot. The process lock
protects the data path's lifetime, not distributed GET/PUT operations.
"""
from contextlib import contextmanager
import fcntl
import math
from pathlib import Path
import re
import sqlite3
import threading
import time

from .models import (ConsistencyMode, Context, Version, Record, ReadResult,
                     InstallResult, LocalStatus as S)


class StorageError(RuntimeError):
    """Storage could not establish or preserve the promised local guarantees."""


class StaleEpochError(StorageError):
    def __init__(self, context):
        super().__init__("context does not match persisted epoch/mode")
        self.context = context


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
            with self._transaction(write=True) as db:
                if bootstrap is not None:
                    db.execute("CREATE TABLE meta (name TEXT PRIMARY KEY, value TEXT NOT NULL)")
                    db.execute("CREATE TABLE records (key TEXT PRIMARY KEY, kind TEXT NOT NULL, number TEXT NOT NULL, writer TEXT NOT NULL, value BLOB NOT NULL)")
                    db.executemany("INSERT INTO meta VALUES (?,?)", [
                        ("schema", "2"), ("vm_id", vm_id), ("epoch", "1"),
                        ("mode", bootstrap.mode.value), ("generation", "0"),
                        ("last_timestamp", "0"), ("strong_counter", "0")])
                if db.execute("PRAGMA quick_check").fetchall() != [("ok",)]:
                    raise StorageError("database integrity check failed")
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
    def _transaction(self, *, write=False, deadline=None):
        """Bound admission and SQLite waits; rollback any unsuccessful operation."""
        local_deadline = time.monotonic() + self.timeout
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
                                 timeout=remaining, isolation_level=None)
            db.execute(f"PRAGMA busy_timeout={max(1, int(remaining * 1000))}")
            if db.execute("PRAGMA journal_mode=WAL").fetchone()[0] != "wal":
                raise StorageError("WAL unavailable")
            db.execute("PRAGMA synchronous=FULL")
            if db.execute("PRAGMA synchronous").fetchone()[0] != 2:
                raise StorageError("FULL unavailable")
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 100)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise StorageError("storage deadline expired")
            db.execute(f"PRAGMA busy_timeout={max(1, int(remaining * 1000))}")
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
        except (sqlite3.Error, StorageError, ValueError, TypeError, KeyError) as error:
            return InstallResult(S.ERROR, None, False, self._context, str(error))

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
