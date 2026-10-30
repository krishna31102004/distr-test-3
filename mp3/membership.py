"""Fixed-manifest membership, suspicion, refutation and recovery (KD-03/04/05/C2).

This adapts the heartbeat/snapshot/suspicion ideas from our MP2 membership code.
The MP2 source remains unchanged; its terminal FAILED state and timestamp identity
are deliberately not used here because MP3 must recover the same fixed VM owner.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from enum import Enum
import json
import math
import random
import socket
import threading
import time
from typing import Callable, Iterable

from .framing import HEADER, send_frame
from .ring import Manifest, Node, node_position


MEMBERSHIP_FRAME_LIMIT = 65_536
MAX_COUNTER_DIGITS = 256
DEFAULT_SUSPICION_SECONDS = 2.5
DEFAULT_GOSSIP_INTERVAL = 0.5
DEFAULT_FANOUT = 3
_START_LOCK = threading.Lock()
_STARTED_STORE_PATHS: set[str] = set()


class MemberState(str, Enum):
    ALIVE = "ALIVE"
    SUSPECTED = "SUSPECTED"


@dataclass(frozen=True)
class Member:
    vm_id: str
    restart_generation: int
    incarnation: int
    heartbeat: int
    status: MemberState

    def __post_init__(self) -> None:
        if not isinstance(self.vm_id, str) or self.vm_id not in {
                f"vm{i:02d}" for i in range(1, 11)}:
            raise ValueError("member vm_id must be vm01 through vm10")
        for name in ("restart_generation", "incarnation", "heartbeat"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
            if len(_decimal(getattr(self, name))) > MAX_COUNTER_DIGITS:
                raise ValueError(f"{name} exceeds the {MAX_COUNTER_DIGITS}-digit resource bound")
        if not isinstance(self.status, MemberState):
            raise ValueError("status must be ALIVE or SUSPECTED")


def _decimal(value: int) -> str:
    if type(value) is not int or value < 0:
        raise ValueError("membership counters must be nonnegative integers")
    if value == 0:
        return "0"
    pieces = []
    while value:
        value, remainder = divmod(value, 1_000_000_000)
        pieces.append(remainder)
    return str(pieces[-1]) + "".join(f"{piece:09d}" for piece in reversed(pieces[:-1]))


def _parse_decimal(value: object, field: str) -> int:
    if not isinstance(value, str) or not value or not value.isascii() or not value.isdecimal():
        raise ValueError(f"{field} must be a canonical nonnegative decimal string")
    if len(value) > 1 and value[0] == "0":
        raise ValueError(f"{field} must not have leading zeroes")
    if len(value) > MAX_COUNTER_DIGITS:
        raise ValueError(f"{field} exceeds the {MAX_COUNTER_DIGITS}-digit resource bound")
    number = 0
    for start in range(0, len(value), 9):
        part = value[start:start + 9]
        number = number * (10 ** len(part)) + int(part)
    return number


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


@dataclass(frozen=True)
class GossipEnvelope:
    manifest_id: str
    sender_id: str
    sender_generation: int
    members: tuple[Member, ...]
    message_type: str = "GOSSIP"

    def __post_init__(self) -> None:
        if not isinstance(self.manifest_id, str) or len(self.manifest_id) != 64:
            raise ValueError("manifest_id must be a SHA-256 hex fingerprint")
        try:
            bytes.fromhex(self.manifest_id)
        except ValueError as error:
            raise ValueError("manifest_id must be hexadecimal") from error
        if self.message_type != "GOSSIP":
            raise ValueError("membership message type must be GOSSIP")
        if self.sender_id not in {f"vm{i:02d}" for i in range(1, 11)}:
            raise ValueError("unknown gossip sender")
        if type(self.sender_generation) is not int or self.sender_generation < 1:
            raise ValueError("sender_generation must be a positive integer")
        if len(_decimal(self.sender_generation)) > MAX_COUNTER_DIGITS:
            raise ValueError("sender_generation exceeds the membership resource bound")
        members = tuple(self.members)
        if len(members) != 10 or any(not isinstance(row, Member) for row in members):
            raise ValueError("gossip must carry exactly ten validated membership rows")
        members = tuple(sorted(members, key=lambda row: node_position(row.vm_id)))
        object.__setattr__(self, "members", members)
        ids = [row.vm_id for row in self.members]
        if len(set(ids)) != 10 or set(ids) != {f"vm{i:02d}" for i in range(1, 11)}:
            raise ValueError("gossip must carry each manifest VM exactly once")
        sender = next(row for row in self.members if row.vm_id == self.sender_id)
        if sender.restart_generation != self.sender_generation:
            raise ValueError("sender generation must match its row in the full snapshot")

    def to_bytes(self) -> bytes:
        payload = {
            "type": self.message_type,
            "manifest_id": self.manifest_id,
            "sender_id": self.sender_id,
            "sender_generation": _decimal(self.sender_generation),
            "members": [{
                "vm_id": member.vm_id,
                "restart_generation": _decimal(member.restart_generation),
                "incarnation": _decimal(member.incarnation),
                "heartbeat": _decimal(member.heartbeat),
                "status": member.status.value,
            } for member in sorted(self.members, key=lambda row: node_position(row.vm_id))],
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8", "strict")
        if len(encoded) > MEMBERSHIP_FRAME_LIMIT:
            raise ValueError("membership message exceeds the 65536-byte cap")
        return encoded

    @classmethod
    def from_bytes(cls, payload: bytes, manifest: Manifest) -> "GossipEnvelope":
        if not isinstance(payload, bytes) or len(payload) > MEMBERSHIP_FRAME_LIMIT:
            raise ValueError("membership payload must be bytes within the frame cap")
        try:
            data = json.loads(payload.decode("utf-8", "strict"), object_pairs_hook=_unique_object)
        except (UnicodeError, json.JSONDecodeError) as error:
            raise ValueError(f"malformed membership JSON: {error}") from error
        if not isinstance(data, dict) or set(data) != {
                "type", "manifest_id", "sender_id", "sender_generation", "members"}:
            raise ValueError("gossip envelope has missing or unknown fields")
        if data["type"] != "GOSSIP" or data["manifest_id"] != manifest.fingerprint:
            raise ValueError("gossip type or manifest fingerprint mismatch")
        if not isinstance(data["members"], list) or len(data["members"]) != 10:
            raise ValueError("gossip members must be a ten-entry list")
        members = []
        for raw in data["members"]:
            if not isinstance(raw, dict) or set(raw) != {
                    "vm_id", "restart_generation", "incarnation", "heartbeat", "status"}:
                raise ValueError("member row has missing or unknown fields")
            try:
                state = MemberState(raw["status"])
            except (ValueError, TypeError) as error:
                raise ValueError("invalid member status") from error
            members.append(Member(
                raw["vm_id"],
                _parse_decimal(raw["restart_generation"], "restart_generation"),
                _parse_decimal(raw["incarnation"], "incarnation"),
                _parse_decimal(raw["heartbeat"], "heartbeat"),
                state,
            ))
        return cls(data["manifest_id"], data["sender_id"],
                   _parse_decimal(data["sender_generation"], "sender_generation"),
                   tuple(members), data["type"])


class MembershipTable:
    """Thread-safe full membership view; down members remain fixed ring owners."""

    def __init__(self, manifest: Manifest, self_id: str, *, clock: Callable[[], float] = time.monotonic,
                 suspicion_seconds: float = DEFAULT_SUSPICION_SECONDS,
                 on_event: Callable[[str], None] | None = None):
        if not isinstance(manifest, Manifest):
            raise TypeError("manifest must be a validated Manifest")
        manifest.node(self_id)
        try:
            valid_suspicion = (type(suspicion_seconds) in (int, float)
                               and math.isfinite(suspicion_seconds) and suspicion_seconds > 0)
        except OverflowError:
            valid_suspicion = False
        if not valid_suspicion:
            raise ValueError("suspicion_seconds must be positive and finite")
        self.manifest = manifest
        self.self_id = self_id
        self._clock = clock
        self.suspicion_seconds = float(suspicion_seconds)
        self._on_event = on_event or (lambda _event: None)
        self._lock = threading.RLock()
        now = self._clock()
        self._members = {node.vm_id: Member(node.vm_id, 0, 0, 0, MemberState.ALIVE)
                         for node in manifest.nodes}
        self._progress = {node.vm_id: now for node in manifest.nodes}
        self._local_generation: int | None = None
        self._target_cursor = 0

    def set_local_generation(self, generation: int) -> None:
        """Install the generation committed by Store.start_incarnation exactly once."""
        if type(generation) is not int or generation < 1:
            raise ValueError("local restart generation must be positive")
        with self._lock:
            if self._local_generation is not None:
                raise RuntimeError("local restart generation was already installed")
            self._local_generation = generation
            self._members[self.self_id] = Member(self.self_id, generation, 0, 0, MemberState.ALIVE)
            self._progress[self.self_id] = self._clock()

    @property
    def local_generation(self) -> int:
        with self._lock:
            if self._local_generation is None:
                raise RuntimeError("start_incarnation must run before membership is served")
            return self._local_generation

    def snapshot(self) -> tuple[Member, ...]:
        """Return every node in ascending ring-position order, including suspects."""
        with self._lock:
            return tuple(sorted(self._members.values(),
                                key=lambda member: self.manifest.node(member.vm_id).ring_id))

    def list_mem_ids(self) -> tuple[tuple[str, int, int, int, MemberState, int], ...]:
        """Rows for the diagnostic command, sorted by numerical ring nodeID."""
        with self._lock:
            return tuple((member.vm_id, member.restart_generation, member.incarnation,
                          member.heartbeat,
                          member.status, self.manifest.node(member.vm_id).ring_id)
                         for member in sorted(
                             self._members.values(),
                             key=lambda row: self.manifest.node(row.vm_id).ring_id))

    def owners(self, key: str) -> tuple[Node, ...]:
        """Membership never filters owner selection by liveness."""
        return self.manifest.owners(key)

    def envelope(self) -> GossipEnvelope:
        return GossipEnvelope(self.manifest.fingerprint, self.self_id,
                              self.local_generation, self.snapshot())

    def tick_heartbeat(self) -> Member:
        with self._lock:
            if self._local_generation is None:
                raise RuntimeError("start_incarnation must run before heartbeat service")
            current = self._members[self.self_id]
            updated = Member(self.self_id, current.restart_generation,
                             current.incarnation, current.heartbeat + 1, MemberState.ALIVE)
            self._members[self.self_id] = updated
            self._progress[self.self_id] = self._clock()
            return updated

    def check_suspicions(self) -> tuple[str, ...]:
        """Mark stale ALIVE peers SUSPECTED using monotonic elapsed time; never delete."""
        now = self._clock()
        events = []
        with self._lock:
            for vm_id, member in tuple(self._members.items()):
                if vm_id == self.self_id or member.status != MemberState.ALIVE:
                    continue
                if now - self._progress[vm_id] >= self.suspicion_seconds:
                    self._members[vm_id] = Member(vm_id, member.restart_generation,
                                                  member.incarnation, member.heartbeat,
                                                  MemberState.SUSPECTED)
                    events.append(f"SUSPECTED {vm_id} generation={member.restart_generation}")
        for event in events:
            self._on_event(event)
        return tuple(event.split()[1] for event in events)

    def merge_gossip(self, envelope: GossipEnvelope) -> tuple[str, ...]:
        """Merge only monotonically newer process-generation/incarnation evidence."""
        if not isinstance(envelope, GossipEnvelope):
            raise TypeError("envelope must be a validated GossipEnvelope")
        if envelope.manifest_id != self.manifest.fingerprint:
            raise ValueError("gossip belongs to a different manifest")
        if envelope.sender_id == self.self_id:
            raise ValueError("a node cannot merge its own network gossip")
        incoming_by_id = {row.vm_id: row for row in envelope.members}
        if set(incoming_by_id) != set(self._members):
            raise ValueError("gossip membership must match the fixed ten-node manifest")
        if incoming_by_id[envelope.sender_id].restart_generation != envelope.sender_generation:
            raise ValueError("sender generation does not match full snapshot")

        now = self._clock()
        events: list[str] = []
        with self._lock:
            # Check state-dependent invariants before changing any row. In particular,
            # a snapshot claiming that this process has a newer durable generation
            # means local identity/storage state is inconsistent; reject it atomically.
            if incoming_by_id[self.self_id].restart_generation > self.local_generation:
                raise RuntimeError("gossip claims a newer self generation; local identity is inconsistent")

            for vm_id in sorted(incoming_by_id):
                incoming = incoming_by_id[vm_id]
                local = self._members[vm_id]
                if vm_id == self.self_id:
                    if incoming.restart_generation == self.local_generation \
                            and incoming.status == MemberState.SUSPECTED \
                            and incoming.incarnation >= local.incarnation:
                        refuted = Member(self.self_id, self.local_generation,
                                         incoming.incarnation + 1, local.heartbeat, MemberState.ALIVE)
                        self._members[self.self_id] = refuted
                        self._progress[self.self_id] = now
                        events.append(f"REFUTED self suspicion incarnation={refuted.incarnation}")
                    continue

                if incoming.restart_generation < local.restart_generation:
                    continue
                if incoming.restart_generation > local.restart_generation:
                    self._members[vm_id] = incoming
                    self._progress[vm_id] = now
                    events.append(f"RECOVERED {vm_id} generation={incoming.restart_generation}"
                                  if incoming.status == MemberState.ALIVE else
                                  f"NEW GENERATION {vm_id} generation={incoming.restart_generation}")
                    continue
                if incoming.incarnation < local.incarnation:
                    continue
                if incoming.incarnation > local.incarnation:
                    self._members[vm_id] = incoming
                    self._progress[vm_id] = now
                    if local.status == MemberState.SUSPECTED and incoming.status == MemberState.ALIVE:
                        events.append(f"SUSPECT CLEARED {vm_id} incarnation={incoming.incarnation}")
                    continue

                # At equal generation and incarnation, suspicion wins. A heartbeat
                # cannot refute suspicion; only a newer self-refutation can do that.
                if incoming.status == MemberState.SUSPECTED and local.status == MemberState.ALIVE:
                    self._members[vm_id] = Member(vm_id, local.restart_generation,
                                                  local.incarnation,
                                                  max(local.heartbeat, incoming.heartbeat),
                                                  MemberState.SUSPECTED)
                    if incoming.heartbeat > local.heartbeat:
                        self._progress[vm_id] = now
                    events.append(f"SUSPECTED {vm_id} via gossip")
                elif incoming.status == local.status and incoming.heartbeat > local.heartbeat:
                    self._members[vm_id] = Member(vm_id, local.restart_generation,
                                                  local.incarnation, incoming.heartbeat,
                                                  local.status)
                    self._progress[vm_id] = now
        for event in events:
            self._on_event(event)
        return tuple(events)

    def next_targets(self, fanout: int = DEFAULT_FANOUT) -> tuple[Node, ...]:
        """Choose bounded peers from all manifest nodes, including retained suspects."""
        if type(fanout) is not int or not 1 <= fanout <= len(self._members) - 1:
            raise ValueError("fanout must be in 1..9")
        with self._lock:
            others = [member for member in self._members.values() if member.vm_id != self.self_id]
            others.sort(key=lambda member: (member.status != MemberState.SUSPECTED, member.vm_id))
            start = self._target_cursor % len(others)
            rotated = others[start:] + others[:start]
            selected = rotated[:fanout]
            self._target_cursor = (start + fanout) % len(others)
            return tuple(self.manifest.node(member.vm_id) for member in selected)


def _recv_exact_deadline(sock: socket.socket, size: int, deadline: float) -> bytes:
    chunks = bytearray()
    while len(chunks) < size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise socket.timeout("membership request deadline expired")
        sock.settimeout(remaining)
        part = sock.recv(size - len(chunks))
        if not part:
            raise ConnectionError("peer closed before complete membership frame")
        chunks.extend(part)
    return bytes(chunks)


def _recv_frame_deadline(sock: socket.socket, deadline: float) -> bytes:
    header = _recv_exact_deadline(sock, HEADER.size, deadline)
    (length,) = HEADER.unpack(header)
    if length > MEMBERSHIP_FRAME_LIMIT:
        raise ValueError("membership frame exceeds 65536-byte limit")
    return _recv_exact_deadline(sock, length, deadline)


def exchange_gossip(endpoint: object, envelope: GossipEnvelope, manifest: Manifest,
                    *, timeout: float = 0.75) -> GossipEnvelope:
    """One bounded TCP/framed full-snapshot exchange; no introducer is special."""
    try:
        valid_timeout = (type(timeout) in (int, float) and math.isfinite(timeout) and timeout > 0)
    except OverflowError:
        valid_timeout = False
    if not valid_timeout:
        raise ValueError("timeout must be positive and finite")
    deadline = time.monotonic() + timeout
    with socket.create_connection((endpoint.host, endpoint.port), timeout=timeout) as sock:
        payload = envelope.to_bytes()
        sock.settimeout(max(0.001, deadline - time.monotonic()))
        send_frame(sock, payload, max_bytes=MEMBERSHIP_FRAME_LIMIT)
        response = GossipEnvelope.from_bytes(_recv_frame_deadline(sock, deadline), manifest)
    return response


class MembershipService:
    """Separate membership TCP service with bounded worker admission."""

    def __init__(self, endpoint: object, table: MembershipTable, *, workers: int = 8,
                 max_pending: int = 32, io_timeout: float = 2.5):
        if type(workers) is not int or workers < 1 or type(max_pending) is not int \
                or max_pending < workers:
            raise ValueError("membership workers/max_pending must be positive and bounded")
        try:
            valid_timeout = (type(io_timeout) in (int, float)
                             and math.isfinite(io_timeout) and io_timeout > 0)
        except OverflowError:
            valid_timeout = False
        if not valid_timeout:
            raise ValueError("io_timeout must be positive and finite")
        self.endpoint = endpoint
        self.table = table
        self.io_timeout = float(io_timeout)
        self._slots = threading.BoundedSemaphore(max_pending)
        self._pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mp3-membership-rpc")
        self._server: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        if self._accept_thread is not None:
            raise RuntimeError("membership service already started")
        self.table.local_generation  # fail before opening a listener if recovery has not run
        server = socket.socket(socket.AF_INET6 if ":" in self.endpoint.host else socket.AF_INET,
                               socket.SOCK_STREAM)
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.endpoint.host, self.endpoint.port))
            server.listen(64)
            server.settimeout(0.2)
        except OSError:
            server.close()
            raise
        self._server = server
        self._accept_thread = threading.Thread(target=self._accept_loop,
                                               name="mp3-membership-accept", daemon=True)
        self._accept_thread.start()

    def _accept_loop(self) -> None:
        assert self._server is not None
        while not self._stop.is_set():
            try:
                conn, _address = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                if self._stop.is_set():
                    return
                continue
            if not self._slots.acquire(blocking=False):
                conn.close()
                continue
            try:
                self._pool.submit(self._handle, conn)
            except RuntimeError:
                self._slots.release()
                conn.close()

    def _handle(self, conn: socket.socket) -> None:
        try:
            deadline = time.monotonic() + self.io_timeout
            payload = _recv_frame_deadline(conn, deadline)
            incoming = GossipEnvelope.from_bytes(payload, self.table.manifest)
            self.table.merge_gossip(incoming)
            response = self.table.envelope().to_bytes()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise socket.timeout("membership response deadline expired")
            conn.settimeout(remaining)
            send_frame(conn, response, max_bytes=MEMBERSHIP_FRAME_LIMIT)
        except (OSError, ValueError, TypeError, RuntimeError):
            # Invalid/stale messages are discarded; the sender learns via timeout.
            pass
        finally:
            conn.close()
            self._slots.release()

    def stop(self) -> bool:
        """Stop admission and wait for bounded handlers; report listener join."""
        self._stop.set()
        if self._server is not None:
            self._server.close()
            self._server = None
        accept_joined = True
        if self._accept_thread is not None:
            if self._accept_thread.ident is not None:
                self._accept_thread.join(timeout=self.io_timeout + 1)
                accept_joined = not self._accept_thread.is_alive()
            if accept_joined:
                self._accept_thread = None
        self._pool.shutdown(wait=True, cancel_futures=True)
        return accept_joined


class MembershipRuntime:
    """Startup/gossip owner; reserves one durable generation before network admission."""

    def __init__(self, store: object, manifest: Manifest, *,
                 suspicion_seconds: float = DEFAULT_SUSPICION_SECONDS,
                 gossip_interval: float = DEFAULT_GOSSIP_INTERVAL,
                 fanout: int = DEFAULT_FANOUT,
                 rpc_timeout: float = 0.75,
                 clock: Callable[[], float] = time.monotonic,
                 rng: random.Random | None = None):
        try:
            valid_interval = (type(gossip_interval) in (int, float)
                              and math.isfinite(gossip_interval) and gossip_interval > 0)
        except OverflowError:
            valid_interval = False
        if not valid_interval:
            raise ValueError("gossip_interval must be positive and finite")
        if type(fanout) is not int or not 1 <= fanout <= 9:
            raise ValueError("fanout must be in 1..9")
        try:
            valid_rpc_timeout = (type(rpc_timeout) in (int, float)
                                 and math.isfinite(rpc_timeout) and rpc_timeout > 0)
        except OverflowError:
            valid_rpc_timeout = False
        if not valid_rpc_timeout:
            raise ValueError("rpc_timeout must be positive and finite")
        self.store = store
        self.manifest = manifest
        self.self_id = store.vm_id
        manifest.node(self.self_id)
        self._clock = clock
        self.gossip_interval = float(gossip_interval)
        self.fanout = fanout
        self.rpc_timeout = float(rpc_timeout)
        self._rng = rng or random.Random()
        self.table = MembershipTable(manifest, self.self_id, clock=clock,
                                     suspicion_seconds=suspicion_seconds)
        self._generation: int | None = None
        self._service: MembershipService | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._stop_report: bool | None = None
        self._rpc_pool = ThreadPoolExecutor(max_workers=fanout,
                                            thread_name_prefix="mp3-membership-gossip")

    @property
    def generation(self) -> int:
        if self._generation is None:
            raise RuntimeError("runtime has not started")
        return self._generation

    def start(self, *, background: bool = True, listen: bool = True) -> None:
        """Recover store, reserve generation exactly once, then admit membership traffic."""
        if self._started:
            raise RuntimeError("runtime may start only once per process/store startup")
        self._started = True
        # Recovery and context validation happen before this generation becomes visible.
        self.store.current_context()
        store_path = str(self.store.path)
        with _START_LOCK:
            if store_path in _STARTED_STORE_PATHS:
                raise RuntimeError("start_incarnation already ran for this store in this process")
            _STARTED_STORE_PATHS.add(store_path)
        try:
            self._generation = self.store.start_incarnation()
        except Exception:
            with _START_LOCK:
                _STARTED_STORE_PATHS.discard(store_path)
            raise
        self.table.set_local_generation(self._generation)
        if listen:
            self._service = MembershipService(self.manifest.node(self.self_id).membership,
                                              self.table)
            self._service.start()
        if background:
            self._thread = threading.Thread(target=self._run, name="mp3-membership-gossip-loop",
                                            daemon=True)
            self._thread.start()

    def gossip_round(self) -> tuple[str, ...]:
        """Probe a bounded rotating sample of all fixed peers, live or suspected."""
        if self._generation is None:
            raise RuntimeError("runtime must start before gossip")
        self.table.tick_heartbeat()
        self.table.check_suspicions()
        targets = self.table.next_targets(self.fanout)
        request = self.table.envelope()
        futures = {
            self._rpc_pool.submit(exchange_gossip, node.membership, request, self.manifest,
                                  timeout=self.rpc_timeout): node.vm_id
            for node in targets
        }
        done, pending = wait(futures, timeout=self.rpc_timeout + 0.1)
        outcomes = []
        for future in done:
            vm_id = futures[future]
            try:
                response = future.result()
                events = self.table.merge_gossip(response)
                outcomes.append(f"{vm_id}:merged:{len(events)}")
            except (OSError, ValueError, TypeError, RuntimeError) as error:
                outcomes.append(f"{vm_id}:unreachable:{type(error).__name__}")
        # Completed calls are bounded by fanout; pending network calls have a finite
        # socket timeout and are not replaced with an unbounded queue of retries.
        if pending:
            outcomes.extend(f"{futures[future]}:pending" for future in pending)
        return tuple(sorted(outcomes))

    def _run(self) -> None:
        while not self._stop.is_set():
            started = self._clock()
            try:
                self.gossip_round()
            except (OSError, RuntimeError):
                pass
            remaining = self.gossip_interval - (self._clock() - started)
            self._stop.wait(max(0.0, remaining))

    def stop(self) -> bool:
        """Stop gossip/listening and report whether their threads have joined."""
        if self._stop_report is True:
            return self._stop_report
        self._stop.set()
        gossip_joined = True
        if self._thread is not None:
            if self._thread.ident is not None:
                self._thread.join(timeout=max(2.0, self.rpc_timeout * 2))
                gossip_joined = not self._thread.is_alive()
            else:
                # A failed Thread.start() during partial startup is already stopped.
                self._thread = None
        if self._service is not None:
            listener_joined = self._service.stop()
            if listener_joined:
                self._service = None
        else:
            listener_joined = True
        self._rpc_pool.shutdown(wait=True, cancel_futures=True)
        if self._thread is not None and self._thread.ident is not None \
                and self._thread.is_alive():
            self._thread.join(timeout=self.rpc_timeout + 0.5)
            gossip_joined = not self._thread.is_alive()
        if gossip_joined:
            self._thread = None
        self._stop_report = gossip_joined and listener_joined
        return self._stop_report
