import time
import threading

ALIVE = "ALIVE"
SUSPECTED = "SUSPECTED"
FAILED = "FAILED"
LEFT = "LEFT"
TERMINAL = (FAILED, LEFT)
PLAIN_FAILURE_TIMEOUT = 2.5
SUSPECT_TIMEOUT = 1.0
CONFIRM_TIMEOUT = 1.5


class Entry:

    def __init__(self, node_id, heartbeat=0, incarnation=0, status=ALIVE):
        self.node_id = node_id
        self.heartbeat = heartbeat
        self.incarnation = incarnation
        self.status = status
        self.last_updated = time.time()
        self.suspected_since = None

    def to_dict(self):
        return {
            "node_id": self.node_id,
            "heartbeat": self.heartbeat,
            "incarnation": self.incarnation,
            "status": self.status
        }


class MembershipList:

    def __init__(self, self_id, on_event=None):
        self.self_id = self_id
        self._lock = threading.Lock()
        self._entries = {self_id: Entry(self_id)}
        self._suspects_log = []
        self.suspicion_enabled = False
        self.drop_rate = 0.0
        self._on_event = on_event or (lambda msg: None)
    def tick_heartbeat(self):
        with self._lock:
            e = self._entries[self.self_id]
            e.heartbeat += 1
            e.last_updated = time.time()
    def snapshot(self):
        with self._lock:
            return [e.to_dict() for e in self._entries.values()]

    def alive_peers(self):
        with self._lock:
            return [
                nid for nid, e in self._entries.items()
                if nid != self.self_id and e.status not in TERMINAL
            ]

    def all_known_peers(self):
        with self._lock:
            return [nid for nid in self._entries if nid != self.self_id]

    def mark_self_left(self):
        with self._lock:
            self._entries[self.self_id].status = LEFT
        self._on_event(f"LEFT self ({self.self_id})")

    def set_suspicion_mode(self, enabled):
        with self._lock:
            self.suspicion_enabled = enabled

    def set_drop_rate(self, rate):
        with self._lock:
            self.drop_rate = rate
    def merge(self, incoming_entries):
        self_refuted = False
        with self._lock:
            for inc in incoming_entries:
                nid = inc["node_id"]
                if nid == self.self_id:
                    if inc["status"] == SUSPECTED:
                        self_entry = self._entries[self.self_id]
                        if (
                            self_entry.status == ALIVE
                            and inc["incarnation"] >= self_entry.incarnation
                        ):
                            old_incarnation = self_entry.incarnation
                            self_entry.incarnation = inc["incarnation"] + 1
                            self._on_event(
                                f"REFUTED self suspicion incarnation "
                                f"{old_incarnation} -> {self_entry.incarnation}"
                            )
                            self_refuted = True
                    continue
                existing = self._entries.get(nid)
                if existing is None:
                    new_entry = Entry(
                        nid,
                        inc["heartbeat"],
                        inc["incarnation"],
                        inc["status"]
                    )
                    if inc["status"] == SUSPECTED:
                        new_entry.suspected_since = time.time()
                    self._entries[nid] = new_entry
                    self._on_event(f"JOIN learned of {nid}")
                    continue
                if self._incoming_wins(existing, inc):
                    old_status = existing.status
                    existing.heartbeat = inc["heartbeat"]
                    existing.incarnation = inc["incarnation"]
                    existing.status = inc["status"]
                    existing.last_updated = time.time()
                    if inc["status"] == SUSPECTED and old_status != SUSPECTED:
                        existing.suspected_since = time.time()
                    elif inc["status"] != SUSPECTED:
                        existing.suspected_since = None
                    if old_status == SUSPECTED and inc["status"] == ALIVE:
                        self._on_event(
                            f"SUSPECT CLEARED {nid} "
                            f"(incarnation {inc['incarnation']})"
                        )
                    if inc["status"] == FAILED and old_status != FAILED:
                        self._on_event(
                            f"FAILED {nid} (communicated via gossip)"
                        )
                    elif inc["status"] == LEFT and old_status != LEFT:
                        self._on_event(
                            f"LEFT {nid} (communicated via gossip)"
                        )
        return self_refuted
    def _incoming_wins(self, existing, incoming):
        if existing.status in TERMINAL:
            return False
        if incoming["status"] in TERMINAL:
            return True
        if incoming["incarnation"] != existing.incarnation:
            return incoming["incarnation"] > existing.incarnation
        if incoming["status"] == SUSPECTED and existing.status == ALIVE:
            return True
        if incoming["status"] == ALIVE and existing.status == SUSPECTED:
            return False
        if incoming["heartbeat"] != existing.heartbeat:
            return incoming["heartbeat"] > existing.heartbeat
        return False
    def run_timeout_check(self):
        with self._lock:
            now = time.time()
            for nid, e in self._entries.items():
                if nid == self.self_id or e.status in TERMINAL:
                    continue
                if e.status == ALIVE:
                    if self.suspicion_enabled:
                        threshold = SUSPECT_TIMEOUT
                    else:
                        threshold = PLAIN_FAILURE_TIMEOUT
                    if now - e.last_updated > threshold:
                        if self.suspicion_enabled:
                            e.status = SUSPECTED
                            e.suspected_since = now
                            self._suspects_log.append((nid, now))
                            self._on_event(
                                f"SUSPECTED {nid} (local detection)"
                            )
                        else:
                            e.status = FAILED
                            self._on_event(
                                f"FAILED {nid} (local detection, plain gossip)"
                            )
                elif e.status == SUSPECTED:
                    if now - e.suspected_since > CONFIRM_TIMEOUT:
                        e.status = FAILED
                        self._on_event(
                            f"FAILED {nid} (confirm timeout expired)"
                        )

    def get_suspects_log(self):
        with self._lock:
            return list(self._suspects_log)
    def list_mem(self):
        with self._lock:
            return [
                (e.node_id, e.heartbeat, e.incarnation, e.status)
                for e in self._entries.values()
                if e.status not in TERMINAL
            ]
