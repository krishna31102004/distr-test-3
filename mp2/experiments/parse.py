import glob
import json
import os
import re
from collections import defaultdict


# Match the timestamp formats written by node.py.
LINE_EPOCH = re.compile(r"^\[[^\]]*\bepoch=([0-9.]+)\]\s+(.*)$")
LINE_T = re.compile(r"^\[[^\]]+\]\s+\[t=([0-9.]+)\]\s+(.*)$")
RE_FAILED = re.compile(r"^FAILED\s+(\S+)\s+\((.*)\)$")
RE_SUSPECTED = re.compile(r"^SUSPECTED\s+(\S+)\s+\((.*)\)$")
RE_REFUTE = re.compile(r"^REFUTED?\b")
RE_JOIN = re.compile(r"^JOIN learned of\s+(\S+)$")
RE_LEFT = re.compile(r"^LEFT\s+(\S+)\s+\((.*)\)$")
RE_DROP = re.compile(r"^DROPPED gossip message from\s+(\S+)$")
RE_CLEARED = re.compile(r"^SUSPECT CLEARED\s+(\S+)")
RE_PARAMS = re.compile(r"^PARAMS\s+(.*)$")
RE_BW = re.compile(
    r"^BW(?:SAMPLE|STATS)\s+(?:bytes_)?sent=(\d+)\s+(?:bytes_)?(?:received|recv)=(\d+)$"
)
RE_MEMDUMP = re.compile(r"^MEMDUMP\s+(.*)$")
RE_MARK = re.compile(r"^MARK\s+(.*)$")


# Keep the parsed events for one VM in one place.
class NodeLog:

    def __init__(self, vm):
        self.vm = vm
        self.failed = []
        self.suspected = []
        self.refutes = []
        self.cleared = []
        self.params = None
        self.joins = []
        self.left = []
        self.drops = []
        self.bw = []
        self.memdumps = []
        self.marks = []
        self.self_id = None


# Parse every machine log collected for a trial.
def parse_dir(log_dir):
    logs = {}
    for path in sorted(glob.glob(os.path.join(log_dir, "machine.*.log"))):
        m = re.search(r"machine\.(\d+)\.log$", path)
        if not m:
            continue
        vm = int(m.group(1))
        logs[vm] = parse_file(path, vm)
    return logs


# Read one log file and sort each line into an event type.
def parse_file(path, vm):
    nl = NodeLog(vm)
    with open(path, errors="replace") as f:
        for raw in f:
            raw = raw.rstrip("\n")
            m = LINE_EPOCH.match(raw) or LINE_T.match(raw)
            if not m:
                continue
            t = float(m.group(1))
            body = m.group(2)
            mm = RE_FAILED.match(body)
            if mm:
                nl.failed.append((t, mm.group(1), mm.group(2)))
                continue
            mm = RE_SUSPECTED.match(body)
            if mm:
                nl.suspected.append((t, mm.group(1), mm.group(2)))
                continue
            if RE_REFUTE.match(body):
                nl.refutes.append(t)
                continue
            mm = RE_CLEARED.match(body)
            if mm:
                nl.cleared.append((t, mm.group(1)))
                continue
            mm = RE_PARAMS.match(body)
            if mm:
                nl.params = dict(
                    kv.split("=", 1) for kv in mm.group(1).split() if "=" in kv
                )
                continue
            mm = RE_JOIN.match(body)
            if mm:
                nl.joins.append((t, mm.group(1)))
                continue
            mm = RE_LEFT.match(body)
            if mm:
                nl.left.append((t, mm.group(1)))
                continue
            if RE_DROP.match(body):
                nl.drops.append(t)
                continue
            mm = RE_BW.match(body)
            if mm:
                nl.bw.append((t, int(mm.group(1)), int(mm.group(2))))
                continue
            mm = RE_MEMDUMP.match(body)
            if mm:
                try:
                    nl.memdumps.append((t, json.loads(mm.group(1))))
                except json.JSONDecodeError:
                    pass
                continue
            mm = RE_MARK.match(body)
            if mm:
                nl.marks.append((t, mm.group(1)))
    return nl


# Count failures that were not caused by an actual killed node.
def false_failures(logs, t0, t1, victims=()):
    victims = set(victims)
    out = []
    for vm, nl in sorted(logs.items()):
        for t, nid, reason in nl.failed:
            if t0 <= t <= t1 and nid not in victims:
                out.append((vm, t - t0, nid, reason))
    return len(out), out


def suspicion_events(logs, t0, t1):
    susp = sum(
        1 for nl in logs.values()
        for t, _, _ in nl.suspected if t0 <= t <= t1
    )
    ref = sum(1 for nl in logs.values() for t in nl.refutes if t0 <= t <= t1)
    return susp, ref


def cleared_count(logs, t0, t1):
    return sum(
        1 for nl in logs.values()
        for t, _ in nl.cleared if t0 <= t <= t1
    )


# Check that every node used the same experiment settings.
def run_params(logs):
    seen = [nl.params for nl in logs.values() if nl.params]
    if not seen:
        return None, True
    first = seen[0]
    return first, all(p == first for p in seen)


def dropped_count(logs, t0, t1):
    return sum(1 for nl in logs.values() for t in nl.drops if t0 <= t <= t1)


def entry_id(e):
    return e.get("id") or e.get("node_id") or ""


def entry_status(e):
    return e.get("st") or e.get("status") or ""


# Check the final visible membership size on each node.
def final_membership(logs, expected):
    counts = {}
    for vm, nl in logs.items():
        if not nl.memdumps:
            continue
        _, entries = nl.memdumps[-1]
        counts[vm] = sum(
            1 for e in entries if entry_status(e) not in ("FAILED", "LEFT")
        )
    agree = sum(1 for c in counts.values() if c == expected)
    return agree, counts


# Measure when surviving nodes marked each victim as failed.
def detection_times(logs, victim_ids, kill_t0, survivors):
    res = {}
    for vid in victim_ids:
        per_vm = {}
        for vm in survivors:
            nl = logs.get(vm)
            if not nl:
                continue
            times = [t for t, nid, _ in nl.failed if nid == vid and t >= kill_t0]
            if times:
                per_vm[vm] = min(times) - kill_t0
        res[vid] = {
            "first": min(per_vm.values()) if per_vm else None,
            "all": max(per_vm.values()) if len(per_vm) == len(survivors) else None,
            "n_detected": len(per_vm),
            "per_vm": per_vm,
        }
    return res


# Use MARK events to find experiment window boundaries.
def marks_index(logs):
    buckets = defaultdict(list)
    for nl in logs.values():
        for t, text in nl.marks:
            buckets[text].append(t)
    out = {}
    for text, ts in buckets.items():
        ts.sort()
        out[text] = ts[len(ts) // 2]
    return out


# Turn cumulative byte counters into per-node byte rates.
def bandwidth_buckets(logs, t0, t1, bucket_s=30.0, guard=3.0):
    lo_t, hi_t = t0 - guard, t1 + guard
    rows = []
    for vm, nl in sorted(logs.items()):
        samples = [(t, s, r) for t, s, r in nl.bw if lo_t <= t <= hi_t]
        if len(samples) < 2:
            continue
        if len(samples) < 3:
            lo, hi = samples[0], samples[-1]
            dt = hi[0] - lo[0]
            if dt > 0:
                rows.append({
                    "vm": vm,
                    "bucket": 0,
                    "sent_bps": (hi[1] - lo[1]) / dt,
                    "recv_bps": (hi[2] - lo[2]) / dt,
                })
            continue
        start = samples[0][0]
        edges = []
        e = start
        while e + bucket_s <= samples[-1][0]:
            edges.append((e, e + bucket_s))
            e += bucket_s
        for i, (a, b) in enumerate(edges):
            lo = _sample_at(samples, a)
            hi = _sample_at(samples, b)
            if lo is None or hi is None or hi[0] <= lo[0]:
                continue
            dt = hi[0] - lo[0]
            rows.append({
                "vm": vm,
                "bucket": i,
                "sent_bps": (hi[1] - lo[1]) / dt,
                "recv_bps": (hi[2] - lo[2]) / dt,
            })
    return rows


def _sample_at(samples, t):
    best = None
    for s in samples:
        if s[0] <= t:
            best = s
        else:
            break
    return best
