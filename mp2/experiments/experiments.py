import argparse
import csv
import os
import statistics
import time
import cluster as C
import parse as P


# Shared folders used for experiment outputs and logs.
RESULTS = C.RESULTS_DIR
LOGS = os.path.join(RESULTS, "logs")


def _trial_dir(name):
    d = os.path.join(LOGS, name)
    os.makedirs(d, exist_ok=True)
    return d


# Find the start and end timestamps for a marked trial window.
def _window(logs, start_mark, end_mark):
    idx = P.marks_index(logs)
    t0, t1 = idx.get(start_mark), idx.get(end_mark)
    if t0 is None or t1 is None:
        raise RuntimeError(f"missing marks {start_mark}/{end_mark}: {list(idx)}")
    return t0, t1


def _victim_ids(logs, victim_vms):
    table = {}
    for nl in logs.values():
        if not nl.memdumps:
            continue
        _, entries = nl.memdumps[-1]
        for e in entries:
            nid = P.entry_id(e)
            for vm in victim_vms:
                if nid.startswith(C.HOSTS[vm - 1] + ":"):
                    table[vm] = nid
    return table


# Append raw trial results without overwriting earlier runs.
def _write_csv(path, rows, fields):
    new = not os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new:
            w.writeheader()
        w.writerows(rows)
    print(f"  -> {path} (+{len(rows)} rows)")
E2_FIELDS = [
    "protocol", "drop_pct", "trial", "duration_s",
    "false_failed", "fp_per_sec", "suspicions", "refutations",
    "suspects_cleared", "dropped_msgs", "nodes_agreeing", "params_ok", "note",
]


# Run one no-failure trial for the false-positive experiment.
def run_e2_trial(drop_pct, mode, trial, window_s, vms=C.ALL_VMS):
    tag = f"e2_{mode}_{drop_pct}pct_t{trial}"
    print(f"[E2] {mode} drop={drop_pct}% trial={trial}")
    C.restart(vms)
    C.settle(15, "converge")
    C.set_mode(mode)
    time.sleep(2)
    C.set_drop_rate(drop_pct)
    time.sleep(2)
    C.mark(vms, "W_START")
    time.sleep(window_s)
    C.mark(vms, "W_END")
    C.dump_mem(vms)
    time.sleep(1.5)
    d = _trial_dir(tag)
    C.collect_logs(vms, d)
    logs = P.parse_dir(d)
    t0, t1 = _window(logs, "W_START", "W_END")
    dur = t1 - t0
    n_fp, detail = P.false_failures(logs, t0, t1)
    susp, ref = P.suspicion_events(logs, t0, t1)
    cleared = P.cleared_count(logs, t0, t1)
    drops = P.dropped_count(logs, t0, t1)
    agree, counts = P.final_membership(logs, expected=len(vms))
    params, params_ok = P.run_params(logs)
    if not params_ok:
        print("  WARNING: nodes report different PARAMS -- mixed config!")
    if detail:
        print(f"  {n_fp} false failure(s):")
        for vm, dt, nid, reason in detail[:6]:
            print(f"    vm{vm} @ +{dt:.2f}s  {nid.split(':')[0]}  [{reason}]")
    print(f"  suspicions={susp} refuted={ref} cleared={cleared} "
          f"drops={drops} agreeing={agree}/{len(vms)}")
    return {
        "protocol": "Gossip+S" if mode == "suspect" else "Gossip",
        "drop_pct": drop_pct,
        "trial": trial,
        "duration_s": round(dur, 2),
        "false_failed": n_fp,
        "fp_per_sec": round(n_fp / dur, 6) if dur > 0 else "",
        "suspicions": susp,
        "refutations": ref,
        "suspects_cleared": cleared,
        "dropped_msgs": drops,
        "nodes_agreeing": agree,
        "params_ok": int(params_ok),
        "note": "" if agree == len(vms) else f"counts={counts}",
    }


def run_e2(rates, trials, window_s):
    path = os.path.join(RESULTS, "e2.csv")
    for rate in rates:
        for mode in ("nosuspect", "suspect"):
            for t in range(1, trials + 1):
                row = run_e2_trial(rate, mode, t, window_s)
                _write_csv(path, [row], E2_FIELDS)


# Use a quick sweep to see where false positives start appearing.
def run_scout(rates=(20, 30, 40, 50), window_s=90):
    print("=== SCOUT: locating the false-positive knee ===")
    path = os.path.join(RESULTS, "scout.csv")
    for rate in rates:
        row = run_e2_trial(rate, "nosuspect", 0, window_s)
        _write_csv(path, [row], E2_FIELDS)
    print("\nPick 5 drop rates spanning the knee, then run e2.")
E3_FIELDS = [
    "protocol", "k_failures", "trial", "victim_vm",
    "first_detect_s", "all_detect_s", "survivors_detected", "survivors_total",
    "false_positives", "nodes_agreeing", "kill_spread_ms",
]


# Kill several nodes together and measure detection time.
def run_e3_trial(k, mode, trial, vms=C.ALL_VMS, wait_after=12):
    victims = [10, 9, 8, 7, 6][:k]
    survivors = [v for v in vms if v not in victims]
    tag = f"e3_{mode}_k{k}_t{trial}"
    print(f"[E3] {mode} k={k} trial={trial} victims={victims}")
    C.restart(vms)
    C.settle(15, "converge")
    C.set_mode(mode)
    time.sleep(2)
    stamps = C.kill_simultaneous(victims)
    good = [t for t in stamps.values() if t]
    if not good:
        raise RuntimeError("no kill timestamps returned")
    kill_t0 = statistics.median(good)
    spread_ms = (max(good) - min(good)) * 1000
    time.sleep(wait_after)
    C.dump_mem(survivors)
    time.sleep(1.5)
    d = _trial_dir(tag)
    C.collect_logs(survivors, d)
    logs = P.parse_dir(d)
    vmap = _victim_ids(logs, victims)
    missing = [v for v in victims if v not in vmap]
    if missing:
        print(f"  WARNING: could not resolve node_id for VMs {missing}")
    det = P.detection_times(logs, list(vmap.values()), kill_t0, survivors)
    agree, counts = P.final_membership(logs, expected=len(vms) - k)
    n_fp, _ = P.false_failures(logs, kill_t0 - 1, kill_t0 + wait_after,
                               victims=set(vmap.values()))
    rows = []
    for vm, nid in sorted(vmap.items()):
        r = det[nid]
        first = r["first"]
        alld = r["all"]
        print(f"  vm{vm}: first={first if first is None else round(first,3)}s "
              f"all={alld if alld is None else round(alld,3)}s "
              f"({r['n_detected']}/{len(survivors)} survivors)")
        rows.append({
            "protocol": "Gossip+S" if mode == "suspect" else "Gossip",
            "k_failures": k,
            "trial": trial,
            "victim_vm": vm,
            "first_detect_s": round(first, 4) if first is not None else "",
            "all_detect_s": round(alld, 4) if alld is not None else "",
            "survivors_detected": r["n_detected"],
            "survivors_total": len(survivors),
            "false_positives": n_fp,
            "nodes_agreeing": agree,
            "kill_spread_ms": round(spread_ms, 1),
        })
    return rows


def run_e3(ks, trials, mode_list=("nosuspect", "suspect")):
    path = os.path.join(RESULTS, "e3.csv")
    for k in ks:
        for mode in mode_list:
            for t in range(1, trials + 1):
                rows = run_e3_trial(k, mode, t)
                _write_csv(path, rows, E3_FIELDS)
E1_FIELDS = [
    "protocol", "n_nodes", "trial", "vm", "bucket", "sent_bps", "recv_bps",
]


# Mark a bandwidth window and record the current byte counters.
def _boundary(vms, label):
    C.mark(vms, label)
    C.bwstats(vms)
    time.sleep(0.4)


# Measure bandwidth for both protocols at one group size.
def run_e1_size(n, trial=1, idle_s=30, bucket_s=None):
    vms = list(range(1, n + 1))
    print(f"[E1] n={n} trial={trial}: {idle_s}s nosuspect, then {idle_s}s suspect")
    C.restart(vms)
    C.settle(8, "converge")
    C.set_mode("nosuspect")
    time.sleep(3)
    _boundary(vms, "PLAIN_START")
    time.sleep(idle_s)
    _boundary(vms, "PLAIN_END")
    C.set_mode("suspect")
    time.sleep(2)
    _boundary(vms, "SUSP_START")
    time.sleep(idle_s)
    _boundary(vms, "SUSP_END")
    C.dump_mem(vms)
    time.sleep(1.5)
    if bucket_s is None:
        bucket_s = max(10.0, min(30.0, idle_s / 3.0))
    d = _trial_dir(f"e1_n{n}_t{trial}")
    C.collect_logs(vms, d)
    logs = P.parse_dir(d)
    agree, counts = P.final_membership(logs, expected=n)
    if agree != n:
        print(f"  WARNING: membership not clean, counts={counts}")
    rows = []
    for proto, (a, b) in (
        ("Gossip", ("PLAIN_START", "PLAIN_END")),
        ("Gossip+S", ("SUSP_START", "SUSP_END")),
    ):
        t0, t1 = _window(logs, a, b)
        for r in P.bandwidth_buckets(logs, t0, t1, bucket_s):
            rows.append({
                "protocol": proto,
                "n_nodes": n,
                "trial": trial,
                "vm": r["vm"],
                "bucket": r["bucket"],
                "sent_bps": round(r["sent_bps"], 2),
                "recv_bps": round(r["recv_bps"], 2),
            })
        sub = [x["sent_bps"] for x in rows if x["protocol"] == proto]
        if sub:
            print(f"  {proto}: mean {statistics.mean(sub):.0f} B/s per node")
    return rows


# Repeat the bandwidth experiment across all requested sizes.
def run_e1(sizes, trials, idle_s):
    path = os.path.join(RESULTS, "e1.csv")
    for n in sizes:
        for t in range(1, trials + 1):
            rows = run_e1_size(n, trial=t, idle_s=idle_s)
            _write_csv(path, rows, E1_FIELDS)


# Parse the command and launch the selected experiment.
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["scout", "e1", "e2", "e3", "push", "stop"])
    ap.add_argument("--trials", type=int, default=2,
                    help="readings per data point; applies to e1, e2 and e3")
    ap.add_argument("--window", type=int, default=120,
                    help="E2 measurement window, seconds")
    ap.add_argument("--idle", type=int, default=30,
                    help="E1 measurement window per protocol, seconds. "
                         "60 is steadier for Gossip+S (refutation bursts have "
                         "less influence on a longer window) at ~2x the time.")
    ap.add_argument("--rates", type=str, default="0,5,10,20,30",
                    help="E2 drop rates in percent")
    ap.add_argument("--ks", type=str, default="1,2,3")
    ap.add_argument("--sizes", type=str, default="5,6,7,8,9,10")
    a = ap.parse_args()
    if C.NETID == "CHANGE_ME":
        raise SystemExit("set MP2_NETID first:  export MP2_NETID=yournetid")
    os.makedirs(RESULTS, exist_ok=True)
    print(f"netid={C.NETID}  warming SSH connections...")
    C.warm_connections()
    started = time.time()
    if a.what == "push":
        C.push()
    elif a.what == "stop":
        C.stop()
    elif a.what == "scout":
        run_scout(window_s=90)
    elif a.what == "e2":
        run_e2([int(x) for x in a.rates.split(",")], a.trials, a.window)
    elif a.what == "e3":
        run_e3([int(x) for x in a.ks.split(",")], a.trials)
    elif a.what == "e1":
        run_e1([int(x) for x in a.sizes.split(",")], a.trials, a.idle)
    print(f"\ndone in {(time.time() - started) / 60:.1f} min")


if __name__ == "__main__":
    main()
