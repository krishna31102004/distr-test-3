import csv
import os
import statistics
from collections import defaultdict
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


# Output folders and plot styles used by every figure.
RESULTS = "results"
FIGS = os.path.join(RESULTS, "figs")
STYLE = {
    "Gossip": dict(color="#1f77b4", marker="o", linestyle="-"),
    "Gossip+S": dict(color="#d62728", marker="s", linestyle="--"),
}


# Read one experiment CSV if it exists.
def load(name):
    path = os.path.join(RESULTS, name)
    if not os.path.exists(path):
        print(f"  (no {name} yet)")
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


# Group raw readings and compute the mean and standard deviation.
def agg(rows, xkey, ykey, series_key="protocol"):
    buckets = defaultdict(lambda: defaultdict(list))
    for r in rows:
        v = r.get(ykey, "")
        if v == "" or v is None:
            continue
        buckets[r[series_key]][float(r[xkey])].append(float(v))
    out = {}
    for s, byx in buckets.items():
        xs = sorted(byx)
        means = [statistics.mean(byx[x]) for x in xs]
        sds = [statistics.stdev(byx[x]) if len(byx[x]) > 1 else 0.0 for x in xs]
        ns = [len(byx[x]) for x in xs]
        out[s] = (xs, means, sds, ns)
    return out


# Draw the Gossip and Gossip+S series with error bars.
def _draw(ax, data, label_fmt="{}"):
    seen = []
    for s in ("Gossip", "Gossip+S"):
        if s not in data:
            continue
        xs, means, sds, ns = data[s]
        seen.extend(xs)
        ax.errorbar(xs, means, yerr=sds, capsize=4, capthick=1.2,
                    linewidth=1.6, markersize=6,
                    label=label_fmt.format(s), **STYLE[s])
        lo = min(ns)
        hi = max(ns)
        print(f"    {s}: {len(xs)} points, n={lo}" + (f"-{hi}" if hi != lo else ""))
    return seen


# Apply labels, ticks, and save each figure.
def _finish(fig, ax, title, xlabel, ylabel, fname,
            xticks=None, legend_loc="best"):
    ax.set_title(title, fontsize=10)
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_ylabel(ylabel, fontsize=9)
    ax.tick_params(labelsize=8)
    if xticks:
        ax.set_xticks(sorted(set(xticks)))
    else:
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.grid(alpha=0.3, linestyle=":")
    ax.legend(fontsize=8, loc=legend_loc, framealpha=0.9)
    fig.tight_layout()
    os.makedirs(FIGS, exist_ok=True)
    for ext in ("png", "pdf"):
        fig.savefig(os.path.join(FIGS, f"{fname}.{ext}"), dpi=200)
    plt.close(fig)
    print(f"  -> {FIGS}/{fname}.png")


# Plot background bandwidth against group size.
def fig_bandwidth():
    print("Fig 1: bandwidth vs group size")
    rows = load("e1.csv")
    if not rows:
        return
    data = agg(rows, "n_nodes", "sent_bps")
    fig, ax = plt.subplots(figsize=(4.4, 3.1))
    xs = _draw(ax, data)
    _finish(fig, ax,
            "Background bandwidth vs group size",
            "Number of VMs in group",
            "Bytes/sec sent per node",
            "fig1_bandwidth", xticks=xs, legend_loc="upper left")


# Plot false positives against receiver-side drop rate.
def fig_false_positives():
    print("Fig 2: false positive rate vs drop rate")
    rows = load("e2.csv")
    if not rows:
        return
    data = agg(rows, "drop_pct", "fp_per_sec")
    fig, ax = plt.subplots(figsize=(4.4, 3.1))
    xs = _draw(ax, data)
    _finish(fig, ax,
            "False positive rate vs drop rate (no failures)",
            "Receiver-side drop rate (%)",
            "False positives / sec (group-wide)",
            "fig2_false_positives", xticks=xs, legend_loc="upper left")
    # Plot refutations separately to help explain Gossip+S behavior.
    ref = agg([r for r in rows if r["protocol"] == "Gossip+S"],
              "drop_pct", "refutations")
    if ref:
        fig, ax = plt.subplots(figsize=(4.4, 3.1))
        xs = _draw(ax, ref)
        _finish(fig, ax,
                "Suspicions refuted before confirmation",
                "Receiver-side drop rate (%)",
                "Refutation events per trial",
                "fig2b_refutations", xticks=xs, legend_loc="upper left")


# Plot first and full-group failure detection times.
def fig_detection():
    print("Fig 3: detection time vs simultaneous failures")
    rows = load("e3.csv")
    if not rows:
        return
    first = agg(rows, "k_failures", "first_detect_s")
    alld = agg(rows, "k_failures", "all_detect_s")
    fig, ax = plt.subplots(figsize=(4.6, 3.2))
    xs = _draw(ax, first, "{} (first)")
    for s in ("Gossip", "Gossip+S"):
        if s not in alld:
            continue
        xs, means, sds, _ = alld[s]
        ax.errorbar(xs, means, yerr=sds, capsize=4, capthick=1.2,
                    linewidth=1.3, markersize=5, alpha=0.55,
                    label=f"{s} (all nodes)",
                    color=STYLE[s]["color"], marker="^", linestyle=":")
    ax.axhline(3.0, color="gray", linewidth=0.9, linestyle="-.", alpha=0.7)
    ax.axhline(6.0, color="gray", linewidth=0.9, linestyle="-.", alpha=0.7)
    x_lo = ax.get_xlim()[0]
    ax.text(x_lo, 3.05, " 3 s bound (first detection)", fontsize=7,
            color="gray", va="bottom", ha="left")
    ax.text(x_lo, 6.05, " 6 s bound (all nodes)", fontsize=7,
            color="gray", va="bottom", ha="left")
    ax.set_ylim(0, 6.8)
    _finish(fig, ax,
            "Detection time vs number of simultaneous failures",
            "Simultaneous failures",
            "Seconds from kill",
            "fig3_detection", xticks=xs, legend_loc="upper right")


# Print a quick text summary before using the numbers in the report.
def summary():
    print("\n--- E2 summary ---")
    for r in load("e2.csv"):
        print(f"  {r['protocol']:>9} {r['drop_pct']:>3}% t{r['trial']}  "
              f"fp={r['false_failed']:>2}  susp={r['suspicions']:>3}  "
              f"refute={r['refutations']:>3}  drops={r['dropped_msgs']:>5}  "
              f"agree={r['nodes_agreeing']}")
    print("\n--- E3 summary ---")
    for r in load("e3.csv"):
        print(f"  {r['protocol']:>9} k={r['k_failures']} t{r['trial']} "
              f"vm{r['victim_vm']}  first={r['first_detect_s']}s  "
              f"all={r['all_detect_s']}s  "
              f"({r['survivors_detected']}/{r['survivors_total']})")


if __name__ == "__main__":
    fig_bandwidth()
    fig_false_positives()
    fig_detection()
    summary()
