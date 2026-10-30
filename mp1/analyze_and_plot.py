import sys
import csv
import statistics
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DEFAULT_CSV = "perf_results.csv"

CATEGORY_ORDER = ["rare", "infrequent", "frequent"]


def load_results(csv_path):
    by_category = defaultdict(list)
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            by_category[row["category"]].append(float(row["latency_seconds"]))
    return by_category


def main():
    csv_path = sys.argv[1] if len(sys.argv) >= 2 else DEFAULT_CSV
    by_category = load_results(csv_path)

    categories = [c for c in CATEGORY_ORDER if c in by_category]
    for c in by_category:
        if c not in categories:
            categories.append(c)

    means = []
    stdevs = []

    print(f"{'category':<12} {'trials':>7} {'mean (s)':>10} {'stdev (s)':>10}")
    for cat in categories:
        values = by_category[cat]
        mean = statistics.mean(values)
        stdev = statistics.stdev(values) if len(values) > 1 else 0.0
        means.append(mean)
        stdevs.append(stdev)
        print(f"{cat:<12} {len(values):>7} {mean:>10.4f} {stdev:>10.4f}")

    fig, ax = plt.subplots(figsize=(6, 3.5))
    x_pos = range(len(categories))
    ax.bar(
        x_pos,
        means,
        yerr=stdevs,
        capsize=6,
        color=["#4C72B0", "#DD8452", "#55A868"]
    )
    ax.set_xticks(list(x_pos))
    ax.set_xticklabels([c.capitalize() for c in categories])
    ax.set_ylabel("Query latency (seconds)")
    ax.set_title("Distributed Grep Query Latency by Pattern Frequency", fontsize=11)
    fig.tight_layout()
    fig.savefig("perf_plot.png", dpi=200)
    print("\nPlot written to perf_plot.png")


if __name__ == "__main__":
    main()