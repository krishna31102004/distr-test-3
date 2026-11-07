"""Three required plots. Smoke plots are visibly watermarked NOT FOR REPORT."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from .metrics import sample_summary


def _pyplot():
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise RuntimeError("matplotlib is required to render benchmark plots; install it "
                           "in the benchmark environment") from error
    return plt


def plot_trials(summaries: list[dict], output_dir: str | Path, *, smoke: bool = False) -> list[Path]:
    if not summaries:
        raise ValueError("no trial summaries to plot")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    plt = _pyplot()
    watermark = "NOT FOR REPORT — LOCAL SMOKE DATA" if smoke else None
    outputs = []

    e1 = [row for row in summaries
          if row["spec"]["experiment"] == "exp1"
          or (smoke and row["spec"]["experiment"] == "smoke")]
    if e1:
        groups = defaultdict(list)
        for row in e1:
            spec = row["spec"]
            label = ("Strong" if spec["mode"] == "strong" else
                     f"Eventual-noRR R={spec['read_quorum']}/W={spec['write_quorum']}")
            groups[(label, spec["clients"])].append(row)
        fig, axis = plt.subplots(figsize=(8.2, 4.8), constrained_layout=True)
        for label in dict.fromkeys(key[0] for key in groups):
            points = sorted((clients, values) for (name, clients), values in groups.items()
                            if name == label)
            x = [sample_summary(row["mean_latency_ms_all_terminal"] or 0.0
                                for row in values)["mean"] for _, values in points]
            y = [sample_summary(row["successful_throughput_ops_sec"]
                                for row in values)["mean"] for _, values in points]
            xerr = [sample_summary(row["mean_latency_ms_all_terminal"] or 0.0
                                   for row in values)["sample_sd"] for _, values in points]
            yerr = [sample_summary(row["successful_throughput_ops_sec"]
                                   for row in values)["sample_sd"] for _, values in points]
            clients = [n for n, _ in points]
            axis.errorbar(x, y, xerr=xerr, yerr=yerr, marker="o", capsize=3, label=label)
            for xx, yy, count in zip(x, y, clients):
                axis.annotate(f"{count} clients", (xx, yy), xytext=(4, 4),
                              textcoords="offset points", fontsize=8)
        axis.set_xlabel("Mean end-to-end latency (ms; all terminal operations)")
        axis.set_ylabel("Aggregate successful operations / second")
        axis.set_title("Experiment 1 — Strong and Eventual-noRR")
        axis.grid(True, alpha=0.25)
        axis.legend()
        if watermark:
            axis.text(0.5, 0.5, watermark, transform=axis.transAxes, ha="center",
                      va="center", rotation=22, color="crimson", alpha=0.24,
                      fontsize=17, weight="bold")
        path = output_dir / "exp1_throughput_vs_latency.png"
        fig.savefig(path, dpi=160)
        plt.close(fig)
        outputs.append(path)

    e2 = [row for row in summaries if row["spec"]["experiment"] == "exp2"]
    if e2:
        groups = defaultdict(list)
        for row in e2:
            spec = row["spec"]
            groups[spec["mode"]].append(row)
        labels = list(groups)
        colors = ["#315b7d", "#d17a35"]
        for field, title, filename, ylabel in (
            ("successful_throughput_ops_sec", "Experiment 2 — Throughput",
             "exp2_throughput.png", "Aggregate successful operations / second"),
            ("mean_latency_ms_all_terminal", "Experiment 2 — Mean latency",
             "exp2_mean_latency.png", "Mean end-to-end latency (ms; all terminal operations)"),
        ):
            means, deviations = [], []
            for label in labels:
                values = [row[field] for row in groups[label] if row[field] is not None]
                stats = sample_summary(values)
                means.append(stats["mean"])
                deviations.append(stats["sample_sd"])
            fig, axis = plt.subplots(figsize=(6.6, 4.3), constrained_layout=True)
            axis.bar(labels, means, yerr=deviations, capsize=5, color=colors[:len(labels)])
            axis.set_ylabel(ylabel)
            axis.set_title(title)
            axis.grid(axis="y", alpha=0.25)
            if watermark:
                axis.text(0.5, 0.5, watermark, transform=axis.transAxes, ha="center",
                          va="center", rotation=22, color="crimson", alpha=0.24,
                          fontsize=15, weight="bold")
            path = output_dir / filename
            fig.savefig(path, dpi=160)
            plt.close(fig)
            outputs.append(path)
    return outputs
