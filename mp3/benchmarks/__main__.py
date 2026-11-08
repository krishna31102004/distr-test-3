"""CLI for local smoke, remote full trials, persistent workers and final report."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

from ..models import ConsistencyMode
from .controller import run_full
from .matrix import MIN_DURATION_SECONDS
from .preload import preload
from .report import build_final_report
from .smoke import run_smoke
from .worker import run_worker


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python3 -m mp3.benchmarks",
                                     description=__doc__)
    commands = parser.add_subparsers(dest="subcommand", required=True)
    smoke = commands.add_parser("smoke", help="isolated, short ten-process loopback rehearsal")
    smoke.add_argument("--duration", type=float, default=2.5,
                       help="per-client smoke duration in seconds (default 2.5)")
    smoke.add_argument("--key-count", type=int, default=8)
    smoke.add_argument("--output-dir", help="save watermarked local evidence outside the repo")

    full = commands.add_parser("full", help="deploy an isolated remote root and run all 51 trials")
    full.add_argument("--manifest", required=True, help="remote ten-node manifest")
    full.add_argument("--run-dir", required=True, help="local durable result directory")
    full.add_argument("--run-id", required=True, help="unique letters/digits/dash/underscore")
    full.add_argument("--remote-root", required=True,
                      help="must be exactly $HOME/mp3_benchmark_<run-id>")
    full.add_argument("--confirm-isolated-root", required=True,
                      help="must exactly match run-id; never the demo root")
    full.add_argument("--ssh-user", default=os.environ.get("MP3_USER"),
                      help="university NetID (or MP3_USER)")
    full.add_argument("--base-seed", type=int, default=4252026)
    full.add_argument("--retry-failed", action="store_true",
                      help="record a new attempt; do not overwrite prior failure artifacts")
    full.add_argument("--minimum-client-seconds", type=int, default=MIN_DURATION_SECONDS,
                      help=argparse.SUPPRESS)

    shard = commands.add_parser("preload-shard",
                                help="VM-local, restartable one-of-ten preload shard")
    shard.add_argument("--manifest", required=True)
    shard.add_argument("--run-id", required=True)
    shard.add_argument("--mode", required=True)
    shard.add_argument("--cluster-epoch", required=True, type=int)
    shard.add_argument("--key-count", required=True, type=int)
    shard.add_argument("--shard-index", required=True, type=int)
    shard.add_argument("--shard-count", required=True, type=int)
    shard.add_argument("--workers", required=True, type=int)
    shard.add_argument("--report", required=True)
    shard.add_argument("--evidence", required=True)

    worker = commands.add_parser("worker", help="run one persistent closed-loop client")
    worker.add_argument("--manifest", required=True)
    worker.add_argument("--client-id", required=True)
    worker.add_argument("--mode", required=True, choices=[mode.value for mode in ConsistencyMode])
    worker.add_argument("--read-quorum", type=int)
    worker.add_argument("--write-quorum", type=int)
    worker.add_argument("--seed", type=int, required=True)
    worker.add_argument("--key-count", type=int, default=100_000)
    worker.add_argument("--distribution", choices=("uniform", "hot99"), required=True)
    worker.add_argument("--run-id", required=True)
    worker.add_argument("--duration", type=float, required=True)
    worker.add_argument("--raw", required=True)
    worker.add_argument("--summary", required=True)
    worker.add_argument("--gate")
    worker.add_argument("--ready")
    worker.add_argument("--not-for-report", action="store_true")

    report = commands.add_parser("report", help="validate all 51 real trials and build plots/PDF")
    report.add_argument("--run-dir", required=True)
    report.add_argument("--output", required=True)
    report.add_argument("--report-source",
                        help="student-reviewed LaTeX source with placeholders replaced")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.subcommand == "smoke":
            result = run_smoke(duration_seconds=args.duration, key_count=args.key_count,
                               output_dir=args.output_dir)
        elif args.subcommand == "full":
            if args.minimum_client_seconds < MIN_DURATION_SECONDS:
                raise ValueError("full trials cannot be shorter than 180 seconds per client")
            if not args.ssh_user:
                raise ValueError("provide --ssh-user or MP3_USER")
            result = run_full(
                manifest_path=args.manifest, run_dir=args.run_dir, run_id=args.run_id,
                remote_root=args.remote_root, confirm_isolated_root=args.confirm_isolated_root,
                ssh_user=args.ssh_user, base_seed=args.base_seed,
                retry_failed=args.retry_failed)
        elif args.subcommand == "preload-shard":
            result = preload(
                manifest_path=args.manifest, run_id=args.run_id, mode=args.mode,
                key_count=args.key_count, workers=args.workers,
                report_path=args.report, evidence_path=args.evidence,
                presence_path=args.report + ".unused-presence.jsonl",
                cluster_epoch=args.cluster_epoch, shard_index=args.shard_index,
                shard_count=args.shard_count)
        elif args.subcommand == "worker":
            result = run_worker(
                manifest_path=args.manifest, client_id=args.client_id, mode=args.mode,
                read_quorum=args.read_quorum, write_quorum=args.write_quorum,
                seed=args.seed, key_count=args.key_count, distribution=args.distribution,
                run_id=args.run_id, duration_seconds=args.duration, raw_path=args.raw,
                summary_path=args.summary, gate_path=args.gate, ready_path=args.ready,
                not_for_report=args.not_for_report)
        else:
            result = build_final_report(args.run_dir, output=args.output,
                                        report_source=args.report_source)
        print(json.dumps(result, sort_keys=True, indent=2))
        return 0
    except Exception as error:
        print(f"BENCHMARK_ERROR {type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
