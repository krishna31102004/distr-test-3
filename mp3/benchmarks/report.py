"""Fail-closed real-results validation, plots, and LaTeX compilation."""

from __future__ import annotations

import json
import hashlib
import math
import csv
from collections import defaultdict
from pathlib import Path
import shutil
import statistics
import subprocess

from .metrics import read_jsonl, sample_summary, validate_real_matrix
from .plotting import plot_trials
from .workload import DEFAULT_KEY_COUNT, VALUE_BYTES, Workload, unique_value
from ..ring import load_manifest


def load_summaries(run_dir: str | Path) -> tuple[dict, ...]:
    run_dir = Path(run_dir)
    run_record_path = run_dir / "run.json"
    if not run_record_path.is_file():
        raise ValueError("run.json metadata is missing; only a verified full run is reportable")
    run_record = json.loads(run_record_path.read_text(encoding="utf-8"))
    if run_record.get("target") != "remote_university_vm":
        raise ValueError("run metadata does not identify a real remote university-VM run")
    manifest_path = run_dir / "deployment-config" / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("the exact run manifest is missing")
    manifest = load_manifest(manifest_path)
    if manifest.fingerprint != run_record.get("manifest_id"):
        raise ValueError("run manifest fingerprint does not match run metadata")
    paths = sorted(run_dir.glob("trials/**/summary.json"))
    rows = []
    checked_preloads = set()
    for path in paths:
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
            raw_paths = row.get("raw_files", [])
            if (not isinstance(raw_paths, list)
                    or not isinstance(row.get("workers"), list)
                    or type(row.get("worker_count")) is not int
                    or row["worker_count"] < 1
                    or len(raw_paths) != row.get("worker_count")
                    or len(row["workers"]) != len(raw_paths)):
                raise ValueError(f"{path}: raw files and worker metadata are incomplete")
            expected_clients = [f"vm{i:02d}" for i in range(1, row["worker_count"] + 1)]
            raw_clients = [Path(raw).name.removesuffix(".jsonl") for raw in raw_paths]
            worker_clients = [worker.get("client_id") for worker in row["workers"]]
            if raw_clients != expected_clients or worker_clients != expected_clients:
                raise ValueError(f"{path}: clients must be the unique prescribed vm01..vmNN set")
            distribution = "hot99" if row.get("spec", {}).get("experiment") == "exp2" else "uniform"
            workload = Workload(row["spec"]["seed"], DEFAULT_KEY_COUNT, distribution)
            raw_count = 0
            for raw_index, raw_path in enumerate(raw_paths):
                absolute = (run_dir / raw_path).resolve()
                if run_dir.resolve() not in absolute.parents:
                    raise ValueError(f"{path}: raw path escapes the run directory")
                operations = read_jsonl(absolute)
                expected_client = Path(raw_path).name.removesuffix(".jsonl")
                worker = row.get("workers", [])[raw_index]
                if (worker.get("client_id") != expected_client
                        or worker.get("mode") != row.get("spec", {}).get("mode")
                        or worker.get("seed") != row.get("spec", {}).get("seed")
                        or worker.get("read_quorum") != row.get("spec", {}).get("read_quorum")
                        or worker.get("write_quorum") != row.get("spec", {}).get("write_quorum")
                        or worker.get("not_for_report")):
                    raise ValueError(f"{absolute}: worker metadata differs from trial")
                indices = []
                for operation in operations:
                    latency = operation.get("latency_ms")
                    index = operation.get("operation_index")
                    key = operation.get("key")
                    key_index = (int(key.removeprefix("bench-key-"))
                                 if isinstance(key, str) and key.startswith("bench-key-")
                                 and key.removeprefix("bench-key-").isdigit() else -1)
                    value_hash = operation.get("value_sha256")
                    if (operation.get("command") not in {"get", "put"}
                            or type(operation.get("latency_ms")) not in (int, float)
                            or not math.isfinite(latency) or latency < 0
                            or not isinstance(operation.get("status"), str)
                            or not operation["status"]
                            or operation.get("client_id") != expected_client
                            or type(index) is not int or index < 0
                            or operation.get("command") != (
                                "get" if index % 2 == 0 else "put")
                            or key_index < 0 or key_index >= DEFAULT_KEY_COUNT
                            or (operation.get("command") == "put"
                                and (operation.get("value_bytes") != 1024
                                     or not isinstance(value_hash, str)
                                     or len(value_hash) != 64
                                     or any(c not in "0123456789abcdef" for c in value_hash)))
                            or (operation.get("command") == "get"
                                and operation.get("value_bytes") is not None
                                and (type(operation["value_bytes"]) is not int
                                     or operation["value_bytes"] < 0))
                            or (operation.get("command") == "get"
                                and operation.get("status") == "OK"
                                and (operation.get("value_bytes") != 1024
                                     or not isinstance(value_hash, str)
                                     or len(value_hash) != 64
                                     or any(c not in "0123456789abcdef" for c in value_hash)))):
                        raise ValueError(f"{absolute}: malformed operation or non-1-KiB PUT")
                    if operation.get("run_id") != run_record.get("run_id"):
                        raise ValueError(f"{absolute}: operation belongs to another run")
                    if operation.get("seed") != row["spec"].get("seed"):
                        raise ValueError(f"{absolute}: operation seed differs from trial")
                    expected_command, expected_key = workload.operation(expected_client, index)
                    if (operation["command"] != expected_command or key != expected_key):
                        raise ValueError(f"{absolute}: key/operation differs from deterministic workload")
                    if (operation["command"] == "put"
                            and value_hash != hashlib.sha256(unique_value(
                                run_record["run_id"], expected_client, index,
                                size=VALUE_BYTES)).hexdigest()):
                        raise ValueError(f"{absolute}: PUT bytes differ from deterministic value")
                    indices.append(index)
                if sorted(indices) != list(range(len(indices))):
                    raise ValueError(f"{absolute}: operation indexes are not contiguous")
                if (raw_index >= len(row.get("workers", []))
                        or len(operations) != worker.get("operations")):
                    # raw_files are emitted in the same order as workers by the controller.
                    raise ValueError(f"{absolute}: worker raw count does not match summary")
                raw_count += len(operations)
                # Keep rows for independently recomputing reported metrics below.
                if "_verified_operations" not in row:
                    row["_verified_operations"] = []
                row["_verified_operations"].extend(operations)
            if raw_count != row.get("operations"):
                raise ValueError(f"{path}: raw operation rows do not match summary")
            _verify_summary_metrics(path, row)
            preload = row.get("preload_evidence", {})
            if preload.get("mode") != row.get("spec", {}).get("mode"):
                raise ValueError(f"{path}: preload mode differs from trial mode")
            preload_id = preload.get("content_evidence_sha256")
            if preload_id not in checked_preloads:
                _verify_preload_evidence(run_dir, preload, manifest,
                                         expected_run_id=run_record.get("run_id"),
                                         expected_mode=row.get("spec", {}).get("mode"))
                checked_preloads.add(preload_id)
            client_hosts = row.get("environment", {}).get("client_hosts", [])
            if (not isinstance(client_hosts, list)
                    or len(client_hosts) != row.get("worker_count")
                    or any(not isinstance(host, str) or not host for host in client_hosts)
                    or len(set(client_hosts)) != len(client_hosts)
                    or any(host in {"127.0.0.1", "localhost", "::1"}
                           for host in client_hosts)):
                raise ValueError(f"{path}: remote trial is missing its ten-VM client hosts")
            rows.append(row)
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"invalid trial summary {path}: {error}") from error
    return validate_real_matrix(rows, base_seed=run_record.get("base_seed", 4252026))


def _verify_summary_metrics(path: Path, row: dict) -> None:
    """Recompute trial totals from retained raw operations before report use."""
    operations = row.pop("_verified_operations", [])
    success = [op for op in operations if op["status"] == "OK"]
    status_counts = {}
    for op in operations:
        status_counts[op["status"]] = status_counts.get(op["status"], 0) + 1
    all_latency = statistics.fmean(op["latency_ms"] for op in operations) if operations else None
    success_latency = statistics.fmean(op["latency_ms"] for op in success) if success else None
    interval = row.get("measurement_interval_seconds")
    if type(interval) not in (int, float) or not math.isfinite(interval) or interval <= 0:
        raise ValueError(f"{path}: invalid measurement interval")
    workers = row.get("workers", [])
    skew = row.get("environment", {}).get("gate_release_skew_upper_seconds")
    if (type(skew) not in (int, float) or not math.isfinite(skew) or skew < 0
            or not workers):
        raise ValueError(f"{path}: missing gate-release skew or worker durations")
    expected_interval = max(worker["actual_elapsed_seconds"] for worker in workers) + skew
    if not math.isclose(interval, expected_interval, rel_tol=1e-9, abs_tol=1e-9):
        raise ValueError(f"{path}: measurement interval disagrees with worker durations and gate skew")
    expected = {
        "operations": len(operations),
        "raw_operation_count": len(operations),
        "successful_operations": len(success),
        "failed_operations": len(operations) - len(success),
        "status_counts": status_counts,
        "mean_latency_ms_all_terminal": all_latency,
        "mean_latency_ms_successful": success_latency,
        "successful_throughput_ops_sec": len(success) / interval,
    }
    for field, value in expected.items():
        actual = row.get(field)
        if isinstance(value, float):
            if (type(actual) not in (int, float) or not math.isfinite(actual)
                    or not math.isclose(actual, value, rel_tol=1e-9, abs_tol=1e-9)):
                raise ValueError(f"{path}: {field} disagrees with retained raw operations")
        elif actual != value:
            raise ValueError(f"{path}: {field} disagrees with retained raw operations")


def _validate_indexed_keys(rows: list[dict], *, key_count: int,
                           key_prefix: str) -> dict[str, int]:
    """Validate an exact one-row-per-index benchmark key evidence stream."""
    by_key = {}
    for row in rows:
        key, index = row.get("key"), row.get("index")
        if (type(index) is not int or not 0 <= index < key_count
                or key != f"{key_prefix}{index:06d}" or key in by_key):
            raise ValueError("preload evidence has duplicate, missing, or invalid key indexes")
        by_key[key] = index
    if len(by_key) != key_count:
        raise ValueError("preload evidence does not cover the exact key set")
    return by_key


def _validate_replica_content_checks(row: dict, expected_owners: list[str],
                                    expected_hash: str) -> None:
    """Require exact preloaded bytes to have been read from each prescribed owner."""
    checks = row.get("replica_content_checks")
    if (not isinstance(checks, list) or len(checks) != 5
            or any(not isinstance(check, dict) for check in checks)
            or [check.get("owner") for check in checks] != expected_owners
            or any(check.get("status") != "OK"
                   or check.get("value_verified") is not True
                   or check.get("value_sha256") != expected_hash
                   or not isinstance(check.get("version"), str)
                   or not check["version"]
                   for check in checks)):
        raise ValueError("preload content row lacks exact-value evidence from all five owners")


def write_validated_csvs(summaries: tuple[dict, ...] | list[dict],
                         output_dir: str | Path) -> tuple[Path, Path]:
    """Write auditable per-trial and per-configuration CSVs from validated summaries."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    trial_path = output_dir / "trial_metrics.csv"
    trial_fields = ("trial_id", "experiment", "mode", "read_quorum", "write_quorum",
                    "clients", "trial", "seed", "duration_seconds", "operations",
                    "successful_operations", "failed_operations",
                    "measurement_interval_seconds", "minimum_worker_elapsed_seconds",
                    "maximum_worker_elapsed_seconds",
                    "successful_throughput_ops_sec", "mean_latency_ms_all_terminal",
                    "mean_latency_ms_successful")
    with trial_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=trial_fields, extrasaction="ignore")
        writer.writeheader()
        for row in summaries:
            workers = row.get("workers", [])
            writer.writerow({**row.get("spec", {}), **row,
                             "minimum_worker_elapsed_seconds": min(
                                 worker["actual_elapsed_seconds"] for worker in workers),
                             "maximum_worker_elapsed_seconds": max(
                                 worker["actual_elapsed_seconds"] for worker in workers)})

    groups = defaultdict(list)
    for row in summaries:
        spec = row["spec"]
        key = (spec["experiment"], spec["mode"], spec["read_quorum"],
               spec["write_quorum"], spec["clients"])
        groups[key].append(row)
    aggregate_path = output_dir / "configuration_summary.csv"
    aggregate_fields = ("experiment", "mode", "read_quorum", "write_quorum", "clients",
                        "trial_count", "throughput_mean_ops_sec", "throughput_sample_sd",
                        "latency_mean_ms", "latency_sample_sd_ms")
    with aggregate_path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=aggregate_fields)
        writer.writeheader()
        for key in sorted(groups, key=lambda item: tuple("" if v is None else str(v)
                                                        for v in item)):
            rows = groups[key]
            throughput = sample_summary(row["successful_throughput_ops_sec"] for row in rows)
            latency = sample_summary(row["mean_latency_ms_all_terminal"] for row in rows)
            writer.writerow(dict(zip(("experiment", "mode", "read_quorum", "write_quorum",
                                      "clients"), key)) | {
                "trial_count": len(rows), "throughput_mean_ops_sec": throughput["mean"],
                "throughput_sample_sd": throughput["sample_sd"],
                "latency_mean_ms": latency["mean"],
                "latency_sample_sd_ms": latency["sample_sd"],
            })
    return trial_path, aggregate_path


def _verify_preload_evidence(run_dir: Path, report: dict, manifest, *,
                             expected_run_id: str | None = None,
                             expected_mode: str | None = None) -> None:
    from .workload import preload_value

    if (report.get("complete") is not True or report.get("key_count") != 100_000
            or report.get("replication_factor") != 5
            or report.get("manifest_id") != manifest.fingerprint
            or (expected_run_id is not None and report.get("run_id") != expected_run_id)
            or (expected_mode is not None and report.get("mode") != expected_mode)):
        raise ValueError("preload report must prove 100,000 keys and RF=5")
    content_path = (run_dir / report.get("content_evidence_file", "")).resolve()
    presence_path = (run_dir / report.get("owner_presence_file", "")).resolve()
    if run_dir.resolve() not in content_path.parents or run_dir.resolve() not in presence_path.parents:
        raise ValueError("preload evidence paths must remain inside the run directory")
    content_rows = read_jsonl(content_path)
    presence_rows = read_jsonl(presence_path)
    if len(content_rows) != 100_000 or len(presence_rows) != 100_000:
        raise ValueError("preload evidence must contain one content and owner row per key")
    digest = hashlib.sha256()
    key_prefix = report.get("key_prefix")
    content_by_key = _validate_indexed_keys(content_rows, key_count=100_000,
                                             key_prefix=key_prefix)
    for row in content_rows:
        key, index = row.get("key"), row.get("index")
        expected_hash = hashlib.sha256(preload_value(report["run_id"], index)).hexdigest()
        encoded = json.dumps(row, sort_keys=True, ensure_ascii=False)
        digest.update(encoded.encode("utf-8") + b"\n")
        expected_owners = [node.vm_id for node in manifest.owners(key)]
        checks = row.get("replica_content_checks")
        if (row.get("content_verified") is not True
                or row.get("owners") != expected_owners
                or row.get("content_verified_owner") != expected_owners[0]
                or row.get("attempts", 0) < 1 or not row.get("version")
                or row.get("value_sha256") != expected_hash):
            raise ValueError("preload content row lacks exact durable setup evidence")
        _validate_replica_content_checks(row, expected_owners, expected_hash)
        if row.get("content_verified_version") != checks[0]["version"]:
            raise ValueError("preload sample-owner version disagrees with owner content checks")
        content_by_key[key] = index
    if digest.hexdigest() != report.get("content_evidence_sha256"):
        raise ValueError("preload content evidence digest mismatch")
    presence_by_key = _validate_indexed_keys(presence_rows, key_count=100_000,
                                              key_prefix=key_prefix)
    for row in presence_rows:
        key, index = row.get("key"), row.get("index")
        if content_by_key.get(key) != index:
            raise ValueError("preload owner evidence has duplicate or mismatched key indexes")
        owners, present = row.get("prescribed_owners", []), row.get("present_owners", [])
        expected_owners = [node.vm_id for node in manifest.owners(key)]
        if (row.get("complete") is not True or owners != expected_owners or len(owners) != 5
                or len(set(owners)) != 5 or len(present) != 5
                or len(set(present)) != 5 or set(present) != set(owners)):
            raise ValueError("preload owner inventory does not prove five copies")
    if presence_by_key != content_by_key:
        raise ValueError("preload owner evidence does not match content key indexes")


def build_final_report(run_dir: str | Path, *, output: str | Path,
                       report_source: str | Path | None = None) -> dict:
    run_dir, output = Path(run_dir), Path(output)
    summaries = load_summaries(run_dir)
    csv_paths = write_validated_csvs(summaries, run_dir / "aggregates")
    transcripts = run_dir / "llm_use"
    if not transcripts.is_dir() or not any(p.is_file() for p in transcripts.rglob("*")):
        raise ValueError("the final report requires the group's truthful LLM-use transcripts "
                         "under <run-dir>/llm_use/")
    plot_dir = run_dir / "plots"
    plots = plot_trials(list(summaries), plot_dir)
    if len(plots) != 3:
        raise ValueError("final report requires all three validated plots")
    source = Path(report_source) if report_source else Path(__file__).resolve().parents[1] / "report" / "report.tex"
    if not source.is_file():
        raise FileNotFoundError(f"report template missing: {source}")
    output.parent.mkdir(parents=True, exist_ok=True)
    build_dir = run_dir / "latex_build"
    build_dir.mkdir(parents=True, exist_ok=True)
    # Copy only the selected real figures and template to a reproducible build dir.
    tex_source = source.read_text(encoding="utf-8")
    if "\\pending{" in tex_source:
        raise ValueError("replace every \\pending{...} placeholder with student-reviewed "
                         "design and measured discussion before building the final report")
    if "\\documentclass[12pt]" not in tex_source:
        raise ValueError("the report source must use 12-point type")
    llm_section = tex_source.split("\\textbf{LLM use}", 1)[-1].split("\\newpage", 1)[0]
    llm_words = len(__import__("re").findall(r"[A-Za-z0-9]+", __import__("re").sub(
        r"\\[A-Za-z]+(?:\{[^}]*\})?", " ", llm_section)))
    if llm_words >= 300:
        raise ValueError(f"LLM-use section has {llm_words} words; keep it below 300")
    (build_dir / "report.tex").write_text(
        tex_source.replace("\\pendingmeasurementstrue", "\\pendingmeasurementsfalse"),
        encoding="utf-8")
    for path in plots:
        shutil.copy2(path, build_dir / path.name)
    engine = shutil.which("pdflatex")
    if not engine:
        raise RuntimeError("pdflatex is not installed; install a LaTeX distribution before "
                           "building the final PDF")
    for _ in range(2):
        subprocess.run([engine, "-halt-on-error", "-interaction=nonstopmode", "report.tex"],
                       cwd=build_dir, check=True, stdout=subprocess.PIPE,
                       stderr=subprocess.STDOUT, text=True)
    built = build_dir / "report.pdf"
    if not built.is_file():
        raise RuntimeError("LaTeX completed without creating report.pdf")
    pdfinfo = shutil.which("pdfinfo")
    if not pdfinfo:
        raise RuntimeError("pdfinfo is required to verify the final page count")
    details = subprocess.run([pdfinfo, str(built)], check=True, capture_output=True, text=True)
    page_line = next((line for line in details.stdout.splitlines()
                      if line.startswith("Pages:")), None)
    if page_line is None or int(page_line.split(":", 1)[1].strip()) >= 3:
        raise RuntimeError("final report must be fewer than three pages")
    shutil.copy2(built, output)
    return {"pdf": str(output), "pages": int(page_line.split(":", 1)[1].strip()),
            "plots": [str(path) for path in plots],
            "csv_outputs": [str(path) for path in csv_paths],
            "validated_trials": len(summaries)}
