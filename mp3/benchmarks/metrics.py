"""Validated trial aggregation and fail-closed final-result matrix checks."""

from __future__ import annotations

import json
import hashlib
import ipaddress
import math
import statistics
from pathlib import Path
from typing import Iterable

from .matrix import MIN_DURATION_SECONDS, full_matrix
from .workload import DEFAULT_KEY_COUNT, VALUE_BYTES, Workload, unique_value


def read_jsonl(path: str | Path) -> list[dict]:
    rows = []
    with Path(path).open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"{path}:{line_number}: invalid JSONL: {error}") from error
            if not isinstance(row, dict):
                raise ValueError(f"{path}:{line_number}: row must be an object")
            rows.append(row)
    return rows


def trial_summary(*, spec: dict, operation_rows: Iterable[dict], workers: Iterable[dict],
                  measurement_interval_seconds: float, environment: dict,
                  preload_evidence: dict, target: str, not_for_report: bool = False) -> dict:
    ops, worker_rows = list(operation_rows), list(workers)
    if (type(measurement_interval_seconds) not in (int, float)
            or not math.isfinite(measurement_interval_seconds)
            or measurement_interval_seconds <= 0):
        raise ValueError("measurement interval must be finite and positive")
    if len(worker_rows) != spec["clients"]:
        raise ValueError("one worker result is required per configured client")
    for worker in worker_rows:
        elapsed = worker.get("actual_elapsed_seconds")
        if (type(elapsed) not in (int, float) or not math.isfinite(elapsed)
                or elapsed < spec["duration_seconds"]):
            raise ValueError("a worker did not complete the required per-client duration")
    if not preload_evidence.get("complete"):
        raise ValueError("complete five-owner preload evidence is required")
    successful = [row for row in ops if row.get("status") == "OK"]
    terminal = [row for row in ops if type(row.get("latency_ms")) in (float, int)]
    latency = statistics.fmean(row["latency_ms"] for row in terminal) if terminal else None
    success_latency = (statistics.fmean(row["latency_ms"] for row in successful)
                       if successful else None)
    statuses: dict[str, int] = {}
    for row in ops:
        status = str(row.get("status", "MALFORMED"))
        statuses[status] = statuses.get(status, 0) + 1
    return {
        "schema": 1, "trial_id": spec["trial_id"], "spec": spec,
        "target": target, "not_for_report": bool(not_for_report),
        "environment": environment, "preload_evidence": preload_evidence,
        "worker_count": len(worker_rows), "workers": worker_rows,
        "measurement_interval_seconds": measurement_interval_seconds,
        "operations": len(ops), "successful_operations": len(successful),
        "failed_operations": len(ops) - len(successful), "status_counts": statuses,
        "successful_throughput_ops_sec": len(successful) / measurement_interval_seconds,
        "mean_latency_ms_all_terminal": latency,
        "mean_latency_ms_successful": success_latency,
        "raw_operation_count": len(ops),
    }


def sample_summary(values: Iterable[float]) -> dict:
    values = tuple(float(value) for value in values)
    if not values or any(not math.isfinite(value) for value in values):
        raise ValueError("at least one finite trial value is required")
    return {"mean": statistics.fmean(values),
            "sample_sd": statistics.stdev(values) if len(values) > 1 else 0.0,
            "n": len(values)}


def validate_real_matrix(summaries: Iterable[dict], *, base_seed: int = 4252026
                         ) -> tuple[dict, ...]:
    rows = tuple(summaries)
    expected = {row.trial_id: row for row in full_matrix(base_seed)}
    by_id = {}
    for row in rows:
        spec = row.get("spec", {})
        trial_id = spec.get("trial_id")
        if not trial_id or trial_id not in expected:
            raise ValueError(f"unexpected or malformed full trial: {trial_id!r}")
        if trial_id in by_id:
            raise ValueError(f"duplicate trial summary: {trial_id}")
        expected_spec = expected[trial_id]
        for field in ("experiment", "mode", "read_quorum", "write_quorum", "clients",
                      "trial", "seed"):
            if spec.get(field) != getattr(expected_spec, field):
                raise ValueError(f"{trial_id} has incorrect {field} configuration")
        if row.get("target") != "remote_university_vm" or row.get("not_for_report"):
            raise ValueError(f"{trial_id} is not an eligible real remote trial")
        if spec.get("duration_seconds", 0) < MIN_DURATION_SECONDS:
            raise ValueError(f"{trial_id} requested less than 180 seconds per client")
        if row.get("worker_count") != spec.get("clients"):
            raise ValueError(f"{trial_id} has an incomplete client result set")
        workers = row.get("workers")
        if (not isinstance(workers, list) or len(workers) != spec["clients"]
                or any(type(worker.get("actual_elapsed_seconds")) not in (int, float)
                       or not math.isfinite(worker["actual_elapsed_seconds"])
                       or worker["actual_elapsed_seconds"] < spec["duration_seconds"]
                       for worker in workers)):
            raise ValueError(f"{trial_id} has a client below the required measured duration")
        preload = row.get("preload_evidence", {})
        if (preload.get("complete") is not True
                or preload.get("all_five_owners_verified") is not True
                or preload.get("key_count") != 100_000
                or preload.get("replication_factor") != 5):
            raise ValueError(f"{trial_id} lacks complete all-replica preload evidence")
        if row.get("raw_operation_count") != row.get("operations"):
            raise ValueError(f"{trial_id} raw operation count does not match summary")
        if not row.get("environment", {}).get("hostname"):
            raise ValueError(f"{trial_id} is missing real environment metadata")
        raw_files = row.get("raw_files")
        if not isinstance(raw_files, list) or not raw_files:
            raise ValueError(f"{trial_id} has no retained raw operation files")
        worker_count = sum(worker.get("operations", 0) for worker in workers)
        if worker_count != row.get("operations"):
            raise ValueError(f"{trial_id} worker operation counts are incomplete")
        by_id[trial_id] = row
    missing = sorted(set(expected) - set(by_id))
    if missing:
        raise ValueError(f"final report needs all 51 valid trials; missing {len(missing)}: "
                         + ", ".join(missing[:5]))
    return tuple(by_id[key] for key in sorted(by_id))


def write_json_atomic(path: str | Path, data: dict) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def complete_checkpoint(summary: dict, run_dir: str | Path, expected_spec: dict) -> bool:
    """True only for a full-duration, remote checkpoint retaining all raw operation files."""
    root = Path(run_dir).resolve()
    if (summary.get("spec") != expected_spec
            or summary.get("target") != "remote_university_vm"
            or summary.get("not_for_report")
            or summary.get("worker_count") != expected_spec.get("clients")
            or not summary.get("preload_evidence", {}).get("complete")
            or summary.get("preload_evidence", {}).get("key_count") != DEFAULT_KEY_COUNT
            or summary.get("preload_evidence", {}).get("replication_factor") != 5
            or summary.get("preload_evidence", {}).get("mode") != expected_spec.get("mode")
            or summary.get("preload_evidence", {}).get("all_five_owners_verified") is not True):
        return False
    workers, raw_files = summary.get("workers", []), summary.get("raw_files", [])
    if len(workers) != expected_spec.get("clients") or len(raw_files) != len(workers):
        return False
    try:
        operation_total = 0
        operations = []
        distribution = "hot99" if expected_spec.get("experiment") == "exp2" else "uniform"
        workload = Workload(expected_spec["seed"], DEFAULT_KEY_COUNT, distribution)
        expected_clients = [f"vm{i:02d}" for i in range(1, expected_spec["clients"] + 1)]
        if ([worker.get("client_id") for worker in workers] != expected_clients
                or [Path(path).name.removesuffix(".jsonl") for path in raw_files]
                != expected_clients):
            return False
        for worker, raw_path in zip(workers, raw_files):
            elapsed = worker.get("actual_elapsed_seconds")
            if (type(elapsed) not in (int, float) or not math.isfinite(elapsed)
                    or elapsed < expected_spec.get("duration_seconds", MIN_DURATION_SECONDS)):
                return False
            client_id = worker.get("client_id")
            if (not isinstance(client_id, str) or worker.get("mode") != expected_spec["mode"]
                    or worker.get("seed") != expected_spec["seed"]
                    or worker.get("read_quorum") != expected_spec["read_quorum"]
                    or worker.get("write_quorum") != expected_spec["write_quorum"]
                    or worker.get("not_for_report")):
                return False
            resolved = (root / raw_path).resolve()
            if root not in resolved.parents or not resolved.is_file():
                return False
            rows = read_jsonl(resolved)
            if len(rows) != worker.get("operations"):
                return False
            indices = []
            for operation in rows:
                index = operation.get("operation_index")
                if (type(index) is not int or operation.get("run_id") !=
                        summary.get("preload_evidence", {}).get("run_id")
                        or operation.get("client_id") != client_id
                        or operation.get("seed") != expected_spec["seed"]
                        or not isinstance(operation.get("status"), str)
                        or not operation["status"]
                        or type(operation.get("latency_ms")) not in (int, float)
                        or not math.isfinite(operation["latency_ms"])
                        or operation["latency_ms"] < 0):
                    return False
                command, key = workload.operation(client_id, index)
                if operation.get("command") != command or operation.get("key") != key:
                    return False
                if command == "put":
                    expected_hash = hashlib.sha256(
                        unique_value(operation["run_id"], client_id, index)).hexdigest()
                    if (operation.get("value_bytes") != VALUE_BYTES
                            or operation.get("value_sha256") != expected_hash):
                        return False
                elif operation.get("status") == "OK":
                    value_hash = operation.get("value_sha256")
                    if (operation.get("value_bytes") != VALUE_BYTES
                            or not isinstance(value_hash, str) or len(value_hash) != 64
                            or any(char not in "0123456789abcdef" for char in value_hash)):
                        return False
                indices.append(index)
            if sorted(indices) != list(range(len(indices))):
                return False
            operation_total += len(rows)
            operations.extend(rows)
        if not (operation_total == summary.get("operations")
                == summary.get("raw_operation_count")):
            return False
        environment = summary.get("environment", {})
        hosts = environment.get("client_hosts", [])
        skew = environment.get("gate_release_skew_upper_seconds")
        if (not isinstance(hosts, list) or len(hosts) != len(workers)
                or any(not isinstance(host, str) or not host for host in hosts)
                or any(host in {"localhost", "localhost.localdomain"}
                       or _is_loopback_address(host) for host in hosts)
                or len(set(hosts)) != len(hosts)
                or type(skew) not in (int, float) or not math.isfinite(skew) or skew < 0
                or not math.isclose(summary.get("measurement_interval_seconds"),
                    max(worker["actual_elapsed_seconds"] for worker in workers) + skew,
                    rel_tol=1e-9, abs_tol=1e-9)):
            return False
        recalculated = trial_summary(
            spec=expected_spec, operation_rows=operations, workers=workers,
            measurement_interval_seconds=summary["measurement_interval_seconds"],
            environment=summary.get("environment", {}),
            preload_evidence=summary.get("preload_evidence", {}),
            target="remote_university_vm")
        for field in ("operations", "raw_operation_count", "successful_operations",
                      "failed_operations", "status_counts", "mean_latency_ms_all_terminal",
                      "mean_latency_ms_successful", "successful_throughput_ops_sec"):
            actual, expected = summary.get(field), recalculated.get(field)
            if isinstance(expected, float):
                if (type(actual) not in (int, float) or not math.isclose(
                        actual, expected, rel_tol=1e-9, abs_tol=1e-9)):
                    return False
            elif actual != expected:
                return False
        return True
    except (OSError, ValueError, TypeError):
        return False


def _is_loopback_address(host: str) -> bool:
    try:
        return ipaddress.ip_address(host.strip("[]")).is_loopback
    except ValueError:
        return False
