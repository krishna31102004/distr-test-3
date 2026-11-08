"""Persistent closed-loop benchmark client; exactly one control RPC at a time."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import socket
import sys
import time

from ..control import ControlError, ControlStatus, send_command
from ..models import Version
from ..ring import load_manifest
from .workload import Workload, unique_value


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def validate_client_configuration(status_reply, *, mode: str,
                                  read_quorum: int | None,
                                  write_quorum: int | None) -> dict:
    """Fail before workload transmission unless this process reports its live settings."""
    details = json.loads(status_reply.detail or "{}")
    if (status_reply.status != ControlStatus.OK or status_reply.mode.value != mode
            or (read_quorum is not None
                and details.get("read_quorum") != read_quorum)
            or (write_quorum is not None
                and details.get("write_quorum") != write_quorum)):
        raise RuntimeError("client node mode/R/W differs from the trial configuration")
    return details


def run_worker(*, manifest_path: str | Path, client_id: str, mode: str,
               read_quorum: int | None, write_quorum: int | None, seed: int,
               key_count: int, distribution: str, run_id: str, duration_seconds: float,
               raw_path: str | Path, summary_path: str | Path,
               gate_path: str | Path | None = None, ready_path: str | Path | None = None,
               not_for_report: bool = False) -> dict:
    manifest = load_manifest(manifest_path)
    manifest.node(client_id)
    if duration_seconds <= 0:
        raise ValueError("worker duration must be positive")
    workload = Workload(seed, key_count, distribution)
    status_reply = send_command(manifest, client_id, "status", timeout=10.0)
    actual_quorums = validate_client_configuration(
        status_reply, mode=mode, read_quorum=read_quorum, write_quorum=write_quorum)
    if ready_path is not None:
        ready = Path(ready_path)
        ready.parent.mkdir(parents=True, exist_ok=True)
        ready.write_text(json.dumps({"client_id": client_id, "hostname": socket.gethostname()})
                         + "\n", encoding="utf-8")
    if gate_path is not None:
        gate = Path(gate_path)
        # Failed controller start attempts must not leave orphaned workers
        # waiting forever or interfering with future benchmark trials.
        gate_deadline = time.monotonic() + 180.0
        while not gate.exists():
            if time.monotonic() >= gate_deadline:
                raise TimeoutError("benchmark start gate was never released")
            time.sleep(0.05)
    start_mono, started_utc = time.monotonic(), utc_now()
    raw_path, summary_path = Path(raw_path), Path(summary_path)
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    operation_index = 0
    with raw_path.open("a", encoding="utf-8", buffering=1) as stream:
        while time.monotonic() - start_mono < duration_seconds or operation_index == 0:
            command, key = workload.operation(client_id, operation_index)
            value = (unique_value(run_id, client_id, operation_index)
                     if command == "put" else b"")
            started = time.monotonic()
            status, version, owners, error, result_bytes = "ERROR", None, [], None, None
            try:
                reply = send_command(manifest, client_id, command, key, value, timeout=25.0)
                status = reply.status.value
                version = None if reply.version is None else {
                    "kind": reply.version.kind, "number": reply.version.number,
                    "writer_id": reply.version.writer_id}
                owners = list(reply.owners)
                error = reply.error
                result_bytes = reply.value if command == "get" and status == "OK" else None
            except (ControlError, OSError, TimeoutError, ValueError) as exc:
                status, error = "TRANSPORT_ERROR", f"{type(exc).__name__}: {exc}"
            finished = time.monotonic()
            row = {
                "schema": 1, "run_id": run_id, "client_id": client_id,
                "operation_index": operation_index, "seed": seed, "command": command,
                "key": key, "status": status, "latency_ms": (finished - started) * 1000.0,
                "completed_utc": utc_now(), "version": version, "owners": owners,
                "value_bytes": (len(value) if command == "put" else
                                None if result_bytes is None else len(result_bytes)),
                "value_sha256": hashlib.sha256(
                    value if command == "put" else result_bytes or b"").hexdigest()
                    if command == "put" or result_bytes is not None else None,
                "error": error,
            }
            stream.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")
            operation_index += 1
            counts[status] = counts.get(status, 0) + 1
            if operation_index % 100 == 0:
                stream.flush()
                os.fsync(stream.fileno())
    elapsed = time.monotonic() - start_mono
    result = {
        "schema": 1, "run_id": run_id, "client_id": client_id, "mode": mode,
        "read_quorum": read_quorum, "write_quorum": write_quorum,
        "seed": seed, "key_count": key_count, "distribution": distribution,
        "requested_duration_seconds": duration_seconds,
        "actual_elapsed_seconds": elapsed, "started_utc": started_utc,
        "finished_utc": utc_now(), "operations": operation_index,
        "status_counts": counts, "hostname": socket.gethostname(),
        "python": sys.version, "platform": platform.platform(),
        "not_for_report": bool(not_for_report),
    }
    summary_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n",
                            encoding="utf-8")
    return result
