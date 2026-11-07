"""Isolated ten-node loopback benchmark smoke; never falls back to remote deployment."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import multiprocessing
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import time

from ..control import ControlStatus, send_command
from ..models import ConsistencyMode
from ..ring import Endpoint, Manifest
from .metrics import trial_summary, read_jsonl
from .plotting import plot_trials
from .preload import preload
from .worker import run_worker
from .workload import Workload


def _free_base() -> int:
    for base in range(32000, 60000 - 40, 47):
        sockets = []
        try:
            for offset in range(40):
                sock = socket.socket()
                sock.bind(("127.0.0.1", base + offset))
                sockets.append(sock)
            return base
        except OSError:
            pass
        finally:
            for sock in sockets:
                sock.close()
    raise RuntimeError("could not find 40 free loopback ports for ten local nodes")


def _manifest(root: Path) -> tuple[Path, str]:
    base = _free_base()
    services = ("kv", "membership", "log_query", "control")
    endpoints = {}
    for index in range(1, 11):
        endpoints[f"vm{index:02d}"] = {
            service: Endpoint("127.0.0.1", base + (index - 1) * 4 + offset)
            for offset, service in enumerate(services)
        }
    manifest = Manifest.from_endpoints(endpoints)
    path = root / "manifest.json"
    path.write_text(manifest.to_json() + "\n", encoding="utf-8")
    release = hashlib.sha256(b"mp3-local-benchmark-smoke").hexdigest()[:16]
    inventory = {"manifest_id": manifest.fingerprint,
                 "nodes": {node.vm_id: {"release_id": release, "schema_version": 3}
                           for node in manifest.nodes}}
    inventory_path = root / "inventory.json"
    inventory_path.write_text(json.dumps(inventory, sort_keys=True) + "\n", encoding="utf-8")
    return path, inventory_path


def _healthy(manifest_path: Path, timeout: float = 25.0) -> None:
    from ..ring import load_manifest
    manifest = load_manifest(manifest_path)
    deadline = time.monotonic() + timeout
    last_error = "not ready"
    while time.monotonic() < deadline:
        try:
            for node in manifest.nodes:
                status = send_command(manifest, node.vm_id, "status", timeout=1.0)
                details = json.loads(status.detail)
                if (status.status != ControlStatus.OK or status.mode != ConsistencyMode.STRONG
                        or details.get("read_quorum") != 2
                        or details.get("write_quorum") != 2):
                    raise RuntimeError(f"{node.vm_id} has unexpected mode or R/W")
                if details.get("generation") != 1:
                    raise RuntimeError(f"{node.vm_id} did not start at generation 1")
                members = send_command(manifest, node.vm_id, "list_mem_ids", timeout=1.0)
                rows = json.loads(members.detail).get("rows", [])
                if (len(rows) != 10 or any(row.get("status") != "ALIVE" for row in rows)):
                    raise RuntimeError(f"{node.vm_id} does not see ten ALIVE members")
                if any(row.get("generation") != 1 for row in rows):
                    raise RuntimeError(f"{node.vm_id} has not learned all ten current generations")
            return
        except Exception as error:
            last_error = str(error)
            time.sleep(0.15)
    raise TimeoutError(f"ten-node smoke cluster did not become healthy: {last_error}")


def run_smoke(*, duration_seconds: float = 2.5, key_count: int = 8,
              output_dir: str | Path | None = None) -> dict:
    if os.environ.get("MP3_TARGET") != "local":
        raise RuntimeError("smoke refuses to run unless MP3_TARGET=local is explicitly set")
    if duration_seconds <= 0 or key_count < 2:
        raise ValueError("smoke needs a positive duration and at least two keys")
    repo_root = Path(__file__).resolve().parents[2]
    run_id = "smoke-" + hashlib.sha256(os.urandom(16)).hexdigest()[:10]
    published_artifacts = (Path(output_dir).resolve() / run_id if output_dir is not None
                           else Path(tempfile.gettempdir()) / f"mp3-benchmark-{run_id}")
    with tempfile.TemporaryDirectory(prefix="mp3-benchmark-smoke-") as temp:
        root = Path(temp)
        manifest_path, inventory_path = _manifest(root)
        node_logs = []
        processes: list[subprocess.Popen] = []
        try:
            node_dir = root / "nodes"
            node_dir.mkdir()
            env = os.environ.copy()
            env["PYTHONPATH"] = str(repo_root)
            env["PYTHONUNBUFFERED"] = "1"
            for index in range(1, 11):
                vm_id = f"vm{index:02d}"
                log_path = node_dir / f"{vm_id}.out"
                handle = log_path.open("wb")
                node_logs.append(handle)
                command = [sys.executable, "-B", "-m", "mp3.node", "--vm-id", vm_id,
                           "--manifest", str(manifest_path), "--db",
                           str(node_dir / f"{vm_id}.db"), "--bootstrap",
                           "--bootstrap-mode", "strong", "--read-quorum", "2",
                           "--write-quorum", "2", "--schema2-inventory",
                           str(inventory_path), "--log",
                           str(node_dir / f"machine.{index}.log")]
                processes.append(subprocess.Popen(command, cwd=repo_root, env=env,
                                                  stdout=handle, stderr=subprocess.STDOUT))
            _healthy(manifest_path)
            preload_report = preload(
                manifest_path=manifest_path, run_id=run_id, mode=ConsistencyMode.STRONG,
                key_count=key_count, max_attempts=3, workers=4,
                report_path=root / "preload.json", evidence_path=root / "preload.jsonl")
            if not preload_report["complete"]:
                raise RuntimeError("smoke preload did not verify every value on all five owners: "
                                   + json.dumps(preload_report["failure_details"][:3],
                                                sort_keys=True))

            spec = {"experiment": "smoke", "mode": "strong", "read_quorum": None,
                    "write_quorum": None, "clients": 2, "trial": 1,
                    "seed": 4252026, "duration_seconds": duration_seconds,
                    "trial_id": run_id}
            raw_files, worker_summaries, worker_processes = [], [], []
            worker_root = root / "workers"
            worker_root.mkdir()
            gate = worker_root / "GO"
            mp_context = multiprocessing.get_context("spawn")
            for index, client_id in enumerate(("vm01", "vm02")):
                raw = worker_root / f"{client_id}.jsonl"
                summary_path = worker_root / f"{client_id}.summary.json"
                ready = worker_root / f"{client_id}.ready.json"
                process = mp_context.Process(target=run_worker, kwargs={
                    "manifest_path": manifest_path, "client_id": client_id,
                    "mode": "strong", "read_quorum": 2, "write_quorum": 2,
                    "seed": spec["seed"], "key_count": key_count,
                    "distribution": "uniform", "run_id": run_id,
                    "duration_seconds": duration_seconds, "raw_path": raw,
                    "summary_path": summary_path, "gate_path": gate,
                    "ready_path": ready, "not_for_report": True})
                process.start()
                worker_processes.append((process, summary_path, ready))
                raw_files.append(raw)
            ready_deadline = time.monotonic() + 10.0
            while time.monotonic() < ready_deadline:
                if all(ready.exists() for _process, _summary, ready in worker_processes):
                    break
                if any(process.exitcode is not None for process, _summary, _ready
                       in worker_processes):
                    raise RuntimeError("a persistent smoke client exited before the GO gate")
                time.sleep(0.02)
            else:
                raise TimeoutError("smoke client processes did not reach the start barrier")
            gate.touch()
            for process, summary_path, _ready in worker_processes:
                process.join(timeout=duration_seconds + 30.0)
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=3.0)
                    raise TimeoutError("smoke benchmark client did not finish within its bound")
                if process.exitcode != 0:
                    raise RuntimeError(f"smoke benchmark client exited {process.exitcode}")
                worker_summaries.append(json.loads(summary_path.read_text(encoding="utf-8")))
            op_rows = [row for path in raw_files for row in read_jsonl(path)]
            interval = max(row["actual_elapsed_seconds"] for row in worker_summaries)
            summary = trial_summary(
                spec=spec, operation_rows=op_rows, workers=worker_summaries,
                measurement_interval_seconds=interval, environment={
                    "hostname": socket.gethostname(), "python": sys.version,
                    "platform": sys.platform, "target": "local_loopback"},
                preload_evidence=preload_report, target="local_loopback", not_for_report=True)
            # Smoke artifacts are summarized and plotted in their isolated temp tree;
            # no synthetic result is written into the repository or eligible for final mode.
            plot_summary = [{"spec": spec, **summary}]
            paths = plot_trials(plot_summary, root / "plots", smoke=True)
            if len(paths) != 1:
                raise RuntimeError("the Strong smoke trial did not generate its plot")
            published_artifacts.mkdir(parents=True, exist_ok=False)
            for source in (root / "preload.json", root / "preload.jsonl",
                           root / "preload-owner-presence.jsonl", *raw_files,
                           *(worker_root / f"vm{i:02d}.summary.json" for i in (1, 2)),
                           paths[0]):
                shutil.copy2(source, published_artifacts / source.name)
            (published_artifacts / "NOT_FOR_REPORT.txt").write_text(
                "Local 127.0.0.1 smoke data. It is not a university-VM experiment and "
                "must not be used in the MP3 report.\n", encoding="utf-8")
            return {
                "status": "OK", "label": "NOT FOR REPORT — LOCAL LOOPBACK SMOKE",
                "node_processes": 10, "client_processes": 2,
                "preloaded_keys": key_count,
                "verified_replica_copies": key_count * 5,
                "workload_operations": len(op_rows),
                "successful_operations": summary["successful_operations"],
                "failed_operations": summary["failed_operations"],
                "successful_throughput_ops_sec": summary["successful_throughput_ops_sec"],
                "mean_latency_ms_all_terminal": summary["mean_latency_ms_all_terminal"],
                "duration_seconds": [row["actual_elapsed_seconds"]
                                     for row in worker_summaries],
                "plot_generated": paths[0].name,
                "artifact_directory": str(published_artifacts),
                "target": "127.0.0.1 only",
            }
        finally:
            for process in processes:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
            for process in processes:
                try:
                    process.wait(timeout=8.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2.0)
            for handle in node_logs:
                handle.close()
