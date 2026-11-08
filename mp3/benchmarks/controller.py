"""Full 51-trial controller. Remote actions require an isolated confirmed root."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict
import json
import os
from pathlib import Path
import re
import shlex
import socket
import subprocess
import sys
import time

from ..control import ControlStatus, send_command
from ..models import ConsistencyMode
from ..ring import Manifest, load_manifest
from .matrix import full_matrix, spec_dict
from .metrics import complete_checkpoint, read_jsonl, trial_summary, write_json_atomic
from .preload import preload

_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{2,48}\Z")
_USER = re.compile(r"[A-Za-z0-9._-]{1,64}\Z")
SSH_OPTIONS = ("-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
               "-o", "StrictHostKeyChecking=accept-new")


def _ssh(user: str, host: str, command: str, *, timeout: float = 20.0) -> subprocess.CompletedProcess:
    return subprocess.run(["ssh", *SSH_OPTIONS, f"{user}@{host}", command],
                          capture_output=True, text=True, timeout=timeout)


def _remote(hosts: tuple[str, ...], user: str, command: str, *, timeout: float = 40.0) -> None:
    with ThreadPoolExecutor(max_workers=min(10, len(hosts))) as pool:
        tasks = [pool.submit(_ssh, user, host, command, timeout=timeout) for host in hosts]
        errors = []
        for host, task in zip(hosts, tasks):
            result = task.result()
            if result.returncode:
                errors.append(f"{host}: {result.stderr.strip() or result.stdout.strip()}")
        if errors:
            raise RuntimeError("remote action failed: " + "; ".join(errors))


def _shell_script(name: str, args: list[str], *, user: str, remote_root: str,
                  mode: str, r: int, w: int) -> None:
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env.update(MP3_TARGET="remote", MP3_USER=user, MP3_REMOTE_ROOT=remote_root,
               MP3_MODE=mode, MP3_R=str(r), MP3_W=str(w))
    if env.get("MP3_TARGET") != "remote":
        raise RuntimeError("remote scripts require explicit MP3_TARGET=remote")
    subprocess.run([str(root / "deploy" / name), *args], cwd=root,
                   env=env, check=True, timeout=180)


def _snapshot(manifest: Manifest, expected_mode: str | None = None,
              expected_r: int | None = None, expected_w: int | None = None) -> dict:
    """Wait boundedly for ten healthy nodes and mutually current generation views."""
    deadline = time.monotonic() + 25.0
    last_error = "membership has not converged"
    while time.monotonic() < deadline:
        try:
            def inspect(node_id: str) -> tuple[str, dict, list[dict]]:
                reply = send_command(manifest, node_id, "status", timeout=2.0)
                if reply.status != ControlStatus.OK:
                    raise RuntimeError(f"status on {node_id}: {reply.status.value}")
                details = json.loads(reply.detail or "{}")
                row = {"mode": reply.mode.value, "epoch": reply.epoch,
                       "read_quorum": details.get("read_quorum"),
                       "write_quorum": details.get("write_quorum"),
                       "generation": details.get("generation")}
                if expected_mode is not None and row["mode"] != expected_mode:
                    raise RuntimeError(f"{node_id} has mode {row['mode']}, expected {expected_mode}")
                if expected_r is not None and row["read_quorum"] != expected_r:
                    raise RuntimeError(f"{node_id} R={row['read_quorum']}, expected {expected_r}")
                if expected_w is not None and row["write_quorum"] != expected_w:
                    raise RuntimeError(f"{node_id} W={row['write_quorum']}, expected {expected_w}")
                members_reply = send_command(manifest, node_id, "list_mem_ids", timeout=2.0)
                if members_reply.status != ControlStatus.OK:
                    raise RuntimeError(f"membership on {node_id}: {members_reply.status.value}")
                members = json.loads(members_reply.detail).get("rows", [])
                return node_id, row, members

            with ThreadPoolExecutor(max_workers=10) as pool:
                inspected = list(pool.map(inspect, [node.vm_id for node in manifest.nodes]))
            statuses = {node_id: row for node_id, row, _members in inspected}
            expected_ids = {f"vm{i:02d}" for i in range(1, 11)}
            generations = {vm: row["generation"] for vm, row in statuses.items()}
            epochs = {row["epoch"] for row in statuses.values()}
            modes = {row["mode"] for row in statuses.values()}
            if len(epochs) != 1 or len(modes) != 1:
                raise RuntimeError("nodes report mixed mode epochs or modes")
            all_members = []
            for node_id, _status, rows in inspected:
                table = {member.get("vm"): member for member in rows}
                if (set(table) != expected_ids
                        or any(member.get("status") != "ALIVE" for member in rows)
                        or any(table[vm].get("generation") != generations[vm]
                               for vm in expected_ids)):
                    raise RuntimeError(f"{node_id} has stale/incomplete membership generations")
                all_members.append(rows)
            return {"nodes": statuses, "membership_views": all_members,
                    "members": all_members[0], "epoch": epochs.pop()}
        except Exception as error:
            last_error = str(error)
            time.sleep(0.2)
    raise TimeoutError(f"all ten nodes did not reach healthy current membership: {last_error}")


def _wait_file(user: str, host: str, path: str, deadline: float) -> None:
    while time.monotonic() < deadline:
        result = _ssh(user, host, f'test -f "$HOME/{path}"', timeout=10.0)
        if result.returncode == 0:
            return
        time.sleep(0.25)
    raise TimeoutError(f"worker on {host} did not create {path}")


def _collect(user: str, host: str, relative_path: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    remote = f"{user}@{host}:{relative_path}"
    result = subprocess.run(["scp", *SSH_OPTIONS, remote, str(destination)],
                            capture_output=True, text=True, timeout=60)
    if result.returncode:
        raise RuntimeError(f"could not collect {remote}: {result.stderr.strip()}")


def _launch_trial(spec, *, manifest: Manifest, user: str, root_name: str,
                  run_id: str, run_dir: Path, preload_report: dict,
                  attempt_id: str = "attempt-01") -> dict:
    clients = tuple(f"vm{i:02d}" for i in range(1, spec.clients + 1))
    trial_rel = f"benchmark/{run_id}/trials/{spec.trial_id}/{attempt_id}"
    launched = []
    for client_id in clients:
        host = manifest.node(client_id).control.host
        gate = f"{trial_rel}/GO-{client_id}"
        ready = f"{trial_rel}/READY-{client_id}.json"
        raw = f"{trial_rel}/{client_id}.jsonl"
        summary = f"{trial_rel}/{client_id}.summary.json"
        log = f"{trial_rel}/{client_id}.worker.log"
        args = ["--manifest", "run/manifest.json", "--client-id", client_id,
                "--mode", spec.mode, "--seed", str(spec.seed),
                "--key-count", "100000", "--distribution",
                "hot99" if spec.experiment == "exp2" else "uniform",
                "--run-id", run_id, "--duration", str(spec.duration_seconds),
                "--raw", raw, "--summary", summary, "--gate", gate, "--ready", ready]
        if spec.read_quorum is not None:
            args += ["--read-quorum", str(spec.read_quorum),
                     "--write-quorum", str(spec.write_quorum)]
        cmd = (f'cd "$HOME/{root_name}" && mkdir -p {shlex.quote(trial_rel)} && '
               f'rm -f {shlex.quote(gate)} {shlex.quote(ready)} {shlex.quote(summary)} && '
               f'(nohup env PYTHONPATH=. python3 -B -m mp3.benchmarks worker '
               f'{shlex.join(args)} > {shlex.quote(log)} 2>&1 < /dev/null & echo STARTED)')
        result = _ssh(user, host, cmd, timeout=20.0)
        if result.returncode:
            raise RuntimeError(f"could not start {client_id}: {result.stderr.strip()}")
        launched.append((client_id, host, gate, ready, raw, summary, log))

    ready_deadline = time.monotonic() + 60.0
    with ThreadPoolExecutor(max_workers=10) as pool:
        tasks = [pool.submit(_wait_file, user, host, ready, ready_deadline)
                 for _vm, host, _gate, ready, _raw, _summary, _log in launched]
        for task in tasks:
            task.result()

    release_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=10) as pool:
        tasks = [pool.submit(_ssh, user, host,
                             f'touch "$HOME/{root_name}/{gate}"', timeout=15.0)
                 for _vm, host, gate, _ready, _raw, _summary, _log in launched]
        for task in tasks:
            result = task.result()
            if result.returncode:
                raise RuntimeError("could not release a client start gate: "
                                   + result.stderr.strip())
    release_skew_upper = time.monotonic() - release_start

    finish_deadline = time.monotonic() + spec.duration_seconds + 180.0
    completed = set()
    while time.monotonic() < finish_deadline and len(completed) < len(launched):
        for vm, host, _gate, _ready, _raw, summary, _log in launched:
            if vm in completed:
                continue
            result = _ssh(user, host, f'test -f "$HOME/{root_name}/{summary}"', timeout=10.0)
            if result.returncode == 0:
                completed.add(vm)
        if len(completed) < len(launched):
            time.sleep(1.0)
    if len(completed) != len(launched):
        raise TimeoutError(f"only {len(completed)}/{len(launched)} workers completed")

    raw_paths, workers, operations = [], [], []
    trial_dir = run_dir / "attempts" / spec.trial_id / attempt_id
    for vm, host, _gate, _ready, raw, summary, _log in launched:
        local_raw = trial_dir / f"{vm}.jsonl"
        local_summary = trial_dir / f"{vm}.worker.json"
        _collect(user, host, f"{root_name}/{raw}", local_raw)
        _collect(user, host, f"{root_name}/{summary}", local_summary)
        worker = json.loads(local_summary.read_text(encoding="utf-8"))
        workers.append(worker)
        raw_paths.append(str(local_raw.relative_to(run_dir)))
        operations.extend(read_jsonl(local_raw))
    interval = max(worker["actual_elapsed_seconds"] for worker in workers) + release_skew_upper
    env = {"hostname": socket.gethostname(), "python": sys.version,
           "platform": sys.platform, "manifest_id": manifest.fingerprint,
           "target": "remote_university_vm",
           "client_hosts": [worker["hostname"] for worker in workers],
           "gate_release_skew_upper_seconds": release_skew_upper}
    summary = trial_summary(
        spec=spec_dict(spec), operation_rows=operations, workers=workers,
        measurement_interval_seconds=interval, environment=env,
        preload_evidence=preload_report, target="remote_university_vm")
    summary["raw_files"] = raw_paths
    summary["workers"] = [{**worker, "operations": worker["operations"]}
                          for worker in workers]
    return summary


def run_full(*, manifest_path: str | Path, run_dir: str | Path, run_id: str,
             remote_root: str, confirm_isolated_root: str, ssh_user: str,
             base_seed: int = 4252026, retry_failed: bool = False) -> dict:
    """Initialize a fresh, isolated VM cluster and execute exactly the 51 full trials."""
    if os.environ.get("MP3_TARGET") != "remote":
        raise RuntimeError("full requires explicit MP3_TARGET=remote")
    if not _RUN_ID.fullmatch(run_id) or confirm_isolated_root != run_id:
        raise ValueError("--confirm-isolated-root must exactly match a safe --run-id")
    if not _USER.fullmatch(ssh_user):
        raise ValueError("SSH user must be a simple NetID")
    expected_root = f"$HOME/mp3_benchmark_{run_id}"
    if remote_root != expected_root or "mp3_deploy" in remote_root:
        raise ValueError("remote root must be exactly $HOME/mp3_benchmark_<run-id>; "
                         "demo deployment roots are refused")
    run_dir = Path(run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(manifest_path)
    hosts = tuple(node.control.host for node in manifest.nodes)
    if any(host in {"127.0.0.1", "localhost", "::1"} for host in hosts):
        raise ValueError("full university-VM mode refuses loopback manifests")

    # Current deploy_mp generates only the course-default host pattern/ports. Require
    # the supplied manifest to be exactly the one the deploy scripts will install.
    check_path = run_dir / "deployment-config"
    check_path.mkdir(exist_ok=True)
    generator = Path(__file__).resolve().parents[1] / "deploy" / "gen_manifest.py"
    subprocess.run([sys.executable, str(generator), "--target", "remote", "--out",
                    str(check_path)], check=True, capture_output=True, text=True)
    deploy_manifest = load_manifest(check_path / "manifest.json")
    if manifest.fingerprint != deploy_manifest.fingerprint:
        raise ValueError("manifest does not match the current deployment generator's "
                         "remote hosts/ports; verify professor deployment instructions first")

    # A resumed run must match its local run record and still have all ten isolated
    # roots. A new run requires all ten roots to be absent before any remote change.
    root_name = f"mp3_benchmark_{run_id}"
    record_path = run_dir / "run.json"
    resuming = False
    if record_path.is_file():
        existing_run = json.loads(record_path.read_text(encoding="utf-8"))
        if (existing_run.get("run_id") != run_id
                or existing_run.get("manifest_id") != manifest.fingerprint
                or existing_run.get("target") != "remote_university_vm"):
            raise ValueError("existing run.json does not match this run id/manifest")
        resuming = True
    # Inspect directories sequentially. During a long preload the user's Mac
    # may temporarily lose UIUC DNS; ten concurrent fresh SSH handshakes can
    # also amplify such failures. Keep connectivity errors distinct from actual
    # missing/existing roots so recovery never asks to delete valid data.
    wrong_state = []
    for host in hosts:
        command = (f'if test -d "$HOME/{root_name}"; then echo EXISTS; '
                   'else echo MISSING; fi')
        result = None
        for attempt in range(3):
            try:
                result = _ssh(ssh_user, host, command, timeout=20.0)
            except (subprocess.TimeoutExpired, OSError) as error:
                if attempt == 2:
                    raise ConnectionError(f"{host}: SSH inspection failed: {error}") from error
                time.sleep(attempt + 1)
                continue
            if result.returncode == 0 and result.stdout.strip() in {"EXISTS", "MISSING"}:
                break
            if attempt == 2:
                raise ConnectionError(
                    f"{host}: SSH inspection failed (exit {result.returncode}): "
                    f"{result.stderr.strip() or result.stdout.strip()}")
            time.sleep(attempt + 1)
        state = result.stdout.strip()
        if (resuming and state != "EXISTS") or (not resuming and state != "MISSING"):
            wrong_state.append(host)
    if wrong_state:
        if resuming:
            raise FileNotFoundError("isolated benchmark directory missing on: "
                                    + ", ".join(wrong_state) + "; refusing to resume")
        raise FileExistsError("isolated benchmark directory already exists on: "
                              + ", ".join(wrong_state) + "; no data was modified")

    (run_dir / "preloads").mkdir(exist_ok=True)
    (run_dir / "trials").mkdir(exist_ok=True)
    (run_dir / "failures").mkdir(exist_ok=True)
    if not resuming:
        _shell_script("deploy_mp", ["all"], user=ssh_user, remote_root=remote_root,
                      mode="strong", r=2, w=2)
        _shell_script("run_mp", ["all"], user=ssh_user, remote_root=remote_root,
                      mode="strong", r=2, w=2)
    run_record = ({"schema": 1, "run_id": run_id, "target": "remote_university_vm",
                   "manifest_id": manifest.fingerprint, "base_seed": base_seed,
                   "started_utc": __import__("datetime").datetime.now(
                       __import__("datetime").timezone.utc).isoformat(),
                   "planned_trials": 51,
                   "notes": "full matrix; 180s minimum per client"}
                  if not resuming else existing_run)
    write_json_atomic(record_path, run_record)

    current_config = None
    preload_evidence = None
    summaries = []
    matrix = full_matrix(base_seed)
    for spec in matrix:
        trial_dir = run_dir / "trials" / spec.trial_id
        canonical_summary_path = trial_dir / "summary.json"
        if canonical_summary_path.exists():
            existing_summary = json.loads(canonical_summary_path.read_text(encoding="utf-8"))
            # Reuse only a complete full-duration checkpoint with retained raw records.
            if complete_checkpoint(existing_summary, run_dir, spec_dict(spec)):
                summaries.append(existing_summary)
                continue
            if not retry_failed:
                raise RuntimeError(f"incomplete checkpoint {spec.trial_id}; use --retry-failed "
                                   "to preserve it and run a new attempt")

        config = (spec.mode, spec.read_quorum, spec.write_quorum)
        if current_config != config:
            target_mode = spec.mode
            snapshot = _snapshot(manifest)
            current_mode = next(iter(snapshot["nodes"].values()))["mode"]
            if current_mode != target_mode:
                mode_reply = send_command(manifest, "vm01", "mode", key=target_mode,
                                          timeout=55.0)
                if mode_reply.status != ControlStatus.OK:
                    raise RuntimeError(f"mode change to {target_mode} failed: "
                                       f"{mode_reply.error or mode_reply.status.value}")
            if spec.read_quorum is not None:
                snapshot = _snapshot(manifest)
                any_quorum_differs = any(
                    row["read_quorum"] != spec.read_quorum
                    or row["write_quorum"] != spec.write_quorum
                    for row in snapshot["nodes"].values())
                if any_quorum_differs:
                    _shell_script("stop_mp", ["all"], user=ssh_user,
                                  remote_root=remote_root, mode=target_mode,
                                  r=spec.read_quorum, w=spec.write_quorum)
                    _shell_script("run_mp", ["all"], user=ssh_user,
                                  remote_root=remote_root, mode=target_mode,
                                  r=spec.read_quorum, w=spec.write_quorum)
            snapshot = _snapshot(manifest, target_mode,
                                 spec.read_quorum, spec.write_quorum)
            preload_prefix = (f"{spec.experiment}-{target_mode}-r{spec.read_quorum}-"
                              f"w{spec.write_quorum}")
            preload_evidence = preload(
                manifest_path=manifest_path, run_id=run_id, mode=target_mode,
                key_count=100_000, key_prefix="bench-key-", workers=16,
                report_path=run_dir / "preloads" / f"{preload_prefix}.json",
                evidence_path=run_dir / "preloads" / f"{preload_prefix}.content.jsonl",
                presence_path=run_dir / "preloads" / f"{preload_prefix}.owners.jsonl",
                cluster_epoch=snapshot["epoch"])
            if not preload_evidence["complete"]:
                raise RuntimeError(f"all-five preload failed: {preload_evidence['failed_keys'][:5]}")
            preload_evidence["cluster_snapshot"] = snapshot
            preload_evidence["content_evidence_file"] = str(
                Path(preload_evidence["content_evidence_file"]).resolve().relative_to(run_dir))
            preload_evidence["owner_presence_file"] = str(
                Path(preload_evidence["owner_presence_file"]).resolve().relative_to(run_dir))
            preload_report_path = run_dir / "preloads" / f"{preload_prefix}.json"
            write_json_atomic(preload_report_path, preload_evidence)
            current_config = config

        attempt_dir = trial_dir
        if trial_dir.exists() and any(trial_dir.iterdir()):
            attempt_number = 1
            while (run_dir / "failures" / f"{spec.trial_id}-attempt-{attempt_number:02d}").exists():
                attempt_number += 1
            attempt_dir = run_dir / "failures" / f"{spec.trial_id}-attempt-{attempt_number:02d}"
            attempt_dir.mkdir(parents=True)
            trial_dir = attempt_dir
        attempt_id = f"attempt-{time.time_ns()}"
        try:
            # Verify full healthy cluster and current R/W immediately before every trial.
            _snapshot(manifest, spec.mode, spec.read_quorum, spec.write_quorum)
            summary = _launch_trial(spec, manifest=manifest, user=ssh_user,
                                    root_name=root_name, run_id=run_id,
                                    run_dir=run_dir, preload_report=preload_evidence,
                                    attempt_id=attempt_id)
            canonical_summary_path.parent.mkdir(parents=True, exist_ok=True)
            summary["raw_files"] = [str(Path("attempts") / spec.trial_id / attempt_id /
                                        f"vm{i:02d}.jsonl")
                                    for i in range(1, spec.clients + 1)]
            # Successful summaries always have one canonical discovery path. Failed
            # retry material stays isolated under failures/<trial>-attempt-NN.
            write_json_atomic(canonical_summary_path, summary)
            summaries.append(summary)
        except Exception as error:
            partial_files = []
            remote_trial = (f"benchmark/{run_id}/trials/{spec.trial_id}/"
                            f"{attempt_id}")
            for index in range(1, spec.clients + 1):
                vm_id = f"vm{index:02d}"
                host = manifest.node(vm_id).control.host
                for suffix in ("jsonl", "summary.json", "worker.log"):
                    relative = f"{remote_trial}/{vm_id}" + (
                        ".jsonl" if suffix == "jsonl" else
                        ".summary.json" if suffix == "summary.json" else ".worker.log")
                    destination = trial_dir / f"{vm_id}.{suffix}"
                    try:
                        _collect(ssh_user, host,
                                 f"{root_name}/{relative}", destination)
                        partial_files.append(str(destination))
                    except (OSError, RuntimeError, subprocess.SubprocessError):
                        continue
            failure = {"trial_id": spec.trial_id, "error": f"{type(error).__name__}: {error}",
                       "recorded_utc": time.time(), "preserved_attempt_dir": str(trial_dir),
                       "partial_files_collected": partial_files}
            write_json_atomic(trial_dir / "failure.json", failure)
            if not retry_failed:
                raise
            raise RuntimeError("trial failure recorded; rerun full with --retry-failed") from error

    write_json_atomic(run_dir / "run.json", run_record | {
        "completed_trials": len(summaries), "finished_utc": time.time()})
    return {"status": "OK", "run_dir": str(run_dir), "completed_trials": len(summaries),
            "planned_trials": len(matrix), "target": "remote_university_vm"}
