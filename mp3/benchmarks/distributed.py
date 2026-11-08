"""University-VM benchmark setup: durable sharded preload and safe dataset reuse."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import shlex
import time

from ..control import ControlStatus
from ..ring import load_manifest
from .preload import _liststore
from .workload import preload_value
from .metrics import write_json_atomic


def inventory_proof(manifest, key_count: int, output: Path) -> str:
    """Require every key to appear in the inventory of its five ring owners."""
    with ThreadPoolExecutor(max_workers=10) as pool:
        jobs = {node.vm_id: pool.submit(_liststore, manifest, node.vm_id)
                for node in manifest.nodes}
        owners_keys = {vm: task.result() for vm, task in jobs.items()}
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_name(output.name + ".pending")
    with temp.open("w", encoding="utf-8") as stream:
        for index in range(key_count):
            key = f"bench-key-{index:06d}"
            owners = [node.vm_id for node in manifest.owners(key)]
            present = [vm for vm in owners if key in owners_keys[vm]]
            if len(present) != 5:
                raise RuntimeError(f"missing replica inventory for {key}: {set(owners)-set(present)}")
            stream.write(json.dumps({
                "index": index, "key": key, "prescribed_owners": owners,
                "present_owners": present, "complete": True,
            }, sort_keys=True) + "\n")
    temp.replace(output)
    digest = hashlib.sha256()
    expected = {f"bench-key-{i:06d}" for i in range(key_count)}
    for vm in sorted(owners_keys):
        for key in sorted(owners_keys[vm] & expected):
            digest.update(f"{vm}\0{key}\n".encode("utf-8"))
    return digest.hexdigest()


def valid_content_row(row: dict, manifest, run_id: str, index: int) -> bool:
    key = f"bench-key-{index:06d}"
    owners = [node.vm_id for node in manifest.owners(key)]
    digest = hashlib.sha256(preload_value(run_id, index)).hexdigest()
    checks = row.get("replica_content_checks")
    return (type(row.get("index")) is int and row["index"] == index
            and row.get("key") == key and row.get("owners") == owners
            and row.get("content_verified") is True
            and row.get("value_sha256") == digest
            and row.get("version") and row.get("attempts", 0) >= 1
            and row.get("content_verified_owner") == owners[0]
            and isinstance(checks, list) and len(checks) == 5
            and [r.get("owner") for r in checks] == owners
            and row.get("content_verified_version") == checks[0].get("version")
            and all(r.get("status") == ControlStatus.OK.value
                    and r.get("version") and r.get("value_verified") is True
                    and r.get("value_sha256") == digest for r in checks))


def merge_shards(*, manifest_path, run_dir, run_id: str, mode: str,
                 cluster_epoch: int, prefix: str, shards: list,
                 key_count: int = 100_000) -> dict:
    """Merge ten original five-owner proofs without inventing missing records."""
    if len(shards) != 10:
        raise ValueError("ten shards required")
    manifest, run_dir = load_manifest(manifest_path), Path(run_dir).resolve()
    directory = run_dir / "preloads"
    directory.mkdir(parents=True, exist_ok=True)
    content_path = directory / f"{prefix}.content.jsonl"
    presence_path = directory / f"{prefix}.owners.jsonl"
    streams = []
    try:
        for idx, (report, evidence) in enumerate(shards):
            if (report.get("complete") is not True
                    or report.get("shard_index") != idx
                    or report.get("shard_count") != 10
                    or report.get("run_id") != run_id
                    or report.get("manifest_id") != manifest.fingerprint
                    or report.get("mode") != mode
                    or report.get("cluster_epoch") != cluster_epoch
                    or report.get("key_count") != key_count
                    or report.get("verified_keys") != len(range(idx, key_count, 10))):
                raise RuntimeError(f"invalid VM-local shard {idx}")
            streams.append(Path(evidence).open(encoding="utf-8"))
        sha = hashlib.sha256()
        pending = content_path.with_name(content_path.name + ".pending")
        with pending.open("w", encoding="utf-8") as target:
            for index in range(key_count):
                line = streams[index % 10].readline()
                if not line:
                    raise RuntimeError(f"missing preload proof index {index}")
                row = json.loads(line)
                if not valid_content_row(row, manifest, run_id, index):
                    raise RuntimeError(f"invalid five-replica proof index {index}")
                canonical = json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n"
                target.write(canonical)
                sha.update(canonical.encode("utf-8"))
            if any(stream.readline() for stream in streams):
                raise RuntimeError("shard proof contains extra rows")
        pending.replace(content_path)
    finally:
        for stream in streams:
            stream.close()
    inventory_hash = inventory_proof(manifest, key_count, presence_path)
    report = {
        "schema": 1, "run_id": run_id, "manifest_id": manifest.fingerprint,
        "mode": mode, "cluster_epoch": cluster_epoch,
        "key_count": key_count, "key_prefix": "bench-key-",
        "replication_factor": 5, "all_five_owners_verified": True,
        "complete": True, "verified_keys": key_count, "failed_keys": [],
        "failure_details": [], "shard_count": 10,
        "resumed_verified_keys": sum(r.get("resumed_verified_keys", 0) for r,_ in shards),
        "retried_keys": sum(r.get("retried_keys", 0) for r,_ in shards),
        "content_evidence_sha256": sha.hexdigest(),
        "inventory_evidence_sha256": inventory_hash,
        "content_evidence_file": str(content_path.relative_to(run_dir)),
        "owner_presence_file": str(presence_path.relative_to(run_dir)),
        "preload_method": "ten-vm-local-shards",
    }
    write_json_atomic(directory / f"{prefix}.json", report)
    return report


def inherit_eventual(*, manifest_path, run_dir, run_id: str, mode: str,
                     epoch: int, prefix: str, source: dict) -> dict:
    """Reuse retained Eventual data, proving fresh five-owner presence.

    Prior content proofs are historical (workload PUTs can change values).
    We never claim those values are still byte-identical after measurement.
    """
    if (source.get("mode") not in ("eventual-noRR", "eventual-RR")
            or mode not in ("eventual-noRR", "eventual-RR")
            or not source.get("complete")
            or not source.get("all_five_owners_verified")
            or source.get("key_count") != 100_000
            or source.get("run_id") != run_id
            or not source.get("content_evidence_sha256")):
        raise ValueError("no verified retained eventual dataset to inherit")
    manifest = load_manifest(manifest_path)
    if manifest.fingerprint != source.get("manifest_id"):
        raise ValueError("ring changed; do not reuse preload")
    directory = Path(run_dir).resolve() / "preloads"
    new_presence = directory / f"{prefix}.owners.jsonl"
    inventory_hash = inventory_proof(manifest, 100_000, new_presence)
    report = dict(source)
    report.update({
        "mode": mode, "cluster_epoch": epoch,
        "preload_method": "retained-eventual-with-fresh-inventory",
        "retained_source_mode": source["mode"],
        "retained_source_epoch": source["cluster_epoch"],
        "retained_source_digest": source["content_evidence_sha256"],
        "inventory_evidence_sha256": inventory_hash,
        "owner_presence_file": str(new_presence.relative_to(Path(run_dir).resolve())),
        "failed_keys": [], "failure_details": [],
        "complete": True, "all_five_owners_verified": True,
    })
    write_json_atomic(directory / f"{prefix}.json", report)
    return report


def run_distributed(*, manifest_path, run_dir, run_id: str, mode: str,
                    epoch: int, prefix: str, root_name: str, user: str,
                    ssh, collect, workers_per_vm: int = 4) -> dict:
    """Ten VM-local background workers survive laptop/SSH disconnects."""
    manifest = load_manifest(manifest_path)
    run_dir = Path(run_dir).resolve()
    remote = f"benchmark/{run_id}/preloads/{prefix}"
    local = run_dir / "preloads" / "shards" / prefix
    local.mkdir(parents=True, exist_ok=True)
    jobs = []
    for idx in range(10):
        host = manifest.node(f"vm{idx+1:02d}").control.host
        label = f"shard-{idx:02d}"
        report, content = f"{remote}/{label}.json", f"{remote}/{label}.jsonl"
        log, exit_file = f"{remote}/{label}.log", f"{remote}/{label}.exit"
        test = ssh(user, host,
                   f'test -f "$HOME/{root_name}/{report}" && '
                   f'grep -q \'"complete": true\' "$HOME/{root_name}/{report}"',
                   timeout=20)
        if test.returncode != 0:
            args = ["--manifest", "run/manifest.json", "--run-id", run_id,
                    "--mode", mode, "--cluster-epoch", str(epoch),
                    "--key-count", "100000", "--shard-index", str(idx),
                    "--shard-count", "10", "--workers", str(workers_per_vm),
                    "--report", report, "--evidence", content]
            work = (f'env PYTHONPATH=. python3 -B -m mp3.benchmarks preload-shard '
                    f'{shlex.join(args)}')
            # flock ensures a resumed controller cannot spawn a duplicate worker.
            # Only the exit marker is reset; all previous content proofs remain.
            cmd = (f'cd "$HOME/{root_name}" && mkdir -p {shlex.quote(remote)} && '
                   f'rm -f {shlex.quote(exit_file)} && '
                   f'(nohup flock -n {shlex.quote(remote + "/" + label + ".lock")} '
                   f'sh -c {shlex.quote(work + "; echo $? > " + exit_file)} '
                   f'> {shlex.quote(log)} 2>&1 < /dev/null &)')
            started = ssh(user, host, cmd, timeout=25)
            if started.returncode:
                raise RuntimeError(f"cannot launch preload shard {idx}: {started.stderr}")
        jobs.append((idx, host, report, content, log, exit_file))

    deadline = time.monotonic() + 7200
    complete = {}
    while len(complete) < 10:
        if time.monotonic() > deadline:
            raise TimeoutError(f"VM-side preload continuing: {len(complete)}/10 complete; resume safely later")
        def inspect(job):
            idx, host, report, _, log, exit_file = job
            cmd = (f'if test -f "$HOME/{root_name}/{exit_file}"; then '
                   f'cat "$HOME/{root_name}/{exit_file}"; else echo RUNNING; fi')
            try:
                return idx, ssh(user, host, cmd, timeout=15).stdout.strip()
            except Exception:
                return idx, "RUNNING"
        with ThreadPoolExecutor(max_workers=10) as pool:
            statuses = list(pool.map(inspect, [job for job in jobs if job[0] not in complete]))
        for idx, status in statuses:
            if status == "0":
                complete[idx] = True
            elif status not in ("", "RUNNING"):
                raise RuntimeError(f"VM-local shard {idx} failed (exit {status}); inspect remote shard log; prior proof preserved")
        if len(complete) < 10:
            time.sleep(8)

    shards = []
    for idx, host, report, content, _, _ in jobs:
        report_file = local / f"shard-{idx:02d}.json"
        proof_file = local / f"shard-{idx:02d}.jsonl"
        collect(user, host, f"{root_name}/{report}", report_file)
        collect(user, host, f"{root_name}/{content}", proof_file)
        shards.append((json.loads(report_file.read_text()), proof_file))
    return merge_shards(
        manifest_path=manifest_path, run_dir=run_dir, run_id=run_id,
        mode=mode, cluster_epoch=epoch, prefix=prefix, shards=shards)
