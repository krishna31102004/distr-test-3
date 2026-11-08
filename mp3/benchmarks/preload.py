"""Preload 100k keys, then prove each exists on its five prescribed owners."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import time

from ..control import ControlError, ControlStatus, send_command
from ..models import ConsistencyMode
from ..ring import Manifest, load_manifest
from .workload import preload_value


def _record_at_owner(manifest: Manifest, coordinator: str, owner: str,
                     key: str, value: bytes) -> dict:
    body = json.dumps({"target": owner}, sort_keys=True).encode("utf-8")
    reply = send_command(manifest, coordinator, "read_replica", key, body, timeout=25.0)
    if reply.status == ControlStatus.NOT_FOUND:
        return {"owner": owner, "status": reply.status.value, "version": None,
                "value_sha256": None, "value_verified": False, "error": reply.error}
    if reply.status != ControlStatus.OK:
        return {"owner": owner, "status": reply.status.value, "version": None,
                "value_sha256": None, "value_verified": False, "error": reply.error}
    version = reply.version
    version_text = None if version is None else (
        f"{version.kind}:{version.number}:{version.writer_id}")
    actual_digest = hashlib.sha256(reply.value).hexdigest()
    return {"owner": owner, "status": reply.status.value, "version": version_text,
            "value_sha256": actual_digest, "value_verified": reply.value == value,
            "error": reply.error}


def _liststore(manifest: Manifest, vm_id: str) -> set[str]:
    cursor = None
    found: set[str] = set()
    while True:
        payload = b"" if cursor is None else json.dumps({"after": cursor}).encode("utf-8")
        reply = send_command(manifest, vm_id, "liststore", "", payload, timeout=25.0)
        if reply.status != ControlStatus.OK:
            raise RuntimeError(f"liststore on {vm_id} failed: {reply.status.value} "
                               f"{reply.error or ''}")
        page = json.loads(reply.value)
        keys, following = page.get("keys"), page.get("next")
        if not isinstance(keys, list) or not all(isinstance(key, str) for key in keys):
            raise ValueError(f"{vm_id} returned malformed liststore keys")
        if found.intersection(keys):
            raise ValueError(f"{vm_id} repeated a key across liststore pages")
        found.update(keys)
        if following is None:
            return found
        if not isinstance(following, str) or (
                cursor is not None and following.encode() <= cursor.encode()):
            raise ValueError(f"{vm_id} returned a non-advancing liststore cursor")
        cursor = following


def preload(*, manifest_path: str | Path, run_id: str, mode: ConsistencyMode | str,
            key_count: int, key_prefix: str = "bench-key-", max_attempts: int = 5,
            workers: int = 16, report_path: str | Path,
            evidence_path: str | Path, presence_path: str | Path | None = None,
            cluster_epoch: int | None = None) -> dict:
    """Use ordinary MP3 PUTs and direct owner reads; never write local DB files.

    Each key gets a bounded number of recorded setup PUTs until all five designated
    owners directly return the exact value. Versions are recorded per owner but need
    not match. Paginated `liststore` snapshots additionally confirm key inventory.
    This is setup only; no retries are hidden in measured worker operations.
    """
    manifest = load_manifest(manifest_path)
    mode = ConsistencyMode(mode)
    if key_count < 1 or max_attempts < 1 or workers < 1:
        raise ValueError("key_count, max_attempts and workers must be positive")
    report_path, evidence_path = Path(report_path), Path(evidence_path)
    presence_path = Path(presence_path) if presence_path else evidence_path.with_name(
        "preload-owner-presence.jsonl")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    presence_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    failures: list[str] = []
    failure_details: list[dict] = []
    digest = hashlib.sha256()
    # A failed preload may already have verified nearly every key. Reuse only
    # complete five-owner content proofs from the same run, mode and cluster epoch.
    # The original evidence is retained until the replacement is complete.
    cached: dict[int, dict] = {}
    if report_path.is_file() and evidence_path.is_file():
        old = json.loads(report_path.read_text(encoding="utf-8"))
        identity = {"run_id": run_id, "manifest_id": manifest.fingerprint,
                    "mode": mode.value, "key_count": key_count, "key_prefix": key_prefix}
        if any(old.get(field) != expected for field, expected in identity.items()):
            raise RuntimeError("existing preload evidence identity differs; refusing to reuse it")
        old_epoch = old.get("cluster_epoch")
        if cluster_epoch is not None:
            # Legacy partial reports did not record epoch. The original strong
            # bootstrap was epoch 1; other legacy contexts cannot be verified.
            if old_epoch is None and mode == ConsistencyMode.STRONG and cluster_epoch == 1:
                old_epoch = 1
            if old_epoch != cluster_epoch:
                raise RuntimeError("preload epoch changed; refusing stale checkpoint reuse")
        with evidence_path.open("r", encoding="utf-8") as stream:
            seen: set[int] = set()
            for line in stream:
                row = json.loads(line)
                index = row.get("index")
                if type(index) is not int or index < 0 or index >= key_count or index in seen:
                    raise RuntimeError("corrupt preload evidence index; refusing resume")
                seen.add(index)
                key, value = f"{key_prefix}{index:06d}", preload_value(run_id, index)
                owners = [node.vm_id for node in manifest.owners(key)]
                checksum = hashlib.sha256(value).hexdigest()
                checks = row.get("replica_content_checks", [])
                if (row.get("content_verified") is True
                        and row.get("key") == key and row.get("owners") == owners
                        and row.get("value_sha256") == checksum
                        and isinstance(checks, list) and len(checks) == 5
                        and [check.get("owner") for check in checks] == owners
                        and all(check.get("status") == ControlStatus.OK.value
                                and check.get("version")
                                and check.get("value_verified") is True
                                and check.get("value_sha256") == checksum
                                for check in checks)):
                    cached[index] = row

    def one(index: int) -> dict:
        key, value = f"{key_prefix}{index:06d}", preload_value(run_id, index)
        owners = tuple(node.vm_id for node in manifest.owners(key))
        coordinator = f"vm{(index % 10) + 1:02d}"
        last = None
        expected_digest = hashlib.sha256(value).hexdigest()
        for attempt in range(1, max_attempts + 1):
            # Transient DNS and control timeouts must consume an attempt,
            # not abandon the key and force another 100,000-key preload.
            try:
                reply = send_command(manifest, coordinator, "put", key, value, timeout=25.0)
                last_version = reply.version
                version_text = None if last_version is None else (
                    f"{last_version.kind}:{last_version.number}:{last_version.writer_id}")
                replica_checks = []
                if reply.status == ControlStatus.OK and version_text is not None:
                    replica_checks = [_record_at_owner(manifest, coordinator, owner, key, value)
                                      for owner in owners]
                confirmed = (len(replica_checks) == len(owners) == 5
                             and all(check["status"] == ControlStatus.OK.value
                                     and check["version"] is not None
                                     and check["value_verified"]
                                     and check["value_sha256"] == expected_digest
                                     for check in replica_checks))
                last = {"index": index, "key": key, "owners": list(owners),
                        "value_sha256": expected_digest,
                        "attempts": attempt, "put_status": reply.status.value,
                        "put_error": reply.error, "version": version_text,
                        "content_verified_owner": owners[0],
                        "content_verified_version": (replica_checks[0]["version"]
                                                      if replica_checks else None),
                        "replica_content_checks": replica_checks,
                        "content_verified": confirmed}
                if confirmed:
                    return last
            except (ControlError, OSError, TimeoutError) as error:
                last = {"index": index, "key": key, "owners": list(owners),
                        "value_sha256": expected_digest, "attempts": attempt,
                        "content_verified": False,
                        "error": f"{type(error).__name__}: {error}"}
            time.sleep(0.05)
        assert last is not None
        return last

    # Atomic replacement is important: a disconnect during the targeted retry
    # must never destroy the 99k+ successful entries from the prior attempt.
    pending_path = evidence_path.with_name(evidence_path.name + ".pending")
    with pending_path.open("w", encoding="utf-8") as evidence:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="mp3-preload") as pool:
            # At most workers*2 outstanding operations; verified keys do no RPCs.
            for first in range(0, key_count, workers * 2):
                last_index = min(key_count, first + workers * 2)
                pending = {index: pool.submit(one, index)
                           for index in range(first, last_index) if index not in cached}
                for index in range(first, last_index):
                    if index in cached:
                        row = cached[index]
                    else:
                        try:
                            row = pending[index].result()
                        except Exception as error:
                            row = {"index": index, "key": f"{key_prefix}{index:06d}",
                                   "owners": [], "content_verified": False,
                                   "error": f"{type(error).__name__}: {error}"}
                    if not row.get("content_verified"):
                        failures.append(row["key"])
                        failure_details.append(row)
                    line = json.dumps(row, sort_keys=True, ensure_ascii=False)
                    evidence.write(line + "\n")
                    digest.update(line.encode("utf-8") + b"\n")
        evidence.flush()
    pending_path.replace(evidence_path)

    # One bounded page walk per node is much less expensive than 500k control reads.
    node_keys: dict[str, set[str]] = {}
    if not failures:
        try:
            with ThreadPoolExecutor(max_workers=10, thread_name_prefix="mp3-liststore") as pool:
                pending_nodes = {pool.submit(_liststore, manifest, node.vm_id): node.vm_id
                                 for node in manifest.nodes}
                for future in as_completed(pending_nodes):
                    node_keys[pending_nodes[future]] = future.result()
        except Exception as error:
            failures.append(f"liststore-verification:{type(error).__name__}:{error}")

    expected_keys = {f"{key_prefix}{index:06d}" for index in range(key_count)}
    presence_errors = []
    with presence_path.open("w", encoding="utf-8") as stream:
        for index in range(key_count):
            key = f"{key_prefix}{index:06d}"
            owners = tuple(node.vm_id for node in manifest.owners(key))
            present = [owner for owner in owners if key in node_keys.get(owner, set())]
            row = {"index": index, "key": key, "prescribed_owners": list(owners),
                   "present_owners": present, "complete": len(present) == 5}
            if not row["complete"]:
                presence_errors.append(key)
                failure_details.append(row)
            stream.write(json.dumps(row, sort_keys=True) + "\n")
        stream.flush()

    # Retain a compact digest of the actual local key inventory. Extra non-benchmark
    # keys are not treated as preload evidence and remain untouched.
    inventory_digest = hashlib.sha256()
    for vm_id in sorted(node_keys):
        for key in sorted(node_keys[vm_id] & expected_keys):
            inventory_digest.update(f"{vm_id}\0{key}\n".encode("utf-8"))
    if presence_errors:
        failures.extend(presence_errors[:100])
    result = {
        "schema": 1, "run_id": run_id, "manifest_id": manifest.fingerprint,
        "mode": mode.value, "cluster_epoch": cluster_epoch,
        "key_count": key_count, "key_prefix": key_prefix,
        "resumed_verified_keys": len(cached), "retried_keys": key_count - len(cached),
        "replication_factor": 5, "all_five_owners_verified": not failures,
        "complete": not failures and key_count > 0,
        "verified_keys": key_count - len(set(presence_errors)),
        "failed_keys": failures, "failure_details": failure_details[:20],
        "content_evidence_sha256": digest.hexdigest(),
        "inventory_evidence_sha256": inventory_digest.hexdigest(),
        "elapsed_seconds": time.monotonic() - started,
        "content_evidence_file": str(evidence_path),
        "owner_presence_file": str(presence_path),
    }
    report_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n",
                           encoding="utf-8")
    return result
