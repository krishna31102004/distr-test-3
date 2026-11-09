from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from typing import Callable

from .config import MAX_VALUE_BYTES
from .control import (
    ControlError, ControlResponse, ControlStatus, MODE_CLIENT_TIMEOUT, send_command,
)
from .models import Version
from .ring import Manifest, key_position, ring_id_hex


def read_value_file(path: str) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError(f"{path} is not a regular file")
    except BaseException:
        os.close(descriptor)
        raise
    with os.fdopen(descriptor, "rb") as handle:
        data = handle.read(MAX_VALUE_BYTES + 1)
    if len(data) > MAX_VALUE_BYTES:
        raise ValueError(f"{path} is larger than the 1 MiB value limit")
    return data


def check_destination(path: str) -> None:
    try:
        mode = os.stat(path).st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISREG(mode):
        raise ValueError(f"{path} exists and is not a regular file")


def write_value_file_atomic(path: str, data: bytes) -> None:
    check_destination(path)
    directory = os.path.dirname(os.path.abspath(path))
    try:
        mode = stat.S_IMODE(os.stat(path).st_mode)
    except FileNotFoundError:
        umask = os.umask(0)
        os.umask(umask)
        mode = 0o666 & ~umask
    descriptor, temporary = tempfile.mkstemp(dir=directory, prefix=".mp3-get-", suffix=".tmp")
    try:
        with os.fdopen(descriptor, "wb") as handle:
            os.fchmod(handle.fileno(), mode)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _sync_directory(directory)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _sync_directory(directory: str) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def format_version(version: Version | None) -> str | None:
    return None if version is None else f"{version.kind}:{version.number}:{version.writer_id}"


def _summary(command: str, key: str, response: ControlResponse) -> dict:
    return {"status": response.status.value, "command": command, "key": key,
            "key_id": response.key_id, "mode": response.mode.value,
            "epoch": response.epoch, "version": format_version(response.version),
            "acks": list(response.owners), "node": response.node_id,
            "message": response.error or ""}


def vm_name(label: str) -> str:
    match = re.fullmatch(r"(?i)vm0?([1-9]|10)", label)
    if match is None:
        raise ValueError(f"{label!r} is not a VM name (VM1 to VM10)")
    return f"vm{int(match.group(1)):02d}"


def resolve_replica(manifest: Manifest, text: str) -> str:
    try:
        return vm_name(text)
    except ValueError:
        pass
    host, separator, port = text.rpartition(":")
    if separator and re.fullmatch(r"[0-9]+", port):
        matches = [node for node in manifest.nodes
                   if node.kv.host == host and node.kv.port == int(port)]
    else:
        matches = [node for node in manifest.nodes if node.kv.host == text]
    if len(matches) != 1:
        raise ValueError(f"{text!r} does not name exactly one configured replica "
                         "(use a VM name or host:port)")
    return matches[0].vm_id


def run_ls(manifest: Manifest, key: str) -> tuple[int, dict]:
    owners = manifest.owners(key)
    return 0, {"status": "OK", "command": "ls", "key": key,
               "key_id": ring_id_hex(key_position(key)),
               "owners": [{"vm": node.vm_id, "ring_id": ring_id_hex(node.ring_id),
                           "address": f"{node.kv.host}:{node.kv.port}"} for node in owners]}


def _json(text: str) -> dict:
    data = json.loads(text or "{}")
    if not isinstance(data, dict):
        raise ValueError("the reply detail is not a JSON object")
    return data


def run_list_mem_ids(manifest: Manifest, vm_id: str) -> tuple[int, dict]:
    try:
        reply = send_command(manifest, vm_id, "list_mem_ids")
    except ControlError as error:
        return 1, {"status": "ERROR", "command": "list_mem_ids", "message": str(error)}
    if reply.status != ControlStatus.OK:
        return 1, {"status": reply.status.value, "command": "list_mem_ids",
                   "message": reply.error or ""}
    try:
        detail = _json(reply.detail)
        node_ring, members = detail["node_ring_id"], detail["rows"]
    except (ValueError, KeyError) as error:
        return 1, {"status": "ERROR", "command": "list_mem_ids",
                   "message": f"unreadable reply from {vm_id}: {error}"}
    return 0, {"status": "OK", "command": "list_mem_ids", "node": reply.node_id,
               "node_ring_id": node_ring, "members": members}


def run_liststore(manifest: Manifest, vm_id: str, emit: Callable[[dict], None]
                  ) -> tuple[int, dict]:
    cursor, total, node_ring = None, 0, None
    while True:
        body = b"" if cursor is None else json.dumps({"after": cursor}).encode("utf-8")
        try:
            reply = send_command(manifest, vm_id, "liststore", "", body)
        except ControlError as error:
            return 1, {"status": "ERROR", "command": "liststore", "keys": total,
                       "message": str(error)}
        if reply.status != ControlStatus.OK:
            return 1, {"status": reply.status.value, "command": "liststore", "keys": total,
                       "message": reply.error or ""}
        try:
            page = json.loads(reply.value)
            node_ring = _json(reply.detail)["node_ring_id"]
            keys, following = page["keys"], page["next"]
            if not isinstance(keys, list) or not all(isinstance(k, str) for k in keys):
                raise ValueError("keys must be a list of text")
            if following is not None and (
                    not isinstance(following, str) or (cursor is not None and
                    following.encode("utf-8") <= cursor.encode("utf-8"))):
                raise ValueError("the page cursor did not advance")
        except (ValueError, KeyError, TypeError) as error:
            return 1, {"status": "ERROR", "command": "liststore", "keys": total,
                       "message": f"unreadable page from {vm_id}: {error}"}
        for key in keys:
            emit({"key": key, "key_id": ring_id_hex(key_position(key))})
            total += 1
        if following is None:
            return 0, {"status": "OK", "command": "liststore", "node": reply.node_id,
                       "node_ring_id": node_ring, "keys": total}
        cursor = following


def run_getfromreplica(manifest: Manifest, vm_id: str, address: str, key: str,
                       path: str) -> tuple[int, dict]:
    try:
        target = resolve_replica(manifest, address)
        check_destination(path)
    except ValueError as error:
        return 2, {"status": "ERROR", "command": "getfromreplica", "key": key,
                   "message": str(error)}
    try:
        reply = send_command(manifest, vm_id, "read_replica", key,
                             json.dumps({"target": target}).encode("utf-8"))
    except ControlError as error:
        return 1, {"status": "ERROR", "command": "getfromreplica", "key": key,
                   "replica": target, "message": str(error)}
    result = {"status": reply.status.value, "command": "getfromreplica", "key": key,
              "key_id": reply.key_id, "replica": target,
              "version": format_version(reply.version), "message": reply.error or ""}
    if reply.status != ControlStatus.OK:
        return 1, result
    try:
        write_value_file_atomic(path, reply.value)
    except OSError as error:
        result.update(status="ERROR", message=f"could not write {path}: {error}")
        return 1, result
    result["bytes"] = len(reply.value)
    return 0, result


def run_multiput(manifest: Manifest, vm_id: str, key: str, labels) -> tuple[int, dict]:
    try:
        names = [vm_name(label) for label in labels]
        reply = send_command(manifest, vm_id, "multiput", key,
                             json.dumps({"vms": names}).encode("utf-8"),
                             timeout=MODE_CLIENT_TIMEOUT)
    except ValueError as error:
        return 2, {"status": "ERROR", "command": "multiput", "key": key, "message": str(error)}
    except ControlError as error:
        return 1, {"status": "ERROR", "command": "multiput", "key": key, "message": str(error)}
    try:
        detail = _json(reply.detail)
    except ValueError:
        detail = {}
    return (0 if reply.status == ControlStatus.OK else 1), {
        "status": reply.status.value, "command": "multiput", "key": key,
        "key_id": reply.key_id, "multiput_id": detail.get("multiput_id"),
        "nodes": detail.get("nodes", []), "message": reply.error or ""}


def run_mode(manifest: Manifest, vm_id: str, mode: str) -> tuple[int, dict]:
    try:
        response = send_command(manifest, vm_id, "mode", mode, timeout=MODE_CLIENT_TIMEOUT)
    except ControlError as error:
        return 1, {"status": "ERROR", "command": "consistency_mode", "message": str(error)}
    try:
        transition = json.loads(response.detail or "{}")
    except ValueError:
        transition = {}
    result = {"status": response.status.value, "command": "consistency_mode",
              "mode": response.mode.value, "epoch": response.epoch, "node": response.node_id,
              "message": response.error or "", "transition": transition}
    ok = response.status in (ControlStatus.OK, ControlStatus.NO_CHANGE)
    return (0 if ok else 1), result


def run_put(manifest: Manifest, vm_id: str, key: str, path: str) -> tuple[int, dict]:
    try:
        value = read_value_file(path)
    except (OSError, ValueError) as error:
        return 2, {"status": "ERROR", "command": "put", "key": key, "message": str(error)}
    try:
        response = send_command(manifest, vm_id, "put", key, value)
    except ControlError as error:
        return 1, {"status": "ERROR", "command": "put", "key": key, "message": str(error)}
    return (0 if response.status == ControlStatus.OK else 1), _summary("put", key, response)


def run_get(manifest: Manifest, vm_id: str, key: str, path: str) -> tuple[int, dict]:
    try:
        check_destination(path)
    except ValueError as error:
        return 2, {"status": "ERROR", "command": "get", "key": key, "message": str(error)}
    try:
        response = send_command(manifest, vm_id, "get", key)
    except ControlError as error:
        return 1, {"status": "ERROR", "command": "get", "key": key, "message": str(error)}
    result = _summary("get", key, response)
    if response.status != ControlStatus.OK:
        return 1, result
    try:
        write_value_file_atomic(path, response.value)
    except OSError as error:
        result.update(status="ERROR", message=f"could not write {path}: {error}")
        return 1, result
    result["bytes"] = len(response.value)
    return 0, result
