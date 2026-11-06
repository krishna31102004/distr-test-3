"""Command-line client: parses the eight commands and runs them against a node."""

import json
import os
import re
import shlex
import sys

from .commands import (
    run_get, run_getfromreplica, run_list_mem_ids, run_liststore, run_ls, run_mode,
    run_multiput, run_put, vm_name,
)
from .config import MAX_VALUE_BYTES, parse_inventory
from .framing import recv_frame, send_frame
from .models import Command, ConsistencyMode
from .ring import RingNode, load_manifest, select_successors

ARITY = {"put": 2, "get": 2, "ls": 1, "liststore": 0,
         "getfromreplica": 3, "list_mem_ids": 0, "consistency_mode": 1}
HELP = """MP3 command line.
python3 -m mp3 [--manifest PATH] [--vm-id vmNN] COMMAND ...
  (or set MP3_MANIFEST and MP3_VM_ID); also: --help | --smoke
Commands (all but ls talk to the node running on --vm-id):
  put key localfilename
  get key localfilename
  ls key                          owners and ring IDs; needs only --manifest
  liststore                       keys stored on this VM, with key IDs
  getfromreplica VMaddress key localfilename   one replica's own copy, no repair
  list_mem_ids                    membership with ring IDs, sorted by ring ID
  multiput key VM1 [VM2 ... VM10]    or  'multiput(key, VM1, VM2)'
  consistency_mode {strong,eventual-noRR,eventual-RR}
Quote keys and paths with spaces. Only put, get and getfromreplica read or write local files.
Exit codes: 0 success, 1 operation failed, 2 usage or configuration error.
"""


def parse_command(line: str) -> Command:
    """Validate one command; return normalized name/arguments or raise ValueError."""
    if line.strip().startswith("multiput("):
        match = re.fullmatch(r"\s*multiput\(([^()]*)\)\s*", line)
        if not match:
            raise ValueError("expected multiput(key, VM1, ..., VM10)")
        parts = ["multiput", *(part.strip() for part in match.group(1).split(","))]
    else:
        parts = shlex.split(line)
    if not parts or any(not part or "\x00" in part for part in parts):
        raise ValueError("command and arguments must be nonempty and contain no NUL")
    name, *args = parts
    if name == "multiput":
        if not 2 <= len(args) <= 11 or any(not re.fullmatch(r"(?i)vm0?(?:[1-9]|10)", vm)
                                           for vm in args[1:]):
            raise ValueError("multiput requires a key and 1 to 10 VM labels (VM1..VM10)")
        # VM3, vm3 and vm03 are the same machine.
        if len({vm_name(vm) for vm in args[1:]}) != len(args[1:]):
            raise ValueError("multiput VM labels must be distinct")
    elif name not in ARITY or len(args) != ARITY[name]:
        raise ValueError("unknown command or incorrect number of arguments; use --help")
    if name == "consistency_mode":
        ConsistencyMode(args[0])
    return Command(name, tuple(args))


def smoke() -> None:
    """Check a local socket pair, 1 MiB bytes, inventory and supplied ring IDs."""
    import socket
    import threading

    payload = bytes(range(256)) * (MAX_VALUE_BYTES // 256)
    sender, receiver = socket.socketpair()
    with sender, receiver:
        sender.settimeout(2)
        receiver.settimeout(2)
        errors = []

        def send():
            """Send in parallel so a full-size value cannot fill a local buffer."""
            try:
                send_frame(sender, payload, max_bytes=MAX_VALUE_BYTES)
                send_frame(sender, b"", max_bytes=MAX_VALUE_BYTES)
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=send, daemon=True)
        thread.start()
        received = recv_frame(receiver, max_bytes=MAX_VALUE_BYTES)
        empty = recv_frame(receiver, max_bytes=MAX_VALUE_BYTES)
        thread.join(timeout=3)
        if errors or thread.is_alive() or received != payload or empty != b"":
            raise RuntimeError("framing smoke failed")
    machines = parse_inventory("vm2 localhost 42002\nvm1 localhost 42001")
    owners = select_successors([RingNode("a", 10), RingNode("b", 20)],
                               21, count=2, include_equal=True)
    if machines[0].node_id != "vm1" or [node.node_id for node in owners] != ["a", "b"]:
        raise RuntimeError("configuration/ring smoke failed")


def split_options(argv: list[str]) -> tuple[str | None, str | None, list[str]]:
    """Take leading --manifest/--vm-id options (environment fallback); return the rest."""
    manifest = os.environ.get("MP3_MANIFEST")
    vm_id = os.environ.get("MP3_VM_ID")
    rest = list(argv)
    while rest and rest[0] in ("--manifest", "--vm-id"):
        if len(rest) < 2:
            raise ValueError(f"{rest[0]} needs a value")
        if rest[0] == "--manifest":
            manifest = rest[1]
        else:
            vm_id = rest[1]
        rest = rest[2:]
    return manifest, vm_id, rest


def _load_manifest(path: str, vm_id: str | None = None):
    """Read the manifest (and check `vm_id` is in it); ValueError explains any problem."""
    try:
        manifest = load_manifest(path)
        if vm_id is not None:
            manifest.node(vm_id)
    except (OSError, ValueError) as error:
        raise ValueError(f"cannot use manifest {path!r} / {vm_id!r}: {error}") from error
    return manifest


def main(argv: list[str] | None = None) -> int:
    """Print help/smoke or one JSON result; return 0 success, 1 failed, 2 usage or configuration."""
    argv = sys.argv[1:] if argv is None else argv
    try:
        manifest_path, vm_id, argv = split_options(argv)
    except ValueError as error:
        print(json.dumps({"status": "ERROR", "message": str(error)}))
        return 2
    if argv in ([], ["--help"], ["-h"]):
        print(HELP)
        return 0
    if argv == ["--smoke"]:
        smoke()
        print("Smoke OK: framing, inventory, successors")
        return 0
    try:
        # A parenthesized command can be passed as one quoted shell argument.
        line = argv[0] if len(argv) == 1 else shlex.join(argv)
        command = parse_command(line)
        if command.name == "ls":
            if not manifest_path:
                raise ValueError("ls needs the manifest: pass --manifest PATH or set MP3_MANIFEST")
            code, output = run_ls(_load_manifest(manifest_path), *command.args)
            print(json.dumps(output))
            return code
        if not manifest_path or not vm_id:
            raise ValueError(f"{command.name} needs a node: pass --manifest PATH and "
                             "--vm-id vmNN, or set MP3_MANIFEST and MP3_VM_ID")
        manifest = _load_manifest(manifest_path, vm_id)
        if command.name == "liststore":
            code, output = run_liststore(manifest, vm_id,
                                         lambda row: print(json.dumps(row), flush=True))
        elif command.name == "list_mem_ids":
            code, output = run_list_mem_ids(manifest, vm_id)
        elif command.name == "multiput":
            code, output = run_multiput(manifest, vm_id, command.args[0], command.args[1:])
        else:
            runner = {"put": run_put, "get": run_get, "consistency_mode": run_mode,
                      "getfromreplica": run_getfromreplica}[command.name]
            code, output = runner(manifest, vm_id, *command.args)
        print(json.dumps(output))
        return code
    except ValueError as error:
        print(json.dumps({"status": "ERROR", "message": str(error)}))
        return 2
