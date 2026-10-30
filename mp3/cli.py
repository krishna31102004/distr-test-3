"""Syntax validation and explicit pending-operation output; no KV service yet."""

import json
import re
import shlex
import sys

from .config import MAX_VALUE_BYTES, parse_inventory
from .framing import recv_frame, send_frame
from .models import Command, ConsistencyMode, Status
from .pending import execute
from .ring import RingNode, select_successors

ARITY = {"put": 2, "get": 2, "ls": 1, "liststore": 0,
         "getfromreplica": 3, "list_mem_ids": 0, "consistency_mode": 1}
HELP = """Phase 0 syntax checker (distributed commands return NOT_IMPLEMENTED).
Working: python3 -m mp3 --help | --smoke
Pending commands:
  put key localfilename
  get key localfilename
  ls key
  liststore
  getfromreplica VMaddress key localfilename
  list_mem_ids
  multiput key VM1 [VM2 ... VM10]
  'multiput(key, VM1, VM2)'
  consistency_mode {strong,eventual-noRR,eventual-RR}
Quote keys and paths with spaces. No command opens local value files yet.
Multiput labels and exact completion text await original-spec review (KD-14).
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
        if len(args) < 2 or any(not re.fullmatch(r"VM(?:[1-9]|10)", vm)
                                for vm in args[1:]):
            raise ValueError("multiput requires a key and VM1..VM10 labels")
        if len(set(args[1:])) != len(args[1:]):
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


def main(argv: list[str] | None = None) -> int:
    """Print help/smoke or guarded JSON result; return 0, 2, or 3 respectively."""
    argv = sys.argv[1:] if argv is None else argv
    if argv in ([], ["--help"], ["-h"]):
        print(HELP)
        return 0
    if argv == ["--smoke"]:
        smoke()
        print("Phase 0 smoke OK: framing, inventory, successors; KV NOT_IMPLEMENTED")
        return 0
    try:
        # A parenthesized command can be passed as one quoted shell argument.
        line = argv[0] if len(argv) == 1 else shlex.join(argv)
        result = execute(parse_command(line))
    except ValueError as error:
        print(json.dumps({"status": "ERROR", "message": str(error)}))
        return 2
    print(json.dumps({"status": result.status.value, "message": result.message}))
    return 3 if result.status == Status.NOT_IMPLEMENTED else 0
