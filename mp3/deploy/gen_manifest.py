#!/usr/bin/env python3
"""Write manifest.json and inventory.json for the ten MP3 nodes.

remote: the ten course VMs, every VM using the same four ports.
local:  ten processes on 127.0.0.1 with distinct ports, for practice on one machine.
The inventory advertises schema 3 and a release id derived from the mp3 sources, so every
node started from the same code activates Strong, Eventual-noRR and Eventual-RR together.
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PACKAGE_DIR.parent))

from mp3.ring import Endpoint, Manifest  # noqa: E402

SERVICES = ("kv", "membership", "log_query", "control")


def release_id() -> str:
    """Stable hash of the node's source files (tests, docs and deploy tooling excluded)."""
    digest = hashlib.sha256()
    for path in sorted(PACKAGE_DIR.glob("*.py")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()[:16]


def endpoints(target: str, hosts: list[str], base_port: int) -> dict:
    result = {}
    for index, host in enumerate(hosts):
        vm_id = f"vm{index + 1:02d}"
        if target == "local":
            ports = [base_port + 4 * index + offset for offset in range(4)]
        else:
            ports = [base_port + offset for offset in range(4)]
        result[vm_id] = {name: Endpoint(host, port) for name, port in zip(SERVICES, ports)}
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("remote", "local"), required=True)
    parser.add_argument("--out", required=True, help="directory for manifest.json and inventory.json")
    parser.add_argument("--host-format", default="fa26-cs425-48{n:02d}.cs.illinois.edu",
                        help="remote host name; {n:02d} is the VM number")
    parser.add_argument("--base-port", type=int, default=None)
    args = parser.parse_args()
    if args.target == "local":
        hosts = ["127.0.0.1"] * 10
        base_port = args.base_port or 43000
    else:
        hosts = [args.host_format.format(n=n) for n in range(1, 11)]
        base_port = args.base_port or 65441
    manifest = Manifest.from_endpoints(endpoints(args.target, hosts, base_port))
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "manifest.json").write_text(manifest.to_json() + "\n", encoding="utf-8")
    release = release_id()
    inventory = {"manifest_id": manifest.fingerprint,
                 "nodes": {node.vm_id: {"release_id": release, "schema_version": 3}
                           for node in manifest.nodes}}
    (out / "inventory.json").write_text(json.dumps(inventory, sort_keys=True) + "\n",
                                        encoding="utf-8")
    print(f"manifest {manifest.fingerprint[:12]} release {release} -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
