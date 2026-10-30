# CS 425 MP3: distributed key-value store

**Batches 1 and 2 are locally implemented and tested. Distributed KV operations
remain unfinished.** The foundation includes local SQLite WAL/FULL storage,
fixed SHA-256 ring ownership, and a restart-aware membership service. KV RPC,
coordinators, consistency protocols, read repair, command integration and mode
changes remain future work. The October 21 specification in [SPEC.md](SPEC.md) is
authoritative.

Start with [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md),
[KEY_DECISIONS.md](KEY_DECISIONS.md), [AGENTS.md](AGENTS.md), and
[EXPLANATION.md](EXPLANATION.md). The separate first-person learning journal is
[BUILD_PROGRESS_EXPLAINED.md](BUILD_PROGRESS_EXPLAINED.md). Claude Code entrypoints
use the same shared agent rules.

## Layout

```text
mp3/
  config.py                    constants and local example inventory parser
  models.py                    local result and storage domain types
  storage.py                   local SQLite WAL/FULL store and version allocation
  framing.py                   bounded four-byte length-prefixed raw-byte frames
  ring.py                      ten-node manifest, SHA-256 IDs and five owners
  membership.py                retained suspicion, TCP gossip and restart recovery
  legacy_membership.py         byte-identical MP2 reference; not the MP3 service
  cli.py, pending.py            parsing and NOT_IMPLEMENTED command boundary
  tests/                        standard-library local tests
  *.md                          shared plans, explanation, journal and decisions
```

## Installation and local verification

No installation or third-party dependency is needed. The development-machine
Python/VM compatibility decision remains pending KD-01. Run these commands from
the repository root:

```bash
python3 -B -m unittest discover -s mp3/tests -v
python3 -B -m mp3 --smoke
python3 -B -m mp3 --help
```

The suite has 71 tests. It uses temporary SQLite directories, fake clocks,
subprocesses killed with SIGKILL, and temporary loopback membership listeners. It
contacts no university VM. The verified results and limits are recorded in the
implementation plan. Do not run `mp1/test/test_suite.py`; its fixtures SSH to real
VMs and replace logs. The MP2 README mentions a `selftest.py` absent from this copy.

## Commands currently working

Help, local smoke, parsing and explicit rejection of unfinished operations work:

```bash
python3 -B -m mp3 put key localfilename
python3 -B -m mp3 get key localfilename
python3 -B -m mp3 ls key
python3 -B -m mp3 liststore
python3 -B -m mp3 getfromreplica VMaddress key localfilename
python3 -B -m mp3 list_mem_ids
python3 -B -m mp3 'multiput(key, VM1, VM10)'
python3 -B -m mp3 consistency_mode eventual-RR
```

All eight distributed commands still return `NOT_IMPLEMENTED` and exit 3. Invalid
syntax exits 2. No command changes a value file, connects to a server, completes a
PUT/GET, or switches consistency modes.

## Local foundation APIs

```python
from mp3.ring import Endpoint, Manifest
from mp3.models import ConsistencyMode, Context, Record
from mp3.storage import open_store
from mp3.membership import MembershipRuntime

endpoints = {
    f"vm{i:02d}": {
        "kv": Endpoint("127.0.0.1", 42000 + i * 3),
        "membership": Endpoint("127.0.0.1", 42001 + i * 3),
        "log_query": Endpoint("127.0.0.1", 42002 + i * 3),
    }
    for i in range(1, 11)
}
manifest = Manifest.from_endpoints(endpoints)  # local example only; contacts no hosts
owners = manifest.owners("photo")             # five fixed prescribed owners

context = Context(1, ConsistencyMode.EVENTUAL_NO_RR)
with open_store("/tmp/example.sqlite", bootstrap=context, vm_id="vm01") as store:
    runtime = MembershipRuntime(store, manifest)
    runtime.start(background=False, listen=False)  # reserves generation before service
    version = store.allocate_eventual_version(context=context)
    store.install_if_newer("key", Record(version, b"value"), context=context)
    runtime.stop()
```

`Manifest` accepts exactly vm01..vm10, validates three unique endpoints per VM,
computes ring IDs as unsigned SHA-256 of `mp3/node/<vm_id>`, and fingerprints
canonical JSON. `Manifest.owners(key)` hashes strict UTF-8 `mp3/key/<key>` and
selects the first five clockwise nodes, including an exact-position match. Crashed
or suspected machines stay in the result. The local `photo` test manifest selects
vm03, vm09, vm04, vm08 and vm06; those numbers are actual local hash output, not a
university deployment.

`MembershipRuntime.start()` validates the recovered Store context, calls
`start_incarnation()` once per runtime/process startup, then opens its membership
listener and optional gossip loop. Membership snapshots include all ten members in
ring-position order. `list_mem_ids()` returns diagnostic rows sorted by ascending
numeric ring ID (`nodeID`) and includes each member's stable VM identity and ring ID.
Peers become SUSPECTED after 2.5 seconds without increasing heartbeat progress and
remain present indefinitely. A newer durable restart generation restores ALIVE;
old generations cannot override it. Gossip defaults to a 0.5-second interval and
fanout three and uses a separate bounded TCP service. Membership messages are capped
at 65,536 bytes; counters have a documented 256-digit defensive resource bound.
These facilities have only local test evidence, not ten-VM validation.

The storage engine uses a separate SQLite connection per operation, WAL and
`synchronous=FULL`, bounded admission (default 8), and a five-second local transaction
budget. It preserves present empty values separately from missing keys. It provides
no distributed quorum, KV RPC, Strong tag allocator, or mode-switching API.

## Logs and MP1

No KV service starts or operation logs are created at CLI import/startup. The
JSON-lines formatter is available for explicit local setup; required KV receipt and
response events and MP1 log-query integration remain future work under KD-17. MP1
uses `machine.<vm>.log` and port 65432; framing interoperability tests do not prove
end-to-end MP1 queries against MP3 logs. MP1 and MP2 source remain unchanged.

## What remains

Batch 3 will add KV TCP RPC and replica request handling. Later work implements
Strong ABD, Eventual-noRR, Eventual-RR/read repair, mode switching, all command
integration, ten-VM correctness, benchmarks, report, deployment and demo readiness.
No deployment/reset script exists. VM inventory, interpreter versions and port
availability are unverified. Obtain explicit authorization before VM access,
deployment or remote fault tests. This workspace has no Git metadata; do not
initialize history, commit or push.
