# CS 425 MP3: distributed key-value store

**Batches 1–4 are implemented and locally tested. Distributed client-facing KV
operations remain unfinished.** The foundation includes local SQLite WAL/FULL
storage, fixed SHA-256 ring ownership, restart-aware membership, and a bounded
single-replica KV TCP request/response service, and an integrated node runtime.
Quorum coordinators, consistency
protocols, read repair, operational command integration and mode changes remain
future work. The October 21 specification in [SPEC.md](SPEC.md) is authoritative.

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
  framing.py                   legacy-compatible and total-deadline byte framing
  rpc.py                       strict C5 request/response codecs and one-call client
  replica.py                   bounded local READ_LOCAL/INSTALL_IF_NEWER service
  eventlog.py                  structured logs and local replica event lines
  ring.py                      ten-node manifest, SHA-256 IDs and five owners
  membership.py                retained suspicion, TCP gossip and restart recovery
  node.py                      one-node lifecycle and diagnostic launcher
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

The suite currently has 127 tests. It uses temporary SQLite directories, fake
clocks, subprocesses killed with SIGKILL, and temporary loopback membership and KV
listeners. `tests/test_node_runtime.py` starts three logical VMs on loopback and also
tests a separate-process crash/restart path. It contacts no university VM. The verified results and limits are recorded
in the implementation plan. Do not run `mp1/test/test_suite.py`; its fixtures SSH to
real VMs and replace logs. The MP2 README mentions a `selftest.py` absent from this copy.

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
budget. It preserves present empty values separately from missing keys. A replica
request passes its absolute service deadline into local storage so admission and
SQLite lock waits do not get a fresh budget.

## Start one local node runtime

`python3 -B -m mp3.node` is a diagnostic node entrypoint, separate from the existing
`python3 -m mp3` command parser. It starts one logical VM's local SQLite store,
membership listener/gossip, and single-replica KV listener. It does not run a
distributed client PUT or GET. Supply a valid ten-node manifest with separate KV,
membership and log-query endpoints. Create the database and log parent directories
first; the node does not create or replace them implicitly:

```bash
mkdir -p data logs
python3 -B -m mp3.node --vm-id vm01 --manifest ./manifest.json \
  --db ./data/vm01.sqlite --bootstrap --log ./logs/vm01.jsonl
```

Use `--bootstrap` only for a new database. It creates Strong epoch 1 by default;
`--bootstrap-mode eventual-noRR` is available for explicitly disposable local
protocol tests. For an existing VM database, use `--recover` instead; its persisted
identity, mode, epoch, records and restart metadata are validated and preserved.
The entrypoint prints local `READY` only after membership and KV listeners start.
That does not mean the other nine VMs are reachable. Press Ctrl-C or send SIGTERM
for bounded shutdown. A forced non-clean stop is reported and the runtime can retry
shutdown after outstanding work exits; SQLite remains open until accepted storage
work has ended.

```bash
python3 -B -m mp3.node --help
```

For local integration, `NodeRuntime.from_files(...)` reads the manifest; `start()`
performs ordered startup; `owners(key)`, `membership`, `generation`, and
`call_replica(...)` expose diagnostics and one remote READ_LOCAL or INSTALL_IF_NEWER
operation; `stop()` returns a `NodeShutdownReport`. A call reaches one replica only.
Strong ABD, Eventual coordinators, quorum behavior, all-owner propagation and repair
remain future work.

## Single-replica RPC foundation

`mp3.rpc.ReplicaClient.call_replica(target, request, deadline=...)` opens one TCP
connection, sends one validated READ_LOCAL or INSTALL_IF_NEWER request, and validates
one response against the request, manifest, target and expected membership generation.
The framing is binary-safe through 1 MiB and uses bounded JSON metadata. Calls do not
retry automatically; transport failure after a write may have been sent is marked as
an unknown completion. Fixed manifest KV hostnames are resolved when the client is
initialized; each call tries up to eight numeric addresses under its remaining
absolute deadline. `mp3.replica.ReplicaService` serves these operations using a
bounded worker pool and queue, then reads or durably installs against that VM's local
SQLite database. These are local replica primitives: they do not compute quorum
results, replicate to other owners, or make CLI PUT/GET operational.

Receipt and response events print the required local timestamp line and can append
structured records when a logger is configured. Full MP1 distributed log-query
integration remains future work.

## Logs and MP1

No KV service starts at CLI import/startup. `ReplicaService` prints/logs receipt and
response events when explicitly started by a caller. MP1
uses `machine.<vm>.log` and port 65432; framing interoperability tests do not prove
end-to-end MP1 queries against MP3 logs. MP1 and MP2 source remain unchanged.

## What remains

Later work implements Strong ABD, Eventual-noRR, Eventual-RR/read repair, mode
switching, all command integration, ten-VM correctness, benchmarks, report,
deployment and demo readiness.
No deployment/reset script exists. VM inventory, interpreter versions and port
availability are unverified. Obtain explicit authorization before VM access,
deployment or remote fault tests. The private review-repository workflow is documented
in `AGENTS.md`; the professor's course repository remains separate.
