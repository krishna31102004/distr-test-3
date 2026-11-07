# CS 425 MP3: distributed key-value store

**Steps 2, 3 and 4 are implemented and locally tested; nothing has been run on the university
VMs yet.** The system is a ten-node key-value store with fixed SHA-256 ring ownership (five owners
per key), restart-aware membership, durable local SQLite storage, a bounded replica TCP service,
and three consistency modes: Strong (two-phase ABD), Eventual-noRR and Eventual-RR (read repair).
Each node also runs a control endpoint, and all eight required commands (`put`, `get`, `ls`,
`liststore`, `getfromreplica`, `list_mem_ids`, `multiput`, `consistency_mode`) run from the command
line against a node. A cluster-wide mode change (coordinated by vm01) clears or keeps data as the
assignment requires, and per-node `machine.<N>.log` logs can be searched with the unchanged MP1
server and client. Ten-VM deployment, benchmarks, the report and the demo are still ahead. The
October 21 specification in [SPEC.md](SPEC.md) is authoritative.

Start with [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md),
[KEY_DECISIONS.md](KEY_DECISIONS.md), [AGENTS.md](AGENTS.md), and
[EXPLANATION.md](EXPLANATION.md). The separate first-person learning journal is
[BUILD_PROGRESS_EXPLAINED.md](BUILD_PROGRESS_EXPLAINED.md). Claude Code entrypoints
use the same shared agent rules.

The Strong ABD and Eventual-noRR design in
[STEP3_PROTOCOL_DESIGN.md](STEP3_PROTOCOL_DESIGN.md) was approved by Krishna on
October 31, 2026. Shared Step 3 Batch 1 infrastructure and Strong ABD Batch 2 are
implemented and independently reviewed for local operation. Eventual-noRR Batch 3
is also independently reviewed for local loopback/subprocess operation; it is not
ten-VM deployment evidence.

The Step 4 design is in [STEP4_IMPLEMENTATION_DESIGN.md](STEP4_IMPLEMENTATION_DESIGN.md); the
approved choices and the two deviations from it are recorded in
[KEY_DECISIONS.md](KEY_DECISIONS.md) ("Step 4 approval record"). Batches A to D and their
evidence are in [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md); all of it is local
loopback/subprocess evidence, not ten-VM evidence.

## Layout

```text
mp3/
  config.py                    constants and local example inventory parser
  models.py                    local result and storage domain types
  storage.py                   local SQLite WAL/FULL store and version allocation
  framing.py                   legacy-compatible and total-deadline byte framing
  rpc.py                       strict C5 request/response codecs and one-call client
  attempts.py                  bounded mutable lifetime lease for replica attempts
  coordinator.py               schema activation, evidence types, distinct collector,
                                and bounded coordinator/outbound worker resources
  eventual.py                  programmatic Eventual-noRR R/W coordinator and
                                bounded post-ack propagation report
  replica.py                   bounded local READ_LOCAL/INSTALL_IF_NEWER service
  eventlog.py                  structured logs and local replica event lines
  ring.py                      ten-node manifest, SHA-256 IDs and five owners
  membership.py                retained suspicion, TCP gossip and restart recovery
  node.py                      one-node lifecycle and diagnostic launcher
  control.py                   bounded control service and client for put/get/status
  commands.py                  CLI put/get file handling (1 MiB cap, atomic get output)
  legacy_membership.py         byte-identical MP2 reference; not the MP3 service
  multiput.py                  simultaneous puts from several VMs (ARM, READY, GO)
  cli.py                       command parsing and routing for all eight commands
  tests/                        standard-library local tests
  *.md                          shared plans, explanation, journal and decisions
```

## Installation and local verification

No installation or third-party dependency is needed. The development-machine
Python/VM compatibility decision remains pending KD-01. Run these commands from
the repository root:

```bash
python3 -B -m unittest discover -s mp3/tests -v
python3 -B -m unittest mp3.tests.test_strong mp3.tests.test_history_checker -v
python3 -B -m unittest mp3.tests.test_eventual -v
python3 -B -m unittest mp3.tests.test_protocol_adversarial -v
MP3_TARGET=local python3 -B -m unittest mp3.tests.test_mp1_ten_process_deployment -v
python3 -B -m mp3 --smoke
python3 -B -m mp3 --help
python3 -B -m mp3.node --help
python3 -B -m compileall -q mp3
git diff --check
```

The original Batch 3 acceptance baseline on November 2 passed **198 tests** (main:
99.114s; independent reviewer: 107.886s). The final Eventual GET dispatch correction
raised that to **202 tests**. Current Step 3 Batch 4 verification on Python 3.12.4
passed **205 tests in 125.887s** (main) and **205 in 125.888s** (independent reviewer).
Step 4 Batch A (control endpoint, schema-3 groundwork, `put`/`get` CLI) raised the suite to
259 tests, and Batch B (Eventual-RR, structured logs, MP1 query) to
284 tests, Batch C (cluster-wide mode switching) to 329 tests, and Batch D (the last five
commands and `multiput`) to 361 tests. After the behavior-preserving cleanup pass the suite is
**359 tests, all passing in 499 s** on Python 3.11.9 (main agent, November 5, 2026; the two removed
tests exercised only the deleted stub-era result types). Haiku read-only review rounds are recorded
in `IMPLEMENTATION_PLAN.md`.
Focused runs passed: **14 Strong/history tests in 30.047s**, **25 Eventual tests in
72.193s**, and **3 adversarial integration tests in 8.276s** (reviewer: 3 in 8.252s).
The suite uses temporary
SQLite directories, fake clocks, subprocesses killed with SIGKILL, and temporary
loopback membership and KV listeners. `tests/test_node_runtime.py` starts three
logical VMs on loopback and also tests a separate-process crash/restart path, plus
schema-2 Strong and Eventual-noRR single-replica calls using an explicit compatibility
fixture. Strong multi-owner behavior is covered separately by `tests/test_strong.py`
and `tests/test_history_checker.py`; those are local tests, not ten-VM evidence. The
Batch 1 regression suite also
checks admitted coordinator work during shutdown, fixed-owner checks at dispatch and
replicas, prompt impossible-threshold handling, and explicit durable-ack tracking.
It contacts no university VM. The verified results and limits are recorded in the
implementation plan. Do not run `mp1/test/test_suite.py`; its fixtures SSH to
real VMs and replace logs. The MP2 README mentions a `selftest.py` absent from this copy.

Step 3 Batch 4 is verified for local adversarial integration and adds three tests: a seeded, barrier-started Strong
history from three coordinator identities, and bounded coordinator admission checks
in separate fresh Strong and Eventual-noRR clusters. These supplement the detailed
protocol and crash/recovery tests in `test_strong.py` and `test_eventual.py`. Final
Batch 4 counts and review status are recorded in the plan. No university VM or
ten-VM validation was performed.

For an explicit schema-2 local test, `mp3.node` accepts `--schema2-inventory PATH`.
The JSON inventory must name all ten manifest identities and give each the same
release ID with schema version 2 or newer. This is an operator-supplied configuration
assertion; it does not check or prove compatibility of deployed VMs. Without the
inventory, protocol-phase RPC remains disabled. Even when enabled, node methods send
only one replica RPC and do not run Strong/Eventual coordination.

## Commands currently working

`put` and `get` work through the control endpoint of a running node. Start a node
(`python3 -B -m mp3.node --vm-id vm01 --manifest manifest.json --db vm01.sqlite
--bootstrap --schema2-inventory inventory.json`; the manifest needs a `control`
endpoint for every node), then run the command from the same VM:

```bash
python3 -B -m mp3 --manifest manifest.json --vm-id vm01 put key localfilename
python3 -B -m mp3 --manifest manifest.json --vm-id vm01 get key localfilename
# or: export MP3_MANIFEST=manifest.json MP3_VM_ID=vm01
```

Each prints one JSON line with the status, the key's ring ID, the mode and epoch, the
version, and the acknowledging nodes. Exit codes: 0 OK, 1 the operation failed
(including NOT_FOUND, TIMEOUT or an unreachable node), 2 usage or configuration error.
Files are capped at 1 MiB; a `get` replaces its output file
atomically and only on success. These commands work in Strong and Eventual-noRR;
in Eventual-RR they use read repair (`--read-quorum`/`--write-quorum` set R and W, default 2 and 2).

`consistency_mode {strong,eventual-noRR,eventual-RR}` switches every node (any VM can run it; vm01
coordinates). Strong <-> Eventual clears all values and version metadata on all ten nodes; the two
Eventual modes keep them. It succeeds only after all ten nodes confirm (`OK`), reports `NO_CHANGE`
if the cluster already runs that mode, `BUSY` if another change is running, and `TIMEOUT` before
anything changes if a node is unreachable. A change that is decided but not yet confirmed on every
node reports `PENDING` and vm01 keeps finishing it. While a node is switching it answers `put` and
`get` with a `TRANSITIONING` error.

The other commands (all print one JSON line; `ls` needs only `--manifest`):

```bash
python3 -B -m mp3 --manifest manifest.json ls key
python3 -B -m mp3 --manifest manifest.json --vm-id vm03 liststore
python3 -B -m mp3 --manifest manifest.json --vm-id vm03 getfromreplica vm05 key localfilename
python3 -B -m mp3 --manifest manifest.json --vm-id vm03 list_mem_ids
python3 -B -m mp3 --manifest manifest.json --vm-id vm03 'multiput(key, VM3, VM5, VM8)'
```

`ls` prints the key's ring ID and its five owners (ring ID and address) from the manifest alone.
`liststore` prints each key stored on that VM with its key ID, then the VM's ring ID and a count.
`getfromreplica` reads exactly one replica's own copy (a VM name, `host:port`, or a unique host) with
no quorum and no repair, and writes the file only if that copy exists. `list_mem_ids` prints every
member with its ring ID, sorted by ring ID, including suspected ones. `multiput` makes each named
VM put its own value (`--multiput-file` on the node, else 1 KiB of random bytes) to the same key at
the same moment (ARM, READY, then GO), and reports one row per VM: its state, put status, version,
acknowledging nodes, size and a SHA-256 prefix, and its own timings. If any VM is not ready, none of
the aborted VMs start a put. Exit codes: 0 success, 1 the operation failed, 2 usage or
configuration error.

## Benchmark and report framework

The full benchmark harness and LaTeX template are implemented, but no university-VM
trials have been run. KD-18 remains pending, and local smoke data is never reportable.
The smoke command starts ten local node processes and two persistent clients; it requires
explicit local targeting and retains watermarked evidence outside the repository:

```bash
MP3_TARGET=local python3 -B -m mp3.benchmarks smoke \
  --duration 2.5 --key-count 8 --output-dir /tmp/mp3-benchmark-smokes
```

Focused framework/control tests:

```bash
python3 -B -m unittest mp3.tests.test_benchmarks mp3.tests.test_control -v
```

The full remote runner has 51 trials at no less than 180 seconds per client and requires
operator confirmation of an isolated run root. **Do not run this example until the ten-VM
manifest, hostnames, Python/SQLite versions, SSH policy and ports have been verified.**
It preloads 100,000 1 KiB keys after each configuration change, verifies each key's exact
content/version at one prescribed owner and key presence on all five owners, then runs
closed-loop resident clients. R/W changes restart nodes because those settings are loaded
at process startup. Example command for the later authorized measurement task:

```bash
MP3_TARGET=remote MP3_USER=<netid> \
python3 -B -m mp3.benchmarks full \
  --manifest /path/to/course-manifest.json \
  --run-dir results/run-2026-11-06 \
  --run-id run-2026-11-06 \
  --remote-root '$HOME/mp3_benchmark_run-2026-11-06' \
  --confirm-isolated-root run-2026-11-06
```

After collecting all real trials, copy and edit the template, replace every pending
placeholder with evidence-based design and results discussion, and save truthful LLM
transcripts in `<run-dir>/llm_use/`. Then validate all trials, generate the three plots,
compile the PDF, and verify it is under three pages:

```bash
cp mp3/report/report.tex results/run-2026-11-06/report.tex
# Edit report.tex and add the actual transcripts under results/run-2026-11-06/llm_use/.
python3 -B -m mp3.benchmarks report \
  --run-dir results/run-2026-11-06 \
  --report-source results/run-2026-11-06/report.tex \
  --output results/run-2026-11-06/report.pdf
```

The report builder requires 51 verified remote trial summaries, retained raw operation
rows, actual per-client durations, all-five-owner preload evidence and LLM-use transcripts.
On success it also writes `aggregates/trial_metrics.csv` (including the throughput
measurement interval and actual worker elapsed bounds) and
`aggregates/configuration_summary.csv` (mean and sample standard deviation across the three
trials in each configuration).
This environment has no `pdflatex`, so PDF compilation remains unverified here. See
[benchmark runbook](benchmarks/README.md) and [report template](report/README.md).

## Local smoke evidence (November 6, 2026)

The short smoke command above was run with `--duration 2 --key-count 8`: ten loopback node
processes and two client processes, 8 preload keys and 40 owner copies, 406 successful
operations, 0 failures, 2.002–2.007 seconds per client, 202.27 successful operations/second,
9.81 ms mean terminal latency, and a watermarked plot. This is **NOT FOR REPORT** and is
not a university deployment or benchmark result. Focused benchmark/control tests passed
54 tests (16 benchmark and 38 control); the benchmark suite passed independently 16/16.
The full MP3 suite passed **376 tests in 499.335 seconds** after the checkpoint and CSV
additions. The final benchmark suite passed independently **16/16**. Real trials,
report discussion, and remote compatibility checks remain.

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
operation; `stop()` returns a `NodeShutdownReport`.

## Programmatic Strong ABD API

With a running Strong-mode `NodeRuntime` and an explicit compatible schema-2 release
inventory, call:

```python
import time

deadline = time.monotonic() + 5.0
put_result = runtime.strong_put("photo", b"image bytes", deadline=deadline)
get_result = runtime.strong_get("photo", deadline=time.monotonic() + 5.0)
```

The methods return `ProtocolResult`. Strong PUT and GET query the same five fixed
owners and require Q=3 in both phases. GET performs durable readback before returning
a present value; three explicit absent replies produce `NOT_FOUND`. Empty bytes are
valid. Failure results distinguish timeout/error and may report
`completion_unknown=True` when a write may have committed. The exact local setup and
adversarial examples are in `tests/test_strong.py`; the bounded history checker is
`tests/history_checker.py`.

The same operations are reachable from the command line (`put` and `get` choose the
coordinator from the node's current mode). Local loopback success does not establish
deployment readiness across the ten university VMs.

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
results or replicate to other owners; the Strong and Eventual coordinators do that
on top of them.

Receipt and response events print the required local timestamp line and can append
structured records when a logger is configured; see "Logs and MP1" below.

## Logs and MP1

No KV service starts at CLI import/startup. A node started with `--log machine.<N>.log` appends
one JSON line per replica receipt and response and one `read_repair` line per Eventual-RR GET.
The unchanged MP1 server (`python3 server.py <N> [port]`, run in the log directory) and the
unchanged MP1 client find those lines. `tests/test_mp1_logs.py` checks MP3 log content, while
`tests/test_mp1_ten_process_deployment.py` starts ten copies of the original server at manifest
`log_query` ports and checks match, count, and no-match queries using the original client.
`deploy/local_release_rehearsal.sh` also verifies the deploy/start/stop/status lifecycle locally.
MP1 and MP2 source remain unchanged.

## Programmatic Eventual-noRR API

The programmatic API works only when the node is ready, schema-2 activation is
configured, and its persisted context is Eventual-noRR. The CLI uses the same calls.

```python
import time

put = node.eventual_put("photo", b"binary-safe bytes", write_quorum=2)
if put.status.value == "OK":
    # The foreground result means two distinct owners durably stored this exact version.
    print(put.version, put.qualifying_owners)
    # A snapshot is immediate; wait is optional and bounded.
    print(put.propagation.snapshot())
    final = put.propagation.wait(timeout=10)

get = node.eventual_get(
    "photo", deadline=time.monotonic() + 5, read_quorum=2)
```

`read_quorum` and `write_quorum` are independent integers in 1..5 and default to
2. An operation captures its quorum setting at admission. A PUT reserves all five
owner attempts before sending any install. It returns when W exact-version durable
acknowledgments arrive; the propagation handle reports each owner separately and
does not imply that every owner committed. The continuation ends at the approved
bounded AttemptLease deadline. A timeout may have partial write effects and is
reported as uncertain when a request may have been dispatched. GET uses only the
first R valid owner replies and issues no install, repair or background comparison.

Run the focused local Eventual-noRR suite with:

```bash
python3 -B -m unittest mp3.tests.test_eventual -v
```

## Local deployment rehearsal

`deploy/` contains MP3 deployment, service lifecycle, status, readiness and demo scripts. Every
deployment rehearsal in this workspace must explicitly select the local target. To run the
isolated ten-process gate, execute:

```bash
MP3_TARGET=local ./mp3/deploy/local_release_rehearsal.sh
```

This script creates its own temporary tree, deploys ten loopback identities, starts MP3 plus the
original MP1 log server for each identity, waits for readiness, exercises all eight commands and
the Strong, Eventual-noRR and Eventual-RR modes, kills and recovers a node, queries logs through
the original MP1 client, then stops services and removes only its own temporary tree. It refuses
to run unless `MP3_TARGET=local` is explicitly set. Exact results are recorded in
`deploy/README.md` and `IMPLEMENTATION_PLAN.md` after each verified release check.

## What remains

The deployment scripts and local ten-process rehearsal exist; actual UIUC VM host access,
interpreter/SQLite compatibility, SSH, firewall and instructor demo instructions remain
unverified. Ten-VM correctness testing, benchmarks, the report and demo readiness are still ahead.
Obtain and compare current instructor VM/demo instructions before remote use. The private
review-repository workflow is documented in `AGENTS.md`; the professor's course repository
remains separate.

## Future final handoff (planned, not available yet)

After implementation, independent review, authorized ten-VM verification, real
benchmark measurements and report preparation, the team plans to publish a concise
handoff with the authoritative branch/full SHA and reproducible checkout; actual
implemented-versus-pending feature matrix; Python/dependency and VM setup/restart
instructions; tested commands and diagnostics; data/log paths and MP1 query usage;
real test/failure/benchmark evidence and its seeds/scripts; remaining course
submission and demo checklist; and explicit next actions for Juhi. This handoff is
not complete, and missing evidence will be labeled pending rather than inferred.

## Deployment and demo scripts

See `deploy/README.md`: start/kill/restart scripts for the ten VMs, a local practice mode, and one script per demo test.


Latest local release check (November 6, 2026): the independent Python 3.12.4 full suite
passed 360 tests in 511.917 seconds. The focused ten-process unchanged-MP1-client test
passed match, count and no-match against all ten local servers. The guarded local rehearsal
passed all eight commands, three consistency modes, crash/recovery and MP1 log querying.
These are loopback results, not university-VM readiness evidence. See
[`deploy/README.md`](deploy/README.md) for verified scope and outstanding remote checks.
