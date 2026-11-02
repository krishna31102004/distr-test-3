# MP3 build progress, in plain English

This is a short learning journal for Krishna and Juhi. It explains what each coding
batch actually did. The technical function-by-function reference is
[EXPLANATION.md](EXPLANATION.md); the shared task/status record is
[IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md).

## Batch 1 — Local storage and versions (completed and independently reviewed)

Batch 1 gave one computer a safe local place to save a key, its bytes, and its version.
It uses SQLite, which is already available in Python. SQLite commits each value and
its version together before the code reports a successful local save. A missing key
is different from a saved empty file. A restart keeps saved data. Each computer also
keeps a durable restart number so other computers can tell a new process from old
messages left over from before a crash.

The main code is in `mp3/storage.py` and shared value/version types are in
`mp3/models.py`. Batch 1's independent review and 51 passing local tests are recorded
in the implementation plan. Those tests used temporary local databases; they were
not a ten-VM test.

## Batch 2 — Consistent hashing and membership

### What exactly is Batch 2?

**Batch 2 = Consistent Hashing + Membership + Crash/Restart Detection.**

We're teaching ten computers:

1. Where does each key belong?
2. Which computers are alive, suspected, or recovering?

### Part A: Consistent hashing

Imagine the ten VMs and the key are each given a number on a circular number line.
For `put photo image.png`, MP3 hashes the word `photo`, finds that number on the
circle, then walks clockwise to select the first five VM positions. The same key and
same ten-VM manifest always produce the same ordered five owners.

For the local test manifest, the implemented SHA-256 function produced this result:

```text
key "photo" --clockwise--> VM03 --> VM09 --> VM04 --> VM08 --> VM06
                       (first five are the prescribed owners)
```

Those VM numbers are **calculated from the test manifest in this repository**. The
loopback addresses and ports used by that fixture are examples; this is not a result
from the university VMs. A failed VM stays in the five-owner list. We do not replace
it with the next live VM.

### Part B: Membership and failure detection

Suppose VM6 crashes. After about 2.5 seconds without seeing its heartbeat advance,
another VM marks it **SUSPECTED**. That means “we have not heard from it recently,”
not “erase it.” VM6 keeps its ring number and remains one of the key's designated
owners.

When VM6 restarts, it opens its old SQLite database, including the values and
versions already saved. Before accepting membership traffic, it durably advances its
restart generation. The new generation tells peers that this is VM6's new process.
Gossip from VM6's older process is ignored, even if that delayed message arrives
after recovery. A newer generation can change the status back to ALIVE.

VM1 is not a required introducer. Every process starts from the same fixed manifest
and contacts a rotating set of peers. A local TCP test stopped VM1 from answering and
showed VM2 and VM3 learn VM6's newer generation directly.

### What Batch 2 actually implemented

**Implementation status: complete and independently reviewed.** The GPT-6 Luna/high
reviewer found no remaining substantive issue after the documented fixes.

- `mp3/ring.py` validates the ten VM identities and endpoint manifest, computes the
  stable SHA-256 ring positions and fingerprint, and returns exactly five clockwise
  owners. It handles wraparound and a key exactly on a VM position.
- `mp3/membership.py` keeps all ten members, shares full snapshots over a bounded TCP
  gossip service, marks silent peers SUSPECTED using a monotonic clock, retains them,
  and accepts newer restart generations while rejecting stale gossip.
- `MembershipRuntime.start()` recovers the local store, calls the durable restart
  generation function once for that store in the process, and only then admits
  membership service or gossip work.
- `MembershipTable.owners("photo")` still returns the same five VMs when some are
  suspected. `MembershipTable.list_mem_ids()` provides all member IDs and ring IDs
  for the future command; the command itself is not connected yet.

The complete local suite has **70 tests and passed** with:

```bash
python3 -B -m unittest discover -s mp3/tests -q
```

The tests ran on this computer using temporary files, fake clocks, loopback TCP
listeners, and child processes killed with SIGKILL. They cover hash results,
manifest ordering, owner wraparound, failure detection, stale gossip, direct-peer
recovery when VM1 is unavailable, and repeated local crashes with stored data retained.
No university VM or ten-machine cluster was tested. The independent reviewer also
ran the full verbose suite and confirmed all 70 tests passed. Batch 2 is complete as
a local foundation; cluster validation, KV RPC and later protocols remain future work.

### Batch 2 focused correctness fixes — October 30, 2026

A final review caught two issues, now fixed:

- `list_mem_ids()` sorts rows by the numeric ring ID (`nodeID`), from smallest to
  largest. VM labels and ring positions are different things, so alphabetical VM
  ordering was not the required order.
- `merge_gossip()` checks that incoming gossip does not claim a newer generation for
  this running machine before changing any membership row. If that claim is invalid,
  the whole gossip message is rejected and the membership snapshot stays unchanged.

The ordering test now checks ring IDs directly. The new atomicity test uses `vm10` as
the local machine and puts an otherwise acceptable newer `vm01` generation earlier
in merge order; it confirms the complete membership snapshot is unchanged after the
invalid self-generation claim is rejected.

After these fixes, `python3 -B -m unittest discover -s mp3/tests -q` passed all **71
tests** locally. The independent GPT-6 Luna/high reviewer also ran the full verbose
suite on the settled files: **71 passed**. An initial reviewer run overlapped a test
edit and saw a transient assertion failure; the focused test and full suite passed
when rerun after the edit finished. These are local tests only; no real university
VMs were tested. The journal update is recorded after code review and both final test
runs.

### What Batch 2 does not implement yet

Full distributed PUT/GET, Strong ABD, Eventual consistency, read repair, mode
switching, and the eight complete demo commands are still future work. The TCP
service in this batch carries membership gossip only.

## Batch 3 — TCP Networking and Remote Replica Requests

### What exactly is Batch 3?

Batch 3 teaches one computer to send a small request to one other computer and get
back that replica's local answer. This kind of request-and-response message is called
**RPC**, short for remote procedure call. It lets VM1 ask VM4 to read or conditionally
save a key using VM4's own durable SQLite database.

This is a building block. It does not yet decide how many of the five replicas must
answer or which version a distributed GET/PUT should return.

### How does a remote read work?

Suppose the future coordinator wants `GET photo` and VM4 is one of the five prescribed
owners. It sends `READ_LOCAL("photo")` to VM4. VM4 checks the request, reads its local
database, and replies with `FOUND` and the stored version/value, `NOT_FOUND`, or an
explicit error. A saved empty byte string is `FOUND` with zero bytes; it is not the
same as a missing key.

```text
VM1 (caller)                            VM4 (replica)
    |                                        |
    |--- READ_LOCAL("photo", request ID) -->|
    |                                        |-- read SQLite
    |<-- FOUND(version, raw bytes) ----------|
    |                                        |
```

The same mechanism sends `INSTALL_IF_NEWER`. For example, if VM4 has v7 and receives
an install request for v6, it keeps v7 and replies `SUPERSEDED` with
`stored_version=v7`. A durable successful reply is only made after storage commits.

### Why the network rules matter

Values are arbitrary bytes, so JSON is used only for metadata. The value travels as
unchanged bytes after the metadata. Length prefixes tell the receiver exactly how
many bytes to read. Explicit size caps prevent a peer from declaring an enormous
message. A fresh request ID lets the caller match the response to its own request.

Each RPC call uses one absolute timer from the time `call_replica()` starts. The
client resolves the fixed manifest endpoints during setup, before an operation starts;
then it tries up to eight numeric address candidates and spends only the remaining
call budget on each connection attempt and each send/receive. A peer sending a few
bytes very slowly cannot restart that timer after each byte. If a VM is down, the
caller gets an unreachable/error/timeout outcome, never a fake `NOT_FOUND`. If the
connection is lost after an install may have committed, the client marks the
completion uncertain: the write might be on disk even though its acknowledgment did
not arrive.

The replica service has a fixed worker count and a bounded waiting queue. If that
capacity is full, a well-formed caller receives `OVERLOADED`; the service does not
create unlimited threads. KV service capacity is separate from membership gossip.

### What Batch 3 actually implemented

**Implementation status: complete and independently reviewed for local behavior.**
The GPT-6 Luna/high reviewer inspected the endpoint-resolution fix and independently
ran the full 106-test suite.

- `mp3/rpc.py` defines the request/response schema, strict JSON validation, binary
  framing limits, send/receive functions, typed RPC outcomes, and
  `ReplicaClient.call_replica()` for exactly one target and one connection.
- `mp3/replica.py` defines `ReplicaService`, which accepts bounded work, checks
  identity/manifest/generation, and calls the local `read_local()` or
  `install_if_newer()` storage function.
- `mp3/framing.py` keeps its MP1/MP2-compatible frame helpers and adds total-deadline
  reads/writes for fragmented network messages.
- `mp3/storage.py` lets the replica service pass its same absolute deadline through
  storage admission and SQLite lock waits. A regression test holds a SQLite writer
  lock, verifies the shorter RPC deadline stops the request, then releases the lock
  and checks that the expired install did not appear.
- `mp3/eventlog.py` prints required local `receivedget`/`respondedget` and
  `receivedput`/`respondedput` lines when the service processes a valid local request.
  It avoids printing the value bytes.

Tests created two logical VM identities on loopback. A real TCP request from logical
VM1 reached logical VM4's Store and returned its actual read/install result. The
process test also killed a disposable local replica with SIGKILL, reopened its same
database, and successfully read the committed value after its generation advanced.
These are local-process/loopback checks, not tests on ten university VMs.

The main agent ran:

```text
python3 -B -m unittest discover -s mp3/tests -v — 106 passed (6.580s)
python3 -B -m mp3 --smoke — exit 0
python3 -B -m mp3 --help — exit 0
python3 -B -m py_compile mp3/rpc.py mp3/replica.py mp3/framing.py mp3/storage.py mp3/eventlog.py — exit 0
```

The independent reviewer reran the full suite: **106 passed (6.605s)**. The focused
RPC/replica/storage suite passed **51 tests (5.603s)** on the main agent. The first
review found a real deadline bug: SQLite could wait beyond the RPC deadline and commit
later. That path now shares the request's deadline. The review also found that
infinite or enormous client deadlines could raise an uncaught overflow; those now
return an explicit error outcome and have regression tests. A later review noticed
that hostname DNS lookup in Python's standard `create_connection` could occur outside
the call deadline. `ReplicaClient` now resolves the fixed KV endpoints during setup
and tries cached numeric addresses under one remaining call deadline; a loopback test
verifies that an operation does not invoke DNS or `create_connection`.

### What Batch 3 does not implement yet

It does not collect R/W replies, count quorum acknowledgments, send a distributed
PUT/GET to all five owners, implement Strong ABD, Eventual-noRR, Eventual-RR, read
repair, full mode switching, or make the eight CLI commands operational. The API is
only the one-replica foundation. A blocked or timed out request is not a distributed
key result.

### What came next after Batch 3?

At the time of the Batch 3 handoff, Batch 4 was planned to connect the tested
components into one node runtime. That integration is now described in the Batch 4
section below. It still does not coordinate requests across all prescribed owners.

### Batch 3 final shutdown and timeout corrections — October 30, 2026

A final review found two edge cases. First, a queue with room for only one waiting
request could not hold shutdown markers for four idle workers. The workers now check
a shared stop signal while waiting on the bounded queue. During a normal stop, they
finish every accepted request, including queued work. If the shutdown time limit is
reached, queued requests are closed and counted as incomplete, active sockets are
interrupted, and the report says whether every worker actually joined. A request
handler exception no longer kills that worker or skips queue bookkeeping.

Second, the client could receive a valid write response but notice that the deadline
had expired while validating it. The client now returns TIMEOUT, so that late response
does not count as an on-time acknowledgment, and marks the write's completion as
uncertain. It also keeps uncertainty for INSTALL storage errors. If the deadline ends
before the first send can start, the client reports that no request was transmitted.
The network helper marks this boundary immediately before `sendall`, after checking
the deadline. No retries were added.

**Final verification after review:** the main agent ran
`python3 -B -m unittest discover -s mp3/tests -v` — **114 passed in 12.527s** — and
both `python3 -B -m mp3 --smoke` and `python3 -B -m mp3 --help` exited successfully.
The independent GPT-6 Luna/high reviewer reran the settled suite — **114 passed in
11.812s** —
and also passed smoke, help, `python3 -B -m compileall -q mp3`, and `git diff --check`.
The eight new tests cover worker shutdown with a one-slot queue, graceful and forced
shutdown, worker exceptions and repeated stops, plus deterministic before-send and
after-response deadline cases and ambiguous storage errors. A lost READ_LOCAL reply
does not set write-completion uncertainty; the test verifies that boundary too. These
checks used local
processes, loopback sockets and temporary storage; no university VM was tested.

Batch 3 is complete with these corrections. Distributed PUT/GET, Strong ABD,
Eventual-noRR, Eventual-RR, read repair and complete CLI operations are still
unimplemented.

## Batch 4 — Putting Everything Together

**Batch 4 = Storage + Hashing + Membership + Networking working together.**

So far, we had useful pieces: a database, a rule for choosing five owners, a way to
track which processes are alive, and a way to send one request to one replica. Batch 4
gives one VM a runtime that starts and stops those pieces in the right order.

### Example: starting VM4

VM4 reads the same ten-machine manifest as the other nodes. It opens its own SQLite
database. If this is its first start, we must say `--bootstrap`; if its database
already exists, we say `--recover`. Recovery checks that this database belongs to
VM4 and keeps its saved values and versions.

Before VM4 opens a network listener, its membership runtime saves the next restart
generation in SQLite. On the first start this might be generation 1; after a crash,
the next process might use generation 2. Only after that number is durable does VM4
start its membership listener and gossip, then its KV replica listener. `READY` means
these local pieces started. It does not mean all ten VMs are reachable.

```text
+------------------------- NodeRuntime: VM4 --------------------------+
| Fixed manifest/ring  -> the five prescribed owners for each key      |
| SQLite Store        -> saved values, versions, mode, restart number  |
| MembershipRuntime   -> alive/suspected peers and restart generations|
| ReplicaService      -> accepts one local READ_LOCAL or INSTALL call  |
| ReplicaClient       -> sends one replica request to another VM      |
+---------------------------------------------------------------------+
```

### One remote request, not a distributed PUT or GET

For example, VM1 can send `READ_LOCAL("photo")` to VM4. VM4 checks the request,
reads its own SQLite copy, and sends back the result. The answer can be FOUND, NOT_FOUND,
or an error. If the saved value is `b""`, VM4 returns FOUND with an empty value; this
is different from NOT_FOUND. VM1 can also send `INSTALL_IF_NEWER` with a version and
bytes. VM4 only replies with a successful durable result after SQLite commits.

VM4 is the example node above. In the actual three-node loopback test, the roles are
vm01 (requester), vm02 (replica), and vm03 (peer); the separate crash/recovery test
uses VM06.

```text
 vm01 (requester)            vm02 (replica)             vm03 (peer)
      |                           |                          |
      |--- membership gossip ---->|<----- gossip ----------->|
      |                           |                          |
      |--- READ_LOCAL("photo") -->|                          |
      |                           |-- read vm02's SQLite    |
      |<-- FOUND / NOT_FOUND -----|                          |
      |                           |                          |
```

The picture shows one request to one replica. Step 3 Batch 2 now adds a programmatic
Strong GET that contacts the five prescribed owners, selects a Q=3 maximum and writes
that record back durably to Q=3 before returning. Eventual coordination and the
file-oriented demo GET command remain future work.

### What happens when VM4 crashes?

If VM4 stops suddenly, it cannot run cleanup. SQLite has already committed any write
that received a durable success reply. The other machines can suspect VM4 after its
heartbeats stop, but they keep VM4 in the key's five-owner list. They do not swap in a
different machine. When VM4 starts again with `--recover`, it opens the same database,
keeps the same VM and ring identity, and durably advances its restart generation.
Gossip tells peers about the newer generation; delayed gossip from VM4's older process
does not make that old process current again.

On a normal stop, the runtime stops accepting new local RPC calls, drains accepted
requests within its bounded shutdown period, stops listeners and gossip, and closes
SQLite after active work has ended. If a short forced stop cannot finish, it reports
incomplete requests and leaves the database open; after that work finishes, a caller
can retry stop and release the lock. Persistent values are not deleted.

### What Batch 4 actually implemented

`mp3/node.py` adds `NodeRuntime`: it validates startup inputs, opens or recovers the
SQLite Store, uses `MembershipRuntime` to advance the durable generation exactly
once, then starts membership and KV listeners and creates a `ReplicaClient`. Its
`owners(key)`, `membership`, `generation`, and `call_replica(...)` methods expose
local state and one remote replica call. `python3 -B -m mp3.node --help` describes the
separate diagnostic launcher. It does not replace `python3 -m mp3` or turn on the
unfinished demo commands.

`mp3/membership.py` now reports whether its service threads stopped, can retry a
non-clean join, and safely cleans up when thread startup fails. `mp3/replica.py` can
refresh a forced-shutdown result on a later stop attempt and reports whether the
accept thread and workers joined. These small lifecycle changes let the runtime keep
SQLite open until its workers are done.

The local integration tests use temporary databases and loopback ports. They start
three logical nodes and exercise real gossip and RPC sockets, verify remote empty,
missing and 1 MiB binary values, and run concurrent replica requests while gossip
continues. A disposable VM6 subprocess receives SIGKILL, then restarts from the same
database with the next generation; tests check its saved value, stable owner list, peer
convergence, and rejection of stale generation evidence. Other tests inject startup
thread failures and retry an incomplete forced shutdown. These are local process and
loopback tests; **no university VM was contacted**.

Initial Batch 4 local results before the final concurrency correction:

- `python3 -B -m unittest discover -s mp3/tests -v` — **127 tests passed in 15.013s**.
- Independent GPT-6 Luna/high reviewer — **127 tests passed in 14.830s**.
- Focused `test_gossip_thread_start_failure_rolls_back_membership_listener_and_store`,
  `test_kv_accept_thread_start_failure_rolls_back_workers_listener_and_store`, and
  `test_forced_node_shutdown_reports_incomplete_active_and_queued_requests` — **3
  tests passed in 0.758s**.
- `python3 -B -m mp3 --smoke`, `python3 -B -m mp3 --help`,
  `python3 -B -m compileall -q mp3`, and `git diff --check` — all passed.

### What Batch 4 did not implement then (historical Step 2 status)

At the Step 2/Batch 4 gate, it did not implement Strong ABD, Eventual-noRR,
Eventual-RR, quorum coordination, read repair, full distributed PUT/GET, mode
switching, benchmark experiments, or the completed demo commands. It also did not
show that ten real VMs can communicate.

### What was next at the Step 3 Batch 1 gate (historical)

At that gate, the shared coordinator foundation was in place and Step 3 Batch 2 was
the next coding batch. Batch 2 is now complete and locally verified as explained
below. The one-replica call and Batch 1 collector by themselves are not distributed
consistency operations.

### Final call-admission and shutdown correction

We found a race between starting an RPC and stopping its node. A call could pass the
admission check, then shutdown could mark the node not ready before that call finished
building its request. The call would incorrectly fail even though it had already been
accepted.

Now the admission check, readiness check, and capture of the current mode/epoch,
restart generation, and RPC client happen together under one short condition lock.
Shutdown closes readiness and new-call admission under that same lock. A call that
already entered keeps its captured information and continues building/sending its
request after the lock is released. No network operation happens under this lock.
Shutdown waits for admitted calls within its existing deadline; if time runs out, it
reports the call as outstanding and leaves SQLite open. Once the call finishes, another
stop attempt can close SQLite safely.

Three event-controlled regression tests verify that an admitted install keeps its
version/context/generation while request construction is paused across shutdown, a new
call is rejected after shutdown closes admission, shutdown reaches its call-drain wait,
and a forced stop reports an outstanding operation then cleans up after retry. They
also check that active-call accounting returns to zero and the database can be reopened.

Final results after independent review:

- `python3 -B -m unittest discover -s mp3/tests -v` — **130 tests passed in 14.674s**.
- Independent GPT-6 Luna/high reviewer — **130 tests passed in 14.818s**.
- Focused `test_admitted_call_keeps_context_and_generation_through_shutdown`,
  `test_shutdown_closes_call_admission_before_later_rpc_attempt`, and
  `test_forced_node_stop_reports_admitted_rpc_and_closes_store_only_after_retry` —
  **3 tests passed in 1.047s**.
- Smoke, `mp3` help, `mp3.node` help, compileall, and `git diff --check` passed.

The Step 2 local foundation remains verified. These tests used temporary storage and
local threads/sockets; no university VM was contacted. Step 3 and all consistency
protocols remain untouched.

## Step 3 — Designing Strong and Eventual Consistency (approved design; status updated below)

Five copies help a key survive a machine failure, but they can disagree after messages
are delayed or a machine restarts. A consistency protocol tells the machines which
version wins and what evidence is enough to answer a client. Krishna approved the
Strong ABD and Eventual-noRR design on October 31. Strong ABD was implemented and
locally reviewed in Batch 2; Eventual-noRR is still not implemented. The proposed
diagrams in this design section explain the algorithm and are not themselves test
evidence.

A **quorum** is a required number of different replica machines. Strong uses three of
the five fixed owners in each phase. Eventual lets us configure R and W from 1 to 5;
the default is R=2 and W=2. Strong's GET writes its selected newest record back to a
majority before returning, so later reads cannot overlook a completed write. Eventual
GET returns the largest version among its first R different valid replies and may be
stale. Missing data is distinct from an empty value and from a machine that did not
answer.

### Proposed Strong flow

```text
PUT: client -> query five owners -> learn max version
     -> allocate a durable higher tag -> install -> 3 durable owner proofs -> reply
GET: client -> query five owners -> select max -> write it back -> 3 proofs -> reply
```

Strong uses a logical counter plus writer identity, not a wall-clock timestamp alone.
The conceptual empty/bottom value is local to the coordinator and is never stored on a
replica. This is the approved design; no Strong PUT/GET coordinator exists yet.

### Proposed Eventual-noRR flow

```text
PUT -> send one attempt to each of five owners
     -> client replies after W exact-version durable acknowledgments
     -> remaining reserved attempts continue under their bounded lease
GET -> query owners -> first R distinct replies -> return their maximum version
```

For example, with R=W=2, a PUT needs two distinct owners to confirm the exact version
is durable. It still preserves the other three owner attempts after the client reply.
Those attempts are bounded: 5 seconds for the foreground response, up to 10 seconds
of continuation after response, and no later than 15 seconds after admission. Each
owner receives at most one attempt. The existing durable timestamp allocator makes
versions unique across local concurrency and restart. Read repair is not part of
Eventual-noRR.

## Step 3, Batch 1 — Shared Coordinator Infrastructure (implemented and reviewed)

Batch 1 built reusable parts needed by both protocols, without implementing either
protocol. One operation has one operation ID; each replica call has its own request
ID. A collector validates replies against the exact owner, request, operation, phase,
context, and dispatched restart generation. It counts a VM at most once. For example,
three different valid owners can satisfy a Strong query; three replies from one VM
cannot. A timeout is not NOT_FOUND.

```text
             +-------------------------------------+
Client ----> | coordinator evidence + bounded work | ----> fixed owners 1..5
             +-------------------------------------+        (no owner substitution)
                     | one result per owner
                     +-- validate, count distinct, record failures
```

The scheduler uses bounded coordinator and outbound capacity and can reserve all five
owner attempts atomically. Its `AttemptLease` separates client response time from
accepted Eventual propagation. The collector leaves unfinished Eventual PUT attempts
available to the future protocol coordinator, including if W becomes impossible; it
does not invent a success result. Shutdown can report incomplete work. This is the
shared mechanism only, not a full Eventual PUT.

The RPC layer now supports strict schema 2 for protocol phases while preserving the
old schema 1 local RPC. Schema 2 adds the shared operation ID; it never downgrades to
schema 1. Protocol RPC stays disabled unless an explicit inventory lists compatible
releases for all ten VM identities. That inventory is a local configuration assertion,
not proof that ten deployed VMs were checked. An operation still contacts only its
five prescribed owners, so three compatible owners can satisfy Strong even when the
other seven are offline.

Batch 1 also added a durable Strong logical-counter allocator. It allocates above both
a Strong query maximum and the local saved high-water counter. The SQLite metadata
migration preserves existing values, versions, identity, context, restart generation,
and Eventual timestamp state. This allocator is not Strong ABD: it only prepares a
tag for a future coordinator.

### What was implemented and where

- `mp3/attempts.py`: shared foreground/continuation lease and cancellation.
- `mp3/coordinator.py`: result/evidence records, `Schema2Activation`, bounded worker
  resources, atomic reservations, one-owner dispatch, and distinct-response collection.
- `mp3/rpc.py` and `mp3/replica.py`: schema-2 phases and operation-ID correlation;
  remote replicas still perform only local reads and conditional installs.
- `mp3/storage.py`: durable Strong counter allocation and preserving metadata migration.
- `mp3/node.py`: owns the shared resources and exposes one-replica dispatch.
- Tests in `mp3/tests/test_coordinator.py`, `test_rpc.py`, `test_replica.py`,
  `test_storage.py`, and `test_node_runtime.py` exercise collector evidence, capacity,
  leases, migration, and real loopback RPC.

### Actual verification

- Main: `python3 -B -m unittest discover -s mp3/tests -v` — **153 passed in 17.118s**.
- Independent reviewer: same suite — **153 passed in 16.871s**.
- Main focused coordinator tests — **13 passed in 0.199s**; storage and RPC focused
  tests — **36 passed in 0.769s**. Reviewer independently ran the coordinator tests:
  **13 passed in 0.195s**.
- `python3 -B -m mp3 --smoke`, `python3 -B -m mp3 --help`,
  `python3 -B -m mp3.node --help`, `python3 -B -m compileall -q mp3`, and
  `git diff --check` all exited successfully.
- Local loopback tests send schema-2 requests between logical nodes and their real
  SQLite stores. No university VMs were contacted; no real ten-VM rollout was tested.

The reviewer found that the collector initially canceled Eventual attempts both after
W and when W became impossible. The fix keeps unfinished Eventual calls in
`QuorumCollection.continuing`, with regression tests for both paths. The reviewer
made no source edits. The main agent reran the full suite after the fix.

### Final Batch 1 corrections — reviewed, local verification only

Four small gaps were fixed without building Strong or Eventual PUT/GET. First, when
a coordinator was accepted before shutdown, the node now gives that callback a
registered one-operation pass. The callback may use it to send its next phase under
the same deadline and identity, even after new work has been blocked. Second, both
the sender and receiving replica check that a schema-2 request goes to one of the
key's five assigned owners. A suspected or unreachable owner remains on that list;
we do not substitute another VM. Third, the collector stops waiting once failures
make its required number of replies impossible. If the operation is an Eventual PUT,
its already accepted attempts are still kept for later completion. Finally, the
attempt record can now clearly say `DURABLE_ACK`, separate from “a higher version is
already stored” (`SUPERSEDED`) or an uncertain result.

The main agent ran the full suite under Python 3.12.4: **163 passed in 19.624s**.
The independent reviewer separately ran the full suite: **163 passed in 19.287s**,
and the focused coordinator/runtime/replica tests: **72 passed in 17.818s**. The
reviewer found no supported defect and made no code changes. These were local tests
using temporary databases and loopback sockets, not university VM tests. Step 3
Batch 1 shared infrastructure is VERIFIED; Strong ABD and Eventual-noRR coordinators
are still NOT_IMPLEMENTED.

### What Batch 1 does not implement

Strong ABD phases, Eventual-noRR GET/PUT coordinators, Eventual-RR/read repair,
quorum operations, full client-facing PUT/GET, consistency mode switching, completed
demo commands, benchmarks, and ten-VM deployment remain future work. The existing
commands continue to report `NOT_IMPLEMENTED` where their full behavior is absent.

### What comes next

Step 3 Batch 2 is now implemented and locally reviewed: it adds the Strong ABD
coordinator using the approved two-phase query/write and query/readback design,
majority evidence, crash/concurrency tests, and bounded history checking. Batch 3 is
Eventual-noRR and remains future work.

## Step 3 — Batch 2: Strong Consistency Using ABD

**Implemented and independently reviewed locally; not ten-VM verified.**

Linearizability means that even when computers act at the same time, completed
operations can be arranged as if one computer handled them one at a time, while
preserving the order of operations that did not overlap.

Simply writing to three of five copies is not enough. A concurrent reader might see a
partly written value and leave without making that value visible to a later reader.
ABD uses two rounds. In each round, the coordinator contacts the five fixed owners
and needs three distinct valid replies. Three out of five is a majority (Q=3); any
two majorities overlap in at least one owner.

### Strong PUT

The coordinator first asks the five owners what they have. It waits for Q=3 valid
FOUND or explicit NOT_FOUND responses and selects the greatest version. If all three
are absent, the current value is conceptually “bottom”; bottom is never stored.
The coordinator then durably reserves a new tag above the observed and local tag
high-water marks, and asks the same five owners to install the tag and bytes. It tells
the caller OK after three durable qualifying replies. An owner that durably stores a
strictly greater version may also confirm that the attempted older tag cannot win.

```text
Client          Coordinator                 Five fixed owners
  | PUT(photo)      |                              |
  |---------------->| READ_LOCAL strong_query ---->|
  |                 |<---- Q=3 version/absence ----|
  |                 | allocate durable new tag     |
  |                 | INSTALL strong_write ------->|
  |                 |<---- Q=3 durable evidence ---|
  |<---- OK --------|                              |
```

For example, the first round may return versions 7, 7, and NOT_FOUND. The coordinator
chooses version 7, allocates version 8, and asks owners to install version 8. If three
distinct owners durably confirm version 8 or a greater version, PUT succeeds.
If only two qualify by the deadline, PUT returns an error or timeout; if some writes
may have committed, `completion_unknown` explains that side effect is uncertain.

### Strong GET and readback

GET asks the five owners and chooses the greatest version among three valid replies.
If all three explicitly say absent, it returns NOT_FOUND. If a record exists, GET
writes that exact version and value back to three owners before returning it. A
present empty byte string is still a value; only explicit absence is NOT_FOUND.

```text
Client          Coordinator                 Five fixed owners
  | GET(photo)       |                              |
  |---------------->| READ_LOCAL strong_query ---->|
  |                 |<---- Q=3 records/absence ----|
  |                 | INSTALL strong_readback --->|
  |                 |<---- Q=3 durable evidence ---|
  |<---- value -----|                              |
```

Two writers can both query the same old maximum. They still get different ordered
tags: for example `(8, vm01/1)` and `(8, vm02/1)`. The counter is durably allocated
locally, while writer identity breaks equal-counter ties. The short SQLite allocation
transaction does not stay open during network calls. In a controlled test, the
lower-tag writer pauses before phase two while the other writer completes a higher
tag. The first writer then gets durable-at-least evidence, and a later Strong GET
returns the higher writer's bytes.

If two owners crash, three remain and may still make a majority. If three crash, only
two remain and neither query nor install quorum can succeed. Owners are not replaced
with other machines. If one write reaches only one replica and its coordinator dies,
the write was not reported successful. A later GET that sees that record among its
three replies writes it to a majority before returning it.

SQLite keeps the value and version together durably. A process that is killed after a
commit but before its reply may have changed disk even though the caller has no
confirmed acknowledgment. On restart it opens the same database and advances its
restart generation; the ring owner identity is unchanged. Shutdown after an operation
has been admitted lets that operation use its authorized second phase within the
original deadline. A timeout can leave an in-flight write uncertain; shutdown does
not turn that uncertainty into success.

### What was implemented and tested

- `mp3/strong.py`: `StrongCoordinator.strong_put`, `strong_get`, the shared two-phase
  runner, fixed-owner dispatch, maximum-record selection, and failure/uncertainty
  results.
- `mp3/node.py`: `NodeRuntime.strong_put` and `.strong_get` expose the programmatic
  API through existing bounded admission and RPC resources.
- `mp3/tests/test_strong.py`: real loopback TCP to the actual SQLite stores of the
  key's ring owners, two- and three-owner failures, empty/Unicode/NUL/1 MiB data,
  conflicts, concurrent writers, shutdown between phases, late replies, and a
  subprocess SIGKILL/restart with generation checks.
- `mp3/tests/history_checker.py` and `test_history_checker.py`: a small checker that
  tests synthetic valid and invalid histories and histories recorded from actual
  Strong calls. It respects non-overlap order and lets a timed-out write possibly
  have taken effect. It checks finite histories; it is not a mathematical proof of
  all possible schedules.

The main full run passed **176 tests in 45.819 seconds**. The independent reviewer
ran the focused Strong/history suite (**13 passed**) and full suite (**176 passed in
45.514 seconds**) and found no concrete correctness blocker. A deterministic delayed
query/install test was added after review found that evidence gap, then the reviewer
reran the current tests successfully. These are local loopback, subprocess and
in-process results; no university VM was tested. Main's `python3 -B -m mp3 --smoke`,
`python3 -B -m mp3 --help`, `python3 -B -m mp3.node --help`,
`python3 -B -m compileall -q mp3`, and `git diff --check` each exited 0. Smoke
reported its narrow foundation check and `KV NOT_IMPLEMENTED`; it does not verify
Strong. The final main focused Strong/history rerun passed **13 tests in 26.834
seconds**.

### What Batch 2 does not implement

Eventual-noRR/RR, read repair, global mode switching, the required file-oriented demo
commands, all-owner Eventual propagation, benchmarks, report plots, and ten-VM
deployment remain unfinished. A local single-replica RPC is different from Strong:
the Strong API coordinates two quorum phases across the five owners, while one RPC
contacts just one replica.

### What comes next?

Step 3 Batch 3 is Juhi's Eventual-noRR coordinator. It will reuse the same fixed-owner,
RPC, storage and bounded-worker foundations, but use configurable R/W and the
approved timestamp-based Eventual rules. Strong is locally verified; the next batch
must not be described as implemented until it has its own tests and review.
