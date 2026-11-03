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
  subprocess SIGKILL/restart with generation checks. An additional deterministic
  subprocess test now SIGKILLs a nonowner writer after exactly one owner durably
  commits; the other phase-two attempts are held before sending. The test verifies
  that only that one owner has the record, then checks that a later Strong GET finds
  and writes it back to a quorum.
- `mp3/tests/history_checker.py` and `test_history_checker.py`: a small checker that
  tests synthetic valid and invalid histories and histories recorded from actual
  Strong calls. It respects non-overlap order and lets a timed-out write possibly
  have taken effect. It checks finite histories; it is not a mathematical proof of
  all possible schedules.

The original reviewed full run passed **176 tests in 45.819 seconds**. After adding
the one-owner writer-crash regression and making the query ordering event-controlled,
the main full run passed **177 tests in 49.206 seconds**; the focused Strong/history
suite passed **14 tests in 30.372 seconds**, and the focused crash regression passed
**1 test in 2.939 seconds**. The independent reviewer reran the focused regression
(**1 passed in 2.940 seconds**) and the full suite (**177 passed in 51.471 seconds**).
No defect was found. A deterministic delayed
query/install test was also added after earlier review found that evidence gap. These are local loopback, subprocess and
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

At that point, Step 3 Batch 3 was planned as Juhi's Eventual-noRR coordinator. It is
now implemented and locally reviewed, as described next.

## Step 3 — Batch 3: Eventual Consistency Without Read Repair

**Batch 3 is implemented and independently verified on local loopback and
subprocesses. No university VM or ten-VM test was run.**

### What does Eventual mean?

Five machines own each key, but an Eventual operation can wait for a smaller number
of replies. With the default R=2 and W=2, a PUT can return after two prescribed
owners durably save the same version. The other copies are still sent in the
background. Replicas can temporarily disagree, and a GET may return an older value.
Strong ABD instead coordinates a majority in two phases so operations behave like a
single ordered register. Eventual-noRR makes a different, less strict tradeoff; it
does not use ABD and it does not repair on reads.

`N=5` never changes: the ring designates exactly five owners. R and W only say how
many distinct, valid replies the caller needs. A failed or suspected owner keeps its
ring position; a sixth machine is never substituted.

### Eventual PUT

```text
Client / coordinator
        | reserve all five bounded attempts
        | allocate one durable (timestamp, process-ID) version
        +---- INSTALL_IF_NEWER ----------------------> owner 1
        +---- INSTALL_IF_NEWER ----------------------> owner 2
        +---- INSTALL_IF_NEWER ----------------------> owner 3
        +---- INSTALL_IF_NEWER ----------------------> owner 4
        +---- INSTALL_IF_NEWER ----------------------> owner 5
        |<--- exact-version durable acknowledgments --+
        | return when W distinct acknowledgments arrive
        |       accepted remaining attempts continue under their lease
```

For R=2/W=2, suppose owners 2 and 4 confirm durable `INSTALLED` for version
`(1710000000000, vm03/7)`. The client receives success with that version and those
two owner IDs. If owners 1, 3 and 5 are still working, they remain accepted attempts;
the response does not claim they have committed. Their final outcomes update the
propagation report, not the already-returned client result.

Only exact-version durable `INSTALLED` or `UNCHANGED` replies count toward W.
`SUPERSEDED` says a newer version exists, so it does not prove this PUT's bytes were
stored and is not counted. A lost reply can leave a value committed on disk while the
client has no confirmed acknowledgment; a timed-out write is never described as
rolled back.

All five attempt slots are reserved before the first write is sent. The foreground
deadline is five seconds from admission. `AttemptLease` separately permits up to ten
seconds after foreground completion, capped at fifteen seconds total from admission.
For example, W at 1 second gives remaining work until at most 11 seconds; W at 4.9
seconds permits continuation until at most 14.9 seconds. If W is not reached by five
seconds, the client gets an error/timeout, but already accepted attempts can continue
to the 15-second cap. Each owner has one attempt; there are no retries. A queued
attempt uses the remaining lease, not a fresh timeout. Graceful shutdown drains work
within its bound; forced shutdown reports incomplete work. SIGKILL loses in-memory
fanout, while SQLite commits already made remain durable.

### Eventual GET

```text
Coordinator                     Five fixed owners
    |---- READ_LOCAL --------------------->|
    |<--- first R valid distinct replies --|
    | choose the maximum version among those R
    | return that record or explicit NOT_FOUND
    | no INSTALL, repair, or later comparison
```

With R=2, if the first two valid replies contain versions v8 and v9, GET returns v9.
Suppose a slower third owner has v10: this noRR read does not wait for it and does
not repair the first two replicas. A later read may also see a stale value if its
first R owners are behind. `b""` is a present value; only R actual `NOT_FOUND`
responses can produce NOT_FOUND. Timeouts and unreachable owners are not absence.

Concurrent coordinators use the existing durable Eventual timestamp/process-ID
allocator. It gives versions a total order and preserves process-generation identity
across restart. Clock skew can affect which concurrent value sorts higher; Eventual
is not Strong consistency.

For example, if a coordinator returns after two owners save v12 and is then killed
with SIGKILL before its other attempts finish, those two committed copies survive.
The remaining owners may miss v12. When a missed owner restarts, it keeps its identity
and persisted data, but Eventual-noRR does not automatically repair its missed key.
GET also does not repair it; Eventual-RR is later work.

### What Batch 3 implemented and verified

- `mp3/eventual.py`: `EventualNoRRCoordinator.eventual_put()` and
  `.eventual_get()`, `PropagationHandle.snapshot()/wait()`, per-owner terminal
  reports, exact-version W qualification, first-R GET selection, foreground/lease
  transition, and explicit uncertainty.
- `mp3/node.py`: `NodeRuntime.eventual_put()` and `.eventual_get()` pass through the
  existing bounded runtime admission, captured context/generation and shared RPC.
- `mp3/coordinator.py`: bounded outbound reservation now separates the time to
  acquire capacity from the lease deadline for already-reserved queued attempts;
  `collect_distinct()` can record a local preflight failure without preventing other
  prescribed owners from receiving their one-shot attempt.
- `mp3/tests/test_eventual.py`: R/W matrix, no-repair and first-R checks, empty and
  1 MiB values, Unicode/NUL keys, overload, queue/deadline fencing, lost ack,
  generation filtering, one/two/three owner outages, synchronous dispatch failure,
  graceful shutdown, concurrent coordinators, local TCP traffic and subprocess
  SIGKILL/restart.

Original Batch 3 acceptance verification on Python 3.12.4:

- Main full suite: **198 passed in 99.114 seconds**.
- Independent reviewer full suite: **198 passed in 107.886 seconds**.
- Main focused Eventual suite: **21 passed in 56.905 seconds**; reviewer: **21 passed
  in 55.468 seconds**.
- Main Strong/history regression suite: **14 passed in 30.756 seconds**.
- Main smoke, both help commands, compileall and `git diff --check` exited 0. Smoke
  still reports `KV NOT_IMPLEMENTED`, so the final demo command interface remains
  explicitly unfinished.

These checks use temporary SQLite files, real loopback TCP and disposable local
subprocesses. They are not ten-VM or university-VM results.

### Final Eventual GET dispatch correction

If one owner cannot even be contacted because the local dispatch step fails, that
owner counts as a failed attempt. It does not stop the coordinator from asking the
other four owners. For example, an R=2 read can still succeed if two of those four
reply with valid records. With R=5, one failed owner makes success impossible, so
the coordinator reports an error instead of waiting for the whole timeout or calling
another, unprescribed machine. If all five dispatches fail, the collector reports the
five failures directly; it does not make up fake NOT_FOUND replies. An empty value
still differs from an explicit NOT_FOUND response. Reads remain read-only and use the
same fixed five owners.

This correction changes only Eventual GET dispatch and its shared failure accounting;
Strong ABD and Eventual PUT are unchanged. The focused suite now passes **25 tests in
73.176 seconds**; the Strong/history suite passes **14 tests in 28.738 seconds**; and
the full suite passes **202 tests in 118.088 seconds**. Smoke, both help commands,
compileall and `git diff --check` exited successfully. The independent reviewer passed
the focused suite (**25 tests in 68.561 seconds**) and full suite (**202 tests in
116.737 seconds**) and found no blocker. These are local tests, not university VM
verification.

### What remains

Step 3 Batch 4 is joint adversarial integration across the completed Strong and
Eventual-noRR protocols. Later work still includes Eventual-RR/read repair, complete
mode switching, the required file-based demo commands, full MP1 log querying, ten-VM
deployment and fault testing, benchmark experiments, the report, and the demo.

## Step 3 — Batch 4: Adversarial Testing of Strong and Eventual

**Status: VERIFIED for the local adversarial integration gate after independent
review. This is not ten-VM verification.**

Passing a PUT test and a GET test one at a time is not enough. Several computers can
send operations at nearly the same moment, and responses can arrive in a different
order from requests. A history records each operation's coordinator, key, operation,
input, start time, response time and result. The existing history checker asks
whether there is a legal one-at-a-time ordering that respects those times and gives
each GET a value written earlier in that ordering. It checks the recorded bounded
examples; it is not a proof of every possible network execution.

### A three-writer Strong example

Three coordinators can ask to write `A`, `B` and `C` while three GETs are also
running. Each Strong operation queries the same five prescribed owners, needs three
valid replies, and (for writes) completes its install phase. The test starts all six
calls behind a barrier, records their real intervals, and checks that history. A
finished Strong PUT must have its required quorum; a failed or uncertain write is
not treated as a confirmed no-op if it may have committed.

```text
Coordinator 1: PUT A  ---- query/install ---- response
Coordinator 2: PUT B    ---- query/install ---- response
Coordinator 3: GET       ---- query/readback ---- response
                 \        shared five owners       /
```

### Eventual and resource pressure

Eventual reads may return an older value if that value is the newest among the first
R valid replies. A later, slower owner does not change a response already returned;
Eventual-noRR does not repair copies during a read. With a partition, Strong may stop
when it cannot obtain its three-of-five protocol quorums. Eventual may continue when
the selected R and W values can still be satisfied. Neither mode replaces a failed
owner with a sixth machine.

The new capacity test blocks an admitted operation at a test-controlled event, fills
one worker and one queue position, and submits another operation with a short
deadline. The third call reports OVERLOADED. Strong may return at its quorum while
minority replica calls finish, so the test waits within a bounded interval before
checking outbound cleanup.

The Eventual half also shows why W=2 is not a promise that all five replicas hold
each particular write. PUT 1 gets its two required exact-version durable replies,
while three other accepted sends are held. PUT 2 then durably installs its higher
version at all five owners. When the old sends are released, the remaining three
report `SUPERSEDED`: each owner already stores the higher version. The test checks
that both clients got two distinct exact-version durable W acknowledgments, every
owner attempt reaches a terminal report, and every SQLite replica still contains
PUT 2's version and bytes. KD-12 intentionally excludes these SUPERSEDED replies
from PUT 1's W count because they do not prove PUT 1's bytes were stored.

### What this batch changed

`mp3/tests/test_protocol_adversarial.py` adds a fresh loopback/SQLite harness for
each consistency mode, a seeded six-operation history across three Strong
coordinator identities, and bounded-admission stress in separate Strong and
Eventual-noRR clusters, including the event-controlled overlapping-write schedule
above. Earlier Strong/Eventual tests already exercise local
subprocess SIGKILL/restart, partial commits, stale generations, delays, owner
failures, post-ack propagation and read-no-repair behavior. This batch found no
demonstrated production defect; the reviewer tightened assertions so a failed PUT
cannot pass the history test as a no-op. The initial capacity assertion also
required five `DURABLE_ACK` outcomes for each successful Eventual PUT; the focused
follow-up below corrects that because an older overlapping PUT may validly receive
`SUPERSEDED` from pending owners. A bounded wait accounts for Strong minority RPCs
that can remain in flight after quorum completion.

Verification on Python 3.12.4:

- Main `python3 -B -m unittest discover -s mp3/tests -v`: **205 passed in 125.887s**.
- Independent reviewer ran the same full suite: **205 passed in 125.888s**.
- Focused Strong/history: **14 passed in 30.047s**; focused Eventual: **25 passed
  in 72.193s**; this batch's adversarial file: **3 passed in 8.276s**. The reviewer
  independently ran the new adversarial file: **3 passed in 8.252s**.
- Smoke, both help commands, compileall, and `git diff --check` exited successfully.
  Smoke still prints `KV NOT_IMPLEMENTED`; the final demo CLI is not implemented.

The reviewer found no remaining blocker. Review improved the tests by requiring
successful status for every completed Strong PUT, checking each Eventual client has
its exact W evidence and every owner attempt reaches a valid terminal result, and
allowing minority Strong RPCs a bounded interval to finish after quorum response.
No production bug was demonstrated and no production code was changed. All evidence
is local loopback, temporary SQLite, and local subprocess testing. Ten-VM behavior
and university VM fault tests remain unverified.

### Final KD-12 overlap correction

The Eventual capacity test now deliberately overlaps two writes to the same key.
The older PUT gets its W=2 durable exact-version acknowledgments while three of its
five accepted sends are held. The newer PUT then reaches all five owners. When the
older sends continue, those three owners reply `SUPERSEDED` because they already
have the newer tag. This is valid: W=2 means the older client proved two durable
copies of its own version, not that all five owners stored that version. The test
checks the two exact durable acknowledgments for each successful PUT, all five
terminal outcomes, and every replica's final record to confirm the older bytes did
not overwrite the newer version.

The independent reviewer inspected the Eventual coordinator, shared collector and
KD-12 rule, then passed the focused adversarial suite (**3 tests in 8.718s**). Main
verification passed the focused adversarial suite (**3 in 7.656s**), Strong/history
suite (**14 in 30.619s**), Eventual suite (**25 in 70.704s**), and full suite
(**205 in 124.085s**). Smoke, both help commands, compileall and `git diff --check`
also exited successfully. This correction changed tests and documentation only; no
production defect was found.

## Step 4 Design Gate: What We Are Planning

**Planning only — no Step 4 production feature has been implemented.** The detailed
proposal is in `STEP4_IMPLEMENTATION_DESIGN.md`; KD-13, full KD-16, KD-17 and the
new interface refinements are still waiting for Krishna's approval.

Eventual-RR adds one thing to Eventual-noRR: after a GET replies using its first R
replicas, the node continues checking the other prescribed owners. If a later
replica has a newer version, the coordinator can repair missing or older copies.
This happens in bounded background work so a slow replica does not hold the user
response open. A conditional install protects a concurrent newer write.

There is an important case: if the first R replies all say NOT_FOUND, the client can
receive NOT_FOUND, but Eventual-RR's background comparison still continues. If a
later valid owner reply contains a record, the background work can use it to repair
reachable missing or older copies. The client's earlier NOT_FOUND is not changed.
Only if the full bounded comparison finds no record is there nothing to repair. A
deterministic acceptance test must hold the later FOUND reply until after the client
has returned NOT_FOUND, then verify both the repair and the unchanged client result.

Changing modes is harder than changing one setting. Ten databases must agree on
the new mode and epoch. Strong-to-Eventual and Eventual-to-Strong must clear values
and version metadata everywhere, while membership and ring positions stay. Old
messages and repairs must be fenced so they cannot put cleared data back. The
proposal requires all ten nodes to prepare and record the same durable transition;
an incomplete or partitioned switch stays visibly pending/fenced instead of claiming
success.

The proposed new `control` endpoint is part of the canonical manifest, so adding it
creates a new manifest fingerprint. The VM labels and ring IDs still come from the
same stable identities; their five-owner lists must remain identical. The design
therefore calls for a paused, all-ten configuration cutover: validate the expanded
manifest, stop operations, point all ten nodes at the same new manifest, and reopen
only after fingerprint agreement. Each node keeps its existing SQLite path and
records. Schema-2 request fields and algorithms stay the same, and their regression
tests must pass with all nodes using the new fingerprint. During a mixed-config
cutover, mismatched nodes are unavailable; the system must not accept both
fingerprints as aliases.

Mode activation also has a local safety rule. After a node PREPAREs, it stays fenced
until it learns and records the durable transition decision. The coordinator records
GLOBAL_ACTIVE only after all ten nodes committed the same new epoch and data action.
If an ACTIVATE message is lost or the coordinator crashes, nodes that recorded
ACTIVE may serve the target mode; the others stay unavailable until the same
transition is replayed and reconciled. Operations count replies only from ACTIVE
owners on that target epoch. They can succeed if their normal quorum is met, or
return an explicit error if not; they never mix old and new modes or treat a fenced
owner as NOT_FOUND. The command remains pending until all ten confirm activation.
Acceptance cases cover lost ACTIVATE, coordinator restart, and requests during this
partial state. These rules are still proposed, not implemented or approved.

The CLI needs to send commands to a node process that is already running, and that
process already owns its SQLite database. The design proposes a small bounded
control service so the command client does not open a second copy of the database.
MP1's server is unsafe to import because it starts listening at import time and
assumes logs live in its current directory. The proposal keeps MP1 code unchanged
and adds an adapter that speaks its existing query framing and response format.

The next implementation batch, after approval, will settle shared RPC/control,
logging and lifecycle interfaces. Then Juhi can build Eventual-RR while Krishna
works on the separate MP1 query/log plumbing. Mode switching, all eight commands,
and full local integration follow in dependency order.

After Step 4 functionality, a separate small code-readability pass is planned. It
will remove only clearly stale comments/imports/dead code, preserve correctness
explanations, keep algorithms and interfaces unchanged, and require review plus full
tests. After authorized real VM checks, measurements and report preparation, the
team will prepare a reproducible handoff: verified branch/SHA, setup and restart
steps, implemented/pending features, exact test and failure evidence, logs/query
instructions, raw benchmark data and scripts when available, submission/demo
obligations, and concrete next actions for Juhi. Neither cleanup nor that handoff is
complete now; missing evidence must remain marked pending.


### Step 4 design review clarifications

The design review found that RR must reserve repair capacity before the first read,
and cancel/release outstanding reads without scheduling repairs if R becomes
impossible. Running slots remain reserved until their socket workers actually exit. The read-comparison window and repair RPCs need separate deadlines: the
proposal now gives the reads up to 10 seconds after the first-R response (capped at
15 seconds from admission), then gives repairs a separate five-second lease capped
at 20 seconds total from admission. The existing coordinator resource class has no
separate background executor, so the proposal calls for a bounded 16-active/64-queued
pool rather than implying that such a pool already exists. The proposed schema-3
path also needs its own activation and lease validation while retaining fixed-owner
checks; direct
`getfromreplica` can use the existing schema-1 local-read path for one nonowner.

A monotonic clock is local to one computer. The mode-control and multiput proposal
therefore sends remaining time budgets between VMs, and each receiver starts its own
monotonic deadline from the remaining budget. Network transit is not subtracted
exactly because monotonic clocks are not shared; the originating process bounds its
own wait, while each receiver has a bounded local lease. Multiput sends GO concurrently after
all nodes are READY; partial GO delivery is reported per node, without claiming an
atomic simultaneous start. Duplicate GO is keyed by command ID; a missing response
is PENDING/UNKNOWN rather than a claim that the node never started. The design now
persists a command and operation ID before dispatching each node's PUT; after a crash,
an accepted-but-unreported operation is UNKNOWN and is never replayed, avoiding a
second write. COMMIT_DECIDED means the transition cannot be aborted; GLOBAL_ACTIVE after all ten
nodes commit is the proposed mode linearization point. A node may serve only after it
receives GLOBAL_ACTIVE, while another node may remain fenced during message delivery.
The command still reports success only after all ten activation acknowledgments.

The MP1 client treats any `FILENAME:` response as success, so the proposed adapter
will close without that response on query errors; valid no-match remains an empty
`FILENAME:` body. These are design clarifications only. No Step 4 behavior has been
implemented or tested.
