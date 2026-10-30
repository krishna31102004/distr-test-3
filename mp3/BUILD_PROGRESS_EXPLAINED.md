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

### What comes next?

Batch 4 will build on this complete local foundation: its coordinator can use the
replica RPC to contact prescribed owners, while applying the approved consistency
protocol rules. CLI integration and remaining steps stay in their planned order.

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
Eventual-noRR, Eventual-RR, read repair, complete CLI operations and Batch 4 are still
unimplemented.
