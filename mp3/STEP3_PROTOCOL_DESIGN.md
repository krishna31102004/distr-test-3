# Step 3 protocol design — proposed for human approval

**Status: DESIGN PROPOSED / NOT IMPLEMENTED.** This document translates the
October 21 specification and the approved C1–C6 foundation into a concrete design
for Strong ABD and Eventual-noRR. It is a proposal for Krishna's review. KD-11 and
KD-12 remain PENDING; the recommendations below do not approve them. No production
protocol code is part of this handoff.

## 1. Authority, requirements, and current boundary

### Fixed by `SPEC.md`

- Ten fixed VMs, replication factor N=5, five clockwise successors, with crashes and
  restarts retaining the original owners and ring IDs.
- Strong is a per-key linearizable multi-writer register. Ordinary Cassandra R/W
  quorum intersection by itself is explicitly insufficient. A correct multi-writer
  atomic-register protocol such as two-phase ABD is required.
- Eventual-noRR uses configurable R and W in 1..5, default R=W=2. PUT targets all
  reachable prescribed replicas, succeeds after W distinct qualifying durable
  acknowledgments, and continues asynchronous propagation after acknowledgment.
  GET returns the highest version among its first R distinct qualifying replies.
- Values are arbitrary bytes up to 1 MiB. Empty bytes are present data. A timeout or
  unavailable replica is never evidence of NOT_FOUND. Writes and versions are durable
  before a qualifying acknowledgement. No global operation lock is allowed.
- Eventual versions order `(timestamp, processID)` pairs. Strong must use a correct
  multi-writer atomic-register protocol. Mode switching and read repair are outside
  this document's implementation scope.

### Existing approved and implemented foundation

- C1–C6 foundation contracts were approved by Krishna. The 10-node SHA-256 manifest,
  ring ownership, membership generations, SQLite WAL/FULL store, `Version` and
  `Record` types, `read_local`, `install_if_newer`, Eventual allocator, C5 single-RPC
  schema/framing, replica service, and `NodeRuntime` are implemented and locally
  tested. The Step 2 interface is exactly one `READ_LOCAL` or `INSTALL_IF_NEWER` call
  with `phase="local"`; it does not implement quorum behavior.
- Existing `Version(kind, number, writer_id)` compares lexicographically by
  `(number, writer_id)` within one kind. Existing `Store.allocate_eventual_version`
  durably reserves `max(time.time_ns(), last_timestamp+1)` and uses
  `vmNN/restart_generation` as the writer ID.
- Existing SQLite conditional installation is atomic and returns the actual
  `stored_version`. It detects equal-version/different-bytes conflicts and validates
  the exact mode epoch at the write boundary.
- Existing `ReplicaClient.call_replica` validates one response against its request,
  stable target identity, dispatch-time target generation and context. It has one
  absolute monotonic deadline and does not retry automatically. `NodeRuntime` owns
  the working lifecycle; Step 3 should extend it rather than create a competing
  runtime.

### New proposals

All interfaces, phase names, qualification rules, capacities and refinements below
are **PROPOSED — AWAITING HUMAN APPROVAL**. The two proposed coordinator protocols
are future behavior, not implemented or locally verified. The only correctness
claims below are informal design reasoning; they are not a formal proof.

## 2. Shared protocol model

### 2.1 Data and context

```python
type Key = str
type Value = bytes                 # length 0..1_048_576 inclusive
type OwnerId = str                 # vm01..vm10
type OperationId = str             # canonical lowercase UUID4; one user operation
type RequestId = str               # canonical lowercase UUID4; one RPC attempt
type Deadline = float              # absolute time.monotonic() value

@dataclass(frozen=True)
class ProtocolResult:
    status: OperationStatus        # OK | NOT_FOUND | TIMEOUT | ERROR | OVERLOADED
    value: bytes | None            # GET's bytes on OK; b"" is present; otherwise null
    version: Version | None        # GET result or successful PUT's allocated tag
    operation_id: OperationId
    qualifying_owners: tuple[OwnerId, ...]  # diagnostic evidence; does not itself mean success
    completion_unknown: bool       # an INSTALL may have committed without confirmation
    propagation: PropagationReport | None = None
    error: str | None = None
```

Proposed result shape by status:

| `status` | `value` | `version` | `completion_unknown` |
|---|---|---|---|
| `OK` PUT | null | allocated Strong/Eventual tag | false; the required write evidence was obtained |
| `OK` GET | returned bytes, including `b""` | actual selected record tag | false for the client-visible read result; Strong readback may have completed writes |
| `NOT_FOUND` | null | null | false; required explicit absence evidence was obtained |
| `TIMEOUT` or `ERROR` | null | null | true only if an INSTALL may have been sent/committed without confirmed operation completion |
| `OVERLOADED` | null | null | false; rejected before any operation write is dispatched |

`qualifying_owners` lists unique evidence actually collected before return, even on
an incomplete result, and must not be labeled as successful acknowledgments unless
`status=OK`. A Strong non-bottom GET that times out during readback returns no value;
its `completion_unknown` can still be true because readback installs may have
committed. Exact serialization of this local result is not part of C5.

`Context(epoch, mode)` comes from the local Store at operation admission and is
captured once. Every phase uses that same context and absolute operation deadline.
The protocol does not read a peer's mode as authority and does not silently switch
context mid-operation. Exact `STALE_EPOCH` or mixed-context evidence never counts.

`owners = manifest.owners(key)` is the immutable, ordered tuple of five prescribed
nodes. Membership status may inform which connection is likely to fail, but it never
removes, reorders for quorum identity, or replaces an owner. A slow or SUSPECTED owner
is still attempted subject to the shared deadline and reserved capacity.

### 2.2 The value order and absence

For an actual stored record, the existing `Version` remains the wire/domain version:

```text
Strong:   Version(kind="strong",   number=logical_counter, writer_id="vmNN/generation")
Eventual: Version(kind="eventual", number=timestamp,       writer_id="vmNN/generation")
```

Within the matching context, compare by integer `number`, then canonical `writer_id`
string. For Eventual, this is the required timestamp/process-ID total order. For
Strong, this provides deterministic tags while the counter component is allocated
above the phase-1 maximum. A tag may identify exactly one byte string for one key;
same-tag/different-value evidence is a hard `ERROR`, never a tie-break opportunity.

**Proposed Strong bottom:** represent conceptual bottom as `None` only in coordinator
logic. It is not a `Version`, is never persisted, and is not serialized as an
`INSTALL_IF_NEWER`. A Strong query reply `NOT_FOUND` is an explicit local bottom
observation. If all three phase-1 replies are `NOT_FOUND`, GET may return `NOT_FOUND`
after phase 1: no tagged value needs writeback. Any timeout/error among the remaining
replicas does not change those three explicit absence observations. If fewer than
three valid responses arrive, GET returns `TIMEOUT`/`ERROR`, not `NOT_FOUND`.

### 2.3 Shared response qualification

One raw TCP response is not quorum evidence. The shared validator first binds a reply
to the exact `operation_id`, `request_id`, manifest fingerprint, owner, sender/target,
phase, key-through-outstanding-request record, context, and expected replica
generation. The response must pass the existing status/field validation. Only then
does a protocol-specific predicate decide whether it qualifies.

Count at most one qualifying response per stable `vm_id` per phase. A duplicated
response, a second connection to the same owner, a late result, a wrong-generation
reply, an invalid response, `STALE_EPOCH`, `ERROR`, `OVERLOADED`, `VERSION_CONFLICT`,
or a transport failure contributes zero. Do not substitute a sixth node. Strong
phases and Eventual GET stop waiting once their threshold is met; their unused queued
attempts may be canceled, and already-running attempts remain bounded by the same
deadline. Eventual PUT is the exception: all five attempts were reserved before
dispatch and any remaining accepted jobs continue after W under the separate bounded
continuation lifetime in §5.1. No late response changes an already returned result.

## 3. Proposed C5 RPC extension

### 3.1 Preserve framing, version the metadata change explicitly

Keep the approved C5 TCP frame unchanged:

```text
[u32 outer payload length][u32 JSON length][UTF-8 JSON][raw value bytes]
```

Keep 16,384-byte JSON, 1 MiB value, and 1,064,964-byte outer-payload limits; validate
declared lengths before reading/allocating bodies; reject truncated, trailing,
malformed, duplicate-key, unknown-field, or contradictory messages. One request and
one response use one connection. Empty values have a zero-length raw body with
`present=True` on FOUND. Preserve the approved key policy: any strictly UTF-8
encodable string is allowed by the storage/RPC API, including empty and NUL-bearing
keys; CLI limitations do not narrow the distributed storage contract.

**Proposal:** add `operation_id` to both metadata envelopes and support protocol phase
names by defining schema version 2. Schema 1 remains accepted only for the existing
`phase="local"` primitive during migration/testing; no automatic downgrade is
permitted. Use a **one-time rollout gate**, separate from request handling: deploy a
schema-2-capable build to all ten VM identities, verify the release/version inventory
for the full fixed manifest, then enable a cluster configuration value such as
`protocol_schema=2` consistently on coordinators and replicas. Keep protocol
operations disabled until this rollout check is complete. This check happens at
rollout/activation, not before every operation, so a
temporarily down VM does not block later quorums. No schema negotiation or ten-node
health poll is added to the operation path. Request ID remains fresh per RPC; operation
ID remains stable across the phases of one client operation.

After the gate, coordinators send schema 2 only. Unsupported schema, a schema-1
response, or an incompatible peer is a nonqualifying failed attempt; never downgrade
that RPC to schema 1. Per operation, contact only the five prescribed owners. Strong
can finish when three compatible owners among those five qualify; Eventual can finish
when its configured R/W qualifying responses are obtained. An incompatible or
unavailable owner counts as no response, without substituting another node. If the
threshold cannot be reached by the deadline, return TIMEOUT/ERROR. The rollout check
must include offline VMs via the deployment/version inventory before activation; it
must not assume that mere membership gossip proves code compatibility.

Version-2 request fields are the existing C5 fields plus `operation_id`:

```text
schema=2, operation_id, request_id, manifest_id,
sender_id, sender_generation, target_id, epoch, mode,
op, phase, key, version, value_len
```

Response fields are the existing C5 response fields plus `operation_id`:

```text
schema=2, operation_id, request_id, manifest_id,
sender_id, sender_generation, target_id, epoch, mode,
op, phase, status, present,
version, stored_version, value_len, durable, error
```

Integers and versions keep C5 canonical decimal strings and `{kind, number,
writer_id}`. `version` on READ means the actual record read. `stored_version` on
INSTALL means the actual committed local version, never the incoming request version.
INSTALL responses carry no body and `version=null`; READ carries `stored_version=null`.
Both UUID fields must be canonical lowercase UUID4 strings; schema-2 epoch and
sender-generation must be positive because protocol messages are admitted only from
a ready node with a durable generation. The response's `sender_generation` is the
target replica generation captured at dispatch. Do not compare remote wall-clock
deadlines; only the caller enforces its local monotonic deadline.

### 3.2 Operations and phases

No new storage operation is required. Reuse `READ_LOCAL` and `INSTALL_IF_NEWER` and
add only these proposed phase values:

| Protocol use | op | phase | Request `version` / body | Valid response evidence |
|---|---|---|---|---|
| Strong PUT phase 1 | READ_LOCAL | `strong_query` | null / empty | FOUND with actual Strong version+bytes, or NOT_FOUND bottom |
| Strong GET phase 1 | READ_LOCAL | `strong_query` | null / empty | FOUND with actual Strong version+bytes, or NOT_FOUND bottom |
| Strong PUT phase 2 | INSTALL_IF_NEWER | `strong_write` | new Strong tag / client's bytes | INSTALLED or UNCHANGED at exact tag, or SUPERSEDED with strictly greater durable tag |
| Strong GET phase 2 | INSTALL_IF_NEWER | `strong_readback` | selected Strong tag / selected bytes | INSTALLED or UNCHANGED at exact tag, or SUPERSEDED with strictly greater durable tag |
| Eventual-noRR GET | READ_LOCAL | `eventual_read` | null / empty | FOUND with actual Eventual version+bytes, or NOT_FOUND |
| Eventual-noRR PUT | INSTALL_IF_NEWER | `eventual_put` | allocated Eventual tag / client's bytes | INSTALLED or UNCHANGED at exact tag only |
| Eventual-noRR post-ack continuation | INSTALL_IF_NEWER | `eventual_put` | same operation's same tag and bytes | Per-owner completion record; not a second client ack |

Later read repair may reuse `INSTALL_IF_NEWER` with a separate `eventual_repair`
phase, but that phase and its logging/budget are deferred to KD-13/Step 4. Do not
enable unknown phase strings.

### 3.3 Status classification by protocol phase

This is the proposed protocol-level layer above the existing RPC status validator.
The transport still returns a typed outcome separately from a valid response. The
existing schema-1 wire status enum has no `OVERLOADED` status: the replica currently
returns `ERROR` with an `OVERLOADED...` error code, which the caller maps to
`RpcOutcomeKind.OVERLOADED`. The proposed protocol keeps that explicit mapping unless
a schema-2 status addition is separately approved.

| Phase | Qualifies | Valid response but does not qualify | Abort/error evidence |
|---|---|---|---|
| `strong_query` | FOUND with valid Strong record; NOT_FOUND as explicit bottom | none beyond valid replies | Same tag with conflicting bytes aborts; stale context, wrong generation, invalid/duplicate response, and transport errors count zero |
| `strong_write` | INSTALLED/UNCHANGED at requested tag; SUPERSEDED only when durable `stored_version` is strictly greater | VERSION_CONFLICT, ERROR, STALE_EPOCH, OVERLOADED, missing/invalid evidence | Same-tag conflicting bytes aborts operation; no quorum means no successful PUT |
| `strong_readback` | INSTALLED/UNCHANGED at selected tag; SUPERSEDED only when durable `stored_version` is strictly greater | VERSION_CONFLICT, ERROR, STALE_EPOCH, OVERLOADED, missing/invalid evidence | Same-tag conflict aborts; non-bottom GET cannot return value without Q=3 readback evidence |
| `eventual_read` | FOUND with a valid Eventual record; NOT_FOUND as explicit absence | none beyond valid replies | Equal version with differing bytes in the selected first-R set aborts; failed replies count zero |
| `eventual_put` | INSTALLED/UNCHANGED with `durable=True` and `stored_version == requested version` | SUPERSEDED, even if it is durable at a higher version | VERSION_CONFLICT, STALE_EPOCH, ERROR, OVERLOADED, timeout, invalid response, and duplicate owner do not count toward W |

For every phase, receiving the declared number of bytes or a syntactically valid
response is insufficient until all request, operation, target, manifest, context,
phase and generation checks pass. `TIMEOUT` means the operation deadline expired
before the threshold; `ERROR` means a deterministic fatal conflict/protocol/context
condition or all attempts terminated with insufficient evidence before the deadline;
`OVERLOADED` is an admission rejection before any write dispatch. A phase may return
early only once it has its threshold, a fatal invariant conflict, a mathematically
impossible remaining threshold, or the same absolute deadline expires.

### 3.4 RPC validator and handler changes

Proposed `ReplicaRequest` and `ReplicaResponse` add `operation_id`; constructors,
wire codecs and strict exact-field validators recognize schema 2 and the listed
phases. Preserve schema-1 local parsing only as an explicit compatibility path.
Validate that `mode=strong` carries only Strong versions and either Eventual mode
carries only Eventual versions. `strong_query`/`eventual_read` require null version
and empty request body; `strong_write`/`strong_readback`/`eventual_put` require a
matching-kind version and may carry any 0..1 MiB bytes. The mode/context and epoch
must match Store state at the atomic storage boundary.

Replica responses are formed from actual Store results. FOUND/NOT_FOUND follow the
existing C5 shape. Durable statuses follow the existing committed `stored_version`
shape. For incoming v6 at a replica holding v7, `SUPERSEDED` reports
`stored_version=v7`. The replica does not decide whether that status qualifies for a
coordinator quorum. Strong and Eventual use different predicates below. Equal-tag
conflict remains `VERSION_CONFLICT` and never counts.

### 3.5 Proposed dispatch/collection contracts

```python
def dispatch_owner(
    node: Node, *, operation_id: OperationId, request: ReplicaRequest,
    deadline: Deadline,
) -> Future[RpcOutcome]: ...

def collect_distinct(
    calls: Iterable[tuple[Node, Future[RpcOutcome]]], *, required: int,
    operation_id: OperationId, phase: str, context: Context, deadline: Deadline,
    qualifies: Callable[[Node, ReplicaResponse], bool],
) -> QuorumCollection: ...

@dataclass(frozen=True)
class QuorumCollection:
    qualifying: tuple[QualifiedReply, ...]   # unique owner IDs only
    failures: tuple[OwnerFailure, ...]
    reached: bool
    deadline_expired: bool
```

The coordinator snapshots each target's current expected generation when dispatching
and requires that generation in the reply. A caller does not infer missing data from
a missing generation or an unreachable endpoint. Transport failure is recorded as
TIMEOUT/UNREACHABLE/INVALID_RESPONSE/OVERLOADED/ERROR. If gossip learns a newer
generation while an RPC is in flight, a response matching the dispatch-time expected
generation remains bound evidence for that already-issued request; do not recheck the
live table and retroactively invalidate it. Do not retry/reissue within that operation.
Later operations use the newer converged generation. A response from any generation
other than the one captured for its own dispatch is invalid evidence.

## 4. Strong multi-writer ABD proposal (KD-11)

**PROPOSED — AWAITING HUMAN APPROVAL.** Use Q=3 for both phases because 3-of-5
majorities intersect. Each phase sends concurrently to the fixed five owners; phase
completion needs three distinct valid owner identities. The caller may return after
the quorum threshold; remaining phase attempts are bounded and late replies are
ignored. A failure to reach three by the same absolute operation deadline returns
an explicit error/timeout.

### 4.1 Strong PUT pseudocode

```text
STRONG_PUT(key, value, context, deadline):
    op_id = uuid4()
    owners = manifest.owners(key)                   # exactly five, includes suspects
    reserve up to 10 bounded replica-call slots      # five query + five install
    replies = concurrently READ_LOCAL(phase=strong_query, operation_id=op_id)
              from all five owners under the same deadline
    q1 = collect 3 distinct qualifying FOUND/NOT_FOUND replies
    if fewer than 3 by deadline: return TIMEOUT/ERROR, no install was dispatched
    max_record = highest reply record; bottom if every reply is NOT_FOUND
    if any equal version has different bytes: return ERROR (invariant conflict)
    tag = store.allocate_strong_version(context, observed_max=max_record.version or None)
    record = (tag, value)
    q2 = concurrently INSTALL_IF_NEWER(phase=strong_write, record)
         to the same five prescribed owners under the original deadline
    count distinct durable-at-least-tag replies:
         INSTALLED/UNCHANGED with stored_version == tag
         SUPERSEDED with stored_version > tag
    if count >= 3: return OK(value, tag, qualifying owner IDs)
    else: return TIMEOUT/ERROR; completion_unknown reflects any possibly sent install
```

The phase-1 maximum is chosen only from valid replies in that phase. The allocator
uses `number > observed_max.number` and also `number > persisted local high_water`,
then binds the new number to the current stable VM and durable restart generation.
This is strictly greater than the observed tag regardless of writer-ID ordering. Two
writers that observed the same maximum may choose the same counter but their distinct
`writer_id` values yield distinct total-order tags. Two simultaneous PUTs from one
writer serialize only the short allocator transaction and obtain different counters.
The counter is committed before it is returned; it is never reserved over network IO.

Phase 2 counts `SUPERSEDED` only as **durable-at-least-tag evidence** when the exact
request/context/owner/generation matches, `durable=True`, and the actual committed
stored tag is strictly greater than this PUT's requested tag. A node with a greater
tag cannot later expose this older write as its current value. `VERSION_CONFLICT`,
`ERROR`, stale context, missing/malformed evidence, and merely received TCP bytes do
not count. This does not claim the replica stored this PUT's exact bytes; it records
that the tag is durably covered by a later value. This qualification is a new
protocol-specific recommendation requiring KD-11 approval.

### 4.2 Strong GET pseudocode

```text
STRONG_GET(key, context, deadline):
    op_id = uuid4()
    owners = manifest.owners(key)
    reserve up to 10 bounded replica-call slots      # phase 2 may be needed
    replies = concurrently READ_LOCAL(phase=strong_query, operation_id=op_id)
              from all five owners under the same deadline
    q1 = collect 3 distinct qualifying FOUND/NOT_FOUND replies
    if fewer than 3 by deadline: return TIMEOUT/ERROR (never NOT_FOUND)
    selected = greatest actual Version and associated bytes; absence is bottom
    if same selected Version has conflicting bytes: return ERROR
    if selected is bottom: return NOT_FOUND (the three explicit absences suffice)
    q2 = concurrently INSTALL_IF_NEWER(phase=strong_readback, selected record)
         to the same five owners under the original deadline
    count distinct durable-at-least-selected-tag replies using the Strong phase-2 rule
    if fewer than 3 by deadline: return TIMEOUT/ERROR, do not return a successful GET
    return OK(selected.value, selected.version)
```

If phase 1 succeeds but a non-bottom readback cannot obtain three qualifying
durable responses, the GET returns TIMEOUT/ERROR and does not return the candidate as
a successful Strong read. Some readback writes may have committed; the operation
result records incomplete/unknown side effects. For an all-bottom query, there is no
record or tagged value to write back; the explicit three-replica absence quorum can
return NOT_FOUND. This is the proposed bottom rule, not yet approved.

### 4.3 Strong completion and safety reasoning

**Assumptions:** This uses the assignment's fail/recover model: nodes may crash,
restart from their own durable local disk, and be partitioned or delayed, but a live
node follows the protocol (no Byzantine behavior or fabricated responses). The
manifest and five owners stay fixed. SQLite commits that report durable are assumed
to survive the specified process crash; local tests do not prove power-loss or VM
filesystem behavior. Messages may be delayed, lost, duplicated by a faulty/test
transport, or arrive out of order; response IDs and durable tags handle these cases.
The failure detector can be wrong temporarily and is not used as quorum evidence.

Each successful PUT first observes a majority, chooses a tag greater than every
tag in that majority, and then durably covers its tag on a majority. Each successful
non-bottom GET reads a majority, selects its maximum, and writes that record back to
a majority before returning. Any two majorities of five intersect. A write's query
majority intersects a prior completed write's phase-2 majority, so the later write
cannot allocate a lower tag than that completed write. A read quorum intersects a
completed write quorum; if the write completed before the read began, the read sees
at least that tag. Read writeback makes a partially observed maximum durable on a
majority before the read completes, preventing a later disjoint older result. The
selected tag/value pair is immutable; same-tag/different-value evidence fails closed.

These are informal ABD arguments under the conditions below, not a formal proof.
Liveness requires at least three prescribed owners to respond and durably install
within the operation deadline in each required phase. Safety does not depend on the
failure detector being accurate. A partition may prevent progress; it must not cause
owner replacement or weaken a quorum.

### 4.4 Adversarial traces

| Execution | Required result under proposal |
|---|---|
| Two writers query same max concurrently | Distinct writer IDs or same-writer serialized counters produce distinct ordered tags. Each installs to a majority; replies/arrival order do not change storage's max-tag CAS. Both may succeed; their overlapping writes are ordered by tag. |
| Writer crashes after installing on one replica | No PUT success unless it had durable-at-least-tag evidence from three distinct owners. A later GET may see the partial tag; if it obtains a majority and completes readback, that GET can order the incomplete PUT before itself. |
| Read sees partially propagated completed-or-incomplete write | If the PUT completed, phase-2 quorum intersection ensures the read quorum sees its tag or a newer one. If the write did not complete, it may still be chosen by a later read that sees it and writes it back; an earlier overlapping GET can be ordered before it. |
| Read overlaps another write | If the write is not visible in the query quorum, GET may return the preceding value and linearize before the overlapping write. If visible, GET selects max and writes it back; an overlapping higher write may linearize afterward. |
| Completed PUT followed by later GET | The PUT phase-2 majority intersects GET's query majority; GET sees that tag or a higher tag. A successful non-bottom GET readback reaches a majority. |
| Two owners fail | Three remain; operation may complete if all needed responses/installations arrive before deadline. Suspected owners remain prescribed. |
| Three owners fail | At most two respond; no phase can reach Q=3. Return TIMEOUT/ERROR, not NOT_FOUND or success. |
| Durable commit, lost acknowledgment | RPC outcome has `completion_unknown=True`; it is not a confirmed failure or quorum ack. Coordinator may succeed only if three other distinct qualifying evidence items arrive before deadline. No retry with a new tag. |
| Replica restarts with persisted version | Same VM/ring ID, higher durable process generation. The stored tag/value survive. A call sent to an older expected generation is rejected as invalid evidence; a later operation can use the recovered generation. |
| Delayed earlier install arrives after newer install | Atomic `install_if_newer` leaves the higher tag intact and replies SUPERSEDED with its actual stored tag. It qualifies only as Strong durable-at-least evidence if strictly greater and all bindings match. |
| Higher tag appears during GET readback | A SUPERSEDED reply can count as durable-at-least-selected-tag evidence. Because the higher write overlaps GET if it was absent from the phase-1 majority, the GET may linearize before it. A tag conflict aborts. |

## 5. Eventual-noRR proposal (KD-12)

**PROPOSED — AWAITING HUMAN APPROVAL.** N stays five. R and W are independently
configurable integers 1..5, initially R=W=2. Each operation captures configuration,
mode context and one absolute foreground deadline at admission. No read repair occurs
in this mode.

### 5.1 Eventual PUT and post-ack propagation

```text
EVENTUAL_PUT(key, value, R, W, context, admitted_at):
    op_id = uuid4(); foreground_deadline = admitted_at + 5 seconds
    atomically reserve capacity for five owner-attempt jobs before any send
    version = store.allocate_eventual_version(context)  # durable unique timestamp pair
    dispatch one INSTALL_IF_NEWER(eventual_put, same version/value/op_id)
        concurrently to each of the five prescribed owners
    collect distinct exact-version durable replies until W, impossibility, or deadline
    count only INSTALLED or UNCHANGED with durable=True and stored_version == version
    at W before deadline: close foreground result; respond OK; continue the same five jobs
    on foreground completion without W: respond TIMEOUT/ERROR; continue the same jobs
    record each owner's durable-confirmed, failed, timed-out, canceled/incomplete result
```

The **client deadline** is `admitted_at + 5s`; it bounds only when the coordinator
may answer the client. It does not set the socket deadline for Eventual PUT attempts.
Each reserved attempt instead has one mutable, thread-safe `AttemptLease` shared by
the five per-owner jobs:

```python
class AttemptLease:
    admitted_at: float
    hard_deadline: float = admitted_at + 15.0
    continuation_deadline: float | None = None
    canceled: bool = False

    def finish_foreground(self, completed_at: float) -> None:
        # exactly once, under the lease lock; reject a second transition
        continuation_deadline = min(completed_at + 10.0, hard_deadline)

    def current_deadline(self) -> float:
        if continuation_deadline is None:
            return hard_deadline
        return min(hard_deadline, continuation_deadline)
```

The lease lock protects `continuation_deadline` and `canceled`; `current_deadline()`
returns a snapshot while holding that lock. Each worker invokes the existing single
target client as `call_replica(target, request, deadline=lease.hard_deadline,
attempt_lease=lease)`. The initial argument satisfies the current API's finite-deadline
contract; when `attempt_lease` is present, the new socket helpers use the lease's
current deadline instead. The coordinator's separate foreground collector uses
`admitted_at + 5s`. It counts only validated acknowledgments received by that time.
The lease does not permit an individual attempt to exceed its current dynamic deadline.

The coordinator calls `finish_foreground()` exactly once when it returns OK at W,
returns TIMEOUT at five seconds, or returns an earlier terminal ERROR after dispatch
has begun. If admission fails before any dispatch (including OVERLOADED), there is
no lease continuation to start and all reservations are released without network
effects.
Consequently the deadline for all remaining attempts is
`min(foreground_completion + 10s, admitted_at + 15s)`. These are the approved C6
durations, unchanged. If W arrives at 4.9s, the attempts may continue to 14.9s. If W
arrives at 1s, they may continue to 11s. If the foreground times out at 5s, accepted
attempts may continue to 15s. The client receives its response immediately at the
foreground event; later responses update only the propagation report and cannot
retroactively satisfy W or change client status.

The existing `ReplicaClient.call_replica(..., deadline: float)` cannot implement
this lifetime by itself: it fixes the absolute deadline at call entry, and existing
`sendall`/`recv` waits cannot be shortened by another thread. **Smallest proposed RPC
extension:** keep the current float-deadline path unchanged for Strong and ordinary
RPCs, and add an optional `attempt_lease: AttemptLease | None` argument for Eventual
PUT. In `rpc.py`/`framing.py`, route that path through lease-aware connect/send/read
helpers. Those helpers use nonblocking sockets and `select`/`selectors` in bounded
poll intervals (at most 50 ms):

1. Before connect and each wait, read `AttemptLease.current_deadline()` and stop if
   canceled or expired. Connect uses `connect_ex` plus writable readiness; it is not
   an uninterruptible blocking `connect()`.
2. Send with `socket.send()` and a retained byte offset, never `sendall()`; partial
   writes resume at the unsent offset, so a deadline change cannot duplicate a frame.
   Preserve the existing “possible INSTALL transmission” boundary and mark
   completion unknown once transmission could have begun.
3. Receive exact frame lengths with the same bounded polling and dynamic deadline.
   On lease expiry/cancel, close the one RPC socket in its owning worker and return a
   typed `CANCELLED`/TIMEOUT outcome with `completion_unknown=True` iff an INSTALL may
   have reached storage. A late response is closed and does not become quorum proof.

Polling observes external cancellation and updated lease deadlines within 50 ms.
An implementation could wake selectors directly, but must preserve this bound and
partial-send accounting. No retry or second connection is made for an owner. The
schema and C5 frame do not change for cancellation; `CANCELLED` is a local
`RpcOutcomeKind`, not a new wire status.

Per-owner state moves once through `RESERVED -> QUEUED -> IN_FLIGHT -> terminal`.
Terminal records are `DURABLE_ACK` (exact requested tag), `SUPERSEDED` (newer tag;
not a W ack), `VERSION_CONFLICT`, `STALE_EPOCH`, `UNREACHABLE`, `ERROR`, `TIMEOUT`,
`CANCELLED_UNKNOWN`, `NOT_STARTED_EXPIRED`, or `NOT_STARTED_SHUTDOWN`. The attempt
ledger records the response/transport status and whether the INSTALL may have been
transmitted. `completion_unknown` is true for an owner if transmission was possible
but no validated response established the stored result; it is false when cancellation
happens before any request bytes could be sent. Each state transition releases that
owner's reserved capacity exactly once. `PropagationReport` becomes final only when
all five owners are terminal; if the process is SIGKILLed, no final report is claimed.

### Deterministic proposed deadline tests

These are acceptance tests to implement later; they have not been run. Use an injected
monotonic clock, an event-gated fake/loopback RPC peer per logical owner, and barriers
for dispatch/response order. Advance the fake clock instead of sleeping to cross
deadlines; use a real loopback socket test for the lease-aware connect/send/read
cancellation and partial-write path.

| Schedule | Controlled events | Required assertions |
|---|---|---|
| W at 4.9s; remaining owners finish at 8–10s | For default W=2, hold two exact durable responses until 4.9; gate the remaining three owners to finish at 8s, 9s and 10s; advance fake time in lockstep | Client receives OK at 4.9; lease expires at 14.9, not 5.0; later owner results are recorded as durable propagation, not late quorum evidence; exactly one request per owner |
| W at 1s; one active owner stays slow | Deliver W at 1; hold one RPC after a partial response/request state; advance to 10.99 then 11.00 | Client is already OK; active socket closes by 11.05; owner is terminal TIMEOUT/CANCELLED_UNKNOWN if INSTALL bytes may have been sent; no live worker/socket remains |
| Foreground deadline with possible commits | Deliver fewer than W acks by 5; make one owner commit but lose/delay its reply; keep other accepted jobs gated until their continuation outcomes | Client returns TIMEOUT at 5 with no value/version; `completion_unknown=True`; the lease continues to 15; later durable per-owner results do not turn the client result into success |
| A queued owner starts after W response | Configure the test scheduler with one active slot; gate the next reserved job until the first owner response causes W and the client response event is observed; release it at fake time 6 | Queued attempt starts once after client success, reads continuation deadline 11 (for W at 1), and either durably completes before then or reports not-started/expired; its reservation is not dropped at client `Future` completion |

The same test group must also assert that no attempt starts after its lease expires,
foreground responses after five seconds never satisfy W, and the last terminal owner
releases the fanout reservation. An explicit short shutdown deadline must cancel
active sockets, mark queued attempts `NOT_STARTED_SHUTDOWN`, return a non-clean report,
and keep SQLite open until replica handlers have joined.

Capacity must be reserved atomically for all five prescribed owner jobs before any
write is dispatched. Use the bounded shared attempt capacity
of 16 active plus 64 queued, with a reservation token that atomically claims five
slots; never enqueue five separately and risk partial admission. The reservation is
released per owner when its single attempt reaches a terminal result or expires.
If five slots cannot be reserved, reject with `OVERLOADED` before sending any write.
This aligns with approved C6 background capacity and guarantees that a successful
foreground W cannot silently lose the remaining fanout merely because the queue is
full. Each owner has one reserved slot and one state record; when a worker takes it,
the reservation transfers from queued to active without a gap. A queued job started
after the client response reads the already-updated lease and gets only the remaining
continuation time. It is never dropped merely because its client `Future` completed.

Reachability is learned by attempting the fixed owners; membership SUSPECTED is not
proof of unreachability. An unreachable owner is recorded as such and is never
replaced. `INSTALLED` means this request version was durably committed; `UNCHANGED`
means the exact version and bytes were already committed. `SUPERSEDED` means the
replica has a newer version and did not store this PUT's version, so it does **not**
count toward W under this recommendation. A protocol-level conflict, stale epoch,
overload, storage error, invalid response, timeout, or unreachable outcome also
contributes zero. This conservative exact-version rule avoids reporting a PUT as
durably acknowledged on a copy that contains only a different value. Whether
SUPERSEDED may qualify is an explicit KD-12 question.

If W becomes impossible because remaining attempts have terminal failures, return
TIMEOUT/ERROR immediately rather than waiting for the deadline; preserve accepted
remaining attempts only while their bounded continuation is valid. A foreground
timeout after some commits is ambiguous, not rollback: return no success, set
`completion_unknown` if an INSTALL may have been transmitted, and keep already
reserved work through its bounded lifetime. Do not claim a failed attempt as stored.
At expiry, mark unresolved owners incomplete and release their reservations.

On graceful shutdown, stop new operation admission; let accepted foreground calls
use their original deadline and accepted propagation use only its remaining
continuation budget. `NodeRuntime.stop()` closes coordinator admission first, then
waits for admitted coordinators/leases under its one monotonic shutdown deadline;
the existing default stop timeout is 12s, so a caller that needs the full approved
15s attempt lifetime must supply a sufficient timeout. A shorter configured stop
bound is a forced truncation: cancel active lease sockets, mark queued owners
`NOT_STARTED_SHUTDOWN`, and report `clean=False`/incomplete rather than success.
Only after outbound workers are canceled or joined and replica handlers are drained
may the runtime close membership sockets and SQLite; never close the Store under a
live handler. The same lease expiry bounds resources if no explicit shutdown occurs.
A forced stop or SIGKILL can truncate in-memory propagation; already committed data
survives, but restart does not recreate the old jobs. A stale mode epoch is checked
atomically by storage, rejects the write,
and is recorded as STALE_EPOCH; no old job is retagged to a new epoch. Full cluster
mode switching is Step 4/KD-16.

### 5.2 Eventual GET

```text
EVENTUAL_GET(key, R, context, deadline):
    owners = manifest.owners(key)                    # all five fixed owners
    dispatch READ_LOCAL(eventual_read) concurrently to all five under one deadline
    collect first R distinct qualifying FOUND/NOT_FOUND replies
    if fewer than R: return TIMEOUT/ERROR, never NOT_FOUND
    among those first R, return highest Version and its bytes
    if all R explicitly say NOT_FOUND: return NOT_FOUND
    if same Version has different bytes: return ERROR
    do not contact/compare additional replies for repair in Eventual-noRR
```

An empty value is FOUND with `b""`; missing is NOT_FOUND with no record. A failed
connection is not an absent reply. Arrival order only determines which R valid
responses constitute the selection set; among that set, version order determines the
answer. Equal versions must carry equal bytes. The answer may be stale because a
newer copy can lie outside the first R or a newer write may overlap; this is the
specified tradeoff of Eventual. Eventual-noRR deliberately performs no background
comparison or repair after returning.

| Setting | PUT foreground success | GET success | Failure example |
|---|---|---|---|
| R=1, W=1 | One distinct exact-version durable ack; all five accepted attempts continue | First one qualifying FOUND/NOT_FOUND | Failure of one owner alone does not fail the operation: another prescribed owner can satisfy R or W=1. It times out only if no qualifying owner responds before the foreground deadline |
| R=2, W=2 default | Two distinct exact-version durable acks | First two distinct qualifying replies; maximum of those two | One reachable read is insufficient; one durable write ack is insufficient |
| R=3, W=3 | Three distinct exact-version durable acks | First three qualifying replies; maximum of those three | Two failed owners can still allow progress; three failures cannot |
| R=5, W=5 | All five prescribed owners must durably acknowledge exact version | Five distinct qualifying replies required | Any one nonresponsive owner prevents the threshold before deadline |

## 6. Common coordinators, concurrency, and runtime integration

### 6.1 Proposed Python boundary

Keep protocol code in two small coordinators plus shared bounded dispatch/validation
helpers. Do not turn `NodeRuntime` into a second protocol implementation.

```python
class StrongCoordinator:
    def put(self, key: str, value: bytes, *, context: Context,
            deadline: Deadline) -> ProtocolResult: ...
    def get(self, key: str, *, context: Context,
            deadline: Deadline) -> ProtocolResult: ...

class EventualNoRRCoordinator:
    def __init__(self, *, read_quorum: int = 2, write_quorum: int = 2, ...): ...
    def put(self, key: str, value: bytes, *, context: Context,
            deadline: Deadline) -> ProtocolResult: ...
    def get(self, key: str, *, context: Context,
            deadline: Deadline) -> ProtocolResult: ...

class NodeRuntime:
    def strong_put(self, key: str, value: bytes, *, deadline: Deadline) -> ProtocolResult: ...
    def strong_get(self, key: str, *, deadline: Deadline) -> ProtocolResult: ...
    def eventual_put(self, key: str, value: bytes, *, deadline: Deadline,
                     read_quorum: int = 2, write_quorum: int = 2) -> ProtocolResult: ...
    def eventual_get(self, key: str, *, deadline: Deadline,
                     read_quorum: int = 2) -> ProtocolResult: ...
```

The NodeRuntime wrappers check local readiness/admission exactly as its current
`call_replica` does, snapshot the existing Store context and durable generation,
admit one bounded coordinator call, and delegate to the appropriate coordinator.
They do not create another node lifecycle, Store, membership table, or RPC client.
The demo CLI remains `NOT_IMPLEMENTED` until full command semantics, file IO and
completion output are completed in Step 4.

### 6.2 Resource ownership and lock rules

Use the already approved C6 capacities as hard active+queue bounds:

| Work | Active | Queued | Owner |
|---|---:|---:|---|
| Coordinator operations | 16 | 32 | NodeRuntime/common admission layer |
| Replica request handlers | 32 | 64 | Existing `ReplicaService` |
| Outbound replica attempts and retained eventual continuation | 16 | 64 | Proposed shared bounded attempt scheduler |
| Membership work | Existing separate configured capacity | Existing separate configured queue | Existing `MembershipRuntime` |

Do not use an unbounded `ThreadPoolExecutor` queue. The outbound scheduler must
support atomic reservation of an operation's fanout slots before an Eventual PUT
starts (five attempts) and before a Strong operation starts (up to ten phase attempts
in total, reused after phase 1). If a full reservation cannot be admitted, return
`OVERLOADED` before a write phase; never report success and then drop guaranteed
continuation work. Releasing/canceling an operation returns unused reservations.
The exact reservation bookkeeping and whether this scheduler should have a dedicated
class or be a small extension of existing bounded worker code are open KD-14 details;
the bounds and no-drop condition are not optional.

Worker roles must not wait cyclically: coordinator workers may wait on outbound
attempts, but outbound workers never wait on coordinator workers or enqueue follow-up
work into a saturated pool. A phase-2 dispatch is bounded by the same operation
deadline and reserved capacity. Shutdown closes new coordinator admission first,
then drains active foreground calls to their original deadlines, preserves accepted
Eventual continuation to its `AttemptLease` deadline unless the configured shutdown
bound is shorter, stops replica services, stops membership, and only then closes
Store. Reports distinguish incomplete operations, uncertain writes and incomplete
owner propagation.

No global operation lock, membership lock, or per-key lock is held over socket I/O.
Storage's short `BEGIN IMMEDIATE` transaction serializes physical SQLite writes. The
only serialization required for Strong tag allocation is its short durable allocator
transaction. Same-key operations may overlap in network phases. Same-node ownership
uses the same RPC path and response validation through its configured local KV
endpoint; it does not bypass the replica handler or durable Store boundary.

### 6.3 Deadline and result rules

- The already approved C6 foreground budget is five seconds from accepted operation
  admission. For Strong operations and Eventual GET, one absolute monotonic deadline
  covers queue admission, all protocol phases, RPC connect/send/receive and response
  validation; nested calls never reset it. For Eventual PUT, the client collector
  still closes at five seconds, but each accepted RPC attempt uses the separate
  `AttemptLease` in §5.1 so its socket can continue after that client response.
- Do not count a response received after the deadline as an on-time quorum response.
  Strong phase transitions consume the same deadline, not a fresh five seconds.
  Eventual PUT replies after its foreground deadline can only update the propagation
  report; they cannot satisfy W for the already-completed client operation.
- A timed-out Strong PUT after an install may have been sent has
  `completion_unknown=True`. A Strong PUT that times out in phase 1 has no write
  effect from that operation. A timed-out GET readback can have durable background
  writes even though no GET result is returned.
- A timed-out Eventual PUT sets completion uncertainty if any install may have been
  sent. The operation may still have accepted propagation attempts; their final
  owner outcomes are reported separately, not retroactively converted to client
  success.
- Never translate insufficient replies, transport errors, stale generation/epoch,
  conflict, or overload into `NOT_FOUND`.
- No automatic retries are proposed. If later added, retries must retain the same
  operation/version identity and remain bounded; that is not in this design's scope.

## 7. Dangerous and ambiguous cases

| Case | Proposed handling | Why / approval state |
|---|---|---|
| Strong `SUPERSEDED` during phase 2 | Count only as durable-at-least requested tag when stored tag is strictly greater and response evidence is fully bound | Recommended KD-11 rule; approval required. Exact requested tag remains the only success criterion for Eventual PUT |
| Ack lost after durable commit | Transport result is uncertain; no credit for that response. Continue collecting other distinct evidence within the original deadline | C5 already represents possible completion; coordinator semantics proposed |
| Strong phase 1 succeeds; phase 2 times out | PUT returns timeout/error with uncertainty if sent; non-bottom GET does not return success; any committed writes may remain | Do not claim operation completion without phase-2 quorum |
| Different replicas return different versions | Select max for Strong read/phase-1; Eventual chooses max among first R. Same tag/different bytes is ERROR | Ordered tags resolve different versions; exact-tag conflict violates tag immutability |
| All R reads explicitly return NOT_FOUND | Eventual GET returns NOT_FOUND; Strong GET returns NOT_FOUND only after its three-reply phase-1 quorum | Explicit absence evidence, not socket silence |
| Some reads say NOT_FOUND and others time out | Count only explicit NOT_FOUND; if fewer than R (or Strong Q=3), return timeout/error | Unavailability is not absence |
| Mixed epoch or mode evidence | Reject as nonqualifying; return error/timeout if threshold cannot be reached | Prevents combining incomparable database generations; KD-16 full transition remains deferred |
| Restart generation changes during an operation | A reply not matching its dispatch-time expected generation is invalid; no retry/substitution in same operation | Preserves C5 binding; later operation uses converged generation |
| Bounded queue exhausted | Reject before writes as OVERLOADED; Eventual PUT must atomically reserve five accepted attempts | No unbounded growth or silently discarded propagation |
| Shutdown during a Strong phase | Stop new admission; accepted call retains context/deadline and drains. On bound expiry report incomplete; don't close Store under live accepted work | Matches verified NodeRuntime lifecycle; future protocol tasks need equivalent accounting |
| Shutdown after Eventual W but before fanout completes | Client success stays success; accepted jobs drain to continuation/shutdown bound and report any unfinished owners | Separate client and background lifetime; shutdown refinement needs approval |
| Higher version discovered during Strong readback | SUPERSEDED higher-tag evidence may count toward readback; return selected version if three durable-at-least responses and linearizability ordering permits | Concurrent higher write can overlap the GET; tag conflict still aborts |
| Equal tag, conflicting bytes | Abort affected operation with ERROR and preserve replica data; do not choose by arrival order | Existing C3 invariant and no silent corruption |
| Only two prescribed replicas available | Strong cannot reach Q=3; Eventual may reach a configured R/W of 1 or 2 | Fixed ownership; no sixth-owner substitution |

## 8. Durable Strong allocator proposal (KD-10/KD-11)

Proposed storage method:

```python
def Store.allocate_strong_version(
    *, context: Context, observed_max: Version | None,
) -> Version:
    """Durably reserve a Strong tag above observed and local high-water marks."""
```

In one short `BEGIN IMMEDIATE` transaction: verify exact persisted Strong context;
verify `observed_max` is `None` or a Strong Version; read durable `strong_counter`;
set `counter = max(strong_counter, observed_max.number if present else 0) + 1`;
persist the new high-water counter; commit under SQLite WAL/FULL; then return
`Version("strong", counter, f"{vm_id}/{restart_generation}")`. Never hold the
transaction or an allocator lock over network I/O. Same-process concurrent calls
serialize through SQLite and return different counters. Different writers may have
equal counters but distinct writer IDs make tags unique and totally ordered. A
restart has a new durable generation in writer ID and a persisted counter high-water,
so it cannot reuse a prior counter/tag. Tag→bytes immutability is checked by
`install_if_newer`.

The proposed schema migration must add the allocator metadata without overwriting
or silently bootstrapping an existing database. It must be atomic, validate the
existing schema/identity/context first, and retain record rows. Whether the counter
is retained across a future Strong↔Eventual epoch switch is a KD-16-dependent detail;
it must never allow an old-epoch message to pass Store's exact epoch validation.

No Strong allocator exists yet. Do not use `time.time_ns()` as a substitute for the
phase-1 maximum.

## 9. Eventual allocator adequacy

The current Batch 1 `Store.allocate_eventual_version(context=...)` is sufficient as
the proposed Eventual allocator for one process per stable VM database: each call
uses a short durable transaction and reserves `max(wall_time_ns, last_timestamp+1)`;
concurrent local calls serialize transactionally; a backwards/frozen clock still
increases the counter; a restart increments the durable generation used in
`writer_id`; distinct VMs with equal timestamp numbers are ordered by different
writer IDs. Allocation metadata commits before a tag can be sent. This is verified
foundation behavior, not proof of cross-machine clock accuracy; timestamps decide
Eventual's specified LWW order, not Strong linearizability.

## 10. Approval questions (only new protocol details)

The following are the concrete KD-11/KD-12/KD-08/KD-14 decisions requested for
Krishna's design review. KD-01..07 and the approved C1–C6 directions are not reopened.

| ID | Question for Krishna | Recommended default |
|---|---|---|
| KD-11-A | Approve `None` as coordinator-only Strong bottom, with all-absent majority GET returning NOT_FOUND without installing a bottom record? | Yes; never serialize/persist bottom |
| KD-11-B | Approve phase names and schema-2 `operation_id` extension while retaining schema-1 `local` primitive only and no downgrade? | Yes; `strong_query`, `strong_write`, `strong_readback`; disable protocol against mixed versions |
| KD-11-C | Approve counting durable `SUPERSEDED` with strictly greater stored version as Strong write/readback quorum evidence? | Yes for Strong only; never count equal-tag conflicts |
| KD-11-D | Approve `allocate_strong_version(context, observed_max)` using a committed local counter greater than both observed counter and local high-water? | Yes; metadata migration preserves records and counter survives process restart |
| KD-12-A | Approve Eventual PUT W evidence as exact requested version in durable INSTALLED/UNCHANGED only, with SUPERSEDED excluded? | Yes; conservative and directly proves this value/version is durably installed |
| KD-12-B | Approve R/W defaults 2/2, first-R max selection, all-explicit-absence requirement, and no repair in noRR? | Yes; within mandated configurable 1..5 rules |
| KD-12-C | Approve atomic reservation of all five owner attempts and applying the already approved C6 continuation to them? | Yes; reuse the approved five-second foreground, ten-second continuation, fifteen-second total cap, and one attempt per owner; no dropped accepted work/retries |
| KD-08/14 | Approve one operation ID across phases, request ID per RPC, and the shared 16-active/64-queued outbound-attempt capacity? | Yes; reuse C6's approved single monotonic foreground deadline; overload before writes if required reservations are unavailable |
| KD-14 | For shutdown, may a configured bound truncate accepted Eventual background attempts and report them incomplete? | Yes; preserve foreground result and never claim unfinished writes succeeded |

This approval set does **not** approve Step 4 read repair, cluster-wide mode switching,
new command behavior, deployment, or remote VM tests. KD-13 and full KD-16 remain
deferred. Exact application of KD-17 logging and KD-18 benchmarks stays in their
roadmap steps.

## 11. Step 3 implementation batches and acceptance

This is the proposed decomposition. Implementation begins only after the human
explicitly approves the relevant decisions. The existing ownership record remains:
Krishna owns membership/routing/RPC and Strong ABD; Juhi owns storage/versioning and
Eventual-noRR; shared APIs must be jointly integrated without silently moving that
ownership.

### Step 3 Batch 1 — Shared coordinator infrastructure

**Work:** add bounded coordinator admission and outbound attempt scheduling, operation
IDs, strict distinct-owner response collection, protocol result/failure types,
context/generation evidence checks, and the schema-2 phase extension. Add the
Strong allocator storage migration/API only if KD-11-D is approved. No Strong or
Eventual coordinator behavior is enabled by this batch.

**Acceptance:** fixed five-owner dispatch; duplicate responses count once; wrong
owner/request/phase/context/generation is rejected; deadline covers every phase;
timeout is not NOT_FOUND; overload is bounded; post-ack work cannot be dropped after
reservation; same-node RPC works; no network-spanning locks; storage stays open until
accepted operations end. A missing identity in the rollout inventory keeps protocol
activation disabled. After activation, an unavailable owner is nonqualifying; an
old-schema owner is rejected without downgrade or owner substitution. Three compatible
prescribed owners can still satisfy Strong, and Eventual may proceed when its
configured R/W compatible prescribed owners qualify. Contract, concurrency, capacity
and restart tests pass.

### Step 3 Batch 2 — Strong ABD (Krishna)

Implement the two-phase PUT and GET rules above with majority 3, durable-at-least
phase-2 evidence, bottom/absence, unique query-aware tags, and exact-context fencing.
Acceptance includes sequential operations, concurrent multi-writer races, delayed and
out-of-order replies, partial writes, read writeback, process crashes/restarts, quorum
loss, lost acknowledgments, and history-based linearizability checking.

### Step 3 Batch 3 — Eventual-noRR (Juhi)

Implement configurable R/W, first-R selection, exact-version W acknowledgments,
durable timestamp versions, atomic all-five capacity reservation, and bounded
post-ack continuation. No read repair. Acceptance includes R/W 1/1, 2/2, 3/3 and 5/5,
partial/unreachable owners, empty values, ties/conflicts, lost acks, background
completion and lifetime/shutdown tests.

### Step 3 Batch 4 — Adversarial protocol integration (Joint)

Exercise several coordinators and real local subprocesses under concurrent operations,
SIGKILL/restart, delays, partitions, mode-context changes injected only through
approved fixtures, overload and shutdown. Check Strong histories with a checker or a
small independently validated history harness; retain raw traces. Confirm no
unapproved read repair, mode switch, full command, benchmark, or VM work crept into
Step 3. Ten-VM validation belongs to later Step 5 and is not implied by local tests.

## 12. Deferred to Step 4 or later

- Eventual-RR compare-all-reachable behavior, repair queues, repair lifetime, and
  RR-specific logging (KD-13).
- Complete cluster-wide consistency-mode transition, strong/eventual data clearing,
  offline-node behavior and epoch advancement (KD-16 partial approval only).
- Eight final CLI commands, file semantics, `liststore`, `ls`, multiput, required
  completion output, and complete MP1 log-query integration (KD-17).
- Ten-VM deployment/fault testing (explicit authorization still required) and
  benchmark harness/plots/report (KD-18/Step 5–6).
- Final supported VM Python version remains subject to KD-01 verification.

## 13. Design review checklist

- [ ] Strong PUT and GET use majority 3 in both required phases.
- [ ] Strong relies on phase-1 maximum, never a wall-clock-only tag.
- [ ] Strong absence has a defined bottom and all-absent quorum rule.
- [ ] Each completed Strong operation can be assigned an informal linearization point.
- [ ] Eventual reads count first R distinct valid owners and select the maximum among them.
- [ ] Eventual writes reserve five attempts before dispatch and count W durable exact-tag acks.
- [ ] Client and background deadlines are independent and bounded.
- [ ] Missing, empty, timeout, conflict, overload and uncertain completion remain distinct.
- [ ] Every phase validates operation/request IDs, owner, context and generation.
- [ ] Bounded resources, shutdown and no-lock-across-network rules are concrete.
- [ ] All new decisions remain PENDING until Krishna explicitly approves them.
