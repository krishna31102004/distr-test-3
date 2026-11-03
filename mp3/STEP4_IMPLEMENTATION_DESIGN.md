# Step 4 Implementation Design — Awaiting Human Approval

**Status: DESIGN PROPOSED / NOT IMPLEMENTED.** This document prepares Step 4
contracts for Krishna's approval. No KD-13, full KD-16, KD-17, or new KD-06/08/14/15
choice is approved here. `SPEC.md` remains authoritative; recommendations below are
engineering proposals. Step 2 and Step 3 behavior remain as implemented and verified
at the accepted local gate.

## Architecture summary

Step 4 adds three connected capabilities around the existing `NodeRuntime`: an
Eventual-RR coordinator that returns after R and performs bounded read repair in the
background; an all-ten-node, fail-closed mode transition; and a small command/control
service that routes the eight specified commands to the existing storage and
coordinator APIs. Replica operation logs will be queryable using the MP1 wire
mechanism through an import-safe adapter. Strong ABD and Eventual-noRR algorithms do
not change.

The recommended implementation boundary is:

```text
mp3 command client
        │ bounded control RPC
        ▼
NodeRuntime ── Eventual-RR coordinator ── five fixed KV owners
     │                    │                 │
     │ membership         └── bounded background compare/repair
     │
     ├── durable Store + epoch fence + mode-transition journal
     ├── KV replica service (existing endpoint)
     ├── proposed control service (new manifest endpoint)
     └── MP1-compatible log-query adapter (existing log_query endpoint)
```

The protocol path continues to use fixed prescribed owners, request/operation
correlation, process-local absolute monotonic deadlines, conditional durable
installation, and bounded workers. Cross-host control messages carry remaining
budgets; remote monotonic clock values are never compared. Membership suspicion affects attempt outcomes, never ring
ownership. A successful client response and background completion remain separate
facts.

## 1. Specification traceability

“Required” means mandated by the October 21 specification. “Proposal” means a
specific engineering route that Krishna must approve before implementation.

| SPEC requirement (short exact phrase) | Current status / source | Step 4 proposal and decision | Owner | Acceptance evidence / dependency |
|---|---|---|---|---|
| “N = 5”; first five clockwise successors stay fixed through crashes | Implemented locally: `ring.Manifest.owners`; `MembershipTable` retains suspected members | Reuse unchanged for RR, direct reads, diagnostics, and transition; never filter or replace owners (KD-02/03 approved) | Krishna | Unit/integration tests compare owner tuple before/during/after failures; existing ring contract |
| Values are opaque bytes, “up to 1 MiB”; empty bytes differ from NOT_FOUND | Store, RPC and Strong/noRR APIs support 0..1 MiB locally; command file paths are not implemented | Preserve raw bytes over control channel; explicit presence/status; no file write on failure | Juhi + Krishna | Empty, arbitrary binary, exact 1 MiB, oversized rejection, missing/error distinctions |
| “All commands should be executable at any of the VMs”; maximize read/write concurrency; “cannot take global locks” | `NodeRuntime` is local integrated service; Strong/noRR programmatic APIs exist; CLI parser is only syntax validation | Each VM runs a daemon and can submit command through its own control endpoint; separate bounded work; no global lock over network IO | Joint | Commands from different logical VMs; same-key overlap and independent-key progress; bounded overload |
| Fail-recover: durable values/version before ACK; restart preserves disk; crashed member remains in list and regains live status | SQLite WAL/FULL and membership generation recovery are implemented/tested locally | Transition journal and old-epoch fencing must survive process restart; preserve membership/ring metadata while clearing only required KV/version fields | Joint; Juhi storage | SIGKILL each transition state; recover DB and stable identity; stale gossip/request cannot undo current epoch |
| Strong is per-key linearizable; quorum intersection alone is insufficient; use multi-writer atomic register such as ABD | Strong ABD implemented and locally adversarial/history tested in Step 3 | No Step 4 algorithm change; include Strong in mode-switch/command integration only | Krishna | Existing bounded history checker plus command path and transition isolation; no ten-VM proof yet |
| Eventual-noRR uses configurable R/W (default 2/2), all-reachable PUT propagation, latest timestamp wins; no required background sync/hinted handoff | Eventual-noRR implemented and locally tested; `eventual.py` provides propagation report | Preserve existing protocol; ensure CLI/mode service invokes it exactly and does not add repair in noRR | Juhi | Existing R/W, lease, crash and no-repair tests plus command integration |
| Eventual GET returns highest version among R distinct qualifying responses | Implemented for noRR in `EventualNoRRCoordinator._run_get` | RR returns exactly the first-R result; late responses never mutate the client result (KD-13 pending) | Juhi | Controlled response order: newer late response repairs stores but returned bytes/version remain first-R result |
| RR “compare versions from all reachable replicas”; discover maximum and “repair stale or missing copies” | Not implemented; current noRR cancels unused reads | Reserve/dispatch all five reads; after first-R response retain attempts through a bounded continuation, compare valid responses, then issue at most one conditional repair pass (KD-13 pending) | Juhi | Late-high, missing, stale, concurrent-newer, expiry, shutdown tests; depends on new RR phases |
| “A newer concurrent write must never be overwritten” | Store `install_if_newer` is atomic and monotonic per replica | Repairs install the observed maximum using the existing conditional storage operation; `SUPERSEDED` is terminal evidence that a still-newer record won, not a reason to retry (KD-09/10 approved; KD-13 pending) | Juhi | Barrier races between repair and higher PUT; stored version never decreases |
| RR must log returned version, reachable replicas, replies, subsequent repairs | Basic opt-in JSON formatter and replica terminal events exist in `eventlog.py`; fields are currently limited; MP1 end-to-end querying is unverified | Extend structured event fields with operation/phase/context/version/status and correlation; add repair lifecycle events and MP1-compatible query adapter (KD-17 pending) | Joint, Krishna adapter | Parseable logs, restart query, late replies/repair outcome and exact terminal output tests |
| Strong ↔ Eventual clears KV data and version metadata on every node; membership/ring IDs retained | No cluster transition; Store validates a local context/epoch | All-ten prepared transition, durable decision, atomic per-node clear/context update, and fail-closed recovery. Retain records for Eventual-noRR ↔ Eventual-RR (KD-16 pending) | Joint; Krishna control, Juhi storage | Ten-node loopback transition matrix, partial failure/restart and delayed old-RPC tests |
| “This will change the mode in all processes” | No cluster-wide mode API; `Context` and storage epoch checks exist | A command is successful only when all ten fixed identities acknowledge the same committed epoch and target mode; partial transition remains explicitly pending/fenced (KD-16 pending) | Joint | No success with missing node; same-mode and duplicate request idempotency; restart reconciliation |
| `put key localfilename` reads “all the content” and prints status, key ring ID, acknowledging nodes | Parser only; `pending.execute` returns NOT_IMPLEMENTED; programmatic Strong/noRR APIs exist | Command client reads bytes locally, sends bounded raw body to its local node service, calls selected coordinator, prints key ID and actual qualifying acknowledgment evidence (KD-14 pending) | Krishna + Juhi | 0/1MiB file tests, success/error output, no raw-value logs; depends on control contract |
| `get key localfilename` writes bytes; `NOT_FOUND` distinct from errors | Parser only; no file command implementation | Write successful bytes to a same-directory temporary file and atomically replace destination; leave destination unchanged on NOT_FOUND/TIMEOUT/ERROR (KD-14 pending) | Joint | Empty/max bytes, preexisting destination unchanged on failure, atomic replace tests |
| `ls key` computes locations from membership; no liveness query required | `Manifest.owners` and key position exist; command remains pending | Print key ring ID and five prescribed VM addresses/ring IDs without RPCs (KD-02/14 approved foundation; command semantics mandated) | Krishna | Exact output test with owner marked SUSPECTED and unreachable |
| `liststore` lists local stored keys and key IDs | Store has point reads/install; no bounded listing API | Add bounded, read-only paginated/snapshot listing, escaped names and `ring.key_position` output (KD-09/14 pending implementation detail) | Juhi + Krishna | Stable pagination, Unicode/NUL key names, maximum rows, no remote calls |
| `getfromreplica VMaddress key localfilename` reads only that local copy; no further action | Single-replica RPC exists; parser only | Resolve address exactly through manifest; use one existing schema-1 `READ_LOCAL`, `phase=local` RPC to that selected node (including a nonowner); do not weaken schema-2/3 owner checks | Krishna | Owner/nonowner tests; no INSTALL or second target; endpoint failure remains error |
| `list_mem_ids` prints ten identities/ring IDs sorted by nodeID | `MembershipTable.list_mem_ids` implemented in numeric ring-ID order | Reuse unchanged and preserve SUSPECTED members (KD-02/03) | Krishna | Exact ten-row numeric ordering test |
| `multiput(key, VMi,…,VMj)` launches same-key puts simultaneously; each selected VM chooses arbitrary local random-byte value | Parser only; no distributed launch protocol | Prepare each selected node using its configured local input file, collect READY, then release one bounded barrier GO to each node; each node invokes its own coordinator and reports its result (KD-14 pending) | Joint | Barrier-controlled overlapping invocations from separate clients; distinct local values and actual invocation intervals |
| `consistency_mode {strong,eventual-noRR,eventual-RR}` changes all processes | Parser only; no cluster transition | Route to designated transition coordinator and report success only after complete ten-node activation; no local-only toggle (KD-16 pending) | Joint | Transition state-machine tests and all six mode-pair cases |
| Each replica prints `<local timestamp>: <received|responded><get|put> <key>` and logs operations | `ReplicaService` already prints required primitive receipt/response line; JSON fields currently only basic node/request metadata | Preserve exact line for valid GET/PUT receipt and response. Add one correlated structured record per actual event, never log value bytes (KD-17 pending) | Joint | Exact console format plus escaped-key and no-value-leak tests |
| Use MP1 query mechanism to inspect MP3 logs | MP1 source available; MP1 server has import-time argv/listener effects and CWD-based filenames | Keep MP1 byte-for-byte; serve compatible 4-byte-framed grep request/response at configured `log_query` endpoint using explicit MP3 log paths (KD-17 pending) | Krishna | Run unchanged MP1 client against local MP3 adapter from a temporary `machines.txt`; query after restart |
| Experiment 1: all 10 VMs, N=5, Strong vs noRR 1/1 and 2/2, 1 KiB, 100k preload, uniform keys, 50/50 GET/PUT, 1/2/4/8/10 clients, ≥3 min each, ≥3 readings | Not performed; Step 6 | Preserve exact matrix/seeds; only real workload results, averages, standard deviations and requested plot | Joint | Raw trial logs and repeatable seeds; Step 5 healthy ten-VM gate first |
| Experiment 2: all 10 VMs, N=5, RR vs noRR at R/W=1/1, 1 KiB, 100k preload, 10 clients, 50/50, 99% hot 10 keys, ≥3 min each, ≥3 readings | Not performed; Step 6 | Preserve exact matrix; include actual RR/logging costs and repeatable workload seed | Joint | Raw trials, throughput/mean-latency bars, average/stdev; Step 5 healthy-cluster gate first |
| Report under 3 pages, 12 pt; actual design, measurements, plots and analysis; code, form, Gradescope tags and demo signup/submission | Not done; Step 6 | Future evidence checklist below; no synthetic measurements or claim of submission | Joint | Verified report/source, upload/tag/submission receipts and demo rehearsal checklist |

## 2. KD-13 — Eventual-RR read repair (PROPOSED / PENDING)

### 2.1 Required semantics versus implementation choice

The specification fixes the client result: reply after R distinct qualifying owner
replies and return the highest version among those first R. RR additionally compares
all reachable prescribed replicas, finds the highest observed copy, repairs stale or
missing copies, and logs the returned version, reachable set, responses and repairs.
It does not prescribe the repair scheduling window or worker design.

**Recommendation:** use a single bounded post-response collection window followed by
one repair pass. Do not repair incrementally as each late reply arrives. Waiting for
the fixed window makes the selected maximum and target set easy to inspect, avoids
repeated writes as later versions arrive, and still bounds repair start and lifetime.
The tradeoff is that repair starts later than incremental repair.

### 2.2 Proposed API and data flow

All signatures below are **PROPOSED — AWAITING KRISHNA APPROVAL**. These are not
implemented functions.

```python
class EventualRRCoordinator:
    def eventual_rr_get(
        self, key: str, *, deadline: float, read_quorum: int = 2
    ) -> ProtocolResult: ...

class ReadRepairHandle:
    def snapshot(self) -> ReadRepairSnapshot: ...
    def wait(self, timeout: float | None = None) -> ReadRepairSnapshot: ...
```

Proposed immutable reports:

```python
ReadReplyEvidence(owner_id, generation, status, version, value_digest, received_at)
RepairOwnerResult(owner_id, generation, outcome, requested_version,
                  stored_version, durable, error)
ReadRepairSnapshot(operation_id, returned_version, returned_status,
                   reachable_owner_ids, replies, maximum_version,
                   repairs, terminal, incomplete_reason)
```

Use the existing `ProtocolResult.propagation` slot for the handle, rather than
creating a second job registry. The coordinator owns the handle and references to at
most five read futures and at most five repair outcomes per operation. The bounded
background executor owns execution. No thread is created per GET.

Execution:

1. Validate that the runtime is ready, schema-3 RR capability is locally activated,
   context mode is Eventual-RR, and `1 <= R <= 5`. Capture context, sender generation,
   admission time, one operation UUID, and one absolute foreground deadline.
2. Compute `manifest.owners(key)` once. Before any read is sent, atomically reserve
   five read attempts, up to five possible repair attempts, and one bounded RR
   completion task. Current `CoordinatorResources` has bounded coordinator and
   outbound-attempt executors, but no separate background executor; propose adding a
   bounded background executor at the already documented 16 active/64 queued limit.
   Reservation spans these pools and either succeeds completely or releases every
   partial reservation and returns OVERLOADED without sending. This guarantees later
   repair capacity without an unbounded per-GET thread or queue.
3. Dispatch one read to each of exactly those five owners using one request UUID per
   owner, phase `eventual_rr_read`, captured target generation, and a shared
   `AttemptLease`. Do not cancel the four non-winning attempts when the first R
   qualifying replies arrive.
4. The foreground collector returns on the first R distinct, valid FOUND or
   NOT_FOUND responses. Among only those R, choose the greatest version and its
   exact bytes. R explicit absences yield NOT_FOUND; any missing/failed response is
   not an absence. Equal-version/different-bytes among the first R fails closed.
   This client result is frozen independently from the background comparison. In
   particular, returning NOT_FOUND after R valid NOT_FOUND replies does not end the
   RR work: the remaining accepted reads continue under the read lease below. A
   later valid FOUND reply can reveal a record, become the background maximum, and
   cause repair of reachable replicas that returned NOT_FOUND or an older version.
   That discovery does not revise the already returned client NOT_FOUND.
   If R becomes mathematically impossible, cancel queued and in-flight unused read
   attempts and close running sockets through the lease-aware cancellation path.
   Queued calls become terminal without send; active slots remain owned until their
   workers actually exit, then release exactly once. Return explicit TIMEOUT/ERROR
   and schedule no repair. A foreground deadline before R is reached follows the
   same cancel/report/no-repair path. A failed RR GET must not leave orphaned reads
   or a reserved background slot.
5. Freeze the client result at that point. A late response never changes the result
   returned to the client. Hand all accepted remaining reads to the bounded
   background owner before returning.
6. On successful foreground completion, finish the read lease exactly once. Proposed
   read-collection cutoff: `min(foreground_completion + 10 seconds,
   admission + 15 seconds)`, using the existing C6 AttemptLease pattern. Stop earlier
   if all five reads are terminal. This is a KD-13 recommendation, not an already
   approved read-repair duration. A queued call may start only while the read lease
   is valid; one-shot attempts are never retried.
7. When all five reads are terminal or the read cutoff arrives, freeze evidence to
   valid responses completed before that deadline. Cancel queued/running calls and
   close their sockets through the bounded cancellation path. Active capacity remains
   reserved until the worker exits and records its terminal or incomplete outcome.
   begin repairs using a distinct repair lease once prior read work has released its
   owned capacity; never reuse the already-expired read lease. Proposed repair cutoff: `min(repair_start + 5 seconds, admission + 20
   seconds)`. The overall proposed RR lifetime is therefore at most 20 seconds from
   admission, with each repair one-shot and socket-bounded. This added repair budget
   is a new KD-13 proposal requiring approval.
   “Reachable” for this operation means a prescribed owner that returned a fully
   validated response under the attempt lease. Record other prescribed owners as
   timeout, unreachable, invalid, canceled, or incomplete; do not call them absent.
8. Find the maximum record among all valid FOUND responses in the complete bounded
   background comparison set, not just the first R client replies. The first-R
   client result remains unchanged. If every valid response in the complete set is
   NOT_FOUND (or there is no valid FOUND record), there is no value to install. If
   the first R were NOT_FOUND but a later valid response is FOUND, use that record
   for the background maximum and repair reachable missing/lower copies. If two
   responses contain equal versions with different bytes, mark a conflict, log it,
   and perform no repairs because the value for that tag is ambiguous.
9. For each reachable owner whose response was NOT_FOUND or whose version is lower
   than the maximum, dispatch exactly one `INSTALL_IF_NEWER`, phase
   `eventual_rr_repair`, for the maximum version and bytes. Do not install on an
   owner with the exact same version and bytes. No sixth owner is contacted.
10. Before repair dispatch, atomically consume the already reserved repair slots and
    background completion ownership. A repair lease uses the proposed five-second
    phase budget, capped at admission+20 seconds. Repair statuses: durable
    INSTALLED/UNCHANGED at the requested version means the selected record is
    installed; durable SUPERSEDED with a greater stored version means a concurrent
    later value won, so record it as `SUPERSEDED_NEWER` and stop; VERSION_CONFLICT,
    STALE_EPOCH, ERROR, overload, invalid response, timeout or unavailable owner is a
    non-successful repair outcome. No retry/loop follows a SUPERSEDED result. A later GET may discover and repair that newer copy.
11. A repair request carries the original read operation ID, fresh request ID,
    captured context/epoch and captured owner generation. Store epoch validation is
    atomic with conditional install. A mode transition cancels queued/running repair
    attempts and the storage boundary rejects any delayed old-epoch install.
12. Graceful shutdown drains the bounded read and repair jobs to their separate
    leases. Forced shutdown cancels sockets and reports incomplete owners with any
    uncertain completion evidence. SIGKILL can lose in-memory repair work; it does
    not lose previously committed values and does not create a promise of later repair.
    No per-operation monitor thread is created; a reserved bounded completion task
    owns the read-close/repair/report sequence.

If a valid newer copy is returned only after the fixed read window, it is outside
this read's reachable set. It cannot change the client result. The report marks the
read incomplete at its cutoff; a subsequent RR GET may discover it. If an owner
returns a higher `stored_version` from a repair SUPERSEDED reply, the RPC does not
contain that record's bytes, so the coordinator does not claim to know or propagate
the new value.

### 2.3 Proposed RPC extension

Existing `rpc.py` schema 2 strictly permits only Strong and Eventual-noRR phases;
unknown phases are rejected. Adding RR operations to schema 2 would make a
schema-2 peer's behavior depend on which build it runs. Recommend preserving schema
2 unchanged and adding **schema 3** for the Step 4 RR read/repair phases and for the
existing KD-12 Eventual PUT phase when mode is Eventual-RR. Eventual-RR changes GET;
PUT keeps its already approved KD-12 semantics:

| Request op / phase | Mode | Purpose | Qualifying response |
|---|---|---|---|
| READ_LOCAL / `eventual_rr_read` | Eventual-RR | One local replica snapshot | Valid FOUND or NOT_FOUND, exact operation/request/owner/context/generation binding |
| INSTALL_IF_NEWER / `eventual_put` | Eventual-RR | Existing Eventual PUT fanout | Same exact-version durable W qualification and bounded five-owner continuation as KD-12; this is not a repair |
| INSTALL_IF_NEWER / `eventual_rr_repair` | Eventual-RR | Conditional install of the known maximum | Durable exact-version INSTALLED/UNCHANGED; durable higher SUPERSEDED is terminal safe-no-overwrite evidence, not “repaired to selected version” |

Preserve C5 framing and JSON/raw-byte body limits. Schema 3 uses the exact schema-2
fields and validation plus `eventual_rr_read` and `eventual_rr_repair`, and allows
the existing `eventual_put` phase with `mode=Eventual-RR`; it adds no generic or
unvalidated fields. Eventual-RR PUT retains KD-12's exact-version W predicate and
bounded post-ack continuation. A release inventory must declare all ten identities
on the same RR-capable release before a runtime's separate schema-3 activation gate
is enabled. At startup the runtime validates its manifest fingerprint, its own
inventory entry, and the operator-supplied complete inventory; this is persistent
configuration, not a live all-ten health check per operation. Replica admission
independently requires schema-3 activation, validates the exact allowed schema-3
mode/phase, and reuses the existing fixed-owner check for each RR read, repair, and
Eventual-RR PUT. `NodeRuntime.dispatch_owner` must select schema 3 only for these
activated operations; `ReplicaClient` lease validation must allow only the exact
schema-3 phases under Eventual-RR. Extend client/replica validators, response
correlation, activation gates, and collector predicates with strict schema-3 cases.
No generic fallback or downgrade is allowed. An unreachable or incompatible target
is a nonqualifying prescribed owner, never a replacement. Strong and noRR schema-2
requests and lease rules remain unchanged.

### 2.4 KD-13 acceptance tests

Each test asserts client result separately from background report:

| Scenario | Required result |
|---|---|
| First R are v2; later owner has v7 | Client returns v2 if those were first R; background discovers v7 and conditionally repairs reachable lower/missing copies |
| First R are NOT_FOUND; a later owner returns v7 | Deterministically hold a third owner's valid FOUND(v7) until the first R absences have returned and the client result is observed as NOT_FOUND; release it before the read cutoff, then assert one conditional repair for each reachable absent/lower owner and that the client result remains NOT_FOUND |
| All five valid read responses are NOT_FOUND | Client returns NOT_FOUND when its first R are absent; complete comparison finds no record and sends no repair INSTALL |
| Client response before background window closes | Client call returns while handle is nonterminal; bounded wait later obtains terminal per-owner report |
| Missing reachable owner | One repair INSTALL is sent to that same owner; durable exact result recorded |
| Stale reachable owner | Conditional install reaches max; stored tag never decreases |
| Higher PUT races repair | Final target is higher PUT or repair max; never older; SUPERSEDED ends repair without loop |
| Repair response SUPERSEDED | Record actual greater stored version and no further write; do not claim selected bytes were installed there |
| Equal-version conflicting bytes | Error/conflict recorded; no repair writes for ambiguous tag |
| 1/2/3 owners unreachable | Use only valid reachable replies; no owner replacement; R failure is TIMEOUT/ERROR, not NOT_FOUND |
| Owner crashes during repair | Incomplete/uncertain owner result; other bounded repairs finish; no hang |
| Owner restarts | Generation must converge; old-generation evidence rejected; no implicit noRR handoff |
| Same prescribed owner set | Recorded targets equal `manifest.owners(key)` even when members are SUSPECTED |
| Shutdown / repair queue full | Explicit incomplete/OVERLOADED; all capacity released once; no worker/store leak |
| R becomes impossible before first-R result | Cancel queued/running reads through the lease-aware cancellation path, release each reservation once, return TIMEOUT/ERROR, retain per-owner terminal/incomplete evidence, schedule no repair |
| noRR control case | Eventual-noRR issues zero repair INSTALLs and does not retain reads after R under existing behavior |
| Required logs | Returned version, five owner identities, each reply/outcome, reachable set, repair target/version/status are queryable; no value body is logged |
| Schema-3 release and server gate | Mixed/absent inventory fails before dispatch; unactivated receiver and nonowner RR requests are rejected before storage mutation; schema-2 behavior is unchanged |

## 3. KD-16 — Cluster-wide consistency-mode transition (PENDING)

### 3.1 Safety contract and recommendation

Mandated by SPEC: there are three modes; switching Strong↔Eventual clears KV values
and version metadata on every node; Eventual-noRR↔Eventual-RR may retain or clear;
membership and ring IDs survive; a process crash/restart preserves ordinary data;
old delayed requests must not resurrect cleared data. The spec does not define a
distributed transition algorithm.

**Recommendation:** a fixed transition coordinator at stable `vm01`, reached by
requests from any VM, runs an all-ten, durable, fail-closed prepare/commit/activate
protocol. All ten fixed identities must acknowledge each barrier. A partition or
missing VM prevents a new transition from committing. This rare admin operation does
not justify Raft/Paxos in the MP; the tradeoff is intentional unavailability while
the coordinator or any required participant is down. A local-only toggle is
prohibited.

The transition has two distinct points. `COMMIT_DECIDED` is the irrevocability point:
vm01 persists it only after ten durable PREPARED acknowledgments, and it can never
roll back after that. `GLOBAL_ACTIVE`, recorded only after all ten nodes durably
commit the target context and clear/retain action, is the proposed mode linearization
point. A participant serves the new mode only after learning and durably recording
GLOBAL_ACTIVE. The command reports success only after all ten acknowledge ACTIVE.
The state may be unavailable during recovery, but it must not accept an operation
from a mismatched mode/epoch.

**Partial-activation safety invariant (part of the recommendation, not a new
approval):** after a participant durably records PREPARED, it serves neither old nor
target-context KV operations until it has durably resolved the matching transition.
No participant serves the target context before vm01 has durably recorded
GLOBAL_ACTIVE, which is possible only after all ten participants durably committed
the same target mode/epoch and clear-or-retain action. After that point, only a node
that receives and durably records that exact GLOBAL_ACTIVE decision may reopen
admission, and it serves only that target mode/epoch. A node that misses ACTIVATE or
cannot reconcile the transition stays fenced: it is an unavailable/nonqualifying
prescribed owner, never an old-mode fallback, NOT_FOUND, or replacement candidate.
An operation during partial activation may complete only if its ordinary protocol
obtains the required evidence from activated prescribed owners in the common target
context (for Strong, three of the five owners); otherwise it returns the ordinary
explicit timeout/error. No quorum combines old-context and target-context evidence.
The control command remains PENDING until all ten ACTIVE acknowledgments arrive.

### 3.2 Persistent records and local gate

Proposal for additive SQLite metadata (exact schema awaits approval):

```text
mode_transition(
  transition_id, coordinator_id, from_epoch, from_mode,
  target_epoch, target_mode, decision, participant_phase
)
cluster_mode_authority(
  active_epoch, active_mode, epoch_high_water, global_decision_id
)
```

Use one row per latest transition plus durable epoch high-water; retain a bounded
history of terminal transition IDs to reject delayed duplicate controls. Exact table
layout is an implementation detail, but the required fields and atomicity are not.
On each node, Store must atomically check both active context and transition phase
inside the same SQLite transaction as every read/install. While phase is not ACTIVE,
normal KV storage returns an explicit TRANSITIONING result and changes no record.
`NodeRuntime` separately closes new local coordinator admission and replica data
admission. Membership/gossip and the transition control path remain available.

The commit transaction on each participant is:

```text
BEGIN IMMEDIATE
  validate transition_id, coordinator, prepared old context and COMMIT decision
  if Strong <-> either Eventual:
      DELETE all records
      reset strong_counter and last_timestamp (version allocator metadata)
  else (Eventual-noRR <-> Eventual-RR): retain records and Eventual timestamp high-water
  set active mode/target epoch
  set local phase = COMMITTED (still reject normal KV traffic)
COMMIT
```

Membership generation, member status, manifest identity, ring IDs, and stable VM
identity are not cleared. Clear plus target-context/epoch update is one SQLite
transaction, so a process crash sees either the whole old state or the whole new
clear/context, never a partial table deletion. The prepared/transition marker is
outside the records table and survives a data clear.

### 3.3 State machine and sequence

```text
Node: ACTIVE(old)
  -> QUIESCING -> PREPARED(old fenced)
  -> COMMITTED(new context, data policy applied; still closed)
  -> ACTIVE(new)

Controller journal: IDLE -> PREPARING -> ABORT_DECIDED -> IDLE(old)
                              \-> COMMIT_DECIDED -> ACTIVATING -> ACTIVE(new)
```

`PREPARING` may coexist with nodes that have not received PREPARE yet. Until all ten
nodes acknowledge PREPARED, the old mode/epoch remains the only committed mode;
operations on nodes already quiescing can fail explicitly. The global
quiescence barrier is reached only after all ten have closed new data admission,
drained/canceled accepted work, fenced storage, and durably acknowledged PREPARED.
No target-mode operations are allowed before the durable global COMMIT decision and
all-ten data/context COMMIT barrier. This avoids claiming atomic simultaneous network
delivery; it guarantees that mixed old/new contexts never qualify as one operation.

Proposed sequence:

1. A command at any node sends `MODE_CHANGE(target_mode, request_id)` to vm01. The
   fixed coordinator serializes transitions in its durable journal. Same target and
   request ID joins/returns the existing status; another active target gets BUSY.
2. vm01 checks current active state from all ten and allocates a unique transition
   UUID plus an epoch strictly above durable `epoch_high_water`. Persist the allocated
   epoch before sending controls; aborted epoch numbers are burned, never reused.
3. Send PREPARE to all ten with old context, target context, transition ID, epoch,
   and remaining control-duration budget. Monotonic clock values are process-local:
   never send vm01's `time.monotonic()` value for another VM to compare. Each
   participant derives a local absolute monotonic deadline at receipt from the
   transmitted remaining budget. vm01's local absolute deadline remains authoritative
   for its wait/command result; elapsed time it observes before forwarding is
   subtracted from the next hop's budget. One-way network transit cannot be measured
   exactly without a clock assumption, so a receiver's local work can extend beyond
   the origin's wall deadline by transit time; do not claim a strict synchronized
   end-to-end cutoff. Transition ID/phase fencing and bounded local deadlines prevent
   stale controls from reopening or applying an incorrect state. Each participant
   closes new local coordinator and replica admission, waits for active operations/storage requests, then cancels
   Eventual-noRR propagation and RR jobs only at their approved bounded lifetimes or
   the configured transition deadline. It persists PREPARED before replying.
4. Strong operations already accepted are drained to their original deadline or
   reported incomplete; no second phase is admitted after cancellation. Eventual
   accepted propagation/RR attempts are drained to their lease or canceled with
   per-owner incomplete/unknown evidence. No transition may hold SQLite open
   transactions while doing network IO.
5. If all ten PREPARED acknowledgments do not arrive, vm01 persists ABORT_DECIDED.
   Send ABORT to prepared nodes. A participant resumes old mode only after durable
   matching ABORT; a node that is offline remains closed until it recovers the
   decision. No data has been cleared. A delayed PREPARE for an aborted ID is
   rejected by its tombstone.
6. If and only if all ten are PREPARED, vm01 durably records COMMIT_DECIDED. This is the irrevocability point: from then on
   the transition cannot abort. It is not yet the mode linearization point. It sends COMMIT to all ten. Each node applies
   the atomic local SQLite transaction above and acknowledges COMMITTED. No KV
   operation is admitted in this phase.
7. vm01 waits for ten COMMITTED responses, persists GLOBAL_ACTIVE for the transition,
   and sends ACTIVATE. A participant may reopen local admission only after it verifies
   this matching global decision and persists ACTIVE. ACTIVATE delivery is not
   simultaneous: a node that has received it may serve the new mode while another
   remains locally fenced. All ten have already durably committed the same context
   and clear/retain action, so old/new epoch evidence cannot qualify together. An
   unactivated owner is an explicit unavailable/nonqualifying owner. vm01 reports
   transition success only after all ten ACTIVE acknowledgments. Lost control replies are resolved by
   idempotent STATUS/COMMIT/ACTIVATE requests using the same transition ID, not a
   new transition.
8. If a node crashes at any point, its startup reads its local journal before
   opening KV admission. It contacts vm01's transition-status endpoint; PREPARED
   nodes apply the durable ABORT or COMMIT decision, COMMITTED nodes finish
   activation, and stale local context stays closed until reconciled. Membership may
   start for recovery, but protocol/client readiness stays false.

All control messages and data operations validate manifest, sender/target identity,
current generation, transition ID, mode, and epoch. Storage checks epoch/phase
atomically, so a delayed prior-epoch write or RR repair cannot resurrect data after
clear. Duplicate control messages are idempotent; a different payload for the same
transition ID is rejected.

### 3.4 Failure behavior, retry and limits

| Failure point | Required behavior |
|---|---|
| One node unavailable before PREPARE completes | No COMMIT; persist ABORT; report INCOMPLETE/TIMEOUT; reachable prepared nodes resume old mode after ABORT |
| Node misses ABORT | It remains transition-fenced on restart until status reconciliation; it does not guess old/new state |
| vm01 crashes before durable COMMIT | On vm01 recovery, absent COMMIT means persist ABORT; participants remain fail-closed until they learn it |
| vm01 crashes after durable COMMIT | Never abort; on restart replay COMMIT to all, finish clear/retain, then activation |
| Participant crashes after PREPARED | Restart consults vm01; apply only its durable COMMIT/ABORT; no KV readiness before resolution |
| Participant crashes after local clear/COMMITTED | SQLite atomicity preserves new context and clear; it remains closed until ACTIVE decision is verified |
| Participant unavailable after COMMIT | Cannot roll back; return PENDING/INCOMPLETE. Nodes that verified GLOBAL_ACTIVE and persisted ACTIVE may serve the committed target context; nodes not yet activated remain fenced. The unavailable node reconciles before serving after restart. |
| Control response lost | Query transition by ID/status; repeat idempotent control phase only; never allocate a second epoch for same request |
| ACTIVATE is lost after GLOBAL_ACTIVE | Nodes that durably recorded GLOBAL_ACTIVE may serve the target context; nodes that missed it remain fenced and nonqualifying. Replay ACTIVATE/status with the same transition ID; never roll back or admit the old context |
| vm01 crashes after GLOBAL_ACTIVE but before sending all ACTIVATE messages/ACKs | The committed decision is irrevocable. On recovery vm01 replays ACTIVATE/status to the same ten identities; already ACTIVE nodes continue the target context, while unreconciled nodes stay fenced |
| Operations arrive while some committed participants remain fenced | Active nodes may serve only the target context; fenced prescribed owners contribute no response/quorum evidence. Complete only if that protocol's threshold is met by active prescribed owners; otherwise return TIMEOUT/ERROR, never NOT_FOUND from silence or a mixed-mode result |
| Partition or vm01 unavailable | No alternate leader is elected. A locally ACTIVE participant with a valid committed context may continue that epoch; PREPARED or COMMITTED-not-ACTIVE participants stay fenced. vm01 unavailability blocks new transition/recovery decisions. No node infers GLOBAL_ACTIVE from silence; delayed controls are checked against durable transition ID/phase. |
| Competing requests | vm01 serializes; exact duplicate joins, different active target returns BUSY with transition ID |
| Same-mode command | No epoch increment or clear; return NO_CHANGE only after ten identities report matching ACTIVE mode/epoch |

Proposed starting control budget is 30 seconds, configurable, but its value,
quiescence budget, and user-visible PENDING versus TIMEOUT policy require KD-16
approval. vm01 maintains one absolute monotonic deadline for its own transition wait;
messages carry remaining duration and participants derive receipt-local deadlines.
Network transit cannot be subtracted exactly without a clock assumption, so this
bounds local processing and caller wait but does not claim a strict synchronized
cross-host wall-time deadline. Stale/delayed messages must be fenced by transition
ID and durable phase; after caller timeout, query the same transition rather than
creating a second one. No automatic retry of KV work; only idempotent bounded control
replays with the same transition ID are proposed.

The proposed service-readiness rule is explicit: after all ten COMMITTED
acknowledgments, vm01 records GLOBAL_ACTIVE; each participant may serve only after
receiving and persisting that decision. Delivery can be partial, so some nodes may
temporarily serve the same committed target context while others remain fenced. The
mode change's logical commit point is durable GLOBAL_ACTIVE after the all-ten commit
barrier; the command still reports success only after all ten ACTIVE acknowledgments.
This is not simultaneous network activation. If the course interpretation requires
no successful target-mode operations until every ACTIVATE acknowledgment exists,
Krishna must choose the stricter additional enable barrier before implementation.

### 3.5 Mode transition matrix

| From → to | Required record action | Recommendation |
|---|---|---|
| Strong → Eventual-noRR | Delete records and both mode-specific allocation metadata; advance epoch | Clear atomically on every node before activation |
| Strong → Eventual-RR | Same Strong-to-Eventual clear | Clear atomically on every node before activation |
| Eventual-noRR → Strong | Delete records and Eventual/Strong version allocator metadata; advance epoch | Clear atomically on every node before activation |
| Eventual-RR → Strong | Same Eventual-to-Strong clear; fence repairs | Cancel/fence repair, then clear atomically |
| Eventual-noRR → Eventual-RR | No record or Eventual timestamp clear; advance epoch | Retain data, enable repair only after all-ten activation |
| Eventual-RR → Eventual-noRR | No record or Eventual timestamp clear; advance epoch | Retain data; cancel/fence in-flight repair; noRR thereafter does no repair |
| Same mode | No clear and no epoch change | Return verified NO_CHANGE after cluster status agrees |
| Duplicate/concurrent request | No second transition for same request; serialize distinct requests | Join exact duplicate; report BUSY for competing mode target |

The only retention choice is Eventual-noRR ↔ Eventual-RR; retention is recommended
because both use the same Eventual version kind and because SPEC explicitly permits
retention. Strong/Eventual clearing is mandatory and not an approval choice.

### 3.6 KD-16 acceptance tests

Use ten logical loopback nodes with separate databases and real control/KV sockets.
No university VM test is included here.

- Complete all six directed mode transitions; assert each node's active mode/epoch,
  records, Strong counter, Eventual timestamp metadata, membership and ring ID.
- Drop one PREPARE reply; assert no commit/clear and no success; verify abort recovery.
- SIGKILL vm01 before and after durable COMMIT; restart same DB and verify abort vs
  commit replay is determined by the persistent journal, never guesswork.
- SIGKILL a participant during PREPARE, after COMMIT decision, during local clear,
  and before ACTIVATE; restart and reconcile.
- After GLOBAL_ACTIVE, drop ACTIVATE to one or more participants. Verify durably
  ACTIVE participants serve only the target epoch, missed-activation participants
  reject/withhold KV service until reconciliation, and replaying the same transition
  ID activates them without rollback.
- Crash vm01 after GLOBAL_ACTIVE but before sending all ACTIVATE messages and after
  sending them but before collecting all ACKs. Restart it and verify the durable
  decision is replayed, never aborted or replaced by a new epoch.
- During partial activation, issue Strong and Eventual operations whose prescribed
  owners include both ACTIVE and fenced nodes. Assert only valid target-context
  responses count; operation succeeds only if its normal threshold is met, otherwise
  TIMEOUT/ERROR. Fenced owners never yield NOT_FOUND or old-context evidence.
- Restart an offline node after a successful epoch change; it must reconcile before
  KV readiness and reject old context.
- Delay old Strong INSTALL, Eventual PUT, and RR repair beyond clear; assert STALE_EPOCH
  or TRANSITIONING and no record resurrection.
- Issue concurrent mode requests from different VMs; verify one serialized transition,
  stable IDs, duplicate idempotency and BUSY for conflict.
- Verify no cross-mode quorum evidence is accepted during mixed transition phases.
- Eventual-noRR↔RR preserves data but fences old-epoch requests/repairs; both
  Strong/Eventual directions clear records/version state everywhere.
- All membership entries, suspicion states as policy allows, VM identities and ring
  IDs survive; owner selection remains byte-for-byte identical.
- Inject control timeouts, duplicate messages and lost ACK after local SQLite commit;
  retries with same transition ID recover the true committed state.

## 4. KD-17 — operation logs and MP1 query adapter (PENDING)

### 4.1 Requirements and current implementation

The required replica terminal line is exactly:

```text
<local timestamp>: <received|responded><get|put> <key>
```

The current `eventlog.log_replica_event` prints this for primitive valid replica
requests and can append JSON Lines. `JsonFormatter` currently writes only timestamp,
level, event, node ID and request ID. The existing handler is opt-in, flushes through
Python logging but does not call `fsync`. It is not a full operation/repair audit
trail.

### 4.2 Proposed record contract

Keep terminal text for each replica receive and response, escaping key control
characters so one key cannot forge terminal lines. Add structured JSONL events with
these fields when applicable (null is explicit when no version/operation exists):

```text
timestamp_local, timestamp_utc, event, node_id, ring_id,
key, key_id, operation_id, request_id, operation, phase,
mode, epoch, sender_id, sender_generation, target_id, target_generation,
status, version, stored_version, durable, reachable_owner_ids,
returned_version, repair_target, repair_outcome, completion_unknown
```

The prescribed terminal line and structured record are separate outputs. Replica
`receivedget/put` and `respondedget/put` events are emitted once around actual local
handling; coordinator completion is a separate event and cannot be inferred from a
replica ACK. A client timeout never creates a success event. A repair attempt is
correlated to its originating read operation ID and a fresh request ID. Never log
value bytes, filenames' contents, or raw request bodies.

**Recommendation:** one configured per-VM path named `machine.<number>.log`, UTF-8
JSONL with stable field names, key encoded as a JSON string, one lock-protected
append+flush per record, and no per-record `fsync` by default. The stated crash model
is process `kill -9`; a flushed append remains visible to the OS after the process
dies, while host/power-loss durability is not a KV requirement. Test kill/reopen
visibility; if host-crash durability is later required, approve/measure `fsync` cost
separately. Use configurable bounded rotation (proposed 64 MiB segments, four
retained) and have the adapter query all retained segments. Rotation size/retention
and whether logs are best-effort or fsync-durable are pending KD-17 choices; the
spec's required events must not be silently skipped while logging is enabled.

Concurrency: a single owned logger and lock serialize each local append across
replica/control/coordinator threads. Use append-only writes. A crash may leave a
partial final line; query parsing skips/reports malformed trailing JSON and continues
valid earlier lines. Do not log while holding storage or membership locks. Log
failure is visible as LOG_ERROR; it cannot transform an unsuccessful operation into
success or undo a committed value.

### 4.3 MP1 reuse and adapter

Inspected MP1 behavior:

- `mp1/protocol.py` is a small, import-safe four-byte big-endian length-prefixed byte
  framing module.
- `mp1/client.py` fans out a grep-pattern request to `machines.txt`, parses the
  `FILENAME:<name>\n<body>` response, and has a `__main__` guard. Its default
  configuration filename is relative to the current working directory.
- `mp1/server.py` has top-level argv validation and immediately binds/listens when
  imported or run; it assumes `machine.<VM_NUMBER>.log` in the current directory,
  creates a thread per connection, and invokes local `grep` without an explicit
  resource bound. Do not import or start it from MP3.

**Recommendation:** keep MP1 source byte-for-byte unchanged. Add an import-safe,
bounded MP3 query adapter on each existing `log_query` endpoint. It accepts the same
single framed UTF-8 grep command, returns the MP1 `FILENAME:` response shape, and
searches the configured current/retained `machine.N.log` segments. Use the existing
`mp3.framing` compatible frame helpers; use a bounded worker pool, total read/query/
write deadline, request/output caps, and subprocess argument arrays (never a shell).
For `-c`, combine retained segments into one grep input so the old client's integer
count parser receives one count. Configure an MP1 `machines.txt` in a separate
run/config directory and launch the unchanged `mp1/client.py` with that directory as
CWD; this avoids changing MP1 files and works with its relative `CONFIG_FILE`.

The MP3 adapter should preserve the MP1 grep use cases needed by the course, but
reject unsafe/unbounded options, excessive patterns, and oversized output. The
unchanged MP1 client treats any response beginning with `FILENAME:` as success and
counts its body, so an adapter error must not use that valid response shape. For an
invalid request, unsupported option, grep execution failure, timeout, or output
limit, log a structured local error and close the connection without a framed
`FILENAME:` response; the existing client then reports the socket failure instead
of fabricating a match/count. A valid no-match query remains a valid `FILENAME:`
response with an empty body. Whether to support all GNU grep flags or a documented
safe subset is pending approval. Do not run MP1 deployment, remote tests, or its destructive test
suite. Any MP1 query acceptance test uses only a temporary loopback server/log set.

### 4.4 Logging/query tests

- Exact local terminal receive and response format for GET and PUT, including
  empty/missing values and escaped NUL/newline/Unicode keys.
- Structured record JSON parse, required fields, correlation, and absence of value
  bytes; distinct replica events versus client completion.
- Concurrent thread appends yield complete independent records without interleaving.
- Flush then local subprocess `kill -9`, reopen and query existing complete lines;
  host power-loss durability is not claimed.
- Eventual-RR returned version, five owner response evidence, reachable identities,
  repair target, selected version and outcome are all queryable.
- Lost ACK after durable install logs durable replica response if sent, and client
  timeout separately with uncertainty; no invented client success.
- Start local MP3 adapter on ephemeral loopback port and query using unchanged MP1
  client/protocol; verify all VM logs, empty match, `-c`, malformed pattern, timeout,
  rotation and process-restart retrieval.

## 5. KD-14 — command/control and file interfaces (PENDING)

### 5.1 Proposed process and service boundary

`python3 -m mp3.node` remains the long-lived process that owns SQLite, membership,
KV service, coordinators and logging. It must not also be launched by the command
client, because Store intentionally prevents two processes from opening one database.
The `python3 -m mp3` command becomes a short-lived client that reads files locally,
contacts the selected VM's daemon, prints the final result, and exits. The current
parser remains syntax-only until this gate is approved.

**Recommended small interface refinement (KD-06/07/15, pending):** add one
configurable `control` endpoint per manifest node, separate from KV, membership and
`log_query`; preserve the existing C5 frame limits and use a strict versioned control
envelope with `request_id`, `command_id`, manifest ID, sender/target VM, context/epoch,
command, status/error, and optional raw value body. Control work has its own bounded
active/queued pool. This avoids multiplexing admin fields into C5 key-replica
messages and keeps command latency separate from membership and KV handlers. Its
cost is a manifest/fingerprint and port assignment update; ring IDs and owner
selection remain unchanged. The command client connects only to the configured
selected VM control endpoint; multiput/mode control can contact remote control
endpoints. Do not add UDS plus a second remote protocol.

**Manifest fingerprint and safe configuration migration (proposal; no compatibility
code is implemented):** `Manifest.fingerprint` hashes the canonical serialized node
entries, including service endpoints. Adding `control` therefore intentionally
produces a new fingerprint even when every VM identity and ring ID is unchanged.
The ring ID is derived from the stable VM identity, not the manifest endpoint list,
so adding this endpoint must leave all ten ring IDs and every `owners(key)` result
unchanged. Do not accept both fingerprints as aliases in one live runtime: schema-2
and schema-3 RPCs bind to one exact `manifest_id`, and mixed fingerprints must fail
closed.

The recommended migration is a quiesced, all-ten configuration cutover. Keep the
current manifest as a versioned old configuration; generate the expanded manifest
separately with a unique control endpoint for each of the same ten VM identities.
Before rollout, validate the complete endpoint set and compare the two manifests:
VM IDs, ring IDs, KV/membership/log-query endpoints, and configured database paths
must match; only the added control endpoint and resulting fingerprint may differ.
Pause KV operations, then configure/restart all ten nodes against the same expanded
manifest. Reuse each existing database path unchanged; do not bootstrap, clear,
rewrite, or migrate its records or version metadata. Each node must validate the
same new fingerprint and preserved identity before listeners/admission reopen. Enable
the separate schema-3 capability only after the complete RR-capable release inventory
is validated. Schema-2 code paths and wire fields remain unchanged. Resume them only
when all participants in the running cluster use the same new manifest fingerprint;
run the existing Step 2/3 schema-2 regression suites against this homogeneous
expanded-manifest fixture as well as the retained legacy fixtures. A mismatch during
cutover remains unavailable/error evidence, never a reason to weaken fingerprint
validation or substitute owners. This has a bounded maintenance window but avoids
dual-fingerprint acceptance and preserves ring placement and durable local state.

Proposed signatures (not implemented):

```python
def execute_client_command(node: NodeRuntime, command: Command,
                           value: bytes | None = None) -> ClientCommandResult: ...
def run_cli_client(command: Command, *, manifest: Manifest, local_vm_id: str,
                    config: ClientConfig) -> ClientCommandResult: ...
def list_local_store(store: Store, *, limit: int, cursor: str | None = None): ...
def write_value_atomically(path: Path, value: bytes) -> None: ...
def multiput_start(key: str, vm_ids: tuple[str, ...], *, deadline: float): ...
def multiput_status(command_id: str, vm_id: str) -> ClientCommandResult: ...
def start_command_put(key: str, value: bytes, *, operation_id: str,
                      deadline: float) -> Future[ProtocolResult]: ...
```

Proposed durable local metadata (not implemented):

```text
command_origin_highwater(origin_vm_id, highest_sequence)
multiput_command(origin_vm_id, command_sequence, command_id, local_vm_id, state,
                 operation_id, result_status, result_version, updated_at)
```

Allocate each origin sequence durably at the origin runtime. A participant accepts a
new ARM only for a valid manifest origin and a sequence newer than that origin's
high-water; an exact duplicate command returns the existing row. Persist ARM before
READY, GO_ACCEPTED plus the generated operation ID before dispatch, and the result
after completion. Keep in-flight rows pinned and bound retained terminal result rows
per origin. When a terminal row is evicted, keep the high-water and return UNKNOWN
for that old sequence; never recreate or execute it. The high-water has at most ten
rows (the ten manifest origins). Preserve this command ledger across consistency
mode data clears because it is replay-fencing metadata, not KV values or version
allocators. A restart finding GO_ACCEPTED without a result reports UNKNOWN and does
not reissue the mode coordinator call. This deliberately chooses possible lost work
over duplicate PUT after a crash window.

The command envelope must have exact fields, strict UTF-8 JSON metadata, request
correlation and bounded raw body (0..1 MiB). It can reuse the current 4-byte
length-prefix plus JSON-length/raw body framing, but remains a distinct `control`
schema and endpoint. A READ result transports bytes unchanged. `NOT_FOUND`, timeout,
overload and error have no value body. The manifest must validate unique control
endpoints; local integration uses ephemeral loopback ports. A deadline is absolute
only inside the process that created it. Cross-host requests carry remaining-duration
budgets; receivers derive local monotonic deadlines and forward only the remainder.
ARM and GO fanout is concurrent. If ARM/READY fails, bounded ABORT is sent to each
armed node and no PUT starts if the participant has not already accepted GO. Each
participant accepts GO only for its matching command while its receipt-local ARM
lease remains valid; ABORT is terminal once processed. Before dispatching the PUT,
atomically persist `GO_ACCEPTED`, the command ID/sequence, and a generated operation
ID in a small SQLite command ledger. `start_command_put` passes that recorded ID to
the existing mode coordinator. Persist the terminal result when available. A duplicate
GO or STATUS with the same command ID returns the ledger state and never dispatches a
second PUT. After restart, a `GO_ACCEPTED` row with no durable result becomes
`UNKNOWN`; do not replay it because the first attempt may have reached replicas.
A crash after GO is persisted but before dispatch may therefore lose that PUT, but
cannot duplicate it or claim success. The origin allocates a durable per-VM command
sequence; selected participants keep a high-water per origin and bounded result rows.
Serialize ARM admission per origin so later sequence numbers do not overtake earlier
ARM fanout. Keep in-flight rows pinned; after terminal-result eviction, the high-water
still rejects an old/replayed sequence and status returns UNKNOWN. At most ten origin
high-water entries are needed because origins are manifest VMs. A delayed GO can race
ABORT, so a node that already accepted GO cannot be rolled back; report per-node
STARTED/result, confirmed NOT_STARTED, PENDING, or UNKNOWN. Missing response alone
never means NOT_STARTED, and the coordinator cannot promise atomic simultaneous
launch. Its local deadline bounds waiting; cross-host transit
is not measured by comparing monotonic clocks.

### 5.2 Command semantics and output

| Command | Proposed path and output |
|---|---|
| `put key localfilename` | Client opens local file in binary mode, reads at most 1 MiB + 1 byte, rejects oversized input before network send. Node chooses current mode coordinator. Print completion status, key ring ID, prescribed five owners, and per-owner actual ACK/propagation outcomes. Do not call every owner a confirmed copy when Eventual propagation is incomplete; show W acknowledgers separately from pending/failed attempts. |
| `get key localfilename` | Node calls current mode GET. On OK, client writes exact bytes (including empty) through a temporary file in the destination directory, flushes/fsyncs it, atomically `os.replace`s target, and best-effort fsyncs directory. On NOT_FOUND/TIMEOUT/ERROR, leave existing destination untouched. Print status/version and completion. |
| `ls key` | Pure local manifest calculation; print canonical key ring ID and five owner labels, configured KV addresses, and ring IDs, irrespective of membership liveness/storage. |
| `liststore` | Call bounded Store snapshot/list API; print local VM/ring ID and keys/key IDs. Pagination caps output and avoids holding a transaction while printing. |
| `getfromreplica VMaddress key localfilename` | Resolve exact manifest VM label or unique configured KV address; send one schema-1 `READ_LOCAL`, `phase=local` request to exactly that node. This uses the existing explicit single-replica diagnostic path, which permits nonowners. Do not weaken schema-2/3 fixed-owner validation. NOT_FOUND reflects only that local copy. No quorum, install, repair or liveness substitution. Atomically write only on FOUND. |
| `list_mem_ids` | Reuse `MembershipTable.list_mem_ids()` ascending numeric ring ID; include suspected entries and stable IDs. |
| `multiput(key, VMi,…,VMj)` | Designated coordinator sends ARM concurrently with one command ID and remaining-duration budget to each selected VM control endpoint. Each node reads its configured local random-value file and returns READY. If any required ARM/READY fails, send bounded ABORT to every armed node; report each node's state and start no PUT unless it already accepted GO. If all are READY, send GO concurrently; each node independently begins its mode coordinator PUT. A participant deduplicates GO/status by command ID and never starts a second PUT. Missing response means PENDING/UNKNOWN, not NOT_STARTED; report only confirmed NOT_STARTED, STARTED/result, PENDING or UNKNOWN. GO already accepted cannot be undone by a later ABORT. Store each result by VM/operation ID. Measure local invocation/response intervals; do not compare monotonic timestamps across VMs or claim globally simultaneous clocks. |
| `consistency_mode mode` | Send to vm01 transition authority. Print success only after its all-ten activation result; otherwise print PENDING/TIMEOUT/ERROR with transition ID. |

No successful distributed operation is printed until its protocol API has returned
the required status. For Strong PUT, list fixed destinations separately from the
actual qualifying second-phase acknowledgments; a durable higher SUPERSEDED response
is at-least-tag evidence, not proof that the client's exact bytes are stored there.
For Eventual PUT, print exact-version W acknowledgers and later propagation report
separately. Avoid raw data, filenames' bytes, or invented replicas in console output.

Command parser keeps shlex path handling, the documented parenthesized multiput
form, and strict no-empty argument validation. All requests carry one command/request
ID, selected node identity and current epoch. A monotonic deadline is absolute only inside the process that created it. Cross-host
control requests carry remaining duration; each receiver derives a local monotonic
deadline at receipt and forwards the residual duration after its own elapsed
processing. Network transit cannot be subtracted exactly without a clock assumption,
so the caller's deadline bounds its own wait while each receiver's bounded local
lease limits remote work. Do not compare host-local monotonic clocks or claim strict
synchronized end-to-end wall-time expiry. Status values are explicit:
OK, NOT_FOUND, TIMEOUT, OVERLOADED, BUSY/TRANSITIONING, ERROR, NOT_IMPLEMENTED
(only for still-unimplemented paths). Each invocation prints one final client-side
completion line; replica service prints its own required receive/respond lines.

### 5.3 Command tests

- Local command client to daemon over loopback, with concurrent clients and bounded
  overload; no second Store owner/process.
- Binary put/get for 0 bytes, arbitrary bytes and 1 MiB; >1 MiB rejected without
  network/storage mutation; paths with spaces; failure leaves existing get file intact.
- `ls` stable while owners are down; exact key/owner IDs.
- `liststore` pagination, duplicates, Unicode/NUL and keys from local installs.
- Direct getfromreplica of owner/nonowner, unavailable target and NOT_FOUND; assert
  no other owner contacted and no install/repair.
- `list_mem_ids` all ten sorted numeric ring ID, including suspected nodes.
- Multiput GO is dispatched concurrently after all READY. Test READY failure sends
  ABORT; GO/ABORT reordering and delayed GO obey each receipt-local ARM lease. Verify
  same-command GO/status is idempotent and cannot start a second PUT. Partial GO
  delivery reports confirmed NOT_STARTED, STARTED/result, PENDING or UNKNOWN; a
  missing response alone is never NOT_STARTED. Record per-node local intervals; do
  not compare `time.monotonic()` values across VMs or claim measured cross-host
  overlap without an explicitly approved clock-offset method.
- All eight help/parser commands; no command returns a false success.
- Mode command waits for actual transition result; any partial transition reports ID
  and incomplete status.
- Build old and expanded manifests; assert the new control endpoint changes the
  fingerprint while each stable VM identity, ring ID, key-owner tuple, KV/membership/
  log-query endpoint and SQLite path is preserved. After a quiesced all-ten cutover
  fixture, run Step 2/3 schema-2 regression tests with one homogeneous expanded
  fingerprint; assert mixed fingerprints fail closed and existing database records
  survive restart. No live rollout is part of this design review.

## 6. Explicit decision and approval matrix

All rows are **PENDING**. The SPEC behavior listed in each row is mandated and is not
optional; only the recommended implementation direction needs Krishna's approval.

| ID / decision | Recommended choice | Meaningful alternative | Why this fits MP3 | Main risk | Modules | Falsifying tests | Human approval | Current status |
|---|---|---|---|---|---|---|---|---|
| KD-13: RR comparison/repair lifetime | Atomically reserve five reads, up to five repairs and one bounded completion task; return first R; collect until all reads terminal or `min(response+10s, admission+15s)`; then repair under a distinct five-second phase lease capped at admission+20s; cancel/release and no repair if R impossible | Repair incrementally as new maxima arrive | Separate read and repair leases make late-read discovery and repair RPC bounds explicit | Adds a proposed 20-second local/admission-based total; remote receiver deadlines start on receipt, so strict cross-host wall expiry is not promised | `eventual.py`, `attempts.py`, `coordinator.py`, `node.py`, `rpc.py` | late high copy, phase deadlines, impossible R, shutdown, worker exhaustion | Krishna approval required | PENDING |
| KD-13: repair behavior | Repair reachable missing/lower replicas once via `INSTALL_IF_NEWER`; exact durable ACK is repaired; higher SUPERSEDED terminates without retry | Retry/re-read after SUPERSEDED | Existing atomic store guard prevents overwrite and avoids repair loops | Does not propagate a value learned only via SUPERSEDED because response has no bytes | `rpc.py`, `replica.py`, `storage.py`, `eventual.py` | concurrent higher PUT, equal-tag conflict, repair ACK loss | Krishna approval required | PENDING |
| KD-16: cluster authority/protocol | Fixed vm01 transition authority; all-ten durable prepare barrier; durable commit only after all prepared; idempotent commit/activation replay | Quorum or Raft leader election | Rare administrative operation; simple fixed cluster; missing node fails closed, no new algorithm | vm01 unavailable blocks new transition/recovery; all-ten required | `node.py`, new transition module, `membership.py`, control service, `storage.py` | coordinator/participant crash at each persisted stage; partitions | Krishna approval required | PENDING |
| KD-16: epoch/state persistence | Persistent transition journal and epoch high-water; store rejects all data operations unless local phase ACTIVE and context exact | Keep only active epoch and query coordinator for each RPC | Crash/restart must distinguish old/committed/prepared state; persistent local gate rejects stale messages atomically | Migration and recovery state-machine bugs | `storage.py`, `models.py`, `replica.py`, `node.py` | kill around SQLite commit; delayed old requests/repairs | Krishna approval required | PENDING |
| KD-16: clear vs retain | Clear records + all version allocators for Strong↔either Eventual; retain Eventual records/timestamp high-water for noRR↔RR; preserve membership/ring | Clear for all transitions | Strong/Eventual clear is required; Eventual retention is expressly permitted and avoids needless data loss | Retained data is eventual and may be stale; RR operates after activation | `storage.py`, `node.py`, coordinators | six transition matrix, data/version checks | Krishna approval required for allowed Eventual↔Eventual choice; Strong clear is mandated | PENDING |
| KD-16: partial transition/deadline | Require all ten fixed identities; no commit on incomplete prepare; after durable commit never abort; only locally activated nodes may serve the common committed context during partial ACTIVATE; command success waits for all ten ACTIVE; proposed configurable 30s budget propagated as remaining duration, not remote monotonic timestamp | Keep every node fenced until all ten ACTIVE acknowledgments | No old/new context overlap; explicit local readiness; bounded request response | Some nodes unavailable while activation delivery/recovery is incomplete | transition module, control service, `NodeRuntime.stop`/admission | partition, lost ACK, restart/status replay, delayed-hop budget | Krishna approval required | PENDING |
| KD-17: event schema/path/flush | Per-VM `machine.N.log` JSONL; exact terminal lines; append+flush every event; no per-event fsync; bounded retained segments queryable together | Text-only logs or fsync each record | Structured evidence supports repair/mode debugging; flush covers specified process crash model without every-RPC fsync latency | Rotation can evict old records; log IO overhead | `eventlog.py`, replica/coordinators, runtime config | concurrent append, SIGKILL/reopen, no value leakage, query across rotation | Krishna approval required | PENDING |
| KD-17: MP1 adapter | New bounded import-safe adapter speaks unchanged MP1 framing and valid FILENAME body at `log_query`; on invalid/error/timeout/output-limit, close without FILENAME so unchanged client reports failure | Launch original MP1 server subprocess | Avoids its import-time listener/CWD/thread assumptions and prevents the old client counting an error body as a match | Client exposes connection failure, not structured detail | new query adapter, `eventlog.py`, `mp1/client.py` as test-only executable | local client-to-adapter query, count/no-match/error failure, restart | Krishna approval required | PENDING |
| KD-14: CLI/control plane | Long-lived `NodeRuntime`; short-lived `python -m mp3` client; one separate bounded control endpoint per manifest node; same framed JSON+raw-bytes style | UDS local-only plus separate multiput RPC, or multiplex commands into KV socket | One command plane works from any VM, separates admin load and avoids opening a second Store | Adds endpoint/fingerprint config and a new strict message type | `ring.py`, `node.py`, `cli.py`, new control service/client, `config.py` | loopback daemon/client, wrong identity/schema, overload/shutdown | Krishna approval required | PENDING |
| KD-06/08/15: wire/control refinement | Preserve schema 2 and C5; schema 3 adds strict RR phases and allows existing `eventual_put` under Eventual-RR; narrowly extend dispatch/client lease validation and replica activation/owner checks; separate control envelope; cross-host remaining-duration budgets, local monotonic deadlines; no downgrade | Add phases to schema 2 or reuse `log_query` for admin | Schema 2 stays stable; RR PUT preserves KD-12; monotonic values are not compared across VMs | New inventory/capability rollout and manifest change | `rpc.py`, `replica.py`, `ring.py`, control module | mixed-release inventory, schema fuzz, owner rejection, delayed-hop budget | Krishna approval required | PENDING |
| KD-14: files and multiput | Local binary read capped at 1 MiB+1; success-only same-directory atomic destination replacement; concurrent ARM/READY then GO; durable GO/operation-ID ledger, per-origin sequence high-water and status dedup; partial GO yields PENDING/UNKNOWN | Sequential calls from one coordinator | Per-VM operations without a second Store opener; reports partial outcomes and prevents replay from starting another PUT across restart | Crash after accepted GO is ambiguous; bounded result eviction returns UNKNOWN; adds small durable command metadata | `cli.py`, control service, node config, `storage.py`, protocol tests | binary/path/failure, GO/ABORT reorder, SIGKILL/replay, duplicate GO, stale sequence, partial response | Krishna approval required | PENDING |
| KD-01: supported Python runtime | Continue dependency-free Python 3; verify supported minor on all university VMs before release, pin scripts to observed compatible runtime | Require Python 3.12 specifically without VM check | Current laptop is 3.12.4; VM version is unverified | Deployment syntax/runtime mismatch | all MP3 modules and deployment docs | compile/test under actual VM interpreter after authorization | Krishna approval required after VM inventory | PENDING |

## 7. Step 4 implementation batches and dependencies

These are proposed batch gates, not implementation progress. Existing task IDs and
owners remain intact: `E-02` Juhi; `C-02` joint; `C-01` joint; `L-01` joint with
Krishna leading MP1; Krishna retains routing/membership diagnostics; Juhi retains
Eventual/storage/version work.

### Step 4 Batch 1 — Shared contracts and control boundary (Joint; NOT STARTED)

- Approve KD-13 read/repair lifecycle, KD-16 transition state/data matrix, KD-17 log
  format/adapter, KD-14 command/service/file/multiput semantics, and any KD-06/08/15
  refinements.
- Freeze the schema-3 RR phase list, manifest `control` endpoint, mode-control
  messages, result/report fields, deadlines and logging event names before parallel
  production work.
- Acceptance: schema/API examples reviewed; decision matrix updated only after
  explicit Krishna approval; no code changes counted as implementation.

### Step 4 Batch 2 — Eventual-RR plus log plumbing (parallel after Batch 1)

- **Juhi:** implement `E-02` using noRR's first-R result and bounded five-owner
  collector; schedule one repair pass and no repair loop.
- **Krishna:** implement/extend opt-in structured log contract and the isolated,
  bounded MP1-compatible query adapter in separate files; do not edit MP1.
- **Joint gate:** tests for RR semantics and queryable event fields; integration may
  use agreed log API but no mode switching/CLI completion yet.

### Step 4 Batch 3 — Durable cluster mode switching (Joint; depends on Batch 1 and
RR background cancellation contract)

- **Krishna:** transition control transport, vm01 request serialization, membership/
  identity wiring and RPC fencing tests.
- **Juhi:** durable journal, atomic per-node clear/retain and storage transition
  validation; coordinate shared schema before touching another's module.
- **Joint:** ten-node local loopback crash/replay tests. Do not change `Context` on
  only the initiating node or report success before all required acknowledgments.

### Step 4 Batch 4 — Eight commands, files and multiput (Joint; depends on Batches
1–3)

- **Krishna:** parser/client adapter, `ls`, `list_mem_ids`, owner output and command
  diagnostics.
- **Juhi:** bounded `liststore` storage API and protocol/file result integration.
- **Joint:** exact completion text, local file atomicity, command overload, true
  per-VM ARM/READY/GO multiput, and mode command.

### Step 4 Batch 5 — Integrated local acceptance and MP1 query (Joint; depends on
Batches 2–4)

- Ten logical loopback VMs, real TCP, independent SQLite files, per-node logs,
  full command transcript, all transition/crash cases, RR repair races and MP1 query
  from the unmodified MP1 client. Record seeds, topology and faults in test output.
- Step 4 passes only if all acceptance requirements have explicit tests and full suite
  passes; this is still not university-VM evidence.

After Step 4 implementation, run a separate **minimal behavior-preserving cleanup
gate** before Step 5: inspect comments/imports/dead code; remove only clearly stale or
redundant items; preserve explanations of ABD, version/fanout, durable writes,
epochs, restart fencing, leases, ownership and shutdown. No broad refactor or
algorithm/schema/API change. Use a separate feature branch/commit, full regression
and focused protocol tests, independent review, and verified private-remote push.
Defer any risky cosmetic edit and record why.

## 8. Step 5/6 critical path, evidence package, and teammate handoff

Critical path: approve Step 4 contracts → RR/log contract → all-ten mode transition
and recovery → all eight commands/multiput → integrated local fault/MP1-query gate →
minimal cleanup review → obtain separate approval for ten-VM activity → verify Python
and ports → deploy/recover/real correctness → run healthy-cluster preloads and all
required benchmark points → analyze and write report → rehearse demo and finalize
handoff. The Nov 8 deadline is close; retain correctness gates and report missing
evidence honestly rather than shortening a required workload or fabricating results.

The future teammate/report handoff is a gate after actual deployment, measurements
and report preparation, not current work:

- authoritative private-review branch and full SHA plus reproducible checkout steps;
- implemented/pending matrix for all modes and eight commands;
- Python/dependency versions, ten-VM inventory, bootstrap/restart, command syntax,
  data/log paths and safe local/remote test procedures;
- actual full/focused tests, pass/fail transcripts, limitations and replayable bugs;
- owner/ring and membership behavior, SQLite locations, MP1 query instructions,
  and operational recovery actions;
- real ten-VM fault evidence, raw benchmark data, script commit/SHA, seeds/configs,
  plots and report source only when collected; otherwise mark each pending;
- separate course submission obligations: report upload/page tags, partner tag,
  code form, demo signup and demo checklist. Do not claim submission/hand-off until
  verified;
- preserved Krishna/Juhi ownership and prioritized next actions for Juhi.

## 9. Approval checklist and open risks

Before Step 4 Batch 1 implementation, Krishna must decide:

1. Approve/revise KD-13: atomically reserve five reads, up to five repairs and one
   bounded completion task; return first R; collect until all reads finish or the
   proposed ten-second continuation (15-second admission cap); then run one
   conditional repair pass under a separate proposed five-second repair lease, with
   a 20-second total admission cap. If R is impossible, cancel/release reads and do
   no repair. No retry/loop; SUPERSEDED terminates safely.
2. Approve/revise KD-16: fixed vm01 authority; all-ten prepare; durable
   COMMIT_DECIDED only after unanimous prepare; GLOBAL_ACTIVE after all-ten durable
   target commit; idempotent recovery/replay; command success only after all-ten
   ACTIVE acknowledgments. Locally ACTIVE participants with a valid committed epoch
   may continue during a partition; PREPARED and COMMITTED-not-ACTIVE participants
   stay fenced. vm01 outage blocks new/recovery decisions, and silence never implies
   activation.
3. Approve mandatory clear semantics and the allowed retention default for
   Eventual-noRR ↔ Eventual-RR; recommendation is retain data/version timestamp,
   advance epoch, fence old work.
4. Approve KD-17: JSONL `machine.N.log`, fields above, flush each line/no per-event
   fsync, bounded rotation, MP1-compatible adapter at existing `log_query` endpoint
   and safe grep subset.
5. Approve KD-14/KD-06/07/15: separate manifest `control` endpoint, daemon plus
   short-lived CLI client, strict control envelope using existing bounded framing,
   file replacement and multiput ARM/READY/GO with durable command-ID/operation-ID
   replay fencing, per-origin sequence high-water, and UNKNOWN after crash ambiguity.
6. Approve the proposed 30-second configurable mode-transition local control budget,
   remaining-duration propagation and receipt-local remote deadline limitation; do
   not rely on synchronized monotonic clocks. Approve the activation rule (locally
   activated nodes may serve the same committed context while others remain fenced,
   while command success waits for all ten ACTIVE acknowledgments). All local
   operation deadlines remain monotonic and absolute.
7. Confirm Python version only after the VM interpreter inventory is checked; no
   VM access is included in this design task.

Open technical risks to review: fixed vm01 is a single control-plane availability
point; a prepared cluster can be unavailable until it recovers; a bounded RR window
cannot discover a copy that replies after cutoff; log retention bounds historical
query coverage; adding a manifest endpoint changes its fingerprint/config; and
cross-VM multiput launch skew cannot be zero without synchronized clock assumptions.
These are visible tradeoffs, not claims of implementation or correctness proof.

**Design exit:** independent review and documentation-only checks complete; explicit
Krishna approval still required. No Step 4 production behavior is implemented by
this design.
