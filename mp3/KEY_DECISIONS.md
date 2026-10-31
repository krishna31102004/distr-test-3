# MP3 architectural decisions — Step 2 handoff

Updated October 29, 2026. Authority: [SPEC.md](SPEC.md). Krishna explicitly
approved the current C1–C6 contracts, starting defaults and qualifications in the
Batch 1 instruction. This supersedes earlier proposed/pending-contract wording.
Approval authorizes only the local Batch 1 types/versioning/storage implementation;
no membership, RPC service, Strong allocator/ABD, coordinators, repair, mode switching,
CLI integration or benchmarks are authorized. Ownership remains unchanged; scoped
work in Juhi-owned foundations is explicitly authorized by Krishna.

KD-01 VM compatibility remains unverified; KD-08 phase-specific qualification,
KD-10 Strong allocation and KD-16 full switching remain unresolved beyond the
approved foundation boundaries. KD-11/12/13/17/18 remain deferred. No runtime
verification is inferred from approval. AI assistance is permitted. Original
student MP1/MP2 code remains preserved; new Batch 1 code is AI-assisted work.

Useful historical explanations were recovered from the 509-line pre-handoff local
snapshot at `/var/folders/bb/1m2v9gh114980pb82ckc9j6h0000gn/T/mp3-handoff-o2jfpurb/KEY_DECISIONS.md`.
Alternatives below explain earlier tradeoffs, not requests to reopen accepted choices.
Planned future tests remain distinct from executed Batch 1 evidence in the plan.

## Fixed requirements

Ten VMs, N=5, fixed five successors, no all-key owner table, MP2 membership,
fail/recover with persistent data, binary values up to 1 MiB including empty bytes,
concurrent same-key operations without global application operation locks, three
consistency configurations, bounded errors, required commands/logs and MP1 querying
are requirements. Strong needs a multi-writer atomic register, not just overlapping
Cassandra quorums; the concrete ABD procedure remains KD-11. Eventual defaults R=W=2
and uses timestamp/process-ID ordering; RR returns after R then compares/repairs all
reachable copies. Strong↔Eventual clears values/version metadata on every node,
retaining membership/ring; Eventual↔Eventual may retain or clear. Both healthy-cluster
experiments use sequential operations per client for >=3 minutes, all-replica preload,
matched seeds and >=3 readings; plot forms and report constraints are fixed by SPEC.
Local persistence libraries are allowed; distributed replication/quorum/repair
libraries are prohibited. These requirements are not approval questions.

## KD-01 — Python runtime and dependency policy

**Status: PENDING — C1–C6 foundation approved; remaining compatibility/protocol scope deferred.**

**Krishna-approved direction (October 29, 2026):** Python; minimal external dependencies. Mac versions reported by Krishna: Python 3.12.4, SQLite 3.45.3.

**Concept / rationale:** All machines and both coding agents need the same language features and library behavior. A package being available on one laptop proves little about VMs.

**Remaining proposal / verification:** VM runtime compatibility and final supported Python minor; do not infer VM versions from the Mac.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Alternatives and tradeoffs (context, not reopened directions):** Standard library only keeps setup small but requires explicit
  networking/lifecycle code. A small justified dependency can save work but introduces
  installation/version constraints. Pinning an available Python minor improves
  repeatability; selecting a newer unavailable interpreter delays integration.

**Concrete example:** Juhi's annotation syntax must import on Krishna's VM, not just her laptop.

**Dependencies:** All modules, CI/local scripts, storage and benchmark tooling.

**Planned tests / acceptance:** Run imports, compile, unit suite, and smoke on the selected runtime
  and both development machines; record versions.

## KD-02 — Deterministic hashing and ring representation

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** SHA-256 of deterministic UTF-8; one permanent position per VM, no virtual nodes, five clockwise successors; down nodes retain positions.

**Concept / rationale:** A hash turns a key or stable machine label into a repeatable location on a circular number line. Successors clockwise own the key.

**Remaining proposal / verification:** Exact domain prefixes, stable labels, equality/collision rules and digest display in contract C1.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Same bytes/canonicalization/algorithm/width everywhere;
  collision handling and exact-position ownership are deterministic; ring positions
  never move after setup. Never use Python's randomized built-in `hash()`.

**Alternatives and tradeoffs (context, not reopened directions):** SHA-256 from hashlib is reproducible and dependency-free but
  gives long IDs. A shorter deterministic digest is compact but needs a collision
  rule. Virtual nodes can smooth balance but add needless assignment complexity unless
  the original spec requires/allows them. Inclusive vs strict successor matters on equality.

**Concrete reasoning/example:** Key 55 on positions 10..60 chooses 60,10,20,30,40. At key 20,
  `include_equal=True` starts at 20; False starts at 30. Phase 0 requires this argument.

**Dependencies:** `ring.py`, initial manifest, routing, `ls`, `list_mem_ids`.

**Planned tests / acceptance:** Cross-process known vectors; input permutations, collisions,
  equality/wraparound, five distinct owners; verify failure does not change owners.

## KD-03 — Stable node identity and restart recovery

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** Stable VM/ring identity, separate durable monotonically increasing restart generation; recover disk and reject stale gossip.

**Concept / rationale:** A machine's permanent ring identity differs from a running process's lifetime. A restart is a new process on the same owner position.

**Remaining proposal / verification:** Generation allocation/admission, same-generation suspicion refutation and merge rules in C1/C2.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Keep all initial owners and their disk contents; allow
  a valid restart to regain live status; delayed pre-crash gossip must not undo it.

**Alternatives and tradeoffs (context, not reopened directions):** Inventory-derived stable ID plus durable boot/incarnation
  generation is simple but requires persisted fencing. New MP2 process IDs mapped to
  stable ring labels preserve the old detector but require deduplication/ordering.

**Concrete reasoning/example:** VM4 restarts after peers mark it FAILED; it still owns the
  same five-way placements and presents a newer generation, rather than adding VM11.

**Dependencies:** Future membership adapter, disk metadata, routing, versions, RPC.

**Planned tests / acceptance:** kill -9/restart preserves positions and bytes; stale failure messages
  cannot re-fail a new generation; introducer-down and repeated-restart tests.

## KD-04 — Failure detector timeouts and suspicion lifecycle

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** Adapt MP2 suspicion detector with monotonic timers and infinite suspicion retention; recover using newer restart generations, never remove crash victims.

**Concept / rationale:** Missing gossip suggests a failure; it does not prove a crash. Suspicion gives a live process time to refute an accusation with a newer incarnation.

**Remaining proposal / verification:** Approximately 2.5s initial detection threshold is proposed, untested; same-generation refutation details in C2.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Liveness hints never change prescribed owners or justify
  a false NOT_FOUND; stale membership must not weaken consistency quorums.

**Alternatives and tradeoffs (context, not reopened directions):** The accepted infinite-suspicion direction retains failed owners and permits recovery; a terminal-failure lifecycle from MP2 cannot be used unchanged. Shorter detection thresholds react faster but risk false suspicion under load; longer ones reduce false positives but delay detection. Monotonic elapsed timing avoids wall-clock jumps.

**Concrete reasoning/example:** A busy VM pauses for 1.2s; suspicion should permit a refutation,
  not cause a new ring or a read of an incorrectly assumed empty replica.

**Dependencies:** Membership adapter, routing hints, diagnostic CLI, failure tests.

**Planned tests / acceptance:** Fake-clock transitions, delayed/out-of-order gossip, false-positive
  rate under load, detection time, live-status restoration after restart.

## KD-05 — Gossip interval and fanout

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** Full membership snapshots, initial 0.5s gossip interval and fanout 3, bounded background resources; tune only with evidence.

**Concept / rationale:** Each node periodically shares its membership view with a few peers.

**Remaining proposal / verification:** Bounded scheduler/queue sizes and retained-suspect probing in C2/C6; starting values are accepted directions, not measured optimal settings.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Alternatives and tradeoffs (context, not reopened directions):** Existing full snapshots are simple for ten nodes but add
  traffic. Delta gossip reduces bytes but adds bookkeeping. Larger fanout improves
  dissemination but costs more; bounded send workers prevent unchecked growth.

**Concrete reasoning/example:** Three unreachable peers must not spawn an unbounded backlog
  every half-second while client requests wait for workers.

**Dependencies:** Membership networking and resource limits (KD-07/14).

**Planned tests / acceptance:** Convergence, bandwidth, queue/thread counts during partitions and
  hot-key workloads; contrast against the MP2 baseline rather than claiming improvement.

## KD-06 — RPC transport, framing, and binary serialization

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** TCP, four-byte length prefix, JSON metadata plus unchanged raw bytes; explicit schema/request IDs/validation/caps; empty and 1 MiB values supported.

**Concept / rationale:** Framing finds message boundaries; serialization explains the fields. TCP is a byte stream, so a single recv is not a whole message.

**Remaining proposal / verification:** Exact envelope, limits and validation in C5 are approved as foundation contracts.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Preserve all 0..1 MiB values byte-for-byte, including
  empty data; reject invalid/truncated/oversized envelopes; distinguish absence from
  unavailable responses; validate fields before dispatch. Timeouts must be finite.

**Alternatives and tradeoffs (context, not reopened directions):** TCP plus JSON metadata and raw bytes avoids base64 overhead
  but needs explicit body lengths. JSON plus base64 simplifies one envelope but adds
  roughly one-third byte overhead. Pickle is unsafe for network input and poorly
  interoperable. UDP needs fragmentation/reliability machinery for large values.

**Concrete reasoning/example:** b'\x00\xff' must arrive unchanged. A zero-length value body
  with a present version differs from a reply explicitly reporting no stored version.

**Dependencies:** RPC, storage boundaries, all modes, direct reads, log-query adapter.

**Planned tests / acceptance:** Fragmentation, schema fuzz cases, 1 MiB plus metadata, multiple
  frames, encoding round trips, malformed envelopes, EOF/deadline behavior.

## KD-07 — Ports, connection reuse, and resource limits

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** Separate configurable endpoints; initially one connection per replica RPC; bounded coordinator, replica and background capacity.

**Concept / rationale:** Shared sockets/workers save overhead but need ownership and limits.

**Remaining proposal / verification:** KV port 65434 is proposed, availability unverified; exact endpoint manifest, capacities and overload behavior in C1/C6.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Concurrent requests must not corrupt frames or be
  accidentally serialized globally; finite buffers/work queues and frame caps must
  allow the largest legal value plus metadata. Reject overload explicitly.

**Alternatives and tradeoffs (context, not reopened directions):** One connection/request simplifies isolation but adds connection
  overhead. Persistent connections need one reader, writer coordination, and request
  multiplexing. Separate membership/KV/log services isolate contention but use ports.

**Concrete reasoning/example:** A delayed read must not monopolize the only outbound worker
  or splice its header into another request's payload on a shared connection.

**Dependencies:** Configuration, RPC service, membership, CLI, benchmarks.

**Planned tests / acceptance:** Parallel requests, slow peers, connection/FD/thread counts, queue
  saturation, RSS, and throughput at 1..10 clients. Local example ports are not selected.

## KD-08 — Deadlines, retries, and acknowledgment qualification

**Status: PENDING — C1–C6 foundation approved; remaining compatibility/protocol scope deferred.**

**Krishna-approved direction (October 29, 2026):** One configurable overall monotonic deadline; distinct qualifying replicas; no initial automatic retries; explicit TIMEOUT/ERROR, never infer absence from unavailability.

**Concept / rationale:** An operation has a total time budget. A timeout may mean a write happened but its acknowledgment was lost; it does not mean the key is absent.

**Remaining proposal / verification:** 5s default proposed; exact C5/C6 response matching; protocol-specific acknowledgment rules deferred to KD-11/12.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Count distinct prescribed replica identities only;
  match request, key, mode/generation and phase; require durable write evidence;
  ignore late/unrelated/duplicate replies. Monotonic operation deadlines bound hangs.

**Alternatives and tradeoffs (context, not reopened directions):** Fail once at a deadline is understandable; retries can mask
  transient loss but add load and ambiguous duplicate writes. Reusing a write's tag
  supports idempotence; allocating a new tag silently changes the operation.

**Concrete reasoning/example:** Three copies of VM1's ack still count as one. GET with one
  missing reply out of required R=2 returns TIMEOUT, not NOT_FOUND.

**Dependencies:** Coordinators, RPC, background jobs, CLI completion and fault tests.

**Planned tests / acceptance:** Duplicate/stale replies, deadline exhaustion, delayed ack, retry
  storms, and timeout latency; confirm successful durable writes can outlive timeouts.

**Step 3 proposal — PENDING, awaiting Krishna's approval:** One `operation_id` spans
all phases of one coordinator operation; every target/phase RPC has its own UUID4
`request_id`. Use one absolute five-second monotonic foreground deadline from
admission across queueing, both Strong phases, response validation and nested RPCs.
Do not reset it between phases or count a late reply. An INSTALL that may have been
sent but whose valid response/quorum is absent yields uncertain completion, not a
definite non-write. No retries are proposed.

For Eventual PUT, the five-second deadline bounds waiting for W. Accepted remaining
owner attempts use a distinct ten-second continuation after the client response,
bounded by fifteen seconds from admission; they continue once each and are reported
incomplete at expiry/shutdown. Recommended response policy: only exact-version,
durable INSTALLED/UNCHANGED counts for Eventual W; a strictly higher SUPERSEDED copy
is not an acknowledgment of this value. Strong may count durable strictly-higher
SUPERSEDED as at-least-tag evidence. Examples, status predicates and acceptance
traces are in [STEP3_PROTOCOL_DESIGN.md](STEP3_PROTOCOL_DESIGN.md).

## KD-09 — Persistent storage and durability

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** SQLite WAL with synchronous=FULL; atomic value+version transactions, durable commit before qualifying acknowledgment, preserve disk on kill -9.

**Concept / rationale:** An acknowledgment promises data and its version survive process death. Keeping a Python dictionary or only flushing a language buffer cannot make that promise.

**Remaining proposal / verification:** Exact C3 transactions/schema/connection ownership and filesystem assumptions; durability settings require runtime verification, no storage implemented.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Durable commit before ack; conditional monotonic
  version update; atomic value+metadata; identical tags identify identical values;
  startup recovery; empty-value/absence distinction; no global KV operation lock.

**Alternatives and tradeoffs (context, not reopened directions):** SQLite offers atomic transactions in the standard library
  but its single-writer behavior needs measurement and short transactions. A file per
  key with fsync/temp+rename/directory sync can work but recovery and version CAS are
  easy to get wrong. Building an LSM tree is unjustified.

**Concrete reasoning/example:** Writing bytes and then crashing before writing their tag
  could resurrect stale data. Store both atomically and acknowledge only afterward.

**Dependencies:** Juhi storage, all protocols, generations, direct reads/liststore.

**Planned tests / acceptance:** Temporary-directory crash-boundary tests; stale writes racing;
  equal-tag/different-bytes rejection; restart reads; commit latency under concurrency.

## KD-10 — Version formats, timestamp collisions, and restart ordering

**Status: PENDING — C1–C6 foundation approved; remaining compatibility/protocol scope deferred.**

**Krishna-approved direction (October 29, 2026):** Eventual ordered (timestamp, processID), restart-safe uniqueness; Strong ordered (logicalCounter, writerID); monotonic installs and equal-tag conflict detection.

**Concept / rationale:** A version is an ordered label telling replicas which value wins. Strong and eventual modes have different tag-generation requirements.

**Remaining proposal / verification:** Exact tag encoding, durable allocator and ordering in C3/C4; Strong query-aware allocation remains KD-11.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Total deterministic comparison, immutable tag→value,
  collision-free writer allocation under same-process concurrency/restart. Strong
  writes must choose tags above a queried majority's maximum; a wall clock alone
  does not satisfy this atomic-register condition.

**Alternatives and tradeoffs (context, not reopened directions):** Wall time plus PID alone is simple but may collide/repeat.
  A persisted monotonic timestamp allocator plus unique writer ID preserves the
  required pair. Strong logical counter/writer tags avoid clock assumptions but
  require atomic counter allocation and careful restart handling.

**Concrete reasoning/example:** Two calls observe time=100; they must allocate 100 and 101,
  rather than reuse (100, same_writer) for different values. A crashed process cannot
  restart and reuse an old pair. Mode clearing must still fence old messages (KD-16).

**Dependencies:** Storage contract, ABD, eventual, RPC, repair, logs, mode switching.

**Planned tests / acceptance:** Frozen/backward clocks, concurrent allocations, reboot, very large
  counters, total-order vectors, equal-tag conflicts, and cross-language JSON precision.

**Step 3 allocation proposal — PENDING:** Add a durable Strong logical-counter
high-water mark. `allocate_strong_version(*, context, observed_max)` takes a short
SQLite write transaction, validates the exact Strong context, chooses one greater
than both the observed maximum's counter and persisted local high-water, commits,
then returns `(counter, vmNN/generation)`. It makes no wall-clock assumption and
holds no lock during RPC. Same-writer concurrent calls get different counters;
different writers can share a counter but have distinct totally ordered writer IDs.
The metadata migration must preserve existing records and fail closed on corrupt
state. Whether the allocator survives a later Strong/Eventual epoch switch is
deferred to KD-16. Eventual allocation is already implemented and durable in
`Store.allocate_eventual_version`; Step 3 proposes reusing it, not adding a second
allocator. See the interface and crash/race acceptance cases in the design document.

## KD-11 — Strong multi-writer atomic-register protocol

**Status: PENDING — deferred.**

**Concept:** A read must carry the newest value it sees to a majority before returning. Otherwise one read may see a partly propagated write and a later read miss it, violating the single-copy illusion.

Multi-writer Strong protocol: full two-phase ABD rules, bottom/absence, tag allocation and durable phase acknowledgments await Step 3. No Strong protocol is approved by this handoff.

**Step 3 proposal — PENDING, awaiting Krishna's approval:** use full two-phase
multi-writer ABD on the five fixed owners, with quorum Q=3 in each phase. PUT queries
all five, takes the maximum actual record (or conceptual bottom), durably allocates
a counter above that maximum and local high-water, then conditionally installs the
new tag/value. GET queries a majority, selects the maximum record and durably writes
that exact pair back to a majority before returning it. `None` is the proposed
coordinator-only bottom; it is never stored or sent. Three explicit NOT_FOUND query
replies may return NOT_FOUND without a bottom writeback; fewer than three valid
responses cannot.

The proposed `SUPERSEDED` rule is durable-at-least evidence only when the actual
stored tag is strictly greater than the requested tag, and all response/context/
owner/generation bindings validate. That rule can count for Strong phase 2 because
the higher committed value orders after the attempted tag; `VERSION_CONFLICT` never
counts. Two writers at the same observed max get distinct ordered tags by writer ID;
same-writer allocation is serialized by a short durable counter transaction. If
three owners do not provide required evidence before the single operation deadline,
return TIMEOUT/ERROR. If a write phase may have committed, report uncertain
completion. The rationale, traces, assumptions and adversarial tests are specified
in [STEP3_PROTOCOL_DESIGN.md](STEP3_PROTOCOL_DESIGN.md). No protocol behavior is
implemented or approved yet.

**Approval question:** Is this ABD procedure, bottom rule, allocation contract,
phase naming and Strong-only `SUPERSEDED` qualification the chosen KD-11 design?
Recommended default: yes, subject to reviewing the detailed traces and tests.

**Approval record:** No engineering approval recorded.

**Correctness conditions:** Fixed five owners; majority=3 in both phases; durable
  monotonic replica tags; unique ordered writer tags above phase-1 max; phase/replica
  deduplication. GET queries a majority, chooses max, writes that pair back durably
  to a majority, then returns. PUT queries a majority then installs a greater tag
  on a majority. Replica compare-and-store is atomic per key. Initial absence needs
  a distinguished bottom tag/value and a correct read/writeback rule. No clock or
  detector accuracy assumptions replace these conditions.

**Alternatives and tradeoffs (context, not reopened directions):** Two-phase multi-writer ABD fits the required register model
  but incurs two round trips and read writes. A single-phase Cassandra quorum read
  is insufficient. Consensus protocols add scope without an approved concrete need.

**Concrete reasoning/example:** PUT reaches one replica then its writer crashes. GET1 sees
  it and writes it to three before returning; GET2's majority must intersect those
  three and cannot return an older version. Concurrent overlapping writes receive
  different tags even when their phase-1 maxima match.

**Dependencies:** Krishna Strong coordinator; KD-03/06/08/09/10/14/16.

**Planned tests / acceptance:** History-based linearizability checks, concurrent same-key writers,
  read-after-partial-write, out-of-order installs, partitions, phase crashes, durable
  replica restarts, lost acks and timeouts. A sequential happy path is insufficient.

## KD-12 — Eventual propagation and response qualification

**Status: PENDING — deferred.**

**Concept:** R/W choose how many replies a client waits for, not how many owners exist. Low thresholds trade freshness for availability and latency.

Eventual fanout/first-R/durable-W requirements are mandated; exact coordinator qualification and lifetime rules await Step 3.

**Approval record:** No engineering approval recorded.

**Correctness conditions:** R/W each 1..5; distinct qualifying replicas; write
  acks durable; max of first R qualifying reads; absent response is explicit and
  differs from timeout. Background propagation continues after W without holding
  a global lock. Replica conditional writes never decrease versions.

**Alternatives and tradeoffs (context, not reopened directions):** Per-request background futures are simple but need bounded
  resources and lifetime. A bounded queue provides explicit backpressure but must
  not silently drop acknowledged requests' remaining fanout. Pure sequential sends
  delay progress and are a poor fit for ten concurrent clients. Client completion must not end remaining propagation: use the separately bounded C6 continuation policy, not the client wait deadline.

**Concrete reasoning/example:** At W=1, return after VM1 durably stores the tag while VM2..5
  sends continue. A GET from an empty reachable replica at R=1 can return NOT_FOUND
  even if another owner has a value; a no-response GET cannot.

**Dependencies:** Juhi eventual coordinator, RPC/storage/versions, KD-07/08/14.

**Planned tests / acceptance:** R/W combinations, one replica replying repeatedly, post-ack fanout,
  partitions, stale replies, empty bytes, and timeout-vs-absence at multiple R values.

**Step 3 proposal — PENDING, awaiting Krishna's approval:** keep R/W in 1..5 and
default 2/2. GET concurrently queries all five, returns after the first R distinct
valid replies, and chooses the maximum version among those R; all R explicit
NOT_FOUND replies mean NOT_FOUND, while fewer than R replies mean TIMEOUT/ERROR.
No comparison or repair follows an Eventual-noRR response. Equal version with
different bytes fails closed.

PUT durably allocates the existing Eventual `(timestamp, vmNN/generation)` version,
atomically reserves all five owner-attempt slots, and dispatches one write to every
fixed owner. Count W only when distinct replicas report durable INSTALLED or
UNCHANGED at the exact requested version. Proposed policy excludes SUPERSEDED
because it confirms a newer value rather than this PUT's value. If capacity cannot
reserve all five attempts, fail OVERLOADED before sending. If W is reached before
the five-second foreground deadline, respond success but retain every remaining
accepted attempt for a separate ten-second continuation, capped at fifteen seconds
from admission. One attempt per owner, no retries, and explicit incomplete-propagation
records at expiry. Foreground timeout after a possible commit reports unknown
completion; it is never NOT_FOUND. A crash may lose in-memory attempts but not
committed SQLite data. These settings reuse the approved C6 starting envelope;
protocol-specific qualification and reservation details still require KD-12 review.

**Approval question:** Approve exact-version W acknowledgments, all-five reservation,
first-R read, and bounded post-ack continuation as specified? Recommended default:
yes; this avoids false acknowledgments and dropped accepted fanout. Full examples,
R/W failure matrix and tests are in [STEP3_PROTOCOL_DESIGN.md](STEP3_PROTOCOL_DESIGN.md).

## KD-13 — Read repair and background execution

**Status: PENDING — deferred.**

**Concept:** An eventual GET can repair the copies it discovers to be behind, while keeping the client's first R-reply latency separate from repair completion.

Read repair requirements are mandated; collection/repair scheduling and background lifetime await Step 4.

**Approval record:** No engineering approval recorded.

**Correctness conditions:** Return max from first R; continue comparing all
  reachable prescribed copies; identify highest discovered version and repair stale
  or absent copies; conditional installs cannot overwrite a concurrent higher PUT.
  Log every response, returned version, reachable set, and repair action/result.

**Alternatives and tradeoffs (context, not reopened directions):** Waiting for all terminal reply/timeout outcomes then repairing
  is easy to reason about but delays repairs. Incremental repair is quicker but must
  repeat when a later higher tag appears. Neither can wait indefinitely for a partition.

**Concrete reasoning/example:** First reply is v2, so R=1 returns v2; later replies v7 and
  absent require repair to v7. If a target already has v8 it retains v8.

**Dependencies:** Juhi Eventual-RR, KD-07/08/09/10/12/14/16/17.

**Planned tests / acceptance:** Slow highest replica, absent replica, racing PUT during repair,
  return-before-repair ordering, bounded partition completion, required log evidence.

## KD-14 — Concurrency, workers, and CLI client semantics

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** Bounded thread pools with separated responsibilities where necessary; short storage transactions, no global application lock around distributed operations.

**Concept / rationale:** Shared data needs small atomic critical sections, while independent requests should continue in parallel. Network waiting must not hold a global lock.

**Remaining proposal / verification:** Exact capacities, admission, queue/deadline/shutdown details in C6; SQLite physical writer serialization must be measured.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Atomic per-key version compare/store and writer tag
  allocation; no global operation lock; finite worker/queue limits; no worker
  starvation where all coordinators wait for replica work in the same exhausted pool.

**Alternatives and tradeoffs (context, not reopened directions):** Threads fit existing code but need explicit pool ownership
  and lock order. Async IO scales connections but adds a different programming model.
  Per-key locks are clear but need lifecycle management; stripes bound memory with
  occasional unrelated-key contention. A database may serialize physical writes
  without justifying a global application lock around whole operations.

**Concrete reasoning/example:** A slow GET on key A must not block a PUT on B. Two PUTs on A
  may overlap in network phases but cannot reuse a tag or store a lower version.

**Dependencies:** All services, storage, CLI, benchmarks; initial parser is syntax only.

**Planned tests / acceptance:** Barrier-controlled same-key races, unrelated-key progress, worker
  exhaustion/deadlock, shutdown, and throughput/latency across client counts.

**Step 3 proposal — PENDING, awaiting Krishna's approval:** retain C6 limits of
coordinator 16 active/32 queued, replica handlers 32/64, and bounded outbound
replica-attempt/background work 16/64, with separate membership capacity. Atomic
reservation is required before Eventual PUT dispatch, so a successful W response
cannot discard queued fanout. Strong operations reserve enough bounded attempt
capacity for both phases before starting writes. Queue exhaustion is explicit
OVERLOADED; it must not spawn extra threads. Coordinators may wait on bounded RPC
workers, but workers never wait on coordinator tasks. Same-key distributed operations
overlap; only SQLite's short transaction and Strong's short durable allocator
transaction serialize. No global/membership/key lock crosses network IO. Exact
reservation bookkeeping and shutdown truncation/reporting are proposed for review,
not implemented. See the resource ownership table and deadlock tests in the design.

## KD-15 — Shared storage/RPC interface contract

**Status: APPROVED — C1–C6 foundation contract; implementation/verification tracked separately.**

**Krishna-approved direction (October 29, 2026):** Small concrete contracts: explicit presence/bytes/versions/errors, atomic conditional writes and epoch validation at storage boundary.

**Concept / rationale:** Krishna's protocols and Juhi's storage must agree on what a response promises; returning from a function is not automatically a durable acknowledgment.

**Remaining proposal / verification:** All proposed C1–C6 signatures and result semantics are approved for the scoped foundation.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Atomic snapshot reads; monotonic durable installs;
  same-version/different-value detection; explicit absence and errors; version
  comparisons consistent across storage/coordinators. Membership liveness is not data.

**Alternatives and tradeoffs (context, not reopened directions):** A small concrete API is easy to review but changes need
  coordination. Large generic protocol/adapter hierarchies add maintenance without
  assignment value. Returning stored-version metadata gives stronger evidence than bools.

**Concrete reasoning/example:** Installing v3 when v5 exists must preserve v5 and return
  evidence of that state, rather than reporting a fictitious v3 replacement.

**Dependencies:** Both owners' modules; KD-06/09/10/11/12/16.

**Planned tests / acceptance:** Shared contract tests against a disposable store, version ordering,
  concurrent installs, disk errors, and serialization round trips.

**Step 3 protocol extension proposal — PENDING:** Preserve the C5 binary frame and
limits. Add stable `operation_id` alongside per-attempt UUID4 `request_id`; introduce
schema 2 for protocol phases while retaining schema 1 only for the existing
`phase="local"` path. Do not auto-downgrade. Reuse `READ_LOCAL` and
`INSTALL_IF_NEWER`; add `strong_query`, `strong_write`, `strong_readback`,
`eventual_read`, and `eventual_put`. Schema-2 responses keep READ `version` as the
actual record and INSTALL `stored_version` as the committed replica value. A
protocol-specific predicate—not the raw status name—decides quorum qualification.
Strong and Eventual use separate `SUPERSEDED` rules. This is an exact proposed
envelope change, and all affected validators and mixed-version behavior are detailed
in [STEP3_PROTOCOL_DESIGN.md](STEP3_PROTOCOL_DESIGN.md).

## KD-16 — Mode changes and stale-message fencing

**Status: PENDING — C1–C6 foundation approved; remaining compatibility/protocol scope deferred.**

**Krishna-approved direction (October 29, 2026):** PARTIAL: durable monotonically increasing mode epochs, epoch-bearing RPCs, atomic storage validation rejecting old-mode requests/background work.

**Concept / rationale:** Clearing disk is insufficient if an old delayed request can put the old data back. A mode generation lets replicas recognize requests from before a switch.

**Remaining proposal / verification:** Exact epoch boundary in C3/C5 remains proposed. Full cluster-wide transition, clearing, partition/offline admission and rollback protocol remain deferred to Step 4.

**Approval record:** Krishna approved the applicable C1–C6 exact foundation details and defaults in the Batch 1 instruction (October 29). Deferred protocol decisions remain outside that approval.

**Correctness conditions:** Strong↔Eventual clears values AND version metadata;
  Eventual↔Eventual may preserve them; membership and fixed ring IDs survive.
  Old RPC/repair work must not repopulate cleared state. Partial switches/partitions
  must not mix incomparable consistency generations or claim a successful full switch.

**Alternatives and tradeoffs (context, not reopened directions):** Coordinated stop/quiesce/clear/restart is easier operationally
  but still needs persistent generation fencing for delayed messages/rejoining peers.
  Online cluster-wide epoch transition improves usability but adds a control protocol
  and difficult failure handling. Independent local clears are not sufficient.

**Concrete reasoning/example:** A repair for eventual generation 4 arrives after a Strong
  generation 5 clear; it must be rejected even if it carries the largest old timestamp.

**Dependencies:** Joint CLI/mode manager, all RPC/storage/background jobs and restarts.

**Planned tests / acceptance:** Delayed old writes/repairs, killed switch coordinator, partitioned
  replica restart, Strong↔Eventual clearing, Eventual↔Eventual retention, ID preservation.

## KD-17 — Logs, MP1 querying, and monitoring

**Status: PENDING — deferred.**

**Concept:** Logs record actual replica receipt/response events and explain which versions a client returned or repaired. They must be queryable across machines.

Required terminal text and operation/RR logs and MP1 querying are mandated; final schema, paths, adapter and overhead choices await Step 4.

**Approval record:** No engineering approval recorded.

**Correctness conditions:** Print/log GET/PUT receipt AND response; correlate
  requests/replicas/versions/status; RR logs include returned version, replies,
  reachable copies and actions. No fabricated success events or value-byte dumps.

**Alternatives and tradeoffs (context, not reopened directions):** JSON lines are parseable but verbose; text is concise but
  fragile to parse. Synchronous handlers simplify ordering but cost latency;
  buffered logging needs flush/loss accounting. MP1 path adaptation avoids copying
  its whole service; importing its current server directly starts a listener.

**Concrete reasoning/example:** `grep -c PUT_RECEIVED machine.4.log` should find MP3 events;
  a client timing out is not logged as a successful durable write by the coordinator.

**Dependencies:** All replica handlers, CLI completion, RR, benchmarks/report evidence.

**Planned tests / acceptance:** Parse escaped newlines/Unicode, append behavior, console+file,
  local end-to-end MP1 querying, required fields, and logging overhead. Only framing
  interoperability and formatter behavior have been verified so far.

## KD-18 — Benchmark orchestration and workload design

**Status: PENDING — deferred.**

**Concept:** Repeatable seeded workloads and real measurements show the cost of consistency. A unit test or made-up chart cannot substitute for a VM experiment.

Healthy cluster, sequential per-client operations, both >=180s durations, all-replica preload, matched seeds and required plots are fixed. Harness/warmup/error accounting and run orchestration await Step 6.

**Approval record:** No engineering approval recorded.

**Alternatives and tradeoffs (context, not reopened directions):** The spec fixes one operation at a time per client (closed loop). Engineering alternatives concern warmup, trial scheduling and raw-event retention: more detailed logs improve auditability but add overhead. Both experiments require >=3 minutes per client; duration and client model are not open choices.

**Concrete reasoning/example:** 1,000 successful operations/10s is 100 ops/s, not 120 because
  200 timed-out attempts were also issued. Keep raw attempts so failures stay visible.

**Dependencies:** Joint harness/report; KD-07/08/14/17; all modes must pass correctness.

**Planned tests / acceptance:** Workload mix/seed determinism, preload/readback, timestamp math,
  raw aggregation and missing-trial checks. See master plan for matrix and time budget.

