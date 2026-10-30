# MP3 implementation plan — shared by Krishna, Juhi, Codex and Claude Code

Prepared October 22, 2026; roadmap reconciled with the local October 21 specification.
**Current gate: Step 2 Batches 1–4 local foundation is VERIFIED and independently
reviewed on the Batch 4 feature branch. This is not ten-VM deployment verification.
Coordinator protocols remain pending.** Read AGENTS.md and
KEY_DECISIONS.md before work. Krishna's
C1–C6 contract approval and accepted KD directions are recorded there. The scope
completed so far is local storage/versioning, fixed ring/membership/recovery,
single-replica KV RPC, and their integrated local node runtime; it does not include distributed consistency protocols. At bootstrap the
snapshot lacked Git metadata, MP3, agent instructions and the specification; these
now exist, and `mp3/SPEC.md` is authoritative.
MP1/MP2 remain read-only references. No university VM was contacted and no dependency
was installed. This roadmap records the current gates; it does not authorize future
protocol batches or VM operations on its own.

## Objective, authority, and boundaries

Build this assignment's fixed-ten-node replicated key-value store with N=5,
Strong and two Eventual configurations, durable restart behavior, useful demo
commands and a measured report. Do not build Cassandra, Chord stabilization, a
storage engine, distributed transactions, hinted handoff, or a platform.

The authoritative October 21 text is [SPEC.md](SPEC.md) in this directory; use that
existing file without moving or recreating it. It
overrides the operational summary and historical uncertainty notes in Phase 0 docs.
This plan now includes its command/output, benchmark and submission requirements.
AI assistance is permitted; it is not an approval blocker. Further Piazza
submission/demo notices still need checking.
Timeline: release Oct 13; update Oct 21; hard deadline Nov 8,
2026 at 11:59 PM; demo Nov 9. Confirm deadline timezone in course instructions.
Evaluation: demo 60%, report 30%, code quality 10%.

## Chronological execution roadmap — six major steps

Follow the steps in order; use the detailed checklist, audit and task table below
for specifics. Approvals are entry gates before implementation, not extra phases.
Step 2 Batches 1–4 are complete for the local foundation; ten-VM validation, multi-owner
coordinator integration, and Steps 3–6 remain future work. Task statuses below reflect verified local work.
KD direction approvals and C1–C6 contract approvals remain recorded; deferred protocol
decisions remain pending.

**Current approval authority:** Krishna alone approves the exact handoff. Joint
ownership/coordination below is retained, not an additional approval requirement.
All C1–C6 foundation schemas/signatures are approved; deferred protocol details remain pending.

**Mandated versus chosen:** Ten VMs, N=5, fixed owners, MP1/MP2 reuse, durable
recovery, concurrent operations without global locks, the three consistency guarantees,
mode-clear semantics, command/output requirements and experiments come from SPEC.md.
Humans approve their implementation choices under the KDs. A multi-writer atomic
register is required; two-phase ABD remains proposed under KD-11. Infinite-suspicion recovery and
SHA-256 are Krishna-approved directions; exact detector and hashing contracts below
are approved as C1/C2 contracts; their implementations remain future work.

### Step 1 — Phase 0: inspect, scaffold and document (complete)

- **Work / owners:** Krishna completed the reuse audit, local utilities/tests and
  shared docs; an independent reviewer checked them. Juhi's future ownership is
  recorded. Tasks P0-01..07; MP1/MP2 remain unchanged.
- **Approval first:** None needed for these limited prototypes. **Can wait:** all
  KD-01..18 are still PENDING; prototype tests approve no final architecture.
- **Exit evidence:** Prior 34-test pass, smoke/compile and review records in
  VERIFICATION.md / REVIEW.md; every distributed command remains NOT_IMPLEMENTED.

### Step 2 — Build the common foundation against agreed shared contracts

- **Work / owners:** The shared C1–C6 contracts were approved by Krishna. Juhi's
  Batch 1 local storage/version foundation and Krishna's Batch 2 hash/ring and
  membership/recovery foundation are independently implemented and locally verified.
  Krishna's bounded KV RPC replica service/client (N-01) is now implemented and
  locally reviewed. Batch 4's joint `NodeRuntime` now starts and stops those foundations
  in order, without implementing a multi-owner coordinator. Ownership is unchanged.
  **Next:** obtain the deferred Step 3 protocol decisions, then Krishna and Juhi can
  build their separately owned coordinators against this integrated local foundation.
- **Decisions already approved for these foundations:** KD-02/03/04/05/06/07/09/14/15
  and the applicable C1–C6 contracts. KD-01's VM runtime compatibility, KD-08's
  protocol-specific qualifications, KD-10's Strong allocation and KD-16's full
  cluster switching remain unresolved or deferred as recorded in KEY_DECISIONS.md.
- **Can wait:** KD-11/12 until Step 3; KD-13, full KD-16 and final KD-17 until
  Step 4; KD-18 until Step 6. Do not implement ABD, Eventual protocols, repair,
  mode switching or command integration as part of this step update.
- **Verified local evidence:** Batch 1 store/version, Batch 2 ring/membership, Batch 3
  TCP replica read/install, and Batch 4 lifecycle integration tests are listed below.
  Three logical VMs communicated over real loopback sockets; a disposable subprocess
  was SIGKILLed and restarted with its value and stable identity preserved. Ten-VM
  operation, multi-owner coordination, cross-VM consistency and real-cluster convergence
  remain unverified.

### Step 3 — Build Strong and Eventual-noRR in parallel

- **Work / owners:** After Step 2 integration, Krishna implements Strong (proposed
  multi-writer ABD, A-01); Juhi implements noRR (E-01) and completes protocol-specific
  V-01 support. Use shared interfaces; test each mode separately before switching.
- **Approve first:** KD-11/12 plus their KD-08/10/14/15 phase-specific response,
  tag-allocation and contract refinements. Earlier approvals carry forward.
- **Can wait:** KD-13, remaining KD-16/17 until Step 4; KD-18 until Step 6.
  Basic replica receipt/response terminal text is already mandated; emit it now.
- **Exit evidence:** Strong passes concurrent-history, partial-write/read-writeback
  and phase-crash/delay tests. noRR proves distinct durable W acks, first-R maximum
  and continued all-reachable-owner fanout. Both distinguish absence from timeout
  and empty data, preserve version order and avoid global operation serialization.

### Step 4 — Integrate repair, switching, commands and MP1 logs

- **Work / owners:** Juhi builds RR after noRR (E-02); Krishna integrates ring/
  membership diagnostics and MP1 log-query reuse. Jointly finish all eight commands,
  actual file IO, simultaneous multiput, required logs/completion output and mode
  switching (C-01/C-02/L-01). RR and CLI/log work can run **in parallel after shared
  mode/log contracts**; final switch tests require all three integrated modes.
- **Approve first:** KD-13, remaining KD-16, KD-17 and remaining KD-14 CLI details;
  obtain Krishna approval of any required KD-06/08/15 interface extensions before using them.
- **Can wait:** KD-18 until Step 6; optional monitoring and connection optimizations.
- **Exit evidence:** All command/output assertions and MP1 queries pass locally;
  RR returns after R then repairs to the reachable maximum without overwriting newer
  writes; direct replica GET never repairs. Switch tests prove mandated clearing,
  approved Eventual-to-Eventual behavior, unchanged IDs and stale-message fencing.

### Step 5 — Deploy on ten VMs and pass integrated correctness tests

- **Work / owners:** With explicit remote authorization, Krishna validates membership/
  ring/RPC/Strong; Juhi validates disk/versions/Eventual/RR. Jointly build persistence-
  safe deployment/restart scripts and run full commands, long histories, kill -9,
  partitions/delays and mode-failure tests (F-01; deployment part of D-02). Component
  checks can be parallel; coordinate shared-cluster switches and fault schedules.
- **Approve first:** Relevant KD-01..17 portions from Steps 2–4; remote/fault-test
  permission is separate. No new KD is inherently required; review design revisions
  together if evidence demands them.
- **Can wait:** KD-18 until Step 6; tuning until baseline evidence exists.
- **Exit evidence:** Real ten-VM command/log-query and concurrency/failure/recovery
  results pass, every VM acts as client/server, and a healthy cluster is restored
  for measurement. Keep real-cluster evidence distinct from local simulations.

### Step 6 — Benchmark, report, submit and rehearse the demo

- **Work / owners:** Krishna leads Strong/noRR scaling; Juhi leads noRR/RR hotspots
  (B-01/B-02). Jointly agree the harness/seeds, audit statistics, produce the report,
  submission and demo/runbook (D-01/D-02). Analysis/writing can be **parallel**;
  schedule shared-cluster trials so they cannot interfere.
- **Approve first:** KD-18's remaining harness/warmup/error-accounting/run-management
  choices. Client model, durations, preload, seeds, measurement definitions and plot
  forms are already fixed by the spec; retain prior correctness approvals.
- **Can wait:** Optional confidence intervals and extra plots, not required results.
  Reserve measurement/rehearsal time and check demo signup/Piazza dates in advance.
- **Exit evidence:** Detailed benchmark matrix below has >=3 real readings per point,
  required plots/averages/standard deviations and reproducible raw trials; <3-page
  12-point report, submission receipts and full demo rehearsal are complete by the
  required dates. No results are inferred from unit tests or invented.

**Current task gate:** Batches 1–4 provide the locally reviewed storage/version,
ring/membership, single-replica RPC, and integrated local node lifecycle foundations.
The shared foundation can start one node and perform one remote replica read/install;
multi-owner protocols and coordinator integration remain future work. The accepted KD directions must not be reopened
without a concrete specification conflict or serious correctness issue. Defer KD-11/12
to Step 3, KD-13/full KD-16/final KD-17 to Step 4, and KD-18 to Step 6.

## Requirement checklist and traceability

`[x]` means the listed behavior has a verified local implementation at this plan
revision; `[ ]` means future work, even if a guard or interface exists. Local tests
do not imply ten-VM verification. The batch evidence section records exact commands.

| Check | Requirement / boundary | Task | Acceptance evidence required |
|---|---|---|---|
| [ ] | Ten VMs; each acts as client/server | M-01, N-01, C-01 | Requests originated/served on every VM; Batch 4 is loopback only |
| [x] | N=5; deterministic consistent hashing | R-01 | SHA-256 vectors plus cross-process, input-order and five-owner tests; no real cluster yet |
| [x] | Select five distinct successors from supplied IDs, including wraparound | R-01 | `RingTests` and `RingMembershipTests`; exact equality is inclusive for Manifest.owners |
| [x] | First five owners fixed after setup; no all-key replica map | R-01 | Failed/suspected status leaves local prescribed owners unchanged |
| [x] | Route key IDs to fixed manifest nodes with bounded ten-node work | R-01 | `Manifest.owners`; no stored per-key mapping; CLI/RPC routing is future work |
| [x] | MP2 membership concepts adapted for MP3 failure detection | M-01 | Fake-clock suspicion, local TCP gossip and generation recovery; ten-VM convergence not tested |
| [x] | Failed nodes retained; same ring ID on local restart | M-01 | Local repeated SIGKILL/restart retains Store data and ring manifest position |
| [ ] | PUT replaces; GET latest appropriate value or NOT_FOUND | S-01, A-01, E-01 | Per-mode histories, absent/empty/racing cases |
| [x] | RPC framing accepts arbitrary bytes and a 1 MiB payload, including empty | N-01 | Codec and loopback replica tests; no ten-VM claim |
| [x] | Local result distinguishes OK empty bytes from NOT_FOUND | P0-03 | `DomainTests`; no distributed read claim |
| [x] | RPC/storage preserve full 1 MiB value plus metadata | N-01, S-01 | Loopback vm01→vm04 SQLite install/readback with arbitrary bytes |
| [ ] | Concurrent same-key operations; no global operation lock | S-01, A-01, E-01 | Barrier races; unrelated-key progress |
| [ ] | Strong per-key multi-writer linearizability; majority 3 | A-01 | Query/install phases, GET writeback; checked concurrent histories |
| [ ] | Strong handles delays, partitions, phase crashes and restart | F-01 | Adversarial schedules + restart durability |
| [x] | Eventual R/W validation 1..5; constants default R=W=2 | P0-03 | `ConfigTests`; no active cluster configuration |
| [ ] | Eventual PUT all reachable owners, W durable distinct acks, continue fanout | E-01 | Delayed post-ack propagation trace |
| [ ] | Eventual GET max of R distinct qualifying replies | E-01 | Duplicate/stale/unavailable/absent replies |
| [ ] | Unique timestamp/process-ID eventual versions | V-01 | Same-clock concurrency, rollback, restart uniqueness |
| [ ] | noRR has no required background synchronization/hinted handoff | E-01 | Only required per-PUT fanout, no invented sync service |
| [ ] | RR returns after R, then compares all reachable replies | E-02 | Client completion precedes late higher reply |
| [ ] | RR finds max, repairs stale/missing reachable copies, logs evidence | E-02, L-01 | Higher-late-version and racing-write repair traces |
| [ ] | Values AND version metadata durable before every qualifying write ack | S-01 | kill -9 at commit/ack boundaries |
| [x] | Local process restart preserves disk contents | S-01, I-02 | Disposable SIGKILL/restart reads committed bytes and version; no VM claim |
| [x] | Recover local durable state before serving; local storage libraries allowed, distributed replication/quorum/repair libraries forbidden | S-01, I-02 | Explicit recovery and generation-before-listener startup tests; distributed recovery remains untested |
| [ ] | Strong↔Eventual clears KV+versions; Eventual↔Eventual may preserve | C-02 | Switch matrix incl. offline/delayed request cases |
| [ ] | Membership/ring IDs survive all mode switches | C-02, M-01 | Before/after manifest/ID comparison |
| [ ] | Single-replica RPC request bounded; failures never become NOT_FOUND | N-01 | Unreachable, slow-fragment and expired-deadline tests; quorum-level error policy remains A-01/E-01 |
| [ ] | Delayed/concurrent writes cannot decrease version ordering | S-01, F-01 | Atomic compare/install races and old-generation traffic |
| [x] | All eight required command syntaxes validated; unfinished calls guarded | P0-04 | `CliTests`; every distributed command NOT_IMPLEMENTED |
| [ ] | Operational put/get/ls/liststore/getfromreplica/list_mem_ids/multiput/mode | C-01, C-02 | Demo transcript incl. file IO and spec completion text |
| [ ] | PUT prints status/keyID/acknowledging and replicated VMs; GET/direct GET print status/version; direct GET performs no repair | C-01 | Command transcript; unchanged replicas after direct read |
| [ ] | `ls` prescribed owners rather than confirmed data locations | R-01, C-01 | Failed/empty replicas still listed in owner set |
| [ ] | ls prints keyID + owner addresses/ring IDs; liststore prints local keys/keyIDs + self ring ID; list_mem_ids sorted by nodeID | R-01, M-01, C-01 | Exact diagnostic output assertions |
| [ ] | multiput originates simultaneous same-key PUTs on selected VMs, each using its own arbitrary local random-byte file | C-01 | Overlapping invocation trace from selected clients |
| [ ] | Replicas log/print GET/PUT receipt and response | L-01 | File+console operation transcript |
| [ ] | Replica terminal format `<local timestamp>: <received\|responded><get\|put> <key>`; initiating clients print completion when DKVS completes | L-01, C-01 | Timestamped receipt/response/completion transcript |
| [x] | Opt-in parseable JSON log formatter, append file+console | P0-03 | `LogTests`; illustrative test events only |
| [ ] | MP1 distributed querying of MP3 logs | L-01 | Temporary local integration then approved ten-VM query |
| [ ] | Experiment 1 full matrix and real workloads | B-01 | Raw trial artifacts, seeds, durations, failures |
| [ ] | Experiment 2 hotspot/noRR-vs-RR comparison | B-02 | Hot-key distribution and repair/staleness evidence |
| [ ] | >=3 readings/config, means, standard deviations, repeatable seeds | B-01, B-02 | Recompute figures from retained raw trials |
| [ ] | Both experiments healthy; one operation/client at a time; >=3 minutes/client; preload all replicas; same seeds across variants | B-01, B-02 | Harness assertions and retained run manifests |
| [ ] | Experiment 1 throughput-vs-mean-latency curves (3 configurations, labeled client counts); Experiment 2 separate throughput/latency bars | B-01, B-02 | Required axes/series/averages/standard deviations verified |
| [ ] | Report under three pages, 12-point, design/measurements/plots/analysis | D-01 | Rendered report plus data provenance |
| [ ] | Deadline, submission and Nov 9 demo readiness | D-01, D-02 | Official-spec checklist and dry-run transcript |

## Proposed modules and dependencies

The existing package is deliberately small. Future names in italics below are
**proposed responsibilities, not empty files or approved interfaces**.

| Actual / proposed module | Responsibility | Owner | Depends on |
|---|---|---|---|
| `config.py` + `config/local.inventory` | Fixed constants; explicit local inventory; quorum validation | Joint scaffold | KD-01/02/07 for final config |
| `models.py` | Local command/result/status types; no version or RPC schema yet | Joint scaffold | KD-06/10/15 |
| `framing.py` | Bounded raw bytes and partial-read handling | Krishna scaffold | Caller caps/deadlines; KD-06/07 |
| `ring.py` | Pure supplied-ID successors; fixed-size ownership | Krishna | KD-02/03 for construction |
| `eventlog.py` | Explicit JSON-lines handler setup | Joint scaffold | KD-17 for final fields/path |
| `cli.py`, `pending.py`, `__main__.py` | Validation, smoke, guard | Joint | Final dispatch blocked |
| `legacy_membership.py` | Exact MP2 state module, quarantined baseline | Krishna | KD-03/04 before adaptation |
| *membership adapter/node runtime* | Stable IDs/generations; gossip and recovery | Krishna | KD-03/04/05/07/14 |
| *RPC coordinator/replica service* | Bounded concurrent transport, validated envelopes | Krishna | KD-06/07/08/14/15 |
| *persistent store* | Atomic durable monotonic versioned values | Juhi | KD-09/10/15/16 |
| *version allocator/comparator* | Eventual timestamps and Strong tag support | Juhi, Krishna Strong caller | KD-03/10/11/15 |
| *Strong coordinator* | Multi-writer atomic-register phases | Krishna | Routing, RPC, storage, versions |
| *Eventual coordinator/repair* | R/W fanout, background propagation, repair | Juhi | Same contracts; KD-12/13 |
| *mode manager/CLI integration* | Safe epoch transitions, actual commands/files | Joint | KD-14/16; integrated modes |
| *log-query adapter* | MP1-compatible queries with explicit MP3 log paths | Joint | KD-17 |
| *benchmark harness/report tooling* | Seeded workloads/raw trials/statistics/plots | Joint | Correctness gate; KD-18 |

Dependencies: inventory + approved fixed identities → ring; membership reports
liveness separately → routing/RPC attempts; codecs + storage/version contract →
replica handlers; routing/RPC/replicas → Strong and Eventual coordinators; all three
modes + fences → CLI/mode switching; correctness + observability → benchmarks.
Membership never supplies a changing ring of only live nodes. `ls` consumes fixed
owners; `liststore` consumes actual local storage.

## Interface contracts

### Implemented, local only

- `parse_inventory(text)` / `load_inventory(path)` return immutable label-sorted
  `Machine(node_id, host, port)` tuples, reject duplicates/invalid ports, contact no
  hosts. Label is not a process ID or ring position. No production manifest is frozen.
- `select_successors(nodes, key_id, count=5, *, include_equal=...)` accepts unique
  `RingNode(node_id, position)` values with already-computed nonnegative integers.
  Returns clockwise distinct owners including wraparound, independent of input order.
  Fails on insufficient nodes/collisions; no hash, virtual nodes or live filtering.
- Legacy `send_frame/recv_frame(..., max_bytes=...)` exchange length-prefixed raw
  bytes and remain compatible with MP1/MP2 framing. New `*_deadline` helpers enforce
  a total monotonic IO deadline. `rpc.py` uses the helpers with the C5 JSON/raw-byte
  envelope and exact connection closure; this is not the MP1 query protocol.
- `Command(name, args)` and `OperationResult(status, message, value)` are local types,
  not final wire responses. Only OK can carry bytes. Empty bytes are not missing data.
- `parse_command(line)` returns a normalized validated Command. `pending.execute`
  always returns NOT_IMPLEMENTED; no file/network/storage/mode side effects.
- `configure_logging` requires an unused caller-owned logger, existing parent path,
  and console stream. It appends; it is not storage durability or complete KV tracing.

### Step 2 shared contracts — APPROVED BY KRISHNA (Batch 1 scope only)

Approved October 29. C1–C6 below are exact contracts; only Batch 1 local APIs are implemented
or additional modules. Krishna owns membership/routing/RPC; Juhi owns storage/version
management; shared integration remains joint. These extend the existing responsibility
boundaries without changing ownership. Krishna has now explicitly approved these exact foundation contracts; this does not authorize later batches.
Historical Phase 0 verification remains evidence only for the existing scaffold.

#### C1. Identity and ring (Krishna)

APPROVED BY KRISHNA — C1–C6 foundation only.

`Endpoint = (host: str, port: int)`; `Node = (vm_id: str, ring_id: int,
kv: Endpoint, membership: Endpoint, log_query: Endpoint)` in an immutable ten-node
manifest. IDs are exactly `vm01`..`vm10`; addresses/ports are explicit manifest data,
never inferred from a process ID. Proposed KV port 65434, membership/log ports
65433/65432; verify availability later, no VM access now. Every RPC carries the same
manifest fingerprint: SHA-256 of canonical JSON manifest bytes (UTF-8, sorted object
keys, compact separators, nodes sorted by vm_id, no extra fields).

Hash input: node `b"mp3/node/" + vm_id.encode("utf-8")`; key
`b"mp3/key/" + key.encode("utf-8", errors="strict")`. No Unicode normalization,
case folding or whitespace rewriting. The spec sets a 1 MiB value limit but no explicit key-length, nonempty-key or
NUL prohibition. Recommended engineering proposal: accept any strictly UTF-8
encodable Python string, including empty and NUL-containing keys, at storage/RPC
boundaries; reject invalid Unicode rather than normalize it. Remove the separate
4096-byte limit. The existing proposed 16 KiB encoded metadata cap remains a bounded-
resource limit requiring Krishna approval; reject over-cap messages explicitly,
never truncate keys. JSON escapes NUL safely; parameterized SQLite statements must
store keys as data. Shell argv cannot carry NUL, and the current scaffold parser
rejects empty/NUL arguments: these are existing CLI limitations, not course-wide
key restrictions. A future approved CLI adapter must handle quoted empty keys;
NUL keys can be tested through the local/RPC API. Keys with newlines/control bytes
need escaped display/logging, not raw terminal interpolation; final formatting is
KD-17 work. These are proposed engineering behaviors, not additional spec mandates. Ring ID is all 256 digest bits interpreted
unsigned big-endian; display as 64 lowercase hexadecimal digits. Sort ascending;
first owner has position >= key ID, wrapping to smallest. Return that owner and
next four distinct nodes, including down/suspected nodes. Node-position collision
aborts initial setup; no reseeding/moving an established ring. Key/node equality uses
inclusive ownership; different keys with colliding digests remain distinct storage
keys. No per-key owner table. Fixed ten-node bounded search meets the fixed-cluster
routing target; do not claim asymptotic constant time for arbitrary cluster sizes.

Proposed signatures: `owners(key: str) -> tuple[Node, ...]` (exactly five);
`membership_snapshot() -> tuple[Member, ...]` (all ten, sorted ring_id).
`Member = (vm_id, restart_generation: int, incarnation: int, heartbeat: int,
status: ALIVE|SUSPECTED)`. Ring position/endpoint data come only from the manifest.

#### C2. Restart and membership state (Krishna, durable metadata via Juhi)

APPROVED BY KRISHNA — C1–C6 foundation only.

Explicit first-cluster bootstrap creates epoch 1 with a mode supplied by Krishna,
not an inferred default; all ten nodes must agree on that context and manifest.
Existing state is reopened without bootstrap; mismatches fail closed. Proposed
`open_store(path: str, *, bootstrap: Context|None = None) -> Store` permits creation
only with explicit bootstrap and refuses bootstrap over existing state. Exact
admission after future mode changes remains Step 4.

`start_incarnation() -> int` durably increments local restart generation before
network admission; recover database and mode context first. One process per VM/data
path, enforced by process-lifetime exclusive file lock; failure to acquire fails
startup. Loss of generation metadata fails closed, never silently creates a new
identity. Generation never clears with KV mode changes.

Merge by generation first, then incarnation, heartbeat and status. Lower generation
is ignored; newer generation permits ALIVE even if previously suspected. Within one
generation a suspicion wins at equal incarnation; only a newer incarnation refutes
it. Self-refutation increments incarnation; restart increments generation, so old
incarnations cannot override it. Only increasing heartbeat/new incarnation evidence
refreshes the monotonic last-progress timer; repeated snapshots do not postpone
failure forever. Approximately 2.5s without progress marks SUSPECTED; never expires
or removes it. Probe retained suspects as well as live peers in 0.5s full-snapshot
rounds with fanout 3, so recovery is discoverable without a live introducer. Receiving
an accusation about self triggers refutation. Snapshot callbacks/logging run outside
membership locks. Incarnation is process-local within the durable restart generation.
Proposed membership wire remains a separate TCP service: four-byte framed UTF-8
JSON `{type:GOSSIP, manifest_id, sender_id, sender_generation, members}` with
<=65536 payload bytes; members use C1 fields with decimal-string generation,
incarnation and heartbeat. Validate manifest/IDs/types/count (exactly ten), no
unknown members or duplicate IDs. No KV body or mode-epoch advancement via gossip.

#### C3. Local durable store and epoch boundary (Juhi)

APPROVED BY KRISHNA — C1–C6 foundation only. These are implemented local Batch 1 signatures (no distributed service):

```text
Mode = strong | eventual-noRR | eventual-RR
Context = (epoch: int, mode: Mode)
Version = (kind: strong|eventual, number: int, writer_id: str)
Record = (version: Version, value: bytes)
ReadResult = (status: FOUND|NOT_FOUND|STALE_EPOCH|ERROR,
              record: Record|None, context: Context, error: str|None)
InstallResult = (status: INSTALLED|UNCHANGED|SUPERSEDED|VERSION_CONFLICT|
                        STALE_EPOCH|ERROR,
                 stored_version: Version|None, durable: bool,
                 context: Context, error: str|None)
read_local(key: str, *, context: Context) -> ReadResult
install_if_newer(key: str, record: Record, *, context: Context) -> InstallResult
current_context() -> Context
```

Presence is explicit: FOUND requires a record, possibly `value=b''`; NOT_FOUND has
no record. Errors never imply absence. NOT_FOUND is local evidence only; coordinator
R/majority qualification remains protocol-specific. Versions compare only within
the same context and kind. Eventual kinds are shared by both Eventual modes.

SQLite WAL, FULL on every connection; bounded connections owned per worker, no
shared cursor. Read context and record in one snapshot transaction. Install uses one
short write transaction: validate persisted exact epoch AND mode, compare current
version, atomically store bytes+tag, commit, then return durable success. A higher
incoming tag installs; equal tag/equal bytes is UNCHANGED; lower tag returns
SUPERSEDED without modification; equal tag/different bytes returns VERSION_CONFLICT.
INSTALLED/UNCHANGED/SUPERSEDED carry committed stored-version evidence and durable=True;
error/conflict/epoch failure have durable=False and never qualify as successful acks.
SUPERSEDED does not claim the incoming value was installed. Protocols decide whether
committed-at-least-tag evidence qualifies; Step 2 does not finalize that rule.
No response is sent before commit/transaction success. Storage exceptions, FULL/WAL
setup failures and disk-full produce ERROR; fail startup if required settings cannot
be established. Crash between commit and response leaves a possibly successful write.

Epoch validation and conditional store share the transaction with any future context
change/clear. If install commits first, a later switch clears it; if switch commits
first, the old install is rejected. Read also validates context atomically. Higher
unknown epochs are rejected too, never adopted from arbitrary requests. Gossip cannot
advance mode epochs. Only the future approved switch control path may update durable
context and clear records atomically. No public epoch-advance API or cluster switch
algorithm is approved here; switching awaits KD-16 Step 4. Startup with unknown cluster
context must not serve until safely admitted by that future policy. Replica restart
generation, epoch and allocator high-water metadata are separate from clearable KV
rows; clearing version tags on rows does not authorize allocator reuse.

#### C4. Versions and unique allocation (Juhi; Strong semantics Krishna)

APPROVED BY KRISHNA — C1–C6 foundation only.

`allocate_eventual_version(*, context: Context) -> Version` uses a SQLite transaction
to validate context and reserve `timestamp=max(time.time_ns(), persisted_last+1)`;
commit the high-water mark before publishing. processID/writerID is
`vm_id + "/" + restart_generation` with canonical decimal generation. Compare number,
then writer_id lexicographically by ASCII bytes, not locale. Global total ordering
comes from unique writer IDs and per-writer allocation, not synchronized clocks.
Numbers are nonnegative arbitrary-size Python integers, stored/transported as canonical
decimal strings (no leading zeros except "0"); SQLite must not coerce them to signed
64-bit integers. Allocation commits survive restart and concurrent callers serialize
only the short local allocation transaction. Gaps are legal after crashes.

Strong uses the same representation with kind=strong and logical counter. No Strong
allocator signature is frozen yet: KD-11 must ensure counter exceeds queried majority
max AND previous local allocations, including simultaneous same-writer requests,
before publishing a unique tag. Storage comparison/immutable tag→bytes already supports
query/install/writeback. Absence has no Version in the store; Strong's bottom tag and
absent writeback wire representation must be approved under KD-11 before Strong RPCs
are enabled. No timestamp-based substitute for ABD's query phase is implied.

#### C5. Replica RPC (Krishna; storage outcomes supplied by Juhi)

APPROVED BY KRISHNA — C1–C6 foundation only.

One request/response per connection. Proposed frame: outer `u32be(total_payload_len)`;
payload `u32be(json_len) || JSON_UTF8 || raw_value_bytes`. JSON <=16384 bytes;
raw value <=1048576 bytes; outer cap=1064964 bytes (4+16384+1048576), excluding outer
header. Validate total cap before allocating body; exact lengths, no trailing bytes.
All metadata fields required, unused nullable fields explicitly null:

```text
Request: schema=1, request_id, manifest_id, sender_id, sender_generation,
         target_id, epoch, mode, op=READ_LOCAL|INSTALL_IF_NEWER,
         phase=local, key, version|null, value_len
Response: schema=1, request_id, manifest_id, sender_id, sender_generation,
          target_id, epoch, mode, op, phase, status, present,
          version|null, stored_version|null, value_len, durable, error|null
```

Request sender is coordinator, target replica; response sender replica, target
coordinator. IDs are manifest vm_ids; request_id is UUID4 lowercase canonical text,
new for every replica RPC; eventual external client operation IDs are separate.
phase=local is the Step 2 primitive phase only; future ABD/query/writeback and repair
phases require KD-11/12/13 schema extensions before use. No generic client PUT/GET
protocol is implemented by this envelope.

READ request has null version, value_len=0; INSTALL requires version and possibly
zero-byte value. Version JSON object is `{kind, number, writer_id}`, number decimal
string; epoch and generation also canonical decimal strings >=0. READ response uses
FOUND with present=True, version and value_len, followed by exactly value_len raw
value bytes (including zero for a present empty value). NOT_FOUND has present=False,
null version, value_len=0 and no raw bytes. Both have durable=False (not write acks).
READ responses always have stored_version=null. INSTALL responses always have
version=null, present=False, value_len=0 and no raw body; stored_version is the
version actually in committed storage, not the requested version. INSTALLED,
UNCHANGED and SUPERSEDED require stored_version and durable=True; VERSION_CONFLICT
may include the committed conflicting stored_version for diagnosis, durable=False;
STALE_EPOCH/ERROR require stored_version=null and durable=False. The INSTALL request
version remains bound to the outstanding request and is never relabeled as stored.
For incoming v6 against stored v7, reply SUPERSEDED/stored_version=v7, not v6.
SUPERSEDED is not automatically a quorum acknowledgment: KD-11/12 must decide
whether this committed-at-least-requested evidence qualifies. Errors carry bounded text
<=1024 UTF-8 bytes, no traceback/value dump; ERROR codes include INVALID_REQUEST,
OVERLOADED, STORAGE_ERROR. STALE_EPOCH reports the responder's current context, no
write acknowledgment. Context mismatch replies are errors, not quorum evidence.

Reject unknown schema/fields, duplicate JSON keys, NaN, booleans masquerading as
integers, invalid UTF-8/surrogates, malformed IDs/versions, inconsistent kind/mode,
wrong target/manifest, length mismatch and oversize values. Validate writer ID against
manifest labels and canonical generation syntax. Metadata/keys must fit JSON cap
as encoded, not only the raw-key limit. On framing/JSON failure close connection;
where a fully validated request identity exists, send bounded ERROR instead.
No request is dispatched or mutates state until validation completes.

`ReplicaClient.call_replica(target: Node, request: Request, *, deadline: float) ->
RpcOutcome` returns a response or a typed TIMEOUT, UNREACHABLE, INVALID_RESPONSE,
OVERLOADED or ERROR outcome. Deadline is a finite local absolute monotonic timestamp,
never sent as a comparable remote clock value. It covers connection, send, receive
and response validation; no automatic retry occurs. Fixed manifest KV endpoints are
resolved during client initialization, and up to eight numeric addresses are tried
using the remaining call deadline; DNS is not performed inside `call_replica`.
Resolution occurs during client initialization, outside an operation deadline. The
initial client budget is 5s and nested foreground RPCs must reuse it. This is the client wait budget, not the
post-success propagation lifetime; Eventual PUT sends are not implemented here and
must use an independently bounded C6 continuation. `RpcOutcome.completion_unknown`
records that an INSTALL may have reached or committed at the replica before a
timeout/reset, including expiration after a response is received and validated or an
INSTALL `STORAGE_ERROR`; callers must not misreport that as confirmed failure or
success. The send boundary is marked only after the deadline check, so expiration
before the first possible transmission remains definite non-transmission. Read-only
RPC failures do not set this write-completion flag.
Replica service has its own finite accepted-request deadline, and passes that same
absolute deadline into Store admission, SQLite lock waits and pre-commit checks.
If an underlying OS/filesystem call itself stalls, Python cannot preempt it; an
unacknowledged write may therefore have an unknown outcome. The client wait still
ends at its own deadline.

`call_replica` validates one bound response; it does not collect or count a quorum.
Future coordinators count by stable replica vm_id from the prescribed five, never by
number of sockets, replies or restart generations. Duplicate responses count once.
The per-call qualification checks request_id, manifest, key via outstanding-request binding,
op/phase, target/sender, expected replica generation, exact context, status and
op-specific version evidence. Validate READ version/presence/body against its status,
with stored_version=null. Validate INSTALL stored_version against requested version:
INSTALLED/UNCHANGED equal requested; SUPERSEDED strictly greater; VERSION_CONFLICT
equal requested with conflicting bytes detected locally. Reject contradictory
field combinations and durable flags; never count the request version as a stored ack. Attempt all five owners within bounded resources/deadlines; membership status is
a hint, not authority to substitute owners or omit potentially reachable copies.
Snapshot the destination generation at dispatch; a mismatch
fails that attempt (refresh membership for later operations; no automatic retry).
Count by stable replica vm_id from the prescribed five, never by number of sockets,
replies or restart generations. Duplicate responses count once. Same-version
conflicts/errors never qualify. Strong/Eventual phase-specific at-least-tag rules
remain KD-11/12; READ_NOT_FOUND alone cannot decide the coordinator result.

#### C6. Concurrency, lifetime and examples (ownership as above)

APPROVED BY KRISHNA — C1–C6 foundation only.

Proposed starting capacities per node: coordinators 16 active/32 queued; replica
handlers 32/64; background 16/64; reserved membership sends 4/10 (active/queued).
Use explicit bounded admission, not ThreadPoolExecutor's unbounded queue. Coordinator
threads never await work in their own pool; replica handlers never await coordinators;
SQLite waits are limited to remaining deadlines. No membership/key lock is held over
network IO. Short SQLite writes serialize physically; no global application lock
surrounds distributed requests. Measure contention before changing the approach.

Reject new overloaded work with ERROR/OVERLOADED, never wait indefinitely.
Before accepting a PUT, reserve or guarantee bounded task/queue capacity for all
five prescribed-owner attempts, including those remaining after W succeeds. If
capacity cannot be guaranteed, fail admission before success; never drop remaining
fanout because the client Future has completed or a queue is full.

**Client wait versus background lifetime — APPROVED BY KRISHNA — C1–C6 foundation only:**
The initial client deadline is admission_time+5s. It bounds waiting for qualifying W
(or R), not completion of all Eventual PUT fanout. Recommended independent PUT
continuation budget: 10s after foreground completion, configurable and subject to
KD-12 review. At success time t_ack, retain reserved unsent/in-flight jobs through
background_deadline=t_ack+10s; success near the 5s boundary still gets that window.
Every job has one attempt, no automatic retries; queue/connect/read/write waits
use the job's remaining background budget after handoff. Do not retain the expired
foreground deadline on transferred jobs. Proposed maximum accepted lifetime is 15s
from admission (client completion occurs by 5s); no repeated deadline extension.
For a client timeout/error with potentially committed writes, accepted remaining
jobs may likewise finish within completion_time+10s, without changing the client
outcome. Both foreground and background phases target all reachable prescribed
owners, never replacement nodes; failure to contact one is recorded, not success.
Exact protocol response/qualification and RR's separate comparison/repair budget
remain deferred to KD-12/13. This is a finite lifetime proposal, not hinted handoff,
anti-entropy, retry machinery or an implemented Eventual protocol.

Record each owner's attempted/pending/durable-confirmed/failed/timed-out result.
A transfer can have committed before its reply was lost: label outcome unknown,
not confirmed success or confirmed absence. At background expiry record incomplete
propagation and release capacity. Membership has separate reserved recurring capacity.
Graceful shutdown stops admission and drains accepted jobs through their individual
remaining foreground/background deadlines (up to the 15s total bound), then closes
sockets/connections and reports canceled/unstarted work as incomplete. A configured
shorter forced-stop bound must explicitly log truncation, not claim full fanout.
kill -9 may lose outstanding propagation but must preserve committed data; restart
has no obligation to resurrect in-memory jobs or add hinted handoff. Every queued/
in-flight write or repair still performs C3's atomic exact-epoch check: a mode
transition can reject old work even before its background timer expires. Never
continue old-mode work by assigning it the new epoch. No lock spans network IO.

| Case | Proposed outcome and guarantee |
|---|---|
| Missing vs empty | READ gives NOT_FOUND/no record vs FOUND/(v,b''); neither is a timeout |
| Durable install v5 | Commit (v5,bytes), return INSTALLED/durable=True; only then send reply |
| Incoming v6, stored v7 | SUPERSEDED/stored_version=v7/version=null/durable=True; no overwrite; KD-11/12 decide qualification |
| Equal v5, different bytes | VERSION_CONFLICT/durable=False, leave v5 unchanged |
| Two writers v5 and v6 | Transaction comparison leaves v6 regardless of arrival order; v5 may install first or be superseded |
| Crash/unreachable | Connect failure ERROR or deadline TIMEOUT; not NOT_FOUND; restart reads durable state |
| Duplicate VM3 acks | One qualifying replica, even if repeated or across connections |
| W succeeds at 4.9s | Client returns success; remaining reserved sends may continue to 14.9s, not stop at 5s; each outcome logged |
| Queue full before admission | Explicit overload failure; never acknowledge PUT then discard required fanout |
| Background expiry/shutdown/crash | Mark incomplete or unknown copies honestly; preserve committed bytes, no hinted handoff |
| Commit then lost reply | Client TIMEOUT/unknown completion; write may exist; no automatic new-version retry |
| Old epoch 4 after local epoch 5 | STALE_EPOCH/durable=False, no read evidence or mutation; epoch 6 also rejected until control admission |

#### Shared acceptance gate (Batch 1 evidence below; remaining integration tests planned)

Focused correction review: GPT-6 Luna/high independently compared the recovered
509-line decision snapshot, corrected contracts, agent guidance and specification;
no blockers or further fixes were found. Read-only SHA-256 comparison checked 62
baseline files: only KEY_DECISIONS.md, IMPLEMENTATION_PLAN.md and AGENTS.md changed;
none were missing. Six roadmap steps, task owners/statuses, directional approvals,
partial KD-16 and deferred KDs were preserved. No application tests, deployments
or Step 2 runtime verification were performed. Exact C1–C6 contracts remain
APPROVED BY KRISHNA — C1–C6 foundation only; implementation remains blocked.

Historical contract questions — now accepted by Krishna with their documented qualifications:
1. Accept C1's domain-prefixed UTF-8 inputs, inclusive equality, collision rejection,
   vm01..vm10 labels and C1's UTF-8 key policy? Recommended: no separate 4096-byte,
   nonempty or NUL restriction; retain the explicit encoded-metadata bound.
2. Accept C2's generation/incarnation merge, process lock, explicit bootstrap epoch 1
   and recovery admission? Recommended: explicit initial mode strong, agreed on all
   nodes; no inferred mode on reopening an existing store.
3. Accept C3/C4's exact result/signature and allocator semantics, including SUPERSEDED
   evidence without automatically counting it as a protocol acknowledgment?
   Recommended: as specified; preserve the KD-11/12 qualification gate.
4. Accept C5's envelope/validation/16 KiB metadata cap, 1 MiB body and per-RPC UUIDs?
   Recommended: as specified; reject over-cap encoded metadata rather than truncate.
5. Accept proposed port 65434, 5s overall deadline and C6 initial pool/queue capacities?
   Recommended: configurable starting defaults, pending compatibility/load tests.
6. Accept C3's epoch boundary with no public advance API until Step 4, plus C6's
   reserved-background and bounded shutdown rules? Recommended: as specified;
   the proposed independent PUT continuation is 10s after foreground completion
   (<=15s total); RR budget and cluster switching remain deferred.

Independent documentation review (GPT-6 Luna/high, October 29) found no contract
correctness blocker. Main corrected two historical roadmap statements that still
called accepted suspicion/epoch directions optional, and made READ raw-body semantics
explicit. Read-only hash checks show only these two requested documents changed;
all task owners/statuses and six roadmap steps remain intact. No application tests
were run; no Step 2 code, durability, VM or performance results are claimed. This is the earlier handoff review, not evidence of the focused correction review.
Agent guidance has now been corrected; unrelated README historical wording remains
outside this task scope. Krishna's explicit instructions control this task.

Krishna approved C1–C6 and local Batch 1; this statement is historical. Juhi's
ownership is unchanged. Batches 1 and 2 are now locally implemented and verified;
Batch 3 KV RPC and later protocol work remain unimplemented. The following
shared criteria include future integration tests, not all completed tests:

- UTF-8 keys including empty, NUL and control characters at local/RPC boundaries;
  CLI limitations distinguished from wire policy; explicit metadata-cap rejection.
- Cross-process hash vectors, Unicode keys, inclusive equality/wraparound,
  node collision rejection, exactly five stable owners while machines are down.
- Generation commit-before-admission, old-generation gossip, same-generation
  suspicion/refutation, retained-suspect probing and restored live status.
- Empty/absent/error distinctions; READ version versus INSTALL stored_version
  field validation, v6-request/v7-stored SUPERSEDED and no automatic quorum credit;
  atomic tag+bytes; stale/equal-conflicting installs;
  concurrent installs; allocator frozen/backward clock and crash/restart uniqueness.
- Crash before/after commit/ack, WAL/FULL failure/disk-full, startup restore-before-serve.
- Full 1 MiB plus metadata, fragmented headers/bodies, malformed JSON/lengths,
  slow trickles exceeding total deadline, wrong request/replica/context, duplicate acks.
- Race install/read against epoch change/clear in a disposable local boundary fixture;
  no old-mode mutation/read evidence after transition. This is not a cluster switch implementation.
- Saturated pools/queues, unrelated-key progress, reserved membership progress,
  post-ack fanout at W success near the foreground deadline, independent 10s
  continuation, reserved queue capacity, explicit per-owner expiry/failure outcomes,
  epoch rejection during continuation, and bounded shutdown/crash behavior. Protocol-specific guarantees wait
  for their actual KD approvals and implementations.

Provenance/limits: MP1/MP2 original source remains untouched; framing and quarantined
membership are Phase 0 adaptations only, not tested Step 2 implementations. No
separate lecture material was found in the project file inventory. Course AI use
is permitted by the human instruction, not a blocker. SQLite WAL/FULL durability
assumes functioning filesystem sync and preserved database/WAL files; VM versions,
port availability and actual contention/crash behavior are unverified. No deployments,
installs or measurements occurred. Existing milestone dates are historical targets,
not proof of work completed by October 29. Full mode coordination and Strong bottom/
allocation semantics remain deferred, not silently implemented or approved.

## MP1/MP2 reuse audit

Classification is the intended disposition; the final column distinguishes actual
Phase 0 reuse from proposals. All source/test/README/config/deployment/experiment
files were inspected; original performance figures/CSV are prior MP1 results only,
not MP3 evidence. No original network service was imported or launched.

| Source/component | Classification | Verified source behavior and MP3 consequence | Phase 0 action |
|---|---|---|---|
| MP1 `protocol.py`: send_msg/recv_exact/recv_msg | Reuse with modifications | 4-byte `!I`, loops partial reads, EOF raises; no size limit; repeated byte concatenation | Adapted into bounded `framing.py`, bytearray receive; cap explicit; interoperability tested |
| MP1 `server.py`: TCP grep server | Reuse with modifications | Top-level argv/listener side effects; CWD `machine.<vm>.log`; thread/connection; no inbound/grep timeout or frame limit; shlex grep argv | No copy; plan import-safe path-configured adapter KD-17 |
| MP1 `client.py`: distributed query | Reuse with modifications | Concurrent ThreadPoolExecutor, each socket 5s; collects futures in submission order (does not serialize sends); global machines.txt; framing/Unicode/count parsing errors not fully caught; empty inventory yields 0 workers | No copy; local framing compatibility only; final config/errors/path adapter pending |
| MP1 `machines.txt`, `perf_machines.txt` | Reuse with modifications | 10 endpoints vs four perf endpoints, port 65432 | Read as reference; don't assume final MP3 ports/manifest |
| MP1 run/deploy/kill scripts | Replace for specific reason | Hardcoded Krishna NetID/path; deploy kills before copying; no MP3 data separation | No execution/copy; later minimal safe scripts with explicit MP3 paths |
| MP1 tests/conftest | Replace for specific reason | test_suite autouse fixtures SSH to 10 VMs, backup/replace real logs and local config; duplicate machines_file fixture overrides an earlier broken MACHINES reference; conftest only adds sys.path | Not run; independent disposable local Phase 0 suite |
| MP1 generate_perf_logs/run_timing/analyze | Not needed for KV workload | Generates log lines and measures grep latency, uses unseeded shuffle; analysis mean/stdev pattern is useful reference | No copy; new seeded KV harness later |
| MP2 membership Entry/MembershipList/snapshot | Reuse directly as quarantined baseline | State protected by one membership lock; snapshot copies dicts; heartbeat/refutation tested; callbacks occur under lock in merge/timeouts (reentrant callbacks can deadlock) | Only `membership.py` copied byte-identically to `legacy_membership.py`; own prior work attribution here |
| MP2 terminal/recovery/list_mem behavior | Reuse with modifications | `_incoming_wins` rejects all updates to terminal entries even newer incarnation; `snapshot` keeps FAILED but `list_mem` hides it; alive_peers includes SUSPECTED; all_known includes FAILED/LEFT | Tests characterize limitation; no recovery edits approved; KD-03/04 |
| MP2 gossip/refutation loops | Reuse with modifications | Full JSON snapshot, fanout 3 per 0.5s; new daemon thread/send; refutes to all alive peers; no bounded pool | No node.py copy; adaptation pending KD-05/07/14 |
| MP2 heartbeat/incarnation/suspicion | Reuse with modifications | wall time timers 2.5 / 1.0 / 1.5s; suspicion refutation bumps own incarnation; terminal wins before incarnation comparison | Exact state copy retained; fake-clock lifecycle tests; final clock/recovery policy pending |
| MP2 startup/identity/join | Replace for specific reason | `host:port:time.time_ns()` changes per startup; VM1 introducer; machine-file ports ignored; joins DEFAULT_PORT despite local override; three 2s join attempts | Stable ring/process split and restart admission pending KD-03; no live membership service |
| MP2 TCP framing/byte counters | Reuse with modifications | Same uncapped framing as MP1; counters guarded by separate global lock, count completed frames | Framing adapted from both; byte counters not copied (optional future measurements) |
| MP2 server/concurrency | Reuse with modifications | Unlimited daemon thread per accepted connection; inbound socket lacks deadline; gossip sends parallel; config broadcast sequential; no whole-KV global lock exists yet | No copy; bounded services after KD-07/08/14 |
| MP2 CLI/FIFO | Reuse with modifications | input loop controls membership/suspicion/drop rate; FIFO startup preserves stdin; not MP3 operation grammar | Replaced Phase 0 grammar only; eventual orchestration pending KD-14 |
| MP2 logging | Reuse with modifications | Thread lock, console + append `machine.<vm>.log`, local epoch timestamp; not KV events | New opt-in JSON prototype; final log-query integration pending KD-17 |
| MP2 deploy/run/kill/cmd/show/reset | Replace for specific reason | SSH hardcoded identity/paths; deploy kills; run rebuilds FIFO/node.out while preserving machine log; reset deletes logs remotely | Read only; no copy/run; MP3 persistence-safe scripts later |
| MP2 experiments/cluster.py | Reuse with modifications | ThreadPool helpers and log collection useful; import creates ~/.ssh/mp2-ctl, disables host-key checking; restart defaults to kill+log deletion; several remote return codes ignored | Not imported/run/copied; only conceptual harness patterns |
| MP2 experiments/experiments.py | Not needed as implementation | Measures gossip bandwidth/false failures/detection, not KV throughput/latency; relative imports/path setup differs from README root commands | No copy; seeded KV trials designed later |
| MP2 experiments/parse.py, plot.py | Reuse with modifications | Parse membership-text event regexes; sample stdev/errorbars reference; `run_params` treats no parameters as OK, inappropriate for new evidence validation | No copy; design strict KV event parser/aggregator later |
| MP2 README/testing claims | Replace for specific reason | Mentions missing `selftest.py`; experiment commands omit existing `experiments/` path | New MP3 README tested against actual package; no assumed legacy coverage |

No MP1/MP2 file is changed. `test_reuse` loads only side-effect-free original
protocol modules with bytecode writes disabled. Passing characterization tests
proves those bounded cases; it does not prove live services, membership integration,
storage durability, linearizability or MP3 log-query functionality.

## Phases, ownership, tasks and evidence

Allowed statuses: COMPLETE (deliverable exists), IN_PROGRESS, BLOCKED_BY_DECISION,
NOT_STARTED, VERIFIED (applicable checks actually passed). Test cases listed for
future tasks are acceptance requirements, not completed tests. Proposed ownership
is the initial agreement from the prompt; coordinate before cross-owner edits.

| ID | Owner | Work | Dependencies / decision IDs | Acceptance tests | Status |
|---|---|---|---|---|---|
| P0-01 | Krishna | Inspect source/config/tests/scripts and capture reuse risks | None | Audit trace to actual files; reference preservation check | VERIFIED |
| P0-02 | Krishna | Bounded framing and tests | Independent prototype; KD-06/07 final | Fragments/EOF/oversize/empty/1 MiB/legacy interoperability | VERIFIED |
| P0-03 | Joint scaffold (Krishna implements) | Inventory/constants/types/successors/log prototype | Supplied IDs, explicit policy; no permanent hash | Config/Ring/Domain/Log tests | VERIFIED |
| P0-04 | Joint scaffold (Krishna implements) | CLI validation, guard, smoke | No distributed runtime | CliTests, smoke; no value-file changes | VERIFIED |
| P0-05 | Krishna | Quarantine exact membership baseline | MP2 existing module only | Exact bytes, heartbeat/refutation/timeouts/failed lifecycle | VERIFIED |
| P0-06 | Joint (Krishna authors) | Five shared docs, single-agent rules + Claude entrypoints | Source audit; all decisions pending | File/function coverage and independent documentation review | VERIFIED |
| P0-07 | Review subagent; Krishna fixes | Independent review and Phase 0 gate | P0-01..06 | Local test rerun, scope/correctness/doc audit; REVIEW.md and VERIFICATION.md | VERIFIED |
| DR-01 | Joint | Check Piazza submission/demo notices; local SPEC.md reviewed; AI assistance permitted | Remaining submission/demo notices required | Record remaining notices; retain spec-correct checklist | NOT_STARTED |
| DR-02 | Joint | Approve/reject decisions in the roadmap's staged reviews, starting with Step 2 tonight | Local SPEC.md; next-step decision group | Actual approval fields and agreed invariants; no need to approve all 18 tonight | BLOCKED_BY_DECISION |
| I-01 | Joint | Agree Step 2 storage/version/RPC/identity contracts; review extensions before Steps 3/4 | DR-02; KD-02/03/06/08/09/10/14/15; KD-16 interface boundary | Written schemas, examples and shared contract-test plan | COMPLETE |
| I-02 | Joint | Integrate Store, fixed Manifest, MembershipRuntime, ReplicaService and ReplicaClient in one local node lifecycle | I-01; M-01/R-01/N-01/S-01; approved C1–C6; KD-16 local epoch fence only | Three-node loopback gossip/RPC, explicit bootstrap/recovery, SIGKILL generation recovery, bounded shutdown/rollback, all local suite checks | VERIFIED |
| M-01 | Krishna | Membership adaptation and restart recovery | I-01; KD-03/04/05/07/14 | Fake-clock suspicion, retained owners, newer generation recovery, stale gossip, local TCP recovery | VERIFIED |
| R-01 | Krishna | Hash/ring construction and routing | I-01; KD-02/03 | SHA-256 vectors, canonical manifest, cross-process five-owner selection, failed-owner stability | VERIFIED |
| N-01 | Krishna | Concurrent bounded single-replica RPC client/service | I-01; KD-06/07/08/14/15 | Binary roundtrip, limits, total deadlines, response binding, overload and loopback store integration | VERIFIED |
| S-01 | Juhi | Persistent atomic monotonic store (local Batch 1 verified; VM evidence in F-01) | I-01; KD-09/10/15 and KD-16 interface boundary (full switch in C-02) | Temporary crash/restart, value+tag atomicity, conditional install | VERIFIED |
| V-01 | Juhi; Krishna reviews Strong tags | Shared version foundations in Step 2; protocol-specific allocation in Step 3 | I-01; KD-03/10 first; KD-11 before Strong allocation | Same-process concurrency, rollback, restart, tag immutability | IN_PROGRESS |
| A-01 | Krishna | Strong ABD | R-01,N-01,S-01,V-01; KD-11 | Adversarial multi-writer/read-writeback linearizability histories | BLOCKED_BY_DECISION |
| E-01 | Juhi | Eventual-noRR | R-01,N-01,S-01,V-01; KD-12 | All-owner fanout, W durable acks, R max, background completion | BLOCKED_BY_DECISION |
| E-02 | Juhi | Eventual-RR | E-01; KD-13/16/17 | Early response, late max, missing/stale repairs, racing newer PUT | BLOCKED_BY_DECISION |
| L-01 | Joint | Required operation logs and MP1 query integration | N-01,protocols; KD-17 | Real receipt/response records, local then approved VM query | BLOCKED_BY_DECISION |
| C-01 | Joint | Actual CLI/files/multiput/output integration | Modes, L-01; KD-14; SPEC.md command/output requirements | Eight commands, empty/1 MiB files, required completion info | BLOCKED_BY_DECISION |
| C-02 | Joint | Mode switching and generation fences | Modes,S-01,M-01; KD-16 | Clear/retain matrix, delayed requests/repairs/offline restart | BLOCKED_BY_DECISION |
| F-01 | Joint | Correctness and failure integration | M/R/N/S/V/A/E/C; VM use needs approval | Partitions, delays, kill -9, timeout ambiguity, concurrency | BLOCKED_BY_DECISION |
| B-01 | Joint | Experiment 1 harness/trials/analysis | F-01; KD-18; explicit VM approval | 45+ real >=180s trials, seeds/raw data/means/stdevs | BLOCKED_BY_DECISION |
| B-02 | Joint | Experiment 2 harness/trials/analysis | F-01,E-02; KD-18; explicit VM approval | 6+ real >=180s trials, 99% hot keys, repeatable seeds and statistics | BLOCKED_BY_DECISION |
| D-01 | Joint | Report and submission package | B-01/02,DR-01 | <3 pages, 12-point, actual plots/design/analysis, code provenance | NOT_STARTED |
| D-02 | Joint | Final demo/deployment/runbook | F-01,C/L,D-01; explicit VM approval | Full timed command/crash/restart/mode/query dry run | NOT_STARTED |

The Phase 0 gate is closed with its contemporaneous review and local checks.
Batch 1 and Batch 2 evidence follows below. Completed local foundation tasks do not
claim integrated KV behavior, ten-VM correctness, or any deferred protocol.

### Integration milestones (historical proposed schedule, not evidence of completion)

| Gate | Target | Owners | Required result |
|---|---|---|---|
| G0 | Oct 22, before design review | Krishna + focused reviewer | Useful tested scaffold, truthful docs, no distributed behavior |
| G1 | Tonight / Oct 23 | Joint | Step 2 decision group approved, shared interface examples; later KD groups wait for their steps |
| G2 | Oct 25 | Krishna + Juhi independent branches | Routing/RPC/membership alongside durable storage/version tests |
| G3 | Oct 28 | Joint integration | Strong/noRR end-to-end local correctness, finite errors |
| G4 | Oct 30 | Joint | RR and fenced mode changes; required logs/CLI/log-query |
| G5 | Nov 2 | Joint | Approved VM deployment, crash/partition correctness gate |
| G6 | Nov 4–5 | Joint | Full real repeated benchmark matrix + raw artifacts |
| G7 | Nov 6–7 | Joint | Report draft, evidence review, full demo rehearsal |
| G8 | Before Nov 8 11:59 PM | Joint | Official submission verified; buffer reserved |
| Demo | Nov 9 | Joint | Both developers explain approved design and observed behavior |

Use separate feature branches in the real Git checkout, e.g. Krishna's membership/
routing/RPC/Strong work and Juhi's storage/versions/eventual work; names are examples.
Do not create branches or initialize Git in this metadata-free snapshot. Establish
the same shared contract before writing either side; integrate coherent tested units
frequently, not one large merge near the deadline. Shared docs and integration files
require coordination to avoid overwriting the other developer's edits.

## Test strategy and acceptance gates

Phase 0 uses standard-library unittest, local socket pairs, fake fragment sockets,
fake wall-clock readings for legacy behavior, and temporary directories. Test only
implemented utilities and NOT_IMPLEMENTED boundaries. Compile imports without
running legacy servers or experiments. Record interpreter/commands/count/results in
VERIFICATION.md. Prior false starts must be corrected and rerun; never hide failures
as successful evidence. Compare reference file content before/after.

After approval, shared contract tests validate bytes/tag ordering/absence/durable
ack evidence before protocols integrate. Then use deterministic delivery schedules
to check duplicate/delayed/stale replies, no-quorum errors, same-key writers, Strong
linearizability and eventual allowed staleness/repair. Simulated cases and local
crash tests do not prove a live ten-VM deployment. All destructive/remote testing
requires explicit permission and isolated MP3 state; never erase MP1/MP2 data.

Strong acceptance needs an actual per-key history checker and a stated model for
pending/timed-out operations, not only assertions about final values. Eventual
tests must show first-R result, distinct W acks and continued all-owner propagation;
RR must demonstrate late higher discoveries and conditional repair, even when the
client has returned. Mode/restart tests must inject old requests after data clears.

## Performance plan (not executed)

No MP3 workload, timing sample, throughput, latency, standard deviation or plot has
been collected. Keep MP1 performance files out of MP3 results. Correctness first.

Experiment 1 matrix: {Strong, Eventual-noRR 1/1, Eventual-noRR 2/2} ×
{1,2,4,8,10 clients} × at least 3 trials = **45 trials minimum**. Ten VMs, N=5,
1 KiB values, 100,000 preloaded keys, uniform seeded key sampling, 50/50 GET/PUT
with unique values, each client workload >=180 seconds. At least **135 minutes** of measured sequential
trial windows, excluding preload, warmup, mode clearing, setup, failures and repeats.
All clients in a trial run for the required duration; don't mistake 180/n seconds
per client for compliance. Each configuration needs verified preload after a clear.

Experiment 2: {noRR 1/1, RR 1/1} × >=3 trials = **6 trials minimum**, ten clients,
1 KiB, 100,000 preloaded keys, 50/50 operations, 99% operations selecting ten hot
keys and 1% randomly selecting from the remaining keys. The original spec also
requires >=180 seconds **per client** here, adding at least **18 minutes** of
sequential measured trial windows. This is mandatory, not a KD-18 duration choice.

Both experiments use a healthy cluster with each client issuing **one operation at
a time**. Complete preload on **all prescribed replicas** before measurement, and
reuse the same generated workload seeds across compared variants. Report aggregate
successful operations/second and mean end-to-end latency across all clients.
Experiment 1 requires throughput on the y-axis against mean latency on the x-axis,
one curve per configuration and one labeled point per client count. Experiment 2
requires separate bar plots for throughput and mean latency. Plot averages and
standard deviations for both; confidence intervals are optional. The client model,
duration and these plot forms are fixed even though KD-18's historical text lists
some of them as open; decide only the remaining engineering details at Step 6.

Proposed harness records: run/trial ID, commit/source identity, VM/runtime versions,
manifest/mode/R/W, base+per-client seeds, preload count/status, key/value digests,
client count, operation mix/distribution, monotonic begin/end, attempted/successful/
timeout/error counts, per-operation latency/status/version and RR event references.
Validate preload is durable on the required owners, not merely 100,000 issued PUTs.
Specify warmup exclusion and wait for residual background work between trials.

Compute successful operations/measurement seconds and mean latency with inclusion
rules documented; separately report failed/timed-out attempts. Retain raw samples,
derive each trial's statistic, then configuration means and sample standard
deviations across >=3 trials. Don't pool correlated requests and call them independent
readings. Record missing/failed trials explicitly and repeat them with provenance.
Review seed reproducibility, clock measurements, log overhead, experiment order and
hot-key distribution before spending VM time. Final figures must be reproducible
from actual raw trials. Report under three pages, 12-point: design, actual results,
plots and interpretation; no placeholder numbers in a submission.

## Phase 0 gate / submission and demo readiness

Phase 0 gate:

- [x] Real `mp3/` beside untouched MP1/MP2, minimal useful code and local tests.
- [x] Shared plan, decisions, rules, explanation and README; Claude uses same rules.
- [x] Explicit blocked command boundary; no storage/hash/protocol/recovery choice made.
- [x] Ownership, dependencies, acceptance criteria and proposed integration milestones.
- [x] Independent selected-model review, focused fixes, rerun and evidence recorded.
- [x] Stop at G0; Krishna/Juhi review pending choices before any Phase 1 behavior.

Future submission/demo gate (all pending):

- [ ] Official spec/submission notices reconciled, approved design explainable by both people.
- [ ] Ten-node fixed owners and MP2 recovery; durable bytes/tags under kill -9.
- [ ] Strong concurrent-history checks; both Eventual configurations and repair traces.
- [ ] Eight commands including actual file IO, prescribed ls, required completion text.
- [ ] Console/file replica logs and working MP1 queries of MP3 logs.
- [ ] Safe coordinated mode changes preserve membership/ring and fence delayed work.
- [ ] Deadline/timeouts/partitions, empty and 1 MiB values, concurrent writers tested.
- [ ] Both real experiments, >=3 readings/config, seeds/raw data/means/stdev/plots.
- [ ] <3-page 12-point report (design <=1 page), required plots/analysis and full demo dry run.
- [ ] Friday demo signup; one partner submits/tagged report pages and partner on Gradescope;
      working code/README in course-repo mp3; code Google form; all due-date receipts checked.

After Krishna’s contract review, record actual approvals in KEY_DECISIONS.md,
coordinate implementation against those contracts, then unblock only dependent tasks. Phase 0 does not
authorize starting those implementations automatically.

## Historical Batch 1 execution evidence — October 29, 2026

Krishna's explicit C1–C6 approval authorizes this batch only. Contract task I-01 is
COMPLETE, S-01's local store is VERIFIED, and V-01 is IN_PROGRESS: Eventual allocation
and shared ordering work, but Strong query-aware allocation remains deferred to
KD-11. No original ownership was reassigned. The older gates/review notes above
are historical snapshots; their pending-contract wording does not revoke this approval.

Implemented: Context/Version/Record/local results; `storage.open_store` and Store
read/install/current_context, durable `start_incarnation`, Eventual allocator and
bounded close. Local configuration extensions are explicit `vm_id` (default vm01,
persisted/checked on reopen), `timeout` (5s), and connection admission `capacity` (8).
A process-lifetime POSIX file lock enforces one store owner; this is a Mac/Linux
foundation, not tested on Windows. No public mode-change API is provided. Startup
validates persisted metadata and records before returning. Every operation uses a
fresh worker-owned connection within bounded admission and SQLite busy deadlines;
no global distributed-operation lock exists. OS/filesystem sync stalls are not
claimed to be preemptible by Python deadlines.

Executed: `python3 -B -m unittest discover -s mp3/tests -v` — 51 tests, all passed
(34 existing +17 new storage tests). No test failures occurred. The independent reviewer also ran all 51 tests successfully.
Its timeout-bool validation nit was fixed; a large-counter regression now tests
2**15000 via chunked decimal conversion without altering global Python limits. Real local child
processes were killed with SIGKILL immediately before/after COMMIT and after allocator
COMMIT; reopen tests prove rollback or retained committed bytes/high-water state.
An injected commit-time SQLITE_FULL-like error proves ERROR/no durable ack/rollback;
this is simulated disk failure, not a physical full-disk experiment. Concurrent
same-key installs and allocator calls, frozen/backward clocks, full 1 MiB/empty/NUL
keys, stale persisted epoch/mode, invalid files/metadata and lock contention are
covered. A standalone epoch update inside a disposable fixture tests the storage
boundary; it is not a cluster switching implementation.

At the end of Batch 1, CLI commands remained NOT_IMPLEMENTED and the membership,
ring, RPC and integration work was still future work. No Strong ABD, Eventual
coordinators, repair, cluster epoch transitions, benchmarks, VM activity or deployment
was part of Batch 1.

Final independent review: GPT-6 Luna/high ran the full 51-test suite successfully,
then reran all 17 storage tests after the counter/validation refinements. No
substantive storage blocker was found. Main fixed its boolean-timeout nit and
subsequent huge-integer timeout error-type nit, and reran the complete suite:
`python3 -B -m unittest discover -s mp3/tests -q` — 51 tests, OK. In-memory source
compilation passed. SHA-256 baseline comparison found exactly seven intended
code/test/doc changes, all 33 MP1/MP2 files and SPEC.md unchanged. No Git metadata
exists in this snapshot (`git status --short` reports not a Git repository);
no Git history, VM state, deployment or packages were changed.

At that earlier handoff, M-01/R-01/N-01 were NOT_STARTED and Batch 2 was not yet
authorized. DR-02 remains open for later deferred protocol reviews. No task ownership
changed.

## Batch 2 execution evidence — October 29, 2026

Krishna explicitly authorized Batch 2: ring, manifest, membership and crash/restart
recovery only. M-01 and R-01 are now VERIFIED from local evidence; N-01 remains
NOT_STARTED, S-01 remains VERIFIED, and V-01 remains IN_PROGRESS. No ownership or
decision approval was changed. The new membership code adapts MP2 concepts but is
not copied into or substituted for the original MP2 source. `legacy_membership.py`
remains a byte-identical quarantined reference.

Implemented in `ring.py`: immutable ten-node vm01..vm10 manifests, strict endpoint
and shape validation, canonical JSON SHA-256 fingerprint, approved prefixed SHA-256
node/key IDs, ring-position collision rejection, inclusive exact-position ownership,
wraparound, and five distinct owners independent of liveness. The manifest contains
all VM endpoints and positions but no per-key mapping. The example `photo` placement
vm03, vm09, vm04, vm08, vm06 is calculated from the tested fixture manifest only;
it is not a deployment inventory.

Implemented in `membership.py`: complete ten-member snapshots; ALIVE/SUSPECTED
states; monotonic heartbeat/suspicion timing; infinite suspicion retention; full
TCP gossip with manifest fingerprint and bounded four-byte framed JSON; rotating
fanout (default 3) and interval (default 0.5s); bounded service worker admission;
one durable `Store.start_incarnation()` call from `MembershipRuntime.start()` before
listener/background admission; generation/incarnation/heartbeat merge rules and
self-refutation; stale-generation rejection; and direct-peer recovery without a
special introducer. Membership snapshots are ring-position ordered. The future
`list_mem_ids` data view sorts by ascending numeric ring ID (`nodeID`) and includes
each stable VM identity and its ring ID; its CLI output remains unimplemented.
Gossip counters use a 256-digit engineering resource bound in addition to the
approved 65,536-byte message cap; this is defensive and not a numeric limit from
SPEC.md.

Executed after the implementation fixes:

- `python3 -B -m unittest discover -s mp3/tests -q` — 70 tests passed (51 previous
  tests plus 19 new ring/membership tests), after adding the per-process startup guard.
- The independent GPT-6 Luna/high reviewer first ran 69 tests and found ordering,
  deep-immutability and counter-bound issues. Those were corrected and independently
  rerun. After the per-process startup guard, regression test, listener cleanup and
  documentation updates, the reviewer reran
  `python3 -B -m unittest discover -s mp3/tests -v` — 70 tests passed in 1.374s.
  Final review found no remaining substantive issues at that time. The snapshot order
  is ring ID. A later focused review corrected `list_mem_ids` ordering to ascending
  numeric ring ID (`nodeID`) and made invalid self-generation rejection atomic; see
  the final correctness-fix evidence below.
- Tests cover known hash outputs, input-order/cross-process determinism, equality,
  wraparound, duplicate positions, fixed owners during suspicion, full snapshot
  validation, fake-clock heartbeat/suspicion, generation recovery, old-generation
  gossip rejection, self-refutation, concurrent merges, bounded admission, and two
  local subprocess SIGKILL/restart cycles preserving a stored value. A local loopback
  TCP test recovers VM06 through VM02/VM03 while VM01 is unavailable.

All evidence is local: temporary directories, local child processes, fake clocks,
and loopback sockets. No university VMs, deployments, package installations, Git
operations, KV RPC, Strong ABD, Eventual coordinator, read repair, CLI operation, or
ten-VM convergence/latency test was performed. Full KV requests, commands and mode
transitions remain pending.

Final review: GPT-6 Luna/high independently inspected the ring, membership, recovery,
bounded networking, tests and synchronized docs. It found no remaining substantive
issue and confirmed the journal distinguishes local evidence from real VM validation.
Its final 70-test verbose run and the main agent's final 70-test quiet run both passed.

## Batch 2 focused final correctness fixes — October 30, 2026

`list_mem_ids()` now sorts by ascending numeric ring ID (`nodeID`), as required by
SPEC.md; all ten identities, states and ring IDs remain in each result. `merge_gossip()`
validates the state-dependent newer-self-generation invariant under its lock before
applying any row, so an invalid snapshot cannot partially update membership. Tests
assert ring-ID ordering and compare complete snapshots before/after the invalid merge.

- Main: `python3 -B -m unittest discover -s mp3/tests -q` — **71 tests passed**.
- Independent reviewer GPT-6 Luna/high: `python3 -B -m unittest discover -s mp3/tests -v`
  — **71 tests passed in 0.954s**, plus focused atomic-rejection test passed.
- The reviewer observed a transient test failure when its first full run overlapped
  an in-progress test edit; the rerun on settled files passed. No source edits were
  made by the reviewer. The existing MP1/MP2 interop and byte-identical legacy-copy
  tests passed in the full suite. No VM or ten-node deployment was tested.

## Batch 3 execution evidence — October 30, 2026

Krishna authorized the bounded C5/C6 single-replica RPC batch only. N-01 is now
VERIFIED for the client primitive, KV replica service, bounded admission, storage
integration and tested local restart path. This does not implement quorum collection,
distributed client-facing GET/PUT, consistency protocols or all-owner replication.

`rpc.py` implements strict schema-v1 request/response validation, canonical UUID4 and
decimal metadata, JSON duplicate-key rejection, the approved 4-byte outer frame plus
4-byte metadata length, JSON metadata and raw bytes, metadata/value/outer caps, and
response binding to the request and expected replica generation. `ReplicaClient` makes
one TCP call without retries and returns typed outcomes; a timeout or reset after a
send can be marked `completion_unknown`. `replica.py` admits a fixed number of request
workers and a bounded queue, validates before dispatch, calls `read_local` or
`install_if_newer`, and replies only with the actual storage result. `framing.py` keeps
the old MP1/MP2-compatible helpers and adds total-deadline socket helpers. `eventlog.py`
prints escaped required receipt/response terminal lines and emits structured local
events without logging value bytes.

The first independent review ran **102 tests** successfully and found: storage lock
waits could exceed the RPC deadline and commit late; nonfinite/oversized RPC deadlines
could escape as `OverflowError`; and implementation docs/status were stale. The main
added deadline propagation from the service into SQLite admission/lock/commit checks,
returns explicit outcomes for nonfinite/out-of-range client deadlines, adds regression
tests for both issues, and synchronized docs. The reviewer reproduced the lock-wait
case before the fix; the new regression holds a SQLite writer lock, proves the request
finishes within its shorter service budget, and confirms the row remains absent after
the lock is released.

Final verification on the settled implementation:

- Main: `python3 -B -m unittest discover -s mp3/tests -v` — **106 tests passed in
  6.580s**.
- Independent GPT-6 Luna/high reviewer: the same full-suite command — **106 tests
  passed in 6.605s**.
- Main focused command `python3 -B -m unittest mp3.tests.test_rpc mp3.tests.test_replica
  mp3.tests.test_storage -v` — **51 tests passed in 5.603s**.
- `python3 -B -m mp3 --smoke` — exit 0, “Phase 0 smoke OK: framing, inventory,
  successors; KV NOT_IMPLEMENTED” (CLI operations remain guarded).
- `python3 -B -m mp3 --help` — exit 0; lists the command syntax and pending status.
- `python3 -B -m py_compile mp3/rpc.py mp3/replica.py mp3/framing.py mp3/storage.py
  mp3/eventlog.py` — exit 0.

The lock-contention regression verifies a request with a shorter service deadline
does not commit after the external SQLite writer lock is released. A separate
connection test verifies `call_replica` uses cached numeric addresses, tries
candidates under the same remaining deadline, and performs no DNS lookup inside the
call. Endpoint DNS resolution occurs once during client setup, outside any operation
deadline. Local loopback and process tests do not constitute ten-VM evidence. The
reviewer made no edits and reported no remaining source correctness issue; the
identified deadline issues and documentation nits were corrected.

### Batch 3 final shutdown/deadline corrections — October 30, 2026

The post-Batch-3 review found that bounded sentinel insertion could leave idle workers
blocked when the worker count exceeded queue capacity. Workers now observe the stop
event while polling the bounded queue. Graceful stop drains accepted work; forced
stop closes and accounts for queued work, interrupts active sockets, and reports the
original unfinished count and whether workers joined. Handler and close exceptions
no longer skip queue accounting or silently shrink the pool.

The client now marks possible request transmission immediately before `sendall`,
after checking the same absolute deadline. Thus a timeout before the first send has
`completion_unknown=False`; expiration after a validated reply remains TIMEOUT (not
an on-time acknowledgment) with `completion_unknown=True`. INSTALL `STORAGE_ERROR`
responses also preserve uncertainty because they provide no durable-success evidence
and may follow an ambiguous storage failure. Read-only RPC errors do not set the
write-completion flag. No automatic retries were added.

- Main: `python3 -B -m unittest discover -s mp3/tests -v` — **114 tests passed in
  12.527s**; `python3 -B -m mp3 --smoke` and `python3 -B -m mp3 --help` exited 0.
- Independent GPT-6 Luna/high reviewer: the settled full suite — **114 tests passed
  in 11.812s**; smoke/help, `python3 -B -m compileall -q mp3`, and
  `git diff --check` passed. Reviewer made no source changes and found no remaining
  blocking defects. Its first follow-up run overlapped the final READ assertion edit
  and was rerun successfully on the settled tree.
- New tests cover idle workers with workers > queue capacity, graceful draining,
  forced full-queue shutdown/accounting, repeated start/stop without handler leaks,
  worker survival after an exception, deterministic post-validation timeout, definite
  pre-send expiration, and INSTALL storage-error uncertainty.

Evidence is local and uses loopback sockets, temporary storage and a deterministic
clock seam. No university VM, Batch 4 code, or Strong/Eventual coordinator was tested
or implemented.

## Batch 4 execution evidence — October 30, 2026

`NodeRuntime` in `node.py` now connects the existing validated Manifest, local SQLite
Store, MembershipRuntime, ReplicaService and ReplicaClient. Startup requires explicit
bootstrap or recovery, verifies the persisted Store/context, advances restart
generation exactly once through MembershipRuntime, then opens membership and KV
listeners before setting local readiness. `python3 -B -m mp3.node` is a separate
diagnostic entrypoint. It supports local node lifecycle and one-replica RPC only; it
does not enable client-facing distributed PUT/GET or any of the eight commands.

Local integration starts three logical VMs on real loopback sockets. It verifies
gossip and consistent owner lists, remote READ_LOCAL and INSTALL_IF_NEWER against the
target's real SQLite Store, empty versus missing values, a full 1 MiB binary value,
concurrent requests while gossip proceeds, and a disposable VM06 subprocess SIGKILL
and recovery on the same database. The recovered process retains its VM/ring identity,
advances its durable generation, serves the committed value after peers converge, and
rejects stale request/response/gossip evidence. Fixed owners do not change. Injected
gossip and KV accept-thread startup failures verify listener and Store cleanup. A
forced-shutdown regression verifies the initial incomplete report, retries after work
exits, then joins workers and closes/reopens the database. No real VM was contacted.

Independent review found two defects: retrying after an initially non-clean forced
shutdown could leave the Store lock open permanently, and a gossip thread start error
could make cleanup try to join an unstarted thread before closing the listener. The
main agent fixed both, added regressions, and also checked KV accept-thread start
rollback. Shutdown reports now refresh after retry, and cleanup preserves the original
startup exception. The reviewer made no code changes and found no remaining issue.

- Main: `python3 -B -m unittest discover -s mp3/tests -v` — **127 tests passed in
  15.013s**. The three focused shutdown/startup regressions passed separately in
  **0.758s**.
- Independent GPT-6 Luna/high reviewer: full suite — **127 tests passed in 14.830s**.
- Main and reviewer: `python3 -B -m mp3 --smoke`, `python3 -B -m mp3 --help`,
  `python3 -B -m compileall -q mp3`, and `git diff --check` — all exited 0.

The Step 2 **local foundation gate is VERIFIED** for Batches 1–4. This says the
individual components start together and pass local loopback/process acceptance tests;
it does not establish ten-VM deployment readiness, real-cluster convergence, multi-owner
protocol correctness, or demo-command readiness. Step 3 still requires its deferred
Strong and Eventual design decisions before those coordinators are implemented.
