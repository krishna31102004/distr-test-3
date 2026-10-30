# Understanding MP3 and its verified Batch 1/2 foundations

This guide describes the actual files. Batch 1's local durable SQLite store and
Batch 2's deterministic ring and membership service are implemented and locally
verified. **Distributed key-value operations are still pending**: there is no KV
RPC, coordinator, quorum, consistency protocol, read repair, or mode-switch service.
Approved contracts and evidence are in IMPLEMENTATION_PLAN.md and KEY_DECISIONS.md.
A local replica read/install is not a distributed GET/PUT.

## The system we will build

A key-value store associates a name (key) with bytes (value). PUT replaces the value
for a key; GET retrieves it. A value might be a file, including an empty file. A
missing key is different from a stored empty file, and a timeout means we could not
obtain enough evidence to answer—not that the key is missing.

Replication stores copies on several machines so a process can fail without losing
every copy. MP3 has ten VMs and five prescribed owners per key. More copies create
a new problem: delayed or concurrent writes can leave different values on different
replicas. A version tag orders those values, and durability keeps both bytes and
tag on disk before acknowledging a write.

Consistent hashing maps keys and machines onto a circle. A key belongs to its first
five clockwise successors. Example: for machines at positions 10,20,30,40,50,60,
key 55 belongs to 60,10,20,30,40. The ring stays fixed after initial setup, including
when a machine fails. MP3 uses SHA-256 over `mp3/key/` plus a strict UTF-8 key, and
`mp3/node/` plus a stable VM label. For the local test manifest, `put photo image.png`
selects VM03, VM09, VM04, VM08 and VM06 in that order; these VM numbers are the
actual result of the implemented hash and test manifest, not a university deployment.
The complete manifest fixes the VM addresses and ports and has a deterministic
SHA-256 fingerprint. No virtual nodes or per-key owner table are used.

Membership tells us whether each configured process is alive or suspected; it does
not tell us whether a key is stored. If VM06 crashes, peers mark it suspected after
2.5 seconds without heartbeat progress, but VM06 stays in every key's prescribed
owner set. Its SQLite values survive. On restart the process recovers its store,
durably increments its restart generation once, then gossips directly with peers
from the fixed manifest. A newer generation restores ALIVE; delayed gossip from an
older generation is ignored. The implementation adapts MP2's heartbeat, full
snapshot, suspicion and refutation ideas. MP2's original source remains unchanged;
its terminal FAILED lifecycle is unsuitable for MP3 recovery.

Strong consistency means per-key linearizability: every completed operation can be
placed at one instant between its invocation and response, respecting real-time
order, as though one reliable register processed operations. Two-phase multi-writer
ABD is the planned approach. A majority is three of the five owners. A Strong
GET must query a majority, then durably write its selected maximum tag/value back
to a majority before returning. Merely using overlapping R/W sets is insufficient.

Eventual consistency permits a read to return an older value. A coordinator sends
PUT to all reachable owners, waits for W distinct durable acknowledgments, then
continues propagation. A GET selects the maximum version from R distinct qualifying
replies. Default R=W=2. noRR adds no read repair; RR returns after R but continues
gathering replies, discovers the greatest reachable version and repairs stale/missing
copies. A repair must never replace a concurrent newer write with an older value.

## Planned PUT and GET paths (not implemented)

The CLI eventually reads a local input file for PUT, or writes a successful GET to
an output file. A coordinator (any VM) computes fixed owners, sends concurrent RPCs,
and counts distinct, matching responses. A replica validates the request/generation,
reads or conditionally installs the key's versioned bytes, commits durably before a
write acknowledgment, and prints/logs receipt and response events. Coordinator
deadlines bound failures and distinguish an unavailable quorum from an absent key.

Strong PUT first queries a majority and allocates a unique tag above what it saw,
then installs on a majority. Strong GET queries and writes back its maximum on a
majority before answering. Eventual PUT uses its approved timestamp/process-ID tag;
GET uses the first R replies, with RR's repair continuation as described above.
C1–C6 foundation contracts are approved and local storage/types are implemented;
distributed protocol response/allocation rules remain deferred. No whole-operation global lock is permitted.

After a crash, disk contents remain and the process returns on the same fixed ring
identity. The implemented durable restart generation lets peers recognize it as
live while rejecting delayed older gossip. An out-of-date copy is still an owner;
repair or subsequent operations may update it under the selected protocol. MP3
does not promise hinted handoff or anti-entropy. The copied MP2 module cannot yet
perform this recovery. Strong↔Eventual switches must clear values/versions while
keeping identities, and fence delayed old-mode writes/repairs (KD-16).

## Implemented files and function contracts

In the tables, **I** means implemented local behavior, **L** means implemented
legacy behavior with known incompatibilities, and **P** means explicitly pending.
An implementation status never implies the associated architecture is approved.
Caller lists name actual callers in this package; future callers are labeled planned.
Unless stated otherwise functions return None on success and raise on invalid inputs.

### `__init__.py`, `__main__.py`

`__init__.py` only identifies the package; no initialization, threads, network or disk
side effects. `__main__.py` imports `cli.main` and passes its exit code to SystemExit
when run as `python3 -m mp3`. It has no separately defined functions. KD-01/14.

### `config.py` and `config/local.inventory`

Constants encode supplied requirements: cluster size 10, replication 5, Strong
majority 3, eventual defaults 2/2, maximum value size 1,048,576 bytes. They are not a
running configuration. `Machine` is a frozen dataclass whose generated constructor
takes `(node_id: str, host: str, port: int)`; the parser validates rows, not the bare
constructor. It does not compute a ring ID. The example has two localhost rows and
no service starts at either address. Production inventory/port choices await review.

| Function | Inputs → output; internal behavior | Actual callers / components | Status / decisions |
|---|---|---|---|
| `parse_inventory` | text → immutable tuple of Machine; strips # comments/blank lines, requires 3 tokens, ASCII decimal port 1..65535, unique label and endpoint; sorts case-sensitive labels | load_inventory, cli.smoke, ConfigTests; dataclass | I; KD-02/07 |
| `load_inventory` | UTF-8 path → parsed tuple; only reads this local file, propagates IO errors | ConfigTests, README example; pathlib, parse_inventory | I; KD-07 |
| `validate_quorums` | integer R,W → None; rejects bools/nonints/outside 1..5; does not require R+W>N | ConfigTests; replication constant | I; fixed eventual requirement, KD-12 |

Inventory ordering is lexical (e.g. vm10 before vm2), not ring order. The format is
a local bootstrap convenience, not a persistent node identity protocol. Hostnames
are not resolved/canonicalized; a future production manifest must handle aliases.

### `models.py`

`ConsistencyMode` has the three mandated CLI strings; `Status` has OK, NOT_FOUND,
TIMEOUT, ERROR, NOT_IMPLEMENTED. These enums have no custom methods. `Command` is
an immutable name/tuple-of-arguments container built by the parser; it does not
validate itself. `OperationResult` is an immutable local status/message/optional
bytes container. OK with b'' represents present empty data. Non-OK with bytes is
rejected. OK without a value is allowed for future non-read local outcomes; this
does not define a GET response. `Context`, `Version`, `Record`, `ReadResult`, and
`InstallResult` are the local storage contract; they are not RPC encodings. Version
numbers are Python integers and writer IDs use `vm01..vm10/generation` syntax.
`Version.compare` orders `(number, writer_id)` only when kinds match; callers validate
the mode/epoch context separately.

| Method | Inputs → output; internal behavior | Actual callers / components | Status / decisions |
|---|---|---|---|
| `OperationResult.__post_init__` | generated constructor fields → None; validates enum type, bytes-or-None, and failure carries no value | dataclass constructor, pending.execute, DomainTests; Status | I; KD-06/10/15 final response pending |
| `Context.__post_init__` | epoch and mode → None; requires positive exact integer epoch and a ConsistencyMode | Context construction, Store; ConsistencyMode | I; local epoch contract |
| `Version.__post_init__` | kind, nonnegative integer number, writer ID → None; rejects bool numbers and noncanonical/out-of-range VM identities | Version construction, allocator, record decoding | I; C4; no Strong allocator |
| `Version.compare` | another Version of the same kind → -1/0/1 by number then writer ID | Store.install_if_newer, tests | I; C3/C4 |
| `Record.__post_init__` | Version and bytes → None; requires bytes no larger than MAX_VALUE_BYTES, including b'' | install_if_newer, storage decoding | I; C3 |
| `ReadResult`, `InstallResult` | generated immutable field containers; no additional validation or IO | Store local methods | I; C3 |

### `storage.py`

`Store` is a local SQLite-backed replica primitive, not a service. Bootstrap is
explicit and only creates epoch 1 state; opening existing state requires the same
persisted `vm_id`. A nonblocking process-lifetime `flock` on `<db>.lock` prevents
two Store instances/processes from owning one path at once. The parent directory
must already exist. SQLite uses WAL and `synchronous=FULL` on each connection;
each operation opens its own connection, has bounded admission (`capacity`, default
8), and spends a total monotonic budget (`timeout`, default 5 seconds) across
admission, setup, lock waits and the transaction. SQLite's writer lock can still
serialize short writes physically. This makes no throughput claim and has not been
tested against power loss, remote filesystems, or VM storage.

| Function/method | Inputs → output; internal behavior | Actual callers / components | Status / decisions |
|---|---|---|---|
| `StorageError`, `StaleEpochError` | Runtime errors for failed storage guarantees; stale error carries the persisted Context | Store startup, transaction helpers, allocator | I; C3/C4 |
| `_decimal` | nonnegative Python int → canonical decimal string, using small chunks to avoid global conversion limits | stored versions, generation/timestamp persistence and writer ID formatting | I; C3/C4 |
| `_natural` | canonical nonnegative decimal string → Python int; rejects leading zeroes, nonstrings and corrupt metadata | metadata recovery and version decoding | I; C3/C4 |
| `_key` | string → None; strict UTF-8 encodability check, preserving empty/NUL/newline keys | local read/install and recovery validation | I; C1/C3 |
| `_kind` | Context → `strong` or `eventual`; both Eventual modes share a version kind | Store read/install validation | I; C3/C4 |
| `Store.__init__` | path and optional explicit epoch-1 bootstrap, vm_id, timeout and capacity → open recovered Store or StorageError; takes process lock, refuses implicit creation/overwrite, checks SQLite integrity, schema/identity/context/metadata and every record | `open_store`, tests | I; C2/C3; local bootstrap/recovery |
| `Store._transaction` | write flag → context-managed SQLite connection; bounded admission, per-connection WAL/FULL, progress/deadline checks, `BEGIN` or `BEGIN IMMEDIATE`, commit on success and close/rollback on failure | all storage methods | I; C3/C6 local budget |
| `Store._read_context` | DB connection → Context from persisted epoch/mode | startup and context guard | I; C3 |
| `Store._decode` | one SQLite record row → validated Record; parses canonical number and validates tag/value | startup scan and local read/install | I; C3/C4 |
| `Store._check_context` | DB connection and caller Context → actual Context or StaleEpochError; compares exact persisted epoch and mode inside the active transaction | local read/install and allocator | I; C3/KD-16 boundary only |
| `Store.current_context` | no args → persisted Context in a snapshot transaction | tests/future local admission | I; C3 |
| `Store.read_local` | key and keyword Context → ReadResult; atomically checks context and reads one row, returns NOT_FOUND or FOUND (including empty bytes), STALE_EPOCH or ERROR | tests; future local replica handler | I; local read only, no quorum |
| `Store.install_if_newer` | key, Record and keyword Context → InstallResult; validates exact context and kind, compares under one short `BEGIN IMMEDIATE`, installs a higher tag, otherwise returns UNCHANGED, SUPERSEDED, or VERSION_CONFLICT with stored-version evidence; commits before durable success | tests; future replica handler | I; local conditional install only, C3 |
| `Store.start_incarnation` | no args → next persisted positive generation; commits increment before return; caller must invoke once per boot before serving | tests; future startup admission | I; C2 local restart foundation |
| `Store.allocate_eventual_version` | keyword Context → Version; transactionally checks current Eventual context, reserves `max(time.time_ns(), last_timestamp+1)`, commits high-water before returning writer ID `vm_id/generation` | tests; future Eventual coordinator | I; C4 eventual only, no Strong allocation |
| `Store.close`, `__enter__`, `__exit__` | close stops new admissions, waits for active bounded operations, releases process lock; context-manager methods return self/None and call close | tests and future owner | I; C2/C6 local lifecycle |
| `open_store` | path, optional bootstrap, vm_id, timeout, capacity → Store | tests and future local owner | I; C2/C3 |

`install_if_newer` does not decide whether SUPERSEDED qualifies for any quorum;
that protocol rule is deferred. `allocate_eventual_version` is not a Strong tag
allocator. There is no public epoch-advance/clear API: requests with any context
other than the locally persisted exact epoch and mode fail closed. The store does
not know membership, owners, RPC identities, or cluster admission. SIGKILL tests
exercise process-crash visibility before/after SQLite COMMIT in disposable local
directories; they are not a power-loss or distributed durability certification.

### `framing.py`

Derived from our MP1/MP2 `protocol.py`; see the reuse audit for attribution. A frame
starts with four bytes containing the unsigned network-order length, followed by
exactly that many payload bytes. A zero header means a valid empty payload. This
module understands neither keys nor versions. It adds explicit caps and bytearray
accumulation without the original global traffic counters. Callers must configure
finite socket timeouts and connection ownership. It neither opens sockets nor
changes their timeouts. A per-recv inactivity timeout alone cannot bound a frame
when a peer keeps trickling fragments. This utility does not enforce an overall
operation budget: future RPC needs monotonic deadline-aware IO or an approved API
extension that checks remaining time throughout reads/writes (KD-08).
After errors/timeouts close the connection to avoid reading
a remainder as the next message. Final transport/schema/caps are KD-06/07/08.

| Function | Inputs → output; internal behavior | Actual callers / components | Status |
|---|---|---|---|
| `_validate_limit` | max_bytes → None; checks exact int 0..2**32-1 | send_frame, recv_frame; no IO | I |
| `recv_exact` | socket-like object, nonnegative size → bytes; loops partial reads into bytearray, EOF raises ConnectionError, socket timeout propagates; zero returns b'' | recv_frame, FramingTests; sock.recv | I |
| `send_frame` | socket, bytes, explicit max_bytes → None; cap/type checks before any write, then header and payload sendall | smoke.send, FramingTests, ReuseTests; HEADER, _validate_limit | I |
| `recv_frame` | socket, explicit max_bytes → bytes; reads header exactly, rejects excessive length before body, then reads full body | smoke, FramingTests, ReuseTests; recv_exact, HEADER | I |

No header bit pattern is intrinsically malformed beyond a caller's cap: all four-
byte unsigned lengths are representable. Truncated headers/bodies are errors. A
full RPC cap must exceed legal value size by its approved metadata overhead.

### `ring.py`

The file implements the approved SHA-256 encoding and validates a complete immutable
ten-node manifest. Hashes are unsigned 256-bit integers. A key exactly on a node's
position belongs to that node; otherwise the first clockwise successor starts the
five-owner list. Owner selection does not inspect membership state.

| Function/method | Purpose, input → output, behavior, and callers | Status / decision |
|---|---|---|
| `_strict_object`, `_canonical_json` | JSON object pairs or a value → unique-key canonical UTF-8 JSON bytes; reject duplicate fields, NaN and non-UTF-8 data; used by Manifest parsing/fingerprinting | I; C1 |
| `_validate_vm_id` | VM label → None; accepts only vm01..vm10; used by node/key constructors | I; C1 |
| `node_position`, `key_position` | VM label or key → integer SHA-256 ring position using `mp3/node/` or `mp3/key/` prefixes; key encoding is strict UTF-8 with no normalization | I; KD-02/C1 |
| `ring_id_hex` | unsigned 256-bit position → 64 lowercase hex characters | I; C1 |
| `Endpoint.__post_init__`, `Node.__post_init__` | Validate immutable endpoint fields and ensure each ring ID equals the SHA-256 position for its VM label | I; C1 |
| `select_successors` | RingNodes, key ID, count and explicit equality policy → ordered tuple; rejects duplicate labels/positions, sorts, bisects and wraps | I; KD-02 |
| `Manifest.__post_init__` | Ten Nodes → sorted immutable manifest, canonical fingerprint, read-only ID index and fixed ring; rejects missing IDs, duplicate positions/endpoints | I; C1 |
| `Manifest._node_dict`, `from_mapping`, `from_json`, `from_endpoints`, `to_json` | Convert between strict JSON/mappings and validated nodes; require the exact fields; generate canonical output; `from_endpoints` computes all approved ring IDs | I; C1 |
| `Manifest.node` | VM ID → its immutable Node or ValueError for an unknown label | I; C1 |
| `Manifest.owners_for_id`, `Manifest.owners` | Ring key ID or UTF-8 key → exactly five prescribed Nodes in clockwise order; no liveness filtering | I; KD-02/C1 |
| `load_manifest` | UTF-8 JSON path → validated Manifest; contacts no hosts | I; C1 |

`Endpoint`, `Node`, `RingNode`, and `Manifest` are frozen data classes. Manifest
nodes are sorted by VM label for canonical JSON; membership snapshots are separately
sorted by ring position. The local fixture hashes `photo` to
`935ba85381249f6e2d550d84d10f37a80b1336ed7e2da3c1748ce9817fa027b4`, which selects
`vm03, vm09, vm04, vm08, vm06` under the test manifest. These are verified local
results, not a ten-VM placement run. No CLI command or KV request router is wired yet.

### `membership.py`

The active MP3 adapter uses two states: ALIVE and SUSPECTED. A suspicion is kept
indefinitely until a higher generation or a newer incarnation refutes it. Each
message carries a complete ten-member snapshot and manifest fingerprint over a
four-byte length-framed TCP connection. The payload cap is 65,536 bytes; membership
counter fields have a 256-decimal-digit resource bound. The latter is a defensive
implementation limit, not a course specification limit. Worker admission and
gossip fanout are bounded.

| Function/method | Purpose, input → output, behavior, and callers | Status / decision |
|---|---|---|
| `_decimal`, `_parse_decimal`, `_unique_object` | Encode/decode canonical counter strings and parse JSON without duplicate fields; reject leading zeros and counters over the documented resource cap | I; C2 |
| `Member.__post_init__` | Validate stable VM ID, generation/incarnation/heartbeat and ALIVE/SUSPECTED state | I; C1/C2 |
| `GossipEnvelope.__post_init__`, `to_bytes`, `from_bytes` | Validate sender, fingerprint and exactly ten unique member rows; serialize sorted ring-position snapshots; validate schema, decimal counters and 65,536-byte cap | I; C2 |
| `MembershipTable.__init__` | Manifest, local VM, monotonic clock and suspicion threshold → initial ten-member table; peers start at generation 0 until heard | I; KD-03/04 |
| `set_local_generation`, `local_generation` | Install/read the durable generation from storage; installation is one-time per table and is required before service starts | I; C2 |
| `snapshot`, `list_mem_ids` | Return all ten members by ring position, or six-field diagnostic rows `(vm_id, generation, incarnation, heartbeat, state, ring_id)` sorted by ascending numeric `ring_id` (`nodeID`) | I; C1/SPEC |
| `owners` | Key → manifest's fixed five owners; status is ignored | I; C1 |
| `envelope` | Current full snapshot → validated GOSSIP message using the manifest fingerprint and local generation | I; C2 |
| `tick_heartbeat`, `check_suspicions` | Increment local heartbeat; use injected monotonic elapsed time to mark silent ALIVE peers SUSPECTED at the configured threshold without deleting them | I; KD-04 |
| `merge_gossip` | Valid full snapshot → monotonic merge events; checks the state-dependent self-generation invariant before mutating any row, rejects invalid claims atomically, ignores stale generations, accepts newer generations, makes equal-generation suspicion win, and self-refutes with a higher incarnation | I; KD-03/04 |
| `next_targets` | Fanout → rotating bounded peers including suspects, independent of owner selection | I; KD-05 |
| `_recv_exact_deadline`, `_recv_frame_deadline` | Socket, byte count/deadline → exact bytes; enforce the membership frame cap and an overall monotonic receive deadline | I; KD-06/08 |
| `exchange_gossip` | Endpoint, envelope, manifest and timeout → peer's full snapshot; one TCP connection, length frame, fixed deadline; no special introducer | I; KD-05/06 |
| `MembershipService.__init__`, `start`, `_accept_loop`, `_handle`, `stop` | Bind the configured membership endpoint, admit bounded service workers, validate/merge gossip and reply with a snapshot; shutdown closes listener and drains workers | I; KD-05/07 |
| `MembershipRuntime.__init__`, `generation`, `start` | Store, manifest and detector/gossip settings → runtime; on start recover context, reserve `Store.start_incarnation()` once per store path per process, then permit listener/background loop | I; C2 |
| `MembershipRuntime.gossip_round`, `_run`, `stop` | Tick/check, probe up to configured fanout, merge responses, repeat at configured interval, stop bounded workers/listener | I; KD-04/05 |

For recovery when VM01 (the usual introducer) is unreachable, each runtime contacts
peers directly from the immutable ten-node manifest. A local TCP test left VM01 down
and verified VM02/VM03 learned VM06's newer generation. No real VM cluster was used.
`MembershipRuntime.start()` guards generation allocation both per runtime and with
a process-local store-path registry. This prevents two runtime objects in one Python
process from accidentally incrementing the same disk generation twice. This guard
is process-local; an actual OS process restart begins with an empty registry and
gets its next durable generation from SQLite. The runtime uses Batch 1
`Store.current_context()` and `Store.start_incarnation()`;
the caller closes its Store after stopping the runtime. It does not implement KV RPC,
join/leave, consistency epochs, automatic membership persistence, or cluster mode
changes. The full membership network runs locally in tests only at this stage.

### `eventlog.py`

Opt-in logging prototype; no handlers installed at import. JSON is escaped into one
physical line, making a newline inside an event parseable and grep-compatible.
These testable utilities do not yet provide required real operation events or MP1
query integration. No values should be logged. File flushes are not KV durability.

| Function/method | Inputs → output; internal behavior | Actual callers / components | Status / decisions |
|---|---|---|---|
| `JsonFormatter.format` | LogRecord → JSON string with UTC timestamp, level, event, optional node_id/request_id; escapes Unicode/newlines | logging handlers created by configure_logging; datetime/json | I prototype; KD-17 |
| `configure_logging` | fresh Logger, path, keyword stream → None; refuses existing handlers, appends UTF-8 file+console with formatter, INFO level, no propagation; caller closes/removes handlers | LogTests; standard logging/pathlib | I prototype; KD-17 |

Final schema must add actual key/version/mode/phase/replica/outcome/reachability and
repair details where required. Current utility is not a claimed complete trace.

### `pending.py`

`DECISIONS` maps eight command names to their pending dependencies. This is the
single stubbed dispatch boundary; there are no separate fake storage/protocol APIs.

| Function | Inputs → output; internal behavior | Actual callers / components | Status / decisions |
|---|---|---|---|
| `execute` | Command → OperationResult(NOT_IMPLEMENTED, reason, None); every invocation is guarded, including unknown command if called directly; no IO | cli.main, CliTests; DECISIONS, local result types | **P**: put KD-06/08/09/10/11/12/15; get also KD-13; ls KD-02/03; liststore KD-09; direct read KD-06/08/09/10; membership IDs KD-02..05; multiput KD-06/08/10/11/12/14; mode KD-16 |

Completion criteria: humans approve the relevant design and shared contracts;
main agent implements actual dispatch/service/file handling with per-command
acceptance tests, then removes only each supported guard. A parsed command or
placeholder result must never count as a successful PUT/GET or a performed switch.

### `cli.py`

The local parser validates syntax; it does not read/write user files. ARITY defines
fixed argument counts; HELP explains pending commands. Shell-style shlex parsing
supports quoted keys/paths, and a small separate grammar accepts parenthesized
multiput examples. Bare multiput accepts VM1..VM10 labels only; exact targets/value
source/completion grammar await official-spec reconciliation KD-14. The parser
does not impose an unapproved key-length/normalization policy.

| Function | Inputs → output; internal behavior | Actual callers / components | Status / decisions |
|---|---|---|---|
| `parse_command` | string → Command or ValueError; validates known names/arity, nonempty/NUL-free args, mode enum and distinct valid multiput VM labels | main, CliTests; shlex/re, ARITY, ConsistencyMode | I syntax only; KD-14 |
| `smoke` | none → None or exception; local socketpair, finite timeouts, 1 MiB binary roundtrip + empty frame, inventory sorting and wraparound check; no TCP listener/VM | main(--smoke), CliTests; framing, config, ring, threading | I local check; KD-06/07 not finalized |
| `smoke.send` (nested helper) | captured sender/payload → None; sends full/empty frames on daemon helper thread, collects exceptions so smoke can fail; joined with finite limit | smoke's thread target; send_frame, local errors list | I local test helper |
| `main` | optional argv list (default sys.argv[1:]) → int; help/smoke exit 0, invalid command JSON ERROR exit 2, guarded JSON NOT_IMPLEMENTED exit 3; shlex.join preserves shell argument boundaries except one full parenthesized command | __main__, CliTests; parse_command, smoke, pending.execute, json | I bootstrap dispatch, distributed behavior P; KD-14 |

### `legacy_membership.py`

This is a **byte-identical copy of our previous MP2 `membership.py`**, owned by
Krishna for eventual adaptation. No node startup/gossip/server code is copied.
It uses `time.time()` and threading.Lock. ALIVE, SUSPECTED, FAILED, LEFT are strings;
FAILED/LEFT are terminal. Callback code inside merge/timeout locks must not re-enter
the membership object. It lacks validation of arbitrary network fields, durable
incarnations and MP3 recovery. No runtime uses it; only characterization tests do.

| Function/method | Inputs → output; internal behavior | Actual callers / components | Status / decisions |
|---|---|---|---|
| `Entry.__init__` | node_id, heartbeat=0, incarnation=0, status=ALIVE → new entry; wall-time last_updated, suspected_since=None | MembershipList constructor/merge; time.time | L; KD-03/04 |
| `Entry.to_dict` | self → fresh identity/heartbeat/incarnation/status dict; local timers excluded | snapshot; entry fields | L; KD-03/04 |
| `MembershipList.__init__` | self_id, optional on_event → object; owns lock, self Entry, suspects list, suspicion=False/drop_rate=0, default no-op lambda callback | ReuseTests; Entry/threading | L; KD-03/04 |
| default no-op callback (lambda) | message → None; ignores event if caller supplied no observer | mark_self_left, merge, run_timeout_check; no IO | L; KD-17 |
| `tick_heartbeat` | self → None; increments own heartbeat and update time under lock | ReuseTests; time/entries; planned gossip loop | L; KD-04/05 |
| `snapshot` | self → fresh list of entry dicts including terminal entries, protected by lock | ReuseTests; Entry.to_dict; planned gossip | L; KD-03/04 |
| `alive_peers` | self → nonself IDs excluding terminal entries, therefore includes SUSPECTED | ReuseTests; entries/lock; planned gossip | L; KD-04/05 |
| `all_known_peers` | self → all nonself IDs, including FAILED/LEFT | ReuseTests; entries/lock | L; KD-03 |
| `mark_self_left` | self → None; writes own LEFT under lock then callback outside lock | no current runtime caller; planned legacy CLI only | L; MP3 dynamic leave not required; KD-03 |
| `set_suspicion_mode` | enabled → None; stores flag under lock, no input validation | ReuseTests; planned control path | L; KD-04 |
| `set_drop_rate` | rate → None; stores rate under lock, caller must validate | no current caller; planned test injection if approved | L; KD-04/05 |
| `merge` | iterable entry dicts → bool self_refuted; learns new nodes, refutes own suspicion with higher incarnation, updates winning peer heartbeat/status/timers and callbacks under lock | ReuseTests; Entry, _incoming_wins, time/lock; planned gossip | L; KD-03/04 |
| `_incoming_wins` | existing Entry, incoming dict → bool; rejects all updates to terminal existing state, then prioritizes incoming terminal status, incarnation, suspicion, heartbeat | merge; no IO | L; incompatible with MP3 restart, KD-03/04 |
| `run_timeout_check` | self → None; stale ALIVE→FAILED or SUSPECTED, suspected→FAILED after confirm timeout; records suspects/callbacks under lock | ReuseTests; wall clock/constants; planned timer loop | L; KD-04 |
| `get_suspects_log` | self → copy of local (ID,time) suspicion list under lock | no current runtime caller; planned diagnostics | L; KD-04/17 |
| `list_mem` | self → tuples (ID,heartbeat,incarnation,status), hides FAILED/LEFT | ReuseTests; entries/lock | L; must not construct fixed MP3 ring from this, KD-03 |

Legacy callback lambda is the sole nested callable not named as a normal method.
New `Entry(..., status=SUSPECTED)` sets no timer until merge fills it; arbitrary
direct state manipulation is not an approved API. Characterization is intentionally
small; it does not establish full MP2 correctness or recovery readiness.

## Test files and meaningful test methods

All test methods below are **implemented local tests**, called by unittest discovery,
take only `self`, return None, and raise assertion failures on a violated condition.
They construct their own fixture inputs rather than accept production state. Their
outputs are the unittest pass/fail report, not KV outcomes. Decision IDs identify
the contract being checked; protocol work beyond the approved local foundation is
not implied. Constructors and setUp helpers are called by unittest or their named
test methods.

### `tests/test_storage.py` — local SQLite and version foundation (C2/C3/C4/C6)

`StorageTests.setUp` creates a fresh TemporaryDirectory and explicitly bootstraps
epoch 1 Eventual-noRR state. Cleanup closes the Store and removes the temporary
directory. `rec` constructs an Eventual Record. Crash tests launch only disposable
local Python children for the SIGKILL boundary check; no network or VM is involved.

| Test method | Fixture/internal behavior → expected assertion |
|---|---|
| `test_missing_empty_full_size_and_keys` | Missing key differs from empty value; empty/NUL/Unicode/newline keys and empty, arbitrary, 1 MiB values round-trip exactly |
| `test_conditional_statuses_and_stored_evidence` | Higher install, lower SUPERSEDED, exact UNCHANGED, equal-tag/different-bytes VERSION_CONFLICT, then newer install; stored version and durable flags match outcomes |
| `test_concurrent_same_key_highest_wins` | Forty concurrent versions on one key → every result durable and highest version remains stored |
| `test_equal_counter_writer_tiebreak` | Same number with different writer IDs → lexical writer ordering and lower tag is SUPERSEDED |
| `test_allocator_repeated_backward_and_concurrent` | Frozen/repeated/backward clock then concurrent calls → unique strictly increasing numbers |
| `test_reopen_generation_and_timestamp` | Close/reopen preserves data and allocator high-water; next incarnation and timestamp advance after restart |
| `test_epoch_and_mode_rejection` | Wrong epoch or mode for reads, installs and allocation → stale-context outcomes/errors and no record mutation |
| `test_epoch_check_against_persisted_not_cached_context` | Disposable direct metadata change to epoch 2 → old-context install is rejected using persisted state |
| `test_strong_local_tags_without_strong_allocator` | Very large Strong local version can be installed/read; Eventual allocator refuses Strong context |
| `test_bad_input_and_closed_store_errors` | Invalid keys/records/version forms and closed store → local ERROR or constructor rejection; over-limit value is rejected |
| `test_bootstrap_rejects_existing_missing_wrong_vm` | No implicit creation, no overwrite/rebootstrap, exclusive path lock, persisted VM identity check, then valid reopen |
| `test_invalid_database_and_metadata_fail_closed` | Non-SQLite file remains unchanged and rejected; noncanonical metadata causes startup rejection |
| `test_sqlite_settings_and_lock_timeout` | Per-connection WAL/FULL settings observed; externally held SQLite write lock returns bounded ERROR without durable success |
| `test_commit_disk_error_rolls_back_no_durable_ack` | Injected COMMIT failure → ERROR, durable=False, and no visible row |
| `test_reader_progress_while_writer_waits_and_close_drains` | External SQLite writer lock blocks a local install while unrelated read completes; close waits for accepted work and reopen confirms no uncommitted row |
| `test_same_tag_conflicting_concurrent_values` | Concurrent installs of different bytes with one tag → exactly one INSTALLED and one VERSION_CONFLICT; stored value is one contender |
| `test_crash_before_and_after_commit_and_allocator_commit` | Own SIGKILL child before/after record COMMIT and after allocator COMMIT → precommit row absent, committed row/high-water survives reopen |

### `tests/test_framing.py` — framing (KD-06/07/08)

`FragmentSocket.__init__(data=b'', fragment=1)` stores in-memory incoming bytes,
maximum chunk size, sent bytearray and read-size history. `.recv(size)` returns
up to min(size, fragment) bytes and consumes them, simulating split reads/EOF.
`.sendall(data)` appends to the sent buffer. FramingTests calls this fake; it never
uses real network hosts. `FramingTests` uses send_frame/recv_frame/recv_exact.

| Test method | Fixture/internal behavior → expected assertion |
|---|---|
| `test_arbitrary_bytes_and_empty` | Empty, NUL/FF/newline/CR and all byte values; one-byte fragments → exact framed roundtrip |
| `test_one_mib_with_fragmentation` | 1 MiB repeating all bytes, chunks <=997 → exact payload |
| `test_multiple_frames` | Three consecutive frames including empty → boundaries/order preserved |
| `test_truncated_header_and_body` | Empty/short header and incomplete body → ConnectionError |
| `test_oversized_header_does_not_read_body` | 2**32-1 announced bytes vs 1 MiB cap → reject before consuming body |
| `test_send_rejects_before_any_write` | Oversized outbound bytes → no header/body emitted |
| `test_bad_limits_and_types` | Invalid cap/size/bool/text payload → ValueError/TypeError |
| `test_real_socket_pair_and_timeout` | Local pair with finite deadlines → binary transfer then propagated timeout |

### `tests/test_foundations.py` — ring/config/results/logs

`RingTests.setUp` creates six nodes at positions 10..60 for each method. Test
components: select_successors (KD-02), inventory/quorums (KD-07/12), local result
types (KD-15), formatter/handlers (KD-17). No distributed server runs.

| Class.method | Fixture/internal behavior → expected assertion |
|---|---|
| `RingTests.test_wraparound_five_distinct` | Reversed input, key55 → 6,1,2,3,4 without duplicates |
| `RingTests.test_supplied_equality_policy` | key20, explicit inclusive/strict → starts at 2 or 3 |
| `RingTests.test_all_nodes_and_far_past_last` | key9999, count6 → full wrapped ring |
| `RingTests.test_invalid_rings` | Empty ring, duplicate labels/positions, negative/bool position, empty label → rejected |
| `RingTests.test_invalid_key_count_and_equality` | Negative/bool key, invalid count, nonbool equality → rejected |
| `RingTests.test_equality_policy_has_no_default` | Omitted equality convention → TypeError |
| `ConfigTests.test_deterministic_order_and_comments` | Permuted/commented rows → same sorted immutable records |
| `ConfigTests.test_invalid_inventory` | Empty, malformed, duplicate and bad-port inputs → rejected |
| `ConfigTests.test_load_local_file` | Temporary UTF-8 inventory with ::1 token → exact host read, no connection |
| `ConfigTests.test_independent_eventual_quorums` | All 25 legal R/W pairs plus bad values → legal accepted independently, bad rejected |
| `DomainTests.test_empty_is_distinct_from_missing` | OK b'' vs NOT_FOUND None → different states |
| `DomainTests.test_invalid_results` | Failure-with-value/text-value/nonenum-status → rejected |
| `LogTests.test_json_lines_append_and_console` | Temporary existing log and StringIO console; Unicode/newline extras; close/remove handlers finally → parseable appended identical records, fresh-logger guard |

### `tests/test_ring_membership.py` — fixed ring and restart-aware membership (KD-02/03/04/05, C1/C2)

Fixtures build a deterministic ten-node loopback manifest. Membership timing uses a
fake monotonic clock except for bounded local TCP exchanges. Restart tests use
temporary SQLite files and kill only their own subprocesses. No test connects to a
university VM.

| Test method | Fixture/internal behavior → expected assertion |
|---|---|
| `RingTests.test_sha256_known_vectors_and_fixed_node_positions` | Known SHA-256/key/node outputs, 64-digit display and invalid UTF-8/VM labels |
| `RingTests.test_clockwise_wraparound_and_inclusive_exact_position` | Exact match, strict successor, wraparound and duplicate ring position rejection |
| `RingTests.test_manifest_is_immutable_canonical_and_order_independent` | Input order normalizes to same fingerprint/JSON/owners; frozen fields and private lookup are immutable |
| `RingTests.test_manifest_rejects_bad_ids_hashes_endpoints_duplicates_and_shape` | Malformed identity/hash, duplicated endpoints/JSON fields, invalid host/port → rejected |
| `RingTests.test_owner_selection_matches_in_separate_process` | Fresh Python process given the same manifest/key → identical five owners |
| `RingTests.test_failed_membership_does_not_change_prescribed_owners` | Fake clock suspects peers → all remain in ring; snapshot ring order and diagnostic node-ID order are checked |
| `MembershipTests.test_gossip_roundtrip_and_schema_validation` | Full snapshot encode/decode succeeds; wrong fingerprint, short snapshot and oversized counter fail |
| `MembershipTests.test_suspicion_is_retained_and_new_generation_recovers` | Silent peer becomes SUSPECTED, remains, then newer generation returns ALIVE |
| `MembershipTests.test_delayed_old_generation_cannot_override_recovery` | Late old-generation high-heartbeat gossip cannot undo newer generation |
| `MembershipTests.test_equal_incarnation_suspicion_wins_and_newer_incarnation_refutes` | Equal-incarnation suspicion wins; same-incarnation heartbeat cannot refute; higher incarnation can |
| `MembershipTests.test_self_suspicion_is_refuted_with_a_strictly_newer_incarnation` | Current process advances its incarnation and stays ALIVE after self-accusation |
| `MembershipTests.test_newer_self_generation_claim_fails_closed` | Gossip claiming a newer generation for this already-running process → rejected |
| `MembershipTests.test_concurrent_merges_are_serialized_and_generation_monotonic` | Ten concurrent updates → no race errors and highest generation retained |
| `MembershipTests.test_target_rotation_is_bounded_and_does_not_need_introducer` | Fanout remains capped at three and target selection rotates across peers |
| `MembershipTests.test_service_rejects_unbounded_or_invalid_worker_configuration` | Invalid worker capacity, nonfinite deadlines and detector thresholds → rejected |
| `MembershipTests.test_repeated_sigkill_restart_uses_durable_generation_and_keeps_values` | Two local SIGKILL restarts advance generation 1→2→3 and preserve stored bytes |
| `MembershipTests.test_tcp_gossip_recovers_without_introducer` | Loopback VM02/VM03 learn VM06 generation 2 while VM01 is unavailable |
| `MembershipTests.test_service_start_requires_generation_before_network_admission` | Service refuses to bind until durable restart generation is installed |
| `MembershipTests.test_runtime_reserves_only_one_generation_per_store_per_process` | A second runtime object cannot increment the same store generation in one process |

### `tests/test_cli.py` — syntax and guard (KD-14 and pending dependencies)

CliTests uses parse_command/main/execute/smoke, captures stdout, and uses a temporary
file for preservation checks. No value file is read by a distributed operation.

| Method | Fixture/internal behavior → expected assertion |
|---|---|
| `test_valid_commands_and_guard` | All command forms/modes → NOT_IMPLEMENTED, decision explanation, no value |
| `test_quotes_and_multiput_forms` | Quoted spaces and two multiput forms → normalized Command |
| `test_invalid_commands` | Bad name/arity/mode/VM, duplicates, empty/NUL arg, broken quote/parentheses → ValueError |
| `test_main_exit_codes_and_json` | Valid pending/invalid syntax/parenthesized input → JSON status and exit 3/2 |
| `test_pending_commands_do_not_touch_files` | Existing temporary GET/PUT target → guarded exit3 and unchanged bytes |
| `test_help_and_smoke` | CLI --help and --smoke with captured output → exit0 |
| `test_smoke` | Direct local smoke function → no exception |

### `tests/test_reuse.py` — prior-code behavior (KD-03/04/06)

`load_reference(relative)` takes a repository-relative source path, loads its module
under an isolated name, returns the module; temporarily disables bytecode writes.
Called only with side-effect-free MP1/MP2 protocol files by ReuseTests; never load
node.py/server.py/experiments with this helper. ReuseTests uses the quarantined
membership class, socketpair framing, pathlib, importlib and mock.patch clocks.

| Method | Fixture/internal behavior → expected assertion |
|---|---|
| `test_legacy_copy_is_byte_identical` | Read copied/original membership bytes → identical |
| `test_both_original_protocols_interoperate` | Each original protocol, both directions over local socketpair, empty/NUL/FF → exact raw framing compatibility |
| `test_heartbeat_and_snapshot_are_independent` | Increment own heartbeat, mutate snapshot dict → internal counter remains correct |
| `test_failed_entry_rejects_restart_known_limitation` | FAILED peer then much newer ALIVE → remains FAILED, retained in all_known/snapshot and omitted in alive/list_mem; documents blocker |
| `test_refutation_increments_incarnation` | Own suspicion incarnation3 → returns refuted True, incarnation4 |
| `test_timeout_lifecycle_with_controlled_clock` | Learn peer at100, checks at102/104 → SUSPECTED then FAILED |

## Pending components have no imaginary implementation

Local storage and Eventual allocation exist as described above. There are no
RPC-service, hashing-construction, Strong/Eventual coordinator, repair, mode manager,
benchmark, plotting or deployment implementations yet. Their proposed
responsibilities/contracts/dependencies are in the plan and KD-01..18, and all
operation entrypoints terminate at `pending.execute`. Add source files only when
their responsibility and approved decisions justify them; document every meaningful
function here when implemented. No fabricated benchmark/report content exists.

Final Batch 1 refinements: timeout rejects booleans; decimal encoding/decoding
roundtrips a 2**15000 Strong counter without changing global Python limits.
Invalid context types return local ERROR; valid mismatched contexts return STALE_EPOCH.
The full 51-test suite passed after these refinements. No power-loss or VM proof is
claimed by local SIGKILL tests.
