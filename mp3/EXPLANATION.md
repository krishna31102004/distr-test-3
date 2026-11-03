# Understanding MP3 and its verified Batch 1–4 foundations

This guide describes the actual files. Step 2 storage, fixed ring/membership,
single-replica RPC, and the integrated node runtime are locally verified. Step 3
Batch 1 provides shared schema-2 evidence/scheduling and a durable Strong allocator;
Batch 2 implements locally verified programmatic Strong ABD PUT/GET; Batch 3 adds
independently reviewed programmatic Eventual-noRR PUT/GET. **Eventual-RR/read repair,
consistency-mode switching and the final CLI operations are still pending.** A
local replica read/install or one coordinator dispatch is not a distributed GET/PUT.

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

## Client command path (not implemented)

The CLI eventually reads a local input file for PUT, or writes a successful GET to
an output file. A coordinator (any VM) computes fixed owners, sends concurrent RPCs,
and counts distinct, matching responses. A replica validates the request/generation,
reads or conditionally installs the key's versioned bytes, commits durably before a
write acknowledgment, and prints/logs receipt and response events. Coordinator
deadlines bound failures and distinguish an unavailable quorum from an absent key.

Strong PUT/GET and Eventual-noRR PUT/GET now use the programmatic APIs documented
below. Final file-oriented CLI commands do not call these APIs yet. Eventual-RR and
read repair remain future implementation. C1–C6 contracts and KD-11/KD-12 are
approved; no whole-operation global lock is permitted.

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
| `Store._transaction` | write flag and optional absolute monotonic deadline → context-managed SQLite connection; uses the earlier of local timeout and caller deadline across admission, WAL/FULL setup, lock waits, progress checks and pre-commit check; commits on success and closes/rolls back on failure | storage methods and replica RPC | I; C3/C6 |
| `Store._read_context` | DB connection → Context from persisted epoch/mode | startup and context guard | I; C3 |
| `Store._decode` | one SQLite record row → validated Record; parses canonical number and validates tag/value | startup scan and local read/install | I; C3/C4 |
| `Store._check_context` | DB connection and caller Context → actual Context or StaleEpochError; compares exact persisted epoch and mode inside the active transaction | local read/install and allocator | I; C3/KD-16 boundary only |
| `Store.current_context` | optional absolute monotonic deadline → persisted Context in a snapshot transaction using the earlier local/caller deadline | tests and ReplicaService stale-context response | I; C3/C6 |
| `Store.read_local` | key, keyword Context and optional absolute deadline → ReadResult; atomically checks context and reads one row, returns NOT_FOUND or FOUND (including empty bytes), STALE_EPOCH or ERROR | tests, ReplicaService | I; local read only, no quorum |
| `Store.install_if_newer` | key, Record, keyword Context and optional absolute deadline → InstallResult; validates exact context and kind, compares under one short `BEGIN IMMEDIATE`, installs a higher tag, otherwise returns UNCHANGED, SUPERSEDED, or VERSION_CONFLICT with stored-version evidence; commits before durable success | tests, ReplicaService | I; local conditional install only, C3/C6 |
| `Store.start_incarnation` | no args → next persisted positive generation; commits increment before return; caller must invoke once per boot before serving | tests; future startup admission | I; C2 local restart foundation |
| `Store.allocate_eventual_version` | keyword Context → Version; transactionally checks current Eventual context, reserves `max(time.time_ns(), last_timestamp+1)`, commits high-water before returning writer ID `vm_id/generation` | tests; `EventualNoRRCoordinator._run_put` | I; C4/KD-12 eventual only |
| `Store.allocate_strong_version(*, context, observed_max, deadline=None)` | Strong Context, optional queried maximum and deadline → committed Version above observed and local durable high-water; writer ID uses the current durable VM generation; never holds a lock over RPC | tests; `StrongCoordinator._run` during Strong PUT | I; approved KD-10/11 allocator; local protocol integration |
| `Store.close`, `__enter__`, `__exit__` | close stops new admissions, waits for active bounded operations, releases process lock; context-manager methods return self/None and call close | tests and future owner | I; C2/C6 local lifecycle |
| `open_store` | path, optional bootstrap, vm_id, timeout, capacity → Store | tests and future local owner | I; C2/C3 |

`install_if_newer` does not decide whether SUPERSEDED qualifies for any quorum;
that protocol rule is deferred. `allocate_eventual_version` is the Eventual tag
allocator; `allocate_strong_version` is a local durable tag primitive, not a Strong coordinator. There is no public epoch-advance/clear API: requests with any context
other than the locally persisted exact epoch and mode fail closed. The store does
not know membership, owners, RPC identities, or cluster admission. SIGKILL tests
exercise process-crash visibility before/after SQLite COMMIT in disposable local
directories; they are not a power-loss or distributed durability certification.

### `framing.py`

Derived from our MP1/MP2 `protocol.py`; see the reuse audit for attribution. A frame
starts with four bytes containing the unsigned network-order length, followed by
exactly that many payload bytes. A zero header means a valid empty payload. This
module understands neither keys nor versions. The legacy functions retain their
original MP1/MP2-compatible framing behavior. Batch 3 adds deadline-aware exact
reads, writes and frame helpers. These set the socket timeout to the remaining
absolute monotonic budget before each blocking operation, so trickled bytes do not
reset the total budget. The RPC layer owns its separate JSON/raw-value envelope and
closes a connection after malformed or incomplete messages.

| Function | Inputs → output; internal behavior | Actual callers / components | Status |
|---|---|---|---|
| `_validate_limit` | max_bytes → None; checks exact int 0..2**32-1 | send_frame, recv_frame; no IO | I |
| `recv_exact` | socket-like object, nonnegative size → bytes; loops partial reads into bytearray, EOF raises ConnectionError, socket timeout propagates; zero returns b'' | recv_frame, FramingTests; sock.recv | I |
| `send_frame` | socket, bytes, explicit max_bytes → None; cap/type checks before any write, then header and payload sendall | smoke.send, FramingTests, ReuseTests; HEADER, _validate_limit | I |
| `recv_frame` | socket, explicit max_bytes → bytes; reads header exactly, rejects excessive length before body, then reads full body | smoke, FramingTests, ReuseTests; recv_exact, HEADER | I |
| `_remaining` | finite monotonic deadline → seconds remaining or TimeoutError; rejects expired deadlines | deadline-aware helpers | I; KD-08/C6 |
| `recv_exact_deadline` | socket, byte count, absolute deadline → bytes; repeats partial reads but recomputes the same deadline each time | RPC codecs, deadline framing tests | I; KD-08/C6 |
| `send_all_deadline` | socket, bytes, absolute deadline, optional `before_send` callback → None; applies remaining budget before calling the callback immediately before `sendall` | RPC codecs; callback lets the client mark possible transmission only after the deadline check | I; KD-08/C6 |
| `recv_frame_deadline`, `send_frame_deadline` | socket/payload, cap, deadline → received bytes or None; compatibility framing with a total deadline | available utility; RPC uses the same lower-level helpers for its two-length envelope | I; KD-08/C6 |

No header bit pattern is intrinsically malformed beyond a caller's cap: all four-
byte unsigned lengths are representable. Truncated headers/bodies are errors. A
full RPC cap includes the approved metadata overhead; see `rpc.py` below.

### `rpc.py`

This module defines one replica request and one response per TCP connection. It does
not gather replica sets or implement Strong/Eventual protocols. The envelope is an
outer four-byte length, an inner four-byte JSON length, UTF-8 JSON metadata, then
unchanged value bytes. Caps are 16,384 metadata bytes, 1 MiB value bytes and 1,064,964
outer payload bytes. Validation rejects duplicate JSON keys, extra/missing fields,
wrong schema/identities, noncanonical values, contradictory status fields and trailing
bytes before a replica operation can reach storage.

| Function/type | Purpose, input → output, behavior and callers | Status |
|---|---|---|
| `RpcOperation`, `RpcStatus`, `RpcOutcomeKind` | Enumerate the two local replica operations, response statuses and client outcome categories | I; C5 |
| `RpcProtocolError` | Marks malformed or contradictory wire messages; raised by validators/codecs | I; C5 |
| `_is_int`, `_validate_request_id`, `_validate_vm_id`, `_parse_decimal`, `_format_decimal` | Validate exact JSON integer types, lowercase UUID4, VM labels and canonical decimal strings; avoid Python's decimal digit limit on large versions | I; C5 |
| `_version_from_wire`, `_version_to_wire` | Convert nullable `{kind, number, writer_id}` to/from `Version`, checking mode kind and manifest writer identity | I; C3–C5 |
| `_unique_object`, `_reject_json_constant`, `_decode_json`, `_encode_json` | Parse strict UTF-8 JSON with duplicate-key/nonfinite-number rejection and enforce the metadata cap | I; C5 |
| `_mode`, `_operation`, `_status` | Convert exact wire strings to approved enums; unknown names fail closed | I; C5 |
| `ReplicaRequest.__post_init__`, `.value_len`, `._validate_version_kind`, `.validate_manifest`, `.to_metadata`, `.from_metadata` | Validate schema-1 local and schema-2 protocol-phase identity, operation/version/value shape, UUID fields and manifest; convert strict fields to/from metadata | I; C5/KD-15 |
| `ReplicaResponse.__post_init__`, `.value_len`, `.validate_for`, `.to_metadata`, `.from_metadata` | Check status evidence and bind schema, request/operation IDs, phase, target, manifest, dispatch-time generation and context; enforce stored-version evidence | I; C3/C5/KD-15 |
| `RpcOutcome` | Carries outcome kind, optional validated response, bounded error text and whether an INSTALL may have completed without acknowledgment; read-only failures never set the write-completion flag | I; C5/C6 |
| `new_operation_id`, `new_request` | Create canonical UUID4 operation IDs and fresh per-RPC request IDs; schema-2 requests require protocol phase and an operation ID shared by the caller | I; C5/KD-08/15 |
| `_encode_payload`, `encode_request`, `encode_response` | Valid metadata/body → capped `JSON length + JSON + bytes` payload after manifest/schema checks | I; C5 |
| `_recv_payload`, `_require_eof`, `receive_request`, `receive_response` | Socket/deadline → validated metadata/body/request/response; checks both declared lengths before body reads and rejects truncation/trailing bytes | I; C5/C6 |
| `send_request`, `send_response` | Socket plus validated type/manifest/deadline → send exactly one outer frame under one absolute deadline | I; C5/C6 |
| `_request_error` | Outcome kind and error → bounded error-only result | I; C5 |
| `ReplicaClient.__init__` | Manifest and matching `MembershipTable` → reusable per-node RPC client; resolves fixed KV endpoints once, caps cached candidates at eight, preserves resolution errors as per-target outcomes | I; C1/C2/C5 |
| `ReplicaClient._connect` | Node and absolute deadline → connected socket; tries cached numeric candidates using the remaining budget before each attempt, without blocking DNS in the operation | I; C5/C6 |
| `_lease_wait`, `_lease_connect`, `_lease_send_all`, `_lease_recv_exact`, `_lease_receive_response` | Lease-aware nonblocking connect and exact frame I/O; poll at most 50ms between lease checks; honor cancellation and evolving deadline; one attempt, no retry | I; KD-08/12/14 |
| `ReplicaClient.call_replica` | Prescribed Node, request and absolute deadline → one response or typed outcome; snapshots/accepts captured target generation, supports schema-2 activation and an optional Eventual PUT lease, no retry, validates response evidence, preserves possible-commit uncertainty | I; C5/C6, KD-08/15 |

`READ_LOCAL` returns FOUND with actual version and raw bytes, or NOT_FOUND with no
record; empty bytes remain FOUND. INSTALL returns the version actually stored.
For example, trying to install v6 when v7 is present returns SUPERSEDED/stored_version
v7. Whether that evidence qualifies a later quorum remains a protocol decision.

### `replica.py`

`ReplicaService` accepts requests at the manifest's KV endpoint using a fixed worker
pool and bounded queue (defaults 32 active and 64 queued). It verifies the complete
request and sender generation before storage dispatch, and delegates only local
READ_LOCAL or INSTALL_IF_NEWER. It does not schedule replication or repair.

| Function/type | Purpose, input → output, behavior and callers | Status |
|---|---|---|
| `ShutdownReport` | Number of unfinished requests and whether handlers joined after bounded shutdown | I; C6 |
| `ReplicaService.__init__` | Store, manifest, membership and worker/deadline limits → service; validates shared identities and positive finite bounds | I; C5/C6 |
| `.address` | Started service → bound host/port, including an ephemeral test port | I; local tests |
| `.start` | None → listener and fixed workers; refuses missing generation; binds only configured KV endpoint | I; C2/C5 |
| `._accept_loop` | Accept sockets, assign one accepted-request deadline, reserve bounded queue capacity or return bounded OVERLOADED | I; C6 |
| `._worker`, `._finish` | Polls the bounded queue so stop wakes idle workers even when worker count exceeds queue capacity; drains accepted work on graceful stop, preserves task accounting on handler/close errors, and releases sockets/outstanding count in `finally` | I; C6 |
| `._error_response`, `._reply_overloaded` | Validated request or bounded overload connection → explicit non-durable ERROR response; malformed identity cannot be trusted | I; C5/C6 |
| `._validate_identity` | Request → None or rejection reason; checks manifest, target, sender membership/current generation and, for schema 2, fixed ownership by the five manifest successors before storage dispatch; schema-1 local compatibility is unchanged | I; C1/C2/C5 |
| `._emit` | Request and receive/respond phase → professor terminal line plus optional structured log; key control chars escaped, values omitted | I; KD-17 primitive only |
| `._process` | Valid request and absolute deadline → mapped storage response; passes same deadline to storage so admission and SQLite lock waits cannot restart the RPC budget | I; C3/C5/C6 |
| `._handle` | Socket and deadline → fully validates, logs receipt, dispatches once, sends actual response only within the deadline, then closes | I; C5/C6 |
| `.stop` | Optional finite shutdown budget → refreshed `ShutdownReport`; stops admission, drains within the grace budget, drops and counts queued requests only on forced stop, interrupts active sockets, and reports unfinished work and whether workers joined. A non-clean stop can be retried to observe completed workers and release Store safely | I; C6 |

The deadline passed into SQLite bounds queue admission, lock waits, transaction work
and the pre-commit check. An OS/filesystem call that itself blocks cannot be cancelled
by Python, so a client-side timeout after a send still may have an unknown write
outcome. Membership gossip uses a separate service and capacity.

`tests/test_replica.py` adds regressions for four idle workers with a one-slot queue,
graceful draining of four active plus one queued request, forced shutdown with a full
queue, repeated start/stop without worker leaks, and a handler exception that must not
shrink the pool. RPC tests use a controlled monotonic clock to prove that a deadline
expiring after a validated INSTALL response returns TIMEOUT with
`completion_unknown=True`; a separate send-boundary test proves expiration before
`sendall` remains definite non-transmission. A valid INSTALL `STORAGE_ERROR` response
also preserves uncertainty because the client has no durable commit evidence.

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
| `MembershipRuntime.gossip_round`, `_run`, `stop` | Tick/check, probe up to configured fanout, merge responses, repeat at configured interval; stop reports whether listener/gossip threads joined, can retry failed joins, and avoids joining a thread that never started | I; KD-04/05/14, Batch 4 lifecycle |

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

### `node.py` — integrated local node lifecycle (Batch 4; C1–C6 foundations)

`NodeRuntime` owns one validated manifest/VM identity, one durable Store, one
MembershipRuntime/table, one ReplicaService, one ReplicaClient, optional logging,
and local lifecycle state. It is intentionally not a distributed coordinator. The
entrypoint `python3 -B -m mp3.node` is independent of `mp3.__main__` and does not
change the eight command guards.

| Function/method | Inputs → output; internal behavior | Callers / dependencies | Status / decisions |
|---|---|---|---|
| `NodeShutdownReport.clean`, `.incomplete_requests` | No inputs → derived bool/count from replica report, outstanding client calls, membership join and Store close state; clean also requires zero incomplete requests | `NodeRuntime.stop`; `ShutdownReport` | I; KD-07/08/14, C6 |
| `NodeRuntime.__init__` | Validated Manifest, VM ID, DB path, explicit bootstrap/recovery, optional mode/logging and bounded capacities → unopened runtime; rejects invalid identity/bootstrap combinations | `from_files`, local tests; ring/types | I; KD-01/02/03/09/14/15, C1–C6 |
| `from_files` | Manifest path and explicit node/database lifecycle → runtime; loads and validates manifest before runtime creation | CLI `main`, tests; `load_manifest` | I; KD-02/03, C1 |
| `ready`, `context`, `generation`, `membership` | No inputs → local readiness or current Store/runtime state; context/generation/membership reject reads before ready | CLI, callers/tests | I; KD-03/09/14, C1–C4 |
| `owners(key)` | UTF-8 key → fixed five prescribed `Node`s independent of liveness | diagnostics/tests; `Manifest.owners` | I; KD-02, C1 |
| `list_mem_ids()` | No inputs → diagnostic membership rows including suspected nodes, ordered by numeric ring ID | tests/future command; `MembershipTable` | I; KD-02/04, C1/C2; command remains guarded |
| `start()` | No inputs → this ready runtime; explicitly opens/recovers Store/context, configures logging, starts MembershipRuntime (the sole durable generation increment), then replica listener, then client, and finally marks local readiness. Any failure attempts cleanup while preserving the startup error | CLI `main`, context manager, tests; storage, membership, replica, rpc | I; KD-03/04/05/07/09/14/16 local epoch, C1–C6 |
| `_cleanup_failed_start()` | No inputs → None; stops whichever services started and closes Store only when membership and replica workers joined; cleanup exceptions do not replace the original startup failure | `start` exception path; membership/replica/store | I; KD-03/07/14, C2/C3/C6 |
| `call_replica(target_id, op, key, deadline, version?, value?)` | One prescribed target and absolute monotonic deadline → one `RpcOutcome`; under the call condition it checks admission, snapshots context/generation/client, and increments active-call accounting. It releases the lock before building/sending the request and decrements accounting in `finally`. A shutdown that starts after admission cannot invalidate the captured request identity; it does not gather owners or calculate a quorum | callers/tests; `new_request`, `ReplicaClient`, live membership table | I; KD-03/06/07/08/10/14/15/16, C3/C5/C6 |
| `CoordinatorAdmission` | Runtime-issued identity-registered capability → captures the original absolute deadline, context, sender generation and operation ID for one admitted coordinator callback; reconstructed or completed tokens fail validation | `submit_coordinator`, `reserve_attempts`, `dispatch_owner` | I; KD-08/14/15, C6 |
| `submit_coordinator(function, *args, deadline)` | An external callback and absolute deadline → bounded Future or explicit admission failure; admits only while ready, injects a one-use admission capability as the callback's first argument, and accounts the callback until completion | future protocol coordinators/tests; `CoordinatorResources` | I; KD-08/14/15, C6; no protocol coordinator |
| `reserve_attempts(count, *, deadline, admission=None)` | Count and absolute deadline → atomic bounded attempt reservation; an external call requires open readiness, while a live callback token permits already-admitted work during graceful drain | future protocol coordinators/tests; `CoordinatorResources` | I; KD-08/14/15, C6 |
| `dispatch_owner(..., admission=None)` | One target, op/key/phase/operation ID and deadline → one schema-2 owner call; checks fixed five-owner placement before dispatch and captures context/generation. During shutdown, only the matching live token may authorize its own operation's next phase | future protocol coordinators/tests; `ReplicaClient`, manifest, attempt executor | I; KD-02/08/14/15, C1/C5/C6; no coordinator |
| `_admission_is_live_locked`, `_finish_coordinator_admission` | Token and child deadline → bool / terminal cleanup; requires object identity in the runtime registry, forbids deadlines beyond the admitted deadline, removes authority exactly once at callback completion | coordinator admission paths; `_calls` condition | I; C6 |
| `stop(timeout)` | Nonnegative finite shutdown budget → `NodeShutdownReport`; atomically closes readiness and call admission under the same condition used by `call_replica`, then drains/forces replica work, stops membership, waits client calls and closes Store only after workers and calls are done. An incomplete stop can be retried; later reports refresh joins/work and release the DB lock after work exits | CLI signal path, context manager, tests | I; KD-07/08/14, C6 |
| `_close_logger()` | No inputs → None; removes/closes this runtime's handlers | startup rollback and stop | I; KD-17 |
| `__enter__`, `__exit__` | Context-manager entry starts; exit stops and raises if shutdown remains incomplete without an earlier exception | Python `with` users | I; KD-14, C6 |
| `_parser()` | No inputs → argparse parser for `--vm-id`, `--manifest`, `--db`, explicit `--bootstrap`/`--recover`, optional bootstrap mode and log | `main`, command help | I; C1/C2/C6 |
| `main(argv?)` | CLI args → process exit code; starts one node, prints local READY, waits for SIGINT/SIGTERM, then prints shutdown evidence. It rejects bootstrap mode on recovery and performs no remote deployment | `python -m mp3.node`; NodeRuntime | I; KD-03/07/14, C1–C6 |

The startup contract matters because a listener must never receive requests before
this process has durably advanced and installed its restart generation. Recovery
never invents a database or replaces the persisted context. A `READY` line only says
this one node's local services bound successfully; peers may still be unavailable.
On partial startup, started listeners and locks are released. If forced shutdown
cannot join workers immediately, the Store stays open and a later `stop()` retry can
finish cleanup; callers must not report clean shutdown before that succeeds.
An admitted coordinator retains only the bounded authority it received at admission:
shutdown rejects new callbacks, but an accepted callback may dispatch its next
same-operation phase until the original deadline. Admission and dispatch release
`_calls` before network I/O; worker drain keeps the Store open until accepted callback
and replica work finish or are reported incomplete.
`MembershipRuntime.stop()` reports thread joins and can retry a failed join; it
skips joining a thread whose `start()` itself failed.

`python3 -B -m mp3.node` supports only single-replica READ_LOCAL and INSTALL_IF_NEWER
through the programmatic `call_replica` primitive. It does not enable distributed
PUT/GET or any CLI operation.

### `eventlog.py`

Opt-in JSON-lines logging plus the Batch 3 local replica event hook; no handlers are
installed at import. JSON is escaped into one physical line, making a newline inside
an event parseable and grep-compatible. `log_replica_event` prints the prescribed
local timestamp and received/responded GET/PUT line and optionally appends a structured
record. This is not full operation tracing or MP1 query integration. No values are
logged. File flushes are not KV durability.

| Function/method | Inputs → output; internal behavior | Actual callers / components | Status / decisions |
|---|---|---|---|
| `JsonFormatter.format` | LogRecord → JSON string with UTC timestamp, level, event, optional node_id/request_id; escapes Unicode/newlines | logging handlers created by configure_logging; datetime/json | I prototype; KD-17 |
| `configure_logging` | fresh Logger, path, keyword stream → None; refuses existing handlers, appends UTF-8 file+console with formatter, INFO level, no propagation; caller closes/removes handlers | LogTests; standard logging/pathlib | I prototype; KD-17 |
| `log_replica_event` | logger or None, phase, operation, key, node/request IDs and optional stream → printed line; escapes key control characters, logs structured fields when given a configured logger, never logs value bytes | `ReplicaService._emit`, loopback logging tests | I; KD-17 replica primitive only |

Required distributed operation reachability, returned-version and repair details are
still future protocol/MP1-adapter work; the local RPC event hook alone is not a claimed
complete trace.

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

### `tests/test_node_runtime.py` — integrated lifecycle and local multi-node behavior (Batch 4)

`NodeRuntimeTests.setUp` creates ten validated loopback identities and a temporary
manifest/database directory. Three-node tests use real membership and KV TCP sockets;
the recovery test launches disposable local processes and sends SIGKILL only to its
own VM06 child. No test contacts a university VM.

The module helpers are test-only: `_free_port(reserved)` binds port 0 and returns a
distinct loopback port; `local_manifest()` builds ten identities with separate KV,
membership and log-query endpoints; `replace_row()` changes one row in a test snapshot;
and `wait_until(predicate, timeout, description)` polls against a monotonic deadline.
`setUp()` creates a temporary directory/manifest, `node()` constructs one runtime for
it, `_member()` looks up one membership row, and `_wait_known_generations()` waits for
gossip convergence. `_start_cli()` launches a disposable node process and waits for
its READY line, `_stop_cli()` signals and collects that process, and
`_target_accepts_requester()` checks remote membership acceptance with a real read
request. None of these helpers is part of the production API.

| Test method | Fixture/internal behavior → expected assertion |
|---|---|
| `test_explicit_bootstrap_defaults_strong_and_stops_without_deleting_data` | Starts VM04 → Strong epoch 1 and generation 1 are ready; stops, reopens SQLite, and confirms stored identity/context survive |
| `test_explicit_disposable_eventual_bootstrap_is_supported` | Explicit test mode bootstrap → Eventual-RR context; clean stop |
| `test_identity_manifest_and_bootstrap_recovery_inputs_fail_closed` | Invalid VM, recovery mode flag, absent/corrupt store and malformed manifest → rejected without serving |
| `test_sqlite_database_lock_fails_startup_within_store_budget` | Separate writer holds SQLite lock → startup fails within configured budget, then database reopens |
| `test_existing_database_is_never_implicitly_bootstrapped_or_reidentified` | Existing store passed to bootstrap or different VM identity → fail without replacing data; legitimate identity can reopen |
| `test_failure_after_membership_start_releases_listener_and_store_lock` | Occupied KV port after membership starts → rollback closes membership endpoint and Store |
| `test_membership_port_conflict_closes_database_without_starting_kv` | Occupied membership endpoint → Store lock released and KV endpoint never binds |
| `test_gossip_thread_start_failure_rolls_back_membership_listener_and_store` | Inject gossip thread start error after membership listener binds → original error propagates, listener rebinds, DB reopens |
| `test_kv_accept_thread_start_failure_rolls_back_workers_listener_and_store` | Inject KV accept thread start error after replica workers start → workers join, KV and membership endpoints rebind, DB reopens |
| `test_forced_node_shutdown_reports_incomplete_active_and_queued_requests` | One active plus one queued socket with a very short shutdown → first report is non-clean with two incomplete; release work, retry stop, then report clean and reopen DB |
| `test_admitted_call_keeps_context_and_generation_through_shutdown` | Event-block request construction after admission, begin stop, then release construction → call uses its captured generation/context/version/value, completes, and shutdown waits before closing Store |
| `test_shutdown_closes_call_admission_before_later_rpc_attempt` | Stop pauses at service shutdown after atomically closing admission → a later RPC is rejected with active-call count unchanged |
| `test_forced_node_stop_reports_admitted_rpc_and_closes_store_only_after_retry` | Block an admitted outgoing call, force a short stop → report one outstanding call and open Store; release call, retry stop, then close/reopen Store and confirm accounting returns to zero |
| `test_real_three_node_gossip_rpc_binary_values_and_concurrency` | Three real loopback NodeRuntime instances → gossip generations/owners agree; remote empty/missing and full 1 MiB values round-trip; concurrent installs run while gossip progresses |
| `test_subprocess_sigkill_recovery_gossip_and_stale_evidence` | VM06 child serves a committed value, receives SIGKILL, restarts on same DB with generation 2 → peers learn recovery, stale request/response/gossip is rejected, ring owners remain fixed |
| `test_sigterm_shutdown_reports_success_and_recovery_keeps_context` | Launch CLI child, SIGTERM, recover same DB → clean shutdown and exactly one new generation with persisted context |

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
| `test_deadline_frame_round_trip_and_slow_fragment_total_bound` | Deadline helper socketpair with one-byte trickle → exact bytes or total timeout |

### `tests/test_rpc.py` — C5 wire schema and response evidence

`RpcCodecTests` uses in-memory sockets and deterministic ten-node manifests. It
does not contact remote machines.

| Test methods | Coverage |
|---|---|
| `test_request_round_trip_preserves_unicode_empty_nul_and_raw_bytes`, `test_full_one_mib_response_round_trip_is_binary_safe`, `test_found_empty_value_is_not_not_found` | Unicode/empty/NUL keys, arbitrary bytes, 1 MiB payload and presence semantics |
| `test_exact_json_and_outer_payload_limits`, `test_oversized_outer_and_json_lengths_rejected_before_body_read` | Exact caps and rejection before body allocation/read |
| `test_canonical_schema_and_decimal_fields`, `test_very_large_canonical_version_round_trips_within_metadata_cap` | Required field names, canonical numbers and large version strings |
| `test_rejects_wrong_uuid_schema_fields_and_integer_coercions`, `test_rejects_bad_version_kind_writer_and_request_combinations`, `test_duplicate_json_fields_and_malformed_utf8_are_rejected` | Fail-closed metadata validation |
| `test_truncated_body_and_trailing_bytes_fail_closed`, `test_response_contradictory_status_and_stored_version_evidence_rejected` | Exact frame boundary and response evidence rules |

### `tests/test_replica.py` — actual loopback service/client integration

These tests create two logical VM identities with temporary SQLite files and TCP
loopback listeners. The process-recovery test kills only its own child process.

| Test methods | Coverage |
|---|---|
| `test_actual_tcp_read_install_outcomes_and_empty_vs_missing`, `test_actual_tcp_unicode_nul_key_and_full_binary_value`, `test_client_connect_uses_cached_addresses_and_shared_candidate_deadline` | Real vm01→vm04 requests reach the replica's Store; cached candidates avoid DNS and per-address retries share the same budget |
| `test_stale_epoch_returns_current_context_and_does_not_mutate`, `test_zero_epoch_returns_stale_context_under_request_deadline`, `test_invalid_sender_generation_and_wrong_target_do_not_mutate`, `test_wrong_manifest_is_closed_before_storage_mutation` | Epoch (including invalid zero epoch), manifest, target and sender-generation fencing before DB mutation |
| `test_committed_write_with_lost_ack_is_reported_unknown`, `test_client_timeout_after_commit_marks_completion_unknown`, `test_deadline_after_validated_install_response_is_unknown`, `test_storage_error_install_response_preserves_uncertain_completion`, `test_expired_deadline_before_send_is_definite_nontransmission`, `test_expiry_after_connect_before_first_send_is_definite_nontransmission`, `test_connection_refused_is_unreachable_not_not_found`, `test_lost_response_after_send_is_unknown_error` | Network errors and client-visible uncertainty remain distinct from NOT_FOUND; a controlled clock checks both sides of the send boundary and a late validated response |
| `test_nonfinite_and_unrepresentable_deadlines_return_outcomes`, `test_service_receive_deadline_bounds_slow_fragmented_header`, `test_slow_fragmented_response_cannot_reset_total_deadline`, `test_storage_lock_wait_uses_service_deadline_and_cannot_commit_late` | Absolute deadlines cover slow traffic, socket numeric edge cases and SQLite lock waits |
| `test_full_worker_and_queue_returns_explicit_overloaded`, `test_concurrent_clients_progress_for_distinct_and_same_keys`, `test_membership_gossip_uses_separate_service_while_kv_load_runs`, `test_shutdown_is_bounded_and_joins_worker_threads`, `test_four_idle_workers_exit_with_queue_capacity_one`, `test_graceful_shutdown_drains_active_workers_and_queued_work`, `test_forced_shutdown_accounts_full_queue_and_active_request`, `test_unexpected_handler_exception_does_not_kill_worker` | Bounded admission, concurrent requests, membership capacity isolation, queue-capacity-independent worker shutdown, accepted-work accounting, forced stop and handler survival |
| `test_receipt_and_response_terminal_lines_do_not_log_values` | Required local log lines, escaped keys and no raw-value logging |
| `test_tcp_install_survives_kill9_and_process_restart` | Local child process SIGKILL/restart preserves committed bytes and increments generation |

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

Local storage, fixed hashing/membership and single-replica KV RPC exist as described
above. Shared distinct-response collection and bounded dispatch are implemented.
Strong ABD is implemented through the programmatic API in `strong.py`; Eventual
coordinators, read repair, mode manager, benchmarks, plotting and deployment
implementation remain future work.
The CLI operation entrypoints still terminate at `pending.execute`; the RPC API is a
library foundation and does not make those commands operational. Later responsibilities
and decision gates are in the plan and KD-01..18. No fabricated benchmark/report
content exists.

Final Batch 1 refinements: timeout rejects booleans; decimal encoding/decoding
roundtrips a 2**15000 Strong counter without changing global Python limits.
Invalid context types return local ERROR; valid mismatched contexts return STALE_EPOCH.
The full 51-test suite passed after these refinements. No power-loss or VM proof is
claimed by local SIGKILL tests.

## Step 3 Batch 1 — implemented shared infrastructure, no coordinators

Krishna approved the Step 3 design on October 31, 2026. Batch 1 implements common
infrastructure only. It does **not** implement Strong ABD or Eventual-noRR PUT/GET
algorithms. The planned coordinator APIs below remain future behavior; shared primitives
are implemented as listed.

### `attempts.py`

| Function/type | Inputs → output; behavior and callers | Status |
|---|---|---|
| `AttemptCancelled` | Exception raised when a queued or active lease-bound attempt is explicitly canceled | I; KD-08/12/14 |
| `AttemptLease.__init__` | Admission time, 5s foreground, 10s continuation, 15s hard maximum and injectable monotonic clock → one lock-protected lease | I; approved KD-12/C6 |
| `foreground_deadline`, `hard_deadline`, `current_deadline`, `foreground_finished`, `cancelled` | Lease → read-only state/deadline snapshots | I; callers in lease-aware RPC and coordinator tests |
| `finish_foreground(completed_at=None)` | Foreground completion/timeout time → continuation deadline `min(completion+10s, admission+15s)`; may run only once | I; future Eventual PUT orchestration; tested primitive |
| `check()` | No input → remaining time or `AttemptCancelled`/`TimeoutError`; uses current dynamic deadline | I; lease socket steps |
| `cancel()` | No input → bool; idempotently cancels only a live lease | I; bounded shutdown/fault handling |

### `coordinator.py`

This file provides types and generic infrastructure. It does not implement an algorithm
that issues Strong phases or decides client-facing Eventual results.

| Function/type | Inputs → output; behavior and callers | Status |
|---|---|---|
| `OperationStatus`, `OwnerOutcome`, `AttemptState` | Enumerate client-result, per-owner and scheduler states without conflating absence, transport failure or overload | I; KD-08/14/15 |
| `ProtocolResult.__post_init__` | Validate status/value/version shape and canonical UUID4 operation ID | I; future coordinator result contract |
| `RpcCompletion.__post_init__` | Require typed `RpcOutcome` and finite monotonic completion time | I; `collect_distinct` |
| `QualifiedReply`, `OwnerFailure`, `QuorumCollection`, `OwnerCall`, `AttemptReport` | Carry response/request evidence, explicit failures, threshold result and any still-running owner calls | I; future coordinators |
| `AttemptLedger` | Five distinct owner IDs → one validated per-owner state machine with terminal outcome and release callback; `DURABLE_ACK` is a confirmed exact-version durable success distinct from `SUPERSEDED`, errors, and uncertain completion | I; C6 shared tracking |
| `Schema2Activation.from_inventory`, `.from_file`, `.enabled_for` | Manifest and complete ten-identity, same-release schema inventory → explicit activation object; this is a deployment assertion, not a per-operation health check | I; approved rollout contract; local fixtures only |
| `CapacityReservation.submit`, `.close`, `.remaining` | Reserved count and tasks → queued tasks or released unused slots; no unbounded thread/queue growth | I; KD-14/C6 |
| `BoundedExecutor.reserve`, `.submit`, `._submit_reserved`, `._release_unused`, `._worker`, `._finish_job`, `._cancel_queued`, `.shutdown` | Fixed worker count, bounded queue and deadline → atomic capacity reservation, Future, explicit overload/timeout or shutdown report; exact active/queued/unsubmitted accounting | I; approved 16/32 coordinator and 16/64 attempt defaults |
| `CoordinatorResources.reserve_attempts`, `.shutdown` | Node-owned worker groups and total shutdown budget → separate coordinator/attempt reservations and combined report | I; KD-14/C6 |
| `dispatch_owner(client, node, request, *, manifest, deadline, expected_generation, executor, reservation=None, attempt_lease=None)` | One validated request → one Future-backed RPC with generation captured at dispatch; schema-2 requests must target one of the key's five manifest owners before a worker is queued; returns structured `OwnerDispatchError.failure` on local ownership rejection; no retry or quorum decision | I; NodeRuntime, future coordinators |
| `_qualifies(phase, request, response)` | Phase and validated request/response → protocol-specific evidence bool: Strong at-least durable rules, Eventual exact-version W rule, or valid read result | I; approved KD-11/12 predicates, not a coordinator |
| `collect_distinct(...)` | Exactly five prescribed owner calls, threshold, phase/context/operation and absolute deadline → `QuorumCollection`; validates request/response binding, counts each identity once, stops at threshold or once terminal outcomes make it mathematically impossible, and keeps all accepted Eventual PUT attempts in `continuing` even when W is impossible | I; approved KD-08/11/12/15 shared collector |

`QuorumCollection.continuing` is important: an Eventual PUT caller must supervise those
owner futures until each attempt has a terminal outcome or the separate lease/shutdown
boundary. Reaching W does not cancel them; an impossible W threshold does not cancel
remaining fanout either. Strong/read calls cancel only queued unneeded calls and expose
non-cancelable active calls as continuing. This preserves attempt ownership without
implementing the Eventual coordinator.

### Changed integration points

`ReplicaRequest` and `ReplicaResponse` now encode schema 2 with operation IDs and the
approved protocol phase set. Schema 1 remains valid only for `phase="local"` and carries
no operation ID. `ReplicaService` handles schema-2 READ_LOCAL/INSTALL_IF_NEWER through
the existing Store methods only when passed a complete compatible-release activation;
it independently rejects schema-2 requests addressed to a VM outside the key's five
prescribed owners before storage access. Schema-1 local primitives retain their prior
non-owner-compatible behavior. The service never decides quorum success. `NodeRuntime` owns separate bounded coordinator and
outbound-attempt executors, can reserve outbound capacity, and can dispatch one schema-2
owner call. Activation is supplied explicitly with `--schema2-inventory`; no operation
contacts all ten VMs for a compatibility check. All paths still use the same NodeRuntime.

`Store` migrates schema-1 metadata to schema 2 within one SQLite transaction, preserves
all rows, derives a Strong high-water mark from existing Strong records, and fails closed
on malformed metadata. `allocate_strong_version` reserves `max(observed_counter,
local_high_water)+1` in a short durable transaction. Same-process concurrency is serialized
only during this transaction; no network lock is held. The persisted high-water survives
SIGKILL/restart. Full mode-switch rules remain KD-16 work.

### Tests added in this batch

`tests/test_coordinator.py` covers AttemptLease timing/cancellation, bounded atomic
reservations, forced shutdown accounting, distinct-owner threshold evidence, duplicate
owners, NOT_FOUND versus transport errors, Eventual SUPERSEDED exclusion, retained
Eventual PUT fanout after W and when W becomes impossible, and dispatch-generation
binding. `tests/test_storage.py` adds Strong concurrent allocation/restart and preserving
schema migration/interrupted migration tests. `tests/test_rpc.py` covers schema-2 phase
and operation-ID validation plus the compatible inventory gate. `tests/test_node_runtime.py`
uses real loopback TCP for Strong schema-2 and Eventual lease-bound installs/reads.
`tests/test_replica.py` checks fail-closed rejection when schema 2 is not activated.

The full suite and command results are recorded at the end of IMPLEMENTATION_PLAN.md;
those results are local only. No ten-VM validation or distributed consistency result is
claimed.

### Protocol coordinators

| Proposed interface | Purpose and dependencies | Status |
|---|---|---|
| `NodeRuntime.strong_put/strong_get` via `StrongCoordinator` | Two-phase Q=3 ABD using `dispatch_owner`, `collect_distinct`, Store allocator, and fixed owners | IMPLEMENTED; locally VERIFIED (A-01); CLI still pending |
| `NodeRuntime.eventual_put/eventual_get` via `EventualNoRRCoordinator` | Configurable R/W, first-R maximum, five-owner propagation and lease supervision | IMPLEMENTED; independently locally VERIFIED (E-01); not ten-VM evidence |
| `EventualRRCoordinator` and full mode/CLI integration | Read repair, cluster-wide epoch transition, files/commands | Deferred to Step 4; KD-13/full KD-16 pending |

## Step 3 Batch 2 — `strong.py` (implemented; KD-11)

`StrongCoordinator` is a focused coordinator over the existing node-owned workers
and RPC/storage APIs. It does not create another executor or transport. A call enters
through `NodeRuntime.submit_coordinator`; the resulting `CoordinatorAdmission`
captures the operation UUID, Strong context, sender generation and absolute monotonic
deadline. One reservation covers up to five phase-one and five phase-two owner calls.

| Exact function | Inputs and outputs | Behavior, callers and dependencies | Status |
|---|---|---|---|
| `StrongCoordinator.__init__(self, runtime: "NodeRuntime") -> None` | One runtime → coordinator wrapper | Stores the owner runtime; no network or worker starts. Called lazily by the NodeRuntime methods. | IMPLEMENTED; KD-11 |
| `StrongCoordinator.strong_put(self, key: str, value: bytes, *, deadline: float) -> ProtocolResult` | UTF-8 key, 0..1 MiB bytes, absolute monotonic deadline → OK with tag/owners or explicit error/timeout/overload evidence | Checks input, readiness and schema-2 activation, submits a bounded operation, then waits for its Future. Empty bytes are valid. Called by `NodeRuntime.strong_put`; uses `_run`. | IMPLEMENTED; locally tested; KD-08/11/14 |
| `StrongCoordinator.strong_get(self, key: str, *, deadline: float) -> ProtocolResult` | UTF-8 key and absolute deadline → OK with bytes/version, NOT_FOUND, or explicit error | Same gates/admission as PUT. Only a phase-one quorum of three explicit absence responses returns NOT_FOUND. Called by `NodeRuntime.strong_get`; uses `_run`. | IMPLEMENTED; locally tested; KD-08/11/14 |
| `StrongCoordinator._validate_operation(self, key: str, deadline: float, operation_id: str) -> ProtocolResult \| None` | Key, deadline, fallback UUID → no error or structured error | Rejects non-text/unencodable keys and nonfinite/nonnumeric deadlines; called before admission by PUT/GET. | IMPLEMENTED; tested |
| `StrongCoordinator._external_gate(self, operation_id: str) -> ProtocolResult \| None` | Operation UUID → no error or structured failure | Requires a ready runtime and complete schema-2 activation; called by PUT/GET. `submit_coordinator` repeats the synchronized admission check. | IMPLEMENTED; tested; C5/C6 |
| `StrongCoordinator._run(self, admission: "CoordinatorAdmission", operation: str, key: str, value: bytes \| None) -> ProtocolResult` | Admitted operation → result | Confirms Strong mode, selects the same five owners, reserves both phases, and queries concurrently. PUT allocates above observed/local maxima, then sends `strong_write`; GET returns NOT_FOUND only for three explicit absences, else sends exact-record `strong_readback`. Both phases reuse deadline/context/operation UUID; each RPC has a fresh request ID. | IMPLEMENTED; local loopback and subprocess tested; KD-11 |
| `StrongCoordinator._dispatch_phase(self, admission: "CoordinatorAdmission", owners: tuple["Node", ...], reservation, operation: RpcOperation, key: str, phase: str, version: Version \| None, value: bytes) -> tuple[OwnerCall, ...]` | Phase data and five owners → one OwnerCall per owner | Calls `NodeRuntime.dispatch_owner` with captured admission/reservation. A local dispatch exception becomes explicit failed evidence and never quorum evidence. Called twice by `_run`. | IMPLEMENTED; local fault-tested; C5/KD-11 |
| `StrongCoordinator._maximum_record(collection) -> Record | None` | Qualified query collection → highest FOUND record or None | Selects only actual records; transport failures never mean absence. Called after phase one. | IMPLEMENTED; tested; KD-11 |
| `StrongCoordinator._write_completion_unknown(collection) -> bool` | Failed phase-two collection → uncertainty flag | True if any RPC is uncertain, an owner call continues, or a durable phase-two response exists. Called when phase two lacks Q=3. | IMPLEMENTED; lost-ack/partial-write tested; KD-08/11 |
| `StrongCoordinator._collection_failure(collection, phase: str, operation_id: str, *, completion_unknown: bool) -> ProtocolResult` | Collector evidence → TIMEOUT, ERROR or OVERLOADED result | Includes phase, qualifying owners, failures and continuing calls; no failed result carries successful value/version. Called on failed quorum. | IMPLEMENTED; fault-tested; KD-08/11 |
| `StrongCoordinator._result(status: OperationStatus, operation_id: str, *, value: bytes \| None = None, version: Version \| None = None, owners: tuple[str, ...] = (), completion_unknown: bool = False, error: str \| None = None) -> ProtocolResult` | Status and evidence → validated result | Deduplicates owner IDs and constructs the shared result. Used by all public/private paths. | IMPLEMENTED; tested |
| `NodeRuntime.strong_put(self, key: str, value: bytes, *, deadline: float)` | Client key/value and absolute deadline → ProtocolResult | Lazily constructs `StrongCoordinator` and delegates; does not expose CLI behavior. | IMPLEMENTED; local integration-tested; A-01 |
| `NodeRuntime.strong_get(self, key: str, *, deadline: float)` | Client key and absolute deadline → ProtocolResult | Lazily constructs `StrongCoordinator` and delegates. | IMPLEMENTED; local integration-tested; A-01 |

The coordinator's response validator and durable conditional install remain in the
existing `rpc.py`, `coordinator.py`, `replica.py` and `storage.py`; these modules were
not rewritten for Strong. `tests/test_strong.py` uses real loopback RPC and SQLite;
`tests/history_checker.py` checks finite register histories against real-time
precedence and version order. The checker is a bounded test aid, not a proof for every
execution. Eventual coordinator APIs remain proposed/not implemented.

### Test-only helpers added for Batch 2

`tests/history_checker.py` defines `HistoryEntry(operation_id: str, coordinator_id:
str, key: str, operation: str, input_value: bytes | None, invoked_at: float,
responded_at: float, result: ProtocolResult)`. `check_register_history(entries:
Iterable[HistoryEntry]) -> bool` groups by key and checks whether a legal sequential
register order respects real-time precedence, versions and returned values; an
ambiguous failed PUT can be absent or take effect within its call interval.
`_check_one_key(rows: list[HistoryEntry]) -> bool` performs the per-key backtracking;
its nested `visit(done: frozenset[int], state: tuple[bytes, object | None] | None) ->
bool` tries legal next operations. These are test helpers, not runtime APIs.

`tests/test_strong.py` helpers are `_free_port(reserved: set[int]) -> int` and
`_manifest() -> Manifest` (allocate unique ephemeral endpoints and build the test ring);
`NodeProcess.__init__(self, vm_id, manifest_path, database_path, inventory_path, *,
bootstrap)`, `_read_line(self, timeout) -> dict`, `snapshot(self) -> dict[str, int]`,
`start_one_commit_put(self, key: str, value: bytes) -> dict`,
`kill(self) -> int`, `stop(self) -> bool`, and `_close_pipes(self) -> None` manage a
disposable independent node process. The test-only `put-one` command permits the
first prescribed-owner phase-two RPC to commit, reports that event to the parent,
and blocks other phase-two sends until the parent SIGKILLs the coordinator.
`StrongIntegrationTests.setUp(self) -> None`,
`node(self, vm_id) -> NodeRuntime`, `start_nodes(self, vm_ids)`,
`start_process(self, vm_id, *, bootstrap=True) -> NodeProcess`,
`wait_membership(self, participant_ids: set[str], timeout=12)`,
`_process_by_id(self) -> dict[str, NodeProcess]`, `close_cluster(self)`, and
`participants_for(self, key: str, *, coordinators: tuple[str, ...]=())` provide
temporary state and await actual local membership.

Its test methods are:
`test_nonowner_coordinator_empty_binary_replacement_and_one_mib_value(self)`;
`test_quorum_succeeds_with_two_unavailable_and_never_fakes_absence(self)`;
`test_failed_put_with_lost_acks_is_unknown_and_get_reads_back_partial_write(self)`;
`test_equal_version_conflicting_bytes_fails_before_readback(self)`;
`test_two_writers_same_observation_order_tags_and_remain_linearizable(self)`;
`test_admitted_put_can_dispatch_phase_two_after_shutdown_closes_admission(self)`;
`test_admission_closed_before_new_strong_operation_is_rejected(self)`;
`test_delayed_query_and_install_responses_arrive_after_quorum_in_order(self)`;
`test_randomized_concurrent_histories_are_linearizable_for_recorded_seeds(self)`; and
`test_sigkill_after_commit_lost_ack_recovery_and_generation_fencing(self)` and
`test_sigkill_writer_after_exactly_one_replica_commit_then_get_reads_back(self)`. The
latter kills an independent nonowner coordinator after exactly one durable owner
commit, verifies the other prescribed owners remain absent, then runs Strong GET and
checks readback to a quorum. They cover
the named integration/fault schedules and use actual key owners from the manifest.

`tests/test_history_checker.py` defines `HistoryCheckerTests.result(self, status, *,
value=None, version=None, unknown=False)` and the test methods
`test_accepts_valid_register_order_and_ambiguous_write_effect(self)`,
`test_rejects_real_time_violation_and_wrong_read_value(self)`, and
`test_rejects_completed_strong_write_that_moves_tag_backward(self)`. This suite checks
the checker itself with synthetic known-valid and known-invalid histories.

## Step 3 Batch 3 — `eventual.py` (Eventual-noRR implemented; KD-12)

The Eventual-noRR coordinator is a programmatic library API on the existing
`NodeRuntime`; it does not change the CLI. PUT uses the existing durable timestamp
allocator, reserves five owner attempts before dispatch, counts only exact-version
durable INSTALL/UNCHANGED responses, and retains accepted one-shot requests under a
bounded continuation lease. GET returns the maximum version among the first R valid
replies and never issues a write or read repair. Each operation captures R/W before
its bounded coordinator task is queued.

| Function or type | Exact behavior, inputs/outputs, callers and dependencies | Status |
|---|---|---|
| `OwnerPropagation(owner_id: str, outcome: OwnerOutcome \| None, completion_unknown: bool = False, error: str \| None = None)` | Immutable terminal evidence for one prescribed owner; used by the handle and snapshot | IMPLEMENTED; tested; C6/KD-12 |
| `PropagationSnapshot(operation_id: str, terminal: bool, owners: tuple[OwnerPropagation, ...])`; `.completion_unknown` | Current five owner rows and aggregate uncertainty; terminal only when all five are terminal | IMPLEMENTED; tested |
| `_ForegroundTransition.finish(completed_at: float) -> float`; `begin_dispatch() -> bool`; `.may_have_dispatched` | Exactly-once foreground-to-continuation transition and race-safe first-send gate; shared by API timeout and admitted worker; lock is never held over network IO | IMPLEMENTED; deadline-race-tested; C6 |
| `PropagationHandle.__init__(operation_id, owners, manifest) -> None` | Creates a bounded per-owner ledger without threads; called by PUT | IMPLEMENTED; C6/KD-12 |
| `PropagationHandle.mark_dispatching(owner_id) -> None` | Marks possible send before executor submission, so timeout cannot falsely say no side effect was possible | IMPLEMENTED; reviewer-inspected; C6 |
| `PropagationHandle.attach(call: OwnerCall) -> None` | Retains the single accepted Future/request, registers callback; called by PUT | IMPLEMENTED; tested; C5/C6 |
| `PropagationHandle.finish_without_dispatch(owner_id, outcome, error=None) -> None`; `finish_unsubmitted(outcome, error) -> None` | Terminally records local preflight/dispatch failures or accepted slots that were never sent; never overwrites attached futures | IMPLEMENTED; tested |
| `PropagationHandle._finish(owner_id, future) -> None`; `_classify(owner_id, future) -> OwnerPropagation` | Converts actual future result into a validated terminal row; validates response identity/context/generation/phase; exact durable INSTALLED/UNCHANGED is DURABLE_ACK, SUPERSEDED is separate, and transport uncertainty is retained | IMPLEMENTED; loopback/fault-tested; C5/C6/KD-12 |
| `PropagationHandle.snapshot() -> PropagationSnapshot`; `wait(timeout: float \| None = None) -> PropagationSnapshot`; `.any_dispatched` | Immediate or bounded report; completed futures are polled to close callback timing races; no monitor thread | IMPLEMENTED; tested |
| `EventualNoRRCoordinator.__init__(runtime, *, read_quorum: int = 2, write_quorum: int = 2, foreground_seconds: float = 5.0)`; `_quorum(value, name) -> int` | Validates immutable per-coordinator R/W in 1..5 (bool rejected) and finite positive foreground budget | IMPLEMENTED; tested; KD-12 |
| `EventualNoRRCoordinator.eventual_put(key: str, value: bytes) -> ProtocolResult` | Captures admission time, UUID4, setting snapshot and AttemptLease; checks ready/schema-2/Eventual-noRR; bounded coordinator admission; returns after W or explicit error while retaining the propagation handle | IMPLEMENTED; local loopback/subprocess-tested; C4/C5/C6/KD-12 |
| `EventualNoRRCoordinator.eventual_get(key: str, *, deadline: float) -> ProtocolResult` | Bounded read of the same five fixed owners; max version among first R valid FOUND/NOT_FOUND responses; no repair | IMPLEMENTED; tested; C5/KD-08/12 |
| `_validate_key(key, operation_id) -> ProtocolResult \| None`; `_external_gate(operation_id) -> ProtocolResult \| None` | Strict UTF-8 key validation (empty, NUL and Unicode accepted by existing storage policy) and readiness/schema/mode checks before dispatch | IMPLEMENTED; tested |
| `_run_put(admission, key, value, admitted_at, lease, transition, propagation, required) -> ProtocolResult` | Atomically reserves five attempts, durably allocates one shared Eventual version, fences initial dispatch against timeout, records a per-owner synchronous preflight failure while continuing other owners, and collects exact W; queued/running calls retain their bounded lease | IMPLEMENTED; independently locally tested; C4/C6/KD-12 |
| `_run_get(admission, key, deadline, required) -> ProtocolResult`; `_dispatch(...) -> tuple[tuple[OwnerCall, ...], tuple[OwnerFailure, ...]]` | Reserves the five fixed-owner read slots, attempts each prescribed owner independently, returns valid calls plus terminal per-owner synchronous dispatch failures, then computes only from the first R qualifying replies under the same absolute deadline | IMPLEMENTED; local dispatch-failure regressions; C5/C6/KD-12 |
| `collect_distinct(..., preflight_failures=(), key: str | None = None) -> QuorumCollection` | For ordinary calls, validates the five prescribed owner identities as before. For an all-preflight-failed read, accepts zero calls only when an explicit key is supplied and the five distinct terminal failures cover exactly the validated owner set for that key; it creates no synthetic network responses | IMPLEMENTED; all-failure regression; C5/KD-08/12 |
| `_failure_status(collection) -> OperationStatus`; `_failure_text(collection, phase) -> str`; `_result(...) -> ProtocolResult` | Converts collector evidence into explicit TIMEOUT/ERROR/OVERLOADED/NOT_FOUND/OK results without faking absence; preserves operation ID, owners, propagation and uncertainty | IMPLEMENTED; tested; KD-08/12 |
| `NodeRuntime.eventual_put(key, value, *, read_quorum=2, write_quorum=2)`; `eventual_get(key, *, deadline, read_quorum=2, write_quorum=2)` | Thin programmatic wrappers constructing the coordinator; CLI still guards distributed commands | IMPLEMENTED; loopback-tested; E-01 |
| `NodeRuntime.submit_coordinator(..., operation_id: str \| None = None)` | Validates/preserves a supplied canonical UUID4 through `CoordinatorAdmission`; existing callers still receive generated IDs | IMPLEMENTED; tested; C5 |
| `BoundedExecutor.reserve(..., queue_deadline=None)`; `CoordinatorResources.reserve_attempts(..., queue_deadline=None)` | Separates foreground capacity acquisition from the hard bounded lease deadline for already reserved queued attempts | IMPLEMENTED; executor/lease-tested; C6 |

`tests/test_eventual.py` exercises actual loopback TCP and SQLite, including separate
process crash/restart. The previous `AttemptLease` primitive tests in
`tests/test_coordinator.py` check the 1s, 4.9s and 5s foreground examples; integrated
tests cover queued-after-W starts, foreground queue/allocation expiry, overload,
shutdown draining, lost acknowledgments, stale generation, no-read-repair, and
concurrent coordinators. Final test evidence is recorded in `IMPLEMENTATION_PLAN.md`.
These are local checks only; no university VM or ten-VM deployment was tested.
