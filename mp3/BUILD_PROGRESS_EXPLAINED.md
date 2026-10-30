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

Full distributed PUT/GET, general KV RPC networking, Strong ABD, Eventual consistency,
read repair, mode switching, and the eight complete demo commands are still future
work. The TCP service in this batch carries membership gossip only.

### What comes next?

Batch 3 is **TCP RPC networking for key-value requests**. It will let a coordinator
ask remote replicas to read or install a value. The consistency protocols and command
integration come after that shared network foundation.
