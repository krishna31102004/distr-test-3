# Phase 0 verification evidence

Date: October 22, 2026. Local Python 3.12.4 on the supplied source snapshot.
Run commands from the repository root. No packages installed, no TCP service started,
no VM contacted, no original remote tests/scripts executed, no commit/push/deployment.
There is no `.git` directory here, so Git status/branch creation is unavailable.

## Executed before independent review

| Exact command | Actual result | Scope |
|---|---|---|
| `python3 -B -m unittest discover -s mp3/tests -v` | 34 tests, OK | Local framing, supplied-ID ring, config, result types, logs, CLI, legacy characterization |
| `python3 -B -m mp3 --smoke` | Exit 0; `Phase 0 smoke OK: framing, inventory, successors; KV NOT_IMPLEMENTED` | Local socket pair with 1 MiB + empty bytes, inventory and successors |

The initial test run failed with five error reports because the socketpair tuple
was incorrectly used as a context manager. The main agent changed each site to
close the individual sockets via their context managers, then reran the full suite
and smoke; the results above are the successful rerun, not the initial failure.

Test cases use temporary directories, fake fragment sockets, local unnamed socket
pairs and controlled legacy clocks. The known terminal-failure limitation test
**passes by confirming broken-for-MP3 behavior**, not by proving restart recovery.
MP1/MP2 protocol interoperability proves raw framing only, not distributed log
querying or a KV RPC schema. Logging tests emit illustrative events, not real PUTs.

## Reference preservation and specification search

Before implementation, SHA-256 hashes of all 33 files in MP1/MP2 were saved to the
temporary session file `/tmp/mp3-phase0-reference-hashes.json`. Final comparison
found all 33 files identical, with no additions/deletions; the unchanged copy also has a persistent
byte-equality test. No original MP3 spec was found in the repository file inventory
or in a bounded filename search for MP3/CS425 documents under Desktop/Downloads;
unrelated course homework and MP2 PDFs were not opened.

## Independent review and final checks

The selected GPT-6 Luna/high reviewer ran independently; see
[REVIEW.md](REVIEW.md). It reran all 34 local tests (OK), smoke (exit 0), help
(exit 0), compiled all 14 MP3 Python files in memory, checked the reuse audit against
source, and compared all 33 reference files. It made no production/test edits and
found no production-code or test blocker.

The reviewer requested one documentation clarification: per-recv inactivity
timeouts do not enforce an overall operation deadline under slow trickle reads.
The main agent clarified that obligation in the framing docstring, README,
implementation plan and explanation. This changed no runtime behavior. Future
RPC must provide deadline-aware IO or an approved framing API extension under KD-08.

Final main-agent checks:

| Exact command / procedure | Actual result |
|---|---|
| `python3 -B -m unittest discover -s mp3/tests -v` | 34 tests, OK |
| `python3 -m compileall -q mp3` | Exit 0; MP3 only |
| `python3 -B -m mp3 --smoke` | Exit 0; local smoke OK |
| `python3 -B -m mp3 --help` | Exit 0; documented syntax |
| `python3 -B -m mp3 put sample /tmp/mp3-phase0-no-value-file` | Expected exit 3; JSON NOT_IMPLEMENTED, no value file created |
| `python3 -B -m mp3 consistency_mode invalid` | Expected exit 2; JSON ERROR |
| Python `compile(read_bytes(), filename, 'exec')` for all source files in MP1/MP2/MP3 | 29 sources compile in memory, including 14 MP3 files |
| SHA-256 dictionary comparison with the pre-work temporary baseline | All 33 reference files identical; no added/deleted files |
| AST function-name inventory compared with EXPLANATION.md | Every defined MP3 function name present; semantic accuracy also independently reviewed |
| Local Markdown link target existence check | All six pre-review links and all seven final links resolved |
| Decision heading/status count | Exactly 18 KD records; all PENDING |

The latter small audit procedures were executed as an inline read-only Python
script, not added as a speculative permanent test framework. No measurements in
this evidence file are performance experiment results. Compile/tests were rerun
after the final framing docstring clarification; no runtime source changed after
that check. Phase 0 is complete; no Phase 1 implementation was started.

## Evidence boundaries

Not verified: live ten-VM membership, stable ring construction, restart recovery,
full RPC envelopes, durable storage, ABD linearizability, eventual fanout/read repair,
mode switching, operational commands, MP1 queries of MP3 logs, deployment, any MP3
performance experiment, report results or submission readiness. None is implemented
by Phase 0. See the plan's unchecked acceptance criteria and KD-01..18.
