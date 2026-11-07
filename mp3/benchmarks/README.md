# MP3 benchmark runbook

This package uses the existing control endpoint for actual `put` and `get` calls.
Client workers are closed-loop: one worker sends one operation and waits for its
response before issuing the next. They persist as local processes during a trial;
the full controller starts each resident worker once over SSH and does not SSH per
measured operation.

## Local rehearsal

```bash
MP3_TARGET=local python3 -B -m mp3.benchmarks smoke
```

This creates an isolated temporary ten-node loopback cluster, preloads a small key
set, checks all five owner copies, runs two concurrent persistent clients briefly,
aggregates the real local calls and renders a plot with a **NOT FOR REPORT** watermark.
It does not contact UIUC hosts. The local node processes, databases and logs are removed
with their temporary cluster directory; watermarked JSONL, summaries, preload proof and
plot are copied into the returned artifact directory. By default it is
`${tempfile.gettempdir()}/mp3-benchmark-<smoke-id>/` (typically a system temporary
directory, not necessarily `/tmp`). `--output-dir` selects a parent. The local smoke
output cannot pass final-report validation.

## Future UIUC VM run (not run by the local verification task)

Confirm current course hostnames, SSH access, interpreter/SQLite support and ports
before proceeding. The runner requires a new isolated remote root and explicit run ID;
it never calls `reset_mp` or targets the demo's `~/mp3_deploy` directory. Example:

```bash
MP3_TARGET=remote MP3_USER=<netid> \
python3 -B -m mp3.benchmarks full \
  --manifest /path/to/course-manifest.json \
  --run-dir results/run-2026-11-06 \
  --run-id run-2026-11-06 \
  --remote-root '$HOME/mp3_benchmark_run-2026-11-06' \
  --confirm-isolated-root run-2026-11-06
```

The explicit confirmation must match the run ID; the controller refuses the known
demo root or an already-existing isolated root. Repeating the same run ID and local
results directory resumes a checkpoint only when the manifest matches and all ten
isolated roots still exist. The controller deploys and starts
the current tree there, switches modes through the existing mode command, restarts
all nodes when R/W startup settings change, verifies all ten nodes' mode/R/W and
ALIVE membership, and preloads/verifies the full dataset before each new config.
Experiment 1 has 45 trials and Experiment 2 has 6. The full command runs all 51
trials at the required 180 seconds per client; it has no short-duration option.
Every trial is checkpointed with raw JSONL and a JSON summary. Failed attempts are
preserved; only an explicit `--retry-failed` reruns them, with the earlier attempt
retained. Never run the full command merely to check connectivity.

The benchmark dataset is 100,000 deterministic 1 KiB keys under a dedicated
`bench-key-` prefix. The 20 `business_*` demo files are separate and are not used as
preload records. Preload retries are setup actions and are explicitly counted. For
each key, the preloader directly reads its value from each of the five prescribed
owners and requires the exact bytes plus a present version at every owner. It records
versions independently; equality is not required. Paginated `liststore` inventories
provide a second key-presence check. Retained evidence binds each key to its exact ring
owners and rejects missing or duplicate key indexes. Preload duration is excluded
from trial timing.

## Final plots and PDF

Copy `mp3/report/report.tex` to the run directory, replace every `\pending{...}`
placeholder with student-reviewed design and measured observations, and save the
team's actual transcripts under `llm_use/`. Then run:

```bash
python3 -B -m mp3.benchmarks report \
  --run-dir results/run-2026-11-06 \
  --report-source results/run-2026-11-06/report.tex \
  --output results/run-2026-11-06/report.pdf
```

This validates all 51 real remote trials, each client's actual 180-second minimum,
worker metadata, exact preload key/index coverage, raw per-operation counts and
recomputed metrics before plotting. It writes per-trial and per-configuration CSVs,
one Experiment 1 throughput-vs-latency plot and two Experiment 2 bar plots with
sample-standard-deviation error bars. It refuses smoke, missing or malformed data. Save the truthful
LLM-use prompt/response transcripts from the project under `llm_use/` in the run
directory before finalizing. The report remains a student-reviewed template: replace
the design and discussion placeholders with the actual design and observed evidence,
keep it under three pages and inspect the PDF. The local environment used for this
framework did not have `pdflatex`; final compilation must be done where LaTeX and
`pdfinfo` are installed.
