# MP3 report template

`report.tex` is a 12-point, two-page starting template. Page 1 reserves at most one
page for design and LLM use; page 2 reserves the required plots and student-written
discussion. It contains no measurements. Its default banner says **PENDING
MEASUREMENTS — NOT A SUBMISSION REPORT**.

The final report command refuses to build unless it finds all 51 valid full trials,
each with at least 180 seconds per client and five-owner preload evidence. It then
emits per-trial and per-configuration CSV summaries, generates the three plots, and
requires truthful transcripts under the run's `llm_use/`
directory, compiles with `pdflatex`, checks `pdfinfo` reports fewer than three pages,
and copies the PDF to the requested destination. `pdflatex` and `pdfinfo` must be
available locally; neither is installed by this project.

CSV files are written under `<run-dir>/aggregates/`: `trial_metrics.csv` includes the
measurement interval used as the throughput denominator and min/max actual worker
durations; `configuration_summary.csv` gives the mean and sample standard deviation
across the three trials for each plotted configuration.

After real trials are collected, from the repository root:

```bash
cp mp3/report/report.tex /path/to/verified-run/report.tex
# Edit that copy: replace every \pending{...} placeholder with verified design and
# measured analysis, and save actual transcripts under verified-run/llm_use/.
python3 -B -m mp3.benchmarks report --run-dir /path/to/verified-run \
  --report-source /path/to/verified-run/report.tex \
  --output /path/to/final-report.pdf
```

The student authors must replace each `\pending{...}` placeholder with accurate,
source-supported explanation and measured analysis, keep the design section within
one page and LLM use below 300 words, then review the compiled PDF before submission.
The final builder does not create transcripts or write results/discussion on the
authors' behalf.
