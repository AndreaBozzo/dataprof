# Why dataprof?

The honest first question about any profiling tool: *why not just
`df.describe()`?* Polars and pandas ship one-line summaries, and
ydata-profiling produces a beautiful HTML report. All three are good tools.
This page explains what dataprof adds, and — just as important — when one of
the alternatives is the better choice.

dataprof's job is the **first ten minutes with unfamiliar data**: finding
sparse columns, unstable types, duplicate keys, stale timestamps, and
suspicious values *before* they turn into pipeline bugs. Everything below
follows from that job.

> **A note on performance claims:** this page makes none. dataprof's
> differences from the tools below are architectural (bounded memory,
> streaming, quality semantics), not benchmark numbers. Rigorous, reproducible
> measurements are published separately on the
> [live benchmark site](https://andreabozzo.github.io/dataprof/benchmarks/).

## What dataprof adds

- **Bounded memory.** dataprof profiles files directly with streaming
  algorithms, so memory use does not grow with file size. You can profile a
  dataset larger than your RAM without sampling it first. The alternatives
  below all require the dataset (or your sample of it) to be materialized in
  memory as a DataFrame.
- **Quality dimensions with inspectable evidence.** Beyond min/max/mean:
  duplicate-key detection, format violations, future and stale timestamps,
  mixed decimal scales, pattern conformance — each scored per dimension, with
  the underlying facts exposed so you can check *why* a score is what it is.
- **Reports as durable artifacts.** `save()` a baseline, `load()` it later,
  `compare()` it against today's drop. Before/after cleaning diffs and
  CI quality gates fall out of this naturally.
- **One tool across sources.** CSV, JSON/JSONL, Parquet, Arrow batches,
  pandas/polars DataFrames, dicts, byte buffers, and (opt-in) live database
  queries — same report shape from all of them. CSV, JSON, and JSONL byte
  buffers are dependency-free; Parquet byte buffers currently require the
  pandas extra.
- **A dependency-free Python wheel.** `pip install dataprof` pulls in no
  Python dependencies. Profiling local files, dicts, and CSV/JSON/JSONL byte
  buffers needs neither pandas nor numpy.
- **Agent-ready output.** `to_llm_context()` produces a token-bounded summary
  with fail-closed redaction of sensitive values — built for handing a
  dataset's shape to an LLM without handing it the data.

## Compared with the alternatives

### Polars `describe()`

**Prefer Polars when** your data already lives in a Polars pipeline, fits
comfortably in memory, and a numeric summary (nulls, min/max, mean, quantiles)
answers your question. Polars is an excellent analytics engine, and if you are
mid-pipeline, `df.describe()` is one line with zero extra tools.

**dataprof adds** the quality layer Polars doesn't attempt: duplicate-key and
uniqueness analysis, pattern/format violations, timeliness checks, per-dimension
scoring, and saved reports you can diff across runs. It also profiles the file
itself under bounded memory, so "is this 40 GB CSV even sane?" doesn't start
with loading 40 GB.

### pandas `describe()` / `info()`

**Prefer pandas when** the data is already a DataFrame in front of you and a
quick glance at dtypes, null counts, and summary stats is enough. It's
ubiquitous and every notebook user knows it.

**dataprof adds** everything in the list above, plus independence from the
pandas stack: the base wheel has no dependencies, and files, dicts, and byte
buffers profile without a DataFrame ever existing. When you *do* have a
DataFrame, `dp.profile(df)` gives it the same quality treatment as a file.

### ydata-profiling

**Prefer ydata-profiling when** the deliverable is a rich visual report for
humans: distributions, correlation matrices, interaction plots, missing-value
heatmaps. Its HTML output is genuinely good for exploratory analysis
presentations, and dataprof does not try to compete with it — dataprof has no
charts.

**dataprof adds** a different set of trade-offs: it runs on files larger than
memory (ydata-profiling requires a pandas DataFrame, so the dataset must be
materialized), produces machine-readable output designed for CI gates, diffs,
and agents rather than for reading in a browser, and installs without the
pandas/numpy/matplotlib dependency stack.

### Rule of thumb

| Situation | Reach for |
|---|---|
| Mid-pipeline sanity check on an in-memory DataFrame | Polars / pandas `describe()` |
| Visual exploratory report to share with humans | ydata-profiling |
| Unfamiliar file, possibly larger than RAM, possibly broken | dataprof |
| Quality gate in CI, drift check between runs | dataprof |
| Giving an LLM/agent a dataset's shape without leaking values | dataprof |
| Statistical deep-dive: correlations, distributions, plots | ydata-profiling or your notebook |

## What dataprof deliberately doesn't do

Knowing a tool's non-goals is half of trusting it:

- **No transformation or cleaning.** dataprof reports on data; it never
  modifies or moves it.
- **No visual dashboards.** Output is structured data (and markdown/JSON
  exports), not charts.
- **No rule-based validation DSL.** dataprof discovers problems it can detect
  from the data itself. If you have explicit business expectations to enforce
  ("this column must match this regex"), pair it with a validation tool —
  dataprof's reports make good inputs for deciding *which* rules to write.
- **No unbounded inference.** Context-sensitive dimensions (timeliness,
  uniqueness-as-key) stay conservative unless you provide explicit hints,
  because a confident wrong answer is worse than a narrow honest one.

If your first ten minutes with a dataset usually start with "what is this file
and what's wrong with it?", dataprof is built for exactly that. Start with
[Getting Started](getting-started.md).
