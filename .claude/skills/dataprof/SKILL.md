---
name: dataprof
description: Profiles CSV, JSON, JSONL, and Parquet files with dataprof to report schema, null rates, detected patterns, and data-quality scores. Use when inspecting an unfamiliar dataset, debugging data quality, comparing two dataset versions for drift, or preparing compact evidence for a data-cleaning or pipeline decision.
---

# dataprof

`dataprof` is a local, deterministic profiler. It reports on data; it never
transforms, cleans, or moves it. It emits structured signals — interpreting them
is your job, not dataprof's.

## Fast path

For the common cases, run the bundled script instead of writing the code. It is
executed, not read, so it costs nothing but its output, and it never prints raw
cell values:

```bash
python scripts/dp_context.py data.csv                    # summary + quality + caveats
python scripts/dp_context.py data.csv --column email     # one column
python scripts/dp_context.py data.csv --structure-only   # cheap first look
python scripts/dp_context.py before.csv --compare after.csv
python scripts/dp_context.py data.csv --root /srv/data   # untrusted path
```

Write Python directly when you need something the script does not cover —
narrowing metric packs, semantic policies, sampling strategies, or feeding a
DataFrame. The workflow below is that path.

## Workflow

### 1. Check what this installation can do

Optional features are compiled in, not always present. Unsupported must not be
reported to the user as broken.

```python
import dataprof as dp

caps = dp.capabilities()
```

Fields include `local_csv`, `local_json`, `local_jsonl`, `local_parquet`,
`pandas_interop`, `polars_interop`, `arrow_interop`, `async_streaming`,
`url_profiling`, `remote_parquet`, and `database`. Check the one you need before
profiling Parquet, a URL, or a database; otherwise skip this step.

### 2. Cheap structural pass

```python
structure = dp.analyze_structure("data.csv")
```

Use this first on an unfamiliar dataset. It answers "what shape is this?"
without paying for full metrics.

### 3. Full profile

```python
report = dp.profile("data.csv")
```

`profile()` computes every metric pack by default. Pass `metrics=[...]` only to
*narrow* the work — the packs are `schema`, `statistics`, `patterns`, `quality`:

```python
report = dp.profile("data.csv", metrics=["schema", "quality"])
```

Quality has seven selectively requestable dimensions: `completeness`,
`consistency`, `uniqueness`, `accuracy`, `timeliness`, `validity`, `precision`.
Narrow them with `quality_dimensions=[...]`.

Semantic policies must be explicit when the data alone cannot establish them:
`positive_columns`, `identifier_columns`, `temporal_columns`.

### 4. Check the trust signals before you report a single number

**Do this every time.** A profile of a truncated, sampled, or partly failed read
still returns confident-looking numbers. Reporting those without the caveat is
the worst failure mode available here — a plausible wrong number.

```python
report.sampling_applied     # True -> numbers describe a sample, not the dataset
report.sampling_ratio       # fraction actually read
report.truncation_reason    # non-None -> the read stopped early, and why
report.source_exhausted     # False -> the source was not read to the end
report.low_sample_warning   # True -> too few rows to trust distributions
report.error_count          # rows that failed to parse
report.ragged_row_count     # rows whose field count did not match the header
```

If any of these is set, say so in the same breath as the numbers.

### 5. Summarize

`to_llm_context()` is the preferred summary for chat. It is the only export that
enforces redaction: when a column carries a detected sensitive pattern (email,
phone, identifier, financial, geographic, network, file path) it reports the
pattern name and counts, never the values.

```python
print(report.to_llm_context(max_tokens=500))
```

`include_samples=True` is an explicit opt-in for non-sensitive numeric extrema
only. Do not enable it for data that might be sensitive.

Structured alternatives:

```python
report.to_markdown()      # markdown table of column profiles
report.quality_summary()  # single-row quality dict
```

`to_dict()` embeds a full per-column entry under `["columns"]`, so it grows with
table width. Select top-level fields instead of surfacing the whole dict:

```python
d = report.to_dict()
summary = {k: d[k] for k in ("source", "source_type", "execution", "quality")}
```

For one column, index the report directly: `report["email"]`.

### 6. Compare for drift

```python
before = dp.profile("data_before.csv")
after = dp.profile("data_after.csv")
delta = before.compare(after)
```

Use this for before/after cleaning, pipeline changes, or version drift. Do not
re-read and eyeball two files by hand.

## Reading the output honestly

- **`None` means "not analyzed"; empty means "analyzed, found nothing."** Never
  present a `None` score as perfect, and never replace it with zero. This holds
  across every metric, not just quality dimensions.
- A quality score is a measurement, not a verdict. Say what drove it.

Before interpreting a specific dimension, an approximate count, a detected
pattern, or a comparison, read [reference/interpretation.md](reference/interpretation.md).
It covers what each signal does and does not mean — including the ones that are
easy to report backwards, like validity on unpatterned columns.

## When the path came from a model or an end user

If the dataset path was chosen by an LLM or supplied by an untrusted caller —
rather than written by the developer — go through the guard instead of calling
`dp.profile()` directly. It resolves paths against a sandbox root, bounds the
work one call can do, and keeps file contents and host paths out of error
messages.

```python
from dataprof.agent import AgentGuard, SandboxPolicy

guard = AgentGuard(SandboxPolicy(roots=["/srv/data"]))
report = guard.profile("customers.csv")     # resolved under /srv/data
print(guard.llm_context(report))            # redacted by construction
```

`SandboxPolicy` bounds `max_file_bytes`, `max_rows`, `max_bytes`, and
`timeout_seconds`, and refuses symlink escape, network schemes, and raw samples
by default. Every rejection raises an `AgentSecurityError` subclass whose
message is safe to hand back to a model verbatim.

## Guardrails

- Prefer aggregates, schema summaries, and quality metrics over raw row dumps.
- Do not paste large raw datasets into the conversation.
- Do not infer data quality from the first few visible rows — that is what the
  profiler is for.
- State the source path, metrics requested, and any sampling or max-row limit.
- If the dataset may be sensitive, keep the work local and share only derived
  summaries.

## Reference

- `scripts/dp_context.py` — run it (see **Fast path**); `--help` lists the flags.
- [reference/api.md](reference/api.md) — the full agent-relevant API surface:
  every field on `ProfileReport`, `ColumnProfile`, `DataQualityMetrics`,
  `StructureReport`, and `Capabilities`; the `Profiler` builder; persistence;
  `SandboxPolicy` defaults.
- [reference/interpretation.md](reference/interpretation.md) — what each signal
  means and does not mean, dimension by dimension.
