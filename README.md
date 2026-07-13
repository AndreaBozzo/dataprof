<div align="center">
  <img src="assets/images/logo.png" alt="dataprof logo" width="800" height="auto" />
  <h1>dataprof</h1>
  <p>
    <strong>High-performance data profiling and quality assessment</strong>
  </p>

  [![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
  [![docs.rs](https://docs.rs/dataprof/badge.svg)](https://docs.rs/dataprof)
  [![PyPI](https://img.shields.io/pypi/v/dataprof.svg)](https://pypi.org/project/dataprof/)
  [![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)

  [Website](https://andreabozzo.github.io/dataprof/) · [Live benchmarks](https://andreabozzo.github.io/dataprof/benchmarks/) · [Getting started](docs/guides/getting-started.md)

</div>

---

dataprof is a Rust and Python library for profiling tabular data. It computes column-level statistics, detects data types and patterns, and assesses data quality across dimensions informed by ISO 8000 and ISO/IEC 25012, all with bounded memory usage that lets you profile datasets far larger than your available RAM.

It is built for the first ten minutes with unfamiliar data: find sparse columns, unstable types, duplicate keys, stale timestamps, and suspicious values before they turn into pipeline bugs.

> [!NOTE]
> dataprof is in beta. Current releases ship a Rust crate and a Python package. The historical CLI remains documented only for older releases.

## What dataprof answers quickly

| Question | What you get back |
|---|---|
| Which columns are thin, empty, or structurally broken? | Null counts, completeness metrics, and schema shape in one pass |
| Did this feed drift or spike somewhere suspicious? | Numeric summaries, outlier signals, and range checks |
| Are these IDs really unique or just pretending to be keys? | Distinct counts, uniqueness ratios, and duplicate warnings |
| Are my timestamps plausible and fresh? | Future-date detection, stale-data signals, and timeliness scoring |
| Did parsing silently go wrong? | Type inference, pattern matches, format violations, and source metadata |

## Pick your entry point

| You are doing this | Start with |
|---|---|
| Embedding profiling in a Rust service, ETL job, or batch tool | `cargo add dataprof` and `Profiler::new().analyze_file(...)` |
| Inspecting files in notebooks, validation scripts, or data apps | `uv pip install dataprof` and `dp.profile(...)` |
| Profiling streams, remote Parquet, or database queries | Rust feature flags, or a source-built Python extension with async/database features enabled |

## Start in 30 Seconds

### Python

```bash
uv pip install dataprof
```

Requires Python 3.10 or newer.

The pre-built PyPI wheels have **no Python dependencies**. Everything below runs on a bare `pip install dataprof`: local files, dicts, row dicts, bytes buffers, and every export in this section. Install the `pandas` extra only for the pandas-typed exports (`to_dataframe()`, `describe()` as a DataFrame) and for Parquet *byte buffers*. Async URL profiling and database helpers are opt-in source builds.

#### 1. Profile

```python
import dataprof as dp

report = dp.profile("data.csv")
print(f"{report.rows} rows, {report.columns} columns")

# Ad-hoc inputs work the same way, with no pandas involved
scratch = dp.profile({"age": [31, 42, 29], "city": ["Rome", "Milan", "Rome"]})
incoming = dp.profile(b"age,city\n31,Rome\n", format="csv")
```

Too big to profile fully? Get the shape first, then commit:

```python
structure = dp.analyze_structure("data.csv")   # cheap first pass
print(structure.format, len(structure.columns), structure.row_count.count)
```

#### 2. Interpret

```python
print(f"quality={report.quality_score:.1f}")   # 0-100, weighted across assessed dimensions
print(report.quality_summary())                # per-dimension scores

age = report["age"]
print(age.data_type, age.mean, age.null_percentage)
```

#### 3. Export

```python
report.save("report.json")       # full report, reloadable
print(report.to_markdown())      # a table for a PR comment or a notebook
```

#### 4. Compare

Profile before and after a cleaning step, or yesterday against today:

```python
before = dp.ProfileReport.load("report.json")
after = dp.profile("data_clean.csv")

delta = before.compare(after)    # what changed, per column
```

#### 5. Hand it to an agent

A token-bounded summary of shape, quality flags, and schema. Values matching a
sensitive pattern are never echoed, and no raw cell values are included unless
you ask:

```python
print(report.to_llm_context(max_tokens=500))
```

## Three problems, solved end to end

Each example generates its own data and runs from a clean checkout. Every number
they print is real profiler output, and CI runs all six on every push.

| Scenario | What it shows | Run it |
|---|---|---|
| [Messy CSV inspection](examples/messy_csv_inspection.rs) · [Python](python/examples/messy_csv_inspection.py) | A duplicated key, null-heavy columns, a negative price, and PII flagged but never printed | `cargo run --example messy_csv_inspection` |
| [ETL quality gate](examples/etl_quality_gate.rs) · [Python](python/examples/etl_quality_gate.py) | Accept or reject a daily drop on thresholds, with the rejection reason in the log | `cargo run --example etl_quality_gate` |
| [Before/after cleaning](examples/before_after_cleaning.rs) · [Python](python/examples/before_after_cleaning.py) | Save a baseline report, diff it against the cleaned data, and check the defects really went away | `cargo run --example before_after_cleaning` |

See [examples/README.md](examples/README.md) for the Python commands and a note on
two quality metrics that surprise people.

### Rust

```toml
[dependencies]
dataprof = "0.9"
# or: dataprof = { version = "0.9", default-features = false }
```

Minimum supported Rust version: 1.96.

```rust
use dataprof::Profiler;

let report = Profiler::new().analyze_file("data.csv")?;
println!("Rows: {}", report.execution.rows_processed);
println!("Quality: {:.1}%", report.quality_score().unwrap_or(0.0));

for col in &report.column_profiles {
  println!("{} {:?} nulls={}", col.name, col.data_type, col.null_count);
}
```

## Why it feels modern

- **Fast first-pass signal** -- surface null pockets, type drift, duplicate keys, and outliers quickly
- **True streaming** -- bounded-memory profiling with online algorithms for files bigger than RAM
- **Multi-format by default** -- move from CSV and JSON to Parquet, live databases, DataFrames, and Arrow batches without changing tools
- **Two polished entry points** -- a compact Rust facade and a Python package that feels natural in notebooks
- **Async-ready** -- Rust async APIs and opt-in Python extension builds cover stream pipelines, services, and remote Parquet sources
- **Explainable quality assessment** -- completeness, consistency, uniqueness, accuracy, and timeliness with inspectable facts behind every score

## Feature Flags

| Feature | Description |
|---|---|
| `arrow` | Arrow-backed columnar engine |
| `parquet` *(default)* | Parquet profiling; includes `arrow` |
| `async-streaming` | Async profiling engine with tokio |
| `parquet-async` | Profile Parquet files over HTTP; includes `parquet` and `async-streaming` |
| `database` | Database profiling (connection handling, retry, SSL) |
| `postgres` | PostgreSQL connector (includes `database`) |
| `mysql` | MySQL/MariaDB connector (includes `database`) |
| `sqlite` | SQLite connector (includes `database`) |
| `all-db` | All three database connectors |

For the leanest Rust build, use `default-features = false` or `cargo --no-default-features` instead of a separate `minimal` alias.

## Supported Formats

| Format | Engine | Notes |
|---|---|---|
| CSV | Incremental, Columnar | Auto-detects `,` `;` `\|` `\t` delimiters |
| JSON | Incremental | Array-of-objects |
| JSONL / NDJSON | Incremental | One object per line |
| Parquet | Columnar | Reads metadata for schema/count without scanning rows |
| Database query | Async | PostgreSQL, MySQL, SQLite via connection string |
| pandas / polars DataFrame | Columnar | Python API only |
| Arrow RecordBatch | Columnar | Via PyCapsule (zero-copy) or Rust API |
| dict / list of dicts | Columnar | Python API only; no dependencies |
| bytes / BytesIO | Columnar | Python API only; requires `format=`. CSV, JSON and JSONL need no dependencies; Parquet bytes need the `pandas` extra |
| Async byte stream | Incremental | Any `AsyncRead` source (HTTP, WebSocket, etc.) |

## Quality Metrics

dataprof reports five quality dimensions informed by concepts in [ISO 8000-8](https://www.iso.org/standard/76834.html) and [ISO/IEC 25012](https://www.iso.org/standard/35749.html):

| Dimension | What it measures |
|---|---|
| **Completeness** | Missing-cell percentage, share of rows with no nulls, columns past the null threshold |
| **Consistency** | Data type consistency, format violations, encoding issues |
| **Uniqueness** | Duplicate rows, key uniqueness, high-cardinality warnings |
| **Accuracy** | Outlier ratio, range violations, negative values in positive-only columns |
| **Timeliness** | Future dates, stale data ratio, temporal ordering violations in explicit temporal columns |

The overall quality score (0 -- 100) is dataprof's weighted average of the
dimensions that had data to assess. The aggregation formula is not mandated or
certified by ISO. Rust callers can customize the relative weights through
`IsoQualityConfig::score_weights`; default weights are 0.30 / 0.25 / 0.20 /
0.15 / 0.10 in the table order above.

## Documentation

### Start here

- [Getting Started](docs/guides/getting-started.md) -- the shortest path from mystery dataset to useful signal
- [Examples Cookbook](docs/guides/examples.md) -- focused Rust and Python recipes you can adapt quickly
- [Agent Workflows](docs/guides/agent-workflows.md) -- copy-paste guidance for AGENTS.md, Cursor rules, and Claude Code skills

### Integrate it

- [Python API Guide](docs/python/README.md) -- files, DataFrames, Arrow interop, exports, and optional source-built async/database features
- [Database Connectors](docs/guides/database-connectors.md) -- PostgreSQL, MySQL, SQLite setup and connection patterns

### Understand it

- [Crate Redesign Notes](docs/architecture/crate-redesign.md) -- what the facade owns and why the workspace is split this way
- [Contributing](docs/CONTRIBUTING.md)
- [Changelog](docs/CHANGELOG.md)

### Historical

- [Archived CLI Guide](docs/archive/CLI_USAGE_GUIDE.md) -- pre-0.8 reference only

## Academic Work

dataprof is the subject of a peer-reviewed paper submitted to **IEEE ScalCom 2026**:

> A. Bozzo, "A Compiled Paradigm for Scalable and Sustainable Edge AI: Out-of-Core Execution and SIMD Acceleration in Telemetry Profiling," *IEEE ScalCom 2026* (under review).
> [[Repository & reproducible benchmarks]](https://github.com/AndreaBozzo/scalcom2026-dataprof)

The paper benchmarks dataprof against YData Profiling, Polars, and pandas across execution efficiency, memory scalability, energy consumption, and zero-copy interoperability in constrained Edge AI environments.

### BibTeX

```bibtex
@inproceedings{bozzo2026compiled,
  author={Bozzo, Andrea},
  title={A Compiled Paradigm for Scalable and Sustainable Edge AI: Out-of-Core Execution and SIMD Acceleration in Telemetry Profiling},
  booktitle={2026 IEEE International Conference on Scalable Computing and Communications (ScalCom)},
  year={2026},
  note={Under review}
}
```


## License

Dual-licensed under either the [MIT License](LICENSE) or the [Apache License, Version 2.0](LICENSE-APACHE), at your option.
