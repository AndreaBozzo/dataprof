<div align="center">
  <img src="assets/images/logo.png" alt="dataprof logo" width="800" height="auto" />
  <h1>dataprof</h1>
  <p>
    <strong>High-performance data profiling with ISO 8000/25012 quality metrics</strong>
  </p>

  [![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
  [![docs.rs](https://docs.rs/dataprof/badge.svg)](https://docs.rs/dataprof)
  [![PyPI](https://img.shields.io/pypi/v/dataprof.svg)](https://pypi.org/project/dataprof/)
  [![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)

</div>

---

dataprof is a Rust and Python library for profiling tabular data. It computes column-level statistics, detects data types and patterns, and evaluates data quality against the ISO 8000/25012 standard, all with bounded memory usage that lets you profile datasets far larger than your available RAM.

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
| Embedding profiling in a Rust service, ETL job, or batch tool | `cargo add dataprof@0.8` and `Profiler::new().analyze_file(...)` |
| Inspecting files in notebooks, validation scripts, or data apps | `uv pip install dataprof` and `dp.profile(...)` |
| Profiling streams, remote Parquet, or database queries | Rust feature flags, or a source-built Python extension with async/database features enabled |

## Start in 30 Seconds

### Python

```bash
uv pip install dataprof
```

Pre-built PyPI wheels ship the base Python API for local files, DataFrames, Arrow objects, and notebook-friendly ad-hoc inputs such as dicts, row dicts, and bytes buffers. Async URL profiling and database helpers remain opt-in source builds.

```python
import dataprof as dp

report = dp.profile("data.csv", metrics=["schema", "statistics", "quality"])
print(f"{report.rows} rows, {report.columns} columns")
print(f"quality={report.quality_score:.1f}")

age = report["age"]
print(age.data_type, age.mean, age.null_percentage)

report.save("report.json")

# Notebook-friendly ad-hoc inputs also work
scratch = dp.profile({"age": [31, 42, 29], "city": ["Rome", "Milan", "Rome"]})
incoming = dp.profile(b"age,city\n31,Rome\n", format="csv")
```

### Rust

```toml
[dependencies]
dataprof = "0.8"
# or: dataprof = { version = "0.8", default-features = false }
```

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
- **ISO 8000/25012 quality assessment** -- five dimensions: Completeness, Consistency, Uniqueness, Accuracy, Timeliness

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
| dict / list of dicts / bytes | Columnar | Python convenience path; bytes require `format=` |
| Async byte stream | Incremental | Any `AsyncRead` source (HTTP, WebSocket, etc.) |

## Quality Metrics

dataprof evaluates data quality against the five dimensions defined in [ISO 8000-8](https://www.iso.org/standard/76834.html) and [ISO/IEC 25012](https://www.iso.org/standard/35749.html):

| Dimension | What it measures |
|---|---|
| **Completeness** | Missing values ratio, complete records ratio, fully-null columns |
| **Consistency** | Data type consistency, format violations, encoding issues |
| **Uniqueness** | Duplicate rows, key uniqueness, high-cardinality warnings |
| **Accuracy** | Outlier ratio, range violations, negative values in positive-only columns |
| **Timeliness** | Future dates, stale data ratio, temporal ordering violations |

An overall quality score (0 -- 100) is computed as a weighted average of dimension scores.

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
