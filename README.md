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

> [!NOTE]
> dataprof is in beta. This branch is library-first: the old CLI has been retired, while Rust and Python APIs remain the primary supported interfaces.

## Highlights

- **Rust core** -- fast columnar and streaming engines
- **ISO 8000/25012 quality assessment** -- five dimensions: Completeness, Consistency, Uniqueness, Accuracy, Timeliness
- **Multi-format** -- CSV (auto-delimiter detection), JSON, JSONL, Parquet, databases, DataFrames, Arrow
- **Boolean Support** -- Native profiling of boolean columns with true/false statistics
- **True streaming** -- bounded-memory profiling with online algorithms (Incremental engine)
- **Library-first interfaces** -- Rust crate and Python package
- **New Python Ecosystem** -- Export to pandas, Polars, Arrow, and JSON with rounding consistency
- **Async-ready** -- `async/await` API for embedding in web services and stream pipelines

## Quick Start

### Rust

```rust
use dataprof::Profiler;

let report = Profiler::new().analyze_file("data.csv")?;
println!("Rows: {}", report.execution.rows_processed);
println!("Quality: {:.1}%", report.quality_score().unwrap_or(0.0));

for col in &report.column_profiles {
    println!("  {} ({:?}): {} nulls", col.name, col.data_type, col.null_count);
}
```

### Python

```python
import dataprof as dp

# Profile with selective metrics for speed
report = dp.profile("data.csv", metrics=["schema", "statistics"])
print(f"{report.rows} rows, {report.columns} columns")

# Dict-like access and export to pandas/Polars
age_stats = report["age"]
print(f"Mean age: {age_stats.mean}")

df = report.to_dataframe()
report.save("report.json")
```

## Which Interface Should I Use?

| Use case | Start here |
|---|---|
| Add profiling to a Rust service or data tool | Rust API: `Profiler::new().analyze_file(...)` |
| Work from notebooks, pandas, Polars, or PyArrow | Python: `dp.profile(...)` |
| Profile PostgreSQL, MySQL, or SQLite | Rust or Python with database feature flags |

## Installation

### Rust library

```toml
[dependencies]
dataprof = "0.7"                  # core library (no CLI deps)
dataprof = { version = "0.7", features = ["async-streaming"] }
```

### Python package

```bash
uv pip install dataprof
# or
pip install dataprof
```

## Feature Flags

| Feature | Description |
|---|---|
| `arrow` | Arrow-backed columnar engine |
| `parquet` *(default)* | Parquet profiling; includes `arrow` |
| `minimal` | No optional interfaces; library only |
| `async-streaming` | Async profiling engine with tokio |
| `parquet-async` | Profile Parquet files over HTTP; includes `parquet` and `async-streaming` |
| `database` | Database profiling (connection handling, retry, SSL) |
| `postgres` | PostgreSQL connector (includes `database`) |
| `mysql` | MySQL/MariaDB connector (includes `database`) |
| `sqlite` | SQLite connector (includes `database`) |
| `all-db` | All three database connectors |
| `python` | Deprecated facade alias; real bindings live in `crates/dataprof-python` |
| `python-async` | Deprecated facade alias; async Python bindings live in `crates/dataprof-python` |
| `cli` | Deprecated compatibility alias; no CLI binary is built |
| `full-cli` | Deprecated compatibility alias for database features; no CLI binary is built |
| `production` | PostgreSQL + MySQL (common deployment) |

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

- [Legacy CLI Usage Guide](docs/archive/CLI_USAGE_GUIDE.md) -- retained for historical reference
- [Python API Guide](docs/python/README.md) -- `profile()`, report types, async, databases
- [Getting Started](docs/guides/getting-started.md) -- tutorial from zero to profiling
- [Examples Cookbook](docs/guides/examples.md) -- copy-pasteable recipes for Python and Rust
- [Database Connectors](docs/guides/database-connectors.md) -- PostgreSQL, MySQL, SQLite setup
- [Crate Redesign Notes](docs/architecture/crate-redesign.md) -- planned architecture direction
- [Contributing](docs/CONTRIBUTING.md)
- [Changelog](docs/CHANGELOG.md)

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
