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

dataprof is a Rust library and CLI for profiling tabular data. It computes column-level statistics, detects data types and patterns, and evaluates data quality against the ISO 8000/25012 standard -- all with bounded memory usage that lets you profile datasets far larger than your available RAM.

## Highlights

- **Rust core** -- fast columnar and streaming engines
- **ISO 8000/25012 quality assessment** -- five dimensions: Completeness, Consistency, Uniqueness, Accuracy, Timeliness
- **Multi-format** -- CSV (auto-delimiter detection), JSON, JSONL, Parquet, databases, DataFrames, Arrow
- **True streaming** -- bounded-memory profiling with online algorithms (Incremental engine)
- **Three interfaces** -- CLI binary, Rust library, Python package
- **Async-ready** -- `async/await` API for embedding in web services and stream pipelines

## Quick Start

### CLI

```bash
cargo install dataprof

dataprof analyze data.csv --detailed
dataprof schema data.csv
dataprof count data.parquet
```

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
import dataprof

report = dataprof.profile("data.csv")
print(f"{report.rows} rows, {report.columns} columns")
print(f"Quality score: {report.quality_score}")

for col in report.column_profiles.values():
    print(f"  {col.name} ({col.data_type}): {col.null_percentage:.1f}% null")
```

## Installation

### CLI binary

```bash
cargo install dataprof                        # default (CLI only)
cargo install dataprof --features full-cli    # CLI + all formats + databases
```

### Rust library

```toml
[dependencies]
dataprof = "0.6"                  # core library (no CLI deps)
dataprof = { version = "0.6", features = ["async-streaming"] }
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
| `cli` *(default)* | CLI binary with clap, colored output, progress bars |
| `minimal` | CSV-only, no CLI -- fastest compile |
| `async-streaming` | Async profiling engine with tokio |
| `parquet-async` | Profile Parquet files over HTTP |
| `database` | Database profiling (connection handling, retry, SSL) |
| `postgres` | PostgreSQL connector (includes `database`) |
| `mysql` | MySQL/MariaDB connector (includes `database`) |
| `sqlite` | SQLite connector (includes `database`) |
| `all-db` | All three database connectors |
| `datafusion` | DataFusion SQL engine integration |
| `python` | Python bindings via PyO3 |
| `python-async` | Async Python API (includes `python` + `async-streaming`) |
| `full-cli` | CLI + Parquet + all databases |
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

- [CLI Usage Guide](docs/guides/CLI_USAGE_GUIDE.md) -- every subcommand and flag
- [Python API Guide](docs/python/README.md) -- `profile()`, report types, async, databases
- [Getting Started](docs/guides/getting-started.md) -- tutorial from zero to profiling
- [Examples Cookbook](docs/guides/examples.md) -- copy-pasteable recipes (CLI, Python, Rust)
- [Database Connectors](docs/guides/database-connectors.md) -- PostgreSQL, MySQL, SQLite setup
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
