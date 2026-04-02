# Getting Started with dataprof

This guide walks you through installing dataprof, running your first profile, and understanding the results. By the end you'll know how to use all three interfaces (CLI, Python, Rust), interpret quality metrics, and handle large datasets.

## What is Data Profiling?

Data profiling is the process of examining a dataset to collect statistics and assess quality. It answers questions like:

- How many rows and columns does the data have?
- What data types are present? Are they consistent?
- How many values are missing? Which columns are most affected?
- Are there duplicates? Outliers? Impossible values?
- Is the data fresh or stale?

dataprof automates this analysis and scores data quality against the ISO 8000/25012 standard, producing a structured report you can use for data validation, ETL pipelines, and quality monitoring.

## Installation

Choose the interface that fits your workflow:

```bash
# CLI (Rust binary)
cargo install dataprof

# Python
uv pip install dataprof
# or: pip install dataprof

# Rust library -- add to Cargo.toml
# dataprof = "0.6"
```

## Your First Profile

### With the CLI

```bash
dataprof analyze sales.csv
```

This prints a text report to your terminal. You'll see:

1. **Source info** -- file name, size, column count, row count, scan time
2. **Column profiles** -- for each column: data type, null count, basic statistics
3. **Quality assessment** -- scores for each ISO dimension, plus an overall score

To get the detailed breakdown of all five quality dimensions:

```bash
dataprof analyze sales.csv --detailed
```

To export as JSON for scripting or CI:

```bash
dataprof analyze sales.csv --format json -o report.json
```

### With Python

```python
import dataprof as dp

report = dp.profile("sales.csv")

# High-level summary
print(f"Rows: {report.rows}")
print(f"Columns: {report.columns}")
print(f"Quality: {report.quality_score}")

# Inspect each column
for col in report.column_profiles.values():
    print(f"  {col.name}: {col.data_type}, {col.null_percentage:.1f}% null")

# Export to pandas for further analysis
df = report.to_dataframe()
```

### With Rust

```rust
use dataprof::Profiler;

fn main() -> Result<(), dataprof::DataProfilerError> {
    let report = Profiler::new().analyze_file("sales.csv")?;

    println!("Rows: {}", report.execution.rows_processed);
    println!("Columns: {}", report.execution.columns_detected);

    if let Some(quality) = &report.quality {
        println!("Quality score: {:.1}%", quality.score());
    }

    for col in &report.column_profiles {
        println!("  {} ({:?}): {} nulls out of {}",
            col.name, col.data_type, col.null_count, col.total_count);
    }

    Ok(())
}
```

## Understanding Quality Metrics

dataprof evaluates data quality across five ISO 8000/25012 dimensions. Each is scored from 0 to 100, and the overall score is a weighted average.

### Completeness

Measures how much data is present vs. missing.

- `missing_values_ratio` -- fraction of null/empty values across all columns
- `complete_records_ratio` -- fraction of rows with zero nulls
- `null_columns` -- columns that are entirely null

A completeness score of 0.95 means 95% of expected values are present.

### Consistency

Measures whether values conform to their expected types and formats.

- `data_type_consistency` -- fraction of values matching the inferred type
- `format_violations` -- values that don't match detected patterns (e.g. an email column with non-email values)
- `encoding_issues` -- invalid character encoding detected

### Uniqueness

Measures duplication in the data.

- `duplicate_rows` -- number of exact duplicate rows
- `key_uniqueness` -- ratio of distinct values in likely-key columns
- `high_cardinality_warning` -- flagged when a column has suspiciously many unique values

### Accuracy

Measures whether values fall within expected ranges.

- `outlier_ratio` -- fraction of values outside the interquartile range
- `range_violations` -- values outside expected bounds
- `negative_values_in_positive` -- negative numbers in columns that should be positive

### Timeliness

Measures whether date/time values are current and consistent.

- `future_dates_count` -- dates that are in the future
- `stale_data_ratio` -- fraction of temporal data that appears outdated
- `temporal_violations` -- ordering inconsistencies in time series

### Threshold Profiles

The CLI supports three threshold profiles that set the bar for "good" quality:

```bash
dataprof analyze data.csv --threshold-profile strict    # high bar
dataprof analyze data.csv --threshold-profile default   # balanced (default)
dataprof analyze data.csv --threshold-profile lenient    # low bar
```

## Working with Different Formats

### CSV

dataprof auto-detects the delimiter (`,`, `;`, `|`, `\t`) by analyzing the first few lines:

```bash
dataprof analyze european_data.csv    # semicolons -- auto-detected
dataprof analyze pipe_export.csv      # pipes -- auto-detected
```

For files with inconsistent column counts, enable flexible parsing:

```python
report = dp.profile("messy.csv", csv_flexible=True)
```

### JSON and JSONL

JSON files should contain an array of objects. JSONL files should have one JSON object per line.

```bash
dataprof analyze users.json
dataprof analyze events.jsonl
```

### Parquet

Parquet is the most efficient format to profile. Schema inference and row counting read only the file metadata -- zero row scanning required.

```bash
dataprof schema data.parquet     # instant schema from metadata
dataprof count data.parquet      # instant row count from metadata
dataprof analyze data.parquet    # full profiling reads all row groups
```

## Scaling Up: Sampling and Stop Conditions

For very large files, you don't always need to read every row. dataprof provides two mechanisms to limit work.

### Sampling

Read a representative subset:

```bash
# CLI: sample first 100k rows
dataprof analyze huge.csv --sample 100000
```

```python
# Python: reservoir sampling (statistically representative)
from dataprof import SamplingStrategy
report = dp.profile("huge.csv", sampling=SamplingStrategy.reservoir(100000))
```

```rust
// Rust
use dataprof::{Profiler, SamplingStrategy};
let report = Profiler::new()
    .sampling(SamplingStrategy::Reservoir { size: 100_000 })
    .analyze_file("huge.csv")?;
```

### Stop Conditions

Stop processing when a criterion is met, without needing to know the total size upfront:

```python
from dataprof import StopCondition

# Stop after 10k rows OR 50 MB, whichever comes first
stop = StopCondition.max_rows(10000) | StopCondition.max_bytes(50_000_000)
report = dp.profile("stream.csv", stop_condition=stop)
```

```rust
use dataprof::{Profiler, StopCondition};
let report = Profiler::new()
    .stop_when(StopCondition::MaxRows(10_000))
    .analyze_file("huge.csv")?;
```

## Async and Streaming

When embedding dataprof in a web service or processing live streams, use the async API:

### Python

```python
from dataprof.asyncio import profile_file, profile_url

# Profile a local file asynchronously
report = await profile_file("data.csv")

# Profile a remote file over HTTP
report = await profile_url("https://example.com/data.parquet")
```

### Rust

```rust
use dataprof::{Profiler, AsyncDataSource, BytesSource, AsyncSourceInfo, FileFormat};

// Profile an async byte stream
let source = BytesSource::new(bytes, AsyncSourceInfo {
    label: "upload".into(),
    format: FileFormat::Csv,
    size_hint: Some(bytes.len() as u64),
    source_system: None,
});
let report = Profiler::new().profile_stream(source).await?;
```

## Database Profiling

Profile data directly from PostgreSQL, MySQL, or SQLite without exporting to files.

### CLI

```bash
cargo install dataprof --features full-cli
dataprof database postgres://user:pass@localhost/mydb --table users --quality
```

### Python

```python
report = await dp.analyze_database_async(
    "postgres://user:pass@localhost/mydb",
    "SELECT * FROM users",
    calculate_quality=True,
)
```

### Rust

```rust
use dataprof::{Profiler, DatabaseConfig};

let report = Profiler::new()
    .connection_string("postgres://user:pass@localhost/mydb")
    .analyze_query("SELECT * FROM users")
    .await?;
```

See the [Database Connectors Guide](database-connectors.md) for connection strings, SSL, retry configuration, and more.

## Next Steps

- [Examples Cookbook](examples.md) -- copy-pasteable recipes for common tasks
- [CLI Usage Guide](CLI_USAGE_GUIDE.md) -- every subcommand and flag
- [Python API Guide](../python/README.md) -- full API reference
- [Database Connectors](database-connectors.md) -- advanced database setup
- [Contributing](../CONTRIBUTING.md) -- how to contribute to dataprof
