# Getting Started with dataprof

This guide walks you through installing dataprof, running your first profile, and understanding the results. By the end you'll know how to move from a mystery dataset to a report that shows where the data is thin, duplicated, inconsistent, or stale.

## What is Data Profiling?

Data profiling is the process of examining a dataset to collect statistics and assess quality. It answers questions like:

- How many rows and columns does the data have?
- What data types are present? Are they consistent?
- How many values are missing? Which columns are most affected?
- Are there duplicates? Outliers? Impossible values?
- Is the data fresh or stale?

dataprof automates this analysis and assesses quality across dimensions informed by ISO 8000 and ISO/IEC 25012, producing a structured report you can use for data validation, ETL pipelines, and quality monitoring. The aggregate score is dataprof's configurable formula, not an ISO certification.

## Installation

Choose the interface that fits your workflow:

```bash
# Python
uv pip install dataprof
# or: pip install dataprof

# Rust library -- add to Cargo.toml
# dataprof = "0.10"
```

The published Python wheels cover the base package API for local files, DataFrames, and Arrow objects. If you need async URL profiling or database helpers, build the extension from source with the corresponding Rust features.

## Your First Profile

### With Python

```python
import dataprof as dp

report = dp.profile("sales.csv")

# High-level summary
print(f"Rows: {report.rows}")
print(f"Columns: {report.columns}")
print(f"Quality: {report.quality_score}")

# Access columns directly (dict-like)
col = report["price"]
print(f"  price: mean={col.mean}, std={col.std_dev}")

# Iterate all columns
for name in report:
    col = report[name]
    print(f"  {col.name}: {col.data_type}, {col.null_percentage:.1f}% null")

# Quick pandas-like summary
print(report.describe())

# Export to pandas, polars, or Arrow for further analysis
df = report.to_dataframe()           # pandas
pl_df = report.to_polars()           # polars (no pandas needed)
table = report.to_arrow()            # pyarrow (no pandas needed)
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

dataprof evaluates seven quality dimensions informed by ISO 8000 and ISO/IEC
25012. Each is scored from 0 to 100, and dataprof computes its overall score as
a configurable weighted average of the dimensions that were actually assessed.

### Completeness

Measures how much data is present vs. missing.

- `missing_values_ratio` -- fraction of null/empty values across all columns
- `complete_records_ratio` -- fraction of rows with zero nulls
- `null_columns` -- columns past the configured null threshold (50% by default)

The completeness score is on the same 0–100 scale as the other dimensions. It
combines cell completeness (`100 - missing_values_ratio`) and row completeness
(`complete_records_ratio`), so inspect both underlying values when setting a
quality gate. `complete_records_ratio` is deliberately strict: every column is
treated as required, so one sparse optional field can drive it to zero. Inspect
`missing_values_ratio` and `null_columns` beside it; a richer optional-column
policy is tracked in [#436](https://github.com/AndreaBozzo/dataprof/issues/436).

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
- `negative_values_in_positive` -- negative numbers in explicit `positive_columns`

### Timeliness

Measures whether confidently inferred date/time values are current and
consistent. Inferred date columns participate automatically. Pass
`temporal_columns=["created_at", "updated_at"]` in Python or call
`.temporal_columns(...)` on the Rust profiler to add columns whose dates cannot
be inferred confidently, such as mixed-format strings.

- `future_dates_count` -- dates that are in the future
- `stale_data_ratio` -- fraction of temporal data that appears outdated
- `temporal_violations` -- ordering inconsistencies in time series

### Validity

Measures whether values conform to a confidently detected semantic pattern.

- `valid_values_ratio` -- share matching the dominant pattern
- `invalid_values` -- non-null values that do not match it
- `values_checked` -- values in columns with enough pattern evidence to assess

Columns without a confident pattern are unassessed; dataprof does not invent a
domain rule from a weak match.

### Precision

Measures consistency of effective decimal scale within floating-point columns.

- `decimal_places_consistency` -- share using each column's modal decimal scale
- `inconsistent_precision_values` -- values whose effective scale differs
- `numeric_values_checked` -- parseable finite float values examined

This describes representation consistency. It does not infer how many decimal
places a business domain requires.

## Working with Different Formats

### CSV

dataprof auto-detects the delimiter (`,`, `;`, `|`, `\t`) by analyzing the first few lines:

```python
report = dp.profile("european_data.csv")  # semicolons auto-detected
report = dp.profile("pipe_export.csv")    # pipes auto-detected
```

For files with inconsistent column counts, enable flexible parsing:

```python
report = dp.profile("messy.csv", csv_flexible=True)
```

### JSON and JSONL

JSON files should contain an array of objects. JSONL files should have one JSON object per line.

```python
report = dp.profile("users.json")
report = dp.profile("events.jsonl")
```

### Parquet

Parquet is the most efficient format to profile. Schema inference and row counting read only the file metadata -- zero row scanning required.

```python
schema = dp.infer_schema("data.parquet")      # instant schema from metadata
rows = dp.quick_row_count("data.parquet")     # instant row count from metadata
report = dp.profile("data.parquet")           # full profiling reads row groups
```

## Scaling Up: Sampling and Stop Conditions

For very large files, you don't always need to read every row. dataprof provides two mechanisms to limit work.

### Sampling

Profile a representative subset instead of every row:

```python
# Python: a uniform random sample of 100,000 rows
from dataprof import SamplingStrategy
report = dp.profile("huge.csv", sampling=SamplingStrategy.reservoir(100000))
print(report.rows, report.sampling_applied, report.sampling_ratio)
```

```rust
// Rust
use dataprof::{Profiler, SamplingStrategy};
let report = Profiler::new()
    .sampling(SamplingStrategy::Reservoir { size: 100_000 })
    .analyze_file("huge.csv")?;
```

Sampling bounds the cost of **analysis**, not of reading. A uniform sample can
only be drawn once the source has been seen to the end, so the file is still
read in full and `reservoir(n)` holds `n` rows in memory. To read less, use a
[stop condition](#stop-conditions) — the two compose.

Sampling applies to CSV sources on the `auto` and `incremental` engines and to
every `dataprof.asyncio` entry point. The columnar engine and the JSON and
Parquet readers cannot sample row by row, so they raise rather than quietly
returning a full profile.

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

Build the Python extension from source with `python-async,async-streaming` before using these helpers.

```python
from dataprof.asyncio import profile_file, profile_url

# Profile a local file asynchronously
report = await profile_file("data.csv")

# Profile a remote file over HTTP
report = await profile_url("https://example.com/data.parquet")
```

### Rust

```rust
use bytes::Bytes;
use dataprof::{Profiler, AsyncDataSource, BytesSource, AsyncSourceInfo, FileFormat};

// Profile an async byte stream
let bytes = Bytes::from("name,score\nada,100\n");
let info = AsyncSourceInfo::new("upload", FileFormat::Csv)
    .size_hint(Some(bytes.len() as u64));
let source = BytesSource::new(bytes, info);
let report = Profiler::new().profile_stream(source).await?;
```

## Database Profiling

Profile data directly from PostgreSQL, MySQL, or SQLite without exporting to files.

### Python

Build the Python extension from source with `python-async,database` and the connector feature you need before using this API.

```python
report = await dp.analyze_database_async(
    "postgres://user:pass@localhost/mydb",
    "SELECT * FROM users",
    calculate_quality=True,
)
```

### Rust

```rust
use dataprof::Profiler;

async fn profile_users() -> Result<(), dataprof::DataProfilerError> {
    let report = Profiler::new()
        .connection_string("postgres://user:pass@localhost/mydb")
        .analyze_query("SELECT * FROM users")
        .await?;

    println!("Profiled {} rows", report.execution.rows_processed);
    Ok(())
}
```

See the [Database Connectors Guide](database-connectors.md) for connection strings, SSL, retry configuration, and more.

## Next Steps

- [Examples Cookbook](examples.md) -- copy-pasteable recipes for common tasks
- [Python API Guide](../python/README.md) -- full API reference
- [Database Connectors](database-connectors.md) -- advanced database setup
- [Contributing](../CONTRIBUTING.md) -- how to contribute to dataprof
