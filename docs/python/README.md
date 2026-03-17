# Python API Guide

Complete reference for the `dataprof` Python package (v0.6.0).

## Installation

```bash
uv pip install dataprof
# or
pip install dataprof
```

Requires Python 3.8+. The package ships pre-built wheels for Linux, macOS, and Windows.

## Quick Start

```python
import dataprof as dp

# Profile a file
report = dp.profile("data.csv")
print(f"{report.rows} rows, {report.columns} columns")
print(f"Quality score: {report.quality_score}")

# Profile a pandas DataFrame
import pandas as pd
df = pd.read_csv("data.csv")
report = dp.profile(df)

# Profile a PyArrow table
import pyarrow.parquet as pq
table = pq.read_table("data.parquet")
report = dp.profile(table)
```

## `profile()` -- Primary Entry Point

```python
dp.profile(
    source,                          # str, Path, DataFrame, or Arrow object
    *,
    engine="auto",                   # "auto", "incremental", "columnar"
    chunk_size=None,                 # int -- custom chunk size
    memory_limit_mb=None,            # int -- memory cap
    format=None,                     # str -- file format override
    max_rows=None,                   # int -- stop after N rows
    name=None,                       # str -- label for DataFrame/Arrow sources
    csv_delimiter=None,              # str -- override auto-detection (e.g. ";")
    csv_flexible=None,               # bool -- allow variable column counts
    sampling=None,                   # SamplingStrategy
    stop_condition=None,             # StopCondition
    on_progress=None,                # Callable[[ProgressEvent], None]
    progress_interval_ms=None,       # int -- ms between progress events
    quality_dimensions=None,         # list[str] -- subset of dimensions to compute
) -> ProfileReport
```

**Source types:**

| Type | Description |
|---|---|
| `str` or `Path` | File path (CSV, JSON, JSONL, Parquet) |
| pandas `DataFrame` | In-memory DataFrame |
| polars `DataFrame` | In-memory Polars DataFrame |
| PyArrow `Table` or `RecordBatch` | Zero-copy via PyCapsule interface |

**Engine options:**

| Engine | When to use |
|---|---|
| `"auto"` | Let dataprof choose based on file size and format (recommended) |
| `"incremental"` | True streaming with bounded memory -- large files, streams |
| `"columnar"` | Arrow-based batch processing -- Parquet, in-memory data |

## `ProfileReport`

Returned by `profile()` and all analysis functions.

**Properties:**

| Property | Type | Description |
|---|---|---|
| `source` | `str` | Source identifier (file path, table name, etc.) |
| `source_type` | `str` | `"file"`, `"query"`, `"dataframe"`, `"stream"` |
| `rows` | `int` | Number of rows processed |
| `columns` | `int` | Number of columns detected |
| `column_profiles` | `list[ColumnProfile]` | Per-column statistics |
| `quality_score` | `float \| None` | Overall quality score (0--100) |
| `quality` | `DataQualityMetrics \| None` | Detailed quality breakdown |
| `execution_time_ms` | `int` | Total processing time |
| `throughput` | `float \| None` | Rows per second |
| `memory_peak_mb` | `float \| None` | Peak memory usage |
| `truncation_reason` | `str \| None` | Why processing stopped early |
| `source_exhausted` | `bool` | Whether the entire source was read |
| `sampling_applied` | `bool` | Whether sampling was used |
| `sampling_ratio` | `float \| None` | Fraction of data sampled |

**Export methods:**

```python
report.to_dict()                  # dict
report.to_json(indent=2)         # JSON string
report.to_dataframe()            # pandas DataFrame (requires pandas)
report.save("report.json")       # write JSON to disk
```

## `ColumnProfile`

Per-column profiling statistics.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Column name |
| `data_type` | `str` | Inferred type: `"String"`, `"Integer"`, `"Float"`, `"Date"` |
| `total_count` | `int` | Total number of values |
| `null_count` | `int` | Number of null/missing values |
| `unique_count` | `int \| None` | Distinct value count |
| `null_percentage` | `float` | Null ratio (0.0--100.0) |
| `uniqueness_ratio` | `float` | Unique values / total values |
| `min` | `float \| None` | Minimum (numeric columns) |
| `max` | `float \| None` | Maximum (numeric columns) |
| `mean` | `float \| None` | Mean (numeric columns) |
| `std_dev` | `float \| None` | Standard deviation |
| `variance` | `float \| None` | Variance |
| `median` | `float \| None` | Median |
| `mode` | `float \| None` | Mode |
| `skewness` | `float \| None` | Skewness |
| `kurtosis` | `float \| None` | Kurtosis |
| `coefficient_of_variation` | `float \| None` | CV |
| `quartiles` | `dict \| None` | `{"q1", "q2", "q3", "iqr"}` |
| `is_approximate` | `bool \| None` | Whether stats were estimated from a sample |

## Configuration

### `ProfilerConfig`

Reusable configuration object:

```python
config = dp.ProfilerConfig(
    engine="incremental",
    chunk_size=5000,
    memory_limit_mb=512,
    max_rows=100000,
    csv_delimiter=";",
    quality_dimensions=["completeness", "uniqueness"],
)
```

### `SamplingStrategy`

Controls how data is sampled during profiling:

```python
from dataprof import SamplingStrategy

SamplingStrategy.none()                              # process everything
SamplingStrategy.random(size=10000)                  # random sample
SamplingStrategy.reservoir(size=10000)               # reservoir sampling
SamplingStrategy.systematic(interval=10)             # every Nth row
SamplingStrategy.stratified(["region"], 1000)         # stratified by column
SamplingStrategy.progressive(5000, 0.95, 100000)     # adaptive progressive
SamplingStrategy.importance(weight_threshold=0.5)    # importance-weighted
SamplingStrategy.multi_stage([s1, s2])               # chained strategies
SamplingStrategy.adaptive(total_rows=1000000, file_size_mb=500.0)
```

### `StopCondition`

Composable early-termination conditions. Combine with `|` (any) or `&` (all):

```python
from dataprof import StopCondition

# Stop after 10k rows or 50 MB
stop = StopCondition.max_rows(10000) | StopCondition.max_bytes(50_000_000)

# Stop when schema stabilizes AND confidence exceeds 95%
stop = StopCondition.schema_stable(500) & StopCondition.confidence_threshold(0.95)

# Built-in presets
StopCondition.schema_inference()   # fast schema-only mode
StopCondition.quality_sample()     # enough rows for quality assessment
StopCondition.never()              # process everything (default)

report = dp.profile("huge.csv", stop_condition=stop)
```

### `ProgressEvent`

Track progress with a callback:

```python
def on_progress(event):
    if event.percentage is not None:
        print(f"{event.percentage:.1f}% ({event.rows_processed} rows)")

report = dp.profile("data.csv", on_progress=on_progress)
```

Event fields: `kind`, `rows_processed`, `bytes_consumed`, `elapsed_ms`, `processing_speed`, `percentage`, `column_names`, `total_rows`, `total_bytes`, `truncated`, `message`, `estimated_total_rows`, `estimated_total_bytes`.

## `DataQualityMetrics`

ISO 8000/25012 quality metrics, accessible via `report.quality`:

```python
q = report.quality

# Overall score
print(q.overall_quality_score())

# Flat accessors (return defaults when dimension not computed)
print(q.missing_values_ratio)
print(q.data_type_consistency)
print(q.duplicate_rows)
print(q.outlier_ratio)
print(q.future_dates_count)

# Nested dimension accessors (None when not computed)
print(q.completeness)    # {"missing_values_ratio": ..., "complete_records_ratio": ..., "null_columns": [...]}
print(q.consistency)     # {"data_type_consistency": ..., "format_violations": ..., "encoding_issues": ...}
print(q.uniqueness)      # {"duplicate_rows": ..., "key_uniqueness": ..., "high_cardinality_warning": ...}
print(q.accuracy)        # {"outlier_ratio": ..., "range_violations": ..., "negative_values_in_positive": ...}
print(q.timeliness)      # {"future_dates_count": ..., "stale_data_ratio": ..., "temporal_violations": ...}
```

**Selective dimensions** -- compute only what you need:

```python
report = dp.profile("data.csv", quality_dimensions=["completeness", "uniqueness"])
# report.quality.consistency will be None
# report.quality.completeness will have values
```

## Partial Analysis

Fast operations that don't require a full profile:

### `infer_schema()`

```python
result = dp.infer_schema("data.csv")
print(f"{result.num_columns} columns, {result.rows_sampled} rows sampled")
for col in result.columns:
    print(f"  {col['name']}: {col['data_type']}")
```

### `quick_row_count()`

```python
result = dp.quick_row_count("data.parquet")
print(f"{result.count} rows ({'exact' if result.exact else 'estimated'})")
print(f"Method: {result.method}, took {result.count_time_ms}ms")
```

## Async API

The `dataprof.asyncio` module provides async variants for use in web frameworks, stream processors, and other async contexts.

```python
from dataprof.asyncio import profile_file, profile_bytes, profile_url

# Async file profiling
report = await profile_file("data.csv", max_rows=10000)

# Profile raw bytes (e.g. from an HTTP request body)
report = await profile_bytes(csv_bytes, format="csv")

# Profile a remote file over HTTP
report = await profile_url("https://example.com/data.parquet")
```

Additional async utilities:

```python
from dataprof.asyncio import infer_schema_stream, quick_row_count_stream

schema = await infer_schema_stream(csv_bytes, format="csv")
count = await quick_row_count_stream(csv_bytes, format="csv")
```

## Database Profiling

Async database functions for PostgreSQL, MySQL, and SQLite:

```python
import asyncio
import dataprof as dp

async def main():
    # Test connection
    ok = await dp.test_connection_async("postgres://user:pass@localhost/mydb")

    # Profile a query
    report = await dp.analyze_database_async(
        "postgres://user:pass@localhost/mydb",
        "SELECT * FROM users",
        batch_size=10000,
        calculate_quality=True,
    )
    print(f"{report.rows} rows, quality: {report.quality_score}")

    # Get table schema
    columns = await dp.get_table_schema_async(
        "postgres://user:pass@localhost/mydb", "users"
    )

    # Count rows
    count = await dp.count_table_rows_async(
        "postgres://user:pass@localhost/mydb", "users"
    )

asyncio.run(main())
```

## Arrow Interop

The `RecordBatch` class supports zero-copy exchange via the [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html):

```python
import dataprof as dp
import pyarrow as pa

# Profile a PyArrow table directly
report = dp.profile(table)

# RecordBatch properties
batch.num_rows
batch.num_columns
batch.column_names

# Convert to other formats
df = batch.to_pandas()
pl_df = batch.to_polars()

# PyCapsule protocol for zero-copy exchange
schema_capsule = batch.__arrow_c_schema__()
array_capsule = batch.__arrow_c_array__()
```
