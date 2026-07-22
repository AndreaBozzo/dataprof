# Python API Guide

Complete reference for the `dataprof` Python package (v0.9.0).

The Python API is built for quick inspection and follow-up analysis: point it at a file, DataFrame, Arrow batch, ad-hoc notebook data, or database query and get back a report you can slice, export, and wire into notebooks or checks.

## Installation

```bash
uv pip install dataprof
# or
pip install dataprof
```

Requires Python 3.10+. The package ships pre-built wheels for Linux, macOS, and Windows, and declares **no Python dependencies**. The base API needs nothing else: local file profiling, DataFrame and Arrow inputs, ad-hoc dict/bytes inputs, and report exports. Install the `pandas` extra only for pandas-typed exports (`to_dataframe()`, `describe()` as a DataFrame) and for Parquet byte buffers.

Async URL profiling and database helpers are not part of the default wheel contract for this release. Use a source build when you need those optional features:

```bash
uv run maturin develop --features "python,python-async,async-streaming"

# Add parquet-async for remote Parquet support
uv run maturin develop --features "python,python-async,async-streaming,parquet-async"

# Add database plus a connector when needed
uv run maturin develop --features "python,python-async,database,sqlite"
```

Inspect the current installation without importing optional packages or trying
a network/database operation:

```python
import dataprof as dp

features = dp.capabilities()
print(features)

if features.database and "sqlite" in features.database_connectors:
    # Database helpers are available in this build.
    ...

if features.pandas_interop and features.pandas_installed:
    # Both compiled interoperability and the optional Python package are present.
    ...
```

## Quick Start

```python
import dataprof as dp

# Profile a file
report = dp.profile("data.csv")
print(f"{report.rows} rows, {report.columns} columns")
print(f"Quality score: {report.quality_score}")

# Access columns directly
col = report["age"]
print(f"mean={col.mean}, nulls={col.null_percentage}%")

# Profile a pandas DataFrame
import pandas as pd
df = pd.read_csv("data.csv")
report = dp.profile(df)

# Profile ad-hoc notebook data
report = dp.profile({"age": [31, 42, 29], "city": ["Rome", "Milan", "Rome"]})
report = dp.profile([{"age": 31, "city": "Rome"}, {"age": 42, "city": "Milan"}])
report = dp.profile(b"age,city\n31,Rome\n", format="csv")

# Profile a PyArrow table
import pyarrow.parquet as pq
table = pq.read_table("data.parquet")
report = dp.profile(table)
```

## `profile()` -- Primary Entry Point

```python
dp.profile(
    source,                          # str, Path, DataFrame, Arrow, dict, rows, or bytes
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
    metrics=None,                    # list[str] -- "schema", "statistics", "patterns", "quality"
    quality_dimensions=None,         # list[str] -- subset of dimensions to compute
    locale=None,                     # str -- pattern locale hint, e.g. "IT"
    positive_columns=None,           # list[str] -- columns expected to be non-negative
    identifier_columns=None,         # list[str] -- semantic IDs, not measures
    temporal_columns=None,           # list[str] -- columns assessed for timeliness
) -> ProfileReport
```

**Source types:**

| Type | Description |
|---|---|
| `str` or `Path` | File path (CSV, JSON, JSONL, Parquet) |
| pandas `DataFrame` | In-memory DataFrame |
| polars `DataFrame` | In-memory Polars DataFrame |
| PyArrow `Table` or `RecordBatch` | Zero-copy via PyCapsule interface |
| `dict[str, list]` | Columns of cells; profiled natively, no dependencies |
| `list[dict]` | Row-oriented notebook data; rows may omit keys, which read as nulls |
| `bytes` or `io.BytesIO` | In-memory file contents; requires `format="csv"`, `"json"`, `"jsonl"`, or `"parquet"` |

Dict, row-dict, and byte inputs are profiled by the Rust core directly, so they
need no third-party package. The one exception is `format="parquet"` for byte
buffers, which needs a columnar reader and therefore the `pandas` extra.

A cell is missing when it is `None`, NaN, or a null-like token (`""`, `"null"`,
`"nan"`) -- the same rule the CSV and Arrow paths use. Note that a `dict` is
*not* round-tripped through pandas, so an integer column containing a null stays
`integer` rather than being widened to `float`.

For async byte streams, use `dataprof.asyncio.profile_bytes()`.

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
| `engine` | `str \| None` | Engine or parser that produced the report |
| `rows` | `int` | Number of rows processed |
| `columns` | `int` | Number of columns detected |
| `column_profiles` | `dict[str, ColumnProfile]` | Per-column statistics (by name) |
| `quality_score` | `float \| None` | Overall quality score (0--100) |
| `quality` | `DataQualityMetrics \| None` | Detailed quality breakdown |
| `execution_time_ms` | `int` | Total processing time |
| `throughput` | `float \| None` | Rows per second |
| `memory_peak_mb` | `float \| None` | Peak memory usage |
| `truncation_reason` | `str \| None` | Why processing stopped early |
| `source_exhausted` | `bool` | Whether the entire source was read |
| `ragged_row_count` | `int` | Rows whose field count differed from the header (`0` = clean parse) |
| `sampling_applied` | `bool` | Whether sampling was used |
| `sampling_ratio` | `float \| None` | Fraction of data sampled |

**Dict-like column access:**

```python
col = report["column_name"]          # -> ColumnProfile
"column_name" in report              # -> True/False
for name in report: print(name)      # iterate column names
len(report)                          # number of columns
```

**Export methods:**

```python
report.to_dict()                  # nested dict (rounded values)
report.to_json(indent=2)         # JSON string
report.to_dataframe()            # pandas DataFrame -- all stats (requires pandas)
report.to_polars()               # polars DataFrame -- all stats (requires polars)
report.to_arrow()                # PyArrow Table -- all stats (requires pyarrow)
report.describe()                # transposed summary like pandas describe()
report.quality_summary()         # single-row dict for quality tracking
report.to_html()                 # standalone HTML (same as the notebook display)
report.to_markdown()             # GitHub-flavored markdown table
report.compare(other)            # dict of quality/schema/null deltas vs another report
report.save("report.json")      # save to JSON
report.save("report.csv")       # save column profiles to CSV
report.save("report.parquet")   # save column profiles to Parquet (requires pyarrow)

# Round-trip a saved report without re-profiling (read-only view)
reloaded = dp.ProfileReport.load("report.json")   # from a saved .json file
reloaded = dp.ProfileReport.from_json(report.to_json())  # from a JSON string
reloaded = dp.ProfileReport.from_dict(report.to_dict())  # from a dict
```

**Rounding:** All floating-point values in exported data are rounded to match the CLI
JSON output -- 2 decimal places for percentages and ratios, 4 decimal places for
statistical metrics. Raw property access on `ColumnProfile` returns unrounded Rust
values; use the export methods for clean output.

## `ColumnProfile`

Per-column profiling statistics.

| Field | Type | Description |
|---|---|---|
| `name` | `str` | Column name |
| `data_type` | `str` | Inferred type: `"string"`, `"identifier"`, `"integer"`, `"float"`, `"date"`, `"boolean"` |
| `total_count` | `int` | Total number of values |
| `null_count` | `int` | Number of null/missing values |
| `unique_count` | `int \| None` | Distinct value count |
| `invalid_count` | `int \| None` | Non-null values that did not parse as a finite number (parse failures and non-finite tokens like `inf`/`NaN`) and are excluded from the statistics. `None` = check did not run (non-numeric column, or statistics skipped); `0` = every non-null value parsed |
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
| `min_length` | `int \| None` | Minimum character length |
| `max_length` | `int \| None` | Maximum character length |
| `avg_length` | `float \| None` | Average character length |
| `true_count` | `int \| None` | Number of `True` values (boolean columns) |
| `false_count` | `int \| None` | Number of `False` values (boolean columns) |
| `true_ratio` | `float \| None` | Ratio of `True` values (0.0--1.0) |
| `patterns` | `list[Pattern] \| None` | List of detected value patterns |

## `Pattern`

Represents a statistical pattern match (regex) found in a column.

| Field | Type | Description |
| --- | --- | --- |
| `name` | `str` | Name of the pattern (e.g. `"email"`, `"url"`) |
| `regex` | `str` | Regular expression used for matching |
| `match_count` | `int` | Number of rows matching the pattern |
| `match_percentage` | `float` | Percentage of non-null rows matching (0.0--100.0) |

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
    positive_columns=["pressure"],
    identifier_columns=["order_id", "customer_id"],
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

Quality metrics informed by ISO 8000 and ISO/IEC 25012, accessible via
`report.quality`. The aggregate score is dataprof's formula, not an ISO-defined
or certified score:

```python
q = report.quality

# Overall score
print(q.overall_quality_score())
print(q.score_weights)  # relative weights used by dataprof's aggregate formula

# Nested dimension accessors (None when not computed)
print(q.completeness)    # {"missing_values_ratio": ..., "complete_records_ratio": ..., "null_columns": [...]}
print(q.consistency)     # {"data_type_consistency": ..., "format_violations": ..., "encoding_issues": ...}
print(q.uniqueness)      # {"duplicate_rows": ..., "key_uniqueness": ..., "high_cardinality_warning": ...}
print(q.accuracy)        # {"outlier_ratio": ..., "range_violations": ..., "negative_values_in_positive": ...}
print(q.timeliness)      # {"future_dates_count": ..., "stale_data_ratio": ..., "temporal_violations": ...}
print(q.validity)        # {"valid_values_ratio": ..., "invalid_values": ..., "values_checked": ...}
print(q.precision)       # {"decimal_places_consistency": ..., "inconsistent_precision_values": ..., "numeric_values_checked": ...}
```

Flat `DataQualityMetrics` accessors are deprecated in 0.9. Use nested
dimensions so skipped dimensions are explicit:

```python
# Old
q.missing_values_ratio

# New
if q.completeness is not None:
    q.completeness["missing_values_ratio"]
```

`negative_values_in_positive` is driven by explicit `positive_columns`; dataprof
does not infer positive-only domains from column names. `identifier_columns`
marks numeric-looking IDs as semantic strings so numeric stats and outlier
metrics do not treat them as measures.
Timeliness scoring is opt-in through `temporal_columns`; inferred date columns
remain visible in column profiles but do not affect the quality score unless
their names are explicitly selected.

Hints are validated, never silently dropped. A hint that names a missing column
raises `ValueError` listing the unmatched names and the available columns; a
`positive_columns` hint on a column with no numeric values, or a
`temporal_columns` hint on a column with no dates, is likewise rejected.
`report.semantic_hint_bindings` records how each hint bound — `column`, `kind`,
`checked_values`, `matched_values`, and `exact` (whether the counts covered
every row or a sample):

```python
report = dp.profile("readings.csv", positive_columns=["pressure"])
report.semantic_hint_bindings
# [{"column": "pressure", "kind": "positive",
#   "checked_values": 1000, "matched_values": 1000, "exact": True}]
```

**Selective dimensions** -- compute only what you need:

```python
report = dp.profile("data.csv", quality_dimensions=["completeness", "uniqueness"])
# report.quality.consistency will be None
# report.quality.completeness will have values
```

## Export Methods

### `to_dataframe()` / `to_polars()` / `to_arrow()`

All three return an enriched table of column profiles with rounded values:

```python
# pandas DataFrame
df = report.to_dataframe()

# polars DataFrame (no pandas dependency needed)
pl_df = report.to_polars()

# PyArrow Table (no pandas dependency needed)
table = report.to_arrow()
```

Columns included: `name`, `data_type`, `total_count`, `null_count`, `null_percentage`,
`unique_count`, `uniqueness_ratio`, `min`, `max`, `mean`, `std_dev`, `variance`,
`median`, `mode`, `skewness`, `kurtosis`, `coefficient_of_variation`, `q1`, `q2`,
`q3`, `iqr`, `is_approximate`, `min_length`, `max_length`, `avg_length`,
`top_pattern`, `top_pattern_pct`.

### `describe()`

Transposed summary similar to `pandas.DataFrame.describe()`:

```python
desc = report.describe()
#          col_a   col_b   col_c
# count    1000    1000    1000
# null%    0.0     2.1     0.0
# unique   45      800     3
# mean     34.5    None    None
# std      12.1    None    None
# min      1.0     None    None
# 25%      25.0    None    None
# 50%      33.0    None    None
# 75%      44.0    None    None
# max      99.0    None    None
```

Returns a pandas DataFrame if available, otherwise a dict-of-dicts.

### `quality_summary()`

Single-row dict for easy aggregation across multiple reports:

```python
qs = report.quality_summary()
# {"source": "data.csv", "rows": 1000, "quality_score": 92.3,
#  "completeness": 98.0, "consistency": 95.0, ...}

# Track quality over time
import pandas as pd
rows = [dp.profile(f).quality_summary() for f in files]
history = pd.DataFrame(rows)
```

### `to_html()` / `to_markdown()`

Render the report for sharing outside a notebook:

```python
html = report.to_html()          # same rich table Jupyter shows, as a string
open("report.html", "w").write(html)

md = report.to_markdown()        # GitHub-flavored markdown table
# Paste straight into a PR comment, issue, or Slack message
```

### `load()` / `from_json()` / `from_dict()`

Rebuild a report from previously exported data without re-profiling. The
reconstructed report is a **read-only view** backed by the exported values, but
all export methods (`to_json`, `to_markdown`, `to_dataframe`, `describe`,
`quality_summary`, mapping access, …) work as usual.

`load(path)` is the path-based entry point — the natural counterpart to
`save()`. Only `.json` files carry a full report; `.csv` / `.parquet` store
column profiles only and cannot round-trip:

```python
report.save("report.json")
# ...later...
reloaded = dp.ProfileReport.load("report.json")
reloaded.quality_score          # == the original report's quality_score
reloaded["email"].null_percentage
```

`from_json(text)` and `from_dict(data)` take an in-memory JSON string or dict
instead of a file path:

```python
reloaded = dp.ProfileReport.from_json(report.to_json())
reloaded = dp.ProfileReport.from_dict(report.to_dict())
```

#### Report schema versioning

Saved reports are durable artifacts — CI baselines, drift references, agent
inputs — so the document carries its own schema version, independent of the
package version:

```python
report.to_dict()["schema_version"]   # == dp.REPORT_SCHEMA_VERSION
```

The compatibility policy when loading:

| Document | Behavior |
|---|---|
| No `schema_version` field | Legacy pre-0.10 report; loads through a compatibility path |
| `schema_version` ≤ `dp.REPORT_SCHEMA_VERSION` | Loads normally; unknown additive fields from newer writers are ignored |
| `schema_version` > `dp.REPORT_SCHEMA_VERSION` | Raises `ValueError` immediately — an incompatible report is never partially decoded |

The version only increments when the document format itself changes
incompatibly, not on every dataprof release. The same field with the same
semantics appears in reports serialized from Rust (`serde`), where readers
enforce the identical policy.

When quality metrics are present, the `quality` block always carries a
`low_sample_warning` boolean (`true` when the profiled sample was below the
recommended minimum of 10 rows, `false` otherwise). It round-trips through
`to_dict()`/`from_dict()`; treat `quality_score` and the per-dimension ratios
as directional rather than reliable whenever it is `true`.

### `compare()`

Detect quality drift or schema changes between two profiles (e.g. the same
dataset before and after a pipeline run):

```python
before = dp.profile("data_v1.csv")
after = dp.profile("data_v2.csv")
delta = before.compare(after)
# {
#   "quality_score": {"a": 92.3, "b": 88.1, "abs": -4.2, "rel_pct": -4.55},
#   "dimensions": {"completeness": {...}, "consistency": {...}, ...},
#   "columns": {"email": {"null_pct_a": 1.0, "null_pct_b": 6.5, "null_pct_delta": 5.5}, ...},
#   "schema": {"added": ["phone"], "removed": [], "common": ["id", "email", ...]},
# }
```

> The `compare()` result shape is provisional and will align with the Rust-side
> `QualityDelta` type once it lands.

### `save()`

```python
report.save("report.json")      # full report as JSON
report.save("profiles.csv")     # column profiles as CSV (no extra deps)
report.save("profiles.parquet") # column profiles as Parquet (requires pyarrow)
report.save("report.html")      # standalone HTML render (same as to_html())
report.save("report.md")        # markdown table (same as to_markdown())
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

## Optional Async API (Source Build)

The `dataprof.asyncio` module provides async variants for use in web frameworks, stream processors, and other async contexts. These helpers require a source build with `python-async` and `async-streaming` enabled.

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

## Optional Database Profiling (Source Build)

Async database functions for PostgreSQL, MySQL, and SQLite require a source build with `python-async`, `database`, and the relevant connector features enabled:

```bash
uv run maturin develop --features "python,python-async,database,sqlite"
```

Then the following APIs become available:

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
