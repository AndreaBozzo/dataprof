# Python API Guide

Complete reference for the `dataprof` Python package (v0.11.0).

For upgrade-sensitive changes to sampling, execution controls, quality scores,
parser behavior, semantic hints, and exception types, read the
[0.11.0 release notes and migration guide](../release-notes.md).

The Python API is built for quick inspection and follow-up analysis: point it at a file, DataFrame, Arrow batch, ad-hoc notebook data, or database query and get back a report you can slice, export, and wire into notebooks or checks.

## Installation

```bash
uv pip install dataprof
# or
pip install dataprof
```

Supports standard, GIL-enabled CPython 3.10–3.14. PyPy, preview Python and
free-threaded builds are outside the supported wheel set; see the
[interpreter policy](../CONTRIBUTING.md#python-interpreters-and-release-wheels).
The package ships pre-built wheels for Linux, macOS, and Windows, and declares **no Python dependencies**. The base API needs nothing else: local file profiling, DataFrame and Arrow inputs, ad-hoc dict/bytes inputs, and report exports. Install the `pandas` extra only for pandas-typed exports (`to_dataframe()`, `describe()` as a DataFrame).

The wheel also carries the async API: `dataprof.asyncio`, HTTP URL profiling,
and remote Parquet all work on a bare `pip install dataprof`.

Database profiling is the one documented feature the wheel does not contain.
Connectors are a compile-time feature rather than a Python dependency, so no
extra can install them; they need a build from source, either with pip from the
published sdist or from a checkout:

```bash
pip install dataprof --no-binary dataprof   --config-settings="build-args=--features python,python-async,async-streaming,parquet-async,database,sqlite"
```

The feature list has to be complete, not just the extra parts. See
[Database Profiling](#database-profiling-source-build-only) for why, and for
the checkout equivalent.

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
    chunk_size=None,                 # int -- bytes per streaming chunk
    memory_limit_mb=None,            # int -- memory cap
    format=None,                     # str -- file format override
    max_rows=None,                   # int -- row cap; Parquet samples across the file
    name=None,                       # str -- label for DataFrame/Arrow sources
    csv_delimiter=None,              # str -- override auto-detection (e.g. ";")
    csv_flexible=None,               # bool -- allow variable column counts
    sampling=None,                   # SamplingStrategy
    stop_condition=None,             # StopCondition
    on_progress=None,                # Callable[[ProgressEvent], None]
    progress_interval_ms=None,       # int -- ms between progress events
    metrics=None,                    # list[str] -- "schema", "statistics", "patterns", "quality"
    quality_dimensions=None,         # list[str] -- subset of dimensions to compute
    columns=None,                    # list[str] -- profile only these columns
    locale=None,                     # str -- "CA"|"DE"|"FR"|"GB"|"IT"|"US"
    positive_columns=None,           # list[str] -- columns expected to be non-negative
    identifier_columns=None,         # list[str] -- semantic IDs, not measures
    temporal_columns=None,           # list[str] -- columns assessed for timeliness
) -> ProfileReport
```

For file profiling, `engine` accepts the case-insensitive names `auto`,
`incremental` (compatibility alias: `streaming`), and `columnar` (compatibility
alias: `arrow`). This applies to `dp.profile(..., engine=...)`,
`dp.profile_file(..., engine=...)`, and `dp.Profiler().engine(...).profile(...)`.
CSV reports use the selected canonical name, `incremental` or `columnar`, in
`report.engine` and `report.to_dict()["execution"]["engine"]`; aliases are never
emitted, and `auto` reports the engine it selected.

**Source types:**

| Type | Description |
|---|---|
| `str` or `Path` | File path (CSV, JSON, JSONL, Parquet) |
| pandas `DataFrame` | In-memory DataFrame |
| polars `DataFrame` | In-memory Polars DataFrame |
| PyArrow `Table` or `RecordBatch` | Zero-copy via PyCapsule interface |
| Arrow `RecordBatchReader` / `__arrow_c_stream__` producer | One-shot, incremental batch profiling; includes DuckDB relations |
| `dict[str, list]` | Columns of cells; profiled natively, no dependencies |
| `list[dict]` | Row-oriented notebook data; rows may omit keys, which read as nulls |
| `bytes` or `io.BytesIO` | In-memory file contents; requires `format="csv"`, `"json"`, `"jsonl"`, or `"parquet"` |

Dict, row-dict, and byte inputs are profiled by the Rust core directly, so they
need no third-party package. That includes `format="parquet"`: byte buffers go
through the same compiled Arrow reader as Parquet files, so the two report the
same types, column order, and statistics, and `capabilities().local_parquet`
predicts both.

A cell is missing when it is `None`, NaN, or a null-like token (`""`, `"null"`,
`"nan"`) -- the same rule the CSV and Arrow paths use. Note that a `dict` is
*not* round-tripped through pandas, so an integer column containing a null stays
`integer` rather than being widened to `float`.

**Column order** follows the source on every input and transport: the CSV
header, the Parquet/Arrow schema, the dict or DataFrame key order, and for
JSON/JSONL the field order of the first record, with fields that only appear in
later records appended where they were first seen. Converting a dataset between
formats therefore does not reshuffle the report.

**Column projection** narrows a profile to the columns you name. `columns`
takes a list of source column names and is accepted by `dp.profile()`,
`dp.profile_file()`, `dp.Profiler().columns([...])`, and every
`dataprof.asyncio` entry point — `profile_file()`, `profile_bytes()` and
`profile_url()` forward it through `ProfilerConfig`, which also carries it into
`analyze_database_async()`:

```python
report = dp.profile("orders.csv", columns=["amount_eur", "city"])
list(report)        # ['city', 'amount_eur']
report.rows         # every row is still read
```

Projection selects columns, never rows: `report.rows` is unchanged, and each
retained column reports exactly the values it reports in an unprojected profile
of the same source — same types, statistics, patterns, and lengths. It applies
identically on every input and transport (CSV, JSON, JSONL, Parquet, bytes,
dicts, row dicts, DataFrames, and Arrow) and on both engines.

The projected report keeps **source order**, not the order you listed the names
in, so a projection is a filter on the report rather than a reordering of it.

Names are validated rather than silently ignored. A name absent from the source
raises `ValueError` listing the unmatched names, and so does a repeated name.
`columns=[]` is a projection onto no columns, which is distinct from
`columns=None` (no projection): it reports `columns == 0` while `rows` still
counts the rows that were read.

Quality is assessed over the projected columns only, so `quality_score`
generally differs from the score for the whole dataset. Two dimensions are
**withheld** rather than narrowed: `completeness` and `uniqueness` both report
row-level measurements (`complete_records_ratio`, `total_cells`,
`duplicate_rows`, `rows_checked`) that mean something different once a row has
been projected, and the report schema cannot mark only those fields as
projected. Rather than publish plausible numbers under full-row names, a
projected report leaves both dimensions `None` and out of
`assessed_dimensions()`:

```python
dp.profile("orders.csv").quality.completeness
# {'complete_records_ratio': 73.8, 'total_cells': 5000, ...}

dp.profile("orders.csv", columns=["amount_eur", "city"]).quality.completeness
# None
```

The overall score is therefore an average over fewer dimensions, and is often
*higher* than the unprojected score even for the same columns — dropping
`completeness` drops the dimension that penalizes nulls. Compare projected
scores against other projected scores, not against whole-dataset ones.

If a projection leaves no assessable dimension — `quality_dimensions` selecting
only `completeness` and/or `uniqueness` — the report carries no quality object
at all: `report.quality` is `None` and so is `quality_score`, which is the
"not analyzed" answer rather than a zero.

Semantic hints are resolved against the projection too — a `positive_columns`
hint naming a column that projection excluded raises, since that column is not
in the report it would describe.

Synchronous byte inputs use the in-memory columnar path. They support
`max_rows`, metric/quality selection, semantic hints, CSV delimiters, and JSONL
error policy, but reject streaming-only controls (`chunk_size`,
`memory_limit_mb`, `stop_condition`, progress callbacks, and flexible CSV
recovery) instead of silently ignoring them. For those controls, use
`dataprof.asyncio.profile_bytes()`. JSON and JSONL byte buffers follow RFC 8259:
the non-standard `NaN` and `Infinity` constants are malformed input.

**JSON record policy.** Only JSON objects are profileable records — they are the
only JSON value with named fields to become columns. A record that is valid JSON
but not an object (a scalar, an array, `null`) is never silently dropped: with
`jsonl_on_error="skip"` (the default) it is counted in `error_count` and the
records after it still profile, and with `jsonl_on_error="strict"` the first one
raises `ValueError` naming its position and JSON kind. Input whose every record
is non-object fails, exactly as all-malformed input does. File, byte, and async
byte transports apply this identically.

**Records with no fields.** A JSON object with no fields (`{}`) is a record: it
was read and analysed, and nothing was found in it. It profiles as a row against
zero columns, so `[{}, {}]` reports `rows == 2, columns == 0` on every transport
— file, bytes, async bytes, URL, and a Python list of dicts. That keeps three
shapes distinct:

| Shape | Meaning | Example |
|---|---|---|
| `rows > 0`, `columns == 0` | rows were read; none of them had fields | `[{}, {}]` |
| `rows == 0`, `columns == 0` | the input held no records | `[]` |
| `rows == 0`, `columns > 0` | a known schema with no rows under it | a CSV header line |

A zero-column report is a normal report: it serialises, round-trips, and
renders. `len(report)` counts columns, so it is `0` while `report.rows` is not.
A record with no fields is well-formed, so it is clean under both error
policies and never counted in `error_count`.

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
| `source_type` | `str` | `"file"`, `"bytes"`, `"query"`, `"dataframe"`, `"stream"` |
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
| `ragged_row_count` | `int` | Rows whose field count differed from the header (`0` = clean parse). Reported for file, async and columnar CSV inputs |
| `sampling_applied` | `bool` | Whether sampling was used |
| `sampling_ratio` | `float \| None` | Fraction of data sampled |
| `sampled_row_ranges` | `list[list[int]] \| None` | Exact zero-based, half-open source row intervals when recorded; `[]` means zero selected rows |

For Parquet files, byte buffers and HTTP URLs, `max_rows=N` selects up to 32
contiguous ranges spread across the file, with exactly N rows when the file
is larger than the cap. The first and last rows are included for N ≥ 2;
N = 1 selects the middle row and N = 0 selects none. Selection is deterministic
and independent of row-group sizes, batch sizes and transport. The exact ranges
appear in `sampled_row_ranges` and `to_dict()["execution"]["sampled_row_ranges"]`.
`sampling_applied` is true, `sampling_ratio` is selected rows / file rows, and
`source_exhausted` is false with the usual `max_rows(N)` truncation reason.
A cap at or above the file row count reads the whole file without sampling.

Statistics and row-level quality metrics describe the selected population.
Duplicate detection includes matches across separate ranges. This deterministic
sample does not establish full-source uniqueness or provide an unbiased
estimate of full-source ratios. Before 0.12, local and byte-buffer Parquet caps
selected a prefix, while HTTP reads ignored the cap. Older reports have no
recorded ranges and load with `sampled_row_ranges=None`. Re-baseline capped
Parquet comparisons when upgrading.

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
report.to_html()                 # embeddable HTML (same as the notebook display)
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

### Rounding

All floating-point values in exported data are rounded. The precision is chosen
by what the number *is*, not by which object it lives on:

| Kind | Precision | Examples |
| --- | --- | --- |
| `0..100` percentage | 2dp | `null_percentage`, `coefficient_of_variation`, every float in the quality dimension dicts |
| statistic | 4dp | `mean`, `std_dev`, `variance`, `skewness`, `kurtosis`, `avg_length` |
| data value | 4dp | `min`, `max`, `median`, `mode` |
| `0..1` ratio | 4dp | `uniqueness_ratio`, `true_ratio` |

A `0..1` ratio takes 4dp so it carries the same resolution as the equivalent
percentage at 2dp. Data values take 4dp because a profiler must not report a
`min` the column never contained. Quartiles are the deliberate exception: they
stay at 2dp, being distribution landmarks rather than exact values.

**Ties** round the stored float, away from zero. This is the value actually
held, not the shortest decimal string that prints for it -- `23 / 4000 * 100`
prints as `0.575` but is stored just below it, and so rounds to `0.57`.

The Rust and Python layers implement the same convention and are held to shared
fixtures (`tests/fixtures/rounding_parity.json` and
`report_rounding_parity.json`), so the same data profiled through either gives
the same numbers. Raw property access on `ColumnProfile` returns unrounded Rust
values; use the export methods for rounded output. Cross-engine numeric
equality is defined on those serialized metrics, with no extra tolerance.
Native attributes may differ in their final digits with accumulation order;
loading a report retains the precision that was saved. Compare the same data,
schema semantics, analysis options and analyzed population, and exclude source
and execution provenance such as timing and memory from equality checks. See
the [numeric contract](../schema/README.md#numeric-equality-contract) for scope,
sampling and absence semantics.

**Round-trip fidelity:** a report reloaded with `from_dict`, `from_json`, or
`load` reports the same values as the report it was saved from, at the precision
above. Reloaded reports are read-only proxies rather than native reports, so the
two are different objects with the same answers.

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
| `type_homogeneity` | `dict[str, int] \| None` | Non-null values counted by lexical class: `{"numeric", "date", "boolean", "text"}`. Tells a `string` column of ordinary text from one that defeated type inference, which `data_type` cannot. All four keys are always present; all-zero = classified with nothing to classify (all-null or no rows); `None` = the classification did not run. Counted over the values the profiler retained, so sum them against `total_count - null_count` to tell an exact count from one bounded by the 10k reservoir sample |
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
| `min_length` | `int \| None` | Shortest value, in Unicode scalar values (0.11: UTF-8 bytes) |
| `max_length` | `int \| None` | Longest value, in Unicode scalar values (0.11: UTF-8 bytes) |
| `avg_length` | `float \| None` | Mean length, in Unicode scalar values (0.11: UTF-8 bytes) |
| `true_count` | `int \| None` | Number of `True` values (boolean columns) |
| `false_count` | `int \| None` | Number of `False` values (boolean columns) |
| `true_ratio` | `float \| None` | Ratio of `True` values (0.0--1.0) |
| `patterns` | `list[Pattern] \| None` | List of detected value patterns |

**Changed in 0.12 (#667).** Numeric columns with no finite parsed values and
boolean columns with no parsed booleans have no statistics block. Their statistic
accessors return `None`, including boolean counts and `true_ratio`. The declared
column type, row/null counts, and numeric `invalid_count` remain available. A
measured numeric zero or an all-false column still reports `0.0`. CSV columns
containing only nulls infer as text because CSV carries no declared type.

This uses the existing absent-statistics representation (`ColumnStats::None` in
Rust); numeric fields do not become optional within `NumericStats`, and the report
schema version is unchanged. Saved reports remain readable as written: loading an
older report does not replace its recorded zeros. Re-profile the source to obtain
the corrected absence semantics.

**Changed in 0.12.** Text lengths count Unicode scalar values, not UTF-8 bytes
and not grapheme clusters. `"東京"` has a length of 2 and `"🙂"` a length of 1.
**In 0.11 and earlier the same values reported 6 and 4**, because every
accumulator used Rust's byte-valued `str::len()` under a field named `length`
(#627). ASCII text is unaffected, and reports written by different releases are
not comparable for non-ASCII text.

A combining sequence counts each scalar, so the decomposed spelling of e-acute
(`e` followed by U+0301) has a length of 2 while the precomposed spelling
(U+00E9) has a length of 1. The two render identically and profile differently.
Segmenting grapheme clusters instead would need a Unicode segmentation table and
a policy for which version of it; encoded size is a property of the encoding
rather than of the value, and is reported at the source level instead.

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
    chunk_size=65536,                # bytes per chunk
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
SamplingStrategy.random(size=10000)                  # uniform sample of 10000 rows
SamplingStrategy.reservoir(size=10000)               # same guarantee, Algorithm R
SamplingStrategy.systematic(interval=10)             # every Nth row
SamplingStrategy.stratified(["region"], 1000)        # up to 1000 rows per region
SamplingStrategy.progressive(5000, 0.95, 100000)     # grow until means are precise
SamplingStrategy.importance("risk_score", 0.8)       # rows whose weight >= 0.8
SamplingStrategy.multi_stage([s1, s2])               # filters, then one fixed-size stage
SamplingStrategy.adaptive(total_rows=1000000, file_size_mb=500.0)
```

Sampling applies to CSV sources on the `auto` and `incremental` engines and to
every `dataprof.asyncio` entry point. The columnar engine and the JSON/Parquet
readers cannot sample row by row and raise `ValueError` rather than silently
returning a full profile.

`random` and `reservoir` give the same guarantee — a uniform sample of exactly
`size` rows — and both hold `size` rows in memory, because which rows belong in
the sample is not settled until the source ends. The other strategies decide
each row as it arrives and add no memory. Sampling bounds the cost of
*analysis*, not of reading: use a `StopCondition` to stop early.

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

# Overall score. None when no dimension was assessable — a header-only file, or
# one whose every dimension had a zero denominator. `assessed_dimensions()` is
# empty for exactly those reports, and `report.quality_score` agrees.
print(q.overall_quality_score())
print(q.assessed_dimensions())
print(q.score_weights)  # relative weights used by dataprof's aggregate formula

# Nested dimension evidence (None when the dimension was not assessed)
print(q.completeness)    # {"missing_values_ratio": ..., "complete_records_ratio": ..., "null_columns": [...]}
print(q.consistency)     # {"data_type_consistency": ..., "format_violations": ..., "encoding_issues": ...}
print(q.uniqueness)      # {"duplicate_rows": ..., "key_uniqueness": ..., "high_cardinality_warning": ...}
print(q.accuracy)        # {"outlier_ratio": ..., "range_violations": ..., "negative_values_in_positive": ...}
print(q.timeliness)      # {"future_dates_count": ..., "stale_data_ratio": ..., "temporal_violations": ...}
print(q.validity)        # {"valid_values_ratio": ..., "invalid_values": ..., "values_checked": ...}
print(q.precision)       # {"decimal_places_consistency": ..., "inconsistent_precision_values": ..., "numeric_values_checked": ...}
```

A dimension's evidence is present only when it assessed something. `None` covers
both "not computed" and "computed with nothing to divide by": no cells, no
non-null values, no numeric values, no dates, no confidently detected pattern.
The dimensions holding evidence are exactly the ones whose `dimension_scores()`
entry is a number.

`assessed_dimensions()` is that set narrowed once more, to the dimensions
carrying weight in the overall score. The two coincide under the default
weights; a dimension given a weight of `0.0` still holds evidence and a score
while sitting outside `assessed_dimensions()`, because it contributes nothing to
the aggregate. Branch on `dimension_scores()` to ask "was this measured", and on
`assessed_dimensions()` to ask "is this behind the overall score".

That distinction is the point rather than a technicality. `validity` reporting
`valid_values_ratio: 100.0` from `values_checked: 0` reads as a clean bill of
health for something nobody looked at, and any file without a pattern-bearing
column used to report exactly that. Serialized reports follow the same rule: an
unassessed dimension has no key, so a stored report cannot be read as a perfect
score either.

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
Timeliness scoring assesses confidently inferred date columns by default.
`temporal_columns` adds columns that inference cannot identify confidently, such
as mixed-format date strings.

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
`unique_count`, `uniqueness_ratio`, `dominant_type`, `dominant_type_share`,
`min`, `max`, `mean`, `std_dev`, `variance`,
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

The committed [JSON Schema 2020-12 contract](../schema/profile-report.v1.schema.json)
can validate `to_dict()`, `to_json()`, and JSON `save()` output in CI or another
consumer. Version 1 includes both the high-level Python export shape and the
complete Rust serialization shape; see the
[schema notes](../schema/README.md) for compatibility and regeneration rules.

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
report.save("report.html")      # HTML fragment (same as to_html())
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

Every format runs the same inference `profile()` does. On a Parquet file that
means the metadata answers for every column it types, and text columns are
sampled: only their values say whether they hold dates, integers or booleans,
which is what a writer that did not type its input leaves behind.
`rows_sampled` reports what that cost -- `0` for a fully typed file.

The sample is bounded in every format, so the type matches a full `profile()`
whenever `schema_stable` is true -- nothing was left unread that could move it,
either because the scan reached the end of the file or because the Parquet
metadata typed every column and no scan was needed. Once inference stops at the
cap, a later row can still move it.

### `analyze_structure()`

```python
structure = dp.analyze_structure("data.parquet")
for column in structure.columns:
    print(f"{column.name}: {column.data_type} nulls={column.null_count} ({column.provenance})")
```

Every counter it reports describes the whole file or is absent -- it never
returns a partial count. For CSV and JSON that means checking `truncated`; for
Parquet the counts come from the file footer, so they cover every row even when
the type sample stopped at `max_rows`. Distinct counts are absent for Parquet,
which has no whole-file source for one.

### `quick_row_count()`

```python
result = dp.quick_row_count("data.parquet")
print(f"{result.count} rows ({'exact' if result.exact else 'estimated'})")
print(f"Method: {result.method}, took {result.count_time_ms}ms")
```

## Async API

The `dataprof.asyncio` module provides async variants for use in web frameworks, stream processors, and other async contexts. These helpers ship in the published wheels.

```python
from dataprof.asyncio import profile_file, profile_bytes, profile_url

# Async file profiling
report = await profile_file("data.csv", max_rows=10000)

# Profile raw bytes (e.g. from an HTTP request body)
report = await profile_bytes(csv_bytes, format="csv")

# Profile a remote file over HTTP
report = await profile_url("https://example.com/data.parquet")
```

Async byte streams use the incremental engine; `engine="columnar"` is rejected.
Local async Parquet profiling honors `max_rows` and row-limit stop conditions,
and rejects stop conditions or sampling strategies the Parquet reader cannot
apply.

Additional async utilities:

```python
from dataprof.asyncio import infer_schema_stream, quick_row_count_stream

schema = await infer_schema_stream(csv_bytes, format="csv")
count = await quick_row_count_stream(csv_bytes, format="csv")
```

## Database Profiling (Source Build Only)

Async database functions for PostgreSQL, MySQL, and SQLite are **not** in the
published wheels, and there is no extra that installs them. On a wheel install
they exist as stubs that raise `ImportError` when called, and
`dp.capabilities().database` is `False`. They need a source build with
`python-async`, `database`, and the connector features you want.

### From the published sdist, with pip

No checkout required:

```bash
pip install dataprof --no-binary dataprof   --config-settings="build-args=--features python,python-async,async-streaming,parquet-async,database,sqlite"
```

### From a checkout

```bash
uv run maturin develop --features "python,python-async,async-streaming,parquet-async,database,sqlite"
```

### List every feature you want, not only the extra ones

`--features` **replaces** the `[tool.maturin] features` list in
`pyproject.toml`; it does not add to it. This is the part that bites, because
the result looks like it worked:

```bash
# Wrong: builds database support and silently drops async, URL profiling,
# and remote Parquet — surface that a plain `pip install dataprof` includes.
--features "python,python-async,database,sqlite"
```

That command yields `capabilities().database == True` alongside
`async_streaming == False`, `url_profiling == False`, and
`remote_parquet == False`. Nothing errors; the package is simply smaller than
the published wheel. The lists above are the shipped set plus the database
features, which is why they are long.

`database` on its own is also not enough: the connectors are reported only when
`python-async` is compiled in as well.

A source build needs a Rust toolchain (1.96 or later; CI compiles the extension
crate on exactly 1.96, so that floor is tested rather than assumed).
`postgres` and `mysql` are pure Rust; `sqlite` compiles `libsqlite3-sys`, which
is C.

Check what you actually got before relying on it:

```python
import dataprof as dp

caps = dp.capabilities()
assert caps.database and "sqlite" in caps.database_connectors
assert caps.async_streaming  # still present, because the list above kept it
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

`batch_size` must be greater than zero. Query result column names must be
unique; duplicate aliases are rejected before values can be merged.

## Arrow Interop

`profile()` also accepts one-shot producers implementing the
[Arrow C Stream capsule protocol](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrowstream-export):

```python
import pyarrow as pa
import dataprof

batches = [pa.record_batch({"id": [1, 2]}), pa.record_batch({"id": [3, 4]})]
reader = pa.RecordBatchReader.from_batches(batches[0].schema, iter(batches))
report = dataprof.profile(reader, max_rows=3)
assert report.rows == 3
assert report.source_type == "stream"

# With DuckDB installed, pass a relation directly:
# report = dataprof.profile(duckdb.sql("select * from my_table"), max_rows=1000)
```

dataprof consumes and releases each batch before fetching another; it does not
call `to_arrow_table()` or collect the stream. Producer batch buffers and the
bounded profiling accumulators determine memory use. A producer may itself
materialize data when exporting; dataprof cannot control that allocation.
PyArrow and DuckDB remain optional producer dependencies.

`max_rows` stops at the requested row boundary, including zero, without asking
for a subsequent batch. Reaching the cap sets `truncation_reason` even if that
row happened to be the stream's last: end-of-stream was not checked. An uncapped
or shorter stream is complete only after its producer signals the end. The
report uses engine `columnar`, source type `stream`, and source system
`arrow_c_stream`; the name identifies one profiling operation (`batch_id="0"`).
The stream is one-shot: use a fresh reader for another profile.

Metric packs, quality dimensions, locale, semantic hints, and column selection
work as on Arrow tables. Unsupported file/transport controls raise `ValueError`
before capsule export. Duplicate names are rejected before any batch is read,
even with column selection. Selected nested types (structs, lists, maps, unions)
raise `TypeError`; select flat columns before exporting, or with `columns=`.
An empty stream with a schema retains its columns and unassessed quality.
Batch/schema failures abort the call; no partial report is returned. The C
interface carries producer error text, which is retained as `__cause__`, rather
than the original Python exception object. Existing Table, C Array, pandas, and
polars adapters retain their behavior.

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
