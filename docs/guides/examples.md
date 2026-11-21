# Examples Cookbook

Copy-pasteable recipes for common dataprof tasks, organized by interface.
These examples focus on the current Rust crate and Python package; pre-0.8 CLI
usage lives in the archived guide.

Recipes marked **0.14** need dataprof 0.14 or later.

## Python Recipes

### Basic file profiling

```python
import dataprof as dp

report = dp.profile("data.csv")
print(f"{report.rows} rows, {report.columns} columns")
print(f"Quality: {report.quality_score}")
```

### Access columns directly

```python
import dataprof as dp

report = dp.profile("data.csv")

# Dict-like access
col = report["age"]
print(f"mean={col.mean}, nulls={col.null_percentage}%")

# Check if a column exists
if "email" in report:
    print(f"email patterns: {report['email'].patterns}")

# Iterate column names
for name in report:
    print(name, report[name].data_type)
```

### Profile a pandas DataFrame

```python
import pandas as pd
import dataprof as dp

df = pd.read_csv("data.csv")
report = dp.profile(df)

for name in report:
    col = report[name]
    print(f"{col.name}: {col.data_type}, {col.null_percentage:.1f}% null")
```

### Profile ad-hoc notebook data

```python
import dataprof as dp

# Column-oriented scratch data, like pandas.DataFrame({...})
report = dp.profile({"age": [31, 42, 29], "city": ["Rome", "Milan", "Rome"]})

# Row-oriented records, common after API calls or lightweight transforms
report = dp.profile([
    {"age": 31, "city": "Rome"},
    {"age": 42, "city": "Milan"},
])

print(report.rows, report.columns, report["city"].unique_count)
```

### Profile in-memory file bytes

```python
import io
import dataprof as dp

csv_bytes = b"age,city\n31,Rome\n42,Milan\n"

# The sync byte path needs an explicit format because there is no filename.
report = dp.profile(io.BytesIO(csv_bytes), format="csv")
```

### Profile a Polars DataFrame

```python
import polars as pl
import dataprof as dp

df = pl.read_csv("data.csv")
report = dp.profile(df)
```

### Profile a PyArrow table

```python
import pyarrow.parquet as pq
import dataprof as dp

table = pq.read_table("data.parquet")
report = dp.profile(table)
```

### Use sampling to limit processing

```python
import dataprof as dp
from dataprof import SamplingStrategy

# A uniform random sample of exactly 50,000 rows.
# Holds 50,000 rows in memory: the sample is not settled until the file ends.
report = dp.profile("huge.csv", sampling=SamplingStrategy.reservoir(50000))
print(f"Sampled {report.rows} rows, sampling_ratio: {report.sampling_ratio}")

# Systematic: every 10th row. Decided per row, so no extra memory.
report = dp.profile("huge.csv", sampling=SamplingStrategy.systematic(10))
```

Sampling limits how much data is **analyzed**, not how much is **read**: pair it
with a `StopCondition` to bound I/O as well.

### Early termination with stop conditions

```python
from dataprof import StopCondition

# Stop after 10k rows or 50 MB
stop = StopCondition.max_rows(10000) | StopCondition.max_bytes(50_000_000)
report = dp.profile("stream.csv", stop_condition=stop)
print(f"Truncated: {report.truncation_reason}")
```

### Selective profiling for speed

```python
# Compute only basic statistics, skip patterns and quality
report = dp.profile("huge.csv", metrics=["schema", "statistics"])

# Use the Profiler builder
report = (dp.Profiler()
          .metrics(["schema", "statistics"])
          .max_rows(10000)
          .profile("huge.csv"))
```

### Boolean Data Analysis

```python
report = dp.profile("data.csv")
active_col = report["is_active"]

if active_col.data_type == "boolean":
    print(f"True: {active_col.true_count} ({active_col.true_ratio:.1%})")
    print(f"False: {active_col.false_count}")
```

### Progress callbacks

```python
import dataprof as dp

def show_progress(event):
    if event.percentage is not None:
        print(f"\r{event.percentage:.0f}% ({event.rows_processed} rows)", end="")

report = dp.profile("large.csv", on_progress=show_progress)
print()  # newline after progress
```

### Selective quality dimensions

```python
# Only compute completeness and uniqueness (faster)
report = dp.profile("data.csv", quality_dimensions=["completeness", "uniqueness"])
print(report.quality.completeness)     # has data
print(report.quality.consistency)      # None (not requested)
```

### Quick pandas-like summary with describe()

```python
import dataprof as dp

report = dp.profile("data.csv")
print(report.describe())
#          col_a   col_b   col_c
# count    1000    1000    1000
# null%    0.0     2.1     0.0
# unique   45      800     3
# mean     34.5    None    None
# std      12.1    None    None
# ...
```

### Export to pandas, polars, or Arrow

```python
import dataprof as dp

report = dp.profile("data.csv")

# pandas DataFrame with all stats
df = report.to_dataframe()
print(df[["name", "data_type", "null_percentage", "mean", "std_dev"]])

# polars DataFrame (no pandas needed)
pl_df = report.to_polars()
high_null = pl_df.filter(pl.col("null_percentage") > 5.0)

# PyArrow Table (no pandas needed)
table = report.to_arrow()
```

### Save and reload reports

```python
import json
import dataprof as dp

report = dp.profile("data.csv")

# Save as JSON (full report)
report.save("report.json")

# Save column profiles as CSV (no extra deps)
report.save("profiles.csv")

# Save column profiles as Parquet (requires pyarrow)
report.save("profiles.parquet")

# Later: reload JSON and inspect
with open("report.json") as f:
    data = json.load(f)
print(data["execution"]["rows_processed"])
```

### Track quality over time

```python
from pathlib import Path
import pandas as pd
import dataprof as dp

files = sorted(Path("warehouse/daily/").glob("orders_*.csv"))
rows = [dp.profile(str(f)).quality_summary() for f in files]
history = pd.DataFrame(rows)
print(history[["source", "rows", "quality_score", "completeness"]])
```

### Async file profiling

```python
import asyncio
from dataprof.asyncio import profile_file

async def main():
    report = await profile_file("data.csv", max_rows=10000)
    print(f"{report.rows} rows")

asyncio.run(main())
```

### Async URL profiling

```python
import asyncio
from dataprof.asyncio import profile_url

async def main():
    report = await profile_url("https://example.com/data.csv", format="csv")
    print(f"{report.rows} rows from remote source")

asyncio.run(main())
```

### Database profiling from Python (source build)

Not in the published wheels. Build the Python extension from a checkout with `python-async,database` and the connector feature you need before using this recipe.

```python
import asyncio
import dataprof as dp

async def main():
    report = await dp.analyze_database_async(
        "postgres://user:pass@localhost/mydb",
        "SELECT * FROM users WHERE active = true",
        batch_size=5000,
    )
    print(f"{report.rows} rows, quality: {report.quality_score}")

asyncio.run(main())
```

### Fast schema and row count

```python
import dataprof as dp

schema = dp.infer_schema("data.csv")
print(f"Columns: {schema.column_names}")
print(f"Sampled {schema.rows_sampled} rows in {schema.inference_time_ms}ms")

count = dp.quick_row_count("data.parquet")
print(f"{count.count} rows ({'exact' if count.exact else 'estimated'})")
```

---

## Data Engineering Integration Recipes

### Validate incoming data in an ETL pipeline

```python
import sys
import dataprof as dp

report = dp.profile("s3_landing/orders_2024-03-17.csv")

result = report.check(
    min_rows=10,
    min_quality_score=90,
    max_null_count={"order_id": 0},
    max_duplicate_rows=0,
    require_metrics=["quality"],
)
report.save("candidate.json")
for check in [*result.violations, *result.unevaluated]:
    print(check.code, check.column, check.observed, check.reason)
print(result.verdict)
sys.exit({"pass": 0, "fail": 1, "inconclusive": 2}[result.verdict])
```

The gate retains missing or incomplete evidence as unevaluated. Only a pass
exits zero. For a shell job, `python -m dataprof.check` also returns `3` on an
argument, policy, input, baseline or output error.

### Checkpoint and volume changes

Compare against the last accepted report, then promote only a passing
candidate. One policy serves the first run too: `allow_missing_baseline`
(**0.15**) records the rules about change as `not_applicable` while no
baseline exists. The row-count rules below need **0.14** and measure changes as
percentages of the baseline's count.

```python
from pathlib import Path
import dataprof as dp

baseline_path = Path("baseline.json")
candidate_path = Path("candidate.json")
report = dp.profile("orders.csv")
report.save(candidate_path)
baseline = dp.ProfileReport.load(baseline_path) if baseline_path.exists() else None
result = report.check(
    min_rows=1,
    max_null_count={"order_id": 0},
    baseline=baseline,
    allow_missing_baseline=True,
    max_quality_score_drop=5,
    max_row_count_drop=10,
    max_row_count_growth=20,
    forbid_removed_columns=True,
    timeliness_clock="rolling",
)
print(result.verdict)
for check in [*result.violations, *result.unevaluated]:
    print(check.code, check.reason)
if result.passed:
    candidate_path.replace(baseline_path)
```

`definition_not_comparable` means an affected metric definition changed or
was not recorded. Review the reason before refreshing the baseline on purpose.
The [runnable checkpoint](../../python/examples/baseline_checkpoint.py) covers
older reports and schema changes too. `min_rows` gives an absolute floor;
relative row-count checks catch loss or growth even when null shares stay the
same. In CI, their flags are `--max-row-drop 10` and `--max-row-growth 20`,
and `--allow-missing-baseline --promote baseline.json` replaces the rename.

### Freshness and missing days

These rules need **0.14**. This example fixes the clock so its result does not
depend on when you run it:

```python
import dataprof as dp

report = dp.profile(
    {"published_at": ["2026-06-01", "2026-06-02", "2026-06-10"]},
    reference_date="2026-06-10",
)
result = report.check(
    max_date_age={"published_at": 3},
    max_date_gap={"published_at": 1},
)
for check in result:
    print(check.code, check.status, check.observed)
# max_date_age passed 0
# max_date_gap failed 8
```

`max_date_age` reads the newest date against the report's recorded reference
date. `max_date_gap` reads the largest gap between consecutive distinct UTC
days, in calendar days. A contiguous daily column has
a gap of `1`. Under `full_source`, a partial scan leaves the gap rule
inconclusive because unread rows could fill or extend a gap.

Inferred date columns contribute to timeliness automatically. For explicit
roles and a start/end relation, pass hints when profiling:

```python
report = dp.profile(
    "contracts.csv",
    temporal_columns=["reviewed_at"],
    temporal_pairs=[("valid_from", "valid_until")],
    reference_date="2026-06-10",
)
print(report.quality.timeliness)
```

`temporal_violations` counts compared rows whose start follows their end.
Without `reference_date`, profiling uses the run's UTC day and records it in
`quality.timeliness["clock"]`. For baseline score comparisons, use the same
fixed date on both runs or `timeliness_clock="rolling"` for two rolling clocks.
The CLI offers `--max-date-age published_at=3` and
`--max-date-gap published_at=1`; it uses today's UTC date and has no reference
date flag.

### Whole-row duplicates and unique keys

In **0.14**, the whole-row tracker can keep a larger exact set. This does not
raise the separate per-column distinct-count limit.

```python
import dataprof as dp

report = dp.profile(
    "orders.csv",
    identifier_columns=["order_id"],
    duplicate_rows_exact_limit=2_000_000,
)
result = report.check(
    max_duplicate_rows=0,
    unique_columns=["order_id"],
    max_null_count={"order_id": 0},
    forbid_findings=["key_duplicates"],
)
print(result.verdict)
print(report.quality.uniqueness)
```

`max_duplicate_rows` compares complete rows; `unique_columns` checks repeated
non-null values in each named column, even when another column differs.
`identifier_columns` selects the first present key for the uniqueness score
and the `key_duplicates` finding. `forbid_findings` lets a detected finding fail
the gate. The example names the key in both places to show the two APIs; the
direct uniqueness rule suffices to enforce the key.

The row tracker defaults to 1,000,000 distinct rows and can estimate earlier
under memory pressure. An explicit `duplicate_rows_exact_limit` retains its
set even past the memory budget, at about 10 to 20 bytes per distinct row.
Read `duplicate_rows_approximate` in the uniqueness metrics. Estimated counts
leave `max_duplicate_rows` unevaluated. A per-column estimated distinct count,
marked `unique_count_is_approximate`, leaves `unique_columns` unevaluated too.

```bash
python -m dataprof.check orders.csv --unique-column order_id \
  --max-null-count order_id=0 --max-duplicate-rows 0 \
  --duplicate-rows-exact-limit 2000000
```

### Inspect schema drift and declared decimals

`forbid_removed_columns=True` rejects a removed column against a baseline.
Retyping is reported as a diagnostic; it does not itself fail a gate.

```python
import dataprof as dp

baseline = dp.ProfileReport.load("baseline.json")
report = dp.profile("orders.csv")
print(baseline.compare(report)["schema"])
result = report.check(baseline=baseline, forbid_removed_columns=True)
print(result.baseline["diagnostics"])
```

In **0.14**, `native_type` records the source's declaration independently of
the inferred `data_type`. Arrow and Parquet decimals and database
`NUMERIC` / `DECIMAL` columns are read from their decimal digits. Declared
decimal columns remain decimal-aware when the statistics pack is omitted.
For example, with pyarrow installed:

```python
from decimal import Decimal
import pyarrow as pa
import dataprof as dp

table = pa.table({
    "amount": pa.array([Decimal("10.25"), Decimal("20.50")], type=pa.decimal128(12, 2)),
})
report = dp.profile(table, metrics=["quality"])
print(report["amount"].native_type)
# {'name': 'decimal128(12, 2)', 'system': 'arrow'}
```

`native_type` is absent for CSV, JSON, JSONL and SQLite, which provide no
usable declared type to the profiler. A pandas `object` column does not declare
a decimal type; use an Arrow decimal dtype when that declaration matters.

### Per-column outlier triage

The global `accuracy.outlier_ratio` tells you *something* is suspicious, but
per-column `outlier_count` tells you *where* to look.

```python
import dataprof as dp

report = dp.profile("sensor_readings.csv")

# Surface the worst offenders
suspicious = sorted(
    (
        (name, report[name].outlier_count, report[name].min, report[name].max)
        for name in report
        if report[name].outlier_count
    ),
    key=lambda row: row[1],
    reverse=True,
)

for name, n_outliers, lo, hi in suspicious[:5]:
    print(f"{name:30} {n_outliers:>4} outliers  range=[{lo}, {hi}]")
```

### Domain hints for IDs and positive values

```python
report = dp.profile(
    "orders.csv",
    identifier_columns=["order_id", "customer_id"],
    positive_columns=["total_amount"],
)

assert report["order_id"].data_type == "identifier"
if report.quality and report.quality.accuracy:
    print(report.quality.accuracy["negative_values_in_positive"])
```

### Serialize a single column

For piping individual column profiles into Airflow XCom, Prometheus, a
warehouse, etc., without dragging the whole report along:

```python
import json
import dataprof as dp

report = dp.profile("data.csv")
payload = dp.column_to_dict(report["amount"])
print(json.dumps(payload, indent=2))
```

The shape is identical to one element of ``report.to_dict()["columns"]``.

### Profile before and after a cleaning step

```python
import pandas as pd
import dataprof as dp

df = pd.read_csv("raw_transactions.csv")
raw = dp.profile(df, name="raw_transactions")

# Standard cleaning
df = df.dropna(subset=["account_id", "amount"])
df = df.drop_duplicates(subset=["txn_id"])
df["amount"] = df["amount"].clip(lower=0)
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

clean = dp.profile(df, name="clean_transactions")

print(f"Before: {raw.quality_score:.2f}  After: {clean.quality_score:.2f}")
print(f"Rows: {raw.rows} -> {clean.rows} ({raw.rows - clean.rows} dropped)")
```

### Catch a key column that collapses to a few values

```python
import dataprof as dp

baseline = dp.ProfileReport.load("quality/baseline.json")  # last accepted run
today = dp.profile("landing/orders.csv", identifier_columns=["order_id"])

result = today.check(
    min_quality_score=85,
    max_null_percentage={"customer_id": 5},
    baseline=baseline,
    # Fails when the distinct count fell by more than 10% of the baseline's
    # and its share of the non-null values fell as far (0.14+).
    max_distinct_drop={"customer_id": 10},
)
print(result.verdict)
for check in result.violations:
    if check.code == "max_distinct_drop":
        print(check.column, check.baseline["observed"], "->", check.observed)
    else:
        print(check.code, check.column, check.observed)
```

A broken join that fills `customer_id` with one default value leaves the score
and the null counts as they were. Only the distinct count moves, from 40 to 1
in the runnable version, [`key_collapse_gate.py`](../../python/examples/key_collapse_gate.py).
A smaller extract with the same share of distinct values passes.

### Monitor table quality across multiple CSVs

```python
from pathlib import Path
import dataprof as dp

files = sorted(Path("warehouse/daily/").glob("orders_*.csv"))
for f in files:
    r = dp.profile(str(f), quality_dimensions=["completeness", "uniqueness"])
    completeness = r.quality.completeness
    uniqueness = r.quality.uniqueness
    print(f"{f.name:40s}  rows={r.rows:>8d}  "
          f"complete={completeness['complete_records_ratio']:.1f}%  "
          f"dupes={uniqueness['duplicate_rows']}")
```

### Profile a Polars DataFrame from a Parquet source

```python
import polars as pl
import dataprof as dp

df = pl.scan_parquet("events/*.parquet").collect()
report = dp.profile(df, engine="columnar")

# Turn the profile into a Polars DataFrame natively
profile_df = report.to_polars()
problematic = profile_df.filter(pl.col("null_percentage") > 5.0)
print(f"{len(problematic)} columns with >5% nulls:")
print(problematic.select(["name", "null_percentage", "unique_count"]))
```

### Profile NumPy feature matrices via pandas

Skewness and excess kurtosis use the adjusted sample estimators from pandas.
They require at least three and four numeric values respectively; a constant
column reports zero. The skewness threshold below uses that convention.

```python
import numpy as np
import pandas as pd
import dataprof as dp

X_train = np.load("features/X_train.npy")
feature_names = [f"feat_{i}" for i in range(X_train.shape[1])]

df = pd.DataFrame(X_train, columns=feature_names)
report = dp.profile(df, name="X_train")

# Check for ML-readiness issues
for name in report:
    col = report[name]
    issues = []
    if col.null_count > 0:
        issues.append(f"{col.null_percentage:.1f}% null")
    if col.std_dev is not None and col.std_dev == 0:
        issues.append("zero variance")
    if col.skewness is not None and abs(col.skewness) > 2:
        issues.append(f"skew={col.skewness:.1f}")
    if issues:
        print(f"  {col.name}: {', '.join(issues)}")
```

### Profile a PyArrow table (zero-copy from Parquet)

```python
import pyarrow.parquet as pq
import dataprof as dp

table = pq.read_table("events.parquet")
report = dp.profile(table)
print(f"{report.rows} rows, {report.columns} columns, {report.execution_time_ms}ms")
```

### Async profiling in a FastAPI endpoint

```python
from fastapi import FastAPI, UploadFile
from dataprof.asyncio import profile_bytes

app = FastAPI()

@app.post("/profile")
async def profile_upload(file: UploadFile):
    data = await file.read()
    fmt = "csv" if file.filename.endswith(".csv") else "json"
    report = await profile_bytes(data, format=fmt, max_rows=100_000)
    return {
        "rows": report.rows,
        "columns": report.columns,
        "quality_score": report.quality_score,
        "column_profiles": report.to_dict()["columns"],
    }
```

### Compare database vs file quality (source build)

Not in the published wheels. Build the Python extension from a checkout with `python-async,database` and the connector feature you need before using this recipe.

```python
import asyncio
import dataprof as dp

async def main():
    # Profile the same logical dataset from two sources
    file_report = dp.profile("exports/users.csv")
    db_report = await dp.analyze_database_async(
        "postgres://ro:pass@prod/app",
        "SELECT * FROM users",
    )

    print(f"File: {file_report.rows} rows, quality={file_report.quality_score:.2f}")
    print(f"DB:   {db_report.rows} rows, quality={db_report.quality_score:.2f}")
    print(f"Row diff: {abs(file_report.rows - db_report.rows)}")

asyncio.run(main())
```

---

## Rust Recipes

The synchronous snippets go inside
`fn main() -> Result<(), dataprof::DataProfilerError>`, before its final `Ok(())`.

### Basic file profiling

```rust
use dataprof::Profiler;

let report = Profiler::new().analyze_file("data.csv")?;
println!("Rows: {}", report.execution.rows_processed);
println!("Columns: {}", report.execution.columns_detected);
```

### Explicit engine selection

```rust
use dataprof::{Profiler, EngineType};

// Force the incremental (streaming) engine
let report = Profiler::new()
    .engine(EngineType::Incremental)
    .analyze_file("large.csv")?;

// Force the columnar (Arrow) engine
let report = Profiler::new()
    .engine(EngineType::Columnar)
    .analyze_file("data.parquet")?;
```

### Streaming with stop conditions

```rust
use dataprof::{Profiler, StopCondition};

let report = Profiler::new()
    .stop_when(StopCondition::MaxRows(50_000))
    .analyze_file("huge.csv")?;

println!("Exhausted: {}", report.execution.source_exhausted);
println!("Truncation: {:?}", report.execution.truncation_reason);
```

### Custom CSV configuration

```rust
use dataprof::Profiler;

let report = Profiler::new()
    .csv_delimiter(b';')
    .csv_flexible(true)
    .analyze_file("european_data.csv")?;
```

### Sampling

```rust
use dataprof::{Profiler, SamplingStrategy};

let report = Profiler::new()
    .sampling(SamplingStrategy::Reservoir { size: 10_000 })
    .analyze_file("huge.csv")?;

println!("Sampling applied: {}", report.execution.sampling_applied);
```

### Memory limits

```rust
use dataprof::Profiler;

let report = Profiler::new()
    .memory_limit_mb(256)
    .analyze_file("data.csv")?;
```

### Progress callbacks

```rust
use dataprof::{Profiler, ProgressSink, ProgressEvent};
use std::sync::Arc;

let report = Profiler::new()
    .progress_sink(ProgressSink::Callback(Arc::new(|event: ProgressEvent| {
        if let ProgressEvent::ChunkProcessed { rows_processed, percentage, .. } = event {
            if let Some(pct) = percentage {
                println!("{:.0}% ({} rows)", pct, rows_processed);
            }
        }
    })))
    .analyze_file("data.csv")?;
```

### Selective quality dimensions

```rust
use dataprof::{Profiler, QualityDimension};

let report = Profiler::new()
    .quality_dimensions(vec![
        QualityDimension::Completeness,
        QualityDimension::Uniqueness,
    ])
    .analyze_file("data.csv")?;
```

### Schema inference and row counting

```rust
use dataprof::{infer_schema, quick_row_count};

let schema = infer_schema("data.csv")?;
for col in &schema.columns {
    println!("{}: {:?}", col.name, col.data_type);
}

let count = quick_row_count("data.parquet")?;
println!("{} rows (exact: {})", count.count, count.exact);
```

### One-liner quality check

```rust
use dataprof::quick_quality_check;

// `None` when nothing was assessable, such as a header-only CSV.
match quick_quality_check("data.csv")? {
    Some(score) => println!("Quality: {score:.1}%"),
    None => println!("Quality: not assessed"),
}
```

### Async stream profiling

Enable `async-streaming` for CSV and JSON byte streams, and add `bytes` directly:

```toml
[dependencies]
dataprof = { version = "0.14", features = ["async-streaming"] }
bytes = "1.10"
```

```rust
use dataprof::{Profiler, BytesSource, AsyncSourceInfo, FileFormat};

async fn profile_upload() -> Result<(), dataprof::DataProfilerError> {
    let csv_bytes = bytes::Bytes::from_static(b"name,score\nada,100\n");
    let source = BytesSource::new(csv_bytes, AsyncSourceInfo::new("upload.csv", FileFormat::Csv));

    let report = Profiler::new().profile_stream(source).await?;
    println!("Profiled {} rows", report.execution.rows_processed);
    Ok(())
}
```

### Async URL profiling

Remote Parquet needs `parquet-async`, which also enables `async-streaming`:

```toml
[dependencies]
dataprof = { version = "0.14", features = ["parquet-async"] }
```

```rust
use dataprof::Profiler;

async fn profile_remote() -> Result<(), dataprof::DataProfilerError> {
    let report = Profiler::new()
        .profile_url("https://example.com/data.parquet")
        .await?;
    println!("Profiled {} rows", report.execution.rows_processed);
    Ok(())
}
```

### Database query profiling

Enable the PostgreSQL connector:

```toml
[dependencies]
dataprof = { version = "0.14", features = ["postgres"] }
```

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
