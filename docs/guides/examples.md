# Examples Cookbook

Copy-pasteable recipes for common dataprof tasks, organized by interface.

---

## CLI Recipes

### Profile a CSV file

```bash
dataprof analyze data.csv
```

### Detailed quality report with strict thresholds

```bash
dataprof analyze data.csv --detailed --threshold-profile strict
```

### Export analysis as JSON for CI/CD

```bash
dataprof analyze data.csv --format json -o report.json

# Check quality score in a script
SCORE=$(dataprof analyze data.csv --format json | jq '.quality.overall_score')
echo "Quality: $SCORE"
```

### Quick schema check before loading

```bash
dataprof schema data.csv
dataprof schema data.parquet --format json
```

### Count rows in a file

```bash
dataprof count data.csv
dataprof count data.parquet    # instant: reads Parquet metadata only
```

### Profile with sampling for large files

```bash
dataprof analyze huge.csv --sample 50000 --progress
```

### Profile with a configuration file

```bash
# .dataprof.toml
# [output]
# format = "json"
# show_progress = true

dataprof analyze data.csv --config .dataprof.toml
```

### Selective profiling (Performance)

```bash
# Skip expensive pattern detection and quality assessment
dataprof analyze data.csv --metrics schema --metrics statistics

# Compute only schema (fastest)
dataprof analyze data.csv --metrics schema
```

### Boolean Analysis

```bash
dataprof analyze data.csv --detailed | grep -A 5 "Boolean"
```

### Profile a database table

```bash
dataprof database postgres://user:pass@localhost/mydb --table users --quality
dataprof database sqlite:///data.db --query "SELECT * FROM events WHERE date > '2024-01-01'"
```

### Pipe-friendly plain output

```bash
dataprof analyze data.csv --format plain | grep "null_count"
dataprof schema data.csv --format json | jq '.columns[] | .name'
```

---

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

# Reservoir sampling: statistically representative subset
report = dp.profile("huge.csv", sampling=SamplingStrategy.reservoir(50000))
print(f"Sampled {report.rows} rows, sampling_ratio: {report.sampling_ratio}")

# Systematic: every 10th row
report = dp.profile("huge.csv", sampling=SamplingStrategy.systematic(10))
```

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

if active_col.data_type == "Boolean":
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

### Database profiling from Python

```python
import asyncio
import dataprof as dp

async def main():
    report = await dp.analyze_database_async(
        "postgres://user:pass@localhost/mydb",
        "SELECT * FROM users WHERE active = true",
        batch_size=5000,
        calculate_quality=True,
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
score = report.quality_score or 0.0

if score < 0.90:
    q = report.quality
    failures = []
    if q.missing_values_ratio > 0.1:
        failures.append(f"missing values: {q.missing_values_ratio:.1%}")
    if q.duplicate_rows > 0:
        failures.append(f"{q.duplicate_rows} duplicate rows")
    if q.outlier_ratio > 0.05:
        failures.append(f"outlier ratio: {q.outlier_ratio:.1%}")
    print(f"REJECTED (score={score:.2f}): {', '.join(failures)}")
    sys.exit(1)

print(f"ACCEPTED (score={score:.2f}), {report.rows} rows")
```

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

### Monitor table quality across multiple CSVs

```python
from pathlib import Path
import dataprof as dp

files = sorted(Path("warehouse/daily/").glob("orders_*.csv"))
for f in files:
    r = dp.profile(str(f), quality_dimensions=["completeness", "uniqueness"])
    q = r.quality
    print(f"{f.name:40s}  rows={r.rows:>8d}  "
          f"complete={q.complete_records_ratio:.1%}  "
          f"dupes={q.duplicate_rows}")
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

### Compare database vs file quality

```python
import asyncio
import dataprof as dp

async def main():
    # Profile the same logical dataset from two sources
    file_report = dp.profile("exports/users.csv")
    db_report = await dp.analyze_database_async(
        "postgres://ro:pass@prod/app",
        "SELECT * FROM users",
        calculate_quality=True,
    )

    print(f"File: {file_report.rows} rows, quality={file_report.quality_score:.2f}")
    print(f"DB:   {db_report.rows} rows, quality={db_report.quality_score:.2f}")
    print(f"Row diff: {abs(file_report.rows - db_report.rows)}")

asyncio.run(main())
```

---

## Rust Recipes

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

let score = quick_quality_check("data.csv")?;
println!("Quality: {:.1}%", score);
```

### Async stream profiling

```rust
use dataprof::{Profiler, BytesSource, AsyncSourceInfo, FileFormat};

let csv_bytes: bytes::Bytes = download_from_somewhere().await;
let source = BytesSource::new(csv_bytes, AsyncSourceInfo {
    label: "upload.csv".into(),
    format: FileFormat::Csv,
    size_hint: None,
    source_system: None,
});

let report = Profiler::new().profile_stream(source).await?;
```

### Async URL profiling

```rust
use dataprof::Profiler;

let report = Profiler::new()
    .profile_url("https://example.com/data.parquet")
    .await?;
```

### Database query profiling

```rust
use dataprof::{Profiler, DatabaseConfig};

let report = Profiler::new()
    .connection_string("postgres://user:pass@localhost/mydb")
    .analyze_query("SELECT * FROM users")
    .await?;
```
