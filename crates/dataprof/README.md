# dataprof

`dataprof` profiles CSV, JSON, JSONL, and Parquet files locally. Use
`Profiler` to inspect column types, missing values, and distributions, then
evaluate the report against a `QualityPolicy` to check whether the data meets
your requirements.

## Install

```bash
cargo add dataprof
```

Minimum supported Rust version: **1.96**.

## Quick start

Put an `orders.csv` file in your working directory. This example profiles the
file and prints the verdict for a minimum quality score of 90.

```rust,no_run
use dataprof::{Profiler, QualityPolicy};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = Profiler::new().analyze_file("orders.csv")?;
    let result = QualityPolicy::new()
        .min_quality_score(90.0)
        .evaluate(&report)?;
    println!("Quality verdict: {:?}", result.verdict);
    Ok(())
}
```

## Cargo features

CSV, JSON, and JSONL file profiling are available with default features
disabled. The default feature set enables `parquet`, which also enables
`arrow`.

| Feature | Enables |
| --- | --- |
| `parquet` (default) | Local Parquet profiling. Includes `arrow`. |
| `arrow` | The Arrow-backed columnar engine. |
| `async-streaming` | Async profiling from files, byte streams, and HTTP sources. |
| `parquet-async` | Async Parquet profiling over HTTP. Includes `parquet` and `async-streaming`. |
| `database` | The database profiling API. Enable a backend feature to connect. |
| `postgres` | The PostgreSQL backend. Includes `database`. |
| `mysql` | The MySQL and MariaDB backend. Includes `database`. |
| `sqlite` | The SQLite backend. Includes `database`. |
| `all-db` | All three database backends. |

## More documentation

- [Rust API on docs.rs](https://docs.rs/dataprof/latest/dataprof/)
- [Cargo feature dependencies on docs.rs](https://docs.rs/crate/dataprof/latest/features)
- [Full project README on GitHub](https://github.com/AndreaBozzo/dataprof/blob/HEAD/README.md)
