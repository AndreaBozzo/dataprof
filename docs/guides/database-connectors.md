# Database Connectors Guide

Profile data directly from PostgreSQL, MySQL, and SQLite databases with quality assessment informed by ISO 8000/25012 concepts.

> [!NOTE]
> Before 0.9.0, every column was read as text and non-text columns profiled as
> all-null strings. Integer, float, and boolean columns now decode correctly.
> Types outside that set — `NUMERIC`/`DECIMAL`, dates and times, and `BLOB` —
> still profile as nulls, because decoding them needs sqlx features this crate
> does not enable. Profile an exported CSV or Parquet file if you need
> statistics for those.

## Feature Requirements

Database support requires feature flags at compile time. Enable them on the
Rust crate or on the Python extension build, depending on the interface you
ship.

```toml
[dependencies]
dataprof = { version = "0.9", features = ["postgres"] }
```

For local Python extension development:

```bash
uv run maturin develop --features "python,python-async,database,sqlite"
```

## Supported Databases

| Database | Feature flag | Connection string format |
|---|---|---|
| PostgreSQL | `postgres` | `postgres://user:pass@host:5432/dbname` |
| MySQL/MariaDB | `mysql` | `mysql://user:pass@host:3306/dbname` |
| SQLite | `sqlite` | `sqlite:///path/to/db.db` or `/path/to/db.db` |

## Quick Start

### Python

Python database functions are async and are not included in the default PyPI wheel. Build the extension from source with `python-async`, `database`, and the connector feature you need first:

```bash
uv run maturin develop --features "python,python-async,database,sqlite"
```

Then the following APIs become available:

```python
import asyncio
import dataprof as dp

async def main():
    # Profile a query
    report = await dp.analyze_database_async(
        "postgres://user:pass@localhost/mydb",
        "SELECT * FROM users",
        batch_size=10000,
        calculate_quality=True,
    )
    print(f"{report.rows} rows, quality: {report.quality_score}")

    # Test connection
    ok = await dp.test_connection_async("postgres://user:pass@localhost/mydb")

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

### Rust

#### Using the Profiler builder

```rust
use dataprof::Profiler;

let report = Profiler::new()
    .connection_string("postgres://user:pass@localhost/mydb")
    .analyze_query("SELECT * FROM users")
    .await?;

println!("Rows: {}", report.execution.rows_processed);
if let Some(quality) = &report.quality {
    println!("Quality: {:.1}%", quality.score());
}
```

#### Using `analyze_database()` directly

```rust
use dataprof::{DatabaseConfig, analyze_database};
use dataprof::{RetryConfig, SslConfig};

let config = DatabaseConfig {
    connection_string: "postgres://user:pass@localhost/mydb".to_string(),
    batch_size: 10000,
    max_connections: Some(5),
    connection_timeout: Some(std::time::Duration::from_secs(30)),
    retry_config: Some(RetryConfig::default()),
    ssl_config: Some(SslConfig::default()),
    sampling_config: None,
    load_credentials_from_env: true,
};

let report = analyze_database(config, "SELECT * FROM users", true, None).await?;
println!("Rows: {}", report.execution.rows_processed);
```

## Connection Strings

### PostgreSQL

```
postgres://username:password@hostname:5432/database
postgresql://username:password@hostname:5432/database
```

Examples:
- `postgres://myuser:mypass@localhost:5432/production_db`
- `postgres://readonly_user@db.company.com:5432/analytics`

### MySQL/MariaDB

```
mysql://username:password@hostname:3306/database
```

Examples:
- `mysql://root:password@localhost:3306/ecommerce`
- `mysql://app_user:secret@mysql.internal:3306/app_data`

### SQLite

```
sqlite:///path/to/database.db
/path/to/database.db
:memory:
```

Examples:
- `sqlite:///home/user/data.db`
- `/var/data/app.sqlite`
- `:memory:` (in-memory database for testing)

## DatabaseConfig Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `connection_string` | `String` | required | Database connection URL |
| `batch_size` | `usize` | `10000` | Rows per streaming batch |
| `max_connections` | `Option<u32>` | `Some(10)` | Connection pool size |
| `connection_timeout` | `Option<Duration>` | `Some(30s)` | Connection timeout |
| `retry_config` | `Option<RetryConfig>` | `Some(default)` | Retry with exponential backoff |
| `sampling_config` | `Option<SamplingConfig>` | `None` | Sampling strategy for large tables |
| `ssl_config` | `Option<SslConfig>` | `Some(default)` | SSL/TLS encryption settings |
| `load_credentials_from_env` | `bool` | `true` | Load credentials from environment variables |

## Streaming for Large Datasets

dataprof processes database results in configurable batches to handle tables with millions of rows without exceeding memory:

1. Counts total rows for progress tracking
2. Fetches data in batches (default: 10,000 rows)
3. Merges statistics from all batches
4. Reports progress

Adjust `batch_size` based on your dataset:

| Dataset size | Recommended batch size |
|---|---|
| < 100k rows | `1000` -- `5000` |
| 100k -- 1M rows | `10000` -- `25000` |
| > 1M rows | `50000` -- `100000` |

## Sampling

For very large tables, configure sampling to analyze a representative subset:

```rust
use dataprof::SamplingConfig;

// Quick random sample
let config = DatabaseConfig {
    sampling_config: Some(SamplingConfig::quick_sample(10000)),
    ..Default::default()
};

// Representative stratified sample
let config = DatabaseConfig {
    sampling_config: Some(SamplingConfig::representative_sample(
        25000,
        Some("category".to_string()),
    )),
    ..Default::default()
};

// Temporal sample for time-series data
let config = DatabaseConfig {
    sampling_config: Some(SamplingConfig::temporal_sample(
        50000,
        "created_at".to_string(),
    )),
    ..Default::default()
};
```

## Security

### SSL/TLS Encryption

```rust
use dataprof::SslConfig;

// Production: requires SSL with certificate verification
let ssl = SslConfig::production();

// Development: relaxed for local testing
let ssl = SslConfig::development();

// Custom configuration
let ssl = SslConfig {
    require_ssl: true,
    verify_server_cert: true,
    ssl_mode: Some("require".to_string()),
    ca_cert_path: Some("/etc/ssl/certs/ca.pem".to_string()),
    ..Default::default()
};
```

### Environment Variables

Avoid embedding credentials in connection strings. Set environment variables instead:

| Variable | Description |
|---|---|
| `POSTGRES_HOST` | PostgreSQL hostname |
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DATABASE` | PostgreSQL database name |
| `POSTGRES_PORT` | PostgreSQL port (default: 5432) |
| `POSTGRES_SSL_MODE` | SSL mode (`require`, `verify-full`, etc.) |
| `POSTGRES_SSL_CA` | Path to CA certificate |
| `MYSQL_HOST` | MySQL hostname |
| `MYSQL_USER` | MySQL username |
| `MYSQL_PASSWORD` | MySQL password |
| `DATABASE_URL` | Generic connection URL |

With `load_credentials_from_env: true` (the default), dataprof will read these automatically.

## Troubleshooting

### Enable Debug Logging

```bash
RUST_LOG=dataprof=debug dataprof database postgres://... --table users
```

### Test Connection (Rust)

```rust
use dataprof::{DatabaseConfig, create_connector};

let config = DatabaseConfig {
    connection_string: "postgres://user:pass@localhost/mydb".to_string(),
    ..Default::default()
};

let mut connector = create_connector(config)?;
connector.connect().await?;
let ok = connector.test_connection().await?;
println!("Connection: {}", if ok { "OK" } else { "FAILED" });
```

### Test Connection (Python)

```python
ok = await dp.test_connection_async("postgres://user:pass@localhost/mydb")
print(f"Connection: {'OK' if ok else 'FAILED'}")
```

### Common Issues

**Connection refused** -- verify the database is running and accessible. dataprof retries with exponential backoff (3 attempts by default).

**Table not found** -- check table name and schema permissions. Use fully qualified names (`schema.table`) if needed.

**SSL errors** -- for local development, use `SslConfig::development()`. For production, ensure the CA certificate path is correct.

**Feature not enabled** -- if you see `Database feature not enabled`, recompile with the appropriate feature flag (e.g. `--features postgres`).

## Performance Tips

1. **Add WHERE clauses** to filter data before profiling
2. **Use indexes** on filtered columns for faster queries
3. **Avoid SELECT \*** on very wide tables -- select relevant columns
4. **Set appropriate batch sizes** based on row count (see table above)
5. **Use connection pooling** (`max_connections`) for repeated queries
