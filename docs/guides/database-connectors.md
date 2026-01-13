# Database Connectors Guide

This guide explains how to use DataProfiler with various database systems for direct data profiling with production-ready features including quality assessment, table sampling, and security.

## ⚙️ Feature Requirements

Database support requires feature flags. Install with:
```bash
cargo install dataprof --features postgres,mysql  # PostgreSQL + MySQL
cargo install dataprof --features all-db          # All databases
```

> **⚠️ Python Bindings Note**: Database connectors are currently **CLI and Rust API only**. Python bindings for database profiling are not available due to async compatibility limitations with PyO3. Python users can:
> - Use the CLI: `dataprof database --connection "postgresql://..." --table users`
> - Export to CSV and use Python bindings: `pd.read_sql().to_csv()` → `dataprof.analyze_csv_with_quality()`
> - Use native SQL connectors (psycopg2, SQLAlchemy) with dataprof for file analysis

## Supported Databases

- **PostgreSQL** - Production databases with connection pooling (✅ **Default**)
- **MySQL/MariaDB** - MySQL-compatible databases (✅ **Default**)
- **SQLite** - Embedded databases and file-based storage (✅ **Default**)
- **DuckDB** - Analytical databases and data warehousing (Optional feature)

## 🚀 Enhanced Features

### Data Quality Assessment (ISO 8000/25012)
Automatically assess database table quality across 5 dimensions:
- **Completeness**: Missing values and null analysis
- **Consistency**: Data type and format validation
- **Uniqueness**: Duplicate detection and key analysis
- **Accuracy**: Outlier detection and range validation
- **Timeliness**: Temporal data quality metrics

### Large Dataset Sampling
Handle massive database tables with intelligent sampling:
- **Multiple sampling strategies**: Random, Systematic, Stratified, Temporal
- **Automatic sample size optimization** based on data characteristics
- **Sample quality assessment** with confidence intervals
- **Representative sampling** to maintain statistical properties

### Production Security
Enterprise-ready security and reliability:
- **SSL/TLS encryption** with certificate validation
- **Environment variable support** for secure credential management
- **Connection retry logic** with exponential backoff
- **Connection health monitoring** and automatic recovery

## Quick Start

### Command Line Usage

```bash
# PostgreSQL with quality assessment
dataprof database --connection "postgresql://user:password@localhost:5432/mydb" --table "users"

# MySQL with automatic sampling for large tables
dataprof --database "mysql://root:password@localhost:3306/mydb" --query "SELECT * FROM orders"

# SQLite file
dataprof --database "data.db" --query "products"

# SQLite file
dataprof --database "file://analytics.sqlite" --query "SELECT * FROM sales WHERE date > '2024-01-01'"

# In-memory SQLite for testing
dataprof --database ":memory:" --query "temp_data"

# Environment variables (production recommended)
export POSTGRES_HOST=prod-db.company.com
export POSTGRES_USER=readonly_user
export POSTGRES_PASSWORD=secure_password
export POSTGRES_DATABASE=analytics
dataprof --database "postgresql://" --query "user_activity"
```

### Programmatic Usage (Rust API)

> **Note**: Python bindings not available for database operations. Use CLI or Rust API.  
> **Feature requirement**: Enable database feature: `dataprof = { version = "0.4", features = ["postgres"] }`

```rust
use dataprof::{DatabaseConfig, analyze_database, SamplingConfig, SslConfig};

#[tokio::main]  // Required: tokio async runtime is mandatory
async fn main() -> anyhow::Result<()> {
    // Production-ready configuration
    let config = DatabaseConfig {
        connection_string: "postgresql://user:pass@localhost:5432/mydb".to_string(),
        batch_size: 10000,
        max_connections: Some(5),
        connection_timeout: Some(std::time::Duration::from_secs(30)),
        sampling_config: Some(SamplingConfig::representative_sample(50000, None)),
        ssl_config: Some(SslConfig::production()),
        load_credentials_from_env: true,
        retry_config: Some(RetryConfig::default()),
    };

    // Get quality report with ISO 8000/25012 metrics
    let report = analyze_database(config, "SELECT * FROM large_table").await?;

    println!("📊 Processed {} rows across {} columns",
        report.file_info.total_rows.unwrap_or(0),
        report.file_info.total_columns
    );

    println!("📈 Quality Score: {:.1}%", report.quality_score());

    Ok(())
}
```

## Connection Strings

### PostgreSQL
```
postgresql://username:password@hostname:port/database
postgres://username:password@hostname:port/database
```

**Examples:**
- `postgresql://myuser:mypass@localhost:5432/production_db`
- `postgres://readonly_user@db.company.com:5432/analytics`

### MySQL/MariaDB
```
mysql://username:password@hostname:port/database
```

**Examples:**
- `mysql://root:password@localhost:3306/ecommerce`
- `mysql://app_user:secret@mysql.internal:3306/app_data`

### SQLite
```
sqlite:///path/to/database.db
/path/to/database.db
:memory:
```

**Examples:**
- `sqlite:///home/user/data.db`
- `/var/data/app.sqlite`
- `:memory:` (in-memory database)

### DuckDB
```
/path/to/database.duckdb
data.duckdb
```

**Examples:**
- `/analytics/warehouse.duckdb`
- `sales_data.duckdb`

## Streaming for Large Datasets

DataProfiler automatically uses streaming for large result sets to handle datasets with millions of rows without memory issues.

### Configuration

```rust
let config = DatabaseConfig {
    connection_string: "postgresql://user:pass@host/db".to_string(),
    batch_size: 50000,  // Process 50k rows per batch
    max_connections: Some(10),
    connection_timeout: Some(std::time::Duration::from_secs(60)),
};
```

### Batch Processing

The system automatically:
1. Counts total rows for progress tracking
2. Processes data in configurable batches (default: 10,000 rows)
3. Merges results from all batches
4. Provides progress updates

## Database-Specific Features

### PostgreSQL
- **Connection pooling** for high-performance access
- **Async processing** with tokio runtime
- **Schema introspection** via `information_schema`
- **Parameterized queries** for security

### MySQL/MariaDB
- **Async MySQL connector** using sqlx
- **Compatible** with MariaDB and MySQL variants
- **UTF-8 support** for international data

### SQLite
- **Embedded database** support
- **In-memory databases** for temporary analysis
- **File-based** databases
- **Single connection** optimized for embedded use

### DuckDB
- **Analytical workloads** optimized
- **Columnar storage** benefits
- **Complex queries** support
- **Thread-safe** operations

## Query Examples

### Basic Table Profiling
```bash
# Profile entire table
dataprof --database "postgresql://user:pass@host/db" --query "users"

# Profile with custom query
dataprof --database "mysql://root:pass@localhost/shop" --query "SELECT * FROM orders WHERE status = 'completed'"
```

### Advanced Queries
```bash
# Time-based filtering
dataprof --database "analytics.duckdb" --query "
  SELECT customer_id, order_total, order_date
  FROM sales
  WHERE order_date >= '2024-01-01'
    AND order_total > 100
"

# Joins and aggregations
dataprof --database "postgresql://user:pass@host/db" --query "
  SELECT u.name, u.email, COUNT(o.id) as order_count
  FROM users u
  LEFT JOIN orders o ON u.id = o.user_id
  GROUP BY u.id, u.name, u.email
"
```

## 🚀 Enhanced Configuration Options

### DatabaseConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_string` | String | Required | Database connection URL |
| `batch_size` | usize | 10000 | Rows per batch for streaming |
| `max_connections` | Option<u32> | Some(10) | Connection pool size |
| `connection_timeout` | Option<Duration> | Some(30s) | Connection timeout |
| `sampling_config` | Option<SamplingConfig> | None | Large dataset sampling strategy |
| `ssl_config` | Option<SslConfig> | Some(default) | SSL/TLS encryption settings |
| `load_credentials_from_env` | bool | true | Load credentials from environment |
| `retry_config` | Option<RetryConfig> | Some(default) | Connection retry with backoff |

### 📊 Quality Assessment

```rust
// Get comprehensive quality report
let config = DatabaseConfig {
    connection_string: "postgresql://user:pass@host/db".to_string(),
    ..Default::default()
};

let report = analyze_database(config, "user_profiles").await?;

println!("Overall Quality Score: {:.1}%", report.quality_score());

// Access quality metrics (ISO 8000/25012)
let metrics = &report.data_quality_metrics;
println!("Completeness: {:.1}%", metrics.complete_records_ratio);
println!("Consistency: {:.1}%", metrics.data_type_consistency);
println!("Uniqueness: {:.1}%", metrics.key_uniqueness);
```

### 📊 Large Dataset Sampling

```rust
use dataprof::database::{SamplingConfig, SamplingStrategy};

// Quick random sampling for fast analysis
let quick_sample = SamplingConfig::quick_sample(10000);

// Representative sampling with stratification
let stratified_sample = SamplingConfig::representative_sample(
    25000,
    Some("category".to_string())  // Stratify by category column
);

// Temporal sampling for time-series data
let temporal_sample = SamplingConfig::temporal_sample(
    50000,
    "created_at".to_string()  // Sample evenly across time
);

let config = DatabaseConfig {
    sampling_config: Some(stratified_sample),
    ..Default::default()
};
```

### 🔒 Production Security

```rust
use dataprof::database::SslConfig;

// Production SSL configuration
let production_ssl = SslConfig::production(); // Requires SSL, verifies certificates
let development_ssl = SslConfig::development(); // Relaxed for dev

// Custom SSL configuration
let custom_ssl = SslConfig {
    require_ssl: true,
    verify_server_cert: true,
    ssl_mode: Some("require".to_string()),
    ca_cert_path: Some("/etc/ssl/certs/ca.pem".to_string()),
    ..Default::default()
};

// Environment variable support (recommended for production)
// Set: POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE
// Set: POSTGRES_SSL_MODE=require, POSTGRES_SSL_CA=/path/to/ca.pem
let config = DatabaseConfig {
    connection_string: "".to_string(), // Will auto-load from environment
    load_credentials_from_env: true,
    ssl_config: Some(production_ssl),
    ..Default::default()
};
```

### CLI Options

| Flag | Description | Example |
|------|-------------|---------|
| `--database` | Database connection string | `--database "postgres://user:pass@host/db"` |
| `--query` | SQL query or table name | `--query "SELECT * FROM users"` |
| `--batch-size` | Streaming batch size | `--batch-size 50000` |
| `--output` | Output format | `--output json` |

### Environment Variables for Production

| Variable | Description | Example |
|----------|-------------|---------|
| `POSTGRES_HOST` | PostgreSQL host | `prod-db.company.com` |
| `POSTGRES_USER` | PostgreSQL username | `readonly_user` |
| `POSTGRES_PASSWORD` | PostgreSQL password | `secure_password123` |
| `POSTGRES_DATABASE` | PostgreSQL database | `analytics` |
| `POSTGRES_PORT` | PostgreSQL port | `5432` |
| `POSTGRES_SSL_MODE` | SSL requirement | `require` |
| `POSTGRES_SSL_CA` | CA certificate path | `/etc/ssl/certs/ca.pem` |
| `MYSQL_HOST` | MySQL host | `mysql.internal` |
| `MYSQL_USER` | MySQL username | `app_user` |
| `MYSQL_PASSWORD` | MySQL password | `mysql_pass` |
| `DATABASE_URL` | Generic database URL | `postgresql://user:pass@host/db` |

## Error Handling & Troubleshooting

### ✅ Automatic Retry Logic

The enhanced database connector automatically retries failed connections:

```
RETRY INFO: Retryable database error in 'connect' (attempt 1/3), retrying in 127ms: Connection refused
RETRY INFO: Retryable database error in 'connect' (attempt 2/3), retrying in 234ms: Connection timeout
✅ Connection successful on attempt 3
```

### Common Issues and Solutions

#### 🔌 Connection Errors
```
Failed to connect to PostgreSQL: connection refused
```
**Enhanced Solutions:**
- ✅ **Automatic retry** with exponential backoff (3 attempts by default)
- ✅ **Environment variables** for secure credential management
- ✅ **SSL/TLS validation** with helpful error messages
- Check database is running and accessible
- Verify firewall rules and network connectivity

#### 🔍 Query Errors
```
Query execution failed: table "users" does not exist
```
**Solutions:**
- Verify table/view exists with `SHOW TABLES` or `\dt`
- Check schema permissions with `SHOW GRANTS`
- Use fully qualified table names (`schema.table`)

#### 💾 Large Dataset Handling
```
Processing large table with 50M rows - using automatic sampling
SAMPLING INFO: Representative sample (2.0%) selected for analysis
📊 Sample quality: Representative ✅, Confidence: ±0.02
```
**Automatic Solutions:**
- ✅ **Intelligent sampling** for tables > 1M rows
- ✅ **Streaming processing** with configurable batch sizes
- ✅ **Memory monitoring** and automatic optimization
- ✅ **Progress tracking** with ETA estimates

#### 🔒 Security Warnings
```
SECURITY WARNING: Password embedded in connection string. Consider using environment variables.
SECURITY WARNING: No SSL/TLS configuration detected. Database traffic may be unencrypted.
```
**Production-Ready Solutions:**
- ✅ **Environment variable** credential loading
- ✅ **SSL/TLS encryption** with certificate validation
- ✅ **Connection string masking** in logs
- ✅ **Security validation** with actionable warnings

## Performance Tips

1. **Use appropriate batch sizes**
   - Small datasets: 1,000-5,000 rows
   - Medium datasets: 10,000-25,000 rows
   - Large datasets: 50,000-100,000 rows

2. **Optimize queries**
   - Add WHERE clauses to filter data
   - Use indexes for better performance
   - Avoid SELECT * on wide tables

3. **Connection pooling**
   - Set appropriate `max_connections`
   - Reuse connections when possible

4. **Use columnar formats**
   - DuckDB for analytical workloads
   - PostgreSQL for transactional data

## Integration Examples

### Data Pipeline
```rust
use dataprof::{DatabaseConfig, analyze_database};

async fn profile_daily_data() -> anyhow::Result<()> {
    let databases = vec![
        ("Production", "postgresql://user:pass@prod-db/app"),
        ("Analytics", "analytics.duckdb"),
        ("Cache", "redis://localhost:6379"),
    ];

    for (name, conn_str) in databases {
        let config = DatabaseConfig {
            connection_string: conn_str.to_string(),
            batch_size: 25000,
            ..Default::default()
        };

        let report = analyze_database(config, "daily_metrics").await?;
        println!("{}: {} rows profiled", name, report.scan_info.rows_scanned);
    }

    Ok(())
}
```

### Quality Monitoring
```rust
use dataprof::{DatabaseConfig, analyze_database, ErrorSeverity};

async fn monitor_data_quality() -> anyhow::Result<()> {
    let config = DatabaseConfig {
        connection_string: "postgresql://monitor@db/quality".to_string(),
        batch_size: 10000,
        ..Default::default()
    };

    let report = analyze_database(config, "
        SELECT * FROM customer_data
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
    ").await?;

    let critical_issues: Vec<_> = report.issues
        .iter()
        .filter(|issue| issue.severity == ErrorSeverity::Critical)
        .collect();

    if !critical_issues.is_empty() {
        eprintln!("⚠️  Found {} critical data quality issues!", critical_issues.len());
        for issue in critical_issues {
            eprintln!("  - {}: {}", issue.column, issue.description);
        }
    }

    Ok(())
}
```

## Troubleshooting

### Enable Debug Logging
```bash
RUST_LOG=debug dataprof --database "postgres://..." --query "..."
```

### Check Feature Compilation
```bash
# Verify database features are enabled
cargo check --features database,postgres,mysql,sqlite,duckdb
```

### Test Connection
```rust
use dataprof::{DatabaseConfig, create_connector};

async fn test_connection() -> anyhow::Result<()> {
    let config = DatabaseConfig {
        connection_string: "your-connection-string".to_string(),
        ..Default::default()
    };

    let mut connector = create_connector(config)?;
    connector.connect().await?;
    let is_connected = connector.test_connection().await?;

    println!("Connection test: {}", if is_connected { "SUCCESS" } else { "FAILED" });

    Ok(())
}
```
