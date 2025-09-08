# Database Connectors Guide

This guide explains how to use DataProfiler with various database systems for direct data profiling.

## Supported Databases

- **PostgreSQL** - Production databases with connection pooling
- **MySQL/MariaDB** - MySQL-compatible databases
- **SQLite** - Embedded databases and file-based storage
- **DuckDB** - Analytical databases and data warehousing

## Quick Start

### Command Line Usage

```bash
# PostgreSQL
dataprof --database "postgresql://user:password@localhost:5432/mydb" --query "users"

# MySQL
dataprof --database "mysql://root:password@localhost:3306/mydb" --query "SELECT * FROM orders"

# SQLite file
dataprof --database "data.db" --query "products"

# DuckDB file
dataprof --database "analytics.duckdb" --query "SELECT * FROM sales WHERE date > '2024-01-01'"

# In-memory SQLite
dataprof --database ":memory:" --query "temp_data"
```

### Programmatic Usage

```rust
use dataprof::{DatabaseConfig, profile_database};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = DatabaseConfig {
        connection_string: "postgresql://user:pass@localhost:5432/mydb".to_string(),
        batch_size: 10000,
        max_connections: Some(5),
        connection_timeout: Some(std::time::Duration::from_secs(30)),
    };

    let report = profile_database(config, "SELECT * FROM large_table").await?;

    println!("Processed {} rows across {} columns",
        report.file_info.total_rows.unwrap_or(0),
        report.file_info.total_columns
    );

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

## Configuration Options

### DatabaseConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `connection_string` | String | Required | Database connection URL |
| `batch_size` | usize | 10000 | Rows per batch for streaming |
| `max_connections` | Option<u32> | Some(10) | Connection pool size |
| `connection_timeout` | Option<Duration> | Some(30s) | Connection timeout |

### CLI Options

| Flag | Description | Example |
|------|-------------|---------|
| `--database` | Database connection string | `--database "postgres://user:pass@host/db"` |
| `--query` | SQL query or table name | `--query "SELECT * FROM users"` |
| `--batch-size` | Streaming batch size | `--batch-size 50000` |
| `--output` | Output format | `--output json` |

## Error Handling

Common issues and solutions:

### Connection Errors
```
Failed to connect to PostgreSQL: connection refused
```
**Solutions:**
- Check database is running
- Verify connection string credentials
- Check network connectivity
- Ensure database accepts connections from your IP

### Query Errors
```
Query execution failed: table "users" does not exist
```
**Solutions:**
- Verify table/view exists
- Check schema permissions
- Use fully qualified table names (`schema.table`)

### Memory Issues
```
Out of memory processing large result set
```
**Solutions:**
- Reduce batch size: `--batch-size 5000`
- Add LIMIT clause to query
- Use streaming features (automatic)

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
use dataprof::{DatabaseConfig, profile_database};

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

        let report = profile_database(config, "daily_metrics").await?;
        println!("{}: {} rows profiled", name, report.scan_info.rows_scanned);
    }

    Ok(())
}
```

### Quality Monitoring
```rust
use dataprof::{DatabaseConfig, profile_database, ErrorSeverity};

async fn monitor_data_quality() -> anyhow::Result<()> {
    let config = DatabaseConfig {
        connection_string: "postgresql://monitor@db/quality".to_string(),
        batch_size: 10000,
        ..Default::default()
    };

    let report = profile_database(config, "
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
