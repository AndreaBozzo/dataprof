//! Database connector examples for DataProfiler
//!
//! This file demonstrates how to use DataProfiler with various database systems.
//! Run examples with: cargo run --example database_examples --features database,postgres

use anyhow::Result;
#[cfg(feature = "database")]
use dataprof::{profile_database, DatabaseConfig};

/// Example: Profile a PostgreSQL table
#[cfg(feature = "database")]
#[allow(dead_code)]
async fn postgresql_example() -> Result<()> {
    println!("🐘 PostgreSQL Example");

    let config = DatabaseConfig {
        connection_string: "postgresql://user:password@localhost:5432/mydb".to_string(),
        batch_size: 10000,
        max_connections: Some(5),
        connection_timeout: Some(std::time::Duration::from_secs(30)),
    };

    // Profile entire table
    let report = profile_database(config.clone(), "users").await?;

    println!(
        "✅ Profiled {} rows across {} columns",
        report.file_info.total_rows.unwrap_or(0),
        report.file_info.total_columns
    );

    // Profile with custom query
    let report = profile_database(
        config,
        "
        SELECT user_id, email, created_at, last_login
        FROM users
        WHERE created_at >= '2024-01-01'
    ",
    )
    .await?;

    println!("📊 Found {} quality issues", report.issues.len());

    Ok(())
}

/// Example: Analyze MySQL e-commerce data
#[cfg(feature = "database")]
#[allow(dead_code)]
async fn mysql_example() -> Result<()> {
    println!("🐬 MySQL Example");

    let config = DatabaseConfig {
        connection_string: "mysql://root:password@localhost:3306/ecommerce".to_string(),
        batch_size: 25000,
        ..Default::default()
    };

    // Complex query with joins
    let report = profile_database(
        config,
        "
        SELECT
            o.order_id,
            o.total_amount,
            o.order_date,
            c.customer_id,
            c.email,
            COUNT(oi.item_id) as item_count
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        JOIN order_items oi ON o.order_id = oi.order_id
        WHERE o.order_date >= '2024-01-01'
        GROUP BY o.order_id, o.total_amount, o.order_date, c.customer_id, c.email
    ",
    )
    .await?;

    // Analyze the results
    for profile in &report.column_profiles {
        let null_percentage = if profile.total_count > 0 {
            (profile.null_count as f64 / profile.total_count as f64) * 100.0
        } else {
            0.0
        };
        println!(
            "📈 Column '{}': {:?} ({:.1}% null)",
            profile.name, profile.data_type, null_percentage
        );
    }

    Ok(())
}

/// Example: SQLite local database analysis
#[cfg(feature = "database")]
#[allow(dead_code)]
async fn sqlite_example() -> Result<()> {
    println!("🗃️ SQLite Example");

    // File-based SQLite
    let config = DatabaseConfig {
        connection_string: "data.db".to_string(),
        batch_size: 5000,
        ..Default::default()
    };

    let report = profile_database(config, "products").await?;

    println!("🔍 SQLite Analysis Complete");
    println!("   Rows: {}", report.scan_info.rows_scanned);
    println!("   Scan Time: {}ms", report.scan_info.scan_time_ms);

    // In-memory SQLite for testing
    let _memory_config = DatabaseConfig {
        connection_string: ":memory:".to_string(),
        ..Default::default()
    };

    // Note: In-memory DB would need to be populated first
    println!("💾 In-memory SQLite configured (populate data first)");

    Ok(())
}

/// Example: DuckDB analytical workload
#[cfg(feature = "database")]
#[allow(dead_code)]
async fn duckdb_example() -> Result<()> {
    println!("🦆 DuckDB Example");

    let config = DatabaseConfig {
        connection_string: "analytics.duckdb".to_string(),
        batch_size: 50000, // Larger batches for analytical workloads
        ..Default::default()
    };

    // Analytical query with aggregations
    let report = profile_database(
        config,
        "
        SELECT
            DATE_TRUNC('month', sale_date) as month,
            product_category,
            AVG(sale_amount) as avg_sale,
            SUM(sale_amount) as total_sales,
            COUNT(*) as transaction_count
        FROM sales_data
        WHERE sale_date >= '2023-01-01'
        GROUP BY DATE_TRUNC('month', sale_date), product_category
        ORDER BY month, product_category
    ",
    )
    .await?;

    println!("📈 DuckDB Analysis:");
    println!("   Processing Time: {}ms", report.scan_info.scan_time_ms);

    // Quality analysis
    let critical_issues = report
        .issues
        .iter()
        .filter(|issue| matches!(issue.severity(), dataprof::types::Severity::High))
        .count();

    if critical_issues > 0 {
        println!(
            "⚠️  {} high severity data quality issues found!",
            critical_issues
        );
    }

    Ok(())
}

/// Example: Streaming large dataset
#[cfg(feature = "database")]
#[allow(dead_code)]
async fn streaming_example() -> Result<()> {
    println!("🌊 Streaming Example - Large Dataset");

    let config = DatabaseConfig {
        connection_string: "postgresql://readonly@warehouse:5432/bigdata".to_string(),
        batch_size: 100000,       // Large batches for efficiency
        max_connections: Some(3), // Conservative connection usage
        connection_timeout: Some(std::time::Duration::from_secs(60)),
    };

    // Query that returns millions of rows
    let start_time = std::time::Instant::now();

    let report = profile_database(
        config,
        "
        SELECT
            user_id,
            event_type,
            timestamp,
            properties,
            session_id
        FROM user_events
        WHERE timestamp >= '2024-01-01'
          AND timestamp < '2024-02-01'
    ",
    )
    .await?;

    let duration = start_time.elapsed();

    println!("📊 Streaming Results:");
    println!("   Total Rows: {}", report.scan_info.rows_scanned);
    println!("   Total Time: {:.2}s", duration.as_secs_f64());
    println!(
        "   Throughput: {:.0} rows/sec",
        report.scan_info.rows_scanned as f64 / duration.as_secs_f64()
    );

    Ok(())
}

/// Example: Quality monitoring pipeline
#[cfg(feature = "database")]
#[allow(dead_code)]
async fn quality_monitoring_example() -> Result<()> {
    println!("🔍 Data Quality Monitoring");

    let databases = vec![
        ("Production DB", "postgresql://monitor@prod-db:5432/app"),
        ("Data Lake", "analytics.duckdb"),
        ("Cache DB", "cache.db"),
    ];

    for (name, conn_str) in databases {
        let config = DatabaseConfig {
            connection_string: conn_str.to_string(),
            batch_size: 10000,
            ..Default::default()
        };

        // Skip if connection fails (for demo purposes)
        match profile_database(config, "SELECT 1 as health_check").await {
            Ok(report) => {
                println!("✅ {}: {} rows", name, report.scan_info.rows_scanned);

                let quality_score = report.quality_score().unwrap_or(0.0);
                if quality_score < 80.0 {
                    println!("   ⚠️  Quality Score: {:.1}%", quality_score);
                }
            }
            Err(e) => {
                println!("❌ {}: Connection failed - {}", name, e);
            }
        }
    }

    Ok(())
}

/// Main function to run all examples
#[cfg(feature = "database")]
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 DataProfiler Database Examples\n");

    // Note: These examples assume databases are set up and accessible
    // In real usage, you would run only the examples relevant to your setup

    println!("Note: Examples require corresponding databases to be set up");
    println!("Modify connection strings to match your environment\n");

    // Uncomment the examples you want to run:

    // postgresql_example().await?;
    // mysql_example().await?;
    // sqlite_example().await?;
    // duckdb_example().await?;
    // streaming_example().await?;
    // quality_monitoring_example().await?;

    println!("✅ All examples completed!");

    Ok(())
}

/// Fallback main function when database features are not enabled
#[cfg(not(feature = "database"))]
fn main() -> Result<()> {
    println!("🚀 DataProfiler Database Examples");
    println!("📝 To run these examples, enable database features:");
    println!("   cargo run --example database_examples --features database,postgres");
    println!("   cargo run --example database_examples --features database,mysql");
    println!("   cargo run --example database_examples --features database,sqlite");
    println!("   cargo run --example database_examples --features database,duckdb");
    println!("   cargo run --example database_examples --features database,all");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "database")]
    #[tokio::test]
    async fn test_connection_string_parsing() {
        use dataprof::database::connection::ConnectionInfo;

        // Test PostgreSQL parsing
        let pg_info = ConnectionInfo::parse("postgresql://user:pass@localhost:5432/testdb")
            .expect("Failed to parse PostgreSQL connection");
        assert_eq!(pg_info.database_type(), "postgresql");
        assert_eq!(pg_info.host, Some("localhost".to_string()));
        assert_eq!(pg_info.port, Some(5432));

        // Test MySQL parsing
        let mysql_info = ConnectionInfo::parse("mysql://root:secret@127.0.0.1:3306/myapp")
            .expect("Failed to parse MySQL connection");
        assert_eq!(mysql_info.database_type(), "mysql");
        assert_eq!(mysql_info.username, Some("root".to_string()));

        // Test SQLite file
        let sqlite_info =
            ConnectionInfo::parse("data.db").expect("Failed to parse SQLite connection");
        assert_eq!(sqlite_info.database_type(), "sqlite");
        assert_eq!(sqlite_info.path, Some("data.db".to_string()));

        // Test DuckDB file
        let duckdb_info =
            ConnectionInfo::parse("analytics.duckdb").expect("Failed to parse DuckDB connection");
        assert_eq!(duckdb_info.database_type(), "duckdb");
    }
}
