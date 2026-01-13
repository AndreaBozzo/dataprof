//! Advanced database testing with detailed profiling
//!
//! This demonstrates dataprof's advanced database features
//!
//! Run with: cargo run --example advanced_database_test --features "database,postgres,mysql"

#[cfg(not(feature = "database"))]
fn main() {
    eprintln!("This example requires the 'database' feature.");
    eprintln!(
        "Run with: cargo run --example advanced_database_test --features \"database,postgres,mysql\""
    );
    std::process::exit(1);
}

#[cfg(feature = "database")]
use anyhow::Result;
#[cfg(feature = "database")]
use dataprof::{DatabaseConfig, profile_database};

#[cfg(feature = "database")]
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 DataProf Advanced Database Testing");
    println!("======================================\n");

    test_postgresql_advanced().await?;
    test_mysql_advanced().await?;

    Ok(())
}

#[cfg(feature = "database")]
async fn test_postgresql_advanced() -> Result<()> {
    println!("📊 PostgreSQL Advanced Profiling");
    println!("----------------------------------");

    let config = DatabaseConfig {
        connection_string: "postgresql://dataprof:dev_password_123@localhost:5432/dataprof_test"
            .to_string(),
        batch_size: 1000,
        max_connections: Some(2),
        connection_timeout: Some(std::time::Duration::from_secs(10)),
        retry_config: Some(dataprof::database::RetryConfig::default()),
        sampling_config: None,
        ssl_config: None,
        load_credentials_from_env: false,
    };

    let report = profile_database(config, "SELECT * FROM test_users").await?;

    println!("\n📈 Overall Statistics:");
    println!("  Total Rows: {}", report.file_info.total_rows.unwrap_or(0));
    println!("  Total Columns: {}", report.file_info.total_columns);
    println!("  Quality Score: {:.2}%", report.quality_score());

    println!("\n📋 Column Analysis:");
    for col in &report.column_profiles {
        let non_null = col.total_count - col.null_count;
        let null_pct = (col.null_count as f64 / col.total_count as f64) * 100.0;

        println!("\n  Column: {}", col.name);
        println!("    Type: {:?}", col.data_type);
        println!("    Non-null: {}/{}", non_null, col.total_count);
        println!("    Null %: {:.1}%", null_pct);
        if let Some(unique) = col.unique_count {
            println!("    Unique: {}", unique);
        }

        // Show stats based on column type
        match &col.stats {
            dataprof::types::ColumnStats::Numeric { min, max, mean, .. } => {
                println!("    📊 Numeric Stats:");
                println!("       Min: {:.2}", min);
                println!("       Max: {:.2}", max);
                println!("       Mean: {:.2}", mean);
            }
            dataprof::types::ColumnStats::Text {
                min_length,
                max_length,
                avg_length,
                ..
            } => {
                println!("    📝 Text Stats:");
                println!(
                    "       Length: {} - {} (avg: {:.1})",
                    min_length, max_length, avg_length
                );
            }
            _ => {}
        }

        // Show detected patterns
        if !col.patterns.is_empty() {
            println!("    🔍 Patterns: {:?}", col.patterns);
        }
    }

    println!("\n📊 Data Quality Metrics:");
    println!(
        "  Complete Records: {:.2}%",
        report.data_quality_metrics.complete_records_ratio
    );
    println!(
        "  Missing Values: {:.2}%",
        report.data_quality_metrics.missing_values_ratio
    );
    println!(
        "  Type Consistency: {:.2}%",
        report.data_quality_metrics.data_type_consistency
    );
    println!(
        "  Duplicate Rows: {}",
        report.data_quality_metrics.duplicate_rows
    );
    println!(
        "  Outlier Ratio: {:.2}%",
        report.data_quality_metrics.outlier_ratio
    );

    println!("\n✅ PostgreSQL advanced profiling complete!\n");
    Ok(())
}

#[cfg(feature = "database")]
async fn test_mysql_advanced() -> Result<()> {
    println!("📊 MySQL Advanced Profiling");
    println!("----------------------------");

    let config = DatabaseConfig {
        connection_string: "mysql://dataprof:dev_password_123@127.0.0.1:3306/dataprof_test"
            .to_string(),
        batch_size: 1000,
        max_connections: Some(2),
        connection_timeout: Some(std::time::Duration::from_secs(10)),
        retry_config: Some(dataprof::database::RetryConfig::default()),
        sampling_config: None,
        ssl_config: None,
        load_credentials_from_env: false,
    };

    let report = profile_database(config, "SELECT * FROM test_products").await?;

    println!("\n📈 Overall Statistics:");
    println!("  Total Rows: {}", report.file_info.total_rows.unwrap_or(0));
    println!("  Total Columns: {}", report.file_info.total_columns);
    println!("  Quality Score: {:.2}%", report.quality_score());

    println!("\n📋 Column Analysis:");
    for col in &report.column_profiles {
        let non_null = col.total_count - col.null_count;
        let null_pct = (col.null_count as f64 / col.total_count as f64) * 100.0;

        println!("\n  Column: {}", col.name);
        println!("    Type: {:?}", col.data_type);
        println!("    Non-null: {}/{}", non_null, col.total_count);
        println!("    Null %: {:.1}%", null_pct);
        if let Some(unique) = col.unique_count {
            println!("    Unique: {}", unique);
        }

        // Show numeric stats if available
        if let dataprof::types::ColumnStats::Numeric { min, max, mean, .. } = &col.stats {
            println!("    Range: {:.2} to {:.2}, Mean: {:.2}", min, max, mean);
        }
    }

    println!("\n📊 Data Quality Metrics:");
    println!(
        "  Complete Records: {:.2}%",
        report.data_quality_metrics.complete_records_ratio
    );
    println!(
        "  Missing Values: {:.2}%",
        report.data_quality_metrics.missing_values_ratio
    );
    println!(
        "  Type Consistency: {:.2}%",
        report.data_quality_metrics.data_type_consistency
    );
    println!(
        "  Duplicate Rows: {}",
        report.data_quality_metrics.duplicate_rows
    );

    println!("\n✅ MySQL advanced profiling complete!\n");
    Ok(())
}
