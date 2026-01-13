use anyhow::Result;
use clap::Args;
use colored::*;
use std::path::PathBuf;

use dataprof::output::output_with_adaptive_formatter;
use dataprof::{
    ColumnProfile, DatabaseConfig, OutputFormat, generate_html_report, profile_database,
};

/// Database analysis arguments
#[derive(Debug, Args)]
pub struct DatabaseArgs {
    /// Database connection string (postgres://, mysql://, sqlite://)
    pub connection_string: String,

    /// SQL query to execute (or table name)
    #[arg(long)]
    pub query: Option<String>,

    /// Table name to analyze (alternative to query)
    pub table: Option<String>,

    /// Batch size for streaming
    #[arg(long, default_value = "10000")]
    pub batch_size: usize,

    /// Show quality metrics
    #[arg(long)]
    pub quality: bool,

    /// Generate HTML report
    #[arg(long)]
    pub html: Option<PathBuf>,
}

/// Execute the database analysis command
pub fn execute(args: &DatabaseArgs) -> Result<()> {
    run_database_analysis(args, &args.connection_string)
}

/// Display a single column profile
fn display_column_profile(profile: &ColumnProfile) {
    println!(
        "📊 {} ({})",
        profile.name.bright_cyan().bold(),
        format!("{:?}", profile.data_type).yellow()
    );
    println!(
        "   Total: {} | Nulls: {} ({:.1}%)",
        profile.total_count,
        profile.null_count,
        (profile.null_count as f64 / profile.total_count as f64) * 100.0
    );

    if let Some(unique_count) = profile.unique_count {
        println!(
            "   Distinct: {} | Unique ratio: {:.1}%",
            unique_count,
            (unique_count as f64 / profile.total_count as f64) * 100.0
        );
    }
}

#[allow(deprecated)]
fn run_database_analysis(args: &DatabaseArgs, connection_string: &str) -> Result<()> {
    use tokio;


    println!(
        "{}",
        "🗃️ DataProfiler v0.3.0 - Database Analysis"
            .bright_blue()
            .bold()
    );
    println!();

    // Determine query: use --query flag, --table flag, or the positional table argument
    let query = if let Some(sql_query) = &args.query {
        sql_query.to_string()
    } else if let Some(table_name) = &args.table {
        format!("SELECT * FROM {}", table_name)
    } else {
        return Err(anyhow::anyhow!(
            "Please specify either --query 'SELECT * FROM table' or provide table name as argument"
        ));
    };


    // Create database configuration
    let config = DatabaseConfig {
        connection_string: connection_string.to_string(),
        batch_size: args.batch_size,
        max_connections: Some(10),
        connection_timeout: Some(std::time::Duration::from_secs(30)),
        retry_config: Some(dataprof::database::RetryConfig::default()),
        sampling_config: None,
        ssl_config: Some(dataprof::database::SslConfig::default()),
        load_credentials_from_env: true,
    };

    // Run async analysis
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create async runtime: {}", e))?;

    let report = rt.block_on(async { profile_database(config, &query).await })?;

    println!(
        "🔗 {} | {} columns | {} rows",
        connection_string
            .split('@')
            .next_back()
            .unwrap_or(connection_string),
        report.file_info.total_columns,
        report.file_info.total_rows.unwrap_or(0)
    );


    if report.scan_info.rows_scanned > 0 {
        let scan_time_sec = report.scan_info.scan_time_ms as f64 / 1000.0;
        let rows_per_sec = report.scan_info.rows_scanned as f64 / scan_time_sec;
        println!(
            "⚡ Processed {} rows in {:.1}s ({:.0} rows/sec)",
            report.scan_info.rows_scanned, scan_time_sec, rows_per_sec
        );
    }
    println!();

    if args.quality {
        // Use modern adaptive formatter for quality output
        output_with_adaptive_formatter(&report, Some(OutputFormat::Text))?;
    }

    if args.quality {
        // Generate HTML report if requested
        if let Some(html_path) = &args.html {
            match generate_html_report(&report, html_path) {
                Ok(_) => {
                    println!(
                        "📄 HTML report saved to: {}",
                        html_path.display().to_string().bright_green()
                    );
                    println!();
                }
                Err(e) => {
                    eprintln!("Failed to generate HTML report: {}", e);
                }
            }
        }
    }

    // Show column profiles
    for profile in &report.column_profiles {
        display_column_profile(profile);
        println!();
    }

    Ok(())
}
