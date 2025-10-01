use anyhow::Result;
use clap::Args;
use colored::*;
use std::path::PathBuf;

use super::script_generator::generate_database_preprocessing_script;
// TODO: Refactor this command to use output_with_adaptive_formatter instead
#[allow(deprecated)]
use dataprof::output::{display_data_quality_metrics, display_ml_score, display_quality_issues};
use dataprof::{generate_html_report, profile_database_with_ml, ColumnProfile, DatabaseConfig};

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

    /// Generate ML readiness score
    #[arg(long)]
    pub ml_score: bool,

    /// Show ML code snippets
    #[arg(long)]
    pub ml_code: bool,

    /// Generate preprocessing script
    #[arg(long)]
    pub output_script: Option<PathBuf>,

    /// Generate HTML report
    #[arg(long)]
    pub html: Option<PathBuf>,
}

/// Execute the database analysis command
#[allow(deprecated)]
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

// Helper function to display ML score with code snippets (reused from analyze.rs)
#[allow(deprecated)]
fn display_ml_score_with_code(ml_score: &dataprof::analysis::MlReadinessScore) {
    use colored::*;

    // First display the standard ML score
    display_ml_score(ml_score);

    // Then show code snippets for recommendations
    if !ml_score.recommendations.is_empty() {
        println!(
            "🐍 {} {}:",
            "Code Snippets".bright_blue().bold(),
            format!("({} recommendations)", ml_score.recommendations.len()).dimmed()
        );
        println!();

        for (i, rec) in ml_score.recommendations.iter().enumerate() {
            if let Some(code) = &rec.code_snippet {
                let priority_color = match rec.priority {
                    dataprof::analysis::RecommendationPriority::Critical => "red",
                    dataprof::analysis::RecommendationPriority::High => "yellow",
                    dataprof::analysis::RecommendationPriority::Medium => "blue",
                    dataprof::analysis::RecommendationPriority::Low => "green",
                };

                println!(
                    "{}. {} {} - {}",
                    (i + 1).to_string().bright_white(),
                    rec.category.color(priority_color).bold(),
                    format!(
                        "[{}]",
                        match rec.priority {
                            dataprof::analysis::RecommendationPriority::Critical => "critical",
                            dataprof::analysis::RecommendationPriority::High => "high",
                            dataprof::analysis::RecommendationPriority::Medium => "medium",
                            dataprof::analysis::RecommendationPriority::Low => "low",
                        }
                    )
                    .color(priority_color),
                    rec.description.dimmed()
                );

                if let Some(framework) = &rec.framework {
                    println!("   📦 Framework: {}", framework.bright_cyan());
                }

                if !rec.imports.is_empty() {
                    println!("   📥 Imports: {}", rec.imports.join(", ").bright_green());
                }

                println!("   💻 Code:");
                // Display code with proper indentation and syntax highlighting
                for line in code.lines() {
                    if line.trim().starts_with('#') {
                        println!("   {}", line.bright_black());
                    } else {
                        println!("   {}", line);
                    }
                }
                println!();
            }
        }
    }

    // Show summary of actionable items
    let code_snippets_count = ml_score
        .recommendations
        .iter()
        .filter(|r| r.code_snippet.is_some())
        .count();

    if code_snippets_count > 0 {
        println!(
            "💡 {} Ready to implement {} actionable code snippets!",
            "Tip:".bright_yellow().bold(),
            code_snippets_count.to_string().bright_white().bold()
        );
        println!(
            "   Use {} to generate a complete preprocessing script.",
            "--output-script preprocess.py".bright_cyan()
        );
        println!();
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
        enable_ml_readiness: args.ml_score,
        ssl_config: Some(dataprof::database::SslConfig::default()),
        load_credentials_from_env: true,
    };

    // Run async analysis
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create async runtime: {}", e))?;

    let (report, ml_score) =
        rt.block_on(async { profile_database_with_ml(config, &query).await })?;

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
        // Show quality issues
        display_quality_issues(&report.issues);

        // Show comprehensive data quality metrics
        if let Some(metrics) = &report.data_quality_metrics {
            display_data_quality_metrics(metrics);
        }
    }

    // Show ML readiness score if requested
    if args.ml_score {
        if let Some(ml_readiness_score) = &ml_score {
            if args.ml_code {
                display_ml_score_with_code(ml_readiness_score);
            } else {
                display_ml_score(ml_readiness_score);
            }

            // Generate preprocessing script if requested
            if let Some(script_path) = &args.output_script {
                match generate_database_preprocessing_script(
                    ml_readiness_score,
                    script_path,
                    connection_string,
                    &query,
                ) {
                    Ok(_) => {
                        println!(
                            "🐍 Preprocessing script saved to: {}",
                            script_path.display().to_string().bright_green()
                        );
                        println!("   Ready to use with: python {}", script_path.display());
                        println!();
                    }
                    Err(e) => {
                        eprintln!("❌ Failed to generate preprocessing script: {}", e);
                    }
                }
            }

            println!();
        }
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
                    eprintln!("❌ Failed to generate HTML report: {}", e);
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
