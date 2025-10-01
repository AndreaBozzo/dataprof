use anyhow::Result;
use colored::*;

use crate::commands::script_generator::generate_database_preprocessing_script;
use dataprof::output::{
    display_data_quality_metrics, display_ml_score, display_profile, display_quality_issues,
};
use dataprof::{generate_html_report, profile_database_with_ml, DatabaseConfig};

// Helper function to display ML score with code snippets (reused from analyze.rs)
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

#[cfg(feature = "database")]
pub fn run_database_analysis(cli: &Cli, connection_string: &str) -> Result<()> {
    use tokio;

    println!(
        "{}",
        "🗃️ DataProfiler v0.3.0 - Database Analysis"
            .bright_blue()
            .bold()
    );
    println!();

    // Default query or table (use file parameter as table name if no query provided)
    let query = if let Some(sql_query) = cli.query.as_ref() {
        sql_query.to_string()
    } else {
        let table_name = cli.file.display().to_string();
        if table_name.is_empty() || table_name == "." {
            return Err(anyhow::anyhow!("Please specify either --query 'SELECT * FROM table' or provide table name as file argument"));
        }
        format!("SELECT * FROM {}", table_name)
    };

    // Create database configuration
    let config = DatabaseConfig {
        connection_string: connection_string.to_string(),
        batch_size: cli.batch_size,
        max_connections: Some(10),
        connection_timeout: Some(std::time::Duration::from_secs(30)),
        retry_config: Some(dataprof::database::RetryConfig::default()),
        sampling_config: None,
        enable_ml_readiness: true,
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

    if cli.quality {
        // Show quality issues
        display_quality_issues(&report.issues);

        // Show comprehensive data quality metrics
        if let Some(metrics) = &report.data_quality_metrics {
            display_data_quality_metrics(metrics);
        }
    }

    // Show ML readiness score if requested
    if cli.ml_score {
        if let Some(ml_readiness_score) = &ml_score {
            if cli.ml_code {
                display_ml_score_with_code(ml_readiness_score);
            } else {
                display_ml_score(ml_readiness_score);
            }

            // Generate preprocessing script if requested
            if let Some(script_path) = &cli.output_script {
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

    if cli.quality {
        // Generate HTML report if requested
        if let Some(html_path) = &cli.html {
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
        display_profile(profile);
        println!();
    }

    Ok(())
}
