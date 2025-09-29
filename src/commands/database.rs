use anyhow::Result;
use colored::*;

use crate::cli::args::Cli;
use crate::commands::script_generator::generate_database_preprocessing_script;
use dataprof::output::{
    display_data_quality_metrics, display_ml_score, display_profile, display_quality_issues,
};
use dataprof::{generate_html_report, profile_database_with_ml, DatabaseConfig};
use std::fs;

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

    // Show ML readiness score if requested (simplified display for database)
    if cli.ml_score {
        if let Some(ml_readiness_score) = &ml_score {
            println!("\n🤖 ML READINESS SCORE Machine Learning Readiness");
            let assessment = if ml_readiness_score.overall_score >= 80.0 {
                "✅ Excellent"
            } else if ml_readiness_score.overall_score >= 60.0 {
                "✅ Good"
            } else if ml_readiness_score.overall_score >= 40.0 {
                "⚠️ Fair"
            } else {
                "❌ Poor"
            };
            println!(
                "  {} Overall Score: {:.1}% {}",
                if ml_readiness_score.overall_score >= 60.0 {
                    "✅"
                } else {
                    "⚠️"
                },
                ml_readiness_score.overall_score,
                assessment
            );

            if !ml_readiness_score.issues.is_empty() {
                println!("\n🚫 Blocking Issues:");
                for issue in &ml_readiness_score.issues {
                    println!("  • {}", issue);
                }
            }

            if !ml_readiness_score.recommendations.is_empty() {
                println!("\n💡 Recommendations:");
                for rec in &ml_readiness_score.recommendations {
                    println!("  🟠 {}", rec);
                }
            }

            // Show code snippets if --ml-code is enabled
            if cli.ml_code && !ml_readiness_score.recommendations.is_empty() {
                println!(
                    "🐍 {} ({} recommendations):",
                    "Code Snippets".bright_blue().bold(),
                    ml_readiness_score.recommendations.len()
                );
                println!();

                for (i, rec) in ml_readiness_score.recommendations.iter().enumerate() {
                    println!(
                        "{}. {} [{}] - {}",
                        i + 1,
                        "Database Preprocessing".bright_yellow(),
                        "high".color("red"),
                        rec
                    );

                    // Generate appropriate code snippet based on recommendation
                    let code_snippet = if rec.contains("dimensionality reduction") {
                        r#"   📦 Framework: pandas + scikit-learn
   📥 Imports: from sklearn.decomposition import PCA
   💻 Code:
   # Apply PCA for dimensionality reduction
   from sklearn.decomposition import PCA
   pca = PCA(n_components=min(n_samples//2, 50))
   X_reduced = pca.fit_transform(X_scaled)
   print(f"Reduced from {X.shape[1]} to {X_reduced.shape[1]} features")"#
                    } else if rec.contains("small dataset") {
                        r#"   📦 Framework: pandas
   📥 Imports: import pandas as pd
   💻 Code:
   # Handle small dataset - use cross-validation and simpler models
   from sklearn.model_selection import cross_val_score, LeaveOneOut
   from sklearn.linear_model import LogisticRegression

   # Use Leave-One-Out CV for small datasets
   loo = LeaveOneOut()
   model = LogisticRegression(penalty='l2', C=1.0)
   scores = cross_val_score(model, X, y, cv=loo)
   print(f"Mean CV Score: {scores.mean():.3f} (+/- {scores.std() * 2:.3f})")"#
                    } else {
                        r#"   📦 Framework: pandas
   📥 Imports: import pandas as pd
   💻 Code:
   # General database preprocessing
   df = pd.read_sql_query("""
       SELECT * FROM your_table
       WHERE your_conditions
   """, connection)

   # Basic preprocessing steps
   df = df.dropna()  # Handle missing values
   df = df.drop_duplicates()  # Remove duplicates"#
                    };

                    println!("{}", code_snippet);
                    println!();
                }

                println!(
                    "💡 Tip: Ready to implement {} actionable code snippets!",
                    ml_readiness_score.recommendations.len()
                );
                println!();
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
