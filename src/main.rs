use anyhow::Result;
use clap::Parser;
use colored::*;
use dataprof::{
    analyze_csv,
    analyze_csv_robust,
    analyze_csv_with_sampling,
    analyze_json,
    analyze_json_with_quality,
    generate_html_report,
    // v0.3.0 imports
    BatchConfig,
    BatchProcessor,
    ChunkSize,
    ColumnProfile,
    ColumnStats,
    DataProfiler,
    DataProfilerError,
    DataType,
    ErrorSeverity,
    ProgressInfo,
    QualityIssue,
    // SamplingStrategy, // Future CLI integration
};
use std::path::{Path, PathBuf};

// Database support (optional)
#[cfg(feature = "database")]
use dataprof::{profile_database, DatabaseConfig};

#[derive(Parser)]
#[command(name = "dataprof")]
#[command(
    about = "Fast CSV data profiler with quality checking - v0.3.5 DB Connectors & Memory Safety Edition"
)]
struct Cli {
    /// File, directory, or glob pattern to analyze (use . for current dir with --glob)
    file: PathBuf,

    /// Enable quality checking (shows data issues)
    #[arg(short, long)]
    quality: bool,

    /// Generate HTML report (requires --quality)
    #[arg(long)]
    html: Option<PathBuf>,

    /// Use streaming engine for large files (v0.3.0)
    #[arg(long)]
    streaming: bool,

    /// Show progress during processing (requires --streaming)
    #[arg(long)]
    progress: bool,

    /// Override chunk size for streaming (default: adaptive)
    #[arg(long)]
    chunk_size: Option<usize>,

    /// Enable sampling for very large datasets
    #[arg(long)]
    sample: Option<usize>,

    /// Process directory recursively
    #[arg(short, long)]
    recursive: bool,

    /// Use glob pattern for file matching (e.g., "data/**/*.csv")
    #[arg(long)]
    glob: Option<String>,

    /// Enable parallel processing for multiple files
    #[arg(long)]
    parallel: bool,

    /// Maximum number of concurrent files to process
    #[arg(long, default_value = "0")]
    max_concurrent: usize,

    /// Database connection string (requires --database feature)
    #[cfg(feature = "database")]
    #[arg(long)]
    database: Option<String>,

    /// SQL query to execute (use with --database)
    #[cfg(feature = "database")]
    #[arg(long)]
    query: Option<String>,

    /// Batch size for database streaming (default: 10000)
    #[cfg(feature = "database")]
    #[arg(long, default_value = "10000")]
    batch_size: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Enhanced error handling wrapper
    if let Err(e) = run_analysis(&cli) {
        handle_error(&e, &cli.file);
        std::process::exit(1);
    }

    Ok(())
}

fn handle_error(error: &anyhow::Error, file_path: &Path) {
    // Check if it's our custom error type
    if let Some(dp_error) = error.downcast_ref::<DataProfilerError>() {
        let severity_icon = match dp_error.severity() {
            ErrorSeverity::Critical => "🔴",
            ErrorSeverity::High => "🟠",
            ErrorSeverity::Medium => "🟡",
            ErrorSeverity::Low => "🔵",
            ErrorSeverity::Info => "ℹ️",
        };

        eprintln!(
            "\n{} {} Error: {}",
            severity_icon,
            dp_error.severity(),
            dp_error
        );
    } else {
        // Handle generic errors with enhanced suggestions
        let error_str = error.to_string();

        if error_str.contains("No such file") || error_str.contains("not found") {
            let file_error = DataProfilerError::file_not_found(file_path.display().to_string());
            eprintln!("\n🔴 CRITICAL Error: {}", file_error);

            // Provide additional context
            if let Some(parent) = file_path.parent() {
                eprintln!("📂 Looking in directory: {}", parent.display());
            }

            // Suggest similar files
            if let Ok(entries) =
                std::fs::read_dir(file_path.parent().unwrap_or(std::path::Path::new(".")))
            {
                let similar_files: Vec<_> = entries
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| {
                        if let Some(ext) = entry.path().extension() {
                            matches!(ext.to_str(), Some("csv") | Some("json") | Some("jsonl"))
                        } else {
                            false
                        }
                    })
                    .take(3)
                    .collect();

                if !similar_files.is_empty() {
                    eprintln!("🔍 Similar files found:");
                    for entry in similar_files {
                        eprintln!("   • {}", entry.path().display());
                    }
                }
            }
        } else if error_str.contains("CSV") {
            let csv_error =
                DataProfilerError::csv_parsing(&error_str, &file_path.display().to_string());
            eprintln!("\n🟠 HIGH Error: {}", csv_error);
        } else {
            eprintln!("\n❌ Error: {}", error);
            eprintln!("💡 For help, run: {} --help", "dataprof".bright_blue());
        }
    }
}

fn run_analysis(cli: &Cli) -> Result<()> {
    // Check for database mode first
    #[cfg(feature = "database")]
    if let Some(connection_string) = &cli.database {
        return run_database_analysis(cli, connection_string);
    }

    // Check for batch processing modes
    if let Some(glob_pattern) = &cli.glob {
        return run_batch_glob(cli, glob_pattern);
    }

    if cli.file.is_dir() {
        return run_batch_directory(cli);
    }

    // Single file processing (existing logic)
    let version_info = if cli.streaming {
        "📊 DataProfiler v0.3.0 - Streaming Analysis"
            .bright_blue()
            .bold()
    } else {
        "📊 DataProfiler - Standard Analysis".bright_blue().bold()
    };

    println!("{}", version_info);
    println!();

    if cli.quality {
        // Generate HTML report if requested
        if let Some(html_path) = &cli.html {
            if html_path.extension().and_then(|s| s.to_str()) != Some("html") {
                eprintln!("❌ HTML output file must have .html extension");
                std::process::exit(1);
            }
        }

        // Use advanced analysis with quality checking
        let report = if cli.streaming && !is_json_file(&cli.file) {
            // v0.3.0 Streaming API
            let mut profiler = DataProfiler::streaming();

            // Configure chunk size
            let chunk_size = if let Some(size) = cli.chunk_size {
                ChunkSize::Fixed(size)
            } else {
                ChunkSize::Adaptive
            };
            profiler = profiler.chunk_size(chunk_size);

            // Configure progress callback if requested
            if cli.progress {
                profiler = profiler.progress_callback(|progress: ProgressInfo| {
                    print!(
                        "\r🔄 Processing: {:.1}% ({} rows, {:.1} rows/sec)",
                        progress.percentage, progress.rows_processed, progress.processing_speed
                    );
                    let _ = std::io::Write::flush(&mut std::io::stdout());
                });
            }

            let result = profiler.analyze_file(&cli.file)?;

            // Clear progress line if it was shown
            if cli.progress {
                println!(); // New line after progress
            }

            result
        } else {
            // Legacy analysis
            if is_json_file(&cli.file) {
                analyze_json_with_quality(&cli.file)?
            } else {
                // Try sampling first, fallback to robust parsing
                match analyze_csv_with_sampling(&cli.file) {
                    Ok(report) => report,
                    Err(e) => {
                        eprintln!(
                            "⚠️ Standard analysis failed: {}. Using robust parsing...",
                            e
                        );
                        analyze_csv_robust(&cli.file)?
                    }
                }
            }
        };

        // Show basic file info
        println!(
            "📁 {} | {:.1} MB | {} columns",
            cli.file.display(),
            report.file_info.file_size_mb,
            report.file_info.total_columns
        );

        if report.scan_info.sampling_ratio < 1.0 {
            println!(
                "📊 Sampled {} rows ({:.1}%)",
                report.scan_info.rows_scanned,
                report.scan_info.sampling_ratio * 100.0
            );
        }
        println!();

        // Show quality issues first
        display_quality_issues(&report.issues);

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

        // Then show column profiles
        for profile in report.column_profiles {
            display_profile(&profile);
            println!();
        }
    } else {
        // Check if HTML output is requested without quality mode
        if cli.html.is_some() {
            eprintln!(
                "❌ HTML report requires --quality flag. Use: {} --quality --html report.html",
                "dataprof".bright_blue()
            );
            std::process::exit(1);
        }

        // Use simple analysis (backwards compatible)
        let profiles = if is_json_file(&cli.file) {
            analyze_json(&cli.file)?
        } else {
            analyze_csv(&cli.file)?
        };

        // Display results
        for profile in profiles {
            display_profile(&profile);
            println!();
        }
    }

    Ok(())
}

fn display_profile(profile: &ColumnProfile) {
    println!(
        "{} {}",
        "Column:".bright_yellow(),
        profile.name.bright_white().bold()
    );

    let type_str = match profile.data_type {
        DataType::String => "String".green(),
        DataType::Integer => "Integer".blue(),
        DataType::Float => "Float".cyan(),
        DataType::Date => "Date".magenta(),
    };
    println!("  Type: {}", type_str);

    println!("  Records: {}", profile.total_count);

    if profile.null_count > 0 {
        let pct = (profile.null_count as f64 / profile.total_count as f64) * 100.0;
        println!(
            "  Nulls: {} ({:.1}%)",
            profile.null_count.to_string().red(),
            pct
        );
    } else {
        println!("  Nulls: {}", "0".green());
    }

    match &profile.stats {
        ColumnStats::Numeric { min, max, mean } => {
            println!("  Min: {:.2}", min);
            println!("  Max: {:.2}", max);
            println!("  Mean: {:.2}", mean);
        }
        ColumnStats::Text {
            min_length,
            max_length,
            avg_length,
        } => {
            println!("  Min Length: {}", min_length);
            println!("  Max Length: {}", max_length);
            println!("  Avg Length: {:.1}", avg_length);
        }
    }

    // Show detected patterns
    if !profile.patterns.is_empty() {
        println!("  {}", "Patterns:".bright_cyan());
        for pattern in &profile.patterns {
            println!(
                "    {} - {} matches ({:.1}%)",
                pattern.name.bright_white(),
                pattern.match_count,
                pattern.match_percentage
            );
        }
    }
}

fn display_quality_issues(issues: &[QualityIssue]) {
    if issues.is_empty() {
        println!("✨ {}", "No quality issues found!".green().bold());
        println!();
        return;
    }

    println!(
        "⚠️  {} {}",
        "QUALITY ISSUES FOUND:".red().bold(),
        format!("({})", issues.len()).red()
    );
    println!();

    let mut critical_count = 0;
    let mut warning_count = 0;
    let mut info_count = 0;

    for (i, issue) in issues.iter().enumerate() {
        let (icon, severity_text) = match issue.severity() {
            dataprof::types::Severity::High => {
                critical_count += 1;
                ("🔴", "CRITICAL".red().bold())
            }
            dataprof::types::Severity::Medium => {
                warning_count += 1;
                ("🟡", "WARNING".yellow().bold())
            }
            dataprof::types::Severity::Low => {
                info_count += 1;
                ("🔵", "INFO".blue().bold())
            }
        };

        print!("{}. {} {} ", i + 1, icon, severity_text);

        match issue {
            QualityIssue::NullValues {
                column,
                count,
                percentage,
            } => {
                println!(
                    "[{}]: {} null values ({:.1}%)",
                    column.yellow(),
                    count.to_string().red(),
                    percentage
                );
            }
            QualityIssue::MixedDateFormats { column, formats } => {
                println!("[{}]: Mixed date formats", column.yellow());
                for (format, count) in formats {
                    println!("     - {}: {} rows", format, count);
                }
            }
            QualityIssue::Duplicates { column, count } => {
                println!(
                    "[{}]: {} duplicate values",
                    column.yellow(),
                    count.to_string().red()
                );
            }
            QualityIssue::Outliers {
                column,
                values,
                threshold,
            } => {
                println!(
                    "[{}]: {} outliers detected (>{}σ)",
                    column.yellow(),
                    values.len().to_string().red(),
                    threshold
                );
                for val in values.iter().take(3) {
                    println!("     - {}", val);
                }
                if values.len() > 3 {
                    println!("     ... and {} more", values.len() - 3);
                }
            }
            QualityIssue::MixedTypes { column, types } => {
                println!("[{}]: Mixed data types", column.yellow());
                for (dtype, count) in types {
                    println!("     - {}: {} rows", dtype, count);
                }
            }
        }
    }

    // Summary
    println!();
    print!("📊 Summary: ");
    if critical_count > 0 {
        print!("{} critical ", critical_count.to_string().red());
    }
    if warning_count > 0 {
        print!("{} warnings ", warning_count.to_string().yellow());
    }
    if info_count > 0 {
        print!("{} info", info_count.to_string().blue());
    }
    println!();
    println!();
}

fn is_json_file(path: &Path) -> bool {
    if let Some(extension) = path.extension().and_then(|e| e.to_str()) {
        matches!(extension.to_lowercase().as_str(), "json" | "jsonl")
    } else {
        false
    }
}

/// Run batch processing with glob pattern
fn run_batch_glob(cli: &Cli, pattern: &str) -> Result<()> {
    println!(
        "{}",
        "🔍 DataProfiler v0.3.0 - Batch Analysis (Glob)"
            .bright_blue()
            .bold()
    );

    let config = create_batch_config(cli);
    let processor = BatchProcessor::with_config(config);

    let result = processor.process_glob(pattern)?;
    display_batch_results(&result, cli);

    Ok(())
}

/// Run batch processing on directory
fn run_batch_directory(cli: &Cli) -> Result<()> {
    println!(
        "{}",
        "📁 DataProfiler v0.3.0 - Batch Analysis (Directory)"
            .bright_blue()
            .bold()
    );

    let config = create_batch_config(cli);
    let processor = BatchProcessor::with_config(config);

    let result = processor.process_directory(&cli.file)?;
    display_batch_results(&result, cli);

    Ok(())
}

/// Create batch configuration from CLI options
fn create_batch_config(cli: &Cli) -> BatchConfig {
    let max_concurrent = if cli.max_concurrent > 0 {
        cli.max_concurrent
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    };

    BatchConfig {
        parallel: cli.parallel,
        max_concurrent,
        recursive: cli.recursive,
        extensions: vec!["csv".to_string(), "json".to_string(), "jsonl".to_string()],
        exclude_patterns: vec![
            "**/.*".to_string(),
            "**/*tmp*".to_string(),
            "**/*temp*".to_string(),
        ],
    }
}

/// Display batch processing results
fn display_batch_results(result: &dataprof::BatchResult, cli: &Cli) {
    println!("\n📈 Batch Quality Analysis");

    // Overall summary
    if result.summary.failed > 0 {
        let failure_rate =
            (result.summary.failed as f64 / result.summary.total_files as f64) * 100.0;
        println!("⚠️ {:.1}% of files failed processing", failure_rate);
    }

    if result.summary.total_issues > 0 {
        println!(
            "🔍 Found {} quality issues across {} files",
            result.summary.total_issues, result.summary.successful
        );

        if result.summary.average_quality_score < 80.0 {
            println!(
                "📊 Average Quality Score: {:.1}% - {} BELOW THRESHOLD",
                result.summary.average_quality_score,
                "⚠️".yellow()
            );
        } else {
            println!(
                "📊 Average Quality Score: {:.1}% - {} GOOD",
                result.summary.average_quality_score,
                "✅".green()
            );
        }
    }

    // Show top problematic files
    if cli.quality && !result.reports.is_empty() {
        println!("\n🔍 Quality Issues by File:");

        let mut file_scores: Vec<_> = result
            .reports
            .iter()
            .filter_map(|(path, report)| {
                report
                    .quality_score()
                    .ok()
                    .map(|score| (path, report, score))
            })
            .collect();

        file_scores.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal));

        for (path, report, score) in file_scores.iter().take(10) {
            let icon = if *score < 60.0 {
                "🔴"
            } else if *score < 80.0 {
                "🟡"
            } else {
                "✅"
            };
            println!(
                "  {} {:.1}% - {} ({} issues)",
                icon,
                score,
                path.file_name()
                    .map_or("unknown".into(), |name| name.to_string_lossy()),
                report.issues.len()
            );
        }
    }

    // Processing performance
    if result.summary.total_files > 1 {
        let files_per_sec =
            result.summary.successful as f64 / result.summary.processing_time_seconds;
        println!(
            "\n⚡ Processed {:.1} files/sec ({:.2}s total)",
            files_per_sec, result.summary.processing_time_seconds
        );
    }
}

/// Run database analysis
#[cfg(feature = "database")]
fn run_database_analysis(cli: &Cli, connection_string: &str) -> Result<()> {
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

    if cli.quality {
        // Show quality issues
        display_quality_issues(&report.issues);

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
    for profile in report.column_profiles {
        display_profile(&profile);
        println!();
    }

    Ok(())
}
