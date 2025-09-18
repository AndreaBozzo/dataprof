use anyhow::Result;
use clap::{Parser, ValueEnum};
use colored::*;
use dataprof::{
    analyze_csv,
    analyze_csv_fast,
    analyze_csv_robust,
    analyze_csv_with_sampling,
    analyze_json,
    analyze_json_with_quality,
    generate_html_report,
    // v0.4.0 imports - Intelligent engine selection
    AdaptiveProfiler,
    // SamplingStrategy, // Future CLI integration
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
    MlReadinessEngine,
    ProgressInfo,
    QualityIssue,
};
use serde::Serialize;
use std::path::{Path, PathBuf};

// Import new output formatters
use dataprof::core::{exit_codes, DataprofConfig, InputValidator, ValidationError};
use dataprof::output::formatters::create_formatter;
use dataprof::output::progress::{display_startup_banner, ProgressManager};

// Database support (default: postgres, mysql, sqlite)
#[cfg(feature = "database")]
use dataprof::{profile_database, DatabaseConfig};

#[derive(Clone, Debug, ValueEnum)]
enum OutputFormat {
    /// Human-readable text output
    Text,
    /// Machine-readable JSON output
    Json,
    /// CSV format for data processing
    Csv,
    /// Plain text without formatting for scripting
    Plain,
}

#[derive(Clone, Debug, ValueEnum)]
enum EngineChoice {
    /// Automatic intelligent selection (RECOMMENDED)
    Auto,
    /// Standard streaming engine
    Streaming,
    /// Memory-efficient streaming engine
    MemoryEfficient,
    /// True streaming for very large files
    TrueStreaming,
    /// Apache Arrow columnar engine (requires arrow feature)
    Arrow,
}

#[derive(Serialize)]
struct BenchmarkOutput {
    rows: usize,
    columns: usize,
    file_size_mb: f64,
    scan_time_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    quality_score: Option<f64>,
}

#[derive(Parser)]
#[command(name = "dataprof")]
#[command(
    about = "Fast CSV data profiler with quality checking - v0.4.1 Enhanced CLI Edition",
    long_about = r#"DataProfiler CLI - Fast CSV data profiling with ML readiness scoring

A powerful command-line tool for analyzing CSV, JSON, and JSONL files with:
• Quality assessment and data profiling
• ML readiness scoring with preprocessing recommendations
• Multiple output formats (JSON, CSV, HTML, plain text)
• Batch processing with progress indicators
• Configuration file support

EXAMPLES:
  # Basic file analysis
  dataprof data.csv

  # Generate HTML report with ML scoring
  dataprof data.csv --html report.html --ml-score

  # Batch process directory with progress
  dataprof /data/folder --progress --recursive

  # Use glob pattern for multiple files
  dataprof --glob "data/**/*.csv" --format json

  # Sample large files for faster analysis
  dataprof large_file.csv --sample 10000 --streaming

  # Custom configuration
  dataprof data.csv --config .dataprof.toml --verbose

  # Quality-focused analysis with detailed output
  dataprof data.csv --quality --format csv --verbosity 2

For more information, visit: https://github.com/AndreaBozzo/dataprof"#
)]
struct Cli {
    /// Input file, directory, or glob pattern to analyze
    ///
    /// Examples:
    ///   data.csv              Single CSV file
    ///   /data/folder          Directory (use with --recursive)
    ///   "data/**/*.csv"       Glob pattern (use with --glob)
    file: PathBuf,

    /// Enable comprehensive data quality assessment
    ///
    /// Performs detailed quality checks including missing value analysis,
    /// data type validation, pattern detection, and outlier identification.
    #[arg(short, long)]
    quality: bool,

    /// Generate interactive HTML report at specified path
    ///
    /// Creates a comprehensive web-based report with visualizations,
    /// quality metrics, and recommendations. Example: --html report.html
    #[arg(long)]
    html: Option<PathBuf>,

    /// Enable streaming mode for large files (recommended for >100MB)
    ///
    /// Processes files in chunks to minimize memory usage. Essential for
    /// very large datasets or memory-constrained environments.
    #[arg(long)]
    streaming: bool,

    /// Show real-time progress indicators (requires --streaming)
    ///
    /// Displays progress bars and processing status for batch operations.
    /// Automatically enabled for long-running operations.
    #[arg(long)]
    progress: bool,

    /// Chunk size for streaming mode (rows per chunk)
    ///
    /// Controls memory usage vs processing speed trade-off. Auto-detected
    /// based on available memory if not specified. Example: --chunk-size 1000
    #[arg(long)]
    chunk_size: Option<usize>,

    /// Sample size for large file analysis (process only N rows)
    ///
    /// Useful for quick analysis of very large files. Randomly samples
    /// from the entire file for representative results. Example: --sample 10000
    #[arg(long)]
    sample: Option<usize>,

    /// Enable recursive directory scanning
    ///
    /// When processing directories, scan all subdirectories for supported files.
    /// Respects exclude patterns and file type filters.
    #[arg(short, long)]
    recursive: bool,

    /// Glob pattern for batch file processing
    ///
    /// Process multiple files matching a pattern. Supports standard glob syntax:
    /// *, **, ?, [abc], {csv,json}. Example: --glob "data/**/*.csv"
    #[arg(long)]
    glob: Option<String>,

    /// Enable parallel processing for multiple files
    ///
    /// Processes multiple files concurrently to improve performance.
    /// Automatically manages thread pool based on system capabilities.
    #[arg(long)]
    parallel: bool,

    /// Maximum concurrent files for batch processing (0 = auto-detect)
    ///
    /// Controls parallelism level for multi-file operations. Auto-detection
    /// uses CPU count. Lower values reduce memory usage.
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

    /// Output format for results
    ///
    /// Available formats:
    /// • text: Human-readable summary (default)
    /// • json: Machine-readable JSON output
    /// • csv:  Structured CSV format
    /// • plain: Simple text without formatting
    #[arg(long, value_enum, default_value = "text")]
    format: OutputFormat,

    /// Processing engine selection (default: auto for intelligent selection)
    ///
    /// Available engines: auto, streaming, memory-mapped, standard.
    /// Auto mode selects the best engine based on file size and system resources.
    #[arg(long, value_enum, default_value = "auto")]
    engine: EngineChoice,

    /// Enable performance benchmark mode
    ///
    /// Runs comprehensive performance tests on the provided file.
    /// Measures processing speed, memory usage, and scalability metrics.
    #[arg(long)]
    benchmark: bool,

    /// Show detailed engine information and exit
    ///
    /// Displays version, build info, feature flags, and system capabilities.
    /// Useful for debugging and support purposes.
    #[arg(long)]
    engine_info: bool,

    /// Enable ML readiness scoring and recommendations
    ///
    /// Analyzes data suitability for machine learning applications:
    /// • Feature quality assessment • Encoding recommendations
    /// • Preprocessing suggestions  • Blocking issue detection
    #[arg(long)]
    ml_score: bool,

    /// Configuration file path (.toml format)
    ///
    /// Load settings from TOML configuration file. Auto-discovers
    /// .dataprof.toml in current/parent directories. Example: --config ~/.dataprof.toml
    #[arg(long)]
    config: Option<PathBuf>,

    /// Verbosity level (can be used multiple times: -v, -vv, -vvv)
    ///
    /// • 0: Quiet mode (errors only)
    /// • 1: Normal output with basic info
    /// • 2: Verbose with detailed progress
    /// • 3: Debug mode with trace information
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    verbosity: u8,

    /// Disable colored output (also set NO_COLOR environment variable)
    ///
    /// Forces plain text output without ANSI color codes.
    /// Useful for CI/CD, logging, or terminal compatibility issues.
    #[arg(long)]
    no_color: bool,
}

fn main() -> Result<()> {
    // Custom parsing to handle engine-info without file argument
    let args: Vec<String> = std::env::args().collect();
    if args.contains(&"--engine-info".to_string()) {
        return show_engine_info();
    }

    let cli = Cli::parse();

    // Input validation with helpful error messages
    if let Err(e) = validate_cli_inputs(&cli) {
        eprintln!("❌ {}", e);
        std::process::exit(InputValidator::get_exit_code(&e));
    }

    // Load configuration with CLI integration
    let mut config = if let Some(config_path) = &cli.config {
        // Validate config file first
        if let Err(e) = InputValidator::validate_config_file(config_path) {
            eprintln!("❌ Configuration Error: {}", e);
            std::process::exit(exit_codes::CONFIG_ERROR);
        }
        DataprofConfig::load_from_file(config_path)?
    } else {
        DataprofConfig::load_with_discovery()
    };

    // Apply CLI overrides to configuration
    config.merge_with_cli_args(
        Some(match cli.format {
            OutputFormat::Text => "text",
            OutputFormat::Json => "json",
            OutputFormat::Csv => "csv",
            OutputFormat::Plain => "plain",
        }),
        Some(cli.quality),
        Some(cli.progress),
    );

    // Handle verbosity and colors
    if cli.no_color || std::env::var("NO_COLOR").is_ok() {
        config.output.colored = false;
    }
    if cli.verbosity > 0 {
        config.output.verbosity = cli.verbosity;
    }

    // Validate configuration
    if let Err(e) = config.validate() {
        eprintln!("❌ Configuration Error: {}", e);
        std::process::exit(exit_codes::CONFIG_ERROR);
    }

    // Enhanced error handling wrapper with proper exit codes
    match run_analysis(&cli, &config) {
        Ok(_) => std::process::exit(exit_codes::SUCCESS),
        Err(e) => {
            let exit_code = determine_exit_code(&e);
            handle_error(&e, &cli.file);
            std::process::exit(exit_code);
        }
    }
}

fn output_json_profiles(profiles: &[ColumnProfile]) -> Result<()> {
    // For simple profiles (without quality report), estimate basic info
    let output = BenchmarkOutput {
        rows: profiles.first().map_or(0, |p| p.total_count),
        columns: profiles.len(),
        file_size_mb: 0.0, // Not available in simple profile mode
        scan_time_ms: 0,   // Not tracked in simple mode
        quality_score: None,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
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

fn run_analysis(cli: &Cli, config: &DataprofConfig) -> Result<()> {
    // Create progress manager
    let progress = ProgressManager::new(
        config.output.show_progress && cli.progress,
        config.output.verbosity.max(cli.verbosity),
    );

    // Display startup banner for non-JSON output
    if !matches!(cli.format, OutputFormat::Json) && config.output.verbosity > 0 {
        display_startup_banner("0.4.1", config.output.colored);
    }

    // Handle benchmark request
    if cli.benchmark {
        return run_benchmark_analysis(cli);
    }

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
    // Skip headers for JSON output
    if !matches!(cli.format, OutputFormat::Json) {
        let version_info = if cli.streaming {
            "📊 DataProfiler v0.3.0 - Streaming Analysis"
                .bright_blue()
                .bold()
        } else {
            "📊 DataProfiler - Standard Analysis".bright_blue().bold()
        };

        println!("{}", version_info);
        println!();
    }

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

        // Handle ML scoring if requested
        let ml_score = if cli.ml_score || config.ml.auto_score {
            let ml_progress = progress.create_ml_progress(4); // 4 component scores

            if let Some(ref pb) = ml_progress {
                pb.set_message("Calculating completeness score...");
                pb.set_position(1);
            }

            let ml_engine = MlReadinessEngine::new();

            if let Some(ref pb) = ml_progress {
                pb.set_message("Calculating consistency score...");
                pb.set_position(2);
            }

            if let Some(ref pb) = ml_progress {
                pb.set_message("Analyzing feature quality...");
                pb.set_position(3);
            }

            let score = ml_engine.calculate_ml_score(&report)?;

            if let Some(ref pb) = ml_progress {
                pb.set_message("Generating recommendations...");
                pb.set_position(4);
                pb.finish_with_message("ML readiness analysis complete");
            }

            Some(score)
        } else {
            None
        };

        // Handle enhanced output formatting
        if !matches!(cli.format, OutputFormat::Text) {
            return output_with_formatter(&report, &cli.format, ml_score.as_ref());
        }

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

        // Show ML score if available
        if let Some(ref score) = ml_score {
            display_ml_score(score);
        }

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
        // Use fast mode for JSON output format (benchmarks)
        let profiles = if is_json_file(&cli.file) {
            analyze_json(&cli.file)?
        } else if matches!(cli.format, OutputFormat::Json) {
            analyze_csv_fast(&cli.file)?
        } else {
            analyze_csv(&cli.file)?
        };

        // Handle JSON output for benchmarks
        if matches!(cli.format, OutputFormat::Json) {
            return output_json_profiles(&profiles);
        }

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
    let progress_manager = ProgressManager::new(cli.progress, cli.verbosity);
    let processor = BatchProcessor::with_progress(config, progress_manager);

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
    let progress_manager = ProgressManager::new(cli.progress, cli.verbosity);
    let processor = BatchProcessor::with_progress(config, progress_manager);

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
        retry_config: Some(dataprof::database::RetryConfig::default()),
        sampling_config: None,
        enable_ml_readiness: true,
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

/// Show engine availability and system information
fn show_engine_info() -> Result<()> {
    println!(
        "{}",
        "🔧 DataProfiler Engine Information".bright_blue().bold()
    );
    println!();

    // System information
    println!("{}", "System Resources:".bright_yellow());
    let mut sys = sysinfo::System::new_all();
    sys.refresh_all();

    let total_memory_gb = sys.total_memory() as f64 / 1_073_741_824.0;
    let available_memory_gb = sys.available_memory() as f64 / 1_073_741_824.0;
    let cpu_cores = num_cpus::get();

    println!("  CPU Cores: {}", cpu_cores);
    println!("  Total Memory: {:.1} GB", total_memory_gb);
    println!("  Available Memory: {:.1} GB", available_memory_gb);
    println!(
        "  Memory Usage: {:.1}%",
        ((total_memory_gb - available_memory_gb) / total_memory_gb) * 100.0
    );
    println!();

    // Engine availability
    println!("{}", "Available Engines:".bright_yellow());

    println!(
        "  ✅ {} - Basic streaming for small files (<100MB)",
        "Streaming".green()
    );
    println!(
        "  ✅ {} - Memory-efficient for medium files (50-200MB)",
        "MemoryEfficient".green()
    );
    println!(
        "  ✅ {} - True streaming for large files (>200MB)",
        "TrueStreaming".green()
    );

    #[cfg(feature = "arrow")]
    {
        println!(
            "  ✅ {} - High-performance columnar processing (>500MB)",
            "Arrow".green()
        );
    }
    #[cfg(not(feature = "arrow"))]
    {
        println!(
            "  ❌ {} - Not available (compile with --features arrow)",
            "Arrow".red()
        );
    }

    println!(
        "  🚀 {} - Intelligent automatic selection",
        "Auto".bright_green().bold()
    );
    println!();

    // Recommendations
    println!("{}", "Recommendations:".bright_yellow());
    println!(
        "  • Use {} for best performance",
        "--engine auto".bright_green()
    );
    println!(
        "  • Use {} to compare engines on your data",
        "--benchmark".bright_cyan()
    );

    #[cfg(not(feature = "arrow"))]
    {
        println!(
            "  • Compile with {} for better large file performance",
            "--features arrow".bright_yellow()
        );
    }

    if available_memory_gb < 2.0 {
        println!(
            "  ⚠️ {} Low memory detected - streaming engines recommended",
            "Warning:".yellow()
        );
    }

    Ok(())
}

/// Run benchmark analysis comparing all engines
fn run_benchmark_analysis(cli: &Cli) -> Result<()> {
    println!(
        "{}",
        "🏁 DataProfiler Engine Benchmark".bright_blue().bold()
    );
    println!("File: {}", cli.file.display());
    println!();

    let profiler = AdaptiveProfiler::new()
        .with_logging(true)
        .with_performance_logging(true);

    let performances = profiler.benchmark_engines(&cli.file)?;

    println!("\n📊 Benchmark Results:");
    println!("{}", "=".repeat(60));

    // Sort by execution time
    let mut sorted = performances.clone();
    sorted.sort_by(|a, b| {
        if a.success != b.success {
            b.success.cmp(&a.success) // Success first
        } else if a.success {
            a.execution_time_ms.cmp(&b.execution_time_ms) // Faster first
        } else {
            std::cmp::Ordering::Equal
        }
    });

    for (i, perf) in sorted.iter().enumerate() {
        let rank = if perf.success {
            match i {
                0 => "🥇".to_string(),
                1 => "🥈".to_string(),
                2 => "🥉".to_string(),
                _ => format!("#{}", i + 1),
            }
        } else {
            "❌".to_string()
        };

        println!("{} {:?}", rank, perf.engine_type);

        if perf.success {
            println!("   Time: {:.2}s", perf.execution_time_ms as f64 / 1000.0);
            if perf.rows_per_second > 0.0 {
                println!("   Speed: {:.0} rows/sec", perf.rows_per_second);
            }
        } else {
            println!("   Status: Failed");
            if let Some(error) = &perf.error_message {
                println!("   Error: {}", error);
            }
        }
        println!();
    }

    // Recommendations
    if let Some(fastest) = sorted.first() {
        if fastest.success {
            println!(
                "🎯 {} Recommendation: Use {:?} for optimal performance on this file type",
                "Best:".green().bold(),
                fastest.engine_type
            );
        }
    }

    Ok(())
}

/// Enhanced output using formatter pattern
fn output_with_formatter(
    report: &dataprof::QualityReport,
    format: &OutputFormat,
    ml_score: Option<&dataprof::analysis::MlReadinessScore>,
) -> Result<()> {
    let format_str = match format {
        OutputFormat::Json => "json",
        OutputFormat::Csv => "csv",
        OutputFormat::Plain => "plain",
        OutputFormat::Text => "text", // Fallback
    };

    let formatter = create_formatter(format_str);
    let mut output = formatter.format_report(report)?;

    // Add ML score to JSON output if available
    if matches!(format, OutputFormat::Json) && ml_score.is_some() {
        let mut json_value: serde_json::Value = serde_json::from_str(&output)?;
        if let Some(summary) = json_value.get_mut("summary") {
            if let Some(score) = ml_score {
                summary["ml_readiness"] = serde_json::json!({
                    "score": score.overall_score,
                    "level": score.readiness_level,
                    "recommendations": score.recommendations,
                    "feature_analysis": score.feature_analysis
                });
            }
        }
        output = serde_json::to_string_pretty(&json_value)?;
    }

    println!("{}", output);
    Ok(())
}

/// Display ML readiness score in human-readable format
fn display_ml_score(score: &dataprof::analysis::MlReadinessScore) {
    use dataprof::analysis::MlReadinessLevel;

    let (level_icon, level_color) = match score.readiness_level {
        MlReadinessLevel::Ready => ("🚀", "green"),
        MlReadinessLevel::Good => ("✅", "green"),
        MlReadinessLevel::NeedsWork => ("⚠️", "yellow"),
        MlReadinessLevel::NotReady => ("❌", "red"),
    };

    println!(
        "🤖 {} Machine Learning Readiness",
        "ML READINESS SCORE".bright_blue().bold()
    );

    // Overall score with color coding
    let score_str = format!("{:.1}%", score.overall_score);
    let colored_score = match score.readiness_level {
        MlReadinessLevel::Ready | MlReadinessLevel::Good => score_str.green().bold(),
        MlReadinessLevel::NeedsWork => score_str.yellow().bold(),
        MlReadinessLevel::NotReady => score_str.red().bold(),
    };

    println!(
        "   {} Overall Score: {} ({})",
        level_icon,
        colored_score,
        format!("{:?}", score.readiness_level).color(level_color)
    );

    // Component scores
    println!("   📊 Component Scores:");
    println!("      • Completeness: {:.1}%", score.completeness_score);
    println!("      • Consistency:  {:.1}%", score.consistency_score);
    println!(
        "      • Type Suitability: {:.1}%",
        score.type_suitability_score
    );
    println!(
        "      • Feature Quality: {:.1}%",
        score.feature_quality_score
    );

    // Blocking issues
    if !score.blocking_issues.is_empty() {
        println!("\n   🔴 {} Blocking Issues:", "CRITICAL".red().bold());
        for issue in &score.blocking_issues {
            println!("      • {}", issue.description.red());
            println!("        → {}", issue.resolution_required);
        }
    }

    // Top recommendations
    if !score.recommendations.is_empty() {
        println!("\n   💡 {} Top Recommendations:", "ML".bright_cyan().bold());
        for (i, rec) in score.recommendations.iter().take(3).enumerate() {
            let priority_icon = match rec.priority {
                dataprof::analysis::ml_readiness::RecommendationPriority::Critical => "🔴",
                dataprof::analysis::ml_readiness::RecommendationPriority::High => "🟠",
                dataprof::analysis::ml_readiness::RecommendationPriority::Medium => "🟡",
                dataprof::analysis::ml_readiness::RecommendationPriority::Low => "🔵",
            };
            println!("      {}. {} {}", i + 1, priority_icon, rec.description);
        }

        if score.recommendations.len() > 3 {
            println!(
                "      ... and {} more recommendations",
                score.recommendations.len() - 3
            );
        }
    }

    // Feature analysis summary
    let ready_features = score
        .feature_analysis
        .iter()
        .filter(|f| f.ml_suitability > 0.7)
        .count();
    let total_features = score.feature_analysis.len();

    println!(
        "\n   🔧 Feature Analysis: {}/{} features ML-ready (>70% suitability)",
        ready_features, total_features
    );

    // Preprocessing steps
    if !score.preprocessing_suggestions.is_empty() {
        println!(
            "   🛠️  Preprocessing Steps: {} recommended",
            score.preprocessing_suggestions.len()
        );
    }

    println!();
}

/// Validate CLI inputs with helpful error messages
fn validate_cli_inputs(cli: &Cli) -> Result<(), ValidationError> {
    // Validate file input (unless it's engine info or benchmark mode)
    if !cli.engine_info && !cli.benchmark {
        InputValidator::validate_file_input(&cli.file)?;
    }

    // Validate HTML output path if specified
    if let Some(html_path) = &cli.html {
        InputValidator::validate_output_directory(html_path)?;

        // Ensure HTML extension
        if html_path.extension().and_then(|s| s.to_str()) != Some("html") {
            return Err(ValidationError {
                message: "HTML output file must have .html extension".to_string(),
                suggestion: format!("Use: {}.html", html_path.with_extension("").display()),
                error_code: exit_codes::INVALID_ARGUMENT,
            });
        }
    }

    // Validate chunk size
    if let Some(chunk_size) = cli.chunk_size {
        InputValidator::validate_chunk_size(chunk_size)?;
    }

    // Validate sample size
    if let Some(sample_size) = cli.sample {
        InputValidator::validate_sample_size(sample_size)?;
    }

    // Validate argument combinations
    InputValidator::validate_argument_combinations(
        cli.streaming,
        cli.sample,
        cli.progress,
        cli.benchmark,
    )?;

    // Validate database connection if provided
    #[cfg(feature = "database")]
    if let Some(connection_string) = &cli.database {
        InputValidator::validate_database_connection(connection_string)?;
    }

    // Validate glob pattern if provided
    if let Some(pattern) = &cli.glob {
        InputValidator::validate_glob_pattern(pattern)?;
    }

    // Validate concurrent settings
    if cli.max_concurrent > 100 {
        return Err(ValidationError {
            message: format!("Max concurrent value too high: {}", cli.max_concurrent),
            suggestion: "Use a reasonable value (1-100) to avoid system overload".to_string(),
            error_code: exit_codes::INVALID_ARGUMENT,
        });
    }

    // HTML requires quality mode
    if cli.html.is_some() && !cli.quality {
        return Err(ValidationError {
            message: "HTML report requires quality checking".to_string(),
            suggestion: "Add --quality flag when using --html".to_string(),
            error_code: exit_codes::INVALID_ARGUMENT,
        });
    }

    Ok(())
}

/// Determine appropriate exit code based on error type
fn determine_exit_code(error: &anyhow::Error) -> i32 {
    // Check if it's our custom error type first
    if let Some(dp_error) = error.downcast_ref::<DataProfilerError>() {
        match dp_error {
            DataProfilerError::FileNotFound { .. } => exit_codes::FILE_NOT_FOUND,
            DataProfilerError::UnsupportedFormat { .. } => exit_codes::INVALID_DATA_FORMAT,
            DataProfilerError::MemoryLimitExceeded => exit_codes::NO_SPACE_LEFT,
            DataProfilerError::InvalidConfiguration { .. } => exit_codes::CONFIG_ERROR,
            DataProfilerError::StreamingError { .. } => exit_codes::PROCESSING_ERROR,
            DataProfilerError::IoError { .. } => exit_codes::GENERAL_ERROR,
            DataProfilerError::JsonParsingError { .. } => exit_codes::INVALID_DATA_FORMAT,
            DataProfilerError::CsvParsingError { .. } => exit_codes::INVALID_DATA_FORMAT,
            _ => exit_codes::PROCESSING_ERROR,
        }
    } else if let Some(validation_error) = error.downcast_ref::<ValidationError>() {
        validation_error.error_code
    } else {
        // Handle common error types
        let error_str = error.to_string().to_lowercase();
        if error_str.contains("no such file") || error_str.contains("not found") {
            exit_codes::FILE_NOT_FOUND
        } else if error_str.contains("permission denied") || error_str.contains("access") {
            exit_codes::PERMISSION_DENIED
        } else if error_str.contains("invalid") || error_str.contains("malformed") {
            exit_codes::INVALID_DATA_FORMAT
        } else if error_str.contains("database") || error_str.contains("connection") {
            exit_codes::DATABASE_ERROR
        } else if error_str.contains("network") || error_str.contains("timeout") {
            exit_codes::NETWORK_ERROR
        } else {
            exit_codes::GENERAL_ERROR
        }
    }
}
