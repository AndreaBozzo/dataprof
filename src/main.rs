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
    ChunkSize,
    ColumnProfile,
    ColumnStats,
    // v0.3.0 imports
    DataProfiler,
    DataProfilerError,
    DataType,
    ErrorSeverity,
    ProgressInfo,
    QualityIssue,
    // SamplingStrategy, // Future CLI integration
};
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "dataprof")]
#[command(about = "Fast CSV data profiler with quality checking - v0.3.0 Streaming Edition")]
struct Cli {
    /// CSV file to analyze
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
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Enhanced error handling wrapper
    if let Err(e) = run_analysis(&cli) {
        handle_error(&e, cli.file.as_path());
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
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();
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
