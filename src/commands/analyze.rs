use anyhow::Result;
use colored::*;
use std::path::Path;

use crate::cli::args::{Cli, CliOutputFormat};
use dataprof::core::DataprofConfig;
use dataprof::output::progress::{display_startup_banner, ProgressManager};
use dataprof::output::{
    display_ml_score, display_profile, display_quality_issues, output_with_formatter,
};
use dataprof::OutputFormat;
use dataprof::{
    analyze_csv_robust, analyze_csv_with_sampling, analyze_json_with_quality, generate_html_report,
    ChunkSize, DataProfiler, MlReadinessEngine, ProgressInfo,
};

use super::batch::{run_batch_directory, run_batch_glob};
use super::benchmark::run_benchmark_analysis;
#[cfg(feature = "database")]
use super::database::run_database_analysis;
use super::script_generator::generate_preprocessing_script;

pub fn is_json_file(path: &Path) -> bool {
    if let Some(extension) = path.extension() {
        matches!(extension.to_str(), Some("json") | Some("jsonl"))
    } else {
        false
    }
}

pub fn run_analysis(cli: &Cli, config: &DataprofConfig) -> Result<()> {
    // Create progress manager
    let progress = ProgressManager::new(
        config.output.show_progress && cli.progress,
        config.output.verbosity.max(cli.verbosity),
    );

    // Display startup banner for non-JSON output
    if !matches!(cli.format, CliOutputFormat::Json) && config.output.verbosity > 0 {
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

    // Single file processing
    run_single_file_analysis(cli, config, &progress)
}

fn run_single_file_analysis(
    cli: &Cli,
    config: &DataprofConfig,
    progress: &ProgressManager,
) -> Result<()> {
    // Skip headers for JSON output
    if !matches!(cli.format, CliOutputFormat::Json) {
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
        run_quality_analysis(cli, config, progress)
    } else {
        run_simple_analysis(cli, config)
    }
}

fn run_quality_analysis(
    cli: &Cli,
    config: &DataprofConfig,
    progress: &ProgressManager,
) -> Result<()> {
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
        calculate_ml_score(&report, progress)?
    } else {
        None
    };

    // Generate HTML report if requested
    if let Some(html_path) = &cli.html {
        match generate_html_report(&report, html_path) {
            Ok(_) => {
                if matches!(cli.format, CliOutputFormat::Text) {
                    println!(
                        "📄 HTML report saved to: {}",
                        html_path.display().to_string().bright_green()
                    );
                    println!();
                }
            }
            Err(e) => {
                eprintln!("❌ Failed to generate HTML report: {}", e);
            }
        }
    }

    // Generate preprocessing script if requested
    if let Some(script_path) = &cli.output_script {
        if let Some(ref ml_score_data) = ml_score {
            match generate_preprocessing_script(
                ml_score_data,
                script_path,
                &cli.file.to_string_lossy(),
            ) {
                Ok(_) => {
                    if matches!(cli.format, CliOutputFormat::Text) {
                        println!(
                            "🐍 Preprocessing script saved to: {}",
                            script_path.display().to_string().bright_green()
                        );
                        println!("   Ready to use with: python {}", script_path.display());
                        println!();
                    }
                }
                Err(e) => {
                    eprintln!("❌ Failed to generate preprocessing script: {}", e);
                }
            }
        } else {
            eprintln!("⚠️  Script generation requires ML scoring. Use --ml-score flag.");
        }
    }

    // Handle enhanced output formatting
    if !matches!(cli.format, CliOutputFormat::Text) {
        let format: OutputFormat = cli.format.clone().into();
        return output_with_formatter(&report, &format, ml_score.as_ref());
    }

    // Display results
    display_analysis_results(cli, &report, ml_score.as_ref());

    Ok(())
}

fn calculate_ml_score(
    report: &dataprof::QualityReport,
    progress: &ProgressManager,
) -> Result<Option<dataprof::analysis::MlReadinessScore>> {
    let ml_progress = progress.create_ml_progress(4);

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

    let score = ml_engine.calculate_ml_score(report)?;

    if let Some(ref pb) = ml_progress {
        pb.set_message("Generating recommendations...");
        pb.set_position(4);
        pb.finish_with_message("ML readiness analysis complete");
    }

    Ok(Some(score))
}

fn display_analysis_results(
    cli: &Cli,
    report: &dataprof::QualityReport,
    ml_score: Option<&dataprof::analysis::MlReadinessScore>,
) {
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
    if let Some(score) = ml_score {
        if cli.ml_code {
            display_ml_score_with_code(score);
        } else {
            display_ml_score(score);
        }
    }

    // Show quality issues first
    display_quality_issues(&report.issues);

    // Then show column profiles
    for profile in &report.column_profiles {
        display_profile(profile);
        println!();
    }
}

/// Display ML score with code snippets for recommendations
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
                    format!("[{}]", priority_to_string(&rec.priority)).color(priority_color),
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

/// Convert RecommendationPriority enum to string
fn priority_to_string(priority: &dataprof::analysis::RecommendationPriority) -> String {
    match priority {
        dataprof::analysis::RecommendationPriority::Critical => "critical".to_string(),
        dataprof::analysis::RecommendationPriority::High => "high".to_string(),
        dataprof::analysis::RecommendationPriority::Medium => "medium".to_string(),
        dataprof::analysis::RecommendationPriority::Low => "low".to_string(),
    }
}

fn run_simple_analysis(cli: &Cli, _config: &DataprofConfig) -> Result<()> {
    // Check if HTML output is requested without quality mode
    if cli.html.is_some() {
        eprintln!(
            "❌ HTML report requires --quality flag. Use: {} --quality --html report.html",
            "dataprof".bright_blue()
        );
        std::process::exit(1);
    }

    // Simple analysis without quality checking
    let profiles = if is_json_file(&cli.file) {
        dataprof::analyze_json(&cli.file)?
    } else {
        // Fast analysis for CSV
        dataprof::analyze_csv_fast(&cli.file)?
    };

    // Handle output formatting
    match cli.format {
        CliOutputFormat::Json => {
            dataprof::output::output_json_profiles(&profiles)?;
        }
        _ => {
            // Simple display for non-JSON formats
            println!(
                "📊 {} - Simple Analysis",
                "DataProfiler".bright_blue().bold()
            );
            println!("📁 {} | {} columns", cli.file.display(), profiles.len());
            println!();

            for profile in profiles {
                display_profile(&profile);
                println!();
            }
        }
    }

    Ok(())
}
