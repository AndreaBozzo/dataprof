use anyhow::Result;
use colored::*;

use crate::cli::args::Cli;
use crate::commands::script_generator::generate_batch_preprocessing_script;
use dataprof::output::batch_results::display_enhanced_batch_results;
use dataprof::output::html::generate_batch_html_report;
use dataprof::output::progress::ProgressManager;
use dataprof::{BatchConfig, BatchProcessor};

pub fn run_batch_glob(cli: &Cli, pattern: &str) -> Result<()> {
    println!(
        "{}",
        "🔍 DataProfiler v0.4.1 - Enhanced Batch Analysis (Glob)"
            .bright_blue()
            .bold()
    );

    let config = create_batch_config(cli);
    let progress_manager = ProgressManager::new(cli.progress, cli.verbosity);
    let processor = BatchProcessor::with_progress(config, progress_manager);

    let mut result = processor.process_glob(pattern)?;

    // Generate HTML report if requested
    if let Some(html_path) = &cli.html {
        if let Err(e) = generate_batch_html_report(&result, html_path) {
            eprintln!("❌ Failed to generate HTML report: {}", e);
        } else {
            println!(
                "📄 HTML dashboard saved to: {}",
                html_path.display().to_string().bright_green()
            );
            result.html_report_path = Some(html_path.clone());
        }
    }

    // Generate batch preprocessing script if requested
    if let Some(script_path) = &cli.output_script {
        if !result.ml_scores.is_empty() {
            if let Err(e) =
                generate_batch_preprocessing_script(&result.ml_scores, script_path, pattern)
            {
                eprintln!("❌ Failed to generate preprocessing script: {}", e);
            } else {
                println!(
                    "🐍 Batch preprocessing script saved to: {}",
                    script_path.display().to_string().bright_green()
                );
                result.script_path = Some(script_path.clone());
            }
        } else {
            eprintln!("⚠️  Script generation requires ML scoring. Use --ml-score flag.");
        }
    }

    // Display enhanced results with ML details
    display_enhanced_batch_results(&result, cli.quality, cli.ml_code);

    Ok(())
}

pub fn run_batch_directory(cli: &Cli) -> Result<()> {
    println!(
        "{}",
        "📁 DataProfiler v0.4.1 - Enhanced Batch Analysis (Directory)"
            .bright_blue()
            .bold()
    );

    let config = create_batch_config(cli);
    let progress_manager = ProgressManager::new(cli.progress, cli.verbosity);
    let processor = BatchProcessor::with_progress(config, progress_manager);

    let mut result = processor.process_directory(&cli.file)?;

    // Generate HTML report if requested
    if let Some(html_path) = &cli.html {
        if let Err(e) = generate_batch_html_report(&result, html_path) {
            eprintln!("❌ Failed to generate HTML report: {}", e);
        } else {
            println!(
                "📄 HTML dashboard saved to: {}",
                html_path.display().to_string().bright_green()
            );
            result.html_report_path = Some(html_path.clone());
        }
    }

    // Generate batch preprocessing script if requested
    if let Some(script_path) = &cli.output_script {
        if !result.ml_scores.is_empty() {
            if let Err(e) = generate_batch_preprocessing_script(
                &result.ml_scores,
                script_path,
                &cli.file.to_string_lossy(),
            ) {
                eprintln!("❌ Failed to generate preprocessing script: {}", e);
            } else {
                println!(
                    "🐍 Batch preprocessing script saved to: {}",
                    script_path.display().to_string().bright_green()
                );
                result.script_path = Some(script_path.clone());
            }
        } else {
            eprintln!("⚠️  Script generation requires ML scoring. Use --ml-score flag.");
        }
    }

    // Display enhanced results with ML details
    display_enhanced_batch_results(&result, cli.quality, cli.ml_code);

    Ok(())
}

pub fn create_batch_config(cli: &Cli) -> BatchConfig {
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
        ml_score: cli.ml_score,
        ml_code: cli.ml_code,
        html_output: cli.html.clone(),
        output_script: cli.output_script.clone(),
    }
}
