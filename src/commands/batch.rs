use anyhow::Result;
use colored::*;

use crate::cli::args::Cli;
use dataprof::output::display_batch_results;
use dataprof::output::progress::ProgressManager;
use dataprof::{BatchConfig, BatchProcessor};

pub fn run_batch_glob(cli: &Cli, pattern: &str) -> Result<()> {
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
    display_batch_results(&result, cli.quality);

    Ok(())
}

pub fn run_batch_directory(cli: &Cli) -> Result<()> {
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
    display_batch_results(&result, cli.quality);

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
    }
}
