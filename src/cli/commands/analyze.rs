use clap::Args;
use std::path::PathBuf;

/// Full analysis arguments
#[derive(Debug, Args)]
pub struct AnalyzeArgs {
    /// Input file to analyze
    pub file: PathBuf,

    /// Show detailed metrics for all dimensions
    #[arg(long)]
    pub detailed: bool,

    /// Output format (text, json, csv)
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Output file path
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// ISO quality threshold profile (default, strict, lenient)
    #[arg(long, default_value = "default")]
    pub threshold_profile: String,

    /// Use streaming for large files
    #[arg(long)]
    pub streaming: bool,

    /// Sample size for large files
    #[arg(long)]
    pub sample: Option<usize>,

    /// Common analysis options (progress, chunk-size, config)
    #[command(flatten)]
    pub common: super::CommonAnalysisOptions,
}

use anyhow::Result;
use dataprof::OutputFormat;
use std::fs;

use crate::cli::{AnalysisOptions, analyze_file_with_options};

/// Execute the analyze command - comprehensive ISO 8000/25012 analysis
pub fn execute(args: &AnalyzeArgs) -> Result<()> {
    // Build analysis options from command arguments
    let options = AnalysisOptions {
        progress: args.common.progress,
        chunk_size: args.common.chunk_size,
        config: args.common.config.clone(),
        streaming: args.streaming,
        sample: args.sample,
        verbosity: Some(args.common.verbosity),
    };

    // Use shared core logic that handles all improvements
    let report = analyze_file_with_options(&args.file, options)?;

    // Convert string format to OutputFormat enum
    let output_format = match args.format.as_str() {
        "json" => Some(OutputFormat::Json),
        "csv" => Some(OutputFormat::Csv),
        "plain" => Some(OutputFormat::Plain),
        "text" => Some(OutputFormat::Text),
        _ => None, // Let adaptive formatter decide
    };

    // Use unified formatter system
    dataprof::output::output_with_adaptive_formatter(&report, output_format)?;

    // Save to file if requested
    if let Some(output_path) = &args.output {
        save_output_to_file(output_path, &report, &args.format)?;
    }

    Ok(())
}

/// Save formatted output to a file
fn save_output_to_file(
    path: &std::path::Path,
    report: &dataprof::types::QualityReport,
    format: &str,
) -> Result<()> {
    // Convert format string to OutputFormat
    let output_format = match format {
        "json" => OutputFormat::Json,
        "csv" => OutputFormat::Csv,
        "plain" => OutputFormat::Plain,
        _ => OutputFormat::Text,
    };

    // Use the same formatter to generate content
    let formatter = dataprof::output::create_adaptive_formatter_with_format(output_format);
    let content = formatter.format_report(report)?;

    fs::write(path, content)?;
    log::info!("Results saved to: {}", path.display());
    Ok(())
}
