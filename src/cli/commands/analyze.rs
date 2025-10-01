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

    /// Include ML readiness scoring
    #[arg(long)]
    pub ml: bool,

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
use dataprof::{MlReadinessEngine, OutputFormat};
use std::fs;

use crate::cli::{analyze_file_with_options, AnalysisOptions};

/// Execute the analyze command - comprehensive ISO 8000/25012 analysis
pub fn execute(args: &AnalyzeArgs) -> Result<()> {
    // Build analysis options from command arguments
    let options = AnalysisOptions {
        progress: args.common.progress,
        chunk_size: args.common.chunk_size,
        config: args.common.config.clone(),
        streaming: args.streaming,
        sample: args.sample,
    };

    // Use shared core logic that handles all improvements
    let report = analyze_file_with_options(&args.file, options)?;

    // Calculate ML score if requested
    let ml_score = if args.ml {
        let ml_engine = MlReadinessEngine::new();
        Some(ml_engine.calculate_ml_score(&report)?)
    } else {
        None
    };

    // Convert string format to OutputFormat enum
    let output_format = match args.format.as_str() {
        "json" => Some(OutputFormat::Json),
        "csv" => Some(OutputFormat::Csv),
        "plain" => Some(OutputFormat::Plain),
        "text" => Some(OutputFormat::Text),
        _ => None, // Let adaptive formatter decide
    };

    // Use unified formatter system
    dataprof::output::output_with_adaptive_formatter(&report, ml_score.as_ref(), output_format)?;

    // Save to file if requested
    if let Some(output_path) = &args.output {
        save_output_to_file(output_path, &report, ml_score.as_ref(), &args.format)?;
    }

    Ok(())
}

/// Save formatted output to a file
fn save_output_to_file(
    path: &std::path::Path,
    report: &dataprof::types::QualityReport,
    ml_score: Option<&dataprof::analysis::MlReadinessScore>,
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
    let mut content = formatter.format_report(report)?;

    // Add ML score to JSON output if available
    if format == "json" && ml_score.is_some() {
        let mut json_value: serde_json::Value = serde_json::from_str(&content)?;
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
        content = serde_json::to_string_pretty(&json_value)?;
    }

    fs::write(path, content)?;
    println!("✅ Results saved to: {}", path.display());
    Ok(())
}
