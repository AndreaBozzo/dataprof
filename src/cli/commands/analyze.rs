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
}
