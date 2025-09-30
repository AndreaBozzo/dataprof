use clap::Args;
use std::path::PathBuf;

/// Quick quality check arguments
#[derive(Debug, Args)]
pub struct CheckArgs {
    /// Input file to analyze
    pub file: PathBuf,

    /// Show ISO 8000/25012 compliance metrics
    #[arg(long)]
    pub iso: bool,

    /// Show detailed breakdown of all dimensions
    #[arg(long)]
    pub detailed: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,

    /// Output file path
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Use streaming for large files (auto-detected by default)
    #[arg(long)]
    pub streaming: bool,

    /// ISO quality threshold profile (default, strict, lenient)
    #[arg(long, default_value = "default")]
    pub threshold_profile: String,
}
