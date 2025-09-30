use clap::Args;
use std::path::PathBuf;

/// ML readiness arguments
#[derive(Debug, Args)]
pub struct MlArgs {
    /// Input file to analyze
    pub file: PathBuf,

    /// Show inline code snippets
    #[arg(long)]
    pub code: bool,

    /// Generate complete preprocessing script
    #[arg(long)]
    pub script: Option<PathBuf>,

    /// Target framework (sklearn, pandas, polars)
    #[arg(long, default_value = "sklearn")]
    pub framework: String,

    /// Show detailed per-feature analysis
    #[arg(long)]
    pub detailed: bool,

    /// Output format (text, json)
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Output file path for analysis results
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Use streaming for large files
    #[arg(long)]
    pub streaming: bool,
}
