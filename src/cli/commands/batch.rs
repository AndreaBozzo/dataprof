use clap::Args;
use std::path::PathBuf;

/// Batch processing arguments
#[derive(Debug, Args)]
pub struct BatchArgs {
    /// Directory or glob pattern to process
    pub path: PathBuf,

    /// Scan subdirectories recursively
    #[arg(short, long)]
    pub recursive: bool,

    /// Enable parallel processing
    #[arg(long)]
    pub parallel: bool,

    /// File filter pattern (glob)
    #[arg(long)]
    pub filter: Option<String>,

    /// Output directory for individual reports
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Summary report path
    #[arg(long)]
    pub summary: Option<PathBuf>,

    /// Show detailed per-file reports
    #[arg(long)]
    pub detailed: bool,

    /// Maximum concurrent files (0 = auto)
    #[arg(long, default_value = "0")]
    pub max_concurrent: usize,

    /// ISO quality threshold profile (default, strict, lenient)
    #[arg(long, default_value = "default")]
    pub threshold_profile: String,

    /// Show progress bars
    #[arg(long)]
    pub progress: bool,
}
