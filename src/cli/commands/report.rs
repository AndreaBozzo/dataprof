use clap::Args;
use std::path::PathBuf;

/// Report generation arguments
#[derive(Debug, Args)]
pub struct ReportArgs {
    /// Input file to analyze
    pub file: PathBuf,

    /// Output report path (default: <filename>_report.html)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Report format (html, pdf)
    #[arg(long, default_value = "html")]
    pub format: String,

    /// Custom HTML template path
    #[arg(long)]
    pub template: Option<PathBuf>,

    /// Include code snippets in report
    #[arg(long)]
    pub include_code: bool,

    /// Show detailed ISO metrics
    #[arg(long)]
    pub detailed: bool,

    /// ISO quality threshold profile (default, strict, lenient)
    #[arg(long, default_value = "default")]
    pub threshold_profile: String,

    /// Use streaming for large files
    #[arg(long)]
    pub streaming: bool,
}
