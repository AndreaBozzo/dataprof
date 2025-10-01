// Command modules (args + implementation)
pub mod analyze;
pub mod batch;
pub mod ml;
pub mod report;

// Utility modules
pub mod benchmark;
pub mod script_generator;

#[cfg(feature = "database")]
pub mod database;

use clap::Subcommand;
use std::path::{Path, PathBuf};

/// Helper to detect JSON files
pub fn is_json_file(path: &Path) -> bool {
    if let Some(extension) = path.extension() {
        matches!(extension.to_str(), Some("json") | Some("jsonl"))
    } else {
        false
    }
}

/// DataProf subcommands
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Full analysis with ISO 8000/25012 metrics
    ///
    /// Comprehensive analysis with all 5 quality dimensions:
    /// Completeness, Consistency, Uniqueness, Accuracy, Timeliness
    ///
    /// Examples:
    ///   dataprof analyze data.csv
    ///   dataprof analyze data.csv --ml
    ///   dataprof analyze data.csv --detailed
    Analyze(analyze::AnalyzeArgs),

    /// ML readiness analysis and code generation
    ///
    /// Analyzes data suitability for ML and generates preprocessing code.
    ///
    /// Examples:
    ///   dataprof ml data.csv
    ///   dataprof ml data.csv --script preprocess.py
    Ml(ml::MlArgs),

    /// Generate comprehensive HTML/PDF reports
    ///
    /// Creates web-based reports with visualizations and metrics.
    ///
    /// Examples:
    ///   dataprof report data.csv
    ///   dataprof report data.csv -o custom_report.html
    Report(report::ReportArgs),

    /// Process multiple files in batch
    ///
    /// Efficiently process directories or file patterns.
    ///
    /// Examples:
    ///   dataprof batch examples/
    ///   dataprof batch examples/ --recursive --parallel
    Batch(batch::BatchArgs),
}

/// Common analysis options inherited by all commands
/// These ensure all commands get the same improvements (progress, config, etc.)
#[derive(Debug, clap::Args)]
pub struct CommonAnalysisOptions {
    /// Show real-time progress bars during analysis
    #[arg(long)]
    pub progress: bool,

    /// Custom chunk size for streaming (e.g., 10000)
    #[arg(long)]
    pub chunk_size: Option<usize>,

    /// Load settings from TOML config file
    #[arg(long)]
    pub config: Option<PathBuf>,
}

/// Common output options shared across commands
#[derive(Debug, clap::Args)]
pub struct CommonOptions {
    /// Output format (text, json, csv, plain)
    #[arg(long, value_enum, default_value = "text")]
    pub format: crate::cli::args::CliOutputFormat,

    /// Output file path (default: stdout)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Verbosity level (-v, -vv, -vvv)
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    pub verbosity: u8,

    /// Disable colored output
    #[arg(long)]
    pub no_color: bool,
}
