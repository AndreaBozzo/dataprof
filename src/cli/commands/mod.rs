// Command modules
pub mod analyze;

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

/// Helper to detect Parquet files
pub fn is_parquet_file(path: &Path) -> bool {
    if let Some(extension) = path.extension() {
        matches!(extension.to_str(), Some("parquet"))
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
    ///   dataprof analyze data.csv --detailed
    Analyze(analyze::AnalyzeArgs),

    /// Analyze database tables or queries
    ///
    /// Profile data directly from databases (PostgreSQL, MySQL, SQLite, DuckDB).
    ///
    /// Examples:
    ///   dataprof database postgres://user:pass@host/db --table users --quality
    ///   dataprof database sqlite://data.db --query "SELECT * FROM users"
    #[cfg(feature = "database")]
    Database(database::DatabaseArgs),
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

    /// Verbosity level (-v, -vv, -vvv for increasing verbosity)
    #[arg(short = 'v', long = "verbose", action = clap::ArgAction::Count)]
    pub verbosity: u8,
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
