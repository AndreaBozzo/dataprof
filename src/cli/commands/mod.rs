// Command modules (args + implementation)
pub mod analyze;
pub mod batch;
pub mod info;
pub mod report;

// Utility modules
pub mod benchmark;

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
#[cfg(feature = "parquet")]
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

    /// Analyze database tables or queries
    ///
    /// Profile data directly from databases (PostgreSQL, MySQL, SQLite, DuckDB).
    ///
    /// Examples:
    ///   dataprof database postgres://user:pass@host/db --table users --quality
    ///   dataprof database sqlite://data.db --query "SELECT * FROM users"
    #[cfg(feature = "database")]
    Database(database::DatabaseArgs),

    /// Benchmark different engines on your data
    ///
    /// Compare streaming, memory-efficient, and Arrow engines.
    ///
    /// Examples:
    ///   dataprof benchmark data.csv
    ///   dataprof benchmark large_file.csv
    Benchmark(benchmark::BenchmarkArgs),

    /// Show engine information and system capabilities
    ///
    /// Display available processing engines, system resources, and recommendations.
    ///
    /// Examples:
    ///   dataprof info
    ///   dataprof info --detailed
    Info(info::InfoArgs),
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
