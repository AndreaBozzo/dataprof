// Command modules
pub mod analyze;
pub mod batch;
pub mod check;
pub mod ml;
pub mod report;

// Command implementations
pub mod analyze_impl;
pub mod batch_impl;
pub mod check_impl;
pub mod ml_impl;
pub mod report_impl;

use clap::Subcommand;
use std::path::PathBuf;

/// DataProf subcommands
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Quick quality check (90% use cases)
    ///
    /// Performs fast analysis with focus on critical issues.
    /// Shows only problems, not all metrics.
    ///
    /// Examples:
    ///   dataprof check data.csv
    ///   dataprof check data.csv --detailed
    Check(check::CheckArgs),

    /// Full analysis with ISO 8000/25012 metrics
    ///
    /// Comprehensive analysis with all 5 quality dimensions:
    /// Completeness, Consistency, Uniqueness, Accuracy, Timeliness
    ///
    /// Examples:
    ///   dataprof analyze data.csv
    ///   dataprof analyze data.csv --ml
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

/// Common options shared across commands
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
