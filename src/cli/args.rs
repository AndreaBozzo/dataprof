use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::path::PathBuf;

#[derive(Clone, Debug, ValueEnum)]
pub enum CliOutputFormat {
    /// Human-readable text output
    Text,
    /// Machine-readable JSON output
    Json,
    /// CSV format for data processing
    Csv,
    /// Plain text without formatting for scripting
    Plain,
}

impl From<CliOutputFormat> for dataprof::OutputFormat {
    fn from(val: CliOutputFormat) -> Self {
        match val {
            CliOutputFormat::Text => dataprof::OutputFormat::Text,
            CliOutputFormat::Json => dataprof::OutputFormat::Json,
            CliOutputFormat::Csv => dataprof::OutputFormat::Csv,
            CliOutputFormat::Plain => dataprof::OutputFormat::Plain,
        }
    }
}

#[derive(Clone, Debug, ValueEnum)]
pub enum EngineChoice {
    /// Automatic intelligent selection (RECOMMENDED)
    Auto,
    /// Standard streaming engine
    Streaming,
    /// Memory-efficient streaming engine
    MemoryEfficient,
    /// True streaming for very large files
    TrueStreaming,
    /// Apache Arrow columnar engine (requires arrow feature)
    Arrow,
}

#[derive(Serialize)]
#[allow(dead_code)] // Used in different feature combinations
pub struct BenchmarkOutput {
    pub rows: usize,
    pub columns: usize,
    pub file_size_mb: f64,
    pub scan_time_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality_score: Option<f64>,
}

#[derive(Parser)]
#[command(name = "dataprof")]
#[command(
    about = "Fast CSV data profiler with quality checking - v0.5.0 Subcommands Edition",
    long_about = r#"DataProfiler CLI - Fast CSV data profiling with ML readiness scoring

A powerful command-line tool for analyzing CSV, JSON, and JSONL files with:
• Quality assessment and data profiling
• ML readiness scoring with preprocessing recommendations
• Multiple output formats (JSON, CSV, HTML, plain text)
• Batch processing with progress indicators
• Configuration file support

EXAMPLES:
  # Basic file analysis
  dataprof data.csv

  # Generate HTML report with ML scoring and code snippets
  dataprof data.csv --html report.html --ml-score --ml-code

  # Batch process directory with progress
  dataprof /data/folder --progress --recursive

  # Use glob pattern for multiple files
  dataprof --glob "data/**/*.csv" --format json

  # Sample large files for faster analysis
  dataprof large_file.csv --sample 10000 --streaming

  # Custom configuration
  dataprof data.csv --config .dataprof.toml --verbose

  # Quality-focused analysis with detailed output
  dataprof data.csv --quality --format csv --verbosity 2

  # Generate ML preprocessing script
  dataprof data.csv --ml-score --ml-code --output-script preprocess.py

For more information, visit: https://github.com/AndreaBozzo/dataprof"#
)]
pub struct Cli {
    /// Input file, directory, or glob pattern to analyze
    ///
    /// Examples:
    ///   data.csv              Single CSV file
    ///   /data/folder          Directory (use with --recursive)
    ///   "data/**/*.csv"       Glob pattern (use with --glob)
    pub file: PathBuf,

    /// Enable comprehensive data quality assessment
    ///
    /// Performs detailed quality checks including missing value analysis,
    /// data type validation, pattern detection, and outlier identification.
    #[arg(short, long)]
    pub quality: bool,

    /// Generate interactive HTML report at specified path
    ///
    /// Creates a comprehensive web-based report with visualizations,
    /// quality metrics, and recommendations. Example: --html report.html
    #[arg(long)]
    pub html: Option<PathBuf>,

    /// Enable streaming mode for large files (recommended for >100MB)
    ///
    /// Processes files in chunks to minimize memory usage. Essential for
    /// very large datasets or memory-constrained environments.
    #[arg(long)]
    pub streaming: bool,

    /// Show real-time progress indicators (requires --streaming)
    ///
    /// Displays progress bars and processing status for batch operations.
    /// Automatically enabled for long-running operations.
    #[arg(long)]
    pub progress: bool,

    /// Chunk size for streaming mode (rows per chunk)
    ///
    /// Controls memory usage vs processing speed trade-off. Auto-detected
    /// based on available memory if not specified. Example: --chunk-size 1000
    #[arg(long)]
    pub chunk_size: Option<usize>,

    /// Sample size for large file analysis (process only N rows)
    ///
    /// Useful for quick analysis of very large files. Randomly samples
    /// from the entire file for representative results. Example: --sample 10000
    #[arg(long)]
    pub sample: Option<usize>,

    /// Enable recursive directory scanning
    ///
    /// When processing directories, scan all subdirectories for supported files.
    /// Respects exclude patterns and file type filters.
    #[arg(short, long)]
    pub recursive: bool,

    /// Glob pattern for batch file processing
    ///
    /// Process multiple files matching a pattern. Supports standard glob syntax:
    /// *, **, ?, [abc], {csv,json}. Example: --glob "data/**/*.csv"
    #[arg(long)]
    pub glob: Option<String>,

    /// Enable parallel processing for multiple files
    ///
    /// Processes multiple files concurrently to improve performance.
    /// Automatically manages thread pool based on system capabilities.
    #[arg(long)]
    pub parallel: bool,

    /// Maximum concurrent files for batch processing (0 = auto-detect)
    ///
    /// Controls parallelism level for multi-file operations. Auto-detection
    /// uses CPU count. Lower values reduce memory usage.
    #[arg(long, default_value = "0")]
    pub max_concurrent: usize,

    /// Database connection string (requires --database feature)
    #[cfg(feature = "database")]
    #[arg(long)]
    pub database: Option<String>,

    /// SQL query to execute (use with --database)
    #[cfg(feature = "database")]
    #[arg(long)]
    pub query: Option<String>,

    /// Batch size for database streaming (default: 10000)
    #[cfg(feature = "database")]
    #[arg(long, default_value = "10000")]
    pub batch_size: usize,

    /// Output format for results
    ///
    /// Available formats:
    /// • text: Human-readable summary (default)
    /// • json: Machine-readable JSON output
    /// • csv:  Structured CSV format
    /// • plain: Simple text without formatting
    #[arg(long, value_enum, default_value = "text")]
    pub format: CliOutputFormat,

    /// Processing engine selection (default: auto for intelligent selection)
    ///
    /// Available engines: auto, streaming, memory-mapped, standard.
    /// Auto mode selects the best engine based on file size and system resources.
    #[arg(long, value_enum, default_value = "auto")]
    pub engine: EngineChoice,

    /// Enable performance benchmark mode
    ///
    /// Runs comprehensive performance tests on the provided file.
    /// Measures processing speed, memory usage, and scalability metrics.
    #[arg(long)]
    pub benchmark: bool,

    /// Show detailed engine information and exit
    ///
    /// Displays version, build info, feature flags, and system capabilities.
    /// Useful for debugging and support purposes.
    #[arg(long)]
    pub engine_info: bool,

    /// Enable ML readiness scoring and recommendations
    ///
    /// Analyzes data suitability for machine learning applications:
    /// • Feature quality assessment • Encoding recommendations
    /// • Preprocessing suggestions  • Blocking issue detection
    #[arg(long)]
    pub ml_score: bool,

    /// Generate actionable code snippets for ML preprocessing
    ///
    /// Provides ready-to-use Python code snippets for each ML recommendation:
    /// • Pandas and scikit-learn implementations
    /// • Context-aware parameter substitution
    /// • Required import statements
    #[arg(long)]
    pub ml_code: bool,

    /// Output path for generated preprocessing script
    ///
    /// Creates a complete Python script with all preprocessing steps
    /// based on ML recommendations. Example: --output-script preprocess.py
    #[arg(long)]
    pub output_script: Option<PathBuf>,

    /// Configuration file path (.toml format)
    ///
    /// Load settings from TOML configuration file. Auto-discovers
    /// .dataprof.toml in current/parent directories. Example: --config ~/.dataprof.toml
    #[arg(long)]
    pub config: Option<PathBuf>,

    /// Verbosity level (can be used multiple times: -v, -vv, -vvv)
    ///
    /// • 0: Quiet mode (errors only)
    /// • 1: Normal output with basic info
    /// • 2: Verbose with detailed progress
    /// • 3: Debug mode with trace information
    #[arg(short = 'v', long, action = clap::ArgAction::Count)]
    pub verbosity: u8,

    /// Disable colored output (also set NO_COLOR environment variable)
    ///
    /// Forces plain text output without ANSI color codes.
    /// Useful for CI/CD, logging, or terminal compatibility issues.
    #[arg(long)]
    pub no_color: bool,
}
