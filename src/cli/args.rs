use clap::ValueEnum;
use serde::Serialize;

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
}
