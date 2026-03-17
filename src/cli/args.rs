use clap::ValueEnum;

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
