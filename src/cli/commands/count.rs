use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

/// Row count arguments
#[derive(Debug, Args)]
pub struct CountArgs {
    /// Input file to count rows in
    pub file: PathBuf,

    /// Output format (text, json)
    #[arg(long, default_value = "text")]
    pub format: String,
}

pub fn execute(args: &CountArgs) -> Result<()> {
    let result = dataprof::quick_row_count(&args.file)?;

    let output = match args.format.as_str() {
        "json" => serde_json::to_string_pretty(&result)?,
        _ => format_count_text(&result),
    };

    println!("{}", output);
    Ok(())
}

fn format_count_text(result: &dataprof::RowCountEstimate) -> String {
    let exact_str = if result.exact { "exact" } else { "estimated" };
    let method_str = match result.method {
        dataprof::CountMethod::ParquetMetadata => "parquet metadata",
        dataprof::CountMethod::FullScan => "full scan",
        dataprof::CountMethod::Sampling => "sampling",
        dataprof::CountMethod::StreamFullScan => "stream full scan",
    };

    let prefix = if result.exact { "" } else { "~" };

    format!(
        "Row count: {}{} ({}, {}, {}ms)",
        prefix, result.count, exact_str, method_str, result.count_time_ms,
    )
}
