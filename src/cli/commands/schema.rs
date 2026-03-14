use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

/// Schema inference arguments
#[derive(Debug, Args)]
pub struct SchemaArgs {
    /// Input file to infer schema from
    pub file: PathBuf,

    /// Output format (text, json)
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Output file path
    #[arg(short, long)]
    pub output: Option<PathBuf>,
}

pub fn execute(args: &SchemaArgs) -> Result<()> {
    let result = dataprof::infer_schema(&args.file)?;

    let output = match args.format.as_str() {
        "json" => serde_json::to_string_pretty(&result)?,
        _ => format_schema_text(&args.file, &result),
    };

    if let Some(path) = &args.output {
        std::fs::write(path, &output)?;
    } else {
        println!("{}", output);
    }

    Ok(())
}

fn format_schema_text(path: &std::path::Path, result: &dataprof::SchemaResult) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Schema for {} ({} rows sampled, {}ms):\n",
        path.display(),
        result.rows_sampled,
        result.inference_time_ms,
    ));

    // Find max column name length for alignment
    let max_name_len = result
        .columns
        .iter()
        .map(|c| c.name.len())
        .max()
        .unwrap_or(0);

    for col in &result.columns {
        out.push_str(&format!(
            "  {:<width$}  {:?}\n",
            col.name,
            col.data_type,
            width = max_name_len,
        ));
    }

    out
}
