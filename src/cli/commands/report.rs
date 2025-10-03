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
use anyhow::{Context, Result};
use dataprof::{generate_html_report, DataProfiler};
use std::fs;

pub fn execute(args: &ReportArgs) -> Result<()> {
    println!("📊 Generating Report...");

    let mut profiler = DataProfiler::streaming();
    let report = profiler.analyze_file(&args.file)?;

    let output_path = args.output.clone().unwrap_or_else(|| {
        let mut path = args.file.clone();
        let file_stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("report");
        path.set_file_name(format!("{}_report", file_stem));
        path.set_extension("html");
        path
    });

    generate_html_report(&report, &output_path).context("Failed to generate HTML report")?;

    let file_size = fs::metadata(&output_path)?.len();
    let size_mb = file_size as f64 / 1_048_576.0;

    println!(
        "\n📄 Report saved: {} ({:.1} MB)",
        output_path.display(),
        size_mb
    );

    if cfg!(target_os = "windows") {
        println!("💡 Open with: start {}", output_path.display());
    } else if cfg!(target_os = "macos") {
        println!("💡 Open with: open {}", output_path.display());
    } else {
        println!("💡 Open with: xdg-open {}", output_path.display());
    }

    Ok(())
}
