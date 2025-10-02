use clap::Args;
use std::path::PathBuf;

/// Batch processing arguments
#[derive(Debug, Args)]
pub struct BatchArgs {
    /// Directory or glob pattern to process
    pub path: PathBuf,

    /// Scan subdirectories recursively
    #[arg(short, long)]
    pub recursive: bool,

    /// Enable parallel processing
    #[arg(long)]
    pub parallel: bool,

    /// File filter pattern (glob)
    #[arg(long)]
    pub filter: Option<String>,

    /// Output directory for individual reports
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Summary report path
    #[arg(long)]
    pub summary: Option<PathBuf>,

    /// Show detailed per-file reports
    #[arg(long)]
    pub detailed: bool,

    /// Maximum concurrent files (0 = auto)
    #[arg(long, default_value = "0")]
    pub max_concurrent: usize,

    /// ISO quality threshold profile (default, strict, lenient)
    #[arg(long, default_value = "default")]
    pub threshold_profile: String,

    /// Show progress bars
    #[arg(long)]
    pub progress: bool,

    /// Generate HTML batch report
    #[arg(long)]
    pub html: Option<PathBuf>,
}

use anyhow::Result;
use dataprof::output::html::generate_batch_html_report;
use dataprof::{BatchConfig, BatchProcessor};

pub fn execute(args: &BatchArgs) -> Result<()> {
    // Build batch configuration
    let config = BatchConfig {
        parallel: args.parallel,
        max_concurrent: if args.max_concurrent > 0 {
            args.max_concurrent
        } else {
            num_cpus::get()
        },
        recursive: args.recursive,
        html_output: args.html.clone(),
        ..Default::default()
    };

    // Create batch processor with config
    let processor = BatchProcessor::with_config(config);

    println!("🔍 Processing batch...");

    // Process directory
    let batch_result = processor.process_directory(&args.path)?;

    // Generate HTML report if requested
    if let Some(html_path) = &args.html {
        println!("📄 Generating HTML batch report...");
        generate_batch_html_report(&batch_result, html_path)?;
        println!("✅ HTML report saved to: {}", html_path.display());
    }

    // Display summary
    println!("\n📊 Batch Summary:");
    println!("  Total files: {}", batch_result.summary.total_files);
    println!("  Successful: {}", batch_result.summary.successful);
    println!("  Failed: {}", batch_result.summary.failed);
    println!(
        "  Duration: {:.2}s",
        batch_result.summary.processing_time_seconds
    );

    if args.detailed {
        println!("\n📋 Detailed Results:");
        for (path, report) in &batch_result.reports {
            let score = report.quality_score();
            println!("  {} - Quality: {:.1}%", path.display(), score);
        }
    }

    Ok(())
}
