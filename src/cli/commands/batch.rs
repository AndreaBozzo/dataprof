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
}
use super::batch::BatchArgs;
use anyhow::Result;
use dataprof::DataProfiler;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

#[derive(Debug)]
struct BatchResult {
    file: PathBuf,
    success: bool,
    quality_score: Option<f64>,
    issue_count: usize,
    error: Option<String>,
}

pub fn execute(args: &BatchArgs) -> Result<()> {
    println!("🔍 Scanning: {}", args.path.display());

    let files = discover_files(&args.path, args.recursive, args.filter.as_deref())?;

    if files.is_empty() {
        println!("⚠️  No files found");
        return Ok(());
    }

    println!("  Found {} files\n", files.len());

    let use_parallel = args.parallel || files.len() >= 3;
    let start_time = Instant::now();

    let results = if use_parallel {
        process_files_parallel(&files, args)?
    } else {
        process_files_sequential(&files, args)?
    };

    print_summary(&results, start_time.elapsed());

    if let Some(summary_path) = &args.summary {
        save_summary(summary_path, &results, start_time.elapsed())?;
    }

    Ok(())
}

fn discover_files(path: &Path, recursive: bool, filter: Option<&str>) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if path.is_file() {
        files.push(path.to_path_buf());
        return Ok(files);
    }

    let walker = if recursive {
        WalkDir::new(path).follow_links(true)
    } else {
        WalkDir::new(path).max_depth(1)
    };

    for entry in walker.into_iter().filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() {
            continue;
        }

        let file_path = entry.path();
        if let Some(ext) = file_path.extension() {
            let ext_str = ext.to_str().unwrap_or("");
            if matches!(ext_str, "csv" | "json" | "jsonl") {
                if let Some(pattern) = filter {
                    if file_path.to_string_lossy().contains(pattern) {
                        files.push(file_path.to_path_buf());
                    }
                } else {
                    files.push(file_path.to_path_buf());
                }
            }
        }
    }

    Ok(files)
}

fn process_files_sequential(files: &[PathBuf], args: &BatchArgs) -> Result<Vec<BatchResult>> {
    let mut results = Vec::new();
    let total = files.len();

    for (i, file) in files.iter().enumerate() {
        if args.progress {
            print!(
                "\r📊 Processing: {}/{} ({:.0}%)",
                i + 1,
                total,
                ((i + 1) as f64 / total as f64) * 100.0
            );
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }

        results.push(process_single_file(file, args));
    }

    if args.progress {
        println!();
    }

    Ok(results)
}

fn process_files_parallel(files: &[PathBuf], args: &BatchArgs) -> Result<Vec<BatchResult>> {
    use rayon::prelude::*;

    println!("📊 Processing (parallel)...");

    let results = files
        .par_iter()
        .map(|file| process_single_file(file, args))
        .collect();

    Ok(results)
}

fn process_single_file(file: &Path, args: &BatchArgs) -> BatchResult {
    let mut profiler = DataProfiler::streaming();

    match profiler.analyze_file(file) {
        Ok(report) => {
            let quality_score = report.data_quality_metrics.as_ref().map(|m| {
                let completeness = 100.0 - m.missing_values_ratio;
                let consistency = m.data_type_consistency;
                let uniqueness = m.key_uniqueness;
                let accuracy = 100.0 - m.outlier_ratio;
                let timeliness = 100.0 - m.stale_data_ratio;
                (completeness + consistency + uniqueness + accuracy + timeliness) / 5.0
            });

            let issue_count = report.issues.len();

            if let Some(output_dir) = &args.output {
                let _ = save_individual_report(output_dir, file, &report);
            }

            BatchResult {
                file: file.to_path_buf(),
                success: true,
                quality_score,
                issue_count,
                error: None,
            }
        }
        Err(e) => BatchResult {
            file: file.to_path_buf(),
            success: false,
            quality_score: None,
            issue_count: 0,
            error: Some(e.to_string()),
        },
    }
}

fn save_individual_report(
    output_dir: &Path,
    file: &Path,
    report: &dataprof::types::QualityReport,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    let file_stem = file
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("report");
    let output_path = output_dir.join(format!("{}_report.json", file_stem));
    let json = serde_json::to_string_pretty(report)?;
    fs::write(output_path, json)?;
    Ok(())
}

fn print_summary(results: &[BatchResult], elapsed: std::time::Duration) {
    let success_count = results.iter().filter(|r| r.success).count();
    let total = results.len();

    println!(
        "\n✅ Batch Complete: {} files in {:.1}s",
        total,
        elapsed.as_secs_f64()
    );
    println!("\n📈 Summary:");
    println!(
        "  Success Rate: {:.0}% ({}/{})",
        (success_count as f64 / total as f64) * 100.0,
        success_count,
        total
    );

    if success_count > 0 {
        let avg_quality: f64 =
            results.iter().filter_map(|r| r.quality_score).sum::<f64>() / success_count as f64;

        println!("  Avg Quality: {:.1}%", avg_quality);

        let total_issues: usize = results.iter().map(|r| r.issue_count).sum();
        println!("  Total Issues: {}", total_issues);
    }

    let failures: Vec<_> = results.iter().filter(|r| !r.success).collect();
    if !failures.is_empty() {
        println!("\n❌ Failed Files ({}):", failures.len());
        for result in failures.iter().take(5) {
            println!(
                "  • {}: {}",
                result.file.display(),
                result.error.as_deref().unwrap_or("Unknown error")
            );
        }
    }
}

fn save_summary(path: &Path, results: &[BatchResult], elapsed: std::time::Duration) -> Result<()> {
    let success_count = results.iter().filter(|r| r.success).count();
    let mut summary = format!(
        "# Batch Processing Summary\n\nTotal: {}\nSuccess: {}\nDuration: {:.1}s\n\n",
        results.len(),
        success_count,
        elapsed.as_secs_f64()
    );

    summary.push_str("| File | Status | Quality |\n|------|--------|---------|\n");
    for result in results {
        let status = if result.success { "✅" } else { "❌" };
        let quality = result
            .quality_score
            .map(|s| format!("{:.1}%", s))
            .unwrap_or_else(|| "N/A".to_string());
        summary.push_str(&format!(
            "| {} | {} | {} |\n",
            result.file.display(),
            status,
            quality
        ));
    }

    fs::write(path, summary)?;
    println!("\n💾 Summary saved to: {}", path.display());
    Ok(())
}
