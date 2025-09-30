use super::batch::BatchArgs;
use dataprof::DataProfiler;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

/// Execute the batch command - process multiple files efficiently
pub fn execute(args: &BatchArgs) -> Result<()> {
    println!("🔍 Scanning: {}", args.path.display());

    // Discover files
    let files = discover_files(&args.path, args.recursive, args.filter.as_deref())?;

    if files.is_empty() {
        println!("⚠️  No files found matching criteria");
        return Ok(());
    }

    println!("  Found {} files\n", files.len());

    // Smart defaults: auto-enable parallel for 3+ files
    let use_parallel = args.parallel || (files.len() >= 3 && args.max_concurrent == 0);

    let start_time = Instant::now();

    // Process files
    let results = if use_parallel {
        process_files_parallel(&files, args)?
    } else {
        process_files_sequential(&files, args)?
    };

    let elapsed = start_time.elapsed();

    // Print summary
    print_summary(&results, elapsed);

    // Save summary if requested
    if let Some(summary_path) = &args.summary {
        save_summary(summary_path, &results, elapsed)?;
    }

    Ok(())
}

#[derive(Debug)]
struct BatchResult {
    file: PathBuf,
    success: bool,
    quality_score: Option<f64>,
    issue_count: usize,
    error: Option<String>,
}

fn discover_files(path: &Path, recursive: bool, filter: Option<&str>) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if path.is_file() {
        files.push(path.to_path_buf());
        return Ok(files);
    }

    if !path.is_dir() {
        return Err(anyhow::anyhow!("Path is neither file nor directory: {}", path.display()));
    }

    // Scan directory
    let entries = if recursive {
        walkdir::WalkDir::new(path)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .collect::<Vec<_>>()
    } else {
        fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            .map(|e| walkdir::DirEntry::from(e))
            .collect::<Vec<_>>()
    };

    for entry in entries {
        let file_path = entry.path();

        // Check extension (CSV, JSON, JSONL)
        if let Some(ext) = file_path.extension() {
            let ext_str = ext.to_str().unwrap_or("");
            if matches!(ext_str, "csv" | "json" | "jsonl") {
                // Apply filter if provided
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
            print!("\r📊 Processing: {}/{} ({:.0}%)", i + 1, total, ((i + 1) as f64 / total as f64) * 100.0);
            let _ = std::io::Write::flush(&mut std::io::stdout());
        }

        let result = process_single_file(file, args);
        results.push(result);
    }

    if args.progress {
        println!(); // New line after progress
    }

    Ok(results)
}

fn process_files_parallel(files: &[PathBuf], args: &BatchArgs) -> Result<Vec<BatchResult>> {
    use rayon::prelude::*;

    println!("📊 Processing (parallel)...");

    // Determine thread count
    let max_concurrent = if args.max_concurrent == 0 {
        num_cpus::get()
    } else {
        args.max_concurrent
    };

    // Configure rayon thread pool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(max_concurrent)
        .build()?;

    let results = pool.install(|| {
        files
            .par_iter()
            .map(|file| process_single_file(file, args))
            .collect::<Vec<_>>()
    });

    Ok(results)
}

fn process_single_file(file: &Path, args: &BatchArgs) -> BatchResult {
    let profiler = DataProfiler::new();

    match profiler.analyze_file(file) {
        Ok(report) => {
            let quality_score = report.data_quality_metrics.as_ref().map(|m| {
                let completeness = 100.0 - m.missing_values_ratio;
                let consistency = 100.0 - m.mixed_type_ratio;
                let uniqueness = 100.0 - m.duplicate_ratio;
                let accuracy = 100.0 - m.outlier_ratio;
                let timeliness = 100.0 - m.stale_data_ratio;
                (completeness + consistency + uniqueness + accuracy + timeliness) / 5.0
            });

            let issue_count = report.data_quality_metrics.as_ref().map(|m| {
                let mut count = 0;
                if m.missing_values_ratio > 10.0 { count += 1; }
                if m.duplicate_rows > 0 { count += 1; }
                if m.outlier_ratio > 5.0 { count += 1; }
                if m.future_dates_count > 0 { count += 1; }
                if !m.temporal_violations.is_empty() { count += 1; }
                count
            }).unwrap_or(0);

            // Save individual report if output directory specified
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
    report: &dataprof::types::DataReport,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;

    let file_stem = file.file_stem().and_then(|s| s.to_str()).unwrap_or("report");
    let output_path = output_dir.join(format!("{}_report.json", file_stem));

    let json = serde_json::to_string_pretty(report)?;
    fs::write(output_path, json)?;

    Ok(())
}

fn print_summary(results: &[BatchResult], elapsed: std::time::Duration) {
    let success_count = results.iter().filter(|r| r.success).count();
    let total = results.len();
    let success_rate = (success_count as f64 / total as f64) * 100.0;

    println!("\n✅ Batch Complete: {} files in {:.1}s", total, elapsed.as_secs_f64());
    println!("\n📈 Summary:");
    println!("  Success Rate: {:.0}% ({}/{})", success_rate, success_count, total);

    if success_count > 0 {
        let avg_quality: f64 = results
            .iter()
            .filter_map(|r| r.quality_score)
            .sum::<f64>() / success_count as f64;

        println!("  Avg Quality: {:.1}% - {}", avg_quality, get_quality_label(avg_quality));

        let total_issues: usize = results.iter().map(|r| r.issue_count).sum();
        println!("  Total Issues: {}", total_issues);
    }

    // Show failures if any
    let failures: Vec<_> = results.iter().filter(|r| !r.success).collect();
    if !failures.is_empty() {
        println!("\n❌ Failed Files ({}):", failures.len());
        for result in failures.iter().take(5) {
            println!("  • {}: {}", result.file.display(), result.error.as_deref().unwrap_or("Unknown error"));
        }
        if failures.len() > 5 {
            println!("  ... and {} more", failures.len() - 5);
        }
    }
}

fn get_quality_label(score: f64) -> &'static str {
    match score {
        s if s >= 95.0 => "EXCELLENT",
        s if s >= 85.0 => "GOOD",
        s if s >= 70.0 => "FAIR",
        s if s >= 50.0 => "POOR",
        _ => "CRITICAL",
    }
}

fn save_summary(path: &Path, results: &[BatchResult], elapsed: std::time::Duration) -> Result<()> {
    let success_count = results.iter().filter(|r| r.success).count();
    let total = results.len();

    let mut summary = String::new();
    summary.push_str(&format!("# Batch Processing Summary\n\n"));
    summary.push_str(&format!("Total files: {}\n", total));
    summary.push_str(&format!("Success: {}\n", success_count));
    summary.push_str(&format!("Failed: {}\n", total - success_count));
    summary.push_str(&format!("Duration: {:.1}s\n\n", elapsed.as_secs_f64()));

    summary.push_str("## Results\n\n");
    summary.push_str("| File | Status | Quality | Issues |\n");
    summary.push_str("|------|--------|---------|--------|\n");

    for result in results {
        let status = if result.success { "✅" } else { "❌" };
        let quality = result.quality_score.map(|s| format!("{:.1}%", s)).unwrap_or_else(|| "N/A".to_string());
        summary.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            result.file.display(),
            status,
            quality,
            result.issue_count
        ));
    }

    fs::write(path, summary)?;
    println!("\n💾 Summary saved to: {}", path.display());

    Ok(())
}