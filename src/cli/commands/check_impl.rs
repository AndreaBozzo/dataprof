use super::check::CheckArgs;
use anyhow::Result;
use dataprof::DataProfiler;
use std::fs;

/// Execute the check command - quick quality check with smart defaults
pub fn execute(args: &CheckArgs) -> Result<()> {
    // Smart defaults: auto-enable streaming for large files
    let file_size_mb = get_file_size_mb(&args.file)?;
    let _use_streaming = args.streaming || file_size_mb > 100.0;

    // Create profiler (always use streaming as there's no new() method)
    let mut profiler = DataProfiler::streaming();

    // Run analysis
    let report = profiler.analyze_file(&args.file)?;

    // Output results
    if args.json {
        let json = serde_json::to_string_pretty(&report)?;
        println!("{}", json);
    } else {
        print_quick_summary(&report, args.detailed, args.iso);
    }

    // Save to file if requested
    if let Some(output_path) = &args.output {
        let content = if args.json {
            serde_json::to_string_pretty(&report)?
        } else {
            format!("{:#?}", report)
        };
        fs::write(output_path, content)?;
        println!("\n✅ Results saved to: {}", output_path.display());
    }

    Ok(())
}

fn get_file_size_mb(path: &std::path::Path) -> Result<f64> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.len() as f64 / 1_048_576.0)
}

fn print_quick_summary(report: &dataprof::types::QualityReport, detailed: bool, show_iso: bool) {
    println!("\n📊 Quick Quality Check\n");

    // Basic info
    println!(
        "📄 File: {} rows, {} columns",
        report.file_info.total_rows.unwrap_or(0),
        report.file_info.total_columns
    );

    // Quality metrics if available
    if let Some(metrics) = &report.data_quality_metrics {
        let quality_score = calculate_overall_quality(metrics);
        let label = get_quality_label(quality_score);

        println!("\n✅ Overall Quality: {:.1}% - {}", quality_score, label);

        let issues = collect_issues(metrics);
        if !issues.is_empty() {
            println!("\n⚠️  Issues found ({}):", issues.len());
            for issue in &issues {
                println!("  • {}", issue);
            }
        } else {
            println!("\n✅ No major issues detected");
        }

        if show_iso {
            print_iso_metrics(metrics);
        }

        if detailed {
            print_detailed_metrics(metrics);
        }

        if !issues.is_empty() {
            println!("\n💡 Run 'dataprof analyze <file> --detailed' for full ISO report");
        }
    }
}

fn calculate_overall_quality(metrics: &dataprof::types::DataQualityMetrics) -> f64 {
    let completeness = 100.0 - metrics.missing_values_ratio;
    let consistency = metrics.data_type_consistency;
    let uniqueness = metrics.key_uniqueness;
    let accuracy = 100.0 - metrics.outlier_ratio;
    let timeliness = 100.0 - metrics.stale_data_ratio;

    (completeness + consistency + uniqueness + accuracy + timeliness) / 5.0
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

fn collect_issues(metrics: &dataprof::types::DataQualityMetrics) -> Vec<String> {
    let mut issues = Vec::new();

    if metrics.missing_values_ratio > 10.0 {
        issues.push(format!(
            "Completeness: {:.1}% missing values",
            metrics.missing_values_ratio
        ));
    }

    if metrics.duplicate_rows > 0 {
        issues.push(format!(
            "Uniqueness: {} duplicate rows",
            metrics.duplicate_rows
        ));
    }

    if metrics.outlier_ratio > 5.0 {
        issues.push(format!(
            "Accuracy: {:.1}% outliers detected",
            metrics.outlier_ratio
        ));
    }

    if metrics.future_dates_count > 0 || metrics.temporal_violations > 0 {
        let temporal = metrics.future_dates_count + metrics.temporal_violations;
        issues.push(format!("Timeliness: {} temporal issues", temporal));
    }

    if metrics.high_cardinality_warning {
        issues.push("High cardinality warning".to_string());
    }

    issues
}

fn print_iso_metrics(metrics: &dataprof::types::DataQualityMetrics) {
    println!("\n📊 ISO 8000/25012 Compliance:");
    println!(
        "  🔍 Completeness: {:.1}%",
        100.0 - metrics.missing_values_ratio
    );
    println!("  ⚡ Consistency: {:.1}%", metrics.data_type_consistency);
    println!("  🔑 Uniqueness: {:.1}%", metrics.key_uniqueness);
    println!("  🎯 Accuracy: {:.1}%", 100.0 - metrics.outlier_ratio);
    println!("  ⏰ Timeliness: {:.1}%", 100.0 - metrics.stale_data_ratio);
}

fn print_detailed_metrics(metrics: &dataprof::types::DataQualityMetrics) {
    println!("\n📋 Detailed Metrics:");
    println!("  Missing values: {:.2}%", metrics.missing_values_ratio);
    println!("  Duplicate rows: {}", metrics.duplicate_rows);
    println!("  Outliers: {:.2}%", metrics.outlier_ratio);
    println!(
        "  Data type consistency: {:.2}%",
        metrics.data_type_consistency
    );
    println!(
        "  High cardinality warning: {}",
        metrics.high_cardinality_warning
    );

    if metrics.future_dates_count > 0 {
        println!("  Future dates: {}", metrics.future_dates_count);
    }
    if metrics.temporal_violations > 0 {
        println!("  Temporal violations: {}", metrics.temporal_violations);
    }
}
