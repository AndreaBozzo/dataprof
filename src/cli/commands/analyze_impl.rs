use super::analyze::AnalyzeArgs;
use dataprof::analysis::MetricsCalculator;
use dataprof::core::config::IsoQualityThresholds;
use dataprof::DataProfiler;
use anyhow::Result;
use std::fs;

/// Execute the analyze command - comprehensive ISO 8000/25012 analysis
pub fn execute(args: &AnalyzeArgs) -> Result<()> {
    // Smart defaults: auto-enable streaming for large files
    let file_size_mb = get_file_size_mb(&args.file)?;
    let use_streaming = args.streaming || file_size_mb > 100.0;

    // Create profiler with optional sampling
    let mut profiler = if use_streaming {
        DataProfiler::streaming()
    } else {
        DataProfiler::new()
    };

    // Apply sampling if requested
    if let Some(sample_size) = args.sample {
        profiler = profiler.with_sample_size(sample_size);
    }

    // Run analysis
    let report = profiler.analyze_file(&args.file)?;

    // Get ISO thresholds
    let thresholds = match args.threshold_profile.as_str() {
        "strict" => IsoQualityThresholds::strict(),
        "lenient" => IsoQualityThresholds::lenient(),
        _ => IsoQualityThresholds::default(),
    };

    // Recalculate metrics with custom thresholds if needed
    let metrics = if args.threshold_profile != "default" && report.data_quality_metrics.is_some() {
        // Get column profiles and data
        let calculator = MetricsCalculator::with_thresholds(thresholds);
        // Note: We'd need to recalculate here, but for now use existing metrics
        report.data_quality_metrics.clone()
    } else {
        report.data_quality_metrics.clone()
    };

    // Output based on format
    match args.format.as_str() {
        "json" => print_json_output(&report)?,
        "csv" => print_csv_output(&report)?,
        _ => print_text_output(&report, metrics.as_ref(), args.detailed, args.ml)?,
    }

    // Save to file if requested
    if let Some(output_path) = &args.output {
        save_output(output_path, &report, &args.format)?;
    }

    Ok(())
}

fn get_file_size_mb(path: &std::path::Path) -> Result<f64> {
    let metadata = fs::metadata(path)?;
    Ok(metadata.len() as f64 / 1_048_576.0)
}

fn print_text_output(
    report: &dataprof::types::DataReport,
    metrics: Option<&dataprof::types::DataQualityMetrics>,
    detailed: bool,
    include_ml: bool,
) -> Result<()> {
    println!("\n📊 COMPREHENSIVE ISO 8000/25012 Analysis\n");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    // Basic stats
    println!("\n📄 Dataset Overview:");
    println!("  Rows: {}", report.basic_stats.total_rows);
    println!("  Columns: {}", report.basic_stats.total_columns);

    // ISO 8000/25012 Five Dimensions
    if let Some(m) = metrics {
        println!("\n📊 ISO 8000/25012 Quality Dimensions:\n");

        let completeness = 100.0 - m.missing_values_ratio;
        let consistency = 100.0 - m.mixed_type_ratio;
        let uniqueness = 100.0 - m.duplicate_ratio;
        let accuracy = 100.0 - m.outlier_ratio;
        let timeliness = 100.0 - m.stale_data_ratio;

        println!("  🔍 Completeness: {:>6.1}% {}", completeness, quality_bar(completeness));
        println!("  ⚡ Consistency:  {:>6.1}% {}", consistency, quality_bar(consistency));
        println!("  🔑 Uniqueness:   {:>6.1}% {}", uniqueness, quality_bar(uniqueness));
        println!("  🎯 Accuracy:     {:>6.1}% {}", accuracy, quality_bar(accuracy));
        println!("  ⏰ Timeliness:   {:>6.1}% {}", timeliness, quality_bar(timeliness));

        let overall = (completeness + consistency + uniqueness + accuracy + timeliness) / 5.0;
        println!("\n  📈 Overall Quality: {:.1}% - {}", overall, get_quality_label(overall));

        if detailed {
            print_detailed_breakdown(m);
        }
    }

    // ML readiness if requested
    if include_ml {
        if let Some(ml_insights) = &report.ml_readiness_insights {
            println!("\n🤖 ML Readiness Analysis:\n");
            println!("  Score: {}%", ml_insights.score);
            println!("  Status: {}", ml_insights.readiness_status);

            if !ml_insights.blocking_issues.is_empty() {
                println!("\n  ❌ Blocking Issues:");
                for issue in &ml_insights.blocking_issues {
                    println!("    • {}", issue);
                }
            }

            if !ml_insights.recommendations.is_empty() {
                println!("\n  💡 Recommendations:");
                for (i, rec) in ml_insights.recommendations.iter().take(5).enumerate() {
                    println!("    {}. {}", i + 1, rec);
                }
            }

            println!("\n💡 Run 'dataprof ml <file>' for detailed ML analysis and code generation");
        }
    }

    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    Ok(())
}

fn print_detailed_breakdown(metrics: &dataprof::types::DataQualityMetrics) {
    println!("\n  📋 Detailed Breakdown:");

    println!("\n    Completeness:");
    println!("      • Missing values: {:.2}%", metrics.missing_values_ratio);
    if !metrics.null_columns.is_empty() {
        println!("      • Columns with high nulls: {}", metrics.null_columns.join(", "));
    }

    println!("\n    Consistency:");
    println!("      • Mixed type ratio: {:.2}%", metrics.mixed_type_ratio);
    if !metrics.mixed_type_columns.is_empty() {
        println!("      • Mixed type columns: {}", metrics.mixed_type_columns.join(", "));
    }

    println!("\n    Uniqueness:");
    println!("      • Duplicate rows: {}", metrics.duplicate_rows);
    println!("      • Duplicate ratio: {:.2}%", metrics.duplicate_ratio);
    if !metrics.high_cardinality_columns.is_empty() {
        println!("      • High cardinality columns: {}", metrics.high_cardinality_columns.len());
    }

    println!("\n    Accuracy:");
    println!("      • Outlier ratio: {:.2}%", metrics.outlier_ratio);
    if !metrics.range_violations.is_empty() {
        println!("      • Range violations: {}", metrics.range_violations.len());
    }

    println!("\n    Timeliness:");
    println!("      • Stale data ratio: {:.2}%", metrics.stale_data_ratio);
    if metrics.future_dates_count > 0 {
        println!("      • Future dates: {}", metrics.future_dates_count);
    }
    if !metrics.temporal_violations.is_empty() {
        println!("      • Temporal violations: {}", metrics.temporal_violations.len());
    }
}

fn quality_bar(score: f64) -> &'static str {
    match score {
        s if s >= 95.0 => "█████████░ EXCELLENT",
        s if s >= 85.0 => "████████░░ GOOD",
        s if s >= 70.0 => "██████░░░░ FAIR",
        s if s >= 50.0 => "████░░░░░░ POOR",
        _ => "██░░░░░░░░ CRITICAL",
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

fn print_json_output(report: &dataprof::types::DataReport) -> Result<()> {
    let json = serde_json::to_string_pretty(report)?;
    println!("{}", json);
    Ok(())
}

fn print_csv_output(report: &dataprof::types::DataReport) -> Result<()> {
    // CSV header
    println!("metric,value");

    // Basic stats
    println!("total_rows,{}", report.basic_stats.total_rows);
    println!("total_columns,{}", report.basic_stats.total_columns);

    // Quality metrics
    if let Some(m) = &report.data_quality_metrics {
        println!("completeness,{:.2}", 100.0 - m.missing_values_ratio);
        println!("consistency,{:.2}", 100.0 - m.mixed_type_ratio);
        println!("uniqueness,{:.2}", 100.0 - m.duplicate_ratio);
        println!("accuracy,{:.2}", 100.0 - m.outlier_ratio);
        println!("timeliness,{:.2}", 100.0 - m.stale_data_ratio);

        let overall = (100.0 - m.missing_values_ratio + 100.0 - m.mixed_type_ratio +
                      100.0 - m.duplicate_ratio + 100.0 - m.outlier_ratio +
                      100.0 - m.stale_data_ratio) / 5.0;
        println!("overall_quality,{:.2}", overall);
    }

    Ok(())
}

fn save_output(path: &std::path::Path, report: &dataprof::types::DataReport, format: &str) -> Result<()> {
    let content = match format {
        "json" => serde_json::to_string_pretty(report)?,
        "csv" => {
            let mut output = String::from("metric,value\n");
            output.push_str(&format!("total_rows,{}\n", report.basic_stats.total_rows));
            output.push_str(&format!("total_columns,{}\n", report.basic_stats.total_columns));
            output
        }
        _ => format!("{:#?}", report),
    };

    fs::write(path, content)?;
    println!("✅ Results saved to: {}", path.display());
    Ok(())
}