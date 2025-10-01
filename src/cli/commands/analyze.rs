use clap::Args;
use std::path::PathBuf;

/// Full analysis arguments
#[derive(Debug, Args)]
pub struct AnalyzeArgs {
    /// Input file to analyze
    pub file: PathBuf,

    /// Show detailed metrics for all dimensions
    #[arg(long)]
    pub detailed: bool,

    /// Include ML readiness scoring
    #[arg(long)]
    pub ml: bool,

    /// Output format (text, json, csv)
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Output file path
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// ISO quality threshold profile (default, strict, lenient)
    #[arg(long, default_value = "default")]
    pub threshold_profile: String,

    /// Use streaming for large files
    #[arg(long)]
    pub streaming: bool,

    /// Sample size for large files
    #[arg(long)]
    pub sample: Option<usize>,

    /// Common analysis options (progress, chunk-size, config)
    #[command(flatten)]
    pub common: super::CommonAnalysisOptions,
}

use anyhow::Result;
use dataprof::MlReadinessEngine;
use std::fs;

use crate::cli::{analyze_file_with_options, AnalysisOptions};

/// Execute the analyze command - comprehensive ISO 8000/25012 analysis
pub fn execute(args: &AnalyzeArgs) -> Result<()> {
    // Build analysis options from command arguments
    let options = AnalysisOptions {
        progress: args.common.progress,
        chunk_size: args.common.chunk_size,
        config: args.common.config.clone(),
        streaming: args.streaming,
        sample: args.sample,
    };

    // Use shared core logic that handles all improvements
    let report = analyze_file_with_options(&args.file, options)?;

    // Calculate ML score if requested
    let ml_score = if args.ml {
        let ml_engine = MlReadinessEngine::new();
        Some(ml_engine.calculate_ml_score(&report)?)
    } else {
        None
    };

    match args.format.as_str() {
        "json" => print_json_output(&report)?,
        "csv" => print_csv_output(&report)?,
        _ => print_text_output(&report, ml_score.as_ref(), args.detailed)?,
    }

    if let Some(output_path) = &args.output {
        save_output(output_path, &report, &args.format)?;
    }

    Ok(())
}

fn print_text_output(
    report: &dataprof::types::QualityReport,
    ml_score: Option<&dataprof::analysis::MlReadinessScore>,
    detailed: bool,
) -> Result<()> {
    println!("\n📊 COMPREHENSIVE ISO 8000/25012 Analysis\n");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    println!("\n📄 Dataset Overview:");
    println!("  Rows: {}", report.file_info.total_rows.unwrap_or(0));
    println!("  Columns: {}", report.file_info.total_columns);

    if let Some(m) = &report.data_quality_metrics {
        println!("\n📊 ISO 8000/25012 Quality Dimensions:\n");
        let completeness = 100.0 - m.missing_values_ratio;
        let consistency = m.data_type_consistency;
        let uniqueness = m.key_uniqueness;
        let accuracy = 100.0 - m.outlier_ratio;
        let timeliness = 100.0 - m.stale_data_ratio;

        println!("  🔍 Completeness: {:>6.1}%", completeness);
        println!("  ⚡ Consistency:  {:>6.1}%", consistency);
        println!("  🔑 Uniqueness:   {:>6.1}%", uniqueness);
        println!("  🎯 Accuracy:     {:>6.1}%", accuracy);
        println!("  ⏰ Timeliness:   {:>6.1}%", timeliness);

        let overall = (completeness + consistency + uniqueness + accuracy + timeliness) / 5.0;
        println!("\n  📈 Overall Quality: {:.1}%", overall);

        if detailed {
            println!("\n  📋 Details:");
            println!(
                "    Missing: {:.2}%, Duplicates: {}",
                m.missing_values_ratio, m.duplicate_rows
            );
        }
    }

    // Show ML readiness if calculated
    if let Some(ml) = ml_score {
        println!("\n🤖 ML Readiness Analysis:\n");
        println!("  Score: {:.0}%", ml.overall_score);

        if !ml.blocking_issues.is_empty() {
            println!("\n  ❌ Blocking Issues: {}", ml.blocking_issues.len());
        }

        if !ml.recommendations.is_empty() {
            println!("  💡 Recommendations: {}", ml.recommendations.len());
            println!("\n💡 Run 'dataprof ml <file>' for detailed ML analysis and code generation");
        }
    }

    println!("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");
    Ok(())
}

fn print_json_output(report: &dataprof::types::QualityReport) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(report)?);
    Ok(())
}

fn print_csv_output(report: &dataprof::types::QualityReport) -> Result<()> {
    println!("metric,value");
    println!("total_rows,{}", report.file_info.total_rows.unwrap_or(0));
    println!("total_columns,{}", report.file_info.total_columns);
    Ok(())
}

fn save_output(
    path: &std::path::Path,
    report: &dataprof::types::QualityReport,
    format: &str,
) -> Result<()> {
    let content = if format == "json" {
        serde_json::to_string_pretty(report)?
    } else {
        format!("{:#?}", report)
    };
    fs::write(path, content)?;
    println!("✅ Results saved to: {}", path.display());
    Ok(())
}
