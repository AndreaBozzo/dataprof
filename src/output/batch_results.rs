use crate::core::batch::BatchResult;
use colored::*;

pub fn display_batch_results(result: &BatchResult, quality_enabled: bool) {
    println!(
        "\n📈 {} Batch Analysis Dashboard",
        "DataProfiler".bright_blue().bold()
    );

    // Display overall summary
    display_batch_summary(&result.summary);

    // Display aggregated quality issues
    if quality_enabled {
        println!("\n⚠️ Quality issues aggregation not yet implemented");
    }

    // Display generated artifacts info
    display_generated_artifacts_info(result);

    // Display performance summary
    display_performance_summary(&result.summary);
}

fn display_batch_summary(summary: &crate::core::batch::BatchSummary) {
    println!("\n📊 {} Summary", "Processing".bright_green().bold());

    let success_rate = if summary.total_files > 0 {
        (summary.successful as f64 / summary.total_files as f64) * 100.0
    } else {
        0.0
    };

    let avg_score_status = if summary.average_quality_score >= 80.0 {
        format!("{} EXCELLENT", "✅".green())
    } else if summary.average_quality_score >= 60.0 {
        format!("{} GOOD", "🟡".yellow())
    } else {
        format!("{} NEEDS ATTENTION", "🔴".red())
    };

    println!(
        "├─ Total Files: {}",
        summary.total_files.to_string().bright_white().bold()
    );
    println!(
        "├─ Success Rate: {:.1}% ({} successful, {} failed)",
        success_rate,
        summary.successful.to_string().green(),
        summary.failed.to_string().red()
    );
    println!(
        "├─ Average Quality Score: {:.1}% - {}",
        summary.average_quality_score, avg_score_status
    );
    println!(
        "├─ Total Records Processed: {}",
        summary.total_records.to_string().bright_white()
    );
    println!(
        "└─ Processing Time: {:.2}s ({:.1} files/sec)",
        summary.processing_time_seconds,
        if summary.processing_time_seconds > 0.0 {
            summary.successful as f64 / summary.processing_time_seconds
        } else {
            0.0
        }
    );
}

fn display_generated_artifacts_info(result: &BatchResult) {
    let mut artifacts = Vec::new();

    if let Some(html_path) = &result.html_report_path {
        artifacts.push(format!(
            "📄 HTML Report: {}",
            html_path.display().to_string().bright_green()
        ));
    }

    if !artifacts.is_empty() {
        println!(
            "\n📦 {} Generated Artifacts",
            "Output".bright_purple().bold()
        );
        for (i, artifact) in artifacts.iter().enumerate() {
            let prefix = if i == artifacts.len() - 1 {
                "└─"
            } else {
                "├─"
            };
            println!("{} {}", prefix, artifact);
        }
    }
}

fn display_performance_summary(summary: &crate::core::batch::BatchSummary) {
    if summary.total_files <= 1 {
        return;
    }

    println!(
        "\n⚡ {} Performance Summary",
        "Processing".bright_cyan().bold()
    );
    let files_per_sec = if summary.processing_time_seconds > 0.0 {
        summary.successful as f64 / summary.processing_time_seconds
    } else {
        0.0
    };

    let avg_time_per_file = if summary.successful > 0 {
        summary.processing_time_seconds / summary.successful as f64
    } else {
        0.0
    };

    println!("├─ Processing Speed: {:.1} files/second", files_per_sec);
    println!("├─ Average Time per File: {:.2}s", avg_time_per_file);
    println!(
        "└─ Total Processing Time: {:.2}s",
        summary.processing_time_seconds
    );

    if files_per_sec > 10.0 {
        println!(
            "   {} Excellent performance! 🚀",
            "Note:".bright_green().bold()
        );
    } else if files_per_sec > 5.0 {
        println!("   {} Good performance 👍", "Note:".bright_blue().bold());
    } else if files_per_sec > 1.0 {
        println!(
            "   {} Consider enabling parallel processing for better performance",
            "Tip:".bright_yellow().bold()
        );
    }
}
