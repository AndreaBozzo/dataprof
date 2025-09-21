use crate::core::batch::BatchResult;
use colored::*;
use std::cmp::Ordering;

pub fn display_batch_results(result: &BatchResult, quality_enabled: bool) {
    println!("\n📈 Batch Quality Analysis");

    if result.summary.failed > 0 {
        let failure_rate =
            (result.summary.failed as f64 / result.summary.total_files as f64) * 100.0;
        println!("⚠️ {:.1}% of files failed processing", failure_rate);
    }

    if result.summary.total_issues > 0 {
        println!(
            "🔍 Found {} quality issues across {} files",
            result.summary.total_issues, result.summary.successful
        );

        if result.summary.average_quality_score < 80.0 {
            println!(
                "📊 Average Quality Score: {:.1}% - {} BELOW THRESHOLD",
                result.summary.average_quality_score,
                "⚠️".yellow()
            );
        } else {
            println!(
                "📊 Average Quality Score: {:.1}% - {} GOOD",
                result.summary.average_quality_score,
                "✅".green()
            );
        }
    }

    if quality_enabled && !result.reports.is_empty() {
        println!("\n🔍 Quality Issues by File:");

        let mut file_scores: Vec<_> = result
            .reports
            .iter()
            .filter_map(|(path, report)| {
                report
                    .quality_score()
                    .ok()
                    .map(|score| (path, report, score))
            })
            .collect();

        file_scores.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(Ordering::Equal));

        for (path, report, score) in file_scores.iter().take(10) {
            let icon = if *score < 60.0 {
                "🔴"
            } else if *score < 80.0 {
                "🟡"
            } else {
                "✅"
            };
            println!(
                "  {} {:.1}% - {} ({} issues)",
                icon,
                score,
                path.file_name()
                    .map_or("unknown".into(), |name| name.to_string_lossy()),
                report.issues.len()
            );
        }
    }

    if result.summary.total_files > 1 {
        let files_per_sec =
            result.summary.successful as f64 / result.summary.processing_time_seconds;
        println!(
            "\n⚡ Processed {:.1} files/sec ({:.2}s total)",
            files_per_sec, result.summary.processing_time_seconds
        );
    }
}
