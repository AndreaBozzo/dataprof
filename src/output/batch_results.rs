use crate::analysis::MlReadinessScore;
use crate::core::batch::BatchResult;
use colored::*;
use std::cmp::Ordering;
use std::path::PathBuf;

pub fn display_batch_results(result: &BatchResult, quality_enabled: bool) {
    display_enhanced_batch_results(result, quality_enabled, false);
}

pub fn display_enhanced_batch_results(
    result: &BatchResult,
    quality_enabled: bool,
    ml_code_enabled: bool,
) {
    println!(
        "\n📈 {} Batch Analysis Dashboard",
        "DataProfiler".bright_blue().bold()
    );

    // Display overall summary
    display_batch_summary(&result.summary);

    // Display ML overview if ML scores are available
    if !result.ml_scores.is_empty() {
        display_batch_ml_overview(&result.ml_scores);
    }

    // Display file-by-file analysis
    if quality_enabled && !result.reports.is_empty() {
        display_detailed_file_analysis(&result.reports, &result.ml_scores, ml_code_enabled);
    }

    // Display aggregated quality issues
    if quality_enabled {
        display_aggregated_quality_issues(&result.reports);
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
        "├─ Total Quality Issues: {}",
        if summary.total_issues > 0 {
            summary.total_issues.to_string().yellow().bold()
        } else {
            summary.total_issues.to_string().green().bold()
        }
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

fn display_batch_ml_overview(ml_scores: &std::collections::HashMap<PathBuf, MlReadinessScore>) {
    println!(
        "\n🤖 {} ML Readiness Overview",
        "Machine Learning".bright_blue().bold()
    );

    let total_score: f32 = ml_scores
        .values()
        .map(|score| score.overall_score as f32)
        .sum();
    let avg_ml_score = total_score / ml_scores.len() as f32;

    // Categorize files by ML readiness
    let mut ready_count = 0;
    let mut good_count = 0;
    let mut needs_work_count = 0;
    let mut not_ready_count = 0;

    for score in ml_scores.values() {
        match score.overall_score {
            s if s >= 80.0 => ready_count += 1,
            s if s >= 60.0 => good_count += 1,
            s if s >= 40.0 => needs_work_count += 1,
            _ => not_ready_count += 1,
        }
    }

    println!(
        "├─ Average ML Score: {:.1}% across {} files",
        avg_ml_score,
        ml_scores.len()
    );
    println!("├─ Readiness Distribution:");
    println!(
        "│  ├─ {} Ready (≥80%): {} files",
        "🟢".green(),
        ready_count.to_string().green().bold()
    );
    println!(
        "│  ├─ {} Good (60-80%): {} files",
        "🔵".blue(),
        good_count.to_string().blue().bold()
    );
    println!(
        "│  ├─ {} Needs Work (40-60%): {} files",
        "🟡".yellow(),
        needs_work_count.to_string().yellow().bold()
    );
    println!(
        "│  └─ {} Not Ready (<40%): {} files",
        "🔴".red(),
        not_ready_count.to_string().red().bold()
    );

    // Show top recommendations across all files
    let mut all_recommendations = Vec::new();
    for score in ml_scores.values() {
        for rec in &score.recommendations {
            all_recommendations.push(rec);
        }
    }

    if !all_recommendations.is_empty() {
        // Group recommendations by type
        let mut rec_counts = std::collections::HashMap::new();
        for rec in &all_recommendations {
            let count = rec_counts.entry(rec.category.clone()).or_insert(0);
            *count += 1;
        }

        let mut sorted_recs: Vec<_> = rec_counts.into_iter().collect();
        sorted_recs.sort_by(|a, b| b.1.cmp(&a.1));

        println!("└─ Top Recommendations:");
        for (i, (category, count)) in sorted_recs.iter().take(3).enumerate() {
            let prefix = if i == sorted_recs.len() - 1 || i == 2 {
                "   └─"
            } else {
                "   ├─"
            };
            println!(
                "{} {}: {} occurrences",
                prefix,
                category.bright_cyan(),
                count.to_string().bright_white()
            );
        }
    }
}

fn display_detailed_file_analysis(
    reports: &std::collections::HashMap<PathBuf, crate::types::QualityReport>,
    ml_scores: &std::collections::HashMap<PathBuf, MlReadinessScore>,
    ml_code_enabled: bool,
) {
    println!(
        "\n🔍 {} Detailed File Analysis",
        "Per-File".bright_magenta().bold()
    );

    // Sort files by quality score (worst first)
    let mut file_data: Vec<_> = reports
        .iter()
        .map(|(path, report)| {
            let quality_score = report.quality_score().unwrap_or(0.0);
            let ml_score = ml_scores.get(path);
            (path, report, quality_score, ml_score)
        })
        .collect();

    file_data.sort_by(|a, b| a.2.partial_cmp(&b.2).unwrap_or(Ordering::Equal));

    let display_count = if file_data.len() > 10 {
        10
    } else {
        file_data.len()
    };

    for (i, (path, report, quality_score, ml_score_opt)) in
        file_data.iter().take(display_count).enumerate()
    {
        let filename = path
            .file_name()
            .map_or("unknown", |name| name.to_str().unwrap_or("unknown"));

        let quality_icon = if *quality_score >= 80.0 {
            "✅"
        } else if *quality_score >= 60.0 {
            "🟡"
        } else {
            "🔴"
        };

        let ml_score_display = if let Some(ml_score) = ml_score_opt {
            format!("ML: {:.1}%", ml_score.overall_score)
        } else {
            "ML: N/A".to_string()
        };

        println!(
            "\n{} {} {} (Quality: {:.1}%, {})",
            quality_icon,
            format!("{}.", i + 1).bright_black(),
            filename.bright_white().bold(),
            quality_score,
            ml_score_display.bright_blue()
        );

        // Show quality issues summary
        if !report.issues.is_empty() {
            println!("   📋 {} quality issues found:", report.issues.len());
            for (j, issue) in report.issues.iter().take(3).enumerate() {
                let issue_desc = format_issue_brief(issue);
                println!(
                    "      {}─ {}",
                    if j == 2 || j == report.issues.len() - 1 {
                        "└"
                    } else {
                        "├"
                    },
                    issue_desc
                );
            }
            if report.issues.len() > 3 {
                println!("      └─ ... and {} more issues", report.issues.len() - 3);
            }
        }

        // Show comprehensive data quality metrics summary if available
        if let Some(ref metrics) = report.data_quality_metrics {
            println!("   📊 Data Quality Metrics:");
            println!(
                "      ├─ Completeness: {:.1}% (Missing: {:.1}%)",
                metrics.complete_records_ratio, metrics.missing_values_ratio
            );
            println!(
                "      ├─ Consistency: {:.1}%",
                metrics.data_type_consistency
            );
            println!(
                "      ├─ Uniqueness: {:.1}% (Duplicates: {})",
                metrics.key_uniqueness, metrics.duplicate_rows
            );
            println!("      └─ Accuracy: {:.1}% outliers", metrics.outlier_ratio);
        }

        // Show ML recommendations and code if enabled
        if let Some(ml_score) = ml_score_opt {
            if !ml_score.recommendations.is_empty() {
                println!(
                    "   🤖 ML Recommendations ({}):",
                    ml_score.recommendations.len()
                );
                let top_recs =
                    ml_score
                        .recommendations
                        .iter()
                        .take(if ml_code_enabled { 2 } else { 3 });

                for rec in top_recs {
                    let priority_icon = match rec.priority {
                        crate::analysis::RecommendationPriority::Critical => "🔴",
                        crate::analysis::RecommendationPriority::High => "🟡",
                        crate::analysis::RecommendationPriority::Medium => "🔵",
                        crate::analysis::RecommendationPriority::Low => "🟢",
                    };

                    println!(
                        "      ├─ {} {} - {}",
                        priority_icon,
                        rec.category.bright_cyan(),
                        rec.description
                    );

                    // Show code snippet if ml_code is enabled
                    if ml_code_enabled && rec.code_snippet.is_some() {
                        if let Some(code) = &rec.code_snippet {
                            println!("      │  💻 Code snippet:");
                            for (line_i, line) in code.lines().take(3).enumerate() {
                                let line_prefix = if line_i == 2 {
                                    "      │     └─"
                                } else {
                                    "      │     ├─"
                                };
                                if line.trim().starts_with('#') {
                                    println!("{} {}", line_prefix, line.bright_black());
                                } else {
                                    println!("{} {}", line_prefix, line);
                                }
                            }
                            if code.lines().count() > 3 {
                                println!(
                                    "      │     └─ ... ({} more lines)",
                                    code.lines().count() - 3
                                );
                            }
                        }
                    }
                }

                if ml_score.recommendations.len() > (if ml_code_enabled { 2 } else { 3 }) {
                    println!(
                        "      └─ ... and {} more recommendations",
                        ml_score.recommendations.len() - (if ml_code_enabled { 2 } else { 3 })
                    );
                }
            }
        }
    }

    if file_data.len() > display_count {
        println!(
            "\n   ... and {} more files analyzed",
            file_data.len() - display_count
        );
    }
}

fn display_aggregated_quality_issues(
    reports: &std::collections::HashMap<PathBuf, crate::types::QualityReport>,
) {
    let mut all_issues = Vec::new();
    let mut affected_files = 0;

    for (path, report) in reports {
        if !report.issues.is_empty() {
            affected_files += 1;
            for issue in &report.issues {
                all_issues.push((path, issue));
            }
        }
    }

    if all_issues.is_empty() {
        println!(
            "\n✨ {} No Quality Issues Found!",
            "Excellent!".bright_green().bold()
        );
        println!("   All files passed quality checks with flying colors! 🎉");
        return;
    }

    println!(
        "\n⚠️ {} Quality Issues Summary",
        "Aggregated".bright_yellow().bold()
    );
    println!(
        "├─ Total Issues: {}",
        all_issues.len().to_string().bright_white().bold()
    );
    println!(
        "├─ Files Affected: {} of {}",
        affected_files.to_string().yellow().bold(),
        reports.len().to_string().bright_white()
    );

    // Group issues by type
    let mut issue_counts = std::collections::HashMap::new();
    for (_, issue) in &all_issues {
        let issue_type = get_issue_type_name(issue);
        let count = issue_counts.entry(issue_type).or_insert(0);
        *count += 1;
    }

    let mut sorted_issues: Vec<_> = issue_counts.into_iter().collect();
    sorted_issues.sort_by(|a, b| b.1.cmp(&a.1));

    println!("└─ Issue Breakdown:");
    for (i, (issue_type, count)) in sorted_issues.iter().take(5).enumerate() {
        let prefix = if i == 4 || i == sorted_issues.len() - 1 {
            "   └─"
        } else {
            "   ├─"
        };
        println!(
            "{} {}: {} occurrences",
            prefix,
            issue_type.bright_red(),
            count.to_string().bright_white()
        );
    }

    if sorted_issues.len() > 5 {
        println!("   └─ ... and {} more issue types", sorted_issues.len() - 5);
    }
}

fn display_generated_artifacts_info(result: &BatchResult) {
    let mut artifacts = Vec::new();

    if let Some(html_path) = &result.html_report_path {
        artifacts.push(format!(
            "📄 HTML Dashboard: {}",
            html_path.display().to_string().bright_green()
        ));
    }

    if let Some(script_path) = &result.script_path {
        artifacts.push(format!(
            "🐍 Preprocessing Script: {}",
            script_path.display().to_string().bright_green()
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

fn format_issue_brief(issue: &crate::types::QualityIssue) -> String {
    match issue {
        crate::types::QualityIssue::NullValues {
            column, percentage, ..
        } => {
            format!("Null values in '{}' ({:.1}%)", column, percentage)
        }
        crate::types::QualityIssue::MixedDateFormats { column, .. } => {
            format!("Mixed date formats in '{}'", column)
        }
        crate::types::QualityIssue::Duplicates { column, count } => {
            format!("Duplicate values in '{}' ({} duplicates)", column, count)
        }
        crate::types::QualityIssue::Outliers { column, values, .. } => {
            format!("Outliers in '{}' ({} detected)", column, values.len())
        }
        crate::types::QualityIssue::MixedTypes { column, .. } => {
            format!("Mixed data types in '{}'", column)
        }
    }
}

fn get_issue_type_name(issue: &crate::types::QualityIssue) -> String {
    match issue {
        crate::types::QualityIssue::NullValues { .. } => "Null Values".to_string(),
        crate::types::QualityIssue::MixedDateFormats { .. } => "Mixed Date Formats".to_string(),
        crate::types::QualityIssue::Duplicates { .. } => "Duplicate Values".to_string(),
        crate::types::QualityIssue::Outliers { .. } => "Outliers".to_string(),
        crate::types::QualityIssue::MixedTypes { .. } => "Mixed Data Types".to_string(),
    }
}
