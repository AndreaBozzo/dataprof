use crate::analysis::{MlReadinessLevel, MlReadinessScore, RecommendationPriority};
use crate::types::{ColumnProfile, ColumnStats, DataType, QualityIssue, Severity};
use anyhow::Result;
use colored::*;
use serde::Serialize;
use serde_json;

#[derive(Serialize)]
pub struct BenchmarkOutput {
    pub rows: usize,
    pub columns: usize,
    pub file_size_mb: f64,
    pub scan_time_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality_score: Option<f64>,
}

pub fn output_json_profiles(profiles: &[ColumnProfile]) -> Result<()> {
    let output = BenchmarkOutput {
        rows: profiles.first().map_or(0, |p| p.total_count),
        columns: profiles.len(),
        file_size_mb: 0.0,
        scan_time_ms: 0,
        quality_score: None,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

pub fn display_profile(profile: &ColumnProfile) {
    println!(
        "{} {}",
        "Column:".bright_yellow(),
        profile.name.bright_white().bold()
    );

    let type_str = match profile.data_type {
        DataType::String => "String".green(),
        DataType::Integer => "Integer".blue(),
        DataType::Float => "Float".cyan(),
        DataType::Date => "Date".magenta(),
    };
    println!("  Type: {}", type_str);

    println!("  Records: {}", profile.total_count);

    if profile.null_count > 0 {
        let pct = (profile.null_count as f64 / profile.total_count as f64) * 100.0;
        println!(
            "  Nulls: {} ({:.1}%)",
            profile.null_count.to_string().red(),
            pct
        );
    } else {
        println!("  Nulls: {}", "0".green());
    }

    match &profile.stats {
        ColumnStats::Numeric { min, max, mean } => {
            println!("  Min: {:.2}", min);
            println!("  Max: {:.2}", max);
            println!("  Mean: {:.2}", mean);
        }
        ColumnStats::Text {
            min_length,
            max_length,
            avg_length,
        } => {
            println!("  Min Length: {}", min_length);
            println!("  Max Length: {}", max_length);
            println!("  Avg Length: {:.1}", avg_length);
        }
    }

    if !profile.patterns.is_empty() {
        println!("  {}", "Patterns:".bright_cyan());
        for pattern in &profile.patterns {
            println!(
                "    {} - {} matches ({:.1}%)",
                pattern.name.bright_white(),
                pattern.match_count,
                pattern.match_percentage
            );
        }
    }
}

pub fn display_quality_issues(issues: &[QualityIssue]) {
    if issues.is_empty() {
        println!("✨ {}", "No quality issues found!".green().bold());
        println!();
        return;
    }

    println!(
        "⚠️  {} {}",
        "QUALITY ISSUES FOUND:".red().bold(),
        format!("({})", issues.len()).red()
    );
    println!();

    let mut critical_count = 0;
    let mut warning_count = 0;
    let mut info_count = 0;

    for (i, issue) in issues.iter().enumerate() {
        let (icon, severity_text) = match issue.severity() {
            Severity::High => {
                critical_count += 1;
                ("🚨", "CRITICAL".red().bold())
            }
            Severity::Medium => {
                warning_count += 1;
                ("⚠️", "WARNING".yellow().bold())
            }
            Severity::Low => {
                info_count += 1;
                ("ℹ️", "INFO".blue().bold())
            }
        };

        let (title, description, suggestion) = format_quality_issue(issue);

        println!(
            "{} {} {} {}",
            format!("[{}]", i + 1).bright_black(),
            icon,
            severity_text,
            title.bright_white().bold()
        );

        if !description.is_empty() {
            println!("    {}", description.bright_black());
        }

        if !suggestion.is_empty() {
            println!("    💡 {}", suggestion.bright_cyan());
        }

        println!();
    }

    println!("{}", "QUALITY SUMMARY:".bright_white().bold());
    if critical_count > 0 {
        println!("  🚨 Critical: {}", critical_count.to_string().red().bold());
    }
    if warning_count > 0 {
        println!(
            "  ⚠️  Warnings: {}",
            warning_count.to_string().yellow().bold()
        );
    }
    if info_count > 0 {
        println!("  ℹ️  Info: {}", info_count.to_string().blue().bold());
    }
    println!();
}

fn format_quality_issue(issue: &QualityIssue) -> (String, String, String) {
    match issue {
        QualityIssue::MixedDateFormats { column, formats } => {
            let title = format!("Mixed date formats in column '{}'", column);
            let description = format!("Found {} different date formats", formats.len());
            let suggestion = "Consider standardizing date formats before analysis".to_string();
            (title, description, suggestion)
        }
        QualityIssue::NullValues {
            column,
            count,
            percentage,
        } => {
            let title = format!("Null values in column '{}'", column);
            let description = format!("{} null values ({:.1}%)", count, percentage);
            let suggestion = if *percentage > 50.0 {
                "Consider removing this column or imputing values".to_string()
            } else {
                "Consider imputing missing values".to_string()
            };
            (title, description, suggestion)
        }
        QualityIssue::Duplicates { column, count } => {
            let title = format!("Duplicate values in column '{}'", column);
            let description = format!("{} duplicate values found", count);
            let suggestion = "Review and remove duplicates if necessary".to_string();
            (title, description, suggestion)
        }
        QualityIssue::Outliers {
            column,
            values,
            threshold,
        } => {
            let title = format!("Outliers detected in column '{}'", column);
            let description = format!("{} outliers beyond {}σ threshold", values.len(), threshold);
            let suggestion = "Review outliers and consider removal or transformation".to_string();
            (title, description, suggestion)
        }
        QualityIssue::MixedTypes { column, types } => {
            let title = format!("Mixed data types in column '{}'", column);
            let description = format!("Found {} different data types", types.len());
            let suggestion = "Clean and standardize data types".to_string();
            (title, description, suggestion)
        }
    }
}

pub fn display_ml_score(score: &MlReadinessScore) {
    let (level_icon, _level_color) = match score.readiness_level {
        MlReadinessLevel::Ready => ("🚀", "green"),
        MlReadinessLevel::Good => ("✅", "green"),
        MlReadinessLevel::NeedsWork => ("⚠️", "yellow"),
        MlReadinessLevel::NotReady => ("❌", "red"),
    };

    println!(
        "🤖 {} Machine Learning Readiness",
        "ML READINESS SCORE".bright_blue().bold()
    );

    let score_str = format!("{:.1}%", score.overall_score);
    let colored_score = match score.readiness_level {
        MlReadinessLevel::Ready | MlReadinessLevel::Good => score_str.green().bold(),
        MlReadinessLevel::NeedsWork => score_str.yellow().bold(),
        MlReadinessLevel::NotReady => score_str.red().bold(),
    };

    println!(
        "  {} Overall Score: {} {:?}",
        level_icon, colored_score, score.readiness_level
    );
    println!();

    println!("📊 {}", "Component Scores:".bright_white().bold());
    println!(
        "  Completeness: {:.1}% {}",
        score.completeness_score,
        if score.completeness_score >= 80.0 {
            "✅".green()
        } else if score.completeness_score >= 60.0 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );

    println!(
        "  Consistency: {:.1}% {}",
        score.consistency_score,
        if score.consistency_score >= 80.0 {
            "✅".green()
        } else if score.consistency_score >= 60.0 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );

    println!(
        "  Feature Quality: {:.1}% {}",
        score.feature_quality_score,
        if score.feature_quality_score >= 80.0 {
            "✅".green()
        } else if score.feature_quality_score >= 60.0 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );

    println!(
        "  Type Suitability: {:.1}% {}",
        score.type_suitability_score,
        if score.type_suitability_score >= 80.0 {
            "✅".green()
        } else if score.type_suitability_score >= 60.0 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    println!();

    if !score.blocking_issues.is_empty() {
        println!("🚫 {}", "Blocking Issues:".red().bold());
        for issue in &score.blocking_issues {
            println!("  • {}", issue.description.red());
            println!("    💡 {}", issue.resolution_required.bright_cyan());
        }
        println!();
    }

    if !score.recommendations.is_empty() {
        println!("💡 {}", "Recommendations:".bright_cyan().bold());
        for rec in &score.recommendations {
            let priority_icon = match rec.priority {
                RecommendationPriority::Critical => "🔴",
                RecommendationPriority::High => "🟠",
                RecommendationPriority::Medium => "🟡",
                RecommendationPriority::Low => "🔵",
            };
            println!("  {} {}", priority_icon, rec.description);
        }
        println!();
    }
}
