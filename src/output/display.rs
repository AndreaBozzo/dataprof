//! Legacy display functions
//!
//! These functions are kept for backward compatibility with the database command.
//! New code should use the unified formatter system in `formatters.rs` instead.
//!
//! See: `output::output_with_adaptive_formatter` for the modern approach.

use crate::types::{ColumnProfile, DataQualityMetrics, QualityIssue};
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
}

/// Display column profiles in formatted JSON
///
/// # Deprecated
/// Use `output_with_adaptive_formatter` instead for consistent formatting across all commands.
#[deprecated(
    since = "0.4.62",
    note = "Use output_with_adaptive_formatter from formatters module instead"
)]
pub fn output_json_profiles(profiles: &[ColumnProfile]) -> Result<()> {
    let output = BenchmarkOutput {
        rows: profiles.first().map_or(0, |p| p.total_count),
        columns: profiles.len(),
        file_size_mb: 0.0,
        scan_time_ms: 0,
    };

    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

/// Display quality issues with formatting
///
/// # Deprecated
/// Use `output_with_adaptive_formatter` instead for consistent formatting across all commands.
#[deprecated(
    since = "0.4.62",
    note = "Use output_with_adaptive_formatter from formatters module instead"
)]
pub fn display_quality_issues(issues: &[QualityIssue]) {
    if issues.is_empty() {
        println!("✅ {}", "No quality issues found!".green().bold());
        return;
    }

    println!(
        "⚠️  {} {}",
        "Quality Issues:".bright_yellow().bold(),
        format!("({} found)", issues.len()).dimmed()
    );
    println!();

    for issue in issues {
        let (severity_icon, issue_desc) = match issue {
            QualityIssue::NullValues { column, count, .. } => (
                "🔵",
                format!("{} null values in column '{}'", count, column),
            ),
            QualityIssue::MixedDateFormats { column, .. } => {
                ("🟡", format!("Mixed date formats in column '{}'", column))
            }
            QualityIssue::Duplicates { column, count } => (
                "🟡",
                format!("{} duplicate values in column '{}'", count, column),
            ),
            QualityIssue::Outliers { column, values, .. } => (
                "🟠",
                format!("{} outliers in column '{}'", values.len(), column),
            ),
            QualityIssue::MixedTypes { column, .. } => {
                ("🔴", format!("Mixed data types in column '{}'", column))
            }
        };

        println!("  {} {}", severity_icon, issue_desc.dimmed());
    }
    println!();
}

/// Display data quality metrics with ISO 8000/25012 dimensions
///
/// # Deprecated
/// Use `output_with_adaptive_formatter` instead for consistent formatting across all commands.
#[deprecated(
    since = "0.4.62",
    note = "Use output_with_adaptive_formatter from formatters module instead"
)]
pub fn display_data_quality_metrics(metrics: &DataQualityMetrics) {
    println!(
        "📊 {}",
        "Data Quality Metrics (ISO 8000/25012)".bright_blue().bold()
    );
    println!();

    // Completeness Dimension
    println!(
        "  {} {}",
        "📋".bright_white(),
        "Completeness".bright_white().bold()
    );
    println!(
        "    Missing Values: {:.1}% {}",
        metrics.missing_values_ratio * 100.0,
        if metrics.missing_values_ratio < 0.05 {
            "✅".green()
        } else if metrics.missing_values_ratio < 0.2 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    println!(
        "    Complete Records: {:.1}% {}",
        metrics.complete_records_ratio * 100.0,
        if metrics.complete_records_ratio > 0.8 {
            "✅".green()
        } else if metrics.complete_records_ratio > 0.6 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );

    if !metrics.null_columns.is_empty() {
        println!(
            "    Null Columns: {} {}",
            metrics.null_columns.len(),
            if metrics.null_columns.is_empty() {
                "✅".green()
            } else {
                "⚠️".yellow()
            }
        );
    }
    println!();

    // Consistency Dimension
    println!(
        "  {} {}",
        "🔄".bright_white(),
        "Consistency".bright_white().bold()
    );
    println!(
        "    Data Type Consistency: {:.1}% {}",
        metrics.data_type_consistency * 100.0,
        if metrics.data_type_consistency > 0.95 {
            "✅".green()
        } else if metrics.data_type_consistency > 0.8 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    println!(
        "    Format Violations: {} {}",
        metrics.format_violations,
        if metrics.format_violations == 0 {
            "✅".green()
        } else if metrics.format_violations < 10 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    println!(
        "    Encoding Issues: {} {}",
        metrics.encoding_issues,
        if metrics.encoding_issues == 0 {
            "✅".green()
        } else {
            "❌".red()
        }
    );
    println!();

    // Uniqueness Dimension
    println!(
        "  {} {}",
        "🔑".bright_white(),
        "Uniqueness".bright_white().bold()
    );
    println!(
        "    Duplicate Rows: {} {}",
        metrics.duplicate_rows,
        if metrics.duplicate_rows == 0 {
            "✅".green()
        } else if metrics.duplicate_rows < 10 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    println!(
        "    Key Uniqueness: {:.1}% {}",
        metrics.key_uniqueness * 100.0,
        if metrics.key_uniqueness > 0.95 {
            "✅".green()
        } else if metrics.key_uniqueness > 0.8 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    if metrics.high_cardinality_warning {
        println!(
            "    High Cardinality: {} {}",
            "Warning".yellow(),
            "⚠️".yellow()
        );
    }
    println!();

    // Accuracy Dimension
    println!(
        "  {} {}",
        "🎯".bright_white(),
        "Accuracy".bright_white().bold()
    );
    println!(
        "    Outlier Ratio: {:.1}% {}",
        metrics.outlier_ratio * 100.0,
        if metrics.outlier_ratio < 0.05 {
            "✅".green()
        } else if metrics.outlier_ratio < 0.1 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    println!(
        "    Range Violations: {} {}",
        metrics.range_violations,
        if metrics.range_violations == 0 {
            "✅".green()
        } else if metrics.range_violations < 5 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    println!(
        "    Negative in Positive: {} {}",
        metrics.negative_values_in_positive,
        if metrics.negative_values_in_positive == 0 {
            "✅".green()
        } else {
            "❌".red()
        }
    );
    println!();

    // Timeliness Dimension
    println!(
        "  {} {}",
        "⏰".bright_white(),
        "Timeliness".bright_white().bold()
    );
    println!(
        "    Future Dates: {} {}",
        metrics.future_dates_count,
        if metrics.future_dates_count == 0 {
            "✅".green()
        } else {
            "⚠️".yellow()
        }
    );
    println!(
        "    Stale Data Ratio: {:.1}% {}",
        metrics.stale_data_ratio * 100.0,
        if metrics.stale_data_ratio < 0.1 {
            "✅".green()
        } else if metrics.stale_data_ratio < 0.3 {
            "⚠️".yellow()
        } else {
            "❌".red()
        }
    );
    println!(
        "    Temporal Violations: {} {}",
        metrics.temporal_violations,
        if metrics.temporal_violations == 0 {
            "✅".green()
        } else {
            "❌".red()
        }
    );
    println!();
}
