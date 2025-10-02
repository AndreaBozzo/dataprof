use crate::core::batch::{BatchResult, BatchSummary};
use crate::types::{ColumnProfile, ColumnStats, DataQualityMetrics, DataType, QualityReport};
use anyhow::Result;
use handlebars::Handlebars;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

// Embed templates at compile time
const SINGLE_REPORT_TEMPLATE: &str = include_str!("../../templates/single_report.hbs");
const BATCH_DASHBOARD_TEMPLATE: &str = include_str!("../../templates/batch_dashboard.hbs");

// Embed assets at compile time
const STYLE_CSS: &str = include_str!("../../templates/assets/style.css");
const BATCH_CSS: &str = include_str!("../../templates/assets/batch.css");
const DASHBOARD_JS: &str = include_str!("../../templates/assets/dashboard.js");

/// Initialize Handlebars registry with templates
fn init_handlebars() -> Result<Handlebars<'static>> {
    let mut handlebars = Handlebars::new();
    handlebars.register_template_string("single_report", SINGLE_REPORT_TEMPLATE)?;
    handlebars.register_template_string("batch_dashboard", BATCH_DASHBOARD_TEMPLATE)?;
    Ok(handlebars)
}

/// Generate HTML report for a single file
pub fn generate_html_report(report: &QualityReport, output_path: &Path) -> Result<()> {
    let handlebars = init_handlebars()?;
    let context = build_report_context(report);
    let html_content = handlebars.render("single_report", &context)?;

    // Write HTML file
    fs::write(output_path, html_content)?;

    // Write CSS files in the same directory
    let output_dir = output_path.parent().unwrap_or(Path::new("."));
    fs::write(output_dir.join("style.css"), STYLE_CSS)?;

    Ok(())
}

/// Build context data for single report template
fn build_report_context(report: &QualityReport) -> serde_json::Value {
    let file_name = Path::new(&report.file_info.path)
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();

    let sampling_info = if report.scan_info.sampling_ratio < 1.0 {
        Some(json!({
            "rows_scanned": report.scan_info.rows_scanned,
            "sampling_percentage": format!("{:.1}", report.scan_info.sampling_ratio * 100.0)
        }))
    } else {
        None
    };

    json!({
        "file_name": file_name,
        "file_path": report.file_info.path,
        "file_size_mb": format!("{:.1}", report.file_info.file_size_mb),
        "total_rows": report.file_info.total_rows.map_or("Unknown".to_string(), |r| r.to_string()),
        "total_columns": report.file_info.total_columns,
        "scan_time_ms": report.scan_info.scan_time_ms,
        "sampling_info": sampling_info,
        "data_quality_metrics": build_data_quality_metrics_context(&report.data_quality_metrics),
        "column_profiles": build_column_profiles_context(&report.column_profiles)
    })
}

/// Build context for data quality metrics
fn build_data_quality_metrics_context(metrics: &DataQualityMetrics) -> serde_json::Value {
    let overall_score = calculate_overall_data_quality_score(metrics);
    let assessment_class = get_assessment_class(overall_score);

    json!({
        "overall_score": format!("{:.1}", overall_score),
        "assessment_class": assessment_class,
        "completeness": {
            "missing_ratio": format!("{:.2}", metrics.missing_values_ratio * 100.0),
            "complete_ratio": format!("{:.2}", (1.0 - metrics.missing_values_ratio) * 100.0),
            "missing_class": get_stat_class(metrics.missing_values_ratio, true),
            "null_columns": if !metrics.null_columns.is_empty() { Some(metrics.null_columns.len()) } else { None }
        },
        "consistency": {
            "type_consistency": format!("{:.1}", metrics.data_type_consistency * 100.0),
            "type_class": get_stat_class(1.0 - metrics.data_type_consistency, true),
            "format_violations": if metrics.format_violations > 0 { Some(metrics.format_violations) } else { None },
            "encoding_issues": if metrics.encoding_issues > 0 { Some(metrics.encoding_issues) } else { None }
        },
        "uniqueness": {
            "key_uniqueness": format!("{:.1}", metrics.key_uniqueness * 100.0),
            "uniqueness_class": get_stat_class(1.0 - metrics.key_uniqueness, true),
            "has_duplicates": metrics.duplicate_rows > 0,
            "duplicate_rows": if metrics.duplicate_rows > 0 { Some(metrics.duplicate_rows) } else { None },
            "high_cardinality_warning": if metrics.high_cardinality_warning { Some(1) } else { None }
        },
        "accuracy": {
            "outlier_ratio": format!("{:.2}", metrics.outlier_ratio * 100.0),
            "outlier_class": get_stat_class(metrics.outlier_ratio, true),
            "range_violations": if metrics.range_violations > 0 { Some(metrics.range_violations) } else { None },
            "negative_values": if metrics.negative_values_in_positive > 0 { Some(metrics.negative_values_in_positive) } else { None }
        },
        "timeliness": {
            "freshness": format!("{:.1}", 100.0 - metrics.stale_data_ratio),
            "freshness_class": get_stat_class(metrics.stale_data_ratio / 100.0, true),
            "future_dates": if metrics.future_dates_count > 0 { Some(metrics.future_dates_count) } else { None },
            "temporal_violations": if metrics.temporal_violations > 0 { Some(metrics.temporal_violations) } else { None }
        }
    })
}

/// Build context for column profiles
fn build_column_profiles_context(columns: &[ColumnProfile]) -> Vec<serde_json::Value> {
    columns
        .iter()
        .map(|col| {
            let (null_class, null_text) = if col.null_count == 0 {
                ("success", "No nulls".to_string())
            } else if col.null_count < col.total_count / 10 {
                (
                    "success",
                    format!(
                        "{:.1}% nulls",
                        (col.null_count as f64 / col.total_count as f64) * 100.0
                    ),
                )
            } else {
                (
                    "warning",
                    format!(
                        "{:.1}% nulls",
                        (col.null_count as f64 / col.total_count as f64) * 100.0
                    ),
                )
            };

            json!({
                "name": col.name,
                "data_type": format_data_type(&col.data_type),
                "type_class": format_type_class(&col.data_type),
                "null_class": null_class,
                "null_text": null_text,
                "total_count": col.total_count,
                "stats": format_column_stats_json(&col.stats),
                "patterns": format_patterns_json(&col.patterns)
            })
        })
        .collect()
}

/// Generate an aggregated HTML report for batch processing results
pub fn generate_batch_html_report(batch_result: &BatchResult, output_path: &Path) -> Result<()> {
    let handlebars = init_handlebars()?;
    let context = build_batch_context(batch_result);
    let html_content = handlebars.render("batch_dashboard", &context)?;

    // Write HTML file
    fs::write(output_path, html_content)?;

    // Write CSS and JS files in the same directory
    let output_dir = output_path.parent().unwrap_or(Path::new("."));
    fs::write(output_dir.join("style.css"), STYLE_CSS)?;
    fs::write(output_dir.join("batch.css"), BATCH_CSS)?;
    fs::write(output_dir.join("dashboard.js"), DASHBOARD_JS)?;

    Ok(())
}

/// Build context data for batch dashboard template
fn build_batch_context(batch_result: &BatchResult) -> serde_json::Value {
    let summary_context = build_batch_summary_context(&batch_result.summary);
    let aggregated_metrics_context = build_batch_aggregated_metrics_context(&batch_result.reports);
    let files_context = build_files_context(&batch_result.reports, &batch_result.errors);

    json!({
        "summary": summary_context,
        "aggregated_metrics": aggregated_metrics_context,
        "files": files_context
    })
}

/// Build batch summary context
fn build_batch_summary_context(summary: &BatchSummary) -> serde_json::Value {
    let success_rate = if summary.total_files > 0 {
        (summary.successful as f64 / summary.total_files as f64) * 100.0
    } else {
        0.0
    };

    let avg_score_class = if summary.average_quality_score >= 80.0 {
        "success"
    } else if summary.average_quality_score >= 60.0 {
        "warning"
    } else {
        "critical"
    };

    let files_per_second = if summary.processing_time_seconds > 0.0 {
        summary.total_files as f64 / summary.processing_time_seconds
    } else {
        0.0
    };

    json!({
        "total_files": summary.total_files,
        "successful": summary.successful,
        "success_rate": format!("{:.1}", success_rate),
        "average_quality_score": format!("{:.1}", summary.average_quality_score),
        "avg_score_class": avg_score_class,
        "processing_time_seconds": format!("{:.2}", summary.processing_time_seconds),
        "files_per_second": format!("{:.2}", files_per_second),
        "total_records": summary.total_records
    })
}

/// Build aggregated metrics context for batch
fn build_batch_aggregated_metrics_context(
    reports: &HashMap<PathBuf, QualityReport>,
) -> Option<serde_json::Value> {
    if reports.is_empty() {
        return None;
    }

    // Calculate aggregated metrics across all reports
    let mut total_missing_ratio = 0.0;
    let mut total_type_consistency = 0.0;
    let mut total_key_uniqueness = 0.0;
    let mut total_outlier_ratio = 0.0;
    let mut total_stale_data_ratio = 0.0;
    let mut count = 0;
    let mut total_null_columns = 0;
    let mut total_format_violations = 0;
    let mut total_encoding_issues = 0;
    let mut total_duplicate_rows = 0;
    let mut total_range_violations = 0;
    let mut total_negative_values = 0;
    let mut total_high_cardinality = 0;
    let mut total_future_dates = 0;
    let mut total_temporal_violations = 0;

    for report in reports.values() {
        if let Some(metrics) = &report.data_quality_metrics {
            total_missing_ratio += metrics.missing_values_ratio;
            total_type_consistency += metrics.data_type_consistency;
            total_key_uniqueness += metrics.key_uniqueness;
            total_outlier_ratio += metrics.outlier_ratio;
            total_stale_data_ratio += metrics.stale_data_ratio;
            total_null_columns += metrics.null_columns.len();
            total_format_violations += metrics.format_violations;
            total_encoding_issues += metrics.encoding_issues;
            total_duplicate_rows += metrics.duplicate_rows;
            total_range_violations += metrics.range_violations;
            total_negative_values += metrics.negative_values_in_positive;
            total_high_cardinality += if metrics.high_cardinality_warning {
                1
            } else {
                0
            };
            total_future_dates += metrics.future_dates_count;
            total_temporal_violations += metrics.temporal_violations;
            count += 1;
        }
    }

    if count == 0 {
        return None;
    }

    let avg_missing_ratio = total_missing_ratio / count as f64;
    let avg_type_consistency = total_type_consistency / count as f64;
    let avg_key_uniqueness = total_key_uniqueness / count as f64;
    let avg_outlier_ratio = total_outlier_ratio / count as f64;
    let avg_stale_data_ratio = total_stale_data_ratio / count as f64;

    // Calculate overall score (including timeliness)
    let completeness_score = (1.0 - avg_missing_ratio) * 100.0;
    let consistency_score = avg_type_consistency * 100.0;
    let uniqueness_score = avg_key_uniqueness * 100.0;
    let accuracy_score = (1.0 - avg_outlier_ratio) * 100.0;
    let timeliness_score = 100.0 - avg_stale_data_ratio;
    let overall_score = (completeness_score
        + consistency_score
        + uniqueness_score
        + accuracy_score
        + timeliness_score)
        / 5.0;

    let assessment_class = get_assessment_class(overall_score);

    Some(json!({
        "overall_score": format!("{:.1}", overall_score),
        "assessment_class": assessment_class,
        "completeness": {
            "missing_ratio": format!("{:.2}", avg_missing_ratio * 100.0),
            "complete_ratio": format!("{:.2}", (1.0 - avg_missing_ratio) * 100.0),
            "missing_class": get_stat_class(avg_missing_ratio, true),
            "null_columns": if total_null_columns > 0 { Some(total_null_columns) } else { None }
        },
        "consistency": {
            "type_consistency": format!("{:.1}", avg_type_consistency * 100.0),
            "type_class": get_stat_class(1.0 - avg_type_consistency, true),
            "format_violations": if total_format_violations > 0 { Some(total_format_violations) } else { None },
            "encoding_issues": if total_encoding_issues > 0 { Some(total_encoding_issues) } else { None }
        },
        "uniqueness": {
            "key_uniqueness": format!("{:.1}", avg_key_uniqueness * 100.0),
            "uniqueness_class": get_stat_class(1.0 - avg_key_uniqueness, true),
            "has_duplicates": total_duplicate_rows > 0,
            "duplicate_rows": if total_duplicate_rows > 0 { Some(total_duplicate_rows) } else { None },
            "high_cardinality_warning": if total_high_cardinality > 0 { Some(total_high_cardinality) } else { None }
        },
        "accuracy": {
            "outlier_ratio": format!("{:.2}", avg_outlier_ratio * 100.0),
            "outlier_class": get_stat_class(avg_outlier_ratio, true),
            "range_violations": if total_range_violations > 0 { Some(total_range_violations) } else { None },
            "negative_values": if total_negative_values > 0 { Some(total_negative_values) } else { None }
        },
        "timeliness": {
            "freshness": format!("{:.1}", 100.0 - avg_stale_data_ratio),
            "freshness_class": get_stat_class(avg_stale_data_ratio / 100.0, true),
            "future_dates": if total_future_dates > 0 { Some(total_future_dates) } else { None },
            "temporal_violations": if total_temporal_violations > 0 { Some(total_temporal_violations) } else { None }
        }
    }))
}

/// Build files overview context
fn build_files_context(
    reports: &HashMap<PathBuf, QualityReport>,
    errors: &HashMap<PathBuf, String>,
) -> Vec<serde_json::Value> {
    let mut files: Vec<serde_json::Value> = reports
        .iter()
        .map(|(path, report)| {
            let quality_score = calculate_overall_data_quality_score(&report.data_quality_metrics);

            let quality_class = if quality_score >= 80.0 {
                "success"
            } else if quality_score >= 60.0 {
                "warning"
            } else {
                "critical"
            };

            json!({
                "filename": path.file_name().unwrap_or_default().to_string_lossy(),
                "path": path.display().to_string(),
                "quality_score": format!("{:.0}", quality_score),
                "quality_class": quality_class,
                "columns": report.file_info.total_columns,
                "rows": report.file_info.total_rows.map_or("Unknown".to_string(), |r| r.to_string()),
                "is_error": false
            })
        })
        .collect();

    // Add error files
    for (path, error_msg) in errors {
        files.push(json!({
            "filename": path.file_name().unwrap_or_default().to_string_lossy(),
            "path": path.display().to_string(),
            "is_error": true,
            "error_short": if error_msg.len() > 50 {
                format!("{}...", &error_msg[..50])
            } else {
                error_msg.clone()
            },
            "error_full": error_msg
        }));
    }

    files
}

// Helper functions

fn calculate_overall_data_quality_score(metrics: &DataQualityMetrics) -> f64 {
    let completeness_score = (1.0 - metrics.missing_values_ratio) * 100.0;
    let consistency_score = metrics.data_type_consistency * 100.0;
    let uniqueness_score = metrics.key_uniqueness * 100.0;
    let accuracy_score = (1.0 - metrics.outlier_ratio) * 100.0;
    let timeliness_score = 100.0 - metrics.stale_data_ratio;
    (completeness_score + consistency_score + uniqueness_score + accuracy_score + timeliness_score)
        / 5.0
}

fn get_assessment_class(score: f64) -> &'static str {
    if score >= 90.0 {
        "excellent"
    } else if score >= 70.0 {
        "good"
    } else if score >= 50.0 {
        "fair"
    } else {
        "poor"
    }
}

fn get_stat_class(value: f64, reverse: bool) -> &'static str {
    let threshold = if reverse { value } else { 1.0 - value };

    if threshold < 0.05 {
        "excellent"
    } else if threshold < 0.15 {
        "good"
    } else if threshold < 0.30 {
        "warning"
    } else {
        "error"
    }
}

fn format_data_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::String => "String",
        DataType::Integer => "Integer",
        DataType::Float => "Float",
        DataType::Date => "Date",
    }
}

fn format_type_class(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::String => "type-string",
        DataType::Integer => "type-integer",
        DataType::Float => "type-float",
        DataType::Date => "type-date",
    }
}

fn format_column_stats_json(stats: &ColumnStats) -> serde_json::Value {
    match stats {
        ColumnStats::Numeric { min, max, mean } => {
            json!({
                "type": "numeric",
                "min": format!("{:.2}", min),
                "max": format!("{:.2}", max),
                "mean": format!("{:.2}", mean)
            })
        }
        ColumnStats::Text {
            min_length,
            max_length,
            avg_length,
        } => {
            json!({
                "type": "text",
                "min_length": min_length,
                "max_length": max_length,
                "avg_length": format!("{:.1}", avg_length)
            })
        }
    }
}

fn format_patterns_json(patterns: &[crate::types::Pattern]) -> Vec<serde_json::Value> {
    patterns
        .iter()
        .take(5)
        .map(|p| {
            json!({
                "name": p.name.clone(),
                "count": p.match_count,
                "percentage": format!("{:.1}", p.match_percentage)
            })
        })
        .collect()
}
