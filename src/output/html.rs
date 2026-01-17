use crate::core::batch::{BatchResult, BatchSummary};
use crate::types::{
    ColumnProfile, ColumnStats, DataQualityMetrics, DataSource, DataType, QualityReport,
};
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
    let mut context = build_report_context(report);

    // Add CSS content to context for inline embedding
    if let Some(obj) = context.as_object_mut() {
        obj.insert(
            "style_css".to_string(),
            serde_json::Value::String(STYLE_CSS.to_string()),
        );
    }

    let html_content = handlebars.render("single_report", &context)?;

    // Write single self-contained HTML file
    fs::write(output_path, html_content)?;

    Ok(())
}

/// Build context data for single report template
fn build_report_context(report: &QualityReport) -> serde_json::Value {
    // Extract file name from data source
    let (file_name, file_path, file_size_mb, parquet_metadata) = match &report.data_source {
        DataSource::File {
            path,
            size_bytes,
            parquet_metadata,
            ..
        } => {
            let name = Path::new(path)
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string();
            (
                name,
                path.clone(),
                *size_bytes as f64 / 1_048_576.0,
                parquet_metadata.clone(),
            )
        }
        DataSource::Query { statement, engine, .. } => {
            let name = format!("{} Query", engine);
            (
                name,
                statement.clone(),
                0.0,
                None,
            )
        }
    };

    let sampling_info = if report.scan_info.sampling_ratio < 1.0 {
        Some(json!({
            "rows_scanned": report.scan_info.rows_scanned,
            "sampling_percentage": format!("{:.1}", report.scan_info.sampling_ratio * 100.0)
        }))
    } else {
        None
    };

    // Build Parquet metadata if available
    let parquet_meta_json = parquet_metadata.as_ref().map(|meta| {
        let compression_ratio = if meta.uncompressed_size_bytes.is_some() && meta.compressed_size_bytes > 0 {
            let uncompressed = meta.uncompressed_size_bytes.unwrap();
            if uncompressed > 0 {
                Some(format!("{:.1}x", uncompressed as f64 / meta.compressed_size_bytes as f64))
            } else {
                None
            }
        } else {
            None
        };

        json!({
            "num_row_groups": meta.num_row_groups,
            "compression": meta.compression,
            "version": meta.version,
            "schema_summary": meta.schema_summary,
            "compressed_size_bytes": meta.compressed_size_bytes,
            "compressed_size_mb": format!("{:.2}", meta.compressed_size_bytes as f64 / 1_048_576.0),
            "uncompressed_size_bytes": meta.uncompressed_size_bytes,
            "uncompressed_size_mb": meta.uncompressed_size_bytes.map(|s| format!("{:.2}", s as f64 / 1_048_576.0)),
            "compression_ratio": compression_ratio
        })
    });

    json!({
        "file_name": file_name,
        "file_path": file_path,
        "file_size_mb": format!("{:.1}", file_size_mb),
        "total_rows": report.scan_info.total_rows.to_string(),
        "total_columns": report.scan_info.total_columns,
        "scan_time_ms": report.scan_info.scan_time_ms,
        "sampling_info": sampling_info,
        "parquet_metadata": parquet_meta_json,
        "data_quality_metrics": build_data_quality_metrics_context(&report.data_quality_metrics),
        "column_profiles": build_column_profiles_context(&report.column_profiles)
    })
}

/// Build context for data quality metrics
fn build_data_quality_metrics_context(metrics: &DataQualityMetrics) -> serde_json::Value {
    // Use the official ISO 8000/25012 weighted calculation from types.rs
    let overall_score = metrics.overall_score();
    let assessment_class = get_assessment_class(overall_score);

    json!({
        "overall_score": format!("{:.1}", overall_score),
        "assessment_class": assessment_class,
        "completeness": {
            "missing_ratio": format!("{:.2}", metrics.missing_values_ratio),
            "complete_ratio": format!("{:.2}", metrics.complete_records_ratio),
            "missing_class": get_stat_class(metrics.missing_values_ratio / 100.0, true),
            "null_columns": if !metrics.null_columns.is_empty() { Some(metrics.null_columns.len()) } else { None }
        },
        "consistency": {
            "type_consistency": format!("{:.1}", metrics.data_type_consistency),
            "type_class": get_stat_class((100.0 - metrics.data_type_consistency) / 100.0, true),
            "format_violations": if metrics.format_violations > 0 { Some(metrics.format_violations) } else { None },
            "encoding_issues": if metrics.encoding_issues > 0 { Some(metrics.encoding_issues) } else { None }
        },
        "uniqueness": {
            "key_uniqueness": format!("{:.1}", metrics.key_uniqueness),
            "uniqueness_class": get_stat_class((100.0 - metrics.key_uniqueness) / 100.0, true),
            "has_duplicates": metrics.duplicate_rows > 0,
            "duplicate_rows": if metrics.duplicate_rows > 0 { Some(metrics.duplicate_rows) } else { None },
            "high_cardinality_warning": if metrics.high_cardinality_warning { Some(1) } else { None }
        },
        "accuracy": {
            "outlier_ratio": format!("{:.2}", metrics.outlier_ratio),
            "outlier_class": get_stat_class(metrics.outlier_ratio / 100.0, true),
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
            let null_percentage = if col.total_count > 0 {
                (col.null_count as f64 / col.total_count as f64) * 100.0
            } else {
                0.0
            };

            // Calculate uniqueness ratio safely
            let unique_count_val = col.unique_count.unwrap_or(0);
            let uniqueness_ratio = if col.total_count > 0 {
                unique_count_val as f64 / col.total_count as f64
            } else {
                0.0
            };

            let (null_class, null_text) = if col.null_count == 0 {
                ("success", "No nulls".to_string())
            } else if col.null_count < col.total_count / 10 {
                ("success", format!("{:.1}% nulls", null_percentage))
            } else {
                ("warning", format!("{:.1}% nulls", null_percentage))
            };

            // Color logic for new UI
            let null_color_class = if col.null_count == 0 {
                "bg-green-500"
            } else if null_percentage < 5.0 {
                "bg-yellow-500"
            } else {
                "bg-red-500"
            };

            let null_text_color_class = if col.null_count > 0 {
                "text-red-600"
            } else {
                "text-gray-900"
            };

            let type_badge_class = match col.data_type {
                DataType::Integer | DataType::Float => "bg-blue-100 text-blue-800",
                DataType::String => "bg-green-100 text-green-800",
                DataType::Date => "bg-purple-100 text-purple-800",
            };

            json!({
                "name": col.name,
                "data_type": format_data_type(&col.data_type),
                "type_class": format_type_class(&col.data_type),
                "type_badge_class": type_badge_class,
                "null_class": null_class,
                "null_color_class": null_color_class,
                "null_text_color_class": null_text_color_class,
                "null_text": null_text,
                "total_count": col.total_count,
                "null_count": col.null_count,
                "null_percentage": format!("{:.1}", null_percentage),
                "null_percentage_raw": null_percentage,
                "completeness_raw": 100.0 - null_percentage,
                "completeness_formatted": format!("{:.1}", 100.0 - null_percentage),
                "has_nulls": col.null_count > 0,
                "unique_count": col.unique_count,
                "uniqueness_ratio": format!("{:.1}", uniqueness_ratio * 100.0),
                "uniqueness_ratio_raw": uniqueness_ratio * 100.0,
                "stats": format_column_stats_json(&col.stats),
                "patterns": format_patterns_json(&col.patterns)
            })
        })
        .collect()
}

/// Generate an aggregated HTML report for batch processing results
pub fn generate_batch_html_report(batch_result: &BatchResult, output_path: &Path) -> Result<()> {
    let handlebars = init_handlebars()?;
    let mut context = build_batch_context(batch_result);

    // Add CSS and JS content to context for inline embedding
    if let Some(obj) = context.as_object_mut() {
        obj.insert(
            "style_css".to_string(),
            serde_json::Value::String(STYLE_CSS.to_string()),
        );
        obj.insert(
            "batch_css".to_string(),
            serde_json::Value::String(BATCH_CSS.to_string()),
        );
        obj.insert(
            "dashboard_js".to_string(),
            serde_json::Value::String(DASHBOARD_JS.to_string()),
        );
    }

    let html_content = handlebars.render("batch_dashboard", &context)?;

    // Write single self-contained HTML file
    fs::write(output_path, html_content)?;

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
        let metrics = &report.data_quality_metrics;
        {
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

    // Calculate average of each metric (all are already 0-100 percentages)
    let avg_complete_records = reports
        .values()
        .map(|r| r.data_quality_metrics.complete_records_ratio)
        .sum::<f64>()
        / count as f64;
    let avg_type_consistency = total_type_consistency / count as f64;
    let avg_key_uniqueness = total_key_uniqueness / count as f64;
    let avg_outlier_ratio = total_outlier_ratio / count as f64;
    let avg_stale_data_ratio = total_stale_data_ratio / count as f64;
    let avg_missing_ratio = total_missing_ratio / count as f64;

    // Calculate weighted overall score using ISO 8000/25012 weights
    // All metrics are already percentages (0-100), so we apply weights directly
    let completeness_score = avg_complete_records * 0.3;
    let consistency_score = avg_type_consistency * 0.25;
    let uniqueness_score = avg_key_uniqueness * 0.2;
    let accuracy_score = (100.0 - avg_outlier_ratio) * 0.15;
    let timeliness_score = (100.0 - avg_stale_data_ratio) * 0.1;
    let overall_score = completeness_score
        + consistency_score
        + uniqueness_score
        + accuracy_score
        + timeliness_score;

    let assessment_class = get_assessment_class(overall_score);

    Some(json!({
        "overall_score": format!("{:.1}", overall_score),
        "assessment_class": assessment_class,
        "completeness": {
            "missing_ratio": format!("{:.2}", avg_missing_ratio),
            "complete_ratio": format!("{:.2}", 100.0 - avg_missing_ratio),
            "missing_class": get_stat_class(avg_missing_ratio / 100.0, true),
            "null_columns": if total_null_columns > 0 { Some(total_null_columns) } else { None }
        },
        "consistency": {
            "type_consistency": format!("{:.1}", avg_type_consistency),
            "type_class": get_stat_class((100.0 - avg_type_consistency) / 100.0, true),
            "format_violations": if total_format_violations > 0 { Some(total_format_violations) } else { None },
            "encoding_issues": if total_encoding_issues > 0 { Some(total_encoding_issues) } else { None }
        },
        "uniqueness": {
            "key_uniqueness": format!("{:.1}", avg_key_uniqueness),
            "uniqueness_class": get_stat_class((100.0 - avg_key_uniqueness) / 100.0, true),
            "has_duplicates": total_duplicate_rows > 0,
            "duplicate_rows": if total_duplicate_rows > 0 { Some(total_duplicate_rows) } else { None },
            "high_cardinality_warning": if total_high_cardinality > 0 { Some(total_high_cardinality) } else { None }
        },
        "accuracy": {
            "outlier_ratio": format!("{:.2}", avg_outlier_ratio),
            "outlier_class": get_stat_class(avg_outlier_ratio / 100.0, true),
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
            let quality_score = report.data_quality_metrics.overall_score();

            let quality_class = if quality_score >= 80.0 {
                "success"
            } else if quality_score >= 60.0 {
                "warning"
            } else {
                "critical"
            };

            // Build Parquet metadata if available (same as single report)
            let parquet_metadata = match &report.data_source {
                DataSource::File { parquet_metadata: Some(meta), .. } => {
                    let compression_ratio = if meta.uncompressed_size_bytes.is_some() && meta.compressed_size_bytes > 0 {
                        let uncompressed = meta.uncompressed_size_bytes.unwrap();
                        if uncompressed > 0 {
                            Some(format!("{:.1}x", uncompressed as f64 / meta.compressed_size_bytes as f64))
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                    Some(json!({
                        "num_row_groups": meta.num_row_groups,
                        "compression": meta.compression,
                        "version": meta.version,
                        "schema_summary": meta.schema_summary,
                        "compressed_size_bytes": meta.compressed_size_bytes,
                        "compressed_size_mb": format!("{:.2}", meta.compressed_size_bytes as f64 / 1_048_576.0),
                        "uncompressed_size_bytes": meta.uncompressed_size_bytes,
                        "uncompressed_size_mb": meta.uncompressed_size_bytes.map(|s| format!("{:.2}", s as f64 / 1_048_576.0)),
                        "compression_ratio": compression_ratio
                    }))
                }
                _ => None,
            };

            json!({
                "file_name": path.file_name().unwrap_or_default().to_string_lossy(),
                "file_path": path.display().to_string(),
                "quality_score": format!("{:.0}", quality_score),
                "quality_class": quality_class,
                "columns": report.scan_info.total_columns,
                "total_rows": report.scan_info.total_rows.to_string(),
                "error": false,
                "metrics": build_data_quality_metrics_context(&report.data_quality_metrics),
                "parquet_metadata": parquet_metadata
            })
        })
        .collect();

    // Add error files
    for (path, error_msg) in errors {
        files.push(json!({
            "file_name": path.file_name().unwrap_or_default().to_string_lossy(),
            "file_path": path.display().to_string(),
            "error": true,
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
        ColumnStats::Numeric {
            min,
            max,
            mean,
            std_dev,
            median,
            ..
        } => {
            let mut stats_json = json!({
                "type": "numeric",
                "min": format!("{:.2}", min),
                "max": format!("{:.2}", max),
                "mean": format!("{:.2}", mean),
                "std_dev": format!("{:.2}", std_dev)
            });

            if let Some(med) = median {
                stats_json["median"] = json!(format!("{:.2}", med));
            }

            stats_json
        }
        ColumnStats::Text {
            min_length,
            max_length,
            avg_length,
            ..
        } => {
            json!({
                "type": "text",
                "min_length": min_length,
                "max_length": max_length,
                "avg_length": format!("{:.1}", avg_length)
            })
        }
        ColumnStats::DateTime {
            min_datetime,
            max_datetime,
            duration_days,
            ..
        } => {
            json!({
                "type": "datetime",
                "min": min_datetime,
                "max": max_datetime,
                "duration_days": format!("{:.0}", duration_days)
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
