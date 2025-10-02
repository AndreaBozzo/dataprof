#![allow(clippy::useless_conversion)]

use pyo3::prelude::*;

use crate::core::batch::BatchResult;
use crate::types::{ColumnProfile, DataQualityMetrics, DataType, QualityIssue, QualityReport};

/// Python wrapper for ColumnProfile
#[pyclass]
#[derive(Clone)]
pub struct PyColumnProfile {
    #[pyo3(get)]
    pub name: String,
    #[pyo3(get)]
    pub data_type: String,
    #[pyo3(get)]
    pub total_count: usize,
    #[pyo3(get)]
    pub null_count: usize,
    #[pyo3(get)]
    pub unique_count: Option<usize>,
    #[pyo3(get)]
    pub null_percentage: f64,
    #[pyo3(get)]
    pub uniqueness_ratio: f64,
}

impl From<&ColumnProfile> for PyColumnProfile {
    fn from(profile: &ColumnProfile) -> Self {
        let null_percentage = if profile.total_count > 0 {
            (profile.null_count as f64 / profile.total_count as f64) * 100.0
        } else {
            0.0
        };

        let uniqueness_ratio = if let Some(unique) = profile.unique_count {
            if profile.total_count > 0 {
                unique as f64 / profile.total_count as f64
            } else {
                0.0
            }
        } else {
            0.0
        };

        Self {
            name: profile.name.clone(),
            data_type: match profile.data_type {
                DataType::Integer => "integer".to_string(),
                DataType::Float => "float".to_string(),
                DataType::String => "string".to_string(),
                DataType::Date => "date".to_string(),
            },
            total_count: profile.total_count,
            null_count: profile.null_count,
            unique_count: profile.unique_count,
            null_percentage,
            uniqueness_ratio,
        }
    }
}

/// Python wrapper for QualityIssue
#[pyclass]
#[derive(Clone)]
pub struct PyQualityIssue {
    #[pyo3(get)]
    pub issue_type: String,
    #[pyo3(get)]
    pub column: String,
    #[pyo3(get)]
    pub severity: String,
    #[pyo3(get)]
    pub count: Option<usize>,
    #[pyo3(get)]
    pub percentage: Option<f64>,
    #[pyo3(get)]
    pub description: String,
}

impl From<&QualityIssue> for PyQualityIssue {
    fn from(issue: &QualityIssue) -> Self {
        match issue {
            QualityIssue::NullValues {
                column,
                count,
                percentage,
            } => Self {
                issue_type: "null_values".to_string(),
                column: column.to_string(),
                severity: "medium".to_string(),
                count: Some(*count),
                percentage: Some(*percentage),
                description: format!(
                    "{} null values ({}%) in column '{}'",
                    count, percentage, column
                ),
            },
            QualityIssue::Duplicates { column, count } => Self {
                issue_type: "duplicates".to_string(),
                column: column.to_string(),
                severity: "low".to_string(),
                count: Some(*count),
                percentage: None,
                description: format!("{} duplicate values in column '{}'", count, column),
            },
            QualityIssue::Outliers {
                column,
                values,
                threshold,
            } => Self {
                issue_type: "outliers".to_string(),
                column: column.to_string(),
                severity: "medium".to_string(),
                count: Some(values.len()),
                percentage: None,
                description: format!(
                    "{} outlier values in column '{}' (threshold: {}): {:?}",
                    values.len(),
                    column,
                    threshold,
                    values
                ),
            },
            QualityIssue::MixedDateFormats { column, formats } => Self {
                issue_type: "mixed_date_formats".to_string(),
                column: column.to_string(),
                severity: "high".to_string(),
                count: Some(formats.len()),
                percentage: None,
                description: format!("Mixed date formats in column '{}': {:?}", column, formats),
            },
            QualityIssue::MixedTypes { column, types } => Self {
                issue_type: "mixed_types".to_string(),
                column: column.to_string(),
                severity: "high".to_string(),
                count: Some(types.len()),
                percentage: None,
                description: format!("Mixed data types in column '{}': {:?}", column, types),
            },
        }
    }
}

/// Python wrapper for QualityReport
#[pyclass]
#[derive(Clone)]
pub struct PyQualityReport {
    #[pyo3(get)]
    pub file_path: String,
    #[pyo3(get)]
    pub total_rows: Option<usize>,
    #[pyo3(get)]
    pub total_columns: usize,
    #[pyo3(get)]
    pub column_profiles: Vec<PyColumnProfile>,
    #[pyo3(get)]
    pub issues: Vec<PyQualityIssue>,
    #[pyo3(get)]
    pub rows_scanned: usize,
    #[pyo3(get)]
    pub sampling_ratio: f64,
    #[pyo3(get)]
    pub scan_time_ms: u128,
    #[pyo3(get)]
    pub data_quality_metrics: Option<PyDataQualityMetrics>,
}

impl From<&QualityReport> for PyQualityReport {
    fn from(report: &QualityReport) -> Self {
        Self {
            file_path: report.file_info.path.clone(),
            total_rows: report.file_info.total_rows,
            total_columns: report.file_info.total_columns,
            column_profiles: report
                .column_profiles
                .iter()
                .map(PyColumnProfile::from)
                .collect(),
            issues: report.issues.iter().map(PyQualityIssue::from).collect(),
            rows_scanned: report.scan_info.rows_scanned,
            sampling_ratio: report.scan_info.sampling_ratio,
            scan_time_ms: report.scan_info.scan_time_ms,
            data_quality_metrics: report
                .data_quality_metrics
                .as_ref()
                .map(PyDataQualityMetrics::from),
        }
    }
}

#[pymethods]
impl PyQualityReport {
    /// Calculate overall quality score (0-100)
    fn quality_score(&self) -> PyResult<f64> {
        if self.issues.is_empty() {
            return Ok(100.0);
        }

        let mut score: f64 = 100.0;

        for issue in &self.issues {
            let penalty = match issue.issue_type.as_str() {
                "mixed_date_formats" => 20.0,
                "null_values" => {
                    if let Some(percentage) = issue.percentage {
                        if percentage > 50.0 {
                            20.0
                        } else if percentage > 20.0 {
                            15.0
                        } else {
                            10.0
                        }
                    } else {
                        10.0
                    }
                }
                "outlier_values" => 15.0,
                "invalid_email_format" => 10.0,
                "duplicate_values" => 5.0,
                "inconsistent_casing" => 3.0,
                _ => 5.0,
            };
            score -= penalty;
        }

        Ok(score.max(0.0))
    }

    /// Get issues by severity
    fn issues_by_severity(&self, severity: &str) -> Vec<PyQualityIssue> {
        self.issues
            .iter()
            .filter(|issue| issue.severity == severity)
            .cloned()
            .collect()
    }
}

/// Python wrapper for DataQualityMetrics
#[pyclass]
#[derive(Clone)]
pub struct PyDataQualityMetrics {
    // Completeness
    #[pyo3(get)]
    pub missing_values_ratio: f64,
    #[pyo3(get)]
    pub complete_records_ratio: f64,
    #[pyo3(get)]
    pub null_columns: Vec<String>,

    // Consistency
    #[pyo3(get)]
    pub data_type_consistency: f64,
    #[pyo3(get)]
    pub format_violations: usize,
    #[pyo3(get)]
    pub encoding_issues: usize,

    // Uniqueness
    #[pyo3(get)]
    pub duplicate_rows: usize,
    #[pyo3(get)]
    pub key_uniqueness: f64,
    #[pyo3(get)]
    pub high_cardinality_warning: bool,

    // Accuracy
    #[pyo3(get)]
    pub outlier_ratio: f64,
    #[pyo3(get)]
    pub range_violations: usize,
    #[pyo3(get)]
    pub negative_values_in_positive: usize,

    // Timeliness (ISO 8000-8)
    #[pyo3(get)]
    pub future_dates_count: usize,
    #[pyo3(get)]
    pub stale_data_ratio: f64,
    #[pyo3(get)]
    pub temporal_violations: usize,
}

impl From<&DataQualityMetrics> for PyDataQualityMetrics {
    fn from(metrics: &DataQualityMetrics) -> Self {
        Self {
            missing_values_ratio: metrics.missing_values_ratio,
            complete_records_ratio: metrics.complete_records_ratio,
            null_columns: metrics.null_columns.clone(),
            data_type_consistency: metrics.data_type_consistency,
            format_violations: metrics.format_violations,
            encoding_issues: metrics.encoding_issues,
            duplicate_rows: metrics.duplicate_rows,
            key_uniqueness: metrics.key_uniqueness,
            high_cardinality_warning: metrics.high_cardinality_warning,
            outlier_ratio: metrics.outlier_ratio,
            range_violations: metrics.range_violations,
            negative_values_in_positive: metrics.negative_values_in_positive,
            future_dates_count: metrics.future_dates_count,
            stale_data_ratio: metrics.stale_data_ratio,
            temporal_violations: metrics.temporal_violations,
        }
    }
}

#[pymethods]
impl PyDataQualityMetrics {
    /// Calculate overall data quality score (0-100) based on ISO 8000/25012 dimensions
    ///
    /// Uses the same weighted formula as the core Rust implementation:
    /// - Completeness: 30% (complete_records_ratio - already percentage 0-100)
    /// - Consistency: 25% (data_type_consistency - already percentage 0-100)
    /// - Uniqueness: 20% (key_uniqueness - already percentage 0-100)
    /// - Accuracy: 15% (100 - outlier_ratio*100)
    /// - Timeliness: 10% (100 - stale_data_ratio*100)
    fn overall_quality_score(&self) -> PyResult<f64> {
        let completeness = self.complete_records_ratio * 0.3;
        let consistency = self.data_type_consistency * 0.25;
        let uniqueness = self.key_uniqueness * 0.2;

        // outlier_ratio and stale_data_ratio are ratios (0-1), convert to percentage
        let accuracy = (100.0 - self.outlier_ratio * 100.0) * 0.15;
        let timeliness = (100.0 - self.stale_data_ratio * 100.0) * 0.1;

        Ok(completeness + consistency + uniqueness + accuracy + timeliness)
    }

    /// Get completeness summary
    fn completeness_summary(&self) -> String {
        format!(
            "Missing values: {:.1}% | Complete records: {:.1}% | Null columns: {}",
            self.missing_values_ratio,
            self.complete_records_ratio,
            self.null_columns.len()
        )
    }

    /// Get consistency summary
    fn consistency_summary(&self) -> String {
        format!(
            "Type consistency: {:.1}% | Format violations: {} | Encoding issues: {}",
            self.data_type_consistency, self.format_violations, self.encoding_issues
        )
    }

    /// Get uniqueness summary
    fn uniqueness_summary(&self) -> String {
        format!(
            "Duplicate rows: {} | Key uniqueness: {:.1}% | High cardinality warning: {}",
            self.duplicate_rows, self.key_uniqueness, self.high_cardinality_warning
        )
    }

    /// Get accuracy summary
    fn accuracy_summary(&self) -> String {
        format!(
            "Outlier ratio: {:.1}% | Range violations: {} | Negative values in positive fields: {}",
            self.outlier_ratio, self.range_violations, self.negative_values_in_positive
        )
    }

    /// Get timeliness summary
    fn timeliness_summary(&self) -> String {
        format!(
            "Future dates: {} | Stale data: {:.1}% | Temporal violations: {}",
            self.future_dates_count,
            self.stale_data_ratio * 100.0,
            self.temporal_violations
        )
    }

    /// Get detailed report as string summary (simplified due to PyO3 type constraints)
    fn summary_dict(&self) -> std::collections::HashMap<String, String> {
        let mut dict = std::collections::HashMap::new();

        // Completeness
        dict.insert(
            "missing_values_ratio".to_string(),
            format!("{:.2}", self.missing_values_ratio),
        );
        dict.insert(
            "complete_records_ratio".to_string(),
            format!("{:.2}", self.complete_records_ratio),
        );
        dict.insert(
            "null_columns_count".to_string(),
            self.null_columns.len().to_string(),
        );

        // Consistency
        dict.insert(
            "data_type_consistency".to_string(),
            format!("{:.2}", self.data_type_consistency),
        );
        dict.insert(
            "format_violations".to_string(),
            self.format_violations.to_string(),
        );
        dict.insert(
            "encoding_issues".to_string(),
            self.encoding_issues.to_string(),
        );

        // Uniqueness
        dict.insert(
            "duplicate_rows".to_string(),
            self.duplicate_rows.to_string(),
        );
        dict.insert(
            "key_uniqueness".to_string(),
            format!("{:.2}", self.key_uniqueness),
        );
        dict.insert(
            "high_cardinality_warning".to_string(),
            self.high_cardinality_warning.to_string(),
        );

        // Accuracy
        dict.insert(
            "outlier_ratio".to_string(),
            format!("{:.2}", self.outlier_ratio),
        );
        dict.insert(
            "range_violations".to_string(),
            self.range_violations.to_string(),
        );
        dict.insert(
            "negative_values_in_positive".to_string(),
            self.negative_values_in_positive.to_string(),
        );

        // Timeliness
        dict.insert(
            "future_dates_count".to_string(),
            self.future_dates_count.to_string(),
        );
        dict.insert(
            "stale_data_ratio".to_string(),
            format!("{:.2}", self.stale_data_ratio),
        );
        dict.insert(
            "temporal_violations".to_string(),
            self.temporal_violations.to_string(),
        );

        dict
    }

    /// Rich HTML representation for Jupyter notebooks
    fn _repr_html_(&self) -> String {
        let overall_score = self.overall_quality_score().unwrap_or(0.0);
        let score_color = if overall_score >= 80.0 {
            "#4CAF50"
        } else if overall_score >= 60.0 {
            "#FF9800"
        } else {
            "#F44336"
        };

        format!(
            r#"
<div style="font-family: Arial, sans-serif; border: 1px solid #ddd; border-radius: 8px; padding: 16px; margin: 8px 0;">
    <h3 style="margin: 0 0 16px 0; color: #333;">
        📊 Data Quality Metrics
        <span style="background-color: {}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.9em; margin-left: 8px;">
            {:.1}%
        </span>
    </h3>

    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px;">

        <!-- Completeness -->
        <div style="border: 1px solid #e0e0e0; border-radius: 4px; padding: 12px;">
            <h4 style="margin: 0 0 8px 0; color: #4CAF50;">📋 Completeness</h4>
            <div style="margin-bottom: 8px;">
                <strong>Missing Values:</strong> {:.1}%
                <div style="background-color: #e0e0e0; border-radius: 4px; height: 6px; margin: 2px 0;">
                    <div style="background-color: #F44336; height: 100%; border-radius: 4px; width: {:.1}%;"></div>
                </div>
            </div>
            <div style="margin-bottom: 8px;">
                <strong>Complete Records:</strong> {:.1}%
                <div style="background-color: #e0e0e0; border-radius: 4px; height: 6px; margin: 2px 0;">
                    <div style="background-color: #4CAF50; height: 100%; border-radius: 4px; width: {:.1}%;"></div>
                </div>
            </div>
            <div><strong>Null Columns:</strong> {}</div>
        </div>

        <!-- Consistency -->
        <div style="border: 1px solid #e0e0e0; border-radius: 4px; padding: 12px;">
            <h4 style="margin: 0 0 8px 0; color: #2196F3;">🔧 Consistency</h4>
            <div style="margin-bottom: 8px;">
                <strong>Type Consistency:</strong> {:.1}%
                <div style="background-color: #e0e0e0; border-radius: 4px; height: 6px; margin: 2px 0;">
                    <div style="background-color: #2196F3; height: 100%; border-radius: 4px; width: {:.1}%;"></div>
                </div>
            </div>
            <div style="margin-bottom: 4px;"><strong>Format Violations:</strong> {}</div>
            <div><strong>Encoding Issues:</strong> {}</div>
        </div>

        <!-- Uniqueness -->
        <div style="border: 1px solid #e0e0e0; border-radius: 4px; padding: 12px;">
            <h4 style="margin: 0 0 8px 0; color: #FF9800;">🔑 Uniqueness</h4>
            <div style="margin-bottom: 8px;">
                <strong>Key Uniqueness:</strong> {:.1}%
                <div style="background-color: #e0e0e0; border-radius: 4px; height: 6px; margin: 2px 0;">
                    <div style="background-color: #FF9800; height: 100%; border-radius: 4px; width: {:.1}%;"></div>
                </div>
            </div>
            <div style="margin-bottom: 4px;"><strong>Duplicate Rows:</strong> {}</div>
            <div><strong>High Cardinality Warning:</strong> {}</div>
        </div>

        <!-- Accuracy -->
        <div style="border: 1px solid #e0e0e0; border-radius: 4px; padding: 12px;">
            <h4 style="margin: 0 0 8px 0; color: #9C27B0;">🎯 Accuracy</h4>
            <div style="margin-bottom: 8px;">
                <strong>Outlier Ratio:</strong> {:.1}%
                <div style="background-color: #e0e0e0; border-radius: 4px; height: 6px; margin: 2px 0;">
                    <div style="background-color: #9C27B0; height: 100%; border-radius: 4px; width: {:.1}%;"></div>
                </div>
            </div>
            <div style="margin-bottom: 4px;"><strong>Range Violations:</strong> {}</div>
            <div><strong>Negative in Positive:</strong> {}</div>
        </div>

        <!-- Timeliness -->
        <div style="border: 1px solid #e0e0e0; border-radius: 4px; padding: 12px;">
            <h4 style="margin: 0 0 8px 0; color: #607D8B;">⏱️ Timeliness</h4>
            <div style="margin-bottom: 8px;">
                <strong>Stale Data Ratio:</strong> {:.1}%
                <div style="background-color: #e0e0e0; border-radius: 4px; height: 6px; margin: 2px 0;">
                    <div style="background-color: #607D8B; height: 100%; border-radius: 4px; width: {:.1}%;"></div>
                </div>
            </div>
            <div style="margin-bottom: 4px;"><strong>Future Dates:</strong> {}</div>
            <div><strong>Temporal Violations:</strong> {}</div>
        </div>

    </div>
</div>
"#,
            score_color,
            overall_score,
            self.missing_values_ratio,
            self.missing_values_ratio.min(100.0),
            self.complete_records_ratio,
            self.complete_records_ratio.min(100.0),
            self.null_columns.len(),
            self.data_type_consistency,
            self.data_type_consistency.min(100.0),
            self.format_violations,
            self.encoding_issues,
            self.key_uniqueness,
            self.key_uniqueness.min(100.0),
            self.duplicate_rows,
            self.high_cardinality_warning,
            self.outlier_ratio,
            self.outlier_ratio.min(100.0),
            self.range_violations,
            self.negative_values_in_positive,
            self.stale_data_ratio * 100.0,
            (self.stale_data_ratio * 100.0).min(100.0),
            self.future_dates_count,
            self.temporal_violations
        )
    }

    /// Summary string representation
    fn __str__(&self) -> String {
        format!(
            "DataQualityMetrics(score={:.1}%, completeness={:.1}%, consistency={:.1}%, uniqueness={:.1}%, accuracy={:.1}%, timeliness={:.1}%)",
            self.overall_quality_score().unwrap_or(0.0),
            self.complete_records_ratio,
            self.data_type_consistency,
            self.key_uniqueness,
            100.0 - self.outlier_ratio * 100.0,
            100.0 - self.stale_data_ratio * 100.0
        )
    }
}

/// Python wrapper for BatchResult
#[pyclass]
#[derive(Clone)]
pub struct PyBatchResult {
    #[pyo3(get)]
    pub processed_files: usize,
    #[pyo3(get)]
    pub failed_files: usize,
    #[pyo3(get)]
    pub total_duration_secs: f64,
    #[pyo3(get)]
    pub total_quality_issues: usize,
    #[pyo3(get)]
    pub average_quality_score: f64,
}

impl From<&BatchResult> for PyBatchResult {
    fn from(result: &BatchResult) -> Self {
        Self {
            processed_files: result.summary.successful,
            failed_files: result.summary.failed,
            total_duration_secs: result.summary.processing_time_seconds,
            total_quality_issues: result.summary.total_issues,
            average_quality_score: result.summary.average_quality_score,
        }
    }
}
