#![allow(clippy::useless_conversion)]

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::path::Path;

use crate::analysis::ml_readiness::{
    FeatureImportancePotential, ImplementationEffort, MlBlockingIssue, MlFeatureType,
    MlReadinessLevel, MlRecommendation, RecommendationPriority,
};
use crate::analysis::{
    FeatureAnalysis, MlReadinessEngine, MlReadinessScore, PreprocessingSuggestion,
};
use crate::core::batch::{BatchProcessor, BatchResult};
use crate::types::{ColumnProfile, DataType, QualityIssue, QualityReport};
use crate::{analyze_csv, analyze_csv_robust, analyze_json};

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

/// Analyze a single CSV file
#[pyfunction]
fn analyze_csv_file(path: &str) -> PyResult<Vec<PyColumnProfile>> {
    let profiles = analyze_csv(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    Ok(profiles.iter().map(PyColumnProfile::from).collect())
}

/// Analyze a single CSV file with quality assessment
#[pyfunction]
fn analyze_csv_with_quality(path: &str) -> PyResult<PyQualityReport> {
    let quality_report = analyze_csv_robust(Path::new(path)).map_err(|e| {
        PyRuntimeError::new_err(format!("Failed to analyze CSV with quality: {}", e))
    })?;

    let py_quality = PyQualityReport::from(&quality_report);

    Ok(py_quality)
}

/// Analyze a JSON file
#[pyfunction]
fn analyze_json_file(path: &str) -> PyResult<Vec<PyColumnProfile>> {
    let profiles = analyze_json(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze JSON: {}", e)))?;

    Ok(profiles.iter().map(PyColumnProfile::from).collect())
}

/// Batch process multiple files using glob pattern
#[pyfunction]
#[pyo3(signature = (pattern, parallel=None, max_concurrent=None))]
fn batch_analyze_glob(
    pattern: &str,
    parallel: Option<bool>,
    max_concurrent: Option<usize>,
) -> PyResult<PyBatchResult> {
    use crate::core::batch::BatchConfig;

    let config = BatchConfig {
        parallel: parallel.unwrap_or(true),
        max_concurrent: max_concurrent.unwrap_or_else(num_cpus::get),
        recursive: false, // Not applicable for glob patterns
        extensions: vec!["csv".to_string(), "json".to_string(), "jsonl".to_string()],
        exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
    };

    let processor = BatchProcessor::with_config(config);
    let result = processor
        .process_glob(pattern)
        .map_err(|e| PyRuntimeError::new_err(format!("Batch processing failed: {}", e)))?;

    Ok(PyBatchResult::from(&result))
}

/// Batch process all files in a directory
#[pyfunction]
#[pyo3(signature = (directory, recursive=None, parallel=None, max_concurrent=None))]
fn batch_analyze_directory(
    directory: &str,
    recursive: Option<bool>,
    parallel: Option<bool>,
    max_concurrent: Option<usize>,
) -> PyResult<PyBatchResult> {
    use crate::core::batch::BatchConfig;

    let config = BatchConfig {
        parallel: parallel.unwrap_or(true),
        max_concurrent: max_concurrent.unwrap_or_else(num_cpus::get),
        recursive: recursive.unwrap_or(false),
        extensions: vec!["csv".to_string(), "json".to_string(), "jsonl".to_string()],
        exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
    };

    let processor = BatchProcessor::with_config(config);
    let result = processor
        .process_directory(std::path::Path::new(directory))
        .map_err(|e| PyRuntimeError::new_err(format!("Batch processing failed: {}", e)))?;

    Ok(PyBatchResult::from(&result))
}

/// Python wrapper for MlReadinessScore
#[pyclass]
#[derive(Clone)]
pub struct PyMlReadinessScore {
    #[pyo3(get)]
    pub overall_score: f64,
    #[pyo3(get)]
    pub completeness_score: f64,
    #[pyo3(get)]
    pub consistency_score: f64,
    #[pyo3(get)]
    pub type_suitability_score: f64,
    #[pyo3(get)]
    pub feature_quality_score: f64,
    #[pyo3(get)]
    pub readiness_level: String,
    #[pyo3(get)]
    pub recommendations: Vec<PyMlRecommendation>,
    #[pyo3(get)]
    pub blocking_issues: Vec<PyMlBlockingIssue>,
    #[pyo3(get)]
    pub feature_analysis: Vec<PyFeatureAnalysis>,
    #[pyo3(get)]
    pub preprocessing_suggestions: Vec<PyPreprocessingSuggestion>,
}

impl From<&MlReadinessScore> for PyMlReadinessScore {
    fn from(score: &MlReadinessScore) -> Self {
        Self {
            overall_score: score.overall_score,
            completeness_score: score.completeness_score,
            consistency_score: score.consistency_score,
            type_suitability_score: score.type_suitability_score,
            feature_quality_score: score.feature_quality_score,
            readiness_level: match score.readiness_level {
                MlReadinessLevel::Ready => "ready".to_string(),
                MlReadinessLevel::Good => "good".to_string(),
                MlReadinessLevel::NeedsWork => "needs_work".to_string(),
                MlReadinessLevel::NotReady => "not_ready".to_string(),
            },
            recommendations: score
                .recommendations
                .iter()
                .map(PyMlRecommendation::from)
                .collect(),
            blocking_issues: score
                .blocking_issues
                .iter()
                .map(PyMlBlockingIssue::from)
                .collect(),
            feature_analysis: score
                .feature_analysis
                .iter()
                .map(PyFeatureAnalysis::from)
                .collect(),
            preprocessing_suggestions: score
                .preprocessing_suggestions
                .iter()
                .map(PyPreprocessingSuggestion::from)
                .collect(),
        }
    }
}

#[pymethods]
impl PyMlReadinessScore {
    /// Get recommendations by priority
    fn recommendations_by_priority(&self, priority: &str) -> Vec<PyMlRecommendation> {
        self.recommendations
            .iter()
            .filter(|rec| rec.priority == priority)
            .cloned()
            .collect()
    }

    /// Get features by ML suitability threshold
    fn features_by_suitability(&self, min_score: f64) -> Vec<PyFeatureAnalysis> {
        self.feature_analysis
            .iter()
            .filter(|feature| feature.ml_suitability >= min_score)
            .cloned()
            .collect()
    }

    /// Get preprocessing steps by priority
    fn preprocessing_by_priority(&self, priority: &str) -> Vec<PyPreprocessingSuggestion> {
        self.preprocessing_suggestions
            .iter()
            .filter(|step| step.priority == priority)
            .cloned()
            .collect()
    }

    /// Check if data is ready for ML (no blocking issues)
    fn is_ml_ready(&self) -> bool {
        self.blocking_issues.is_empty() && self.overall_score >= 60.0
    }

    /// Get summary statistics
    fn summary(&self) -> String {
        format!(
            "ML Readiness: {} ({:.1}%) | Features: {} | Recommendations: {} | Blocking Issues: {}",
            self.readiness_level,
            self.overall_score,
            self.feature_analysis.len(),
            self.recommendations.len(),
            self.blocking_issues.len()
        )
    }

    /// Rich HTML representation for Jupyter notebooks
    fn _repr_html_(&self) -> String {
        let status_color = match self.readiness_level.as_str() {
            "ready" => "#4CAF50",
            "good" => "#8BC34A",
            "needs_work" => "#FF9800",
            "not_ready" => "#F44336",
            _ => "#9E9E9E",
        };

        let status_icon = match self.readiness_level.as_str() {
            "ready" => "✅",
            "good" => "👍",
            "needs_work" => "⚠️",
            "not_ready" => "❌",
            _ => "❓",
        };

        let mut html = format!(
            r#"
<div style="font-family: Arial, sans-serif; border: 1px solid #ddd; border-radius: 8px; padding: 16px; margin: 8px 0;">
    <h3 style="margin: 0 0 16px 0; color: #333;">
        {icon} ML Readiness Assessment
        <span style="background-color: {color}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.9em; margin-left: 8px;">
            {level}
        </span>
    </h3>

    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 16px;">
        <div style="text-align: center;">
            <div style="font-size: 2em; font-weight: bold; color: {color};">{score:.1}%</div>
            <div style="color: #666;">Overall Score</div>
        </div>
        <div style="text-align: center;">
            <div style="font-size: 1.5em; font-weight: bold; color: #333;">{features}</div>
            <div style="color: #666;">Features Analyzed</div>
        </div>
        <div style="text-align: center;">
            <div style="font-size: 1.5em; font-weight: bold; color: #333;">{recs}</div>
            <div style="color: #666;">Recommendations</div>
        </div>
        <div style="text-align: center;">
            <div style="font-size: 1.5em; font-weight: bold; color: {blocking_color};">{blocks}</div>
            <div style="color: #666;">Blocking Issues</div>
        </div>
    </div>

    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 8px; margin-bottom: 16px;">
        <div>
            <strong>Completeness:</strong>
            <div style="background-color: #e0e0e0; border-radius: 4px; height: 8px; margin: 4px 0;">
                <div style="background-color: #4CAF50; height: 100%; border-radius: 4px; width: {comp:.0}%;"></div>
            </div>
            <span style="font-size: 0.9em; color: #666;">{comp:.1}%</span>
        </div>
        <div>
            <strong>Consistency:</strong>
            <div style="background-color: #e0e0e0; border-radius: 4px; height: 8px; margin: 4px 0;">
                <div style="background-color: #2196F3; height: 100%; border-radius: 4px; width: {cons:.0}%;"></div>
            </div>
            <span style="font-size: 0.9em; color: #666;">{cons:.1}%</span>
        </div>
        <div>
            <strong>Type Suitability:</strong>
            <div style="background-color: #e0e0e0; border-radius: 4px; height: 8px; margin: 4px 0;">
                <div style="background-color: #FF9800; height: 100%; border-radius: 4px; width: {type_suit:.0}%;"></div>
            </div>
            <span style="font-size: 0.9em; color: #666;">{type_suit:.1}%</span>
        </div>
        <div>
            <strong>Feature Quality:</strong>
            <div style="background-color: #e0e0e0; border-radius: 4px; height: 8px; margin: 4px 0;">
                <div style="background-color: #9C27B0; height: 100%; border-radius: 4px; width: {feat_qual:.0}%;"></div>
            </div>
            <span style="font-size: 0.9em; color: #666;">{feat_qual:.1}%</span>
        </div>
    </div>
"#,
            icon = status_icon,
            color = status_color,
            level = self.readiness_level,
            score = self.overall_score,
            features = self.feature_analysis.len(),
            recs = self.recommendations.len(),
            blocks = self.blocking_issues.len(),
            blocking_color = if self.blocking_issues.is_empty() {
                "#4CAF50"
            } else {
                "#F44336"
            },
            comp = self.completeness_score,
            cons = self.consistency_score,
            type_suit = self.type_suitability_score,
            feat_qual = self.feature_quality_score,
        );

        // Add blocking issues if any
        if !self.blocking_issues.is_empty() {
            html.push_str(r#"
    <div style="background-color: #ffebee; border-left: 4px solid #f44336; padding: 12px; margin: 8px 0;">
        <h4 style="margin: 0 0 8px 0; color: #c62828;">🚫 Blocking Issues</h4>
        <ul style="margin: 0; padding-left: 20px;">"#);

            for issue in &self.blocking_issues {
                html.push_str(&format!(
                    r#"<li style="margin: 4px 0; color: #666;"><strong>{}:</strong> {}</li>"#,
                    issue.issue_type, issue.description
                ));
            }
            html.push_str("</ul></div>");
        }

        // Add top recommendations
        if !self.recommendations.is_empty() {
            html.push_str(r#"
    <div style="background-color: #f3e5f5; border-left: 4px solid #9c27b0; padding: 12px; margin: 8px 0;">
        <h4 style="margin: 0 0 8px 0; color: #7b1fa2;">💡 Top Recommendations</h4>
        <ul style="margin: 0; padding-left: 20px;">"#);

            for rec in self.recommendations.iter().take(3) {
                let priority_color = match rec.priority.as_str() {
                    "critical" => "#f44336",
                    "high" => "#ff9800",
                    "medium" => "#2196f3",
                    "low" => "#4caf50",
                    _ => "#666",
                };
                html.push_str(&format!(
                    r#"<li style="margin: 4px 0; color: #666;">
                        <span style="background-color: {}; color: white; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; margin-right: 8px;">{}</span>
                        <strong>{}:</strong> {}
                    </li>"#,
                    priority_color, rec.priority.to_uppercase(), rec.category, rec.description
                ));
            }
            html.push_str("</ul></div>");
        }

        // Add feature summary
        let ready_features: Vec<_> = self
            .feature_analysis
            .iter()
            .filter(|f| f.ml_suitability > 0.7)
            .collect();

        html.push_str(&format!(r#"
    <div style="background-color: #e8f5e8; border-left: 4px solid #4caf50; padding: 12px; margin: 8px 0;">
        <h4 style="margin: 0 0 8px 0; color: #2e7d32;">🎯 Feature Summary</h4>
        <p style="margin: 0; color: #666;">
            <strong>{}/{}</strong> features are ready for ML (>70% suitability)
        </p>
    </div>
</div>"#, ready_features.len(), self.feature_analysis.len()));

        html
    }
}

/// Python wrapper for MlRecommendation
#[pyclass]
#[derive(Clone)]
pub struct PyMlRecommendation {
    #[pyo3(get)]
    pub category: String,
    #[pyo3(get)]
    pub priority: String,
    #[pyo3(get)]
    pub description: String,
    #[pyo3(get)]
    pub expected_impact: String,
    #[pyo3(get)]
    pub implementation_effort: String,
}

impl From<&MlRecommendation> for PyMlRecommendation {
    fn from(rec: &MlRecommendation) -> Self {
        Self {
            category: rec.category.clone(),
            priority: match rec.priority {
                RecommendationPriority::Critical => "critical".to_string(),
                RecommendationPriority::High => "high".to_string(),
                RecommendationPriority::Medium => "medium".to_string(),
                RecommendationPriority::Low => "low".to_string(),
            },
            description: rec.description.clone(),
            expected_impact: rec.expected_impact.clone(),
            implementation_effort: match rec.implementation_effort {
                ImplementationEffort::Trivial => "trivial".to_string(),
                ImplementationEffort::Easy => "easy".to_string(),
                ImplementationEffort::Moderate => "moderate".to_string(),
                ImplementationEffort::Significant => "significant".to_string(),
                ImplementationEffort::Complex => "complex".to_string(),
            },
        }
    }
}

/// Python wrapper for MlBlockingIssue
#[pyclass]
#[derive(Clone)]
pub struct PyMlBlockingIssue {
    #[pyo3(get)]
    pub issue_type: String,
    #[pyo3(get)]
    pub column: Option<String>,
    #[pyo3(get)]
    pub description: String,
    #[pyo3(get)]
    pub resolution_required: String,
}

impl From<&MlBlockingIssue> for PyMlBlockingIssue {
    fn from(issue: &MlBlockingIssue) -> Self {
        Self {
            issue_type: issue.issue_type.clone(),
            column: issue.column.clone(),
            description: issue.description.clone(),
            resolution_required: issue.resolution_required.clone(),
        }
    }
}

/// Python wrapper for FeatureAnalysis
#[pyclass]
#[derive(Clone)]
pub struct PyFeatureAnalysis {
    #[pyo3(get)]
    pub column_name: String,
    #[pyo3(get)]
    pub ml_suitability: f64,
    #[pyo3(get)]
    pub feature_type: String,
    #[pyo3(get)]
    pub encoding_suggestions: Vec<String>,
    #[pyo3(get)]
    pub potential_issues: Vec<String>,
    #[pyo3(get)]
    pub feature_importance_potential: String,
}

impl From<&FeatureAnalysis> for PyFeatureAnalysis {
    fn from(analysis: &FeatureAnalysis) -> Self {
        Self {
            column_name: analysis.column_name.clone(),
            ml_suitability: analysis.ml_suitability,
            feature_type: match analysis.feature_type {
                MlFeatureType::NumericReady => "numeric_ready".to_string(),
                MlFeatureType::NumericNeedsScaling => "numeric_needs_scaling".to_string(),
                MlFeatureType::CategoricalNeedsEncoding => "categorical_needs_encoding".to_string(),
                MlFeatureType::TextNeedsProcessing => "text_needs_processing".to_string(),
                MlFeatureType::TemporalNeedsEngineering => "temporal_needs_engineering".to_string(),
                MlFeatureType::HighCardinalityRisky => "high_cardinality_risky".to_string(),
                MlFeatureType::TooManyMissing => "too_many_missing".to_string(),
                MlFeatureType::LowVariance => "low_variance".to_string(),
            },
            encoding_suggestions: analysis.encoding_suggestions.clone(),
            potential_issues: analysis.potential_issues.clone(),
            feature_importance_potential: match analysis.feature_importance_potential {
                FeatureImportancePotential::High => "high".to_string(),
                FeatureImportancePotential::Medium => "medium".to_string(),
                FeatureImportancePotential::Low => "low".to_string(),
                FeatureImportancePotential::Unknown => "unknown".to_string(),
            },
        }
    }
}

/// Python wrapper for PreprocessingSuggestion
#[pyclass]
#[derive(Clone)]
pub struct PyPreprocessingSuggestion {
    #[pyo3(get)]
    pub step: String,
    #[pyo3(get)]
    pub description: String,
    #[pyo3(get)]
    pub columns_affected: Vec<String>,
    #[pyo3(get)]
    pub priority: String,
    #[pyo3(get)]
    pub tools_frameworks: Vec<String>,
}

impl From<&PreprocessingSuggestion> for PyPreprocessingSuggestion {
    fn from(suggestion: &PreprocessingSuggestion) -> Self {
        Self {
            step: suggestion.step.clone(),
            description: suggestion.description.clone(),
            columns_affected: suggestion.columns_affected.clone(),
            priority: match suggestion.priority {
                RecommendationPriority::Critical => "critical".to_string(),
                RecommendationPriority::High => "high".to_string(),
                RecommendationPriority::Medium => "medium".to_string(),
                RecommendationPriority::Low => "low".to_string(),
            },
            tools_frameworks: suggestion.tools_frameworks.clone(),
        }
    }
}

/// Calculate ML readiness score for a CSV file
#[pyfunction]
fn ml_readiness_score(path: &str) -> PyResult<PyMlReadinessScore> {
    // First get the quality report
    let quality_report = analyze_csv_robust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    // Calculate ML readiness using the engine
    let ml_engine = MlReadinessEngine::new();
    let ml_score = ml_engine
        .calculate_ml_score(&quality_report)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to calculate ML score: {}", e)))?;

    Ok(PyMlReadinessScore::from(&ml_score))
}

/// Comprehensive analysis combining data quality and ML readiness
#[pyfunction]
fn analyze_csv_for_ml(path: &str) -> PyResult<(PyQualityReport, PyMlReadinessScore)> {
    // Get quality report
    let quality_report = analyze_csv_robust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    // Calculate ML readiness
    let ml_engine = MlReadinessEngine::new();
    let ml_score = ml_engine
        .calculate_ml_score(&quality_report)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to calculate ML score: {}", e)))?;

    Ok((
        PyQualityReport::from(&quality_report),
        PyMlReadinessScore::from(&ml_score),
    ))
}

/// Analyze CSV and return column profiles as pandas DataFrame (if pandas available)
#[pyfunction]
fn analyze_csv_dataframe(py: Python, path: &str) -> PyResult<PyObject> {
    // Get column profiles
    let profiles = analyze_csv(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    // Try to import pandas
    let pandas = match py.import_bound("pandas") {
        Ok(pd) => pd,
        Err(_) => {
            return Err(PyRuntimeError::new_err(
                "pandas not available. Install with: pip install pandas",
            ));
        }
    };

    // Create DataFrame data
    let mut data: std::collections::HashMap<&str, Vec<PyObject>> = std::collections::HashMap::new();
    data.insert("column_name", Vec::new());
    data.insert("data_type", Vec::new());
    data.insert("total_count", Vec::new());
    data.insert("null_count", Vec::new());
    data.insert("null_percentage", Vec::new());
    data.insert("unique_count", Vec::new());
    data.insert("uniqueness_ratio", Vec::new());

    for profile in &profiles {
        let py_profile = PyColumnProfile::from(profile);
        data.get_mut("column_name")
            .unwrap()
            .push(py_profile.name.into_py(py));
        data.get_mut("data_type")
            .unwrap()
            .push(py_profile.data_type.into_py(py));
        data.get_mut("total_count")
            .unwrap()
            .push(py_profile.total_count.into_py(py));
        data.get_mut("null_count")
            .unwrap()
            .push(py_profile.null_count.into_py(py));
        data.get_mut("null_percentage")
            .unwrap()
            .push(py_profile.null_percentage.into_py(py));
        data.get_mut("unique_count")
            .unwrap()
            .push(py_profile.unique_count.into_py(py));
        data.get_mut("uniqueness_ratio")
            .unwrap()
            .push(py_profile.uniqueness_ratio.into_py(py));
    }

    // Create DataFrame
    let df = pandas.call_method1("DataFrame", (data,))?;
    Ok(df.into())
}

/// Get ML feature analysis as pandas DataFrame (if pandas available)
#[pyfunction]
fn feature_analysis_dataframe(py: Python, path: &str) -> PyResult<PyObject> {
    // Get ML readiness score
    let quality_report = analyze_csv_robust(Path::new(path))
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to analyze CSV: {}", e)))?;

    let ml_engine = MlReadinessEngine::new();
    let ml_score = ml_engine
        .calculate_ml_score(&quality_report)
        .map_err(|e| PyRuntimeError::new_err(format!("Failed to calculate ML score: {}", e)))?;

    // Try to import pandas
    let pandas = match py.import_bound("pandas") {
        Ok(pd) => pd,
        Err(_) => {
            return Err(PyRuntimeError::new_err(
                "pandas not available. Install with: pip install pandas",
            ));
        }
    };

    // Create DataFrame data
    let mut data: std::collections::HashMap<&str, Vec<PyObject>> = std::collections::HashMap::new();
    data.insert("column_name", Vec::new());
    data.insert("ml_suitability", Vec::new());
    data.insert("feature_type", Vec::new());
    data.insert("importance_potential", Vec::new());
    data.insert("encoding_suggestions", Vec::new());
    data.insert("potential_issues", Vec::new());

    for feature in &ml_score.feature_analysis {
        let py_feature = PyFeatureAnalysis::from(feature);
        data.get_mut("column_name")
            .unwrap()
            .push(py_feature.column_name.into_py(py));
        data.get_mut("ml_suitability")
            .unwrap()
            .push(py_feature.ml_suitability.into_py(py));
        data.get_mut("feature_type")
            .unwrap()
            .push(py_feature.feature_type.into_py(py));
        data.get_mut("importance_potential")
            .unwrap()
            .push(py_feature.feature_importance_potential.into_py(py));
        data.get_mut("encoding_suggestions")
            .unwrap()
            .push(py_feature.encoding_suggestions.join(", ").into_py(py));
        data.get_mut("potential_issues")
            .unwrap()
            .push(py_feature.potential_issues.join(", ").into_py(py));
    }

    // Create DataFrame
    let df = pandas.call_method1("DataFrame", (data,))?;
    Ok(df.into())
}

// Note: Async support temporarily disabled due to pyo3-asyncio compatibility issues
// Will be re-enabled once pyo3-asyncio supports pyo3 0.24

// ============================================================================
// Context Managers for Resource Management
// ============================================================================

/// Context manager for batch analysis with automatic cleanup
#[pyclass]
pub struct PyBatchAnalyzer {
    temp_files: Vec<String>,
    results: Vec<PyObject>,
}

#[pymethods]
impl PyBatchAnalyzer {
    #[new]
    fn new() -> Self {
        PyBatchAnalyzer {
            temp_files: Vec::new(),
            results: Vec::new(),
        }
    }

    /// Enter context manager
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Exit context manager with cleanup
    fn __exit__(
        &mut self,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        // Clean up temporary files
        for temp_file in &self.temp_files {
            let _ = std::fs::remove_file(temp_file);
        }
        self.temp_files.clear();
        self.results.clear();
        Ok(false) // Don't suppress exceptions
    }

    /// Add a file to analysis queue
    fn add_file(&mut self, py: Python, path: &str) -> PyResult<()> {
        let result = analyze_csv_file(path)?;
        self.results.push(result.into_py(py));
        Ok(())
    }

    /// Add a temporary file that needs cleanup
    fn add_temp_file(&mut self, path: String) {
        self.temp_files.push(path);
    }

    /// Get all analysis results
    fn get_results(&self, py: Python) -> PyResult<PyObject> {
        let results_ref: Vec<&PyObject> = self.results.iter().collect();
        Ok(results_ref.into_py(py))
    }

    /// Analyze multiple files in batch
    fn analyze_batch(&mut self, py: Python, paths: Vec<String>) -> PyResult<PyObject> {
        let mut batch_results = Vec::new();

        for path in paths {
            match analyze_csv_file(&path) {
                Ok(result) => {
                    batch_results.push(result.into_py(py));
                }
                Err(e) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Failed to analyze {}: {}",
                        path, e
                    )));
                }
            }
        }

        for result in &batch_results {
            self.results.push(result.clone_ref(py));
        }
        Ok(batch_results.into_py(py))
    }
}

/// Context manager for ML analysis with resource tracking
#[pyclass]
pub struct PyMlAnalyzer {
    temp_files: Vec<String>,
    ml_results: Vec<PyMlReadinessScore>,
    processing_stats: std::collections::HashMap<String, f64>,
}

#[pymethods]
impl PyMlAnalyzer {
    #[new]
    fn new() -> Self {
        PyMlAnalyzer {
            temp_files: Vec::new(),
            ml_results: Vec::new(),
            processing_stats: std::collections::HashMap::new(),
        }
    }

    /// Enter context manager
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Exit context manager with cleanup
    fn __exit__(
        &mut self,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        // Clean up temporary files
        for temp_file in &self.temp_files {
            let _ = std::fs::remove_file(temp_file);
        }
        self.temp_files.clear();
        self.ml_results.clear();
        self.processing_stats.clear();
        Ok(false)
    }

    /// Analyze ML readiness with timing
    fn analyze_ml(&mut self, path: &str) -> PyResult<PyMlReadinessScore> {
        let start_time = std::time::Instant::now();

        let result = ml_readiness_score(path)?;

        let duration = start_time.elapsed().as_secs_f64();
        self.processing_stats.insert(path.to_string(), duration);
        self.ml_results.push(result.clone());

        Ok(result)
    }

    /// Add temporary file for cleanup
    fn add_temp_file(&mut self, path: String) {
        self.temp_files.push(path);
    }

    /// Get processing statistics
    fn get_stats(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.processing_stats.clone().into_py(py))
    }

    /// Get all ML results
    fn get_ml_results(&self, py: Python) -> PyResult<PyObject> {
        Ok(self.ml_results.clone().into_py(py))
    }

    /// Get summary statistics
    fn get_summary(&self, py: Python) -> PyResult<PyObject> {
        if self.ml_results.is_empty() {
            return Ok(py.None());
        }

        let total_files = self.ml_results.len();
        let avg_score =
            self.ml_results.iter().map(|r| r.overall_score).sum::<f64>() / total_files as f64;

        let ready_files = self
            .ml_results
            .iter()
            .filter(|r| r.readiness_level == "ready")
            .count();

        let total_time: f64 = self.processing_stats.values().sum();
        let avg_time = if total_files > 0 {
            total_time / total_files as f64
        } else {
            0.0
        };

        let mut summary = std::collections::HashMap::new();
        summary.insert("total_files", total_files.into_py(py));
        summary.insert("average_score", avg_score.into_py(py));
        summary.insert("ready_files", ready_files.into_py(py));
        summary.insert(
            "ready_percentage",
            ((ready_files as f64 / total_files as f64) * 100.0).into_py(py),
        );
        summary.insert("total_processing_time", total_time.into_py(py));
        summary.insert("average_processing_time", avg_time.into_py(py));

        Ok(summary.into_py(py))
    }
}

/// Context manager for CSV file processing with automatic handling
#[pyclass]
pub struct PyCsvProcessor {
    file_handle: Option<String>,
    temp_files: Vec<String>,
    chunk_size: usize,
    processed_rows: usize,
}

#[pymethods]
impl PyCsvProcessor {
    #[new]
    fn new(chunk_size: Option<usize>) -> Self {
        PyCsvProcessor {
            file_handle: None,
            temp_files: Vec::new(),
            chunk_size: chunk_size.unwrap_or(1000),
            processed_rows: 0,
        }
    }

    /// Enter context manager
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    /// Exit context manager with cleanup
    fn __exit__(
        &mut self,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<bool> {
        // Clean up all temporary files
        for temp_file in &self.temp_files {
            let _ = std::fs::remove_file(temp_file);
        }
        self.temp_files.clear();
        self.file_handle = None;
        self.processed_rows = 0;
        Ok(false)
    }

    /// Open a CSV file for processing
    fn open_file(&mut self, path: &str) -> PyResult<()> {
        // Validate file exists and is readable
        if !std::path::Path::new(path).exists() {
            return Err(PyRuntimeError::new_err(format!("File not found: {}", path)));
        }

        self.file_handle = Some(path.to_string());
        self.processed_rows = 0;
        Ok(())
    }

    /// Process the file in chunks
    fn process_chunks(&mut self, py: Python) -> PyResult<PyObject> {
        let file_path = self
            .file_handle
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("No file opened. Call open_file() first."))?;

        // Read and process file in chunks
        let content = std::fs::read_to_string(file_path)
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to read file: {}", e)))?;

        let lines: Vec<&str> = content.lines().collect();
        if lines.is_empty() {
            return Err(PyRuntimeError::new_err("Empty file"));
        }

        let header = lines[0];
        let data_lines = &lines[1..];

        let mut chunk_results = Vec::new();
        let mut chunk_number = 0;

        for chunk in data_lines.chunks(self.chunk_size) {
            chunk_number += 1;

            // Create temporary chunk file
            let chunk_content = format!("{}\n{}", header, chunk.join("\n"));
            let temp_path = format!("{}.chunk_{}.csv", file_path, chunk_number);

            std::fs::write(&temp_path, chunk_content)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to write chunk: {}", e)))?;

            self.temp_files.push(temp_path.clone());

            // Analyze the chunk
            let chunk_result = analyze_csv_file(&temp_path)?;
            chunk_results.push(chunk_result.into_py(py));

            self.processed_rows += chunk.len();
        }

        Ok(chunk_results.into_py(py))
    }

    /// Get processing statistics
    fn get_processing_info(&self, py: Python) -> PyResult<PyObject> {
        let mut info = std::collections::HashMap::new();
        info.insert(
            "file_path",
            self.file_handle
                .as_ref()
                .unwrap_or(&"None".to_string())
                .clone()
                .into_py(py),
        );
        info.insert("chunk_size", self.chunk_size.into_py(py));
        info.insert("processed_rows", self.processed_rows.into_py(py));
        info.insert("temp_files_created", self.temp_files.len().into_py(py));

        Ok(info.into_py(py))
    }
}

// ============================
// Python Logging Integration
// ============================

/// Configure Python logging for DataProf
#[pyfunction]
#[pyo3(signature = (level = None, format = None))]
fn configure_logging(py: Python, level: Option<String>, format: Option<String>) -> PyResult<()> {
    let logging = py.import_bound("logging")?;

    // Set default level to INFO if not specified
    let log_level = match level.as_deref() {
        Some("DEBUG") => logging.getattr("DEBUG")?,
        Some("INFO") => logging.getattr("INFO")?,
        Some("WARNING") => logging.getattr("WARNING")?,
        Some("ERROR") => logging.getattr("ERROR")?,
        Some("CRITICAL") => logging.getattr("CRITICAL")?,
        _ => logging.getattr("INFO")?, // Default level
    };

    // Set default format if not specified
    let log_format = format
        .unwrap_or_else(|| "%(asctime)s - %(name)s - %(levelname)s - %(message)s".to_string());

    // Configure basic logging
    let kwargs = pyo3::types::PyDict::new(py);
    kwargs.set_item("level", log_level)?;
    kwargs.set_item("format", log_format)?;
    logging.call_method("basicConfig", (), Some(&kwargs))?;

    Ok(())
}

/// Get a logger instance for DataProf
#[pyfunction]
#[pyo3(signature = (name = None))]
fn get_logger(py: Python, name: Option<String>) -> PyResult<PyObject> {
    let logging = py.import_bound("logging")?;
    let logger_name = name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    Ok(logger.into())
}

/// Log a message at INFO level
#[pyfunction]
#[pyo3(signature = (message, logger_name = None))]
fn log_info(py: Python, message: String, logger_name: Option<String>) -> PyResult<()> {
    let logging = py.import_bound("logging")?;
    let logger_name = logger_name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    logger.call_method1("info", (message,))?;
    Ok(())
}

/// Log a message at DEBUG level
#[pyfunction]
#[pyo3(signature = (message, logger_name = None))]
fn log_debug(py: Python, message: String, logger_name: Option<String>) -> PyResult<()> {
    let logging = py.import_bound("logging")?;
    let logger_name = logger_name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    logger.call_method1("debug", (message,))?;
    Ok(())
}

/// Log a message at WARNING level
#[pyfunction]
#[pyo3(signature = (message, logger_name = None))]
fn log_warning(py: Python, message: String, logger_name: Option<String>) -> PyResult<()> {
    let logging = py.import_bound("logging")?;
    let logger_name = logger_name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    logger.call_method1("warning", (message,))?;
    Ok(())
}

/// Log a message at ERROR level
#[pyfunction]
#[pyo3(signature = (message, logger_name = None))]
fn log_error(py: Python, message: String, logger_name: Option<String>) -> PyResult<()> {
    let logging = py.import_bound("logging")?;
    let logger_name = logger_name.unwrap_or_else(|| "dataprof".to_string());
    let logger = logging.call_method1("getLogger", (logger_name,))?;
    logger.call_method1("error", (message,))?;
    Ok(())
}

/// Enhanced CSV analysis with logging
#[pyfunction]
#[pyo3(signature = (file_path, log_level = None))]
fn analyze_csv_with_logging(
    py: Python,
    file_path: String,
    log_level: Option<String>,
) -> PyResult<Vec<PyColumnProfile>> {
    // Configure logging if level is provided
    if let Some(level) = log_level {
        configure_logging(py, Some(level), None)?;
    }

    log_info(
        py,
        format!("Starting CSV analysis for: {}", file_path),
        None,
    )?;

    // Perform the actual analysis
    let start_time = std::time::Instant::now();
    let result = analyze_csv_file(&file_path);
    let duration = start_time.elapsed();

    match result {
        Ok(profiles) => {
            log_info(
                py,
                format!(
                    "CSV analysis completed in {:.3}s. Analyzed {} columns",
                    duration.as_secs_f64(),
                    profiles.len()
                ),
                None,
            )?;
            Ok(profiles)
        }
        Err(e) => {
            log_error(
                py,
                format!("CSV analysis failed for {}: {}", file_path, e),
                None,
            )?;
            Err(e)
        }
    }
}

/// Enhanced ML readiness analysis with logging
#[pyfunction]
#[pyo3(signature = (file_path, log_level = None))]
fn ml_readiness_score_with_logging(
    py: Python,
    file_path: String,
    log_level: Option<String>,
) -> PyResult<PyMlReadinessScore> {
    // Configure logging if level is provided
    if let Some(level) = log_level {
        configure_logging(py, Some(level), None)?;
    }

    log_info(
        py,
        format!("Starting ML readiness analysis for: {}", file_path),
        None,
    )?;

    // Perform the actual analysis
    let start_time = std::time::Instant::now();
    let result = ml_readiness_score(&file_path);
    let duration = start_time.elapsed();

    match result {
        Ok(score) => {
            log_info(
                py,
                format!(
                    "ML readiness analysis completed in {:.3}s. Score: {:.1}% ({})",
                    duration.as_secs_f64(),
                    score.overall_score,
                    score.readiness_level
                ),
                None,
            )?;
            Ok(score)
        }
        Err(e) => {
            log_error(
                py,
                format!("ML readiness analysis failed for {}: {}", file_path, e),
                None,
            )?;
            Err(e)
        }
    }
}

/// Python module definition
#[pymodule]
fn dataprof(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add version information from Cargo.toml
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    // Core data profiling classes
    m.add_class::<PyColumnProfile>()?;
    m.add_class::<PyQualityReport>()?;
    m.add_class::<PyQualityIssue>()?;
    m.add_class::<PyBatchResult>()?;

    // ML readiness classes
    m.add_class::<PyMlReadinessScore>()?;
    m.add_class::<PyMlRecommendation>()?;
    m.add_class::<PyMlBlockingIssue>()?;
    m.add_class::<PyFeatureAnalysis>()?;
    m.add_class::<PyPreprocessingSuggestion>()?;

    // Context manager classes
    m.add_class::<PyBatchAnalyzer>()?;
    m.add_class::<PyMlAnalyzer>()?;
    m.add_class::<PyCsvProcessor>()?;

    // Single file analysis
    m.add_function(wrap_pyfunction!(analyze_csv_file, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_csv_with_quality, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_json_file, m)?)?;

    // ML readiness analysis
    m.add_function(wrap_pyfunction!(ml_readiness_score, m)?)?;
    m.add_function(wrap_pyfunction!(analyze_csv_for_ml, m)?)?;

    // Pandas integration (optional)
    m.add_function(wrap_pyfunction!(analyze_csv_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(feature_analysis_dataframe, m)?)?;

    // Batch processing
    m.add_function(wrap_pyfunction!(batch_analyze_glob, m)?)?;
    m.add_function(wrap_pyfunction!(batch_analyze_directory, m)?)?;

    // Python logging integration
    m.add_function(wrap_pyfunction!(configure_logging, m)?)?;
    m.add_function(wrap_pyfunction!(get_logger, m)?)?;
    m.add_function(wrap_pyfunction!(log_info, m)?)?;
    m.add_function(wrap_pyfunction!(log_debug, m)?)?;
    m.add_function(wrap_pyfunction!(log_warning, m)?)?;
    m.add_function(wrap_pyfunction!(log_error, m)?)?;

    // Enhanced analysis functions with logging
    m.add_function(wrap_pyfunction!(analyze_csv_with_logging, m)?)?;
    m.add_function(wrap_pyfunction!(ml_readiness_score_with_logging, m)?)?;

    // Note: Async functions temporarily disabled due to compatibility issues

    Ok(())
}
