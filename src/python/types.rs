#![allow(clippy::useless_conversion)]

use pyo3::prelude::*;

use crate::analysis::ml_readiness::{
    FeatureImportancePotential, ImplementationEffort, MlBlockingIssue, MlFeatureType,
    MlReadinessLevel, MlRecommendation, RecommendationPriority,
};
use crate::analysis::{FeatureAnalysis, MlReadinessScore, PreprocessingSuggestion};
use crate::core::batch::BatchResult;
use crate::types::{ColumnProfile, DataType, QualityIssue, QualityReport};

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
    /// Ready-to-use code snippet for implementing the recommendation
    #[pyo3(get)]
    pub code_snippet: Option<String>,
    /// Framework used in the code snippet (pandas, scikit-learn, etc.)
    #[pyo3(get)]
    pub framework: Option<String>,
    /// Required imports for the code snippet
    #[pyo3(get)]
    pub imports: Vec<String>,
    /// Variables used in code snippet for customization
    #[pyo3(get)]
    pub variables: std::collections::HashMap<String, String>,
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
            code_snippet: rec.code_snippet.clone(),
            framework: rec.framework.clone(),
            imports: rec.imports.clone(),
            variables: rec.variables.clone(),
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
