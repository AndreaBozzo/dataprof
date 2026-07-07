use std::collections::HashMap;

use dataprof_core::{ColumnProfile, QualityDimension};
use serde::{Deserialize, Serialize};

use crate::core::errors::DataProfilerError;

/// Completeness metrics (ISO 8000-8).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompletenessMetrics {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub missing_values_ratio: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub complete_records_ratio: f64,
    pub null_columns: Vec<String>,
}

/// Consistency metrics (ISO 8000-61).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConsistencyMetrics {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub data_type_consistency: f64,
    pub format_violations: usize,
    pub encoding_issues: usize,
}

/// Uniqueness metrics (ISO 8000-110).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UniquenessMetrics {
    pub duplicate_rows: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub key_uniqueness: f64,
    pub high_cardinality_warning: bool,
}

/// Accuracy metrics (ISO 25012).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccuracyMetrics {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub outlier_ratio: f64,
    pub range_violations: usize,
    pub negative_values_in_positive: usize,
}

/// Timeliness metrics (ISO 8000-8).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimelinessMetrics {
    pub future_dates_count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub stale_data_ratio: f64,
    pub temporal_violations: usize,
}

/// Comprehensive data quality metrics following industry standards.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QualityMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completeness: Option<CompletenessMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistency: Option<ConsistencyMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniqueness: Option<UniquenessMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accuracy: Option<AccuracyMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeliness: Option<TimelinessMetrics>,
    /// True when the sample used to compute these metrics was below the
    /// minimum recommended size (10 rows). When set, the quality scores and
    /// per-dimension ratios should be treated as directional rather than
    /// reliable. Backwards-compatible: defaults to `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub low_sample_warning: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl QualityMetrics {
    pub fn empty() -> Self {
        Self {
            completeness: Some(CompletenessMetrics {
                missing_values_ratio: 0.0,
                complete_records_ratio: 100.0,
                null_columns: vec![],
            }),
            consistency: Some(ConsistencyMetrics {
                data_type_consistency: 100.0,
                format_violations: 0,
                encoding_issues: 0,
            }),
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: 0,
                key_uniqueness: 100.0,
                high_cardinality_warning: false,
            }),
            accuracy: Some(AccuracyMetrics {
                outlier_ratio: 0.0,
                range_violations: 0,
                negative_values_in_positive: 0,
            }),
            timeliness: Some(TimelinessMetrics {
                future_dates_count: 0,
                stale_data_ratio: 0.0,
                temporal_violations: 0,
            }),
            low_sample_warning: false,
        }
    }

    pub fn calculate_from_data(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<Self, DataProfilerError> {
        let calculator = crate::analysis::MetricsCalculator::new();
        calculator.calculate_comprehensive_metrics(data, column_profiles, None)
    }

    pub fn overall_score(&self) -> f64 {
        let mut total_weight = 0.0;
        let mut score = 0.0;

        if let Some(c) = &self.completeness {
            total_weight += 0.3;
            score += c.complete_records_ratio * 0.3;
        }
        if let Some(c) = &self.consistency {
            total_weight += 0.25;
            score += c.data_type_consistency * 0.25;
        }
        if let Some(u) = &self.uniqueness {
            total_weight += 0.2;
            score += u.key_uniqueness * 0.2;
        }
        if let Some(a) = &self.accuracy {
            total_weight += 0.15;
            score += (100.0 - a.outlier_ratio) * 0.15;
        }
        if let Some(t) = &self.timeliness {
            total_weight += 0.1;
            score += (100.0 - t.stale_data_ratio) * 0.1;
        }

        if total_weight > 0.0 {
            (score / total_weight).min(100.0)
        } else {
            0.0
        }
    }

    pub fn missing_values_ratio(&self) -> f64 {
        self.completeness
            .as_ref()
            .map_or(0.0, |c| c.missing_values_ratio)
    }

    pub fn complete_records_ratio(&self) -> f64 {
        self.completeness
            .as_ref()
            .map_or(100.0, |c| c.complete_records_ratio)
    }

    pub fn null_columns(&self) -> &[String] {
        self.completeness.as_ref().map_or(&[], |c| &c.null_columns)
    }

    pub fn data_type_consistency(&self) -> f64 {
        self.consistency
            .as_ref()
            .map_or(100.0, |c| c.data_type_consistency)
    }

    pub fn format_violations(&self) -> usize {
        self.consistency.as_ref().map_or(0, |c| c.format_violations)
    }

    pub fn encoding_issues(&self) -> usize {
        self.consistency.as_ref().map_or(0, |c| c.encoding_issues)
    }

    pub fn duplicate_rows(&self) -> usize {
        self.uniqueness.as_ref().map_or(0, |u| u.duplicate_rows)
    }

    pub fn key_uniqueness(&self) -> f64 {
        self.uniqueness.as_ref().map_or(100.0, |u| u.key_uniqueness)
    }

    pub fn high_cardinality_warning(&self) -> bool {
        self.uniqueness
            .as_ref()
            .is_some_and(|u| u.high_cardinality_warning)
    }

    pub fn outlier_ratio(&self) -> f64 {
        self.accuracy.as_ref().map_or(0.0, |a| a.outlier_ratio)
    }

    pub fn range_violations(&self) -> usize {
        self.accuracy.as_ref().map_or(0, |a| a.range_violations)
    }

    pub fn negative_values_in_positive(&self) -> usize {
        self.accuracy
            .as_ref()
            .map_or(0, |a| a.negative_values_in_positive)
    }

    pub fn future_dates_count(&self) -> usize {
        self.timeliness.as_ref().map_or(0, |t| t.future_dates_count)
    }

    pub fn stale_data_ratio(&self) -> f64 {
        self.timeliness.as_ref().map_or(0.0, |t| t.stale_data_ratio)
    }

    pub fn temporal_violations(&self) -> usize {
        self.timeliness
            .as_ref()
            .map_or(0, |t| t.temporal_violations)
    }

    pub fn supports_dimension(&self, dimension: QualityDimension) -> bool {
        match dimension {
            QualityDimension::Completeness => self.completeness.is_some(),
            QualityDimension::Consistency => self.consistency.is_some(),
            QualityDimension::Uniqueness => self.uniqueness.is_some(),
            QualityDimension::Accuracy => self.accuracy.is_some(),
            QualityDimension::Timeliness => self.timeliness.is_some(),
        }
    }
}

/// Confidence level for quality metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricConfidence {
    Exact,
    Approximate {
        sample_size: usize,
        population_size: Option<usize>,
    },
    Mixed {
        exact_dimensions: Vec<String>,
        sampled_dimensions: Vec<String>,
        sample_size: usize,
    },
}

/// Wraps quality metrics with confidence information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityAssessment {
    pub metrics: QualityMetrics,
    pub confidence: MetricConfidence,
}

impl QualityAssessment {
    pub fn exact(metrics: QualityMetrics) -> Self {
        Self {
            metrics,
            confidence: MetricConfidence::Exact,
        }
    }

    pub fn approximate(
        metrics: QualityMetrics,
        sample_size: usize,
        population_size: Option<usize>,
    ) -> Self {
        Self {
            metrics,
            confidence: MetricConfidence::Approximate {
                sample_size,
                population_size,
            },
        }
    }

    pub fn score(&self) -> f64 {
        self.metrics.overall_score()
    }
}

impl From<QualityMetrics> for QualityAssessment {
    fn from(metrics: QualityMetrics) -> Self {
        Self::exact(metrics)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_metrics_perfect_score() {
        let metrics = QualityMetrics::empty();
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_quality_score_weights_sum_to_100() {
        let metrics = QualityMetrics::empty();
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_quality_score_completeness_weight() {
        let mut metrics = QualityMetrics::empty();
        if let Some(ref mut c) = metrics.completeness {
            c.complete_records_ratio = 0.0;
        }
        assert!((metrics.overall_score() - 70.0).abs() < 0.01);
    }

    #[test]
    fn test_quality_score_all_bad() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 0.0,
                ..CompletenessMetrics::default()
            }),
            consistency: Some(ConsistencyMetrics {
                data_type_consistency: 0.0,
                ..ConsistencyMetrics::default()
            }),
            uniqueness: Some(UniquenessMetrics {
                key_uniqueness: 0.0,
                ..UniquenessMetrics::default()
            }),
            accuracy: Some(AccuracyMetrics {
                outlier_ratio: 100.0,
                ..AccuracyMetrics::default()
            }),
            timeliness: Some(TimelinessMetrics {
                stale_data_ratio: 100.0,
                ..TimelinessMetrics::default()
            }),
            ..QualityMetrics::default()
        };

        assert!((metrics.overall_score() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_partial_dimensions_only_completeness() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 100.0,
                missing_values_ratio: 0.0,
                null_columns: vec![],
            }),
            ..QualityMetrics::default()
        };

        assert!(metrics.completeness.is_some());
        assert!(metrics.consistency.is_none());
        assert!(metrics.uniqueness.is_none());
        assert!(metrics.accuracy.is_none());
        assert!(metrics.timeliness.is_none());
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_partial_dimensions_two_dimensions() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 50.0,
                ..CompletenessMetrics::default()
            }),
            uniqueness: Some(UniquenessMetrics {
                key_uniqueness: 80.0,
                ..UniquenessMetrics::default()
            }),
            ..QualityMetrics::default()
        };

        assert!((metrics.overall_score() - 62.0).abs() < 0.01);
    }

    #[test]
    fn test_all_dimensions_none_score_zero() {
        let metrics = QualityMetrics::default();

        assert!((metrics.overall_score() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_partial_dimensions_json_skips_none() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics::default()),
            ..QualityMetrics::default()
        };

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("completeness"));
        assert!(!json.contains("consistency"));
        assert!(!json.contains("uniqueness"));
        assert!(!json.contains("accuracy"));
        assert!(!json.contains("timeliness"));
    }

    #[test]
    fn test_partial_dimensions_flat_accessors_return_defaults() {
        let metrics = QualityMetrics::default();

        assert!((metrics.complete_records_ratio() - 100.0).abs() < 0.01);
        assert!((metrics.data_type_consistency() - 100.0).abs() < 0.01);
        assert!((metrics.key_uniqueness() - 100.0).abs() < 0.01);
        assert!((metrics.missing_values_ratio() - 0.0).abs() < 0.01);
        assert_eq!(metrics.duplicate_rows(), 0);
        assert!(!metrics.high_cardinality_warning());
    }

    #[test]
    fn test_partial_dimension_flat_defaults_table() {
        struct Case {
            name: &'static str,
            metrics: QualityMetrics,
            has_completeness: bool,
            has_uniqueness: bool,
            has_accuracy: bool,
            missing_values_ratio: f64,
            key_uniqueness: f64,
            outlier_ratio: f64,
        }

        let cases = [
            Case {
                name: "only completeness",
                metrics: QualityMetrics {
                    completeness: Some(CompletenessMetrics {
                        missing_values_ratio: 12.5,
                        complete_records_ratio: 87.5,
                        null_columns: vec!["email".to_string()],
                    }),
                    ..QualityMetrics::default()
                },
                has_completeness: true,
                has_uniqueness: false,
                has_accuracy: false,
                missing_values_ratio: 12.5,
                key_uniqueness: 100.0,
                outlier_ratio: 0.0,
            },
            Case {
                name: "only uniqueness",
                metrics: QualityMetrics {
                    uniqueness: Some(UniquenessMetrics {
                        duplicate_rows: 2,
                        key_uniqueness: 92.0,
                        high_cardinality_warning: true,
                    }),
                    ..QualityMetrics::default()
                },
                has_completeness: false,
                has_uniqueness: true,
                has_accuracy: false,
                missing_values_ratio: 0.0,
                key_uniqueness: 92.0,
                outlier_ratio: 0.0,
            },
            Case {
                name: "only accuracy",
                metrics: QualityMetrics {
                    accuracy: Some(AccuracyMetrics {
                        outlier_ratio: 6.25,
                        range_violations: 1,
                        negative_values_in_positive: 1,
                    }),
                    ..QualityMetrics::default()
                },
                has_completeness: false,
                has_uniqueness: false,
                has_accuracy: true,
                missing_values_ratio: 0.0,
                key_uniqueness: 100.0,
                outlier_ratio: 6.25,
            },
        ];

        for case in cases {
            assert_eq!(
                case.metrics.completeness.is_some(),
                case.has_completeness,
                "{} completeness presence",
                case.name
            );
            assert_eq!(
                case.metrics.uniqueness.is_some(),
                case.has_uniqueness,
                "{} uniqueness presence",
                case.name
            );
            assert_eq!(
                case.metrics.accuracy.is_some(),
                case.has_accuracy,
                "{} accuracy presence",
                case.name
            );
            assert!(
                (case.metrics.missing_values_ratio() - case.missing_values_ratio).abs() < 0.01,
                "{} missing_values_ratio",
                case.name
            );
            assert!(
                (case.metrics.key_uniqueness() - case.key_uniqueness).abs() < 0.01,
                "{} key_uniqueness",
                case.name
            );
            assert!(
                (case.metrics.outlier_ratio() - case.outlier_ratio).abs() < 0.01,
                "{} outlier_ratio",
                case.name
            );
        }
    }
}
