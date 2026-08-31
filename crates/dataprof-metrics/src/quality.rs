use std::collections::HashMap;

use dataprof_core::{ColumnProfile, QualityDimension, QualityScoreWeights};
use serde::{Deserialize, Serialize};

use crate::core::errors::DataProfilerError;

/// Completeness metrics (ISO 8000-8).
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CompletenessMetrics {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub missing_values_ratio: f64,
    /// Percentage of records in which every field is present, counted over
    /// every row the engine read. Strict by design: a record missing one
    /// optional field is not complete, so read it beside `null_columns`,
    /// which names the columns responsible.
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub complete_records_ratio: f64,
    pub null_columns: Vec<String>,
    /// Total cells examined (rows × columns). 0 means the dimension had
    /// nothing to assess and it is excluded from the overall score.
    #[serde(default)]
    pub total_cells: usize,
}

/// Consistency metrics (ISO 8000-61).
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ConsistencyMetrics {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub data_type_consistency: f64,
    pub format_violations: usize,
    pub encoding_issues: usize,
    /// Non-null values examined for type consistency. 0 means the dimension
    /// had nothing to assess and it is excluded from the overall score.
    #[serde(default)]
    pub values_checked: usize,
}

/// Uniqueness metrics (ISO 8000-110).
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct UniquenessMetrics {
    pub duplicate_rows: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub key_uniqueness: f64,
    pub high_cardinality_warning: bool,
    /// Rows scanned for exact duplicates. 0 means the dimension had nothing
    /// to assess and it is excluded from the overall score.
    #[serde(default)]
    pub rows_checked: usize,
    /// Column whose uniqueness `key_uniqueness` describes. `None` means no
    /// key column was identified; `key_uniqueness` then carries no signal
    /// and does not contribute to the dimension score.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_column: Option<String>,
    /// True when `duplicate_rows` comes from the full-stream distinct-count
    /// estimator after it spilled to its HLL sketch (~1% relative error on
    /// the distinct count), rather than an exact count.
    #[serde(default, skip_serializing_if = "is_false")]
    pub duplicate_rows_approximate: bool,
}

/// Full-stream row-duplicate counts produced by an engine's row tracker.
///
/// Engines that see whole records (CSV, JSON, streaming readers) count
/// duplicates over *every* row with bounded memory: exact below the
/// distinct-row threshold, HLL-estimated (and flagged) beyond it. When
/// available this supersedes the sample-based duplicate scan, which cannot
/// run at all on misaligned per-column samples.
#[derive(Debug, Clone, Copy)]
pub struct RowDuplicateSummary {
    pub duplicate_rows: usize,
    pub rows_checked: usize,
    pub approximate: bool,
}

/// Full-stream complete-record counts produced by an engine's row tracker.
///
/// A "complete" record is one whose every field is present. That is a
/// property of a row, so it can only be counted while whole rows are in
/// hand; per-column null totals cannot recover it, because they do not say
/// whether two nulls landed in the same record. Engines that see whole
/// records accumulate this exactly over the full stream with one counter,
/// independent of sampling.
#[derive(Debug, Clone, Copy)]
pub struct RowCompletenessSummary {
    pub complete_rows: usize,
    pub rows_checked: usize,
}

/// Accuracy metrics (ISO 25012).
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AccuracyMetrics {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub outlier_ratio: f64,
    pub range_violations: usize,
    pub negative_values_in_positive: usize,
    /// Finite numeric values examined across all columns. 0 means the
    /// dimension had nothing to assess and it is excluded from the overall
    /// score.
    #[serde(default)]
    pub numeric_values_checked: usize,
}

/// Timeliness metrics (ISO 8000-8).
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TimelinessMetrics {
    pub future_dates_count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub stale_data_ratio: f64,
    pub temporal_violations: usize,
    /// Non-null values in inferred or explicitly configured temporal columns
    /// that failed calendar-date parsing.
    #[serde(default)]
    pub invalid_date_values: usize,
    /// Non-null values examined in inferred or explicitly configured temporal
    /// columns. 0 means
    /// the dimension had nothing to assess and it is excluded from the
    /// overall score.
    #[serde(default)]
    pub date_values_checked: usize,
    /// Start/end value pairs actually compared for temporal ordering.
    /// `temporal_violations` is bounded by this, not by
    /// `date_values_checked` — the pair scan may cover columns the date
    /// scan does not.
    #[serde(default)]
    pub temporal_pairs_checked: usize,
}

/// Validity metrics derived from confidently detected semantic patterns.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ValidityMetrics {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub valid_values_ratio: f64,
    pub invalid_values: usize,
    /// Non-null values checked against a dominant semantic pattern.
    #[serde(default)]
    pub values_checked: usize,
}

/// Precision metrics for effective decimal-scale consistency.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct PrecisionMetrics {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub decimal_places_consistency: f64,
    pub inconsistent_precision_values: usize,
    /// Parseable finite values examined in floating-point columns.
    #[serde(default)]
    pub numeric_values_checked: usize,
}

/// Whether a dimension's metrics were computed against anything.
///
/// Every dimension carries the counter it divides by, and a zero there means
/// the dimension had nothing to assess: no cells, no non-null values, no
/// numeric values, no dates, no confidently detected pattern. Its ratios are
/// then defaults rather than measurements, so they are withheld instead of
/// reported. `None` means "not analyzed"; it is not a score of 100.
///
/// This is the single definition of "assessed" behind the dimension scores,
/// the serialized report and the Python evidence accessors.
///
/// [`QualityMetrics::assessed_dimensions`] applies it and then narrows further,
/// to the dimensions carrying a positive score weight, because it answers what
/// is behind [`QualityMetrics::overall_score`] rather than what was measured. A
/// dimension weighted `0.0` is assessed and reported while contributing nothing
/// to the aggregate.
pub(crate) trait Assessed {
    fn assessed(&self) -> bool;
}

/// Serde predicate: skip a dimension that assessed nothing.
fn is_unassessed<T: Assessed>(metrics: &Option<T>) -> bool {
    metrics.as_ref().is_none_or(|metrics| !metrics.assessed())
}

impl CompletenessMetrics {
    /// True when at least one cell was examined.
    pub fn is_assessed(&self) -> bool {
        self.total_cells > 0
    }
}

impl ConsistencyMetrics {
    /// True when at least one non-null value was examined.
    pub fn is_assessed(&self) -> bool {
        self.values_checked > 0
    }
}

impl UniquenessMetrics {
    /// True when either component had data: rows scanned for duplicates, or an
    /// identified key column whose uniqueness `key_uniqueness` describes.
    pub fn is_assessed(&self) -> bool {
        self.rows_checked > 0 || self.key_column.is_some()
    }
}

impl AccuracyMetrics {
    /// True when at least one finite numeric value was examined.
    pub fn is_assessed(&self) -> bool {
        self.numeric_values_checked > 0
    }
}

impl TimelinessMetrics {
    /// True when at least one value in a temporal column was examined.
    pub fn is_assessed(&self) -> bool {
        self.date_values_checked > 0
    }
}

impl ValidityMetrics {
    /// True when at least one value was checked against a dominant pattern.
    pub fn is_assessed(&self) -> bool {
        self.values_checked > 0
    }
}

impl PrecisionMetrics {
    /// True when at least one parseable float was examined.
    pub fn is_assessed(&self) -> bool {
        self.numeric_values_checked > 0
    }
}

macro_rules! impl_assessed {
    ($($metrics:ty),+ $(,)?) => {
        $(impl Assessed for $metrics {
            fn assessed(&self) -> bool {
                self.is_assessed()
            }
        })+
    };
}

impl_assessed!(
    CompletenessMetrics,
    ConsistencyMetrics,
    UniquenessMetrics,
    AccuracyMetrics,
    TimelinessMetrics,
    ValidityMetrics,
    PrecisionMetrics,
);

/// Comprehensive data quality metrics following industry standards.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct QualityMetrics {
    #[serde(skip_serializing_if = "is_unassessed")]
    pub completeness: Option<CompletenessMetrics>,
    #[serde(skip_serializing_if = "is_unassessed")]
    pub consistency: Option<ConsistencyMetrics>,
    #[serde(skip_serializing_if = "is_unassessed")]
    pub uniqueness: Option<UniquenessMetrics>,
    #[serde(skip_serializing_if = "is_unassessed")]
    pub accuracy: Option<AccuracyMetrics>,
    #[serde(skip_serializing_if = "is_unassessed")]
    pub timeliness: Option<TimelinessMetrics>,
    #[serde(skip_serializing_if = "is_unassessed")]
    pub validity: Option<ValidityMetrics>,
    #[serde(skip_serializing_if = "is_unassessed")]
    pub precision: Option<PrecisionMetrics>,
    /// True when the sample used to compute these metrics was below the
    /// minimum recommended size (10 rows). When set, the quality scores and
    /// per-dimension ratios should be treated as directional rather than
    /// reliable. Backwards-compatible: defaults to `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub low_sample_warning: bool,
    /// Weights used to aggregate dimension scores. Default weights are omitted
    /// from serialized reports; custom weights are retained for reproducible
    /// score calculation after a round trip.
    #[serde(default, skip_serializing_if = "QualityScoreWeights::is_default")]
    pub score_weights: QualityScoreWeights,
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
                total_cells: 0,
            }),
            consistency: Some(ConsistencyMetrics {
                data_type_consistency: 100.0,
                format_violations: 0,
                encoding_issues: 0,
                values_checked: 0,
            }),
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: 0,
                key_uniqueness: 100.0,
                high_cardinality_warning: false,
                rows_checked: 0,
                key_column: None,
                duplicate_rows_approximate: false,
            }),
            accuracy: Some(AccuracyMetrics {
                outlier_ratio: 0.0,
                range_violations: 0,
                negative_values_in_positive: 0,
                numeric_values_checked: 0,
            }),
            timeliness: Some(TimelinessMetrics {
                future_dates_count: 0,
                stale_data_ratio: 0.0,
                temporal_violations: 0,
                invalid_date_values: 0,
                date_values_checked: 0,
                temporal_pairs_checked: 0,
            }),
            validity: Some(ValidityMetrics {
                valid_values_ratio: 100.0,
                invalid_values: 0,
                values_checked: 0,
            }),
            precision: Some(PrecisionMetrics {
                decimal_places_consistency: 100.0,
                inconsistent_precision_values: 0,
                numeric_values_checked: 0,
            }),
            low_sample_warning: false,
            score_weights: QualityScoreWeights::default(),
        }
    }

    pub fn calculate_from_data(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<Self, DataProfilerError> {
        let calculator = crate::analysis::MetricsCalculator::new();
        calculator.calculate_comprehensive_metrics(data, column_profiles, None)
    }

    /// Score for the completeness dimension (0-100), or `None` when the
    /// dimension was not computed or had no cells to assess.
    ///
    /// Mean of cell-level completeness (`100 - missing_values_ratio`) and
    /// row-level completeness (`complete_records_ratio`).
    pub fn completeness_score(&self) -> Option<f64> {
        let c = self.completeness.as_ref().filter(|c| c.is_assessed())?;
        let cell_level = 100.0 - c.missing_values_ratio;
        Some(((cell_level + c.complete_records_ratio) / 2.0).clamp(0.0, 100.0))
    }

    /// Score for the consistency dimension (0-100), or `None` when the
    /// dimension was not computed or had no non-null values to assess.
    ///
    /// Type consistency, penalized by format violations and encoding issues
    /// as a share of the values checked.
    pub fn consistency_score(&self) -> Option<f64> {
        let c = self.consistency.as_ref().filter(|c| c.is_assessed())?;
        let violation_ratio =
            (c.format_violations + c.encoding_issues) as f64 / c.values_checked as f64;
        Some((c.data_type_consistency - violation_ratio * 100.0).clamp(0.0, 100.0))
    }

    /// Score for the uniqueness dimension (0-100), or `None` when the
    /// dimension was not computed or neither component had data.
    ///
    /// Mean of the available components: share of non-duplicate rows (when
    /// a row tracker or an aligned sample scan produced a count) and
    /// `key_uniqueness` (when a key column was identified). Engines without
    /// a row tracker whose samples cannot be proven row-aligned contribute
    /// only the key component.
    pub fn uniqueness_score(&self) -> Option<f64> {
        let u = self.uniqueness.as_ref().filter(|u| u.is_assessed())?;
        let duplicate_score = (u.rows_checked > 0)
            .then(|| (1.0 - u.duplicate_rows as f64 / u.rows_checked as f64) * 100.0);
        let key_score = u.key_column.is_some().then_some(u.key_uniqueness);

        let (sum, count) = [duplicate_score, key_score]
            .iter()
            .flatten()
            .fold((0.0, 0u32), |(sum, count), score| (sum + score, count + 1));
        if count == 0 {
            return None;
        }
        Some((sum / count as f64).clamp(0.0, 100.0))
    }

    /// Score for the accuracy dimension (0-100), or `None` when the
    /// dimension was not computed or no numeric values were found.
    ///
    /// `100 - outlier_ratio`, penalized by range violations and negative
    /// values in positive-only columns as a share of the numeric values
    /// checked.
    pub fn accuracy_score(&self) -> Option<f64> {
        let a = self.accuracy.as_ref().filter(|a| a.is_assessed())?;
        let violation_ratio = (a.range_violations + a.negative_values_in_positive) as f64
            / a.numeric_values_checked as f64;
        Some((100.0 - a.outlier_ratio - violation_ratio * 100.0).clamp(0.0, 100.0))
    }

    /// Score for the timeliness dimension (0-100), or `None` when the
    /// dimension was not computed or no values were found in inferred or
    /// explicitly configured temporal columns.
    ///
    /// `100 - stale_data_ratio`, penalized by future dates as a share of
    /// the date values checked and by temporal ordering violations as a
    /// share of the pairs actually compared.
    pub fn timeliness_score(&self) -> Option<f64> {
        let t = self.timeliness.as_ref().filter(|t| t.is_assessed())?;
        let value_violation_ratio =
            (t.future_dates_count + t.invalid_date_values) as f64 / t.date_values_checked as f64;
        let temporal_ratio = if t.temporal_pairs_checked > 0 {
            t.temporal_violations as f64 / t.temporal_pairs_checked as f64
        } else {
            0.0
        };
        Some(
            (100.0 - t.stale_data_ratio - (value_violation_ratio + temporal_ratio) * 100.0)
                .clamp(0.0, 100.0),
        )
    }

    /// Score for semantic-pattern validity (0-100), or `None` when no column
    /// had a confidently detected pattern to validate.
    pub fn validity_score(&self) -> Option<f64> {
        let validity = self.validity.as_ref().filter(|v| v.is_assessed())?;
        Some(validity.valid_values_ratio.clamp(0.0, 100.0))
    }

    /// Score for decimal-scale precision consistency (0-100), or `None` when
    /// no floating-point values were available to assess.
    pub fn precision_score(&self) -> Option<f64> {
        let precision = self.precision.as_ref().filter(|p| p.is_assessed())?;
        Some(precision.decimal_places_consistency.clamp(0.0, 100.0))
    }

    /// Weighted components of the overall score: `(dimension, weight, score)`.
    fn weighted_scores(&self) -> [(QualityDimension, f64, Option<f64>); 7] {
        [
            (
                QualityDimension::Completeness,
                self.score_weights.completeness,
                self.completeness_score(),
            ),
            (
                QualityDimension::Consistency,
                self.score_weights.consistency,
                self.consistency_score(),
            ),
            (
                QualityDimension::Uniqueness,
                self.score_weights.uniqueness,
                self.uniqueness_score(),
            ),
            (
                QualityDimension::Accuracy,
                self.score_weights.accuracy,
                self.accuracy_score(),
            ),
            (
                QualityDimension::Timeliness,
                self.score_weights.timeliness,
                self.timeliness_score(),
            ),
            (
                QualityDimension::Validity,
                self.score_weights.validity,
                self.validity_score(),
            ),
            (
                QualityDimension::Precision,
                self.score_weights.precision,
                self.precision_score(),
            ),
        ]
    }

    /// Dimensions that were computed *and* had data to assess. Only these
    /// contribute to [`overall_score`](Self::overall_score).
    pub fn assessed_dimensions(&self) -> Vec<QualityDimension> {
        self.weighted_scores()
            .iter()
            .filter(|(_, weight, score)| *weight > 0.0 && score.is_some())
            .map(|(dim, _, _)| *dim)
            .collect()
    }

    /// Overall quality score (0-100): weighted average of the assessed
    /// dimension scores, with weights renormalized over the assessed
    /// dimensions. A dimension with nothing to assess (no numeric values,
    /// no date columns, ...) is excluded instead of counting as perfect.
    ///
    /// `None` when no dimension was assessable — the renormalization has no
    /// remainder to average, and an empty set of evidence is "not analyzed",
    /// not a score of zero. Equivalent to
    /// [`assessed_dimensions`](Self::assessed_dimensions) being empty.
    pub fn overall_score(&self) -> Option<f64> {
        let mut total_weight = 0.0;
        let mut score = 0.0;

        for (_, weight, dimension_score) in self.weighted_scores() {
            if let Some(value) = dimension_score {
                total_weight += weight;
                score += value * weight;
            }
        }

        (total_weight > 0.0).then(|| (score / total_weight).min(100.0))
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

    pub fn invalid_date_values(&self) -> usize {
        self.timeliness
            .as_ref()
            .map_or(0, |t| t.invalid_date_values)
    }

    pub fn valid_values_ratio(&self) -> f64 {
        self.validity
            .as_ref()
            .map_or(100.0, |v| v.valid_values_ratio)
    }

    pub fn invalid_values(&self) -> usize {
        self.validity.as_ref().map_or(0, |v| v.invalid_values)
    }

    pub fn decimal_places_consistency(&self) -> f64 {
        self.precision
            .as_ref()
            .map_or(100.0, |p| p.decimal_places_consistency)
    }

    pub fn inconsistent_precision_values(&self) -> usize {
        self.precision
            .as_ref()
            .map_or(0, |p| p.inconsistent_precision_values)
    }

    pub fn supports_dimension(&self, dimension: QualityDimension) -> bool {
        match dimension {
            QualityDimension::Completeness => self.completeness.is_some(),
            QualityDimension::Consistency => self.consistency.is_some(),
            QualityDimension::Uniqueness => self.uniqueness.is_some(),
            QualityDimension::Accuracy => self.accuracy.is_some(),
            QualityDimension::Timeliness => self.timeliness.is_some(),
            QualityDimension::Validity => self.validity.is_some(),
            QualityDimension::Precision => self.precision.is_some(),
        }
    }
}

/// Confidence level for quality metrics.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
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
    /// No dimension had anything to assess, so there is no score for the
    /// other variants to describe. Reporting `Exact` here would claim
    /// certainty about a number that was never computed.
    NotAssessed,
}

/// Wraps quality metrics with confidence information.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct QualityAssessment {
    pub metrics: QualityMetrics,
    pub confidence: MetricConfidence,
}

impl QualityAssessment {
    /// Pair metrics with the confidence of the run that produced them.
    ///
    /// `confidence` is downgraded to [`MetricConfidence::NotAssessed`] when
    /// no dimension was assessable: how the numbers were obtained is only
    /// meaningful when there are numbers.
    pub fn new(metrics: QualityMetrics, confidence: MetricConfidence) -> Self {
        let confidence = if metrics.assessed_dimensions().is_empty() {
            MetricConfidence::NotAssessed
        } else {
            confidence
        };
        Self {
            metrics,
            confidence,
        }
    }

    pub fn exact(metrics: QualityMetrics) -> Self {
        Self::new(metrics, MetricConfidence::Exact)
    }

    pub fn approximate(
        metrics: QualityMetrics,
        sample_size: usize,
        population_size: Option<usize>,
    ) -> Self {
        Self::new(
            metrics,
            MetricConfidence::Approximate {
                sample_size,
                population_size,
            },
        )
    }

    /// Overall quality score, or `None` when no dimension was assessable.
    /// See [`QualityMetrics::overall_score`].
    pub fn score(&self) -> Option<f64> {
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

    /// Metrics where every dimension has data to assess and a perfect score.
    fn perfect_assessed() -> QualityMetrics {
        QualityMetrics {
            completeness: Some(CompletenessMetrics {
                missing_values_ratio: 0.0,
                complete_records_ratio: 100.0,
                null_columns: vec![],
                total_cells: 100,
            }),
            consistency: Some(ConsistencyMetrics {
                data_type_consistency: 100.0,
                format_violations: 0,
                encoding_issues: 0,
                values_checked: 100,
            }),
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: 0,
                key_uniqueness: 100.0,
                high_cardinality_warning: false,
                rows_checked: 100,
                key_column: None,
                duplicate_rows_approximate: false,
            }),
            accuracy: Some(AccuracyMetrics {
                outlier_ratio: 0.0,
                range_violations: 0,
                negative_values_in_positive: 0,
                numeric_values_checked: 100,
            }),
            timeliness: Some(TimelinessMetrics {
                future_dates_count: 0,
                stale_data_ratio: 0.0,
                temporal_violations: 0,
                invalid_date_values: 0,
                date_values_checked: 100,
                temporal_pairs_checked: 100,
            }),
            validity: Some(ValidityMetrics {
                valid_values_ratio: 100.0,
                invalid_values: 0,
                values_checked: 100,
            }),
            precision: Some(PrecisionMetrics {
                decimal_places_consistency: 100.0,
                inconsistent_precision_values: 0,
                numeric_values_checked: 100,
            }),
            low_sample_warning: false,
            score_weights: QualityScoreWeights::default(),
        }
    }

    #[test]
    fn test_custom_weights_change_and_survive_serialized_score() {
        let mut metrics = perfect_assessed();
        if let Some(ref mut c) = metrics.completeness {
            c.missing_values_ratio = 100.0;
            c.complete_records_ratio = 0.0;
        }
        metrics.score_weights = QualityScoreWeights {
            completeness: 1.0,
            consistency: 0.0,
            uniqueness: 0.0,
            accuracy: 0.0,
            timeliness: 0.0,
            validity: 0.0,
            precision: 0.0,
        };

        assert_eq!(metrics.overall_score(), Some(0.0));

        let json = serde_json::to_string(&metrics).expect("serialize custom weights");
        assert!(json.contains("score_weights"));
        let restored: QualityMetrics =
            serde_json::from_str(&json).expect("deserialize custom weights");
        assert_eq!(restored.score_weights, metrics.score_weights);
        assert_eq!(restored.overall_score(), metrics.overall_score());
        assert_eq!(
            restored.assessed_dimensions(),
            vec![QualityDimension::Completeness]
        );
    }

    #[test]
    fn test_empty_metrics_nothing_assessed() {
        let metrics = QualityMetrics::empty();
        assert!(metrics.assessed_dimensions().is_empty());
        // Every denominator is zero, so there is nothing to average. A 0.0
        // here would read as "this data is terrible" (#571).
        assert_eq!(metrics.overall_score(), None);
    }

    #[test]
    fn test_unassessable_assessment_reports_no_score_and_no_confidence() {
        let assessment = QualityAssessment::exact(QualityMetrics::empty());
        assert_eq!(assessment.score(), None);
        assert!(
            matches!(assessment.confidence, MetricConfidence::NotAssessed),
            "confidence must not claim exactness about a score that was never computed, got {:?}",
            assessment.confidence
        );

        let sampled = QualityAssessment::approximate(QualityMetrics::empty(), 0, Some(0));
        assert!(matches!(sampled.confidence, MetricConfidence::NotAssessed));
    }

    #[test]
    fn test_assessed_assessment_keeps_its_confidence() {
        let assessment = QualityAssessment::exact(perfect_assessed());
        assert_eq!(assessment.score(), Some(100.0));
        assert!(matches!(assessment.confidence, MetricConfidence::Exact));
    }

    #[test]
    fn test_perfect_assessed_scores_100() {
        let metrics = perfect_assessed();
        assert_eq!(metrics.assessed_dimensions().len(), 7);
        assert_eq!(metrics.overall_score(), Some(100.0));
    }

    #[test]
    fn test_quality_score_completeness_weight() {
        let mut metrics = perfect_assessed();
        if let Some(ref mut c) = metrics.completeness {
            c.missing_values_ratio = 100.0;
            c.complete_records_ratio = 0.0;
        }
        assert_eq!(metrics.overall_score(), Some(75.0));
    }

    #[test]
    fn test_quality_score_all_bad() {
        let mut metrics = perfect_assessed();
        if let Some(ref mut c) = metrics.completeness {
            c.missing_values_ratio = 100.0;
            c.complete_records_ratio = 0.0;
        }
        if let Some(ref mut c) = metrics.consistency {
            c.data_type_consistency = 0.0;
        }
        if let Some(ref mut u) = metrics.uniqueness {
            u.duplicate_rows = 100;
        }
        if let Some(ref mut a) = metrics.accuracy {
            a.outlier_ratio = 100.0;
        }
        if let Some(ref mut t) = metrics.timeliness {
            t.stale_data_ratio = 100.0;
        }
        if let Some(ref mut v) = metrics.validity {
            v.valid_values_ratio = 0.0;
        }
        if let Some(ref mut p) = metrics.precision {
            p.decimal_places_consistency = 0.0;
        }

        // Assessed and genuinely bad: 0.0, not None.
        assert_eq!(metrics.overall_score(), Some(0.0));
    }

    #[test]
    fn test_vacuous_dimensions_drop_out() {
        // Text-only dataset shape: nothing numeric, no dates, no rows scanned
        // for duplicates. Under the old aggregation these dimensions counted
        // as perfect and floored the score at 70; now they are excluded and
        // the weights renormalize over what was actually assessed.
        let mut metrics = perfect_assessed();
        if let Some(ref mut c) = metrics.completeness {
            c.missing_values_ratio = 50.0;
            c.complete_records_ratio = 50.0;
        }
        if let Some(ref mut u) = metrics.uniqueness {
            u.rows_checked = 0;
        }
        if let Some(ref mut a) = metrics.accuracy {
            a.numeric_values_checked = 0;
        }
        if let Some(ref mut t) = metrics.timeliness {
            t.date_values_checked = 0;
        }
        if let Some(ref mut v) = metrics.validity {
            v.values_checked = 0;
        }
        if let Some(ref mut p) = metrics.precision {
            p.numeric_values_checked = 0;
        }

        assert_eq!(
            metrics.assessed_dimensions(),
            vec![
                QualityDimension::Completeness,
                QualityDimension::Consistency
            ]
        );
        // (0.25 * 50 + 0.20 * 100) / 0.45 = 72.22..
        assert!((metrics.overall_score().expect("assessed") - 72.2222).abs() < 0.01);
    }

    #[test]
    fn test_duplicate_rows_lower_uniqueness_score() {
        let mut metrics = perfect_assessed();
        if let Some(ref mut u) = metrics.uniqueness {
            u.duplicate_rows = 30;
        }
        let score = metrics
            .uniqueness_score()
            .expect("uniqueness should be assessed");
        assert!((score - 70.0).abs() < 0.01);
    }

    #[test]
    fn test_key_only_uniqueness_when_duplicate_scan_not_assessable() {
        // Streaming shape: per-column samples are misaligned so the
        // duplicate scan did not run, but key uniqueness is exact.
        let mut metrics = perfect_assessed();
        if let Some(ref mut u) = metrics.uniqueness {
            u.rows_checked = 0;
            u.key_column = Some("order_id".to_string());
            u.key_uniqueness = 90.0;
        }
        let score = metrics
            .uniqueness_score()
            .expect("key component alone should keep uniqueness assessed");
        assert!((score - 90.0).abs() < 0.01);
    }

    #[test]
    fn test_key_column_blends_into_uniqueness_score() {
        let mut metrics = perfect_assessed();
        if let Some(ref mut u) = metrics.uniqueness {
            u.key_column = Some("order_id".to_string());
            u.key_uniqueness = 60.0;
        }
        // mean(duplicate-free score 100, key uniqueness 60)
        let score = metrics
            .uniqueness_score()
            .expect("uniqueness should be assessed");
        assert!((score - 80.0).abs() < 0.01);
    }

    #[test]
    fn test_format_and_encoding_violations_lower_consistency_score() {
        let mut metrics = perfect_assessed();
        if let Some(ref mut c) = metrics.consistency {
            c.format_violations = 5;
            c.encoding_issues = 5;
        }
        let score = metrics
            .consistency_score()
            .expect("consistency should be assessed");
        assert!((score - 90.0).abs() < 0.01);
    }

    #[test]
    fn test_range_and_negative_violations_lower_accuracy_score() {
        let mut metrics = perfect_assessed();
        if let Some(ref mut a) = metrics.accuracy {
            a.outlier_ratio = 10.0;
            a.range_violations = 5;
            a.negative_values_in_positive = 5;
        }
        let score = metrics
            .accuracy_score()
            .expect("accuracy should be assessed");
        assert!((score - 80.0).abs() < 0.01);
    }

    #[test]
    fn test_future_dates_and_temporal_violations_lower_timeliness_score() {
        let mut metrics = perfect_assessed();
        if let Some(ref mut t) = metrics.timeliness {
            t.stale_data_ratio = 20.0;
            t.future_dates_count = 5;
            t.temporal_violations = 5;
        }
        let score = metrics
            .timeliness_score()
            .expect("timeliness should be assessed");
        assert!((score - 70.0).abs() < 0.01);
    }

    #[test]
    fn test_legacy_json_without_denominators_is_not_assessed() {
        // Reports serialized before the denominator fields existed
        // deserialize with zero denominators: facts remain readable, but no
        // dimension is assessable and no score is fabricated.
        let json = r#"{
            "completeness": {
                "missing_values_ratio": 5.0,
                "complete_records_ratio": 95.0,
                "null_columns": []
            }
        }"#;
        let metrics: QualityMetrics = serde_json::from_str(json).unwrap();

        assert!((metrics.missing_values_ratio() - 5.0).abs() < 0.01);
        assert!(metrics.completeness_score().is_none());
        assert!(metrics.assessed_dimensions().is_empty());
    }

    #[test]
    fn test_partial_dimensions_only_completeness() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 100.0,
                missing_values_ratio: 0.0,
                null_columns: vec![],
                total_cells: 10,
            }),
            ..QualityMetrics::default()
        };

        assert!(metrics.completeness.is_some());
        assert!(metrics.consistency.is_none());
        assert!(metrics.uniqueness.is_none());
        assert!(metrics.accuracy.is_none());
        assert!(metrics.timeliness.is_none());
        assert_eq!(metrics.overall_score(), Some(100.0));
    }

    #[test]
    fn test_partial_dimensions_two_dimensions() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                missing_values_ratio: 50.0,
                complete_records_ratio: 50.0,
                null_columns: vec![],
                total_cells: 100,
            }),
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: 20,
                key_uniqueness: 100.0,
                high_cardinality_warning: false,
                rows_checked: 100,
                key_column: None,
                duplicate_rows_approximate: false,
            }),
            ..QualityMetrics::default()
        };

        // (0.25 * 50 + 0.15 * 80) / 0.40 = 61.25
        assert!((metrics.overall_score().expect("assessed") - 61.25).abs() < 0.01);
    }

    #[test]
    fn test_all_dimensions_none_score_is_none() {
        let metrics = QualityMetrics::default();

        assert_eq!(metrics.overall_score(), None);
        assert!(metrics.assessed_dimensions().is_empty());
    }

    #[test]
    fn test_partial_dimensions_json_skips_none() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                total_cells: 10,
                ..CompletenessMetrics::default()
            }),
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
    fn a_dimension_with_no_denominator_is_not_serialized() {
        // `empty()` builds every dimension with zero counters, which is what an
        // engine produces for a header-only file. Serializing those defaults
        // published `valid_values_ratio: 100.0` from `values_checked: 0` (#622).
        let metrics = QualityMetrics::empty();
        assert!(metrics.assessed_dimensions().is_empty());

        let json = serde_json::to_string(&metrics).unwrap();
        for dimension in [
            "completeness",
            "consistency",
            "uniqueness",
            "accuracy",
            "timeliness",
            "validity",
            "precision",
        ] {
            assert!(
                !json.contains(dimension),
                "{dimension} was serialized without having assessed anything: {json}"
            );
        }
    }

    #[test]
    fn a_zero_weighted_dimension_keeps_its_evidence() {
        // Serialization asks "did this measure anything", which is a wider
        // question than "is this behind the overall score". A weight of 0.0
        // removes a dimension from `assessed_dimensions` and from the
        // aggregate, and removes nothing from what it actually observed.
        let mut metrics = QualityMetrics::empty();
        metrics.validity = Some(ValidityMetrics {
            valid_values_ratio: 80.0,
            invalid_values: 2,
            values_checked: 10,
        });
        metrics.score_weights = QualityScoreWeights {
            validity: 0.0,
            ..QualityScoreWeights::default()
        };

        assert!(metrics.validity.as_ref().expect("present").is_assessed());
        assert!(metrics.validity_score().is_some());
        assert!(
            !metrics
                .assessed_dimensions()
                .contains(&QualityDimension::Validity)
        );
        assert_eq!(metrics.overall_score(), None);

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(
            json.contains("valid_values_ratio"),
            "a measured dimension was dropped for carrying no weight: {json}"
        );
    }

    #[test]
    fn serialized_dimensions_are_exactly_the_ones_with_a_denominator() {
        let mut metrics = QualityMetrics::empty();
        metrics.completeness = Some(CompletenessMetrics {
            missing_values_ratio: 10.0,
            complete_records_ratio: 90.0,
            null_columns: vec!["a".to_string()],
            total_cells: 100,
        });
        metrics.validity = Some(ValidityMetrics {
            valid_values_ratio: 80.0,
            invalid_values: 2,
            values_checked: 10,
        });

        let assessed = metrics.assessed_dimensions();
        assert_eq!(
            assessed,
            vec![QualityDimension::Completeness, QualityDimension::Validity]
        );

        let json = serde_json::to_string(&metrics).unwrap();
        assert!(json.contains("completeness"));
        assert!(json.contains("validity"));
        assert!(!json.contains("consistency"));
        assert!(!json.contains("precision"));
    }

    #[test]
    fn is_assessed_agrees_with_the_dimension_score() {
        // One definition of "assessed" behind both, so a score can never be
        // None while its evidence claims to have measured something.
        let assessed = QualityMetrics {
            uniqueness: Some(UniquenessMetrics {
                duplicate_rows: 0,
                key_uniqueness: 100.0,
                high_cardinality_warning: false,
                rows_checked: 0,
                // No rows scanned, but a key column was identified, so the key
                // component alone still carries a measurement.
                key_column: Some("id".to_string()),
                duplicate_rows_approximate: false,
            }),
            ..QualityMetrics::default()
        };
        let uniqueness = assessed.uniqueness.as_ref().expect("present");
        assert!(uniqueness.is_assessed());
        assert!(assessed.uniqueness_score().is_some());

        let unassessed = QualityMetrics::empty();
        for metrics in [
            unassessed.completeness.as_ref().map(|m| m.is_assessed()),
            unassessed.consistency.as_ref().map(|m| m.is_assessed()),
            unassessed.uniqueness.as_ref().map(|m| m.is_assessed()),
            unassessed.accuracy.as_ref().map(|m| m.is_assessed()),
            unassessed.timeliness.as_ref().map(|m| m.is_assessed()),
            unassessed.validity.as_ref().map(|m| m.is_assessed()),
            unassessed.precision.as_ref().map(|m| m.is_assessed()),
        ] {
            assert_eq!(metrics, Some(false));
        }
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
                        total_cells: 16,
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
                        rows_checked: 25,
                        key_column: Some("user_id".to_string()),
                        duplicate_rows_approximate: false,
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
                        numeric_values_checked: 16,
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
