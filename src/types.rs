use crate::core::errors::DataProfilerError;
use std::collections::HashMap;

pub use dataprof_core::quality::{MetricPack, QualityDimension};
pub use dataprof_core::source::{
    DataFrameLibrary, DataSource, FileFormat, ParquetMetadata, QueryEngine, StreamSourceSystem,
};

// ============================================================================
// Source-Agnostic Data Source Types
// ============================================================================

// ============================================================================
// ISO 25012 Quality Dimension Enum
// ============================================================================

// ============================================================================
// Metric Pack Selection
// ============================================================================

// ============================================================================
// Per-Dimension Metric Sub-Structs (ISO 25012 "Metric Packs")
// ============================================================================

/// Completeness metrics (ISO 8000-8)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CompletenessMetrics {
    /// Percentage of missing values across all cells
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub missing_values_ratio: f64,
    /// Percentage of rows with no null values
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub complete_records_ratio: f64,
    /// Columns with more than the threshold of null values
    pub null_columns: Vec<String>,
}

/// Consistency metrics (ISO 8000-61)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ConsistencyMetrics {
    /// Percentage of values conforming to expected data type
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub data_type_consistency: f64,
    /// Number of format violations (e.g., malformed dates)
    pub format_violations: usize,
    /// Number of UTF-8 encoding issues detected
    pub encoding_issues: usize,
}

/// Uniqueness metrics (ISO 8000-110)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct UniquenessMetrics {
    /// Number of exact duplicate rows
    pub duplicate_rows: usize,
    /// Percentage of unique values in key columns (if applicable)
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub key_uniqueness: f64,
    /// Warning flag for columns with excessive unique values
    pub high_cardinality_warning: bool,
}

/// Accuracy metrics (ISO 25012)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AccuracyMetrics {
    /// Percentage of statistically anomalous values (outliers)
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub outlier_ratio: f64,
    /// Number of values outside expected ranges
    pub range_violations: usize,
    /// Number of negative values in positive-only fields (e.g., age)
    pub negative_values_in_positive: usize,
}

/// Timeliness metrics (ISO 8000-8)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TimelinessMetrics {
    /// Number of future dates detected (dates beyond current date)
    pub future_dates_count: usize,
    /// Percentage of dates older than staleness threshold (e.g., >5 years)
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub stale_data_ratio: f64,
    /// Temporal ordering violations (e.g., end_date < start_date)
    pub temporal_violations: usize,
}

// ============================================================================
// Composable QualityMetrics (opt-in dimensions)
// ============================================================================

/// Comprehensive data quality metrics following industry standards.
///
/// Each ISO 25012 dimension is an `Option` — dimensions that were not requested
/// (or not computed) are `None`. When all dimensions are requested (the default),
/// all fields are `Some`.
///
/// This composable design enables "lazy metric packs": the engine only computes
/// the dimensions explicitly requested by the user, saving CPU/memory.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QualityMetrics {
    /// Completeness dimension (ISO 8000-8)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completeness: Option<CompletenessMetrics>,

    /// Consistency dimension (ISO 8000-61)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consistency: Option<ConsistencyMetrics>,

    /// Uniqueness dimension (ISO 8000-110)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uniqueness: Option<UniquenessMetrics>,

    /// Accuracy dimension (ISO 25012)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accuracy: Option<AccuracyMetrics>,

    /// Timeliness dimension (ISO 8000-8)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeliness: Option<TimelinessMetrics>,
}

impl QualityMetrics {
    /// Create metrics for an empty dataset (perfect quality, no data).
    /// All dimensions are populated with default "perfect" values.
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
        }
    }

    /// Calculate comprehensive data quality metrics from column data.
    ///
    /// Delegates to the specialized MetricsCalculator for proper separation of concerns.
    /// Uses default ISO 8000/25012 thresholds. Computes all dimensions.
    pub fn calculate_from_data(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<Self, DataProfilerError> {
        let calculator = crate::analysis::MetricsCalculator::new();
        calculator.calculate_comprehensive_metrics(data, column_profiles, None)
    }

    /// Calculate overall quality score (0-100) based on ISO 8000/25012 dimensions.
    ///
    /// Weighted formula (only computed dimensions contribute):
    /// - Completeness: 30% (complete_records_ratio)
    /// - Consistency: 25% (data_type_consistency)
    /// - Uniqueness: 20% (key_uniqueness)
    /// - Accuracy: 15% (100 - outlier_ratio)
    /// - Timeliness: 10% (100 - stale_data_ratio)
    ///
    /// When some dimensions are `None`, the weights of computed dimensions
    /// are re-normalized so the score is still on a 0–100 scale.
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

    // -- Convenience accessors for backward compatibility --

    /// Missing values ratio (from completeness dimension, 0.0 if not computed)
    pub fn missing_values_ratio(&self) -> f64 {
        self.completeness
            .as_ref()
            .map_or(0.0, |c| c.missing_values_ratio)
    }

    /// Complete records ratio (from completeness dimension, 100.0 if not computed)
    pub fn complete_records_ratio(&self) -> f64 {
        self.completeness
            .as_ref()
            .map_or(100.0, |c| c.complete_records_ratio)
    }

    /// Null columns (from completeness dimension, empty if not computed)
    pub fn null_columns(&self) -> &[String] {
        self.completeness.as_ref().map_or(&[], |c| &c.null_columns)
    }

    /// Data type consistency (from consistency dimension, 100.0 if not computed)
    pub fn data_type_consistency(&self) -> f64 {
        self.consistency
            .as_ref()
            .map_or(100.0, |c| c.data_type_consistency)
    }

    /// Format violations (from consistency dimension, 0 if not computed)
    pub fn format_violations(&self) -> usize {
        self.consistency.as_ref().map_or(0, |c| c.format_violations)
    }

    /// Encoding issues (from consistency dimension, 0 if not computed)
    pub fn encoding_issues(&self) -> usize {
        self.consistency.as_ref().map_or(0, |c| c.encoding_issues)
    }

    /// Duplicate rows (from uniqueness dimension, 0 if not computed)
    pub fn duplicate_rows(&self) -> usize {
        self.uniqueness.as_ref().map_or(0, |u| u.duplicate_rows)
    }

    /// Key uniqueness (from uniqueness dimension, 100.0 if not computed)
    pub fn key_uniqueness(&self) -> f64 {
        self.uniqueness.as_ref().map_or(100.0, |u| u.key_uniqueness)
    }

    /// High cardinality warning (from uniqueness dimension, false if not computed)
    pub fn high_cardinality_warning(&self) -> bool {
        self.uniqueness
            .as_ref()
            .is_some_and(|u| u.high_cardinality_warning)
    }

    /// Outlier ratio (from accuracy dimension, 0.0 if not computed)
    pub fn outlier_ratio(&self) -> f64 {
        self.accuracy.as_ref().map_or(0.0, |a| a.outlier_ratio)
    }

    /// Range violations (from accuracy dimension, 0 if not computed)
    pub fn range_violations(&self) -> usize {
        self.accuracy.as_ref().map_or(0, |a| a.range_violations)
    }

    /// Negative values in positive fields (from accuracy dimension, 0 if not computed)
    pub fn negative_values_in_positive(&self) -> usize {
        self.accuracy
            .as_ref()
            .map_or(0, |a| a.negative_values_in_positive)
    }

    /// Future dates count (from timeliness dimension, 0 if not computed)
    pub fn future_dates_count(&self) -> usize {
        self.timeliness.as_ref().map_or(0, |t| t.future_dates_count)
    }

    /// Stale data ratio (from timeliness dimension, 0.0 if not computed)
    pub fn stale_data_ratio(&self) -> f64 {
        self.timeliness.as_ref().map_or(0.0, |t| t.stale_data_ratio)
    }

    /// Temporal violations (from timeliness dimension, 0 if not computed)
    pub fn temporal_violations(&self) -> usize {
        self.timeliness
            .as_ref()
            .map_or(0, |t| t.temporal_violations)
    }
}

/// Confidence level for quality metrics — indicates whether metrics were
/// computed from the full dataset, a bounded sample, or a mix of both.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum MetricConfidence {
    /// All metrics computed from the full dataset (exact)
    Exact,
    /// Metrics computed from a bounded sample (reservoir/HyperLogLog)
    Approximate {
        sample_size: usize,
        population_size: Option<usize>,
    },
    /// Mix of exact stream counters (e.g., completeness from Welford)
    /// and sampled metrics (e.g., uniqueness from HyperLogLog)
    Mixed {
        exact_dimensions: Vec<String>,
        sampled_dimensions: Vec<String>,
        sample_size: usize,
    },
}

/// Wraps quality metrics with confidence information.
///
/// This replaces the former mandatory `DataQualityMetrics` field on reports,
/// adding information about how trustworthy each metric dimension is.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QualityAssessment {
    /// The underlying quality metrics (ISO 8000/25012)
    pub metrics: QualityMetrics,
    /// How the metrics were computed (exact, approximate, or mixed)
    pub confidence: MetricConfidence,
}

impl QualityAssessment {
    /// Create a new QualityAssessment with Exact confidence (full dataset)
    pub fn exact(metrics: QualityMetrics) -> Self {
        Self {
            metrics,
            confidence: MetricConfidence::Exact,
        }
    }

    /// Create a new QualityAssessment with Approximate confidence (sampled)
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

    /// Calculate overall quality score (0-100) using ISO 8000/25012 dimensions
    pub fn score(&self) -> f64 {
        self.metrics.overall_score()
    }
}

impl From<QualityMetrics> for QualityAssessment {
    /// Convert bare metrics into an assessment assuming exact confidence.
    fn from(metrics: QualityMetrics) -> Self {
        Self::exact(metrics)
    }
}

/// Complete profiling report for a data source.
///
/// Contains column-level statistics, execution metadata, and an optional
/// ISO 8000/25012 quality assessment. This is the primary output of all
/// profiling operations (`Profiler::analyze_file`, `Profiler::analyze_source`,
/// `Profiler::profile_stream`, etc.).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ProfileReport {
    /// Unique identifier for this report (UUID v4)
    pub id: String,
    /// Timestamp when the report was generated (ISO 8601 / RFC 3339)
    pub timestamp: String,
    /// Data source metadata (file, query, etc.)
    pub data_source: DataSource,
    /// Column-level profiling results
    pub column_profiles: Vec<ColumnProfile>,
    /// Execution metadata (timing, rows processed, truncation info, etc.)
    #[serde(alias = "scan_info")]
    pub execution: ExecutionMetadata,
    /// Data quality assessment (optional — partial analysis may skip quality)
    #[serde(
        alias = "data_quality_metrics",
        skip_serializing_if = "Option::is_none",
        default,
        deserialize_with = "deserialize_quality_compat"
    )]
    pub quality: Option<QualityAssessment>,
}

impl ProfileReport {
    /// Create a new ProfileReport with auto-generated id and timestamp
    pub fn new(
        data_source: DataSource,
        column_profiles: Vec<ColumnProfile>,
        execution: ExecutionMetadata,
        quality: Option<QualityAssessment>,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            data_source,
            column_profiles,
            execution,
            quality,
        }
    }

    /// Override the auto-generated ID (useful for deterministic caching/testing)
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Override the auto-generated timestamp
    pub fn with_timestamp(mut self, timestamp: impl Into<String>) -> Self {
        self.timestamp = timestamp.into();
        self
    }

    /// Calculate overall quality score using ISO 8000/25012 metrics.
    /// Returns `None` if quality metrics were not computed.
    pub fn quality_score(&self) -> Option<f64> {
        self.quality.as_ref().map(|q| q.score())
    }

    /// Get the data source identifier (for backwards compatibility)
    pub fn source_identifier(&self) -> String {
        self.data_source.identifier()
    }
}

/// Custom deserializer that handles both legacy `DataQualityMetrics` (flat)
/// and new `QualityAssessment` (wrapped with confidence) JSON formats.
fn deserialize_quality_compat<'de, D>(
    deserializer: D,
) -> Result<Option<QualityAssessment>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    // Try to deserialize as the new QualityAssessment first,
    // then fall back to bare QualityMetrics (legacy format)
    let value: Option<serde_json::Value> = Option::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(v) => {
            // Try new format first (has "metrics" and "confidence" fields)
            if v.get("metrics").is_some() && v.get("confidence").is_some() {
                let assessment: QualityAssessment =
                    serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                Ok(Some(assessment))
            } else {
                // Legacy format: bare QualityMetrics object
                let metrics: QualityMetrics =
                    serde_json::from_value(v).map_err(serde::de::Error::custom)?;
                Ok(Some(QualityAssessment::exact(metrics)))
            }
        }
    }
}

/// Reason why profiling was truncated before exhausting the source
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TruncationReason {
    /// Stopped after processing a maximum number of rows
    MaxRows(u64),
    /// Stopped after consuming a maximum number of bytes
    MaxBytes(u64),
    /// Stopped due to memory pressure
    MemoryPressure,
    /// Stopped due to a user-defined stop condition (see #220)
    StopCondition(String),
    /// The input stream was closed by the producer
    StreamClosed,
    /// Stopped due to a timeout
    Timeout,
}

/// Metadata about the profiling execution — replaces the former `ScanInfo`.
///
/// Designed to work for both batch (file-based) and streaming scenarios.
/// For streams, `source_exhausted` indicates whether all data was consumed,
/// and `truncation_reason` explains why processing stopped early.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ExecutionMetadata {
    /// Number of rows actually processed/analyzed
    pub rows_processed: usize,
    /// Number of bytes consumed from the source (if known)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_consumed: Option<u64>,
    /// Number of columns detected in the data
    pub columns_detected: usize,
    /// Total execution time in milliseconds
    pub scan_time_ms: u128,
    /// Throughput in rows per second (auto-calculated)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throughput_rows_sec: Option<f64>,
    /// Peak memory usage in megabytes (if tracked)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_peak_mb: Option<f64>,
    /// Number of errors encountered during profiling
    pub error_count: usize,
    /// Whether the entire source was consumed (false for truncated/partial analysis)
    pub source_exhausted: bool,
    /// If the source was not exhausted, why processing stopped
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<TruncationReason>,
    /// Whether sampling was applied (i.e., not all rows were analyzed)
    pub sampling_applied: bool,
    /// Ratio of rows analyzed to total rows (only meaningful when source_exhausted=true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampling_ratio: Option<f64>,
}

impl ExecutionMetadata {
    /// Create new ExecutionMetadata with throughput calculated automatically.
    ///
    /// Defaults: `source_exhausted=true`, `sampling_applied=false`, no truncation.
    pub fn new(rows_processed: usize, columns_detected: usize, scan_time_ms: u128) -> Self {
        let throughput_rows_sec = if scan_time_ms > 0 {
            Some(rows_processed as f64 / (scan_time_ms as f64 / 1000.0))
        } else {
            None
        };

        Self {
            rows_processed,
            bytes_consumed: None,
            columns_detected,
            scan_time_ms,
            throughput_rows_sec,
            memory_peak_mb: None,
            error_count: 0,
            source_exhausted: true,
            truncation_reason: None,
            sampling_applied: false,
            sampling_ratio: None,
        }
    }

    /// Set sampling information.
    ///
    /// Note: this does **not** change `source_exhausted`. A file can be fully
    /// read yet still sampled (e.g., skip every other row). Call
    /// `.with_source_exhausted(false)` separately when the source was not
    /// fully consumed.
    pub fn with_sampling(mut self, ratio: f64) -> Self {
        self.sampling_applied = true;
        self.sampling_ratio = Some(ratio);
        self
    }

    /// Explicitly set whether the source was fully consumed.
    pub fn with_source_exhausted(mut self, exhausted: bool) -> Self {
        self.source_exhausted = exhausted;
        self
    }

    /// Mark as truncated (sets `source_exhausted=false`).
    pub fn with_truncation(mut self, reason: TruncationReason) -> Self {
        self.source_exhausted = false;
        self.truncation_reason = Some(reason);
        self
    }

    /// Set the number of bytes consumed from the source.
    pub fn with_bytes_consumed(mut self, bytes: u64) -> Self {
        self.bytes_consumed = Some(bytes);
        self
    }

    /// Set the error count.
    pub fn with_error_count(mut self, count: usize) -> Self {
        self.error_count = count;
        self
    }

    /// Set peak memory usage.
    pub fn with_memory_peak_mb(mut self, mb: f64) -> Self {
        self.memory_peak_mb = Some(mb);
        self
    }
}

/// Profiling statistics for a single column.
///
/// Includes data type, null counts, unique counts, type-specific statistics
/// (numeric, text, or datetime), and detected patterns (e.g. email, phone).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnProfile {
    /// Column name
    pub name: String,
    /// Inferred data type
    pub data_type: DataType,
    /// Number of null/missing values
    pub null_count: usize,
    /// Total number of values (including nulls)
    pub total_count: usize,
    /// Number of distinct values (when computed)
    pub unique_count: Option<usize>,
    /// Type-specific statistics (numeric, text, or datetime)
    pub stats: ColumnStats,
    /// Detected value patterns (e.g. email, phone, UUID)
    pub patterns: Vec<Pattern>,
}

/// Inferred column data type.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    /// Text / string values
    String,
    /// Whole numbers (i64 range)
    Integer,
    /// Floating-point numbers
    Float,
    /// Date or datetime values
    Date,
    /// Boolean (true / false) values
    Boolean,
}

/// Quartile statistics for numeric distributions.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Quartiles {
    /// 25th percentile
    pub q1: f64,
    /// 50th percentile (median)
    pub q2: f64,
    /// 75th percentile
    pub q3: f64,
    /// Interquartile range (Q3 - Q1)
    pub iqr: f64,
}

/// A value and its frequency count within a column.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FrequencyItem {
    /// The value as a string
    pub value: String,
    /// Number of occurrences
    pub count: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub percentage: f64,
}

/// Statistics for numeric (integer or float) columns.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NumericStats {
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub min: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub max: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub mean: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub std_dev: f64,
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub variance: f64,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_2_opt"
    )]
    pub median: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::quartiles::serialize"
    )]
    pub quartiles: Option<Quartiles>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_2_opt"
    )]
    pub mode: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_2_opt"
    )]
    pub coefficient_of_variation: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_4_opt"
    )]
    pub skewness: Option<f64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "crate::serde_helpers::round_4_opt"
    )]
    pub kurtosis: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_approximate: Option<bool>,
}

impl NumericStats {
    /// Empty/default numeric stats (all zeros, no optional fields).
    pub fn empty() -> Self {
        Self {
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            std_dev: 0.0,
            variance: 0.0,
            median: None,
            quartiles: None,
            mode: None,
            coefficient_of_variation: None,
            skewness: None,
            kurtosis: None,
            is_approximate: None,
        }
    }
}

/// Statistics for text/string columns.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TextStats {
    pub min_length: usize,
    pub max_length: usize,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub avg_length: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub most_frequent: Option<Vec<FrequencyItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub least_frequent: Option<Vec<FrequencyItem>>,
}

impl TextStats {
    /// Empty/default text stats.
    pub fn empty() -> Self {
        Self {
            min_length: 0,
            max_length: 0,
            avg_length: 0.0,
            most_frequent: None,
            least_frequent: None,
        }
    }

    /// Build text stats from pre-computed lengths (streaming/columnar engines).
    /// Handles the `usize::MAX` sentinel for `min_length`.
    pub fn from_lengths(min_length: usize, max_length: usize, avg_length: f64) -> Self {
        Self {
            min_length: if min_length == usize::MAX {
                0
            } else {
                min_length
            },
            max_length,
            avg_length,
            most_frequent: None,
            least_frequent: None,
        }
    }
}

/// Statistics for date/datetime columns.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DateTimeStats {
    pub min_datetime: String,
    pub max_datetime: String,
    #[serde(serialize_with = "crate::serde_helpers::round_2")]
    pub duration_days: f64,
    pub year_distribution: HashMap<i32, usize>,
    pub month_distribution: HashMap<u32, usize>,
    pub day_of_week_distribution: HashMap<String, usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hour_distribution: Option<HashMap<u32, usize>>,
}

impl DateTimeStats {
    /// Empty/default datetime stats.
    pub fn empty() -> Self {
        Self {
            min_datetime: String::new(),
            max_datetime: String::new(),
            duration_days: 0.0,
            year_distribution: HashMap::new(),
            month_distribution: HashMap::new(),
            day_of_week_distribution: HashMap::new(),
            hour_distribution: None,
        }
    }
}

/// Statistics for boolean columns.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BooleanStats {
    /// Number of `true` values
    pub true_count: usize,
    /// Number of `false` values
    pub false_count: usize,
    /// Proportion of `true` among non-null values (0.0 -- 1.0)
    #[serde(serialize_with = "crate::serde_helpers::round_4")]
    pub true_ratio: f64,
}

/// Type-specific statistics for a column, determined by the inferred [`DataType`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ColumnStats {
    /// Statistics for integer and float columns
    Numeric(NumericStats),
    /// Statistics for string columns
    Text(TextStats),
    /// Statistics for date/datetime columns
    DateTime(DateTimeStats),
    /// Statistics for boolean columns
    Boolean(BooleanStats),
    /// Statistics were not computed (MetricPack::Statistics was excluded).
    None,
}

/// Semantic category for a detected pattern.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatternCategory {
    /// Email addresses, phone numbers
    Contact,
    /// UUIDs, fiscal codes, tax IDs
    Identifier,
    /// IPv4, IPv6, MAC addresses, URLs
    Network,
    /// Coordinates, postal codes
    Geographic,
    /// IBANs, credit cards, SWIFT/BIC
    Financial,
    /// Unix/Windows file paths
    FilePath,
    /// Uncategorized patterns
    Other,
}

impl std::fmt::Display for PatternCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contact => write!(f, "contact"),
            Self::Identifier => write!(f, "identifier"),
            Self::Network => write!(f, "network"),
            Self::Geographic => write!(f, "geographic"),
            Self::Financial => write!(f, "financial"),
            Self::FilePath => write!(f, "file_path"),
            Self::Other => write!(f, "other"),
        }
    }
}

/// A detected value pattern within a column (e.g. email, phone, UUID).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Pattern {
    /// Pattern name (e.g. "Email", "Phone (US)", "UUID")
    pub name: String,
    /// Regex used for detection
    pub regex: String,
    /// Number of values matching this pattern
    pub match_count: usize,
    /// Percentage of non-null values matching (0.0--100.0)
    pub match_percentage: f64,
    /// Semantic category (e.g. contact, network, financial)
    pub category: PatternCategory,
    /// Detection confidence score (0.0--1.0), derived from pattern specificity and match rate
    pub confidence: f64,
}

/// Output format for CLI and programmatic output.
#[derive(Clone, Debug)]
pub enum OutputFormat {
    /// Human-readable text output
    Text,
    /// Machine-readable JSON output
    Json,
    /// CSV format for data processing
    Csv,
    /// Plain text without formatting for scripting
    Plain,
}

// ---------------------------------------------------------------------------
// Partial analysis types (#226)
// ---------------------------------------------------------------------------

/// Result of fast schema inference — column names paired with inferred data types.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaResult {
    /// Columns with their inferred types. For CSV/Parquet the order matches
    /// the source; for JSON/JSONL columns are sorted alphabetically.
    pub columns: Vec<ColumnSchema>,
    /// How many rows were sampled to infer the schema (0 for Parquet metadata).
    pub rows_sampled: usize,
    /// Time taken for inference in milliseconds.
    pub inference_time_ms: u128,
    /// `true` when the entire file was consumed or schema was read from
    /// metadata; `false` when inference stopped at the sample-size cap and
    /// the schema may not have fully stabilized.
    pub schema_stable: bool,
}

/// A single column's name and inferred data type.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
}

/// Result of a quick row count operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RowCountEstimate {
    /// The estimated or exact row count.
    pub count: u64,
    /// Whether the count is exact or an estimate.
    pub exact: bool,
    /// How the count was obtained.
    pub method: CountMethod,
    /// Time taken in milliseconds.
    pub count_time_ms: u128,
}

/// Method used to obtain the row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CountMethod {
    /// Read from Parquet file footer metadata (exact, zero row reading).
    ParquetMetadata,
    /// Full scan of the file (exact).
    FullScan,
    /// Sample-based estimation (approximate).
    Sampling,
    /// Full scan of a streaming source (no file metadata available).
    StreamFullScan,
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Quality score calculation --

    #[test]
    fn test_empty_metrics_perfect_score() {
        let metrics = QualityMetrics::empty();
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_quality_score_weights_sum_to_100() {
        // With all dimensions at 100%, score should be 100
        let metrics = QualityMetrics::empty();
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_quality_score_completeness_weight() {
        // Zero completeness, everything else perfect
        let mut metrics = QualityMetrics::empty();
        if let Some(ref mut c) = metrics.completeness {
            c.complete_records_ratio = 0.0;
        }
        // Score drops by 30% (completeness weight)
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
        };
        assert!((metrics.overall_score() - 0.0).abs() < 0.01);
    }

    // -- JSON serialization roundtrip --

    #[test]
    fn test_column_profile_json_roundtrip() {
        let profile = ColumnProfile {
            name: "test_col".to_string(),
            data_type: DataType::Integer,
            null_count: 2,
            total_count: 10,
            unique_count: Some(8),
            stats: ColumnStats::Numeric(NumericStats {
                min: 1.0,
                max: 100.0,
                mean: 50.5,
                std_dev: 28.87,
                variance: 833.25,
                median: Some(50.0),
                quartiles: Some(Quartiles {
                    q1: 25.0,
                    q2: 50.0,
                    q3: 75.0,
                    iqr: 50.0,
                }),
                mode: Some(42.0),
                coefficient_of_variation: Some(57.17),
                skewness: Some(0.0),
                kurtosis: Some(-1.2),
                is_approximate: Some(false),
            }),
            patterns: vec![],
        };

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: ColumnProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "test_col");
        assert_eq!(deserialized.data_type, DataType::Integer);
        assert_eq!(deserialized.total_count, 10);
        assert_eq!(deserialized.null_count, 2);

        if let ColumnStats::Numeric(n) = &deserialized.stats {
            assert!((n.min - 1.0).abs() < 0.01);
            assert!((n.max - 100.0).abs() < 0.01);
            assert!((n.mean - 50.5).abs() < 0.01);
            assert!(n.median.is_some());
            assert!(n.quartiles.is_some());
        } else {
            panic!("Expected Numeric stats after roundtrip");
        }
    }

    #[test]
    fn test_text_stats_json_roundtrip() {
        let profile = ColumnProfile {
            name: "name".to_string(),
            data_type: DataType::String,
            null_count: 0,
            total_count: 3,
            unique_count: Some(3),
            stats: ColumnStats::Text(TextStats {
                min_length: 3,
                max_length: 7,
                avg_length: 5.0,
                most_frequent: None,
                least_frequent: None,
            }),
            patterns: vec![],
        };

        let json = serde_json::to_string(&profile).unwrap();
        let deserialized: ColumnProfile = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.data_type, DataType::String);
        if let ColumnStats::Text(t) = &deserialized.stats {
            assert_eq!(t.min_length, 3);
            assert_eq!(t.max_length, 7);
        } else {
            panic!("Expected Text stats after roundtrip");
        }
    }

    #[test]
    fn test_profile_report_json_roundtrip() {
        let report = ProfileReport::new(
            DataSource::File {
                path: "test.csv".to_string(),
                format: FileFormat::Csv,
                size_bytes: 1024,
                modified_at: None,
                parquet_metadata: None,
            },
            vec![],
            ExecutionMetadata::new(100, 5, 50),
            Some(QualityAssessment::exact(QualityMetrics::empty())),
        );

        let json = serde_json::to_string(&report).unwrap();
        let deserialized: ProfileReport = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.execution.rows_processed, 100);
        assert_eq!(deserialized.execution.columns_detected, 5);
        assert!((deserialized.quality_score().unwrap() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_profile_report_without_quality() {
        let report = ProfileReport::new(
            DataSource::File {
                path: "test.csv".to_string(),
                format: FileFormat::Csv,
                size_bytes: 0,
                modified_at: None,
                parquet_metadata: None,
            },
            vec![],
            ExecutionMetadata::new(0, 0, 0),
            None,
        );

        assert!(report.quality_score().is_none());
        assert!(report.quality.is_none());

        // Roundtrip: None quality should survive
        let json = serde_json::to_string(&report).unwrap();
        let deserialized: ProfileReport = serde_json::from_str(&json).unwrap();
        assert!(deserialized.quality.is_none());
    }

    // -- ExecutionMetadata --

    #[test]
    fn test_execution_metadata_throughput_calculation() {
        let meta = ExecutionMetadata::new(1000, 5, 500); // 500ms
        // 1000 rows / 0.5s = 2000 rows/sec
        assert!(meta.throughput_rows_sec.is_some());
        assert!((meta.throughput_rows_sec.unwrap() - 2000.0).abs() < 1.0);
        assert!(meta.source_exhausted);
        assert!(!meta.sampling_applied);
        assert!(meta.sampling_ratio.is_none());
    }

    #[test]
    fn test_execution_metadata_zero_time_no_throughput() {
        let meta = ExecutionMetadata::new(100, 3, 0);
        assert!(meta.throughput_rows_sec.is_none());
    }

    #[test]
    fn test_execution_metadata_with_sampling() {
        let meta = ExecutionMetadata::new(500, 3, 100).with_sampling(0.5);
        assert!(meta.sampling_applied);
        assert_eq!(meta.sampling_ratio, Some(0.5));
    }

    #[test]
    fn test_execution_metadata_with_truncation() {
        let meta =
            ExecutionMetadata::new(1000, 5, 200).with_truncation(TruncationReason::MaxRows(1000));
        assert!(!meta.source_exhausted);
        assert!(meta.truncation_reason.is_some());
    }

    #[test]
    fn test_truncation_reason_serde_roundtrip() {
        let reasons = vec![
            TruncationReason::MaxRows(5000),
            TruncationReason::MaxBytes(1_000_000),
            TruncationReason::MemoryPressure,
            TruncationReason::StopCondition("accuracy > 0.95".to_string()),
            TruncationReason::StreamClosed,
            TruncationReason::Timeout,
        ];
        for reason in reasons {
            let json = serde_json::to_string(&reason).unwrap();
            let deserialized: TruncationReason = serde_json::from_str(&json).unwrap();
            let json2 = serde_json::to_string(&deserialized).unwrap();
            assert_eq!(json, json2);
        }
    }

    // -- DataSource --

    #[test]
    fn test_data_source_file_identifier() {
        let ds = DataSource::File {
            path: "/path/to/data.csv".to_string(),
            format: FileFormat::Csv,
            size_bytes: 0,
            modified_at: None,
            parquet_metadata: None,
        };
        assert_eq!(ds.identifier(), "/path/to/data.csv");
        assert!(ds.is_file());
        assert!(!ds.is_query());
        assert!(!ds.is_dataframe());
        assert!(!ds.is_stream());
    }

    #[test]
    fn test_data_source_stream_identifier_and_helpers() {
        let ds = DataSource::Stream {
            topic: "events".to_string(),
            batch_id: "b1".to_string(),
            partition: Some(0),
            consumer_group: None,
            source_system: StreamSourceSystem::Kafka,
            session_id: None,
            first_record_at: None,
            last_record_at: None,
        };

        assert_eq!(ds.identifier(), "kafka[events]-batch:b1");
        assert!(ds.is_stream());
        assert_eq!(ds.stream_topic(), Some("events"));
        assert_eq!(ds.batch_id(), Some("b1"));
        assert!(!ds.is_file());
        assert!(!ds.is_query());
        assert!(ds.size_mb().is_none());
    }

    #[test]
    fn test_stream_json_serialization() {
        let ds = DataSource::Stream {
            topic: "sensor-data".to_string(),
            batch_id: "batch-789".to_string(),
            partition: Some(2),
            consumer_group: Some("processing-group".to_string()),
            source_system: StreamSourceSystem::Kinesis,
            session_id: Some("session-1".to_string()),
            first_record_at: Some("2023-01-01T10:00:00Z".to_string()),
            last_record_at: Some("2023-01-01T10:05:00Z".to_string()),
        };

        let json = serde_json::to_string(&ds).unwrap();
        assert!(json.contains(r#""type":"stream""#));
        assert!(json.contains(r#""source_system":"kinesis""#));
        assert!(json.contains(r#""topic":"sensor-data""#));

        let deserialized: DataSource = serde_json::from_str(&json).unwrap();
        assert!(deserialized.is_stream());
        assert_eq!(deserialized.stream_topic(), Some("sensor-data"));
    }

    #[test]
    fn test_stream_source_system_serialization_names() {
        let object_store = serde_json::to_string(&StreamSourceSystem::ObjectStore).unwrap();
        let message_queue = serde_json::to_string(&StreamSourceSystem::MessageQueue).unwrap();
        let database = serde_json::to_string(&StreamSourceSystem::Database).unwrap();

        assert_eq!(object_store, r#""object_store""#);
        assert_eq!(message_queue, r#""message_queue""#);
        assert_eq!(database, r#""database""#);

        let object_store: StreamSourceSystem = serde_json::from_str(r#""object_store""#).unwrap();
        let message_queue: StreamSourceSystem = serde_json::from_str(r#""message_queue""#).unwrap();
        let database: StreamSourceSystem = serde_json::from_str(r#""database""#).unwrap();

        assert_eq!(object_store, StreamSourceSystem::ObjectStore);
        assert_eq!(message_queue, StreamSourceSystem::MessageQueue);
        assert_eq!(database, StreamSourceSystem::Database);
    }

    // -- Partial dimension requests --

    #[test]
    fn test_partial_dimensions_only_completeness() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 100.0,
                missing_values_ratio: 0.0,
                null_columns: vec![],
            }),
            consistency: None,
            uniqueness: None,
            accuracy: None,
            timeliness: None,
        };
        assert!(metrics.completeness.is_some());
        assert!(metrics.consistency.is_none());
        assert!(metrics.uniqueness.is_none());
        assert!(metrics.accuracy.is_none());
        assert!(metrics.timeliness.is_none());
        // Score should re-normalize: 100% completeness alone → 100
        assert!((metrics.overall_score() - 100.0).abs() < 0.01);
    }

    #[test]
    fn test_partial_dimensions_two_dimensions() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics {
                complete_records_ratio: 50.0,
                ..CompletenessMetrics::default()
            }),
            consistency: None,
            uniqueness: Some(UniquenessMetrics {
                key_uniqueness: 80.0,
                ..UniquenessMetrics::default()
            }),
            accuracy: None,
            timeliness: None,
        };
        // Weights: completeness=30, uniqueness=20 → total=50
        // Score: (50*0.30 + 80*0.20) / 0.50 = (15+16)/0.50 = 62.0
        assert!((metrics.overall_score() - 62.0).abs() < 0.01);
    }

    #[test]
    fn test_all_dimensions_none_score_zero() {
        let metrics = QualityMetrics {
            completeness: None,
            consistency: None,
            uniqueness: None,
            accuracy: None,
            timeliness: None,
        };
        assert!((metrics.overall_score() - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_partial_dimensions_json_skips_none() {
        let metrics = QualityMetrics {
            completeness: Some(CompletenessMetrics::default()),
            consistency: None,
            uniqueness: None,
            accuracy: None,
            timeliness: None,
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
        let metrics = QualityMetrics {
            completeness: None,
            consistency: None,
            uniqueness: None,
            accuracy: None,
            timeliness: None,
        };
        // Flat accessors return "perfect" defaults when dimension is None
        assert!((metrics.complete_records_ratio() - 100.0).abs() < 0.01);
        assert!((metrics.data_type_consistency() - 100.0).abs() < 0.01);
        assert!((metrics.key_uniqueness() - 100.0).abs() < 0.01);
        assert!((metrics.missing_values_ratio() - 0.0).abs() < 0.01);
        assert_eq!(metrics.duplicate_rows(), 0);
        assert!(!metrics.high_cardinality_warning());
    }

    // -- MetricPack helpers --

    #[test]
    fn test_metric_pack_include_helpers_none_means_all() {
        assert!(MetricPack::include_statistics(None));
        assert!(MetricPack::include_patterns(None));
        assert!(MetricPack::include_quality(None));
    }

    #[test]
    fn test_metric_pack_include_helpers_selective() {
        let packs = vec![MetricPack::Schema, MetricPack::Quality];
        assert!(!MetricPack::include_statistics(Some(&packs)));
        assert!(!MetricPack::include_patterns(Some(&packs)));
        assert!(MetricPack::include_quality(Some(&packs)));
    }

    #[test]
    fn test_metric_pack_from_str() {
        assert_eq!(
            "statistics".parse::<MetricPack>().unwrap(),
            MetricPack::Statistics
        );
        assert_eq!(
            "QUALITY".parse::<MetricPack>().unwrap(),
            MetricPack::Quality
        );
        assert!("invalid".parse::<MetricPack>().is_err());
    }
}
