//! Data Quality Metrics Calculation Module
//!
//! This module implements comprehensive data quality metric calculations following industry standards.
//! It provides structured assessment across five key dimensions: Completeness, Consistency, Uniqueness, Accuracy, and Timeliness.
//!
//! ## ISO Compliance
//!
//! This module follows:
//! - **ISO 8000-8**: Data Quality (Completeness, Timeliness)
//! - **ISO 8000-61**: Master Data Quality (Consistency)
//! - **ISO 8000-110**: Duplicate Detection (Uniqueness)
//! - **ISO 25012**: Data Quality Model (Accuracy)
//!
//! ## TODO: Future ISO 25012 Dimensions
//!
//! ISO/IEC 25012 defines 15 data quality characteristics. We currently implement 5.
//! Below are candidates for future implementation:
//!
//! ### Inherent Data Quality (content-focused)
//! - **Credibility**: Trustworthiness and believability of data origin
//!   - Impl: Track data source metadata, provenance scoring
//! - **Currentness**: Already partially covered by Timeliness
//!
//! ### System-Dependent Data Quality
//! - **Accessibility**: Data retrievability when needed
//!   - Impl: Response time metrics, availability checks
//! - **Portability**: Transferability across data models/platforms
//!   - Impl: Schema compatibility scoring, format conversion success rate
//! - **Recoverability**: Data integrity during system failures
//!   - Impl: Backup validation, checksum verification
//!
//! ### Additional Dimensions (easy wins)
//! - **Validity**: Conformance to domain-specific rules
//!   - Impl: Custom rule engine, regex pattern validation
//!   - Example: Email format, phone format, business rules
//! - **Precision**: Level of detail/decimal places
//!   - Impl: Decimal place analysis, significant figures check
//!
//! ### Research-Proposed Dimensions (MDPI 2024-2025)
//! - **Governance**: Data ownership and responsibility tracking
//! - **Usefulness**: Practical utility scoring for intended purpose
//! - **Semantics**: Meaning clarity and interpretability metrics
//!
//! ### How to Add a New Dimension
//! 1. Create `metrics/{dimension}.rs` with `{Dimension}Metrics` struct + `{Dimension}Calculator`
//! 2. Add `mod {dimension};` and `use {dimension}::...;` in this file
//! 3. Extend `DataQualityMetrics` in `src/types.rs` with new fields
//! 4. Call the calculator in `calculate_comprehensive_metrics()` and map fields
//! 5. Update `IsoQualityConfig` in `src/core/config.rs` if configurable thresholds needed
//!
//! ### References
//! - ISO 25012: <https://iso25000.com/index.php/en/iso-25000-standards/iso-25012>
//! - ISO 8000 Wikipedia: <https://en.wikipedia.org/wiki/ISO_8000>
//! - Data Quality 2025 Guide: <https://www.ewsolutions.com/data-quality-quide/>
//! - MDPI Framework Review: <https://www.mdpi.com/2504-2289/9/4/93>

mod accuracy;
mod completeness;
mod consistency;
mod timeliness;
mod uniqueness;
mod utils;

// Re-export public types for backward compatibility
pub use utils::{StatisticalValidation, validate_sample_size};

use accuracy::AccuracyCalculator;
use completeness::CompletenessCalculator;
use consistency::ConsistencyCalculator;
use timeliness::TimelinessCalculator;
use uniqueness::UniquenessCalculator;

use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use crate::types::{ColumnProfile, QualityMetrics};
use std::collections::HashMap;

/// Engine for calculating comprehensive data quality metrics
/// Supports ISO 8000/25012 configurable thresholds
pub struct MetricsCalculator {
    /// ISO-compliant quality thresholds
    pub thresholds: IsoQualityConfig,
}

impl Default for MetricsCalculator {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCalculator {
    /// Create a new calculator with default ISO thresholds
    pub fn new() -> Self {
        Self {
            thresholds: IsoQualityConfig::default(),
        }
    }

    /// Create a calculator with custom thresholds
    pub fn with_thresholds(thresholds: IsoQualityConfig) -> Self {
        Self { thresholds }
    }

    /// Create a calculator with strict thresholds (finance, healthcare)
    pub fn strict() -> Self {
        Self {
            thresholds: IsoQualityConfig::strict(),
        }
    }

    /// Create a calculator with lenient thresholds (exploratory, marketing)
    pub fn lenient() -> Self {
        Self {
            thresholds: IsoQualityConfig::lenient(),
        }
    }

    /// Validate statistical requirements for metric calculation
    ///
    /// Checks if the dataset has sufficient sample size for reliable metrics.
    /// Based on central limit theorem and statistical best practices.
    ///
    /// # Arguments
    /// * `sample_size` - Actual number of observations
    /// * `metric_type` - Type of metric being calculated
    ///
    /// # Returns
    /// StatisticalValidation with sufficiency status and recommendations
    pub fn validate_sample_size(sample_size: usize, metric_type: &str) -> StatisticalValidation {
        utils::validate_sample_size(sample_size, metric_type)
    }

    /// Calculate comprehensive data quality metrics from column data
    ///
    /// # Arguments
    /// * `data` - HashMap containing column names and their values
    /// * `column_profiles` - Vector of analyzed column profiles
    ///
    /// # Returns
    /// * `Result<QualityMetrics>` - Comprehensive quality metrics or error
    ///
    /// # Errors
    /// Returns error if data is malformed or calculation fails
    pub fn calculate_comprehensive_metrics(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<QualityMetrics, DataProfilerError> {
        if data.is_empty() {
            return Ok(Self::default_metrics_for_empty_dataset());
        }

        let total_rows = Self::calculate_total_rows(data)?;

        // Validate sample size for statistical reliability
        let validation = Self::validate_sample_size(total_rows, "general");
        if !validation.sufficient_sample {
            eprintln!(
                "Warning: Sample size ({}) is below recommended minimum ({}) for reliable statistics",
                validation.actual_sample_size, validation.min_sample_size
            );
        }

        // Completeness dimension
        let completeness = CompletenessCalculator::new(&self.thresholds).calculate(
            data,
            column_profiles,
            total_rows,
        )?;

        // Consistency dimension
        let consistency = ConsistencyCalculator::calculate(data, column_profiles)?;

        // Uniqueness dimension
        let uniqueness = UniquenessCalculator::new(&self.thresholds).calculate(
            data,
            column_profiles,
            total_rows,
        )?;

        // Accuracy dimension
        let accuracy =
            AccuracyCalculator::new(&self.thresholds).calculate(data, column_profiles)?;

        // Timeliness dimension
        let timeliness =
            TimelinessCalculator::new(&self.thresholds).calculate(data, column_profiles)?;

        Ok(QualityMetrics {
            missing_values_ratio: completeness.missing_values_ratio,
            complete_records_ratio: completeness.complete_records_ratio,
            null_columns: completeness.null_columns,
            data_type_consistency: consistency.data_type_consistency,
            format_violations: consistency.format_violations,
            encoding_issues: consistency.encoding_issues,
            duplicate_rows: uniqueness.duplicate_rows,
            key_uniqueness: uniqueness.key_uniqueness,
            high_cardinality_warning: uniqueness.high_cardinality_warning,
            outlier_ratio: accuracy.outlier_ratio,
            range_violations: accuracy.range_violations,
            negative_values_in_positive: accuracy.negative_values_in_positive,
            future_dates_count: timeliness.future_dates_count,
            stale_data_ratio: timeliness.stale_data_ratio,
            temporal_violations: timeliness.temporal_violations,
        })
    }

    /// Create default metrics for empty dataset
    fn default_metrics_for_empty_dataset() -> QualityMetrics {
        QualityMetrics {
            missing_values_ratio: 0.0,
            complete_records_ratio: 100.0,
            null_columns: vec![],
            data_type_consistency: 100.0,
            format_violations: 0,
            encoding_issues: 0,
            duplicate_rows: 0,
            key_uniqueness: 100.0,
            high_cardinality_warning: false,
            outlier_ratio: 0.0,
            range_violations: 0,
            negative_values_in_positive: 0,
            future_dates_count: 0,
            stale_data_ratio: 0.0,
            temporal_violations: 0,
        }
    }

    /// Calculate quality metrics with bifurcated computation for streaming.
    ///
    /// **Phase A (exact from global counters)**: Completeness metrics are computed
    /// from `ColumnProfile` stats (`null_count`, `total_count`) which are exact
    /// even for infinite streams. Key uniqueness already uses `ColumnProfile`.
    ///
    /// **Phase B (sampled)**: Consistency, Accuracy, Timeliness, and duplicate_rows
    /// are computed from the bounded reservoir sample.
    ///
    /// Returns a [`BifurcatedResult`] containing the metrics plus provenance
    /// for which dimensions are exact vs sampled.
    pub fn calculate_bifurcated_metrics(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<BifurcatedResult, DataProfilerError> {
        if data.is_empty() && column_profiles.is_empty() {
            return Ok(BifurcatedResult {
                metrics: Self::default_metrics_for_empty_dataset(),
                exact_dimensions: vec![],
                sampled_dimensions: vec![],
                sample_size: 0,
            });
        }

        let total_rows = column_profiles.first().map(|p| p.total_count).unwrap_or(0);

        // Phase A: Completeness from exact global counters
        let completeness = CompletenessCalculator::new(&self.thresholds)
            .calculate_from_profiles(column_profiles)?;

        // Phase B: Sampled dimensions from reservoir data
        let sample_rows = Self::calculate_total_rows(data).unwrap_or(0);

        let consistency = if !data.is_empty() {
            ConsistencyCalculator::calculate(data, column_profiles)?
        } else {
            consistency::ConsistencyMetrics {
                data_type_consistency: 100.0,
                format_violations: 0,
                encoding_issues: 0,
            }
        };

        let uniqueness = UniquenessCalculator::new(&self.thresholds).calculate(
            data,
            column_profiles,
            total_rows,
        )?;

        let accuracy = if !data.is_empty() {
            AccuracyCalculator::new(&self.thresholds).calculate(data, column_profiles)?
        } else {
            accuracy::AccuracyMetrics {
                outlier_ratio: 0.0,
                range_violations: 0,
                negative_values_in_positive: 0,
            }
        };

        let timeliness = if !data.is_empty() {
            TimelinessCalculator::new(&self.thresholds).calculate(data, column_profiles)?
        } else {
            timeliness::TimelinessMetrics {
                future_dates_count: 0,
                stale_data_ratio: 0.0,
                temporal_violations: 0,
            }
        };

        let metrics = QualityMetrics {
            missing_values_ratio: completeness.missing_values_ratio,
            complete_records_ratio: completeness.complete_records_ratio,
            null_columns: completeness.null_columns,
            data_type_consistency: consistency.data_type_consistency,
            format_violations: consistency.format_violations,
            encoding_issues: consistency.encoding_issues,
            duplicate_rows: uniqueness.duplicate_rows,
            key_uniqueness: uniqueness.key_uniqueness,
            high_cardinality_warning: uniqueness.high_cardinality_warning,
            outlier_ratio: accuracy.outlier_ratio,
            range_violations: accuracy.range_violations,
            negative_values_in_positive: accuracy.negative_values_in_positive,
            future_dates_count: timeliness.future_dates_count,
            stale_data_ratio: timeliness.stale_data_ratio,
            temporal_violations: timeliness.temporal_violations,
        };

        Ok(BifurcatedResult {
            metrics,
            exact_dimensions: vec!["completeness".to_string(), "key_uniqueness".to_string()],
            sampled_dimensions: vec![
                "consistency".to_string(),
                "accuracy".to_string(),
                "timeliness".to_string(),
                "duplicate_rows".to_string(),
            ],
            sample_size: sample_rows,
        })
    }

    /// Calculate total number of rows from data
    fn calculate_total_rows(
        data: &HashMap<String, Vec<String>>,
    ) -> Result<usize, DataProfilerError> {
        data.values().next().map(|v| v.len()).ok_or_else(|| {
            DataProfilerError::MetricsCalculationError {
                message: "No data columns found".to_string(),
            }
        })
    }
}

/// Result of bifurcated quality metric calculation.
///
/// Contains the computed metrics plus provenance information about which
/// dimensions were computed exactly (from global streaming counters) and
/// which were computed from a bounded sample.
pub struct BifurcatedResult {
    /// The computed quality metrics
    pub metrics: QualityMetrics,
    /// Dimensions computed from exact global counters (e.g., "completeness", "key_uniqueness")
    pub exact_dimensions: Vec<String>,
    /// Dimensions computed from the bounded reservoir sample (e.g., "consistency", "accuracy")
    pub sampled_dimensions: Vec<String>,
    /// Number of sample rows used for Phase B dimensions
    pub sample_size: usize,
}
