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
//! 5. Update `IsoQualityThresholds` in `src/core/config.rs` if configurable thresholds needed
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

use crate::core::config::IsoQualityThresholds;
use crate::types::{ColumnProfile, DataQualityMetrics};
use anyhow::Result;
use std::collections::HashMap;

/// Engine for calculating comprehensive data quality metrics
/// Supports ISO 8000/25012 configurable thresholds
pub struct MetricsCalculator {
    /// ISO-compliant quality thresholds
    pub thresholds: IsoQualityThresholds,
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
            thresholds: IsoQualityThresholds::default(),
        }
    }

    /// Create a calculator with custom thresholds
    pub fn with_thresholds(thresholds: IsoQualityThresholds) -> Self {
        Self { thresholds }
    }

    /// Create a calculator with strict thresholds (finance, healthcare)
    pub fn strict() -> Self {
        Self {
            thresholds: IsoQualityThresholds::strict(),
        }
    }

    /// Create a calculator with lenient thresholds (exploratory, marketing)
    pub fn lenient() -> Self {
        Self {
            thresholds: IsoQualityThresholds::lenient(),
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
    /// * `Result<DataQualityMetrics>` - Comprehensive quality metrics or error
    ///
    /// # Errors
    /// Returns error if data is malformed or calculation fails
    pub fn calculate_comprehensive_metrics(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<DataQualityMetrics> {
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

        Ok(DataQualityMetrics {
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
    fn default_metrics_for_empty_dataset() -> DataQualityMetrics {
        DataQualityMetrics {
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

    /// Calculate total number of rows from data
    fn calculate_total_rows(data: &HashMap<String, Vec<String>>) -> Result<usize> {
        data.values()
            .next()
            .map(|v| v.len())
            .ok_or_else(|| anyhow::anyhow!("No data columns found"))
    }
}
