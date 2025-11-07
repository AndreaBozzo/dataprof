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

use crate::core::config::IsoQualityThresholds;
use crate::types::{ColumnProfile, DataQualityMetrics, DataType};
use anyhow::Result;
use chrono::Datelike;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

// Pre-compile date validation regex patterns for better performance
static DATE_VALIDATION_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| vec![
        Regex::new(r"^\d{4}-\d{2}-\d{2}$")
            .expect("BUG: Invalid hardcoded regex for date validation YYYY-MM-DD"),
        Regex::new(r"^\d{2}/\d{2}/\d{4}$")
            .expect("BUG: Invalid hardcoded regex for date validation DD/MM/YYYY"),
        Regex::new(r"^\d{2}-\d{2}-\d{4}$")
            .expect("BUG: Invalid hardcoded regex for date validation DD-MM-YYYY"),
        Regex::new(r"^\d{4}/\d{2}/\d{2}$")
            .expect("BUG: Invalid hardcoded regex for date validation YYYY/MM/DD"),
        Regex::new(r"^\d{1,2}/\d{1,2}/\d{4}$")
            .expect("BUG: Invalid hardcoded regex for date validation M/D/YYYY"),
        Regex::new(r"^\d{4}-\d{1,2}-\d{1,2}$")
            .expect("BUG: Invalid hardcoded regex for date validation YYYY-M-D"),
        Regex::new(r"^\d{1,2}-\d{1,2}-\d{4}$")
            .expect("BUG: Invalid hardcoded regex for date validation M-D-YYYY"),
    ]);

static DATE_FORMAT_REGEXES: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| vec![
        (
            "YYYY-MM-DD",
            Regex::new(r"^\d{4}-\d{2}-\d{2}$")
                .expect("BUG: Invalid hardcoded regex for format YYYY-MM-DD"),
        ),
        (
            "DD/MM/YYYY",
            Regex::new(r"^\d{2}/\d{2}/\d{4}$")
                .expect("BUG: Invalid hardcoded regex for format DD/MM/YYYY"),
        ),
        (
            "DD-MM-YYYY",
            Regex::new(r"^\d{2}-\d{2}-\d{4}$")
                .expect("BUG: Invalid hardcoded regex for format DD-MM-YYYY"),
        ),
        (
            "YYYY/MM/DD",
            Regex::new(r"^\d{4}/\d{2}/\d{2}$")
                .expect("BUG: Invalid hardcoded regex for format YYYY/MM/DD"),
        ),
    ]);

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
}

impl MetricsCalculator {
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
        // Minimum sample sizes based on statistical theory
        let min_sample = match metric_type {
            "outlier_detection" => 30, // Central Limit Theorem threshold
            "distribution_analysis" => 100,
            "pattern_detection" => 50,
            "general" => 10,
            _ => 10,
        };

        StatisticalValidation {
            sufficient_sample: sample_size >= min_sample,
            min_sample_size: min_sample,
            actual_sample_size: sample_size,
            confidence_level: if sample_size >= min_sample { 0.95 } else { 0.0 },
        }
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
        let completeness =
            self.calculate_completeness_metrics(data, column_profiles, total_rows)?;

        // Consistency dimension
        let consistency = Self::calculate_consistency_metrics(data, column_profiles)?;

        // Uniqueness dimension
        let uniqueness = self.calculate_uniqueness_metrics(data, column_profiles, total_rows)?;

        // Accuracy dimension
        let accuracy = self.calculate_accuracy_metrics(data, column_profiles)?;

        // Timeliness dimension
        let timeliness = self.calculate_timeliness_metrics(data, column_profiles)?;

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

/// Completeness metrics container
#[derive(Debug)]
struct CompletenessMetrics {
    missing_values_ratio: f64,
    complete_records_ratio: f64,
    null_columns: Vec<String>,
}

/// Consistency metrics container
#[derive(Debug)]
struct ConsistencyMetrics {
    data_type_consistency: f64,
    format_violations: usize,
    encoding_issues: usize,
}

/// Uniqueness metrics container
#[derive(Debug)]
struct UniquenessMetrics {
    duplicate_rows: usize,
    key_uniqueness: f64,
    high_cardinality_warning: bool,
}

/// Accuracy metrics container
#[derive(Debug)]
struct AccuracyMetrics {
    outlier_ratio: f64,
    range_violations: usize,
    negative_values_in_positive: usize,
}

/// Statistical validation result
#[derive(Debug)]
pub struct StatisticalValidation {
    /// Whether the sample size is sufficient for the metric
    pub sufficient_sample: bool,
    /// Minimum recommended sample size
    pub min_sample_size: usize,
    /// Actual sample size
    pub actual_sample_size: usize,
    /// Confidence level (e.g., 0.95 for 95%)
    pub confidence_level: f64,
}

/// Timeliness metrics container (ISO 8000-8)
#[derive(Debug)]
struct TimelinessMetrics {
    future_dates_count: usize,
    stale_data_ratio: f64,
    temporal_violations: usize,
}

impl MetricsCalculator {
    /// Calculate completeness dimension metrics
    fn calculate_completeness_metrics(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
        total_rows: usize,
    ) -> Result<CompletenessMetrics> {
        let missing_values_ratio = Self::calculate_missing_values_ratio(data)?;
        let complete_records_ratio = Self::calculate_complete_records_ratio(data, total_rows)?;
        let null_columns = self.identify_null_columns(column_profiles);

        Ok(CompletenessMetrics {
            missing_values_ratio,
            complete_records_ratio,
            null_columns,
        })
    }

    /// Calculate percentage of missing values across all cells
    fn calculate_missing_values_ratio(data: &HashMap<String, Vec<String>>) -> Result<f64> {
        let total_cells: usize = data.values().map(|v| v.len()).sum();
        let null_cells: usize = data
            .values()
            .map(|v| v.iter().filter(|s| s.is_empty()).count())
            .sum();

        if total_cells == 0 {
            Ok(0.0)
        } else {
            Ok((null_cells as f64 / total_cells as f64) * 100.0)
        }
    }

    /// Calculate percentage of rows with no null values
    fn calculate_complete_records_ratio(
        data: &HashMap<String, Vec<String>>,
        total_rows: usize,
    ) -> Result<f64> {
        if total_rows == 0 {
            return Ok(100.0);
        }

        let mut complete_rows = 0;
        for row_idx in 0..total_rows {
            let is_complete = data
                .values()
                .all(|column| column.get(row_idx).is_some_and(|v| !v.is_empty()));
            if is_complete {
                complete_rows += 1;
            }
        }

        Ok((complete_rows as f64 / total_rows as f64) * 100.0)
    }

    /// Identify columns with null percentage above ISO threshold
    /// Uses configurable max_null_percentage from ISO thresholds (default: 50%)
    fn identify_null_columns(&self, column_profiles: &[ColumnProfile]) -> Vec<String> {
        let threshold = self.thresholds.max_null_percentage / 100.0;

        column_profiles
            .iter()
            .filter(|profile| {
                if profile.total_count == 0 {
                    false
                } else {
                    (profile.null_count as f64 / profile.total_count as f64) > threshold
                }
            })
            .map(|profile| profile.name.clone())
            .collect()
    }

    /// Calculate consistency dimension metrics
    fn calculate_consistency_metrics(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<ConsistencyMetrics> {
        let data_type_consistency = Self::calculate_type_consistency(data, column_profiles)?;
        let format_violations = Self::count_format_violations(data)?;
        let encoding_issues = Self::detect_encoding_issues(data)?;

        Ok(ConsistencyMetrics {
            data_type_consistency,
            format_violations,
            encoding_issues,
        })
    }

    /// Calculate data type consistency percentage
    fn calculate_type_consistency(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<f64> {
        let mut total_values = 0;
        let mut consistent_values = 0;

        for profile in column_profiles {
            if let Some(column_data) = data.get(&profile.name) {
                for value in column_data {
                    if value.is_empty() {
                        continue; // Skip null values in consistency check
                    }

                    total_values += 1;

                    // Check if value is consistent with inferred type
                    let is_consistent = match profile.data_type {
                        DataType::Integer => value.parse::<i64>().is_ok(),
                        DataType::Float => value.parse::<f64>().is_ok(),
                        DataType::Date => Self::is_valid_date_format(value),
                        DataType::String => true, // String type is always consistent
                    };

                    if is_consistent {
                        consistent_values += 1;
                    }
                }
            }
        }

        if total_values == 0 {
            Ok(100.0)
        } else {
            Ok((consistent_values as f64 / total_values as f64) * 100.0)
        }
    }

    /// Check if a string represents a valid date format
    /// Uses pre-compiled regex patterns for optimal performance
    fn is_valid_date_format(value: &str) -> bool {
        DATE_VALIDATION_REGEXES
            .iter()
            .any(|regex| regex.is_match(value))
    }

    /// Count format violations (malformed dates, inconsistent formats)
    fn count_format_violations(data: &HashMap<String, Vec<String>>) -> Result<usize> {
        let mut violations = 0;

        for (column_name, values) in data {
            // Check for mixed date formats
            violations += Self::count_mixed_date_formats(column_name, values);

            // Check for other format inconsistencies
            violations += Self::count_other_format_violations(values);
        }

        Ok(violations)
    }

    /// Count mixed date formats within a column
    /// Uses pre-compiled regex patterns for optimal performance
    fn count_mixed_date_formats(column_name: &str, values: &[String]) -> usize {
        // Skip if column name doesn't suggest it contains dates
        if !Self::is_likely_date_column(column_name) {
            return 0;
        }

        let mut format_counts = HashMap::new();

        // Use trim() for consistent whitespace handling
        let non_empty: Vec<&String> = values.iter().filter(|s| !s.trim().is_empty()).collect();

        let sample_size = 50.min(non_empty.len());

        for value in non_empty.iter().take(sample_size) {
            let trimmed = value.trim();
            for (format_name, regex) in DATE_FORMAT_REGEXES.iter() {
                if regex.is_match(trimmed) {
                    *format_counts.entry(*format_name).or_insert(0) += 1;
                    break;
                }
            }
        }

        // Return violation count if more than one format detected
        if format_counts.len() > 1 {
            format_counts.values().sum::<usize>() - format_counts.values().max().unwrap_or(&0)
        } else {
            0
        }
    }

    /// Check if column name suggests it contains dates
    fn is_likely_date_column(column_name: &str) -> bool {
        let date_indicators = [
            "date",
            "time",
            "created",
            "updated",
            "timestamp",
            "birth",
            "expiry",
        ];
        let name_lower = column_name.to_lowercase();
        date_indicators
            .iter()
            .any(|indicator| name_lower.contains(indicator))
    }

    /// Count other format violations (e.g., inconsistent number formats)
    fn count_other_format_violations(values: &[String]) -> usize {
        // Track number format inconsistencies accurately
        let mut dot_decimal_count = 0;
        let mut comma_decimal_count = 0;
        let mut violations = 0;

        for value in values {
            if value.is_empty() {
                continue;
            }

            // Check for mixed decimal separators in same value (immediate violation)
            if value.contains('.') && value.contains(',') {
                violations += 1;
                continue;
            }

            // Count decimal separator usage patterns
            if value.contains('.') {
                // Check if it's likely a decimal separator (not thousands)
                let dot_count = value.chars().filter(|&c| c == '.').count();
                if dot_count == 1 {
                    dot_decimal_count += 1;
                }
            } else if value.contains(',') {
                // Single comma might be decimal separator (European format)
                let comma_count = value.chars().filter(|&c| c == ',').count();
                if comma_count == 1 {
                    comma_decimal_count += 1;
                }
            }
        }

        // If both formats are used significantly, count the minority as violations
        if dot_decimal_count > 0 && comma_decimal_count > 0 {
            // Count the less common format as violations (indicates inconsistency)
            violations += dot_decimal_count.min(comma_decimal_count);
        }

        violations
    }

    /// Detect UTF-8 encoding issues
    fn detect_encoding_issues(data: &HashMap<String, Vec<String>>) -> Result<usize> {
        let mut issues = 0;

        for values in data.values() {
            for value in values {
                // Check for replacement characters (�) which indicate encoding issues
                if value.contains('\u{FFFD}') {
                    issues += 1;
                }

                // Check for other problematic characters
                if Self::has_encoding_artifacts(value) {
                    issues += 1;
                }
            }
        }

        Ok(issues)
    }

    /// Check for encoding artifacts in text
    fn has_encoding_artifacts(text: &str) -> bool {
        // Common encoding artifacts
        let artifacts = ["Ã¡", "Ã©", "Ã\u{AD}", "Ã³", "Ãº", "Ã±", "Ã§"];
        artifacts.iter().any(|artifact| text.contains(artifact))
    }

    /// Calculate uniqueness dimension metrics
    fn calculate_uniqueness_metrics(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
        total_rows: usize,
    ) -> Result<UniquenessMetrics> {
        let duplicate_rows = Self::count_exact_duplicate_rows(data)?;
        let key_uniqueness = Self::calculate_key_uniqueness(column_profiles)?;
        let high_cardinality_warning = self.check_high_cardinality(column_profiles, total_rows);

        Ok(UniquenessMetrics {
            duplicate_rows,
            key_uniqueness,
            high_cardinality_warning,
        })
    }

    /// Count exact duplicate rows
    fn count_exact_duplicate_rows(data: &HashMap<String, Vec<String>>) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }

        let total_rows = Self::calculate_total_rows(data)?;
        let column_names: Vec<&String> = data.keys().collect();

        let mut row_signatures = HashSet::new();
        let mut duplicates = 0;

        for row_idx in 0..total_rows {
            let row_signature: Vec<&String> = column_names
                .iter()
                .filter_map(|col_name| data.get(*col_name)?.get(row_idx))
                .collect();

            if !row_signatures.insert(row_signature) {
                duplicates += 1;
            }
        }

        Ok(duplicates)
    }

    /// Calculate key uniqueness percentage
    fn calculate_key_uniqueness(column_profiles: &[ColumnProfile]) -> Result<f64> {
        // Look for ID-like columns
        let key_column = column_profiles.iter().find(|profile| {
            let name_lower = profile.name.to_lowercase();
            name_lower.contains("id")
                || name_lower.contains("key")
                || name_lower == "pk"
                || name_lower.ends_with("_id")
        });

        if let Some(key_col) = key_column {
            if let Some(unique_count) = key_col.unique_count {
                if key_col.total_count == 0 {
                    Ok(100.0)
                } else {
                    Ok((unique_count as f64 / key_col.total_count as f64) * 100.0)
                }
            } else {
                Ok(100.0) // Assume perfect if unique_count not available
            }
        } else {
            Ok(100.0) // No key column found, assume perfect
        }
    }

    /// Check for high cardinality warning
    /// Uses configurable high_cardinality_threshold from ISO thresholds (default: 95%)
    fn check_high_cardinality(&self, column_profiles: &[ColumnProfile], total_rows: usize) -> bool {
        if total_rows == 0 {
            return false;
        }

        let threshold = self.thresholds.high_cardinality_threshold / 100.0;

        column_profiles.iter().any(|profile| {
            if let Some(unique_count) = profile.unique_count {
                let cardinality_ratio = unique_count as f64 / total_rows as f64;
                // Flag if a column exceeds threshold (excluding obvious ID columns)
                cardinality_ratio > threshold && !Self::is_likely_id_column(&profile.name)
            } else {
                false
            }
        })
    }

    /// Check if column is likely an ID column
    fn is_likely_id_column(column_name: &str) -> bool {
        let name_lower = column_name.to_lowercase();
        name_lower.contains("id")
            || name_lower.contains("key")
            || name_lower == "pk"
            || name_lower.ends_with("_id")
    }

    /// Calculate accuracy dimension metrics
    fn calculate_accuracy_metrics(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<AccuracyMetrics> {
        let outlier_ratio = self.calculate_outlier_ratio(data, column_profiles)?;
        let range_violations = Self::count_range_violations(data)?;
        let negative_values_in_positive = Self::count_negative_in_positive_fields(data)?;

        Ok(AccuracyMetrics {
            outlier_ratio,
            range_violations,
            negative_values_in_positive,
        })
    }

    /// Calculate percentage of statistical outliers
    fn calculate_outlier_ratio(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<f64> {
        let mut total_numeric_values = 0;
        let mut total_outliers = 0;

        for profile in column_profiles {
            if !matches!(profile.data_type, DataType::Integer | DataType::Float) {
                continue;
            }

            if let Some(column_data) = data.get(&profile.name) {
                let numeric_count = column_data
                    .iter()
                    .filter(|v| !v.is_empty() && v.parse::<f64>().is_ok())
                    .count();

                // Validate sample size before outlier detection
                let validation = Self::validate_sample_size(numeric_count, "outlier_detection");
                if !validation.sufficient_sample {
                    // Skip outlier detection for insufficient sample size
                    continue;
                }

                let outliers = self.detect_outliers_in_column(column_data);
                total_outliers += outliers.len();
                total_numeric_values += numeric_count;
            }
        }

        if total_numeric_values == 0 {
            Ok(0.0)
        } else {
            Ok((total_outliers as f64 / total_numeric_values as f64) * 100.0)
        }
    }

    /// Detect outliers in a numeric column using ISO 25012 compliant IQR method
    ///
    /// Uses configurable IQR multiplier (default: 1.5) from ISO thresholds.
    /// Values outside [Q1 - k*IQR, Q3 + k*IQR] are considered outliers.
    ///
    /// Uses proper percentile calculation with linear interpolation (Type 7 - Excel/R default)
    /// as per NIST Engineering Statistics Handbook.
    ///
    /// # Arguments
    /// * `values` - Column values as strings
    ///
    /// # Returns
    /// Vector of outlier values detected
    fn detect_outliers_in_column(&self, values: &[String]) -> Vec<f64> {
        let numeric_values: Vec<f64> = values
            .iter()
            .filter_map(|v| {
                if v.is_empty() {
                    None
                } else {
                    v.parse::<f64>().ok()
                }
            })
            .collect();

        // Use configurable minimum sample size (default: 4)
        if numeric_values.len() < self.thresholds.outlier_min_samples {
            return vec![]; // Insufficient data for outlier detection
        }

        let mut sorted = numeric_values.clone();
        // Safe total ordering: NaN/Infinity values are placed at the end (IEEE 754)
        sorted.sort_by(|a, b| a.total_cmp(b));

        // Calculate Q1 and Q3 using linear interpolation (percentile method Type 7)
        let q1 = Self::calculate_percentile(&sorted, 25.0);
        let q3 = Self::calculate_percentile(&sorted, 75.0);
        let iqr = q3 - q1;

        // Use configurable IQR multiplier (ISO 25012: default 1.5)
        let k = self.thresholds.outlier_iqr_multiplier;
        let lower_bound = q1 - k * iqr;
        let upper_bound = q3 + k * iqr;

        numeric_values
            .into_iter()
            .filter(|&value| value < lower_bound || value > upper_bound)
            .collect()
    }

    /// Calculate percentile using linear interpolation (Type 7 - R/Excel default)
    ///
    /// This is the scientifically correct method as per:
    /// - NIST Engineering Statistics Handbook
    /// - R default quantile method (type=7)
    /// - Excel PERCENTILE function
    ///
    /// # Arguments
    /// * `sorted_values` - Values sorted in ascending order
    /// * `percentile` - Percentile to calculate (0-100)
    ///
    /// # Returns
    /// Calculated percentile value
    fn calculate_percentile(sorted_values: &[f64], percentile: f64) -> f64 {
        let n = sorted_values.len();

        if n == 0 {
            return 0.0;
        }
        if n == 1 {
            return sorted_values[0];
        }

        // Type 7 formula: h = (N-1) * p/100 + 1
        let h = (n - 1) as f64 * (percentile / 100.0);
        let h_floor = h.floor() as usize;
        let h_ceil = h.ceil() as usize;

        // Handle edge cases
        if h_floor >= n - 1 {
            return sorted_values[n - 1];
        }

        // Linear interpolation between the two nearest values
        let lower = sorted_values[h_floor];
        let upper = sorted_values[h_ceil];
        let fraction = h - h_floor as f64;

        lower + fraction * (upper - lower)
    }

    /// Count values outside expected ranges
    fn count_range_violations(data: &HashMap<String, Vec<String>>) -> Result<usize> {
        let mut violations = 0;

        for (column_name, values) in data {
            violations += Self::check_domain_specific_ranges(column_name, values);
        }

        Ok(violations)
    }

    /// Check domain-specific range violations
    fn check_domain_specific_ranges(column_name: &str, values: &[String]) -> usize {
        let name_lower = column_name.to_lowercase();
        let mut violations = 0;

        for value in values {
            if value.is_empty() {
                continue;
            }

            if let Ok(num_value) = value.parse::<f64>() {
                // Age should be reasonable (0-150)
                if name_lower.contains("age") && !(0.0..=150.0).contains(&num_value) {
                    violations += 1;
                }

                // Percentage should be 0-100
                if (name_lower.contains("percent") || name_lower.contains("rate"))
                    && !(0.0..=100.0).contains(&num_value)
                {
                    violations += 1;
                }

                // Counts should be non-negative
                if name_lower.contains("count") && num_value < 0.0 {
                    violations += 1;
                }

                // Years should be reasonable (1900-2100)
                if name_lower.contains("year") && !(1900.0..=2100.0).contains(&num_value) {
                    violations += 1;
                }
            }
        }

        violations
    }

    /// Count negative values in positive-only fields
    fn count_negative_in_positive_fields(data: &HashMap<String, Vec<String>>) -> Result<usize> {
        let positive_field_patterns = [
            "age", "price", "cost", "amount", "count", "quantity", "salary", "revenue", "profit",
            "distance", "weight", "height", "length", "width", "area", "volume",
        ];
        let mut violations = 0;

        for (column_name, values) in data {
            let is_positive_field = positive_field_patterns
                .iter()
                .any(|pattern| column_name.to_lowercase().contains(pattern));

            if is_positive_field {
                violations += values
                    .iter()
                    .filter_map(|v| v.parse::<f64>().ok())
                    .filter(|&num| num < 0.0)
                    .count();
            }
        }

        Ok(violations)
    }

    /// Calculate timeliness dimension metrics (ISO 8000-8)
    fn calculate_timeliness_metrics(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<TimelinessMetrics> {
        let future_dates_count = Self::count_future_dates(data, column_profiles)?;
        let stale_data_ratio = self.calculate_stale_data_ratio(data, column_profiles)?;
        let temporal_violations = Self::count_temporal_violations(data)?;

        Ok(TimelinessMetrics {
            future_dates_count,
            stale_data_ratio,
            temporal_violations,
        })
    }

    /// Count dates that are in the future (beyond current date)
    fn count_future_dates(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<usize> {
        let mut future_count = 0;

        // Get current year from system time
        let current_year = chrono::Utc::now().year();

        for profile in column_profiles {
            if !matches!(profile.data_type, DataType::Date) {
                continue;
            }

            if let Some(column_data) = data.get(&profile.name) {
                for value in column_data {
                    if value.is_empty() {
                        continue;
                    }

                    // Extract year from common date formats
                    if let Some(year) = Self::extract_year(value) {
                        if year > current_year {
                            future_count += 1;
                        }
                    }
                }
            }
        }

        Ok(future_count)
    }

    /// Calculate percentage of stale data (older than threshold)
    fn calculate_stale_data_ratio(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<f64> {
        let mut total_dates = 0;
        let mut stale_dates = 0;

        // Get current year from system time
        let current_year = chrono::Utc::now().year();
        let threshold_year = current_year - self.thresholds.max_data_age_years as i32;

        for profile in column_profiles {
            if !matches!(profile.data_type, DataType::Date) {
                continue;
            }

            if let Some(column_data) = data.get(&profile.name) {
                for value in column_data {
                    if value.is_empty() {
                        continue;
                    }

                    if let Some(year) = Self::extract_year(value) {
                        total_dates += 1;
                        if year < threshold_year {
                            stale_dates += 1;
                        }
                    }
                }
            }
        }

        if total_dates == 0 {
            Ok(0.0)
        } else {
            Ok((stale_dates as f64 / total_dates as f64) * 100.0)
        }
    }

    /// Count temporal ordering violations (e.g., end_date < start_date)
    fn count_temporal_violations(data: &HashMap<String, Vec<String>>) -> Result<usize> {
        let mut violations = 0;

        // Look for column pairs like start_date/end_date, created_at/updated_at
        let temporal_pairs = [
            ("start_date", "end_date"),
            ("start", "end"),
            ("created_at", "updated_at"),
            ("created", "updated"),
            ("begin_date", "end_date"),
            ("from_date", "to_date"),
        ];

        for (start_col, end_col) in &temporal_pairs {
            let start_data = data
                .iter()
                .find(|(k, _)| k.to_lowercase().contains(start_col));
            let end_data = data
                .iter()
                .find(|(k, _)| k.to_lowercase().contains(end_col));

            if let (Some((_, start_values)), Some((_, end_values))) = (start_data, end_data) {
                for (start_val, end_val) in start_values.iter().zip(end_values.iter()) {
                    if start_val.is_empty() || end_val.is_empty() {
                        continue;
                    }

                    // Compare as sortable strings (works for ISO dates YYYY-MM-DD)
                    if start_val > end_val {
                        violations += 1;
                    }
                }
            }
        }

        Ok(violations)
    }

    /// Extract year from common date formats
    /// Supports: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, YYYY/MM/DD
    fn extract_year(date_str: &str) -> Option<i32> {
        // Try YYYY-MM-DD or YYYY/MM/DD format (year first)
        if date_str.len() >= 4 {
            if let Ok(year) = date_str[0..4].parse::<i32>() {
                if (1900..=2100).contains(&year) {
                    return Some(year);
                }
            }
        }

        // Try DD/MM/YYYY or DD-MM-YYYY format (year last)
        if date_str.len() >= 10 {
            if let Ok(year) = date_str[6..10].parse::<i32>() {
                if (1900..=2100).contains(&year) {
                    return Some(year);
                }
            }
        }

        None
    }
}
