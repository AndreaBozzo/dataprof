//! Accuracy Dimension (ISO 25012)
//!
//! Measures the correctness of data values (syntactic and semantic accuracy).
//! Key metrics: outlier ratio, range violations, negative values in positive fields.

use super::utils::{calculate_percentile, validate_sample_size};
use crate::core::config::IsoQualityConfig;
use crate::types::{ColumnProfile, DataType};
use anyhow::Result;
use std::collections::HashMap;

/// Accuracy metrics container
#[derive(Debug)]
pub(crate) struct AccuracyMetrics {
    pub outlier_ratio: f64,
    pub range_violations: usize,
    pub negative_values_in_positive: usize,
}

/// Calculator for accuracy dimension metrics
pub(crate) struct AccuracyCalculator<'a> {
    thresholds: &'a IsoQualityConfig,
}

impl<'a> AccuracyCalculator<'a> {
    pub fn new(thresholds: &'a IsoQualityConfig) -> Self {
        Self { thresholds }
    }

    /// Calculate accuracy dimension metrics
    pub fn calculate(
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
                let validation = validate_sample_size(numeric_count, "outlier_detection");
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
        let q1 = calculate_percentile(&sorted, 25.0);
        let q3 = calculate_percentile(&sorted, 75.0);
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
}
