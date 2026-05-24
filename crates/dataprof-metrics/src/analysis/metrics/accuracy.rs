//! Accuracy Dimension (ISO 25012)
//!
//! Measures the correctness of data values (syntactic and semantic accuracy).
//! Key metrics: outlier ratio, range violations, negative values in positive fields.

use super::utils::calculate_percentile;
use crate::analysis::inference::is_null_like_token;
use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use crate::types::{ColumnProfile, DataType};
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
    ) -> Result<AccuracyMetrics, DataProfilerError> {
        self.calculate_with_positive_columns(data, column_profiles, &[])
    }

    /// Calculate accuracy dimension metrics with explicit positive-only columns.
    pub fn calculate_with_positive_columns(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
        positive_columns: &[String],
    ) -> Result<AccuracyMetrics, DataProfilerError> {
        let outlier_ratio = self.calculate_outlier_ratio(data, column_profiles)?;
        let range_violations = Self::count_range_violations(data)?;
        let negative_values_in_positive =
            Self::count_negative_in_positive_fields(data, positive_columns)?;

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
    ) -> Result<f64, DataProfilerError> {
        let mut total_numeric_values = 0;
        let mut total_outliers = 0;

        for profile in column_profiles {
            if !matches!(profile.data_type, DataType::Integer | DataType::Float) {
                continue;
            }

            if let Some(column_data) = data.get(&profile.name) {
                // Parse once into a numeric vector; the helper below operates
                // on the pre-parsed values directly to avoid a second pass.
                let numeric_values: Vec<f64> = column_data
                    .iter()
                    .filter_map(|v| {
                        if is_null_like_token(v.trim()) {
                            None
                        } else {
                            v.parse::<f64>().ok().filter(|n| n.is_finite())
                        }
                    })
                    .collect();

                if numeric_values.len() < self.thresholds.outlier_min_samples {
                    continue;
                }

                let outlier_count = self.count_outliers_preparsed(&numeric_values);
                total_outliers += outlier_count;
                total_numeric_values += numeric_values.len();
            }
        }

        if total_numeric_values == 0 {
            Ok(0.0)
        } else {
            Ok((total_outliers as f64 / total_numeric_values as f64) * 100.0)
        }
    }

    /// Count IQR outliers in a pre-parsed numeric vector using the Tukey rule
    /// (`Q1 − k·IQR`, `Q3 + k·IQR` with configurable `k`, default 1.5).
    ///
    /// Operates on pre-parsed `f64` to avoid a second `parse::<f64>()` pass
    /// on every value — the caller already paid that cost while counting.
    fn count_outliers_preparsed(&self, numeric_values: &[f64]) -> usize {
        if numeric_values.len() < self.thresholds.outlier_min_samples {
            return 0;
        }

        let mut sorted = numeric_values.to_vec();
        // Safe total ordering: NaN/Infinity values are placed at the end (IEEE 754)
        sorted.sort_by(|a, b| a.total_cmp(b));

        let q1 = calculate_percentile(&sorted, 25.0);
        let q3 = calculate_percentile(&sorted, 75.0);
        let iqr = q3 - q1;

        let k = self.thresholds.outlier_iqr_multiplier;
        let lower_bound = q1 - k * iqr;
        let upper_bound = q3 + k * iqr;

        numeric_values
            .iter()
            .filter(|&&value| value < lower_bound || value > upper_bound)
            .count()
    }

    /// Count values outside expected ranges
    fn count_range_violations(
        data: &HashMap<String, Vec<String>>,
    ) -> Result<usize, DataProfilerError> {
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
            if is_null_like_token(value.trim()) {
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
    fn count_negative_in_positive_fields(
        data: &HashMap<String, Vec<String>>,
        positive_columns: &[String],
    ) -> Result<usize, DataProfilerError> {
        let mut violations = 0;

        for (column_name, values) in data {
            if positive_columns
                .iter()
                .any(|candidate| candidate == column_name)
            {
                violations += values
                    .iter()
                    .filter_map(|v| {
                        if is_null_like_token(v.trim()) {
                            None
                        } else {
                            v.parse::<f64>().ok()
                        }
                    })
                    .filter(|&num| num < 0.0)
                    .count();
            }
        }

        Ok(violations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnStats;

    fn numeric_profile(name: &str) -> ColumnProfile {
        ColumnProfile {
            name: name.to_string(),
            data_type: DataType::Float,
            null_count: 0,
            total_count: 0,
            unique_count: None,
            stats: ColumnStats::None,
            patterns: vec![],
        }
    }

    #[test]
    fn test_outlier_ratio_uses_iso_min_samples_not_generic_30() {
        let thresholds = IsoQualityConfig::default();
        let calc = AccuracyCalculator::new(&thresholds);
        let data = HashMap::from([(
            "temperature".to_string(),
            vec![
                "22.5".to_string(),
                "23.1".to_string(),
                "22.8".to_string(),
                "999.9".to_string(),
                "23.2".to_string(),
                "22.9".to_string(),
                "23.0".to_string(),
                "22.7".to_string(),
                "23.1".to_string(),
            ],
        )]);
        let profiles = vec![numeric_profile("temperature")];

        let metrics = calc
            .calculate(&data, &profiles)
            .expect("accuracy metrics should be computed");

        assert!(
            metrics.outlier_ratio > 0.0,
            "small numeric samples above outlier_min_samples should still detect obvious outliers"
        );
    }

    #[test]
    fn test_negative_values_require_positive_column_hint() {
        let thresholds = IsoQualityConfig::default();
        let calc = AccuracyCalculator::new(&thresholds);
        let data = HashMap::from([(
            "pressure".to_string(),
            vec![
                "101325".to_string(),
                "-500".to_string(),
                "100900".to_string(),
            ],
        )]);
        let profiles = vec![numeric_profile("pressure")];

        let without_hint = calc
            .calculate(&data, &profiles)
            .expect("accuracy metrics should compute");
        assert_eq!(without_hint.negative_values_in_positive, 0);

        let with_hint = calc
            .calculate_with_positive_columns(&data, &profiles, &["pressure".to_string()])
            .expect("accuracy metrics should compute");
        assert_eq!(with_hint.negative_values_in_positive, 1);
    }
}
