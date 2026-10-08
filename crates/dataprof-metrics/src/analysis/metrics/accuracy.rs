//! Accuracy Dimension (ISO 25012)
//!
//! Measures the correctness of data values (syntactic and semantic accuracy).
//! Key metrics: outlier ratio, range violations, negative values in positive fields.

use super::utils::{calculate_percentile, has_identifier_word};
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
    pub numeric_values_checked: usize,
    /// Values outside the Tukey fences: the count behind `outlier_ratio`.
    pub outliers: usize,
    /// Numeric values in columns with enough of them for an outlier test:
    /// the denominator of `outlier_ratio`.
    pub outlier_values_checked: usize,
}

/// Which name-based range rules apply to a column.
///
/// A rule applies when a whole word of the name, split as identifier names are,
/// is one of the rule's words: `age` and `customer_age` are held to ages, while
/// `average_price`, `mileage` and `page_views` are not; `conversion_rate` is a
/// percentage, `migrated_rows` is not; `item_count` must be non-negative,
/// `discount` and `account_number` need not be (#871). Concatenated lowercase
/// names such as `itemcount` no longer match. `years` is not a year word, since
/// a plural names a duration (`years_of_service`), not a calendar year.
///
/// The counter and the score bounds both read this, so they cannot disagree on
/// which rules a column is under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeRules {
    age: bool,
    percent: bool,
    count: bool,
    year: bool,
}

impl RangeRules {
    fn for_column(column_name: &str) -> Self {
        Self {
            age: has_identifier_word(column_name, &["age"]),
            percent: has_identifier_word(column_name, &["percent", "percentage", "rate", "rates"]),
            count: has_identifier_word(column_name, &["count", "counts"]),
            year: has_identifier_word(column_name, &["year"]),
        }
    }
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
        let (outliers, outlier_values_checked) = self.count_outliers(data, column_profiles)?;
        let outlier_ratio = if outlier_values_checked == 0 {
            0.0
        } else {
            (outliers as f64 / outlier_values_checked as f64) * 100.0
        };
        let (range_violations, numeric_values_checked) = Self::count_range_violations(data)?;
        let negative_values_in_positive =
            Self::count_negative_in_positive_fields(data, positive_columns)?;

        Ok(AccuracyMetrics {
            outlier_ratio,
            range_violations,
            negative_values_in_positive,
            numeric_values_checked,
            outliers,
            outlier_values_checked,
        })
    }

    /// The most range violations and negative-in-positive findings a single
    /// value of `column_name` can add, since one value can break several of
    /// the name-based rules at once.
    pub fn max_violations_per_value(column_name: &str, positive_columns: &[String]) -> usize {
        let range = RangeRules::for_column(column_name);
        let rules = [
            range.age,
            range.percent,
            range.count,
            range.year,
            positive_columns
                .iter()
                .any(|candidate| candidate == column_name),
        ];
        rules.into_iter().filter(|matched| *matched).count()
    }

    /// Count statistical outliers and the numeric values they were drawn from.
    fn count_outliers(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<(usize, usize), DataProfilerError> {
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
                            v.trim().parse::<f64>().ok().filter(|n| n.is_finite())
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

        Ok((total_outliers, total_numeric_values))
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

    /// Count values outside expected ranges; returns `(violations, finite
    /// numeric values seen across all columns)`. The second value is the
    /// denominator that makes the violation counts interpretable and marks
    /// the accuracy dimension as assessable.
    fn count_range_violations(
        data: &HashMap<String, Vec<String>>,
    ) -> Result<(usize, usize), DataProfilerError> {
        let mut violations = 0;
        let mut numeric_values_checked = 0;

        for (column_name, values) in data {
            let (column_violations, column_numeric) =
                Self::check_domain_specific_ranges(column_name, values);
            violations += column_violations;
            numeric_values_checked += column_numeric;
        }

        Ok((violations, numeric_values_checked))
    }

    /// Check domain-specific range violations; returns `(violations, finite
    /// numeric values seen)`.
    fn check_domain_specific_ranges(column_name: &str, values: &[String]) -> (usize, usize) {
        let rules = RangeRules::for_column(column_name);
        let mut violations = 0;
        let mut numeric_values = 0;

        for value in values {
            if is_null_like_token(value.trim()) {
                continue;
            }

            if let Ok(num_value) = value.trim().parse::<f64>() {
                if !num_value.is_finite() {
                    continue;
                }
                numeric_values += 1;

                // Age should be reasonable (0-150)
                if rules.age && !(0.0..=150.0).contains(&num_value) {
                    violations += 1;
                }

                // Percentage should be 0-100
                if rules.percent && !(0.0..=100.0).contains(&num_value) {
                    violations += 1;
                }

                // Counts should be non-negative
                if rules.count && num_value < 0.0 {
                    violations += 1;
                }

                // Years should be reasonable (1900-2100)
                if rules.year && !(1900.0..=2100.0).contains(&num_value) {
                    violations += 1;
                }
            }
        }

        (violations, numeric_values)
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
                            v.trim().parse::<f64>().ok().filter(|n| n.is_finite())
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
            unique_count_is_approximate: None,
            unique_count_lower_bound: None,
            invalid_count: None,
            type_homogeneity: None,
            locale_number_count: None,
            stats: ColumnStats::None,
            patterns: Some(vec![]),
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
    fn range_rules_match_words_not_substrings() {
        let rules = |age, percent, count, year| RangeRules {
            age,
            percent,
            count,
            year,
        };
        for (name, expected) in [
            ("age", rules(true, false, false, false)),
            ("customer_age", rules(true, false, false, false)),
            ("customerAge", rules(true, false, false, false)),
            ("conversion_rate", rules(false, true, false, false)),
            ("conversion_rates", rules(false, true, false, false)),
            ("discount_percentage", rules(false, true, false, false)),
            ("item_count", rules(false, false, true, false)),
            ("item_counts", rules(false, false, true, false)),
            ("birth_year", rules(false, false, false, true)),
            ("average_price", rules(false, false, false, false)),
            ("mileage", rules(false, false, false, false)),
            ("page_views", rules(false, false, false, false)),
            ("migrated_rows", rules(false, false, false, false)),
            ("generated_tokens", rules(false, false, false, false)),
            ("discount", rules(false, false, false, false)),
            ("account_number", rules(false, false, false, false)),
            ("years_of_service", rules(false, false, false, false)),
        ] {
            assert_eq!(RangeRules::for_column(name), expected, "{name}");
        }
    }

    /// Each rule counts exactly the values outside its range, bounds included
    /// in the range, and a name under no rule counts none.
    #[test]
    fn each_range_rule_counts_only_values_outside_its_range() {
        for (name, values, expected) in [
            ("customer_age", ["0", "150", "30", "-1"], 1),
            ("conversion_rate", ["0", "100", "50", "101"], 1),
            ("item_count", ["0", "5", "-1", "-2"], 2),
            ("birth_year", ["1900", "2100", "1990", "1899"], 1),
            ("price", ["-1", "200", "3000", "101"], 0),
        ] {
            let values: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            assert_eq!(
                AccuracyCalculator::check_domain_specific_ranges(name, &values),
                (expected, 4),
                "{name}"
            );
        }
    }

    /// The score bounds allow an unseen value as many violations as the rules
    /// its column is under, so they must read the name as the counter does.
    #[test]
    fn max_violations_per_value_reads_the_name_by_words() {
        for (name, expected) in [
            ("average_price", 0),
            ("mileage", 0),
            ("discount", 0),
            ("customer_age", 1),
            ("item_count", 1),
        ] {
            assert_eq!(
                AccuracyCalculator::max_violations_per_value(name, &[]),
                expected,
                "{name}"
            );
        }
        assert_eq!(
            AccuracyCalculator::max_violations_per_value("mileage", &["mileage".to_string()]),
            1
        );
    }

    #[test]
    fn test_negative_values_require_positive_column_hint() {
        let thresholds = IsoQualityConfig::default();
        let calc = AccuracyCalculator::new(&thresholds);
        let data = HashMap::from([(
            "pressure".to_string(),
            vec![
                " 101325".to_string(),
                " -500 ".to_string(),
                "100900 ".to_string(),
                " -inf ".to_string(),
                "NaN".to_string(),
                "inf".to_string(),
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
        assert_eq!(with_hint.numeric_values_checked, 3);
    }
}
