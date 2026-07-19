//! Shared utilities for metrics calculation
//!
//! Contains regex patterns, validation helpers, and common functions
//! used across all metric dimensions.

use regex::Regex;
use std::sync::LazyLock;

use crate::stats::datetime::parse_raw_datetime_year;

// Pre-compile date validation regex patterns for better performance
pub(crate) static DATE_VALIDATION_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
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
    ]
});

pub(crate) static DATE_FORMAT_REGEXES: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| {
    vec![
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
    ]
});

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

/// Check if a string represents a valid date format
/// Uses pre-compiled regex patterns for optimal performance
pub(crate) fn is_valid_date_format(value: &str) -> bool {
    DATE_VALIDATION_REGEXES
        .iter()
        .any(|regex| regex.is_match(value))
}

/// Check if column name suggests it contains dates
pub(crate) fn is_likely_date_column(column_name: &str) -> bool {
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

/// Check if column is likely an ID column
pub(crate) fn is_likely_id_column(column_name: &str) -> bool {
    identifier_words(column_name).into_iter().any(|word| {
        word.eq_ignore_ascii_case("id")
            || word.eq_ignore_ascii_case("key")
            || word.eq_ignore_ascii_case("pk")
    })
}

/// Split snake/kebab/spaced and camel/Pascal case names into semantic words.
/// This keeps identifier inference from treating unrelated names such as
/// `paid`, `valid`, or `monkey` as keys merely because they contain `id` or
/// `key` as a substring.
fn identifier_words(column_name: &str) -> Vec<&str> {
    let mut boundaries = Vec::with_capacity(column_name.len() / 2 + 2);
    boundaries.push(0);

    let chars: Vec<(usize, char)> = column_name.char_indices().collect();
    for (index, &(byte_index, current)) in chars.iter().enumerate() {
        if !current.is_alphanumeric() {
            boundaries.push(byte_index);
            boundaries.push(byte_index + current.len_utf8());
            continue;
        }

        let previous = index
            .checked_sub(1)
            .and_then(|i| chars.get(i))
            .map(|(_, c)| *c);
        let next = chars.get(index + 1).map(|(_, c)| *c);
        let camel_boundary = current.is_uppercase() && previous.is_some_and(char::is_lowercase)
            || current.is_uppercase()
                && previous.is_some_and(char::is_uppercase)
                && next.is_some_and(char::is_lowercase);
        if camel_boundary {
            boundaries.push(byte_index);
        }
    }
    boundaries.push(column_name.len());
    boundaries.sort_unstable();
    boundaries.dedup();

    boundaries
        .windows(2)
        .filter_map(|range| column_name.get(range[0]..range[1]))
        .filter(|word| word.chars().any(char::is_alphanumeric))
        .collect()
}

/// Extract year from common date formats
/// Supports: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, YYYY/MM/DD
pub(crate) fn extract_year(date_str: &str) -> Option<i32> {
    parse_raw_datetime_year(date_str)
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
pub(crate) fn calculate_percentile(sorted_values: &[f64], percentile: f64) -> f64 {
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

#[cfg(test)]
mod tests {
    use super::is_likely_id_column;

    #[test]
    fn identifier_heuristic_matches_words_not_substrings() {
        for name in [
            "id",
            "user_id",
            "order-key",
            "primary key",
            "CustomerId",
            "APIKey",
            "pk",
        ] {
            assert!(
                is_likely_id_column(name),
                "expected {name:?} to be identifier-like"
            );
        }

        for name in ["paid", "valid", "identity", "monkey", "skid", "keyboard"] {
            assert!(
                !is_likely_id_column(name),
                "expected {name:?} not to be identifier-like"
            );
        }
    }
}
