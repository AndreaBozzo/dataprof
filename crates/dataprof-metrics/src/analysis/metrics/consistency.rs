//! Consistency Dimension (ISO 8000-61)
//!
//! Measures the coherence and contradiction-free nature of data.
//! Key metrics: data type consistency, format violations, encoding issues.

use super::utils::{DATE_FORMAT_REGEXES, is_likely_date_column, is_valid_date_format};
use crate::analysis::inference::{is_null_like_token, parse_strict_boolean_token};
use crate::core::errors::DataProfilerError;
use crate::types::{ColumnProfile, DataType};
use std::collections::HashMap;

/// Consistency metrics container
#[derive(Debug)]
pub(crate) struct ConsistencyMetrics {
    pub data_type_consistency: f64,
    pub format_violations: usize,
    pub encoding_issues: usize,
    pub values_checked: usize,
}

/// Calculator for consistency dimension metrics
pub(crate) struct ConsistencyCalculator;

impl ConsistencyCalculator {
    /// Calculate consistency dimension metrics
    pub fn calculate(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<ConsistencyMetrics, DataProfilerError> {
        let (data_type_consistency, values_checked) =
            Self::calculate_type_consistency(data, column_profiles)?;
        let format_violations = Self::count_format_violations(data)?;
        let encoding_issues = Self::detect_encoding_issues(data)?;

        Ok(ConsistencyMetrics {
            data_type_consistency,
            format_violations,
            encoding_issues,
            values_checked,
        })
    }

    /// Calculate data type consistency percentage and the number of non-null
    /// values checked.
    fn calculate_type_consistency(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<(f64, usize), DataProfilerError> {
        let mut total_values = 0;
        let mut consistent_values = 0;

        for profile in column_profiles {
            if let Some(column_data) = data.get(&profile.name) {
                for value in column_data {
                    let trimmed = value.trim();
                    if is_null_like_token(trimmed) {
                        continue; // Skip null values in consistency check
                    }

                    total_values += 1;

                    // Check if value is consistent with inferred type
                    let is_consistent = match profile.data_type {
                        DataType::Integer => trimmed.parse::<i64>().is_ok(),
                        DataType::Float => trimmed.parse::<f64>().is_ok(),
                        DataType::Date => is_valid_date_format(trimmed),
                        DataType::Boolean => parse_strict_boolean_token(trimmed).is_some(),
                        DataType::String | DataType::Identifier => {
                            !is_likely_date_column(&profile.name) || is_valid_date_format(trimmed)
                        }
                    };

                    if is_consistent {
                        consistent_values += 1;
                    }
                }
            }
        }

        if total_values == 0 {
            Ok((100.0, 0))
        } else {
            Ok((
                (consistent_values as f64 / total_values as f64) * 100.0,
                total_values,
            ))
        }
    }

    /// Count format violations (malformed dates, inconsistent formats)
    fn count_format_violations(
        data: &HashMap<String, Vec<String>>,
    ) -> Result<usize, DataProfilerError> {
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
        if !is_likely_date_column(column_name) {
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

    /// Detect UTF-8 encoding issues.
    ///
    /// Counts each affected value once, even when it shows several symptoms,
    /// so the count stays comparable to `values_checked`.
    fn detect_encoding_issues(
        data: &HashMap<String, Vec<String>>,
    ) -> Result<usize, DataProfilerError> {
        let mut issues = 0;

        for values in data.values() {
            for value in values {
                // Replacement characters (�) or mojibake artifacts both
                // indicate the same defect: the value was mis-decoded.
                if value.contains('\u{FFFD}') || Self::has_encoding_artifacts(value) {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnStats;

    fn string_profile(name: &str) -> ColumnProfile {
        ColumnProfile {
            name: name.to_string(),
            data_type: DataType::String,
            null_count: 0,
            total_count: 0,
            unique_count: None,
            unique_count_is_approximate: None,
            invalid_count: None,
            stats: ColumnStats::None,
            patterns: Some(vec![]),
        }
    }

    #[test]
    fn test_encoding_issues_count_each_value_once() {
        let data = HashMap::from([(
            "name".to_string(),
            vec![
                // Both a replacement character and a mojibake artifact in
                // one value: still a single mis-decoded value.
                "Jos\u{FFFD} GarcÃ\u{AD}a".to_string(),
                "clean".to_string(),
            ],
        )]);
        let profiles = vec![string_profile("name")];

        let metrics = ConsistencyCalculator::calculate(&data, &profiles)
            .expect("consistency metrics should be computed");

        assert_eq!(metrics.encoding_issues, 1);
    }

    #[test]
    fn test_likely_date_string_column_is_not_automatically_consistent() {
        let data = HashMap::from([(
            "event_date".to_string(),
            vec![
                "2024-01-01".to_string(),
                "not-a-date".to_string(),
                "15/01/2024".to_string(),
            ],
        )]);
        let profiles = vec![string_profile("event_date")];

        let metrics = ConsistencyCalculator::calculate(&data, &profiles)
            .expect("consistency metrics should be computed");

        assert!(
            metrics.data_type_consistency < 100.0,
            "likely date columns inferred as strings should still lose consistency when values are malformed"
        );
    }
}
