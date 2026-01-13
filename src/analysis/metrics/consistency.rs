//! Consistency Dimension (ISO 8000-61)
//!
//! Measures the coherence and contradiction-free nature of data.
//! Key metrics: data type consistency, format violations, encoding issues.

use super::utils::{DATE_FORMAT_REGEXES, is_likely_date_column, is_valid_date_format};
use crate::types::{ColumnProfile, DataType};
use anyhow::Result;
use std::collections::HashMap;

/// Consistency metrics container
#[derive(Debug)]
pub(crate) struct ConsistencyMetrics {
    pub data_type_consistency: f64,
    pub format_violations: usize,
    pub encoding_issues: usize,
}

/// Calculator for consistency dimension metrics
pub(crate) struct ConsistencyCalculator;

impl ConsistencyCalculator {
    /// Calculate consistency dimension metrics
    pub fn calculate(
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
                        DataType::Date => is_valid_date_format(value),
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
}
