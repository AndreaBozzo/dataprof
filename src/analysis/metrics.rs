//! Data Quality Metrics Calculation Module
//!
//! This module implements comprehensive data quality metric calculations following industry standards.
//! It provides structured assessment across four key dimensions: Completeness, Consistency, Uniqueness, and Accuracy.

use crate::types::{ColumnProfile, DataQualityMetrics, DataType};
use anyhow::Result;
use regex::Regex;
use std::collections::{HashMap, HashSet};

/// Engine for calculating comprehensive data quality metrics
pub struct MetricsCalculator;

impl MetricsCalculator {
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
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<DataQualityMetrics> {
        if data.is_empty() {
            return Ok(Self::default_metrics_for_empty_dataset());
        }

        let total_rows = Self::calculate_total_rows(data)?;

        // Completeness dimension
        let completeness = Self::calculate_completeness_metrics(data, column_profiles, total_rows)?;

        // Consistency dimension
        let consistency = Self::calculate_consistency_metrics(data, column_profiles)?;

        // Uniqueness dimension
        let uniqueness = Self::calculate_uniqueness_metrics(data, column_profiles, total_rows)?;

        // Accuracy dimension
        let accuracy = Self::calculate_accuracy_metrics(data, column_profiles)?;

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

impl MetricsCalculator {
    /// Calculate completeness dimension metrics
    fn calculate_completeness_metrics(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
        total_rows: usize,
    ) -> Result<CompletenessMetrics> {
        let missing_values_ratio = Self::calculate_missing_values_ratio(data)?;
        let complete_records_ratio = Self::calculate_complete_records_ratio(data, total_rows)?;
        let null_columns = Self::identify_null_columns(column_profiles);

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

    /// Identify columns with more than 50% null values
    fn identify_null_columns(column_profiles: &[ColumnProfile]) -> Vec<String> {
        column_profiles
            .iter()
            .filter(|profile| {
                if profile.total_count == 0 {
                    false
                } else {
                    (profile.null_count as f64 / profile.total_count as f64) > 0.5
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
    fn is_valid_date_format(value: &str) -> bool {
        // Common date patterns
        let date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",     // YYYY-MM-DD
            r"^\d{2}/\d{2}/\d{4}$",     // DD/MM/YYYY or MM/DD/YYYY
            r"^\d{2}-\d{2}-\d{4}$",     // DD-MM-YYYY
            r"^\d{4}/\d{2}/\d{2}$",     // YYYY/MM/DD
            r"^\d{1,2}/\d{1,2}/\d{4}$", // M/D/YYYY or MM/DD/YYYY
            r"^\d{4}-\d{1,2}-\d{1,2}$", // YYYY-M-D
            r"^\d{1,2}-\d{1,2}-\d{4}$", // M-D-YYYY
        ];

        for pattern in &date_patterns {
            if let Ok(regex) = Regex::new(pattern) {
                if regex.is_match(value) {
                    return true;
                }
            }
        }

        false
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
    fn count_mixed_date_formats(column_name: &str, values: &[String]) -> usize {
        // Skip if column name doesn't suggest it contains dates
        if !Self::is_likely_date_column(column_name) {
            return 0;
        }

        let mut format_counts = HashMap::new();
        let date_patterns = [
            ("YYYY-MM-DD", r"^\d{4}-\d{2}-\d{2}$"),
            ("DD/MM/YYYY", r"^\d{2}/\d{2}/\d{4}$"),
            ("DD-MM-YYYY", r"^\d{2}-\d{2}-\d{4}$"),
            ("YYYY/MM/DD", r"^\d{4}/\d{2}/\d{2}$"),
        ];

        let non_empty: Vec<&String> = values.iter().filter(|s| !s.is_empty()).collect();
        let sample_size = 50.min(non_empty.len());

        for value in non_empty.iter().take(sample_size) {
            for (format_name, pattern) in &date_patterns {
                if let Ok(regex) = Regex::new(pattern) {
                    if regex.is_match(value) {
                        *format_counts.entry(*format_name).or_insert(0) += 1;
                        break;
                    }
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
        // For now, focus on number format inconsistencies
        let mut decimal_formats = HashSet::new();
        let mut violations = 0;

        for value in values {
            if value.is_empty() {
                continue;
            }

            // Check for different decimal separators
            if value.contains('.') && value.contains(',') {
                violations += 1;
            } else if value.contains('.') {
                decimal_formats.insert("dot");
            } else if value.contains(',') && value.chars().filter(|&c| c == ',').count() == 1 {
                // Single comma might be decimal separator
                decimal_formats.insert("comma");
            }
        }

        // If multiple decimal formats detected, count as violations
        if decimal_formats.len() > 1 {
            violations += values.len() / 10; // Estimate violations
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
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
        total_rows: usize,
    ) -> Result<UniquenessMetrics> {
        let duplicate_rows = Self::count_exact_duplicate_rows(data)?;
        let key_uniqueness = Self::calculate_key_uniqueness(column_profiles)?;
        let high_cardinality_warning = Self::check_high_cardinality(column_profiles, total_rows);

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
    fn check_high_cardinality(column_profiles: &[ColumnProfile], total_rows: usize) -> bool {
        if total_rows == 0 {
            return false;
        }

        column_profiles.iter().any(|profile| {
            if let Some(unique_count) = profile.unique_count {
                let cardinality_ratio = unique_count as f64 / total_rows as f64;
                // Flag if a column has >95% unique values (excluding obvious ID columns)
                cardinality_ratio > 0.95 && !Self::is_likely_id_column(&profile.name)
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
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<AccuracyMetrics> {
        let outlier_ratio = Self::calculate_outlier_ratio(data, column_profiles)?;
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
                let outliers = Self::detect_outliers_in_column(column_data);
                total_outliers += outliers.len();
                total_numeric_values += column_data
                    .iter()
                    .filter(|v| !v.is_empty() && v.parse::<f64>().is_ok())
                    .count();
            }
        }

        if total_numeric_values == 0 {
            Ok(0.0)
        } else {
            Ok((total_outliers as f64 / total_numeric_values as f64) * 100.0)
        }
    }

    /// Detect outliers in a numeric column using IQR method
    fn detect_outliers_in_column(values: &[String]) -> Vec<f64> {
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

        if numeric_values.len() < 4 {
            return vec![]; // Need at least 4 values for IQR
        }

        let mut sorted = numeric_values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let q1_idx = sorted.len() / 4;
        let q3_idx = (sorted.len() * 3) / 4;
        let q1 = sorted[q1_idx];
        let q3 = sorted[q3_idx];
        let iqr = q3 - q1;

        let lower_bound = q1 - 1.5 * iqr;
        let upper_bound = q3 + 1.5 * iqr;

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
