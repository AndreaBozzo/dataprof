//! Completeness Dimension (ISO 8000-8)
//!
//! Measures the presence of all required data elements.
//! Key metrics: missing values ratio, complete records ratio, null columns.

use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use crate::types::ColumnProfile;
use std::collections::HashMap;

/// Completeness metrics container
#[derive(Debug)]
pub(crate) struct CompletenessMetrics {
    pub missing_values_ratio: f64,
    pub complete_records_ratio: f64,
    pub null_columns: Vec<String>,
}

/// Calculator for completeness dimension metrics
pub(crate) struct CompletenessCalculator<'a> {
    thresholds: &'a IsoQualityConfig,
}

impl<'a> CompletenessCalculator<'a> {
    pub fn new(thresholds: &'a IsoQualityConfig) -> Self {
        Self { thresholds }
    }

    /// Calculate completeness dimension metrics
    pub fn calculate(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
        total_rows: usize,
    ) -> Result<CompletenessMetrics, DataProfilerError> {
        // Prefer ColumnProfile counters for missing_values_ratio when available,
        // because the reservoir sample does not include empty/null values.
        let missing_values_ratio = if !column_profiles.is_empty() {
            let total_cells: usize = column_profiles.iter().map(|p| p.total_count).sum();
            let null_cells: usize = column_profiles.iter().map(|p| p.null_count).sum();
            if total_cells == 0 {
                0.0
            } else {
                (null_cells as f64 / total_cells as f64) * 100.0
            }
        } else {
            Self::calculate_missing_values_ratio(data)?
        };
        let complete_records_ratio = if !column_profiles.is_empty() {
            // Use profile-based lower bound (same as calculate_from_profiles)
            let total = column_profiles.first().map(|p| p.total_count).unwrap_or(0);
            let null_cells: usize = column_profiles.iter().map(|p| p.null_count).sum();
            if total == 0 {
                100.0
            } else {
                (total.saturating_sub(null_cells) as f64 / total as f64 * 100.0).max(0.0)
            }
        } else {
            Self::calculate_complete_records_ratio(data, total_rows)?
        };
        let null_columns = self.identify_null_columns(column_profiles);

        Ok(CompletenessMetrics {
            missing_values_ratio,
            complete_records_ratio,
            null_columns,
        })
    }

    /// Calculate percentage of missing values across all cells
    fn calculate_missing_values_ratio(
        data: &HashMap<String, Vec<String>>,
    ) -> Result<f64, DataProfilerError> {
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
    ) -> Result<f64, DataProfilerError> {
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

    /// Compute completeness from global [`ColumnProfile`] counters (exact for streaming).
    ///
    /// `missing_values_ratio` and `null_columns` are exact.
    /// `complete_records_ratio` uses a pessimistic lower bound: it assumes nulls
    /// are maximally spread across different rows. The bound is tight when nulls
    /// are sparse and exact when no column has nulls.
    pub fn calculate_from_profiles(
        &self,
        column_profiles: &[ColumnProfile],
    ) -> Result<CompletenessMetrics, DataProfilerError> {
        let total_cells: usize = column_profiles.iter().map(|p| p.total_count).sum();
        let null_cells: usize = column_profiles.iter().map(|p| p.null_count).sum();

        let missing_values_ratio = if total_cells == 0 {
            0.0
        } else {
            (null_cells as f64 / total_cells as f64) * 100.0
        };

        let total_rows = column_profiles.first().map(|p| p.total_count).unwrap_or(0);
        let complete_records_ratio = if total_rows == 0 {
            100.0
        } else {
            (total_rows.saturating_sub(null_cells) as f64 / total_rows as f64 * 100.0).max(0.0)
        };

        let null_columns = self.identify_null_columns(column_profiles);

        Ok(CompletenessMetrics {
            missing_values_ratio,
            complete_records_ratio,
            null_columns,
        })
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnStats, DataType, TextStats};

    fn make_profile(name: &str, total: usize, nulls: usize) -> ColumnProfile {
        ColumnProfile {
            name: name.to_string(),
            data_type: DataType::String,
            null_count: nulls,
            total_count: total,
            unique_count: Some(total - nulls),
            stats: ColumnStats::Text(TextStats::from_lengths(1, 10, 5.0)),
            patterns: Some(vec![]),
        }
    }

    #[test]
    fn test_calculate_from_profiles_all_complete() {
        let thresholds = IsoQualityConfig::default();
        let calc = CompletenessCalculator::new(&thresholds);
        let profiles = vec![make_profile("a", 100, 0), make_profile("b", 100, 0)];

        let result = calc.calculate_from_profiles(&profiles).unwrap();
        assert_eq!(result.missing_values_ratio, 0.0);
        assert_eq!(result.complete_records_ratio, 100.0);
        assert!(result.null_columns.is_empty());
    }

    #[test]
    fn test_calculate_from_profiles_with_nulls() {
        let thresholds = IsoQualityConfig::default();
        let calc = CompletenessCalculator::new(&thresholds);
        let profiles = vec![
            make_profile("a", 100, 10), // 10% null
            make_profile("b", 100, 20), // 20% null
        ];

        let result = calc.calculate_from_profiles(&profiles).unwrap();
        // 30 null cells out of 200 total = 15%
        assert!((result.missing_values_ratio - 15.0).abs() < 0.01);
        // Pessimistic bound: max(0, 100 - 30) / 100 = 70%
        assert!((result.complete_records_ratio - 70.0).abs() < 0.01);
        assert!(result.null_columns.is_empty()); // Neither exceeds 50% threshold
    }

    #[test]
    fn test_calculate_from_profiles_null_column_detected() {
        let thresholds = IsoQualityConfig::default();
        let calc = CompletenessCalculator::new(&thresholds);
        let profiles = vec![
            make_profile("good", 100, 0),
            make_profile("bad", 100, 60), // 60% null → above 50% threshold
        ];

        let result = calc.calculate_from_profiles(&profiles).unwrap();
        assert_eq!(result.null_columns, vec!["bad".to_string()]);
    }

    #[test]
    fn test_calculate_from_profiles_empty() {
        let thresholds = IsoQualityConfig::default();
        let calc = CompletenessCalculator::new(&thresholds);
        let profiles: Vec<ColumnProfile> = vec![];

        let result = calc.calculate_from_profiles(&profiles).unwrap();
        assert_eq!(result.missing_values_ratio, 0.0);
        assert_eq!(result.complete_records_ratio, 100.0);
    }
}
