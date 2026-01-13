//! Completeness Dimension (ISO 8000-8)
//!
//! Measures the presence of all required data elements.
//! Key metrics: missing values ratio, complete records ratio, null columns.

use crate::core::config::IsoQualityConfig;
use crate::types::ColumnProfile;
use anyhow::Result;
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
}
