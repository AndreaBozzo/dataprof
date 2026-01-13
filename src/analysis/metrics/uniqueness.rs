//! Uniqueness Dimension (ISO 8000-110)
//!
//! Measures duplicate detection and key uniqueness.
//! Key metrics: duplicate rows, key uniqueness, high cardinality warnings.

use super::utils::is_likely_id_column;
use crate::core::config::IsoQualityThresholds;
use crate::types::ColumnProfile;
use anyhow::Result;
use std::collections::{HashMap, HashSet};

/// Uniqueness metrics container
#[derive(Debug)]
pub(crate) struct UniquenessMetrics {
    pub duplicate_rows: usize,
    pub key_uniqueness: f64,
    pub high_cardinality_warning: bool,
}

/// Calculator for uniqueness dimension metrics
pub(crate) struct UniquenessCalculator<'a> {
    thresholds: &'a IsoQualityThresholds,
}

impl<'a> UniquenessCalculator<'a> {
    pub fn new(thresholds: &'a IsoQualityThresholds) -> Self {
        Self { thresholds }
    }

    /// Calculate uniqueness dimension metrics
    pub fn calculate(
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

        let total_rows = data
            .values()
            .next()
            .map(|v| v.len())
            .ok_or_else(|| anyhow::anyhow!("No data columns found"))?;
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
                cardinality_ratio > threshold && !is_likely_id_column(&profile.name)
            } else {
                false
            }
        })
    }
}
