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
    pub total_cells: usize,
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
        let total_cells = if !column_profiles.is_empty() {
            column_profiles.iter().map(|p| p.total_count).sum()
        } else {
            data.values().map(|v| v.len()).sum()
        };
        let missing_values_ratio = if !column_profiles.is_empty() {
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
            total_cells,
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
            total_cells,
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
    use super::super::testing::{
        CompletenessInput, CompletenessScenario, assert_completeness, column_data,
        expected_completeness, null_threshold, string_profile,
    };
    use super::*;

    #[test]
    fn completeness_scenarios_cover_boundaries() {
        let scenarios = vec![
            CompletenessScenario {
                name: "empty input is computed but neutral",
                input: CompletenessInput::Profiles(vec![]),
                config: IsoQualityConfig::default(),
                expected: expected_completeness(0.0, 100.0, &[], 0),
            },
            CompletenessScenario {
                name: "perfect raw rows",
                input: CompletenessInput::Rows {
                    data: column_data(&[("a", &["x", "y"]), ("b", &["1", "2"])]),
                    total_rows: 2,
                },
                config: IsoQualityConfig::default(),
                expected: expected_completeness(0.0, 100.0, &[], 4),
            },
            CompletenessScenario {
                name: "exact null threshold is not a null column",
                input: CompletenessInput::Profiles(vec![string_profile("boundary", 100, 25)]),
                config: null_threshold(25.0),
                expected: expected_completeness(25.0, 75.0, &[], 100),
            },
            CompletenessScenario {
                name: "degraded profiles expose exact counters",
                input: CompletenessInput::Profiles(vec![
                    string_profile("partial", 100, 10),
                    string_profile("mostly_null", 100, 60),
                ]),
                config: IsoQualityConfig::default(),
                expected: expected_completeness(35.0, 30.0, &["mostly_null"], 200),
            },
        ];

        for scenario in scenarios {
            let calculator = CompletenessCalculator::new(&scenario.config);
            let actual = match &scenario.input {
                CompletenessInput::Rows { data, total_rows } => {
                    calculator.calculate(data, &[], *total_rows)
                }
                CompletenessInput::Profiles(profiles) => {
                    calculator.calculate_from_profiles(profiles)
                }
            }
            .unwrap_or_else(|error| panic!("scenario `{}` failed: {error}", scenario.name));

            assert_completeness(scenario.name, &actual, &scenario.expected);
        }
    }
}
