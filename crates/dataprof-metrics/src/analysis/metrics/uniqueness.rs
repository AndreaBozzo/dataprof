//! Uniqueness Dimension (ISO 8000-110)
//!
//! Measures duplicate detection and key uniqueness.
//! Key metrics: duplicate rows, key uniqueness, high cardinality warnings.

use super::utils::is_likely_id_column;
use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use crate::quality::RowDuplicateSummary;
use crate::types::ColumnProfile;
use std::collections::{HashMap, HashSet};

/// Uniqueness metrics container
#[derive(Debug)]
pub(crate) struct UniquenessMetrics {
    pub duplicate_rows: usize,
    pub key_uniqueness: f64,
    pub high_cardinality_warning: bool,
    pub rows_checked: usize,
    pub key_column: Option<String>,
    pub duplicate_rows_approximate: bool,
}

/// Calculator for uniqueness dimension metrics
pub(crate) struct UniquenessCalculator<'a> {
    thresholds: &'a IsoQualityConfig,
}

impl<'a> UniquenessCalculator<'a> {
    pub fn new(thresholds: &'a IsoQualityConfig) -> Self {
        Self { thresholds }
    }

    /// Calculate uniqueness dimension metrics.
    ///
    /// `row_duplicates` carries full-stream duplicate counts from an
    /// engine's row tracker; when present it supersedes the sample-based
    /// duplicate scan (which only works on provably row-aligned samples).
    pub fn calculate(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
        total_rows: usize,
        identifier_columns: &[String],
        row_duplicates: Option<RowDuplicateSummary>,
    ) -> Result<UniquenessMetrics, DataProfilerError> {
        let (duplicate_rows, rows_checked, duplicate_rows_approximate) = match row_duplicates {
            Some(summary) if summary.rows_checked > 0 => (
                summary.duplicate_rows,
                summary.rows_checked,
                summary.approximate,
            ),
            _ => {
                let (duplicates, rows) = Self::count_exact_duplicate_rows(data, column_profiles)?;
                (duplicates, rows, false)
            }
        };
        let (key_uniqueness, key_column) =
            Self::calculate_key_uniqueness(column_profiles, identifier_columns)?;
        let high_cardinality_warning =
            self.check_high_cardinality(column_profiles, total_rows, identifier_columns);

        Ok(UniquenessMetrics {
            duplicate_rows,
            key_uniqueness,
            high_cardinality_warning,
            rows_checked,
            key_column,
            duplicate_rows_approximate,
        })
    }

    /// Count exact duplicate rows; returns `(duplicates, rows_scanned)`.
    ///
    /// The per-column sample arrays are only row-aligned when every column
    /// kept every value: null-like values never enter the reservoir, and
    /// past its capacity each column is evicted independently. Comparing
    /// misaligned columns fabricates duplicates out of unrelated values, so
    /// when alignment cannot be proven (unequal lengths, or lengths that
    /// don't match the known row count) this returns `(0, 0)` — "not
    /// assessed" — rather than a plausible-looking wrong count.
    fn count_exact_duplicate_rows(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<(usize, usize), DataProfilerError> {
        if data.is_empty() {
            return Ok((0, 0));
        }

        let total_rows = data.values().next().map(|v| v.len()).ok_or_else(|| {
            DataProfilerError::MetricsCalculationError {
                message: "No data columns found".to_string(),
            }
        })?;

        let aligned_lengths = data.values().all(|column| column.len() == total_rows);
        let matches_known_row_count = match column_profiles.first() {
            Some(profile) => profile.total_count == total_rows,
            None => true,
        };
        if !aligned_lengths || !matches_known_row_count {
            return Ok((0, 0));
        }

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

        Ok((duplicates, total_rows))
    }

    /// Calculate key uniqueness percentage and the column it describes.
    ///
    /// Returns `(100.0, None)` when no key column was identified or its
    /// unique count is unavailable — the caller treats `None` as "no key
    /// signal" rather than "perfect key".
    fn calculate_key_uniqueness(
        column_profiles: &[ColumnProfile],
        identifier_columns: &[String],
    ) -> Result<(f64, Option<String>), DataProfilerError> {
        // Explicit semantic hints take precedence and retain user-supplied
        // order. Fall back to the conservative name heuristic when absent.
        let key_column = identifier_columns
            .iter()
            .find_map(|name| {
                column_profiles
                    .iter()
                    .find(|profile| profile.name == name.as_str())
            })
            .or_else(|| {
                column_profiles
                    .iter()
                    .find(|profile| is_likely_id_column(&profile.name))
            });

        if let Some(key_col) = key_column {
            if let Some(unique_count) = key_col.unique_count {
                if key_col.total_count == 0 {
                    Ok((100.0, None))
                } else {
                    Ok((
                        (unique_count as f64 / key_col.total_count as f64) * 100.0,
                        Some(key_col.name.clone()),
                    ))
                }
            } else {
                // unique_count unavailable: no evidence either way
                Ok((100.0, None))
            }
        } else {
            // No key column found: no key signal
            Ok((100.0, None))
        }
    }

    /// Check for high cardinality warning
    /// Uses configurable high_cardinality_threshold from ISO thresholds (default: 95%)
    fn check_high_cardinality(
        &self,
        column_profiles: &[ColumnProfile],
        total_rows: usize,
        identifier_columns: &[String],
    ) -> bool {
        if total_rows == 0 {
            return false;
        }

        let threshold = self.thresholds.high_cardinality_threshold / 100.0;

        column_profiles.iter().any(|profile| {
            if let Some(unique_count) = profile.unique_count {
                let cardinality_ratio = unique_count as f64 / total_rows as f64;
                // Flag if a column exceeds threshold (excluding obvious ID columns)
                let is_identifier = identifier_columns.contains(&profile.name)
                    || is_likely_id_column(&profile.name);
                cardinality_ratio > threshold && !is_identifier
            } else {
                false
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnStats, DataType};

    fn profile(name: &str, total: usize, unique: Option<usize>) -> ColumnProfile {
        ColumnProfile {
            name: name.to_string(),
            data_type: DataType::String,
            null_count: 0,
            total_count: total,
            unique_count: unique,
            unique_count_is_approximate: None,
            stats: ColumnStats::None,
            patterns: Some(vec![]),
        }
    }

    fn column(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    fn cardinality_column(total: usize, unique: usize) -> Vec<String> {
        (0..total).map(|i| (i % unique).to_string()).collect()
    }

    #[test]
    fn test_uniqueness_case_table() {
        struct Case {
            name: &'static str,
            data: HashMap<String, Vec<String>>,
            profiles: Vec<ColumnProfile>,
            total_rows: usize,
            duplicate_rows: usize,
            rows_checked: usize,
            key_uniqueness: f64,
            key_column: Option<&'static str>,
            high_cardinality_warning: bool,
        }

        let cases = [
            Case {
                name: "all rows distinct",
                data: HashMap::from([("city".to_string(), column(&["a", "b", "c"]))]),
                profiles: vec![profile("city", 3, Some(3))],
                total_rows: 3,
                duplicate_rows: 0,
                rows_checked: 3,
                key_uniqueness: 100.0,
                key_column: None,
                high_cardinality_warning: true, // 3/3 unique, non-id column
            },
            Case {
                name: "one exact duplicate row",
                data: HashMap::from([("city".to_string(), column(&["a", "b", "a"]))]),
                profiles: vec![profile("city", 3, Some(2))],
                total_rows: 3,
                duplicate_rows: 1,
                rows_checked: 3,
                key_uniqueness: 100.0,
                key_column: None,
                high_cardinality_warning: false,
            },
            Case {
                name: "all rows identical",
                data: HashMap::from([("city".to_string(), column(&["a", "a", "a", "a"]))]),
                profiles: vec![profile("city", 4, Some(1))],
                total_rows: 4,
                duplicate_rows: 3,
                rows_checked: 4,
                key_uniqueness: 100.0,
                key_column: None,
                high_cardinality_warning: false,
            },
            Case {
                name: "fully unique identifier column",
                data: HashMap::from([("user_id".to_string(), column(&["1", "2", "3"]))]),
                profiles: vec![profile("user_id", 3, Some(3))],
                total_rows: 3,
                duplicate_rows: 0,
                rows_checked: 3,
                key_uniqueness: 100.0,
                key_column: Some("user_id"),
                high_cardinality_warning: false, // id columns are excluded
            },
            Case {
                name: "partially unique identifier column",
                data: HashMap::from([("user_id".to_string(), column(&["1", "2", "2", "3"]))]),
                profiles: vec![profile("user_id", 4, Some(3))],
                total_rows: 4,
                duplicate_rows: 1,
                rows_checked: 4,
                key_uniqueness: 75.0,
                key_column: Some("user_id"),
                high_cardinality_warning: false,
            },
            Case {
                name: "high cardinality just below threshold",
                data: HashMap::from([("note".to_string(), cardinality_column(100, 94))]),
                profiles: vec![profile("note", 100, Some(94))],
                total_rows: 100,
                duplicate_rows: 6,
                rows_checked: 100,
                key_uniqueness: 100.0,
                key_column: None,
                high_cardinality_warning: false,
            },
            Case {
                name: "high cardinality at threshold (not above)",
                data: HashMap::from([("note".to_string(), cardinality_column(100, 95))]),
                profiles: vec![profile("note", 100, Some(95))],
                total_rows: 100,
                duplicate_rows: 5,
                rows_checked: 100,
                key_uniqueness: 100.0,
                key_column: None,
                // 95/100 == threshold exactly; the check requires strictly above
                high_cardinality_warning: false,
            },
            Case {
                name: "high cardinality above threshold",
                data: HashMap::from([("note".to_string(), cardinality_column(100, 96))]),
                profiles: vec![profile("note", 100, Some(96))],
                total_rows: 100,
                duplicate_rows: 4,
                rows_checked: 100,
                key_uniqueness: 100.0,
                key_column: None,
                high_cardinality_warning: true,
            },
            Case {
                name: "empty input",
                data: HashMap::new(),
                profiles: vec![],
                total_rows: 0,
                duplicate_rows: 0,
                rows_checked: 0,
                key_uniqueness: 100.0,
                key_column: None,
                high_cardinality_warning: false,
            },
            Case {
                name: "single row",
                data: HashMap::from([("city".to_string(), column(&["a"]))]),
                profiles: vec![profile("city", 1, Some(1))],
                total_rows: 1,
                duplicate_rows: 0,
                rows_checked: 1,
                key_uniqueness: 100.0,
                key_column: None,
                high_cardinality_warning: true, // 1/1 unique, non-id column
            },
        ];

        let thresholds = IsoQualityConfig::default();
        let calculator = UniquenessCalculator::new(&thresholds);

        for case in cases {
            let metrics = calculator
                .calculate(&case.data, &case.profiles, case.total_rows, &[], None)
                .unwrap_or_else(|e| panic!("{}: calculation failed: {e}", case.name));

            assert_eq!(
                metrics.duplicate_rows, case.duplicate_rows,
                "{}: duplicate_rows",
                case.name
            );
            assert_eq!(
                metrics.rows_checked, case.rows_checked,
                "{}: rows_checked",
                case.name
            );
            assert!(
                (metrics.key_uniqueness - case.key_uniqueness).abs() < 0.01,
                "{}: key_uniqueness {} != {}",
                case.name,
                metrics.key_uniqueness,
                case.key_uniqueness
            );
            assert_eq!(
                metrics.key_column.as_deref(),
                case.key_column,
                "{}: key_column",
                case.name
            );
            assert_eq!(
                metrics.high_cardinality_warning, case.high_cardinality_warning,
                "{}: high_cardinality_warning",
                case.name
            );
            assert!(
                !metrics.duplicate_rows_approximate,
                "{}: sample-scan duplicates are never approximate",
                case.name
            );
        }
    }

    #[test]
    fn test_row_duplicate_summary_supersedes_sample_scan() {
        let thresholds = IsoQualityConfig::default();
        let calculator = UniquenessCalculator::new(&thresholds);
        // Misaligned sample (2 vs 3 values): the scan alone would refuse it.
        let data = HashMap::from([
            ("a".to_string(), column(&["x", "y"])),
            ("b".to_string(), column(&["1", "2", "3"])),
        ]);
        let profiles = vec![profile("a", 1000, Some(2)), profile("b", 1000, Some(3))];

        let without = calculator
            .calculate(&data, &profiles, 1000, &[], None)
            .expect("scan-only path");
        assert_eq!(without.rows_checked, 0, "misaligned sample must not scan");

        let summary = RowDuplicateSummary {
            duplicate_rows: 40,
            rows_checked: 1000,
            approximate: true,
        };
        let with = calculator
            .calculate(&data, &profiles, 1000, &[], Some(summary))
            .expect("summary path");
        assert_eq!(with.duplicate_rows, 40);
        assert_eq!(with.rows_checked, 1000);
        assert!(with.duplicate_rows_approximate);
    }

    #[test]
    fn test_misaligned_sample_lengths_are_not_scanned() {
        let thresholds = IsoQualityConfig::default();
        let calculator = UniquenessCalculator::new(&thresholds);
        let data = HashMap::from([
            ("a".to_string(), column(&["x", "x", "x"])),
            ("b".to_string(), column(&["1", "1"])),
        ]);
        let profiles = vec![profile("a", 3, Some(1)), profile("b", 3, Some(1))];

        let metrics = calculator
            .calculate(&data, &profiles, 3, &[], None)
            .expect("metrics");
        assert_eq!(metrics.duplicate_rows, 0);
        assert_eq!(metrics.rows_checked, 0);
    }
}
