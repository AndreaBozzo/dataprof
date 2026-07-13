//! Timeliness Dimension (ISO 8000-8)
//!
//! Measures the currentness and temporal validity of data.
//! Key metrics: future dates, stale data ratio, temporal violations.

use super::utils::extract_year;
use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use chrono::Datelike;
use std::collections::HashMap;

/// Timeliness metrics container
#[derive(Debug)]
pub(crate) struct TimelinessMetrics {
    pub future_dates_count: usize,
    pub stale_data_ratio: f64,
    pub temporal_violations: usize,
    pub date_values_checked: usize,
    pub temporal_pairs_checked: usize,
}

/// Calculator for timeliness dimension metrics
pub(crate) struct TimelinessCalculator<'a> {
    thresholds: &'a IsoQualityConfig,
}

impl<'a> TimelinessCalculator<'a> {
    pub fn new(thresholds: &'a IsoQualityConfig) -> Self {
        Self { thresholds }
    }

    /// Calculate timeliness dimension metrics
    pub fn calculate(
        &self,
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
    ) -> Result<TimelinessMetrics, DataProfilerError> {
        let future_dates_count = Self::count_future_dates(data, temporal_columns)?;
        let (stale_data_ratio, date_values_checked) =
            self.calculate_stale_data_ratio(data, temporal_columns)?;
        let (temporal_violations, temporal_pairs_checked) =
            Self::count_temporal_violations(data, temporal_columns)?;

        Ok(TimelinessMetrics {
            future_dates_count,
            stale_data_ratio,
            temporal_violations,
            date_values_checked,
            temporal_pairs_checked,
        })
    }

    /// Count dates that are in the future (beyond current date)
    fn count_future_dates(
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
    ) -> Result<usize, DataProfilerError> {
        let mut future_count = 0;

        // Get current year from system time
        let current_year = chrono::Utc::now().year();

        for (column_name, column_data) in data {
            if !temporal_columns.contains(column_name) {
                continue;
            }
            for value in column_data {
                if value.is_empty() {
                    continue;
                }

                // Extract year from common date formats
                if let Some(year) = extract_year(value)
                    && year > current_year
                {
                    future_count += 1;
                }
            }
        }

        Ok(future_count)
    }

    /// Calculate percentage of stale data (older than threshold); returns
    /// `(ratio, date values with an extractable year)`.
    fn calculate_stale_data_ratio(
        &self,
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
    ) -> Result<(f64, usize), DataProfilerError> {
        let mut total_dates = 0;
        let mut stale_dates = 0;

        // Get current year from system time
        let current_year = chrono::Utc::now().year();
        let threshold_year = current_year - self.thresholds.max_data_age_years as i32;

        for (column_name, column_data) in data {
            if !temporal_columns.contains(column_name) {
                continue;
            }
            for value in column_data {
                if value.is_empty() {
                    continue;
                }

                if let Some(year) = extract_year(value) {
                    total_dates += 1;
                    if year < threshold_year {
                        stale_dates += 1;
                    }
                }
            }
        }

        if total_dates == 0 {
            Ok((0.0, 0))
        } else {
            Ok((
                (stale_dates as f64 / total_dates as f64) * 100.0,
                total_dates,
            ))
        }
    }

    /// Count temporal ordering violations (e.g., end_date < start_date);
    /// returns `(violations, pairs compared)`. The pair count is the
    /// denominator that makes the violation count interpretable — it is not
    /// bounded by the number of date-typed values.
    fn count_temporal_violations(
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
    ) -> Result<(usize, usize), DataProfilerError> {
        let mut violations = 0;
        let mut pairs_checked = 0;

        // Look for column pairs like start_date/end_date, created_at/updated_at
        let temporal_pairs = [
            ("start_date", "end_date"),
            ("start", "end"),
            ("created_at", "updated_at"),
            ("created", "updated"),
            ("begin_date", "end_date"),
            ("from_date", "to_date"),
        ];

        for (start_col, end_col) in &temporal_pairs {
            // Resolve ambiguous role matches in the order supplied by the user.
            // Iterating `data` here would make the selected pair depend on the
            // randomized iteration order of its HashMap.
            let start_data = temporal_columns.iter().find_map(|name| {
                name.to_lowercase()
                    .contains(start_col)
                    .then(|| data.get(name))
                    .flatten()
            });
            let end_data = temporal_columns.iter().find_map(|name| {
                name.to_lowercase()
                    .contains(end_col)
                    .then(|| data.get(name))
                    .flatten()
            });

            if let (Some(start_values), Some(end_values)) = (start_data, end_data) {
                for (start_val, end_val) in start_values.iter().zip(end_values.iter()) {
                    if start_val.is_empty() || end_val.is_empty() {
                        continue;
                    }

                    pairs_checked += 1;
                    // Compare as sortable strings (works for ISO dates YYYY-MM-DD)
                    if start_val > end_val {
                        violations += 1;
                    }
                }
            }
        }

        Ok((violations, pairs_checked))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inferred_dates_are_not_assessed_without_explicit_temporal_columns() {
        let data = HashMap::from([(
            "observed_on".to_string(),
            vec!["2020-01-01".to_string(), "2021-01-01".to_string()],
        )]);
        let config = IsoQualityConfig::default();

        let metrics = TimelinessCalculator::new(&config)
            .calculate(&data, &[])
            .expect("timeliness metrics");

        assert_eq!(metrics.date_values_checked, 0);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.temporal_pairs_checked, 0);
    }

    #[test]
    fn explicit_temporal_columns_assess_parseable_values_even_in_mixed_columns() {
        let data = HashMap::from([(
            "event_value".to_string(),
            vec!["2020-01-01".to_string(), "not-a-date".to_string()],
        )]);
        let config = IsoQualityConfig::default();

        let metrics = TimelinessCalculator::new(&config)
            .calculate(&data, &["event_value".to_string()])
            .expect("timeliness metrics");

        assert_eq!(metrics.date_values_checked, 1);
    }

    #[test]
    fn temporal_ordering_requires_both_columns_to_be_explicit() {
        let data = HashMap::from([
            (
                "start".to_string(),
                vec!["2024-01-02".to_string(), "2024-01-01".to_string()],
            ),
            (
                "end".to_string(),
                vec!["2024-01-01".to_string(), "2024-01-02".to_string()],
            ),
        ]);
        let config = IsoQualityConfig::default();
        let calculator = TimelinessCalculator::new(&config);

        let partial = calculator
            .calculate(&data, &["start".to_string()])
            .expect("partial timeliness metrics");
        assert_eq!(partial.temporal_pairs_checked, 0);

        let complete = calculator
            .calculate(&data, &["start".to_string(), "end".to_string()])
            .expect("complete timeliness metrics");
        assert_eq!(complete.temporal_pairs_checked, 2);
        assert_eq!(complete.temporal_violations, 1);
    }

    #[test]
    fn temporal_ordering_uses_explicit_column_order_for_ambiguous_roles() {
        let data = HashMap::from([
            ("primary_start".to_string(), vec!["2024-01-01".to_string()]),
            (
                "secondary_start".to_string(),
                vec!["2024-01-03".to_string()],
            ),
            ("end".to_string(), vec!["2024-01-02".to_string()]),
        ]);
        let config = IsoQualityConfig::default();
        let calculator = TimelinessCalculator::new(&config);

        let primary_first = calculator
            .calculate(
                &data,
                &[
                    "primary_start".to_string(),
                    "secondary_start".to_string(),
                    "end".to_string(),
                ],
            )
            .expect("primary-first timeliness metrics");
        assert_eq!(primary_first.temporal_violations, 0);

        let secondary_first = calculator
            .calculate(
                &data,
                &[
                    "secondary_start".to_string(),
                    "primary_start".to_string(),
                    "end".to_string(),
                ],
            )
            .expect("secondary-first timeliness metrics");
        assert_eq!(secondary_first.temporal_violations, 1);
    }
}
