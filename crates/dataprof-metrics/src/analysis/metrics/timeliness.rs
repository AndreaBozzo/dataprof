//! Timeliness Dimension (ISO 8000-8)
//!
//! Measures the currentness and temporal validity of data.
//! Key metrics: future dates, stale data ratio, temporal violations.

use super::utils::extract_date;
use crate::analysis::inference::is_null_like_token;
use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use chrono::{Datelike, NaiveDate, Utc};
use std::collections::{HashMap, HashSet};

/// Timeliness metrics container
#[derive(Debug)]
pub(crate) struct TimelinessMetrics {
    pub future_dates_count: usize,
    pub stale_data_ratio: f64,
    pub temporal_violations: usize,
    pub invalid_date_values: usize,
    pub date_values_checked: usize,
    pub temporal_pairs_checked: usize,
}

/// Calculator for timeliness dimension metrics
pub(crate) struct TimelinessCalculator<'a> {
    thresholds: &'a IsoQualityConfig,
    /// "Now" for every comparison this calculator makes.
    ///
    /// Captured once at construction so a single report cannot straddle a
    /// midnight boundary and classify two equal values differently, and so
    /// tests can pin it. Internal only: the public API still reads the clock.
    reference_date: NaiveDate,
}

impl<'a> TimelinessCalculator<'a> {
    pub fn new(thresholds: &'a IsoQualityConfig) -> Self {
        Self::with_reference_date(thresholds, Utc::now().date_naive())
    }

    /// Construct with an explicit "now", so assertions do not rot as the
    /// calendar advances.
    pub(crate) fn with_reference_date(
        thresholds: &'a IsoQualityConfig,
        reference_date: NaiveDate,
    ) -> Self {
        Self {
            thresholds,
            reference_date,
        }
    }

    /// Calculate timeliness dimension metrics
    pub fn calculate(
        &self,
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
    ) -> Result<TimelinessMetrics, DataProfilerError> {
        let (future_dates_count, stale_data_ratio, date_values_checked, invalid_date_values) =
            self.calculate_date_summary(data, temporal_columns);
        let (temporal_violations, temporal_pairs_checked) =
            Self::count_temporal_violations(data, temporal_columns)?;

        Ok(TimelinessMetrics {
            future_dates_count,
            stale_data_ratio,
            temporal_violations,
            invalid_date_values,
            date_values_checked,
            temporal_pairs_checked,
        })
    }

    /// Assess every non-null value in the selected temporal columns.
    ///
    /// `date_values_checked` is the complete denominator. Values that do not
    /// parse as real calendar dates remain visible as `invalid_date_values`
    /// instead of disappearing from both the metric and its score.
    fn calculate_date_summary(
        &self,
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[String],
    ) -> (usize, f64, usize, usize) {
        let mut future_count = 0;
        let mut stale_dates = 0;
        let mut valid_dates = 0;
        let mut checked = 0;
        let mut invalid_dates = 0;

        let threshold_year = self.reference_date.year() - self.thresholds.max_data_age_years as i32;

        for column_name in temporal_columns {
            let Some(column_data) = data.get(column_name) else {
                continue;
            };
            for value in column_data {
                if is_null_like_token(value.trim()) {
                    continue;
                }
                checked += 1;

                if let Some(date) = extract_date(value) {
                    valid_dates += 1;
                    // Compare the whole date, not its year. A year-only test
                    // cannot see any future date inside the current calendar
                    // year, which on 1 January hides an entire year of them.
                    if date > self.reference_date {
                        future_count += 1;
                    }
                    // Staleness stays at year granularity because its threshold
                    // is expressed in whole years.
                    if date.year() < threshold_year {
                        stale_dates += 1;
                    }
                } else {
                    invalid_dates += 1;
                }
            }
        }

        let stale_ratio = if valid_dates == 0 {
            0.0
        } else {
            (stale_dates as f64 / valid_dates as f64) * 100.0
        };
        (future_count, stale_ratio, checked, invalid_dates)
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

        // The role patterns overlap: `start_date`/`end_date` is matched by both
        // ("start_date", "end_date") and ("start", "end"). Without this the same
        // two columns are compared once per matching pattern, and every pair and
        // every violation is counted as many times as patterns happened to hit.
        let mut evaluated: HashSet<(&str, &str)> = HashSet::new();

        for (start_col, end_col) in &temporal_pairs {
            // Resolve ambiguous role matches in the order supplied by the user.
            // Iterating `data` here would make the selected pair depend on the
            // randomized iteration order of its HashMap.
            let start_match = temporal_columns
                .iter()
                .find(|name| name.to_lowercase().contains(start_col))
                .and_then(|name| data.get(name).map(|values| (name.as_str(), values)));
            let end_match = temporal_columns
                .iter()
                .find(|name| name.to_lowercase().contains(end_col))
                .and_then(|name| data.get(name).map(|values| (name.as_str(), values)));

            let (Some((start_name, start_values)), Some((end_name, end_values))) =
                (start_match, end_match)
            else {
                continue;
            };

            // A column cannot be both ends of the same comparison: "start" and
            // "end" both match a lone `start_end_date`, which would compare it
            // with itself and report a clean pair that was never checked.
            if start_name == end_name || !evaluated.insert((start_name, end_name)) {
                continue;
            }

            for (start_val, end_val) in start_values.iter().zip(end_values.iter()) {
                if is_null_like_token(start_val.trim()) || is_null_like_token(end_val.trim()) {
                    continue;
                }

                // Invalid calendar values are already reported by
                // `invalid_date_values` and cannot form a meaningful pair.
                let (Some(start_date), Some(end_date)) =
                    (extract_date(start_val), extract_date(end_val))
                else {
                    continue;
                };
                pairs_checked += 1;
                // Compare parsed dates. The previous string comparison only held
                // for ISO values: `DD/MM/YYYY` and `MM/DD/YYYY` sort by day and
                // month first, which both hid real inversions and invented ones.
                if start_date > end_date {
                    violations += 1;
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

        assert_eq!(metrics.date_values_checked, 2);
        assert_eq!(metrics.invalid_date_values, 1);
    }

    #[test]
    fn invalid_calendar_values_are_not_compared_as_temporal_pairs() {
        let data = HashMap::from([
            ("start".to_string(), vec!["2024-13-45".to_string()]),
            ("end".to_string(), vec!["2024-01-01".to_string()]),
        ]);
        let config = IsoQualityConfig::default();

        let metrics = TimelinessCalculator::new(&config)
            .calculate(&data, &["start".to_string(), "end".to_string()])
            .expect("timeliness metrics");

        assert_eq!(metrics.invalid_date_values, 1);
        assert_eq!(metrics.temporal_pairs_checked, 0);
        assert_eq!(metrics.temporal_violations, 0);
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

    // ---------------------------------------------------------------- #378
    // Deterministic coverage for the timeliness calculator. Every case pins
    // "now" through `with_reference_date`, so no assertion here changes result
    // as the calendar advances.

    /// A fixed "today" for every case below.
    fn reference() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 6, 15).expect("valid reference date")
    }

    /// Default freshness policy is 5 years, so the stale cutoff is 2021.
    fn config() -> IsoQualityConfig {
        IsoQualityConfig::default()
    }

    fn column(name: &str, values: &[&str]) -> HashMap<String, Vec<String>> {
        HashMap::from([(
            name.to_string(),
            values.iter().map(|value| value.to_string()).collect(),
        )])
    }

    fn metrics_for(
        thresholds: &IsoQualityConfig,
        data: &HashMap<String, Vec<String>>,
        temporal_columns: &[&str],
    ) -> TimelinessMetrics {
        let named: Vec<String> = temporal_columns
            .iter()
            .map(|name| name.to_string())
            .collect();
        TimelinessCalculator::with_reference_date(thresholds, reference())
            .calculate(data, &named)
            .expect("timeliness metrics")
    }

    #[test]
    fn no_date_columns_assesses_nothing() {
        let data = column("label", &["alpha", "beta"]);
        let metrics = metrics_for(&config(), &data, &[]);

        assert_eq!(metrics.date_values_checked, 0);
        assert_eq!(metrics.invalid_date_values, 0);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
        assert_eq!(metrics.temporal_pairs_checked, 0);
        assert_eq!(metrics.temporal_violations, 0);
    }

    #[test]
    fn empty_column_assesses_nothing() {
        let data = column("event_date", &[]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.date_values_checked, 0);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
    }

    #[test]
    fn single_recent_value_is_neither_future_nor_stale() {
        let data = column("event_date", &["2026-06-14"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.date_values_checked, 1);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
    }

    #[test]
    fn past_dates_within_the_freshness_window_are_clean() {
        let data = column("event_date", &["2022-01-01", "2024-06-01", "2026-06-14"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.date_values_checked, 3);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
    }

    #[test]
    fn future_dates_inside_the_current_year_are_counted() {
        // Regression: comparing years alone reported 0 future dates for every
        // value between today and 31 December.
        let data = column(
            "event_date",
            &[
                "2026-06-16", // tomorrow
                "2026-07-01", // later the same year
                "2026-12-31", // last day of the same year
                "2027-01-01", // next year, caught even by the year-only check
            ],
        );
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(
            metrics.future_dates_count, 4,
            "every value after the reference date is in the future"
        );
        assert_eq!(metrics.date_values_checked, 4);
    }

    #[test]
    fn one_future_date_among_past_ones_is_counted_once() {
        let data = column("event_date", &["2026-06-14", "2026-06-16", "2025-01-01"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.future_dates_count, 1);
    }

    #[test]
    fn the_reference_date_itself_is_not_future() {
        let data = column("event_date", &["2026-06-15"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(metrics.future_dates_count, 0, "today is not in the future");
    }

    #[test]
    fn values_beyond_the_stale_threshold_are_reported_as_a_percentage() {
        // Reference year 2026 and a 5-year policy put the cutoff at 2021.
        let data = column(
            "event_date",
            &["2019-01-01", "2020-12-31", "2021-01-01", "2026-01-01"],
        );
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(
            metrics.stale_data_ratio, 50.0,
            "two of four values fall before the cutoff, on the 0-100 scale"
        );
    }

    #[test]
    fn a_shorter_freshness_policy_makes_more_values_stale() {
        let mut thresholds = config();
        thresholds.max_data_age_years = 2.0; // cutoff 2024
        let data = column("event_date", &["2022-01-01", "2023-01-01", "2025-01-01"]);
        let metrics = metrics_for(&thresholds, &data, &["event_date"]);

        assert!(
            (metrics.stale_data_ratio - (2.0 / 3.0 * 100.0)).abs() < 1e-9,
            "expected two of three stale, got {}",
            metrics.stale_data_ratio
        );
    }

    #[test]
    fn ordered_temporal_values_report_no_violation() {
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2023-01-01".to_string()]),
            ("end_date".to_string(), vec!["2023-06-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_violations, 0);
        assert_eq!(metrics.temporal_pairs_checked, 1);
    }

    #[test]
    fn out_of_order_temporal_values_report_a_violation() {
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2023-06-01".to_string()]),
            ("end_date".to_string(), vec!["2023-01-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_violations, 1);
        assert_eq!(metrics.temporal_pairs_checked, 1);
    }

    #[test]
    fn day_first_dates_are_ordered_by_calendar_not_by_text() {
        // Regression: `start_val > end_val` compared raw strings. `DD/MM/YYYY`
        // sorts by day first, so a real inversion read as clean.
        let data = HashMap::from([
            ("start_date".to_string(), vec!["15/01/2023".to_string()]),
            ("end_date".to_string(), vec!["20/03/2022".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(
            metrics.temporal_violations, 1,
            "January 2023 starts after March 2022 ends"
        );
        assert_eq!(metrics.temporal_pairs_checked, 1);
    }

    #[test]
    fn day_first_dates_in_valid_order_report_no_violation() {
        // The same bug in the other direction: text ordering invented a
        // violation for a correctly ordered pair.
        let data = HashMap::from([
            ("start_date".to_string(), vec!["20/03/2022".to_string()]),
            ("end_date".to_string(), vec!["15/01/2023".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(
            metrics.temporal_violations, 0,
            "March 2022 to January 2023 runs forwards"
        );
        assert_eq!(metrics.temporal_pairs_checked, 1);
    }

    #[test]
    fn overlapping_role_patterns_evaluate_a_column_pair_once() {
        // Regression: `start_date`/`end_date` matched both the
        // ("start_date", "end_date") and ("start", "end") patterns, so every
        // row of the pair was compared and counted twice.
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2023-05-01".to_string()]),
            ("end_date".to_string(), vec!["2023-04-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(
            metrics.temporal_pairs_checked, 1,
            "one row of one column pair is one comparison"
        );
        assert_eq!(metrics.temporal_violations, 1);
    }

    #[test]
    fn null_tokens_in_a_pair_are_skipped_rather_than_compared() {
        let data = HashMap::from([
            (
                "start_date".to_string(),
                vec!["2023-06-01".to_string(), "".to_string()],
            ),
            (
                "end_date".to_string(),
                vec!["2023-01-01".to_string(), "2023-01-01".to_string()],
            ),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_pairs_checked, 1, "only one complete pair");
        assert_eq!(metrics.temporal_violations, 1);
    }

    #[test]
    fn unparseable_values_are_invalid_rather_than_future_or_stale() {
        // Absence rule: a value that is not a date must not be silently read as
        // a valid timestamp in either direction.
        let data = column("event_date", &["2024-13-45", "not a date", "2026-06-14"]);
        let metrics = metrics_for(&config(), &data, &["event_date"]);

        assert_eq!(
            metrics.date_values_checked, 3,
            "the denominator is complete"
        );
        assert_eq!(metrics.invalid_date_values, 2);
        assert_eq!(metrics.future_dates_count, 0);
        assert_eq!(metrics.stale_data_ratio, 0.0);
    }

    #[test]
    fn invalid_values_do_not_form_temporal_pairs() {
        let data = HashMap::from([
            ("start_date".to_string(), vec!["2024-13-45".to_string()]),
            ("end_date".to_string(), vec!["2023-01-01".to_string()]),
        ]);
        let metrics = metrics_for(&config(), &data, &["start_date", "end_date"]);

        assert_eq!(metrics.temporal_pairs_checked, 0);
        assert_eq!(metrics.temporal_violations, 0);
    }
}
