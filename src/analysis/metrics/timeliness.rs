//! Timeliness Dimension (ISO 8000-8)
//!
//! Measures the currentness and temporal validity of data.
//! Key metrics: future dates, stale data ratio, temporal violations.

use super::utils::extract_year;
use crate::core::config::IsoQualityConfig;
use crate::types::{ColumnProfile, DataType};
use anyhow::Result;
use chrono::Datelike;
use std::collections::HashMap;

/// Timeliness metrics container
#[derive(Debug)]
pub(crate) struct TimelinessMetrics {
    pub future_dates_count: usize,
    pub stale_data_ratio: f64,
    pub temporal_violations: usize,
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
        column_profiles: &[ColumnProfile],
    ) -> Result<TimelinessMetrics> {
        let future_dates_count = Self::count_future_dates(data, column_profiles)?;
        let stale_data_ratio = self.calculate_stale_data_ratio(data, column_profiles)?;
        let temporal_violations = Self::count_temporal_violations(data)?;

        Ok(TimelinessMetrics {
            future_dates_count,
            stale_data_ratio,
            temporal_violations,
        })
    }

    /// Count dates that are in the future (beyond current date)
    fn count_future_dates(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<usize> {
        let mut future_count = 0;

        // Get current year from system time
        let current_year = chrono::Utc::now().year();

        for profile in column_profiles {
            if !matches!(profile.data_type, DataType::Date) {
                continue;
            }

            if let Some(column_data) = data.get(&profile.name) {
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
        }

        Ok(future_count)
    }

    /// Calculate percentage of stale data (older than threshold)
    fn calculate_stale_data_ratio(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<f64> {
        let mut total_dates = 0;
        let mut stale_dates = 0;

        // Get current year from system time
        let current_year = chrono::Utc::now().year();
        let threshold_year = current_year - self.thresholds.max_data_age_years as i32;

        for profile in column_profiles {
            if !matches!(profile.data_type, DataType::Date) {
                continue;
            }

            if let Some(column_data) = data.get(&profile.name) {
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
        }

        if total_dates == 0 {
            Ok(0.0)
        } else {
            Ok((stale_dates as f64 / total_dates as f64) * 100.0)
        }
    }

    /// Count temporal ordering violations (e.g., end_date < start_date)
    fn count_temporal_violations(data: &HashMap<String, Vec<String>>) -> Result<usize> {
        let mut violations = 0;

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
            let start_data = data
                .iter()
                .find(|(k, _)| k.to_lowercase().contains(start_col));
            let end_data = data
                .iter()
                .find(|(k, _)| k.to_lowercase().contains(end_col));

            if let (Some((_, start_values)), Some((_, end_values))) = (start_data, end_data) {
                for (start_val, end_val) in start_values.iter().zip(end_values.iter()) {
                    if start_val.is_empty() || end_val.is_empty() {
                        continue;
                    }

                    // Compare as sortable strings (works for ISO dates YYYY-MM-DD)
                    if start_val > end_val {
                        violations += 1;
                    }
                }
            }
        }

        Ok(violations)
    }
}
