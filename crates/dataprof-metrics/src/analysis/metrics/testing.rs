//! Test-only scenario helpers for quality-dimension calculators.
//!
//! Keep these builders small and dimension-neutral. Future uniqueness and
//! timeliness scenario matrices can reuse `column_data` and `string_profile`,
//! adding dimension-specific scenario and assertion helpers only when needed.

use super::completeness::CompletenessMetrics;
use crate::core::config::IsoQualityConfig;
use crate::types::{ColumnProfile, ColumnStats, DataType, TextStats};
use std::collections::HashMap;

pub(super) fn column_data(columns: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
    columns
        .iter()
        .map(|(name, values)| {
            (
                (*name).to_string(),
                values.iter().map(|value| (*value).to_string()).collect(),
            )
        })
        .collect()
}

pub(super) fn string_profile(name: &str, total: usize, nulls: usize) -> ColumnProfile {
    let unique_count = total.checked_sub(nulls).unwrap_or_else(|| {
        panic!("profile `{name}` has {nulls} nulls but only {total} total values")
    });

    ColumnProfile {
        name: name.to_string(),
        data_type: DataType::String,
        null_count: nulls,
        total_count: total,
        unique_count: Some(unique_count),
        unique_count_is_approximate: Some(false),
        invalid_count: None,
        stats: ColumnStats::Text(TextStats::from_lengths(1, 10, 5.0)),
        patterns: Some(vec![]),
    }
}

pub(super) fn null_threshold(max_null_percentage: f64) -> IsoQualityConfig {
    IsoQualityConfig {
        max_null_percentage,
        ..IsoQualityConfig::default()
    }
}

pub(super) enum CompletenessInput {
    Rows {
        data: HashMap<String, Vec<String>>,
        total_rows: usize,
    },
    Profiles(Vec<ColumnProfile>),
}

pub(super) struct CompletenessScenario {
    pub name: &'static str,
    pub input: CompletenessInput,
    pub config: IsoQualityConfig,
    pub expected: CompletenessMetrics,
}

pub(super) fn expected_completeness(
    missing_values_ratio: f64,
    complete_records_ratio: f64,
    null_columns: &[&str],
    total_cells: usize,
) -> CompletenessMetrics {
    CompletenessMetrics {
        missing_values_ratio,
        complete_records_ratio,
        null_columns: null_columns
            .iter()
            .map(|column| (*column).to_string())
            .collect(),
        total_cells,
    }
}

pub(super) fn assert_completeness(
    scenario_name: &str,
    actual: &CompletenessMetrics,
    expected: &CompletenessMetrics,
) {
    assert_float_field(
        scenario_name,
        "missing_values_ratio",
        actual.missing_values_ratio,
        expected.missing_values_ratio,
    );
    assert_float_field(
        scenario_name,
        "complete_records_ratio",
        actual.complete_records_ratio,
        expected.complete_records_ratio,
    );
    assert_eq!(
        actual.null_columns, expected.null_columns,
        "scenario `{scenario_name}` field `null_columns`"
    );
    assert_eq!(
        actual.total_cells, expected.total_cells,
        "scenario `{scenario_name}` field `total_cells`"
    );
}

fn assert_float_field(scenario_name: &str, field: &str, actual: f64, expected: f64) {
    const ABSOLUTE_TOLERANCE: f64 = 0.01;
    assert!(
        (actual - expected).abs() <= ABSOLUTE_TOLERANCE,
        "scenario `{scenario_name}` field `{field}`: expected {expected} ± {ABSOLUTE_TOLERANCE}, got {actual}"
    );
}
