//! Validity dimension.
//!
//! Measures conformance to a confidently detected semantic pattern. Columns
//! without a dominant pattern are left unassessed rather than assumed valid.

use crate::analysis::inference::is_null_like_token;
use crate::types::ColumnProfile;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub(crate) struct ValidityMetrics {
    pub valid_values_ratio: f64,
    pub invalid_values: usize,
    pub values_checked: usize,
}

pub(crate) struct ValidityCalculator;

impl ValidityCalculator {
    pub fn calculate(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> ValidityMetrics {
        let mut valid_values = 0;
        let mut values_checked = 0;

        for profile in column_profiles {
            let Some(patterns) = profile.patterns.as_ref() else {
                continue;
            };
            let Some(pattern) = patterns
                .iter()
                .filter(|pattern| pattern.confidence >= 0.5)
                .max_by(|left, right| {
                    left.confidence
                        .total_cmp(&right.confidence)
                        .then_with(|| left.match_count.cmp(&right.match_count))
                        .then_with(|| right.name.cmp(&left.name))
                })
            else {
                continue;
            };
            let Some(values) = data.get(&profile.name) else {
                continue;
            };

            let non_null = values
                .iter()
                .filter(|value| !is_null_like_token(value.trim()))
                .count();
            if non_null == 0 {
                continue;
            }

            values_checked += non_null;
            valid_values += pattern.match_count.min(non_null);
        }

        let invalid_values = values_checked.saturating_sub(valid_values);
        let valid_values_ratio = if values_checked == 0 {
            100.0
        } else {
            valid_values as f64 / values_checked as f64 * 100.0
        };

        ValidityMetrics {
            valid_values_ratio,
            invalid_values,
            values_checked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnStats, DataType, Pattern, PatternCategory};

    fn profile(patterns: Option<Vec<Pattern>>) -> ColumnProfile {
        ColumnProfile {
            name: "email".to_string(),
            data_type: DataType::String,
            null_count: 0,
            total_count: 4,
            unique_count: Some(4),
            unique_count_is_approximate: Some(false),
            invalid_count: None,
            stats: ColumnStats::None,
            patterns,
        }
    }

    #[test]
    fn confidently_detected_pattern_drives_validity() {
        let data = HashMap::from([(
            "email".to_string(),
            vec![
                "a@example.com".to_string(),
                "b@example.com".to_string(),
                "not-an-email".to_string(),
                "".to_string(),
            ],
        )]);
        let patterns = vec![Pattern {
            name: "Email".to_string(),
            regex: String::new(),
            match_count: 2,
            match_percentage: 66.67,
            category: PatternCategory::Contact,
            confidence: 0.9,
        }];

        let metrics = ValidityCalculator::calculate(&data, &[profile(Some(patterns))]);

        assert_eq!(metrics.values_checked, 3);
        assert_eq!(metrics.invalid_values, 1);
        assert!((metrics.valid_values_ratio - 66.666).abs() < 0.01);
    }

    #[test]
    fn missing_or_weak_pattern_is_not_assessed() {
        let data = HashMap::from([("email".to_string(), vec!["x".to_string()])]);
        let weak = Pattern {
            name: "Email".to_string(),
            regex: String::new(),
            match_count: 1,
            match_percentage: 10.0,
            category: PatternCategory::Contact,
            confidence: 0.2,
        };

        assert_eq!(
            ValidityCalculator::calculate(&data, &[profile(None)]).values_checked,
            0
        );
        assert_eq!(
            ValidityCalculator::calculate(&data, &[profile(Some(vec![weak]))]).values_checked,
            0
        );
    }
}
