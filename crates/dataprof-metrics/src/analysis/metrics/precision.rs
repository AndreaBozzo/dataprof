//! Precision dimension.
//!
//! Measures consistency of effective decimal scale within floating-point
//! columns. It does not infer a business-required scale; it reports deviation
//! from each column's dominant observed scale.

use crate::analysis::inference::is_null_like_token;
use crate::types::{ColumnProfile, DataType};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Default)]
pub(crate) struct PrecisionMetrics {
    pub decimal_places_consistency: f64,
    pub inconsistent_precision_values: usize,
    pub numeric_values_checked: usize,
}

pub(crate) struct PrecisionCalculator;

impl PrecisionCalculator {
    pub fn calculate(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> PrecisionMetrics {
        let mut numeric_values_checked = 0;
        let mut inconsistent_precision_values = 0;

        for profile in column_profiles {
            if profile.data_type != DataType::Float {
                continue;
            }
            let Some(values) = data.get(&profile.name) else {
                continue;
            };

            let mut scales = BTreeMap::<usize, usize>::new();
            for value in values {
                let trimmed = value.trim();
                if is_null_like_token(trimmed) {
                    continue;
                }
                let Ok(number) = trimmed.parse::<f64>() else {
                    continue;
                };
                if !number.is_finite() {
                    continue;
                }
                if let Some(scale) = decimal_scale(trimmed) {
                    *scales.entry(scale).or_default() += 1;
                }
            }

            let column_count = scales.values().sum::<usize>();
            let dominant_count = scales.values().copied().max().unwrap_or(0);
            numeric_values_checked += column_count;
            inconsistent_precision_values += column_count.saturating_sub(dominant_count);
        }

        let decimal_places_consistency = if numeric_values_checked == 0 {
            100.0
        } else {
            (numeric_values_checked - inconsistent_precision_values) as f64
                / numeric_values_checked as f64
                * 100.0
        };

        PrecisionMetrics {
            decimal_places_consistency,
            inconsistent_precision_values,
            numeric_values_checked,
        }
    }
}

fn decimal_scale(value: &str) -> Option<usize> {
    let unsigned = value
        .strip_prefix('+')
        .or_else(|| value.strip_prefix('-'))
        .unwrap_or(value);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (unsigned, 0_i32),
    };
    let fractional_digits = mantissa
        .split_once('.')
        .map_or(0, |(_, fraction)| fraction.trim_end_matches('0').len());
    Some((fractional_digits as i32 - exponent).max(0) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnStats;

    fn float_profile() -> ColumnProfile {
        ColumnProfile {
            name: "amount".to_string(),
            data_type: DataType::Float,
            null_count: 0,
            total_count: 4,
            unique_count: Some(4),
            unique_count_is_approximate: Some(false),
            stats: ColumnStats::None,
            patterns: Some(vec![]),
        }
    }

    #[test]
    fn modal_decimal_scale_drives_precision_consistency() {
        let data = HashMap::from([(
            "amount".to_string(),
            vec![
                "1.20".to_string(),
                "2.30".to_string(),
                "3.45".to_string(),
                "not-a-number".to_string(),
            ],
        )]);

        let metrics = PrecisionCalculator::calculate(&data, &[float_profile()]);

        assert_eq!(metrics.numeric_values_checked, 3);
        assert_eq!(metrics.inconsistent_precision_values, 1);
        assert!((metrics.decimal_places_consistency - 66.666).abs() < 0.01);
    }

    #[test]
    fn scientific_notation_uses_effective_decimal_scale() {
        assert_eq!(decimal_scale("1.23e2"), Some(0));
        assert_eq!(decimal_scale("1.23e-2"), Some(4));
        assert_eq!(decimal_scale("-1.20"), Some(1));
    }
}
