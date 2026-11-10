//! Accuracy Dimension (ISO 25012)
//!
//! Measures the correctness of data values (syntactic and semantic accuracy).
//! Key metrics: outlier ratio, range violations, negative values in positive fields.

use super::utils::{calculate_percentile, has_identifier_word};
use crate::analysis::inference::is_null_like_token;
use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use crate::stats::numeric::ExactTukey;
use crate::stats::{ExactDecimal, ParsedNumber};
use crate::types::{ColumnProfile, ColumnStats, DataType};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Accuracy metrics container
#[derive(Debug)]
pub(crate) struct AccuracyMetrics {
    pub outlier_ratio: f64,
    pub range_violations: usize,
    pub negative_values_in_positive: usize,
    pub numeric_values_checked: usize,
    /// Numeric values of columns under a range or sign rule.
    pub rule_values_checked: usize,
    /// Of `rule_values_checked`, those breaking at least one rule.
    pub rule_violating_values: usize,
    /// Values outside the Tukey fences: the count behind `outlier_ratio`.
    pub outliers: usize,
    /// Numeric values in columns with enough of them for an outlier test:
    /// the denominator of `outlier_ratio`.
    pub outlier_values_checked: usize,
}

/// Which name-based range rules apply to a column.
///
/// A rule applies when a whole word of the name, split as identifier names are,
/// is one of the rule's words: `age` and `customer_age` are held to ages, while
/// `average_price`, `mileage` and `page_views` are not; `discount_percent` is a
/// percentage, `migrated_rows` is not; `item_count` must be non-negative,
/// `discount` and `account_number` need not be (#871). Concatenated lowercase
/// names such as `itemcount` no longer match. `years` is not a year word, since
/// a plural names a duration (`years_of_service`), not a calendar year.
///
/// A rule applies only where the name pins the unit its range is in (#880).
/// `rate` is not a percentage word: `sample_rate`, `heart_rate` and
/// `frame_rate` are rates in hertz, beats and frames. An `age` is in years
/// unless another word of the name is a smaller unit, as in
/// `account_age_days`.
///
/// The score bounds count each column through the same calculator, so they
/// cannot disagree with the counter on which rules a column is under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeRules {
    age: bool,
    percent: bool,
    count: bool,
    year: bool,
}

/// Time units an `age` can be counted in other than years.
const AGE_UNITS_BELOW_YEARS: [&str; 23] = [
    "ms",
    "millis",
    "milliseconds",
    "s",
    "sec",
    "secs",
    "second",
    "seconds",
    "min",
    "mins",
    "minute",
    "minutes",
    "h",
    "hr",
    "hrs",
    "hour",
    "hours",
    "day",
    "days",
    "week",
    "weeks",
    "month",
    "months",
];

impl RangeRules {
    fn for_column(column_name: &str) -> Self {
        Self {
            age: has_identifier_word(column_name, &["age"])
                && !has_identifier_word(column_name, &AGE_UNITS_BELOW_YEARS),
            percent: has_identifier_word(column_name, &["percent", "percentage", "pct"]),
            count: has_identifier_word(column_name, &["count", "counts"]),
            year: has_identifier_word(column_name, &["year"]),
        }
    }

    /// Whether any rule applies: a range rule from the name, or the sign rule
    /// when the column is hinted `positive`.
    fn applies(self, positive: bool) -> bool {
        self.age || self.percent || self.count || self.year || positive
    }
}

fn is_positive(column_name: &str, positive_columns: &[String]) -> bool {
    positive_columns
        .iter()
        .any(|candidate| candidate == column_name)
}

/// Calculator for accuracy dimension metrics
pub(crate) struct AccuracyCalculator<'a> {
    thresholds: &'a IsoQualityConfig,
}

impl<'a> AccuracyCalculator<'a> {
    pub fn new(thresholds: &'a IsoQualityConfig) -> Self {
        Self { thresholds }
    }

    /// Calculate accuracy dimension metrics
    pub fn calculate(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<AccuracyMetrics, DataProfilerError> {
        self.calculate_with_positive_columns(data, column_profiles, &[])
    }

    /// Calculate accuracy dimension metrics with explicit positive-only columns.
    pub fn calculate_with_positive_columns(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
        positive_columns: &[String],
    ) -> Result<AccuracyMetrics, DataProfilerError> {
        let (outliers, outlier_values_checked) = self.count_outliers(data, column_profiles)?;
        let outlier_ratio = if outlier_values_checked == 0 {
            0.0
        } else {
            (outliers as f64 / outlier_values_checked as f64) * 100.0
        };
        let rules = data
            .iter()
            .map(|(column_name, values)| {
                let positive = is_positive(column_name, positive_columns);
                Self::check_value_rules(column_name, values, positive)
            })
            .fold(RuleCounts::default(), RuleCounts::add);

        Ok(AccuracyMetrics {
            outlier_ratio,
            range_violations: rules.range_violations,
            negative_values_in_positive: rules.negative_values,
            numeric_values_checked: rules.numeric_values,
            rule_values_checked: rules.rule_values,
            rule_violating_values: rules.violating_values,
            outliers,
            outlier_values_checked,
        })
    }

    /// Whether a range or sign rule applies to `column_name`, so that its
    /// values are units of the rule component. The score bounds select
    /// columns by this rather than by the numbers a sample happened to hold:
    /// a ruled column whose sample held none can still hold violating values
    /// outside it.
    pub fn is_ruled(column_name: &str, positive_columns: &[String]) -> bool {
        RangeRules::for_column(column_name).applies(is_positive(column_name, positive_columns))
    }

    /// Count statistical outliers and the numeric values they were drawn from.
    fn count_outliers(
        &self,
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<(usize, usize), DataProfilerError> {
        let mut total_numeric_values = 0;
        let mut total_outliers = 0;

        for profile in column_profiles {
            if !matches!(profile.data_type, DataType::Integer | DataType::Float) {
                continue;
            }

            if let Some(column_data) = data.get(&profile.name) {
                // Read as the column's statistics read it, so a value held
                // exactly there is held exactly here (#1181).
                let parse = if read_as_decimal(profile) {
                    ParsedNumber::parse_decimal
                } else {
                    ParsedNumber::parse
                };
                let values: Vec<ParsedNumber> = column_data
                    .iter()
                    .filter(|v| !is_null_like_token(v.trim()))
                    .filter_map(|v| parse(v))
                    .collect();

                if values.len() < self.thresholds.outlier_min_samples {
                    continue;
                }

                total_outliers += self.count_column_outliers(&values);
                total_numeric_values += values.len();
            }
        }

        Ok((total_outliers, total_numeric_values))
    }

    /// Count one column's outliers as its `outlier_count` does: a column
    /// holding a value exactly, an integer past 2^53 or a decimal past `f64`,
    /// takes its fences from exact differences between values, where the
    /// `f64` fences snap to a grid coarser than its spread (#1181).
    fn count_column_outliers(&self, values: &[ParsedNumber]) -> usize {
        let wide = values
            .iter()
            .any(|number| !matches!(number, ParsedNumber::Float(_)));
        if wide {
            let mut sorted = values.to_vec();
            sorted.sort_by(|a, b| a.value_cmp(*b));
            if let Some(tukey) = ExactTukey::new(&sorted, self.thresholds.outlier_iqr_multiplier) {
                return tukey.count_outliers(values);
            }
        }
        let numeric: Vec<f64> = values.iter().map(|number| number.to_f64()).collect();
        self.count_outliers_preparsed(&numeric)
    }

    /// Count IQR outliers in a pre-parsed numeric vector using the Tukey rule
    /// (`Q1 − k·IQR`, `Q3 + k·IQR` with configurable `k`, default 1.5).
    ///
    /// Operates on pre-parsed `f64` to avoid a second `parse::<f64>()` pass
    /// on every value — the caller already paid that cost while counting.
    fn count_outliers_preparsed(&self, numeric_values: &[f64]) -> usize {
        if numeric_values.len() < self.thresholds.outlier_min_samples {
            return 0;
        }

        let mut sorted = numeric_values.to_vec();
        // Safe total ordering: NaN/Infinity values are placed at the end (IEEE 754)
        sorted.sort_by(|a, b| a.total_cmp(b));

        let q1 = calculate_percentile(&sorted, 25.0);
        let q3 = calculate_percentile(&sorted, 75.0);
        let iqr = q3 - q1;

        let k = self.thresholds.outlier_iqr_multiplier;
        let lower_bound = q1 - k * iqr;
        let upper_bound = q3 + k * iqr;

        numeric_values
            .iter()
            .filter(|&&value| value < lower_bound || value > upper_bound)
            .count()
    }

    /// Check one column's numeric values against the name-based range rules
    /// and, when `positive`, the sign rule, each value compared with a bound
    /// exactly (#1181).
    ///
    /// The rule component's unit is a value of a column under at least one
    /// rule, counted once however many rules it breaks (#1005): a negative
    /// `discount_percent` hinted positive is one violating value, though both
    /// the percentage and the sign rule count it as a failure. Values of
    /// columns under no rule are numeric values, but no rule's units.
    fn check_value_rules(column_name: &str, values: &[String], positive: bool) -> RuleCounts {
        let rules = RangeRules::for_column(column_name);
        let ruled = rules.applies(positive);
        let mut counts = RuleCounts::default();

        for value in values {
            let text = value.trim();
            if is_null_like_token(text) {
                continue;
            }
            let Some(nearest) = rule_value(text) else {
                continue;
            };
            counts.numeric_values += 1;
            if !ruled {
                continue;
            }
            counts.rule_values += 1;

            let outside = |low: i32, high: i32| {
                cmp_bound(text, nearest, low).is_lt() || cmp_bound(text, nearest, high).is_gt()
            };
            let below_zero = || cmp_bound(text, nearest, 0).is_lt();
            let range_failures = [
                // Age should be reasonable (0-150)
                rules.age && outside(0, 150),
                // Percentage should be 0-100
                rules.percent && outside(0, 100),
                // Counts should be non-negative
                rules.count && below_zero(),
                // Years should be reasonable (1900-2100)
                rules.year && outside(1900, 2100),
            ]
            .into_iter()
            .filter(|failed| *failed)
            .count();
            let negative = positive && below_zero();

            counts.range_violations += range_failures;
            counts.negative_values += usize::from(negative);
            counts.violating_values += usize::from(range_failures > 0 || negative);
        }

        counts
    }
}

/// Whether `profile`'s statistics read its values as a decimal's renderings,
/// with [`ParsedNumber::parse_decimal`]: a `Float` column reports exact order
/// statistics only then. A decimal column whose exact form was dropped reads
/// as text, as before #1181.
fn read_as_decimal(profile: &ColumnProfile) -> bool {
    profile.data_type == DataType::Float
        && matches!(&profile.stats, ColumnStats::Numeric(stats) if stats.exact.is_some())
}

/// The nearest `f64` of a cell the range and sign rules check: a finite
/// number, or a literal past the `f64` range, whose infinity still orders it
/// beyond every bound (#1181). `None` for a non-number and for `inf` and
/// `NaN` spelled out, which hold no digit. The `positive` hint binds on the
/// same cells.
pub(crate) fn rule_value(text: &str) -> Option<f64> {
    // decode-audit: no-data — a cell that does not parse is not a number,
    // and no rule applies to it.
    let value = text.parse::<f64>().ok()?;
    (value.is_finite() || (value.is_infinite() && text.bytes().any(|b| b.is_ascii_digit())))
        .then_some(value)
}

/// The order of the value `text` holds against `bound`, exactly. `nearest` is
/// its nearest `f64`; rounding never crosses a bound an `f64` holds, so only a
/// value that rounds onto the bound needs its digits: `100.000000000000000001`
/// lies above 100 and `-1e-400` below 0, though they read as `100.0` and
/// `-0.0`.
fn cmp_bound(text: &str, nearest: f64, bound: i32) -> Ordering {
    let at = f64::from(bound);
    if nearest != at {
        return nearest.total_cmp(&at);
    }
    if let Some(exact) = ExactDecimal::from_text(text) {
        return exact.exact_cmp(ExactDecimal::integer(i128::from(bound)));
    }
    // Too wide to hold: past 38 digits, or with a scale past 600. A value
    // rounding to 0 is still placed by its sign; one this close to another
    // bound counts as on it.
    let mantissa = text.split(['e', 'E']).next().unwrap_or(text);
    if bound != 0 || !mantissa.bytes().any(|b| matches!(b, b'1'..=b'9')) {
        return Ordering::Equal;
    }
    if text.starts_with('-') {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

/// What the range and sign rules found in one column, or summed over several.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct RuleCounts {
    /// Finite numeric values, in every column.
    numeric_values: usize,
    /// Range rule failures: a value breaking two range rules adds two.
    range_violations: usize,
    /// Negative values in positive-only columns.
    negative_values: usize,
    /// Finite numeric values of columns under at least one rule.
    rule_values: usize,
    /// Of `rule_values`, those breaking at least one rule.
    violating_values: usize,
}

impl RuleCounts {
    fn add(self, other: Self) -> Self {
        Self {
            numeric_values: self.numeric_values + other.numeric_values,
            range_violations: self.range_violations + other.range_violations,
            negative_values: self.negative_values + other.negative_values,
            rule_values: self.rule_values + other.rule_values,
            violating_values: self.violating_values + other.violating_values,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnStats;

    fn numeric_profile(name: &str) -> ColumnProfile {
        ColumnProfile {
            name: name.to_string(),
            data_type: DataType::Float,
            null_count: 0,
            total_count: 0,
            unique_count: None,
            unique_count_is_approximate: None,
            unique_count_lower_bound: None,
            invalid_count: None,
            type_homogeneity: None,
            locale_number_count: None,
            native_type: None,
            stats: ColumnStats::None,
            patterns: Some(vec![]),
        }
    }

    #[test]
    fn test_outlier_ratio_uses_iso_min_samples_not_generic_30() {
        let thresholds = IsoQualityConfig::default();
        let calc = AccuracyCalculator::new(&thresholds);
        let data = HashMap::from([(
            "temperature".to_string(),
            vec![
                "22.5".to_string(),
                "23.1".to_string(),
                "22.8".to_string(),
                "999.9".to_string(),
                "23.2".to_string(),
                "22.9".to_string(),
                "23.0".to_string(),
                "22.7".to_string(),
                "23.1".to_string(),
            ],
        )]);
        let profiles = vec![numeric_profile("temperature")];

        let metrics = calc
            .calculate(&data, &profiles)
            .expect("accuracy metrics should be computed");

        assert!(
            metrics.outlier_ratio > 0.0,
            "small numeric samples above outlier_min_samples should still detect obvious outliers"
        );
    }

    #[test]
    fn range_rules_match_words_not_substrings() {
        let rules = |age, percent, count, year| RangeRules {
            age,
            percent,
            count,
            year,
        };
        for (name, expected) in [
            ("age", rules(true, false, false, false)),
            ("customer_age", rules(true, false, false, false)),
            ("customerAge", rules(true, false, false, false)),
            ("discount_percent", rules(false, true, false, false)),
            ("discount_percentage", rules(false, true, false, false)),
            ("conversion_rate_pct", rules(false, true, false, false)),
            ("account_age_years", rules(true, false, false, false)),
            ("item_count", rules(false, false, true, false)),
            ("item_counts", rules(false, false, true, false)),
            ("birth_year", rules(false, false, false, true)),
            ("average_price", rules(false, false, false, false)),
            ("mileage", rules(false, false, false, false)),
            ("page_views", rules(false, false, false, false)),
            ("migrated_rows", rules(false, false, false, false)),
            ("generated_tokens", rules(false, false, false, false)),
            ("discount", rules(false, false, false, false)),
            ("account_number", rules(false, false, false, false)),
            ("years_of_service", rules(false, false, false, false)),
            // #880: a rate is not a percentage, and an age in days is not in years.
            ("conversion_rate", rules(false, false, false, false)),
            ("conversion_rates", rules(false, false, false, false)),
            ("sample_rate", rules(false, false, false, false)),
            ("heart_rate", rules(false, false, false, false)),
            ("frame_rate", rules(false, false, false, false)),
            ("account_age_days", rules(false, false, false, false)),
            ("session_age_ms", rules(false, false, false, false)),
            ("cacheAgeHours", rules(false, false, false, false)),
            ("account_age_month", rules(false, false, false, false)),
        ] {
            assert_eq!(RangeRules::for_column(name), expected, "{name}");
        }
    }

    /// Each rule counts exactly the values outside its range, bounds included
    /// in the range, and a name under no rule counts none.
    #[test]
    fn each_range_rule_counts_only_values_outside_its_range() {
        for (name, values, expected) in [
            ("customer_age", ["0", "150", "30", "-1"], 1),
            ("discount_percent", ["0", "100", "50", "101"], 1),
            ("item_count", ["0", "5", "-1", "-2"], 2),
            ("birth_year", ["1900", "2100", "1990", "1899"], 1),
            ("price", ["-1", "200", "3000", "101"], 0),
        ] {
            let values: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            let counts = AccuracyCalculator::check_value_rules(name, &values, false);
            assert_eq!(
                (counts.range_violations, counts.numeric_values),
                (expected, 4),
                "{name}"
            );
        }
    }

    /// A value is one unit of the rule component however many rules it
    /// breaks, and a column under no rule adds no units (#1005).
    #[test]
    fn rule_units_are_values_of_ruled_columns_counted_once() {
        let values: Vec<String> = ["-5", "50", "150", "NaN", "inf", "n/a"]
            .iter()
            .map(|v| v.to_string())
            .collect();
        let counts =
            |name, positive| AccuracyCalculator::check_value_rules(name, &values, positive);

        // -5 breaks the percentage and the sign rule, 150 the percentage rule.
        assert_eq!(
            counts("discount_percent", true),
            RuleCounts {
                numeric_values: 3,
                range_violations: 2,
                negative_values: 1,
                rule_values: 3,
                violating_values: 2,
            }
        );
        // A redundant rule finds no value the others missed, so it moves
        // neither side of the component.
        let without_sign = counts("discount_percent", false);
        assert_eq!(
            (without_sign.rule_values, without_sign.violating_values),
            (3, 2)
        );
        // Only the sign rule applies: one unit per value, one violation.
        let sign_only = counts("balance", true);
        assert_eq!((sign_only.rule_values, sign_only.violating_values), (3, 1));
        assert_eq!(
            counts("price", false),
            RuleCounts {
                numeric_values: 3,
                ..RuleCounts::default()
            }
        );
    }

    /// Clean numeric columns beside a broken rule column change neither the
    /// rule component's units nor its violations (#1005).
    #[test]
    fn unruled_columns_do_not_dilute_the_rule_component() {
        let thresholds = IsoQualityConfig::default();
        let calc = AccuracyCalculator::new(&thresholds);
        let column = |values: &[&str]| values.iter().map(|v| v.to_string()).collect::<Vec<_>>();
        let broken = ("discount_percent".to_string(), column(&["150", "200"]));
        let alone = HashMap::from([broken.clone()]);
        let beside = HashMap::from([broken, ("price".to_string(), column(&["1", "2", "3"]))]);
        let profiles = vec![
            numeric_profile("discount_percent"),
            numeric_profile("price"),
        ];

        let alone = calc.calculate(&alone, &profiles).unwrap();
        let beside = calc.calculate(&beside, &profiles).unwrap();
        assert_eq!(
            (alone.rule_values_checked, alone.rule_violating_values),
            (2, 2)
        );
        assert_eq!(
            (beside.rule_values_checked, beside.rule_violating_values),
            (2, 2)
        );
        assert_eq!(
            (alone.numeric_values_checked, beside.numeric_values_checked),
            (2, 5)
        );
    }

    /// A value that reads as a bound, or past the `f64` range, is placed by
    /// its digits (#1181): `-1e-700` has a scale no exact decimal holds and
    /// is placed by its sign.
    #[test]
    fn rules_compare_values_with_their_bounds_exactly() {
        let counts = |name, values: &[&str]| {
            let values: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            AccuracyCalculator::check_value_rules(name, &values, true)
        };
        let balance = counts("balance", &["-1e-400", "-1e-700", "0", "-0", "1e-700"]);
        assert_eq!((balance.numeric_values, balance.negative_values), (5, 2));
        let percent = counts(
            "discount_percent",
            &["100.000000000000000001", "1e400", "100"],
        );
        assert_eq!((percent.rule_values, percent.range_violations), (3, 2));
        // `inf` and `NaN` spelled out hold no number.
        assert_eq!(
            counts("balance", &["inf", "-Infinity", "NaN"]).numeric_values,
            0
        );
    }

    #[test]
    fn test_negative_values_require_positive_column_hint() {
        let thresholds = IsoQualityConfig::default();
        let calc = AccuracyCalculator::new(&thresholds);
        let data = HashMap::from([(
            "pressure".to_string(),
            vec![
                " 101325".to_string(),
                " -500 ".to_string(),
                "100900 ".to_string(),
                " -inf ".to_string(),
                "NaN".to_string(),
                "inf".to_string(),
            ],
        )]);
        let profiles = vec![numeric_profile("pressure")];

        let without_hint = calc
            .calculate(&data, &profiles)
            .expect("accuracy metrics should compute");
        assert_eq!(without_hint.negative_values_in_positive, 0);

        let with_hint = calc
            .calculate_with_positive_columns(&data, &profiles, &["pressure".to_string()])
            .expect("accuracy metrics should compute");
        assert_eq!(with_hint.negative_values_in_positive, 1);
        assert_eq!(with_hint.numeric_values_checked, 3);
    }
}
