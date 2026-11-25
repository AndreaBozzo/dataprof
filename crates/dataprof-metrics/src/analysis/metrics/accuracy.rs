//! Accuracy Dimension (ISO 25012)
//!
//! Measures the correctness of data values (syntactic and semantic accuracy).
//! Key metrics: outlier ratio, range violations, negative values in positive fields.

use super::utils::{calculate_percentile, has_identifier_word};
use crate::analysis::inference::{LexicalTally, is_null_like_token};
use crate::core::config::IsoQualityConfig;
use crate::core::errors::DataProfilerError;
use crate::stats::numeric::ExactTukey;
use crate::stats::{ExactDecimal, ParsedNumber};
use crate::types::{ColumnProfile, ColumnStats, DataType};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Number of disjoint real-line regions partitioned by the exact accuracy rule
/// bounds `{0, 100, 150, 1900, 2100}`.
pub(crate) const RULE_REGION_COUNT: usize = 6;

const REGION_BELOW_ZERO: usize = 0; // x < 0
const REGION_0_TO_100: usize = 1; // 0 <= x <= 100
const REGION_100_TO_150: usize = 2; // 100 < x <= 150
const REGION_150_TO_1900: usize = 3; // 150 < x < 1900
const REGION_1900_TO_2100: usize = 4; // 1900 <= x <= 2100
const REGION_ABOVE_2100: usize = 5; // x > 2100

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

    /// Calculate accuracy dimension metrics with range and sign rules read from
    /// exact per-column [`LexicalTally`] counters over every scanned value
    /// (#788 step 3), while outlier fences continue to read `data`.
    pub fn calculate_with_tallies(
        &self,
        data: &HashMap<String, Vec<String>>,
        tallies: &HashMap<String, LexicalTally>,
        column_profiles: &[ColumnProfile],
        positive_columns: &[String],
    ) -> Result<AccuracyMetrics, DataProfilerError> {
        let (outliers, outlier_values_checked) = self.count_outliers(data, column_profiles)?;
        let outlier_ratio = if outlier_values_checked == 0 {
            0.0
        } else {
            (outliers as f64 / outlier_values_checked as f64) * 100.0
        };
        let rules = column_profiles
            .iter()
            .filter(|profile| profile.data_type != DataType::Nested)
            .filter_map(|profile| {
                let tally = tallies.get(&profile.name)?;
                let positive = is_positive(&profile.name, positive_columns);
                Some(rule_counts_from_regions(
                    &profile.name,
                    tally.rule_regions(),
                    positive,
                ))
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
            let Some(column_data) = data.get(&profile.name) else {
                continue;
            };
            let Some(values) = self.outlier_test_values(profile, column_data) else {
                continue;
            };
            let sorted = sorted_numbers(&values);
            // A column whose quartiles coincide has no spread to fence: every
            // value off that one quartile would be an outlier, so a column of
            // 75% zeros and 25% fives would fail a quarter of its values. It
            // is not tested, as a column below `outlier_min_samples` is not
            // (#1304).
            if quartiles_coincide(&sorted) {
                continue;
            }

            total_outliers += self.count_column_outliers(&values, &sorted);
            total_numeric_values += values.len();
        }

        Ok((total_outliers, total_numeric_values))
    }

    /// The numbers of `column_data` the outlier test reads, or `None` when
    /// the column is not numeric or holds fewer than `outlier_min_samples`
    /// of them. Read as the column's statistics read them, so a value held
    /// exactly there is held exactly here (#1181).
    pub(crate) fn outlier_test_values(
        &self,
        profile: &ColumnProfile,
        column_data: &[String],
    ) -> Option<Vec<ParsedNumber>> {
        if !matches!(profile.data_type, DataType::Integer | DataType::Float) {
            return None;
        }
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
        (values.len() >= self.thresholds.outlier_min_samples).then_some(values)
    }

    /// Whether a scan of `population` non-null values of `profile` may run
    /// the outlier test: the column is numeric and the source holds enough
    /// values to reach `outlier_min_samples`, whatever its sample holds. A
    /// minimum above the sample size leaves the sample untested while the
    /// source is tested.
    pub(crate) fn may_test_source(&self, profile: &ColumnProfile, population: usize) -> bool {
        matches!(profile.data_type, DataType::Integer | DataType::Float)
            && population >= self.thresholds.outlier_min_samples
    }

    /// The fewest and the most of `values` outside the Tukey fences drawn
    /// from the quartiles `q1` and `q3`, which need not be the quartiles of
    /// `values`: the score bounds count a sample against the widest and the
    /// narrowest fences the source's quartiles allow (#1304).
    ///
    /// A scan compares by exact differences when any value is held exactly,
    /// as [`count_column_outliers`](Self::count_column_outliers) does. A
    /// sample holding such a value settles that for the source. A sample of
    /// floats alone does not: the source may hold an exact value the sample
    /// missed. Both comparisons count then, and the range covers them.
    pub(crate) fn count_outside_fences(
        &self,
        values: &[ParsedNumber],
        q1: ParsedNumber,
        q3: ParsedNumber,
    ) -> (usize, usize) {
        let exact = ExactTukey::from_quartiles(&q1, &q3, self.thresholds.outlier_iqr_multiplier)
            .count_outliers(values);
        if values
            .iter()
            .any(|number| !matches!(number, ParsedNumber::Float(_)))
        {
            return (exact, exact);
        }
        let nearest = self.count_outside_nearest_fences(values, q1, q3);
        (exact.min(nearest), exact.max(nearest))
    }

    /// Values outside the fences drawn from `q1` and `q3`, each compared
    /// through its nearest `f64`.
    fn count_outside_nearest_fences(
        &self,
        values: &[ParsedNumber],
        q1: ParsedNumber,
        q3: ParsedNumber,
    ) -> usize {
        let k = self.thresholds.outlier_iqr_multiplier;
        let (q1, q3) = (q1.to_f64(), q3.to_f64());
        let iqr = q3 - q1;
        let (lower_bound, upper_bound) = (q1 - k * iqr, q3 + k * iqr);
        values
            .iter()
            .map(|number| number.to_f64())
            .filter(|&value| value < lower_bound || value > upper_bound)
            .count()
    }

    /// Count one column's outliers as its `outlier_count` does: a column
    /// holding a value exactly, an integer past 2^53 or a decimal past `f64`,
    /// takes its fences from exact differences between values, where the
    /// `f64` fences snap to a grid coarser than its spread (#1181). `sorted`
    /// is `values` in ascending order.
    fn count_column_outliers(&self, values: &[ParsedNumber], sorted: &[ParsedNumber]) -> usize {
        let wide = values
            .iter()
            .any(|number| !matches!(number, ParsedNumber::Float(_)));
        if wide && let Some(tukey) = ExactTukey::new(sorted, self.thresholds.outlier_iqr_multiplier)
        {
            return tukey.count_outliers(values);
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
        let mut regions = [0usize; RULE_REGION_COUNT];

        for value in values {
            let text = value.trim();
            if is_null_like_token(text) {
                continue;
            }
            let Some(nearest) = rule_value(text) else {
                continue;
            };
            regions[classify_rule_region(text, nearest)] += 1;
        }

        rule_counts_from_regions(column_name, regions, positive)
    }
}

/// `values` in ascending order, as the outlier test reads them.
pub(crate) fn sorted_numbers(values: &[ParsedNumber]) -> Vec<ParsedNumber> {
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.value_cmp(*b));
    sorted
}

/// Whether the first and third quartiles of `sorted` are one value, so the
/// IQR is 0. With Type 7 interpolation they are exactly when the value at or
/// below the first quartile's position equals the one at or above the
/// third's, so two values decide it and no interpolation is needed. Values
/// are compared by their difference, so `-0` and `0` are one value.
pub(crate) fn quartiles_coincide(sorted: &[ParsedNumber]) -> bool {
    let Some(last) = sorted.len().checked_sub(1) else {
        return false;
    };
    let below_q1 = last / 4;
    let above_q3 = (3 * last).div_ceil(4);
    sorted[above_q3].minus_value(sorted[below_q1]) == 0.0
}

/// Whether `profile`'s values are a decimal's renderings, read with
/// [`ParsedNumber::parse_decimal`] as its statistics read them: a `Float`
/// column whose source declared a decimal (#1268), or whose statistics hold
/// its order exactly, which a `Float` column's do only then. The declaration
/// decides a run without statistics the same way as one with them.
fn read_as_decimal(profile: &ColumnProfile) -> bool {
    profile.data_type == DataType::Float
        && (profile
            .native_type
            .as_ref()
            .is_some_and(|native| native.declares_decimal())
            || matches!(&profile.stats, ColumnStats::Numeric(stats) if stats.exact.is_some()))
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

/// Classify an exact `i64` into its accuracy rule region.
pub(crate) fn classify_rule_region_i64(value: i64) -> usize {
    if value < 0 {
        REGION_BELOW_ZERO
    } else if value <= 100 {
        REGION_0_TO_100
    } else if value <= 150 {
        REGION_100_TO_150
    } else if value < 1900 {
        REGION_150_TO_1900
    } else if value <= 2100 {
        REGION_1900_TO_2100
    } else {
        REGION_ABOVE_2100
    }
}

/// A `u64` above `i64::MAX` always lies strictly above 2100.
pub(crate) const fn classify_rule_region_u64() -> usize {
    REGION_ABOVE_2100
}

/// Classify a trimmed `f64` token into its accuracy rule region when
/// [`rule_value`] accepts it (finite, or a literal with an ASCII digit that
/// overflows `f64`), or `None` for spelled-out `inf`/`NaN`.
pub(crate) fn classify_rule_region_f64(text: &str, nearest: f64) -> Option<usize> {
    let accepted =
        nearest.is_finite() || (nearest.is_infinite() && text.bytes().any(|b| b.is_ascii_digit()));
    accepted.then(|| classify_rule_region(text, nearest))
}

/// Place a rule-checkable value `(text, nearest)` in one of the six disjoint
/// regions partitioned by `{0, 100, 150, 1900, 2100}`, comparing exact digits
/// whenever `nearest` lands on a bound.
fn classify_rule_region(text: &str, nearest: f64) -> usize {
    if nearest < 0.0 || (nearest == 0.0 && cmp_bound(text, nearest, 0).is_lt()) {
        REGION_BELOW_ZERO
    } else if nearest < 100.0 || (nearest == 100.0 && !cmp_bound(text, nearest, 100).is_gt()) {
        REGION_0_TO_100
    } else if nearest < 150.0 || (nearest == 150.0 && !cmp_bound(text, nearest, 150).is_gt()) {
        REGION_100_TO_150
    } else if nearest < 1900.0 || (nearest == 1900.0 && cmp_bound(text, nearest, 1900).is_lt()) {
        REGION_150_TO_1900
    } else if nearest < 2100.0 || (nearest == 2100.0 && !cmp_bound(text, nearest, 2100).is_gt()) {
        REGION_1900_TO_2100
    } else {
        REGION_ABOVE_2100
    }
}

/// Whether a column name triggers any name-based accuracy range rule (`age`,
/// percentage, `count`, `year`), independent of semantic hints.
pub fn is_accuracy_ruled_column(column_name: &str) -> bool {
    RangeRules::for_column(column_name).applies(false)
}

/// Evaluate the name-based range rules and, when `positive`, the sign rule
/// over a column's six region counts.
pub(crate) fn rule_counts_from_regions(
    column_name: &str,
    regions: [usize; RULE_REGION_COUNT],
    positive: bool,
) -> RuleCounts {
    let numeric_values: usize = regions.iter().sum();
    let rules = RangeRules::for_column(column_name);
    if !rules.applies(positive) || numeric_values == 0 {
        return RuleCounts {
            numeric_values,
            ..RuleCounts::default()
        };
    }

    let mut range_violations = 0usize;
    let mut violating_values = 0usize;
    for (region, count) in regions.into_iter().enumerate() {
        if count == 0 {
            continue;
        }
        let below_zero = region == REGION_BELOW_ZERO;
        // Age should be reasonable (0-150)
        let outside_age = below_zero || region >= REGION_150_TO_1900;
        // Percentage should be 0-100
        let outside_percent = below_zero || region >= REGION_100_TO_150;
        // Years should be reasonable (1900-2100)
        let outside_year = region != REGION_1900_TO_2100;

        let range_failures = usize::from(rules.age && outside_age)
            + usize::from(rules.percent && outside_percent)
            + usize::from(rules.count && below_zero)
            + usize::from(rules.year && outside_year);
        let negative = positive && below_zero;

        range_violations += count * range_failures;
        if range_failures > 0 || negative {
            violating_values += count;
        }
    }

    RuleCounts {
        numeric_values,
        range_violations,
        negative_values: if positive {
            regions[REGION_BELOW_ZERO]
        } else {
            0
        },
        rule_values: numeric_values,
        violating_values,
    }
}

/// What the range and sign rules found in one column, or summed over several.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuleCounts {
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
            zero_padded_count: None,
            pattern_values_checked: None,
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
    fn a_column_whose_quartiles_coincide_is_not_tested_for_outliers() {
        // 75% zeros: Q1 and Q3 are both 0, so the fences sit on 0 and every
        // five would be an outlier. One zero fewer moves Q3 to 5 and nothing
        // is. The first column is left out of the test, the second tested
        // (#1304). Wide integers past 2^53 take the exact path and agree.
        let thresholds = IsoQualityConfig::default();
        let calc = AccuracyCalculator::new(&thresholds);
        let column = |zeros: usize, base: i128| -> Vec<String> {
            (0..400)
                .map(|index| (base + if index < zeros { 0 } else { 5 }).to_string())
                .collect()
        };
        let mut wrong = Vec::new();
        for base in [0, 1 << 60] {
            for (zeros, tested) in [(301, false), (299, true)] {
                let data = HashMap::from([("v".to_string(), column(zeros, base))]);
                let metrics = calc
                    .calculate(&data, &[numeric_profile("v")])
                    .expect("accuracy metrics should be computed");
                let expected = if tested { (0, 400) } else { (0, 0) };
                if (metrics.outliers, metrics.outlier_values_checked) != expected {
                    wrong.push(format!(
                        "base {base}, {zeros} zeros: {} of {}",
                        metrics.outliers, metrics.outlier_values_checked
                    ));
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
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

    #[test]
    fn rule_regions_and_tallies_match_check_value_rules_on_every_boundary() {
        let values: Vec<String> = [
            "-1e400",
            "-1e-700",
            "-1e-400",
            "-1",
            "-0.0",
            "0",
            "1e-700",
            "50",
            "100",
            "100.000000000000000001",
            "150",
            "150.000000000000000001",
            "1899.999999999999999999",
            "1900",
            "2024",
            "2100",
            "2100.000000000000000001",
            "1e400",
            &i64::MIN.to_string(),
            &i64::MAX.to_string(),
            &u64::MAX.to_string(),
            "inf",
            "-Infinity",
            "NaN",
            "null",
            "abc",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        let tally = LexicalTally::from_values(&values);
        // Hardcoded exact region counts, independent of `check_value_rules`:
        // r0 (x < 0): -1e400, -1e-700, -1e-400, -1, i64::MIN = 5
        // r1 (0 <= x <= 100): -0.0, 0, 1e-700, 50, 100 = 5
        // r2 (100 < x <= 150): 100.000000000000000001, 150 = 2
        // r3 (150 < x < 1900): 150.000000000000000001, 1899.999999999999999999 = 2
        // r4 (1900 <= x <= 2100): 1900, 2024, 2100 = 3
        // r5 (x > 2100): 2100.000000000000000001, 1e400, i64::MAX, u64::MAX = 4
        assert_eq!(tally.rule_regions(), [5, 5, 2, 2, 3, 4]);

        for (name, positive, expected_violating, expected_range, expected_negative) in [
            ("customer_age", false, 14, 14, 0),
            ("discount_percent", false, 16, 16, 0),
            ("discount_percent", true, 16, 16, 5),
            ("item_count", false, 5, 5, 0),
            ("birth_year", false, 18, 18, 0),
            ("balance", true, 5, 0, 5),
            ("price", false, 0, 0, 0),
        ] {
            let direct = AccuracyCalculator::check_value_rules(name, &values, positive);
            let from_tally = rule_counts_from_regions(name, tally.rule_regions(), positive);
            assert_eq!(from_tally, direct, "column={name} positive={positive}");
            assert_eq!(
                (
                    from_tally.violating_values,
                    from_tally.range_violations,
                    from_tally.negative_values,
                ),
                (expected_violating, expected_range, expected_negative),
                "hardcoded counts: column={name} positive={positive}"
            );
        }
    }
}
