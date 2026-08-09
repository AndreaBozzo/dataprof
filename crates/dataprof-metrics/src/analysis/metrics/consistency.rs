//! Consistency Dimension (ISO 8000-61)
//!
//! Measures the coherence and contradiction-free nature of data.
//! Key metrics: data type consistency, format violations, encoding issues.

use super::utils::{DATE_FORMAT_REGEXES, is_likely_date_column};
use crate::analysis::inference::{
    is_date_token, is_integer_token, is_null_like_token, parse_strict_boolean_token,
};
use crate::core::errors::DataProfilerError;
use crate::types::{ColumnProfile, DataType};
use std::collections::HashMap;

/// Consistency metrics container
#[derive(Debug)]
pub(crate) struct ConsistencyMetrics {
    pub data_type_consistency: f64,
    pub format_violations: usize,
    pub encoding_issues: usize,
    pub values_checked: usize,
}

/// Mutually exclusive lexical form of a single non-null value.
///
/// The variants partition non-null values — every value belongs to exactly one —
/// which is what lets the share held by the largest class stand in for
/// consistency on a column that has no inferred type of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LexicalClass {
    Numeric,
    Date,
    Boolean,
    Text,
}

/// [`LexicalClass`] indexed by discriminant, used to count without a `HashMap`
/// so that a tie between two classes resolves the same way on every run.
/// `lexical_class_order_matches_discriminants` pins the two together.
const LEXICAL_CLASS_ORDER: [LexicalClass; 4] = [
    LexicalClass::Numeric,
    LexicalClass::Date,
    LexicalClass::Boolean,
    LexicalClass::Text,
];

/// Classify one trimmed, non-null value.
///
/// The precedence matches the order `infer_type` tries types on whole columns,
/// so a column that just missed a type threshold reports the same share of
/// matching values that it reported while it still had that type. Integers and
/// fractions share `Numeric` deliberately: `["1.5", "2", "3"]` is one numeric
/// column, not a two-class mixture.
///
/// Dates use [`is_date_token`], the union of the forms inference and validation
/// each recognize. Using only the validation set would drop ISO datetimes and
/// dotted dates into `Text` alongside genuine junk, and a column of 70%
/// datetimes and 30% junk would report a perfect score again.
fn lexical_class(value: &str) -> LexicalClass {
    if is_integer_token(value) || value.parse::<f64>().is_ok() {
        LexicalClass::Numeric
    } else if is_date_token(value) {
        LexicalClass::Date
    } else if parse_strict_boolean_token(value).is_some() {
        LexicalClass::Boolean
    } else {
        LexicalClass::Text
    }
}

/// The class holding the most non-null values, or `None` when the column has no
/// non-null values to classify.
fn dominant_lexical_class(values: &[String]) -> Option<LexicalClass> {
    let mut counts = [0usize; LEXICAL_CLASS_ORDER.len()];
    for value in values {
        let trimmed = value.trim();
        if !is_null_like_token(trimmed) {
            counts[lexical_class(trimmed) as usize] += 1;
        }
    }

    // Strict `>` leaves the earliest-declared class holding a tie. Which class
    // wins does not change the reported share — both hold the same count — it
    // only keeps the answer stable across runs.
    let mut best: Option<(usize, usize)> = None;
    for (index, count) in counts.into_iter().enumerate() {
        if count > 0 && best.is_none_or(|(_, best_count)| count > best_count) {
            best = Some((index, count));
        }
    }

    best.map(|(index, _)| LEXICAL_CLASS_ORDER[index])
}

/// Calculator for consistency dimension metrics
pub(crate) struct ConsistencyCalculator;

impl ConsistencyCalculator {
    /// Calculate consistency dimension metrics
    pub fn calculate(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<ConsistencyMetrics, DataProfilerError> {
        let (data_type_consistency, values_checked) =
            Self::calculate_type_consistency(data, column_profiles)?;
        let format_violations = Self::count_format_violations(data)?;
        let encoding_issues = Self::detect_encoding_issues(data)?;

        Ok(ConsistencyMetrics {
            data_type_consistency,
            format_violations,
            encoding_issues,
            values_checked,
        })
    }

    /// Calculate data type consistency percentage and the number of non-null
    /// values checked.
    fn calculate_type_consistency(
        data: &HashMap<String, Vec<String>>,
        column_profiles: &[ColumnProfile],
    ) -> Result<(f64, usize), DataProfilerError> {
        let mut total_values = 0;
        let mut consistent_values = 0;

        for profile in column_profiles {
            if let Some(column_data) = data.get(&profile.name) {
                // Every value conforms to `String`, so scoring a string column
                // against its own type reports 100% for any mixture of forms and
                // erases the finding this metric exists to make: below the
                // inference thresholds a half-numeric column used to score a
                // perfect 100. Score those columns against their largest lexical
                // class instead, which reports ~50% for a half-numeric column,
                // leaves genuinely textual columns at 100%, and puts no
                // threshold in between to fall off.
                //
                // Two kinds of string column keep the older rule. `Identifier`
                // is only ever set by an explicit semantic hint, and identifier
                // schemes mix forms on purpose ("A1", "123"). A column whose
                // *name* announces dates is held to dates however its values
                // happen to classify.
                let dominant_class = if profile.data_type == DataType::String
                    && !is_likely_date_column(&profile.name)
                {
                    dominant_lexical_class(column_data)
                } else {
                    None
                };

                for value in column_data {
                    let trimmed = value.trim();
                    if is_null_like_token(trimmed) {
                        continue; // Skip null values in consistency check
                    }

                    total_values += 1;

                    // Check if value is consistent with inferred type. Dates use
                    // `is_date_token`, the same predicate that types a column as
                    // `Date` in the first place. Validating against a narrower
                    // set scored a column of clean ISO datetimes or dotted dates
                    // at 0%: inference typed it `Date` on forms the validation
                    // regexes did not accept, so every value in it failed.
                    let is_consistent = match profile.data_type {
                        DataType::Integer => is_integer_token(trimmed),
                        DataType::Float => trimmed.parse::<f64>().is_ok(),
                        DataType::Date => is_date_token(trimmed),
                        DataType::Boolean => parse_strict_boolean_token(trimmed).is_some(),
                        DataType::String | DataType::Identifier => match dominant_class {
                            Some(class) => lexical_class(trimmed) == class,
                            None => !is_likely_date_column(&profile.name) || is_date_token(trimmed),
                        },
                    };

                    if is_consistent {
                        consistent_values += 1;
                    }
                }
            }
        }

        if total_values == 0 {
            Ok((100.0, 0))
        } else {
            Ok((
                (consistent_values as f64 / total_values as f64) * 100.0,
                total_values,
            ))
        }
    }

    /// Count format violations (malformed dates, inconsistent formats)
    fn count_format_violations(
        data: &HashMap<String, Vec<String>>,
    ) -> Result<usize, DataProfilerError> {
        let mut violations = 0;

        for (column_name, values) in data {
            // Check for mixed date formats
            violations += Self::count_mixed_date_formats(column_name, values);

            // Check for other format inconsistencies
            violations += Self::count_other_format_violations(values);
        }

        Ok(violations)
    }

    /// Count mixed date formats within a column
    /// Uses pre-compiled regex patterns for optimal performance
    fn count_mixed_date_formats(column_name: &str, values: &[String]) -> usize {
        // Skip if column name doesn't suggest it contains dates
        if !is_likely_date_column(column_name) {
            return 0;
        }

        let mut format_counts = HashMap::new();

        // Use trim() for consistent whitespace handling
        let non_empty: Vec<&String> = values.iter().filter(|s| !s.trim().is_empty()).collect();

        let sample_size = 50.min(non_empty.len());

        for value in non_empty.iter().take(sample_size) {
            let trimmed = value.trim();
            for (format_name, regex) in DATE_FORMAT_REGEXES.iter() {
                if regex.is_match(trimmed) {
                    *format_counts.entry(*format_name).or_insert(0) += 1;
                    break;
                }
            }
        }

        // Return violation count if more than one format detected
        if format_counts.len() > 1 {
            format_counts.values().sum::<usize>() - format_counts.values().max().unwrap_or(&0)
        } else {
            0
        }
    }

    /// Count other format violations (e.g., inconsistent number formats)
    fn count_other_format_violations(values: &[String]) -> usize {
        // Track number format inconsistencies accurately
        let mut dot_decimal_count = 0;
        let mut comma_decimal_count = 0;
        let mut violations = 0;

        for value in values {
            if value.is_empty() {
                continue;
            }

            // Check for mixed decimal separators in same value (immediate violation)
            if value.contains('.') && value.contains(',') {
                violations += 1;
                continue;
            }

            // Count decimal separator usage patterns
            if value.contains('.') {
                // Check if it's likely a decimal separator (not thousands)
                let dot_count = value.chars().filter(|&c| c == '.').count();
                if dot_count == 1 {
                    dot_decimal_count += 1;
                }
            } else if value.contains(',') {
                // Single comma might be decimal separator (European format)
                let comma_count = value.chars().filter(|&c| c == ',').count();
                if comma_count == 1 {
                    comma_decimal_count += 1;
                }
            }
        }

        // If both formats are used significantly, count the minority as violations
        if dot_decimal_count > 0 && comma_decimal_count > 0 {
            // Count the less common format as violations (indicates inconsistency)
            violations += dot_decimal_count.min(comma_decimal_count);
        }

        violations
    }

    /// Detect UTF-8 encoding issues.
    ///
    /// Counts each affected value once, even when it shows several symptoms,
    /// so the count stays comparable to `values_checked`.
    fn detect_encoding_issues(
        data: &HashMap<String, Vec<String>>,
    ) -> Result<usize, DataProfilerError> {
        let mut issues = 0;

        for values in data.values() {
            for value in values {
                // Replacement characters (�) or mojibake artifacts both
                // indicate the same defect: the value was mis-decoded.
                if value.contains('\u{FFFD}') || Self::has_encoding_artifacts(value) {
                    issues += 1;
                }
            }
        }

        Ok(issues)
    }

    /// Check for encoding artifacts in text
    fn has_encoding_artifacts(text: &str) -> bool {
        // Common encoding artifacts
        let artifacts = ["Ã¡", "Ã©", "Ã\u{AD}", "Ã³", "Ãº", "Ã±", "Ã§"];
        artifacts.iter().any(|artifact| text.contains(artifact))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ColumnStats;

    fn string_profile(name: &str) -> ColumnProfile {
        ColumnProfile {
            name: name.to_string(),
            data_type: DataType::String,
            null_count: 0,
            total_count: 0,
            unique_count: None,
            unique_count_is_approximate: None,
            invalid_count: None,
            stats: ColumnStats::None,
            patterns: Some(vec![]),
        }
    }

    /// `data_type_consistency` for one string column of `values`.
    fn string_column_consistency(name: &str, values: Vec<String>) -> f64 {
        let data = HashMap::from([(name.to_string(), values)]);
        ConsistencyCalculator::calculate(&data, &[string_profile(name)])
            .expect("consistency metrics should be computed")
            .data_type_consistency
    }

    /// `data_type_consistency` for one column of `values` typed `data_type`.
    fn typed_column_consistency(name: &str, data_type: DataType, values: Vec<String>) -> f64 {
        let mut profile = string_profile(name);
        profile.data_type = data_type;
        let data = HashMap::from([(name.to_string(), values)]);
        ConsistencyCalculator::calculate(&data, &[profile])
            .expect("consistency metrics should be computed")
            .data_type_consistency
    }

    /// `junk` non-numeric values padded out to 1000 with integers, the shape
    /// from the report in #544.
    fn numeric_with_junk(junk: usize) -> Vec<String> {
        (0..1000 - junk)
            .map(|i| (1000 + i).to_string())
            .chain((0..junk).map(|i| format!("junk{i}")))
            .collect()
    }

    #[test]
    fn lexical_class_order_matches_discriminants() {
        for (index, class) in LEXICAL_CLASS_ORDER.into_iter().enumerate() {
            assert_eq!(
                class as usize, index,
                "LEXICAL_CLASS_ORDER must be indexed by discriminant: {class:?}"
            );
        }
    }

    #[test]
    fn mixed_string_column_reports_the_share_of_its_dominant_class() {
        // 800 integers and 200 non-numeric values. Inference falls back to
        // String here, which used to report a perfect 100.
        assert_eq!(string_column_consistency("v", numeric_with_junk(200)), 80.0);
        assert_eq!(string_column_consistency("v", numeric_with_junk(500)), 50.0);
        // Past the halfway mark the text values are the dominant class and the
        // shrinking numeric minority is what costs the column its consistency.
        assert_eq!(string_column_consistency("v", numeric_with_junk(800)), 80.0);
    }

    #[test]
    fn a_date_column_is_validated_against_the_forms_that_typed_it() {
        // Inference types a column `Date` on forms the validation regexes do not
        // accept, so validating against the narrower set failed every value in a
        // clean column: 28 identical ISO datetimes scored 0.0 consistency, and
        // the file's overall score came out at 60.94 for perfect data.
        for (label, date) in [
            ("iso datetime", "2024-01-15T10:30:00"),
            ("fractional iso datetime", "2024-01-15T10:30:00.123"),
            ("spaced datetime", "2024-01-15 10:30:00"),
            ("dotted", "15.01.2024"),
            ("slashed datetime", "15/01/2024 10:30:00"),
            ("iso date", "2024-01-15"),
        ] {
            let values = vec![date.to_string(); 28];
            assert_eq!(
                typed_column_consistency("ts", DataType::Date, values),
                100.0,
                "a column of {label} values is typed Date but scored as malformed"
            );
        }
    }

    #[test]
    fn a_date_column_of_malformed_values_still_loses_consistency() {
        // Sharing one predicate must not turn the date branch permissive.
        let values = ["2024-01-15", "not-a-date", "2024", "2024-01-16"]
            .map(String::from)
            .to_vec();
        assert_eq!(typed_column_consistency("ts", DataType::Date, values), 50.0);
    }

    #[test]
    fn a_date_named_string_column_accepts_every_recognized_date_form() {
        // The date-named string column keeps its stricter name-driven rule, but
        // "is this a date" has to mean the same thing there too.
        let values = [
            "2024-01-15T10:30:00",
            "15.01.2024",
            "2024-01-15 10:30:00",
            "2024-01-15",
        ]
        .map(String::from)
        .to_vec();
        assert_eq!(string_column_consistency("event_date", values), 100.0);
    }

    #[test]
    fn every_inference_supported_date_form_is_its_own_class() {
        // The date forms inference recognizes are not the forms date *validation*
        // recognizes, and neither set contains the other. Classifying against
        // validation alone put ISO datetimes and dotted dates in the text class
        // beside genuine junk, so a column just under the 70% date threshold
        // reported a perfect score — the original bug, one type over.
        for (label, date) in [
            ("iso datetime", "2024-01-01T10:00:00"),
            ("spaced datetime", "2024-01-01 10:00:00"),
            ("dotted", "01.02.2024"),
            ("iso date", "2024-01-01"),
            ("lenient slash", "1/2/2024"),
        ] {
            let values: Vec<String> = std::iter::repeat_n(date.to_string(), 70)
                .chain((0..30).map(|i| format!("junk{i}")))
                .collect();
            assert_eq!(
                string_column_consistency("v", values),
                70.0,
                "{label} was not scored as a date form distinct from junk"
            );
        }
    }

    #[test]
    fn genuinely_textual_column_stays_fully_consistent() {
        let names = ["Alice", "Bob", "Charlie", "Dora"]
            .map(String::from)
            .to_vec();
        assert_eq!(string_column_consistency("name", names), 100.0);

        // The 100%-junk end of the sweep is a text column like any other.
        assert_eq!(
            string_column_consistency("v", numeric_with_junk(1000)),
            100.0
        );
    }

    #[test]
    fn whole_and_fractional_numbers_are_one_class() {
        // Splitting Integer from Float here would report 66.7% for an ordinary
        // numeric column that happens to contain whole numbers.
        let values = ["1.5", "2", "3.25", "4"].map(String::from).to_vec();
        assert_eq!(string_column_consistency("v", values), 100.0);
    }

    #[test]
    fn identifier_columns_may_mix_forms_without_penalty() {
        // `Identifier` comes from an explicit semantic hint, so a scheme that
        // mixes numeric and alphanumeric keys is intended, not a defect.
        let mut profile = string_profile("customer_id");
        profile.data_type = DataType::Identifier;
        let data = HashMap::from([(
            "customer_id".to_string(),
            ["A1", "123", "B2", "456"].map(String::from).to_vec(),
        )]);

        let metrics = ConsistencyCalculator::calculate(&data, &[profile])
            .expect("consistency metrics should be computed");

        assert_eq!(metrics.data_type_consistency, 100.0);
    }

    #[test]
    fn no_column_holding_more_than_one_class_scores_perfect() {
        // Invariant (a): a perfect consistency score means one lexical class.
        for junk in (10..=990).step_by(10) {
            let consistency = string_column_consistency("v", numeric_with_junk(junk));
            assert!(
                consistency < 100.0,
                "{junk} junk values in 1000 scored {consistency}, but the column holds two classes"
            );
        }
    }

    #[test]
    fn moving_values_into_the_minority_never_raises_consistency() {
        // Invariant (b). Each step moves ten more values out of the dominant
        // class, so this asserts a strict decrease rather than merely
        // "non-increasing": a flat sequence satisfies the invariant while
        // ignoring the values being moved, which is precisely what the old
        // every-value-conforms-to-String rule did.
        //
        // Approaching a 50/50 split from the numeric side.
        let mut previous = f64::INFINITY;
        for junk in (0..=500).step_by(10) {
            let consistency = string_column_consistency("v", numeric_with_junk(junk));
            assert!(
                consistency < previous,
                "consistency held at {previous} through {junk} junk values"
            );
            previous = consistency;
        }

        // And from the text side, where the numeric values are now the
        // minority being added to.
        let mut previous = f64::INFINITY;
        for junk in (50..=100).rev().map(|tenth| tenth * 10) {
            let consistency = string_column_consistency("v", numeric_with_junk(junk));
            assert!(
                consistency < previous,
                "consistency held at {previous} through {junk} junk values"
            );
            previous = consistency;
        }
    }

    #[test]
    fn consistency_is_continuous_across_the_inference_threshold() {
        // 19% junk still infers as Float and scores against numeric parsing;
        // 20% falls back to String and scores against the dominant class. The
        // fix is only worth having if nothing jumps at the handover.
        let mut float_profile = string_profile("v");
        float_profile.data_type = DataType::Float;
        let below = HashMap::from([("v".to_string(), numeric_with_junk(190))]);
        let below_consistency = ConsistencyCalculator::calculate(&below, &[float_profile])
            .expect("consistency metrics should be computed")
            .data_type_consistency;

        let above_consistency = string_column_consistency("v", numeric_with_junk(200));

        assert_eq!(below_consistency, 81.0);
        assert_eq!(above_consistency, 80.0);
    }

    #[test]
    fn test_encoding_issues_count_each_value_once() {
        let data = HashMap::from([(
            "name".to_string(),
            vec![
                // Both a replacement character and a mojibake artifact in
                // one value: still a single mis-decoded value.
                "Jos\u{FFFD} GarcÃ\u{AD}a".to_string(),
                "clean".to_string(),
            ],
        )]);
        let profiles = vec![string_profile("name")];

        let metrics = ConsistencyCalculator::calculate(&data, &profiles)
            .expect("consistency metrics should be computed");

        assert_eq!(metrics.encoding_issues, 1);
    }

    #[test]
    fn test_likely_date_string_column_is_not_automatically_consistent() {
        let data = HashMap::from([(
            "event_date".to_string(),
            vec![
                "2024-01-01".to_string(),
                "not-a-date".to_string(),
                "15/01/2024".to_string(),
            ],
        )]);
        let profiles = vec![string_profile("event_date")];

        let metrics = ConsistencyCalculator::calculate(&data, &profiles)
            .expect("consistency metrics should be computed");

        assert!(
            metrics.data_type_consistency < 100.0,
            "likely date columns inferred as strings should still lose consistency when values are malformed"
        );
    }

    #[test]
    fn unsigned_integer_tokens_are_consistent_with_integer_profiles() {
        let data = HashMap::from([(
            "value".to_string(),
            vec![
                "42".to_string(),
                (i64::MAX as u64 + 1).to_string(),
                u64::MAX.to_string(),
            ],
        )]);
        let mut profile = string_profile("value");
        profile.data_type = DataType::Integer;

        let metrics = ConsistencyCalculator::calculate(&data, &[profile])
            .expect("consistency metrics should be computed");

        assert_eq!(metrics.values_checked, 3);
        assert_eq!(metrics.data_type_consistency, 100.0);
    }
}
