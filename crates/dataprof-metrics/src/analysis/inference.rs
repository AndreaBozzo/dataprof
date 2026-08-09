use regex::Regex;
use std::sync::LazyLock;

use crate::types::DataType;

// Pre-compile regex patterns for better performance
// These patterns are compiled once at startup instead of on every column analysis
//
// NOTE: Some patterns (^\d{2}/\d{2}/\d{4}$) are ambiguous between DD/MM/YYYY and MM/DD/YYYY.
// The datetime parsing module assumes European format (DD/MM/YYYY) by default.
// See datetime.rs documentation for details on date format handling.
static DATE_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"^\d{4}-\d{2}-\d{2}$")
            .expect("BUG: Invalid hardcoded regex pattern for ISO 8601 date"),
        Regex::new(r"^\d{2}/\d{2}/\d{4}$")
            .expect("BUG: Invalid hardcoded regex pattern for DD/MM/YYYY date"),
        Regex::new(r"^\d{2}-\d{2}-\d{4}$")
            .expect("BUG: Invalid hardcoded regex pattern for DD-MM-YYYY date"),
        Regex::new(r"^\d{4}/\d{2}/\d{2}$")
            .expect("BUG: Invalid hardcoded regex pattern for YYYY/MM/DD date"),
        Regex::new(r"^\d{2}\.\d{2}\.\d{4}$")
            .expect("BUG: Invalid hardcoded regex pattern for DD.MM.YYYY date"),
        Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$")
            .expect("BUG: Invalid hardcoded regex pattern for ISO datetime"),
        Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
            .expect("BUG: Invalid hardcoded regex pattern for spaced ISO datetime"),
        Regex::new(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$")
            .expect("BUG: Invalid hardcoded regex pattern for DD/MM/YYYY datetime"),
    ]
});

pub fn infer_type(data: &[String]) -> DataType {
    // Filter null-like strings for more robust inference.
    let non_empty: Vec<&String> = data
        .iter()
        .filter(|s| !is_null_like_token(s.trim()))
        .collect();

    if non_empty.is_empty() {
        return DataType::String;
    }

    // Single pass for numeric type checking (optimization)
    // Since all integers are valid floats, we can check both in one iteration
    let mut integer_count = 0;
    let mut float_count = 0;

    for s in &non_empty {
        let trimmed = s.trim();
        if is_integer_token(trimmed) {
            integer_count += 1;
            float_count += 1; // integers are also valid floats
        } else if trimmed.parse::<f64>().is_ok() {
            // Infinity is a numeric lexical form even though it cannot enter
            // finite statistics. Classify the column as numeric and let the
            // profile's invalid_count disclose the unusable value.
            float_count += 1;
        }
    }

    if integer_count == non_empty.len() {
        return DataType::Integer;
    }

    // 80% threshold: tolerates a few non-numeric values (e.g. "N/A", missing)
    if float_count as f64 / non_empty.len() as f64 > 0.8 {
        return DataType::Float;
    }

    // Check booleans after numeric — strict string literals only (pure 0/1 columns
    // already matched as Integer above). 90% threshold to tolerate a few nulls.
    let bool_count = non_empty
        .iter()
        .filter(|s| parse_strict_boolean_token(s.trim()).is_some())
        .count();

    if bool_count as f64 / non_empty.len() as f64 >= 0.9 {
        return DataType::Boolean;
    }

    // Check dates after boolean (70% threshold, consistent with streaming inference).
    // Treat supported date formats cumulatively so mixed date columns still infer as dates.
    let date_matches = non_empty
        .iter()
        .filter(|s| is_inferred_date_token(s.trim()))
        .count();

    if date_matches as f64 / non_empty.len() as f64 > 0.7 {
        return DataType::Date;
    }

    DataType::String
}

/// Whether a value carries one of the date forms `infer_type` counts towards
/// typing a column as [`DataType::Date`].
///
/// Deliberately narrower than [`is_date_token`]: this set decides the *type*, so
/// widening it changes which columns are dates.
pub(crate) fn is_inferred_date_token(value: &str) -> bool {
    DATE_REGEXES.iter().any(|regex| regex.is_match(value))
}

/// Whether a value has the lexical form of a date in any format dataprof
/// recognizes.
///
/// The union of the forms [`is_inferred_date_token`] scores when typing a column
/// and the forms `is_valid_date_format` accepts when validating one. Neither set
/// contains the other — inference alone misses `1/2/2024`, validation alone
/// misses dotted dates and both datetime forms — and a value that only one of
/// them recognizes is still a date rather than free text.
///
/// Classifying against only one set silently reunites dates with junk: a column
/// of 70% ISO datetimes and 30% junk falls short of the inference threshold, and
/// if the datetimes then fail the date test too, every value lands in the text
/// class and the column reports a perfect consistency score. Reconciling the two
/// sets into one is tracked separately; until then this union is what "looks like
/// a date" means for classification.
pub(crate) fn is_date_token(value: &str) -> bool {
    is_inferred_date_token(value) || crate::analysis::metrics::utils::is_valid_date_format(value)
}

/// Return whether a token is an integer representable by dataprof's signed or
/// unsigned 64-bit integer contract.
///
/// Keep inference and quality validation on this shared predicate: values above
/// `i64::MAX` are valid integer tokens even though they require `u64` to parse.
pub fn is_integer_token(value: &str) -> bool {
    value.parse::<i64>().is_ok() || value.parse::<u64>().is_ok()
}

pub fn is_null_like_token(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("null")
        || trimmed.eq_ignore_ascii_case("nan")
}

pub fn parse_strict_boolean_token(value: &str) -> Option<bool> {
    let trimmed = value.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        Some(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn test_infer_integer() {
        let data = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        assert!(matches!(infer_type(&data), DataType::Integer));
    }

    #[test]
    fn test_infer_float() {
        let data = vec!["1.5".to_string(), "2.3".to_string(), "3.7".to_string()];
        assert!(matches!(infer_type(&data), DataType::Float));
    }

    #[test]
    fn test_infer_mixed_numeric_as_float() {
        // Mix of integers and floats should be detected as Float
        let data = vec!["1".to_string(), "2.5".to_string(), "3".to_string()];
        assert!(matches!(infer_type(&data), DataType::Float));
    }

    #[test]
    fn test_infer_unsigned_integer_beyond_i64() {
        let data = vec![
            u64::MAX.to_string(),
            (u64::MAX - 1).to_string(),
            (i64::MAX as u64 + 1).to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Integer));
    }

    #[test]
    fn test_infer_non_finite_numeric_tokens_as_float() {
        let data = vec![
            "1.0".to_string(),
            "Infinity".to_string(),
            "-inf".to_string(),
            "2.0".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Float));
    }

    #[test]
    fn test_infer_date_iso() {
        let data = vec![
            "2023-01-15".to_string(),
            "2023-02-20".to_string(),
            "2023-03-25".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_date_european_slash() {
        let data = vec![
            "15/01/2023".to_string(),
            "20/02/2023".to_string(),
            "25/03/2023".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_date_european_dash() {
        let data = vec![
            "15-01-2023".to_string(),
            "20-02-2023".to_string(),
            "25-03-2023".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_date_european_dot() {
        let data = vec![
            "15.01.2023".to_string(),
            "20.02.2023".to_string(),
            "25.03.2023".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_date_threshold() {
        // 71.4% dates (5 out of 7), should still be detected as Date (threshold > 70%)
        let data = vec![
            "2023-01-15".to_string(),
            "2023-02-20".to_string(),
            "2023-03-25".to_string(),
            "2023-04-30".to_string(),
            "2023-05-15".to_string(),
            "not a date".to_string(),
            "also not".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_mixed_date_formats() {
        let data = vec![
            "2024-01-15".to_string(),
            "15/01/2024".to_string(),
            "2024-01-16".to_string(),
            "16-01-2024".to_string(),
            "2024-01-17".to_string(),
            "2024-01-18".to_string(),
            "2024/01/19".to_string(),
            "19/01/2024".to_string(),
            "".to_string(),
            "2024-01-20".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_iso_datetime() {
        let data = vec![
            "2024-01-15T10:00:00".to_string(),
            "2024-01-15T10:15:00".to_string(),
            "2024-01-15T10:30:00".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_string() {
        let data = vec!["hello".to_string(), "world".to_string()];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_empty_data() {
        let data: Vec<String> = vec![];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_all_empty_strings() {
        let data = vec!["".to_string(), "".to_string(), "".to_string()];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_whitespace_handling() {
        // Integers with leading/trailing whitespace
        let data = vec![" 1 ".to_string(), "  2".to_string(), "3  ".to_string()];
        assert!(matches!(infer_type(&data), DataType::Integer));
    }

    #[test]
    fn test_infer_whitespace_only_strings() {
        let data = vec!["  ".to_string(), "\t".to_string(), " \n ".to_string()];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_dates_with_whitespace() {
        let data = vec![
            " 2023-01-15 ".to_string(),
            "  2023-02-20".to_string(),
            "2023-03-25  ".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Date));
    }

    #[test]
    fn test_infer_floats_with_whitespace() {
        let data = vec![
            " 1.5 ".to_string(),
            "  2.3".to_string(),
            "3.7  ".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Float));
    }

    #[test]
    fn test_infer_mixed_non_numeric() {
        // Mix of different non-numeric types should be String
        let data = vec![
            "hello".to_string(),
            "123abc".to_string(),
            "2023".to_string(), // This looks like a year but alone is an integer
        ];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_boolean_lowercase() {
        let data = vec![
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "false".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_titlecase() {
        let data = vec!["True".to_string(), "False".to_string(), "True".to_string()];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_uppercase() {
        let data = vec!["TRUE".to_string(), "FALSE".to_string(), "TRUE".to_string()];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_yes_no() {
        let data = vec!["yes".to_string(), "no".to_string(), "yes".to_string()];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_boolean_mixed_case() {
        let data = vec![
            "True".to_string(),
            "false".to_string(),
            "TRUE".to_string(),
            "False".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_with_whitespace() {
        let data = vec![
            " true ".to_string(),
            "  false".to_string(),
            "true  ".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_boolean_threshold() {
        // 90% threshold: 9 of 10 are boolean → should detect
        let data = vec![
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "false".to_string(),
            "true".to_string(),
            "maybe".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_infer_not_boolean_below_threshold() {
        // Only 50% are boolean → should be String
        let data = vec![
            "true".to_string(),
            "false".to_string(),
            "hello".to_string(),
            "world".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::String));
    }

    #[test]
    fn test_infer_pure_01_stays_integer() {
        // Pure 0/1 columns should remain Integer, not Boolean
        let data = vec![
            "0".to_string(),
            "1".to_string(),
            "0".to_string(),
            "1".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Integer));
    }

    #[test]
    fn test_infer_boolean_with_null_like_tokens() {
        let data = vec![
            "true".to_string(),
            "FALSE".to_string(),
            "null".to_string(),
            "NULL".to_string(),
            "nan".to_string(),
            "NaN".to_string(),
            "".to_string(),
        ];
        assert!(matches!(infer_type(&data), DataType::Boolean));
    }

    #[test]
    fn test_date_regex_patterns_are_valid() {
        // Validate that all hardcoded regex patterns compile successfully
        // This test will fail at initialization if any pattern is invalid
        assert_eq!(DATE_REGEXES.len(), 8);
    }

    /// Every date pattern dataprof recognizes, paired with an example of it.
    ///
    /// Keyed by regex source rather than by example, because the two sets
    /// overlap: the lenient `^\d{1,2}/\d{1,2}/\d{4}$` also matches `15/01/2024`,
    /// so a table checked only by "some example matches this pattern" stays green
    /// when a pattern is added or its example deleted. Pinning the pattern set
    /// makes any change to either set fail here until this table is updated.
    ///
    /// The four patterns the two sets share appear once, so this is the union.
    const DATE_FORM_EXAMPLES: [(&str, &str); 11] = [
        (r"^\d{4}-\d{2}-\d{2}$", "2024-01-15"),
        (r"^\d{2}/\d{2}/\d{4}$", "15/01/2024"),
        (r"^\d{2}-\d{2}-\d{4}$", "15-01-2024"),
        (r"^\d{4}/\d{2}/\d{2}$", "2024/01/15"),
        (r"^\d{2}\.\d{2}\.\d{4}$", "15.01.2024"),
        (
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?$",
            "2024-01-15T10:30:00",
        ),
        (
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            "2024-01-15 10:30:00",
        ),
        (
            r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$",
            "15/01/2024 10:30:00",
        ),
        (r"^\d{1,2}/\d{1,2}/\d{4}$", "1/2/2024"),
        (r"^\d{4}-\d{1,2}-\d{1,2}$", "2024-1-5"),
        (r"^\d{1,2}-\d{1,2}-\d{4}$", "1-2-2024"),
    ];

    #[test]
    fn is_date_token_accepts_every_form_either_regex_set_recognizes() {
        // The two sets exist for different jobs — one types a column, the other
        // validates its values — and neither contains the other. When they were
        // used independently a column of clean ISO datetimes was typed `Date` on
        // one set and then failed every value against the other, scoring 0%
        // consistency. `is_date_token` is the union both jobs now share.
        let validation = &crate::analysis::metrics::utils::DATE_VALIDATION_REGEXES;
        let recognized: BTreeSet<&str> = DATE_REGEXES
            .iter()
            .chain(validation.iter())
            .map(|regex| regex.as_str())
            .collect();
        let covered: BTreeSet<&str> = DATE_FORM_EXAMPLES
            .iter()
            .map(|(pattern, _)| *pattern)
            .collect();

        // Adding, removing, or editing a pattern in either set fails here until
        // this table is updated, so no date form can arrive without an example.
        assert_eq!(
            recognized, covered,
            "the recognized date patterns and the examples below have diverged"
        );

        for (pattern, example) in DATE_FORM_EXAMPLES {
            let regex = DATE_REGEXES
                .iter()
                .chain(validation.iter())
                .find(|regex| regex.as_str() == pattern)
                .expect("checked by the set equality above");
            assert!(
                regex.is_match(example),
                "{example:?} is not an example of {pattern}"
            );
            assert!(
                is_date_token(example),
                "{example:?} is a recognized date form but is_date_token rejects it"
            );
        }
    }

    #[test]
    fn a_value_that_is_not_a_date_is_not_a_date_token() {
        // The union must not become a predicate that accepts anything; a date
        // column full of malformed values still has to lose consistency.
        for value in [
            "not-a-date",
            "2024",
            "15/01",
            "2024-13-45x",
            "",
            "junk1",
            "10:30:00",
        ] {
            assert!(!is_date_token(value), "{value:?} was accepted as a date");
        }
    }
}
