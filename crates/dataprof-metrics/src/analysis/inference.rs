use regex::Regex;
use std::sync::LazyLock;

use dataprof_core::{LexicalClass, TypeHomogeneity};

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
        // RFC 3339: the `T` form with an optional fractional part and an
        // optional `Z`/`±HH:MM` offset. The offset alternatives are exactly the
        // ones `chrono::DateTime::parse_from_rfc3339` accepts, so a value this
        // pattern calls a date is a value the parser can turn into an instant.
        //
        // The offset is range-bound, not `\d{2}:\d{2}`: chrono rejects `+24:00`
        // and `+00:60` as out of range, so a looser pattern would type the
        // column `Date` and then count every value as `invalid_date_values` —
        // the exact mismatch this pattern exists to avoid. The ISO 8601 basic
        // form (`+0200`) is absent for the same reason.
        Regex::new(
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)?$",
        )
        .expect("BUG: Invalid hardcoded regex pattern for ISO datetime"),
        Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
            .expect("BUG: Invalid hardcoded regex pattern for spaced ISO datetime"),
        Regex::new(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$")
            .expect("BUG: Invalid hardcoded regex pattern for DD/MM/YYYY datetime"),
    ]
});

/// A number written with a decimal comma or with digit-group separators: the
/// forms locale-formatted exports carry and plain numeric parsing rejects.
///
/// Digits are ASCII on purpose: `\d` is Unicode-aware in this crate and would
/// accept digits no numeric parser here reads. Space-like grouping requires a
/// decimal part, because `333 123 456` is more often a phone number or a code
/// than an integer. The pattern alone also accepts `1.234`, which plain parsing
/// reads as a number; [`is_locale_number_token`] keeps only text values.
static LOCALE_NUMBER_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(concat!(
        r"^[+-]?(?:",
        // 10,50 and 1,234: a decimal comma, or one comma-grouped thousand.
        r"[0-9]+,[0-9]+",
        // 1.234,56 and 1.234.567: dot grouping, optional comma decimals.
        r"|[0-9]{1,3}(?:\.[0-9]{3})+(?:,[0-9]+)?",
        // 1,234.56 and 1,234,567: comma grouping, optional dot decimals.
        r"|[0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?",
        // 12,34,567.89: Indian grouping, pairs above the first thousand.
        r"|[0-9]{1,2}(?:,[0-9]{2})+,[0-9]{3}(?:\.[0-9]+)?",
        // 1'234.56: apostrophe grouping, either decimal mark.
        r"|[0-9]{1,3}(?:['\u{2019}][0-9]{3})+(?:[.,][0-9]+)?",
        // 1 234,56: space, no-break or narrow no-break space grouping.
        r"|[0-9]{1,3}(?:[ \u{A0}\u{202F}][0-9]{3})+[.,][0-9]+",
        r")$",
    ))
    .expect("BUG: Invalid hardcoded regex pattern for locale-formatted numbers")
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

    // Codes written in digits are text: typed numeric, they get a mean (#814).
    let digit_codes = holds_digit_codes(non_empty.iter().map(|s| s.trim()));

    if !digit_codes && integer_count == non_empty.len() {
        return DataType::Integer;
    }

    // 80% threshold: tolerates a few non-numeric values (e.g. "N/A", missing)
    if !digit_codes && float_count as f64 / non_empty.len() as f64 > 0.8 {
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

/// Whether trimmed, non-null `values` are codes written in digits rather than
/// quantities, so a numeric type would give them a mean (#814).
///
/// Two shapes qualify:
///
/// - Any value that keeps a leading zero, such as `007` or a postal code
///   `08001`. No quantity is written that way, so one such value is evidence
///   for the whole column. `0` alone is a quantity and does not count.
/// - Every numeric value is an eight-digit `YYYYMMDD` calendar date, such as
///   `20240115`. Recognizing these as dates is #815; until then they are text.
///
/// Both inference paths, [`infer_type`] and the streaming one, ask this before
/// typing a column numeric, so every engine and input path agrees. A column a
/// source declares numeric (an Arrow integer type) is not re-inferred: it has
/// no leading zeros to see.
pub fn holds_digit_codes<'a>(values: impl IntoIterator<Item = &'a str>) -> bool {
    let mut numeric = 0usize;
    let mut compact_dates = 0usize;
    for value in values {
        if is_zero_padded_digits(value) {
            return true;
        }
        if is_integer_token(value) || value.parse::<f64>().is_ok() {
            numeric += 1;
            if is_compact_date_token(value) {
                compact_dates += 1;
            }
        }
    }
    numeric > 0 && compact_dates == numeric
}

/// `0` followed by one or more ASCII digits, and nothing else.
fn is_zero_padded_digits(value: &str) -> bool {
    value.len() > 1 && value.starts_with('0') && value.bytes().all(|b| b.is_ascii_digit())
}

/// An eight-digit `YYYYMMDD` value naming a real calendar day in 1800-2199.
///
/// The year window keeps arbitrary eight-digit numbers out: `10101010` is a
/// valid date in year 1010, and no data this profiler reads dates from then.
fn is_compact_date_token(value: &str) -> bool {
    let digits = value.as_bytes();
    if digits.len() != 8 || !digits.iter().all(u8::is_ascii_digit) {
        return false;
    }
    let number = |range: std::ops::Range<usize>| {
        digits[range]
            .iter()
            .fold(0u32, |n, digit| n * 10 + u32::from(digit - b'0'))
    };
    let year = number(0..4);
    (1800..=2199).contains(&year)
        && chrono::NaiveDate::from_ymd_opt(year as i32, number(4..6), number(6..8)).is_some()
}

/// Return whether a token is an integer representable by dataprof's signed or
/// unsigned 64-bit integer contract.
///
/// Keep inference and quality validation on this shared predicate: values above
/// `i64::MAX` are valid integer tokens even though they require `u64` to parse.
pub fn is_integer_token(value: &str) -> bool {
    value.parse::<i64>().is_ok() || value.parse::<u64>().is_ok()
}

pub use dataprof_core::is_null_like_token;

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

/// Classify one trimmed, non-null value into its [`LexicalClass`].
///
/// The precedence matches the order [`infer_type`] tries types on whole columns,
/// so a column that just missed a type threshold reports the same share of
/// matching values that it reported while it still had that type. Integers and
/// fractions share [`LexicalClass::Numeric`] deliberately: `["1.5", "2", "3"]`
/// is one numeric column, not a two-class mixture.
///
/// Dates use [`is_date_token`], the union of the forms inference and validation
/// each recognize. Using only the validation set would drop ISO datetimes and
/// dotted dates into `Text` alongside genuine junk.
pub fn lexical_class(value: &str) -> LexicalClass {
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

/// Distribute `values` across the lexical classes, skipping null-like tokens.
///
/// This is the single classifier behind both the consistency score of a column
/// with no inferred type and the `type_homogeneity` a profile reports, so the
/// two can never disagree about what a column holds.
pub fn classify_lexical_forms<S: AsRef<str>>(values: &[S]) -> TypeHomogeneity {
    let mut homogeneity = TypeHomogeneity::default();
    for value in values {
        let trimmed = value.as_ref().trim();
        if !is_null_like_token(trimmed) {
            homogeneity.record(lexical_class(trimmed));
        }
    }
    homogeneity
}

/// Whether a trimmed value is a number written with a decimal comma or with
/// digit-group separators, such as `10,50`, `1.234,56`, `1,234.56`,
/// `1'234.56` or `1 234,56` (#433).
///
/// Only [`LexicalClass::Text`] values qualify, so every value this accepts is
/// one [`classify_lexical_forms`] counted as text, and one no statistic read
/// as a number. Which convention a value uses is not decided here: `1,234` is
/// a thousand in one locale and a fraction in another, and either way it was
/// not read as a number.
pub fn is_locale_number_token(value: &str) -> bool {
    // The regex first: almost no value matches it, and `lexical_class` parses
    // and runs the date patterns.
    LOCALE_NUMBER_REGEX.is_match(value) && lexical_class(value) == LexicalClass::Text
}

/// Count the non-null `values` that [`is_locale_number_token`] accepts.
///
/// Classified over the same values as [`classify_lexical_forms`], so the count
/// is a subset of that result's `text` count.
pub fn count_locale_numbers<S: AsRef<str>>(values: &[S]) -> usize {
    values
        .iter()
        .map(|value| value.as_ref().trim())
        .filter(|trimmed| !is_null_like_token(trimmed) && is_locale_number_token(trimmed))
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn each_value_lands_in_exactly_one_class() {
        for (value, expected) in [
            ("42", LexicalClass::Numeric),
            ("-1.5", LexicalClass::Numeric),
            (u64::MAX.to_string().as_str(), LexicalClass::Numeric),
            ("2024-01-15", LexicalClass::Date),
            ("2024-01-15T10:30:00", LexicalClass::Date),
            ("15.01.2024", LexicalClass::Date),
            ("true", LexicalClass::Boolean),
            ("FALSE", LexicalClass::Boolean),
            ("junk0", LexicalClass::Text),
            ("N/A", LexicalClass::Text),
        ] {
            assert_eq!(lexical_class(value), expected, "{value}");
        }
    }

    #[test]
    fn digit_codes_are_text_and_quantities_stay_numeric() {
        let strings = |values: &[&str]| values.iter().map(|v| v.to_string()).collect::<Vec<_>>();

        // #814: a leading zero anywhere, or every value a YYYYMMDD date.
        for codes in [
            &["00123", "00456", "01234", "09999"][..],
            &["28013", "08001", "41001"],
            &["20240115", "20240216", "19991231"],
            &["0612345678", "3471234567"],
            &["007", "12", "N/A"],
            &["20240115", "", "null"],
        ] {
            assert_eq!(infer_type(&strings(codes)), DataType::String, "{codes:?}");
        }

        for (quantities, expected) in [
            (&["0", "1", "10", "250"][..], DataType::Integer),
            (&["12345678", "20240115"], DataType::Integer),
            (&["20241315", "20240230"], DataType::Integer),
            (&["17991231", "22000101"], DataType::Integer),
            (&["0.5", "0.25", "12"], DataType::Float),
            (&["-01", "-02", "3"], DataType::Integer),
        ] {
            assert_eq!(infer_type(&strings(quantities)), expected, "{quantities:?}");
        }
    }

    #[test]
    fn classification_counts_only_non_null_values() {
        // Null-like tokens are absence, not a lexical form: counting them would
        // make a sparse numeric column look mixed.
        let values = ["1", "2", "", "null", "NaN", "junk", "2024-01-15"].map(String::from);

        let counts = classify_lexical_forms(&values);

        assert_eq!(counts.numeric, 2);
        assert_eq!(counts.date, 1);
        assert_eq!(counts.text, 1);
        assert_eq!(counts.classified_count(), 4);
    }

    #[test]
    fn classification_matches_the_consistency_score_it_shares_a_classifier_with() {
        // 800 integers and 200 junk values: the shape from #544. The dominant
        // share and the consistency score of the same column are one number,
        // computed once, so the flag and the score can never disagree.
        let values: Vec<String> = (0..800)
            .map(|i| (1000 + i).to_string())
            .chain((0..200).map(|i| format!("junk{i}")))
            .collect();

        let counts = classify_lexical_forms(&values);

        assert_eq!(counts.dominant(), Some((LexicalClass::Numeric, 800)));
        assert_eq!(counts.dominant_share(), Some(0.8));
    }

    #[test]
    fn a_column_of_only_nulls_is_classified_and_holds_nothing() {
        let values = ["", "   ", "NULL"].map(String::from);

        // Not the same as "never classified": the caller can tell the two apart
        // because one is `Some` and the other is absent from the profile.
        assert_eq!(classify_lexical_forms(&values), TypeHomogeneity::default());
    }

    #[test]
    fn locale_formatted_numbers_are_recognized_and_nothing_else_is() {
        for value in [
            "10,50",
            "-0,5",
            "+3,14",
            "1,234",
            "1.234,56",
            "1.234.567",
            "12.345.678,9",
            "1,234.56",
            "1,234,567",
            "12,34,567",
            "1,00,000.50",
            "1'234.56",
            "1\u{2019}234,5",
            "1 234,56",
            "1\u{A0}234,56",
            "1\u{202F}234.5",
        ] {
            assert!(is_locale_number_token(value), "{value:?} not recognized");
            assert_eq!(lexical_class(value), LexicalClass::Text, "{value:?}");
        }

        for value in [
            // Read as numbers already: not text, so not a missed number.
            "1.234",
            "10.50",
            "1234",
            "-7",
            "1e3",
            // Dates and other dotted or grouped forms.
            "15.01.2024",
            "2024-01-15",
            "1.2.3",
            "192.168.1.1",
            // Space grouping without decimals is a phone number or a code as
            // often as it is an integer.
            "333 123 456",
            "1 234",
            // Malformed grouping and separators without digits.
            "1,2,3",
            "1,23,4",
            "123,45,678",
            "1.23,4",
            ",5",
            "5,",
            "1,,5",
            // Not ASCII digits.
            "\u{0661},\u{0665}",
            // Units, currencies and other text around a number are out of
            // scope for this recognizer.
            "10,50 EUR",
            "\u{20AC} 10,50",
            "10,5%",
            "junk",
        ] {
            assert!(!is_locale_number_token(value), "{value:?} recognized");
        }
    }

    #[test]
    fn locale_numbers_are_counted_among_the_text_values_only() {
        let values = [
            "1.234,56",
            " 10,50 ",
            "",
            "null",
            "1.234",
            "12",
            "junk",
            "15.01.2024",
        ]
        .map(String::from);

        assert_eq!(count_locale_numbers(&values), 2);
        // The same values `classify_lexical_forms` puts in `text`, and no more.
        assert_eq!(classify_lexical_forms(&values).text, 3);
    }

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
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-](?:[01]\d|2[0-3]):[0-5]\d)?$",
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
