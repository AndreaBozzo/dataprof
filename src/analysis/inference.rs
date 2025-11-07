use regex::Regex;
use std::sync::LazyLock;

use crate::types::DataType;

// Pre-compile regex patterns for better performance
// These patterns are compiled once at startup instead of on every column analysis
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
    ]
});

pub fn infer_type(data: &[String]) -> DataType {
    // Filter empty and whitespace-only strings for more robust inference
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.trim().is_empty()).collect();

    if non_empty.is_empty() {
        return DataType::String;
    }

    // Check dates first (before numeric to catch date-like numbers)
    for regex in DATE_REGEXES.iter() {
        let date_matches = non_empty
            .iter()
            .filter(|s| regex.is_match(s.trim()))
            .count();

        if date_matches as f64 / non_empty.len() as f64 > 0.8 {
            return DataType::Date;
        }
    }

    // Single pass for numeric type checking (optimization)
    // Since all integers are valid floats, we can check both in one iteration
    let mut integer_count = 0;
    let mut float_count = 0;

    for s in &non_empty {
        let trimmed = s.trim();
        if trimmed.parse::<i64>().is_ok() {
            integer_count += 1;
            float_count += 1; // integers are also valid floats
        } else if trimmed.parse::<f64>().is_ok() {
            float_count += 1;
        }
    }

    if integer_count == non_empty.len() {
        return DataType::Integer;
    }

    if float_count == non_empty.len() {
        return DataType::Float;
    }

    DataType::String
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // 83.3% dates (5 out of 6), should still be detected as Date (threshold > 80%)
        let data = vec![
            "2023-01-15".to_string(),
            "2023-02-20".to_string(),
            "2023-03-25".to_string(),
            "2023-04-30".to_string(),
            "2023-05-15".to_string(),
            "not a date".to_string(), // 16.7% non-date
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
    fn test_date_regex_patterns_are_valid() {
        // Validate that all hardcoded regex patterns compile successfully
        // This test will fail at initialization if any pattern is invalid
        assert_eq!(DATE_REGEXES.len(), 5);
    }
}
