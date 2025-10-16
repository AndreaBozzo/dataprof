use lazy_static::lazy_static;
use regex::Regex;

use crate::types::Pattern;

// Pre-compile pattern regexes for better performance
// These patterns are compiled once at startup instead of on every detect_patterns call
lazy_static! {
    static ref PATTERN_REGEXES: Vec<(&'static str, Regex)> = vec![
        (
            "Email",
            Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
                .expect("BUG: Invalid hardcoded regex pattern for Email"),
        ),
        (
            "Phone (US)",
            Regex::new(r"^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$")
                .expect("BUG: Invalid hardcoded regex pattern for US Phone"),
        ),
        (
            "Phone (IT)",
            Regex::new(r"^\+39|0039|39?[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{5,10}$")
                .expect("BUG: Invalid hardcoded regex pattern for IT Phone"),
        ),
        (
            "URL",
            Regex::new(r"^https?://[^\s/$.?#].[^\s]*$")
                .expect("BUG: Invalid hardcoded regex pattern for URL"),
        ),
        (
            "UUID",
            Regex::new(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
                .expect("BUG: Invalid hardcoded regex pattern for UUID"),
        ),
    ];
}

/// Detect common data patterns in a column
///
/// Analyzes string data to identify common patterns like emails, phone numbers, URLs, etc.
/// Uses pre-compiled regex patterns for optimal performance.
///
/// # Arguments
/// * `data` - Slice of string values to analyze
///
/// # Returns
/// Vector of detected patterns with match counts and percentages
///
/// # Performance
/// - Pre-compiled regexes (lazy_static) eliminate compilation overhead
/// - Only patterns with >5% match rate are returned
/// - Whitespace is trimmed for robust matching
pub fn detect_patterns(data: &[String]) -> Vec<Pattern> {
    let mut patterns = Vec::new();

    // Filter empty and whitespace-only strings for robust analysis
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.trim().is_empty()).collect();

    if non_empty.is_empty() {
        return patterns;
    }

    // Check pre-compiled patterns
    for (name, regex) in PATTERN_REGEXES.iter() {
        let matches = non_empty
            .iter()
            .filter(|s| regex.is_match(s.trim()))
            .count();

        let percentage = (matches as f64 / non_empty.len() as f64) * 100.0;

        // Only show patterns with >5% matches to reduce noise
        if percentage > 5.0 {
            patterns.push(Pattern {
                name: name.to_string(),
                regex: regex.as_str().to_string(),
                match_count: matches,
                match_percentage: percentage,
            });
        }
    }

    patterns
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_email_pattern() {
        let data = vec![
            "user@example.com".to_string(),
            "admin@test.org".to_string(),
            "contact@company.com".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Email");
        assert_eq!(patterns[0].match_count, 3);
        assert_eq!(patterns[0].match_percentage, 100.0);
    }

    #[test]
    fn test_detect_us_phone_pattern() {
        let data = vec![
            "(555) 123-4567".to_string(),
            "555-123-4567".to_string(),
            "5551234567".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Phone (US)");
    }

    #[test]
    fn test_detect_url_pattern() {
        let data = vec![
            "https://example.com".to_string(),
            "http://test.org/path".to_string(),
            "https://github.com/user/repo".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "URL");
        assert_eq!(patterns[0].match_percentage, 100.0);
    }

    #[test]
    fn test_detect_uuid_pattern() {
        let data = vec![
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_string(),
            "123e4567-e89b-12d3-a456-426614174000".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "UUID");
    }

    #[test]
    fn test_detect_no_patterns() {
        let data = vec![
            "random text".to_string(),
            "some string".to_string(),
            "no pattern".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_detect_mixed_data() {
        let data = vec![
            "user@example.com".to_string(),
            "random text".to_string(),
            "admin@test.org".to_string(),
            "more text".to_string(),
        ];
        let patterns = detect_patterns(&data);

        // 50% emails should be detected
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Email");
        assert_eq!(patterns[0].match_count, 2);
        assert_eq!(patterns[0].match_percentage, 50.0);
    }

    #[test]
    fn test_pattern_threshold_5_percent() {
        // Only 1 out of 20 = 5% - should NOT be detected
        let mut data = vec!["user@example.com".to_string()];
        for _ in 0..19 {
            data.push("random text".to_string());
        }
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 0); // Below threshold
    }

    #[test]
    fn test_pattern_threshold_above_5_percent() {
        // 2 out of 20 = 10% - should be detected
        let mut data = vec!["user@example.com".to_string(), "admin@test.org".to_string()];
        for _ in 0..18 {
            data.push("random text".to_string());
        }
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].match_percentage, 10.0);
    }

    #[test]
    fn test_empty_data() {
        let data: Vec<String> = vec![];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_all_empty_strings() {
        let data = vec!["".to_string(), "".to_string(), "".to_string()];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_whitespace_handling() {
        let data = vec![
            " user@example.com ".to_string(),
            "  admin@test.org".to_string(),
            "contact@company.com  ".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Email");
        assert_eq!(patterns[0].match_count, 3);
    }

    #[test]
    fn test_whitespace_only_strings() {
        let data = vec!["  ".to_string(), "\t".to_string(), " \n ".to_string()];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 0);
    }

    #[test]
    fn test_pattern_regexes_compiled() {
        // Validate that all hardcoded regex patterns compile successfully
        // This test will fail at initialization if any pattern is invalid
        assert_eq!(PATTERN_REGEXES.len(), 5);
    }
}
