use regex::Regex;
use std::sync::LazyLock;

use crate::types::Pattern;

// Pre-compile pattern regexes for better performance
// These patterns are compiled once at startup instead of on every detect_patterns call
static PATTERN_REGEXES: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| {
    vec![
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
            Regex::new(r"^(?:\+39|0039|39)[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{5,10}$")
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
        (
            "IPv4",
            Regex::new(r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
                .expect("BUG: Invalid hardcoded regex pattern for IPv4"),
        ),
        (
            "IPv6",
            Regex::new(r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$")
                .expect("BUG: Invalid hardcoded regex pattern for IPv6"),
        ),
        (
            "MAC Address",
            Regex::new(r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$")
                .expect("BUG: Invalid hardcoded regex pattern for MAC Address"),
        ),
        (
            "Geographic Coordinates",
            Regex::new(r"^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$")
                .expect("BUG: Invalid hardcoded regex pattern for Geographic Coordinates"),
        ),
        (
            "IBAN",
            Regex::new(r"^[A-Z]{2}\d{2}[A-Z0-9]{1,30}$")
                .expect("BUG: Invalid hardcoded regex pattern for IBAN"),
        ),
        (
            "Codice Fiscale (IT)",
            Regex::new(r"^[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]$")
                .expect("BUG: Invalid hardcoded regex pattern for Codice Fiscale"),
        ),
        (
            "P.IVA (IT)",
            Regex::new(r"^\d{11}$")
                .expect("BUG: Invalid hardcoded regex pattern for P.IVA"),
        ),
        (
            "CAP (IT)",
            Regex::new(r"^\d{5}$")
                .expect("BUG: Invalid hardcoded regex pattern for CAP"),
        ),
        (
            "ZIP Code (US)",
            Regex::new(r"^\d{5}(-\d{4})?$")
                .expect("BUG: Invalid hardcoded regex pattern for ZIP Code"),
        ),
        (
            "File Path (Unix)",
            Regex::new(r"^(/[^/\x00]+)+/?$")
                .expect("BUG: Invalid hardcoded regex pattern for Unix File Path"),
        ),
        (
            "File Path (Windows)",
            Regex::new(r##"^[A-Z]:\\(?:[^\\/:*?"<>|\r\n]+\\)*[^\\/:*?"<>|\r\n]*$"##)
                .expect("BUG: Invalid hardcoded regex pattern for Windows File Path"),
        ),
    ]
});

// Pre-compile date pattern regexes for type inference
// Previously this was duplicated across 5 engine files without pre-compilation
static DATE_PATTERN_REGEXES: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    vec![
        Regex::new(r"^\d{4}-\d{2}-\d{2}$").expect("BUG: Invalid date pattern YYYY-MM-DD"),
        Regex::new(r"^\d{2}/\d{2}/\d{4}$").expect("BUG: Invalid date pattern DD/MM/YYYY"),
        Regex::new(r"^\d{2}-\d{2}-\d{4}$").expect("BUG: Invalid date pattern DD-MM-YYYY"),
        Regex::new(r"^\d{4}/\d{2}/\d{2}$").expect("BUG: Invalid date pattern YYYY/MM/DD"),
        Regex::new(r"^\d{2}\.\d{2}\.\d{4}$").expect("BUG: Invalid date pattern DD.MM.YYYY"),
        Regex::new(r"^\d{4}-\d{2}-\d{2}T\d{2}:").expect("BUG: Invalid date pattern ISO datetime"),
    ]
});

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

/// Check if a string value looks like a date
///
/// Uses pre-compiled regex patterns to detect common date formats.
/// This function consolidates date detection logic that was previously
/// duplicated across multiple engine files.
///
/// # Arguments
/// * `value` - String value to check
///
/// # Returns
/// `true` if the value matches any common date pattern
///
/// # Supported Formats
/// - YYYY-MM-DD (ISO 8601)
/// - DD/MM/YYYY (European slash)
/// - DD-MM-YYYY (European dash)
/// - YYYY/MM/DD (Asian format)
/// - DD.MM.YYYY (European dot)
/// - ISO datetime (YYYY-MM-DDTHH:...)
pub fn looks_like_date(value: &str) -> bool {
    DATE_PATTERN_REGEXES.iter().any(|re| re.is_match(value))
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
    fn test_detect_ipv4_pattern() {
        let data = vec![
            "192.168.1.1".to_string(),
            "10.0.0.1".to_string(),
            "255.255.255.0".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "IPv4");
        assert_eq!(patterns[0].match_count, 3);
        assert_eq!(patterns[0].match_percentage, 100.0);
    }

    #[test]
    fn test_detect_ipv6_pattern() {
        let data = vec![
            "2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string(),
            "fe80:0000:0000:0000:0204:61ff:fe9d:f156".to_string(),
            "2001:0db8:0001:0000:0000:0ab9:C0A8:0102".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "IPv6");
    }

    #[test]
    fn test_detect_mac_address_pattern() {
        let data = vec![
            "00:1B:44:11:3A:B7".to_string(),
            "A0-B1-C2-D3-E4-F5".to_string(),
            "aa:bb:cc:dd:ee:ff".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "MAC Address");
    }

    #[test]
    fn test_detect_coordinates_pattern() {
        let data = vec![
            "41.9028, 12.4964".to_string(),   // Rome
            "40.7128, -74.0060".to_string(),  // New York
            "-33.8688, 151.2093".to_string(), // Sydney
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Geographic Coordinates");
    }

    #[test]
    fn test_detect_iban_pattern() {
        let data = vec![
            "IT60X0542811101000000123456".to_string(),
            "GB82WEST12345698765432".to_string(),
            "DE89370400440532013000".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "IBAN");
    }

    #[test]
    fn test_detect_codice_fiscale_pattern() {
        let data = vec![
            "RSSMRA85M01H501Z".to_string(),
            "BNCGVN90A01F205X".to_string(),
            "VRDLCU75D15L219K".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "Codice Fiscale (IT)");
    }

    #[test]
    fn test_detect_piva_pattern() {
        let data = vec![
            "12345678901".to_string(),
            "98765432109".to_string(),
            "11111111111".to_string(),
        ];
        let patterns = detect_patterns(&data);

        // P.IVA is 11 digits, so it should match
        assert!(!patterns.is_empty());
        assert!(patterns.iter().any(|p| p.name == "P.IVA (IT)"));
    }

    #[test]
    fn test_detect_cap_pattern() {
        let data = vec![
            "00118".to_string(),
            "20121".to_string(),
            "10100".to_string(),
        ];
        let patterns = detect_patterns(&data);

        // Note: CAP might conflict with ZIP Code pattern
        assert!(!patterns.is_empty());
        assert!(patterns
            .iter()
            .any(|p| p.name == "CAP (IT)" || p.name == "ZIP Code (US)"));
    }

    #[test]
    fn test_detect_zip_code_pattern() {
        let data = vec![
            "12345".to_string(),
            "90210-1234".to_string(),
            "10001".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert!(!patterns.is_empty());
        assert!(patterns
            .iter()
            .any(|p| p.name == "ZIP Code (US)" || p.name == "CAP (IT)"));
    }

    #[test]
    fn test_detect_unix_file_path_pattern() {
        let data = vec![
            "/home/user/documents".to_string(),
            "/var/log/system.log".to_string(),
            "/etc/config/app.conf".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "File Path (Unix)");
    }

    #[test]
    fn test_detect_windows_file_path_pattern() {
        let data = vec![
            "C:\\Users\\Admin\\Documents".to_string(),
            "D:\\Projects\\myapp\\src".to_string(),
            "E:\\Data\\file.txt".to_string(),
        ];
        let patterns = detect_patterns(&data);

        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].name, "File Path (Windows)");
    }

    #[test]
    fn test_looks_like_date_function() {
        // Test various date formats
        assert!(looks_like_date("2024-01-15"));
        assert!(looks_like_date("15/01/2024"));
        assert!(looks_like_date("15-01-2024"));
        assert!(looks_like_date("2024/01/15"));
        assert!(looks_like_date("15.01.2024"));
        assert!(looks_like_date("2024-01-15T10:30:00"));

        // Test non-dates
        assert!(!looks_like_date("not a date"));
        assert!(!looks_like_date("12345"));
        assert!(!looks_like_date("user@example.com"));
    }

    #[test]
    fn test_date_pattern_regexes_precompiled() {
        // Validate that all date patterns compile successfully
        assert_eq!(DATE_PATTERN_REGEXES.len(), 6);
    }

    #[test]
    fn test_pattern_regexes_compiled() {
        // Validate that all hardcoded regex patterns compile successfully
        // This test will fail at initialization if any pattern is invalid
        assert_eq!(PATTERN_REGEXES.len(), 16);
    }
}
