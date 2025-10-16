use crate::types::{ColumnProfile, DataType};

use crate::analysis::inference::infer_type;
use crate::analysis::patterns::detect_patterns;
use crate::stats::{calculate_numeric_stats, calculate_text_stats};

/// Analyze a column with full profiling (includes pattern detection and unique counts)
pub fn analyze_column(name: &str, data: &[String]) -> ColumnProfile {
    analyze_column_with_options(name, data, false)
}

/// Analyze a column in fast mode (skips expensive operations)
pub fn analyze_column_fast(name: &str, data: &[String]) -> ColumnProfile {
    analyze_column_with_options(name, data, true)
}

/// Analyze a column with configurable options
///
/// # Arguments
/// * `name` - Column name
/// * `data` - Column data as strings
/// * `fast_mode` - If true, skips expensive operations (pattern detection, unique counts)
///
/// # Performance Considerations
/// - Fast mode skips pattern detection and unique count calculation
/// - Whitespace-only values are treated as null (aligned with inference logic)
///
/// # Returns
/// Complete column profile including type, stats, and optionally patterns
fn analyze_column_with_options(name: &str, data: &[String], fast_mode: bool) -> ColumnProfile {
    let total_count = data.len();

    // Aligned with inference.rs: whitespace-only strings are treated as null
    let null_count = data.iter().filter(|s| s.trim().is_empty()).count();

    // Infer type (uses same whitespace logic internally)
    let data_type = infer_type(data);

    // Calculate stats
    let stats = match data_type {
        DataType::Integer | DataType::Float => calculate_numeric_stats(data),
        DataType::String | DataType::Date => calculate_text_stats(data),
    };

    // Skip expensive operations in fast mode
    let patterns = if fast_mode {
        Vec::new()
    } else {
        detect_patterns(data)
    };

    let unique_count = if fast_mode {
        None // Skip expensive unique count in fast mode
    } else {
        // Count unique non-null values (aligned with whitespace logic)
        Some(
            data.iter()
                .filter(|s| !s.trim().is_empty())
                .collect::<std::collections::HashSet<_>>()
                .len(),
        )
    };

    ColumnProfile {
        name: name.to_string(),
        data_type,
        null_count,
        total_count,
        unique_count,
        stats,
        patterns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_column_basic() {
        let data = vec!["1".to_string(), "2".to_string(), "3".to_string()];
        let profile = analyze_column("test_col", &data);

        assert_eq!(profile.name, "test_col");
        assert!(matches!(profile.data_type, DataType::Integer));
        assert_eq!(profile.total_count, 3);
        assert_eq!(profile.null_count, 0);
        assert_eq!(profile.unique_count, Some(3));
    }

    #[test]
    fn test_analyze_column_with_nulls() {
        let data = vec![
            "1".to_string(),
            "".to_string(),
            "3".to_string(),
            "".to_string(),
        ];
        let profile = analyze_column("test_col", &data);

        assert_eq!(profile.total_count, 4);
        assert_eq!(profile.null_count, 2);
        assert_eq!(profile.unique_count, Some(2)); // Only non-null unique values
    }

    #[test]
    fn test_analyze_column_whitespace_as_null() {
        let data = vec![
            "1".to_string(),
            "  ".to_string(), // Whitespace-only treated as null
            "3".to_string(),
            "\t".to_string(), // Tab treated as null
        ];
        let profile = analyze_column("test_col", &data);

        assert_eq!(profile.total_count, 4);
        assert_eq!(profile.null_count, 2); // Whitespace counted as null
        assert_eq!(profile.unique_count, Some(2)); // Only "1" and "3"
        assert!(matches!(profile.data_type, DataType::Integer));
    }

    #[test]
    fn test_analyze_column_with_whitespace_values() {
        let data = vec![" 1 ".to_string(), "  2".to_string(), "3  ".to_string()];
        let profile = analyze_column("test_col", &data);

        assert_eq!(profile.null_count, 0); // Trimmed values are not null
        assert!(matches!(profile.data_type, DataType::Integer));
    }

    #[test]
    fn test_analyze_column_fast_mode() {
        let data = vec![
            "user@example.com".to_string(),
            "admin@test.org".to_string(),
            "contact@company.com".to_string(),
        ];
        let profile = analyze_column_fast("test_col", &data);

        assert_eq!(profile.patterns.len(), 0); // Fast mode skips patterns
        assert_eq!(profile.unique_count, None); // Fast mode skips unique count
    }

    #[test]
    fn test_analyze_column_normal_mode() {
        let data = vec![
            "user@example.com".to_string(),
            "admin@test.org".to_string(),
            "contact@company.com".to_string(),
        ];
        let profile = analyze_column("test_col", &data);

        assert!(!profile.patterns.is_empty()); // Normal mode detects patterns
        assert_eq!(profile.unique_count, Some(3)); // Normal mode calculates unique count
    }

    #[test]
    fn test_analyze_column_empty_data() {
        let data: Vec<String> = vec![];
        let profile = analyze_column("test_col", &data);

        assert_eq!(profile.total_count, 0);
        assert_eq!(profile.null_count, 0);
        assert_eq!(profile.unique_count, Some(0));
    }

    #[test]
    fn test_analyze_column_all_null() {
        let data = vec!["".to_string(), "  ".to_string(), "\t".to_string()];
        let profile = analyze_column("test_col", &data);

        assert_eq!(profile.total_count, 3);
        assert_eq!(profile.null_count, 3);
        assert_eq!(profile.unique_count, Some(0)); // No non-null unique values
        assert!(matches!(profile.data_type, DataType::String)); // Default for all-null
    }

    #[test]
    fn test_analyze_column_float_detection() {
        let data = vec!["1.5".to_string(), "2.3".to_string(), "3.7".to_string()];
        let profile = analyze_column("test_col", &data);

        assert!(matches!(profile.data_type, DataType::Float));
        assert_eq!(profile.null_count, 0);
    }

    #[test]
    fn test_analyze_column_date_detection() {
        let data = vec![
            "2023-01-15".to_string(),
            "2023-02-20".to_string(),
            "2023-03-25".to_string(),
        ];
        let profile = analyze_column("test_col", &data);

        assert!(matches!(profile.data_type, DataType::Date));
    }

    #[test]
    fn test_analyze_column_unique_count_consistency() {
        // Test that unique count excludes whitespace-only values
        let data = vec![
            "value1".to_string(),
            "value2".to_string(),
            "  ".to_string(),
            "value1".to_string(), // Duplicate
            "\t".to_string(),
        ];
        let profile = analyze_column("test_col", &data);

        assert_eq!(profile.null_count, 2); // 2 whitespace-only
        assert_eq!(profile.unique_count, Some(2)); // "value1" and "value2"
    }
}
