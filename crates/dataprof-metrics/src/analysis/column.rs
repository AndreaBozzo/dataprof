use crate::types::{ColumnProfile, ColumnStats, DataType};

use crate::analysis::inference::{infer_type, is_null_like_token, parse_strict_boolean_token};
use crate::analysis::patterns::detect_patterns;
use crate::stats::numeric::compute_numeric_stats_with_parsed_count;
use crate::stats::{calculate_datetime_stats, calculate_text_stats};

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
    let null_count = data.iter().filter(|s| is_null_like_token(s.trim())).count();

    // Infer type (uses same whitespace logic internally)
    let data_type = infer_type(data);

    // Calculate stats. Numeric columns also yield how many values parsed as
    // finite numbers, so the invalid count comes from the same single pass.
    let mut invalid_count = None;
    let stats = match data_type {
        DataType::Integer | DataType::Float => {
            let (numeric, parsed) = compute_numeric_stats_with_parsed_count(data);
            // Non-null values that fail the finite-numeric parse are excluded
            // from the statistics; expose the count so denominators stay
            // auditable.
            invalid_count = Some(
                total_count
                    .saturating_sub(null_count)
                    .saturating_sub(parsed),
            );
            ColumnStats::Numeric(numeric)
        }
        DataType::Date => calculate_datetime_stats(data),
        DataType::Boolean => {
            let tc = data
                .iter()
                .filter(|v| parse_strict_boolean_token(v.trim()) == Some(true))
                .count();
            let fc = data
                .iter()
                .filter(|v| parse_strict_boolean_token(v.trim()) == Some(false))
                .count();
            let total = tc + fc;
            let true_ratio = if total > 0 {
                tc as f64 / total as f64
            } else {
                0.0
            };
            ColumnStats::Boolean(crate::types::BooleanStats {
                true_count: tc,
                false_count: fc,
                true_ratio,
            })
        }
        DataType::String | DataType::Identifier => calculate_text_stats(data),
    };

    // Skip expensive operations in fast mode
    let patterns = if fast_mode {
        None
    } else {
        Some(detect_patterns(data, None))
    };

    let unique_count = if fast_mode {
        None // Skip expensive unique count in fast mode
    } else {
        // Count unique non-null values (aligned with whitespace logic)
        Some(
            data.iter()
                .filter(|s| !is_null_like_token(s.trim()))
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
        // analyze_column counts distinct values with an exact HashSet, so the
        // count is exact whenever it was computed (never in fast mode).
        unique_count_is_approximate: unique_count.map(|_| false),
        invalid_count,
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

        assert!(profile.patterns.is_none()); // Fast mode skips patterns entirely
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

        // Normal mode detects patterns
        assert!(profile.patterns.is_some_and(|p| !p.is_empty()));
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
