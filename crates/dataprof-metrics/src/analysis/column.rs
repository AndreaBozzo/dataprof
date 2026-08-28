use dataprof_core::AnalysisOptions;

use crate::types::{ColumnProfile, ColumnStats, DataType, Locale};

use crate::analysis::inference::{
    classify_lexical_forms, infer_type, is_null_like_token, parse_strict_boolean_token,
};
use crate::analysis::patterns::detect_patterns;
use crate::stats::numeric::compute_numeric_stats_with_parsed_count;
use crate::stats::{calculate_datetime_stats, calculate_text_stats};

/// Which parts of a column analysis to perform.
///
/// The streaming engines express the same choices through
/// `profile_builder::build_column_profile`; this is the in-memory equivalent for
/// callers that hold a whole column as `Vec<String>` (the database connectors).
struct ColumnAnalysis {
    /// Produce [`ColumnStats::None`] instead of computing statistics.
    skip_statistics: bool,
    /// Produce `patterns: None` instead of running detection.
    skip_patterns: bool,
    /// Locale used to rank detected patterns.
    locale: Option<Locale>,
    /// Leave `unique_count` absent instead of counting distinct values.
    skip_unique_count: bool,
}

/// Analyze a column with full profiling (includes pattern detection and unique counts)
pub fn analyze_column(name: &str, data: &[String]) -> ColumnProfile {
    analyze_column_with_options(
        name,
        data,
        &ColumnAnalysis {
            skip_statistics: false,
            skip_patterns: false,
            locale: None,
            skip_unique_count: false,
        },
    )
}

/// Analyze a column in fast mode (skips expensive operations)
pub fn analyze_column_fast(name: &str, data: &[String]) -> ColumnProfile {
    analyze_column_with_options(
        name,
        data,
        &ColumnAnalysis {
            skip_statistics: false,
            skip_patterns: true,
            locale: None,
            skip_unique_count: true,
        },
    )
}

/// Analyze a column, honouring the caller's analysis selection.
///
/// Metric packs decide whether statistics and pattern detection run, and the
/// locale ranks whatever patterns are detected — the same contract the file and
/// streaming paths honour. Distinct-value counting is schema-level information
/// rather than a pack, so it always runs here; use [`analyze_column_fast`] to
/// skip it.
pub fn analyze_column_with_analysis_options(
    name: &str,
    data: &[String],
    options: &AnalysisOptions,
) -> ColumnProfile {
    analyze_column_with_options(
        name,
        data,
        &ColumnAnalysis {
            skip_statistics: !options.include_statistics(),
            skip_patterns: !options.include_patterns(),
            locale: options.locale(),
            skip_unique_count: false,
        },
    )
}

/// Analyze a column with configurable options
///
/// # Performance Considerations
/// - Skipping patterns and unique counts avoids the expensive passes
/// - Whitespace-only values are treated as null (aligned with inference logic)
///
/// # Returns
/// Complete column profile including type, stats, and optionally patterns
fn analyze_column_with_options(
    name: &str,
    data: &[String],
    analysis: &ColumnAnalysis,
) -> ColumnProfile {
    let total_count = data.len();

    // Aligned with inference.rs: whitespace-only strings are treated as null
    let null_count = data.iter().filter(|s| is_null_like_token(s.trim())).count();

    // Infer type (uses same whitespace logic internally)
    let data_type = infer_type(data);

    // Calculate stats. Numeric columns also yield how many values parsed as
    // finite numbers, so the invalid count comes from the same single pass —
    // which is why skipping statistics also leaves `invalid_count` absent: it is
    // a by-product of the parse, not an independently measured fact.
    let mut invalid_count = None;
    let stats = if analysis.skip_statistics {
        ColumnStats::None
    } else {
        match data_type {
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
            DataType::Date => {
                let parsed = data
                    .iter()
                    .filter(|value| {
                        super::metrics::value_matches_hint(
                            value,
                            dataprof_core::SemanticHintKind::Temporal,
                        )
                    })
                    .count();
                invalid_count = Some(
                    total_count
                        .saturating_sub(null_count)
                        .saturating_sub(parsed),
                );
                calculate_datetime_stats(data)
            }
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
        }
    };

    let patterns = if analysis.skip_patterns {
        None
    } else {
        Some(detect_patterns(data, analysis.locale))
    };

    let unique_count = if analysis.skip_unique_count {
        None
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
        // Distinct values are counted with an exact HashSet, so the count is
        // exact whenever it was computed at all.
        unique_count_is_approximate: unique_count.map(|_| false),
        invalid_count,
        // Classified over `data`, which on this path is the whole column, so the
        // counts are exact rather than sampled. Not gated by the analysis
        // selection: which forms a column holds is schema-level evidence like
        // the inferred type itself, not a statistic derived from a pack.
        type_homogeneity: Some(classify_lexical_forms(data)),
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
        assert_eq!(profile.invalid_count, Some(0));
        let ColumnStats::Numeric(stats) = profile.stats else {
            panic!("whitespace-padded integers should have numeric statistics");
        };
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 3.0);
        assert_eq!(stats.mean, 2.0);
    }

    #[test]
    fn type_homogeneity_covers_the_whole_column_on_this_path() {
        // The in-memory path holds every value, so the counts are exact rather
        // than sampled: they must add up to the non-null total.
        let data: Vec<String> = (0..80)
            .map(|i| i.to_string())
            .chain((0..20).map(|i| format!("junk{i}")))
            .chain(std::iter::once(String::new()))
            .collect();

        let profile = analyze_column("v", &data);
        let counts = profile
            .type_homogeneity
            .expect("classification runs on every column");

        assert_eq!(counts.numeric, 80);
        assert_eq!(counts.text, 20);
        assert_eq!(
            counts.classified_count(),
            profile.total_count - profile.null_count
        );
    }

    #[test]
    fn type_homogeneity_is_recorded_even_when_the_column_is_ordinary() {
        // Absence has to mean "not classified". A clean numeric column reports
        // one class holding everything, not a missing field.
        let data = ["1", "2", "3"].map(String::from).to_vec();

        let counts = analyze_column("v", &data)
            .type_homogeneity
            .expect("present");

        assert_eq!(counts.dominant_share(), Some(1.0));
    }

    #[test]
    fn a_narrowed_metric_selection_still_classifies_the_column() {
        // Which forms a column holds is schema-level evidence like the inferred
        // type, not a statistic a narrowed pack selection can take away.
        let data = ["1", "2", "junk"].map(String::from).to_vec();
        let options = AnalysisOptions::default().with_metric_packs(Some(vec![]));
        assert!(!options.include_statistics());

        let profile = analyze_column_with_analysis_options("v", &data, &options);

        assert!(matches!(profile.stats, ColumnStats::None));
        assert_eq!(
            profile
                .type_homogeneity
                .expect("present")
                .classified_count(),
            3
        );
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
