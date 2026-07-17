//! Shared conversion from [`StreamingColumnCollection`] / [`StreamingStatistics`]
//! into [`ColumnProfile`] and quality-check sample maps.
//!
//! All engines that need to produce a [`ColumnProfile`] should call
//! [`build_column_profile`] instead of constructing one manually.
//! This ensures consistent stats calculation and pattern detection.

use std::collections::HashMap;

use dataprof_core::{
    BooleanStats, ColumnProfile, ColumnStats, DataType, DateTimeStats, SemanticHints, TextStats,
};
use dataprof_metrics::{
    analysis::inference::{is_null_like_token, parse_strict_boolean_token},
    analysis::patterns::looks_like_date,
    calculate_datetime_stats, calculate_text_stats, detect_patterns,
    stats::numeric::{calculate_coefficient_of_variation, compute_numeric_stats},
};

use crate::streaming_stats::{StreamingColumnCollection, StreamingStatistics};

/// Inputs that every engine can provide for centralized profile construction.
pub struct ColumnProfileInput<'a> {
    pub name: String,
    pub data_type: DataType,
    pub total_count: usize,
    pub null_count: usize,
    pub unique_count: Option<usize>,
    /// Whether `unique_count` is an approximate (HLL) estimate. `None` mirrors
    /// `unique_count: None`; `Some(false)` for exact counts; `Some(true)` once
    /// the engine's cardinality estimator has spilled to its HLL sketch.
    pub unique_count_is_approximate: Option<bool>,
    pub sample_values: &'a [String],
    /// Pre-computed text lengths for engines that track them incrementally.
    /// When `Some`, text stats are built from these instead of re-scanning samples.
    pub text_lengths: Option<TextLengths>,
    /// Pre-computed boolean counts (true_count, false_count) for boolean columns.
    pub boolean_counts: Option<(usize, usize)>,
    /// When true, skip statistics computation (produce `ColumnStats::None`).
    pub skip_statistics: bool,
    /// When true, skip pattern detection (produce `patterns: None`).
    pub skip_patterns: bool,
    /// Optional locale for pattern detection (e.g. "IT", "US").
    pub locale: Option<&'a str>,
    /// Exact aggregates over every numeric value the engine streamed.
    ///
    /// When present, these override the sample-derived `min`, `max`, `mean`,
    /// `std_dev`, and `variance` on numeric columns, so those fields stay
    /// exact even when `sample_values` no longer covers the full stream.
    /// `None` means `sample_values` *is* the full data (in-memory sources)
    /// or the engine has no exact accumulators for the column.
    pub exact_numeric: Option<ExactNumericAggregates>,
}

/// Exact streaming aggregates for a numeric column: O(1)-memory statistics an
/// engine computed over the entire stream, as opposed to the bounded
/// `sample_values` it retained.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExactNumericAggregates {
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    /// Standard deviation with the unbiased (n-1) denominator.
    pub std_dev: f64,
    /// Variance with the unbiased (n-1) denominator.
    pub variance: f64,
    /// Number of parsed numeric values covered by these aggregates.
    pub count: usize,
}

/// Pre-computed text length stats from streaming/columnar engines.
pub struct TextLengths {
    pub min_length: usize,
    pub max_length: usize,
    pub avg_length: f64,
}

/// Build a [`ColumnProfile`] from engine-agnostic inputs.
///
/// This is the single canonical construction path. Engines provide
/// pre-inferred `DataType`, counters, sample values, and optionally
/// pre-computed text lengths; this function handles stats calculation
/// and pattern detection.
pub fn build_column_profile(input: ColumnProfileInput<'_>) -> ColumnProfile {
    let stats = if input.skip_statistics {
        ColumnStats::None
    } else {
        match input.data_type {
            DataType::Integer | DataType::Float => {
                let mut numeric = compute_numeric_stats(input.sample_values);
                if let Some(exact) = &input.exact_numeric {
                    numeric.min = exact.min;
                    numeric.max = exact.max;
                    numeric.mean = exact.mean;
                    numeric.std_dev = exact.std_dev;
                    numeric.variance = exact.variance;
                    numeric.coefficient_of_variation =
                        calculate_coefficient_of_variation(exact.std_dev, exact.mean);
                    // Order statistics (median, quartiles, mode, skewness,
                    // kurtosis, outliers) still come from the retained sample;
                    // disclose that whenever the sample no longer covers every
                    // numeric value the exact aggregates saw.
                    let sampled_numeric = input
                        .sample_values
                        .iter()
                        .filter(|s| s.parse::<f64>().ok().is_some_and(|v| v.is_finite()))
                        .count();
                    if exact.count > sampled_numeric {
                        numeric.is_approximate = Some(true);
                    }
                }
                ColumnStats::Numeric(numeric)
            }
            DataType::Date => {
                if !input.sample_values.is_empty() {
                    calculate_datetime_stats(input.sample_values)
                } else if let Some(tl) = &input.text_lengths {
                    ColumnStats::Text(TextStats::from_lengths(
                        tl.min_length,
                        tl.max_length,
                        tl.avg_length,
                    ))
                } else {
                    ColumnStats::DateTime(DateTimeStats::empty())
                }
            }
            DataType::Boolean => {
                let (true_count, false_count) = input.boolean_counts.unwrap_or_else(|| {
                    let tc = input
                        .sample_values
                        .iter()
                        .filter(|v| parse_strict_boolean_token(v.trim()) == Some(true))
                        .count();
                    let fc = input
                        .sample_values
                        .iter()
                        .filter(|v| parse_strict_boolean_token(v.trim()) == Some(false))
                        .count();
                    (tc, fc)
                });
                let total = true_count + false_count;
                let true_ratio = if total > 0 {
                    true_count as f64 / total as f64
                } else {
                    0.0
                };
                ColumnStats::Boolean(BooleanStats {
                    true_count,
                    false_count,
                    true_ratio,
                })
            }
            DataType::String | DataType::Identifier => {
                if let Some(tl) = &input.text_lengths {
                    ColumnStats::Text(TextStats::from_lengths(
                        tl.min_length,
                        tl.max_length,
                        tl.avg_length,
                    ))
                } else {
                    calculate_text_stats(input.sample_values)
                }
            }
        }
    };

    let patterns = if input.skip_patterns {
        None
    } else {
        Some(detect_patterns(input.sample_values, input.locale))
    };

    ColumnProfile {
        name: input.name,
        data_type: input.data_type,
        null_count: input.null_count,
        total_count: input.total_count,
        unique_count: input.unique_count,
        unique_count_is_approximate: input.unique_count_is_approximate,
        stats,
        patterns,
    }
}

/// Convert all columns in a [`StreamingColumnCollection`] into [`ColumnProfile`]s.
pub fn profiles_from_streaming(
    column_stats: &StreamingColumnCollection,
    skip_statistics: bool,
    skip_patterns: bool,
    locale: Option<&str>,
) -> Vec<ColumnProfile> {
    profiles_from_streaming_with_hints(
        column_stats,
        skip_statistics,
        skip_patterns,
        locale,
        &SemanticHints::default(),
    )
}

/// Convert all columns into [`ColumnProfile`]s while applying semantic hints.
pub fn profiles_from_streaming_with_hints(
    column_stats: &StreamingColumnCollection,
    skip_statistics: bool,
    skip_patterns: bool,
    locale: Option<&str>,
    semantic_hints: &SemanticHints,
) -> Vec<ColumnProfile> {
    let mut profiles = Vec::new();

    for column_name in column_stats.column_names() {
        if let Some(stats) = column_stats.get_column_stats(&column_name) {
            let profile = profile_from_stats_with_hints(
                &column_name,
                stats,
                skip_statistics,
                skip_patterns,
                locale,
                semantic_hints,
            );
            profiles.push(profile);
        }
    }

    profiles
}

/// Convert a single column's [`StreamingStatistics`] into a [`ColumnProfile`].
pub fn profile_from_stats(
    name: &str,
    stats: &StreamingStatistics,
    skip_statistics: bool,
    skip_patterns: bool,
    locale: Option<&str>,
) -> ColumnProfile {
    profile_from_stats_with_hints(
        name,
        stats,
        skip_statistics,
        skip_patterns,
        locale,
        &SemanticHints::default(),
    )
}

/// Convert a single column into a [`ColumnProfile`] while applying semantic hints.
pub fn profile_from_stats_with_hints(
    name: &str,
    stats: &StreamingStatistics,
    skip_statistics: bool,
    skip_patterns: bool,
    locale: Option<&str>,
    semantic_hints: &SemanticHints,
) -> ColumnProfile {
    let data_type = if semantic_hints.is_identifier_column(name) {
        DataType::Identifier
    } else {
        infer_data_type_streaming(stats)
    };
    let text_stats = stats.text_length_stats();

    build_column_profile(ColumnProfileInput {
        name: name.to_string(),
        data_type,
        total_count: stats.count,
        null_count: stats.null_count,
        unique_count: Some(stats.unique_count()),
        unique_count_is_approximate: Some(stats.unique_count_is_approximate()),
        sample_values: stats.sample_values(),
        text_lengths: Some(TextLengths {
            min_length: text_stats.min_length,
            max_length: text_stats.max_length,
            avg_length: text_stats.avg_length,
        }),
        boolean_counts: None,
        skip_statistics,
        skip_patterns,
        locale,
        exact_numeric: stats.exact_numeric_aggregates(),
    })
}

/// Infer [`DataType`] from [`StreamingStatistics`] sample values.
pub fn infer_data_type_streaming(stats: &StreamingStatistics) -> DataType {
    if stats.min.is_finite() && stats.max.is_finite() {
        let sample_values = stats.sample_values();
        let non_empty: Vec<&String> = sample_values
            .iter()
            .filter(|s| !is_null_like_token(s.trim()))
            .collect();

        if !non_empty.is_empty() {
            let all_integers = non_empty.iter().all(|s| s.parse::<i64>().is_ok());
            if all_integers {
                return DataType::Integer;
            }

            let numeric_count = non_empty
                .iter()
                .filter(|s| s.parse::<f64>().is_ok())
                .count();
            if numeric_count as f64 / non_empty.len() as f64 > 0.8 {
                return DataType::Float;
            }
        }
    }

    let sample_values = stats.sample_values();
    let non_empty: Vec<&String> = sample_values
        .iter()
        .filter(|s| !is_null_like_token(s.trim()))
        .collect();

    if !non_empty.is_empty() {
        let date_like_count = non_empty
            .iter()
            .take(100)
            .filter(|s| looks_like_date(s))
            .count();

        if date_like_count as f64 / non_empty.len().min(100) as f64 > 0.7 {
            return DataType::Date;
        }

        let bool_count = non_empty
            .iter()
            .filter(|s| parse_strict_boolean_token(s.trim()).is_some())
            .count();

        if bool_count as f64 / non_empty.len() as f64 >= 0.9 {
            return DataType::Boolean;
        }
    }

    DataType::String
}

/// Build a sample `HashMap` from a [`StreamingColumnCollection`] suitable for
/// `QualityMetrics::calculate_from_data()`.
pub fn quality_check_samples(
    column_stats: &StreamingColumnCollection,
) -> HashMap<String, Vec<String>> {
    let mut samples = HashMap::new();

    for column_name in column_stats.column_names() {
        if let Some(stats) = column_stats.get_column_stats(&column_name) {
            let sample_values: Vec<String> = stats.sample_values().to_vec();
            samples.insert(column_name, sample_values);
        }
    }

    samples
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming_stats::StreamingColumnCollection;

    #[test]
    fn test_profiles_from_streaming() {
        let mut collection = StreamingColumnCollection::new();
        let headers = vec!["name".to_string(), "age".to_string()];

        collection.process_record(&headers, vec!["Alice".to_string(), "30".to_string()]);
        collection.process_record(&headers, vec!["Bob".to_string(), "25".to_string()]);
        collection.process_record(&headers, vec!["Charlie".to_string(), "35".to_string()]);

        let profiles = profiles_from_streaming(&collection, false, false, None);
        assert_eq!(profiles.len(), 2);

        let age = profiles.iter().find(|p| p.name == "age").unwrap();
        assert_eq!(age.data_type, DataType::Integer);
        assert_eq!(age.total_count, 3);
    }

    #[test]
    fn test_quality_check_samples() {
        let mut collection = StreamingColumnCollection::new();
        let headers = vec!["col".to_string()];

        collection.process_record(&headers, vec!["val1".to_string()]);
        collection.process_record(&headers, vec!["val2".to_string()]);

        let samples = quality_check_samples(&collection);
        assert!(samples.contains_key("col"));
        assert_eq!(samples["col"].len(), 2);
    }

    #[test]
    fn test_boolean_stats_with_counts() {
        let samples = vec!["True".to_string(), "False".to_string(), "True".to_string()];
        let profile = build_column_profile(ColumnProfileInput {
            name: "flag".to_string(),
            data_type: DataType::Boolean,
            total_count: 3,
            null_count: 0,
            unique_count: Some(2),
            unique_count_is_approximate: Some(false),
            sample_values: &samples,
            text_lengths: None,
            boolean_counts: Some((2, 1)),
            skip_statistics: false,
            skip_patterns: false,
            exact_numeric: None,
            locale: None,
        });

        match &profile.stats {
            ColumnStats::Boolean(b) => {
                assert_eq!(b.true_count, 2);
                assert_eq!(b.false_count, 1);
                assert!((b.true_ratio - 2.0 / 3.0).abs() < 0.001);
            }
            other => panic!("expected Boolean stats, got {:?}", other),
        }
    }

    #[test]
    fn test_boolean_stats_fallback_case_insensitive() {
        let samples = vec![
            "true".to_string(),
            "FALSE".to_string(),
            " True ".to_string(),
        ];
        let profile = build_column_profile(ColumnProfileInput {
            name: "flag".to_string(),
            data_type: DataType::Boolean,
            total_count: 3,
            null_count: 0,
            unique_count: Some(2),
            unique_count_is_approximate: Some(false),
            sample_values: &samples,
            text_lengths: None,
            boolean_counts: None,
            skip_statistics: false,
            skip_patterns: false,
            exact_numeric: None,
            locale: None,
        });

        match &profile.stats {
            ColumnStats::Boolean(b) => {
                assert_eq!(b.true_count, 2);
                assert_eq!(b.false_count, 1);
                assert!((b.true_ratio - 2.0 / 3.0).abs() < 0.001);
            }
            other => panic!("expected Boolean stats, got {:?}", other),
        }
    }

    #[test]
    fn test_skip_statistics() {
        let samples = vec!["10".to_string(), "20".to_string(), "30".to_string()];
        let profile = build_column_profile(ColumnProfileInput {
            name: "num".to_string(),
            data_type: DataType::Integer,
            total_count: 3,
            null_count: 0,
            unique_count: Some(3),
            unique_count_is_approximate: Some(false),
            sample_values: &samples,
            text_lengths: None,
            boolean_counts: None,
            skip_statistics: true,
            skip_patterns: false,
            exact_numeric: None,
            locale: None,
        });

        assert!(matches!(profile.stats, ColumnStats::None));
        assert_eq!(profile.data_type, DataType::Integer);
    }

    #[test]
    fn test_skip_patterns() {
        let samples = vec!["hello".to_string(), "world".to_string()];
        let profile = build_column_profile(ColumnProfileInput {
            name: "text".to_string(),
            data_type: DataType::String,
            total_count: 2,
            null_count: 0,
            unique_count: Some(2),
            unique_count_is_approximate: Some(false),
            sample_values: &samples,
            text_lengths: None,
            boolean_counts: None,
            skip_statistics: false,
            skip_patterns: true,
            exact_numeric: None,
            locale: None,
        });

        // skip_patterns must be distinguishable from "scanned, nothing matched",
        // otherwise downstream redaction gates cannot fail closed.
        assert!(profile.patterns.is_none());
        assert!(matches!(profile.stats, ColumnStats::Text(_)));
    }

    #[test]
    fn test_scanned_without_matches_is_not_none() {
        // The counterpart to `test_skip_patterns`: a column that *was* scanned
        // and matched nothing yields `Some([])`, which downstream consumers may
        // safely read as "no sensitive data here".
        let samples = vec!["hello".to_string(), "world".to_string()];
        let profile = build_column_profile(ColumnProfileInput {
            name: "text".to_string(),
            data_type: DataType::String,
            total_count: 2,
            null_count: 0,
            unique_count: Some(2),
            unique_count_is_approximate: Some(false),
            sample_values: &samples,
            text_lengths: None,
            boolean_counts: None,
            skip_statistics: false,
            skip_patterns: false,
            exact_numeric: None,
            locale: None,
        });

        assert!(profile.patterns.is_some_and(|p| p.is_empty()));
    }

    #[test]
    fn test_all_packs_default() {
        let samples = vec!["42".to_string(), "99".to_string()];
        let profile = build_column_profile(ColumnProfileInput {
            name: "val".to_string(),
            data_type: DataType::Integer,
            total_count: 2,
            null_count: 0,
            unique_count: Some(2),
            unique_count_is_approximate: Some(false),
            sample_values: &samples,
            text_lengths: None,
            boolean_counts: None,
            skip_statistics: false,
            skip_patterns: false,
            exact_numeric: None,
            locale: None,
        });

        assert!(matches!(profile.stats, ColumnStats::Numeric(_)));
        assert_eq!(profile.data_type, DataType::Integer);
    }
}
