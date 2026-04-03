//! Shared conversion from [`StreamingColumnCollection`] / [`StreamingStatistics`]
//! into [`ColumnProfile`] and quality-check sample maps.
//!
//! All engines that need to produce a [`ColumnProfile`] should call
//! [`build_column_profile`] instead of constructing one manually.
//! This ensures consistent stats calculation and pattern detection.

use crate::analysis::patterns::looks_like_date;
use crate::core::streaming_stats::{StreamingColumnCollection, StreamingStatistics};
use crate::types::{ColumnProfile, ColumnStats, DataType, TextStats};

use std::collections::HashMap;

// ── Canonical builder ───────────────────────────────────────────────────

/// Inputs that every engine can provide for centralized profile construction.
pub struct ColumnProfileInput<'a> {
    pub name: String,
    pub data_type: DataType,
    pub total_count: usize,
    pub null_count: usize,
    pub unique_count: Option<usize>,
    pub sample_values: &'a [String],
    /// Pre-computed text lengths for engines that track them incrementally.
    /// When `Some`, text stats are built from these instead of re-scanning samples.
    pub text_lengths: Option<TextLengths>,
    /// Pre-computed boolean counts (true_count, false_count) for boolean columns.
    pub boolean_counts: Option<(usize, usize)>,
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
    let stats = match input.data_type {
        DataType::Integer | DataType::Float => {
            crate::stats::numeric::calculate_numeric_stats(input.sample_values)
        }
        DataType::Date => {
            // Produce real datetime stats when sample values are available;
            // fall back to text lengths for engines that only tracked lengths.
            if !input.sample_values.is_empty() {
                crate::stats::datetime::calculate_datetime_stats(input.sample_values)
            } else if let Some(tl) = &input.text_lengths {
                ColumnStats::Text(TextStats::from_lengths(
                    tl.min_length,
                    tl.max_length,
                    tl.avg_length,
                ))
            } else {
                ColumnStats::DateTime(crate::types::DateTimeStats::empty())
            }
        }
        DataType::Boolean => {
            let (true_count, false_count) = input.boolean_counts.unwrap_or_else(|| {
                // Fall back: count from sample values
                let tc = input.sample_values.iter().filter(|v| *v == "True").count();
                let fc = input.sample_values.iter().filter(|v| *v == "False").count();
                (tc, fc)
            });
            let total = true_count + false_count;
            let true_ratio = if total > 0 {
                true_count as f64 / total as f64
            } else {
                0.0
            };
            ColumnStats::Boolean(crate::types::BooleanStats {
                true_count,
                false_count,
                true_ratio,
            })
        }
        DataType::String => {
            if let Some(tl) = &input.text_lengths {
                ColumnStats::Text(TextStats::from_lengths(
                    tl.min_length,
                    tl.max_length,
                    tl.avg_length,
                ))
            } else {
                crate::stats::text::calculate_text_stats(input.sample_values)
            }
        }
    };

    let patterns = crate::detect_patterns(input.sample_values);

    ColumnProfile {
        name: input.name,
        data_type: input.data_type,
        null_count: input.null_count,
        total_count: input.total_count,
        unique_count: input.unique_count,
        stats,
        patterns,
    }
}

// ── Streaming helpers ───────────────────────────────────────────────────

/// Convert all columns in a [`StreamingColumnCollection`] into [`ColumnProfile`]s.
pub fn profiles_from_streaming(column_stats: &StreamingColumnCollection) -> Vec<ColumnProfile> {
    let mut profiles = Vec::new();

    for column_name in column_stats.column_names() {
        if let Some(stats) = column_stats.get_column_stats(&column_name) {
            let profile = profile_from_stats(&column_name, stats);
            profiles.push(profile);
        }
    }

    profiles
}

/// Convert a single column's [`StreamingStatistics`] into a [`ColumnProfile`].
pub fn profile_from_stats(name: &str, stats: &StreamingStatistics) -> ColumnProfile {
    let data_type = infer_data_type_streaming(stats);
    let text_stats = stats.text_length_stats();

    build_column_profile(ColumnProfileInput {
        name: name.to_string(),
        data_type,
        total_count: stats.count,
        null_count: stats.null_count,
        unique_count: Some(stats.unique_count()),
        sample_values: stats.sample_values(),
        text_lengths: Some(TextLengths {
            min_length: text_stats.min_length,
            max_length: text_stats.max_length,
            avg_length: text_stats.avg_length,
        }),
        boolean_counts: None,
    })
}

/// Infer [`DataType`] from [`StreamingStatistics`] sample values.
pub fn infer_data_type_streaming(stats: &StreamingStatistics) -> DataType {
    if stats.min.is_finite() && stats.max.is_finite() {
        let sample_values = stats.sample_values();
        let non_empty: Vec<&String> = sample_values.iter().filter(|s| !s.is_empty()).collect();

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
    let non_empty: Vec<&String> = sample_values.iter().filter(|s| !s.is_empty()).collect();

    if !non_empty.is_empty() {
        let date_like_count = non_empty
            .iter()
            .take(100)
            .filter(|s| looks_like_date(s))
            .count();

        if date_like_count as f64 / non_empty.len().min(100) as f64 > 0.7 {
            return DataType::Date;
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
    use crate::core::streaming_stats::StreamingColumnCollection;

    #[test]
    fn test_profiles_from_streaming() {
        let mut collection = StreamingColumnCollection::new();
        let headers = vec!["name".to_string(), "age".to_string()];

        collection.process_record(&headers, vec!["Alice".to_string(), "30".to_string()]);
        collection.process_record(&headers, vec!["Bob".to_string(), "25".to_string()]);
        collection.process_record(&headers, vec!["Charlie".to_string(), "35".to_string()]);

        let profiles = profiles_from_streaming(&collection);
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
}
