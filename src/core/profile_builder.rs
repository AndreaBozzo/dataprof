//! Shared conversion from [`StreamingColumnCollection`] / [`StreamingStatistics`]
//! into [`ColumnProfile`] and quality-check sample maps.
//!
//! Extracted from `IncrementalProfiler` and `AsyncStreamingProfiler` to avoid
//! duplication (see #228).

use std::collections::HashMap;

use crate::analysis::patterns::looks_like_date;
use crate::core::streaming_stats::{StreamingColumnCollection, StreamingStatistics};
use crate::types::{ColumnProfile, ColumnStats, DataType};

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

    let column_stats = match data_type {
        DataType::Integer | DataType::Float => {
            let sample_strs: Vec<String> = stats.sample_values().to_vec();
            crate::stats::numeric::calculate_numeric_stats(&sample_strs)
        }
        DataType::String | DataType::Date => {
            let text_stats = stats.text_length_stats();
            ColumnStats::Text {
                min_length: text_stats.min_length,
                max_length: text_stats.max_length,
                avg_length: text_stats.avg_length,
                most_frequent: None,
                least_frequent: None,
            }
        }
    };

    let patterns = crate::detect_patterns(stats.sample_values());

    ColumnProfile {
        name: name.to_string(),
        data_type,
        null_count: stats.null_count,
        total_count: stats.count,
        unique_count: Some(stats.unique_count()),
        stats: column_stats,
        patterns,
    }
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
/// [`DataQualityMetrics::calculate_from_data`].
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
