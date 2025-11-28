use crate::types::{ColumnStats, FrequencyItem};
use std::collections::HashMap;

const TOP_N_DEFAULT: usize = 5;
const BOTTOM_N_DEFAULT: usize = 5;

pub fn calculate_text_stats(data: &[String]) -> ColumnStats {
    let non_empty: Vec<&String> = data.iter().filter(|s| !s.trim().is_empty()).collect();

    if non_empty.is_empty() {
        return ColumnStats::Text {
            min_length: 0,
            max_length: 0,
            avg_length: 0.0,
            most_frequent: None,
            least_frequent: None,
        };
    }

    // Existing length calculations
    let lengths: Vec<usize> = non_empty.iter().map(|s| s.len()).collect();
    let min_length = lengths.iter().min().copied().unwrap_or(0);
    let max_length = lengths.iter().max().copied().unwrap_or(0);
    let avg_length = if lengths.is_empty() {
        0.0
    } else {
        lengths.iter().sum::<usize>() as f64 / lengths.len() as f64
    };

    // NEW: Frequency analysis
    let most_frequent = if !non_empty.is_empty() {
        calculate_most_frequent(&non_empty, TOP_N_DEFAULT)
    } else {
        None
    };

    let least_frequent = if non_empty.len() > BOTTOM_N_DEFAULT {
        calculate_least_frequent(&non_empty, BOTTOM_N_DEFAULT)
    } else {
        None
    };

    ColumnStats::Text {
        min_length,
        max_length,
        avg_length,
        most_frequent,
        least_frequent,
    }
}

/// Calculate most frequent strings
pub fn calculate_most_frequent(data: &[&String], top_n: usize) -> Option<Vec<FrequencyItem>> {
    if data.is_empty() {
        return None;
    }

    let freq_map = build_frequency_map(data);
    let total = data.len();

    let mut items = frequency_map_to_items(freq_map, total);

    // Sort by count descending
    items.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.value.cmp(&b.value)));

    // Take top N
    items.truncate(top_n);

    if items.is_empty() {
        None
    } else {
        Some(items)
    }
}

/// Calculate least frequent strings
pub fn calculate_least_frequent(data: &[&String], bottom_n: usize) -> Option<Vec<FrequencyItem>> {
    if data.is_empty() {
        return None;
    }

    let freq_map = build_frequency_map(data);
    let total = data.len();

    let mut items = frequency_map_to_items(freq_map, total);

    // Sort by count ascending (least frequent first)
    items.sort_by(|a, b| a.count.cmp(&b.count).then_with(|| a.value.cmp(&b.value)));

    // Take bottom N
    items.truncate(bottom_n);

    if items.is_empty() {
        None
    } else {
        Some(items)
    }
}

/// Build frequency map from data
fn build_frequency_map(data: &[&String]) -> HashMap<String, usize> {
    let mut freq_map = HashMap::new();
    for s in data {
        *freq_map.entry(s.to_string()).or_insert(0) += 1;
    }
    freq_map
}

/// Convert frequency map to FrequencyItem vector
fn frequency_map_to_items(map: HashMap<String, usize>, total: usize) -> Vec<FrequencyItem> {
    map.into_iter()
        .map(|(value, count)| FrequencyItem {
            value,
            count,
            percentage: (count as f64 / total as f64) * 100.0,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_most_frequent() {
        let data = [
            "apple".to_string(),
            "banana".to_string(),
            "apple".to_string(),
            "cherry".to_string(),
            "apple".to_string(),
        ];
        let refs: Vec<&String> = data.iter().collect();
        let freq = calculate_most_frequent(&refs, 2).unwrap();

        assert_eq!(freq[0].value, "apple");
        assert_eq!(freq[0].count, 3);
        assert_eq!(freq[0].percentage, 60.0);
        assert_eq!(freq.len(), 2);
    }

    #[test]
    fn test_least_frequent() {
        let data = [
            "apple".to_string(),
            "banana".to_string(),
            "apple".to_string(),
            "cherry".to_string(),
            "apple".to_string(),
        ];
        let refs: Vec<&String> = data.iter().collect();
        let freq = calculate_least_frequent(&refs, 2).unwrap();

        // Should have banana and cherry (both count 1)
        assert_eq!(freq.len(), 2);
        assert_eq!(freq[0].count, 1);
        assert_eq!(freq[1].count, 1);
    }

    #[test]
    fn test_frequency_ignores_empty() {
        let data = vec!["value".to_string(), "".to_string(), "  ".to_string()];
        let stats = calculate_text_stats(&data);

        match stats {
            ColumnStats::Text { most_frequent, .. } => {
                let freq = most_frequent.unwrap();
                assert_eq!(freq.len(), 1);
                assert_eq!(freq[0].value, "value");
            }
            _ => panic!("Expected Text stats"),
        }
    }

    #[test]
    fn test_calculate_text_stats_empty() {
        let data: Vec<String> = vec![];
        let stats = calculate_text_stats(&data);

        match stats {
            ColumnStats::Text {
                min_length,
                max_length,
                avg_length,
                most_frequent,
                ..
            } => {
                assert_eq!(min_length, 0);
                assert_eq!(max_length, 0);
                assert_eq!(avg_length, 0.0);
                assert_eq!(most_frequent, None);
            }
            _ => panic!("Expected Text stats"),
        }
    }

    #[test]
    fn test_calculate_text_stats_basic() {
        let data = vec!["hello".to_string(), "world".to_string(), "test".to_string()];
        let stats = calculate_text_stats(&data);

        match stats {
            ColumnStats::Text {
                min_length,
                max_length,
                avg_length,
                most_frequent,
                ..
            } => {
                assert_eq!(min_length, 4); // "test"
                assert_eq!(max_length, 5); // "hello"/"world"
                assert!((avg_length - 4.666666).abs() < 0.01);
                assert!(most_frequent.is_some());
            }
            _ => panic!("Expected Text stats"),
        }
    }
}
