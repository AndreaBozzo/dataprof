use crate::types::{ColumnStats, Quartiles};
use std::collections::HashMap;

const SAMPLE_THRESHOLD: usize = 10_000;

pub fn calculate_numeric_stats(data: &[String]) -> ColumnStats {
    let numbers: Vec<f64> = data
        .iter()
        .filter_map(|s| s.parse::<f64>().ok())
        .filter(|x| x.is_finite())
        .collect();

    if numbers.is_empty() {
        return ColumnStats::Numeric {
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            std_dev: 0.0,
            variance: 0.0,
            median: None,
            quartiles: None,
            mode: None,
            coefficient_of_variation: None,
            skewness: None,
            kurtosis: None,
            is_approximate: None,
        };
    }

    // Always calculable statistics
    let min = numbers.iter().copied().fold(f64::INFINITY, f64::min);
    let max = numbers.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let sum: f64 = numbers.iter().sum();
    let mean = sum / numbers.len() as f64;

    // Variance and std_dev (streaming-compatible)
    let sum_squares: f64 = numbers.iter().map(|&x| x * x).sum();
    let variance = calculate_variance(sum_squares, sum, numbers.len());
    let std_dev = variance.sqrt();

    // Determine if we need sampling for large datasets
    let (sample_data, is_approximate) = if numbers.len() > SAMPLE_THRESHOLD {
        (reservoir_sample(&numbers, SAMPLE_THRESHOLD), Some(true))
    } else {
        (numbers.clone(), None)
    };

    // Statistics requiring sorted/complete data
    let mut sorted_data = sample_data.clone();
    sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let median = calculate_median(&sorted_data);
    let quartiles = calculate_quartiles(&sorted_data);
    let mode = calculate_mode(&sample_data);
    let coefficient_of_variation = calculate_coefficient_of_variation(std_dev, mean);

    // Advanced metrics
    let skewness = calculate_skewness(&sample_data, mean, std_dev);
    let kurtosis = calculate_kurtosis(&sample_data, mean, std_dev);

    ColumnStats::Numeric {
        min,
        max,
        mean,
        std_dev,
        variance,
        median,
        quartiles,
        mode,
        coefficient_of_variation,
        skewness,
        kurtosis,
        is_approximate,
    }
}

/// Calculate variance using sum of squares
/// Uses sample variance (n-1) for unbiased estimation
pub fn calculate_variance(sum_squares: f64, sum: f64, count: usize) -> f64 {
    let n = count as f64;
    if n <= 1.0 {
        return 0.0;
    }

    let mean = sum / n;
    (sum_squares - n * mean * mean) / (n - 1.0)
}

/// Calculate median from sorted data
pub fn calculate_median(sorted_data: &[f64]) -> Option<f64> {
    if sorted_data.is_empty() {
        return None;
    }

    let len = sorted_data.len();
    #[allow(clippy::manual_is_multiple_of)]
    if len % 2 == 0 {
        // Even number of elements - average of two middle values
        let mid1 = sorted_data[len / 2 - 1];
        let mid2 = sorted_data[len / 2];
        Some((mid1 + mid2) / 2.0)
    } else {
        // Odd number of elements - middle value
        Some(sorted_data[len / 2])
    }
}

/// Calculate quartiles using Type 7 interpolation (R/Excel default)
pub fn calculate_quartiles(sorted_data: &[f64]) -> Option<Quartiles> {
    if sorted_data.len() < 4 {
        return None;
    }

    let n = sorted_data.len() as f64;

    let q1 = calculate_percentile(sorted_data, 0.25, n);
    let q2 = calculate_percentile(sorted_data, 0.50, n);
    let q3 = calculate_percentile(sorted_data, 0.75, n);

    let iqr = q3 - q1;

    Some(Quartiles { q1, q2, q3, iqr })
}

/// Calculate percentile using linear interpolation (Excel/R method)
fn calculate_percentile(sorted_data: &[f64], p: f64, n: f64) -> f64 {
    // Use position formula: p * (n + 1) - 1 for 0-indexed arrays
    let pos = p * (n + 1.0) - 1.0;

    if pos < 0.0 {
        return sorted_data[0];
    }
    if pos >= n - 1.0 {
        return sorted_data[(n as usize) - 1];
    }

    let lower_idx = pos.floor() as usize;
    let upper_idx = pos.ceil() as usize;

    if lower_idx == upper_idx {
        sorted_data[lower_idx]
    } else {
        let weight = pos - lower_idx as f64;
        sorted_data[lower_idx] * (1.0 - weight) + sorted_data[upper_idx] * weight
    }
}

/// Calculate mode (most frequent value)
/// For multimodal distributions, returns the smallest value with maximum frequency
pub fn calculate_mode(data: &[f64]) -> Option<f64> {
    if data.is_empty() {
        return None;
    }

    let mut freq_map: HashMap<String, usize> = HashMap::new();

    // Use string representation to handle floating point comparison
    for &value in data {
        let key = format!("{:.10}", value); // 10 decimal precision
        *freq_map.entry(key).or_insert(0) += 1;
    }

    // Find maximum frequency
    let max_freq = *freq_map.values().max()?;

    // If all values are unique, return None
    if max_freq == 1 {
        return None;
    }

    // Find all values with maximum frequency and return the smallest (deterministic)
    let mut modes: Vec<f64> = freq_map
        .iter()
        .filter(|(_, &count)| count == max_freq)
        .filter_map(|(value, _)| value.parse::<f64>().ok())
        .collect();
    modes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    modes.first().copied()
}

/// Calculate coefficient of variation (CV)
pub fn calculate_coefficient_of_variation(std_dev: f64, mean: f64) -> Option<f64> {
    if mean.abs() < 0.001 {
        // Avoid division by zero
        None
    } else {
        Some((std_dev / mean.abs()) * 100.0)
    }
}

/// Calculate skewness (Pearson's moment coefficient)
pub fn calculate_skewness(data: &[f64], mean: f64, std_dev: f64) -> Option<f64> {
    if data.len() < 3 || std_dev < 1e-10 {
        return None;
    }

    let n = data.len() as f64;
    let sum_cubed: f64 = data
        .iter()
        .map(|&x| {
            let z = (x - mean) / std_dev;
            z * z * z
        })
        .sum();

    Some(sum_cubed / n)
}

/// Calculate kurtosis (excess kurtosis)
pub fn calculate_kurtosis(data: &[f64], mean: f64, std_dev: f64) -> Option<f64> {
    if data.len() < 4 || std_dev < 1e-10 {
        return None;
    }

    let n = data.len() as f64;
    let sum_fourth: f64 = data
        .iter()
        .map(|&x| {
            let z = (x - mean) / std_dev;
            let z2 = z * z;
            z2 * z2
        })
        .sum();

    // Excess kurtosis (subtract 3 for normal distribution)
    Some((sum_fourth / n) - 3.0)
}

/// Reservoir sampling for large datasets
fn reservoir_sample(data: &[f64], k: usize) -> Vec<f64> {
    use rand::Rng;

    if data.len() <= k {
        return data.to_vec();
    }

    let mut rng = rand::rng();
    let mut reservoir: Vec<f64> = data[0..k].to_vec();

    for (i, &value) in data.iter().enumerate().skip(k) {
        let j = rng.random_range(0..=i);
        if j < k {
            reservoir[j] = value;
        }
    }

    reservoir
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_median_odd_count() {
        let sorted = vec![1.0, 2.0, 3.0];
        assert_eq!(calculate_median(&sorted), Some(2.0));
    }

    #[test]
    fn test_median_even_count() {
        let sorted = vec![1.0, 2.0, 3.0, 4.0];
        assert_eq!(calculate_median(&sorted), Some(2.5));
    }

    #[test]
    fn test_median_empty() {
        let sorted: Vec<f64> = vec![];
        assert_eq!(calculate_median(&sorted), None);
    }

    #[test]
    fn test_quartiles() {
        let sorted = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let q = calculate_quartiles(&sorted).unwrap();
        assert!((q.q1 - 1.5).abs() < 0.01);
        assert_eq!(q.q2, 3.0);
        assert!((q.q3 - 4.5).abs() < 0.01);
        assert!((q.iqr - 3.0).abs() < 0.01);
    }

    #[test]
    fn test_quartiles_small_dataset() {
        let sorted = vec![1.0, 2.0];
        assert_eq!(calculate_quartiles(&sorted), None);
    }

    #[test]
    fn test_mode_unique_values() {
        let data = vec![1.0, 2.0, 3.0];
        assert_eq!(calculate_mode(&data), None);
    }

    #[test]
    fn test_mode_with_duplicates() {
        let data = vec![1.0, 2.0, 2.0, 3.0];
        assert_eq!(calculate_mode(&data), Some(2.0));
    }

    #[test]
    fn test_mode_empty() {
        let data: Vec<f64> = vec![];
        assert_eq!(calculate_mode(&data), None);
    }

    #[test]
    fn test_skewness_symmetric() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mean = 3.0;
        let std_dev = (2.0_f64).sqrt();
        let skew = calculate_skewness(&data, mean, std_dev).unwrap();
        assert!(skew.abs() < 0.1); // Symmetric distribution
    }

    #[test]
    fn test_coefficient_of_variation() {
        let cv = calculate_coefficient_of_variation(10.0, 100.0);
        assert_eq!(cv, Some(10.0));
    }

    #[test]
    fn test_cv_zero_mean() {
        let cv = calculate_coefficient_of_variation(1.0, 0.0);
        assert_eq!(cv, None);
    }

    #[test]
    fn test_kurtosis() {
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let mean = 3.0;
        let std_dev = (2.0_f64).sqrt();
        let kurt = calculate_kurtosis(&data, mean, std_dev);
        assert!(kurt.is_some());
    }

    #[test]
    fn test_variance() {
        let sum_squares = 55.0; // 1² + 2² + 3² + 4² + 5²
        let sum = 15.0; // 1 + 2 + 3 + 4 + 5
        let count = 5;
        let var = calculate_variance(sum_squares, sum, count);
        // Sample variance (n-1): 2.5
        assert!((var - 2.5).abs() < 0.01);
    }

    #[test]
    fn test_calculate_numeric_stats_basic() {
        let data = vec!["10".to_string(), "20".to_string(), "30".to_string()];
        let stats = calculate_numeric_stats(&data);

        match stats {
            ColumnStats::Numeric {
                min,
                max,
                mean,
                std_dev,
                median,
                ..
            } => {
                assert_eq!(min, 10.0);
                assert_eq!(max, 30.0);
                assert_eq!(mean, 20.0);
                assert!(std_dev > 0.0);
                assert_eq!(median, Some(20.0));
            }
            _ => panic!("Expected Numeric stats"),
        }
    }

    #[test]
    fn test_calculate_numeric_stats_empty() {
        let data: Vec<String> = vec![];
        let stats = calculate_numeric_stats(&data);

        match stats {
            ColumnStats::Numeric {
                min,
                max,
                mean,
                median,
                ..
            } => {
                assert_eq!(min, 0.0);
                assert_eq!(max, 0.0);
                assert_eq!(mean, 0.0);
                assert_eq!(median, None);
            }
            _ => panic!("Expected Numeric stats"),
        }
    }

    #[test]
    fn test_numeric_stats_filters_nan() {
        let data = vec!["1.0".to_string(), "NaN".to_string(), "3.0".to_string()];
        let stats = calculate_numeric_stats(&data);

        match stats {
            ColumnStats::Numeric { min, max, mean, .. } => {
                assert_eq!(min, 1.0);
                assert_eq!(max, 3.0);
                assert_eq!(mean, 2.0);
            }
            _ => panic!("Expected Numeric stats"),
        }
    }

    #[test]
    fn test_numeric_stats_filters_infinity() {
        let data = vec![
            "1.0".to_string(),
            "inf".to_string(),
            "3.0".to_string(),
            "-inf".to_string(),
        ];
        let stats = calculate_numeric_stats(&data);

        match stats {
            ColumnStats::Numeric { min, max, mean, .. } => {
                assert_eq!(min, 1.0);
                assert_eq!(max, 3.0);
                assert_eq!(mean, 2.0);
            }
            _ => panic!("Expected Numeric stats"),
        }
    }

    #[test]
    fn test_numeric_stats_all_nan() {
        let data = vec!["NaN".to_string(), "inf".to_string(), "-inf".to_string()];
        let stats = calculate_numeric_stats(&data);

        match stats {
            ColumnStats::Numeric {
                min,
                max,
                mean,
                median,
                ..
            } => {
                // Should return empty stats when all values are non-finite
                assert_eq!(min, 0.0);
                assert_eq!(max, 0.0);
                assert_eq!(mean, 0.0);
                assert_eq!(median, None);
            }
            _ => panic!("Expected Numeric stats"),
        }
    }
}
