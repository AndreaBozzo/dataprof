/// Incremental statistics computation for streaming data processing
/// This module provides memory-efficient statistical computation that doesn't require
/// keeping all data in memory
use std::collections::{HashMap, HashSet};

/// Streaming statistical aggregator that computes statistics incrementally
#[derive(Debug, Clone)]
pub struct StreamingStatistics {
    /// Count of processed values
    pub count: usize,
    /// Count of null/empty values
    pub null_count: usize,
    /// Sum for mean calculation
    pub sum: f64,
    /// Sum of squares for variance calculation
    pub sum_squares: f64,
    /// Minimum value seen
    pub min: f64,
    /// Maximum value seen
    pub max: f64,
    /// Unique value counter (with memory limit)
    unique_values: HashSet<String>,
    /// Sample values for pattern detection
    sample_values: Vec<String>,
    /// Text length statistics
    text_lengths: Vec<usize>,
    /// Memory limit for unique values tracking
    max_unique_values: usize,
    /// Max sample size for pattern detection
    max_sample_size: usize,
}

impl StreamingStatistics {
    pub fn new() -> Self {
        Self {
            count: 0,
            null_count: 0,
            sum: 0.0,
            sum_squares: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            unique_values: HashSet::new(),
            sample_values: Vec::new(),
            text_lengths: Vec::new(),
            max_unique_values: 10_000,
            max_sample_size: 1_000,
        }
    }

    pub fn with_limits(max_unique: usize, max_sample: usize) -> Self {
        Self {
            max_unique_values: max_unique,
            max_sample_size: max_sample,
            ..Self::new()
        }
    }

    /// Process a single value incrementally
    pub fn update(&mut self, value: &str) {
        self.count += 1;

        if value.is_empty() {
            self.null_count += 1;
            return;
        }

        // Track unique values (with memory limit)
        if self.unique_values.len() < self.max_unique_values {
            self.unique_values.insert(value.to_string());
        }

        // Keep sample for pattern detection
        if self.sample_values.len() < self.max_sample_size {
            self.sample_values.push(value.to_string());
        }

        // Track text length
        self.text_lengths.push(value.len());

        // Try to update numeric statistics
        if let Ok(num_val) = value.parse::<f64>() {
            self.sum += num_val;
            self.sum_squares += num_val * num_val;
            self.min = self.min.min(num_val);
            self.max = self.max.max(num_val);
        }
    }

    /// Merge statistics from another streaming aggregator
    pub fn merge(&mut self, other: &StreamingStatistics) {
        self.count += other.count;
        self.null_count += other.null_count;
        self.sum += other.sum;
        self.sum_squares += other.sum_squares;

        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        // Merge unique values (with limit)
        for value in &other.unique_values {
            if self.unique_values.len() >= self.max_unique_values {
                break;
            }
            self.unique_values.insert(value.to_string());
        }

        // Merge sample values (with limit)
        for value in &other.sample_values {
            if self.sample_values.len() >= self.max_sample_size {
                break;
            }
            self.sample_values.push(value.to_string());
        }

        // Merge text lengths
        self.text_lengths.extend(&other.text_lengths);
    }

    /// Calculate mean
    pub fn mean(&self) -> f64 {
        if self.count > self.null_count {
            self.sum / (self.count - self.null_count) as f64
        } else {
            0.0
        }
    }

    /// Calculate variance (population)
    pub fn variance(&self) -> f64 {
        let n = (self.count - self.null_count) as f64;
        if n <= 1.0 {
            return 0.0;
        }

        let mean = self.mean();
        (self.sum_squares - n * mean * mean) / n
    }

    /// Calculate standard deviation
    pub fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }

    /// Get unique value count (approximation if limit exceeded)
    pub fn unique_count(&self) -> usize {
        self.unique_values.len()
    }

    /// Check if unique count hit the limit
    pub fn unique_count_is_approximate(&self) -> bool {
        self.unique_values.len() >= self.max_unique_values
    }

    /// Get sample values for pattern detection
    pub fn sample_values(&self) -> &[String] {
        &self.sample_values
    }

    /// Calculate text length statistics
    pub fn text_length_stats(&self) -> TextLengthStats {
        if self.text_lengths.is_empty() {
            return TextLengthStats {
                min_length: 0,
                max_length: 0,
                avg_length: 0.0,
            };
        }

        let min_length = self.text_lengths.iter().min().copied().unwrap_or(0);
        let max_length = self.text_lengths.iter().max().copied().unwrap_or(0);
        let avg_length = if self.text_lengths.is_empty() {
            0.0
        } else {
            self.text_lengths.iter().sum::<usize>() as f64 / self.text_lengths.len() as f64
        };

        TextLengthStats {
            min_length,
            max_length,
            avg_length,
        }
    }

    /// Get memory usage estimate in bytes
    pub fn memory_usage_bytes(&self) -> usize {
        let unique_values_size: usize = self.unique_values.iter().map(|s| s.len()).sum();
        let sample_values_size: usize = self.sample_values.iter().map(|s| s.len()).sum();
        let text_lengths_size = self.text_lengths.len() * std::mem::size_of::<usize>();

        std::mem::size_of::<Self>() + unique_values_size + sample_values_size + text_lengths_size
    }
}

impl Default for StreamingStatistics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct TextLengthStats {
    pub min_length: usize,
    pub max_length: usize,
    pub avg_length: f64,
}

/// Collection of streaming statistics for all columns
pub struct StreamingColumnCollection {
    columns: HashMap<String, StreamingStatistics>,
    memory_limit_bytes: usize,
}

impl StreamingColumnCollection {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            memory_limit_bytes: 100 * 1024 * 1024, // 100MB default limit
        }
    }

    pub fn with_memory_limit(limit_mb: usize) -> Self {
        Self {
            columns: HashMap::new(),
            memory_limit_bytes: limit_mb * 1024 * 1024,
        }
    }

    /// Process a record (row) of data
    pub fn process_record<I>(&mut self, headers: &[String], values: I)
    where
        I: IntoIterator<Item = String>,
    {
        for (header, value) in headers.iter().zip(values) {
            let stats = self.columns.entry(header.to_string()).or_default();
            stats.update(&value);
        }
    }

    /// Get statistics for a specific column
    pub fn get_column_stats(&self, column_name: &str) -> Option<&StreamingStatistics> {
        self.columns.get(column_name)
    }

    /// Get all column names
    pub fn column_names(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }

    /// Get total memory usage
    pub fn memory_usage_bytes(&self) -> usize {
        self.columns.values().map(|s| s.memory_usage_bytes()).sum()
    }

    /// Check if memory usage is approaching the limit
    pub fn is_memory_pressure(&self) -> bool {
        self.memory_usage_bytes() > (self.memory_limit_bytes * 80 / 100) // 80% of limit
    }

    /// Reduce memory usage by limiting sample sizes
    pub fn reduce_memory_usage(&mut self) {
        for stats in self.columns.values_mut() {
            // Reduce sample sizes to free memory
            stats.sample_values.truncate(stats.max_sample_size / 2);
            if stats.unique_values.len() > stats.max_unique_values / 2 {
                let to_keep: Vec<_> = stats
                    .unique_values
                    .iter()
                    .take(stats.max_unique_values / 2)
                    .cloned()
                    .collect();
                stats.unique_values = to_keep.into_iter().collect();
            }
        }
    }

    /// Merge another collection into this one
    pub fn merge(&mut self, other: StreamingColumnCollection) {
        for (column_name, other_stats) in other.columns {
            match self.columns.get_mut(&column_name) {
                Some(existing_stats) => {
                    existing_stats.merge(&other_stats);
                }
                None => {
                    self.columns.insert(column_name, other_stats);
                }
            }
        }
    }
}

impl Default for StreamingColumnCollection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_statistics() {
        let mut stats = StreamingStatistics::new();

        // Add some numeric values
        stats.update("10.5");
        stats.update("20.0");
        stats.update("15.5");
        stats.update(""); // null value

        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.unique_count(), 3);
        assert!((stats.mean() - 15.333333333333334).abs() < 1e-10);
        assert_eq!(stats.min, 10.5);
        assert_eq!(stats.max, 20.0);
    }

    #[test]
    fn test_streaming_statistics_merge() {
        let mut stats1 = StreamingStatistics::new();
        stats1.update("10");
        stats1.update("20");

        let mut stats2 = StreamingStatistics::new();
        stats2.update("30");
        stats2.update("40");

        stats1.merge(&stats2);

        assert_eq!(stats1.count, 4);
        assert_eq!(stats1.unique_count(), 4);
        assert_eq!(stats1.mean(), 25.0);
        assert_eq!(stats1.min, 10.0);
        assert_eq!(stats1.max, 40.0);
    }

    #[test]
    fn test_column_collection() {
        let mut collection = StreamingColumnCollection::new();
        let headers = vec!["name".to_string(), "age".to_string()];

        collection.process_record(&headers, vec!["Alice".to_string(), "25".to_string()]);
        collection.process_record(&headers, vec!["Bob".to_string(), "30".to_string()]);

        let age_stats = collection
            .get_column_stats("age")
            .expect("Age column should exist in test");
        assert_eq!(age_stats.count, 2);
        assert_eq!(age_stats.mean(), 27.5);
    }
}
