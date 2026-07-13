use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use crate::profile_builder::infer_data_type_streaming;
use dataprof_metrics::analysis::inference::is_null_like_token;
use dataprof_metrics::{CardinalityEstimator, HyperLogLog, RowDuplicateSummary};

/// Incremental statistics computation for streaming data processing.
///
/// This module provides bounded-memory statistical computation using:
/// - **Welford's algorithm** for numerically stable variance/stddev (O(1) memory)
/// - **HyperLogLog** for approximate distinct counts (~16 KB fixed registers)
/// - **Reservoir sampling** for unbiased samples (fixed capacity; total memory
///   depends on the capacity and the length of sampled strings)
/// - **Streaming text-length tracking** with min/max/mean/histogram (O(1) memory)

#[derive(Debug, Clone)]
pub struct WelfordAccumulator {
    count: u64,
    mean: f64,
    m2: f64,
}

impl WelfordAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    #[inline]
    pub fn update(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    #[inline]
    pub fn mean(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.mean }
    }

    pub fn variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            self.m2 / self.count as f64
        }
    }

    pub fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }

    pub fn merge(&mut self, other: &WelfordAccumulator) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }

        let combined_count = self.count + other.count;
        let delta = other.mean - self.mean;
        let new_mean = self.mean + delta * (other.count as f64 / combined_count as f64);
        let new_m2 = self.m2
            + other.m2
            + delta * delta * (self.count as f64 * other.count as f64 / combined_count as f64);

        self.count = combined_count;
        self.mean = new_mean;
        self.m2 = new_m2;
    }
}

impl Default for WelfordAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StreamReservoirSampler {
    reservoir: Vec<String>,
    capacity: usize,
    count: u64,
    rng: SmallRng,
}

impl StreamReservoirSampler {
    const DEFAULT_SEED: u64 = 0xDA7A_900D_F00D_5EED;

    pub fn new(capacity: usize) -> Self {
        Self {
            reservoir: Vec::with_capacity(capacity.min(1024)),
            capacity,
            count: 0,
            // Profiling the same ordered source must produce the same report.
            // Callers that need randomized admission have an explicit sampling
            // strategy; this internal bounded-memory sample is deterministic.
            rng: SmallRng::seed_from_u64(Self::DEFAULT_SEED),
        }
    }

    #[cfg(test)]
    pub fn seed(capacity: usize, seed: u64) -> Self {
        Self {
            reservoir: Vec::with_capacity(capacity.min(1024)),
            capacity,
            count: 0,
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    #[inline]
    pub fn offer(&mut self, value: String) {
        self.count += 1;
        if self.reservoir.len() < self.capacity {
            self.reservoir.push(value);
        } else {
            let index = self.rng.random_range(0..self.count as usize);
            if index < self.capacity {
                self.reservoir[index] = value;
            }
        }
    }

    pub fn shrink_to(&mut self, new_capacity: usize) {
        let new_capacity = new_capacity.max(1);
        self.capacity = new_capacity;
        self.reservoir.truncate(new_capacity);
        self.reservoir.shrink_to_fit();
    }

    pub fn samples(&self) -> &[String] {
        &self.reservoir
    }

    pub fn memory_usage_bytes(&self) -> usize {
        self.reservoir
            .iter()
            .map(|value| std::mem::size_of::<String>() + value.capacity())
            .sum()
    }

    pub fn merge(&mut self, other: &StreamReservoirSampler) {
        if other.count == 0 {
            return;
        }

        let mut combined: Vec<String> = self.reservoir.drain(..).collect();
        combined.extend(other.reservoir.iter().cloned());

        let total = combined.len();
        if total <= self.capacity {
            self.reservoir = combined;
        } else {
            for index in 0..self.capacity {
                let swap_with = self.rng.random_range(index..total);
                combined.swap(index, swap_with);
            }
            combined.truncate(self.capacity);
            self.reservoir = combined;
        }

        self.count += other.count;
    }
}

#[derive(Debug, Clone)]
pub struct TextLengthStats {
    pub min_length: usize,
    pub max_length: usize,
    pub avg_length: f64,
    welford: WelfordAccumulator,
    histogram: [u64; 32],
}

impl TextLengthStats {
    pub fn new() -> Self {
        Self {
            min_length: usize::MAX,
            max_length: 0,
            avg_length: 0.0,
            welford: WelfordAccumulator::new(),
            histogram: [0u64; 32],
        }
    }

    pub fn update(&mut self, length: usize) {
        self.min_length = self.min_length.min(length);
        self.max_length = self.max_length.max(length);
        self.welford.update(length as f64);
        self.avg_length = self.welford.mean();

        let bucket = if length == 0 {
            0
        } else {
            (usize::BITS - length.leading_zeros()).min(31) as usize
        };
        self.histogram[bucket] += 1;
    }

    pub fn merge(&mut self, other: &TextLengthStats) {
        if other.welford.count == 0 {
            return;
        }
        if self.welford.count == 0 {
            *self = other.clone();
            return;
        }

        self.min_length = self.min_length.min(other.min_length);
        self.max_length = self.max_length.max(other.max_length);
        self.welford.merge(&other.welford);
        self.avg_length = self.welford.mean();

        for (left, right) in self.histogram.iter_mut().zip(other.histogram.iter()) {
            *left += *right;
        }
    }

    pub fn empty() -> Self {
        Self {
            min_length: 0,
            max_length: 0,
            avg_length: 0.0,
            welford: WelfordAccumulator::new(),
            histogram: [0u64; 32],
        }
    }
}

impl Default for TextLengthStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct StreamingStatistics {
    pub count: usize,
    pub null_count: usize,
    pub min: f64,
    pub max: f64,
    welford: WelfordAccumulator,
    hll: HyperLogLog,
    sampler: StreamReservoirSampler,
    text_length_tracker: TextLengthStats,
}

impl StreamingStatistics {
    pub fn new() -> Self {
        Self {
            count: 0,
            null_count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            welford: WelfordAccumulator::new(),
            hll: HyperLogLog::new(),
            sampler: StreamReservoirSampler::new(10_000),
            text_length_tracker: TextLengthStats::new(),
        }
    }

    pub fn with_sample_capacity(max_sample: usize) -> Self {
        Self {
            sampler: StreamReservoirSampler::new(max_sample),
            ..Self::new()
        }
    }

    pub fn update(&mut self, value: &str) {
        self.count += 1;

        if is_null_like_token(value) {
            self.null_count += 1;
            return;
        }

        self.hll.insert(value);
        self.sampler.offer(value.to_string());
        self.text_length_tracker.update(value.len());

        if let Some(number) = value.parse::<f64>().ok().filter(|num| num.is_finite()) {
            self.welford.update(number);
            self.min = self.min.min(number);
            self.max = self.max.max(number);
        }
    }

    pub fn merge(&mut self, other: &StreamingStatistics) {
        self.count += other.count;
        self.null_count += other.null_count;

        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }

        self.welford.merge(&other.welford);
        self.hll.merge(&other.hll);
        self.sampler.merge(&other.sampler);
        self.text_length_tracker.merge(&other.text_length_tracker);
    }

    pub fn mean(&self) -> f64 {
        self.welford.mean()
    }

    pub fn variance(&self) -> f64 {
        self.welford.variance()
    }

    pub fn std_dev(&self) -> f64 {
        self.welford.std_dev()
    }

    pub fn unique_count(&self) -> usize {
        if !self.unique_count_is_approximate() {
            return self.sampler.samples().iter().collect::<HashSet<_>>().len();
        }
        self.hll.count() as usize
    }

    pub fn unique_count_is_approximate(&self) -> bool {
        (self.sampler.samples().len() as u64) < self.sampler.count
    }

    pub fn sample_values(&self) -> &[String] {
        self.sampler.samples()
    }

    pub fn text_length_stats(&self) -> TextLengthStats {
        if self.text_length_tracker.welford.count == 0 {
            return TextLengthStats::empty();
        }
        self.text_length_tracker.clone()
    }

    pub fn reduce_sample_capacity(&mut self) {
        self.sampler.shrink_to(self.sampler.capacity / 2);
    }

    pub fn memory_usage_bytes(&self) -> usize {
        let struct_size = std::mem::size_of::<Self>();
        let hll_size = self.hll.memory_usage_bytes();
        let reservoir_size = self.sampler.memory_usage_bytes();

        struct_size + hll_size + reservoir_size
    }
}

impl Default for StreamingStatistics {
    fn default() -> Self {
        Self::new()
    }
}

/// Full-stream row-duplicate tracking with bounded memory.
///
/// Every record's fields are folded into a canonical length-prefixed
/// signature and fed to a [`CardinalityEstimator`]: duplicates are counted
/// exactly while the distinct-row signatures fit the estimator's exact set,
/// and estimated (flagged approximate) once it spills to its HLL sketch.
/// Unlike the per-column reservoirs, this sees whole rows — including
/// null-like values — so the count is row-aligned by construction.
#[derive(Debug, Clone, Default)]
pub struct RowUniquenessTracker {
    rows_seen: usize,
    distinct: CardinalityEstimator,
}

impl RowUniquenessTracker {
    pub fn observe(&mut self, signature: String) {
        self.rows_seen += 1;
        self.distinct.insert_owned(signature);
    }

    pub fn rows_seen(&self) -> usize {
        self.rows_seen
    }

    /// Rows minus distinct rows; exact until the estimator spills.
    pub fn duplicate_rows(&self) -> usize {
        self.rows_seen.saturating_sub(self.distinct.estimate())
    }

    pub fn is_approximate(&self) -> bool {
        self.distinct.is_approximate()
    }

    pub fn merge(&mut self, other: &RowUniquenessTracker) {
        self.rows_seen += other.rows_seen;
        self.distinct.merge(&other.distinct);
    }

    pub fn memory_usage_bytes(&self) -> usize {
        self.distinct.memory_usage_bytes()
    }

    /// Summary for quality metrics, or `None` when no rows were observed
    /// (e.g. an engine that never fed whole records through this tracker).
    pub fn summary(&self) -> Option<RowDuplicateSummary> {
        if self.rows_seen == 0 {
            return None;
        }
        Some(RowDuplicateSummary {
            duplicate_rows: self.duplicate_rows(),
            rows_checked: self.rows_seen,
            approximate: self.is_approximate(),
        })
    }
}

pub struct StreamingColumnCollection {
    columns: HashMap<String, StreamingStatistics>,
    ordered_names: Vec<String>,
    memory_limit_bytes: usize,
    row_tracker: RowUniquenessTracker,
}

impl StreamingColumnCollection {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            ordered_names: Vec::new(),
            memory_limit_bytes: 100 * 1024 * 1024,
            row_tracker: RowUniquenessTracker::default(),
        }
    }

    pub fn memory_limit(limit_mb: usize) -> Self {
        Self {
            columns: HashMap::new(),
            ordered_names: Vec::new(),
            memory_limit_bytes: limit_mb * 1024 * 1024,
            row_tracker: RowUniquenessTracker::default(),
        }
    }

    pub fn init_columns(&mut self, headers: &[String]) {
        for header in headers {
            if !self.columns.contains_key(header) {
                self.columns
                    .insert(header.clone(), StreamingStatistics::default());
                self.ordered_names.push(header.clone());
            }
        }
    }

    /// Add a column discovered after `prior_rows` records have already passed.
    ///
    /// JSON objects may introduce keys at any point in a stream. Those earlier
    /// objects are missing the new key, so the column must start with matching
    /// total/null counters rather than looking shorter and more complete than
    /// the dataset.
    pub fn init_column_with_missing(&mut self, header: &str, prior_rows: usize) {
        if self.columns.contains_key(header) {
            return;
        }

        let stats = StreamingStatistics {
            count: prior_rows,
            null_count: prior_rows,
            ..Default::default()
        };
        self.columns.insert(header.to_string(), stats);
        self.ordered_names.push(header.to_string());
    }

    pub fn process_record<I>(&mut self, headers: &[String], values: I)
    where
        I: IntoIterator<Item = String>,
    {
        // Length-prefixed so field boundaries are unambiguous:
        // ["ab", "c"] and ["a", "bc"] must produce different signatures.
        let mut row_signature = String::new();
        let mut values = values.into_iter();

        // Headers define the row schema. Normalize a missing trailing field to
        // the profiler's empty/null representation so ragged flexible records
        // update every column and hash identically to an explicit empty field.
        for header in headers {
            let value = values.next().unwrap_or_default();
            let _ = write!(row_signature, "{}:", value.len());
            row_signature.push_str(&value);

            if !self.columns.contains_key(header) {
                self.ordered_names.push(header.clone());
            }
            let stats = self.columns.entry(header.to_string()).or_default();
            stats.update(&value);
        }

        if !headers.is_empty() {
            self.row_tracker.observe(row_signature);
        }
    }

    /// Full-stream row-duplicate counts, or `None` when no rows were seen.
    pub fn row_duplicate_summary(&self) -> Option<RowDuplicateSummary> {
        self.row_tracker.summary()
    }

    pub fn get_column_stats(&self, column_name: &str) -> Option<&StreamingStatistics> {
        self.columns.get(column_name)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.ordered_names.clone()
    }

    pub fn memory_usage_bytes(&self) -> usize {
        self.columns
            .values()
            .map(|stats| stats.memory_usage_bytes())
            .sum::<usize>()
            + self.row_tracker.memory_usage_bytes()
    }

    pub fn is_memory_pressure(&self) -> bool {
        self.memory_usage_bytes() > (self.memory_limit_bytes * 80 / 100)
    }

    pub fn reduce_memory_usage(&mut self) {
        for stats in self.columns.values_mut() {
            stats.reduce_sample_capacity();
        }
    }

    /// Fingerprint of each column's currently inferred data type.
    ///
    /// Returns a `u64` hash suitable for cheap comparison in a schema
    /// stability tracker.
    pub fn column_type_fingerprint(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        let mut names: Vec<&String> = self.columns.keys().collect();
        names.sort();
        for name in names {
            let stats = &self.columns[name];
            let data_type = infer_data_type_streaming(stats);
            name.hash(&mut hasher);
            std::mem::discriminant(&data_type).hash(&mut hasher);
        }
        hasher.finish()
    }

    pub fn merge(&mut self, other: StreamingColumnCollection) {
        for (column_name, other_stats) in other.columns {
            match self.columns.get_mut(&column_name) {
                Some(existing_stats) => existing_stats.merge(&other_stats),
                None => {
                    self.columns.insert(column_name, other_stats);
                }
            }
        }
        self.row_tracker.merge(&other.row_tracker);
    }
}

impl Default for StreamingColumnCollection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod row_tracker_tests {
    use super::*;

    fn record(collection: &mut StreamingColumnCollection, headers: &[String], values: &[&str]) {
        collection.process_record(headers, values.iter().map(|v| v.to_string()));
    }

    #[test]
    fn test_exact_duplicates_including_null_rows() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut collection = StreamingColumnCollection::new();
        record(&mut collection, &headers, &["x", ""]);
        record(&mut collection, &headers, &["x", ""]);
        record(&mut collection, &headers, &["x", "1"]);
        record(&mut collection, &headers, &["", ""]);

        let summary = collection
            .row_duplicate_summary()
            .expect("rows were observed");
        assert_eq!(summary.rows_checked, 4);
        // Null-like values are part of the row identity: the per-column
        // reservoirs drop them, but the row tracker must not.
        assert_eq!(summary.duplicate_rows, 1);
        assert!(!summary.approximate);
    }

    #[test]
    fn test_field_boundaries_are_unambiguous() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut collection = StreamingColumnCollection::new();
        record(&mut collection, &headers, &["ab", "c"]);
        record(&mut collection, &headers, &["a", "bc"]);

        let summary = collection
            .row_duplicate_summary()
            .expect("rows were observed");
        assert_eq!(
            summary.duplicate_rows, 0,
            "different field splits must not collide"
        );
    }

    #[test]
    fn test_ragged_rows_normalize_missing_trailing_fields() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut collection = StreamingColumnCollection::new();
        record(&mut collection, &headers, &["x"]);
        record(&mut collection, &headers, &["x", ""]);

        let summary = collection
            .row_duplicate_summary()
            .expect("rows were observed");
        assert_eq!(summary.rows_checked, 2);
        assert_eq!(summary.duplicate_rows, 1);
        assert_eq!(
            collection
                .get_column_stats("b")
                .expect("column b")
                .null_count,
            2
        );
    }

    #[test]
    fn test_no_rows_means_no_summary() {
        let collection = StreamingColumnCollection::new();
        assert!(collection.row_duplicate_summary().is_none());
    }

    #[test]
    fn test_spills_to_approximate_beyond_distinct_threshold() {
        let headers = vec!["n".to_string()];
        let mut collection = StreamingColumnCollection::new();
        let distinct = dataprof_metrics::EXACT_CARDINALITY_THRESHOLD + 500;
        for i in 0..distinct {
            record(&mut collection, &headers, &[&i.to_string()]);
        }
        // Every row twice: duplicates == distinct.
        for i in 0..distinct {
            record(&mut collection, &headers, &[&i.to_string()]);
        }

        let summary = collection
            .row_duplicate_summary()
            .expect("rows were observed");
        assert!(summary.approximate, "past the threshold the count is HLL");
        assert_eq!(summary.rows_checked, distinct * 2);
        let error = (summary.duplicate_rows as f64 - distinct as f64).abs() / distinct as f64;
        assert!(
            error < 0.05,
            "estimated {} duplicates for {distinct} true, off by {error:.4}",
            summary.duplicate_rows
        );
    }

    #[test]
    fn test_merge_combines_row_trackers() {
        let headers = vec!["a".to_string()];
        let mut left = StreamingColumnCollection::new();
        let mut right = StreamingColumnCollection::new();
        record(&mut left, &headers, &["x"]);
        record(&mut left, &headers, &["y"]);
        record(&mut right, &headers, &["x"]);

        left.merge(right);
        let summary = left.row_duplicate_summary().expect("rows were observed");
        assert_eq!(summary.rows_checked, 3);
        assert_eq!(summary.duplicate_rows, 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_statistics() {
        let mut stats = StreamingStatistics::new();

        stats.update("10.5");
        stats.update("20.0");
        stats.update("15.5");
        stats.update("");

        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.unique_count(), 3);
        assert!(!stats.unique_count_is_approximate());
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
        assert!(!stats1.unique_count_is_approximate());
        assert!((stats1.mean() - 25.0).abs() < 1e-10);
        assert_eq!(stats1.min, 10.0);
        assert_eq!(stats1.max, 40.0);
    }

    #[test]
    fn test_column_collection() {
        let mut collection = StreamingColumnCollection::new();
        let headers = vec!["name".to_string(), "age".to_string()];

        collection.process_record(&headers, vec!["Alice".to_string(), "25".to_string()]);
        collection.process_record(&headers, vec!["Bob".to_string(), "30".to_string()]);

        let age_stats = collection.get_column_stats("age").unwrap();
        assert_eq!(age_stats.count, 2);
        assert!((age_stats.mean() - 27.5).abs() < 1e-10);
    }

    #[test]
    fn test_unique_count_becomes_approximate_only_after_reservoir_truncation() {
        let mut stats = StreamingStatistics::with_sample_capacity(2);
        stats.update("a");
        stats.update("b");
        assert_eq!(stats.unique_count(), 2);
        assert!(!stats.unique_count_is_approximate());

        stats.update("c");
        assert!(stats.unique_count_is_approximate());
    }

    #[test]
    fn test_default_reservoir_sampling_is_deterministic() {
        let mut left = StreamReservoirSampler::new(10);
        let mut right = StreamReservoirSampler::new(10);
        for value in 0..1_000 {
            left.offer(value.to_string());
            right.offer(value.to_string());
        }

        assert_eq!(left.samples(), right.samples());
    }

    #[test]
    fn test_late_column_is_backfilled_as_missing() {
        let mut collection = StreamingColumnCollection::new();
        collection.init_column_with_missing("late", 3);
        collection.process_record(&["late".to_string()], ["value".to_string()]);

        let stats = collection.get_column_stats("late").unwrap();
        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.unique_count(), 1);
    }

    #[test]
    fn test_welford_accuracy() {
        let mut accumulator = WelfordAccumulator::new();
        for value in 1..=1000 {
            accumulator.update(value as f64);
        }
        let expected_mean = 500.5;
        let expected_variance = (1000.0 * 1000.0 - 1.0) / 12.0;
        assert!((accumulator.mean() - expected_mean).abs() < 1e-6);
        assert!((accumulator.variance() - expected_variance).abs() < 1.0);
    }

    #[test]
    fn test_welford_merge() {
        let mut left = WelfordAccumulator::new();
        let mut right = WelfordAccumulator::new();
        let mut full = WelfordAccumulator::new();

        for value in 1..=500 {
            left.update(value as f64);
            full.update(value as f64);
        }
        for value in 501..=1000 {
            right.update(value as f64);
            full.update(value as f64);
        }

        left.merge(&right);
        assert!((left.mean() - full.mean()).abs() < 1e-10);
        assert!((left.variance() - full.variance()).abs() < 1e-6);
    }

    #[test]
    fn test_hll_cardinality() {
        let mut counter = HyperLogLog::new();
        let total = 100_000;
        for index in 0..total {
            counter.insert(&format!("item_{index}"));
        }
        let estimate = counter.count();
        let error = (estimate as f64 - total as f64).abs() / total as f64;
        assert!(error < 0.05);
    }

    #[test]
    fn test_reservoir_uniformity() {
        let mut sampler = StreamReservoirSampler::seed(1000, 42);
        let total = 100_000;
        for index in 0..total {
            sampler.offer(index.to_string());
        }

        assert_eq!(sampler.samples().len(), 1000);
        let values: Vec<usize> = sampler
            .samples()
            .iter()
            .map(|value| value.parse().unwrap())
            .collect();
        let max_value = *values.iter().max().unwrap();
        assert!(max_value > total / 2);
    }

    #[test]
    fn test_text_length_stats_streaming() {
        let mut stats = TextLengthStats::new();
        for &length in &[3, 5, 10, 1, 7] {
            stats.update(length);
        }
        assert_eq!(stats.min_length, 1);
        assert_eq!(stats.max_length, 10);
        assert!((stats.avg_length - 5.2).abs() < 1e-10);
    }

    #[test]
    fn test_memory_usage_bounded() {
        let mut stats = StreamingStatistics::new();
        for index in 0..50_000 {
            stats.update(&format!("value_{index}"));
        }
        let usage = stats.memory_usage_bytes();
        assert!(usage < 1_000_000);
    }
}
