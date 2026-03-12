/// Incremental statistics computation for streaming data processing.
///
/// This module provides truly bounded-memory statistical computation using:
/// - **Welford's algorithm** for numerically stable variance/stddev (O(1) memory)
/// - **HyperLogLog++** for approximate distinct counts (~16 KB fixed)
/// - **Reservoir sampling** for unbiased samples (fixed capacity)
/// - **Streaming text-length tracking** with min/max/mean/histogram (O(1) memory)
use std::collections::HashMap;
use std::hash::{BuildHasher, RandomState};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

// ─── Welford accumulator ─────────────────────────────────────────────

/// Numerically stable online variance via Welford's algorithm.
///
/// Supports single-pass updates and parallel merging (Chan's formula).
#[derive(Debug, Clone)]
pub struct WelfordAccumulator {
    count: u64,
    mean: f64,
    m2: f64, // sum of squared differences from running mean
}

impl WelfordAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }

    /// Add a single observation.
    pub fn update(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
    }

    /// Running mean.
    #[inline]
    pub fn mean(&self) -> f64 {
        if self.count == 0 { 0.0 } else { self.mean }
    }

    /// Population variance.
    pub fn variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            self.m2 / self.count as f64
        }
    }

    /// Population standard deviation.
    pub fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }

    /// Merge another accumulator into this one (Chan's parallel algorithm).
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

// ─── Reservoir sampler ───────────────────────────────────────────────

/// Algorithm R reservoir sampling — every item has equal probability of being
/// in the final sample, using O(`capacity`) memory.
#[derive(Debug, Clone)]
pub(crate) struct StreamReservoirSampler {
    pub(crate) reservoir: Vec<String>,
    pub(crate) capacity: usize,
    count: u64,
    rng: SmallRng,
}

impl StreamReservoirSampler {
    pub fn new(capacity: usize) -> Self {
        Self {
            reservoir: Vec::with_capacity(capacity.min(1024)), // lazy grow
            capacity,
            count: 0,
            rng: SmallRng::from_os_rng(),
        }
    }

    /// Offer an item to the reservoir.
    pub fn offer(&mut self, value: String) {
        self.count += 1;
        if self.reservoir.len() < self.capacity {
            self.reservoir.push(value);
        } else {
            let j = self.rng.random_range(0..self.count as usize);
            if j < self.capacity {
                self.reservoir[j] = value;
            }
        }
    }

    /// Current contents (unordered).
    pub fn samples(&self) -> &[String] {
        &self.reservoir
    }

    /// Merge by concatenating reservoirs, then re-sampling to capacity.
    pub fn merge(&mut self, other: &StreamReservoirSampler) {
        if other.count == 0 {
            return;
        }

        // Combine both reservoirs
        let mut combined: Vec<String> = self.reservoir.drain(..).collect();
        combined.extend(other.reservoir.iter().cloned());

        // Re-sample using Fisher-Yates partial shuffle
        let n = combined.len();
        if n <= self.capacity {
            self.reservoir = combined;
        } else {
            for i in 0..self.capacity {
                let j = self.rng.random_range(i..n);
                combined.swap(i, j);
            }
            combined.truncate(self.capacity);
            self.reservoir = combined;
        }

        self.count += other.count;
    }
}

// ─── Streaming text-length stats ─────────────────────────────────────

/// O(1)-memory text length tracking: min, max, mean/variance via Welford,
/// plus a 32-bucket power-of-2 histogram.
#[derive(Debug, Clone)]
pub struct TextLengthStats {
    pub min_length: usize,
    pub max_length: usize,
    pub avg_length: f64,
    welford: WelfordAccumulator,
    /// Power-of-2 histogram buckets: bucket i covers lengths [2^i, 2^(i+1)).
    /// Bucket 0 = length 0, bucket 1 = length 1, bucket 2 = lengths 2-3, etc.
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

    /// Record a single text length.
    pub fn update(&mut self, length: usize) {
        self.min_length = self.min_length.min(length);
        self.max_length = self.max_length.max(length);
        self.welford.update(length as f64);
        self.avg_length = self.welford.mean();

        let bucket = if length == 0 {
            0
        } else {
            // floor(log2(length)) + 1, clamped to 31
            (usize::BITS - length.leading_zeros()).min(31) as usize
        };
        self.histogram[bucket] += 1;
    }

    /// Merge another `TextLengthStats` into this one.
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

        for (a, b) in self.histogram.iter_mut().zip(other.histogram.iter()) {
            *a += *b;
        }
    }

    /// Return a default "empty" stats object (used when no text values exist).
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

// ─── HyperLogLog wrapper ─────────────────────────────────────────────

/// Thin wrapper around `HyperLogLogPlus` with a fixed hasher so we can
/// implement `Debug` / `Clone` without leaking generics.
#[derive(Clone)]
struct HllCounter {
    /// Raw HLL registers (14-bit precision → 2^14 = 16 384 registers).
    registers: Vec<u8>,
    hasher_state: RandomState,
}

impl std::fmt::Debug for HllCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HllCounter")
            .field("precision", &14u8)
            .field("registers_len", &self.registers.len())
            .finish()
    }
}

impl HllCounter {
    const PRECISION: usize = 14;
    const NUM_REGISTERS: usize = 1 << Self::PRECISION; // 16 384

    fn new() -> Self {
        Self {
            registers: vec![0u8; Self::NUM_REGISTERS],
            hasher_state: RandomState::new(),
        }
    }

    /// Add a value.
    fn insert(&mut self, value: &str) {
        let hash = self.hasher_state.hash_one(value);

        let index = (hash as usize) & (Self::NUM_REGISTERS - 1);
        let remaining = hash >> Self::PRECISION;
        let rank = (remaining.trailing_zeros() + 1) as u8;
        if rank > self.registers[index] {
            self.registers[index] = rank;
        }
    }

    /// Estimate cardinality using the HLL algorithm.
    fn count(&self) -> u64 {
        let m = Self::NUM_REGISTERS as f64;
        // α_m constant for m = 16384
        let alpha_m = 0.7213 / (1.0 + 1.079 / m);

        let raw_estimate: f64 = alpha_m * m * m
            / self
                .registers
                .iter()
                .map(|&r| 2.0_f64.powi(-(r as i32)))
                .sum::<f64>();

        if raw_estimate <= 2.5 * m {
            // Small range correction
            let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                (m * (m / zeros).ln()) as u64
            } else {
                raw_estimate as u64
            }
        } else if raw_estimate <= (1u64 << 32) as f64 / 30.0 {
            raw_estimate as u64
        } else {
            // Large range correction
            let two32 = (1u64 << 32) as f64;
            (-two32 * (1.0 - raw_estimate / two32).ln()) as u64
        }
    }

    /// Merge another counter's registers (element-wise max).
    fn merge(&mut self, other: &HllCounter) {
        for (a, b) in self.registers.iter_mut().zip(other.registers.iter()) {
            *a = (*a).max(*b);
        }
    }
}

// ─── StreamingStatistics ─────────────────────────────────────────────

/// Streaming statistical aggregator with bounded memory.
///
/// Uses O(1) accumulators for numeric stats (Welford), cardinality (HLL),
/// text lengths, and a fixed-capacity reservoir for sample values.
#[derive(Debug, Clone)]
pub struct StreamingStatistics {
    /// Count of processed values
    pub count: usize,
    /// Count of null/empty values
    pub null_count: usize,
    /// Minimum numeric value seen
    pub min: f64,
    /// Maximum numeric value seen
    pub max: f64,

    // ── Bounded accumulators ──
    /// Welford's online accumulator for mean / variance / stddev
    welford: WelfordAccumulator,
    /// HyperLogLog for approximate unique-value counting
    hll: HllCounter,
    /// Reservoir sampler for unbiased samples
    sampler: StreamReservoirSampler,
    /// Streaming text-length tracker
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
            hll: HllCounter::new(),
            sampler: StreamReservoirSampler::new(10_000),
            text_length_tracker: TextLengthStats::new(),
        }
    }

    pub fn limits(_max_unique: usize, max_sample: usize) -> Self {
        Self {
            sampler: StreamReservoirSampler::new(max_sample),
            ..Self::new()
        }
    }

    /// Process a single value incrementally.
    pub fn update(&mut self, value: &str) {
        self.count += 1;

        if value.is_empty() {
            self.null_count += 1;
            return;
        }

        // HLL cardinality (always, O(1))
        self.hll.insert(value);

        // Reservoir sample
        self.sampler.offer(value.to_string());

        // Text length tracking (O(1))
        self.text_length_tracker.update(value.len());

        // Numeric statistics via Welford
        if let Ok(num_val) = value.parse::<f64>() {
            self.welford.update(num_val);
            self.min = self.min.min(num_val);
            self.max = self.max.max(num_val);
        }
    }

    /// Merge statistics from another streaming aggregator.
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

    /// Calculate mean (Welford).
    pub fn mean(&self) -> f64 {
        self.welford.mean()
    }

    /// Calculate population variance (Welford).
    pub fn variance(&self) -> f64 {
        self.welford.variance()
    }

    /// Calculate standard deviation (Welford).
    pub fn std_dev(&self) -> f64 {
        self.welford.std_dev()
    }

    /// Approximate unique-value count (HyperLogLog).
    pub fn unique_count(&self) -> usize {
        self.hll.count() as usize
    }

    /// Always `true` — HLL estimates are inherently approximate.
    pub fn unique_count_is_approximate(&self) -> bool {
        true
    }

    /// Unbiased sample values from reservoir sampling.
    pub fn sample_values(&self) -> &[String] {
        self.sampler.samples()
    }

    /// O(1) text-length statistics (min, max, avg).
    pub fn text_length_stats(&self) -> TextLengthStats {
        if self.text_length_tracker.welford.count == 0 {
            return TextLengthStats::empty();
        }
        self.text_length_tracker.clone()
    }

    /// Memory usage estimate in bytes (now ~constant).
    pub fn memory_usage_bytes(&self) -> usize {
        let struct_size = std::mem::size_of::<Self>();
        let hll_size = self.hll.registers.len(); // 16 KB
        let reservoir_size: usize = self
            .sampler
            .reservoir
            .iter()
            .map(|s| std::mem::size_of::<String>() + s.len())
            .sum();

        struct_size + hll_size + reservoir_size
    }
}

impl Default for StreamingStatistics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── StreamingColumnCollection ───────────────────────────────────────

/// Collection of streaming statistics for all columns.
pub struct StreamingColumnCollection {
    columns: HashMap<String, StreamingStatistics>,
    memory_limit_bytes: usize,
}

impl StreamingColumnCollection {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            memory_limit_bytes: 100 * 1024 * 1024, // 100 MB default
        }
    }

    pub fn memory_limit(limit_mb: usize) -> Self {
        Self {
            columns: HashMap::new(),
            memory_limit_bytes: limit_mb * 1024 * 1024,
        }
    }

    /// Process a record (row) of data.
    pub fn process_record<I>(&mut self, headers: &[String], values: I)
    where
        I: IntoIterator<Item = String>,
    {
        for (header, value) in headers.iter().zip(values) {
            let stats = self.columns.entry(header.to_string()).or_default();
            stats.update(&value);
        }
    }

    /// Get statistics for a specific column.
    pub fn get_column_stats(&self, column_name: &str) -> Option<&StreamingStatistics> {
        self.columns.get(column_name)
    }

    /// Get all column names.
    pub fn column_names(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }

    /// Get total memory usage.
    pub fn memory_usage_bytes(&self) -> usize {
        self.columns.values().map(|s| s.memory_usage_bytes()).sum()
    }

    /// Check if memory usage is approaching the limit.
    pub fn is_memory_pressure(&self) -> bool {
        self.memory_usage_bytes() > (self.memory_limit_bytes * 80 / 100)
    }

    /// Reduce memory usage by truncating reservoir samples.
    ///
    /// With bounded accumulators the main knob is the reservoir size.
    pub fn reduce_memory_usage(&mut self) {
        for stats in self.columns.values_mut() {
            let half = stats.sampler.capacity / 2;
            stats.sampler.reservoir.truncate(half);
        }
    }

    /// Merge another collection into this one.
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

// ─── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_statistics() {
        let mut stats = StreamingStatistics::new();

        stats.update("10.5");
        stats.update("20.0");
        stats.update("15.5");
        stats.update(""); // null value

        assert_eq!(stats.count, 4);
        assert_eq!(stats.null_count, 1);
        // HLL gives approximate counts — for 3 distinct values it should be close
        let uc = stats.unique_count();
        assert!((2..=5).contains(&uc), "unique_count {uc} not in [2,5]");
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
        // HLL approximate: 4 distinct values
        let uc = stats1.unique_count();
        assert!((3..=6).contains(&uc), "unique_count {uc} not in [3,6]");
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

        let age_stats = collection
            .get_column_stats("age")
            .expect("Age column should exist in test");
        assert_eq!(age_stats.count, 2);
        assert!((age_stats.mean() - 27.5).abs() < 1e-10);
    }

    #[test]
    fn test_welford_accuracy() {
        let mut w = WelfordAccumulator::new();
        // Known distribution: 1..=1000
        for i in 1..=1000 {
            w.update(i as f64);
        }
        let expected_mean = 500.5;
        let expected_var = (1000.0 * 1000.0 - 1.0) / 12.0; // population variance of 1..N
        assert!(
            (w.mean() - expected_mean).abs() < 1e-6,
            "mean {} != {}",
            w.mean(),
            expected_mean
        );
        assert!(
            (w.variance() - expected_var).abs() < 1.0,
            "variance {} != {}",
            w.variance(),
            expected_var
        );
    }

    #[test]
    fn test_welford_merge() {
        let mut a = WelfordAccumulator::new();
        let mut b = WelfordAccumulator::new();
        let mut full = WelfordAccumulator::new();

        for i in 1..=500 {
            a.update(i as f64);
            full.update(i as f64);
        }
        for i in 501..=1000 {
            b.update(i as f64);
            full.update(i as f64);
        }

        a.merge(&b);
        assert!(
            (a.mean() - full.mean()).abs() < 1e-10,
            "merged mean {} != {}",
            a.mean(),
            full.mean()
        );
        assert!(
            (a.variance() - full.variance()).abs() < 1e-6,
            "merged var {} != {}",
            a.variance(),
            full.variance()
        );
    }

    #[test]
    fn test_hll_cardinality() {
        let mut hll = HllCounter::new();
        let n = 100_000;
        for i in 0..n {
            hll.insert(&format!("item_{i}"));
        }
        let est = hll.count();
        let error = (est as f64 - n as f64).abs() / n as f64;
        assert!(
            error < 0.05,
            "HLL estimate {est} deviates {:.1}% from {n}",
            error * 100.0
        );
    }

    #[test]
    fn test_reservoir_uniformity() {
        let mut sampler = StreamReservoirSampler::new(1000);
        let n = 100_000;
        for i in 0..n {
            sampler.offer(i.to_string());
        }

        assert_eq!(sampler.samples().len(), 1000);

        // Check that the reservoir contains items from across the full range,
        // not just the first 1000
        let values: Vec<usize> = sampler
            .samples()
            .iter()
            .map(|s| s.parse().unwrap())
            .collect();
        let max_val = *values.iter().max().unwrap();
        assert!(
            max_val > n / 2,
            "reservoir max {max_val} should be > {}, not biased toward early items",
            n / 2
        );
    }

    #[test]
    fn test_text_length_stats_streaming() {
        let mut tls = TextLengthStats::new();
        for &len in &[3, 5, 10, 1, 7] {
            tls.update(len);
        }
        assert_eq!(tls.min_length, 1);
        assert_eq!(tls.max_length, 10);
        assert!((tls.avg_length - 5.2).abs() < 1e-10);
    }

    #[test]
    fn test_memory_usage_bounded() {
        let mut stats = StreamingStatistics::new();
        for i in 0..50_000 {
            stats.update(&format!("value_{i}"));
        }
        // With bounded accumulators, memory should be well under 1 MB
        let usage = stats.memory_usage_bytes();
        assert!(usage < 1_000_000, "memory {usage} bytes exceeds 1 MB bound");
    }
}
