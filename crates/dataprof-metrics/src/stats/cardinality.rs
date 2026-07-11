//! Bounded-memory distinct-count estimation shared across engines.
//!
//! Every engine that reports `unique_count` needs the same guarantee: exact for
//! small columns, and an *honest* estimate for large ones -- never a hard cap
//! exposed as if it were the true count. [`CardinalityEstimator`] keeps an exact
//! set until it crosses [`EXACT_CARDINALITY_THRESHOLD`] distinct values, then
//! frees it and relies on a [`HyperLogLog`] sketch that has seen every value from
//! the start. Memory is bounded by the threshold plus the fixed HLL registers.
//!
//! Because the HLL uses the same precision and hash for all engines, two engines
//! fed the same values produce the same estimate; they agree exactly in the
//! high-cardinality regime and within HLL's ~1% relative error elsewhere.

use std::collections::HashSet;
use std::hash::{BuildHasher, Hasher};

/// Distinct values retained exactly before switching to the HLL sketch.
///
/// Chosen to match the streaming reservoir's order of magnitude so the file and
/// streaming engines report the same exact counts for small/medium columns.
pub const EXACT_CARDINALITY_THRESHOLD: usize = 10_000;

/// Fixed-seed hasher so a given string maps to the same HLL register on every
/// run and in every engine -- distinct-count estimates must be reproducible.
struct HllBuildHasher;

impl BuildHasher for HllBuildHasher {
    type Hasher = std::collections::hash_map::DefaultHasher;

    fn build_hasher(&self) -> Self::Hasher {
        std::collections::hash_map::DefaultHasher::new()
    }
}

/// HyperLogLog distinct-count sketch (~16 KB of fixed registers).
#[derive(Clone)]
pub struct HyperLogLog {
    registers: Vec<u8>,
}

impl std::fmt::Debug for HyperLogLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperLogLog")
            .field("precision", &Self::PRECISION)
            .field("registers_len", &self.registers.len())
            .finish()
    }
}

impl HyperLogLog {
    const PRECISION: usize = 14;
    const NUM_REGISTERS: usize = 1 << Self::PRECISION;

    pub fn new() -> Self {
        Self {
            registers: vec![0u8; Self::NUM_REGISTERS],
        }
    }

    #[inline]
    pub fn insert(&mut self, value: &str) {
        let mut hasher = HllBuildHasher.build_hasher();
        hasher.write(value.as_bytes());
        let hash = hasher.finish();

        let index = (hash as usize) & (Self::NUM_REGISTERS - 1);
        let window = hash >> Self::PRECISION;
        let rank = (window.leading_zeros() - Self::PRECISION as u32 + 1) as u8;

        if rank > self.registers[index] {
            self.registers[index] = rank;
        }
    }

    pub fn count(&self) -> u64 {
        let register_count = Self::NUM_REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / register_count);

        let raw_estimate: f64 = alpha * register_count * register_count
            / self
                .registers
                .iter()
                .map(|&register| 2.0_f64.powi(-(register as i32)))
                .sum::<f64>();

        if raw_estimate <= 2.5 * register_count {
            let zeros = self
                .registers
                .iter()
                .filter(|&&register| register == 0)
                .count() as f64;
            if zeros > 0.0 {
                (register_count * (register_count / zeros).ln()) as u64
            } else {
                raw_estimate as u64
            }
        } else if raw_estimate <= (1u64 << 32) as f64 / 30.0 {
            raw_estimate as u64
        } else {
            let two32 = (1u64 << 32) as f64;
            (-two32 * (1.0 - raw_estimate / two32).ln()) as u64
        }
    }

    pub fn merge(&mut self, other: &HyperLogLog) {
        for (left, right) in self.registers.iter_mut().zip(other.registers.iter()) {
            *left = (*left).max(*right);
        }
    }

    /// Heap bytes held by the registers, for memory accounting.
    pub fn memory_usage_bytes(&self) -> usize {
        self.registers.len()
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

/// Exact-then-approximate distinct-count estimator with bounded memory.
///
/// Reports an exact count while fewer than [`EXACT_CARDINALITY_THRESHOLD`]
/// distinct values have been seen, then an HLL estimate. [`Self::is_approximate`]
/// tells callers which regime a given count came from.
#[derive(Debug, Clone)]
pub struct CardinalityEstimator {
    /// Exact distinct values, dropped once the threshold is crossed.
    exact: Option<HashSet<String>>,
    /// Fed on every insert so the estimate is accurate the moment `exact` is gone.
    hll: HyperLogLog,
    threshold: usize,
}

impl CardinalityEstimator {
    pub fn new() -> Self {
        Self::with_threshold(EXACT_CARDINALITY_THRESHOLD)
    }

    pub fn with_threshold(threshold: usize) -> Self {
        Self {
            exact: Some(HashSet::new()),
            hll: HyperLogLog::new(),
            threshold,
        }
    }

    #[inline]
    pub fn insert(&mut self, value: &str) {
        self.hll.insert(value);
        if let Some(exact) = self.exact.as_mut() {
            exact.insert(value.to_string());
            // Once we exceed the exact budget, drop the set: the HLL has already
            // seen every value, so the estimate stays accurate without the memory.
            if exact.len() > self.threshold {
                self.exact = None;
            }
        }
    }

    /// Best available distinct count: exact when retained, else the HLL estimate.
    pub fn estimate(&self) -> usize {
        match &self.exact {
            Some(exact) => exact.len(),
            None => self.hll.count() as usize,
        }
    }

    /// Whether [`Self::estimate`] is an HLL approximation rather than an exact count.
    pub fn is_approximate(&self) -> bool {
        self.exact.is_none()
    }

    pub fn merge(&mut self, other: &CardinalityEstimator) {
        self.hll.merge(&other.hll);
        match (self.exact.as_mut(), other.exact.as_ref()) {
            (Some(mine), Some(theirs)) => {
                mine.extend(theirs.iter().cloned());
                if mine.len() > self.threshold {
                    self.exact = None;
                }
            }
            // If either side already spilled, the union can only be larger, so the
            // merged result is approximate too; the merged HLL carries the estimate.
            _ => self.exact = None,
        }
    }

    pub fn memory_usage_bytes(&self) -> usize {
        let exact_bytes = self
            .exact
            .as_ref()
            .map(|set| set.iter().map(|value| value.len()).sum::<usize>())
            .unwrap_or(0);
        std::mem::size_of::<Self>() + self.hll.memory_usage_bytes() + exact_bytes
    }
}

impl Default for CardinalityEstimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn estimator_over(distinct: usize) -> CardinalityEstimator {
        let mut est = CardinalityEstimator::new();
        for value in 0..distinct {
            est.insert(&value.to_string());
        }
        est
    }

    #[test]
    fn small_columns_are_exact() {
        for distinct in [0usize, 1, 999, 1_000, 1_001] {
            let est = estimator_over(distinct);
            assert!(
                !est.is_approximate(),
                "{distinct} distinct should stay exact"
            );
            assert_eq!(est.estimate(), distinct);
        }
    }

    #[test]
    fn exactly_at_threshold_is_still_exact() {
        let est = estimator_over(EXACT_CARDINALITY_THRESHOLD);
        assert!(!est.is_approximate());
        assert_eq!(est.estimate(), EXACT_CARDINALITY_THRESHOLD);
    }

    #[test]
    fn large_columns_estimate_without_the_hard_cap() {
        // The bug this replaces reported exactly the cap; the estimate must be
        // close to the truth instead, and flagged approximate.
        for distinct in [10_000usize + 1, 100_000, 500_000] {
            let est = estimator_over(distinct);
            assert!(
                est.is_approximate(),
                "{distinct} distinct should spill to HLL"
            );
            let estimate = est.estimate();
            assert_ne!(estimate, EXACT_CARDINALITY_THRESHOLD);
            let error = (estimate as f64 - distinct as f64).abs() / distinct as f64;
            assert!(
                error < 0.03,
                "{distinct} distinct: estimate {estimate} off by {error:.4}"
            );
        }
    }

    #[test]
    fn merge_of_exact_halves_stays_exact() {
        let mut left = CardinalityEstimator::new();
        let mut right = CardinalityEstimator::new();
        for value in 0..400 {
            left.insert(&value.to_string());
        }
        for value in 200..600 {
            right.insert(&value.to_string());
        }
        left.merge(&right);
        assert!(!left.is_approximate());
        assert_eq!(left.estimate(), 600);
    }

    #[test]
    fn merge_with_spilled_side_is_approximate() {
        let mut small = estimator_over(10);
        let big = estimator_over(100_000);
        small.merge(&big);
        assert!(small.is_approximate());
        let error = (small.estimate() as f64 - 100_000.0).abs() / 100_000.0;
        assert!(error < 0.03, "merged estimate off by {error:.4}");
    }

    #[test]
    fn estimate_is_deterministic_across_runs() {
        assert_eq!(
            estimator_over(250_000).estimate(),
            estimator_over(250_000).estimate()
        );
    }
}
