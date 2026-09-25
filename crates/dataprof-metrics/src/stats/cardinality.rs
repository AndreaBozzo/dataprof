//! Bounded-memory distinct-count estimation shared across engines.
//!
//! Every engine that reports `unique_count` needs the same guarantee: exact for
//! small columns, and an *honest* estimate for large ones -- never a hard cap
//! exposed as if it were the true count. [`CardinalityEstimator`] keeps an exact
//! set until it crosses [`EXACT_CARDINALITY_THRESHOLD`] distinct values, then
//! frees it and relies on a [`HyperLogLog`] sketch that has seen every value from
//! the start. Memory is bounded by the threshold plus the fixed HLL registers.
//!
//! The exact set holds 64-bit fingerprints (the hash the sketch computes
//! anyway), not the values: about 10 bytes per distinct value instead of the
//! string plus its allocation. That is what lets the exact regime reach a
//! million distinct values. Two distinct values share a fingerprint with
//! probability about `n^2 / 2^65` over `n` values, under 3e-8 at the threshold,
//! so the count is exact in every practical sense; a collision can only make
//! it one lower.
//!
//! Because the HLL uses the same precision and hash for all engines, two engines
//! fed the same values produce the same estimate; they agree exactly in the
//! high-cardinality regime and within HLL's ~1% relative error elsewhere.

use std::collections::HashSet;
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};

/// Distinct values retained exactly before switching to the HLL sketch.
///
/// A key column is as distinct as the table is long, and its uniqueness feeds
/// the quality score, so the exact regime has to reach realistic table sizes:
/// at 10,000 a clean 200,000-row table read 181 phantom duplicate rows and a
/// key uniqueness of 99.6%. At about 10 bytes per value this is ~10-20 MB for
/// a fully distinct column; the streaming engines spill earlier under memory
/// pressure and report the count as approximate.
pub const EXACT_CARDINALITY_THRESHOLD: usize = 1_000_000;

/// Hasher for keys that already are well-mixed 64-bit hashes.
#[derive(Default)]
struct FingerprintHasher(u64);

impl Hasher for FingerprintHasher {
    fn write(&mut self, bytes: &[u8]) {
        // Only `u64` fingerprints are hashed with this, through `write_u64`.
        for byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(*byte);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type Fingerprints = HashSet<u64, BuildHasherDefault<FingerprintHasher>>;

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
        self.insert_hash(Self::hash(value));
    }

    /// The fixed-seed 64-bit hash a value is placed by.
    #[inline]
    fn hash(value: &str) -> u64 {
        let mut hasher = HllBuildHasher.build_hasher();
        hasher.write(value.as_bytes());
        hasher.finish()
    }

    #[inline]
    fn insert_hash(&mut self, hash: u64) {
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
    /// Fingerprints of the distinct values, dropped once the threshold is
    /// crossed or the caller spills them under memory pressure.
    exact: Option<Fingerprints>,
    /// Fed on every insert so the estimate is accurate the moment `exact` is gone.
    hll: HyperLogLog,
    threshold: usize,
    /// Values inserted, duplicates included: the ceiling on any distinct count.
    inserted: u64,
}

impl CardinalityEstimator {
    pub fn new() -> Self {
        Self::with_threshold(EXACT_CARDINALITY_THRESHOLD)
    }

    pub fn with_threshold(threshold: usize) -> Self {
        Self {
            exact: Some(Fingerprints::default()),
            hll: HyperLogLog::new(),
            threshold,
            inserted: 0,
        }
    }

    #[inline]
    pub fn insert(&mut self, value: &str) {
        self.inserted += 1;
        let hash = HyperLogLog::hash(value);
        self.hll.insert_hash(hash);
        if let Some(exact) = self.exact.as_mut() {
            exact.insert(hash);
            self.spill_if_over_budget();
        }
    }

    /// Same as [`Self::insert`]. Kept for callers that hold an owned `String`;
    /// the exact set stores fingerprints, so nothing is moved into it.
    #[inline]
    pub fn insert_owned(&mut self, value: String) {
        self.insert(&value);
    }

    /// Drop the exact set now and answer from the sketch from here on.
    ///
    /// For callers under memory pressure: the sketch has seen every value, so
    /// the estimate stays honest, and [`Self::is_approximate`] reports it.
    pub fn spill(&mut self) {
        self.exact = None;
    }

    /// Drop the exact set once it exceeds the budget: the HLL has already seen
    /// every value, so the estimate stays accurate without holding the memory.
    #[inline]
    fn spill_if_over_budget(&mut self) {
        if self
            .exact
            .as_ref()
            .is_some_and(|exact| exact.len() > self.threshold)
        {
            self.exact = None;
        }
    }

    /// Best available distinct count: exact when retained, else the HLL
    /// estimate, which is capped at the number of values inserted. The sketch
    /// can overshoot by its error margin (50,755 for 50,000 unique ids), and a
    /// count above the values seen is impossible rather than approximate.
    pub fn estimate(&self) -> usize {
        match &self.exact {
            Some(exact) => exact.len(),
            None => (self.hll.count() as usize)
                .min(usize::try_from(self.inserted).unwrap_or(usize::MAX)),
        }
    }

    /// Whether [`Self::estimate`] is an HLL approximation rather than an exact count.
    pub fn is_approximate(&self) -> bool {
        self.exact.is_none()
    }

    pub fn merge(&mut self, other: &CardinalityEstimator) {
        self.inserted += other.inserted;
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
        std::mem::size_of::<Self>() + self.hll.memory_usage_bytes() + self.exact_bytes()
    }

    /// Heap held by the exact set alone: what [`Self::spill`] gives back.
    /// Zero once spilled, and zero for a set that has never held a value.
    pub fn exact_bytes(&self) -> usize {
        // decode-audit: no-data — once the estimator spills, the exact set is
        // dropped (None), so it genuinely holds zero bytes.
        // A hashbrown table holds one control byte per bucket beside the key.
        self.exact
            .as_ref()
            .map(|set| set.capacity() * (std::mem::size_of::<u64>() + 1))
            .unwrap_or(0)
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

    /// The pre-1.0 threshold, kept for the tests that exercise the sketch:
    /// spilling at the default bound would take a million inserts per case.
    const SPILL_AT: usize = 10_000;

    fn estimator_over(distinct: usize) -> CardinalityEstimator {
        fill(CardinalityEstimator::new(), distinct)
    }

    fn spilled_over(distinct: usize) -> CardinalityEstimator {
        fill(CardinalityEstimator::with_threshold(SPILL_AT), distinct)
    }

    fn fill(mut est: CardinalityEstimator, distinct: usize) -> CardinalityEstimator {
        for value in 0..distinct {
            est.insert(&value.to_string());
        }
        est
    }

    #[test]
    fn a_table_sized_column_stays_exact() {
        // At the old 10,000 bound this column read 50,755 distinct values.
        for distinct in [50_000usize, 200_000] {
            let est = estimator_over(distinct);
            assert!(
                !est.is_approximate(),
                "{distinct} distinct should stay exact"
            );
            assert_eq!(est.estimate(), distinct);
        }
    }

    #[test]
    fn a_spilled_estimate_never_exceeds_the_values_inserted() {
        let est = spilled_over(50_000);
        assert!(est.is_approximate());
        assert!(
            est.hll.count() > 50_000,
            "the sketch no longer overshoots here; this test reaches nothing"
        );
        assert_eq!(est.estimate(), 50_000);

        // Merging adds the sides' inserts, so the cap follows the union.
        let mut merged = spilled_over(50_000);
        merged.merge(&spilled_over(50_000));
        assert!(merged.estimate() <= 100_000);
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
        let est = spilled_over(SPILL_AT);
        assert!(!est.is_approximate());
        assert_eq!(est.estimate(), SPILL_AT);
        assert!(spilled_over(SPILL_AT + 1).is_approximate());
    }

    #[test]
    fn spilling_on_demand_keeps_an_honest_estimate() {
        let mut est = estimator_over(20_000);
        assert!(!est.is_approximate());
        let exact_bytes = est.memory_usage_bytes();
        est.spill();
        assert!(est.is_approximate());
        assert!(est.memory_usage_bytes() < exact_bytes);
        let error = (est.estimate() as f64 - 20_000.0).abs() / 20_000.0;
        assert!(error < 0.03, "spilled estimate off by {error:.4}");
    }

    #[test]
    fn large_columns_estimate_without_the_hard_cap() {
        // The bug this replaces reported exactly the cap; the estimate must be
        // close to the truth instead, and flagged approximate.
        for distinct in [SPILL_AT + 1, 100_000, 500_000] {
            let est = spilled_over(distinct);
            assert!(
                est.is_approximate(),
                "{distinct} distinct should spill to HLL"
            );
            let estimate = est.estimate();
            assert_ne!(estimate, SPILL_AT);
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
        let big = spilled_over(100_000);
        small.merge(&big);
        assert!(small.is_approximate());
        let error = (small.estimate() as f64 - 100_000.0).abs() / 100_000.0;
        assert!(error < 0.03, "merged estimate off by {error:.4}");
    }

    #[test]
    fn estimate_is_deterministic_across_runs() {
        assert_eq!(
            spilled_over(250_000).estimate(),
            spilled_over(250_000).estimate()
        );
    }
}
