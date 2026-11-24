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
//! A column the caller names in `unique_count_exact_limits` (#1330) keeps
//! XXH3-128 fingerprints instead, up to its own limit, and memory pressure does
//! not release that set: the caller opted into its memory, about 19 to 39 bytes
//! per distinct value with the table's overhead. A collision there has probability
//! about `n^2 / 2^129`, under 1e-24 at 25 million values.
//!
//! Because the HLL uses the same precision and hash for all engines, two engines
//! fed the same values in the same counting mode produce the same estimate; they
//! agree exactly in the high-cardinality regime and within HLL's ~1% relative
//! error elsewhere.

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

/// Hasher for keys that already are well-mixed fingerprints.
#[derive(Default)]
struct FingerprintHasher(u64);

impl Hasher for FingerprintHasher {
    fn write(&mut self, bytes: &[u8]) {
        // Fingerprints use the specialized integer methods below.
        for byte in bytes {
            self.0 = self.0.rotate_left(8) ^ u64::from(*byte);
        }
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }

    fn write_u128(&mut self, value: u128) {
        self.0 = value as u64 ^ (value >> 64) as u64;
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

type Fingerprints<T> = HashSet<T, BuildHasherDefault<FingerprintHasher>>;

#[derive(Debug, Clone)]
enum DistinctCount {
    DefaultWidth(Fingerprints<u64>, usize),
    Wide(Fingerprints<u128>, usize),
    // The count held when the exact set was dropped. Collisions can only
    // reduce this count, so it is a lower bound on the true count (#819).
    Spilled(usize),
}

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
/// The default exact set holds up to [`EXACT_CARDINALITY_THRESHOLD`]
/// distinct values. The count becomes an HLL estimate if it exceeds its limit
/// or releases the set under memory pressure. [`Self::is_approximate`] reports
/// which kind of count is available.
#[derive(Debug, Clone)]
pub struct CardinalityEstimator {
    count: DistinctCount,
    /// Fed on every insert so estimates are available when the exact set is dropped.
    hll: HyperLogLog,
    /// Values inserted, duplicates included: the ceiling on any distinct count.
    inserted: u64,
}

impl CardinalityEstimator {
    pub fn new() -> Self {
        Self::with_threshold(EXACT_CARDINALITY_THRESHOLD)
    }

    pub fn with_threshold(threshold: usize) -> Self {
        Self {
            count: DistinctCount::DefaultWidth(Fingerprints::default(), threshold),
            hll: HyperLogLog::new(),
            inserted: 0,
        }
    }

    /// Use a caller-chosen limit with 128-bit fingerprints. This exact set
    /// spills only when its distinct count exceeds the limit.
    pub fn with_exact_limit(limit: Option<usize>) -> Self {
        match limit {
            Some(limit) => Self {
                count: DistinctCount::Wide(Fingerprints::default(), limit),
                hll: HyperLogLog::new(),
                inserted: 0,
            },
            None => Self::new(),
        }
    }

    #[inline]
    pub fn insert(&mut self, value: &str) {
        self.inserted += 1;
        let hash = HyperLogLog::hash(value);
        self.hll.insert_hash(hash);
        match &mut self.count {
            DistinctCount::DefaultWidth(set, _) => {
                set.insert(hash);
            }
            DistinctCount::Wide(set, _) => {
                set.insert(twox_hash::XxHash3_128::oneshot(value.as_bytes()));
            }
            DistinctCount::Spilled(_) => return,
        }
        self.spill_if_over_budget();
    }

    /// Same as [`Self::insert`]. Kept for callers that hold an owned `String`;
    /// the exact set stores fingerprints, so nothing is moved into it.
    #[inline]
    pub fn insert_owned(&mut self, value: String) {
        self.insert(&value);
    }

    /// Drop the exact set now and answer from the sketch from here on.
    ///
    /// Under memory pressure, release a default-width set. Named sets stay
    /// exact up to their configured limit. After a spill, [`Self::is_approximate`] reports it.
    pub fn spill(&mut self) {
        if let DistinctCount::DefaultWidth(set, _) = &self.count {
            self.count = DistinctCount::Spilled(set.len());
        }
    }

    /// Drop the exact set once it exceeds the budget: the HLL has already seen
    /// every value, so the estimate stays accurate without holding the memory.
    #[inline]
    fn spill_if_over_budget(&mut self) {
        let (count, limit) = match &self.count {
            DistinctCount::DefaultWidth(set, limit) => (set.len(), *limit),
            DistinctCount::Wide(set, limit) => (set.len(), *limit),
            DistinctCount::Spilled(_) => return,
        };
        if count > limit {
            self.count = DistinctCount::Spilled(count);
        }
    }

    /// Best available distinct count: exact when retained, else the HLL
    /// estimate, which is capped at the number of values inserted. The sketch
    /// can overshoot by its error margin (50,755 for 50,000 unique ids), and a
    /// count above the values seen is impossible rather than approximate.
    /// It can undershoot too, and a count below the distinct values the exact
    /// set held is impossible for the same reason, so the estimate never
    /// falls under [`Self::distinct_lower_bound`] (#819).
    pub fn estimate(&self) -> usize {
        match &self.count {
            DistinctCount::Spilled(floor) => (self.hll.count() as usize)
                .min(usize::try_from(self.inserted).unwrap_or(usize::MAX))
                .max(*floor),
            _ => self.distinct_lower_bound(),
        }
    }

    /// Whether [`Self::estimate`] is an HLL approximation rather than an exact count.
    pub fn is_approximate(&self) -> bool {
        matches!(self.count, DistinctCount::Spilled(_))
    }

    /// A count the true distinct count is certain to reach: the exact count
    /// while it is retained, and afterwards the distinct values the exact set
    /// held when it was dropped.
    ///
    /// Unlike [`Self::estimate`], this holds with certainty, so it can bound a
    /// score without spending any of a confidence budget (#819).
    pub fn distinct_lower_bound(&self) -> usize {
        match &self.count {
            DistinctCount::DefaultWidth(set, _) => set.len(),
            DistinctCount::Wide(set, _) => set.len(),
            DistinctCount::Spilled(floor) => *floor,
        }
    }

    pub fn merge(&mut self, other: &CardinalityEstimator) {
        self.inserted += other.inserted;
        self.hll.merge(&other.hll);
        // The union holds at least as many distinct values as either side.
        let floor = self
            .distinct_lower_bound()
            .max(other.distinct_lower_bound());
        match (&mut self.count, &other.count) {
            (DistinctCount::DefaultWidth(mine, _), DistinctCount::DefaultWidth(theirs, _)) => {
                mine.extend(theirs.iter().copied());
            }
            (DistinctCount::Wide(mine, _), DistinctCount::Wide(theirs, _)) => {
                mine.extend(theirs.iter().copied());
            }
            // A spilled side or a different hash width cannot contribute to
            // an exact union. The merged HLL still has both inputs.
            _ => self.count = DistinctCount::Spilled(floor),
        }
        self.spill_if_over_budget();
    }

    pub fn memory_usage_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.hll.memory_usage_bytes() + self.exact_bytes()
    }

    /// Heap held by the exact set alone: what [`Self::spill`] gives back.
    /// Zero once spilled, and zero for a set that has never held a value.
    pub fn exact_bytes(&self) -> usize {
        // decode-audit: no-data — after a spill, the estimator holds only its sketch
        // and the distinct count at the time of the spill.
        match &self.count {
            DistinctCount::DefaultWidth(set, _) => hash_table_bytes::<u64>(set.capacity()),
            DistinctCount::Wide(set, _) => hash_table_bytes::<u128>(set.capacity()),
            DistinctCount::Spilled(_) => 0,
        }
    }

    /// Heap held by the exact set that can be reclaimed under memory pressure.
    pub fn budgeted_exact_bytes(&self) -> usize {
        match &self.count {
            DistinctCount::Wide(..) => 0,
            _ => self.exact_bytes(),
        }
    }

    /// Count distinct values from a materialized column. Keep the full-value
    /// set when no limit was requested, as this route already has the values.
    /// Returns the count, approximation flag and lower bound after spilling.
    pub fn count_materialized<'a>(
        values: impl Iterator<Item = &'a str>,
        exact_limit: Option<usize>,
    ) -> (usize, bool, Option<usize>) {
        let Some(limit) = exact_limit else {
            return (values.collect::<HashSet<_>>().len(), false, None);
        };
        let mut estimator = Self::with_exact_limit(Some(limit));
        for value in values {
            estimator.insert(value);
        }
        let approximate = estimator.is_approximate();
        (
            estimator.estimate(),
            approximate,
            approximate.then(|| estimator.distinct_lower_bound()),
        )
    }
}

/// Heap held by a hashbrown table that reports `capacity`.
///
/// `capacity` is what the table holds before it grows, not what it allocated:
/// a power-of-two bucket count with an 7/8 load factor (one empty bucket below
/// eight), plus a control byte per bucket and one group of trailing control
/// bytes. Counting `capacity` alone under-reports a large table by about 1/8.
fn hash_table_bytes<T>(capacity: usize) -> usize {
    const GROUP_WIDTH: usize = 16;
    if capacity == 0 {
        return 0;
    }
    let buckets = if capacity < 8 {
        capacity + 1
    } else {
        capacity / 7 * 8
    }
    .next_power_of_two();
    buckets * (std::mem::size_of::<T>() + 1) + GROUP_WIDTH
}

/// Spill reclaimable exact sets, the largest first, until those left charge
/// at most `budget` bytes to the budget. Returns the bytes still charged.
///
/// Ties go by position, so pass the estimators in column order and the choice
/// does not depend on hash order. A spilled count is reported approximate.
/// This is the order the incremental engine spills in under memory pressure;
/// a caller with a row tracker spills it only if this was not enough.
pub fn spill_largest_exact_sets(
    estimators: &mut [&mut CardinalityEstimator],
    budget: usize,
) -> usize {
    let mut held: usize = estimators
        .iter()
        .map(|estimator| estimator.budgeted_exact_bytes())
        .sum();
    if held <= budget {
        return held;
    }
    let mut by_size: Vec<(usize, usize)> = estimators
        .iter()
        .map(|estimator| estimator.budgeted_exact_bytes())
        .enumerate()
        .filter(|&(_, bytes)| bytes > 0)
        .collect();
    by_size.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));
    for (position, bytes) in by_size {
        if held <= budget {
            break;
        }
        estimators[position].spill();
        held -= bytes;
    }
    held
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

    #[test]
    fn a_named_limit_stores_and_merges_full_xxh3_128_fingerprints() {
        let mut left = CardinalityEstimator::with_exact_limit(Some(2));
        left.insert("001");
        let mut right = CardinalityEstimator::with_exact_limit(Some(2));
        right.insert("001");
        right.insert("1");
        left.merge(&right);

        let DistinctCount::Wide(fingerprints, _) = &left.count else {
            panic!("named limits must retain a 128-bit set");
        };
        let expected = ["001", "1"].map(|value| twox_hash::XxHash3_128::oneshot(value.as_bytes()));
        assert_eq!(fingerprints.len(), expected.len());
        assert!(expected.iter().all(|hash| fingerprints.contains(hash)));
        left.insert("2");
        assert!(left.is_approximate());
        assert_eq!(left.distinct_lower_bound(), 3);
    }

    #[test]
    fn pinned_distinct_sets_are_outside_the_spill_budget() {
        let mut pinned = fill(CardinalityEstimator::with_exact_limit(Some(20)), 20);
        let mut default = fill(CardinalityEstimator::new(), 100);
        pinned.spill();
        assert!(pinned.exact_bytes() > 0);
        assert_eq!(pinned.budgeted_exact_bytes(), 0);
        let budget = default.exact_bytes();
        assert_eq!(
            spill_largest_exact_sets(&mut [&mut pinned, &mut default], budget),
            budget
        );
        assert!(!default.is_approximate());
        assert_eq!(
            spill_largest_exact_sets(&mut [&mut pinned, &mut default], 0),
            0
        );
        assert_eq!(pinned.estimate(), 20);
        assert!(!pinned.is_approximate());
        assert!(default.is_approximate());
    }

    #[test]
    fn the_distinct_floor_survives_every_way_the_exact_set_is_dropped() {
        // Past the threshold: the set held one value more than it.
        let over = spilled_over(SPILL_AT + 500);
        assert!(over.is_approximate());
        assert_eq!(over.distinct_lower_bound(), SPILL_AT + 1);

        // Under memory pressure, well below it.
        let mut pressured = estimator_over(1_234);
        pressured.spill();
        assert!(pressured.is_approximate());
        assert_eq!(pressured.distinct_lower_bound(), 1_234);
        // Spilling again keeps what is known rather than resetting it.
        pressured.spill();
        assert_eq!(pressured.distinct_lower_bound(), 1_234);

        // Exact: the count itself.
        assert_eq!(estimator_over(77).distinct_lower_bound(), 77);
    }

    #[test]
    fn a_merge_keeps_the_larger_floor_or_the_union() {
        // One side spilled: the union holds at least what either side knew.
        let mut spilled = estimator_over(300);
        spilled.spill();
        let mut merged = estimator_over(900);
        merged.merge(&spilled);
        assert!(merged.is_approximate());
        assert_eq!(merged.distinct_lower_bound(), 900);

        // Both exact, and the union crosses the threshold: its own size.
        let mut left = CardinalityEstimator::with_threshold(SPILL_AT);
        let mut right = CardinalityEstimator::with_threshold(SPILL_AT);
        for value in 0..8_000 {
            left.insert(&value.to_string());
        }
        for value in 4_000..12_000 {
            right.insert(&value.to_string());
        }
        left.merge(&right);
        assert!(left.is_approximate());
        assert_eq!(left.distinct_lower_bound(), 12_000);
    }

    #[test]
    fn an_estimate_never_falls_below_the_floor() {
        // Just past the threshold the sketch undershoots about half the time;
        // a count below the distinct values the exact set held is impossible.
        let mut undershoots = 0;
        for set in 0..20 {
            let mut est = CardinalityEstimator::with_threshold(SPILL_AT);
            for value in 0..=SPILL_AT {
                est.insert(&format!("{set}-{value}"));
            }
            assert!(est.is_approximate());
            if (est.hll.count() as usize) < est.distinct_lower_bound() {
                undershoots += 1;
            }
            assert!(
                est.estimate() >= est.distinct_lower_bound(),
                "set {set}: {} < {}",
                est.estimate(),
                est.distinct_lower_bound()
            );
        }
        // The clamp is exercised, not vacuous.
        assert!(undershoots > 0, "no set made the sketch undershoot");
    }

    #[test]
    fn the_floor_never_exceeds_the_true_distinct_count() {
        // Duplicates past the spill add inserts, not distinct values.
        let mut est = CardinalityEstimator::with_threshold(SPILL_AT);
        for round in 0..3 {
            for value in 0..(SPILL_AT + 10) {
                est.insert(&format!("{value}"));
            }
            assert!(est.distinct_lower_bound() <= SPILL_AT + 10, "round {round}");
        }
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
    fn the_largest_sets_spill_first_and_only_until_under_budget() {
        let mut small = estimator_over(1_000);
        let mut large = estimator_over(50_000);
        let mut tied_first = estimator_over(20_000);
        let mut tied_second = estimator_over(20_000);
        let large_bytes = large.exact_bytes();
        let tie_bytes = tied_first.exact_bytes();
        let small_bytes = small.exact_bytes();
        assert!(large_bytes > tie_bytes && tie_bytes > small_bytes);

        // Room for one tied set and the small one: the large set goes, then
        // the first of the tie by position.
        let budget = tie_bytes + small_bytes;
        let held = spill_largest_exact_sets(
            &mut [&mut small, &mut large, &mut tied_first, &mut tied_second],
            budget,
        );
        assert_eq!(held, budget);
        assert!(large.is_approximate());
        assert!(tied_first.is_approximate());
        assert!(!tied_second.is_approximate());
        assert!(!small.is_approximate());
        assert_eq!(small.estimate(), 1_000);

        // Under budget, nothing spills.
        let mut kept = estimator_over(1_000);
        let bytes = kept.exact_bytes();
        assert_eq!(spill_largest_exact_sets(&mut [&mut kept], bytes), bytes);
        assert!(!kept.is_approximate());
    }

    #[test]
    fn table_bytes_count_the_buckets_behind_the_capacity() {
        // Each capacity a growing table reports maps back to its bucket count.
        let mut set = Fingerprints::default();
        let mut seen = Vec::new();
        for value in 0..100_000u64 {
            set.insert(value);
            if seen.last() != Some(&set.capacity()) {
                seen.push(set.capacity());
            }
        }
        for capacity in seen {
            let buckets = (hash_table_bytes::<u64>(capacity) - 16) / 9;
            assert!(buckets.is_power_of_two(), "capacity {capacity}");
            let expected = if buckets < 8 {
                buckets - 1
            } else {
                buckets / 8 * 7
            };
            assert_eq!(capacity, expected, "{buckets} buckets");
        }
        assert_eq!(hash_table_bytes::<u64>(0), 0);
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
