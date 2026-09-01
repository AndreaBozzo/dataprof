use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use crate::{ValueHintBindingAccumulator, profile_builder::infer_data_type_streaming};
use dataprof_core::{SemanticHintBinding, SemanticHintKind, SemanticHints, char_len};
use dataprof_metrics::analysis::inference::is_null_like_token;
use dataprof_metrics::{
    CardinalityEstimator, HyperLogLog, RowCompletenessSummary, RowDuplicateSummary,
    value_matches_hint,
};

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

    /// Number of values folded into this accumulator.
    #[inline]
    pub fn count(&self) -> u64 {
        self.count
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

    /// Unbiased sample variance (n-1 denominator), matching the convention of
    /// the batch numeric stats in `dataprof-metrics`.
    pub fn sample_variance(&self) -> f64 {
        if self.count < 2 {
            0.0
        } else {
            (self.m2 / (self.count - 1) as f64).max(0.0)
        }
    }

    /// Standard deviation derived from [`Self::sample_variance`].
    pub fn sample_std_dev(&self) -> f64 {
        self.sample_variance().sqrt()
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
pub struct StreamReservoirSampler {
    reservoir: Vec<String>,
    capacity: usize,
    count: u64,
    rng: SmallRng,
}

impl StreamReservoirSampler {
    const DEFAULT_SEED: u64 = 0xDA7A_900D_F00D_5EED;

    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
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

    /// Fold another sampler in, leaving every value of the combined population
    /// equally likely to have survived.
    ///
    /// The two reservoirs stand for populations of very different sizes: a
    /// partition of a million rows and one of a hundred each hand over at most
    /// `capacity` values. Pooling the reservoirs and subsampling uniformly
    /// would let the small partition supply half the merged sample, so sample
    /// values, and the patterns inferred from them, would depend on how the
    /// input happened to be split. The number of survivors drawn from each
    /// side follows the hypergeometric distribution over the two population
    /// counts instead, which is the split a single pass would have produced.
    ///
    /// A sampler shrunk under memory pressure holds fewer values than its
    /// population earns it. Nothing can be conjured in their place, so the
    /// shortfall is filled from the other side rather than returning a sample
    /// shorter than the capacity allows.
    pub fn merge(&mut self, other: &StreamReservoirSampler) {
        if other.count == 0 {
            return;
        }

        let available_self = self.reservoir.len();
        let available_other = other.reservoir.len();
        let target = self.capacity.min(available_self + available_other);

        let mut take_self = 0usize;
        let mut take_other = 0usize;
        let mut population_self = self.count;
        let mut population_other = other.count;

        for _ in 0..target {
            // Fewer draws remain than the two sides hold, so at most one of
            // them is exhausted here. Each side also keeps at least one
            // unconsumed population row per unconsumed sample slot, so the
            // range below is never empty.
            let from_self = if take_self == available_self {
                false
            } else if take_other == available_other {
                true
            } else {
                self.rng.random_range(0..population_self + population_other) < population_self
            };

            if from_self {
                take_self += 1;
                population_self -= 1;
            } else {
                take_other += 1;
                population_other -= 1;
            }
        }

        // Partial Fisher-Yates over each side: the first `take_*` slots become
        // a uniform draw without replacement from that side's sample.
        let mut merged: Vec<String> = std::mem::take(&mut self.reservoir);
        for index in 0..take_self {
            let swap_with = self.rng.random_range(index..available_self);
            merged.swap(index, swap_with);
        }
        merged.truncate(take_self);

        let mut indices: Vec<usize> = (0..available_other).collect();
        for index in 0..take_other {
            let swap_with = self.rng.random_range(index..available_other);
            indices.swap(index, swap_with);
        }
        merged.extend(
            indices[..take_other]
                .iter()
                .map(|&index| other.reservoir[index].clone()),
        );

        self.reservoir = merged;
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
    date_match_count: usize,
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
            date_match_count: 0,
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
        // Unicode scalar values, not UTF-8 bytes: see `dataprof_core::text_units`.
        self.text_length_tracker.update(char_len(value));
        if value_matches_hint(value, SemanticHintKind::Temporal) {
            self.date_match_count += 1;
        }

        if let Some(number) = value
            .trim()
            .parse::<f64>()
            .ok()
            .filter(|num| num.is_finite())
        {
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
        self.date_match_count += other.date_match_count;
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

    /// Values over the full stream accepted by the temporal calculator.
    pub fn date_match_count(&self) -> usize {
        self.date_match_count
    }

    /// Exact aggregates over every numeric value this column has streamed,
    /// or `None` when no value parsed as a finite number.
    ///
    /// These come from the O(1)-memory min/max fields and the Welford
    /// accumulator, so they cover the full stream even when the reservoir
    /// sample no longer does.
    pub fn exact_numeric_aggregates(
        &self,
    ) -> Option<crate::profile_builder::ExactNumericAggregates> {
        let count = self.welford.count();
        if count == 0 {
            return None;
        }
        Some(crate::profile_builder::ExactNumericAggregates {
            min: self.min,
            max: self.max,
            mean: self.welford.mean(),
            std_dev: self.welford.sample_std_dev(),
            variance: self.welford.sample_variance(),
            count: count as usize,
        })
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

/// Canonical encoding of one row, for duplicate detection.
///
/// Fields are length-prefixed so their boundaries are unambiguous:
/// `["ab", "c"]` and `["a", "bc"]` must not sign alike. Trailing absent
/// fields are then dropped, which makes the signature independent of how wide
/// the schema had grown when the row was read. A JSON key discovered
/// mid-stream appends a column, and without the trim every row that predates
/// it signs one field shorter than an identical row read afterwards, so the
/// two count as distinct and the duplicate total comes up silently short.
///
/// The trim is safe because an absent field and an explicitly empty one are
/// the same null to this profiler: it collapses only rows that already hold
/// equal values in every column they share. On a fixed schema it applies
/// uniformly to every row, so it changes no count there.
#[derive(Debug, Default)]
pub struct RowSignature {
    buffer: String,
    /// Length of `buffer` through the last field that held a value.
    len_through_last_present: usize,
}

impl RowSignature {
    /// Append one field. An absent field is the empty string, the same
    /// representation [`StreamingColumnCollection::process_record`] gives a
    /// missing trailing value and an Arrow null.
    ///
    /// Only an exactly empty value counts as absent here, deliberately: the
    /// broader `is_null_like_token` rule that decides `null_count` also matches
    /// `null`, `nan` and whitespace, and trimming on it would collapse a row
    /// ending in one of those against a row that ends in no field at all,
    /// inventing a duplicate.
    pub fn push_field(&mut self, value: &str) {
        let _ = write!(self.buffer, "{}:", value.len());
        self.buffer.push_str(value);
        if !value.is_empty() {
            self.len_through_last_present = self.buffer.len();
        }
    }

    /// The signature, with trailing absent fields dropped.
    pub fn finish(mut self) -> String {
        self.buffer.truncate(self.len_through_last_present);
        self.buffer
    }
}

/// Full-stream row-duplicate tracking with bounded memory.
///
/// Every record's fields are folded into a [`RowSignature`] and fed to a
/// [`CardinalityEstimator`]: duplicates are counted exactly while the
/// distinct-row signatures fit the estimator's exact set, and estimated
/// (flagged approximate) once it spills to its HLL sketch. Unlike the
/// per-column reservoirs, this sees whole rows — including null-like values —
/// so the count is row-aligned by construction.
#[derive(Debug, Clone, Default)]
pub struct RowUniquenessTracker {
    rows_seen: usize,
    distinct: CardinalityEstimator,
    /// Set once signatures made under different schemas have been folded in.
    ///
    /// A signature encodes length-prefixed values and nothing else, so a row
    /// `{a: "x"}` and a row `{b: "x"}` both sign as `1:x` while the union
    /// schema makes them different rows; a differing field *order* collides
    /// the same way. The signatures are already folded into the estimator and
    /// cannot be rebuilt against the union schema, so the count is withheld
    /// rather than reported as a plausible number. Defaults to false, which is
    /// what a tracker fed from a single schema is.
    incomparable_signatures: bool,
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

    /// Record that the signatures folded in so far were not all made against
    /// the same schema, so they cannot be compared with each other.
    pub fn mark_incomparable(&mut self) {
        self.incomparable_signatures = true;
    }

    pub fn merge(&mut self, other: &RowUniquenessTracker) {
        self.rows_seen += other.rows_seen;
        self.distinct.merge(&other.distinct);
        self.incomparable_signatures |= other.incomparable_signatures;
    }

    pub fn memory_usage_bytes(&self) -> usize {
        self.distinct.memory_usage_bytes()
    }

    /// Summary for quality metrics, or `None` when no rows were observed
    /// (e.g. an engine that never fed whole records through this tracker) or
    /// when the signatures seen are not comparable with one another.
    pub fn summary(&self) -> Option<RowDuplicateSummary> {
        if self.rows_seen == 0 || self.incomparable_signatures {
            return None;
        }
        Some(RowDuplicateSummary {
            duplicate_rows: self.duplicate_rows(),
            rows_checked: self.rows_seen,
            approximate: self.is_approximate(),
        })
    }
}

/// Full-stream count of records in which every field is present.
///
/// Completeness of a *record* is not recoverable from per-column null
/// totals: those say how many nulls exist, not whether two of them shared a
/// row. One counter fed whole rows answers it exactly, at any scale and
/// regardless of sampling.
#[derive(Debug, Clone, Default)]
pub struct RowCompletenessTracker {
    rows_seen: usize,
    complete_rows: usize,
}

impl RowCompletenessTracker {
    /// Record one row. `had_null` is true when any of its fields was absent.
    pub fn observe(&mut self, had_null: bool) {
        self.rows_seen += 1;
        if !had_null {
            self.complete_rows += 1;
        }
    }

    /// A column appeared after rows had already been counted, so every row
    /// counted so far is missing it and none of them was complete after all.
    pub fn invalidate_completed_rows(&mut self) {
        self.complete_rows = 0;
    }

    /// Count complete records across columns that are already row-aligned
    /// and hold every value, as the database and ad-hoc input paths do.
    ///
    /// A row shorter than `total_rows` in some column is missing that field,
    /// which is the same as holding a null there — the reading
    /// [`StreamingColumnCollection::process_record`] gives ragged records.
    pub fn observe_aligned_columns(&mut self, columns: &[&[String]], total_rows: usize) {
        if columns.is_empty() {
            return;
        }
        for row_index in 0..total_rows {
            let had_null = columns.iter().any(|cells| {
                cells
                    .get(row_index)
                    .is_none_or(|value| is_null_like_token(value))
            });
            self.observe(had_null);
        }
    }

    pub fn merge(&mut self, other: &RowCompletenessTracker) {
        self.rows_seen += other.rows_seen;
        self.complete_rows += other.complete_rows;
    }

    /// Exact complete-record counts, or `None` when no rows were observed.
    pub fn summary(&self) -> Option<RowCompletenessSummary> {
        if self.rows_seen == 0 {
            return None;
        }
        Some(RowCompletenessSummary {
            complete_rows: self.complete_rows,
            rows_checked: self.rows_seen,
        })
    }
}

pub struct StreamingColumnCollection {
    columns: HashMap<String, StreamingStatistics>,
    ordered_names: Vec<String>,
    memory_limit_bytes: usize,
    row_tracker: RowUniquenessTracker,
    completeness_tracker: RowCompletenessTracker,
    hint_bindings: ValueHintBindingAccumulator,
}

impl StreamingColumnCollection {
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
            ordered_names: Vec::new(),
            memory_limit_bytes: 100 * 1024 * 1024,
            row_tracker: RowUniquenessTracker::default(),
            completeness_tracker: RowCompletenessTracker::default(),
            hint_bindings: ValueHintBindingAccumulator::default(),
        }
    }

    pub fn memory_limit(limit_mb: usize) -> Self {
        Self {
            columns: HashMap::new(),
            ordered_names: Vec::new(),
            memory_limit_bytes: limit_mb * 1024 * 1024,
            row_tracker: RowUniquenessTracker::default(),
            completeness_tracker: RowCompletenessTracker::default(),
            hint_bindings: ValueHintBindingAccumulator::default(),
        }
    }

    /// Configure value-driven semantic hints before records are processed.
    pub fn with_semantic_hints(mut self, hints: &SemanticHints) -> Self {
        self.hint_bindings = ValueHintBindingAccumulator::new(hints);
        self
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
        if prior_rows > 0 {
            self.completeness_tracker.invalidate_completed_rows();
        }
    }

    pub fn process_record<I>(&mut self, headers: &[String], values: I)
    where
        I: IntoIterator<Item = String>,
    {
        let mut row_signature = RowSignature::default();
        let mut row_has_null = false;
        let mut values = values.into_iter();

        // Headers define the row schema. Normalize a missing trailing field to
        // the profiler's empty/null representation so ragged flexible records
        // update every column and hash identically to an explicit empty field.
        for header in headers {
            let value = values.next().unwrap_or_default();
            row_signature.push_field(&value);

            if !self.columns.contains_key(header) {
                self.ordered_names.push(header.clone());
            }
            let stats = self.columns.entry(header.to_string()).or_default();
            stats.update(&value);
            // Same null definition the column counters use, so the record
            // count and the cell counts always describe the same nulls.
            row_has_null |= is_null_like_token(&value);
            self.hint_bindings.observe(header, &value);
        }

        if !headers.is_empty() {
            self.row_tracker.observe(row_signature.finish());
            self.completeness_tracker.observe(row_has_null);
        }
    }

    /// Full-stream row-duplicate counts, or `None` when no rows were seen.
    pub fn row_duplicate_summary(&self) -> Option<RowDuplicateSummary> {
        self.row_tracker.summary()
    }

    /// Full-stream complete-record counts, or `None` when no rows were seen.
    pub fn row_completeness_summary(&self) -> Option<RowCompletenessSummary> {
        self.completeness_tracker.summary()
    }

    /// Exact value-driven semantic-hint evidence over every processed record.
    pub fn semantic_hint_bindings(&self) -> Vec<SemanticHintBinding> {
        self.hint_bindings
            .bindings(self.ordered_names.iter().map(String::as_str))
    }

    pub fn get_column_stats(&self, column_name: &str) -> Option<&StreamingStatistics> {
        self.columns.get(column_name)
    }

    pub fn column_names(&self) -> Vec<String> {
        self.ordered_names.clone()
    }

    /// Retain only the selected columns, preserving their source order.
    ///
    /// Parsers call this after validating the selection against the complete
    /// source schema. Row-level trackers intentionally remain unchanged; a
    /// projected report withholds the row-level quality dimensions because
    /// their meaning changes when columns are removed.
    pub fn retain_columns(&mut self, names: &[String]) {
        let selected = names.iter().map(String::as_str).collect::<HashSet<_>>();
        self.columns
            .retain(|name, _| selected.contains(name.as_str()));
        self.ordered_names
            .retain(|name| selected.contains(name.as_str()));
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

    /// Fold another collection into this one, as if a single pass had read
    /// both sides.
    ///
    /// The merged profile describes the union of the two schemas, and three
    /// consequences of that are each a silently wrong number when skipped:
    ///
    /// - A column only one side saw has to join `ordered_names`, or
    ///   [`Self::column_names`] omits it and the column vanishes from the
    ///   report while its statistics sit unreachable in the map.
    /// - That column is absent from every record the *other* side read, so it
    ///   is padded with those rows as missing values, the way
    ///   [`Self::init_column_with_missing`] pads a key that appears mid-stream.
    ///   Skipping the padding leaves the column looking shorter and more
    ///   complete than the dataset.
    /// - Rows counted complete against the narrower schema are not complete
    ///   against the union, so that side's complete-row count is invalidated
    ///   for the same reason.
    ///
    /// Row signatures are built per record from that record's own headers, and
    /// encode values only. Two sides that read different columns, or the same
    /// columns in a different order, therefore sign different rows identically.
    /// Those signatures cannot be rebuilt against the union schema once folded
    /// into the estimator, so the merged duplicate count is withheld rather
    /// than reported wrong.
    pub fn merge(&mut self, other: StreamingColumnCollection) {
        let self_rows = self.row_tracker.rows_seen();
        let other_rows = other.row_tracker.rows_seen();

        let StreamingColumnCollection {
            columns,
            ordered_names,
            memory_limit_bytes: _,
            row_tracker,
            mut completeness_tracker,
            hint_bindings,
        } = other;

        // Signatures are positional over the record's own headers, so they
        // only mean the same thing on both sides when both read the same
        // columns in the same order. Captured before the orders are joined.
        let schemas_match = self.ordered_names == ordered_names;

        // A column this side saw and the other did not is absent from every
        // record the other side read.
        let mut other_lacks_columns = false;
        for (column_name, stats) in self.columns.iter_mut() {
            if columns.contains_key(column_name) {
                continue;
            }
            other_lacks_columns = true;
            stats.count += other_rows;
            stats.null_count += other_rows;
        }

        // Columns the other side introduces keep its discovery order, appended
        // after the ones already known here. Names carry the order; the map
        // carries the statistics, and the two always hold the same keys.
        let mut self_lacks_columns = false;
        for column_name in &ordered_names {
            if !self.columns.contains_key(column_name) {
                self.ordered_names.push(column_name.clone());
                self_lacks_columns = true;
            }
        }

        for (column_name, other_stats) in columns {
            match self.columns.get_mut(&column_name) {
                Some(existing_stats) => existing_stats.merge(&other_stats),
                None => {
                    let mut stats = other_stats;
                    // Records this side already read had no such field.
                    stats.count += self_rows;
                    stats.null_count += self_rows;
                    self.columns.insert(column_name, stats);
                }
            }
        }

        if self_lacks_columns && self_rows > 0 {
            self.completeness_tracker.invalidate_completed_rows();
        }
        if other_lacks_columns && other_rows > 0 {
            completeness_tracker.invalidate_completed_rows();
        }

        self.completeness_tracker.merge(&completeness_tracker);
        self.row_tracker.merge(&row_tracker);
        // Only a side that signed rows can make them incomparable, so folding
        // a partition into a fresh accumulator stays a plain accumulation.
        if self_rows > 0 && other_rows > 0 && !schemas_match {
            self.row_tracker.mark_incomparable();
        }
        self.hint_bindings.merge(&hint_bindings);
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

    fn completeness(collection: &StreamingColumnCollection) -> (usize, usize) {
        let summary = collection
            .row_completeness_summary()
            .expect("rows were observed");
        (summary.complete_rows, summary.rows_checked)
    }

    #[test]
    fn test_complete_rows_count_rows_not_null_cells() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut collection = StreamingColumnCollection::new();
        // Both nulls land in the same row, so 3 of 4 rows are complete. Per
        // column the nulls total 2, which the cell-based lower bound would
        // read as only 2 complete rows.
        record(&mut collection, &headers, &["", ""]);
        record(&mut collection, &headers, &["x", "1"]);
        record(&mut collection, &headers, &["y", "2"]);
        record(&mut collection, &headers, &["z", "3"]);

        assert_eq!(completeness(&collection), (3, 4));
    }

    #[test]
    fn test_null_like_tokens_make_a_row_incomplete() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut collection = StreamingColumnCollection::new();
        // The column counters treat these as nulls, so the record count must
        // too, or the two halves of the dimension describe different data.
        record(&mut collection, &headers, &["x", "NULL"]);
        record(&mut collection, &headers, &["y", "NaN"]);
        record(&mut collection, &headers, &["z", "  "]);
        record(&mut collection, &headers, &["w", "3"]);

        assert_eq!(completeness(&collection), (1, 4));
    }

    #[test]
    fn test_ragged_rows_are_incomplete() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut collection = StreamingColumnCollection::new();
        record(&mut collection, &headers, &["x"]);
        record(&mut collection, &headers, &["y", "1"]);

        assert_eq!(completeness(&collection), (1, 2));
    }

    #[test]
    fn test_a_late_column_makes_earlier_rows_incomplete() {
        let mut collection = StreamingColumnCollection::new();
        let first = vec!["a".to_string()];
        record(&mut collection, &first, &["x"]);
        record(&mut collection, &first, &["y"]);
        assert_eq!(completeness(&collection), (2, 2));

        // A JSON object introduces `b` on the third record. The first two
        // records never carried it, so neither of them was complete.
        collection.init_column_with_missing("b", 2);
        let both = vec!["a".to_string(), "b".to_string()];
        record(&mut collection, &both, &["z", "1"]);

        assert_eq!(completeness(&collection), (1, 3));
    }

    #[test]
    fn test_no_rows_means_no_completeness_summary() {
        let collection = StreamingColumnCollection::new();
        assert!(collection.row_completeness_summary().is_none());
    }

    #[test]
    fn test_aligned_columns_count_the_same_complete_rows() {
        let a: Vec<String> = ["", "x", "y"].iter().map(|v| v.to_string()).collect();
        let b: Vec<String> = ["", "1", "2"].iter().map(|v| v.to_string()).collect();
        let mut tracker = RowCompletenessTracker::default();
        tracker.observe_aligned_columns(&[a.as_slice(), b.as_slice()], 3);

        let summary = tracker.summary().expect("rows were observed");
        assert_eq!((summary.complete_rows, summary.rows_checked), (2, 3));
    }

    #[test]
    fn test_aligned_columns_treat_a_short_column_as_missing() {
        let a: Vec<String> = ["x", "y"].iter().map(|v| v.to_string()).collect();
        let b: Vec<String> = ["1"].iter().map(|v| v.to_string()).collect();
        let mut tracker = RowCompletenessTracker::default();
        tracker.observe_aligned_columns(&[a.as_slice(), b.as_slice()], 2);

        let summary = tracker.summary().expect("rows were observed");
        assert_eq!((summary.complete_rows, summary.rows_checked), (1, 2));
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
    fn test_trailing_null_like_token_is_not_trimmed() {
        // Only an exactly empty field is absent. A trailing "null" or " " is
        // a value the row holds, and `is_null_like_token` matches both;
        // trimming on that broader rule would collapse these rows against the
        // absent-field one and report duplicates that are not there.
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut collection = StreamingColumnCollection::new();
        record(&mut collection, &headers, &["x", "null"]);
        record(&mut collection, &headers, &["x", " "]);
        record(&mut collection, &headers, &["x", ""]);

        let summary = collection
            .row_duplicate_summary()
            .expect("rows were observed");
        assert_eq!(summary.duplicate_rows, 0);
    }

    #[test]
    fn test_late_column_does_not_split_identical_rows() {
        // A JSON key discovered mid-stream appends a column. Rows read before
        // and after that point are signed against header lists of different
        // lengths, and two identical records must still count as duplicates.
        let mut collection = StreamingColumnCollection::new();
        let short = vec!["a".to_string()];
        let wide = vec!["a".to_string(), "b".to_string()];

        record(&mut collection, &short, &["x"]);
        collection.init_column_with_missing("b", 1);
        record(&mut collection, &wide, &["x", "y"]);
        record(&mut collection, &wide, &["x", ""]);

        let summary = collection
            .row_duplicate_summary()
            .expect("rows were observed");
        assert_eq!(summary.rows_checked, 3);
        assert_eq!(
            summary.duplicate_rows, 1,
            "rows 1 and 3 hold the same data; they differ only in how many \
             columns had been discovered when each was read"
        );
    }

    #[test]
    fn test_late_column_in_the_middle_does_not_split_identical_rows() {
        // A second late key pushes the first one away from the end, so the
        // absent field sits in the middle of the later signatures.
        let mut collection = StreamingColumnCollection::new();
        let one = vec!["a".to_string()];
        let two = vec!["a".to_string(), "b".to_string()];
        let three = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        record(&mut collection, &one, &["1"]);
        collection.init_column_with_missing("b", 1);
        record(&mut collection, &two, &["1", "2"]);
        collection.init_column_with_missing("c", 2);
        record(&mut collection, &three, &["1", "", "3"]);
        record(&mut collection, &three, &["1", "", ""]);

        let summary = collection
            .row_duplicate_summary()
            .expect("rows were observed");
        assert_eq!(summary.rows_checked, 4);
        assert_eq!(summary.duplicate_rows, 1, "row 4 repeats row 1");
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

    #[test]
    fn test_merge_keeps_columns_only_the_other_side_saw() {
        let mut left = StreamingColumnCollection::new();
        let mut right = StreamingColumnCollection::new();
        record(&mut left, &["a".to_string()], &["1"]);
        record(&mut right, &["b".to_string()], &["2"]);

        left.merge(right);

        // A column reachable in the map but missing from the order is a column
        // the report never renders.
        assert_eq!(left.column_names(), vec!["a".to_string(), "b".to_string()]);
        assert!(left.get_column_stats("b").is_some());
    }

    #[test]
    fn test_merge_pads_columns_the_other_side_never_saw() {
        let left_headers = vec!["a".to_string()];
        let right_headers = vec!["a".to_string(), "b".to_string()];
        let mut left = StreamingColumnCollection::new();
        let mut right = StreamingColumnCollection::new();
        record(&mut left, &left_headers, &["1"]);
        record(&mut left, &left_headers, &["2"]);
        record(&mut right, &right_headers, &["3", "x"]);

        left.merge(right);

        let b = left.get_column_stats("b").expect("column b survived");
        // Three records in the merged dataset, two of which had no `b` at all.
        assert_eq!(b.count, 3);
        assert_eq!(b.null_count, 2);

        let a = left.get_column_stats("a").expect("column a");
        assert_eq!(a.count, 3);
        assert_eq!(a.null_count, 0);
    }

    #[test]
    fn test_merge_combines_completeness() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut left = StreamingColumnCollection::new();
        let mut right = StreamingColumnCollection::new();
        record(&mut left, &headers, &["1", "2"]);
        record(&mut left, &headers, &["3", ""]);
        record(&mut right, &headers, &["4", "5"]);

        left.merge(right);

        assert_eq!(completeness(&left), (2, 3));
    }

    #[test]
    fn test_merge_invalidates_completeness_for_the_narrower_side() {
        let left_headers = vec!["a".to_string()];
        let right_headers = vec!["a".to_string(), "b".to_string()];
        let mut left = StreamingColumnCollection::new();
        let mut right = StreamingColumnCollection::new();
        record(&mut left, &left_headers, &["1"]);
        record(&mut right, &right_headers, &["2", "y"]);

        left.merge(right);

        // The left record has no `b`, so it is not complete against the union.
        assert_eq!(completeness(&left), (1, 2));
    }

    #[test]
    fn test_merge_withholds_duplicates_when_schemas_differ() {
        let mut left = StreamingColumnCollection::new();
        let mut right = StreamingColumnCollection::new();
        record(&mut left, &["a".to_string()], &["x"]);
        record(&mut right, &["b".to_string()], &["x"]);

        left.merge(right);

        // Both rows sign as "1:x", so the count would claim one duplicate,
        // while under the union schema the rows differ in every field.
        assert!(left.row_duplicate_summary().is_none());
    }

    #[test]
    fn test_merge_withholds_duplicates_when_column_order_differs() {
        let mut left = StreamingColumnCollection::new();
        let mut right = StreamingColumnCollection::new();
        record(&mut left, &["a".to_string(), "b".to_string()], &["x", "y"]);
        record(&mut right, &["b".to_string(), "a".to_string()], &["x", "y"]);

        left.merge(right);

        // Same columns, opposite order: the two signatures collide on rows
        // that hold the values the other way round.
        assert!(left.row_duplicate_summary().is_none());
    }

    #[test]
    fn test_merge_keeps_duplicates_when_schemas_match() {
        let headers = vec!["a".to_string(), "b".to_string()];
        let mut left = StreamingColumnCollection::new();
        let mut right = StreamingColumnCollection::new();
        record(&mut left, &headers, &["x", "y"]);
        record(&mut right, &headers, &["x", "y"]);

        left.merge(right);

        let summary = left
            .row_duplicate_summary()
            .expect("one schema on both sides");
        assert_eq!(summary.rows_checked, 2);
        assert_eq!(summary.duplicate_rows, 1);
    }

    #[test]
    fn test_merge_into_an_empty_accumulator_keeps_duplicates() {
        let headers = vec!["a".to_string()];
        let mut accumulator = StreamingColumnCollection::new();
        let mut partition = StreamingColumnCollection::new();
        record(&mut partition, &headers, &["x"]);
        record(&mut partition, &headers, &["x"]);

        // A reduce starts from an empty accumulator. That is not a schema
        // mismatch: the empty side signed nothing to be confused with.
        accumulator.merge(partition);

        let summary = accumulator
            .row_duplicate_summary()
            .expect("the empty side contributed no signatures");
        assert_eq!(summary.rows_checked, 2);
        assert_eq!(summary.duplicate_rows, 1);
    }

    /// Merging partitions must produce the profile a single pass would.
    ///
    /// This is the acceptance criterion for any distributed profiling path:
    /// without it, the numbers silently depend on how the input was split.
    #[test]
    fn test_merged_partitions_match_a_single_pass() {
        let headers = vec!["id".to_string(), "city".to_string(), "score".to_string()];
        let rows: Vec<[String; 3]> = (0..300)
            .map(|index| {
                [
                    format!("{}", index % 10),
                    ["Rome", "Milan", "Turin", ""][index % 4].to_string(),
                    format!("{}.5", index % 5),
                ]
            })
            .collect();

        let mut single = StreamingColumnCollection::new();
        for row in &rows {
            single.process_record(&headers, row.iter().cloned());
        }

        // Seven partitions over 300 rows, so the split is uneven.
        let mut partitions: Vec<StreamingColumnCollection> =
            (0..7).map(|_| StreamingColumnCollection::new()).collect();
        for (index, row) in rows.iter().enumerate() {
            partitions[index % 7].process_record(&headers, row.iter().cloned());
        }
        let mut merged = partitions.remove(0);
        for partition in partitions {
            merged.merge(partition);
        }

        assert_eq!(merged.column_names(), single.column_names());
        for name in single.column_names() {
            let expected = single.get_column_stats(&name).expect("single-pass column");
            let actual = merged.get_column_stats(&name).expect("merged column");
            assert_eq!(actual.count, expected.count, "count for {name}");
            assert_eq!(actual.null_count, expected.null_count, "nulls for {name}");
            assert_eq!(
                actual.unique_count(),
                expected.unique_count(),
                "distinct for {name}"
            );
            assert_eq!(actual.min, expected.min, "min for {name}");
            assert_eq!(actual.max, expected.max, "max for {name}");
            assert!(
                (actual.mean() - expected.mean()).abs() < 1e-9,
                "mean for {name}"
            );
            assert!(
                (actual.variance() - expected.variance()).abs() < 1e-9,
                "variance for {name}"
            );
        }

        let merged_rows = merged.row_completeness_summary().expect("rows observed");
        let single_rows = single.row_completeness_summary().expect("rows observed");
        assert_eq!(merged_rows.rows_checked, single_rows.rows_checked);
        assert_eq!(merged_rows.complete_rows, single_rows.complete_rows);

        let merged_dupes = merged.row_duplicate_summary().expect("rows observed");
        let single_dupes = single.row_duplicate_summary().expect("rows observed");
        assert_eq!(merged_dupes.rows_checked, single_dupes.rows_checked);
        assert_eq!(merged_dupes.duplicate_rows, single_dupes.duplicate_rows);
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
    fn test_reservoir_merge_weights_sides_by_population() {
        let mut large = StreamReservoirSampler::new(100);
        for index in 0..10_000 {
            large.offer(format!("large-{index}"));
        }
        let mut small = StreamReservoirSampler::new(100);
        for index in 0..100 {
            small.offer(format!("small-{index}"));
        }

        large.merge(&small);

        assert_eq!(large.samples().len(), 100);
        let from_small = large
            .samples()
            .iter()
            .filter(|value| value.starts_with("small-"))
            .count();
        // The small side is 100 rows of 10,100, so it earns about one slot.
        // Pooling both reservoirs and subsampling uniformly would hand it
        // roughly half of them, making the sample depend on the partitioning.
        assert!(
            from_small <= 10,
            "small partition took {from_small} of 100 slots"
        );
    }

    #[test]
    fn test_reservoir_merge_keeps_every_value_when_capacity_allows() {
        let mut left = StreamReservoirSampler::new(100);
        left.offer("a".to_string());
        left.offer("b".to_string());
        let mut right = StreamReservoirSampler::new(100);
        right.offer("c".to_string());

        left.merge(&right);

        let mut samples: Vec<String> = left.samples().to_vec();
        samples.sort();
        assert_eq!(samples, vec!["a", "b", "c"]);
        assert_eq!(left.count, 3);
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
    fn test_reservoir_zero_capacity_still_retains_a_sample() {
        let mut sampler = StreamReservoirSampler::new(0);
        sampler.offer("value".to_string());

        assert_eq!(sampler.samples(), ["value"]);
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
