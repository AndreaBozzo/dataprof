//! Composable stop conditions for early termination of stream-based profiling.
//!
//! A [`StopCondition`] describes *when* to stop; a [`StopEvaluator`] is the
//! mutable runtime checker that tracks counters and evaluates the condition
//! after each chunk.

use crate::execution::TruncationReason;

/// A composable condition that can trigger early termination of profiling.
///
/// Conditions are checked per-chunk (not per-row) for performance.
/// The actual row count at termination may slightly exceed the limit.
#[derive(Debug, Clone, Default)]
pub enum StopCondition {
    /// Stop after processing this many rows.
    MaxRows(u64),
    /// Stop after consuming this many bytes from the source.
    MaxBytes(u64),
    /// Stop when column types have not changed for approximately N rows
    /// (accumulated across chunks).
    SchemaStable {
        /// Approximate number of rows with no type changes before stopping.
        /// Rows are accumulated per-chunk, so the actual count depends on
        /// chunk granularity.
        consecutive_stable_rows: u64,
    },
    /// Stop when `rows_processed / estimated_total >= threshold`.
    ///
    /// Only meaningful when an estimated total row count is available.
    /// When no estimate exists, this condition is inert.
    ConfidenceThreshold(f64),
    /// Stop when memory usage exceeds this fraction of the configured limit.
    ///
    /// Value in `0.0..=1.0` (e.g., `0.9` = 90% of memory limit).
    MemoryPressure(f64),
    /// Stop when **any** sub-condition triggers.
    Any(Vec<StopCondition>),
    /// Stop when **all** sub-conditions have triggered.
    All(Vec<StopCondition>),
    /// Never stop early — process the entire stream (default).
    #[default]
    Never,
}

impl StopCondition {
    /// Preset for schema-only profiling: stop after 10K rows or when schema
    /// stabilizes (no type changes for 1,000 consecutive rows).
    pub fn schema_inference() -> Self {
        StopCondition::Any(vec![
            StopCondition::MaxRows(10_000),
            StopCondition::SchemaStable {
                consecutive_stable_rows: 1_000,
            },
        ])
    }

    /// Preset for quality sampling: stop after 50K rows, 50 MB, or 95% confidence.
    pub fn quality_sample() -> Self {
        StopCondition::Any(vec![
            StopCondition::MaxRows(50_000),
            StopCondition::MaxBytes(50 * 1024 * 1024),
            StopCondition::ConfidenceThreshold(0.95),
        ])
    }

    /// The row count at which this condition can first trigger on rows alone,
    /// if any.
    ///
    /// Mirrors [`evaluate`]: `Any` fires as soon as one child fires, so it takes
    /// the *minimum* of the children that a row count can trigger. `All` fires
    /// only once every child has fired, so it takes the *maximum*, and yields
    /// `None` if any child cannot be triggered by rows at all (a byte cap,
    /// `Never`, …) — such a condition can never be satisfied by rows alone.
    ///
    /// Used two ways: parsers that can only enforce a row cap consult it after
    /// checking [`is_row_limit_only`](Self::is_row_limit_only), and the
    /// incremental engine uses it as a per-row guardrail alongside the real
    /// evaluator.
    pub fn max_rows(&self) -> Option<u64> {
        match self {
            StopCondition::MaxRows(n) => Some(*n),
            // Earliest child cap wins: reaching it fires the whole `Any`.
            StopCondition::Any(conditions) => {
                conditions.iter().filter_map(StopCondition::max_rows).min()
            }
            // Every child must fire, so rows alone suffice only if every child is
            // row-triggerable; then the last one to fire sets the bound.
            StopCondition::All(conditions) => {
                if conditions.is_empty() {
                    return None; // `evaluate` never fires on an empty `All`
                }
                conditions
                    .iter()
                    .map(StopCondition::max_rows)
                    .try_fold(0u64, |acc, cap| Some(acc.max(cap?)))
            }
            _ => None,
        }
    }

    /// Whether this condition is expressible purely as a row limit.
    ///
    /// `Never` never fires; a bare `MaxRows` is a row cap; a composite qualifies
    /// when every leaf is one of those. Anything else (byte caps, schema
    /// stability, confidence) needs a real evaluator, so a parser that can only
    /// cap rows must not silently accept it.
    pub fn is_row_limit_only(&self) -> bool {
        match self {
            StopCondition::Never | StopCondition::MaxRows(_) => true,
            StopCondition::Any(conditions) | StopCondition::All(conditions) => {
                conditions.iter().all(StopCondition::is_row_limit_only)
            }
            _ => false,
        }
    }
}

/// Runtime evaluator that checks a [`StopCondition`] against accumulated counters.
///
/// Create one before the processing loop and call [`update`](Self::update)
/// after each chunk. When it returns `true`, profiling should stop.
pub struct StopEvaluator {
    condition: StopCondition,
    rows_processed: u64,
    bytes_consumed: u64,
    estimated_total_rows: Option<u64>,
    triggered_reason: Option<TruncationReason>,
}

impl StopEvaluator {
    pub fn new(condition: StopCondition) -> Self {
        let condition = Self::clamp_thresholds(condition);
        Self {
            condition,
            rows_processed: 0,
            bytes_consumed: 0,
            estimated_total_rows: None,
            triggered_reason: None,
        }
    }

    /// Clamp `ConfidenceThreshold` and `MemoryPressure` values to `0.0..=1.0`,
    /// recursing into `Any`/`All` composites.
    fn clamp_thresholds(condition: StopCondition) -> StopCondition {
        match condition {
            StopCondition::ConfidenceThreshold(t) => {
                StopCondition::ConfidenceThreshold(t.clamp(0.0, 1.0))
            }
            StopCondition::MemoryPressure(t) => StopCondition::MemoryPressure(t.clamp(0.0, 1.0)),
            StopCondition::Any(cs) => {
                StopCondition::Any(cs.into_iter().map(Self::clamp_thresholds).collect())
            }
            StopCondition::All(cs) => {
                StopCondition::All(cs.into_iter().map(Self::clamp_thresholds).collect())
            }
            other => other,
        }
    }

    /// Provide an estimated total row count (enables `ConfidenceThreshold`).
    pub fn with_estimated_total(mut self, rows: u64) -> Self {
        self.estimated_total_rows = Some(rows);
        self
    }

    /// Update counters and evaluate the stop condition.
    ///
    /// Returns `true` when profiling should stop.
    ///
    /// - `chunk_rows`: number of rows in the chunk just processed
    /// - `chunk_bytes`: number of bytes consumed by this chunk
    /// - `memory_fraction`: current memory usage as a fraction of the limit (`0.0..1.0`)
    pub fn update(&mut self, chunk_rows: u64, chunk_bytes: u64, memory_fraction: f64) -> bool {
        self.rows_processed += chunk_rows;
        self.bytes_consumed += chunk_bytes;

        if self.triggered_reason.is_some() {
            return true;
        }

        let reason = evaluate(
            &self.condition,
            self.rows_processed,
            self.bytes_consumed,
            memory_fraction,
            self.estimated_total_rows,
        );

        if reason.is_some() {
            self.triggered_reason = reason;
            true
        } else {
            false
        }
    }

    /// Returns `true` if a stop condition has already been triggered.
    pub fn should_stop(&self) -> bool {
        self.triggered_reason.is_some()
    }

    /// The reason profiling stopped, mapped to [`TruncationReason`].
    pub fn truncation_reason(&self) -> Option<TruncationReason> {
        self.triggered_reason.clone()
    }

    /// Total rows processed so far.
    pub fn rows_processed(&self) -> u64 {
        self.rows_processed
    }

    /// Total bytes consumed so far.
    pub fn bytes_consumed(&self) -> u64 {
        self.bytes_consumed
    }
}

/// Recursively evaluate a condition against current counters.
/// Returns `Some(reason)` if the condition is met.
fn evaluate(
    condition: &StopCondition,
    rows: u64,
    bytes: u64,
    memory_fraction: f64,
    estimated_total: Option<u64>,
) -> Option<TruncationReason> {
    match condition {
        StopCondition::MaxRows(limit) => {
            if rows >= *limit {
                Some(TruncationReason::MaxRows(*limit))
            } else {
                None
            }
        }
        StopCondition::MaxBytes(limit) => {
            if bytes >= *limit {
                Some(TruncationReason::MaxBytes(*limit))
            } else {
                None
            }
        }
        StopCondition::SchemaStable { .. } => {
            // SchemaStable requires column type tracking which is handled
            // at the engine level via `SchemaStabilityTracker`. The evaluator
            // alone cannot detect schema changes.
            // See: IncrementalProfiler and AsyncStreamingProfiler integration.
            None
        }
        StopCondition::ConfidenceThreshold(threshold) => {
            if let Some(total) = estimated_total
                && total > 0
            {
                let confidence = rows as f64 / total as f64;
                if confidence >= *threshold {
                    return Some(TruncationReason::StopCondition(format!(
                        "confidence_threshold({})",
                        threshold
                    )));
                }
            }
            None
        }
        StopCondition::MemoryPressure(threshold) => {
            if memory_fraction >= *threshold {
                Some(TruncationReason::MemoryPressure)
            } else {
                None
            }
        }
        StopCondition::Any(conditions) => {
            for c in conditions {
                if let Some(reason) = evaluate(c, rows, bytes, memory_fraction, estimated_total) {
                    return Some(reason);
                }
            }
            None
        }
        StopCondition::All(conditions) => {
            if conditions.is_empty() {
                return None;
            }
            // All must have triggered — collect reasons, return first
            let mut first_reason = None;
            for c in conditions {
                let reason = evaluate(c, rows, bytes, memory_fraction, estimated_total)?;
                if first_reason.is_none() {
                    first_reason = Some(reason);
                }
            }
            first_reason
        }
        StopCondition::Never => None,
    }
}

/// Extracts the `consecutive_stable_rows` threshold from a `StopCondition`,
/// searching through `Any`/`All` composites. Returns `None` if no
/// `SchemaStable` variant is present.
pub fn schema_stable_threshold(condition: &StopCondition) -> Option<u64> {
    match condition {
        StopCondition::SchemaStable {
            consecutive_stable_rows,
        } => Some(*consecutive_stable_rows),
        StopCondition::Any(conditions) | StopCondition::All(conditions) => {
            conditions.iter().find_map(schema_stable_threshold)
        }
        _ => None,
    }
}

/// Tracks consecutive rows where the schema (column types) has not changed.
/// Used by engines to implement `SchemaStable` stop conditions.
///
/// Accepts a fingerprint (hash) of the column types and accumulates rows
/// across chunks. When the accumulated stable-row count reaches the
/// threshold, the tracker signals that profiling may stop.
pub struct SchemaStabilityTracker {
    threshold: u64,
    consecutive_stable: u64,
    last_fingerprint: Option<u64>,
}

impl SchemaStabilityTracker {
    /// Create a tracker for the given threshold. Returns `None` if no
    /// `SchemaStable` condition is present.
    pub fn from_condition(condition: &StopCondition) -> Option<Self> {
        schema_stable_threshold(condition).map(|threshold| Self {
            threshold,
            consecutive_stable: 0,
            last_fingerprint: None,
        })
    }

    /// Update with the current schema fingerprint and the number of rows in
    /// the chunk. Returns `true` when the accumulated stable-row count reaches
    /// the threshold.
    pub fn update(&mut self, fingerprint: u64, chunk_rows: u64) -> bool {
        match self.last_fingerprint {
            Some(prev) if prev == fingerprint => {
                self.consecutive_stable += chunk_rows;
            }
            _ => {
                self.consecutive_stable = chunk_rows;
                self.last_fingerprint = Some(fingerprint);
            }
        }
        self.consecutive_stable >= self.threshold
    }

    /// The truncation reason when schema stability is reached.
    pub fn truncation_reason(&self) -> TruncationReason {
        TruncationReason::StopCondition(format!("schema_stable({})", self.threshold))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_rows_leaf_and_never() {
        assert_eq!(StopCondition::MaxRows(10).max_rows(), Some(10));
        assert_eq!(StopCondition::Never.max_rows(), None);
        assert_eq!(StopCondition::MaxBytes(10).max_rows(), None);
    }

    #[test]
    fn test_max_rows_any_takes_the_earliest_cap() {
        // `Any` fires as soon as one child fires.
        let condition =
            StopCondition::Any(vec![StopCondition::MaxRows(20), StopCondition::MaxRows(10)]);
        assert_eq!(condition.max_rows(), Some(10));

        // Order must not matter.
        let flipped =
            StopCondition::Any(vec![StopCondition::MaxRows(10), StopCondition::MaxRows(20)]);
        assert_eq!(flipped.max_rows(), Some(10));

        // `Never` can never fire, so it does not constrain an `Any`.
        let with_never = StopCondition::Any(vec![StopCondition::Never, StopCondition::MaxRows(10)]);
        assert_eq!(with_never.max_rows(), Some(10));

        // A row cap still fires the `Any` even beside a condition rows cannot trigger.
        let mixed = StopCondition::Any(vec![
            StopCondition::MaxBytes(1024),
            StopCondition::MaxRows(10),
        ]);
        assert_eq!(mixed.max_rows(), Some(10));
    }

    #[test]
    fn test_max_rows_all_needs_every_child_row_triggerable() {
        // `All` fires only once every child has fired: the last cap bounds it.
        let condition =
            StopCondition::All(vec![StopCondition::MaxRows(10), StopCondition::MaxRows(20)]);
        assert_eq!(condition.max_rows(), Some(20));

        // A child rows cannot trigger means rows alone can never satisfy the `All`.
        let with_bytes = StopCondition::All(vec![
            StopCondition::MaxRows(10),
            StopCondition::MaxBytes(1024),
        ]);
        assert_eq!(with_bytes.max_rows(), None);

        let with_never = StopCondition::All(vec![StopCondition::MaxRows(10), StopCondition::Never]);
        assert_eq!(with_never.max_rows(), None);

        // `evaluate` never fires on an empty `All`.
        assert_eq!(StopCondition::All(vec![]).max_rows(), None);
        assert_eq!(StopCondition::Any(vec![]).max_rows(), None);
    }

    #[test]
    fn test_is_row_limit_only_recurses() {
        assert!(StopCondition::Never.is_row_limit_only());
        assert!(StopCondition::MaxRows(10).is_row_limit_only());
        assert!(!StopCondition::MaxBytes(10).is_row_limit_only());

        // A composite of pure row caps stays expressible as a row cap.
        assert!(
            StopCondition::Any(vec![StopCondition::MaxRows(10), StopCondition::MaxRows(20)])
                .is_row_limit_only()
        );
        assert!(
            StopCondition::All(vec![StopCondition::Never, StopCondition::MaxRows(20)])
                .is_row_limit_only()
        );

        // One non-row leaf disqualifies the whole tree.
        assert!(
            !StopCondition::Any(vec![
                StopCondition::MaxRows(10),
                StopCondition::MaxBytes(1024)
            ])
            .is_row_limit_only()
        );

        // The `schema_inference` preset mixes MaxRows with SchemaStable.
        assert!(!StopCondition::schema_inference().is_row_limit_only());
        // …but a row cap alone still fires its `Any`, so the guardrail holds.
        assert_eq!(StopCondition::schema_inference().max_rows(), Some(10_000));
    }

    #[test]
    fn test_max_rows_stops() {
        let mut eval = StopEvaluator::new(StopCondition::MaxRows(100));
        assert!(!eval.update(50, 0, 0.0));
        assert!(!eval.should_stop());
        assert!(eval.update(50, 0, 0.0));
        assert!(eval.should_stop());
        assert_eq!(
            eval.truncation_reason(),
            Some(TruncationReason::MaxRows(100))
        );
    }

    #[test]
    fn test_max_bytes_stops() {
        let mut eval = StopEvaluator::new(StopCondition::MaxBytes(1000));
        assert!(!eval.update(10, 500, 0.0));
        assert!(eval.update(10, 600, 0.0));
        assert_eq!(
            eval.truncation_reason(),
            Some(TruncationReason::MaxBytes(1000))
        );
    }

    #[test]
    fn test_memory_pressure_stops() {
        let mut eval = StopEvaluator::new(StopCondition::MemoryPressure(0.9));
        assert!(!eval.update(100, 0, 0.5));
        assert!(eval.update(100, 0, 0.95));
        assert_eq!(
            eval.truncation_reason(),
            Some(TruncationReason::MemoryPressure)
        );
    }

    #[test]
    fn test_confidence_threshold_without_estimate() {
        // Without estimated_total, ConfidenceThreshold is inert
        let mut eval = StopEvaluator::new(StopCondition::ConfidenceThreshold(0.95));
        assert!(!eval.update(100_000, 0, 0.0));
        assert!(!eval.should_stop());
    }

    #[test]
    fn test_confidence_threshold_with_estimate() {
        let mut eval =
            StopEvaluator::new(StopCondition::ConfidenceThreshold(0.95)).with_estimated_total(1000);
        assert!(!eval.update(900, 0, 0.0));
        assert!(eval.update(100, 0, 0.0)); // 1000/1000 = 1.0 >= 0.95
    }

    #[test]
    fn test_never_never_stops() {
        let mut eval = StopEvaluator::new(StopCondition::Never);
        for _ in 0..100 {
            assert!(!eval.update(1_000_000, 1_000_000, 1.0));
        }
        assert!(!eval.should_stop());
    }

    #[test]
    fn test_any_stops_on_first() {
        let condition = StopCondition::Any(vec![
            StopCondition::MaxRows(100),
            StopCondition::MaxBytes(1_000_000),
        ]);
        let mut eval = StopEvaluator::new(condition);
        // MaxRows triggers first (100 rows, only 500 bytes)
        assert!(eval.update(100, 500, 0.0));
        assert_eq!(
            eval.truncation_reason(),
            Some(TruncationReason::MaxRows(100))
        );
    }

    #[test]
    fn test_all_requires_all() {
        let condition = StopCondition::All(vec![
            StopCondition::MaxRows(100),
            StopCondition::MaxBytes(1000),
        ]);
        let mut eval = StopEvaluator::new(condition);
        // Only rows met, bytes not yet
        assert!(!eval.update(100, 500, 0.0));
        // Now both met
        assert!(eval.update(0, 600, 0.0));
        assert_eq!(
            eval.truncation_reason(),
            Some(TruncationReason::MaxRows(100))
        );
    }

    #[test]
    fn test_all_empty_never_triggers() {
        let mut eval = StopEvaluator::new(StopCondition::All(vec![]));
        assert!(!eval.update(100, 100, 1.0));
    }

    #[test]
    fn test_convenience_schema_inference() {
        let condition = StopCondition::schema_inference();
        match &condition {
            StopCondition::Any(conditions) => {
                assert_eq!(conditions.len(), 2);
                assert!(matches!(conditions[0], StopCondition::MaxRows(10_000)));
                assert!(matches!(
                    conditions[1],
                    StopCondition::SchemaStable {
                        consecutive_stable_rows: 1_000
                    }
                ));
            }
            _ => panic!("Expected Any variant"),
        }
    }

    #[test]
    fn test_convenience_quality_sample() {
        let condition = StopCondition::quality_sample();
        match &condition {
            StopCondition::Any(conditions) => {
                assert_eq!(conditions.len(), 3);
                assert!(matches!(conditions[0], StopCondition::MaxRows(50_000)));
                assert!(matches!(conditions[1], StopCondition::MaxBytes(52_428_800)));
            }
            _ => panic!("Expected Any variant"),
        }
    }

    #[test]
    fn test_once_triggered_stays_triggered() {
        let mut eval = StopEvaluator::new(StopCondition::MaxRows(10));
        assert!(eval.update(10, 0, 0.0));
        // Calling update again still returns true
        assert!(eval.update(5, 0, 0.0));
        assert!(eval.should_stop());
    }

    #[test]
    fn test_schema_stability_tracker() {
        let condition = StopCondition::SchemaStable {
            consecutive_stable_rows: 3,
        };
        let mut tracker = SchemaStabilityTracker::from_condition(&condition).unwrap();

        let fp: u64 = 0xABCD;
        assert!(!tracker.update(fp, 1)); // consecutive = 1
        assert!(!tracker.update(fp, 1)); // consecutive = 2
        assert!(tracker.update(fp, 1)); // consecutive = 3 -> triggers
    }

    #[test]
    fn test_schema_stability_tracker_resets_on_change() {
        let condition = StopCondition::SchemaStable {
            consecutive_stable_rows: 3,
        };
        let mut tracker = SchemaStabilityTracker::from_condition(&condition).unwrap();

        let fp1: u64 = 0x1111;
        let fp2: u64 = 0x2222;

        assert!(!tracker.update(fp1, 1)); // consecutive = 1
        assert!(!tracker.update(fp1, 1)); // consecutive = 2
        // Schema changes - counter resets
        assert!(!tracker.update(fp2, 1)); // consecutive = 1
        assert!(!tracker.update(fp2, 1)); // consecutive = 2
        assert!(tracker.update(fp2, 1)); // consecutive = 3 -> triggers
    }

    #[test]
    fn test_schema_stable_threshold_extraction() {
        assert_eq!(schema_stable_threshold(&StopCondition::Never), None);
        assert_eq!(
            schema_stable_threshold(&StopCondition::SchemaStable {
                consecutive_stable_rows: 500
            }),
            Some(500)
        );
        // Nested in Any
        let nested = StopCondition::Any(vec![
            StopCondition::MaxRows(100),
            StopCondition::SchemaStable {
                consecutive_stable_rows: 200,
            },
        ]);
        assert_eq!(schema_stable_threshold(&nested), Some(200));
    }

    #[test]
    fn test_rows_and_bytes_accessors() {
        let mut eval = StopEvaluator::new(StopCondition::Never);
        eval.update(100, 500, 0.0);
        eval.update(200, 1000, 0.0);
        assert_eq!(eval.rows_processed(), 300);
        assert_eq!(eval.bytes_consumed(), 1500);
    }
}
