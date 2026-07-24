//! The runtime that applies a [`SamplingStrategy`] to a stream of rows.
//!
//! Strategies fall into two families, and the difference is not cosmetic:
//!
//! * **Streaming** strategies (`Systematic`, `Stratified`, `Progressive`,
//!   `Importance`) decide each row on the spot, so a row can be folded into the
//!   running statistics immediately and memory stays bounded.
//! * **Fixed-size** strategies (`Reservoir`, `Random`) cannot. Whether row 5
//!   belongs in a uniform sample of 10 is not known until the stream ends —
//!   row 5 may be evicted at row 900. Streaming statistics are not retractable,
//!   so a row that was folded in and later evicted would silently corrupt the
//!   profile. These strategies therefore buffer the candidate rows and hand the
//!   final sample back at end of stream, costing `size` rows of memory.
//!
//! [`RowSampler`] hides that split behind one interface so every engine treats
//! sampling identically. Consult [`RowSampler::is_buffered`] to know which of
//! [`RowSampler::accept`] or [`RowSampler::offer`] to call.

use crate::errors::DataProfilerError;

use super::reservoir::ReservoirSampler;
use super::strategies::{SamplingState, SamplingStrategy};

/// A borrowed view of one row, addressable by column name.
///
/// Sampling reads at most a couple of columns per row, so this scans the header
/// slice rather than building a map — allocating a `HashMap` per row would cost
/// far more than the lookup it saves.
#[derive(Clone, Copy)]
pub struct RowView<'a> {
    headers: &'a [String],
    values: &'a [String],
}

impl<'a> RowView<'a> {
    pub fn new(headers: &'a [String], values: &'a [String]) -> Self {
        Self { headers, values }
    }

    /// The value of `column`, or `None` when the row has no such field.
    pub fn get(&self, column: &str) -> Option<&'a str> {
        let index = self.headers.iter().position(|h| h == column)?;
        self.values.get(index).map(String::as_str)
    }

    pub fn headers(&self) -> &'a [String] {
        self.headers
    }

    pub fn values(&self) -> &'a [String] {
        self.values
    }
}

/// Running mean and variance over one column, used by `Progressive` to measure
/// how precise the sample has become. Welford's method: numerically stable and
/// single-pass, so precision can be checked after every row without a rescan.
#[derive(Debug, Default, Clone)]
struct RunningMoments {
    count: u64,
    mean: f64,
    m2: f64,
}

impl RunningMoments {
    fn push(&mut self, value: f64) {
        self.count += 1;
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (value - self.mean);
    }

    /// Relative standard error of the mean: `stderr / |mean|`.
    ///
    /// `None` until there are enough observations to have a variance, or when
    /// the mean sits at zero and a *relative* error is undefined.
    fn relative_standard_error(&self) -> Option<f64> {
        if self.count < 2 {
            return None;
        }
        let variance = self.m2 / (self.count - 1) as f64;
        let standard_error = (variance / self.count as f64).sqrt();
        let mean_magnitude = self.mean.abs();
        if mean_magnitude <= f64::EPSILON {
            return None;
        }
        Some(standard_error / mean_magnitude)
    }
}

/// Precision tracker behind the `Progressive` strategy.
#[derive(Debug, Default)]
struct PrecisionTracker {
    /// Moments per column position, for columns seen holding numbers.
    columns: Vec<Option<RunningMoments>>,
    numeric_columns: usize,
}

impl PrecisionTracker {
    fn observe(&mut self, row: RowView<'_>) {
        if self.columns.len() < row.values().len() {
            self.columns.resize(row.values().len(), None);
        }
        for (index, raw) in row.values().iter().enumerate() {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                continue;
            }
            // A column counts as numeric once a value parses; a later
            // non-numeric value does not retract that, it is simply skipped.
            let Ok(value) = trimmed.parse::<f64>() else {
                continue;
            };
            if !value.is_finite() {
                continue;
            }
            let slot = &mut self.columns[index];
            if slot.is_none() {
                *slot = Some(RunningMoments::default());
                self.numeric_columns += 1;
            }
            // decode-audit: impossible — the slot was just populated above.
            slot.as_mut()
                .expect("numeric column slot is present")
                .push(value);
        }
    }

    /// Whether every numeric column has reached the target relative standard
    /// error. `false` when nothing numeric has been seen: precision cannot be
    /// claimed for data it could not measure.
    fn meets_target(&self, target: f64) -> bool {
        if self.numeric_columns == 0 {
            return false;
        }
        self.columns
            .iter()
            .flatten()
            .all(|moments| match moments.relative_standard_error() {
                Some(rse) => rse <= target,
                // Not yet measurable — not yet precise enough.
                None => false,
            })
    }
}

/// A fixed-size uniform sample held in memory until the stream ends.
#[derive(Debug)]
struct SampleBuffer {
    capacity: usize,
    rows: Vec<Vec<String>>,
    sampler: ReservoirSampler,
    seen: usize,
}

impl SampleBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            rows: Vec::new(),
            sampler: ReservoirSampler::new(capacity),
            seen: 0,
        }
    }

    /// Offer a row to the sample, replacing an existing member if selected.
    ///
    /// Algorithm R: the first `capacity` rows fill the reservoir, after which
    /// row `n` replaces a uniformly chosen member with probability
    /// `capacity / n`. Every row of the stream ends up equally likely to be in
    /// the final sample.
    fn offer(&mut self, values: Vec<String>) {
        self.seen += 1;
        if self.capacity == 0 {
            return;
        }
        if self.rows.len() < self.capacity {
            self.rows.push(values);
            return;
        }
        if let Some(position) = self.sampler.replacement_slot(self.seen) {
            self.rows[position] = values;
        }
    }

    fn take(&mut self) -> Vec<Vec<String>> {
        std::mem::take(&mut self.rows)
    }
}

/// Applies a [`SamplingStrategy`] to a stream of rows, holding all the state
/// the strategy needs across rows.
///
/// A fresh state per row — which is what calling a stateless helper amounts to
/// — silently disables every stateful strategy, so engines must create one
/// sampler per scan and keep it for the whole scan.
#[derive(Debug)]
pub struct RowSampler {
    /// Streaming filters, applied in order; a row must pass all of them.
    filters: Vec<SamplingStrategy>,
    /// Terminal fixed-size stage, if the strategy has one.
    buffer: Option<SampleBuffer>,
    state: SamplingState,
    precision: PrecisionTracker,
    /// Rows offered to the sampler, whether or not they were kept.
    iterated: usize,
    /// Rows the sampler accepted for immediate folding (streaming path only).
    accepted: usize,
}

impl RowSampler {
    /// Build a sampler for `strategy`, validating that it can be applied.
    ///
    /// Rejects here rather than mid-scan: a caller learns that a strategy is
    /// unusable before the source is read, not after a partial profile exists.
    pub fn new(strategy: &SamplingStrategy) -> Result<Self, DataProfilerError> {
        let mut filters = Vec::new();
        let mut buffer = None;
        Self::flatten(strategy, &mut filters, &mut buffer)?;

        Ok(Self {
            filters,
            buffer,
            state: SamplingState::new(),
            precision: PrecisionTracker::default(),
            iterated: 0,
            accepted: 0,
        })
    }

    /// Split a (possibly multi-stage) strategy into streaming filters and at
    /// most one terminal fixed-size stage.
    fn flatten(
        strategy: &SamplingStrategy,
        filters: &mut Vec<SamplingStrategy>,
        buffer: &mut Option<SampleBuffer>,
    ) -> Result<(), DataProfilerError> {
        match strategy {
            SamplingStrategy::None => Ok(()),
            SamplingStrategy::Reservoir { size } | SamplingStrategy::Random { size } => {
                if buffer.is_some() {
                    // Two fixed-size stages have no combined meaning: each wants
                    // to define the final sample.
                    return Err(DataProfilerError::InvalidConfiguration {
                        message: "a multi-stage strategy may contain at most one fixed-size stage \
                                  (random or reservoir)"
                            .to_string(),
                        suggestion: "Keep a single fixed-size stage and express the rest as \
                                     filters, e.g. multi_stage([systematic(10), reservoir(1000)])."
                            .to_string(),
                    });
                }
                *buffer = Some(SampleBuffer::new(*size));
                Ok(())
            }
            SamplingStrategy::MultiStage { stages } => {
                for stage in stages {
                    // A filter after the fixed-size stage would have nothing to
                    // filter: the sample is only final once the source ends.
                    if buffer.is_some() && !Self::is_fixed_size(stage) {
                        return Err(DataProfilerError::InvalidConfiguration {
                            message: "a fixed-size stage (random or reservoir) must be the last \
                                      stage of a multi-stage strategy"
                                .to_string(),
                            suggestion: "Reorder the stages so filters such as systematic or \
                                         stratified come first, e.g. \
                                         multi_stage([systematic(10), reservoir(1000)])."
                                .to_string(),
                        });
                    }
                    Self::flatten(stage, filters, buffer)?;
                }
                Ok(())
            }
            other => {
                filters.push(other.clone());
                Ok(())
            }
        }
    }

    /// Whether a stage draws a fixed-size sample, and so must come last.
    fn is_fixed_size(strategy: &SamplingStrategy) -> bool {
        match strategy {
            SamplingStrategy::Reservoir { .. } | SamplingStrategy::Random { .. } => true,
            SamplingStrategy::MultiStage { stages } => stages.iter().any(Self::is_fixed_size),
            _ => false,
        }
    }

    /// Whether rows must be handed to [`offer`](Self::offer) instead of being
    /// folded in as [`accept`](Self::accept) approves them.
    pub fn is_buffered(&self) -> bool {
        self.buffer.is_some()
    }

    /// Whether this sampler can ever exclude a row.
    pub fn is_noop(&self) -> bool {
        self.filters.is_empty() && self.buffer.is_none()
    }

    /// Decide whether a row passes the streaming filters.
    ///
    /// Always call this, including on the buffered path, so the filters of a
    /// multi-stage strategy run before the fixed-size stage sees a row.
    pub fn accept(&mut self, row: RowView<'_>) -> bool {
        self.iterated += 1;
        let index = self.iterated - 1;

        for filter in &self.filters {
            if !Self::passes(filter, index, row, &mut self.state, &mut self.precision) {
                return false;
            }
        }

        if self.buffer.is_none() {
            self.accepted += 1;
        }
        true
    }

    /// Hand a row that passed [`accept`](Self::accept) to the fixed-size stage.
    ///
    /// Only meaningful when [`is_buffered`](Self::is_buffered) is true.
    pub fn offer(&mut self, values: Vec<String>) {
        if let Some(buffer) = self.buffer.as_mut() {
            buffer.offer(values);
        }
    }

    /// The final sample from the fixed-size stage, ready to fold into the
    /// statistics. Empty for a purely streaming strategy.
    pub fn take_sample(&mut self) -> Vec<Vec<String>> {
        match self.buffer.as_mut() {
            Some(buffer) => {
                let rows = buffer.take();
                self.accepted += rows.len();
                rows
            }
            None => Vec::new(),
        }
    }

    /// Rows the sampler has seen, sampled or not.
    pub fn iterated_rows(&self) -> usize {
        self.iterated
    }

    /// Rows that ended up in the sample.
    pub fn sampled_rows(&self) -> usize {
        self.accepted
    }

    fn passes(
        filter: &SamplingStrategy,
        index: usize,
        row: RowView<'_>,
        state: &mut SamplingState,
        precision: &mut PrecisionTracker,
    ) -> bool {
        match filter {
            #[allow(clippy::manual_is_multiple_of)]
            SamplingStrategy::Systematic { interval } => {
                if *interval == 0 {
                    return true;
                }
                index % interval == 0
            }
            SamplingStrategy::Stratified {
                key_columns,
                samples_per_stratum,
            } => state.take_from_stratum(row, key_columns, *samples_per_stratum),
            SamplingStrategy::Importance {
                weight_column,
                weight_threshold,
            } => match row.get(weight_column) {
                // A row whose weight is missing or unparseable has no stated
                // importance, so it is not important enough to keep.
                Some(raw) => matches!(raw.trim().parse::<f64>(), Ok(w) if w >= *weight_threshold),
                None => false,
            },
            SamplingStrategy::Progressive {
                initial_size,
                confidence_level,
                max_size,
            } => {
                let taken = state.progressive_taken();
                if taken >= *max_size {
                    return false;
                }
                if taken >= *initial_size && precision.meets_target(1.0 - confidence_level) {
                    return false;
                }
                precision.observe(row);
                state.record_progressive();
                true
            }
            // Fixed-size stages never reach here; `None` and `MultiStage` are
            // flattened away by `flatten`.
            _ => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> Vec<String> {
        vec!["group".into(), "weight".into(), "value".into()]
    }

    fn row(group: &str, weight: &str, value: &str) -> Vec<String> {
        vec![group.into(), weight.into(), value.into()]
    }

    fn run(strategy: SamplingStrategy, rows: usize) -> (usize, usize) {
        let headers = headers();
        let mut sampler = RowSampler::new(&strategy).expect("valid strategy");
        for i in 0..rows {
            let values = row(
                &format!("g{}", i % 3),
                &format!("{}", i % 10),
                &format!("{}", 100 + (i % 7)),
            );
            let view = RowView::new(&headers, &values);
            if sampler.accept(view) && sampler.is_buffered() {
                sampler.offer(values);
            }
        }
        let final_rows = sampler.take_sample().len();
        (sampler.sampled_rows(), final_rows)
    }

    #[test]
    fn reservoir_yields_exactly_its_size() {
        let (sampled, buffered) = run(SamplingStrategy::Reservoir { size: 10 }, 100);
        assert_eq!(buffered, 10);
        assert_eq!(sampled, 10);
    }

    #[test]
    fn reservoir_smaller_than_its_size_keeps_every_row() {
        let (sampled, buffered) = run(SamplingStrategy::Reservoir { size: 50 }, 20);
        assert_eq!(buffered, 20, "a short stream cannot fill the reservoir");
        assert_eq!(sampled, 20);
    }

    #[test]
    fn random_matches_reservoir_semantics() {
        let (_, buffered) = run(SamplingStrategy::Random { size: 25 }, 500);
        assert_eq!(buffered, 25);
    }

    #[test]
    fn systematic_takes_every_nth_row() {
        let (sampled, _) = run(SamplingStrategy::Systematic { interval: 10 }, 100);
        assert_eq!(sampled, 10);
    }

    #[test]
    fn stratified_caps_each_stratum() {
        let (sampled, _) = run(
            SamplingStrategy::Stratified {
                key_columns: vec!["group".into()],
                samples_per_stratum: 2,
            },
            100,
        );
        // Three distinct groups, two rows each.
        assert_eq!(sampled, 6);
    }

    #[test]
    fn importance_keeps_rows_at_or_above_the_weight() {
        let (sampled, _) = run(
            SamplingStrategy::Importance {
                weight_column: "weight".into(),
                weight_threshold: 8.0,
            },
            100,
        );
        // weight cycles 0..9, so 8 and 9 qualify: 20 of 100.
        assert_eq!(sampled, 20);
    }

    #[test]
    fn importance_on_a_missing_column_keeps_nothing() {
        let (sampled, _) = run(
            SamplingStrategy::Importance {
                weight_column: "absent".into(),
                weight_threshold: 0.0,
            },
            50,
        );
        assert_eq!(sampled, 0);
    }

    #[test]
    fn progressive_stays_within_its_bounds() {
        let (sampled, _) = run(
            SamplingStrategy::Progressive {
                initial_size: 5,
                confidence_level: 0.95,
                max_size: 40,
            },
            500,
        );
        assert!(
            (5..=40).contains(&sampled),
            "progressive took {sampled} rows, outside 5..=40"
        );
    }

    #[test]
    fn progressive_stops_once_precision_is_reached() {
        // A constant column has zero variance, so its relative standard error
        // is 0 immediately: the strategy must stop at initial_size rather than
        // run to max_size.
        let headers = vec!["value".to_string()];
        let strategy = SamplingStrategy::Progressive {
            initial_size: 10,
            confidence_level: 0.95,
            max_size: 1_000,
        };
        let mut sampler = RowSampler::new(&strategy).unwrap();
        for _ in 0..500 {
            let values = vec!["42".to_string()];
            sampler.accept(RowView::new(&headers, &values));
        }
        assert_eq!(
            sampler.sampled_rows(),
            10,
            "zero-variance data reaches any precision target at initial_size"
        );
    }

    #[test]
    fn progressive_without_numeric_columns_runs_to_max_size() {
        let headers = vec!["label".to_string()];
        let strategy = SamplingStrategy::Progressive {
            initial_size: 5,
            confidence_level: 0.95,
            max_size: 30,
        };
        let mut sampler = RowSampler::new(&strategy).unwrap();
        for i in 0..500 {
            let values = vec![format!("text_{i}")];
            sampler.accept(RowView::new(&headers, &values));
        }
        assert_eq!(
            sampler.sampled_rows(),
            30,
            "precision is unmeasurable without numbers, so the cap decides"
        );
    }

    #[test]
    fn multi_stage_applies_the_filter_then_the_fixed_size_stage() {
        let strategy = SamplingStrategy::MultiStage {
            stages: vec![
                SamplingStrategy::Systematic { interval: 2 },
                SamplingStrategy::Reservoir { size: 10 },
            ],
        };
        let (sampled, buffered) = run(strategy, 100);
        assert_eq!(buffered, 10, "the reservoir bounds the final sample");
        assert_eq!(sampled, 10);
    }

    #[test]
    fn multi_stage_rejects_two_fixed_size_stages() {
        let strategy = SamplingStrategy::MultiStage {
            stages: vec![
                SamplingStrategy::Reservoir { size: 10 },
                SamplingStrategy::Random { size: 5 },
            ],
        };
        let error = RowSampler::new(&strategy).expect_err("two fixed-size stages are ambiguous");
        assert!(error.to_string().contains("at most one fixed-size stage"));
    }

    #[test]
    fn multi_stage_rejects_a_filter_after_a_fixed_size_stage() {
        let strategy = SamplingStrategy::MultiStage {
            stages: vec![
                SamplingStrategy::Reservoir { size: 10 },
                SamplingStrategy::Systematic { interval: 2 },
            ],
        };
        let error = RowSampler::new(&strategy).expect_err("a filter cannot follow the reservoir");
        assert!(error.to_string().contains("must be the last stage"));
    }

    #[test]
    fn none_is_a_noop() {
        let sampler = RowSampler::new(&SamplingStrategy::None).unwrap();
        assert!(sampler.is_noop());
        assert!(!sampler.is_buffered());
    }

    #[test]
    fn row_view_reads_by_name() {
        let headers = headers();
        let values = row("a", "1.5", "9");
        let view = RowView::new(&headers, &values);
        assert_eq!(view.get("weight"), Some("1.5"));
        assert_eq!(view.get("missing"), None);
    }
}
