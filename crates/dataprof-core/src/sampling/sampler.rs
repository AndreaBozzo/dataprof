//! The runtime that applies a [`SamplingStrategy`] to a stream of rows.
//!
//! Strategies fall into two families, and the difference is not cosmetic:
//!
//! * `Systematic` decides each row on the spot, so a row can be folded into
//!   the running statistics immediately and memory stays bounded.
//! * `Reservoir` and `Random` cannot. Whether row 5 belongs in a uniform sample
//!   of 10 is not known until the stream ends: row 5 may be evicted at row 900.
//!   Streaming statistics are not retractable, so a row that was folded in and
//!   later evicted would silently corrupt the profile. These strategies
//!   therefore buffer the candidate rows and hand the final sample back at end
//!   of stream, costing `size` rows of memory.
//!
//! [`RowSampler`] hides that split behind one interface so every engine treats
//! sampling identically. Consult [`RowSampler::is_buffered`] to know which of
//! [`RowSampler::accept`] or [`RowSampler::offer`] to call.

use crate::errors::DataProfilerError;

use super::reservoir::ReservoirSampler;
use super::strategies::SamplingStrategy;

/// A fixed-size uniform sample held in memory until the stream ends.
#[derive(Debug)]
struct SampleBuffer<R> {
    capacity: usize,
    rows: Vec<R>,
    sampler: ReservoirSampler,
    seen: usize,
}

impl<R> SampleBuffer<R> {
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
    fn offer(&mut self, values: R) {
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

    fn take(&mut self) -> Vec<R> {
        std::mem::take(&mut self.rows)
    }
}

/// Applies a [`SamplingStrategy`] to a stream of rows, holding the state the
/// strategy needs across rows.
///
/// Engines create one sampler per scan and keep it for the whole scan.
///
/// `R` is what the fixed-size stage holds for each row until the stream ends.
/// It is the row's field values unless an engine has to keep something about
/// the row that its values cannot say, and must then get it back with exactly
/// the rows that were sampled.
#[derive(Debug)]
pub struct RowSampler<R = Vec<String>> {
    /// Keep every `interval`-th row (systematic sampling).
    interval: Option<usize>,
    /// Fixed-size stage, for random and reservoir sampling.
    buffer: Option<SampleBuffer<R>>,
    /// Rows offered to the sampler, whether or not they were kept.
    iterated: usize,
    /// Rows the sampler accepted for immediate folding (streaming path only).
    accepted: usize,
}

impl RowSampler {
    /// Build a sampler for `strategy`.
    pub fn new(strategy: &SamplingStrategy) -> Result<Self, DataProfilerError> {
        Self::for_rows(strategy)
    }
}

impl<R> RowSampler<R> {
    /// As [`RowSampler::new`], for a sampler whose fixed-size stage holds `R`
    /// for each row rather than its field values alone.
    pub fn for_rows(strategy: &SamplingStrategy) -> Result<Self, DataProfilerError> {
        let (interval, buffer) = match strategy {
            SamplingStrategy::None => (None, None),
            SamplingStrategy::Random { size } | SamplingStrategy::Reservoir { size } => {
                (None, Some(SampleBuffer::new(*size)))
            }
            // An interval of 0 or 1 keeps every row.
            SamplingStrategy::Systematic { interval } => {
                ((*interval > 1).then_some(*interval), None)
            }
        };
        Ok(Self {
            interval,
            buffer,
            iterated: 0,
            accepted: 0,
        })
    }

    /// Whether rows must be handed to [`offer`](Self::offer) instead of being
    /// folded in as [`accept`](Self::accept) approves them.
    pub fn is_buffered(&self) -> bool {
        self.buffer.is_some()
    }

    /// Whether this sampler can ever exclude a row.
    pub fn is_noop(&self) -> bool {
        self.interval.is_none() && self.buffer.is_none()
    }

    /// Decide whether the next row passes the systematic filter.
    ///
    /// Call this for every row, including on the buffered path, so the row
    /// count stays exact.
    pub fn accept(&mut self) -> bool {
        let index = self.iterated;
        self.iterated += 1;
        if self
            .interval
            .is_some_and(|interval| !index.is_multiple_of(interval))
        {
            return false;
        }
        if self.buffer.is_none() {
            self.accepted += 1;
        }
        true
    }

    /// Hand a row that passed [`accept`](Self::accept) to the fixed-size stage.
    ///
    /// Only meaningful when [`is_buffered`](Self::is_buffered) is true.
    pub fn offer(&mut self, values: R) {
        if let Some(buffer) = self.buffer.as_mut() {
            buffer.offer(values);
        }
    }

    /// The final sample from the fixed-size stage, ready to fold into the
    /// statistics. Empty for a purely streaming strategy.
    pub fn take_sample(&mut self) -> Vec<R> {
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(strategy: SamplingStrategy, rows: usize) -> (usize, usize) {
        let mut sampler = RowSampler::new(&strategy).expect("valid strategy");
        for i in 0..rows {
            if sampler.accept() && sampler.is_buffered() {
                sampler.offer(vec![i.to_string()]);
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
    fn systematic_with_interval_zero_or_one_keeps_every_row() {
        for interval in [0, 1] {
            let (sampled, _) = run(SamplingStrategy::Systematic { interval }, 37);
            assert_eq!(sampled, 37, "interval {interval}");
        }
    }

    #[test]
    fn none_is_a_noop() {
        let sampler = RowSampler::new(&SamplingStrategy::None).unwrap();
        assert!(sampler.is_noop());
        assert!(!sampler.is_buffered());
    }
}
