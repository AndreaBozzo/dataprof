//! Numerically stable accumulation of the aggregates a numeric column reports.

/// Running count, min, max, mean and sample variance over a stream of finite
/// `f64` values.
///
/// Two accumulations run side by side because neither alone is enough:
///
/// - **Welford's online mean and M2** give a translation-invariant variance.
///   `sum_squares - n * mean²` loses every significant digit of a small spread
///   sitting on a large offset: four consecutive integers near 1e9 came back
///   with a variance of exactly 0.0, describing a varying column as constant.
/// - **A compensated (Knuth two-sum) running sum** gives a mean that survives
///   cancellation. Welford's running mean rounds `[1e16, 1.0, -1e16]` to 0.0
///   because the unit contribution disappears into the intermediate mean; the
///   compensated sum keeps it and returns 1/3.
///
/// The compensated sum can still overflow where the mean itself is
/// representable (`[1e308, 1e308]`), so [`mean`](Self::mean) falls back to
/// Welford's running mean — which cannot overflow — whenever the sum is not
/// finite.
///
/// [`merge`](Self::merge) combines accumulators computed over disjoint parts of
/// a column, so chunked, batched and SIMD-lane accumulation report what a
/// single pass would.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NumericAccumulator {
    count: u64,
    /// Welford's running mean: overflow-proof, used when `sum` is not finite.
    running_mean: f64,
    /// Welford's sum of squared deviations from the running mean.
    m2: f64,
    /// Running sum, with `compensation` carrying the bits it rounded away.
    sum: f64,
    compensation: f64,
    min: f64,
    max: f64,
}

impl Default for NumericAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Knuth's two-sum: the rounded sum of `a + b` and the exact rounding error,
/// with no branch and no wider type. `err` is exact whenever `a + b` is finite.
#[inline]
fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let sum = a + b;
    let b_virtual = sum - a;
    let err = (a - (sum - b_virtual)) + (b - b_virtual);
    (sum, err)
}

impl NumericAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            running_mean: 0.0,
            m2: 0.0,
            sum: 0.0,
            compensation: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    /// Fold one value in. Callers pass finite values only; non-finite input is
    /// filtered out at parse time so it can never reach the aggregates.
    #[inline]
    pub fn update(&mut self, value: f64) {
        debug_assert!(value.is_finite());
        self.count += 1;

        let delta = value - self.running_mean;
        self.running_mean += delta / self.count as f64;
        self.m2 += delta * (value - self.running_mean);

        let (sum, err) = two_sum(self.sum, value);
        self.sum = sum;
        self.compensation += err;

        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Rebuild an accumulator from state computed elsewhere.
    ///
    /// Used by the SIMD accumulation in [`crate::acceleration::simd`], which
    /// runs these same recurrences over four lanes at once and then merges the
    /// lanes back together. Nothing else should reach past [`Self::update`].
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_lane_state(
        count: u64,
        running_mean: f64,
        m2: f64,
        sum: f64,
        compensation: f64,
        min: f64,
        max: f64,
    ) -> Self {
        Self {
            count,
            running_mean,
            m2,
            sum,
            compensation,
            min,
            max,
        }
    }

    /// Fold in an accumulator built over a disjoint set of values.
    pub fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = *other;
            return;
        }

        // Chan's parallel form of Welford's update.
        let (a, b) = (self.count as f64, other.count as f64);
        let combined = a + b;
        let delta = other.running_mean - self.running_mean;
        self.running_mean += delta * (b / combined);
        self.m2 += other.m2 + delta * delta * (a * b / combined);
        self.count += other.count;

        let (sum, err) = two_sum(self.sum, other.sum);
        self.sum = sum;
        self.compensation += err + other.compensation;

        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Smallest value seen, or `None` when nothing was accumulated.
    #[inline]
    pub fn min(&self) -> Option<f64> {
        (self.count > 0).then_some(self.min)
    }

    /// Largest value seen, or `None` when nothing was accumulated.
    #[inline]
    pub fn max(&self) -> Option<f64> {
        (self.count > 0).then_some(self.max)
    }

    /// Arithmetic mean. Zero for an empty accumulator, matching the callers
    /// that report absent statistics separately.
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        // `compensation` is NaN once the sum overflows, which fails this test
        // just as an infinite sum does.
        let total = self.sum + self.compensation;
        if total.is_finite() {
            total / self.count as f64
        } else {
            self.running_mean
        }
    }

    /// Unbiased sample variance (n-1 denominator).
    pub fn sample_variance(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        (self.m2 / (self.count - 1) as f64).max(0.0)
    }

    /// Standard deviation derived from [`sample_variance`](Self::sample_variance).
    pub fn sample_std_dev(&self) -> f64 {
        self.sample_variance().sqrt()
    }

    /// Population variance (n denominator).
    pub fn population_variance(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        (self.m2 / self.count as f64).max(0.0)
    }

    /// Standard deviation derived from [`population_variance`](Self::population_variance).
    pub fn population_std_dev(&self) -> f64 {
        self.population_variance().sqrt()
    }
}

impl FromIterator<f64> for NumericAccumulator {
    fn from_iter<I: IntoIterator<Item = f64>>(values: I) -> Self {
        let mut accumulator = Self::new();
        for value in values {
            accumulator.update(value);
        }
        accumulator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn accumulate(values: &[f64]) -> NumericAccumulator {
        values.iter().copied().collect()
    }

    #[test]
    fn empty_accumulator_reports_no_values() {
        let accumulator = NumericAccumulator::new();
        assert_eq!(accumulator.count(), 0);
        assert_eq!(accumulator.min(), None);
        assert_eq!(accumulator.max(), None);
        assert_eq!(accumulator.mean(), 0.0);
        assert_eq!(accumulator.sample_variance(), 0.0);
    }

    #[test]
    fn reports_textbook_statistics() {
        let accumulator = accumulate(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert_eq!(accumulator.count(), 8);
        assert_eq!(accumulator.min(), Some(2.0));
        assert_eq!(accumulator.max(), Some(9.0));
        assert_eq!(accumulator.mean(), 5.0);
        // Sample variance of the classic 8-value set is 32/7.
        assert!((accumulator.sample_variance() - 32.0 / 7.0).abs() < 1e-12);
        assert!((accumulator.population_variance() - 4.0).abs() < 1e-12);
    }

    #[test]
    fn variance_is_translation_invariant() {
        for offset in [0.0, 1e6, 1e8, 1e9, 1e12] {
            let accumulator = accumulate(&[offset, offset + 1.0, offset + 2.0, offset + 3.0]);
            assert!(
                (accumulator.sample_variance() - 5.0 / 3.0).abs() < 1e-9,
                "offset {offset} reported variance {}",
                accumulator.sample_variance()
            );
        }
    }

    #[test]
    fn mean_survives_cancellation_in_any_order() {
        for values in [
            [1e16, 1.0, -1e16],
            [1e16, -1e16, 1.0],
            [-1e16, 1e16, 1.0],
            [1.0, 1e16, -1e16],
        ] {
            let accumulator = accumulate(&values);
            assert!(
                (accumulator.mean() - 1.0 / 3.0).abs() < 1e-12,
                "{values:?} reported mean {}",
                accumulator.mean()
            );
        }
    }

    #[test]
    fn mean_stays_finite_when_the_sum_overflows() {
        let accumulator = accumulate(&[1e308, 1e308]);
        assert_eq!(accumulator.mean(), 1e308);
        assert_eq!(accumulator.sample_variance(), 0.0);
    }

    #[test]
    fn merge_matches_a_single_pass() {
        let values: Vec<f64> = (0..97).map(|i| 1e9 + (i % 7) as f64).collect();
        let single = accumulate(&values);

        for split in [1, 13, 48, 96] {
            let mut merged = accumulate(&values[..split]);
            merged.merge(&accumulate(&values[split..]));

            assert_eq!(merged.count(), single.count());
            assert_eq!(merged.min(), single.min());
            assert_eq!(merged.max(), single.max());
            // Relative, not exact: combining two means near 1e9 rounds, so a
            // merge is never bit-identical to a single pass. It is still three
            // orders tighter than the error this fix is about — the naive
            // sum-of-squares reported 0.0 for this column.
            assert!(
                (merged.mean() - single.mean()).abs() <= single.mean().abs() * 1e-12,
                "split {split} mean {} vs {}",
                merged.mean(),
                single.mean()
            );
            assert!(
                (merged.sample_variance() - single.sample_variance()).abs()
                    <= single.sample_variance() * 1e-7,
                "split {split} variance {} vs {}",
                merged.sample_variance(),
                single.sample_variance()
            );
        }
    }

    #[test]
    fn merging_an_empty_accumulator_changes_nothing() {
        let single = accumulate(&[1.0, 2.0, 3.0]);

        let mut with_empty = single;
        with_empty.merge(&NumericAccumulator::new());
        assert_eq!(with_empty, single);

        let mut from_empty = NumericAccumulator::new();
        from_empty.merge(&single);
        assert_eq!(from_empty, single);
    }
}
