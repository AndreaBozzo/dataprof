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
///   Welford alone still rounds at the scale of the offset: its running mean
///   near 1e12 is only resolved to ~1e-4, which left a standard deviation of
///   ~575 wrong by 2e-8 relative (1.7e-6 at 1e14), enough to change the fourth
///   decimal and to differ between input paths. So Welford runs on
///   `value - shift`, where `shift` is the first value seen: for values within
///   a factor of two of it the subtraction is exact (Sterbenz), and the
///   recurrence then works at the scale of the spread.
/// - **A compensated (Knuth two-sum) running sum** gives a mean that survives
///   cancellation. Welford's running mean rounds `[1e16, 1.0, -1e16]` to 0.0
///   because the unit contribution disappears into the intermediate mean; the
///   compensated sum keeps it and returns 1/3.
///
/// The sum can overflow where the mean itself is representable
/// (`[1e308, 1e308]`, or `[1e308, 1e308, -1e308, -1e308]` whose mean is 0.0).
/// When an addition would overflow, the sum and its compensation are rescaled
/// by [`OVERFLOW_SCALE`] and every later value is added at that scale. Welford's
/// running mean is no fallback: its `value - running_mean` overflows once the
/// two sit near opposite ends of the range, and turns the mean into NaN.
///
/// [`merge`](Self::merge) combines accumulators computed over disjoint parts of
/// a column, so chunked, batched and SIMD-lane accumulation report what a
/// single pass would.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NumericAccumulator {
    count: u64,
    /// The first value accumulated. Welford's recurrence runs on values minus
    /// this shift.
    shift: f64,
    /// Welford's running mean of the shifted values, the reference point for
    /// `m2`. Not overflow-proof, so the reported mean comes from `sum` instead.
    running_mean: f64,
    /// Welford's sum of squared deviations from the running mean.
    m2: f64,
    /// Running sum, with `compensation` carrying the bits it rounded away. Both
    /// are multiplied by [`OVERFLOW_SCALE`] once `scaled` is set.
    sum: f64,
    compensation: f64,
    scaled: bool,
    min: f64,
    max: f64,
}

impl Default for NumericAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

/// Factor the running sum is rescaled by once it would overflow: 2^-64.
///
/// A power of two, so rescaling is exact except for bits below 2^-1074 after
/// scaling (2^-1010 before), which only matter to a column already holding
/// values near the top of the range. Small enough that the scaled sum of 2^64
/// values at `f64::MAX` still fits.
const OVERFLOW_SCALE: f64 = 1.0 / 18_446_744_073_709_551_616.0;

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
            shift: 0.0,
            running_mean: 0.0,
            m2: 0.0,
            sum: 0.0,
            compensation: 0.0,
            scaled: false,
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
        if self.count == 1 {
            self.shift = value;
        }

        let shifted = value - self.shift;
        let delta = shifted - self.running_mean;
        self.running_mean += delta / self.count as f64;
        self.m2 += delta * (shifted - self.running_mean);

        self.add_to_sum(value, 0.0, false);

        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    /// Rebuild an accumulator from state computed elsewhere.
    ///
    /// Used by the SIMD accumulation in [`crate::acceleration::simd`], which
    /// runs these same recurrences over four lanes at once and then merges the
    /// lanes back together. Nothing else should reach past [`Self::update`].
    /// Lane sums are never scaled: the SIMD path falls back to scalar
    /// accumulation when a lane sum overflows.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_lane_state(
        count: u64,
        shift: f64,
        running_mean: f64,
        m2: f64,
        sum: f64,
        compensation: f64,
        min: f64,
        max: f64,
    ) -> Self {
        Self {
            count,
            shift,
            running_mean,
            m2,
            sum,
            compensation,
            scaled: false,
            min,
            max,
        }
    }

    /// Add `sum + compensation`, expressed at `OVERFLOW_SCALE` when `scaled`, to
    /// the running sum, rescaling the running sum first if the addition would
    /// overflow it.
    #[inline]
    fn add_to_sum(&mut self, sum: f64, compensation: f64, scaled: bool) {
        if scaled && !self.scaled {
            self.rescale_sum();
        }
        let unit = if self.scaled && !scaled {
            OVERFLOW_SCALE
        } else {
            1.0
        };
        let (total, err) = two_sum(self.sum, sum * unit);
        if total.is_finite() {
            self.sum = total;
            self.compensation += err + compensation * unit;
            return;
        }
        // Only an unscaled sum can get here: a scaled one would need 2^64
        // values at `f64::MAX` to overflow.
        self.rescale_sum();
        let (total, err) = two_sum(self.sum, sum * OVERFLOW_SCALE);
        self.sum = total;
        self.compensation += err + compensation * OVERFLOW_SCALE;
    }

    fn rescale_sum(&mut self) {
        debug_assert!(!self.scaled, "the running sum is rescaled at most once");
        self.sum *= OVERFLOW_SCALE;
        self.compensation *= OVERFLOW_SCALE;
        self.scaled = true;
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

        // Chan's parallel form of Welford's update, in this accumulator's
        // shifted frame. The difference of the shifts is taken first: forming
        // either unshifted mean would round it at the scale of the offset.
        let (a, b) = (self.count as f64, other.count as f64);
        let combined = a + b;
        let delta = (other.shift - self.shift) + (other.running_mean - self.running_mean);
        self.running_mean += delta * (b / combined);
        self.m2 += other.m2 + delta * delta * (a * b / combined);
        self.count += other.count;

        self.add_to_sum(other.sum, other.compensation, other.scaled);

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
        let count = self.count as f64;
        if !self.scaled {
            let total = self.sum + self.compensation;
            if total.is_finite() {
                return total / count;
            }
        }
        // The sum is scaled, or it and its compensation are each finite but
        // their total is not: take the mean at the reduced scale, where it fits.
        let unit = if self.scaled { 1.0 } else { OVERFLOW_SCALE };
        (self.sum * unit + self.compensation * unit) / count / OVERFLOW_SCALE
    }

    /// Unbiased sample variance (n-1 denominator).
    pub fn sample_variance(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        let variance = self.m2 / (self.count - 1) as f64;
        if variance.is_nan() {
            variance
        } else {
            variance.max(0.0)
        }
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
        let variance = self.m2 / self.count as f64;
        if variance.is_nan() {
            variance
        } else {
            variance.max(0.0)
        }
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

    /// The sum overflows at the second value and the true mean is 0.0. Welford's
    /// running mean reaches `-inf` at the third value and NaN at the fourth.
    #[test]
    fn mean_survives_an_overflowing_sum_that_cancels() {
        for values in [
            [1e308, 1e308, -1e308, -1e308],
            [-1e308, -1e308, 1e308, 1e308],
            [1e308, 1e308, -1e308, -1e308].map(|value| value * 1.7),
        ] {
            let accumulator = accumulate(&values);
            assert_eq!(accumulator.mean(), 0.0, "{values:?}");
            // The spread still overflows, and still reports as such (#784).
            assert!(!accumulator.sample_variance().is_finite(), "{values:?}");
        }

        let accumulator = accumulate(&[1e308, 1e308, -1e308, -1e308, 3.0]);
        assert_eq!(accumulator.mean(), 3.0 / 5.0);
    }

    /// Each addition rounds back to `f64::MAX`, so the sum itself never
    /// overflows; the compensation collects the two halves that do.
    #[test]
    fn mean_survives_a_compensation_that_overflows_the_total() {
        let small = 9e291;
        let accumulator = accumulate(&[f64::MAX, small, small]);
        assert_eq!(accumulator.sum, f64::MAX);
        assert!(!accumulator.scaled);
        let expected = f64::MAX / 3.0 + small * 2.0 / 3.0;
        assert!(
            (accumulator.mean() - expected).abs() <= expected * 1e-15,
            "reported mean {}",
            accumulator.mean()
        );
    }

    #[test]
    fn merge_rescales_a_sum_that_overflows_across_parts() {
        let values = [1e308, 1e308, -1e308, -1e308, 3.0];
        for split in 1..values.len() {
            let (left, right) = values.split_at(split);
            let mut merged = accumulate(left);
            merged.merge(&accumulate(right));
            assert_eq!(merged.mean(), 3.0 / 5.0, "split {split}");

            let mut reversed = accumulate(right);
            reversed.merge(&accumulate(left));
            assert_eq!(reversed.mean(), 3.0 / 5.0, "reversed split {split}");
        }
    }

    #[test]
    fn overflowing_spread_is_not_reported_as_zero() {
        let accumulator = accumulate(&[1e308, -1e308, 5.0, 0.5, 5.0, 7.0]);
        assert!(!accumulator.sample_variance().is_finite());
        assert!(!accumulator.sample_std_dev().is_finite());
        assert!(!accumulator.population_variance().is_finite());
    }

    /// Exact sample variance of integers, from integer sums: the reference a
    /// floating-point accumulation is measured against.
    fn exact_integer_variance(values: &[i64]) -> f64 {
        let n = values.len() as i128;
        let sum: i128 = values.iter().map(|&v| v as i128).sum();
        let squares: i128 = values.iter().map(|&v| (v as i128) * (v as i128)).sum();
        // Both terms are below 2^53, so each converts exactly and the quotient
        // is correctly rounded.
        (n * squares - sum * sum) as f64 / (n * (n - 1)) as f64
    }

    /// A spread of about 580 on offsets up to 1e14. Welford on the raw values
    /// resolves its running mean only to the offset's precision and was 2e-8
    /// relative off at 1e12, 1.7e-6 at 1e14 (#783).
    #[test]
    fn variance_on_a_large_offset_matches_the_exact_value() {
        let spread: Vec<i64> = (0..20_000).map(|i: i64| (i * 7919) % 2001 - 1000).collect();
        let expected = exact_integer_variance(&spread);
        for offset in [1e9, 1e12, 1e14] {
            let values: Vec<f64> = spread.iter().map(|&k| offset + k as f64).collect();
            let single = accumulate(&values);
            let mut merged = accumulate(&values[..7_001]);
            merged.merge(&accumulate(&values[7_001..]));
            for (path, accumulator) in [("single", single), ("merged", merged)] {
                let variance = accumulator.sample_variance();
                assert!(
                    (variance - expected).abs() <= expected * 1e-13,
                    "{path} at offset {offset}: variance {variance}, exact {expected}"
                );
            }
        }
    }

    /// Unshifted, the second value's `delta` overflowed to -inf while M2 was
    /// still 0.0, M2 became -inf, and the clamp at zero reported a constant
    /// column. Shifted, the first delta is exactly zero and an overflowing
    /// difference turns M2 into NaN, which reports as overflowed.
    #[test]
    fn an_overflowing_pair_is_not_reported_as_zero_variance() {
        let big = 0.9 * f64::MAX;
        for values in [[big, -big], [-big, big]] {
            let accumulator = accumulate(&values);
            assert!(!accumulator.sample_variance().is_finite(), "{values:?}");
            assert!(!accumulator.population_variance().is_finite(), "{values:?}");
        }
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
            // Relative, not exact: a merge combines two rounded means, so it
            // is not bit-identical to a single pass. Both work on values
            // shifted to the scale of the spread, so they agree to rounding
            // there; unshifted they differed by 1e-8 at this offset (#783), and
            // the naive sum-of-squares reported 0.0 for this column.
            assert!(
                (merged.mean() - single.mean()).abs() <= single.mean().abs() * 1e-12,
                "split {split} mean {} vs {}",
                merged.mean(),
                single.mean()
            );
            assert!(
                (merged.sample_variance() - single.sample_variance()).abs()
                    <= single.sample_variance() * 1e-13,
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
