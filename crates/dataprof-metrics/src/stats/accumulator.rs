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
///   A 64-bit integer beyond 2^53 is rounded by `as f64` before any of this,
///   so [`update_integer`](Self::update_integer) takes it exactly, split into
///   its nearest `f64` and the exact remainder, and the shift is kept in the
///   same two parts. Epoch nanoseconds near 1.76e18 sit 256 apart as `f64`,
///   and three of them six apart reported a variance of 0.0 (#877).
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
/// M2 overflows the same way where the variance fits: `[1e154, -1e154]`
/// repeated has a sample variance of about 1e308, and M2, that times n - 1,
/// does not fit (#805). It is rescaled by the same factor once it would
/// overflow, so `null` is reported only for a spread whose variance itself
/// exceeds `f64`.
///
/// At the other end, the squared deviations of values near 1e-200 underflow
/// to zero, and a varying column reported a variance of zero, which the
/// coefficient of variation and the kurtosis then read as a constant column
/// (#1088). So M2 starts out held at [`UNDERFLOW_SCALE`] and is brought down
/// to its true scale once it grows past what that leaves room for. M2 never
/// decreases, so it only ever moves towards the reduced scale. The standard
/// deviation is taken at the held scale, and survives where the variance
/// itself is too small for `f64`.
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
    /// The exact remainder of a first value given to
    /// [`update_integer`](Self::update_integer) beyond `shift`; zero otherwise.
    shift_low: f64,
    /// Welford's running mean of the shifted values, the reference point for
    /// `m2`. Not overflow-proof, so the reported mean comes from `sum` instead.
    running_mean: f64,
    /// Welford's sum of squared deviations from the running mean, multiplied
    /// by the factor of `m2_scale`.
    m2: f64,
    m2_scale: M2Scale,
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

/// Factor M2 is held at while it is small: 2^1000, as the square of
/// [`UNDERFLOW_HALF`] so the squared deviations it is built from are formed at
/// it without rounding. A squared deviation stays a normal `f64` at this scale
/// down to 2^-2022, which deviations of about 2^-1011 (1e-304) reach; below
/// that it rounds in the subnormal range, and for deviations of subnormal
/// values it can vanish. [`LOST_SPREAD_PER_VALUE`] decides when that has
/// reached M2's digits. M2 itself fits up to 2^24 before it is brought down.
/// 2^1000 is about as far as a factor can go: `f64` holds no power of two
/// above 2^1023.
const UNDERFLOW_SCALE: f64 = f64::from_bits((1023 + 1000) << 52);

/// 2^-1034, the smallest held M2 per value whose digits a squared deviation
/// rounded in the subnormal range cannot reach: each such product is off by at
/// most 2^-1075, so `count` of them are off by less than 2^-41 of an M2 of at
/// least `count` times this. Below it, a varying column's spread is reported
/// as lost rather than as a number missing most of its digits, or as zero.
const LOST_SPREAD_PER_VALUE: f64 = f64::from_bits(1 << 40);

/// 2^500, the factor each deviation is raised by before it is squared.
const UNDERFLOW_HALF: f64 = f64::from_bits((1023 + 500) << 52);

/// The scale M2 is held at. M2 never decreases, so it only moves down this
/// list: from [`Raised`](Self::Raised), where a tiny spread keeps its digits,
/// to [`Unit`](Self::Unit), and to [`Reduced`](Self::Reduced) once it would
/// overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum M2Scale {
    Raised,
    Unit,
    Reduced,
}

impl M2Scale {
    /// What M2 is multiplied by at this scale.
    fn factor(self) -> f64 {
        match self {
            Self::Raised => UNDERFLOW_SCALE,
            Self::Unit => 1.0,
            Self::Reduced => OVERFLOW_SCALE,
        }
    }

    /// The square root of [`factor`](Self::factor), exact as a power of two.
    fn root(self) -> f64 {
        match self {
            Self::Raised => UNDERFLOW_HALF,
            Self::Unit => 1.0,
            Self::Reduced => 1.0 / 4_294_967_296.0,
        }
    }

    fn next(self) -> Option<Self> {
        match self {
            Self::Raised => Some(Self::Unit),
            Self::Unit => Some(Self::Reduced),
            Self::Reduced => None,
        }
    }

    /// `value`, held at this scale, held at `to` instead. Only called with
    /// `to` at or after `self`, so the factor shrinks and nothing overflows.
    fn convert(self, value: f64, to: Self) -> f64 {
        if self == to {
            value
        } else {
            value / self.factor() * to.factor()
        }
    }
}

/// Every integer of at most this magnitude, 2^53, is exact as an `f64`.
const EXACT_F64_INTEGER: u128 = 1 << 53;

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
            shift_low: 0.0,
            running_mean: 0.0,
            m2: 0.0,
            m2_scale: M2Scale::Raised,
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
        self.push(value, 0.0);
    }

    /// Fold in an integer without rounding it to `f64` first. Min, max and the
    /// mean are still the nearest `f64`; the variance is that of the exact
    /// values. Within ±2^53 this is [`update`](Self::update), bit for bit.
    #[inline]
    pub fn update_integer(&mut self, value: i64) {
        self.update_wide_integer(i128::from(value));
    }

    /// [`update_integer`](Self::update_integer) for an unsigned 64-bit value.
    #[inline]
    pub fn update_unsigned(&mut self, value: u64) {
        self.update_wide_integer(i128::from(value));
    }

    /// Fold in a parsed text value. See [`ParsedNumber`].
    #[inline]
    pub fn update_number(&mut self, number: ParsedNumber) {
        match number {
            ParsedNumber::Float(value) => self.update(value),
            ParsedNumber::Integer(value) => self.update_wide_integer(value),
        }
    }

    /// Callers pass a value in `i64::MIN..=u64::MAX`, where the nearest `f64`
    /// converts back to `i128` exactly and the remainder, below 2^11, is exact
    /// as an `f64` too.
    fn update_wide_integer(&mut self, value: i128) {
        if value.unsigned_abs() <= EXACT_F64_INTEGER {
            self.update(value as f64);
            return;
        }
        let high = value as f64;
        let low = (value - high as i128) as f64;
        self.push(high, low);
    }

    /// Fold in the value `high + low`, where `high` is its nearest `f64` and
    /// `low` the exact remainder (zero for an `f64` input).
    #[inline]
    fn push(&mut self, high: f64, low: f64) {
        self.count += 1;
        if self.count == 1 {
            self.shift = high;
            self.shift_low = low;
        }

        // With both remainders zero this is `high - shift`, bit for bit.
        let shifted = (high - self.shift) - (self.shift_low - low);
        let delta = shifted - self.running_mean;
        self.running_mean += delta / self.count as f64;
        let deviation = shifted - self.running_mean;
        self.add_to_m2(0.0, M2Scale::Raised, |scale| match scale {
            M2Scale::Raised => (delta * UNDERFLOW_HALF) * (deviation * UNDERFLOW_HALF),
            M2Scale::Unit => delta * deviation,
            M2Scale::Reduced => (delta * OVERFLOW_SCALE) * deviation,
        });

        self.add_to_sum(high, low, false);

        self.min = self.min.min(high);
        self.max = self.max.max(high);
    }

    /// Rebuild an accumulator from state computed elsewhere.
    ///
    /// Used by the SIMD accumulation in [`crate::acceleration::simd`], which
    /// runs these same recurrences over four lanes at once and then merges the
    /// lanes back together. Nothing else should reach past [`Self::update`].
    /// Lane sums and M2 come at their true scale: the SIMD path falls back to
    /// scalar accumulation when either overflows in a lane, or when a lane's M2
    /// is small enough to have lost digits to underflow.
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
        // Held raised where it fits, as a scalar pass would hold it, so the
        // merge of lanes with tiny means keeps its digits too: values cycling
        // every four rows leave each lane constant, and the whole spread is in
        // the merge.
        let raised = m2 * UNDERFLOW_SCALE;
        let (m2, m2_scale) = if raised.is_finite() {
            (raised, M2Scale::Raised)
        } else {
            (m2, M2Scale::Unit)
        };
        Self {
            count,
            shift,
            shift_low: 0.0,
            running_mean,
            m2,
            m2_scale,
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

    /// Add `m2 + increment`, the `m2` held at `scale`, to M2, moving M2 to the
    /// next scale for as long as the addition overflows. `increment` forms the
    /// increment at the scale it is given: the product at the true scale can
    /// underflow, or overflow, where the one at another scale fits.
    ///
    /// An increment that still overflows at the reduced scale exceeds
    /// `f64::MAX * 2^64`, so the variance it belongs to cannot be finite for
    /// fewer than 2^64 values, and reporting it as overflowed is correct.
    #[inline]
    fn add_to_m2(&mut self, m2: f64, scale: M2Scale, increment: impl Fn(M2Scale) -> f64) {
        let mut target = self.m2_scale.max(scale);
        loop {
            // Associated as before the scales existed. Scaling by a power of
            // two is exact, so a spread that fits reports the same bits at
            // every scale.
            let total = self.m2_scale.convert(self.m2, target)
                + (scale.convert(m2, target) + increment(target));
            match target.next() {
                Some(next) if !total.is_finite() => target = next,
                _ => {
                    self.m2 = total;
                    self.m2_scale = target;
                    return;
                }
            }
        }
    }

    /// M2 divided by `denominator`, back at its true scale.
    fn m2_over(&self, denominator: f64) -> f64 {
        self.m2 / denominator / self.m2_scale.factor()
    }

    /// The square root of M2 divided by `denominator`, taken at the scale M2
    /// is held at. Infinite or NaN where the variance is: an overflowing
    /// spread reports no standard deviation either, as before #1088. NaN
    /// where the spread was lost to underflow.
    fn std_dev_over(&self, denominator: f64) -> f64 {
        let variance = self.m2_over(denominator);
        if !variance.is_finite() {
            return variance.sqrt();
        }
        if self.spread_lost() {
            return f64::NAN;
        }
        (self.m2 / denominator).max(0.0).sqrt() / self.m2_scale.root()
    }

    /// A variance from [`m2_over`](Self::m2_over), clamped at zero, or NaN
    /// when a non-zero spread underflowed to zero or was lost: the variance is
    /// then too small for `f64`, as an infinite one is too large, and callers
    /// report neither.
    fn reported_variance(&self, variance: f64) -> f64 {
        if variance.is_nan() {
            variance
        } else if (variance <= 0.0 && self.m2 > 0.0) || self.spread_lost() {
            f64::NAN
        } else {
            variance.max(0.0)
        }
    }

    /// Whether the values vary but M2, as held, is too small to be sure of its
    /// digits: deviations of subnormal values square to products that round,
    /// or vanish, even at [`UNDERFLOW_SCALE`]. See [`LOST_SPREAD_PER_VALUE`].
    fn spread_lost(&self) -> bool {
        self.min < self.max && self.m2 < self.count as f64 * LOST_SPREAD_PER_VALUE
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
        let shift_gap = (other.shift - self.shift) - (self.shift_low - other.shift_low);
        let delta = shift_gap + (other.running_mean - self.running_mean);
        self.running_mean += delta * (b / combined);
        let weight = a * b / combined;
        self.add_to_m2(other.m2, other.m2_scale, |scale| match scale {
            M2Scale::Raised => (delta * UNDERFLOW_HALF) * (delta * UNDERFLOW_HALF) * weight,
            M2Scale::Unit => delta * delta * weight,
            M2Scale::Reduced => (delta * OVERFLOW_SCALE) * delta * weight,
        });
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

    /// Unbiased sample variance (n-1 denominator). Infinite when it is too
    /// large for `f64`, NaN when a non-zero spread is too small for it.
    pub fn sample_variance(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        self.reported_variance(self.m2_over((self.count - 1) as f64))
    }

    /// Sample standard deviation, taken at the scale M2 is held at, so it is
    /// finite where [`sample_variance`](Self::sample_variance) underflowed.
    pub fn sample_std_dev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        self.std_dev_over((self.count - 1) as f64)
    }

    /// Population variance (n denominator), reported as
    /// [`sample_variance`](Self::sample_variance) is.
    pub fn population_variance(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        self.reported_variance(self.m2_over(self.count as f64))
    }

    /// Population standard deviation, taken as
    /// [`sample_std_dev`](Self::sample_std_dev) is.
    pub fn population_std_dev(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        self.std_dev_over(self.count as f64)
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

impl FromIterator<ParsedNumber> for NumericAccumulator {
    fn from_iter<I: IntoIterator<Item = ParsedNumber>>(values: I) -> Self {
        let mut accumulator = Self::new();
        for value in values {
            accumulator.update_number(value);
        }
        accumulator
    }
}

/// A finite number read from a text cell.
///
/// An integer literal of 2^53 or more in magnitude, within the 64-bit range, is
/// kept exact, so a text path accumulates it as an Arrow `Int64` column does
/// (#877). Everything else, integers beyond 64 bits included, is its `f64`.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum ParsedNumber {
    Float(f64),
    Integer(i128),
}

impl ParsedNumber {
    /// The number `text` holds, or `None` when it holds none or a non-finite
    /// one. Accepts exactly what `text.trim().parse::<f64>()` accepts.
    pub fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        // decode-audit: no-data — a cell that does not parse is a non-numeric
        // value, excluded from numeric stats by design.
        let value = text.parse::<f64>().ok().filter(|value| value.is_finite())?;
        if value.abs() >= EXACT_F64_INTEGER as f64
            && let Ok(integer) = text.parse::<i128>()
            && (i128::from(i64::MIN)..=i128::from(u64::MAX)).contains(&integer)
        {
            return Some(Self::Integer(integer));
        }
        Some(Self::Float(value))
    }

    /// The nearest `f64`.
    pub fn to_f64(self) -> f64 {
        match self {
            Self::Float(value) => value,
            Self::Integer(value) => value as f64,
        }
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
    fn integers_within_2_53_accumulate_as_their_f64() {
        let values = [
            -(1_i64 << 53),
            -7,
            0,
            1,
            42,
            1_000_000_007,
            (1_i64 << 53) - 1,
            1 << 53,
        ];
        let mut integers = NumericAccumulator::new();
        for value in values {
            integers.update_integer(value);
        }
        let floats = accumulate(&values.map(|value| value as f64));
        assert_eq!(integers, floats);
    }

    #[test]
    fn integers_beyond_2_53_keep_their_spread_through_a_merge() {
        // #877: 256 apart as `f64`, so their variance used to be 0.0.
        let values: [i64; 4] = [
            1_759_700_000_123_456_789,
            1_759_700_000_123_456_791,
            1_759_700_000_123_456_795,
            1_759_700_000_123_456_800,
        ];
        let (mut left, mut right, mut whole) = (
            NumericAccumulator::new(),
            NumericAccumulator::new(),
            NumericAccumulator::new(),
        );
        for (index, value) in values.into_iter().enumerate() {
            whole.update_integer(value);
            if index < 2 { &mut left } else { &mut right }.update_integer(value);
        }
        left.merge(&right);
        for accumulator in [whole, left] {
            assert!((accumulator.sample_variance() - 283.0 / 12.0).abs() < 1e-12);
            assert_eq!(
                accumulator.max(),
                Some(1_759_700_000_123_456_800_i64 as f64)
            );
        }

        let mut unsigned = NumericAccumulator::new();
        for value in [u64::MAX - 15, u64::MAX - 10, u64::MAX - 5, u64::MAX] {
            unsigned.update_unsigned(value);
        }
        assert!((unsigned.sample_variance() - 125.0 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn parsed_numbers_keep_only_wide_64_bit_integer_literals_exact() {
        assert_eq!(
            ParsedNumber::parse(" +9007199254740993 "),
            Some(ParsedNumber::Integer(9_007_199_254_740_993))
        );
        assert_eq!(
            ParsedNumber::parse("18446744073709551615"),
            Some(ParsedNumber::Integer(i128::from(u64::MAX)))
        );
        assert_eq!(
            ParsedNumber::parse("-9223372036854775808"),
            Some(ParsedNumber::Integer(i128::from(i64::MIN)))
        );
        // Exact as `f64` already, or not an integer literal, or beyond 64 bits.
        assert_eq!(ParsedNumber::parse("42"), Some(ParsedNumber::Float(42.0)));
        assert_eq!(ParsedNumber::parse("1e20"), Some(ParsedNumber::Float(1e20)));
        assert_eq!(
            ParsedNumber::parse("9007199254740993.0"),
            Some(ParsedNumber::Float(9_007_199_254_740_992.0))
        );
        assert_eq!(
            ParsedNumber::parse("18446744073709551616"),
            Some(ParsedNumber::Float(18_446_744_073_709_551_616.0))
        );
        for text in ["", "N/A", "inf", "NaN", "1e400"] {
            assert_eq!(ParsedNumber::parse(text), None, "{text:?}");
        }
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

    /// `1e154` and `-1e154` in alternating blocks of four, 104 values: the
    /// sample variance, 1.04e310 / 103, fits in `f64`; M2, the same times 103,
    /// does not, and neither does an increment (2e308). Both used to turn the
    /// variance into `null` (#805). Blocks of four flip every SIMD lane too, so
    /// a lane M2 overflows as well as the merge of the lanes.
    #[test]
    fn variance_fits_where_its_sum_of_squares_does_not() {
        let values: Vec<f64> = (0..104)
            .map(|i| if (i / 4) % 2 == 0 { 1e154 } else { -1e154 })
            .collect();
        // 1.04e310 / 103, the sample variance by exact arithmetic.
        let exact = 1.009_708_737_864_077_7e308;
        let single = accumulate(&values);
        let mut merged = accumulate(&values[..37]);
        merged.merge(&accumulate(&values[37..]));
        let simd = crate::acceleration::simd::compute_stats_simd(&values);
        for (path, accumulator) in [("single", single), ("merged", merged), ("simd", simd)] {
            let variance = accumulator.sample_variance();
            assert!(
                (variance - exact).abs() <= exact * 1e-14,
                "{path}: variance {variance}, exact {exact}"
            );
            assert!(accumulator.population_variance().is_finite(), "{path}");
        }

        // Ten times the spread is a variance of about 1e310: still overflowed.
        let wider: Vec<f64> = values.iter().map(|value| value * 10.0).collect();
        assert!(!accumulate(&wider).sample_variance().is_finite());
    }

    /// 2^exponent, built from its bits so it is exact.
    fn power_of_two(exponent: i32) -> f64 {
        f64::from_bits(((1023 + exponent) as u64) << 52)
    }

    /// Values near 1e-200 have squared deviations that underflow: the variance
    /// read 0.0 and the standard deviation with it, so a varying column looked
    /// constant (#1088). Scaled by a power of two, every step is scaled
    /// exactly, so the standard deviation is the unit-scale one times the
    /// scale, bit for bit, on every path. Cycling every four values leaves each
    /// SIMD lane constant and puts the whole spread in the merge of the lanes;
    /// blocks of four put it inside each lane.
    #[test]
    fn a_tiny_spread_keeps_its_standard_deviation() {
        let pattern = [1.0, 2.0, 4.0, 9.0];
        let cycled: Vec<f64> = (0..104).map(|i| pattern[i % 4]).collect();
        let blocked: Vec<f64> = (0..104).map(|i| pattern[(i / 4) % 4]).collect();
        let paths = |values: &[f64]| {
            let mut merged = accumulate(&values[..37]);
            merged.merge(&accumulate(&values[37..]));
            [
                ("single", accumulate(values)),
                ("merged", merged),
                (
                    "simd",
                    crate::acceleration::simd::compute_stats_simd(values),
                ),
            ]
        };
        let mut wrong = Vec::new();
        for (shape, unit) in [("cycled", &cycled), ("blocked", &blocked)] {
            let expected: Vec<f64> = paths(unit)
                .iter()
                .map(|(_, accumulator)| accumulator.sample_std_dev())
                .collect();
            // About 1e-160, 1e-200 and 1e-300.
            for exponent in [-530, -665, -997] {
                let scale = power_of_two(exponent);
                let values: Vec<f64> = unit.iter().map(|value| value * scale).collect();
                for ((path, accumulator), unit_std_dev) in paths(&values).iter().zip(&expected) {
                    let std_dev = accumulator.sample_std_dev();
                    let scaled = unit_std_dev * scale;
                    // A tiny lane M2 sends SIMD to the scalar pass, which
                    // rounds in another order than the lanes at unit scale.
                    let tolerance = if *path == "simd" { 1e-14 * scaled } else { 0.0 };
                    if (std_dev - scaled).abs() > tolerance {
                        wrong.push(format!("{shape} 2^{exponent} {path}: std_dev {std_dev:e}"));
                    }
                    // The variance itself, about 2^(2 * exponent), is below
                    // f64's range from 2^-665 on: not finite, not zero.
                    let variance = accumulator.sample_variance();
                    let underflowed = exponent <= -665;
                    if variance.is_finite() == underflowed || variance == 0.0 {
                        wrong.push(format!(
                            "{shape} 2^{exponent} {path}: variance {variance:e}"
                        ));
                    }
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// Subnormal values square to products that round or vanish even at the
    /// raised scale: at 2^-1040 a few survive and the spread came out with
    /// almost none of its digits, at 2^-1074 none do and it came out zero. Both
    /// are reported as lost, not as a number. A constant subnormal column is
    /// still exactly constant.
    #[test]
    fn a_spread_lost_to_subnormal_rounding_is_not_reported() {
        let pattern = [1.0, 2.0, 4.0, 9.0];
        let mut wrong = Vec::new();
        for exponent in [-1040, -1074] {
            let unit = f64::from_bits(1 << (1074 + exponent));
            let cycled: Vec<f64> = (0..104).map(|i| pattern[i % 4] * unit).collect();
            let mut merged = accumulate(&cycled[..37]);
            merged.merge(&accumulate(&cycled[37..]));
            let simd = crate::acceleration::simd::compute_stats_simd(&cycled);
            for (path, accumulator) in [
                ("single", accumulate(&cycled)),
                ("merged", merged),
                ("simd", simd),
            ] {
                let (variance, std_dev) =
                    (accumulator.sample_variance(), accumulator.sample_std_dev());
                if variance.is_finite() || std_dev.is_finite() {
                    wrong.push(format!(
                        "2^{exponent} {path}: variance {variance:e}, std_dev {std_dev:e}"
                    ));
                }
            }
            let constant = accumulate(&[unit; 8]);
            assert_eq!(
                (constant.sample_variance(), constant.sample_std_dev()),
                (0.0, 0.0)
            );
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
    }

    /// A spread that fits keeps the bits it had before M2 was held raised:
    /// scaling by a power of two is exact for it.
    #[test]
    fn a_spread_that_fits_reports_the_same_bits_at_every_scale() {
        let values = [0.1, 0.2, 0.30000000000000004, 1e3, 7.5, -2.25];
        let accumulator = accumulate(&values);
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let squares: f64 = values.iter().map(|value| (value - mean).powi(2)).sum();
        let variance = accumulator.sample_variance();
        assert!((variance - squares / 5.0).abs() <= 1e-12 * variance);
        assert_eq!(accumulator.sample_std_dev(), variance.sqrt());
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
