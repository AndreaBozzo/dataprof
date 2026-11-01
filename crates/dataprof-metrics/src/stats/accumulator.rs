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
///   A decimal with more digits than `f64` keeps is rounded the same way, and
///   its remainder is not exact as an `f64` either: `1000000000000000.01` and
///   `.05` are one `f64`, and their variance was 0.0 (#1149). So
///   [`update_number`](Self::update_number) takes it as an [`ExactDecimal`]
///   and the first such value becomes the shift's exact value, the anchor.
///   Each later decimal's deviation is its exact difference from the anchor,
///   rounded once. A value whose deviation cannot be taken exactly (one too
///   wide to hold, or a difference beyond `i128`) is taken in `f64`, and a
///   spread too small to stand above that rounding is reported as lost.
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
/// exceeds `f64`. Its standard deviation usually fits, so M2 moves to a
/// further scale, 2^-1000, once it overflows even at that one, and the standard
/// deviation is taken at the scale M2 is held at (#1150).
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
    /// [`update_integer`](Self::update_integer) beyond `shift`, the remainder
    /// of a decimal anchor (see [`ExactDecimal::low_part`]), zero otherwise.
    shift_low: f64,
    /// The exact value of the shift, once a decimal beyond `f64`, or an
    /// integer beyond 2^53, has set it. `None` while every value was an `f64`.
    anchor: Option<ExactDecimal>,
    /// The largest magnitude involved in a deviation that should have been
    /// exact and was taken in `f64` instead: zero while there was none.
    unheld: f64,
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
    /// The smallest and largest value as given, which `min` and `max` round
    /// past what `f64` holds, so they can be reported exactly (#1038).
    min_value: Option<ParsedNumber>,
    max_value: Option<ParsedNumber>,
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

/// 2^-1000, the factor M2 is held at once it would overflow even at
/// [`OVERFLOW_SCALE`]: a variance past about `f64::MAX * 2^64 / n`, whose
/// standard deviation can still fit (#1150). M2 fits here up to
/// `f64::MAX * 2^1000`, about 2e609, so the standard deviation is lost only
/// past about 4e304 divided by the square root of the count. The square of
/// [`SHRUNK_HALF`], as [`UNDERFLOW_SCALE`] is of [`UNDERFLOW_HALF`].
const SHRUNK_SCALE: f64 = f64::from_bits((1023 - 1000) << 52);

/// 2^-500, the factor each deviation is shrunk by before it is squared at
/// [`SHRUNK_SCALE`]. A product of two deviations then overflows only past
/// about 4e304 each, near the top of `f64`'s range.
const SHRUNK_HALF: f64 = f64::from_bits((1023 - 500) << 52);

/// The scale M2 is held at. M2 never decreases, so it only moves down this
/// list: from [`Raised`](Self::Raised), where a tiny spread keeps its digits,
/// to [`Unit`](Self::Unit), to [`Reduced`](Self::Reduced) once it would
/// overflow, and to [`Shrunk`](Self::Shrunk) once it would overflow there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum M2Scale {
    Raised,
    Unit,
    Reduced,
    Shrunk,
}

impl M2Scale {
    /// What M2 is multiplied by at this scale.
    fn factor(self) -> f64 {
        match self {
            Self::Raised => UNDERFLOW_SCALE,
            Self::Unit => 1.0,
            Self::Reduced => OVERFLOW_SCALE,
            Self::Shrunk => SHRUNK_SCALE,
        }
    }

    /// The square root of [`factor`](Self::factor), exact as a power of two.
    fn root(self) -> f64 {
        match self {
            Self::Raised => UNDERFLOW_HALF,
            Self::Unit => 1.0,
            Self::Reduced => 1.0 / 4_294_967_296.0,
            Self::Shrunk => SHRUNK_HALF,
        }
    }

    fn next(self) -> Option<Self> {
        match self {
            Self::Raised => Some(Self::Unit),
            Self::Unit => Some(Self::Reduced),
            Self::Reduced => Some(Self::Shrunk),
            Self::Shrunk => None,
        }
    }

    /// `value`, held at this scale, held at `to` instead. Only called with
    /// `to` at or after `self`, so the factor shrinks. It is applied first:
    /// a value held at [`Reduced`](Self::Reduced) can overflow at its true
    /// scale on the way to [`Shrunk`](Self::Shrunk).
    fn convert(self, value: f64, to: Self) -> f64 {
        if self == to {
            value
        } else {
            value * to.factor() / self.factor()
        }
    }
}

/// 2^-32: a spread is reported as lost when its standard deviation is below
/// this fraction of [`NumericAccumulator::unheld`]. A deviation taken in `f64`
/// is off by about 2^-53 of the magnitudes it was taken from, so a standard
/// deviation at this fraction of them is still right to about 2^-21.
const UNHELD_RESOLUTION: f64 = f64::from_bits((1023 - 32) << 52);

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
            anchor: None,
            unheld: 0.0,
            running_mean: 0.0,
            m2: 0.0,
            m2_scale: M2Scale::Raised,
            sum: 0.0,
            compensation: 0.0,
            scaled: false,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            min_value: None,
            max_value: None,
        }
    }

    /// Fold one value in. Callers pass finite values only; non-finite input is
    /// filtered out at parse time so it can never reach the aggregates.
    #[inline]
    pub fn update(&mut self, value: f64) {
        debug_assert!(value.is_finite());
        self.note_extremes(ParsedNumber::Float(value));
        self.push(value, 0.0);
    }

    /// Keep `number` as the smallest or largest value where it is one. Only a
    /// value whose `f64` reaches the running `min` or `max` can be, so the
    /// exact comparison runs for those alone. Called before `min` and `max`
    /// take the value in, or after, in `merge`, for values already in them.
    #[inline]
    fn note_extremes(&mut self, number: ParsedNumber) {
        let value = number.to_f64();
        if value <= self.min
            && self
                .min_value
                .is_none_or(|min| number.value_cmp(min).is_lt())
        {
            self.min_value = Some(number);
        }
        if value >= self.max
            && self
                .max_value
                .is_none_or(|max| number.value_cmp(max).is_gt())
        {
            self.max_value = Some(number);
        }
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
            ParsedNumber::Decimal { exact, nearest } => {
                self.note_extremes(number);
                self.update_decimal(exact, nearest);
            }
            ParsedNumber::Unheld(value) => {
                debug_assert!(value.is_finite());
                self.note_extremes(number);
                self.push(value, 0.0);
                self.note_unheld(value);
            }
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
        self.note_extremes(ParsedNumber::Integer(value));
        let high = value as f64;
        let exact = ExactDecimal::integer(value);
        // Against an integer anchor, or none, the split below is already
        // exact, and integer columns keep the bits they had before #1149.
        if self.anchor.is_some_and(|anchor| anchor.scale > 0) {
            self.update_decimal(exact, high);
            return;
        }
        if self.count == 0 {
            self.anchor = Some(exact);
        }
        let low = (value - high as i128) as f64;
        self.push(high, low);
    }

    /// Fold in a decimal, `nearest` being its nearest `f64`.
    ///
    /// Only an anchor, or a deviation that cannot be exact, needs the
    /// remainder beyond `nearest`. The running sum goes without it otherwise:
    /// the mean is reported as an `f64`, which cannot hold it either.
    fn update_decimal(&mut self, exact: ExactDecimal, nearest: f64) {
        let anchor = match self.anchor {
            Some(anchor) => anchor,
            None if self.count == 0 => {
                self.anchor = Some(exact);
                self.push(nearest, exact.low_part(nearest));
                return;
            }
            None => {
                self.reanchor(exact, nearest, exact.low_part(nearest));
                exact
            }
        };
        self.count += 1;
        let (shifted, low) = match exact.minus(anchor) {
            Some(shifted) => (shifted, 0.0),
            None => {
                self.note_unheld(nearest);
                let low = exact.low_part(nearest);
                ((nearest - self.shift) - (self.shift_low - low), low)
            }
        };
        self.fold(shifted, nearest, low);
    }

    /// Move the shift to `exact`, whose nearest `f64` is `nearest` and whose
    /// remainder is `low`. M2 does not depend on the shift; the running mean
    /// is translated by the gap between the two, which is rounded once.
    fn reanchor(&mut self, exact: ExactDecimal, nearest: f64, low: f64) {
        self.running_mean += (self.shift - nearest) + (self.shift_low - low);
        self.shift = nearest;
        self.shift_low = low;
        self.anchor = Some(exact);
    }

    /// Record that a deviation involving `value` was taken in `f64`.
    fn note_unheld(&mut self, value: f64) {
        self.unheld = self.unheld.max(value.abs()).max(self.shift.abs());
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
        self.fold(shifted, high, low);
    }

    /// Fold in a value already counted, `shifted` being its deviation from
    /// the shift and `high + low` the value itself.
    #[inline]
    fn fold(&mut self, shifted: f64, high: f64, low: f64) {
        let delta = shifted - self.running_mean;
        self.running_mean += delta / self.count as f64;
        let deviation = shifted - self.running_mean;
        self.add_to_m2(0.0, M2Scale::Raised, |scale| match scale {
            M2Scale::Raised => (delta * UNDERFLOW_HALF) * (deviation * UNDERFLOW_HALF),
            M2Scale::Unit => delta * deviation,
            M2Scale::Reduced => (delta * OVERFLOW_SCALE) * deviation,
            M2Scale::Shrunk => (delta * SHRUNK_HALF) * (deviation * SHRUNK_HALF),
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
            anchor: None,
            unheld: 0.0,
            running_mean,
            m2,
            m2_scale,
            sum,
            compensation,
            scaled: false,
            min,
            max,
            // Lanes hold `f64` values only, so their extremes are exact.
            min_value: (count > 0).then_some(ParsedNumber::Float(min)),
            max_value: (count > 0).then_some(ParsedNumber::Float(max)),
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
    /// A total that still overflows at the shrunk scale exceeds
    /// `f64::MAX * 2^1000`, about 2e609, and the standard deviation is reported
    /// as overflowed with the variance.
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
    /// is held at, so it is finite where the variance overflows but M2, as
    /// held, does not (#1150). Infinite or NaN where M2 is, and NaN where the
    /// spread was lost to underflow.
    fn std_dev_over(&self, denominator: f64) -> f64 {
        if !self.m2.is_finite() {
            return self.m2.sqrt();
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
        (self.min < self.max && self.m2 < self.count as f64 * LOST_SPREAD_PER_VALUE)
            || self.spread_unresolved()
    }

    /// Whether the population standard deviation is too small to stand above
    /// the rounding of a deviation taken in `f64` that should have been exact.
    /// See [`UNHELD_RESOLUTION`].
    fn spread_unresolved(&self) -> bool {
        self.unheld > 0.0
            && (self.m2 / self.count as f64).max(0.0).sqrt() / self.m2_scale.root()
                < self.unheld * UNHELD_RESOLUTION
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
        // Between two anchors it is taken exactly where a decimal is involved;
        // two integer anchors are split exactly already.
        if self.anchor.is_none()
            && let Some(anchor) = other.anchor.filter(|anchor| anchor.scale > 0)
        {
            self.reanchor(anchor, other.shift, other.shift_low);
        }
        let split_gap = (other.shift - self.shift) - (self.shift_low - other.shift_low);
        let shift_gap = match (self.anchor, other.anchor) {
            (Some(mine), Some(theirs)) if mine.scale > 0 || theirs.scale > 0 => {
                theirs.minus(mine).unwrap_or_else(|| {
                    self.note_unheld(other.shift);
                    split_gap
                })
            }
            _ => split_gap,
        };
        self.unheld = self.unheld.max(other.unheld);
        let (a, b) = (self.count as f64, other.count as f64);
        let combined = a + b;
        let delta = shift_gap + (other.running_mean - self.running_mean);
        self.running_mean += delta * (b / combined);
        let weight = a * b / combined;
        self.add_to_m2(other.m2, other.m2_scale, |scale| match scale {
            M2Scale::Raised => (delta * UNDERFLOW_HALF) * (delta * UNDERFLOW_HALF) * weight,
            M2Scale::Unit => delta * delta * weight,
            M2Scale::Reduced => (delta * OVERFLOW_SCALE) * delta * weight,
            // The last scale: weighed before the square is completed, so a
            // weight below one, as two single values have, cannot let the
            // square overflow where the increment fits.
            M2Scale::Shrunk => {
                let gap = delta * SHRUNK_HALF;
                gap * (gap * weight)
            }
        });
        self.count += other.count;

        self.add_to_sum(other.sum, other.compensation, other.scaled);

        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        for extreme in [other.min_value, other.max_value].into_iter().flatten() {
            self.note_extremes(extreme);
        }
    }

    #[inline]
    pub fn count(&self) -> u64 {
        self.count
    }

    /// The smallest value as it was given, or `None` when nothing was
    /// accumulated. Exact where [`min`](Self::min) rounds (#1038).
    #[inline]
    pub fn min_value(&self) -> Option<ParsedNumber> {
        self.min_value
    }

    /// The largest value as it was given. See [`min_value`](Self::min_value).
    #[inline]
    pub fn max_value(&self) -> Option<ParsedNumber> {
        self.max_value
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
    /// finite where [`sample_variance`](Self::sample_variance) underflowed or
    /// overflowed and the standard deviation itself fits.
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
/// An integer of 2^53 or more in magnitude, within the 64-bit range, is kept
/// exact, so a text path accumulates it as an Arrow `Int64` column does
/// (#877). A longer literal that is not one of those, or an `f64` integer, is
/// kept as an [`ExactDecimal`] where it fits one (#1149), and is
/// [`Unheld`](Self::Unheld) where it does not: from [`TEXT_EXACT_DIGITS`]
/// significant digits for [`parse`](Self::parse), from
/// [`DECIMAL_EXACT_DIGITS`] for [`parse_decimal`](Self::parse_decimal).
/// Everything else is its `f64`.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum ParsedNumber {
    Float(f64),
    Integer(i128),
    Decimal {
        exact: ExactDecimal,
        nearest: f64,
    },
    /// A literal with more significant digits than an [`ExactDecimal`]
    /// holds, as its nearest `f64`.
    Unheld(f64),
}

/// Significant digits, counted as written from the first non-zero digit, from
/// which a text literal is kept exact. The shortest form of an `f64`, what
/// Python's `repr` and Rust write, has at most 17, so a literal of 18 cannot
/// be one, and reading it as a decimal leaves float data as it was; Rust pads
/// a large float with zeros to its units digit, and that integer still reads
/// as the float. Below 18 text is ambiguous: a float written out reads as that
/// float, for parity with the same column typed `Float64` (#783).
const TEXT_EXACT_DIGITS: usize = 18;

/// Significant digits from which a literal known to be a decimal, an Arrow or
/// Parquet decimal's rendering, is kept exact. An `f64` is within 2^-53 of a
/// value, so it is within 2^-19 of the last place of a literal of 10 digits,
/// as its CSV is, and of 15 digits only within about a tenth:
/// `9999999999999.01` to `.05` reported a standard deviation of 0.0175 for
/// 0.0171. From 16 digits two decimals can share one `f64`.
const DECIMAL_EXACT_DIGITS: usize = 11;

impl ParsedNumber {
    /// The number `text` holds, or `None` when it holds none or a non-finite
    /// one. Accepts exactly what `text.trim().parse::<f64>()` accepts.
    pub fn parse(text: &str) -> Option<Self> {
        Self::read(text, TEXT_EXACT_DIGITS, true)
    }

    /// [`parse`](Self::parse) for the rendering of a value typed as a
    /// decimal, which no float wrote: kept exact from fewer digits.
    pub fn parse_decimal(text: &str) -> Option<Self> {
        Self::read(text, DECIMAL_EXACT_DIGITS, false)
    }

    /// `may_be_float`: the text may be a float written out, which Rust does
    /// in full: 1.2345678901234567e20 is `123456789012345670000`. Past the
    /// 64-bit range such an integer reads as the float it writes, as before
    /// #1149, and only that case pays for the check.
    fn read(text: &str, exact_from: usize, may_be_float: bool) -> Option<Self> {
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
        // A plain integer below 2^53 is its `f64` exactly: ids and epoch
        // milliseconds skip the digit scan.
        if text.len() < exact_from
            || (value.abs() < EXACT_F64_INTEGER as f64 && !text.contains(['.', 'e', 'E']))
        {
            return Some(Self::Float(value));
        }
        Some(match DecimalLiteral::read(text) {
            Some(literal) if literal.written_digits >= exact_from => match literal.exact() {
                Some(exact)
                    if may_be_float
                        && exact.scale == 0
                        && i64::try_from(exact.unscaled).is_err()
                        && u64::try_from(exact.unscaled).is_err()
                        && literal.round_trips(value) =>
                {
                    Self::Float(value)
                }
                Some(exact) => Self::from_exact(exact, value),
                None if literal.round_trips(value) => Self::Float(value),
                None => Self::Unheld(value),
            },
            _ => Self::Float(value),
        })
    }

    fn from_exact(exact: ExactDecimal, nearest: f64) -> Self {
        match exact.as_integer() {
            Some(integer) if integer.unsigned_abs() <= EXACT_F64_INTEGER => Self::Float(nearest),
            Some(integer) if (i128::from(i64::MIN)..=i128::from(u64::MAX)).contains(&integer) => {
                Self::Integer(integer)
            }
            _ => Self::Decimal { exact, nearest },
        }
    }

    /// The nearest `f64`.
    pub fn to_f64(self) -> f64 {
        match self {
            Self::Float(value) | Self::Unheld(value) => value,
            Self::Integer(value) => value as f64,
            Self::Decimal { nearest, .. } => nearest,
        }
    }

    /// The value exactly, where it is held exactly.
    pub(crate) fn exact(self) -> Option<ExactDecimal> {
        match self {
            Self::Integer(value) => Some(ExactDecimal::integer(value)),
            Self::Decimal { exact, .. } => Some(exact),
            Self::Float(_) | Self::Unheld(_) => None,
        }
    }

    /// The key two values share when they are the same number, so counting
    /// them does not merge distinct values whose `f64` rounds alike (#1038).
    /// An integer keys as one however it was read: a short float, a 64-bit
    /// integer or a decimal with no fraction.
    pub(crate) fn value_key(self) -> ValueKey {
        match self {
            Self::Integer(value) => ValueKey::Integer(value),
            Self::Decimal { exact, .. } => exact.as_integer().map_or_else(
                || {
                    let (unscaled, scale) = exact.normalized();
                    ValueKey::Decimal(unscaled, scale)
                },
                ValueKey::Integer,
            ),
            Self::Float(value) | Self::Unheld(value) => {
                if value.fract() == 0.0 && value.abs() < i128::MAX as f64 {
                    // Signed zeros both become 0.
                    ValueKey::Integer(value as i128)
                } else {
                    ValueKey::Float(value.to_bits())
                }
            }
        }
    }

    /// Orders two values by their exact value, as a total order: by their
    /// nearest `f64`, then by what each holds beyond it, then, among values
    /// still tied, a float before a held value and held values exactly.
    ///
    /// The remainder is an `f64` too, rounded from the exact one, so it never
    /// orders two values against their exact order, but it can tie values it
    /// cannot tell apart: cents near 1.2e35, whose remainders are about 1e19,
    /// and every value past `i128`, whose remainder reads as 0. The last key
    /// resolves those. Each key is an order on its own, so the whole is one:
    /// comparing held values exactly first and the rest by remainder made
    /// two held values unequal while both equalled a float (#1185 review).
    pub(crate) fn value_cmp(self, other: Self) -> std::cmp::Ordering {
        self.to_f64()
            .total_cmp(&other.to_f64())
            .then_with(|| self.remainder().total_cmp(&other.remainder()))
            .then_with(|| match (self.exact(), other.exact()) {
                (Some(mine), Some(theirs)) => mine.exact_cmp(theirs),
                (mine, theirs) => mine.is_some().cmp(&theirs.is_some()),
            })
    }

    /// `self - other`, exactly where both are held and then rounded once;
    /// otherwise from their nearest `f64` and what each holds beyond it.
    pub(crate) fn minus_value(self, other: Self) -> f64 {
        if let (Some(mine), Some(theirs)) = (self.exact(), other.exact())
            && let Some(difference) = mine.minus(theirs)
        {
            return difference;
        }
        (self.to_f64() - other.to_f64()) + (self.remainder() - other.remainder())
    }

    /// The value as an exact decimal: an integer or a held decimal as it is,
    /// a float as the shortest decimal that reads back as it, which is the
    /// value itself for an integral float and the written decimal for one
    /// of a decimal column too short to be held. `None` for a literal too
    /// wide to hold.
    pub(crate) fn exact_value(self) -> Option<ExactDecimal> {
        match self {
            Self::Integer(_) | Self::Decimal { .. } => self.exact(),
            Self::Float(value) => ExactDecimal::from_text(&value.to_string()),
            Self::Unheld(_) => None,
        }
    }

    /// The value as a canonical decimal string (see
    /// `dataprof_core::ExactNumericStats`), or `None` where it has no exact
    /// form.
    pub fn exact_string(self) -> Option<String> {
        self.exact_value().map(ExactDecimal::canonical)
    }

    /// The exact value minus its nearest `f64`, zero where it is not held.
    fn remainder(self) -> f64 {
        self.exact()
            .map_or(0.0, |exact| exact.low_part(self.to_f64()))
    }
}

/// See [`ParsedNumber::value_key`]. A fraction keys as a decimal with no
/// trailing zeros, so `2.50` and `2.5` written as decimals are one value; a
/// fractional float keys by its bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ValueKey {
    Integer(i128),
    Decimal(i128, u32),
    Float(u64),
}

/// The largest scale an [`ExactDecimal`] takes: `10^scale` is formed in two
/// finite `f64` halves up to it, and every finite `f64` written out in full
/// needs less.
const MAX_DECIMAL_SCALE: u32 = 600;

/// A decimal held exactly, as `unscaled × 10^-scale`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExactDecimal {
    unscaled: i128,
    scale: u32,
}

impl ExactDecimal {
    fn integer(value: i128) -> Self {
        Self {
            unscaled: value,
            scale: 0,
        }
    }

    /// The value, when it is an integer. In `i64` where both fit: `i128`
    /// division is a library call.
    fn as_integer(self) -> Option<i128> {
        if self.scale == 0 {
            return Some(self.unscaled);
        }
        let unit = pow10(self.scale)?;
        if let (Ok(unscaled), Ok(unit)) = (i64::try_from(self.unscaled), i64::try_from(unit)) {
            return (unscaled % unit == 0).then_some(i128::from(unscaled / unit));
        }
        (self.unscaled % unit == 0).then_some(self.unscaled / unit)
    }

    /// The order of two decimals, exactly and whatever their scales: by sign,
    /// then by the digits of their magnitudes, so no product can overflow.
    pub(crate) fn exact_cmp(self, other: Self) -> std::cmp::Ordering {
        // Repeated values tie on every earlier key and land here, so the
        // common cases stay in `i128`: the digits are only the fallback.
        if self.scale == other.scale {
            return self.unscaled.cmp(&other.unscaled);
        }
        let scale = self.scale.max(other.scale);
        if let (Some(mine), Some(theirs)) = (
            scale_up(self.unscaled, scale - self.scale),
            scale_up(other.unscaled, scale - other.scale),
        ) {
            return mine.cmp(&theirs);
        }
        let sign = self.unscaled.signum().cmp(&other.unscaled.signum());
        if sign.is_ne() {
            return sign;
        }
        let magnitude = self.magnitude_digits().cmp(&other.magnitude_digits());
        if self.unscaled < 0 {
            magnitude.reverse()
        } else {
            magnitude
        }
    }

    /// The magnitude as (digit count of the whole part, whole digits,
    /// fraction digits padded to the longest fraction an `ExactDecimal`
    /// takes), which order as the magnitudes do.
    fn magnitude_digits(self) -> (usize, String, String) {
        let (unscaled, scale) = self.normalized();
        let digits = unscaled.unsigned_abs().to_string();
        let scale = scale as usize;
        let (whole, fraction) = if digits.len() > scale {
            let (whole, fraction) = digits.split_at(digits.len() - scale);
            (whole.to_string(), fraction.to_string())
        } else {
            (
                String::new(),
                format!("{}{digits}", "0".repeat(scale - digits.len())),
            )
        };
        let whole = whole.trim_start_matches('0').to_string();
        let width = MAX_DECIMAL_SCALE as usize;
        (whole.len(), whole, format!("{fraction:0<width$}"))
    }

    /// The decimal `text` holds exactly, or `None` when it is not a finite
    /// decimal literal or its digits leave `i128`.
    pub(crate) fn from_text(text: &str) -> Option<Self> {
        DecimalLiteral::read(text.trim())?.exact()
    }

    /// `self + other`, or `None` past `i128`.
    pub(crate) fn plus(self, other: Self) -> Option<Self> {
        let scale = self.scale.max(other.scale);
        let left = scale_up(self.unscaled, scale - self.scale)?;
        let right = scale_up(other.unscaled, scale - other.scale)?;
        Some(Self {
            unscaled: left.checked_add(right)?,
            scale,
        })
    }

    /// `self - other` exactly, or `None` past `i128`.
    pub(crate) fn exact_minus(self, other: Self) -> Option<Self> {
        other.negated()?.plus(self)
    }

    fn negated(self) -> Option<Self> {
        Some(Self {
            unscaled: self.unscaled.checked_neg()?,
            scale: self.scale,
        })
    }

    /// `self × numerator / 4` for `numerator` in `0..=4`: a Type 7 quartile
    /// weight, which `10^2 / 4` turns into an integer factor of 25.
    pub(crate) fn quarters(self, numerator: i128) -> Option<Self> {
        Some(Self {
            unscaled: self.unscaled.checked_mul(numerator * 25)?,
            scale: self.scale + 2,
        })
    }

    /// The canonical decimal spelling: no exponent, no trailing zeros in a
    /// fraction, `-` before a negative value, `0` for zero.
    pub(crate) fn canonical(self) -> String {
        let (unscaled, scale) = self.normalized();
        let digits = unscaled.unsigned_abs().to_string();
        let sign = if unscaled < 0 { "-" } else { "" };
        let scale = scale as usize;
        if scale == 0 {
            return format!("{sign}{digits}");
        }
        let digits = if digits.len() <= scale {
            format!("{}{digits}", "0".repeat(scale + 1 - digits.len()))
        } else {
            digits
        };
        let (whole, fraction) = digits.split_at(digits.len() - scale);
        format!("{sign}{whole}.{fraction}")
    }

    /// The unscaled digits and scale with trailing zeros of the fraction
    /// removed, one form for every way of writing the same decimal.
    fn normalized(self) -> (i128, u32) {
        let (mut unscaled, mut scale) = (self.unscaled, self.scale);
        while scale > 0 && unscaled % 10 == 0 {
            unscaled /= 10;
            scale -= 1;
        }
        (unscaled, scale)
    }

    /// `self - other`, exact and then rounded, or `None` when bringing both to
    /// one scale, or their difference, leaves `i128`.
    pub(crate) fn minus(self, other: Self) -> Option<f64> {
        let scale = self.scale.max(other.scale);
        let left = scale_up(self.unscaled, scale - self.scale)?;
        let right = scale_up(other.unscaled, scale - other.scale)?;
        Some(scaled_to_f64(left.checked_sub(right)?, scale))
    }

    /// `self - nearest`, `nearest` being its nearest `f64`: the difference
    /// from its whole part is exact and rounded once, and the fraction of
    /// `nearest` then cancels without rounding. Exact for an integer
    /// `nearest`; otherwise off by at most 2^-53 of the fraction, which is
    /// within the rounding of any `f64` beside it. Zero where the whole part
    /// leaves `i128`.
    pub(crate) fn low_part(self, nearest: f64) -> f64 {
        let whole = nearest.trunc();
        if whole.abs() >= i128::MAX as f64 {
            return 0.0;
        }
        self.minus(Self::integer(whole as i128))
            .map_or(0.0, |above| above - (nearest - whole))
    }
}

/// `10^k` for every `k` an `i128` holds.
const POW10: [i128; 39] = {
    let mut table = [1_i128; 39];
    let mut k = 1;
    while k < 39 {
        table[k] = table[k - 1] * 10;
        k += 1;
    }
    table
};

/// `10^k` as an `f64`, exact for each `k` here.
const POW10_F64: [f64; 23] = {
    let mut table = [1.0_f64; 23];
    let mut k = 1;
    while k < 23 {
        table[k] = table[k - 1] * 10.0;
        k += 1;
    }
    table
};

fn pow10(exponent: u32) -> Option<i128> {
    POW10.get(exponent as usize).copied()
}

/// `value × 10^exponent`, or `None` past `i128`. Factors below 2^63 multiply
/// without the overflow check, a library call for `i128`.
fn scale_up(value: i128, exponent: u32) -> Option<i128> {
    if exponent == 0 {
        return Some(value);
    }
    let factor = pow10(exponent)?;
    const HALF: u128 = 1 << 63;
    if value.unsigned_abs() < HALF && factor.unsigned_abs() < HALF {
        Some(value * factor)
    } else {
        value.checked_mul(factor)
    }
}

/// `unscaled × 10^-scale`, the division rounded once more. `10^scale` is
/// exact up to 10^22, and taken in two halves beyond it so that neither
/// overflows.
fn scaled_to_f64(unscaled: i128, scale: u32) -> f64 {
    // Converting from `i64` is one instruction, from `i128` a library call.
    let value = i64::try_from(unscaled).map_or(unscaled as f64, |small| small as f64);
    if let Some(&unit) = POW10_F64.get(scale as usize) {
        return value / unit;
    }
    let half = scale / 2;
    value / 10_f64.powi(half as i32) / 10_f64.powi((scale - half) as i32)
}

/// The digits of a literal `str::parse::<f64>` accepted: an optional sign,
/// digits with an optional point, an optional exponent.
struct DecimalLiteral<'a> {
    negative: bool,
    /// The digits and the point, without the sign or the exponent.
    mantissa: &'a str,
    exponent: i64,
    /// The digits as an integer, `None` when they overflow `i128`.
    digits: Option<i128>,
    /// Digits from the first non-zero one to the last written.
    written_digits: usize,
    /// Digits after the point, less the exponent.
    scale: i64,
}

impl<'a> DecimalLiteral<'a> {
    /// `None` for a literal that is not of that form (`inf` and `nan` are
    /// filtered out before), or whose exponent leaves `i64`.
    fn read(text: &'a str) -> Option<Self> {
        let (negative, rest) = match text.as_bytes().first()? {
            b'-' => (true, &text[1..]),
            b'+' => (false, &text[1..]),
            _ => (false, text),
        };
        // Digits gather in a `u64` while they fit, 19 of them; checked `i128`
        // arithmetic is a library call per digit.
        let (mut small, mut wide) = (0_u64, None::<Option<i128>>);
        let (mut written, mut fraction, mut point) = (0, 0_i64, false);
        let (mut mantissa, mut exponent) = (rest, 0);
        for (at, byte) in rest.bytes().enumerate() {
            match byte {
                b'.' => point = true,
                b'e' | b'E' => {
                    mantissa = &rest[..at];
                    // decode-audit: no-data — `f64` parsing accepted the text,
                    // so only an exponent past `i64` fails here: the literal
                    // has no exact form and reads as the `f64` already parsed.
                    exponent = rest[at + 1..].parse::<i64>().ok()?;
                    break;
                }
                b'0'..=b'9' => {
                    let digit = byte - b'0';
                    match &mut wide {
                        None if small < 1_000_000_000_000_000_000 => {
                            small = small * 10 + u64::from(digit);
                        }
                        None => {
                            wide = Some(
                                i128::from(small)
                                    .checked_mul(10)
                                    .and_then(|d| d.checked_add(i128::from(digit))),
                            );
                        }
                        Some(value) => {
                            *value = value
                                .and_then(|d| d.checked_mul(10)?.checked_add(i128::from(digit)));
                        }
                    }
                    if written > 0 || digit != 0 {
                        written += 1;
                    }
                    if point {
                        fraction += 1;
                    }
                }
                _ => return None,
            }
        }
        Some(Self {
            negative,
            mantissa,
            exponent,
            digits: wide.unwrap_or(Some(i128::from(small))),
            written_digits: written,
            scale: fraction.checked_sub(exponent)?,
        })
    }

    /// The value as an [`ExactDecimal`], or `None` where it does not fit one.
    fn exact(&self) -> Option<ExactDecimal> {
        // decode-audit: no-data — a scale past `u32` is past both `i128` and
        // `MAX_DECIMAL_SCALE`, so the literal has no exact form and reads as
        // its `f64`, as any literal too wide to hold does.
        let mut unscaled = self.digits?;
        let scale = if self.scale < 0 {
            unscaled = scale_up(unscaled, u32::try_from(-self.scale).ok()?)?;
            0
        } else {
            u32::try_from(self.scale)
                .ok()
                .filter(|&scale| scale <= MAX_DECIMAL_SCALE)?
        };
        Some(ExactDecimal {
            unscaled: if self.negative { -unscaled } else { unscaled },
            scale,
        })
    }

    /// Whether this literal has the value of the shortest decimal that reads
    /// back as `value`, its nearest `f64`: what `f64` formatting writes,
    /// padded with zeros to the units digit when it writes no exponent. Such a
    /// literal reads as that `f64`, as it did before #1149: an Arrow `Float64`
    /// sample of 1e300 is written out in 301 digits. Only called for a literal
    /// too wide for an [`ExactDecimal`], or for text of an integer past the
    /// 64-bit range, so the allocations stay off the common path.
    fn round_trips(&self, value: f64) -> bool {
        let mut significant = String::new();
        let (mut whole, mut leading, mut point) = (0_i64, 0_i64, false);
        for byte in self.mantissa.bytes() {
            match byte {
                b'.' => point = true,
                _ => {
                    if !point {
                        whole += 1;
                    }
                    if significant.is_empty() && byte == b'0' {
                        leading += 1;
                    } else {
                        significant.push(char::from(byte));
                    }
                }
            }
        }
        let shortest = format!("{:e}", value.abs());
        let Some((digits, exponent)) = shortest.split_once('e') else {
            return false;
        };
        let digits: String = digits.chars().filter(char::is_ascii_digit).collect();
        significant.trim_end_matches('0') == digits.trim_end_matches('0')
            && Some(
                exponent
                    .parse::<i64>()
                    .expect("`{:e}` writes a decimal exponent"),
            ) == (whole - leading - 1).checked_add(self.exponent)
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
        // A decimal's integer written with a point or an exponent is the
        // same integer; in text it may be a float written out, and reads as one.
        for text in ["9007199254740993.0", "9.007199254740993e15"] {
            assert_eq!(
                ParsedNumber::parse_decimal(text),
                Some(ParsedNumber::Integer(9_007_199_254_740_993)),
                "{text:?}"
            );
            assert_eq!(
                ParsedNumber::parse(text),
                Some(ParsedNumber::Float(9_007_199_254_740_992.0)),
                "{text:?}"
            );
        }
        // Exact as `f64` already, or short enough for its `f64` to stand in.
        assert_eq!(ParsedNumber::parse("42"), Some(ParsedNumber::Float(42.0)));
        assert_eq!(ParsedNumber::parse("1e20"), Some(ParsedNumber::Float(1e20)));
        assert_eq!(
            ParsedNumber::parse("1000000000000000.00"),
            Some(ParsedNumber::Float(1e15))
        );
        assert_eq!(
            ParsedNumber::parse_decimal("0.1234567891"),
            Some(ParsedNumber::Float(0.1234567891))
        );
        // The shortest forms of floats, which have up to 17 digits (#783).
        for value in [
            0.30000000000000004,
            1000000000123.4567,
            1.2345678901234567e-7,
            1.2345678901234567e20,
        ] {
            for text in [
                format!("{value}"),
                format!("{value:?}"),
                format!("{value:e}"),
            ] {
                assert_eq!(
                    ParsedNumber::parse(&text),
                    Some(ParsedNumber::Float(value)),
                    "{text:?}"
                );
            }
        }
        for text in ["", "N/A", "inf", "NaN", "1e400"] {
            assert_eq!(ParsedNumber::parse(text), None, "{text:?}");
            assert_eq!(ParsedNumber::parse_decimal(text), None, "{text:?}");
        }
    }

    #[test]
    fn parsed_numbers_keep_long_decimals_exact() {
        // (text, unscaled, scale, whether text alone is held exactly)
        for (text, unscaled, scale, in_text) in [
            ("1000000000000000.01", 100_000_000_000_000_001, 2, true),
            ("-1000000000000000.01", -100_000_000_000_000_001, 2, true),
            ("0.123456789012345678", 123_456_789_012_345_678, 18, true),
            ("0.1234567890123456", 1_234_567_890_123_456, 16, false),
            ("9999999999999.01", 999_999_999_999_901, 2, false),
            ("0.12345678912", 12_345_678_912, 11, false),
            ("1.234567890123456e-3", 1_234_567_890_123_456, 18, false),
            // Beyond 64 bits an integer is a decimal of scale 0.
            ("18446744073709551616", 18_446_744_073_709_551_616, 0, true),
            // How Rust writes the float 1.2345678901234567e20: text reads it
            // as that float, a decimal column as the integer it shows.
            (
                "123456789012345670000",
                123_456_789_012_345_670_000,
                0,
                false,
            ),
            (
                "1.8446744073709551616e19",
                18_446_744_073_709_551_616,
                0,
                true,
            ),
        ] {
            let expected = Some(ExactDecimal { unscaled, scale });
            let decimal = ParsedNumber::parse_decimal(text).unwrap();
            assert_eq!(decimal.exact(), expected, "{text:?}");
            let parsed = ParsedNumber::parse(text).unwrap();
            assert_eq!(parsed.exact(), expected.filter(|_| in_text), "{text:?}");
            let nearest: f64 = text.parse().unwrap();
            assert_eq!((decimal.to_f64(), parsed.to_f64()), (nearest, nearest));
        }

        // Too wide for `i128`: unheld, unless it is how an `f64` writes out.
        let wide = format!("1{}1", "0".repeat(74));
        assert_eq!(ParsedNumber::parse(&wide), Some(ParsedNumber::Unheld(1e75)));
        for value in [1e300, 1.2345678901234567e300, 3e-300, -1.5e200] {
            let written = value.to_string();
            assert!(written.len() > 40, "{written}");
            assert_eq!(
                ParsedNumber::parse(&written),
                Some(ParsedNumber::Float(value)),
                "{written}"
            );
        }
    }

    fn accumulate_text(cells: &[&str]) -> NumericAccumulator {
        cells
            .iter()
            .map(|cell| ParsedNumber::parse(cell).expect("a number"))
            .collect()
    }

    /// One `f64` holds all of these, and their variance was 0.0 (#1149).
    #[test]
    fn decimals_beyond_f64_keep_their_spread_in_every_order_and_split() {
        let cells = [
            "1000000000000000.01",
            "1000000000000000.05",
            "1000000000000000.02",
            // `f64` values beside them, one integer, one not: they set the
            // shift when they come first, and are moved off it after.
            "1000000000000000",
            "1000000000000000.5",
        ];
        let cents = [1.0, 5.0, 2.0, 0.0, 50.0];
        let mean = cents.iter().sum::<f64>() / 5.0;
        let expected = cents.iter().map(|c| (c - mean).powi(2)).sum::<f64>() / 4.0 / 1e4;
        let check = |label: &str, accumulator: NumericAccumulator| {
            let variance = accumulator.sample_variance();
            assert!(
                (variance - expected).abs() <= expected * 1e-12,
                "{label}: variance {variance}, exact {expected}"
            );
        };
        for rotation in 0..cells.len() {
            let mut order = cells;
            order.rotate_left(rotation);
            check(&format!("order {order:?}"), accumulate_text(&order));
            for split in 1..order.len() {
                let (left, right) = order.split_at(split);
                let mut merged = accumulate_text(left);
                merged.merge(&accumulate_text(right));
                check(&format!("{left:?} + {right:?}"), merged);
            }
        }
    }

    #[test]
    fn decimals_and_wide_integers_share_an_exact_anchor() {
        // A decimal(20, 2) column: whole values parse as integers.
        let cells = [
            "12345678901234567.01",
            "12345678901234567",
            "12345678901234567.00",
            "12345678901234568.03",
        ];
        let cents = [1.0, 0.0, 0.0, 103.0];
        let mean = cents.iter().sum::<f64>() / 4.0;
        let expected = cents.iter().map(|c| (c - mean).powi(2)).sum::<f64>() / 3.0 / 1e4;
        for rotation in 0..cells.len() {
            let mut order = cells;
            order.rotate_left(rotation);
            let variance = accumulate_text(&order).sample_variance();
            assert!(
                (variance - expected).abs() <= expected * 1e-12,
                "{order:?}: variance {variance}, exact {expected}"
            );
        }
    }

    #[test]
    fn a_spread_below_the_rounding_of_unheld_values_is_lost() {
        // 10^75 and 100 above it: too wide to hold, and one `f64`.
        let base = format!("1{}", "0".repeat(75));
        let above = format!("1{}100", "0".repeat(72));
        let accumulator = accumulate_text(&[&base, &above, &above]);
        assert!(accumulator.sample_variance().is_nan());
        assert!(accumulator.sample_std_dev().is_nan());

        // A spread far above that rounding is still reported.
        let accumulator = accumulate_text(&[&base, &above, "1e74"]);
        assert!(accumulator.sample_variance().is_finite());
        assert!(accumulator.sample_std_dev() > 1e74);
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

        // Ten times the spread is a variance of about 1e310: still overflowed,
        // but its standard deviation, ten times the one above, fits (#1150).
        let wider: Vec<f64> = values.iter().map(|value| value * 10.0).collect();
        let mut merged = accumulate(&wider[..37]);
        merged.merge(&accumulate(&wider[37..]));
        let simd = crate::acceleration::simd::compute_stats_simd(&wider);
        let std_dev = 10.0 * exact.sqrt();
        for (path, accumulator) in [
            ("single", accumulate(&wider)),
            ("merged", merged),
            ("simd", simd),
        ] {
            assert!(!accumulator.sample_variance().is_finite(), "{path}");
            assert!(!accumulator.population_variance().is_finite(), "{path}");
            let got = accumulator.sample_std_dev();
            assert!(
                (got - std_dev).abs() <= std_dev * 1e-14,
                "{path}: std_dev {got}, exact {std_dev}"
            );
            assert!(accumulator.population_std_dev().is_finite(), "{path}");
        }
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

    /// At the shrunk scale a merged increment is the square of a scaled gap
    /// times a weight, which is 0.5 for two single values. Squared before
    /// weighing, the gap of `0.0` and `5e304` overflowed although the weighted
    /// increment fits, and the merge lost a standard deviation a single pass
    /// reports.
    #[test]
    fn merging_single_values_keeps_a_standard_deviation_that_fits() {
        let single = accumulate(&[0.0, 5e304]);
        let mut merged = accumulate(&[0.0]);
        merged.merge(&accumulate(&[5e304]));
        let expected = 5e304 / 2f64.sqrt();
        for (path, accumulator) in [("single", single), ("merged", merged)] {
            let got = accumulator.sample_std_dev();
            assert!(
                (got - expected).abs() <= expected * 1e-15,
                "{path}: std_dev {got:e}, exact {expected:e}"
            );
        }
    }

    /// Decimals whose common scale leaves `i128` are ordered by their digits,
    /// sign first, as the ones that fit are by their unscaled values.
    #[test]
    fn exact_cmp_orders_decimals_past_a_common_scale() {
        use std::cmp::Ordering::{Equal, Greater, Less};
        let decimal = |unscaled: i128, scale: u32| ExactDecimal { unscaled, scale };
        let big = decimal(10_i128.pow(37), 0);
        let tiny = decimal(1, 37);
        assert_eq!(big.exact_cmp(tiny), Greater);
        assert_eq!(tiny.exact_cmp(big), Less);
        assert_eq!(
            decimal(-(10_i128.pow(37)), 0).exact_cmp(decimal(-1, 37)),
            Less
        );
        assert_eq!(decimal(-1, 37).exact_cmp(big), Less);
        assert_eq!(decimal(250, 2).exact_cmp(decimal(25, 1)), Equal);
        assert_eq!(decimal(0, 5).exact_cmp(decimal(0, 0)), Equal);
    }

    /// One spelling per value: no exponent, no trailing zeros in a fraction,
    /// a leading zero before a fraction below one (#1038).
    #[test]
    fn canonical_decimals_have_one_spelling() {
        for (text, canonical) in [
            ("0", "0"),
            ("-0.0", "0"),
            ("12.30", "12.3"),
            ("-0.05", "-0.05"),
            ("1e20", "100000000000000000000"),
            ("1.5e-3", "0.0015"),
            ("18446744073709551615", "18446744073709551615"),
            ("99999999999999999999.990", "99999999999999999999.99"),
        ] {
            let exact = ExactDecimal::from_text(text).unwrap_or_else(|| panic!("{text}"));
            assert_eq!(exact.canonical(), canonical, "{text}");
        }
        // A quarter of a gap is exact, beside a base past 2^53.
        let base = ExactDecimal::from_text("9007199254740993").unwrap();
        let gap = ExactDecimal::from_text("3").unwrap();
        assert_eq!(
            base.plus(gap.quarters(1).unwrap()).unwrap().canonical(),
            "9007199254740993.75"
        );
    }

    /// The smallest and largest value as given, past what `min` and `max`
    /// hold, through single values, merges and SIMD lanes (#1038).
    #[test]
    fn extremes_are_kept_as_given() {
        let mut accumulator = NumericAccumulator::new();
        for value in ["9007199254740995", "9007199254740993", "9007199254740997"] {
            accumulator.update_number(ParsedNumber::parse(value).unwrap());
        }
        let mut merged = accumulate(&[1.5, -2.0]);
        merged.merge(&accumulator);
        let text = |number: Option<ParsedNumber>| number.and_then(ParsedNumber::exact_string);
        assert_eq!(
            text(accumulator.min_value()).as_deref(),
            Some("9007199254740993")
        );
        assert_eq!(
            text(accumulator.max_value()).as_deref(),
            Some("9007199254740997")
        );
        assert_eq!(text(merged.min_value()).as_deref(), Some("-2"));
        assert_eq!(
            text(merged.max_value()).as_deref(),
            Some("9007199254740997")
        );
        let simd = crate::acceleration::simd::compute_stats_simd(&[3.0, -1.25, 8.0, 0.5, 7.0]);
        assert_eq!(text(simd.min_value()).as_deref(), Some("-1.25"));
        assert_eq!(text(simd.max_value()).as_deref(), Some("8"));
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
