/// SIMD-accelerated numerical computations for data profiling
/// Uses the `wide` crate for portable SIMD operations
use crate::stats::NumericAccumulator;
use wide::*;

/// Values processed per SIMD step, one per `f64x4` lane.
const LANES: usize = 4;

/// Accumulate stable numeric aggregates over `values`, four lanes at a time.
///
/// Each lane runs the same Welford and compensated-sum recurrences as
/// [`NumericAccumulator::update`] over every fourth value, and the four lane
/// accumulators are merged at the end. Accumulating a naive `sum` and
/// `sum_squares` here would vectorize just as well and lose the small spread of
/// a large-offset column entirely, which is what this path used to do.
pub fn compute_stats_simd(values: &[f64]) -> NumericAccumulator {
    let (chunks, remainder) = values.as_chunks::<LANES>();

    // One count for all four lanes: every step feeds every lane, and the tail
    // goes through the scalar path below.
    let mut count = 0u64;
    let mut mean = f64x4::splat(0.0);
    let mut m2 = f64x4::splat(0.0);
    let mut sum = f64x4::splat(0.0);
    let mut compensation = f64x4::splat(0.0);
    let mut min = f64x4::splat(f64::INFINITY);
    let mut max = f64x4::splat(f64::NEG_INFINITY);

    for chunk in chunks {
        let value = f64x4::new(*chunk);
        count += 1;

        let delta = value - mean;
        mean += delta / f64x4::splat(count as f64);
        m2 += delta * (value - mean);

        // Knuth's two-sum, lane-wise.
        let total = sum + value;
        let value_virtual = total - sum;
        compensation += (sum - (total - value_virtual)) + (value - value_virtual);
        sum = total;

        min = min.min(value);
        max = max.max(value);
    }

    let (mean, m2) = (mean.to_array(), m2.to_array());
    let (sum, compensation) = (sum.to_array(), compensation.to_array());
    let (min, max) = (min.to_array(), max.to_array());

    let mut stats = NumericAccumulator::new();
    for lane in 0..LANES {
        stats.merge(&NumericAccumulator::from_lane_state(
            count,
            mean[lane],
            m2[lane],
            sum[lane],
            compensation[lane],
            min[lane],
            max[lane],
        ));
    }

    for &value in remainder {
        stats.update(value);
    }

    stats
}

/// Check if SIMD is beneficial for the given data size
pub fn should_use_simd(data_size: usize) -> bool {
    // SIMD is beneficial for larger datasets due to setup overhead
    data_size >= 64
}

/// Auto-choose between SIMD and regular computation
pub fn compute_stats_auto(values: &[f64]) -> NumericAccumulator {
    if should_use_simd(values.len()) && is_simd_available() {
        compute_stats_simd(values)
    } else {
        compute_stats_fallback(values)
    }
}

/// Check if SIMD is available on current platform
pub fn is_simd_available() -> bool {
    // The wide crate handles platform detection internally
    // For now, we assume SIMD is available on most modern platforms
    true
}

/// Fallback non-SIMD implementation
fn compute_stats_fallback(values: &[f64]) -> NumericAccumulator {
    values.iter().copied().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_stats() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let stats = compute_stats_simd(&values);

        assert_eq!(stats.count(), 8);
        assert_eq!(stats.min(), Some(1.0));
        assert_eq!(stats.max(), Some(8.0));
        assert!((stats.mean() - 4.5).abs() < 1e-10);
        assert!((stats.sample_variance() - 6.0).abs() < 1e-10);
    }

    #[test]
    fn test_simd_vs_fallback() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();

        let simd_stats = compute_stats_simd(&values);
        let fallback_stats = compute_stats_fallback(&values);

        assert_eq!(simd_stats.count(), fallback_stats.count());
        assert_eq!(simd_stats.min(), fallback_stats.min());
        assert_eq!(simd_stats.max(), fallback_stats.max());
        assert!((simd_stats.mean() - fallback_stats.mean()).abs() < 1e-10);
        assert!((simd_stats.sample_variance() - fallback_stats.sample_variance()).abs() < 1e-10);
    }

    /// The lane accumulation has to be as stable as the scalar one: a column
    /// large enough to reach SIMD is exactly where a naive sum does its damage.
    #[test]
    fn simd_lanes_stay_stable_on_hard_inputs() {
        // A small spread on a large offset, and a tail that is not a whole
        // number of lanes, so both halves of the routine are exercised.
        let offset: Vec<f64> = (0..102).map(|i| 1e9 + (i % 4) as f64).collect();
        let expected = compute_stats_fallback(&offset);
        let actual = compute_stats_simd(&offset);
        assert!(expected.sample_variance() > 1.0);
        assert!((actual.sample_variance() - expected.sample_variance()).abs() < 1e-6);

        // Large values that cancel, leaving a mean far smaller than any of them.
        let mut cancelling = [1e16, -1e16].repeat(50);
        cancelling.push(1.0);
        let stats = compute_stats_simd(&cancelling);
        assert_eq!(stats.count(), 101);
        assert!((stats.mean() - 1.0 / 101.0).abs() < 1e-12);
    }

    #[test]
    fn test_auto_selection() {
        let small_values = vec![1.0, 2.0, 3.0];
        let large_values: Vec<f64> = (1..=1000).map(|x| x as f64).collect();

        // Both should work correctly regardless of implementation chosen
        let small_stats = compute_stats_auto(&small_values);
        let large_stats = compute_stats_auto(&large_values);

        assert!((small_stats.mean() - 2.0).abs() < 1e-10);
        assert!((large_stats.mean() - 500.5).abs() < 1e-10);
    }
}
