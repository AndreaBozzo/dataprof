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

/// Fast parallel sum using SIMD
pub fn sum_simd(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let chunk_size = 4;
    let chunks = values.chunks_exact(chunk_size);
    let remainder = chunks.remainder();

    let mut sum_vec = f64x4::splat(0.0);

    for chunk in chunks {
        let vec = f64x4::new([chunk[0], chunk[1], chunk[2], chunk[3]]);
        sum_vec += vec;
    }

    let sum_array: [f64; 4] = sum_vec.to_array();
    let mut total = sum_array[0] + sum_array[1] + sum_array[2] + sum_array[3];

    // Add remainder
    for &value in remainder {
        total += value;
    }

    total
}

/// Fast min/max finding using SIMD
pub fn min_max_simd(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0);
    }

    if values.len() == 1 {
        return (values[0], values[0]);
    }

    let chunk_size = 4;
    let chunks = values.chunks_exact(chunk_size);
    let remainder = chunks.remainder();

    let mut min_vec = f64x4::splat(f64::INFINITY);
    let mut max_vec = f64x4::splat(f64::NEG_INFINITY);

    for chunk in chunks {
        let vec = f64x4::new([chunk[0], chunk[1], chunk[2], chunk[3]]);
        min_vec = min_vec.min(vec);
        max_vec = max_vec.max(vec);
    }

    let min_array: [f64; 4] = min_vec.to_array();
    let max_array: [f64; 4] = max_vec.to_array();

    let mut min_val = min_array[0]
        .min(min_array[1])
        .min(min_array[2])
        .min(min_array[3]);
    let mut max_val = max_array[0]
        .max(max_array[1])
        .max(max_array[2])
        .max(max_array[3]);

    // Process remainder
    for &value in remainder {
        min_val = min_val.min(value);
        max_val = max_val.max(value);
    }

    (min_val, max_val)
}

/// SIMD-accelerated dot product (useful for correlation computations)
pub fn dot_product_simd(a: &[f64], b: &[f64]) -> f64 {
    assert_eq!(a.len(), b.len());

    if a.is_empty() {
        return 0.0;
    }

    let chunk_size = 4;
    let chunks_a = a.chunks_exact(chunk_size);
    let chunks_b = b.chunks_exact(chunk_size);
    let remainder_a = chunks_a.remainder();
    let remainder_b = chunks_b.remainder();

    let mut dot_vec = f64x4::splat(0.0);

    for (chunk_a, chunk_b) in chunks_a.zip(chunks_b) {
        let vec_a = f64x4::new([chunk_a[0], chunk_a[1], chunk_a[2], chunk_a[3]]);
        let vec_b = f64x4::new([chunk_b[0], chunk_b[1], chunk_b[2], chunk_b[3]]);

        dot_vec += vec_a * vec_b;
    }

    let dot_array: [f64; 4] = dot_vec.to_array();
    let mut result = dot_array[0] + dot_array[1] + dot_array[2] + dot_array[3];

    // Process remainder
    for (&val_a, &val_b) in remainder_a.iter().zip(remainder_b.iter()) {
        result += val_a * val_b;
    }

    result
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
    fn test_min_max_simd() {
        let values = vec![3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0];
        let (min, max) = min_max_simd(&values);

        assert_eq!(min, 1.0);
        assert_eq!(max, 9.0);
    }

    #[test]
    fn test_dot_product_simd() {
        let a = vec![1.0, 2.0, 3.0, 4.0];
        let b = vec![5.0, 6.0, 7.0, 8.0];
        let dot = dot_product_simd(&a, &b);

        // 1*5 + 2*6 + 3*7 + 4*8 = 5 + 12 + 21 + 32 = 70
        assert!((dot - 70.0).abs() < 1e-10);
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
