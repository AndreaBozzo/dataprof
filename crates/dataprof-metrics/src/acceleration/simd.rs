/// SIMD-accelerated numerical computations for data profiling
/// Uses the `wide` crate for portable SIMD operations
use wide::*;

/// SIMD-accelerated statistical computations
pub struct SimdStats {
    pub count: usize,
    pub sum: f64,
    pub sum_squares: f64,
    pub min: f64,
    pub max: f64,
}

impl Default for SimdStats {
    fn default() -> Self {
        Self::new()
    }
}

impl SimdStats {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_squares: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    pub fn mean(&self) -> f64 {
        if self.count > 0 {
            self.sum / self.count as f64
        } else {
            0.0
        }
    }

    pub fn variance(&self) -> f64 {
        if self.count <= 1 {
            return 0.0;
        }

        let n = self.count as f64;
        let mean = self.mean();
        (self.sum_squares - n * mean * mean) / n
    }

    pub fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }
}

/// Compute basic statistics using SIMD operations
pub fn compute_stats_simd(values: &[f64]) -> SimdStats {
    if values.is_empty() {
        return SimdStats::new();
    }

    let mut stats = SimdStats::new();
    stats.count = values.len();

    // Process in SIMD chunks of 4 f64 values
    let chunk_size = 4;
    let chunks = values.chunks_exact(chunk_size);
    let remainder = chunks.remainder();

    // SIMD variables for accumulation
    let mut sum_vec = f64x4::splat(0.0);
    let mut sum_squares_vec = f64x4::splat(0.0);
    let mut min_vec = f64x4::splat(f64::INFINITY);
    let mut max_vec = f64x4::splat(f64::NEG_INFINITY);

    // Process SIMD chunks
    for chunk in chunks {
        let vec = f64x4::new([chunk[0], chunk[1], chunk[2], chunk[3]]);

        // Accumulate sum
        sum_vec += vec;

        // Accumulate sum of squares
        sum_squares_vec += vec * vec;

        // Update min/max
        min_vec = min_vec.min(vec);
        max_vec = max_vec.max(vec);
    }

    // Reduce SIMD vectors to scalar values
    let sum_array: [f64; 4] = sum_vec.to_array();
    let sum_squares_array: [f64; 4] = sum_squares_vec.to_array();
    let min_array: [f64; 4] = min_vec.to_array();
    let max_array: [f64; 4] = max_vec.to_array();

    stats.sum = sum_array[0] + sum_array[1] + sum_array[2] + sum_array[3];
    stats.sum_squares =
        sum_squares_array[0] + sum_squares_array[1] + sum_squares_array[2] + sum_squares_array[3];
    stats.min = min_array[0]
        .min(min_array[1])
        .min(min_array[2])
        .min(min_array[3]);
    stats.max = max_array[0]
        .max(max_array[1])
        .max(max_array[2])
        .max(max_array[3]);

    // Process remainder values (non-SIMD)
    for &value in remainder {
        stats.sum += value;
        stats.sum_squares += value * value;
        stats.min = stats.min.min(value);
        stats.max = stats.max.max(value);
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
pub fn compute_stats_auto(values: &[f64]) -> SimdStats {
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
fn compute_stats_fallback(values: &[f64]) -> SimdStats {
    let mut stats = SimdStats::new();
    stats.count = values.len();

    if values.is_empty() {
        return stats;
    }

    stats.min = values[0];
    stats.max = values[0];

    for &value in values {
        stats.sum += value;
        stats.sum_squares += value * value;
        stats.min = stats.min.min(value);
        stats.max = stats.max.max(value);
    }

    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_stats() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let stats = compute_stats_simd(&values);

        assert_eq!(stats.count, 8);
        assert!((stats.sum - 36.0).abs() < 1e-10);
        assert_eq!(stats.min, 1.0);
        assert_eq!(stats.max, 8.0);
        assert!((stats.mean() - 4.5).abs() < 1e-10);
    }

    #[test]
    fn test_simd_vs_fallback() {
        let values: Vec<f64> = (1..=100).map(|x| x as f64).collect();

        let simd_stats = compute_stats_simd(&values);
        let fallback_stats = compute_stats_fallback(&values);

        assert!((simd_stats.sum - fallback_stats.sum).abs() < 1e-10);
        assert!((simd_stats.min - fallback_stats.min).abs() < 1e-10);
        assert!((simd_stats.max - fallback_stats.max).abs() < 1e-10);
        assert!((simd_stats.mean() - fallback_stats.mean()).abs() < 1e-10);
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
