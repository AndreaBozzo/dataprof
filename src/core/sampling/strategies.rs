use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone)]
pub enum SamplingStrategy {
    /// No sampling - analyze all data
    None,

    /// Simple random sampling with fixed size
    Random { size: usize },

    /// Reservoir sampling for streaming data
    Reservoir { size: usize },

    /// Stratified sampling balanced by categories
    Stratified {
        key_columns: Vec<String>,
        samples_per_stratum: usize,
    },

    /// Progressive sampling - stop when confidence is reached
    Progressive {
        initial_size: usize,
        confidence_level: f64,
        max_size: usize,
    },

    /// Systematic sampling (every Nth row)
    Systematic { interval: usize },
}

impl SamplingStrategy {
    /// Create adaptive strategy based on data characteristics
    pub fn adaptive(total_rows: Option<usize>, _file_size_mb: f64) -> Self {
        match total_rows {
            Some(rows) if rows <= 10_000 => SamplingStrategy::None,
            Some(rows) if rows <= 100_000 => SamplingStrategy::Random { size: 10_000 },
            Some(rows) if rows <= 1_000_000 => SamplingStrategy::Progressive {
                initial_size: 10_000,
                confidence_level: 0.95,
                max_size: 50_000,
            },
            _ => SamplingStrategy::Reservoir { size: 100_000 },
        }
    }

    /// Check if row should be included in sample
    pub fn should_include(&self, row_index: usize, total_processed: usize) -> bool {
        match self {
            SamplingStrategy::None => true,
            SamplingStrategy::Random { size } => {
                // Simple hash-based sampling for deterministic results
                let mut hasher = DefaultHasher::new();
                row_index.hash(&mut hasher);
                let hash = hasher.finish();
                (hash % 100_000) < (*size as u64 * 100_000 / total_processed.max(1) as u64)
            }
            SamplingStrategy::Systematic { interval } => row_index % interval == 0,
            SamplingStrategy::Reservoir { size } => {
                if total_processed <= *size {
                    true
                } else {
                    // Reservoir algorithm: replace with probability size/total_processed
                    let mut hasher = DefaultHasher::new();
                    row_index.hash(&mut hasher);
                    let hash = hasher.finish();
                    (hash % total_processed as u64) < (*size as u64)
                }
            }
            _ => true, // TODO: implement advanced strategies
        }
    }

    pub fn target_sample_size(&self) -> Option<usize> {
        match self {
            SamplingStrategy::None => None,
            SamplingStrategy::Random { size } => Some(*size),
            SamplingStrategy::Reservoir { size } => Some(*size),
            SamplingStrategy::Stratified {
                samples_per_stratum,
                ..
            } => Some(*samples_per_stratum),
            SamplingStrategy::Progressive { max_size, .. } => Some(*max_size),
            SamplingStrategy::Systematic { .. } => None,
        }
    }
}
