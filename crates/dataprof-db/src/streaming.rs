//! Streaming utilities for processing large database result sets

use crate::DataProfilerError;
use crate::query_columns::QueryColumns;

/// Merging lives with [`QueryColumns`], which owns the ordering rule; re-exported
/// here because the streaming macros have always reached for it through this
/// module.
pub use crate::query_columns::merge_column_batches;

/// Configuration for streaming database results
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    pub batch_size: usize,
    pub max_memory_mb: usize,
    pub progress_callback: Option<fn(u64, u64)>,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            batch_size: 10000,
            max_memory_mb: 512,
            progress_callback: None,
        }
    }
}

/// Calculate memory usage of column data (rough estimate)
pub fn estimate_memory_usage(columns: &QueryColumns) -> usize {
    columns
        .iter()
        .map(|(name, data)| name.len() + data.iter().map(|s| s.len()).sum::<usize>())
        .sum::<usize>()
}

/// Sample large datasets to fit within memory constraints
pub fn apply_sampling_if_needed(
    mut columns: QueryColumns,
    max_memory_mb: usize,
    sampling_ratio: f64,
) -> Result<(QueryColumns, bool), DataProfilerError> {
    let memory_usage_bytes = estimate_memory_usage(&columns);
    let memory_usage_mb = memory_usage_bytes / 1_048_576;

    if memory_usage_mb <= max_memory_mb {
        return Ok((columns, false));
    }

    // decode-audit: no-data — an empty column set has zero rows; the sampling
    // below then returns an empty result rather than fabricating data.
    let total_rows = columns.row_count();
    let target_rows = (total_rows as f64 * sampling_ratio) as usize;

    if target_rows == 0 {
        return Ok((QueryColumns::new(), true));
    }

    let step = total_rows / target_rows;
    if step <= 1 {
        return Ok((columns, false));
    }

    for column_data in columns.values_mut() {
        let sampled: Vec<String> = column_data
            .iter()
            .step_by(step)
            .take(target_rows)
            .cloned()
            .collect();
        *column_data = sampled;
    }

    Ok((columns, true))
}

/// Progress tracking for streaming operations
pub struct StreamingProgress {
    pub total_rows: Option<u64>,
    pub processed_rows: u64,
    pub batches_processed: u64,
    pub start_time: std::time::Instant,
}

impl StreamingProgress {
    pub fn new(total_rows: Option<u64>) -> Self {
        Self {
            total_rows,
            processed_rows: 0,
            batches_processed: 0,
            start_time: std::time::Instant::now(),
        }
    }

    pub fn update(&mut self, batch_size: u64) {
        self.processed_rows += batch_size;
        self.batches_processed += 1;
    }

    pub fn percentage(&self) -> Option<f64> {
        self.total_rows.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.processed_rows as f64 / total as f64) * 100.0
            }
        })
    }

    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    pub fn estimated_total_time(&self) -> Option<std::time::Duration> {
        if let Some(percentage) = self.percentage()
            && percentage > 0.0
        {
            let elapsed_secs = self.elapsed().as_secs_f64();
            let total_secs = elapsed_secs * (100.0 / percentage);
            return Some(std::time::Duration::from_secs_f64(total_secs));
        }
        None
    }

    pub fn rows_per_second(&self) -> f64 {
        let elapsed_secs = self.elapsed().as_secs_f64();
        if elapsed_secs > 0.0 {
            self.processed_rows as f64 / elapsed_secs
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_column_batches() {
        let batch = |a: [&str; 2], b: [&str; 2]| -> QueryColumns {
            [
                (
                    "col1".to_string(),
                    a.iter().map(|v| v.to_string()).collect(),
                ),
                (
                    "col2".to_string(),
                    b.iter().map(|v| v.to_string()).collect(),
                ),
            ]
            .into_iter()
            .collect()
        };

        let merged = merge_column_batches(vec![
            batch(["a", "b"], ["1", "2"]),
            batch(["c", "d"], ["3", "4"]),
        ]);

        assert_eq!(merged.names().collect::<Vec<_>>(), ["col1", "col2"]);
        assert_eq!(
            merged.get("col1").expect("col1 not found"),
            &vec!["a", "b", "c", "d"]
        );
        assert_eq!(
            merged.get("col2").expect("col2 not found"),
            &vec!["1", "2", "3", "4"]
        );
    }

    #[test]
    fn test_memory_estimation() {
        let columns: QueryColumns = [(
            "test".to_string(),
            vec!["hello".to_string(), "world".to_string()],
        )]
        .into_iter()
        .collect();

        let memory = estimate_memory_usage(&columns);
        assert_eq!(memory, 14);
    }

    #[test]
    fn test_streaming_progress() {
        let mut progress = StreamingProgress::new(Some(1000));

        assert_eq!(progress.percentage(), Some(0.0));

        progress.update(250);
        assert_eq!(progress.percentage(), Some(25.0));

        progress.update(250);
        assert_eq!(progress.percentage(), Some(50.0));

        assert_eq!(progress.batches_processed, 2);
        assert_eq!(progress.processed_rows, 500);
    }
}
