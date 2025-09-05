use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct ProgressInfo {
    pub percentage: f64,
    pub rows_processed: usize,
    pub estimated_total_rows: Option<usize>,
    pub elapsed_time: Duration,
    pub estimated_remaining_time: Option<Duration>,
    pub current_chunk: usize,
    pub processing_speed: f64, // rows per second
}

impl ProgressInfo {
    pub fn new(
        rows_processed: usize,
        estimated_total_rows: Option<usize>,
        elapsed_time: Duration,
        current_chunk: usize,
    ) -> Self {
        let percentage = match estimated_total_rows {
            Some(total) if total > 0 => (rows_processed as f64 / total as f64) * 100.0,
            _ => 0.0,
        };

        let processing_speed = if elapsed_time.as_secs() > 0 {
            rows_processed as f64 / elapsed_time.as_secs_f64()
        } else {
            0.0
        };

        let estimated_remaining_time = match (estimated_total_rows, processing_speed > 0.0) {
            (Some(total), true) => {
                let remaining_rows = total.saturating_sub(rows_processed);
                Some(Duration::from_secs_f64(
                    remaining_rows as f64 / processing_speed,
                ))
            }
            _ => None,
        };

        Self {
            percentage,
            rows_processed,
            estimated_total_rows,
            elapsed_time,
            estimated_remaining_time,
            current_chunk,
            processing_speed,
        }
    }
}

pub type ProgressCallback = Arc<dyn Fn(ProgressInfo) + Send + Sync>;

#[derive(Clone)]
pub struct ProgressTracker {
    callback: Option<ProgressCallback>,
    start_time: Instant,
    last_update: Instant,
    update_interval: Duration,
}

impl ProgressTracker {
    pub fn new(callback: Option<ProgressCallback>) -> Self {
        let now = Instant::now();
        Self {
            callback,
            start_time: now,
            last_update: now,
            update_interval: Duration::from_millis(500), // Update every 500ms
        }
    }

    pub fn update(
        &mut self,
        rows_processed: usize,
        estimated_total: Option<usize>,
        current_chunk: usize,
    ) {
        let now = Instant::now();

        // Rate limit updates
        if now.duration_since(self.last_update) < self.update_interval {
            return;
        }

        self.last_update = now;

        if let Some(ref callback) = self.callback {
            let progress = ProgressInfo::new(
                rows_processed,
                estimated_total,
                now.duration_since(self.start_time),
                current_chunk,
            );

            callback(progress);
        }
    }

    pub fn finish(&self, total_rows_processed: usize) {
        if let Some(ref callback) = self.callback {
            let progress = ProgressInfo::new(
                total_rows_processed,
                Some(total_rows_processed),
                Instant::now().duration_since(self.start_time),
                0,
            );

            callback(progress);
        }
    }
}
