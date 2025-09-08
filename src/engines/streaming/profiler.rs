use anyhow::Result;
use csv::ReaderBuilder;
use std::path::Path;
use std::sync::Arc;

use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::progress::{ProgressCallback, ProgressTracker};
use crate::types::{FileInfo, QualityReport, ScanInfo};
use crate::{analyze_column, QualityChecker};

pub struct StreamingProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    progress_callback: Option<ProgressCallback>,
}

impl StreamingProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            progress_callback: None,
        }
    }

    pub fn chunk_size(mut self, chunk_size: ChunkSize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn sampling(mut self, strategy: SamplingStrategy) -> Self {
        self.sampling_strategy = strategy;
        self
    }

    pub fn progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(super::ProgressInfo) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Arc::new(callback));
        self
    }

    pub fn analyze_file(&self, file_path: &Path) -> Result<QualityReport> {
        let metadata = std::fs::metadata(file_path)?;
        let file_size_bytes = metadata.len();
        let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        let start = std::time::Instant::now();

        // Estimate total rows for progress tracking
        let estimated_total_rows = self.estimate_total_rows(file_path)?;

        // Calculate optimal chunk size
        let chunk_size = self.chunk_size.calculate(file_size_bytes);

        // Set up progress tracking
        let mut progress_tracker = ProgressTracker::new(self.progress_callback.clone());

        // Initialize aggregated data storage
        let mut all_column_data: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut total_rows_processed = 0;
        let mut chunk_count = 0;

        // Create CSV reader
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_path(file_path)?;

        let headers = reader.headers()?.clone();

        // Initialize columns
        for header in headers.iter() {
            all_column_data.insert(header.to_string(), Vec::new());
        }

        // Process in chunks
        let mut current_chunk_data: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for header in headers.iter() {
            current_chunk_data.insert(header.to_string(), Vec::new());
        }

        let mut rows_in_current_chunk = 0;

        for (row_index, result) in reader.records().enumerate() {
            let record = result?;

            // Check if we should include this row based on sampling strategy
            if !self
                .sampling_strategy
                .should_include(row_index, total_rows_processed + 1)
            {
                continue;
            }

            // Add record to current chunk
            for (i, field) in record.iter().enumerate() {
                if let Some(header) = headers.get(i) {
                    if let Some(column_data) = current_chunk_data.get_mut(header) {
                        column_data.push(field.to_string());
                    }
                }
            }

            rows_in_current_chunk += 1;
            total_rows_processed += 1;

            // Process chunk when it reaches target size
            if rows_in_current_chunk >= chunk_size {
                self.process_chunk(&mut all_column_data, &current_chunk_data);

                chunk_count += 1;
                progress_tracker.update(total_rows_processed, estimated_total_rows, chunk_count);

                // Clear current chunk
                for values in current_chunk_data.values_mut() {
                    values.clear();
                }
                rows_in_current_chunk = 0;
            }
        }

        // Process remaining data in last chunk
        if rows_in_current_chunk > 0 {
            self.process_chunk(&mut all_column_data, &current_chunk_data);
            chunk_count += 1;
            progress_tracker.update(total_rows_processed, estimated_total_rows, chunk_count);
        }

        progress_tracker.finish(total_rows_processed);

        // Analyze aggregated data
        let mut column_profiles = Vec::new();
        for (name, data) in &all_column_data {
            let profile = analyze_column(name, data);
            column_profiles.push(profile);
        }

        // Check quality issues
        let issues = QualityChecker::check_columns(&column_profiles, &all_column_data);

        let scan_time_ms = start.elapsed().as_millis();
        let sampling_ratio = if let Some(total) = estimated_total_rows {
            total_rows_processed as f64 / total as f64
        } else {
            1.0
        };

        Ok(QualityReport {
            file_info: FileInfo {
                path: file_path.display().to_string(),
                total_rows: estimated_total_rows,
                total_columns: column_profiles.len(),
                file_size_mb,
            },
            column_profiles,
            issues,
            scan_info: ScanInfo {
                rows_scanned: total_rows_processed,
                sampling_ratio,
                scan_time_ms,
            },
        })
    }

    fn process_chunk(
        &self,
        all_data: &mut std::collections::HashMap<String, Vec<String>>,
        chunk_data: &std::collections::HashMap<String, Vec<String>>,
    ) {
        // For now, simply append chunk data to all_data
        // In future versions, this could aggregate statistics incrementally
        for (column_name, chunk_values) in chunk_data {
            if let Some(all_values) = all_data.get_mut(column_name) {
                all_values.extend(chunk_values.iter().cloned());
            }
        }
    }

    fn estimate_total_rows(&self, path: &Path) -> Result<Option<usize>> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let file = File::open(path)?;
        let file_size = file.metadata()?.len();

        // For small files, don't estimate
        if file_size < 1_000_000 {
            return Ok(None);
        }

        // Sample first 1000 lines to estimate average line size
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut bytes_read = 0u64;
        let mut line_count = 0;

        while line_count < 1000 {
            match lines.next() {
                Some(Ok(line)) => {
                    bytes_read += line.len() as u64 + 1; // +1 for newline
                    line_count += 1;
                }
                _ => break,
            }
        }

        if line_count > 0 {
            let avg_line_size = bytes_read / line_count;
            let estimated_rows = (file_size / avg_line_size) as usize;
            Ok(Some(estimated_rows))
        } else {
            Ok(None)
        }
    }
}

impl Default for StreamingProfiler {
    fn default() -> Self {
        Self::new()
    }
}
