use std::path::Path;
use std::sync::Arc;

use crate::core::errors::DataProfilerError;
use crate::core::profile_builder;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::core::streaming_stats::StreamingColumnCollection;
use crate::engines::streaming::{MemoryMappedCsvReader, ProgressCallback, ProgressTracker};
use crate::types::{DataQualityMetrics, DataSource, ExecutionMetadata, FileFormat, QualityReport};

/// Incremental profiler that processes data without loading everything into memory
/// Uses online/streaming algorithms and memory mapping for maximum efficiency.
/// Never accumulates full column data - maintains only running statistics.
pub struct IncrementalProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    progress_callback: Option<ProgressCallback>,
    memory_limit_mb: usize,
}

impl IncrementalProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            progress_callback: None,
            memory_limit_mb: 256, // Default 256MB memory limit
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

    pub fn memory_limit_mb(mut self, limit: usize) -> Self {
        self.memory_limit_mb = limit;
        self
    }

    pub fn analyze_file(&self, file_path: &Path) -> Result<QualityReport, DataProfilerError> {
        let start = std::time::Instant::now();
        let reader = MemoryMappedCsvReader::new(file_path)?;

        let file_size_bytes = reader.file_size();
        let _file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Estimate total rows for progress tracking
        let estimated_total_rows = reader.estimate_row_count()?;

        // Calculate optimal chunk size based on memory limit and file size
        let chunk_size_bytes = self.calculate_optimal_chunk_size(file_size_bytes);

        // Initialize streaming statistics collection
        let mut column_stats = StreamingColumnCollection::memory_limit(self.memory_limit_mb);
        let mut progress_tracker = ProgressTracker::new(self.progress_callback.clone());

        let mut headers: Option<csv::StringRecord> = None;
        let mut processed_rows = 0;
        let mut analyzed_rows = 0;
        let mut chunk_count = 0;
        let mut offset = 0u64;

        // Process file in chunks using true streaming
        loop {
            let (chunk_headers, records) =
                reader.read_csv_chunk(offset, chunk_size_bytes, headers.is_none())?;

            if records.is_empty() {
                break;
            }

            // Store headers from first chunk
            if headers.is_none() && chunk_headers.is_some() {
                headers = chunk_headers;
            }

            // Process this chunk incrementally
            if let Some(ref header_record) = headers {
                let header_names: Vec<String> =
                    header_record.iter().map(|s| s.to_string()).collect();

                for (row_idx, record) in records.iter().enumerate() {
                    let global_row_idx = processed_rows + row_idx;

                    // Apply sampling strategy
                    if !self
                        .sampling_strategy
                        .should_include(global_row_idx, global_row_idx + 1)
                    {
                        continue;
                    }

                    // Convert record to values
                    let values: Vec<String> = record.iter().map(|s| s.to_string()).collect();

                    // Process record incrementally (no memory accumulation)
                    column_stats.process_record(&header_names, values);
                    analyzed_rows += 1;
                }
            }

            processed_rows += records.len();
            chunk_count += 1;
            offset += chunk_size_bytes as u64;

            // Update progress
            progress_tracker.update(processed_rows, Some(estimated_total_rows), chunk_count);

            // Check memory pressure and reduce if needed
            if column_stats.is_memory_pressure() {
                column_stats.reduce_memory_usage();
            }

            // Break if we've read all data (small chunk indicates EOF)
            if records.len() < 100 {
                break;
            }
        }

        progress_tracker.finish(processed_rows);

        // Convert streaming statistics to column profiles
        let column_profiles = profile_builder::profiles_from_streaming(&column_stats);

        // Calculate quality metrics from sample data
        let sample_columns = profile_builder::quality_check_samples(&column_stats);
        let data_quality_metrics =
            DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
                .unwrap_or_else(|_| DataQualityMetrics::empty());

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        let mut execution = ExecutionMetadata::new(analyzed_rows, num_columns, scan_time_ms)
            .with_bytes_consumed(file_size_bytes);
        if estimated_total_rows > 0 && analyzed_rows < estimated_total_rows {
            let ratio = analyzed_rows as f64 / estimated_total_rows as f64;
            execution = execution.with_sampling(ratio);
        }

        Ok(QualityReport::new(
            DataSource::File {
                path: file_path.display().to_string(),
                format: FileFormat::Csv,
                size_bytes: file_size_bytes,
                modified_at: None,
                parquet_metadata: None,
            },
            column_profiles,
            execution,
            data_quality_metrics,
        ))
    }

    fn calculate_optimal_chunk_size(&self, file_size: u64) -> usize {
        let max_memory_bytes = self.memory_limit_mb * 1024 * 1024;

        // Reserve memory for statistics (estimate)
        let reserved_for_stats = max_memory_bytes / 4;
        let available_for_chunks = max_memory_bytes - reserved_for_stats;

        // Calculate chunk size (ensure minimum size)
        let chunk_size = available_for_chunks.max(64 * 1024); // At least 64KB

        // Don't make chunks larger than 5% of file size for better progress tracking
        let max_chunk_from_file = (file_size / 20).max(64 * 1024) as usize;

        chunk_size.min(max_chunk_from_file)
    }
}

impl Default for IncrementalProfiler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;
    use anyhow::Result;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_incremental_profiler() -> Result<()> {
        // Create a test CSV file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,salary")?;
        for i in 1..=1000 {
            writeln!(temp_file, "Person{},{},{}", i, 20 + i % 40, 30000 + i * 10)?;
        }
        temp_file.flush()?;

        // Test incremental profiler
        let profiler = IncrementalProfiler::new().memory_limit_mb(10); // Very small memory limit to test streaming

        let report = profiler.analyze_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 3);

        // Find age column and verify it's detected as integer
        let age_column = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .expect("Age column should exist");

        assert_eq!(age_column.data_type, DataType::Integer);
        assert_eq!(age_column.total_count, 1000);

        // Verify advanced numeric stats are computed
        match &age_column.stats {
            crate::types::ColumnStats::Numeric {
                std_dev,
                median,
                skewness,
                kurtosis,
                coefficient_of_variation,
                ..
            } => {
                assert!(*std_dev > 0.0, "std_dev should be positive");
                assert!(median.is_some(), "median should be computed");
                assert!(skewness.is_some(), "skewness should be computed");
                assert!(kurtosis.is_some(), "kurtosis should be computed");
                assert!(coefficient_of_variation.is_some(), "CV should be computed");
            }
            _ => panic!("Expected Numeric stats for age column"),
        }

        Ok(())
    }

    #[test]
    fn test_memory_efficient_processing() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id,value")?;

        // Create a file with many unique values to test memory limits
        for i in 1..=5000 {
            writeln!(temp_file, "{},unique_value_{}", i, i)?;
        }
        temp_file.flush()?;

        let profiler = IncrementalProfiler::new().memory_limit_mb(5); // Small memory limit

        let report = profiler.analyze_file(temp_file.path())?;
        assert_eq!(report.column_profiles.len(), 2);

        Ok(())
    }
}
