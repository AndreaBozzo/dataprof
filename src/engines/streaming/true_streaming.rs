use anyhow::Result;
use std::path::Path;
use std::sync::Arc;

use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::core::streaming_stats::{StreamingColumnCollection, StreamingStatistics};
use crate::engines::streaming::{MemoryMappedCsvReader, ProgressCallback, ProgressTracker};
use crate::types::{ColumnProfile, ColumnStats, DataType, FileInfo, QualityReport, ScanInfo};
use crate::QualityChecker;

/// True streaming profiler that processes data without loading everything into memory
/// Uses incremental statistics and memory mapping for maximum efficiency
pub struct TrueStreamingProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    progress_callback: Option<ProgressCallback>,
    memory_limit_mb: usize,
}

impl TrueStreamingProfiler {
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

    pub fn analyze_file(&self, file_path: &Path) -> Result<QualityReport> {
        let start = std::time::Instant::now();
        let reader = MemoryMappedCsvReader::new(file_path)?;

        let file_size_bytes = reader.file_size();
        let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Estimate total rows for progress tracking
        let estimated_total_rows = reader.estimate_row_count()?;

        // Calculate optimal chunk size based on memory limit and file size
        let chunk_size_bytes = self.calculate_optimal_chunk_size(file_size_bytes);

        // Initialize streaming statistics collection
        let mut column_stats = StreamingColumnCollection::with_memory_limit(self.memory_limit_mb);
        let mut progress_tracker = ProgressTracker::new(self.progress_callback.clone());

        let mut headers: Option<csv::StringRecord> = None;
        let mut processed_rows = 0;
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
        let column_profiles = self.convert_to_profiles(&column_stats);

        // For quality checking, we need some sample data
        // Create minimal samples from the streaming stats
        let sample_columns = self.create_quality_check_samples(&column_stats);
        let issues = QualityChecker::check_columns(&column_profiles, &sample_columns);

        let scan_time_ms = start.elapsed().as_millis();
        let sampling_ratio = processed_rows as f64 / estimated_total_rows as f64;

        Ok(QualityReport {
            file_info: FileInfo {
                path: file_path.display().to_string(),
                total_rows: Some(estimated_total_rows),
                total_columns: column_profiles.len(),
                file_size_mb,
            },
            column_profiles,
            issues,
            scan_info: ScanInfo {
                rows_scanned: processed_rows,
                sampling_ratio,
                scan_time_ms,
            },
        })
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

    fn convert_to_profiles(&self, column_stats: &StreamingColumnCollection) -> Vec<ColumnProfile> {
        let mut profiles = Vec::new();

        for column_name in column_stats.column_names() {
            if let Some(stats) = column_stats.get_column_stats(&column_name) {
                let profile = self.convert_single_column_profile(&column_name, stats);
                profiles.push(profile);
            }
        }

        profiles
    }

    fn convert_single_column_profile(
        &self,
        name: &str,
        stats: &StreamingStatistics,
    ) -> ColumnProfile {
        // Infer data type from streaming statistics
        let data_type = self.infer_data_type(stats);

        // Convert to appropriate column stats
        let column_stats = match data_type {
            DataType::Integer | DataType::Float => ColumnStats::Numeric {
                min: stats.min,
                max: stats.max,
                mean: stats.mean(),
            },
            DataType::String | DataType::Date => {
                let text_stats = stats.text_length_stats();
                ColumnStats::Text {
                    min_length: text_stats.min_length,
                    max_length: text_stats.max_length,
                    avg_length: text_stats.avg_length,
                }
            }
        };

        // Detect patterns using sample values
        let patterns = crate::detect_patterns(stats.sample_values());

        ColumnProfile {
            name: name.to_string(),
            data_type,
            null_count: stats.null_count,
            total_count: stats.count,
            unique_count: Some(stats.unique_count()),
            stats: column_stats,
            patterns,
        }
    }

    fn infer_data_type(&self, stats: &StreamingStatistics) -> DataType {
        // If we have numeric statistics and they make sense, it's numeric
        if stats.min.is_finite() && stats.max.is_finite() && stats.sum.is_finite() {
            // Check if sample values are all integers
            let sample_values = stats.sample_values();
            let non_empty: Vec<&String> = sample_values.iter().filter(|s| !s.is_empty()).collect();

            if !non_empty.is_empty() {
                let all_integers = non_empty.iter().all(|s| s.parse::<i64>().is_ok());

                if all_integers {
                    return DataType::Integer;
                } else {
                    // Check if most are numeric
                    let numeric_count = non_empty
                        .iter()
                        .filter(|s| s.parse::<f64>().is_ok())
                        .count();

                    if numeric_count as f64 / non_empty.len() as f64 > 0.8 {
                        return DataType::Float;
                    }
                }
            }
        }

        // Check for date patterns
        let sample_values = stats.sample_values();
        let non_empty: Vec<&String> = sample_values.iter().filter(|s| !s.is_empty()).collect();

        if !non_empty.is_empty() {
            let date_like_count = non_empty
                .iter()
                .take(100) // Only check first 100 samples
                .filter(|s| self.looks_like_date(s))
                .count();

            if date_like_count as f64 / non_empty.len().min(100) as f64 > 0.7 {
                return DataType::Date;
            }
        }

        DataType::String
    }

    fn looks_like_date(&self, value: &str) -> bool {
        use regex::Regex;

        let date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",       // YYYY-MM-DD
            r"^\d{2}/\d{2}/\d{4}$",       // MM/DD/YYYY
            r"^\d{2}-\d{2}-\d{4}$",       // DD-MM-YYYY
            r"^\d{4}-\d{2}-\d{2}T\d{2}:", // ISO datetime
        ];

        date_patterns.iter().any(|pattern| {
            Regex::new(pattern)
                .map(|re| re.is_match(value))
                .unwrap_or(false)
        })
    }

    fn create_quality_check_samples(
        &self,
        column_stats: &StreamingColumnCollection,
    ) -> std::collections::HashMap<String, Vec<String>> {
        let mut samples = std::collections::HashMap::new();

        for column_name in column_stats.column_names() {
            if let Some(stats) = column_stats.get_column_stats(&column_name) {
                // Use sample values from streaming stats for quality checking
                let sample_values: Vec<String> = stats.sample_values().to_vec();

                samples.insert(column_name, sample_values);
            }
        }

        samples
    }
}

impl Default for TrueStreamingProfiler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_true_streaming_profiler() -> Result<()> {
        // Create a test CSV file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,salary")?;
        for i in 1..=1000 {
            writeln!(temp_file, "Person{},{},{}", i, 20 + i % 40, 30000 + i * 10)?;
        }
        temp_file.flush()?;

        // Test true streaming profiler
        let profiler = TrueStreamingProfiler::new().memory_limit_mb(10); // Very small memory limit to test streaming

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

        let profiler = TrueStreamingProfiler::new().memory_limit_mb(5); // Small memory limit

        let report = profiler.analyze_file(temp_file.path())?;
        assert_eq!(report.column_profiles.len(), 2);

        Ok(())
    }
}
