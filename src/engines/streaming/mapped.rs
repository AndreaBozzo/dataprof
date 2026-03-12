use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::analysis::patterns::looks_like_date;
use crate::core::errors::DataProfilerError;
use crate::core::report_assembler::ReportAssembler;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::core::streaming_stats::StreamingStatistics;
use crate::engines::streaming::{MemoryMappedCsvReader, ProgressCallback, ProgressTracker};
use crate::types::{ColumnProfile, DataSource, ExecutionMetadata, FileFormat, ProfileReport};

/// Column metadata for streaming aggregation
/// Now uses the canonical StreamingStatistics from core module
#[derive(Debug)]
struct StreamingColumnInfo {
    name: String,
    /// Streaming statistics aggregator (from core::streaming_stats)
    stats: StreamingStatistics,
}

impl StreamingColumnInfo {
    fn new(name: String) -> Self {
        Self {
            name,
            // Use the canonical streaming statistics with default limits
            stats: StreamingStatistics::new(),
        }
    }

    fn process_value(&mut self, value: &str) {
        // Delegate to canonical StreamingStatistics implementation
        self.stats.update(value);
    }
}

/// Memory-mapped profiler that uses mmap for large files
/// Falls back to BufferedProfiler for files under 10MB.
pub struct MappedProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    progress_callback: Option<ProgressCallback>,
    max_memory_mb: usize,
}

impl MappedProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            progress_callback: None,
            max_memory_mb: 512, // Default 512MB memory limit
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
        self.max_memory_mb = limit;
        self
    }

    pub fn analyze_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        let file_size_bytes = std::fs::metadata(file_path)?.len();
        let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Use memory mapping for files larger than 10MB
        if file_size_mb > 10.0 {
            self.analyze_with_memory_mapping(file_path)
        } else {
            // Fall back to regular processing for small files
            self.analyze_small_file(file_path)
        }
    }

    fn analyze_with_memory_mapping(
        &self,
        file_path: &Path,
    ) -> Result<ProfileReport, DataProfilerError> {
        let start = std::time::Instant::now();
        let reader = MemoryMappedCsvReader::new(file_path)?;

        let file_size_bytes = reader.file_size();
        let _file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Estimate total rows
        let estimated_total_rows = reader.estimate_row_count()?;

        // Calculate chunk size based on memory limit and file size
        let chunk_size_bytes = self.calculate_memory_efficient_chunk_size(file_size_bytes);

        let mut progress_tracker = ProgressTracker::new(self.progress_callback.clone());
        let mut column_infos: HashMap<String, StreamingColumnInfo> = HashMap::new();
        let mut headers: Option<csv::StringRecord> = None;

        let mut iterated_rows = 0;
        let mut analyzed_rows = 0;
        let mut chunk_count = 0;
        let mut offset = 0u64;

        // Process file in chunks using memory mapping
        loop {
            let (chunk_headers, records) =
                reader.read_csv_chunk(offset, chunk_size_bytes, headers.is_none())?;

            if records.is_empty() {
                break;
            }

            // Store headers from first chunk
            if headers.is_none() && chunk_headers.is_some() {
                headers = chunk_headers;

                // Initialize column info structures
                if let Some(ref header_record) = headers {
                    for header in header_record.iter() {
                        column_infos.insert(
                            header.to_string(),
                            StreamingColumnInfo::new(header.to_string()),
                        );
                    }
                }
            }

            // Process records in this chunk
            for (row_idx, record) in records.iter().enumerate() {
                let global_row_idx = iterated_rows + row_idx;

                // Apply sampling strategy
                if !self
                    .sampling_strategy
                    .should_include(global_row_idx, global_row_idx + 1)
                {
                    continue;
                }

                // Process each field in the record
                for (field_idx, field) in record.iter().enumerate() {
                    if let Some(ref header_record) = headers
                        && let Some(header) = header_record.get(field_idx)
                        && let Some(column_info) = column_infos.get_mut(header)
                    {
                        column_info.process_value(field);
                    }
                }
                analyzed_rows += 1;
            }

            iterated_rows += records.len();
            chunk_count += 1;
            offset += chunk_size_bytes as u64;

            progress_tracker.update(iterated_rows, Some(estimated_total_rows), chunk_count);

            // Break if we've read all data
            if records.len() < 100 {
                // Arbitrary threshold for end-of-file
                break;
            }
        }

        progress_tracker.finish(iterated_rows);

        // Convert streaming stats to column profiles
        let mut column_profiles = Vec::new();
        for (_, column_info) in column_infos.into_iter() {
            let profile = self.convert_to_column_profile(column_info);
            column_profiles.push(profile);
        }

        // Calculate quality metrics from sample data
        let sample_columns = self.create_sample_columns_for_quality_check(&column_profiles);

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        let mut execution = ExecutionMetadata::new(analyzed_rows, num_columns, scan_time_ms)
            .with_bytes_consumed(file_size_bytes);
        if estimated_total_rows > 0 && analyzed_rows < estimated_total_rows {
            let ratio = analyzed_rows as f64 / estimated_total_rows as f64;
            execution = execution.with_sampling(ratio).with_source_exhausted(false);
        }

        Ok(ReportAssembler::new(
            DataSource::File {
                path: file_path.display().to_string(),
                format: FileFormat::Csv,
                size_bytes: file_size_bytes,
                modified_at: None,
                parquet_metadata: None,
            },
            execution,
        )
        .columns(column_profiles)
        .with_quality_data(sample_columns)
        .build())
    }

    #[allow(deprecated)]
    fn analyze_small_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        // For small files, fall back to the buffered profiler
        let profiler = super::BufferedProfiler::new()
            .chunk_size(self.chunk_size.clone())
            .sampling(self.sampling_strategy.clone());

        let mut profiler = if let Some(callback) = &self.progress_callback {
            let cb = callback.clone();
            profiler.progress_callback(move |info| cb(info))
        } else {
            profiler
        };

        profiler.analyze_file(file_path)
    }

    fn calculate_memory_efficient_chunk_size(&self, file_size: u64) -> usize {
        let max_memory_bytes = self.max_memory_mb * 1_048_576;
        let suggested_chunk = (max_memory_bytes / 4).max(64 * 1024); // At least 64KB chunks

        // Don't make chunks larger than 10% of file size
        let max_chunk_from_file = (file_size / 10).max(64 * 1024) as usize;

        suggested_chunk.min(max_chunk_from_file)
    }

    fn convert_to_column_profile(&self, column_info: StreamingColumnInfo) -> ColumnProfile {
        use crate::types::DataType;

        // Get sample values from StreamingStatistics
        let sample_values = column_info.stats.sample_values();

        // Infer data type - check if numeric data was collected
        let has_numeric = column_info.stats.min < f64::INFINITY;
        let data_type = if has_numeric {
            let all_integers = sample_values
                .iter()
                .filter(|s| !s.is_empty())
                .all(|s| s.parse::<i64>().is_ok());

            if all_integers {
                DataType::Integer
            } else {
                DataType::Float
            }
        } else {
            let date_like = sample_values
                .iter()
                .filter(|s| !s.is_empty())
                .take(100)
                .filter(|s| looks_like_date(s))
                .count();

            if date_like > sample_values.len() / 2 {
                DataType::Date
            } else {
                DataType::String
            }
        };

        let text_stats = column_info.stats.text_length_stats();

        crate::core::profile_builder::build_column_profile(
            crate::core::profile_builder::ColumnProfileInput {
                name: column_info.name,
                data_type,
                total_count: column_info.stats.count,
                null_count: column_info.stats.null_count,
                unique_count: Some(column_info.stats.unique_count()),
                sample_values,
                text_lengths: Some(crate::core::profile_builder::TextLengths {
                    min_length: text_stats.min_length,
                    max_length: text_stats.max_length,
                    avg_length: text_stats.avg_length,
                }),
            },
        )
    }

    fn create_sample_columns_for_quality_check(
        &self,
        profiles: &[ColumnProfile],
    ) -> HashMap<String, Vec<String>> {
        // Create minimal sample data for quality checking
        // Since we don't have all the data, create empty columns
        let mut columns = HashMap::new();
        for profile in profiles {
            columns.insert(profile.name.clone(), Vec::new());
        }
        columns
    }
}

impl Default for MappedProfiler {
    fn default() -> Self {
        Self::new()
    }
}
