use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::{MemoryMappedCsvReader, ProgressCallback, ProgressTracker};
use crate::types::{ColumnProfile, FileInfo, QualityReport, ScanInfo};
use crate::QualityChecker;

/// Streaming statistics for incremental computation
#[derive(Debug, Clone)]
struct StreamingStats {
    count: usize,
    sum: f64,
    sum_squares: f64,
    min: f64,
    max: f64,
}

impl StreamingStats {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_squares: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.sum_squares += value * value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }

    fn mean(&self) -> f64 {
        if self.count > 0 {
            self.sum / self.count as f64
        } else {
            0.0
        }
    }

    #[allow(dead_code)] // Future use for distributed processing
    fn merge(&mut self, other: &StreamingStats) {
        self.count += other.count;
        self.sum += other.sum;
        self.sum_squares += other.sum_squares;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }
}

/// Column metadata for streaming aggregation
#[derive(Debug)]
struct StreamingColumnInfo {
    name: String,
    total_count: usize,
    null_count: usize,
    unique_values: std::collections::HashSet<String>,
    numeric_stats: Option<StreamingStats>,
    text_lengths: Vec<usize>,
    sample_values: Vec<String>, // Keep a sample for pattern detection
}

impl StreamingColumnInfo {
    fn new(name: String) -> Self {
        Self {
            name,
            total_count: 0,
            null_count: 0,
            unique_values: std::collections::HashSet::new(),
            numeric_stats: None,
            text_lengths: Vec::new(),
            sample_values: Vec::new(),
        }
    }

    fn process_value(&mut self, value: &str) {
        self.total_count += 1;

        if value.is_empty() {
            self.null_count += 1;
            return;
        }

        // Track unique values (with limit to prevent memory explosion)
        if self.unique_values.len() < 10_000 {
            self.unique_values.insert(value.to_string());
        }

        // Keep sample values for pattern detection (limit to prevent memory issues)
        if self.sample_values.len() < 1000 {
            self.sample_values.push(value.to_string());
        }

        // Track text length
        self.text_lengths.push(value.len());

        // Try to parse as numeric
        if let Ok(num) = value.parse::<f64>() {
            if self.numeric_stats.is_none() {
                self.numeric_stats = Some(StreamingStats::new());
            }
            if let Some(stats) = &mut self.numeric_stats {
                stats.update(num);
            }
        }
    }

    #[allow(dead_code)] // Future use for distributed processing
    fn merge(&mut self, other: StreamingColumnInfo) {
        self.total_count += other.total_count;
        self.null_count += other.null_count;

        // Merge unique values (with size limit)
        for value in other.unique_values {
            if self.unique_values.len() < 10_000 {
                self.unique_values.insert(value);
            }
        }

        // Merge text lengths
        self.text_lengths.extend(other.text_lengths);

        // Merge sample values
        for value in other.sample_values {
            if self.sample_values.len() < 1000 {
                self.sample_values.push(value);
            }
        }

        // Merge numeric stats
        if let (Some(self_stats), Some(other_stats)) =
            (&mut self.numeric_stats, &other.numeric_stats)
        {
            self_stats.merge(other_stats);
        } else if other.numeric_stats.is_some() {
            self.numeric_stats = other.numeric_stats;
        }
    }
}

/// Memory-efficient streaming profiler that uses memory mapping
pub struct MemoryEfficientProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    progress_callback: Option<ProgressCallback>,
    max_memory_mb: usize,
}

impl MemoryEfficientProfiler {
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

    pub fn analyze_file(&self, file_path: &Path) -> Result<QualityReport> {
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

    fn analyze_with_memory_mapping(&self, file_path: &Path) -> Result<QualityReport> {
        let start = std::time::Instant::now();
        let reader = MemoryMappedCsvReader::new(file_path)?;

        let file_size_bytes = reader.file_size();
        let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Estimate total rows
        let estimated_total_rows = reader.estimate_row_count()?;

        // Calculate chunk size based on memory limit and file size
        let chunk_size_bytes = self.calculate_memory_efficient_chunk_size(file_size_bytes);

        let mut progress_tracker = ProgressTracker::new(self.progress_callback.clone());
        let mut column_infos: HashMap<String, StreamingColumnInfo> = HashMap::new();
        let mut headers: Option<csv::StringRecord> = None;

        let mut processed_rows = 0;
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
                let global_row_idx = processed_rows + row_idx;

                // Apply sampling strategy
                if !self
                    .sampling_strategy
                    .should_include(global_row_idx, global_row_idx + 1)
                {
                    continue;
                }

                // Process each field in the record
                for (field_idx, field) in record.iter().enumerate() {
                    if let Some(ref header_record) = headers {
                        if let Some(header) = header_record.get(field_idx) {
                            if let Some(column_info) = column_infos.get_mut(header) {
                                column_info.process_value(field);
                            }
                        }
                    }
                }
            }

            processed_rows += records.len();
            chunk_count += 1;
            offset += chunk_size_bytes as u64;

            progress_tracker.update(processed_rows, Some(estimated_total_rows), chunk_count);

            // Break if we've read all data
            if records.len() < 100 {
                // Arbitrary threshold for end-of-file
                break;
            }
        }

        progress_tracker.finish(processed_rows);

        // Convert streaming stats to column profiles
        let mut column_profiles = Vec::new();
        for (_, column_info) in column_infos.into_iter() {
            let profile = self.convert_to_column_profile(column_info);
            column_profiles.push(profile);
        }

        // For quality checking, we need to provide sample data
        // Since we don't keep all data in memory, use sample values
        let sample_columns = self.create_sample_columns_for_quality_check(&column_profiles);
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

    fn analyze_small_file(&self, file_path: &Path) -> Result<QualityReport> {
        // For small files, fall back to the existing streaming profiler
        let profiler = super::StreamingProfiler::new()
            .chunk_size(self.chunk_size.clone())
            .sampling(self.sampling_strategy.clone());

        let profiler = if let Some(callback) = &self.progress_callback {
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
        use crate::types::{ColumnStats, DataType};

        // Infer data type
        let data_type = if column_info.numeric_stats.is_some() {
            // Check if all numeric values are integers
            let all_integers = column_info
                .sample_values
                .iter()
                .filter(|s| !s.is_empty())
                .all(|s| s.parse::<i64>().is_ok());

            if all_integers {
                DataType::Integer
            } else {
                DataType::Float
            }
        } else {
            // Check if looks like dates
            let date_like = column_info
                .sample_values
                .iter()
                .filter(|s| !s.is_empty())
                .take(100)
                .filter(|s| self.looks_like_date(s))
                .count();

            if date_like > column_info.sample_values.len() / 2 {
                DataType::Date
            } else {
                DataType::String
            }
        };

        // Calculate stats
        let stats = match data_type {
            DataType::Integer | DataType::Float => {
                if let Some(numeric_stats) = &column_info.numeric_stats {
                    ColumnStats::Numeric {
                        min: numeric_stats.min,
                        max: numeric_stats.max,
                        mean: numeric_stats.mean(),
                    }
                } else {
                    ColumnStats::Numeric {
                        min: 0.0,
                        max: 0.0,
                        mean: 0.0,
                    }
                }
            }
            DataType::String | DataType::Date => {
                let min_length = column_info.text_lengths.iter().min().copied().unwrap_or(0);
                let max_length = column_info.text_lengths.iter().max().copied().unwrap_or(0);
                let avg_length = if !column_info.text_lengths.is_empty() {
                    column_info.text_lengths.iter().sum::<usize>() as f64
                        / column_info.text_lengths.len() as f64
                } else {
                    0.0
                };

                ColumnStats::Text {
                    min_length,
                    max_length,
                    avg_length,
                }
            }
        };

        // Detect patterns using sample values
        let patterns = crate::detect_patterns(&column_info.sample_values);

        ColumnProfile {
            name: column_info.name,
            data_type,
            null_count: column_info.null_count,
            total_count: column_info.total_count,
            unique_count: Some(column_info.unique_values.len()),
            stats,
            patterns,
        }
    }

    fn looks_like_date(&self, value: &str) -> bool {
        use regex::Regex;

        let date_patterns = [
            r"^\d{4}-\d{2}-\d{2}$",
            r"^\d{2}/\d{2}/\d{4}$",
            r"^\d{2}-\d{2}-\d{4}$",
        ];

        date_patterns.iter().any(|pattern| {
            Regex::new(pattern)
                .map(|re| re.is_match(value))
                .unwrap_or(false)
        })
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

impl Default for MemoryEfficientProfiler {
    fn default() -> Self {
        Self::new()
    }
}
