use std::path::Path;
use std::time::Duration;

use dataprof_core::{
    ChunkSize, DataProfilerError, DataSource, ExecutionMetadata, FileFormat, MetricPack,
    PeakMemorySampler, ProgressSink, QualityDimension, SamplingStrategy, SchemaStabilityTracker,
    SemanticHints, StopCondition, StopEvaluator,
};
use dataprof_csv::{CsvParserConfig, MemoryMappedCsvReader};
use dataprof_runtime::{
    MemoryConfig, ProfileReport, ReportAssembler, StreamingColumnCollection, profile_builder,
};

use crate::progress_tracker::ProgressTracker;

/// Incremental profiler that processes data without loading everything into memory
/// Uses online/streaming algorithms and memory mapping for maximum efficiency.
/// Never accumulates full column data - maintains only running statistics.
pub struct IncrementalProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    progress_sink: ProgressSink,
    progress_interval: Duration,
    memory: MemoryConfig,
    stop_condition: StopCondition,
    quality_dimensions: Option<Vec<QualityDimension>>,
    metric_packs: Option<Vec<MetricPack>>,
    csv_config: Option<CsvParserConfig>,
    locale: Option<String>,
    semantic_hints: SemanticHints,
}

impl IncrementalProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            progress_sink: ProgressSink::None,
            progress_interval: Duration::from_millis(500),
            memory: MemoryConfig::default(),
            stop_condition: StopCondition::Never,
            quality_dimensions: None,
            metric_packs: None,
            csv_config: None,
            locale: None,
            semantic_hints: SemanticHints::default(),
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

    pub fn progress(mut self, sink: ProgressSink, interval: Duration) -> Self {
        self.progress_sink = sink;
        self.progress_interval = interval;
        self
    }

    pub fn memory_limit_mb(mut self, limit: usize) -> Self {
        self.memory = MemoryConfig::new(limit);
        self
    }

    pub fn stop_condition(mut self, condition: StopCondition) -> Self {
        self.stop_condition = condition;
        self
    }

    pub fn quality_dimensions(mut self, dims: Vec<QualityDimension>) -> Self {
        self.quality_dimensions = Some(dims);
        self
    }

    pub fn metric_packs(mut self, packs: Vec<MetricPack>) -> Self {
        self.metric_packs = Some(packs);
        self
    }

    pub fn csv_config(mut self, config: CsvParserConfig) -> Self {
        self.csv_config = Some(config);
        self
    }

    pub fn locale(mut self, locale: String) -> Self {
        self.locale = Some(locale);
        self
    }

    pub fn semantic_hints(mut self, hints: SemanticHints) -> Self {
        self.semantic_hints = hints;
        self
    }

    pub fn analyze_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        let start = std::time::Instant::now();
        let reader = MemoryMappedCsvReader::new(file_path)?;

        let file_size_bytes = reader.file_size();
        let _file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Estimate total rows for progress tracking
        let estimated_total_rows = reader.estimate_row_count()?;

        // Calculate optimal chunk size based on memory limit and file size
        let chunk_size_bytes = self.calculate_optimal_chunk_size(file_size_bytes);

        // Initialize streaming statistics collection
        let mut column_stats = StreamingColumnCollection::memory_limit(self.memory.limit_mb)
            .with_semantic_hints(&self.semantic_hints);

        let mut progress_tracker =
            ProgressTracker::new(self.progress_sink.clone(), self.progress_interval);

        // Initialize stop condition evaluator
        let mut stop_eval = StopEvaluator::new(self.stop_condition.clone())
            .with_estimated_total(estimated_total_rows as u64);
        let mut schema_tracker = SchemaStabilityTracker::from_condition(&self.stop_condition);

        // estimate_row_count includes the header line; subtract 1 for data-only count
        let estimated_data_rows = estimated_total_rows.saturating_sub(1);

        progress_tracker.emit_started(Some(estimated_data_rows), Some(file_size_bytes));

        let mut memory_sampler = PeakMemorySampler::new();
        let mut headers = None;
        let mut iterated_rows = 0;
        let mut analyzed_rows = 0;
        let mut ragged_rows = 0;
        let mut offset = 0u64;
        let mut source_exhausted = true;

        // Extract the max_rows limit (if any) for per-row stop checking
        let row_limit = Self::extract_max_rows(&self.stop_condition);

        // Process file in chunks using true streaming
        loop {
            let (chunk_headers, records, actual_bytes) = reader.read_csv_chunk(
                offset,
                chunk_size_bytes,
                headers.is_none(),
                self.csv_config.as_ref(),
            )?;

            if records.is_empty() && actual_bytes == 0 {
                break;
            }

            // Store headers from first chunk and initialize column collection
            if headers.is_none() && chunk_headers.is_some() {
                headers = chunk_headers;
                if let Some(ref h) = headers {
                    let names: Vec<String> = h.iter().map(|s| s.to_string()).collect();
                    column_stats.init_columns(&names);
                    progress_tracker.emit_schema(names);
                }
            }

            // Process this chunk incrementally
            let mut hit_row_limit = false;
            if let Some(ref header_record) = headers {
                let header_names: Vec<String> =
                    header_record.iter().map(|s| s.to_string()).collect();

                for (row_idx, record) in records.iter().enumerate() {
                    let global_row_idx = iterated_rows + row_idx;

                    // A record whose field count differs from the header is a
                    // structural violation. It is still recovered below (padded
                    // or truncated to header width), but must not vanish from
                    // the report — counted here, before sampling, so the signal
                    // reflects every row read rather than only sampled ones.
                    if record.len() != header_names.len() {
                        ragged_rows += 1;
                    }

                    // Apply sampling strategy
                    if !self
                        .sampling_strategy
                        .should_include(global_row_idx, global_row_idx + 1)
                    {
                        continue;
                    }

                    // Convert record to values, padding ragged rows to header length
                    // so missing trailing fields are counted as empty (null-like)
                    let mut values: Vec<String> = record.iter().map(|s| s.to_string()).collect();
                    values.resize(header_names.len(), String::new());

                    // Process record incrementally (no memory accumulation)
                    column_stats.process_record(&header_names, values);
                    analyzed_rows += 1;

                    // Per-row stop check for MaxRows to avoid chunk-boundary overshoot
                    if let Some(limit) = row_limit
                        && analyzed_rows as u64 >= limit
                    {
                        iterated_rows += row_idx + 1;
                        hit_row_limit = true;

                        // Reaching the cap is only truncation if a row actually
                        // remains. A file holding exactly `limit` rows was read in
                        // full, and must not report a truncation reason.
                        let more_rows_remain = row_idx + 1 < records.len() || {
                            let (_, next_records, next_bytes) = reader.read_csv_chunk(
                                offset + actual_bytes as u64,
                                chunk_size_bytes,
                                false,
                                self.csv_config.as_ref(),
                            )?;
                            !(next_records.is_empty() && next_bytes == 0)
                        };

                        if more_rows_remain {
                            source_exhausted = false;
                            stop_eval.update((row_idx + 1) as u64, actual_bytes as u64, 0.0);
                        }
                        break;
                    }
                }
            }

            // Sample after processing, while the chunk buffer and the stats it
            // grew are both resident, so per-chunk allocation spikes count
            // toward the peak.
            memory_sampler.sample();

            if hit_row_limit {
                break;
            }

            iterated_rows += records.len();
            offset += actual_bytes as u64;

            // Update progress
            progress_tracker.emit_chunk(
                records.len(),
                actual_bytes as u64,
                Some(estimated_data_rows),
            );

            // Check memory pressure and reduce if needed
            if column_stats.is_memory_pressure() {
                column_stats.reduce_memory_usage();
            }

            // Evaluate stop condition (chunk-level for non-MaxRows conditions)
            let memory_fraction = if self.memory.limit_mb > 0 {
                column_stats.memory_usage_bytes() as f64
                    / (self.memory.limit_mb * 1024 * 1024) as f64
            } else {
                0.0
            };

            if stop_eval.update(records.len() as u64, actual_bytes as u64, memory_fraction) {
                source_exhausted = false;
                break;
            }

            // Check schema stability (separate from main evaluator)
            if let Some(ref mut tracker) = schema_tracker {
                let fingerprint = column_stats.column_type_fingerprint();
                if tracker.update(fingerprint, records.len() as u64) {
                    stop_eval = StopEvaluator::new(StopCondition::Never); // prevent double-reason
                    source_exhausted = false;
                    break;
                }
            }

            // Break if we've consumed the entire file
            if offset >= file_size_bytes {
                break;
            }
        }

        memory_sampler.sample();
        progress_tracker.emit_finished(!source_exhausted);

        // Convert streaming statistics to column profiles
        let packs = self.metric_packs.as_deref();
        let skip_stats = !MetricPack::include_statistics(packs);
        let skip_patterns = !MetricPack::include_patterns(packs);
        let column_profiles = profile_builder::profiles_from_streaming_with_hints(
            &column_stats,
            skip_stats,
            skip_patterns,
            self.locale.as_deref(),
            &self.semantic_hints,
        );

        // Calculate quality metrics from sample data
        let sample_columns = profile_builder::quality_check_samples(&column_stats);

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        let bytes_consumed = if source_exhausted {
            file_size_bytes
        } else {
            stop_eval.bytes_consumed()
        };

        let mut execution = ExecutionMetadata::new(analyzed_rows, num_columns, scan_time_ms)
            .with_engine("incremental")
            .with_bytes_consumed(bytes_consumed)
            .with_ragged_row_count(ragged_rows);

        if let Some(peak_mb) = memory_sampler.peak_mb() {
            execution = execution.with_memory_peak_mb(peak_mb);
        }

        // Apply stop condition truncation reason
        if let Some(reason) = stop_eval.truncation_reason() {
            execution = execution.with_truncation(reason);
        } else if let Some(ref tracker) = schema_tracker
            && !source_exhausted
        {
            execution = execution.with_truncation(tracker.truncation_reason());
        }

        // Determine if actual row-level sampling occurred (rows skipped by strategy)
        let sampling_was_applied = analyzed_rows < iterated_rows;
        if sampling_was_applied {
            let ratio = analyzed_rows as f64 / iterated_rows as f64;
            execution = execution.with_sampling(ratio);
        }

        if !source_exhausted {
            execution = execution.with_source_exhausted(false);
        }

        let mut assembler = ReportAssembler::new(
            DataSource::File {
                path: file_path.display().to_string(),
                format: FileFormat::Csv,
                size_bytes: file_size_bytes,
                modified_at: None,
                parquet_metadata: None,
            },
            execution,
        )
        .columns(column_profiles);

        if MetricPack::include_quality(packs) {
            assembler = assembler
                .with_quality_data(sample_columns)
                .with_row_duplicates(column_stats.row_duplicate_summary())
                .with_exact_value_hint_bindings(column_stats.semantic_hint_bindings())
                .with_semantic_hints(self.semantic_hints.clone());
            if let Some(ref dims) = self.quality_dimensions {
                assembler = assembler.with_requested_dimensions(dims.clone());
            }
        } else {
            assembler = assembler.skip_quality();
        }

        Ok(assembler.build())
    }

    /// Extract a top-level `MaxRows` limit from the stop condition (including
    /// inside `Any` composites) so we can check it per-row instead of per-chunk.
    fn extract_max_rows(condition: &StopCondition) -> Option<u64> {
        condition.max_rows()
    }

    fn calculate_optimal_chunk_size(&self, file_size: u64) -> usize {
        let max_memory_bytes = self.memory.limit_mb * 1024 * 1024;

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
    use anyhow::Result;
    use dataprof_core::{ColumnStats, DataType, TruncationReason};
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
            ColumnStats::Numeric(n) => {
                assert!(n.std_dev > 0.0, "std_dev should be positive");
                assert!(n.median.is_some(), "median should be computed");
                assert!(n.skewness.is_some(), "skewness should be computed");
                assert!(n.kurtosis.is_some(), "kurtosis should be computed");
                assert!(
                    n.coefficient_of_variation.is_some(),
                    "CV should be computed"
                );
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

    #[test]
    fn test_memory_peak_is_populated() -> Result<()> {
        // Regression for #419: memory_peak_mb was declared but never set,
        // so every report shipped null for a headline instrumentation field.
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id,value")?;
        for i in 1..=100 {
            writeln!(temp_file, "{},val_{}", i, i)?;
        }
        temp_file.flush()?;

        let report = IncrementalProfiler::new().analyze_file(temp_file.path())?;

        let peak = report
            .execution
            .memory_peak_mb
            .expect("incremental engine must report peak memory");
        assert!(peak > 0.0, "peak memory must be positive, got {peak}");

        Ok(())
    }

    #[test]
    fn test_early_stop_max_rows() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id,value")?;
        for i in 1..=5000 {
            writeln!(temp_file, "{},val_{}", i, i)?;
        }
        temp_file.flush()?;

        let profiler = IncrementalProfiler::new()
            .memory_limit_mb(10)
            .stop_condition(StopCondition::MaxRows(100));

        let report = profiler.analyze_file(temp_file.path())?;

        // Should have stopped early (chunk granularity — may exceed 100 slightly)
        assert!(
            report.execution.rows_processed < 5000,
            "Should stop before processing all 5000 rows, got {}",
            report.execution.rows_processed
        );
        assert!(!report.execution.source_exhausted);
        assert!(matches!(
            report.execution.truncation_reason,
            Some(TruncationReason::MaxRows(100))
        ));

        Ok(())
    }

    #[test]
    fn test_stop_condition_never_processes_all() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id,value")?;
        for i in 1..=500 {
            writeln!(temp_file, "{},val_{}", i, i)?;
        }
        temp_file.flush()?;

        let profiler = IncrementalProfiler::new()
            .memory_limit_mb(10)
            .stop_condition(StopCondition::Never);

        let report = profiler.analyze_file(temp_file.path())?;

        assert_eq!(report.execution.rows_processed, 500);
        assert!(report.execution.truncation_reason.is_none());

        Ok(())
    }
}
