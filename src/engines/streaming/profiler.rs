//! Streaming CSV profiler - refactored to use modular components
//!
//! This module coordinates streaming analysis by delegating to:
//! - ProgressManager (from output/progress.rs) for progress tracking
//! - ChunkProcessor for chunk processing logic
//! - ReportBuilder for report construction
//!
//! REFACTORED: Eliminated God Object pattern (was 350 lines doing everything)

use anyhow::Result;
use csv::ReaderBuilder;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::core::performance::PerformanceIntelligence;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::chunk_processor::ChunkProcessor;
use crate::engines::streaming::progress::{ProgressCallback, ProgressTracker};
use crate::engines::streaming::report_builder::ReportBuilder;
use crate::output::progress::{EnhancedProgressBar, ProgressManager};
use crate::types::QualityReport;

/// Streaming profiler - now a clean coordinator delegating to specialized modules
pub struct StreamingProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    progress_callback: Option<ProgressCallback>,
    progress_manager: Option<ProgressManager>,
    performance_intelligence: Option<PerformanceIntelligence>,
}

impl StreamingProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            progress_callback: None,
            progress_manager: None,
            performance_intelligence: None,
        }
    }

    /// Enable enhanced progress tracking with memory monitoring
    ///
    /// Now uses ProgressManager instead of manual setup
    pub fn with_enhanced_progress(mut self, leak_threshold_mb: usize) -> Self {
        self.progress_manager = Some(ProgressManager::with_memory_tracking(
            true,
            0,
            leak_threshold_mb,
        ));
        self.performance_intelligence = Some(PerformanceIntelligence::new());
        self
    }

    /// Enable performance intelligence without enhanced progress
    pub fn with_performance_intelligence(mut self) -> Self {
        self.performance_intelligence = Some(PerformanceIntelligence::new());
        self
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

    /// Analyze a file using streaming approach
    ///
    /// REFACTORED: From 224 lines to ~80 lines by delegating to specialized modules
    pub fn analyze_file(&mut self, file_path: &Path) -> Result<QualityReport> {
        let metadata = std::fs::metadata(file_path)?;
        let file_size_bytes = metadata.len();
        let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        // Estimate total rows for progress tracking
        let estimated_total_rows = self.estimate_total_rows(file_path)?;

        // Calculate optimal chunk size
        let chunk_size = self.chunk_size.calculate(file_size_bytes);

        // Set up progress tracking - now delegated to ProgressManager
        let (mut enhanced_progress, mut progress_tracker) =
            self.setup_progress(file_path, file_size_bytes);

        // Initialize data storage
        let mut all_column_data: HashMap<String, Vec<String>> = HashMap::new();

        // Create CSV reader
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_path(file_path)?;

        let headers = reader.headers()?.clone();
        let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

        // Initialize columns
        for header in &header_names {
            all_column_data.insert(header.clone(), Vec::new());
        }

        // Create chunk processor - delegates chunk processing logic
        let mut chunk_processor = ChunkProcessor::new(chunk_size, self.sampling_strategy.clone());
        chunk_processor.initialize_headers(&header_names);

        // Process records in chunks
        for (row_index, result) in reader.records().enumerate() {
            let record = result?;

            // Process record - ChunkProcessor handles sampling and chunking
            if chunk_processor.process_record(&record, &header_names, row_index) {
                // Flush chunk to aggregated data
                chunk_processor.flush_chunk(&mut all_column_data);

                // Update progress
                self.update_progress(
                    &mut enhanced_progress,
                    &mut progress_tracker,
                    &chunk_processor,
                    estimated_total_rows,
                    file_size_mb,
                    &header_names,
                );
            }
        }

        // Flush final chunk
        chunk_processor.flush_chunk(&mut all_column_data);

        // Finish progress tracking
        self.finish_progress(enhanced_progress);

        // Clean up memory tracking
        if let Some(ref manager) = self.progress_manager {
            manager.track_deallocation("main_column_data");
        }

        // Build report - ReportBuilder handles all report construction
        let report_builder = ReportBuilder::new(file_path, file_size_mb, estimated_total_rows);
        report_builder.build(all_column_data, chunk_processor.stats())
    }

    /// Setup progress tracking - delegates to ProgressManager
    fn setup_progress(
        &self,
        file_path: &Path,
        file_size_bytes: u64,
    ) -> (Option<EnhancedProgressBar>, ProgressTracker) {
        let mut enhanced_progress = None;
        let progress_tracker = ProgressTracker::new(self.progress_callback.clone());

        if let Some(ref manager) = self.progress_manager {
            let file_name = file_path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("file");

            // ProgressManager creates the EnhancedProgressBar for us
            enhanced_progress = manager.create_enhanced_file_progress(file_size_bytes, file_name);

            // Track memory allocation
            manager.track_allocation("main_column_data".to_string(), 0, "hashmap");
        }

        (enhanced_progress, progress_tracker)
    }

    /// Update progress tracking
    fn update_progress(
        &mut self,
        enhanced_progress: &mut Option<EnhancedProgressBar>,
        progress_tracker: &mut ProgressTracker,
        chunk_processor: &ChunkProcessor,
        estimated_total_rows: Option<usize>,
        file_size_mb: f64,
        headers: &[String],
    ) {
        let stats = chunk_processor.stats();

        if let Some(ref mut enhanced_pb) = enhanced_progress {
            // Use PerformanceIntelligence for hints if available
            let hints = if let Some(ref mut perf_intel) = self.performance_intelligence {
                let memory_tracker = self
                    .progress_manager
                    .as_ref()
                    .and_then(|m| m.memory_tracker.as_ref());

                let _metrics = perf_intel.analyze_performance(
                    file_size_mb,
                    stats.total_rows_processed as u64,
                    headers.len() as u64,
                    memory_tracker,
                );
                perf_intel.get_performance_hints()
            } else {
                Vec::new()
            };

            let hint_strs: Vec<&str> = hints.iter().map(|s| s.as_str()).collect();
            enhanced_pb.update_with_hints(
                stats.bytes_processed,
                stats.total_rows_processed as u64,
                headers.len() as u64,
                hint_strs,
            );
        } else {
            progress_tracker.update(
                stats.total_rows_processed,
                estimated_total_rows,
                stats.total_chunks,
            );
        }
    }

    /// Finish progress tracking and report memory leaks if any
    fn finish_progress(&self, enhanced_progress: Option<EnhancedProgressBar>) {
        if let Some(ref enhanced_pb) = enhanced_progress {
            enhanced_pb.finish_with_summary();

            // Check for memory leaks and report if any
            if let Some(leak_report) = enhanced_pb.check_memory_leaks() {
                if !leak_report.contains("No memory leaks") {
                    eprintln!("⚠️ Memory leak detection report:");
                    eprintln!("{}", leak_report);
                }
            }
        }
    }

    /// Estimate total rows by sampling first 1000 lines
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_streaming_profiler_basic() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "col1,col2").unwrap();
        writeln!(file, "a,b").unwrap();
        writeln!(file, "c,d").unwrap();
        file.flush().unwrap();

        let mut profiler = StreamingProfiler::new();
        let report = profiler.analyze_file(file.path()).unwrap();

        assert_eq!(report.file_info.total_rows, Some(2));
        assert_eq!(report.file_info.total_columns, 2);
    }
}
