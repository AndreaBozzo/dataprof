use anyhow::Result;
use csv::ReaderBuilder;
use std::path::Path;
use std::sync::Arc;

use crate::core::memory_tracker::MemoryTracker;
use crate::core::performance::PerformanceIntelligence;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::progress::{ProgressCallback, ProgressTracker};
use crate::output::progress::{generate_performance_hints, EnhancedProgressBar};
use crate::types::{FileInfo, QualityReport, ScanInfo};
use crate::{analysis::analyze_column, QualityChecker};

pub struct StreamingProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    progress_callback: Option<ProgressCallback>,
    memory_tracker: Option<MemoryTracker>,
    enable_enhanced_progress: bool,
    performance_intelligence: Option<PerformanceIntelligence>,
}

impl StreamingProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            progress_callback: None,
            memory_tracker: None,
            enable_enhanced_progress: false,
            performance_intelligence: None,
        }
    }

    /// Enable enhanced progress tracking with memory monitoring
    pub fn with_enhanced_progress(mut self, leak_threshold_mb: usize) -> Self {
        self.memory_tracker = Some(MemoryTracker::new(leak_threshold_mb));
        self.enable_enhanced_progress = true;
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

    pub fn analyze_file(&mut self, file_path: &Path) -> Result<QualityReport> {
        let metadata = std::fs::metadata(file_path)?;
        let file_size_bytes = metadata.len();
        let file_size_mb = file_size_bytes as f64 / 1_048_576.0;

        let start = std::time::Instant::now();

        // Estimate total rows for progress tracking
        let estimated_total_rows = self.estimate_total_rows(file_path)?;

        // Calculate optimal chunk size
        let chunk_size = self.chunk_size.calculate(file_size_bytes);

        // Set up enhanced or standard progress tracking
        let mut progress_tracker = ProgressTracker::new(self.progress_callback.clone());
        let mut enhanced_progress: Option<EnhancedProgressBar> = None;

        // Initialize enhanced progress if enabled
        if self.enable_enhanced_progress {
            use indicatif::{ProgressBar, ProgressStyle};

            let pb = ProgressBar::new(file_size_bytes);
            if let Ok(style) = ProgressStyle::with_template(
                "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) | {msg}"
            ) {
                pb.set_style(style.progress_chars("🔄🔵⚪"));
            }
            pb.set_message("Streaming analysis...");
            enhanced_progress = Some(EnhancedProgressBar::new(pb, self.memory_tracker.clone()));
        }

        // Initialize aggregated data storage
        let mut all_column_data: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut total_rows_processed = 0;
        let mut chunk_count = 0;
        let mut bytes_processed = 0u64;

        // Track main data structure allocation
        if let Some(ref tracker) = self.memory_tracker {
            tracker.track_allocation(
                "main_column_data".to_string(),
                std::mem::size_of_val(&all_column_data),
                "hashmap",
            );
        }

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
                bytes_processed += (rows_in_current_chunk * headers.len() * 50) as u64; // Rough estimate

                // Update enhanced progress and performance intelligence if available
                if let Some(ref mut enhanced_pb) = enhanced_progress {
                    // Use performance intelligence for advanced hints if available
                    let hints: Vec<String> =
                        if let Some(ref mut perf_intel) = self.performance_intelligence {
                            let _metrics = perf_intel.analyze_performance(
                                file_size_mb,
                                total_rows_processed as u64,
                                headers.len() as u64,
                                self.memory_tracker.as_ref(),
                            );
                            perf_intel.get_performance_hints()
                        } else {
                            // Fallback to legacy hint generation
                            let memory_mb =
                                self.memory_tracker.as_ref().map(|t| t.get_memory_stats().2);
                            generate_performance_hints(
                                file_size_mb,
                                total_rows_processed as u64,
                                total_rows_processed as f64 / start.elapsed().as_secs_f64(),
                                memory_mb,
                            )
                        };

                    let hint_strs: Vec<&str> = hints.iter().map(|s| s.as_str()).collect();
                    enhanced_pb.update_with_hints(
                        bytes_processed,
                        total_rows_processed as u64,
                        headers.len() as u64,
                        hint_strs,
                    );
                } else {
                    // Standard progress update
                    progress_tracker.update(
                        total_rows_processed,
                        estimated_total_rows,
                        chunk_count,
                    );
                }

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
            bytes_processed += (rows_in_current_chunk * headers.len() * 50) as u64;

            if let Some(ref mut enhanced_pb) = enhanced_progress {
                enhanced_pb.update(
                    bytes_processed,
                    total_rows_processed as u64,
                    headers.len() as u64,
                    Some("Final processing..."),
                );
            } else {
                progress_tracker.update(total_rows_processed, estimated_total_rows, chunk_count);
            }
        }

        // Finish progress tracking
        if let Some(ref enhanced_pb) = enhanced_progress {
            enhanced_pb.finish_with_summary();

            // Check for memory leaks and report if any
            if let Some(leak_report) = enhanced_pb.check_memory_leaks() {
                if !leak_report.contains("No memory leaks") {
                    eprintln!("⚠️ Memory leak detection report:");
                    eprintln!("{}", leak_report);
                }
            }
        } else {
            progress_tracker.finish(total_rows_processed);
        }

        // Clean up tracked allocations
        if let Some(ref tracker) = self.memory_tracker {
            tracker.track_deallocation("main_column_data");
        }

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
            data_quality_metrics: None,
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
