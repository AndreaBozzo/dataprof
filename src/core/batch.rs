//! Batch processing module for analyzing multiple files efficiently
//!
//! Provides clean, DRY architecture for processing directories and file patterns
//! with parallel/sequential execution and comprehensive quality reporting.

use anyhow::{Context, Result};
use glob::glob;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[cfg(feature = "parquet")]
use crate::analyze_parquet_with_quality;
use crate::output::progress::ProgressManager;
use crate::types::{DataQualityMetrics, QualityReport};
use crate::{analyze_csv_robust, analyze_json_with_quality};

/// Configuration for batch processing operations
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Enable parallel processing
    pub parallel: bool,
    /// Maximum number of concurrent files to process
    pub max_concurrent: usize,
    /// Recursive directory scanning
    pub recursive: bool,
    /// File extensions to include
    pub extensions: Vec<String>,
    /// Files to exclude (supports glob patterns)
    pub exclude_patterns: Vec<String>,
    /// HTML output file path for batch report
    pub html_output: Option<PathBuf>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            parallel: true,
            max_concurrent: num_cpus::get(),
            recursive: false,
            extensions: vec![
                "csv".to_string(),
                "json".to_string(),
                "jsonl".to_string(),
                "parquet".to_string(),
            ],
            exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
            html_output: None,
        }
    }
}

/// Result of aggregating batch processing results
type AggregatedResults = (
    HashMap<PathBuf, QualityReport>,
    HashMap<PathBuf, String>,
    Vec<f64>,
    usize,
);

/// Batch processor for multiple files and directories
pub struct BatchProcessor {
    config: BatchConfig,
    progress_manager: Option<ProgressManager>,
}

/// Result of batch processing operation
#[derive(Debug, serde::Serialize)]
pub struct BatchResult {
    /// Individual file reports
    pub reports: HashMap<PathBuf, QualityReport>,
    /// Processing errors by file
    pub errors: HashMap<PathBuf, String>,
    /// Summary statistics
    pub summary: BatchSummary,
    /// Generated HTML report path (optional)
    pub html_report_path: Option<PathBuf>,
}

/// Summary statistics for batch processing
#[derive(Debug, serde::Serialize)]
pub struct BatchSummary {
    pub total_files: usize,
    pub successful: usize,
    pub failed: usize,
    pub total_records: usize,
    pub average_quality_score: f64,
    pub processing_time_seconds: f64,
    /// Aggregated data quality metrics across all processed files
    pub aggregated_data_quality_metrics: Option<DataQualityMetrics>,
}

impl BatchProcessor {
    /// Create a new batch processor with default configuration
    pub fn new() -> Self {
        Self {
            config: BatchConfig::default(),
            progress_manager: None,
        }
    }

    /// Create a batch processor with custom configuration
    pub fn with_config(config: BatchConfig) -> Self {
        Self {
            config,
            progress_manager: None,
        }
    }

    /// Create a batch processor with progress manager
    pub fn with_progress(config: BatchConfig, progress_manager: ProgressManager) -> Self {
        Self {
            config,
            progress_manager: Some(progress_manager),
        }
    }

    /// Process files matching a glob pattern
    pub fn process_glob(&self, pattern: &str) -> Result<BatchResult> {
        let start_time = std::time::Instant::now();
        let paths = self.collect_glob_paths(pattern)?;

        log::info!("Found {} files matching pattern: {}", paths.len(), pattern);

        self.process_paths(&paths, start_time)
    }

    /// Process all supported files in a directory
    pub fn process_directory(&self, dir_path: &Path) -> Result<BatchResult> {
        let start_time = std::time::Instant::now();
        let paths = self.collect_directory_paths(dir_path)?;

        log::info!(
            "Found {} files in directory: {}",
            paths.len(),
            dir_path.display()
        );

        self.process_paths(&paths, start_time)
    }

    /// Process a specific list of file paths
    pub fn process_files(&self, file_paths: &[PathBuf]) -> Result<BatchResult> {
        let start_time = std::time::Instant::now();
        let paths: Vec<PathBuf> = file_paths
            .iter()
            .filter(|p| self.should_include_file(p))
            .cloned()
            .collect();

        log::info!("Processing {} files from provided list", paths.len());

        self.process_paths(&paths, start_time)
    }

    // ===== Internal Methods =====

    /// Collect file paths from glob pattern
    fn collect_glob_paths(&self, pattern: &str) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();

        for entry in glob(pattern).context("Failed to parse glob pattern")? {
            match entry {
                Ok(path) => {
                    if path.is_file() && self.should_include_file(&path) {
                        paths.push(path);
                    }
                }
                Err(e) => log::warn!("Glob error: {}", e),
            }
        }

        paths.sort();
        Ok(paths)
    }

    /// Collect file paths from directory (recursive or single level)
    fn collect_directory_paths(&self, dir_path: &Path) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();

        if self.config.recursive {
            self.collect_recursive(&mut paths, dir_path)?;
        } else {
            self.collect_single_dir(&mut paths, dir_path)?;
        }

        paths.sort();
        Ok(paths)
    }

    /// Recursively collect file paths
    fn collect_recursive(&self, paths: &mut Vec<PathBuf>, dir_path: &Path) -> Result<()> {
        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                self.collect_recursive(paths, &path)?;
            } else if path.is_file() && self.should_include_file(&path) {
                paths.push(path);
            }
        }
        Ok(())
    }

    /// Collect files from single directory
    fn collect_single_dir(&self, paths: &mut Vec<PathBuf>, dir_path: &Path) -> Result<()> {
        for entry in std::fs::read_dir(dir_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && self.should_include_file(&path) {
                paths.push(path);
            }
        }
        Ok(())
    }

    /// Check if file should be included based on extension and exclusion patterns
    fn should_include_file(&self, path: &Path) -> bool {
        // Check extension
        let ext_str = match path.extension().and_then(|e| e.to_str()) {
            Some(ext) => ext.to_lowercase(),
            None => return false,
        };

        if !self.config.extensions.contains(&ext_str) {
            return false;
        }

        // Check exclusion patterns
        let path_str = path.to_string_lossy();
        for pattern in &self.config.exclude_patterns {
            if glob_match::glob_match(pattern, &path_str) {
                return false;
            }
        }

        true
    }

    /// Process collected file paths (core batch logic)
    fn process_paths(
        &self,
        paths: &[PathBuf],
        start_time: std::time::Instant,
    ) -> Result<BatchResult> {
        if paths.is_empty() {
            return Ok(Self::empty_result());
        }

        // Create progress bar for batch processing
        let progress_bar = self
            .progress_manager
            .as_ref()
            .and_then(|pm| pm.create_batch_progress(paths.len() as u64));

        // Process files (parallel or sequential)
        let results = if self.config.parallel {
            // Build a scoped thread pool for this batch operation
            // This avoids issues with build_global() which can only be called once
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(self.config.max_concurrent)
                .build()
                .context("Failed to configure thread pool")?;

            // Execute processing within the pool's scope
            pool.install(|| self.process_parallel(paths, &progress_bar))
        } else {
            self.process_sequential(paths, &progress_bar)
        };

        // Aggregate results
        let (reports, errors, quality_scores, total_records) = self.aggregate_results(results);

        // Calculate summary
        let processing_time = start_time.elapsed().as_secs_f64();
        let average_quality_score = if !quality_scores.is_empty() {
            quality_scores.iter().sum::<f64>() / quality_scores.len() as f64
        } else {
            0.0
        };

        let aggregated_data_quality_metrics = Self::calculate_aggregated_metrics(&reports);

        let summary = BatchSummary {
            total_files: paths.len(),
            successful: reports.len(),
            failed: errors.len(),
            total_records,
            average_quality_score,
            processing_time_seconds: processing_time,
            aggregated_data_quality_metrics,
        };

        // Finish progress bar
        if let Some(pb) = &progress_bar {
            pb.finish_with_message(format!(
                "Completed {} files ({} successful, {} failed)",
                summary.total_files, summary.successful, summary.failed
            ));
        }

        // Print summary
        self.print_summary(&summary);

        Ok(BatchResult {
            reports,
            errors,
            summary,
            html_report_path: self.config.html_output.clone(),
        })
    }

    /// Process files in parallel using Rayon
    fn process_parallel(
        &self,
        paths: &[PathBuf],
        progress_bar: &Option<indicatif::ProgressBar>,
    ) -> Vec<(PathBuf, Result<QualityReport, String>)> {
        let progress_mutex = std::sync::Mutex::new(progress_bar);

        paths
            .par_iter()
            .map(|path| {
                let result = self.process_single_file(path);

                // Update progress bar
                if let Ok(pb_guard) = progress_mutex.lock()
                    && let Some(pb) = pb_guard.as_ref()
                {
                    pb.inc(1);
                    pb.set_message(format!(
                        "Processed {}",
                        path.file_name().unwrap_or_default().to_string_lossy()
                    ));
                }

                (path.clone(), result)
            })
            .collect()
    }

    /// Process files sequentially
    fn process_sequential(
        &self,
        paths: &[PathBuf],
        progress_bar: &Option<indicatif::ProgressBar>,
    ) -> Vec<(PathBuf, Result<QualityReport, String>)> {
        paths
            .iter()
            .enumerate()
            .map(|(i, path)| {
                let result = self.process_single_file(path);

                // Update progress bar
                if let Some(pb) = progress_bar {
                    pb.set_position((i + 1) as u64);
                    pb.set_message(format!(
                        "Processed {}",
                        path.file_name().unwrap_or_default().to_string_lossy()
                    ));
                }

                (path.clone(), result)
            })
            .collect()
    }

    /// Process a single file (CSV or JSON)
    fn process_single_file(&self, path: &Path) -> Result<QualityReport, String> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .ok_or_else(|| "File has no extension".to_string())?;

        match ext.to_lowercase().as_str() {
            "csv" => analyze_csv_robust(path).map_err(|e| format!("CSV processing failed: {}", e)),
            "json" | "jsonl" => analyze_json_with_quality(path)
                .map_err(|e| format!("JSON processing failed: {}", e)),
            "parquet" => {
                #[cfg(feature = "parquet")]
                {
                    analyze_parquet_with_quality(path)
                        .map_err(|e| format!("Parquet processing failed: {}", e))
                }
                #[cfg(not(feature = "parquet"))]
                {
                    Err("Parquet support not enabled. Rebuild with --features parquet".to_string())
                }
            }
            _ => Err(format!("Unsupported file type: {}", ext)),
        }
    }

    /// Aggregate processing results into structured data
    fn aggregate_results(
        &self,
        results: Vec<(PathBuf, Result<QualityReport, String>)>,
    ) -> AggregatedResults {
        let mut reports = HashMap::new();
        let mut errors = HashMap::new();
        let mut quality_scores = Vec::new();
        let mut total_records = 0;

        for (path, result) in results {
            match result {
                Ok(report) => {
                    total_records += report
                        .column_profiles
                        .iter()
                        .map(|profile| profile.total_count)
                        .max()
                        .unwrap_or(0);

                    let score = report.quality_score();
                    quality_scores.push(score);

                    reports.insert(path, report);
                }
                Err(error) => {
                    errors.insert(path, error);
                }
            }
        }

        (reports, errors, quality_scores, total_records)
    }

    /// Print processing summary
    fn print_summary(&self, summary: &BatchSummary) {
        println!("\n📊 Batch Processing Summary");
        println!("├─ Total Files: {}", summary.total_files);
        println!("├─ Successful: {} ✅", summary.successful);
        println!("├─ Failed: {} ❌", summary.failed);
        println!("├─ Total Records: {}", summary.total_records);
        println!(
            "├─ Average Quality Score: {:.1}%",
            summary.average_quality_score
        );
        println!(
            "└─ Processing Time: {:.2}s",
            summary.processing_time_seconds
        );

        if summary.failed > 0 {
            log::warn!(
                "\n{} files failed to process. Check logs for details.",
                summary.failed
            );
        }
    }

    /// Create an empty batch result (for zero files)
    fn empty_result() -> BatchResult {
        BatchResult {
            reports: HashMap::new(),
            errors: HashMap::new(),
            summary: BatchSummary {
                total_files: 0,
                successful: 0,
                failed: 0,
                total_records: 0,
                average_quality_score: 0.0,
                processing_time_seconds: 0.0,
                aggregated_data_quality_metrics: None,
            },
            html_report_path: None,
        }
    }

    /// Calculate aggregated data quality metrics across all processed files
    fn calculate_aggregated_metrics(
        reports: &HashMap<PathBuf, QualityReport>,
    ) -> Option<DataQualityMetrics> {
        if reports.is_empty() {
            return None;
        }

        let all_metrics: Vec<&DataQualityMetrics> = reports
            .values()
            .map(|report| &report.data_quality_metrics)
            .collect();

        if all_metrics.is_empty() {
            return None;
        }

        let count = all_metrics.len() as f64;

        // Helper to compute average
        let avg = |f: fn(&DataQualityMetrics) -> f64| {
            all_metrics.iter().map(|m| f(m)).sum::<f64>() / count
        };

        // Helper to compute sum
        let sum = |f: fn(&DataQualityMetrics) -> usize| all_metrics.iter().map(|m| f(m)).sum();

        // Aggregate null columns
        let mut all_null_columns = std::collections::HashSet::new();
        for metrics in &all_metrics {
            for col in &metrics.null_columns {
                all_null_columns.insert(col.clone());
            }
        }

        Some(DataQualityMetrics {
            // Completeness
            missing_values_ratio: avg(|m| m.missing_values_ratio),
            complete_records_ratio: avg(|m| m.complete_records_ratio),
            null_columns: all_null_columns.into_iter().collect(),

            // Consistency
            data_type_consistency: avg(|m| m.data_type_consistency),
            format_violations: sum(|m| m.format_violations),
            encoding_issues: sum(|m| m.encoding_issues),

            // Uniqueness
            duplicate_rows: sum(|m| m.duplicate_rows),
            key_uniqueness: avg(|m| m.key_uniqueness),
            high_cardinality_warning: all_metrics.iter().any(|m| m.high_cardinality_warning),

            // Accuracy
            outlier_ratio: avg(|m| m.outlier_ratio),
            range_violations: sum(|m| m.range_violations),
            negative_values_in_positive: sum(|m| m.negative_values_in_positive),

            // Timeliness
            future_dates_count: sum(|m| m.future_dates_count),
            stale_data_ratio: avg(|m| m.stale_data_ratio),
            temporal_violations: sum(|m| m.temporal_violations),
        })
    }
}

impl Default for BatchProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();
        assert!(config.parallel);
        assert!(config.max_concurrent > 0);
        assert!(!config.recursive);
        assert!(config.extensions.contains(&"csv".to_string()));
    }

    #[test]
    fn test_should_include_file() {
        let processor = BatchProcessor::new();

        // Should include CSV files
        assert!(processor.should_include_file(Path::new("test.csv")));
        assert!(processor.should_include_file(Path::new("data.json")));

        // Should exclude other extensions
        assert!(!processor.should_include_file(Path::new("test.txt")));
        assert!(!processor.should_include_file(Path::new("data.xml")));

        // Should exclude hidden files
        assert!(!processor.should_include_file(Path::new(".hidden.csv")));
    }

    #[test]
    fn test_process_files() -> Result<()> {
        let temp_dir = std::env::temp_dir();
        let test_file1 = temp_dir.join("test_batch1.csv");
        let test_file2 = temp_dir.join("test_batch2.csv");

        // Write test data
        std::fs::write(&test_file1, "name,age\nAlice,25\nBob,30\n")?;
        std::fs::write(&test_file2, "id,value\n1,100\n")?;

        // Ensure cleanup
        let _cleanup = FileCleanup {
            files: vec![test_file1.clone(), test_file2.clone()],
        };

        let config = BatchConfig {
            parallel: false,
            max_concurrent: 1,
            recursive: false,
            extensions: vec!["csv".to_string()],
            exclude_patterns: vec![],
            html_output: None,
        };
        let processor = BatchProcessor::with_config(config);
        let files = vec![test_file1, test_file2];

        let result = processor.process_files(&files)?;

        assert_eq!(result.summary.total_files, 2);
        assert_eq!(result.summary.successful, 2);
        assert_eq!(result.summary.failed, 0);

        Ok(())
    }

    struct FileCleanup {
        files: Vec<PathBuf>,
    }

    impl Drop for FileCleanup {
        fn drop(&mut self) {
            for file in &self.files {
                let _ = std::fs::remove_file(file);
            }
        }
    }
}
