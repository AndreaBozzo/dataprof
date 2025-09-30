use anyhow::{Context, Result};
use glob::glob;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::analysis::MlReadinessScore;
use crate::output::progress::ProgressManager;
use crate::types::QualityReport;
use crate::{analyze_csv_robust, analyze_json_with_quality, MlReadinessEngine};

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
    /// Enable ML readiness scoring
    pub ml_score: bool,
    /// Generate ML code snippets
    pub ml_code: bool,
    /// HTML output file path for batch report
    pub html_output: Option<std::path::PathBuf>,
    /// Output script path for batch preprocessing script
    pub output_script: Option<std::path::PathBuf>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            parallel: true,
            max_concurrent: num_cpus::get(),
            recursive: false,
            extensions: vec!["csv".to_string(), "json".to_string(), "jsonl".to_string()],
            exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
            ml_score: false,
            ml_code: false,
            html_output: None,
            output_script: None,
        }
    }
}

/// Batch processor for multiple files and directories
pub struct BatchProcessor {
    config: BatchConfig,
    progress_manager: Option<ProgressManager>,
}

/// Result of batch processing operation
#[derive(Debug)]
pub struct BatchResult {
    /// Individual file reports
    pub reports: HashMap<PathBuf, QualityReport>,
    /// Processing errors by file
    pub errors: HashMap<PathBuf, String>,
    /// Summary statistics
    pub summary: BatchSummary,
    /// ML readiness scores per file (optional)
    pub ml_scores: HashMap<PathBuf, MlReadinessScore>,
    /// Generated HTML report path (optional)
    pub html_report_path: Option<PathBuf>,
    /// Generated preprocessing script path (optional)
    pub script_path: Option<PathBuf>,
}

/// Summary statistics for batch processing
#[derive(Debug)]
pub struct BatchSummary {
    pub total_files: usize,
    pub successful: usize,
    pub failed: usize,
    pub total_records: usize,
    pub total_issues: usize,
    pub average_quality_score: f64,
    pub processing_time_seconds: f64,
    /// Aggregated data quality metrics across all processed files
    pub aggregated_data_quality_metrics: Option<crate::types::DataQualityMetrics>,
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

        println!(
            "🔍 Found {} files matching pattern: {}",
            paths.len(),
            pattern
        );

        self.process_paths(&paths, start_time)
    }

    /// Process all supported files in a directory
    pub fn process_directory(&self, dir_path: &Path) -> Result<BatchResult> {
        let start_time = std::time::Instant::now();
        let paths = self.collect_directory_paths(dir_path)?;

        println!(
            "📁 Found {} files in directory: {}",
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

        println!("📋 Processing {} files from provided list", paths.len());

        self.process_paths(&paths, start_time)
    }

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
                Err(e) => eprintln!("⚠️ Glob error: {}", e),
            }
        }

        paths.sort();
        Ok(paths)
    }

    /// Collect file paths from directory
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
        if let Some(ext) = path.extension() {
            if let Some(ext_str) = ext.to_str() {
                if !self.config.extensions.contains(&ext_str.to_lowercase()) {
                    return false;
                }
            }
        } else {
            return false; // No extension
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

    /// Process collected file paths
    fn process_paths(
        &self,
        paths: &[PathBuf],
        start_time: std::time::Instant,
    ) -> Result<BatchResult> {
        if paths.is_empty() {
            return Ok(BatchResult {
                reports: HashMap::new(),
                errors: HashMap::new(),
                summary: BatchSummary {
                    total_files: 0,
                    successful: 0,
                    failed: 0,
                    total_records: 0,
                    total_issues: 0,
                    average_quality_score: 0.0,
                    processing_time_seconds: 0.0,
                    aggregated_data_quality_metrics: None,
                },
                ml_scores: HashMap::new(),
                html_report_path: None,
                script_path: None,
            });
        }

        // Create progress bar for batch processing
        let progress_bar = if let Some(pm) = &self.progress_manager {
            pm.create_batch_progress(paths.len() as u64)
        } else {
            None
        };

        // Configure thread pool if parallel processing is enabled
        if self.config.parallel {
            rayon::ThreadPoolBuilder::new()
                .num_threads(self.config.max_concurrent)
                .build_global()
                .context("Failed to configure thread pool")?;
        }

        // Process files
        // Type alias to simplify complex type
        type ProcessResult = (
            PathBuf,
            Result<(QualityReport, Option<MlReadinessScore>), String>,
        );
        let results: Vec<ProcessResult> = if self.config.parallel {
            // For parallel processing, we use a mutex to synchronize progress updates
            let progress_mutex = std::sync::Mutex::new(&progress_bar);

            paths
                .par_iter()
                .map(|path| {
                    let result = self.process_single_file(path);

                    // Update progress bar
                    if let Ok(pb_guard) = progress_mutex.lock() {
                        if let Some(pb) = pb_guard.as_ref() {
                            pb.inc(1);
                            pb.set_message(format!(
                                "Processed {}",
                                path.file_name().unwrap_or_default().to_string_lossy()
                            ));
                        }
                    }

                    (path.clone(), result)
                })
                .collect()
        } else {
            // Sequential processing with direct progress updates
            paths
                .iter()
                .enumerate()
                .map(|(i, path)| {
                    let result = self.process_single_file(path);

                    // Update progress bar
                    if let Some(pb) = &progress_bar {
                        pb.set_position((i + 1) as u64);
                        pb.set_message(format!(
                            "Processed {}",
                            path.file_name().unwrap_or_default().to_string_lossy()
                        ));
                    }

                    (path.clone(), result)
                })
                .collect()
        };

        // Collect results
        let mut reports = HashMap::new();
        let mut errors = HashMap::new();
        let mut ml_scores = HashMap::new();
        let mut total_records = 0;
        let mut total_issues = 0;
        let mut quality_scores = Vec::new();

        for (path, result) in results {
            match result {
                Ok((report, ml_score_opt)) => {
                    total_records += report
                        .column_profiles
                        .iter()
                        .map(|profile| profile.total_count)
                        .max()
                        .unwrap_or(0);
                    total_issues += report.issues.len();

                    if let Ok(score) = report.quality_score() {
                        quality_scores.push(score);
                    }

                    // Store ML score if available
                    if let Some(ml_score) = ml_score_opt {
                        ml_scores.insert(path.clone(), ml_score);
                    }

                    reports.insert(path, report);
                }
                Err(error) => {
                    errors.insert(path, error);
                }
            }
        }

        let processing_time = start_time.elapsed().as_secs_f64();
        let average_quality_score = if !quality_scores.is_empty() {
            quality_scores.iter().sum::<f64>() / quality_scores.len() as f64
        } else {
            0.0
        };

        // Calculate aggregated data quality metrics across all files
        let aggregated_data_quality_metrics = Self::calculate_aggregated_metrics(&reports);

        let summary = BatchSummary {
            total_files: paths.len(),
            successful: reports.len(),
            failed: errors.len(),
            total_records,
            total_issues,
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
            ml_scores,
            html_report_path: self.config.html_output.clone(),
            script_path: self.config.output_script.clone(),
        })
    }

    /// Process a single file with optional ML scoring
    fn process_single_file(
        &self,
        path: &Path,
    ) -> Result<(QualityReport, Option<MlReadinessScore>), String> {
        // First get the quality report
        let report = if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            match ext.to_lowercase().as_str() {
                "csv" => {
                    analyze_csv_robust(path).map_err(|e| format!("CSV processing failed: {}", e))?
                }
                "json" | "jsonl" => analyze_json_with_quality(path)
                    .map_err(|e| format!("JSON processing failed: {}", e))?,
                _ => return Err(format!("Unsupported file type: {}", ext)),
            }
        } else {
            return Err("File has no extension".to_string());
        };

        // Calculate ML score if enabled
        let ml_score = if self.config.ml_score {
            match self.calculate_ml_score_for_file(&report) {
                Ok(score) => Some(score),
                Err(e) => {
                    eprintln!("⚠️ ML scoring failed for {}: {}", path.display(), e);
                    None
                }
            }
        } else {
            None
        };

        Ok((report, ml_score))
    }

    /// Calculate ML readiness score for a single file's quality report
    fn calculate_ml_score_for_file(
        &self,
        report: &QualityReport,
    ) -> Result<MlReadinessScore, String> {
        let ml_engine = MlReadinessEngine::new();
        ml_engine
            .calculate_ml_score(report)
            .map_err(|e| format!("ML scoring failed: {}", e))
    }

    /// Print processing summary
    fn print_summary(&self, summary: &BatchSummary) {
        println!("\n📊 Batch Processing Summary");
        println!("├─ Total Files: {}", summary.total_files);
        println!("├─ Successful: {} ✅", summary.successful);
        println!("├─ Failed: {} ❌", summary.failed);
        println!("├─ Total Records: {}", summary.total_records);
        println!("├─ Total Issues: {}", summary.total_issues);
        println!(
            "├─ Average Quality Score: {:.1}%",
            summary.average_quality_score
        );
        println!(
            "└─ Processing Time: {:.2}s",
            summary.processing_time_seconds
        );

        if summary.failed > 0 {
            println!(
                "\n⚠️ {} files failed processing. Use --verbose for details.",
                summary.failed
            );
        }
    }

    /// Calculate aggregated data quality metrics across all processed files
    fn calculate_aggregated_metrics(
        reports: &HashMap<PathBuf, QualityReport>,
    ) -> Option<crate::types::DataQualityMetrics> {
        if reports.is_empty() {
            return None;
        }

        let mut all_metrics = Vec::new();
        for report in reports.values() {
            if let Some(metrics) = &report.data_quality_metrics {
                all_metrics.push(metrics);
            }
        }

        if all_metrics.is_empty() {
            return None;
        }

        // Aggregate completeness metrics
        let avg_missing_values_ratio: f64 = all_metrics
            .iter()
            .map(|m| m.missing_values_ratio)
            .sum::<f64>()
            / all_metrics.len() as f64;
        let avg_complete_records_ratio: f64 = all_metrics
            .iter()
            .map(|m| m.complete_records_ratio)
            .sum::<f64>()
            / all_metrics.len() as f64;

        let mut all_null_columns = std::collections::HashSet::new();
        for metrics in &all_metrics {
            for col in &metrics.null_columns {
                all_null_columns.insert(col.clone());
            }
        }
        let null_columns: Vec<String> = all_null_columns.into_iter().collect();

        // Aggregate consistency metrics
        let avg_data_type_consistency: f64 = all_metrics
            .iter()
            .map(|m| m.data_type_consistency)
            .sum::<f64>()
            / all_metrics.len() as f64;
        let total_format_violations: usize = all_metrics.iter().map(|m| m.format_violations).sum();
        let total_encoding_issues: usize = all_metrics.iter().map(|m| m.encoding_issues).sum();

        // Aggregate uniqueness metrics
        let total_duplicate_rows: usize = all_metrics.iter().map(|m| m.duplicate_rows).sum();
        let avg_key_uniqueness: f64 =
            all_metrics.iter().map(|m| m.key_uniqueness).sum::<f64>() / all_metrics.len() as f64;
        let high_cardinality_warning: bool = all_metrics.iter().any(|m| m.high_cardinality_warning);

        // Aggregate accuracy metrics
        let avg_outlier_ratio: f64 =
            all_metrics.iter().map(|m| m.outlier_ratio).sum::<f64>() / all_metrics.len() as f64;
        let total_range_violations: usize = all_metrics.iter().map(|m| m.range_violations).sum();
        let total_negative_values_in_positive: usize = all_metrics
            .iter()
            .map(|m| m.negative_values_in_positive)
            .sum();

        Some(crate::types::DataQualityMetrics {
            // Completeness
            missing_values_ratio: avg_missing_values_ratio,
            complete_records_ratio: avg_complete_records_ratio,
            null_columns,

            // Consistency
            data_type_consistency: avg_data_type_consistency,
            format_violations: total_format_violations,
            encoding_issues: total_encoding_issues,

            // Uniqueness
            duplicate_rows: total_duplicate_rows,
            key_uniqueness: avg_key_uniqueness,
            high_cardinality_warning,

            // Accuracy
            outlier_ratio: avg_outlier_ratio,
            range_violations: total_range_violations,
            negative_values_in_positive: total_negative_values_in_positive,

            // Timeliness (aggregate from all files)
            future_dates_count: all_metrics.iter().map(|m| m.future_dates_count).sum(),
            stale_data_ratio: all_metrics.iter().map(|m| m.stale_data_ratio).sum::<f64>()
                / all_metrics.len() as f64,
            temporal_violations: all_metrics.iter().map(|m| m.temporal_violations).sum(),
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
        // Create test CSV files in current directory to avoid temp path exclusions
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
            exclude_patterns: vec![], // No exclusions for test
            ml_score: false,
            ml_code: false,
            html_output: None,
            output_script: None,
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
        files: Vec<std::path::PathBuf>,
    }

    impl Drop for FileCleanup {
        fn drop(&mut self) {
            for file in &self.files {
                let _ = std::fs::remove_file(file);
            }
        }
    }
}
