use anyhow::{Context, Result};
use glob::glob;
use rayon::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::types::QualityReport;
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
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            parallel: true,
            max_concurrent: num_cpus::get(),
            recursive: false,
            extensions: vec!["csv".to_string(), "json".to_string(), "jsonl".to_string()],
            exclude_patterns: vec!["**/.*".to_string(), "**/*tmp*".to_string()],
        }
    }
}

/// Batch processor for multiple files and directories
pub struct BatchProcessor {
    config: BatchConfig,
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
}

impl BatchProcessor {
    /// Create a new batch processor with default configuration
    pub fn new() -> Self {
        Self {
            config: BatchConfig::default(),
        }
    }

    /// Create a batch processor with custom configuration
    pub fn with_config(config: BatchConfig) -> Self {
        Self { config }
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
                },
            });
        }

        // Configure thread pool if parallel processing is enabled
        if self.config.parallel {
            rayon::ThreadPoolBuilder::new()
                .num_threads(self.config.max_concurrent)
                .build_global()
                .context("Failed to configure thread pool")?;
        }

        // Process files
        let results: Vec<(PathBuf, Result<QualityReport, String>)> = if self.config.parallel {
            paths
                .par_iter()
                .map(|path| {
                    let result = self.process_single_file(path);
                    (path.clone(), result)
                })
                .collect()
        } else {
            paths
                .iter()
                .map(|path| {
                    let result = self.process_single_file(path);
                    (path.clone(), result)
                })
                .collect()
        };

        // Collect results
        let mut reports = HashMap::new();
        let mut errors = HashMap::new();
        let mut total_records = 0;
        let mut total_issues = 0;
        let mut quality_scores = Vec::new();

        for (path, result) in results {
            match result {
                Ok(report) => {
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

        let summary = BatchSummary {
            total_files: paths.len(),
            successful: reports.len(),
            failed: errors.len(),
            total_records,
            total_issues,
            average_quality_score,
            processing_time_seconds: processing_time,
        };

        // Print summary
        self.print_summary(&summary);

        Ok(BatchResult {
            reports,
            errors,
            summary,
        })
    }

    /// Process a single file
    fn process_single_file(&self, path: &Path) -> Result<QualityReport, String> {
        // Determine file type and process
        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            match ext.to_lowercase().as_str() {
                "csv" => {
                    analyze_csv_robust(path).map_err(|e| format!("CSV processing failed: {}", e))
                }
                "json" | "jsonl" => analyze_json_with_quality(path)
                    .map_err(|e| format!("JSON processing failed: {}", e)),
                _ => Err(format!("Unsupported file type: {}", ext)),
            }
        } else {
            Err("File has no extension".to_string())
        }
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
        };
        let processor = BatchProcessor { config };
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
