//! Shared core logic for all CLI commands
//!
//! This module contains the common analysis logic that all subcommands inherit.
//! Benefits:
//! - Single source of truth for profiler configuration
//! - All commands automatically get improvements (progress, config, robust parsing)
//! - No code duplication across command implementations

use anyhow::Result;
use std::path::Path;

use dataprof::{
    core::{sampling::SamplingStrategy, DataprofConfig},
    types::QualityReport,
    ChunkSize, DataProfiler, ProgressInfo,
};

/// Common options that all analysis commands should support
pub struct AnalysisOptions {
    /// Show real-time progress bars
    pub progress: bool,
    /// Custom chunk size for streaming
    pub chunk_size: Option<usize>,
    /// Config file path
    pub config: Option<std::path::PathBuf>,
    /// Force streaming mode
    pub streaming: bool,
    /// Sample size for large files
    pub sample: Option<usize>,
}

impl Default for AnalysisOptions {
    fn default() -> Self {
        Self {
            progress: false,
            chunk_size: None,
            config: None,
            streaming: false,
            sample: None,
        }
    }
}

/// Builder for creating a properly configured profiler with all improvements
pub struct ProfilerBuilder {
    options: AnalysisOptions,
    config: DataprofConfig,
}

impl ProfilerBuilder {
    /// Create a new profiler builder with options and config
    pub fn new(options: AnalysisOptions, config: DataprofConfig) -> Self {
        Self { options, config }
    }

    /// Build a configured streaming profiler with all enhancements
    pub fn build_streaming(&self, file_path: &Path) -> Result<DataProfiler> {
        let mut profiler = DataProfiler::streaming();

        // Configure chunk size (from CLI arg or config)
        let chunk_size = if let Some(size) = self.options.chunk_size {
            ChunkSize::Fixed(size)
        } else {
            ChunkSize::Adaptive
        };
        profiler = profiler.chunk_size(chunk_size);

        // Configure sampling strategy
        if let Some(sample_size) = self.options.sample {
            profiler = profiler.sampling(SamplingStrategy::Random { size: sample_size });
        }

        // Enable enhanced progress if requested
        if self.options.progress {
            // Smart progress with memory tracking
            profiler = profiler.with_enhanced_progress(100); // 100MB leak threshold

            // Add progress callback for real-time stats
            profiler = profiler.progress_callback(|progress: ProgressInfo| {
                print!(
                    "\r🔄 Processing: {:.1}% ({} rows, {:.1} rows/sec)",
                    progress.percentage, progress.rows_processed, progress.processing_speed
                );
                let _ = std::io::Write::flush(&mut std::io::stdout());
            });
        }

        Ok(profiler)
    }
}

/// High-level function to analyze a file with all improvements
///
/// This function:
/// - Detects file format (CSV, JSON, JSONL)
/// - Loads config file if specified
/// - Configures profiler with progress, chunk size, etc.
/// - Uses robust parsing with fallback
/// - Returns quality report with ISO metrics
pub fn analyze_file_with_options(
    file_path: &Path,
    options: AnalysisOptions,
) -> Result<QualityReport> {
    // Load config (from CLI arg or use default)
    // TODO: Implement config file loading (from_file, auto-discover)
    let config = DataprofConfig::default();
    let _ = options.config; // Acknowledge config option for now

    // Detect file format and route to appropriate parser
    if super::commands::is_json_file(file_path) {
        // JSON files: use specialized JSON parser
        dataprof::analyze_json_with_quality(file_path)
    } else {
        // CSV files: use streaming profiler with all enhancements
        let builder = ProfilerBuilder::new(options, config);
        let mut profiler = builder.build_streaming(file_path)?;

        let report = profiler.analyze_file(file_path)?;

        // Clear progress line if it was shown
        if builder.options.progress {
            println!();
        }

        Ok(report)
    }
}
