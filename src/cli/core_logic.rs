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
#[derive(Default)]
pub struct AnalysisOptions {
    /// Show real-time progress bars
    pub progress: bool,
    /// Custom chunk size for streaming
    pub chunk_size: Option<usize>,
    /// Config file path
    pub config: Option<std::path::PathBuf>,
    /// Force streaming mode (currently unused, reserved for future use)
    #[allow(dead_code)]
    pub streaming: bool,
    /// Sample size for large files
    pub sample: Option<usize>,
    /// Verbosity level (0=quiet, 1=normal, 2=verbose, 3=debug)
    pub verbosity: Option<u8>,
}

/// Builder for creating a properly configured profiler with all improvements
pub struct ProfilerBuilder {
    options: AnalysisOptions,
    #[allow(dead_code)]
    config: DataprofConfig,
}

impl ProfilerBuilder {
    /// Create a new profiler builder with options and config
    pub fn new(options: AnalysisOptions, config: DataprofConfig) -> Self {
        Self { options, config }
    }

    /// Build a configured streaming profiler with all enhancements
    pub fn build_streaming(&self, _file_path: &Path) -> Result<DataProfiler> {
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
/// - Detects file format (CSV, JSON, JSONL, Parquet)
/// - Loads config file if specified
/// - Configures profiler with progress, chunk size, etc.
/// - Uses robust parsing with fallback
/// - Returns quality report with ISO metrics
pub fn analyze_file_with_options(
    file_path: &Path,
    options: AnalysisOptions,
) -> Result<QualityReport> {
    // Load config (from CLI arg, auto-discover, or use default)
    let mut config = if let Some(config_path) = &options.config {
        // Explicit config file path provided via CLI
        match DataprofConfig::load_from_file(config_path) {
            Ok(cfg) => {
                log::info!("Loaded configuration from: {}", config_path.display());
                cfg
            }
            Err(e) => {
                log::warn!(
                    "Failed to load config from {}: {}. Using defaults.",
                    config_path.display(),
                    e
                );
                DataprofConfig::default()
            }
        }
    } else {
        // No explicit config, try auto-discovery
        DataprofConfig::load_with_discovery()
    };

    // Override verbosity from CLI if provided
    if let Some(verbosity) = options.verbosity {
        config.output.verbosity = verbosity;
    }

    // Detect file format and route to appropriate parser
    #[cfg(feature = "parquet")]
    let is_parquet = super::commands::is_parquet_file(file_path);
    #[cfg(not(feature = "parquet"))]
    let is_parquet = false;

    if super::commands::is_json_file(file_path) {
        // JSON files: use specialized JSON parser
        dataprof::analyze_json_with_quality(file_path)
    } else if is_parquet {
        // Parquet files: use Parquet parser
        #[cfg(feature = "parquet")]
        {
            dataprof::analyze_parquet_with_quality(file_path)
        }
        #[cfg(not(feature = "parquet"))]
        {
            unreachable!()
        }
    } else {
        // CSV files: try streaming profiler, fallback to robust parser
        let builder = ProfilerBuilder::new(options, config.clone());
        let verbosity = config.output.verbosity;

        // Try streaming profiler first
        match builder.build_streaming(file_path) {
            Ok(mut profiler) => {
                match profiler.analyze_file(file_path) {
                    Ok(report) => {
                        // Clear progress line if it was shown
                        if builder.options.progress {
                            println!();
                        }
                        Ok(report)
                    }
                    Err(e) => {
                        // Streaming failed, try robust CSV parser with flexible mode
                        // Only show this warning at verbose level (actual failures shown regardless)
                        if verbosity >= 2 {
                            eprintln!(
                                "ℹ️  Streaming analysis failed: {}. Trying robust parser...",
                                e
                            );
                        }
                        dataprof::analyze_csv_robust(file_path)
                    }
                }
            }
            Err(e) => {
                // Build failed, try robust CSV parser
                // Only show this warning at verbose level
                if verbosity >= 2 {
                    eprintln!(
                        "ℹ️  Profiler initialization failed: {}. Trying robust parser...",
                        e
                    );
                }
                dataprof::analyze_csv_robust(file_path)
            }
        }
    }
}
