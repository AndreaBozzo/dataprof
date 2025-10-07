use anyhow::Result;
use std::path::Path;
use std::time::Instant;

use crate::engines::selection::{EngineSelector, EngineType, ProcessingType};
use crate::engines::streaming::{
    MemoryEfficientProfiler, StreamingProfiler, TrueStreamingProfiler,
};
use crate::types::QualityReport;

/// Performance metrics for engine execution
#[derive(Debug, Clone)]
pub struct EnginePerformance {
    pub engine_type: EngineType,
    pub execution_time_ms: u128,
    pub memory_usage_mb: f64,
    pub rows_per_second: f64,
    pub success: bool,
    pub error_message: Option<String>,
}

/// Logger for engine selection and performance
pub struct EngineLogger {
    pub(crate) enabled: bool,
}

impl EngineLogger {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    pub fn log_selection(&self, engine: &EngineType, reasoning: &str) {
        if self.enabled {
            println!("🚀 Engine selected: {:?} - {}", engine, reasoning);
        }
    }

    pub fn log_fallback(&self, from: &EngineType, to: &EngineType, reason: &str) {
        if self.enabled {
            println!("⚠️  Fallback: {:?} → {:?} - {}", from, to, reason);
        }
    }

    pub fn log_performance(&self, performance: &EnginePerformance) {
        if self.enabled {
            if performance.success {
                println!(
                    "✅ {:?}: {:.1}s, {:.0} rows/sec, {:.1}MB memory",
                    performance.engine_type,
                    performance.execution_time_ms as f64 / 1000.0,
                    performance.rows_per_second,
                    performance.memory_usage_mb
                );
            } else {
                println!(
                    "❌ {:?}: Failed - {}",
                    performance.engine_type,
                    performance
                        .error_message
                        .as_deref()
                        .unwrap_or("Unknown error")
                );
            }
        }
    }

    pub fn log_comparison(&self, performances: &[EnginePerformance]) {
        if !self.enabled || performances.len() < 2 {
            return;
        }

        println!("📊 Engine Performance Comparison:");
        let mut sorted = performances.to_vec();
        sorted.sort_by(|a, b| a.execution_time_ms.cmp(&b.execution_time_ms));

        for (i, perf) in sorted.iter().enumerate() {
            let icon = if i == 0 {
                "🥇"
            } else if i == 1 {
                "🥈"
            } else {
                "🥉"
            };
            println!(
                "  {} {:?}: {:.2}s ({:.0} rows/sec)",
                icon,
                perf.engine_type,
                perf.execution_time_ms as f64 / 1000.0,
                perf.rows_per_second
            );
        }
    }
}

/// Adaptive profiler that automatically selects the best engine and handles fallbacks
pub struct AdaptiveProfiler {
    selector: EngineSelector,
    logger: EngineLogger,
    enable_fallback: bool,
    enable_performance_logging: bool,
}

impl AdaptiveProfiler {
    pub fn new() -> Self {
        Self {
            selector: EngineSelector::new(),
            logger: EngineLogger::new(true), // Enable logging by default
            enable_fallback: true,
            enable_performance_logging: false,
        }
    }

    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.logger = EngineLogger::new(enabled);
        self
    }

    pub fn with_fallback(mut self, enabled: bool) -> Self {
        self.enable_fallback = enabled;
        self
    }

    pub fn with_performance_logging(mut self, enabled: bool) -> Self {
        self.enable_performance_logging = enabled;
        self
    }

    /// Select the best engine for a file without running benchmarks
    pub fn select_engine(&self, file_path: &Path) -> Result<EngineType> {
        let characteristics = self.selector.analyze_file_characteristics(file_path)?;
        let recommendation = self
            .selector
            .select_engine(&characteristics, ProcessingType::BatchAnalysis);
        Ok(recommendation.primary_engine)
    }

    /// Analyze file with automatic engine selection and fallback
    pub fn analyze_file(&self, file_path: &Path) -> Result<QualityReport> {
        self.analyze_file_with_context(file_path, ProcessingType::BatchAnalysis)
    }

    /// Analyze file with specific processing context
    pub fn analyze_file_with_context(
        &self,
        file_path: &Path,
        processing_type: ProcessingType,
    ) -> Result<QualityReport> {
        // Check if this is a Parquet file - handle separately as it's a binary format
        // Use both extension check (fast) and magic number check (robust)
        let is_parquet = file_path
            .extension()
            .map(|ext| ext.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false);

        #[cfg(feature = "parquet")]
        let is_parquet = is_parquet || crate::parsers::parquet::is_parquet_file(file_path);

        if is_parquet {
            #[cfg(feature = "parquet")]
            {
                if self.logger.enabled {
                    println!("🗂️  Parquet file detected - using native Parquet parser");
                }
                return crate::parsers::parquet::analyze_parquet_with_quality(file_path);
            }
            #[cfg(not(feature = "parquet"))]
            {
                return Err(anyhow::anyhow!(
                    "Parquet file detected but parquet feature is not enabled. \
                     Recompile with --features parquet"
                ));
            }
        }

        // Analyze file characteristics
        let characteristics = self.selector.analyze_file_characteristics(file_path)?;

        // Get engine recommendation
        let recommendation = self
            .selector
            .select_engine(&characteristics, processing_type);

        self.logger
            .log_selection(&recommendation.primary_engine, &recommendation.reasoning);

        // Try primary engine first
        let primary_result = self.try_engine(&recommendation.primary_engine, file_path);

        match primary_result {
            Ok(report) => {
                if self.enable_performance_logging {
                    // For successful execution, we'd collect performance data here
                    // This would require modifying the engine implementations to return metrics
                }
                Ok(report)
            }
            Err(primary_error) => {
                if !self.enable_fallback || recommendation.fallback_engines.is_empty() {
                    return Err(primary_error);
                }

                // Try fallback engines
                for fallback_engine in &recommendation.fallback_engines {
                    self.logger.log_fallback(
                        &recommendation.primary_engine,
                        fallback_engine,
                        &format!("Primary engine failed: {}", primary_error),
                    );

                    match self.try_engine(fallback_engine, file_path) {
                        Ok(report) => {
                            self.logger.log_performance(&EnginePerformance {
                                engine_type: fallback_engine.clone(),
                                execution_time_ms: 0, // Would need to measure this
                                memory_usage_mb: 0.0,
                                rows_per_second: 0.0,
                                success: true,
                                error_message: None,
                            });
                            return Ok(report);
                        }
                        Err(fallback_error) => {
                            self.logger.log_performance(&EnginePerformance {
                                engine_type: fallback_engine.clone(),
                                execution_time_ms: 0,
                                memory_usage_mb: 0.0,
                                rows_per_second: 0.0,
                                success: false,
                                error_message: Some(fallback_error.to_string()),
                            });
                            continue;
                        }
                    }
                }

                // All engines failed
                Err(anyhow::anyhow!(
                    "All engines failed. Primary: {}. Tried {} fallbacks.",
                    primary_error,
                    recommendation.fallback_engines.len()
                ))
            }
        }
    }

    /// Try to execute analysis with a specific engine
    fn try_engine(&self, engine_type: &EngineType, file_path: &Path) -> Result<QualityReport> {
        let start = Instant::now();

        let result = match engine_type {
            EngineType::Arrow => {
                #[cfg(feature = "arrow")]
                {
                    use crate::engines::columnar::ArrowProfiler;
                    let profiler = ArrowProfiler::new();
                    profiler.analyze_csv_file(file_path)
                }
                #[cfg(not(feature = "arrow"))]
                {
                    Err(anyhow::anyhow!(
                        "Arrow engine not available (compiled without arrow feature)"
                    ))
                }
            }
            EngineType::TrueStreaming => {
                let profiler = TrueStreamingProfiler::new();
                profiler.analyze_file(file_path)
            }
            EngineType::MemoryEfficient => {
                let profiler = MemoryEfficientProfiler::new();
                profiler.analyze_file(file_path)
            }
            EngineType::Streaming => {
                let mut profiler = StreamingProfiler::new();
                profiler.analyze_file(file_path)
            }
        };

        let execution_time = start.elapsed();

        // Log performance if enabled
        if self.enable_performance_logging {
            let performance = match &result {
                Ok(report) => EnginePerformance {
                    engine_type: engine_type.clone(),
                    execution_time_ms: execution_time.as_millis(),
                    memory_usage_mb: 0.0, // Would need to measure this
                    rows_per_second: if execution_time.as_secs() > 0 {
                        report.scan_info.rows_scanned as f64 / execution_time.as_secs_f64()
                    } else {
                        0.0
                    },
                    success: true,
                    error_message: None,
                },
                Err(e) => EnginePerformance {
                    engine_type: engine_type.clone(),
                    execution_time_ms: execution_time.as_millis(),
                    memory_usage_mb: 0.0,
                    rows_per_second: 0.0,
                    success: false,
                    error_message: Some(e.to_string()),
                },
            };
            self.logger.log_performance(&performance);
        }

        result
    }

    /// Benchmark multiple engines and return performance comparison
    pub fn benchmark_engines(&self, file_path: &Path) -> Result<Vec<EnginePerformance>> {
        let characteristics = self.selector.analyze_file_characteristics(file_path)?;
        let recommendation = self
            .selector
            .select_engine(&characteristics, ProcessingType::BatchAnalysis);

        let mut performances = Vec::new();
        let engines_to_test = std::iter::once(&recommendation.primary_engine)
            .chain(recommendation.fallback_engines.iter())
            .collect::<std::collections::HashSet<_>>(); // Remove duplicates

        for engine_type in engines_to_test {
            let start = Instant::now();
            let result = self.try_engine(engine_type, file_path);
            let execution_time = start.elapsed();

            let performance = match result {
                Ok(report) => EnginePerformance {
                    engine_type: engine_type.clone(),
                    execution_time_ms: execution_time.as_millis(),
                    memory_usage_mb: 0.0, // Would need actual memory measurement
                    rows_per_second: if execution_time.as_secs() > 0 {
                        report.scan_info.rows_scanned as f64 / execution_time.as_secs_f64()
                    } else {
                        0.0
                    },
                    success: true,
                    error_message: None,
                },
                Err(e) => EnginePerformance {
                    engine_type: engine_type.clone(),
                    execution_time_ms: execution_time.as_millis(),
                    memory_usage_mb: 0.0,
                    rows_per_second: 0.0,
                    success: false,
                    error_message: Some(e.to_string()),
                },
            };

            performances.push(performance);
        }

        self.logger.log_comparison(&performances);
        Ok(performances)
    }
}

impl Default for AdaptiveProfiler {
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
    fn test_adaptive_profiler_basic() -> Result<()> {
        let profiler = AdaptiveProfiler::new().with_logging(false);

        // Create test file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,salary")?;
        writeln!(temp_file, "Alice,25,50000")?;
        writeln!(temp_file, "Bob,30,60000")?;
        temp_file.flush()?;

        let report = profiler.analyze_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.scan_info.rows_scanned, 2);

        Ok(())
    }

    #[test]
    fn test_fallback_mechanism() -> Result<()> {
        let profiler = AdaptiveProfiler::new()
            .with_logging(false)
            .with_fallback(true);

        // Create test file that might cause some engines to fail
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,data")?;
        writeln!(temp_file, "test,\"complex,data\"with\"quotes\"")?;
        temp_file.flush()?;

        // Should still succeed due to fallback
        let report = profiler.analyze_file(temp_file.path())?;
        assert_eq!(report.column_profiles.len(), 2);

        Ok(())
    }
}
