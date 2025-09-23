/// Unified result collection system for bridging criterion and JSON formats
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Comprehensive metric types for benchmarking
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MetricType {
    // Performance Metrics
    ExecutionTime,
    MemoryPeak,
    MemoryAverage,
    CpuUtilization,

    // Quality Metrics
    AccuracyScore,      // How accurate vs baseline
    QualityIssuesFound, // Number of issues detected
    FalsePositiveRate,  // Over-detection rate

    // Engine-Specific Metrics
    ChunkProcessingTime,    // For streaming engines
    ColumnCompressionRatio, // For Arrow engine
    SampleConvergenceRate,  // For sampling strategies

    // Additional Engine Metrics
    AdaptiveDecisionAccuracy, // How often AdaptiveProfiler chooses optimal engine
    EngineSelectionTime,      // Time spent choosing engine
    MemoryEfficiency,         // Memory usage per row processed
    ThroughputMBps,           // MB/s processing speed

    // Statistical Metrics
    ConfidenceInterval,     // Statistical confidence range
    CoefficientOfVariation, // Statistical variance measure
    OutliersDetected,       // Number of outliers removed
}

impl MetricType {
    /// Get display name for metric
    pub fn display_name(&self) -> &'static str {
        match self {
            MetricType::ExecutionTime => "Execution Time",
            MetricType::MemoryPeak => "Peak Memory",
            MetricType::MemoryAverage => "Average Memory",
            MetricType::CpuUtilization => "CPU Utilization",
            MetricType::AccuracyScore => "Accuracy Score",
            MetricType::QualityIssuesFound => "Quality Issues Found",
            MetricType::FalsePositiveRate => "False Positive Rate",
            MetricType::ChunkProcessingTime => "Chunk Processing Time",
            MetricType::ColumnCompressionRatio => "Column Compression Ratio",
            MetricType::SampleConvergenceRate => "Sample Convergence Rate",
            MetricType::AdaptiveDecisionAccuracy => "Adaptive Decision Accuracy",
            MetricType::EngineSelectionTime => "Engine Selection Time",
            MetricType::MemoryEfficiency => "Memory Efficiency",
            MetricType::ThroughputMBps => "Throughput (MB/s)",
            MetricType::ConfidenceInterval => "Confidence Interval",
            MetricType::CoefficientOfVariation => "Coefficient of Variation",
            MetricType::OutliersDetected => "Outliers Detected",
        }
    }

    /// Get unit for metric
    pub fn unit(&self) -> &'static str {
        match self {
            MetricType::ExecutionTime
            | MetricType::ChunkProcessingTime
            | MetricType::EngineSelectionTime => "seconds",
            MetricType::MemoryPeak | MetricType::MemoryAverage => "MB",
            MetricType::CpuUtilization => "%",
            MetricType::AccuracyScore
            | MetricType::FalsePositiveRate
            | MetricType::AdaptiveDecisionAccuracy => "%",
            MetricType::QualityIssuesFound | MetricType::OutliersDetected => "count",
            MetricType::ColumnCompressionRatio | MetricType::SampleConvergenceRate => "ratio",
            MetricType::MemoryEfficiency => "MB/row",
            MetricType::ThroughputMBps => "MB/s",
            MetricType::ConfidenceInterval => "±",
            MetricType::CoefficientOfVariation => "%",
        }
    }

    /// Check if higher values are better
    pub fn higher_is_better(&self) -> bool {
        match self {
            MetricType::AccuracyScore
            | MetricType::ColumnCompressionRatio
            | MetricType::SampleConvergenceRate
            | MetricType::AdaptiveDecisionAccuracy
            | MetricType::ThroughputMBps => true,
            MetricType::ExecutionTime
            | MetricType::MemoryPeak
            | MetricType::MemoryAverage
            | MetricType::CpuUtilization
            | MetricType::QualityIssuesFound
            | MetricType::FalsePositiveRate
            | MetricType::ChunkProcessingTime
            | MetricType::EngineSelectionTime
            | MetricType::MemoryEfficiency
            | MetricType::CoefficientOfVariation
            | MetricType::OutliersDetected => false,
            MetricType::ConfidenceInterval => false, // Smaller confidence intervals are better
        }
    }
}

/// Individual metric measurement with statistical metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricMeasurement {
    pub metric_type: MetricType,
    pub value: f64,
    pub unit: String,
    pub confidence_interval: Option<(f64, f64)>,
    pub sample_count: Option<usize>,
    pub outliers_removed: Option<usize>,
    pub statistical_significance: bool,
    pub timestamp: f64,
}

impl MetricMeasurement {
    pub fn new(metric_type: MetricType, value: f64) -> Self {
        Self {
            unit: metric_type.unit().to_string(),
            metric_type,
            value,
            confidence_interval: None,
            sample_count: None,
            outliers_removed: None,
            statistical_significance: false,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        }
    }

    pub fn with_confidence_interval(mut self, ci: (f64, f64)) -> Self {
        self.confidence_interval = Some(ci);
        self
    }

    pub fn with_samples(mut self, count: usize) -> Self {
        self.sample_count = Some(count);
        self
    }

    pub fn with_outliers_removed(mut self, count: usize) -> Self {
        self.outliers_removed = Some(count);
        self
    }

    pub fn with_statistical_significance(mut self, significant: bool) -> Self {
        self.statistical_significance = significant;
        self
    }

    /// Format measurement for display
    pub fn format_display(&self) -> String {
        let mut parts = vec![format!("{:.3} {}", self.value, self.unit)];

        if let Some((lower, upper)) = self.confidence_interval {
            parts.push(format!("CI: [{:.3}, {:.3}]", lower, upper));
        }

        if let Some(count) = self.sample_count {
            parts.push(format!("n={}", count));
        }

        if self.statistical_significance {
            parts.push("✓".to_string());
        }

        parts.join(" ")
    }
}

/// Collection of metrics for a single benchmark run
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricCollection {
    pub benchmark_id: String,
    pub engine_type: String,
    pub dataset_info: DatasetInfo,
    pub metrics: HashMap<MetricType, MetricMeasurement>,
    pub execution_metadata: ExecutionMetadata,
}

/// Dataset information for metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetInfo {
    pub pattern: String,
    pub size_category: String,
    pub file_size_mb: f64,
    pub rows_count: u64,
    pub columns_count: u32,
}

/// Execution metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionMetadata {
    pub timestamp: f64,
    pub environment: String,
    pub rust_version: Option<String>,
    pub system_info: SystemInfo,
}

/// System information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    pub cpu_cores: usize,
    pub total_memory_gb: f64,
    pub os: String,
}

impl MetricCollection {
    pub fn new(benchmark_id: String, engine_type: String, dataset_info: DatasetInfo) -> Self {
        Self {
            benchmark_id,
            engine_type,
            dataset_info,
            metrics: HashMap::new(),
            execution_metadata: ExecutionMetadata {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64(),
                environment: std::env::var("CI")
                    .map(|_| "CI".to_string())
                    .unwrap_or_else(|_| "Local".to_string()),
                rust_version: option_env!("RUSTC_VERSION").map(String::from),
                system_info: SystemInfo {
                    cpu_cores: num_cpus::get(),
                    total_memory_gb: 0.0, // Would need system query
                    os: std::env::consts::OS.to_string(),
                },
            },
        }
    }

    /// Add a metric measurement
    pub fn add_metric(&mut self, measurement: MetricMeasurement) {
        self.metrics
            .insert(measurement.metric_type.clone(), measurement);
    }

    /// Get specific metric
    pub fn get_metric(&self, metric_type: &MetricType) -> Option<&MetricMeasurement> {
        self.metrics.get(metric_type)
    }

    /// Generate performance vs accuracy trade-off analysis
    pub fn analyze_performance_accuracy_tradeoff(&self) -> Option<PerformanceAccuracyAnalysis> {
        let execution_time = self.get_metric(&MetricType::ExecutionTime)?.value;
        let accuracy_score = self.get_metric(&MetricType::AccuracyScore)?.value;
        let memory_usage = self.get_metric(&MetricType::MemoryPeak)?.value;

        Some(PerformanceAccuracyAnalysis {
            execution_time,
            accuracy_score,
            memory_usage,
            efficiency_score: accuracy_score / (execution_time * memory_usage),
            trade_off_rating: self.calculate_tradeoff_rating(
                execution_time,
                accuracy_score,
                memory_usage,
            ),
        })
    }

    fn calculate_tradeoff_rating(&self, time: f64, accuracy: f64, memory: f64) -> TradeoffRating {
        let efficiency = accuracy / (time * memory);

        if efficiency > 10.0 {
            TradeoffRating::Excellent
        } else if efficiency > 5.0 {
            TradeoffRating::Good
        } else if efficiency > 1.0 {
            TradeoffRating::Acceptable
        } else {
            TradeoffRating::Poor
        }
    }
}

/// Performance vs accuracy analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAccuracyAnalysis {
    pub execution_time: f64,
    pub accuracy_score: f64,
    pub memory_usage: f64,
    pub efficiency_score: f64,
    pub trade_off_rating: TradeoffRating,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TradeoffRating {
    Excellent,
    Good,
    Acceptable,
    Poor,
}

/// Parameters for adding a criterion benchmark result
#[derive(Debug)]
pub struct CriterionResultParams {
    pub dataset_pattern: String,
    pub dataset_size: String,
    pub file_size_mb: f64,
    pub time_seconds: f64,
    pub memory_mb: Option<f64>,
    pub rows_processed: u64,
    pub columns_processed: Option<u32>,
}

/// Standard benchmark result format compatible with CI/CD workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub tool: String,
    pub time_seconds: f64,
    pub memory_mb: f64,
    pub rows_processed: u64,
    pub columns_processed: Option<u32>,
    pub success: bool,
    pub error: Option<String>,
    pub metadata: HashMap<String, serde_json::Value>,
}

/// File-based benchmark result aggregating multiple tools
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileBenchmarkResult {
    pub file_size_mb: f64,
    pub dataset_pattern: String,
    pub dataset_size: String,
    pub results: Vec<BenchmarkResult>,
    pub timestamp: f64,
}

/// Complete benchmark suite results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSuiteResults {
    pub benchmarks: Vec<FileBenchmarkResult>,
    pub metadata: SuiteMetadata,
}

/// Metadata about the benchmark suite
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuiteMetadata {
    pub version: String,
    pub commit: Option<String>,
    pub branch: Option<String>,
    pub timestamp: f64,
    pub environment: String,
    pub rust_version: Option<String>,
    pub total_benchmarks: usize,
    pub total_duration_seconds: f64,
}

/// Result collector for bridging criterion and JSON formats
pub struct ResultCollector {
    results: Vec<FileBenchmarkResult>,
    start_time: SystemTime,
}

impl ResultCollector {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
            start_time: SystemTime::now(),
        }
    }

    /// Add a criterion benchmark result converted to unified format
    pub fn add_criterion_result(&mut self, params: CriterionResultParams) {
        // Find existing file benchmark or create new one
        if let Some(file_benchmark) = self.results.iter_mut().find(|r| {
            r.dataset_pattern == params.dataset_pattern && r.dataset_size == params.dataset_size
        }) {
            let benchmark_result = BenchmarkResult {
                tool: "DataProfiler".to_string(),
                time_seconds: params.time_seconds,
                memory_mb: params.memory_mb.unwrap_or(0.0),
                rows_processed: params.rows_processed,
                columns_processed: params.columns_processed,
                success: true,
                error: None,
                metadata: {
                    let mut meta = HashMap::new();
                    meta.insert("engine".into(), "adaptive".into());
                    meta.insert("pattern".into(), params.dataset_pattern.into());
                    meta.insert("size_category".into(), params.dataset_size.into());
                    meta
                },
            };
            file_benchmark.results.push(benchmark_result);
        } else {
            let benchmark_result = BenchmarkResult {
                tool: "DataProfiler".to_string(),
                time_seconds: params.time_seconds,
                memory_mb: params.memory_mb.unwrap_or(0.0),
                rows_processed: params.rows_processed,
                columns_processed: params.columns_processed,
                success: true,
                error: None,
                metadata: {
                    let mut meta = HashMap::new();
                    meta.insert("engine".into(), "adaptive".into());
                    meta.insert("pattern".into(), params.dataset_pattern.as_str().into());
                    meta.insert("size_category".into(), params.dataset_size.as_str().into());
                    meta
                },
            };
            let file_benchmark = FileBenchmarkResult {
                file_size_mb: params.file_size_mb,
                dataset_pattern: params.dataset_pattern,
                dataset_size: params.dataset_size,
                results: vec![benchmark_result],
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64(),
            };
            self.results.push(file_benchmark);
        }
    }

    /// Add external tool comparison result (from benchmark_comparison.py)
    pub fn add_external_result(
        &mut self,
        dataset_pattern: &str,
        dataset_size: &str,
        result: BenchmarkResult,
    ) {
        if let Some(file_benchmark) = self
            .results
            .iter_mut()
            .find(|r| r.dataset_pattern == dataset_pattern && r.dataset_size == dataset_size)
        {
            file_benchmark.results.push(result);
        }
    }

    /// Generate complete benchmark suite results
    pub fn finalize(&self) -> BenchmarkSuiteResults {
        let total_duration = self.start_time.elapsed().unwrap_or_default().as_secs_f64();

        let metadata = SuiteMetadata {
            version: "2.0".to_string(),
            commit: std::env::var("GITHUB_SHA").ok(),
            branch: std::env::var("GITHUB_REF_NAME").ok(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
            environment: std::env::var("CI")
                .map(|_| "GitHub Actions".to_string())
                .unwrap_or_else(|_| "Local".to_string()),
            rust_version: option_env!("RUSTC_VERSION").map(String::from),
            total_benchmarks: self.results.len(),
            total_duration_seconds: total_duration,
        };

        BenchmarkSuiteResults {
            benchmarks: self.results.clone(),
            metadata,
        }
    }

    /// Save results in CI/CD compatible format
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let results = self.finalize();
        let json = serde_json::to_string_pretty(&results.benchmarks)?;
        fs::write(path, json)?;
        Ok(())
    }

    /// Save metadata separately for trend analysis
    pub fn save_metadata<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<dyn std::error::Error>> {
        let results = self.finalize();
        let json = serde_json::to_string_pretty(&results.metadata)?;
        fs::write(path, json)?;
        Ok(())
    }

    /// Generate performance report compatible with CI workflow
    pub fn generate_performance_report(&self) -> String {
        let results = self.finalize();
        let mut report = Vec::new();

        report.push("# 📊 Unified Benchmark Results Report".to_string());
        report.push("".to_string());
        report.push(format!("**Environment**: {}", results.metadata.environment));
        report.push(format!(
            "**Total Duration**: {:.2}s",
            results.metadata.total_duration_seconds
        ));
        report.push(format!(
            "**Total Benchmarks**: {}",
            results.metadata.total_benchmarks
        ));
        if let Some(commit) = &results.metadata.commit {
            report.push(format!("**Commit**: `{}`", commit));
        }
        if let Some(branch) = &results.metadata.branch {
            report.push(format!("**Branch**: `{}`", branch));
        }
        report.push("".to_string());

        // Group by dataset pattern
        let mut patterns: HashMap<String, Vec<&FileBenchmarkResult>> = HashMap::new();
        for benchmark in &results.benchmarks {
            patterns
                .entry(benchmark.dataset_pattern.clone())
                .or_default()
                .push(benchmark);
        }

        for (pattern, benchmarks) in patterns {
            report.push(format!("## 🎯 {} Dataset Results", pattern.to_uppercase()));
            report.push("".to_string());

            for benchmark in benchmarks {
                report.push(format!(
                    "### {} ({:.1}MB)",
                    benchmark.dataset_size, benchmark.file_size_mb
                ));

                // Find DataProfiler result
                if let Some(dataprof_result) =
                    benchmark.results.iter().find(|r| r.tool == "DataProfiler")
                {
                    report.push(format!(
                        "- **DataProfiler**: {:.3}s | {:.1}MB memory | {} rows",
                        dataprof_result.time_seconds,
                        dataprof_result.memory_mb,
                        dataprof_result.rows_processed
                    ));

                    // Calculate throughput
                    let throughput_mb_s = benchmark.file_size_mb / dataprof_result.time_seconds;
                    report.push(format!("- **Throughput**: {:.1} MB/s", throughput_mb_s));

                    // Add performance assessment
                    if throughput_mb_s > 50.0 {
                        report.push("- **Assessment**: 🚀 **EXCELLENT** performance".to_string());
                    } else if throughput_mb_s > 20.0 {
                        report.push("- **Assessment**: ✅ **GOOD** performance".to_string());
                    } else if throughput_mb_s > 10.0 {
                        report.push("- **Assessment**: ⚖️ **ACCEPTABLE** performance".to_string());
                    } else {
                        report.push("- **Assessment**: ⚠️ **NEEDS OPTIMIZATION**".to_string());
                    }
                }

                // Show other tool results if available
                let other_results: Vec<_> = benchmark
                    .results
                    .iter()
                    .filter(|r| r.tool != "DataProfiler" && r.success)
                    .collect();

                if !other_results.is_empty() {
                    report.push("".to_string());
                    report.push("**Comparison with other tools:**".to_string());
                    for result in other_results {
                        report.push(format!(
                            "- **{}**: {:.3}s | {:.1}MB memory",
                            result.tool, result.time_seconds, result.memory_mb
                        ));
                    }
                }

                report.push("".to_string());
            }
        }

        // Summary
        let total_dataprof_results: Vec<_> = results
            .benchmarks
            .iter()
            .flat_map(|b| &b.results)
            .filter(|r| r.tool == "DataProfiler" && r.success)
            .collect();

        if !total_dataprof_results.is_empty() {
            let avg_time: f64 = total_dataprof_results
                .iter()
                .map(|r| r.time_seconds)
                .sum::<f64>()
                / total_dataprof_results.len() as f64;

            let avg_memory: f64 = total_dataprof_results
                .iter()
                .map(|r| r.memory_mb)
                .sum::<f64>()
                / total_dataprof_results.len() as f64;

            let total_rows: u64 = total_dataprof_results
                .iter()
                .map(|r| r.rows_processed)
                .sum();

            report.push("## 📈 Performance Summary".to_string());
            report.push("".to_string());
            report.push(format!("- **Average processing time**: {:.3}s", avg_time));
            report.push(format!("- **Average memory usage**: {:.1}MB", avg_memory));
            report.push(format!("- **Total rows processed**: {}", total_rows));
            report.push(format!(
                "- **Benchmarks completed**: {}",
                total_dataprof_results.len()
            ));
        }

        report.push("".to_string());
        report.push("---".to_string());
        report.push("🤖 Generated by unified benchmarking system".to_string());

        report.join("\n")
    }
}

impl Default for ResultCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_result_collector_basic() {
        let mut collector = ResultCollector::new();

        collector.add_criterion_result(CriterionResultParams {
            dataset_pattern: "basic".to_string(),
            dataset_size: "micro".to_string(),
            file_size_mb: 0.5,
            time_seconds: 0.123,
            memory_mb: Some(15.2),
            rows_processed: 5000,
            columns_processed: Some(4),
        });

        let results = collector.finalize();
        assert_eq!(results.benchmarks.len(), 1);
        assert_eq!(results.benchmarks[0].results.len(), 1);

        let result = &results.benchmarks[0].results[0];
        assert_eq!(result.tool, "DataProfiler");
        assert_eq!(result.time_seconds, 0.123);
        assert_eq!(result.memory_mb, 15.2);
        assert_eq!(result.rows_processed, 5000);
        assert!(result.success);
    }

    #[test]
    fn test_external_result_integration() {
        let mut collector = ResultCollector::new();

        // Add DataProfiler result
        collector.add_criterion_result(CriterionResultParams {
            dataset_pattern: "mixed".to_string(),
            dataset_size: "small".to_string(),
            file_size_mb: 5.0,
            time_seconds: 1.23,
            memory_mb: Some(20.0),
            rows_processed: 50000,
            columns_processed: Some(10),
        });

        // Add external tool result
        let pandas_result = BenchmarkResult {
            tool: "pandas".to_string(),
            time_seconds: 6.78,
            memory_mb: 150.0,
            rows_processed: 50000,
            columns_processed: Some(10),
            success: true,
            error: None,
            metadata: HashMap::new(),
        };

        collector.add_external_result("mixed", "small", pandas_result);

        let results = collector.finalize();
        assert_eq!(results.benchmarks.len(), 1);
        assert_eq!(results.benchmarks[0].results.len(), 2);

        // Verify both tools are present
        let tools: Vec<_> = results.benchmarks[0]
            .results
            .iter()
            .map(|r| &r.tool)
            .collect();
        assert!(tools.contains(&&"DataProfiler".to_string()));
        assert!(tools.contains(&&"pandas".to_string()));
    }

    #[test]
    fn test_performance_report_generation() {
        let mut collector = ResultCollector::new();
        collector.add_criterion_result(CriterionResultParams {
            dataset_pattern: "basic".to_string(),
            dataset_size: "micro".to_string(),
            file_size_mb: 1.0,
            time_seconds: 0.5,
            memory_mb: Some(10.0),
            rows_processed: 10000,
            columns_processed: Some(4),
        });

        let report = collector.generate_performance_report();
        assert!(report.contains("Unified Benchmark Results Report"));
        assert!(report.contains("BASIC Dataset Results"));
        assert!(report.contains("DataProfiler"));
        assert!(report.contains("Throughput"));
    }

    #[test]
    fn test_metric_type_properties() {
        assert_eq!(MetricType::ExecutionTime.display_name(), "Execution Time");
        assert_eq!(MetricType::ExecutionTime.unit(), "seconds");
        assert!(!MetricType::ExecutionTime.higher_is_better());

        assert_eq!(MetricType::AccuracyScore.display_name(), "Accuracy Score");
        assert_eq!(MetricType::AccuracyScore.unit(), "%");
        assert!(MetricType::AccuracyScore.higher_is_better());

        assert_eq!(
            MetricType::ThroughputMBps.display_name(),
            "Throughput (MB/s)"
        );
        assert_eq!(MetricType::ThroughputMBps.unit(), "MB/s");
        assert!(MetricType::ThroughputMBps.higher_is_better());
    }

    #[test]
    fn test_metric_measurement_creation() {
        let measurement = MetricMeasurement::new(MetricType::ExecutionTime, 1.234)
            .with_confidence_interval((1.1, 1.4))
            .with_samples(30)
            .with_outliers_removed(2)
            .with_statistical_significance(true);

        assert_eq!(measurement.metric_type, MetricType::ExecutionTime);
        assert_eq!(measurement.value, 1.234);
        assert_eq!(measurement.unit, "seconds");
        assert_eq!(measurement.confidence_interval, Some((1.1, 1.4)));
        assert_eq!(measurement.sample_count, Some(30));
        assert_eq!(measurement.outliers_removed, Some(2));
        assert!(measurement.statistical_significance);
    }

    #[test]
    fn test_metric_measurement_display() {
        let measurement = MetricMeasurement::new(MetricType::ExecutionTime, 1.234)
            .with_confidence_interval((1.1, 1.4))
            .with_samples(30)
            .with_statistical_significance(true);

        let display = measurement.format_display();
        assert!(display.contains("1.234 seconds"));
        assert!(display.contains("CI: [1.100, 1.400]"));
        assert!(display.contains("n=30"));
        assert!(display.contains("✓"));
    }

    #[test]
    fn test_metric_collection() {
        let dataset_info = DatasetInfo {
            pattern: "basic".to_string(),
            size_category: "small".to_string(),
            file_size_mb: 5.0,
            rows_count: 50000,
            columns_count: 4,
        };

        let mut collection = MetricCollection::new(
            "test_benchmark".to_string(),
            "adaptive".to_string(),
            dataset_info,
        );

        let execution_measurement = MetricMeasurement::new(MetricType::ExecutionTime, 1.5);
        let accuracy_measurement = MetricMeasurement::new(MetricType::AccuracyScore, 95.0);

        collection.add_metric(execution_measurement);
        collection.add_metric(accuracy_measurement);

        assert_eq!(collection.metrics.len(), 2);
        assert!(collection.get_metric(&MetricType::ExecutionTime).is_some());
        assert!(collection.get_metric(&MetricType::AccuracyScore).is_some());
        assert!(collection.get_metric(&MetricType::MemoryPeak).is_none());
    }

    #[test]
    fn test_performance_accuracy_tradeoff() {
        let dataset_info = DatasetInfo {
            pattern: "basic".to_string(),
            size_category: "small".to_string(),
            file_size_mb: 5.0,
            rows_count: 50000,
            columns_count: 4,
        };

        let mut collection = MetricCollection::new(
            "test_benchmark".to_string(),
            "adaptive".to_string(),
            dataset_info,
        );

        collection.add_metric(MetricMeasurement::new(MetricType::ExecutionTime, 1.0));
        collection.add_metric(MetricMeasurement::new(MetricType::AccuracyScore, 90.0));
        collection.add_metric(MetricMeasurement::new(MetricType::MemoryPeak, 100.0));

        let analysis = collection
            .analyze_performance_accuracy_tradeoff()
            .expect("Failed to analyze performance accuracy tradeoff in test");
        assert_eq!(analysis.execution_time, 1.0);
        assert_eq!(analysis.accuracy_score, 90.0);
        assert_eq!(analysis.memory_usage, 100.0);
        assert_eq!(analysis.efficiency_score, 0.9); // 90 / (1.0 * 100.0)

        // Test different tradeoff ratings
        let mut poor_collection = collection.clone();
        poor_collection.add_metric(MetricMeasurement::new(MetricType::ExecutionTime, 10.0));
        poor_collection.add_metric(MetricMeasurement::new(MetricType::AccuracyScore, 50.0));
        poor_collection.add_metric(MetricMeasurement::new(MetricType::MemoryPeak, 1000.0));

        let poor_analysis = poor_collection
            .analyze_performance_accuracy_tradeoff()
            .expect("Failed to analyze poor performance in test");
        assert!(matches!(
            poor_analysis.trade_off_rating,
            TradeoffRating::Poor
        ));
    }

    #[test]
    fn test_system_info_creation() {
        let dataset_info = DatasetInfo {
            pattern: "test".to_string(),
            size_category: "micro".to_string(),
            file_size_mb: 1.0,
            rows_count: 1000,
            columns_count: 3,
        };

        let collection =
            MetricCollection::new("test".to_string(), "test_engine".to_string(), dataset_info);

        assert!(collection.execution_metadata.system_info.cpu_cores > 0);
        assert_eq!(
            collection.execution_metadata.system_info.os,
            std::env::consts::OS
        );
    }

    #[test]
    fn test_tradeoff_rating_calculation() {
        let dataset_info = DatasetInfo {
            pattern: "test".to_string(),
            size_category: "micro".to_string(),
            file_size_mb: 1.0,
            rows_count: 1000,
            columns_count: 3,
        };

        let collection =
            MetricCollection::new("test".to_string(), "test_engine".to_string(), dataset_info);

        // Test excellent rating (efficiency > 10.0)
        let excellent = collection.calculate_tradeoff_rating(0.1, 95.0, 1.0); // 95/(0.1*1.0) = 950
        assert!(matches!(excellent, TradeoffRating::Excellent));

        // Test good rating (efficiency > 5.0)
        let good = collection.calculate_tradeoff_rating(1.0, 80.0, 10.0); // 80/(1.0*10.0) = 8.0
        assert!(matches!(good, TradeoffRating::Good));

        // Test acceptable rating (efficiency > 1.0)
        let acceptable = collection.calculate_tradeoff_rating(1.0, 70.0, 50.0); // 70/(1.0*50.0) = 1.4
        assert!(matches!(acceptable, TradeoffRating::Acceptable));

        // Test poor rating (efficiency <= 1.0)
        let poor = collection.calculate_tradeoff_rating(10.0, 60.0, 100.0); // 60/(10.0*100.0) = 0.06
        assert!(matches!(poor, TradeoffRating::Poor));
    }
}
