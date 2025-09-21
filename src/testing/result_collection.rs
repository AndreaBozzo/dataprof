/// Unified result collection system for bridging criterion and JSON formats
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

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
}
