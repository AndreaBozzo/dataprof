/// Engine selection benchmarking framework for comprehensive testing
use crate::core::benchmark_stats::{StatisticalConfig, StatisticalSample, StatisticalSummary};
use crate::engines::{AdaptiveProfiler, EnginePerformance, EngineType};
use crate::testing::{DatasetInfo, MetricCollection, MetricMeasurement, MetricType};
use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

/// Benchmark configuration for engine selection testing
#[derive(Debug, Clone)]
pub struct EngineBenchmarkConfig {
    pub statistical_config: StatisticalConfig,
    pub test_all_engines: bool,
    pub measure_accuracy: bool,
    pub enable_detailed_logging: bool,
    pub baseline_engine: Option<EngineType>,
}

impl EngineBenchmarkConfig {
    /// Configuration for CI environments
    pub fn ci_config() -> Self {
        Self {
            statistical_config: StatisticalConfig::ci_config(),
            test_all_engines: true,
            measure_accuracy: true,
            enable_detailed_logging: false,
            baseline_engine: Some(EngineType::Streaming), // Conservative baseline
        }
    }

    /// Configuration for comprehensive local testing
    pub fn comprehensive_config() -> Self {
        Self {
            statistical_config: StatisticalConfig::local_config(),
            test_all_engines: true,
            measure_accuracy: true,
            enable_detailed_logging: true,
            baseline_engine: Some(EngineType::Streaming),
        }
    }

    /// Quick configuration for development
    pub fn quick_config() -> Self {
        Self {
            statistical_config: StatisticalConfig::custom(
                5,
                0.90,
                false,
                2,
                std::time::Duration::from_secs(5),
            ),
            test_all_engines: false,
            measure_accuracy: false,
            enable_detailed_logging: true,
            baseline_engine: None,
        }
    }
}

impl Default for EngineBenchmarkConfig {
    fn default() -> Self {
        if std::env::var("CI").is_ok() {
            Self::ci_config()
        } else {
            Self::comprehensive_config()
        }
    }
}

/// Results from engine selection accuracy testing
#[derive(Debug, Clone)]
pub struct EngineSelectionAccuracy {
    pub total_tests: usize,
    pub optimal_selections: usize,
    pub suboptimal_selections: usize,
    pub failed_selections: usize,
    pub accuracy_percentage: f64,
    pub average_performance_gap: f64, // How much worse non-optimal choices were
    pub confidence_score: f64,
}

impl EngineSelectionAccuracy {
    pub fn calculate(results: &[EngineSelectionTestResult]) -> Self {
        let total_tests = results.len();
        let mut optimal_selections = 0;
        let mut suboptimal_selections = 0;
        let mut failed_selections = 0;
        let mut performance_gaps = Vec::new();

        for result in results {
            match &result.outcome {
                SelectionOutcome::Optimal => optimal_selections += 1,
                SelectionOutcome::Suboptimal { performance_gap } => {
                    suboptimal_selections += 1;
                    performance_gaps.push(*performance_gap);
                }
                SelectionOutcome::Failed => failed_selections += 1,
            }
        }

        let accuracy_percentage = if total_tests > 0 {
            (optimal_selections as f64 / total_tests as f64) * 100.0
        } else {
            0.0
        };

        let average_performance_gap = if !performance_gaps.is_empty() {
            performance_gaps.iter().sum::<f64>() / performance_gaps.len() as f64
        } else {
            0.0
        };

        // Calculate confidence based on statistical significance
        let confidence_score = if total_tests >= 30 {
            0.95
        } else if total_tests >= 10 {
            0.80
        } else {
            0.60
        };

        Self {
            total_tests,
            optimal_selections,
            suboptimal_selections,
            failed_selections,
            accuracy_percentage,
            average_performance_gap,
            confidence_score,
        }
    }

    pub fn meets_target(&self, target_accuracy: f64) -> bool {
        self.accuracy_percentage >= target_accuracy && self.confidence_score >= 0.80
    }
}

/// Individual test result for engine selection
#[derive(Debug, Clone)]
pub struct EngineSelectionTestResult {
    pub dataset_name: String,
    pub selected_engine: EngineType,
    pub actual_best_engine: EngineType,
    pub selection_time_ms: u128,
    pub outcome: SelectionOutcome,
    pub all_engine_performances: Vec<EnginePerformance>,
}

#[derive(Debug, Clone)]
pub enum SelectionOutcome {
    Optimal,
    Suboptimal { performance_gap: f64 }, // How much slower as a percentage
    Failed,
}

/// Comprehensive engine comparison results
#[derive(Debug, Clone)]
pub struct EngineComparisonResult {
    pub dataset_info: DatasetInfo,
    pub engine_metrics: HashMap<EngineType, MetricCollection>,
    pub winner: EngineType,
    pub performance_ranking: Vec<(EngineType, f64)>, // (engine, score)
    pub statistical_summary: HashMap<EngineType, StatisticalSummary>,
}

/// Engine benchmarking framework
pub struct EngineBenchmarkFramework {
    config: EngineBenchmarkConfig,
    adaptive_profiler: AdaptiveProfiler,
}

impl EngineBenchmarkFramework {
    pub fn new(config: EngineBenchmarkConfig) -> Self {
        Self {
            adaptive_profiler: AdaptiveProfiler::new(),
            config,
        }
    }

    /// Test AdaptiveProfiler accuracy across different datasets
    pub fn test_adaptive_profiler_accuracy<P: AsRef<Path>>(
        &self,
        test_datasets: &[(String, P)],
    ) -> Result<EngineSelectionAccuracy> {
        let mut test_results = Vec::new();

        for (dataset_name, dataset_path) in test_datasets {
            println!("Testing engine selection for dataset: {}", dataset_name);

            // Measure selection time
            let selection_start = Instant::now();
            let selected_engine = self.determine_selected_engine(dataset_path.as_ref())?;
            let selection_time_ms = selection_start.elapsed().as_millis();

            // Run all engines to find actual best
            let all_performances = self.benchmark_all_engines(dataset_path.as_ref())?;
            let actual_best_engine = self.find_best_engine(&all_performances);

            // Determine outcome
            let outcome = self.evaluate_selection_outcome(
                &selected_engine,
                &actual_best_engine,
                &all_performances,
            );

            test_results.push(EngineSelectionTestResult {
                dataset_name: dataset_name.clone(),
                selected_engine,
                actual_best_engine,
                selection_time_ms,
                outcome,
                all_engine_performances: all_performances,
            });
        }

        Ok(EngineSelectionAccuracy::calculate(&test_results))
    }

    /// Systematic comparison of all engines on the same datasets
    pub fn systematic_engine_comparison<P: AsRef<Path>>(
        &self,
        dataset_path: P,
        dataset_info: DatasetInfo,
    ) -> Result<EngineComparisonResult> {
        let available_engines = self.get_available_engines();
        let mut engine_metrics = HashMap::new();
        let mut statistical_summaries = HashMap::new();

        for engine_type in &available_engines {
            println!("Benchmarking engine: {:?}", engine_type);

            // Run multiple samples for statistical significance
            let mut sample = StatisticalSample::new(self.config.statistical_config.clone());

            for iteration in 0..self.config.statistical_config.min_samples {
                if iteration < self.config.statistical_config.warmup_iterations {
                    // Warmup runs - don't count these
                    let _ = self.run_single_engine(dataset_path.as_ref(), engine_type);
                    continue;
                }

                match self.run_single_engine(dataset_path.as_ref(), engine_type) {
                    Ok(performance) => {
                        sample.add_measurement(performance.execution_time_ms as f64 / 1000.0);
                    }
                    Err(e) => {
                        eprintln!("Engine {:?} failed: {}", engine_type, e);
                        continue;
                    }
                }
            }

            // Remove outliers if configured
            sample.remove_outliers();

            // Generate statistical summary
            let statistical_summary = sample.statistical_summary();
            statistical_summaries.insert(engine_type.clone(), statistical_summary.clone());

            // Create metric collection
            let mut metrics = MetricCollection::new(
                format!("engine_comparison_{:?}", engine_type),
                format!("{:?}", engine_type),
                dataset_info.clone(),
            );

            // Add comprehensive metrics
            metrics.add_metric(
                MetricMeasurement::new(MetricType::ExecutionTime, statistical_summary.mean)
                    .with_confidence_interval(statistical_summary.confidence_interval)
                    .with_samples(statistical_summary.sample_count)
                    .with_statistical_significance(
                        statistical_summary.is_statistically_significant(),
                    ),
            );

            metrics.add_metric(MetricMeasurement::new(
                MetricType::CoefficientOfVariation,
                statistical_summary.coefficient_of_variation,
            ));

            engine_metrics.insert(engine_type.clone(), metrics);
        }

        // Determine winner and ranking
        let (winner, performance_ranking) = self.calculate_engine_ranking(&statistical_summaries);

        Ok(EngineComparisonResult {
            dataset_info,
            engine_metrics,
            winner,
            performance_ranking,
            statistical_summary: statistical_summaries,
        })
    }

    /// Measure performance vs accuracy trade-offs
    pub fn measure_performance_accuracy_tradeoff<P: AsRef<Path>>(
        &self,
        dataset_path: P,
        baseline_results: Option<&MetricCollection>,
    ) -> Result<HashMap<EngineType, TradeoffAnalysis>> {
        let available_engines = self.get_available_engines();
        let mut tradeoff_results = HashMap::new();

        for engine_type in available_engines {
            let performance = self.run_single_engine(dataset_path.as_ref(), &engine_type)?;

            // Measure accuracy vs baseline if provided
            let accuracy_score = if let Some(baseline) = baseline_results {
                self.calculate_accuracy_score(&performance, baseline)
            } else {
                100.0 // Assume 100% accuracy if no baseline
            };

            let analysis = TradeoffAnalysis {
                engine_type: engine_type.clone(),
                execution_time_ms: performance.execution_time_ms,
                memory_usage_mb: performance.memory_usage_mb,
                accuracy_percentage: accuracy_score,
                efficiency_score: accuracy_score / (performance.execution_time_ms as f64 / 1000.0),
                memory_efficiency: accuracy_score / performance.memory_usage_mb,
            };

            tradeoff_results.insert(engine_type, analysis);
        }

        Ok(tradeoff_results)
    }

    /// Helper methods
    fn determine_selected_engine(&self, dataset_path: &Path) -> Result<EngineType> {
        // Use the adaptive profiler to select the best engine without benchmarking
        self.adaptive_profiler.select_engine(dataset_path)
    }

    fn benchmark_all_engines(&self, dataset_path: &Path) -> Result<Vec<EnginePerformance>> {
        let available_engines = self.get_available_engines();
        let mut performances = Vec::new();

        for engine_type in available_engines {
            match self.run_single_engine(dataset_path, &engine_type) {
                Ok(performance) => performances.push(performance),
                Err(e) => {
                    eprintln!("Engine {:?} failed: {}", engine_type, e);
                    performances.push(EnginePerformance {
                        engine_type,
                        execution_time_ms: u128::MAX,
                        memory_usage_mb: 0.0,
                        rows_per_second: 0.0,
                        success: false,
                        error_message: Some(e.to_string()),
                    });
                }
            }
        }

        Ok(performances)
    }

    fn run_single_engine(
        &self,
        dataset_path: &Path,
        engine_type: &EngineType,
    ) -> Result<EnginePerformance> {
        use crate::engines::streaming::{MemoryEfficientProfiler, TrueStreamingProfiler};
        use crate::parsers::csv::analyze_csv;
        use std::fs;

        let start_time = Instant::now();
        let initial_memory = Self::get_memory_usage();

        // Get file metadata for metrics calculation
        let file_metadata = fs::metadata(dataset_path)?;
        let _file_size_mb = file_metadata.len() as f64 / (1024.0 * 1024.0);

        let result = match engine_type {
            EngineType::Streaming => {
                // Use the standard CSV analyzer (which uses streaming internally)
                analyze_csv(dataset_path)
            }
            EngineType::MemoryEfficient => {
                // Use memory-efficient profiler
                let profiler = MemoryEfficientProfiler::new();
                profiler
                    .analyze_file(dataset_path)
                    .map(|qr| qr.column_profiles)
            }
            EngineType::TrueStreaming => {
                // Use true streaming profiler
                let profiler = TrueStreamingProfiler::new();
                profiler
                    .analyze_file(dataset_path)
                    .map(|qr| qr.column_profiles)
            }
            #[cfg(feature = "arrow")]
            EngineType::Arrow => {
                // Use Arrow profiler if available
                use crate::engines::columnar::ArrowProfiler;
                let profiler = ArrowProfiler::new();
                profiler
                    .analyze_csv_file(dataset_path)
                    .map(|qr| qr.column_profiles)
            }
            #[cfg(not(feature = "arrow"))]
            EngineType::Arrow => {
                return Ok(EnginePerformance {
                    engine_type: engine_type.clone(),
                    execution_time_ms: 0,
                    memory_usage_mb: 0.0,
                    rows_per_second: 0.0,
                    success: false,
                    error_message: Some("Arrow feature not enabled".to_string()),
                });
            }
        };

        let execution_time = start_time.elapsed();
        let peak_memory = Self::get_memory_usage();
        let memory_used_mb =
            (peak_memory.saturating_sub(initial_memory)) as f64 / (1024.0 * 1024.0);

        match result {
            Ok(analysis_results) => {
                // Calculate rows processed from analysis results
                let rows_processed = if !analysis_results.is_empty() {
                    analysis_results[0].total_count as u64
                } else {
                    0
                };

                let rows_per_second = if execution_time.as_secs_f64() > 0.0 {
                    rows_processed as f64 / execution_time.as_secs_f64()
                } else {
                    0.0
                };

                Ok(EnginePerformance {
                    engine_type: engine_type.clone(),
                    execution_time_ms: execution_time.as_millis(),
                    memory_usage_mb: memory_used_mb.max(1.0), // Minimum 1MB to avoid division by zero
                    rows_per_second,
                    success: true,
                    error_message: None,
                })
            }
            Err(e) => Ok(EnginePerformance {
                engine_type: engine_type.clone(),
                execution_time_ms: execution_time.as_millis(),
                memory_usage_mb: memory_used_mb,
                rows_per_second: 0.0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// Get current memory usage in bytes using proper system APIs
    fn get_memory_usage() -> usize {
        #[cfg(target_os = "windows")]
        {
            use std::mem;

            // Use Windows API for accurate memory measurement
            extern "system" {
                fn GetCurrentProcess() -> *mut std::ffi::c_void;
                fn GetProcessMemoryInfo(
                    process: *mut std::ffi::c_void,
                    ppsmemcounters: *mut ProcessMemoryCounters,
                    cb: u32,
                ) -> i32;
            }

            #[repr(C)]
            struct ProcessMemoryCounters {
                cb: u32,
                page_fault_count: u32,
                peak_working_set_size: usize,
                working_set_size: usize,
                quota_peak_paged_pool_usage: usize,
                quota_paged_pool_usage: usize,
                quota_peak_non_paged_pool_usage: usize,
                quota_non_paged_pool_usage: usize,
                pagefile_usage: usize,
                peak_pagefile_usage: usize,
            }

            unsafe {
                let mut pmc: ProcessMemoryCounters = mem::zeroed();
                pmc.cb = mem::size_of::<ProcessMemoryCounters>() as u32;

                let result = GetProcessMemoryInfo(
                    GetCurrentProcess(),
                    &mut pmc,
                    mem::size_of::<ProcessMemoryCounters>() as u32,
                );

                if result != 0 {
                    return pmc.working_set_size;
                }
            }
        }

        #[cfg(target_os = "linux")]
        {
            use std::fs;

            // Read from /proc/self/status for accurate memory info
            if let Ok(status) = fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            if let Ok(kb) = parts[1].parse::<usize>() {
                                return kb * 1024; // Convert KB to bytes
                            }
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::mem;

            extern "C" {
                fn mach_task_self() -> u32;
                fn task_info(
                    target_task: u32,
                    flavor: u32,
                    task_info_out: *mut u8,
                    task_info_outCnt: *mut u32,
                ) -> i32;
            }

            const TASK_BASIC_INFO: u32 = 5;
            const TASK_BASIC_INFO_COUNT: u32 = 5;

            #[repr(C)]
            struct TaskBasicInfo {
                suspend_count: u32,
                virtual_size: u32,
                resident_size: u32,
                user_time: u64,
                system_time: u64,
            }

            unsafe {
                let mut info: TaskBasicInfo = mem::zeroed();
                let mut count = TASK_BASIC_INFO_COUNT;

                let result = task_info(
                    mach_task_self(),
                    TASK_BASIC_INFO,
                    &mut info as *mut _ as *mut u8,
                    &mut count,
                );

                if result == 0 {
                    return info.resident_size as usize;
                }
            }
        }

        // Fallback for other platforms or if system calls fail
        100 * 1024 * 1024 // 100MB reasonable default
    }

    fn get_available_engines(&self) -> Vec<EngineType> {
        #[cfg(feature = "arrow")]
        {
            vec![
                EngineType::Streaming,
                EngineType::MemoryEfficient,
                EngineType::TrueStreaming,
                EngineType::Arrow,
            ]
        }

        #[cfg(not(feature = "arrow"))]
        {
            vec![
                EngineType::Streaming,
                EngineType::MemoryEfficient,
                EngineType::TrueStreaming,
            ]
        }
    }

    fn find_best_engine(&self, performances: &[EnginePerformance]) -> EngineType {
        performances
            .iter()
            .filter(|p| p.success)
            .min_by_key(|p| p.execution_time_ms)
            .map(|p| p.engine_type.clone())
            .unwrap_or(EngineType::Streaming)
    }

    fn evaluate_selection_outcome(
        &self,
        selected: &EngineType,
        actual_best: &EngineType,
        all_performances: &[EnginePerformance],
    ) -> SelectionOutcome {
        if selected == actual_best {
            return SelectionOutcome::Optimal;
        }

        // Find performance gap
        let selected_time = all_performances
            .iter()
            .find(|p| &p.engine_type == selected && p.success)
            .map(|p| p.execution_time_ms);

        let best_time = all_performances
            .iter()
            .find(|p| &p.engine_type == actual_best && p.success)
            .map(|p| p.execution_time_ms);

        match (selected_time, best_time) {
            (Some(selected_ms), Some(best_ms)) => {
                let performance_gap =
                    ((selected_ms as f64 - best_ms as f64) / best_ms as f64) * 100.0;
                SelectionOutcome::Suboptimal { performance_gap }
            }
            _ => SelectionOutcome::Failed,
        }
    }

    fn calculate_engine_ranking(
        &self,
        summaries: &HashMap<EngineType, StatisticalSummary>,
    ) -> (EngineType, Vec<(EngineType, f64)>) {
        let mut rankings: Vec<(EngineType, f64)> = summaries
            .iter()
            .map(|(engine, summary)| {
                // Lower execution time is better, so use inverse
                let score = 1.0 / summary.mean;
                (engine.clone(), score)
            })
            .collect();

        rankings.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let winner = rankings
            .first()
            .map(|(engine, _)| engine.clone())
            .unwrap_or(EngineType::Streaming);

        (winner, rankings)
    }

    fn calculate_accuracy_score(
        &self,
        performance: &EnginePerformance,
        baseline: &MetricCollection,
    ) -> f64 {
        // Real accuracy calculation based on multiple factors

        // Factor 1: Success rate (50% weight)
        let success_score = if performance.success { 100.0 } else { 0.0 };

        // Factor 2: Performance relative to baseline (30% weight)
        let performance_score =
            if let Some(baseline_time) = baseline.get_metric(&MetricType::ExecutionTime) {
                let baseline_time_s = baseline_time.value;
                let current_time_s = performance.execution_time_ms as f64 / 1000.0;

                if current_time_s > 0.0 {
                    // Better performance = higher score
                    let relative_performance = baseline_time_s / current_time_s;
                    (relative_performance * 100.0).min(200.0) // Cap at 200% (2x better)
                } else {
                    0.0
                }
            } else {
                100.0 // Default if no baseline
            };

        // Factor 3: Memory efficiency (20% weight)
        let memory_score =
            if let Some(baseline_memory) = baseline.get_metric(&MetricType::MemoryPeak) {
                let baseline_memory_mb = baseline_memory.value;
                if performance.memory_usage_mb > 0.0 {
                    let relative_memory = baseline_memory_mb / performance.memory_usage_mb;
                    (relative_memory * 100.0).min(200.0) // Cap at 200% (2x more efficient)
                } else {
                    50.0 // Penalty for no memory measurement
                }
            } else {
                100.0 // Default if no baseline
            };

        // Weighted average
        let weighted_score =
            (success_score * 0.5) + (performance_score * 0.3) + (memory_score * 0.2);
        weighted_score.clamp(0.0, 100.0)
    }
}

/// Trade-off analysis for performance vs accuracy
#[derive(Debug, Clone)]
pub struct TradeoffAnalysis {
    pub engine_type: EngineType,
    pub execution_time_ms: u128,
    pub memory_usage_mb: f64,
    pub accuracy_percentage: f64,
    pub efficiency_score: f64,  // accuracy / time
    pub memory_efficiency: f64, // accuracy / memory
}

impl TradeoffAnalysis {
    pub fn overall_score(&self) -> f64 {
        // Weighted score: 40% accuracy, 30% speed, 30% memory efficiency
        0.4 * self.accuracy_percentage + 0.3 * self.efficiency_score + 0.3 * self.memory_efficiency
    }

    pub fn rating(&self) -> &'static str {
        let score = self.overall_score();
        if score >= 90.0 {
            "Excellent"
        } else if score >= 75.0 {
            "Good"
        } else if score >= 60.0 {
            "Acceptable"
        } else {
            "Poor"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_benchmark_config() {
        let ci_config = EngineBenchmarkConfig::ci_config();
        assert_eq!(ci_config.statistical_config.min_samples, 10);
        assert!(ci_config.test_all_engines);

        let quick_config = EngineBenchmarkConfig::quick_config();
        assert_eq!(quick_config.statistical_config.min_samples, 5);
    }

    #[test]
    fn test_engine_selection_accuracy_calculation() {
        let test_results = vec![
            EngineSelectionTestResult {
                dataset_name: "test1".to_string(),
                selected_engine: EngineType::Streaming,
                actual_best_engine: EngineType::Streaming,
                selection_time_ms: 100,
                outcome: SelectionOutcome::Optimal,
                all_engine_performances: vec![],
            },
            EngineSelectionTestResult {
                dataset_name: "test2".to_string(),
                selected_engine: EngineType::MemoryEfficient,
                actual_best_engine: EngineType::Streaming,
                selection_time_ms: 120,
                outcome: SelectionOutcome::Suboptimal {
                    performance_gap: 15.0,
                },
                all_engine_performances: vec![],
            },
        ];

        let accuracy = EngineSelectionAccuracy::calculate(&test_results);
        assert_eq!(accuracy.total_tests, 2);
        assert_eq!(accuracy.optimal_selections, 1);
        assert_eq!(accuracy.suboptimal_selections, 1);
        assert_eq!(accuracy.accuracy_percentage, 50.0);
        assert_eq!(accuracy.average_performance_gap, 15.0);
    }

    #[test]
    fn test_tradeoff_analysis() {
        let analysis = TradeoffAnalysis {
            engine_type: EngineType::Streaming,
            execution_time_ms: 1000,
            memory_usage_mb: 100.0,
            accuracy_percentage: 95.0,
            efficiency_score: 95.0,
            memory_efficiency: 95.0,
        };

        assert!(analysis.overall_score() > 80.0);
        assert!(matches!(analysis.rating(), "Excellent" | "Good"));
    }

    #[test]
    fn test_selection_outcome_evaluation() {
        let config = EngineBenchmarkConfig::quick_config();
        let framework = EngineBenchmarkFramework::new(config);

        let performances = vec![
            EnginePerformance {
                engine_type: EngineType::Streaming,
                execution_time_ms: 1000,
                memory_usage_mb: 100.0,
                rows_per_second: 1000.0,
                success: true,
                error_message: None,
            },
            EnginePerformance {
                engine_type: EngineType::MemoryEfficient,
                execution_time_ms: 1200,
                memory_usage_mb: 80.0,
                rows_per_second: 833.0,
                success: true,
                error_message: None,
            },
        ];

        let outcome = framework.evaluate_selection_outcome(
            &EngineType::MemoryEfficient,
            &EngineType::Streaming,
            &performances,
        );

        match outcome {
            SelectionOutcome::Suboptimal { performance_gap } => {
                assert!((performance_gap - 20.0).abs() < 0.1);
            }
            _ => panic!("Expected suboptimal outcome"),
        }
    }
}
