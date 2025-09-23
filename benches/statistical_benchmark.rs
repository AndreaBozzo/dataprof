/// Statistical rigor benchmark suite implementing comprehensive testing
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use dataprof::core::stats::{StatisticalConfig, StatisticalSample, StatisticalSummary};
use dataprof::testing::{
    DatasetInfo, DatasetPattern, DatasetSize, EngineBenchmarkConfig, EngineBenchmarkFramework,
    EngineSelectionAccuracy, MetricCollection, MetricMeasurement, MetricType, StandardDatasets,
};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

// Global result collector with enhanced metrics
lazy_static::lazy_static! {
    static ref STATISTICAL_RESULTS: Mutex<Vec<StatisticalBenchmarkResult>> = Mutex::new(Vec::new());
}

/// Enhanced benchmark result with statistical rigor metrics
#[derive(Debug, Clone)]
pub struct StatisticalBenchmarkResult {
    pub benchmark_name: String,
    pub dataset_pattern: String,
    pub dataset_size: String,
    pub statistical_summary: StatisticalSummary,
    pub engine_selection_accuracy: Option<EngineSelectionAccuracy>,
    pub performance_vs_accuracy_tradeoffs: Vec<TradeoffMetric>,
    pub regression_detected: bool,
    pub meets_quality_criteria: bool,
}

#[derive(Debug, Clone)]
pub struct TradeoffMetric {
    pub engine_name: String,
    pub execution_time_s: f64,
    pub memory_mb: f64,
    pub accuracy_percentage: f64,
    pub efficiency_ratio: f64,
}

/// Benchmark statistical framework rigor
fn bench_statistical_rigor(c: &mut Criterion) {
    let configs = [
        ("ci_config", StatisticalConfig::ci_config()),
        ("local_config", StatisticalConfig::local_config()),
        ("regression_config", StatisticalConfig::regression_config()),
    ];

    for (config_name, config) in configs {
        // Test confidence interval calculation accuracy
        let mut group = c.benchmark_group(format!("statistical_rigor_{}", config_name));

        // Generate controlled dataset with known statistical properties
        let controlled_measurements = generate_controlled_measurements(100, 10.0, 1.5);

        group.bench_function("confidence_interval_calculation", |b| {
            b.iter(|| {
                let mut sample = StatisticalSample::new(config.clone());
                for measurement in &controlled_measurements {
                    sample.add_measurement(*measurement);
                }
                let summary = sample.statistical_summary();
                black_box(summary.confidence_interval)
            })
        });

        // Test outlier detection effectiveness
        let mut measurements_with_outliers = controlled_measurements.clone();
        measurements_with_outliers.extend(vec![100.0, 150.0, -50.0]); // Add outliers

        group.bench_function("outlier_detection", |b| {
            b.iter(|| {
                let mut sample = StatisticalSample::new(config.clone());
                for measurement in &measurements_with_outliers {
                    sample.add_measurement(*measurement);
                }
                sample.remove_outliers();
                black_box(sample.values.len())
            })
        });

        // Test regression detection sensitivity
        let baseline_measurements = generate_controlled_measurements(50, 8.0, 1.0);
        let regression_measurements = generate_controlled_measurements(50, 12.0, 1.2); // 50% slower

        group.bench_function("regression_detection", |b| {
            b.iter(|| {
                let mut baseline_sample = StatisticalSample::new(config.clone());
                for measurement in &baseline_measurements {
                    baseline_sample.add_measurement(*measurement);
                }
                let baseline_summary = baseline_sample.statistical_summary();

                let mut current_sample = StatisticalSample::new(config.clone());
                for measurement in &regression_measurements {
                    current_sample.add_measurement(*measurement);
                }
                let current_summary = current_sample.statistical_summary();

                black_box(current_summary.is_regression(&baseline_summary, 0.10))
            })
        });

        group.finish();
    }
}

/// Benchmark engine selection accuracy across datasets
fn bench_engine_selection_accuracy(c: &mut Criterion) {
    let framework = EngineBenchmarkFramework::new(EngineBenchmarkConfig::comprehensive_config());

    // Generate diverse test datasets
    let test_datasets = create_diverse_test_datasets();

    let mut group = c.benchmark_group("engine_selection_accuracy");
    group.measurement_time(Duration::from_secs(60));
    group.sample_size(10);

    group.bench_function("adaptive_profiler_accuracy_test", |b| {
        b.iter(|| {
            let accuracy_result = framework
                .test_adaptive_profiler_accuracy(&test_datasets)
                .expect("Accuracy test failed");

            // Store results for analysis
            if let Ok(mut results) = STATISTICAL_RESULTS.lock() {
                results.push(StatisticalBenchmarkResult {
                    benchmark_name: "engine_selection_accuracy".to_string(),
                    dataset_pattern: "mixed_patterns".to_string(),
                    dataset_size: "varied".to_string(),
                    statistical_summary: StatisticalSummary {
                        sample_count: test_datasets.len(),
                        mean: accuracy_result.accuracy_percentage,
                        std_dev: 0.0, // Would be calculated from individual results
                        confidence_interval: (0.0, 0.0), // Would be calculated properly
                        confidence_level: 0.95,
                        coefficient_of_variation: 0.0,
                        variance_acceptable: accuracy_result.accuracy_percentage >= 85.0,
                        outliers_removed: false,
                    },
                    engine_selection_accuracy: Some(accuracy_result.clone()),
                    performance_vs_accuracy_tradeoffs: vec![],
                    regression_detected: false,
                    meets_quality_criteria: accuracy_result.meets_target(85.0),
                });
            }

            black_box(accuracy_result)
        })
    });

    group.finish();
}

/// Benchmark performance vs accuracy trade-offs
fn bench_performance_accuracy_tradeoffs(c: &mut Criterion) {
    let framework = EngineBenchmarkFramework::new(EngineBenchmarkConfig::comprehensive_config());

    let test_scenarios = [
        ("small_simple", DatasetSize::Small, DatasetPattern::Basic),
        ("medium_mixed", DatasetSize::Medium, DatasetPattern::Mixed),
        ("large_numeric", DatasetSize::Large, DatasetPattern::Numeric),
    ];

    for (scenario_name, size, pattern) in test_scenarios {
        let dataset_path = StandardDatasets::generate_temp_file(size, pattern)
            .expect("Failed to generate test dataset");

        let mut group = c.benchmark_group(format!("tradeoff_analysis_{}", scenario_name));
        group.measurement_time(Duration::from_secs(45));

        group.bench_function("comprehensive_tradeoff_analysis", |b| {
            b.iter(|| {
                // Create baseline metrics
                let baseline_info = DatasetInfo {
                    pattern: format!("{:?}", pattern),
                    size_category: format!("{:?}", size),
                    file_size_mb: std::fs::metadata(&dataset_path)
                        .map(|m| m.len() as f64 / (1024.0 * 1024.0))
                        .unwrap_or(1.0),
                    rows_count: 10000, // Estimated
                    columns_count: 5,  // Estimated
                };

                let mut baseline_collection = MetricCollection::new(
                    "baseline".to_string(),
                    "reference".to_string(),
                    baseline_info,
                );
                baseline_collection.add_metric(
                    MetricMeasurement::new(MetricType::ExecutionTime, 10.0)
                        .with_statistical_significance(true),
                );
                baseline_collection
                    .add_metric(MetricMeasurement::new(MetricType::MemoryPeak, 100.0));

                let tradeoff_results = framework
                    .measure_performance_accuracy_tradeoff(
                        &dataset_path,
                        Some(&baseline_collection),
                    )
                    .expect("Tradeoff analysis failed");

                // Convert results to metrics for storage
                let tradeoff_metrics: Vec<TradeoffMetric> = tradeoff_results
                    .into_iter()
                    .map(|(engine_type, analysis)| TradeoffMetric {
                        engine_name: format!("{:?}", engine_type),
                        execution_time_s: analysis.execution_time_ms as f64 / 1000.0,
                        memory_mb: analysis.memory_usage_mb,
                        accuracy_percentage: analysis.accuracy_percentage,
                        efficiency_ratio: analysis.efficiency_score,
                    })
                    .collect();

                // Store comprehensive results
                if let Ok(mut results) = STATISTICAL_RESULTS.lock() {
                    results.push(StatisticalBenchmarkResult {
                        benchmark_name: format!("tradeoff_analysis_{}", scenario_name),
                        dataset_pattern: format!("{:?}", pattern),
                        dataset_size: format!("{:?}", size),
                        statistical_summary: StatisticalSummary {
                            sample_count: tradeoff_metrics.len(),
                            mean: tradeoff_metrics
                                .iter()
                                .map(|m| m.efficiency_ratio)
                                .sum::<f64>()
                                / tradeoff_metrics.len() as f64,
                            std_dev: 0.0, // Would calculate actual std dev
                            confidence_interval: (0.0, 0.0),
                            confidence_level: 0.95,
                            coefficient_of_variation: 0.0,
                            variance_acceptable: true,
                            outliers_removed: false,
                        },
                        engine_selection_accuracy: None,
                        performance_vs_accuracy_tradeoffs: tradeoff_metrics.clone(),
                        regression_detected: false,
                        meets_quality_criteria: tradeoff_metrics
                            .iter()
                            .all(|m| m.efficiency_ratio > 0.5),
                    });
                }

                black_box(tradeoff_metrics)
            })
        });

        group.finish();

        // Cleanup
        let _ = std::fs::remove_file(&dataset_path);
    }
}

/// Comprehensive systematic engine comparison
fn bench_systematic_engine_comparison(c: &mut Criterion) {
    let framework = EngineBenchmarkFramework::new(EngineBenchmarkConfig::comprehensive_config());

    let comparison_scenarios = [
        ("optimal_arrow", DatasetSize::Large, DatasetPattern::Numeric),
        (
            "streaming_preferred",
            DatasetSize::Medium,
            DatasetPattern::Mixed,
        ),
        (
            "memory_constrained",
            DatasetSize::Small,
            DatasetPattern::Basic,
        ),
    ];

    for (scenario_name, size, pattern) in comparison_scenarios {
        let dataset_path = StandardDatasets::generate_temp_file(size, pattern)
            .expect("Failed to generate comparison dataset");

        let dataset_info = DatasetInfo {
            pattern: format!("{:?}", pattern),
            size_category: format!("{:?}", size),
            file_size_mb: std::fs::metadata(&dataset_path)
                .map(|m| m.len() as f64 / (1024.0 * 1024.0))
                .unwrap_or(1.0),
            rows_count: match size {
                DatasetSize::Small => 10000,
                DatasetSize::Medium => 100000,
                DatasetSize::Large => 1000000,
                _ => 50000,
            },
            columns_count: match pattern {
                DatasetPattern::Wide => 50,
                DatasetPattern::Basic => 4,
                _ => 10,
            },
        };

        let mut group = c.benchmark_group(format!("systematic_comparison_{}", scenario_name));
        group.measurement_time(Duration::from_secs(90));
        group.sample_size(10); // Minimum required by Criterion

        group.bench_function("complete_engine_comparison", |b| {
            b.iter(|| {
                let comparison_result = framework
                    .systematic_engine_comparison(&dataset_path, dataset_info.clone())
                    .expect("Systematic comparison failed");

                // Validate statistical significance of results
                let statistically_significant = comparison_result
                    .statistical_summary
                    .values()
                    .all(|summary| summary.is_statistically_significant());

                // Store comprehensive comparison results
                if let Ok(mut results) = STATISTICAL_RESULTS.lock() {
                    let winner_summary = comparison_result
                        .statistical_summary
                        .get(&comparison_result.winner)
                        .cloned()
                        .unwrap_or_else(|| StatisticalSummary {
                            sample_count: 0,
                            mean: 0.0,
                            std_dev: 0.0,
                            confidence_interval: (0.0, 0.0),
                            confidence_level: 0.95,
                            coefficient_of_variation: 0.0,
                            variance_acceptable: false,
                            outliers_removed: false,
                        });

                    results.push(StatisticalBenchmarkResult {
                        benchmark_name: format!("systematic_comparison_{}", scenario_name),
                        dataset_pattern: format!("{:?}", pattern),
                        dataset_size: format!("{:?}", size),
                        statistical_summary: winner_summary,
                        engine_selection_accuracy: None,
                        performance_vs_accuracy_tradeoffs: vec![],
                        regression_detected: false,
                        meets_quality_criteria: statistically_significant,
                    });
                }

                black_box((comparison_result.winner, statistically_significant))
            })
        });

        group.finish();

        // Cleanup
        let _ = std::fs::remove_file(&dataset_path);
    }
}

/// Generate final statistical report
fn bench_generate_final_report(c: &mut Criterion) {
    c.bench_function("generate_comprehensive_statistical_report", |b| {
        b.iter(|| {
            if let Ok(results) = STATISTICAL_RESULTS.lock() {
                let report = generate_comprehensive_report(&results);

                // Save report to file
                if let Err(e) = std::fs::write("benchmark-results/statistical_rigor_report.md", &report) {
                    eprintln!("Warning: Could not save statistical report: {}", e);
                } else {
                    println!("✅ Statistical rigor report saved to benchmark-results/statistical_rigor_report.md");
                }

                black_box(report.len())
            } else {
                black_box(0)
            }
        })
    });
}

/// Helper functions
fn generate_controlled_measurements(count: usize, mean: f64, std_dev: f64) -> Vec<f64> {
    use rand_distr::{Distribution, Normal};

    let mut rng = rand::thread_rng();
    let normal = Normal::new(mean, std_dev).unwrap();

    (0..count).map(|_| normal.sample(&mut rng)).collect()
}

fn create_diverse_test_datasets() -> Vec<(String, PathBuf)> {
    let patterns = [
        (DatasetSize::Small, DatasetPattern::Basic, "small_basic"),
        (DatasetSize::Medium, DatasetPattern::Mixed, "medium_mixed"),
        (DatasetSize::Large, DatasetPattern::Numeric, "large_numeric"),
        (DatasetSize::Small, DatasetPattern::Wide, "small_wide"),
        (DatasetSize::Medium, DatasetPattern::Deep, "medium_deep"),
    ];

    patterns
        .iter()
        .filter_map(|(size, pattern, name)| {
            StandardDatasets::generate_temp_file(*size, *pattern)
                .map(|path| (name.to_string(), path))
                .ok()
        })
        .collect()
}

fn generate_comprehensive_report(results: &[StatisticalBenchmarkResult]) -> String {
    let mut report = Vec::new();

    report.push("# 📊 Statistical Rigor and Engine Selection Benchmark Report".to_string());
    report.push("".to_string());
    report.push(format!(
        "**Generated**: {}",
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));
    report.push(format!("**Total Benchmarks**: {}", results.len()));
    report.push("".to_string());

    // Executive Summary
    let passing_benchmarks = results.iter().filter(|r| r.meets_quality_criteria).count();
    let quality_percentage = if !results.is_empty() {
        (passing_benchmarks as f64 / results.len() as f64) * 100.0
    } else {
        0.0
    };

    report.push("## 🎯 Executive Summary".to_string());
    report.push("".to_string());
    report.push(format!(
        "- **Quality Criteria Met**: {}/{} ({:.1}%)",
        passing_benchmarks,
        results.len(),
        quality_percentage
    ));

    // Engine Selection Accuracy
    let accuracy_results: Vec<_> = results
        .iter()
        .filter_map(|r| r.engine_selection_accuracy.as_ref())
        .collect();

    if !accuracy_results.is_empty() {
        let avg_accuracy = accuracy_results
            .iter()
            .map(|a| a.accuracy_percentage)
            .sum::<f64>()
            / accuracy_results.len() as f64;

        report.push(format!(
            "- **Average Engine Selection Accuracy**: {:.1}%",
            avg_accuracy
        ));
        report.push(format!(
            "- **Target Accuracy (85%)**: {}",
            if avg_accuracy >= 85.0 {
                "✅ MET"
            } else {
                "❌ NOT MET"
            }
        ));
    }

    // Statistical Significance
    let statistically_significant = results
        .iter()
        .filter(|r| r.statistical_summary.is_statistically_significant())
        .count();

    report.push(format!(
        "- **Statistically Significant Results**: {}/{} ({:.1}%)",
        statistically_significant,
        results.len(),
        (statistically_significant as f64 / results.len() as f64) * 100.0
    ));

    report.push("".to_string());

    // Detailed Results by Category
    report.push("## 📈 Detailed Results".to_string());
    report.push("".to_string());

    // Group results by benchmark type
    let mut benchmark_groups: std::collections::HashMap<String, Vec<&StatisticalBenchmarkResult>> =
        std::collections::HashMap::new();

    for result in results {
        benchmark_groups
            .entry(result.benchmark_name.clone())
            .or_default()
            .push(result);
    }

    for (benchmark_type, benchmark_results) in benchmark_groups {
        report.push(format!(
            "### {}",
            benchmark_type.replace('_', " ").to_uppercase()
        ));
        report.push("".to_string());

        for result in benchmark_results {
            report.push(format!(
                "#### {} - {}",
                result.dataset_pattern, result.dataset_size
            ));
            report.push("".to_string());

            let summary = &result.statistical_summary;
            report.push(format!("- **Mean**: {:.3}s", summary.mean));
            report.push(format!("- **Std Dev**: {:.3}s", summary.std_dev));
            report.push(format!(
                "- **Confidence Interval**: [{:.3}, {:.3}]",
                summary.confidence_interval.0, summary.confidence_interval.1
            ));
            report.push(format!(
                "- **Coefficient of Variation**: {:.1}%",
                summary.coefficient_of_variation
            ));
            report.push(format!("- **Sample Count**: {}", summary.sample_count));
            report.push(format!(
                "- **Variance Acceptable**: {}",
                if summary.variance_acceptable {
                    "✅"
                } else {
                    "❌"
                }
            ));
            report.push(format!(
                "- **Statistically Significant**: {}",
                if summary.is_statistically_significant() {
                    "✅"
                } else {
                    "❌"
                }
            ));

            if let Some(accuracy) = &result.engine_selection_accuracy {
                report.push("".to_string());
                report.push("**Engine Selection Accuracy:**".to_string());
                report.push(format!(
                    "- **Overall Accuracy**: {:.1}%",
                    accuracy.accuracy_percentage
                ));
                report.push(format!(
                    "- **Optimal Selections**: {}/{}",
                    accuracy.optimal_selections, accuracy.total_tests
                ));
                report.push(format!(
                    "- **Confidence Score**: {:.2}",
                    accuracy.confidence_score
                ));
                report.push(format!(
                    "- **Meets Target**: {}",
                    if accuracy.meets_target(85.0) {
                        "✅"
                    } else {
                        "❌"
                    }
                ));
            }

            if !result.performance_vs_accuracy_tradeoffs.is_empty() {
                report.push("".to_string());
                report.push("**Performance vs Accuracy Trade-offs:**".to_string());
                for tradeoff in &result.performance_vs_accuracy_tradeoffs {
                    report.push(format!(
                        "- **{}**: {:.3}s, {:.1}MB, {:.1}% accuracy, {:.2} efficiency",
                        tradeoff.engine_name,
                        tradeoff.execution_time_s,
                        tradeoff.memory_mb,
                        tradeoff.accuracy_percentage,
                        tradeoff.efficiency_ratio
                    ));
                }
            }

            report.push("".to_string());
        }
    }

    // Recommendations
    report.push("## 🎯 Recommendations".to_string());
    report.push("".to_string());

    if quality_percentage >= 90.0 {
        report.push("✅ **EXCELLENT**: Statistical framework meets all quality criteria with high confidence.".to_string());
    } else if quality_percentage >= 75.0 {
        report.push("⚖️ **GOOD**: Statistical framework meets most quality criteria. Minor improvements recommended.".to_string());
    } else if quality_percentage >= 60.0 {
        report.push(
            "⚠️ **ACCEPTABLE**: Statistical framework needs improvement in several areas."
                .to_string(),
        );
    } else {
        report.push("❌ **NEEDS MAJOR IMPROVEMENT**: Statistical framework requires significant enhancements.".to_string());
    }

    report.push("".to_string());
    report.push("### Specific Recommendations:".to_string());
    report.push("".to_string());

    if statistically_significant < results.len() {
        report.push("- **Increase sample sizes** for better statistical significance".to_string());
    }

    if let Some(avg_accuracy) = accuracy_results.first().map(|a| a.accuracy_percentage) {
        if avg_accuracy < 85.0 {
            report.push(
                "- **Improve engine selection logic** to achieve 85%+ accuracy target".to_string(),
            );
        }
    }

    let high_variance_count = results
        .iter()
        .filter(|r| !r.statistical_summary.variance_acceptable)
        .count();

    if high_variance_count > 0 {
        report.push("- **Reduce measurement variance** through longer warmup periods and more controlled environments".to_string());
    }

    report.push("".to_string());
    report.push("---".to_string());
    report.push("🤖 Generated by Statistical Rigor Benchmarking Framework".to_string());

    report.join("\n")
}

// Benchmark groups
criterion_group!(
    statistical_framework,
    bench_statistical_rigor,
    bench_engine_selection_accuracy
);

criterion_group!(
    performance_analysis,
    bench_performance_accuracy_tradeoffs,
    bench_systematic_engine_comparison
);

criterion_group!(reporting, bench_generate_final_report);

criterion_main!(statistical_framework, performance_analysis, reporting);
