/// Domain-specific benchmark suite for comprehensive performance testing
/// Tests CSV vs JSON vs Database-style data across different engines
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dataprof::analyze_csv;
use dataprof::testing::{
    CriterionResultParams, DatasetConfig, DatasetPattern, DatasetSize, ResultCollector,
};
use std::path::PathBuf;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

mod domain_datasets {
    use super::{DatasetConfig, DatasetPattern, DatasetSize};
    use std::fs::File;
    use std::io::{BufWriter, Write};
    use std::path::PathBuf;

    /// Simplified domain datasets for benchmarks
    pub struct DomainDatasets;

    impl DomainDatasets {
        /// Generate transaction CSV dataset
        pub fn transactions(size: DatasetSize) -> Result<PathBuf, Box<dyn std::error::Error>> {
            let config = DatasetConfig::new(size, DatasetPattern::Mixed);
            let temp_path = std::env::temp_dir().join(format!(
                "transactions_{}_{}.csv",
                format!("{:?}", size).to_lowercase(),
                std::process::id()
            ));

            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);

            writeln!(writer, "transaction_id,user_id,amount,currency,timestamp,status,payment_method,merchant_id")?;

            let rows = config.default_rows();
            for i in 0..rows {
                writeln!(
                    writer,
                    "txn_{:08},user_{},{:.2},USD,2023-{:02}-{:02}T{:02}:{:02}:{:02}Z,completed,credit_card,merchant_{}",
                    i,
                    i % 10000,
                    (i as f64 * 1.7) % 1000.0 + 10.0,
                    1 + (i % 12),
                    1 + (i % 28),
                    i % 24,
                    (i * 7) % 60,
                    (i * 13) % 60,
                    i % 1000
                )?;

                if i % 10000 == 0 {
                    writer.flush()?;
                }
            }

            writer.flush()?;
            Ok(temp_path)
        }

        /// Generate time series CSV dataset
        pub fn timeseries(size: DatasetSize) -> Result<PathBuf, Box<dyn std::error::Error>> {
            let config = DatasetConfig::new(size, DatasetPattern::Numeric);
            let temp_path = std::env::temp_dir().join(format!(
                "timeseries_{}_{}.csv",
                format!("{:?}", size).to_lowercase(),
                std::process::id()
            ));

            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);

            writeln!(writer, "timestamp,sensor_id,value,unit,quality,location")?;

            let rows = config.default_rows();
            let base_time = 1640995200; // 2022-01-01 00:00:00 UTC

            for i in 0..rows {
                let timestamp = base_time + (i as i64 * 60);
                let sensor_id = format!("sensor_{:03}", i % 100);
                let value = 20.0 + (i as f64 * 0.1).sin() * 10.0 + (i as f64 * 0.01) % 5.0;
                let unit = "celsius";
                let quality = if i % 100 == 0 { "poor" } else { "good" };
                let location = format!("zone_{}", (i % 10) + 1);

                writeln!(
                    writer,
                    "{},{},{:.3},{},{},{}",
                    timestamp, sensor_id, value, unit, quality, location
                )?;

                if i % 10000 == 0 {
                    writer.flush()?;
                }
            }

            writer.flush()?;
            Ok(temp_path)
        }

        /// Generate streaming-optimized dataset
        pub fn streaming(
            size: DatasetSize,
            pattern: DatasetPattern,
        ) -> Result<PathBuf, Box<dyn std::error::Error>> {
            let config = DatasetConfig::new(size, pattern);
            let temp_path = std::env::temp_dir().join(format!(
                "streaming_{:?}_{}_{}.csv",
                pattern,
                format!("{:?}", size).to_lowercase(),
                std::process::id()
            ));

            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);

            match pattern {
                DatasetPattern::Mixed => {
                    writeln!(writer, "timestamp,session_id,event_type,value,user_agent")?;

                    let rows = config.default_rows();
                    for i in 0..rows {
                        writeln!(
                            writer,
                            "{},session_{},{},{:.2},Mozilla/5.0",
                            1640995200 + (i as i64 * 10),
                            i % 1000,
                            match i % 4 {
                                0 => "page_view",
                                1 => "click",
                                2 => "scroll",
                                _ => "exit",
                            },
                            (i as f64 * 1.1) % 100.0
                        )?;

                        if i % 1000 == 0 {
                            writer.flush()?;
                        }
                    }
                }
                DatasetPattern::Numeric => {
                    writeln!(writer, "timestamp,value1,value2,value3,calculated")?;

                    let rows = config.default_rows();
                    for i in 0..rows {
                        let v1 = (i as f64 * 0.01).sin();
                        let v2 = (i as f64 * 0.02).cos();
                        let v3 = (i as f64 * 0.005).tan();
                        let calculated = v1 + v2 + v3;

                        writeln!(
                            writer,
                            "{},{:.6},{:.6},{:.6},{:.6}",
                            1640995200 + (i as i64),
                            v1,
                            v2,
                            v3,
                            calculated
                        )?;

                        if i % 1000 == 0 {
                            writer.flush()?;
                        }
                    }
                }
                DatasetPattern::Basic => {
                    writeln!(writer, "id,timestamp,sensor_value,status")?;

                    let rows = config.default_rows();
                    for i in 0..rows {
                        writeln!(
                            writer,
                            "{},{},{:.3},{}",
                            i,
                            1640995200 + (i as i64 * 60), // Unix timestamp
                            (i as f64 * 0.1).sin() * 100.0, // Predictable sine wave
                            if i % 100 == 0 { "error" } else { "ok" }
                        )?;

                        if i % 1000 == 0 {
                            writer.flush()?;
                        }
                    }
                }
                _ => {
                    return Err("Pattern not supported for streaming domain".into());
                }
            }

            writer.flush()?;
            Ok(temp_path)
        }
    }
}

use domain_datasets::DomainDatasets;

// Global result collector for CI/CD integration
static DOMAIN_RESULT_COLLECTOR: LazyLock<Mutex<ResultCollector>> =
    LazyLock::new(|| Mutex::new(ResultCollector::new()));

/// Helper to collect domain benchmark results
fn collect_domain_result(
    domain: &str,
    pattern: &str,
    size: &str,
    file_size_mb: f64,
    rows_processed: u64,
    columns_processed: Option<u32>,
) -> impl Fn(&mut criterion::Bencher, &std::path::PathBuf) {
    let domain = domain.to_string();
    let pattern = pattern.to_string();
    let size = size.to_string();

    move |b, path| {
        let start_time = std::time::Instant::now();

        let result = b.iter(|| {
            let analysis_result = analyze_csv(black_box(path)).expect("Analysis failed");
            black_box(analysis_result)
        });

        let elapsed_time = start_time.elapsed().as_secs_f64();

        // Collect timing result for CI/CD integration
        if let Ok(mut collector) = DOMAIN_RESULT_COLLECTOR.lock() {
            collector.add_criterion_result(CriterionResultParams {
                dataset_pattern: format!("{}_{}", domain, pattern),
                dataset_size: size.clone(),
                file_size_mb,
                time_seconds: elapsed_time,
                memory_mb: None,
                rows_processed,
                columns_processed,
            });
        }

        result
    }
}

/// Save domain benchmark results
fn save_domain_results() {
    if let Ok(collector) = DOMAIN_RESULT_COLLECTOR.lock() {
        if let Err(e) = std::fs::create_dir_all("benchmark-results") {
            eprintln!(
                "Warning: Could not create benchmark-results directory: {}",
                e
            );
        }

        if let Err(e) = collector.save_to_file("benchmark-results/domain_benchmark_results.json") {
            eprintln!("Warning: Could not save domain results: {}", e);
        }

        if let Err(e) = collector.save_metadata("benchmark-results/domain_benchmark_metadata.json")
        {
            eprintln!("Warning: Could not save domain metadata: {}", e);
        }

        let report = collector.generate_performance_report();
        if let Err(e) = std::fs::write("benchmark-results/domain_benchmark_report.md", report) {
            eprintln!("Warning: Could not save domain report: {}", e);
        }

        println!("✅ Domain benchmark results saved to benchmark-results/");
    }
}

// Type alias for complex function type
type DatasetGenerator = Box<dyn Fn() -> Result<PathBuf, Box<dyn std::error::Error>>>;

/// Benchmark database-style CSV data patterns
fn bench_database_patterns(c: &mut Criterion) {
    let patterns: Vec<(&str, DatasetGenerator)> = vec![
        (
            "transactions",
            Box::new(|| DomainDatasets::transactions(DatasetSize::Small)),
        ),
        (
            "timeseries",
            Box::new(|| DomainDatasets::timeseries(DatasetSize::Small)),
        ),
    ];

    for (pattern_name, generator) in patterns {
        let file_path = generator().expect("Failed to generate dataset");
        let file_size = std::fs::metadata(&file_path)
            .expect("Failed to read metadata")
            .len();

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        let mut group = c.benchmark_group("database_patterns");
        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(
            BenchmarkId::new("database_csv", pattern_name),
            &file_path,
            collect_domain_result(
                "database",
                pattern_name,
                "small",
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );

        group.finish();
    }
}

/// Benchmark streaming data patterns (optimized for high-throughput)
fn bench_streaming_patterns(c: &mut Criterion) {
    let patterns = [
        ("basic", DatasetPattern::Basic),
        ("numeric", DatasetPattern::Numeric),
        ("mixed", DatasetPattern::Mixed),
    ];

    for (pattern_name, pattern) in patterns {
        let file_path =
            DomainDatasets::streaming(DatasetSize::Medium, pattern).unwrap_or_else(|e| {
                panic!(
                    "Failed to generate streaming dataset for pattern '{}': {}",
                    pattern_name, e
                )
            });
        let file_size = std::fs::metadata(&file_path)
            .expect("Failed to read metadata")
            .len();

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        let mut group = c.benchmark_group("streaming_patterns");
        group.throughput(Throughput::Bytes(file_size));
        group.measurement_time(Duration::from_secs(45));
        group.sample_size(10);

        group.bench_with_input(
            BenchmarkId::new(
                "streaming_csv",
                format!("{}_{}mb", pattern_name, file_size / (1024 * 1024)),
            ),
            &file_path,
            collect_domain_result(
                "streaming",
                pattern_name,
                "medium",
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );

        group.finish();
    }
}

/// Benchmark real-world data complexity patterns
fn bench_realworld_complexity(c: &mut Criterion) {
    // Test different complexity levels using our domain datasets
    let complexity_tests: Vec<(&str, DatasetGenerator)> = vec![
        (
            "simple_transactions",
            Box::new(|| DomainDatasets::transactions(DatasetSize::Micro)),
        ),
        (
            "complex_timeseries",
            Box::new(|| DomainDatasets::timeseries(DatasetSize::Small)),
        ),
    ];

    for (test_name, generator) in complexity_tests {
        let file_path = generator().expect("Failed to generate complexity dataset");
        let file_size = std::fs::metadata(&file_path)
            .expect("Failed to read metadata")
            .len();

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        let mut group = c.benchmark_group("realworld_complexity");
        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(
            BenchmarkId::new("complexity_test", test_name),
            &file_path,
            collect_domain_result(
                "realworld",
                test_name,
                "variable",
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );

        group.finish();
    }
}

/// Cross-domain performance comparison
fn bench_cross_domain_comparison(c: &mut Criterion) {
    // Generate datasets representing different data domains
    let domains: Vec<(&str, DatasetGenerator)> = vec![
        (
            "ecommerce",
            Box::new(|| DomainDatasets::transactions(DatasetSize::Small)),
        ),
        (
            "monitoring",
            Box::new(|| DomainDatasets::timeseries(DatasetSize::Small)),
        ),
        (
            "analytics",
            Box::new(|| DomainDatasets::streaming(DatasetSize::Small, DatasetPattern::Mixed)),
        ),
    ];

    let mut group = c.benchmark_group("cross_domain_comparison");
    group.measurement_time(Duration::from_secs(30));

    for (domain_name, generator) in domains {
        let file_path = generator().expect("Failed to generate domain dataset");
        let file_size = std::fs::metadata(&file_path)
            .expect("Failed to read metadata")
            .len();

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(
            BenchmarkId::new("domain_comparison", domain_name),
            &file_path,
            collect_domain_result(
                "comparison",
                domain_name,
                "small",
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );
    }

    group.finish();
}

/// Data volume scaling test across domains
fn bench_domain_scaling(c: &mut Criterion) {
    let sizes = [DatasetSize::Micro, DatasetSize::Small];

    for size in sizes {
        let file_path =
            DomainDatasets::transactions(size).expect("Failed to generate scaling dataset");
        let file_size = std::fs::metadata(&file_path)
            .expect("Failed to read metadata")
            .len();

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        let mut group = c.benchmark_group("domain_scaling");
        group.throughput(Throughput::Bytes(file_size));

        if matches!(size, DatasetSize::Small) {
            group.measurement_time(Duration::from_secs(45));
            group.sample_size(10);
        }

        group.bench_with_input(
            BenchmarkId::new(
                "scaling_test",
                format!("{:?}_{}mb", size, file_size / (1024 * 1024)),
            ),
            &file_path,
            collect_domain_result(
                "scaling",
                "transactions",
                &format!("{:?}", size).to_lowercase(),
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );

        group.finish();
    }
}

/// Engine selection effectiveness across domains
fn bench_engine_selection_domains(c: &mut Criterion) {
    // Test how adaptive engine selection performs on different data domains
    let domain_datasets: Vec<(&str, DatasetGenerator)> = vec![
        (
            "wide_ecommerce",
            Box::new(|| DomainDatasets::transactions(DatasetSize::Small)),
        ),
        (
            "deep_timeseries",
            Box::new(|| DomainDatasets::timeseries(DatasetSize::Medium)),
        ),
        (
            "streaming_analytics",
            Box::new(|| DomainDatasets::streaming(DatasetSize::Small, DatasetPattern::Numeric)),
        ),
    ];

    let mut group = c.benchmark_group("engine_selection_domains");
    group.measurement_time(Duration::from_secs(60));

    for (test_name, generator) in domain_datasets {
        let file_path = generator().expect("Failed to generate engine selection dataset");
        let file_size = std::fs::metadata(&file_path)
            .expect("Failed to read metadata")
            .len();

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(
            BenchmarkId::new("adaptive_engine", test_name),
            &file_path,
            |b, path| {
                let start_overall = std::time::Instant::now();

                let result = b.iter(|| {
                    // Test adaptive engine selection
                    let analysis_result = analyze_csv(black_box(path)).expect("Analysis failed");
                    black_box(analysis_result)
                });

                // Collect results after benchmark
                let elapsed_total = start_overall.elapsed().as_secs_f64();
                if let Ok(mut collector) = DOMAIN_RESULT_COLLECTOR.lock() {
                    collector.add_criterion_result(CriterionResultParams {
                        dataset_pattern: format!("adaptive_{}", test_name),
                        dataset_size: "adaptive_test".to_string(),
                        file_size_mb: file_size as f64 / (1024.0 * 1024.0),
                        time_seconds: elapsed_total,
                        memory_mb: None,
                        rows_processed: row_count,
                        columns_processed: Some(col_count),
                    });
                }

                result
            },
        );
    }

    group.finish();
}

/// Finalization benchmark to save domain results
fn bench_domain_finalize(c: &mut Criterion) {
    c.bench_function("domain_finalize_save_results", |b| {
        b.iter(|| {
            save_domain_results();
            black_box(())
        })
    });
}

// Organize domain benchmarks into logical groups
criterion_group!(
    database_benchmarks,
    bench_database_patterns,
    bench_streaming_patterns
);

criterion_group!(
    domain_analysis,
    bench_realworld_complexity,
    bench_cross_domain_comparison,
    bench_domain_scaling
);

criterion_group!(engine_effectiveness, bench_engine_selection_domains);

criterion_group!(domain_finalization, bench_domain_finalize);

criterion_main!(
    database_benchmarks,
    domain_analysis,
    engine_effectiveness,
    domain_finalization
);
