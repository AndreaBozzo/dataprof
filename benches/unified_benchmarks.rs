/// Unified benchmarking suite using standardized datasets
///
/// This consolidates the previously fragmented benchmark files:
/// - simple_benchmarks.rs (basic functionality)
/// - memory_benchmarks.rs (memory patterns)
/// - large_scale_benchmarks.rs (performance claims)
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dataprof::engines::local::analyze_csv;
use dataprof::testing::{
    CriterionResultParams, DatasetPattern, DatasetSize, ResultCollector, StandardDatasets,
};
use std::sync::Mutex;
use std::time::Duration;

// Global result collector for CI/CD integration
lazy_static::lazy_static! {
    static ref RESULT_COLLECTOR: Mutex<ResultCollector> = Mutex::new(ResultCollector::new());
}

/// Helper to collect benchmark results for CI/CD integration with precise timing
fn collect_benchmark_result(
    pattern: &str,
    size: &str,
    file_size_mb: f64,
    rows_processed: u64,
    columns_processed: Option<u32>,
) -> impl Fn(&mut criterion::Bencher, &std::path::PathBuf) {
    let pattern = pattern.to_string();
    let size = size.to_string();

    move |b, path| {
        let start_time = std::time::Instant::now();

        b.iter(|| {
            let analysis_result = analyze_csv(black_box(path)).expect("CSV analysis failed");
            black_box(analysis_result)
        });

        let elapsed_time = start_time.elapsed().as_secs_f64();

        // Collect actual timing result for CI/CD integration
        if let Ok(mut collector) = RESULT_COLLECTOR.lock() {
            collector.add_criterion_result(CriterionResultParams {
                dataset_pattern: pattern.clone(),
                dataset_size: size.clone(),
                file_size_mb,
                time_seconds: elapsed_time,
                memory_mb: None, // Memory collection would require external profiling
                rows_processed,
                columns_processed,
            });
        }
    }
}

/// Save collected results at the end of benchmark suite
fn save_benchmark_results() {
    if let Ok(collector) = RESULT_COLLECTOR.lock() {
        // Save to CI/CD expected location
        if let Err(e) = std::fs::create_dir_all("benchmark-results") {
            eprintln!(
                "Warning: Could not create benchmark-results directory: {}",
                e
            );
        }

        if let Err(e) = collector.save_to_file("benchmark-results/unified_benchmark_results.json") {
            eprintln!("Warning: Could not save benchmark results: {}", e);
        }

        if let Err(e) = collector.save_metadata("benchmark-results/unified_benchmark_metadata.json")
        {
            eprintln!("Warning: Could not save benchmark metadata: {}", e);
        }

        // Generate performance report
        let report = collector.generate_performance_report();
        if let Err(e) = std::fs::write("benchmark-results/unified_benchmark_report.md", report) {
            eprintln!("Warning: Could not save benchmark report: {}", e);
        }

        println!("✅ Benchmark results saved to benchmark-results/");
    }
}

/// Performance benchmarks using micro datasets (unit test scale)
fn bench_micro_performance(c: &mut Criterion) {
    let patterns = [
        ("basic", DatasetPattern::Basic),
        ("mixed", DatasetPattern::Mixed),
        ("unicode", DatasetPattern::Unicode),
    ];

    for (name, pattern) in patterns {
        let file_path = StandardDatasets::generate_temp_file(DatasetSize::Micro, pattern)
            .expect("Failed to generate micro test dataset");
        let file_size = std::fs::metadata(&file_path)
            .expect("Failed to read file metadata")
            .len();

        let mut group = c.benchmark_group("micro_performance");
        group.throughput(Throughput::Bytes(file_size));

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        group.bench_with_input(
            BenchmarkId::new("micro_analysis", name),
            &file_path,
            collect_benchmark_result(
                name,
                "micro",
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );

        group.finish();
    }
}

/// Small dataset benchmarks (integration test scale)
fn bench_small_performance(c: &mut Criterion) {
    let patterns = [
        ("basic", DatasetPattern::Basic),
        ("mixed", DatasetPattern::Mixed),
        ("numeric", DatasetPattern::Numeric),
        ("messy", DatasetPattern::Messy),
    ];

    for (name, pattern) in patterns {
        let file_path = StandardDatasets::generate_temp_file(DatasetSize::Small, pattern)
            .expect("Operation failed");
        let file_size = std::fs::metadata(&file_path)
            .expect("Operation failed")
            .len();

        let mut group = c.benchmark_group("small_performance");
        group.throughput(Throughput::Bytes(file_size));

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        group.bench_with_input(
            BenchmarkId::new("small_analysis", name),
            &file_path,
            collect_benchmark_result(
                name,
                "small",
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );

        group.finish();
    }
}

/// Memory pattern benchmarks (wide vs deep structures)
fn bench_memory_patterns(c: &mut Criterion) {
    let test_cases = [
        (DatasetSize::Micro, DatasetPattern::Wide, "micro_wide"),
        (DatasetSize::Micro, DatasetPattern::Deep, "micro_deep"),
        (DatasetSize::Small, DatasetPattern::Wide, "small_wide"),
        (DatasetSize::Small, DatasetPattern::Deep, "small_deep"),
    ];

    for (size, pattern, description) in test_cases {
        let file_path =
            StandardDatasets::generate_temp_file(size, pattern).expect("Operation failed");

        let mut group = c.benchmark_group("memory_patterns");
        group.measurement_time(Duration::from_secs(30));

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        group.bench_with_input(
            BenchmarkId::new("memory_analysis", description),
            &file_path,
            |b, path| {
                let start_overall = std::time::Instant::now();

                let result = b.iter_custom(|iters| {
                    let start_memory = get_memory_usage();
                    let start_time = std::time::Instant::now();

                    for _i in 0..iters {
                        let result = analyze_csv(black_box(path));
                        assert!(result.is_ok());
                        black_box(result.expect("Operation failed"));
                    }

                    let elapsed = start_time.elapsed();
                    let end_memory = get_memory_usage();

                    // Print memory usage info for single iteration
                    if iters == 1 {
                        println!(
                            "Memory delta for {}: {:.1}MB",
                            description,
                            (end_memory - start_memory) as f64 / 1024.0 / 1024.0
                        );
                    }

                    elapsed
                });

                // Collect results after benchmark
                let elapsed_total = start_overall.elapsed().as_secs_f64();
                if let Ok(mut collector) = RESULT_COLLECTOR.lock() {
                    collector.add_criterion_result(CriterionResultParams {
                        dataset_pattern: format!("memory_{}", description),
                        dataset_size: "pattern_test".to_string(),
                        file_size_mb: std::fs::metadata(path)
                            .map(|m| m.len() as f64 / (1024.0 * 1024.0))
                            .unwrap_or(0.0),
                        time_seconds: elapsed_total,
                        memory_mb: None,
                        rows_processed: row_count,
                        columns_processed: Some(col_count),
                    });
                }

                result
            },
        );

        group.finish();
    }
}

/// Large-scale performance benchmarks (stress test scale)
fn bench_large_scale_performance(c: &mut Criterion) {
    let patterns = [
        ("mixed", DatasetPattern::Mixed),
        ("numeric", DatasetPattern::Numeric),
    ];

    for (name, pattern) in patterns {
        // Use medium size for CI performance (large might be too slow)
        let file_path = StandardDatasets::generate_temp_file(DatasetSize::Medium, pattern)
            .expect("Operation failed");
        let file_size = std::fs::metadata(&file_path)
            .expect("Operation failed")
            .len();

        let mut group = c.benchmark_group("large_scale_performance");
        group.throughput(Throughput::Bytes(file_size));
        group.measurement_time(Duration::from_secs(60));
        group.sample_size(10);

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        group.bench_with_input(
            BenchmarkId::new(
                "large_analysis",
                format!("{}_{}mb", name, file_size / (1024 * 1024)),
            ),
            &file_path,
            collect_benchmark_result(
                name,
                "medium", // Using medium size for CI performance
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );

        group.finish();
    }
}

/// SIMD performance benchmarks (numeric-heavy data)
fn bench_simd_performance(c: &mut Criterion) {
    let sizes = [DatasetSize::Micro, DatasetSize::Small];

    for size in sizes {
        let file_path = StandardDatasets::generate_temp_file(size, DatasetPattern::Numeric)
            .expect("Operation failed");
        let file_size = std::fs::metadata(&file_path)
            .expect("Operation failed")
            .len();

        let mut group = c.benchmark_group("simd_performance");
        group.throughput(Throughput::Bytes(file_size));

        if matches!(size, DatasetSize::Small) {
            group.measurement_time(Duration::from_secs(45));
            group.sample_size(10);
        }

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        group.bench_with_input(
            BenchmarkId::new(
                "numeric_heavy",
                format!("{:?}_{}mb", size, file_size / (1024 * 1024)),
            ),
            &file_path,
            collect_benchmark_result(
                "numeric",
                &format!("{:?}", size).to_lowercase(),
                file_size as f64 / (1024.0 * 1024.0),
                row_count,
                Some(col_count),
            ),
        );

        group.finish();
    }
}

/// Memory efficiency benchmarks (streaming mode validation)
fn bench_memory_efficiency(c: &mut Criterion) {
    let sizes = [DatasetSize::Small, DatasetSize::Medium];

    for size in sizes {
        let file_path = StandardDatasets::generate_temp_file(size, DatasetPattern::Deep)
            .expect("Operation failed");

        let mut group = c.benchmark_group("memory_efficiency");

        // Get sample result for metadata
        let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
        let row_count = if !sample_result.is_empty() {
            sample_result[0].total_count as u64
        } else {
            0
        };
        let col_count = sample_result.len() as u32;

        group.bench_with_input(
            BenchmarkId::new("streaming_mode", format!("{:?}", size)),
            &file_path,
            |b, path| {
                let start_overall = std::time::Instant::now();

                let result = b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _i in 0..iters {
                        let result = analyze_csv(black_box(path));
                        assert!(result.is_ok());
                        black_box(result.expect("Operation failed"));
                    }
                    start.elapsed()
                });

                // Collect results after benchmark
                let elapsed_total = start_overall.elapsed().as_secs_f64();
                if let Ok(mut collector) = RESULT_COLLECTOR.lock() {
                    collector.add_criterion_result(CriterionResultParams {
                        dataset_pattern: "streaming".to_string(),
                        dataset_size: format!("{:?}", size).to_lowercase(),
                        file_size_mb: std::fs::metadata(path)
                            .map(|m| m.len() as f64 / (1024.0 * 1024.0))
                            .unwrap_or(0.0),
                        time_seconds: elapsed_total,
                        memory_mb: None,
                        rows_processed: row_count,
                        columns_processed: Some(col_count),
                    });
                }

                result
            },
        );

        group.finish();
    }
}

/// Memory leak detection benchmark
fn bench_memory_leak_detection(c: &mut Criterion) {
    let file_path = StandardDatasets::generate_temp_file(DatasetSize::Small, DatasetPattern::Basic)
        .expect("Operation failed");

    let mut group = c.benchmark_group("memory_leak_detection");
    group.measurement_time(Duration::from_secs(60));

    // Get sample result for metadata
    let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
    let row_count = if !sample_result.is_empty() {
        sample_result[0].total_count as u64
    } else {
        0
    };
    let col_count = sample_result.len() as u32;

    group.bench_function("repeated_analysis", |b| {
        let start_overall = std::time::Instant::now();

        let result = b.iter_custom(|iters| {
            let initial_memory = get_memory_usage();
            let start_time = std::time::Instant::now();

            // Run many iterations to detect memory leaks
            for i in 0..iters.max(10) {
                let result = analyze_csv(black_box(&file_path));
                assert!(result.is_ok());
                black_box(result.expect("Operation failed"));

                // Check for memory growth every 10 iterations
                if i % 10 == 0 && i > 0 {
                    let current_memory = get_memory_usage();
                    let growth = current_memory.saturating_sub(initial_memory);

                    if growth > 100 * 1024 * 1024 { // 100MB growth threshold
                        println!("WARNING: Potential memory leak detected. Growth: {:.1}MB after {} iterations",
                                growth as f64 / 1024.0 / 1024.0, i);
                    }
                }
            }

            let final_memory = get_memory_usage();
            let total_growth = final_memory.saturating_sub(initial_memory);

            if iters == 1 {
                println!("Total memory growth after {} iterations: {:.1}MB",
                        iters.max(10), total_growth as f64 / 1024.0 / 1024.0);
            }

            start_time.elapsed()
        });

        // Collect results after benchmark
        let elapsed_total = start_overall.elapsed().as_secs_f64();
        if let Ok(mut collector) = RESULT_COLLECTOR.lock() {
            collector.add_criterion_result(CriterionResultParams {
                dataset_pattern: "memory_leak_test".to_string(),
                dataset_size: "small".to_string(),
                file_size_mb: std::fs::metadata(&file_path).map(|m| m.len() as f64 / (1024.0 * 1024.0)).unwrap_or(0.0),
                time_seconds: elapsed_total,
                memory_mb: None,
                rows_processed: row_count,
                columns_processed: Some(col_count),
            });
        }

        result
    });

    group.finish();
}

/// Regression baseline benchmark (standardized dataset for CI)
fn bench_regression_baseline(c: &mut Criterion) {
    let file_path = StandardDatasets::generate_temp_file(DatasetSize::Small, DatasetPattern::Mixed)
        .expect("Operation failed");

    let mut group = c.benchmark_group("regression_baseline");
    group.measurement_time(Duration::from_secs(30));

    // Get sample result for metadata
    let sample_result = analyze_csv(&file_path).expect("Sample analysis failed");
    let row_count = if !sample_result.is_empty() {
        sample_result[0].total_count as u64
    } else {
        0
    };
    let col_count = sample_result.len() as u32;

    group.bench_function("standard_mixed_baseline", |b| {
        let start_overall = std::time::Instant::now();

        let result = b.iter(|| {
            let result = analyze_csv(black_box(&file_path));
            assert!(result.is_ok());
            result.expect("Operation failed")
        });

        // Collect results after benchmark
        let elapsed_total = start_overall.elapsed().as_secs_f64();
        if let Ok(mut collector) = RESULT_COLLECTOR.lock() {
            collector.add_criterion_result(CriterionResultParams {
                dataset_pattern: "baseline_mixed".to_string(),
                dataset_size: "small".to_string(),
                file_size_mb: std::fs::metadata(&file_path)
                    .map(|m| m.len() as f64 / (1024.0 * 1024.0))
                    .unwrap_or(0.0),
                time_seconds: elapsed_total,
                memory_mb: None,
                rows_processed: row_count,
                columns_processed: Some(col_count),
            });
        }

        result
    });

    group.finish();
}

/// Cross-platform memory usage detection
fn get_memory_usage() -> usize {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        if let Ok(status) = fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    if let Some(kb_str) = line.split_whitespace().nth(1) {
                        if let Ok(kb) = kb_str.parse::<usize>() {
                            return kb * 1024; // Convert KB to bytes
                        }
                    }
                }
            }
        }
        return 0;
    }

    #[cfg(target_os = "macos")]
    {
        use std::mem;

        let mut info: libc::mach_task_basic_info = unsafe { mem::zeroed() };
        let mut count = libc::MACH_TASK_BASIC_INFO_COUNT;

        unsafe {
            if libc::task_info(
                libc::mach_task_self(),
                libc::MACH_TASK_BASIC_INFO,
                &mut info as *mut _ as *mut _,
                &mut count,
            ) == libc::KERN_SUCCESS
            {
                return info.resident_size;
            }
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Windows memory detection simplified for benchmarking
        return 0;
    }

    // Fallback for other platforms
    0
}

// Organize benchmarks into logical groups
criterion_group!(
    basic_performance,
    bench_micro_performance,
    bench_small_performance,
    bench_regression_baseline
);

criterion_group!(
    memory_benchmarks,
    bench_memory_patterns,
    bench_memory_efficiency,
    bench_memory_leak_detection
);

criterion_group!(
    advanced_performance,
    bench_large_scale_performance,
    bench_simd_performance
);

/// Finalization benchmark to save results
fn bench_finalize(c: &mut Criterion) {
    c.bench_function("finalize_save_results", |b| {
        b.iter(|| {
            save_benchmark_results();
            black_box(())
        })
    });
}

criterion_group!(finalization, bench_finalize);
criterion_main!(
    basic_performance,
    memory_benchmarks,
    advanced_performance,
    finalization
);
