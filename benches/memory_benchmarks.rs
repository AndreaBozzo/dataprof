use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use dataprof::engines::local::analyze_csv;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Duration;
use tempfile::TempDir;

// Note: pprof disabled on Windows due to compatibility issues
// use pprof::criterion::{Output, PProfProfiler};

/// Generate memory-intensive CSV for profiling
fn generate_memory_test_csv(temp_dir: &TempDir, rows: usize, wide: bool) -> std::path::PathBuf {
    let suffix = if wide { "wide" } else { "deep" };
    let file_path = temp_dir
        .path()
        .join(format!("memory_test_{}_{}.csv", rows, suffix));
    let file = File::create(&file_path).unwrap();
    let mut writer = BufWriter::new(file);

    if wide {
        // Wide format: many columns (tests memory usage per row)
        let headers: Vec<String> = (0..50).map(|i| format!("col_{}", i)).collect();
        writeln!(writer, "{}", headers.join(",")).unwrap();

        for i in 0..rows {
            let values: Vec<String> = (0..50).map(|j| format!("value_{}_{}", i, j)).collect();
            writeln!(writer, "{}", values.join(",")).unwrap();
        }
    } else {
        // Deep format: few columns, many rows (tests streaming efficiency)
        writeln!(writer, "id,name,value,timestamp").unwrap();

        for i in 0..rows {
            writeln!(
                writer,
                "{},Name_{},{:.3},2023-01-{:02}",
                i,
                i,
                (i as f64 * 1.7) % 1000.0,
                1 + (i % 30)
            )
            .unwrap();
        }
    }

    writer.flush().unwrap();
    file_path
}

/// Benchmark memory usage patterns
fn bench_memory_usage(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let test_cases = [
        (1000, false, "1K rows deep"),
        (10000, false, "10K rows deep"),
        (50000, false, "50K rows deep"),
        (1000, true, "1K rows wide"),
        (5000, true, "5K rows wide"),
    ];

    for &(rows, wide, description) in &test_cases {
        let file_path = generate_memory_test_csv(&temp_dir, rows, wide);

        let mut group = c.benchmark_group("memory_usage");
        group.measurement_time(Duration::from_secs(30));

        group.bench_with_input(
            BenchmarkId::new("memory_pattern", description),
            &file_path,
            |b, path| {
                b.iter_custom(|iters| {
                    let start_memory = get_memory_usage();
                    let start_time = std::time::Instant::now();

                    for _i in 0..iters {
                        let result = analyze_csv(black_box(path));
                        assert!(result.is_ok());
                        black_box(result.unwrap());
                    }

                    let elapsed = start_time.elapsed();
                    let end_memory = get_memory_usage();

                    // Print memory usage info
                    if iters == 1 {
                        println!(
                            "Memory delta for {}: {:.1}MB",
                            description,
                            (end_memory - start_memory) as f64 / 1024.0 / 1024.0
                        );
                    }

                    elapsed
                })
            },
        );

        group.finish();
    }
}

/// Benchmark streaming mode memory efficiency
fn bench_streaming_memory(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    // Generate progressively larger files to test memory scaling
    for &rows in &[10000, 50000, 100000] {
        let file_path = generate_memory_test_csv(&temp_dir, rows, false);

        let mut group = c.benchmark_group("streaming_memory");
        group.measurement_time(Duration::from_secs(45));

        group.bench_with_input(
            BenchmarkId::new("constant_memory", format!("{} rows", rows)),
            &file_path,
            |b, path| {
                b.iter_custom(|iters| {
                    let start_time = std::time::Instant::now();
                    let mut peak_memory = 0;

                    for _i in 0..iters {
                        let before_mem = get_memory_usage();
                        let result = analyze_csv(black_box(path));
                        let after_mem = get_memory_usage();

                        peak_memory = peak_memory.max(after_mem - before_mem);

                        assert!(result.is_ok());
                        black_box(result.unwrap());
                    }

                    let elapsed = start_time.elapsed();

                    // Report peak memory usage
                    if iters == 1 {
                        println!(
                            "Peak memory for {} rows: {:.1}MB",
                            rows,
                            peak_memory as f64 / 1024.0 / 1024.0
                        );
                    }

                    elapsed
                })
            },
        );

        group.finish();
    }
}

/// Get current process memory usage in bytes
fn get_memory_usage() -> usize {
    // Cross-platform memory usage detection
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
    }

    #[cfg(target_os = "macos")]
    {
        use std::mem;
        use std::ptr;

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
        // Windows memory detection disabled - would require winapi dependency
        // For benchmarking purposes, we'll use a simplified approach
        0
    }

    // Fallback: return 0 if unable to detect
    // Note: This code is unreachable on Windows due to early return above
}

/// Memory leak detection benchmark
fn bench_memory_leak_detection(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let file_path = generate_memory_test_csv(&temp_dir, 10000, false);

    let mut group = c.benchmark_group("memory_leak_detection");
    group.measurement_time(Duration::from_secs(60));

    group.bench_function("repeated_analysis", |b| {
        b.iter_custom(|iters| {
            let initial_memory = get_memory_usage();
            let start_time = std::time::Instant::now();

            // Run many iterations to detect memory leaks
            for i in 0..iters.max(10) {
                let result = analyze_csv(black_box(&file_path));
                assert!(result.is_ok());
                black_box(result.unwrap());

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
        })
    });

    group.finish();
}

// Set up benchmarks (profiler disabled on Windows)
criterion_group!(
    memory_benches,
    bench_memory_usage,
    bench_streaming_memory,
    bench_memory_leak_detection
);

criterion_main!(memory_benches);
