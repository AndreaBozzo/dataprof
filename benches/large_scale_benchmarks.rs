use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dataprof::engines::local::analyze_csv;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::Duration;
use tempfile::TempDir;

/// Generate large-scale CSV data for benchmarking performance claims
fn generate_large_csv(temp_dir: &TempDir, size_mb: usize) -> std::path::PathBuf {
    let file_path = temp_dir
        .path()
        .join(format!("large_benchmark_{}mb.csv", size_mb));
    let file = File::create(&file_path).unwrap();
    let mut writer = BufWriter::new(file);

    // Write headers - mix of data types similar to real-world data
    writeln!(
        writer,
        "id,name,email,age,salary,created_at,active,score,category,description"
    )
    .unwrap();

    // Calculate rows needed for target size (approximate)
    let avg_row_size = 120; // estimated bytes per row
    let target_bytes = size_mb * 1024 * 1024;
    let rows_needed = target_bytes / avg_row_size;

    println!("Generating {}MB CSV with ~{} rows...", size_mb, rows_needed);

    for i in 0..rows_needed {
        let name = format!("User_{}", i);
        let email = format!("user{}@example.com", i);
        let age = 18 + (i % 65);
        let salary = 30000 + (i % 100000);
        let created_at = format!("2023-{:02}-{:02}", 1 + (i % 12), 1 + (i % 28));
        let active = i % 3 == 0;
        let score = (i as f64 * 1.7) % 100.0;
        let category = match i % 5 {
            0 => "Premium",
            1 => "Standard",
            2 => "Basic",
            3 => "Enterprise",
            _ => "Trial",
        };
        let description = format!(
            "Description for user {} with extended content and metadata",
            i
        );

        writeln!(
            writer,
            "{},{},{},{},{},{},{},{:.2},{},{}",
            i, name, email, age, salary, created_at, active, score, category, description
        )
        .unwrap();

        // Flush periodically to avoid memory issues
        if i % 10000 == 0 {
            writer.flush().unwrap();
            if i % 100000 == 0 {
                println!("Generated {} rows...", i);
            }
        }
    }

    writer.flush().unwrap();

    let actual_size = std::fs::metadata(&file_path).unwrap().len() as f64 / (1024.0 * 1024.0);
    println!("Generated file: {:.1}MB", actual_size);

    file_path
}

/// Generate numeric-heavy CSV for SIMD performance testing
fn generate_numeric_csv(temp_dir: &TempDir, size_mb: usize) -> std::path::PathBuf {
    let file_path = temp_dir.path().join(format!("numeric_{}mb.csv", size_mb));
    let file = File::create(&file_path).unwrap();
    let mut writer = BufWriter::new(file);

    writeln!(writer, "id,value1,value2,value3,value4,value5,calculated").unwrap();

    let target_bytes = size_mb * 1024 * 1024;
    let avg_row_size = 50;
    let rows_needed = target_bytes / avg_row_size;

    for i in 0..rows_needed {
        let v1 = (i as f64 * 1.1) % 1000.0;
        let v2 = (i as f64 * 2.3) % 1000.0;
        let v3 = (i as f64 * 3.7) % 1000.0;
        let v4 = (i as f64 * 5.1) % 1000.0;
        let v5 = (i as f64 * 7.9) % 1000.0;
        let calculated = v1 + v2 + v3 + v4 + v5;

        writeln!(
            writer,
            "{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
            i, v1, v2, v3, v4, v5, calculated
        )
        .unwrap();

        if i % 10000 == 0 {
            writer.flush().unwrap();
        }
    }

    writer.flush().unwrap();
    file_path
}

/// Benchmark large-scale CSV processing to validate "10x faster" claims
fn bench_large_scale_performance(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    // Test different file sizes to validate scaling claims
    for &size_mb in &[1, 10, 50, 100] {
        let file_path = generate_large_csv(&temp_dir, size_mb);
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        let mut group = c.benchmark_group("large_scale_analysis");
        group.throughput(Throughput::Bytes(file_size));

        // Set longer measurement time for large files
        if size_mb >= 50 {
            group.measurement_time(Duration::from_secs(60));
            group.sample_size(10);
        }

        group.bench_with_input(
            BenchmarkId::new("mixed_data_large", format!("{}MB", size_mb)),
            &file_path,
            |b, path| {
                b.iter(|| {
                    let result = analyze_csv(black_box(path));
                    assert!(result.is_ok(), "Analysis should succeed");
                    result.unwrap()
                })
            },
        );

        group.finish();
    }
}

/// Benchmark numeric-heavy data for SIMD performance validation
fn bench_simd_performance(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    for &size_mb in &[5, 25, 50] {
        let file_path = generate_numeric_csv(&temp_dir, size_mb);
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        let mut group = c.benchmark_group("simd_numeric_analysis");
        group.throughput(Throughput::Bytes(file_size));

        if size_mb >= 25 {
            group.measurement_time(Duration::from_secs(45));
            group.sample_size(10);
        }

        group.bench_with_input(
            BenchmarkId::new("numeric_heavy", format!("{}MB", size_mb)),
            &file_path,
            |b, path| {
                b.iter(|| {
                    let result = analyze_csv(black_box(path));
                    assert!(result.is_ok());
                    result.unwrap()
                })
            },
        );

        group.finish();
    }
}

/// Memory efficiency benchmark for streaming mode validation
fn bench_memory_efficiency(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    // Generate progressively larger files to test memory usage
    for &size_mb in &[10, 50, 100] {
        let file_path = generate_large_csv(&temp_dir, size_mb);

        let mut group = c.benchmark_group("memory_efficiency");

        group.bench_with_input(
            BenchmarkId::new("streaming_mode", format!("{}MB", size_mb)),
            &file_path,
            |b, path| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _i in 0..iters {
                        let result = analyze_csv(black_box(path));
                        assert!(result.is_ok());
                        // Force result to be used
                        black_box(result.unwrap());
                    }
                    start.elapsed()
                })
            },
        );

        group.finish();
    }
}

/// Regression benchmark using fixed datasets
fn bench_regression_detection(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    // Create standardized test dataset for regression detection
    let standard_file = generate_large_csv(&temp_dir, 10); // 10MB standard

    let mut group = c.benchmark_group("regression_baseline");
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("standard_10mb_baseline", |b| {
        b.iter(|| {
            let result = analyze_csv(black_box(&standard_file));
            assert!(result.is_ok());
            result.unwrap()
        })
    });

    group.finish();
}

criterion_group!(
    large_benchmarks,
    bench_large_scale_performance,
    bench_simd_performance,
    bench_memory_efficiency,
    bench_regression_detection
);
criterion_main!(large_benchmarks);
