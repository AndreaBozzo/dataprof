//! # DataProf Benchmarks
//!
//! Benchmark suite modulare con output leggibile attraverso Benchmark Groups.
//! Ogni gruppo è una sezione separata nel report HTML di Criterion.
//!
//! ## Struttura Gerarchica
//!
//! - **csv_parsing**: Streaming CSV scan and column profiling
//! - **full_analysis**: End-to-end facade profiling and report assembly
//! - **throughput_metrics**: Fast row-count throughput across dataset sizes
//! - **scaling_behavior**: Linear vs non-linear scaling tests
//! - **large_scale**: Stress tests with 100k+ rows (optional)
//!
//! ## Esecuzione
//!
//! ```bash
//! # Tutti i benchmark
//! cargo bench
//!
//! # Solo un gruppo specifico
//! cargo bench -- csv_parsing
//!
//! # Solo un benchmark specifico
//! cargo bench -- "csv_parsing/small"
//! ```

use anyhow::Result;
use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use dataprof::{CsvParserConfig, ProfileReport, analyze_csv_from_reader, quick_row_count};
use std::hint::black_box;
use std::io::{BufReader, Write};
use std::path::PathBuf;
use std::time::Duration;

// ============================================================================
// Dataset Generation Utilities
// ============================================================================

/// Standard dataset sizes for benchmarks
#[derive(Debug, Clone, Copy)]
pub enum DatasetSize {
    Tiny,   // 100 rows
    Small,  // 1,000 rows
    Medium, // 10,000 rows
    Large,  // 100,000 rows
}

impl DatasetSize {
    fn rows(&self) -> usize {
        match self {
            DatasetSize::Tiny => 100,
            DatasetSize::Small => 1_000,
            DatasetSize::Medium => 10_000,
            DatasetSize::Large => 100_000,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            DatasetSize::Tiny => "tiny",
            DatasetSize::Small => "small",
            DatasetSize::Medium => "medium",
            DatasetSize::Large => "large",
        }
    }
}

/// Generates a mixed-type CSV file for benchmarking with caching
fn generate_benchmark_csv(size: DatasetSize) -> PathBuf {
    let temp_dir = std::env::temp_dir();
    let path = temp_dir.join(format!("dataprof_bench_{}.csv", size.name()));

    // Cache: reuse existing file if present
    if path.exists() {
        return path;
    }

    let file = std::fs::File::create(&path).expect("Failed to create temp file");
    let mut writer = std::io::BufWriter::new(file);

    writeln!(
        writer,
        "id,name,email,age,salary,is_active,created_at,score"
    )
    .unwrap();

    let rows = size.rows();
    for i in 0..rows {
        writeln!(
            writer,
            "{},User_{},user{}@example.com,{},{:.2},{},2024-{:02}-{:02},{:.3}",
            i,
            i,
            i,
            20 + (i % 50),
            30000.0 + (i as f64 * 1.5) % 70000.0,
            if i % 3 == 0 { "true" } else { "false" },
            1 + (i % 12),
            1 + (i % 28),
            (i as f64 * 0.01) % 100.0
        )
        .unwrap();
    }

    writer.flush().unwrap();
    path
}

// ============================================================================
// Benchmark Group 1: CSV Parsing Performance
// ============================================================================

/// Core CSV parsing performance across multiple file sizes.
/// This group isolates the CSV scan and column profiling phase before report assembly.
///
/// **Metrics**: Time (ms), Throughput (MB/s)
fn bench_csv_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_parsing");

    // Configuration optimized for CI
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    let sizes = [DatasetSize::Tiny, DatasetSize::Small, DatasetSize::Medium];

    for size in sizes {
        let path = generate_benchmark_csv(size);
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        // KEY: throughput metric shows MB/s in the report
        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(BenchmarkId::new("parse", size.name()), &path, |b, path| {
            b.iter(|| {
                let result = parse_csv_phase(black_box(path)).expect("CSV parsing failed");
                black_box(result)
            })
        });
    }

    group.finish();
}

// ============================================================================
// Benchmark Group 2: Full Analysis Pipeline
// ============================================================================

/// End-to-end analysis including type detection, statistics, and report assembly.
/// Tests the complete facade path from raw CSV to final profile report.
///
/// **Metrics**: Time (ms), Throughput (MB/s)
fn bench_full_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_analysis");

    group.sample_size(25);
    group.measurement_time(Duration::from_secs(8));
    group.warm_up_time(Duration::from_secs(2));

    for size in [DatasetSize::Tiny, DatasetSize::Small, DatasetSize::Medium].iter() {
        let path = generate_benchmark_csv(*size);
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(
            BenchmarkId::new("analyze", size.name()),
            &path,
            |b, path| {
                b.iter(|| {
                    let result =
                        analyze_full_report(black_box(path)).expect("Full analysis failed");
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark Group 3: Throughput Metrics
// ============================================================================

/// Tests fast row counting throughput across representative dataset sizes.
/// This isolates the lightweight cardinality path from CSV parsing and full profiling.
///
/// **Metrics**: Rows/sec
fn bench_throughput_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_metrics");

    // More aggressive sampling for throughput testing
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    for size in [DatasetSize::Small, DatasetSize::Medium, DatasetSize::Large] {
        let path = generate_benchmark_csv(size);

        group.throughput(Throughput::Elements(size.rows() as u64));

        group.bench_with_input(
            BenchmarkId::new("row_count", size.name()),
            &path,
            |b, path| {
                b.iter(|| {
                    let result = count_rows_phase(black_box(path)).expect("Throughput test failed");
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark Group 4: Scaling Tests
// ============================================================================

/// Tests how performance scales with data size.
/// Verifies linear or near-linear scaling properties.
///
/// **Metrics**: Time (ms), Scaling factor
fn bench_scaling_behavior(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling_behavior");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.sampling_mode(SamplingMode::Flat);

    // Test scaling: tiny -> small -> medium
    let sizes = [DatasetSize::Tiny, DatasetSize::Small, DatasetSize::Medium];

    for size in sizes.iter() {
        let path = generate_benchmark_csv(*size);
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let row_count = size.rows();

        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(
            BenchmarkId::new("rows", format!("{}", row_count)),
            &path,
            |b, path| {
                b.iter(|| {
                    let result = analyze_full_report(black_box(path)).expect("Scaling test failed");
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

// ============================================================================
// Benchmark Group 5: Large Scale Performance (Optional)
// ============================================================================

/// Stress test with large datasets (100k rows).
/// Only included in full benchmark suite, not in CI by default.
///
/// **Metrics**: Time (ms), Throughput (MB/s)
fn bench_large_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale");

    // Conservative sampling for large files
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.sampling_mode(SamplingMode::Flat);

    let path = generate_benchmark_csv(DatasetSize::Large);
    let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    group.throughput(Throughput::Bytes(file_size));

    group.bench_function("large_dataset_100k_rows", |b| {
        b.iter(|| {
            let result = analyze_full_report(black_box(&path)).expect("Large dataset test failed");
            black_box(result)
        })
    });

    group.finish();
}

// ============================================================================
// Criterion Configuration & Main
// ============================================================================

// Quick suite: for CI/CD, covers parser-only and full end-to-end profiling
// Runs: csv_parsing, full_analysis
criterion_group! {
    name = quick_benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1));
    targets = bench_csv_parsing, bench_full_analysis
}

// Full suite: comprehensive set for local runs
// Runs: throughput_metrics, scaling_behavior, large_scale
criterion_group! {
    name = full_benches;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(2));
    targets =
        bench_throughput_metrics,
        bench_scaling_behavior,
        bench_large_scale
}

// Main entry point: include both suites; filter via CLI if needed
criterion_main!(quick_benches, full_benches);

// ============================================================================
// Local helpers: benchmark the core phases directly without widening the public API surface
// ============================================================================

fn parse_csv_phase(path: &std::path::Path) -> Result<(usize, usize)> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let config = CsvParserConfig::default();
    let (profiles, _stats, rows_read, _headers) = analyze_csv_from_reader(reader, &config)?;

    Ok((profiles.len(), rows_read))
}

fn analyze_full_report(path: &std::path::Path) -> Result<ProfileReport> {
    use dataprof::Profiler;

    let report = Profiler::new().analyze_file(path)?;
    Ok(report)
}

fn count_rows_phase(path: &std::path::Path) -> Result<u64> {
    let estimate = quick_row_count(path)?;
    Ok(estimate.count)
}
