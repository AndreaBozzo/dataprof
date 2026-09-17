//! CSV scan, full report assembly, and fast row-count estimation.

use crate::support::{
    CsvFixture, DatasetSize, analyze_full_report, count_rows_phase, parse_csv_phase,
};
use criterion::{BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::time::Duration;

/// Core CSV parsing performance across multiple file sizes.
/// This group isolates the CSV scan and column profiling phase before report assembly.
///
/// **Metrics**: Time (ms), Throughput (bytes/sec)
pub fn bench_csv_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_parsing");

    // Configuration optimized for CI
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));
    group.warm_up_time(Duration::from_secs(1));

    let sizes = [DatasetSize::Tiny, DatasetSize::Small, DatasetSize::Medium];

    for size in sizes {
        let fixture = CsvFixture::new(size);
        let path = fixture.path();
        let file_size = fixture.bytes();

        // Criterion derives byte throughput from the measured fixture size.
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

/// End-to-end analysis including type detection, statistics, and report assembly.
/// Tests the complete facade path from raw CSV to final profile report.
///
/// **Metrics**: Time (ms), Throughput (bytes/sec)
pub fn bench_full_analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_analysis");

    group.sample_size(25);
    group.measurement_time(Duration::from_secs(8));
    group.warm_up_time(Duration::from_secs(2));

    for size in [DatasetSize::Tiny, DatasetSize::Small, DatasetSize::Medium].iter() {
        let fixture = CsvFixture::new(*size);
        let path = fixture.path();
        let file_size = fixture.bytes();

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

/// Tests fast row counting throughput across representative dataset sizes.
/// This isolates the lightweight cardinality path from CSV parsing and full profiling.
///
/// **Metrics**: Rows/sec
pub fn bench_throughput_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_metrics");

    // More aggressive sampling for throughput testing
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(2));

    for size in [DatasetSize::Small, DatasetSize::Medium, DatasetSize::Large] {
        let fixture = CsvFixture::new(size);
        let path = fixture.path();

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
