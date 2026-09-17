//! Scaling and larger inputs, enabled by the full benchmark run.

use crate::support::{CsvFixture, DatasetSize, analyze_full_report};
use criterion::SamplingMode;
use criterion::{BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use std::time::Duration;

/// Tests how performance scales with data size.
/// Measures scaling; this benchmark does not assert a complexity class.
///
/// **Metrics**: Time (ms), Scaling factor
pub fn bench_scaling_behavior(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling_behavior");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));
    group.sampling_mode(SamplingMode::Flat);

    // Test scaling: tiny -> small -> medium
    let sizes = [DatasetSize::Tiny, DatasetSize::Small, DatasetSize::Medium];

    for size in sizes.iter() {
        let fixture = CsvFixture::new(*size);
        let path = fixture.path();
        let file_size = fixture.bytes();
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

/// Stress test with large datasets (100k rows).
/// Only included in full benchmark suite, not in CI by default.
///
/// **Metrics**: Time (ms), Throughput (bytes/sec)
pub fn bench_large_scale(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale");

    // Conservative sampling for large files
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(30));
    group.sampling_mode(SamplingMode::Flat);

    let fixture = CsvFixture::new(DatasetSize::Large);
    let path = fixture.path();
    let file_size = fixture.bytes();

    group.throughput(Throughput::Bytes(file_size));

    group.bench_function("large_dataset_100k_rows", |b| {
        b.iter(|| {
            let result = analyze_full_report(black_box(path)).expect("Large dataset test failed");
            black_box(result)
        })
    });

    group.finish();
}
