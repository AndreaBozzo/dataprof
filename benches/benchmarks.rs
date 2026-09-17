//! Rust benchmark entry point. See `benches/README.md` for the suite inventory.
//!
//! Scenarios live in `scenarios/`; reusable fixtures and operations in `support/`.
//! Keep group/function/input IDs stable: CI filters, Criterion histories, and
//! published website links depend on them. Fixture generation is never timed.
//!
//! Run all scenarios: `cargo bench --bench benchmarks`.
//! Filter a group: `cargo bench --bench benchmarks -- csv_parsing`.
//! Filter one case: `cargo bench --bench benchmarks -- csv_parsing/parse/small`.
//! Run each case once: `cargo bench --bench benchmarks -- --test`.

mod scenarios;
mod support;

use criterion::{Criterion, criterion_group, criterion_main};
use scenarios::csv::{bench_csv_parsing, bench_full_analysis, bench_throughput_metrics};
use scenarios::scaling::{bench_large_scale, bench_scaling_behavior};
use std::time::Duration;

// Quick suite: CSV scan/column profiling and full report assembly.
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
