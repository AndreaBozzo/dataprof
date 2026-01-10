//! # DataProf Benchmarks
//!
//! Benchmark suite modulare e espandibile per dataprof.
//!
//! ## Struttura
//!
//! I benchmark sono organizzati in gruppi:
//! - `csv_analysis`: Analisi di file CSV di diverse dimensioni
//! - `type_detection`: Performance del sistema di type detection
//! - `statistics`: Calcolo statistiche (quando si aggiungono nuovi moduli)
//!
//! ## Esecuzione
//!
//! ```bash
//! # Tutti i benchmark
//! cargo bench
//!
//! # Solo un gruppo specifico
//! cargo bench -- csv_analysis
//!
//! # Solo un benchmark specifico
//! cargo bench -- "csv_analysis/small"
//! ```

use criterion::{
    BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use std::hint::black_box;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use dataprof::analyze_csv;

// ============================================================================
// Dataset Generation Utilities
// ============================================================================

/// Dimensioni standard dei dataset per i benchmark
#[derive(Debug, Clone, Copy)]
pub enum DatasetSize {
    /// 100 righe - test unitari veloci
    Tiny,
    /// 1,000 righe - benchmark rapidi
    Small,
    /// 10,000 righe - benchmark standard
    Medium,
    /// 100,000 righe - test di performance
    Large,
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

/// Genera un file CSV temporaneo con dati misti per benchmark
fn generate_benchmark_csv(size: DatasetSize) -> PathBuf {
    let temp_dir = std::env::temp_dir();
    let path = temp_dir.join(format!("dataprof_bench_{}.csv", size.name()));

    // Se il file esiste già, riutilizzalo (caching)
    if path.exists() {
        return path;
    }

    let file = std::fs::File::create(&path).expect("Failed to create temp file");
    let mut writer = std::io::BufWriter::new(file);

    // Header con tipi misti
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
            20 + (i % 50),                             // age: 20-69
            30000.0 + (i as f64 * 1.5) % 70000.0,      // salary
            if i % 3 == 0 { "true" } else { "false" }, // is_active
            1 + (i % 12),                              // month
            1 + (i % 28),                              // day
            (i as f64 * 0.01) % 100.0                  // score
        )
        .unwrap();
    }

    writer.flush().unwrap();
    path
}

// ============================================================================
// CSV Analysis Benchmarks
// ============================================================================

/// Benchmark di analisi CSV per diverse dimensioni
fn csv_analysis_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_analysis");

    // Configurazione per CI: sampling ridotto per velocità
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));

    let sizes = [DatasetSize::Tiny, DatasetSize::Small, DatasetSize::Medium];

    for size in sizes {
        let path = generate_benchmark_csv(size);
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        group.throughput(Throughput::Bytes(file_size));
        group.bench_with_input(
            BenchmarkId::new("analyze", size.name()),
            &path,
            |b, path| {
                b.iter(|| {
                    let result = analyze_csv(black_box(path)).expect("Analysis failed");
                    black_box(result)
                })
            },
        );
    }

    group.finish();
}

/// Benchmark con dataset large (opzionale, più lento)
fn csv_analysis_large(c: &mut Criterion) {
    let mut group = c.benchmark_group("csv_analysis_large");

    // Configurazione per benchmark lunghi
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.sampling_mode(SamplingMode::Flat);

    let path = generate_benchmark_csv(DatasetSize::Large);
    let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

    group.throughput(Throughput::Bytes(file_size));
    group.bench_with_input(BenchmarkId::new("analyze", "large"), &path, |b, path| {
        b.iter(|| {
            let result = analyze_csv(black_box(path)).expect("Analysis failed");
            black_box(result)
        })
    });

    group.finish();
}

// ============================================================================
// Criterion Configuration
// ============================================================================

// Gruppo principale - benchmark veloci per CI
criterion_group! {
    name = quick_benches;
    config = Criterion::default()
        .sample_size(20)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1));
    targets = csv_analysis_benchmarks
}

// Gruppo completo - include benchmark grandi
criterion_group! {
    name = full_benches;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(Duration::from_secs(5))
        .warm_up_time(Duration::from_secs(2));
    targets = csv_analysis_benchmarks, csv_analysis_large
}

// Entry point - usa quick_benches per CI, full_benches per sviluppo locale
criterion_main!(quick_benches);
