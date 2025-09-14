use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dataprof::engines::local::analyze_csv;
use std::fs::File;
use std::io::{BufWriter, Write};
use tempfile::TempDir;

/// Generate simple CSV data for benchmarking
fn generate_simple_csv(temp_dir: &TempDir, rows: usize) -> std::path::PathBuf {
    let file_path = temp_dir.path().join(format!("benchmark_{}.csv", rows));
    let file = File::create(&file_path).unwrap();
    let mut writer = BufWriter::new(file);

    // Write headers
    writeln!(writer, "id,name,age,score").unwrap();

    // Write data rows
    for i in 0..rows {
        writeln!(
            writer,
            "{},Person_{},{},{:.1}",
            i,
            i,
            25 + (i % 50),
            (i as f64 * 1.5) % 100.0
        )
        .unwrap();

        if i % 1000 == 0 {
            writer.flush().unwrap();
        }
    }

    writer.flush().unwrap();
    file_path
}

/// Generate CSV with mixed data types
fn generate_mixed_csv(temp_dir: &TempDir, rows: usize) -> std::path::PathBuf {
    let file_path = temp_dir.path().join(format!("mixed_{}.csv", rows));
    let file = File::create(&file_path).unwrap();
    let mut writer = BufWriter::new(file);

    writeln!(writer, "id,text,number,flag").unwrap();

    for i in 0..rows {
        let text = match i % 4 {
            0 => "Short",
            1 => "Medium length text",
            2 => "Much longer text with more content",
            _ => "Very very long text content that contains multiple words and characters",
        };

        writeln!(writer, "{},{},{},{}", i, text, (i * 7) % 1000, i % 2 == 0).unwrap();
    }

    writer.flush().unwrap();
    file_path
}

fn bench_csv_analysis_performance(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    // Test different data sizes
    for &size in &[100, 500, 1000, 2000] {
        let file_path = generate_simple_csv(&temp_dir, size);
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        let mut group = c.benchmark_group("csv_analysis");
        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(
            BenchmarkId::new("simple_data", size),
            &file_path,
            |b, path| b.iter(|| analyze_csv(black_box(path)).unwrap()),
        );

        group.finish();
    }
}

fn bench_mixed_data_types(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    // Test mixed data processing
    for &size in &[200, 500, 1000] {
        let file_path = generate_mixed_csv(&temp_dir, size);
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        let mut group = c.benchmark_group("mixed_data_analysis");
        group.throughput(Throughput::Bytes(file_size));

        group.bench_with_input(
            BenchmarkId::new("mixed_types", size),
            &file_path,
            |b, path| b.iter(|| analyze_csv(black_box(path)).unwrap()),
        );

        group.finish();
    }
}

fn bench_unicode_data(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let file_path = temp_dir.path().join("unicode_test.csv");
    let file = File::create(&file_path).unwrap();
    let mut writer = BufWriter::new(file);

    writeln!(writer, "name,city,country").unwrap();

    let unicode_data = [
        ("João", "São Paulo", "Brasil"),
        ("田中", "東京", "日本"),
        ("Γιάννης", "Αθήνα", "Ελλάδα"),
        ("François", "Paris", "France"),
        ("Ahmed", "Cairo", "مصر"),
    ];

    // Repeat data to make meaningful benchmark
    for _ in 0..200 {
        for (name, city, country) in &unicode_data {
            writeln!(writer, "{},{},{}", name, city, country).unwrap();
        }
    }

    writer.flush().unwrap();
    let file_size = std::fs::metadata(&file_path).unwrap().len();

    let mut group = c.benchmark_group("unicode_processing");
    group.throughput(Throughput::Bytes(file_size));

    group.bench_function("unicode_data", |b| {
        b.iter(|| analyze_csv(black_box(&file_path)).unwrap())
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_csv_analysis_performance,
    bench_mixed_data_types,
    bench_unicode_data
);
criterion_main!(benches);
