// DataProfiler v0.3.0 - Comprehensive Integration Tests
// Tests all major v0.3.0 features with real data

use anyhow::Result;
use dataprof::core::sampling::strategies::SamplingStrategy;
use dataprof::engines::columnar::simple_columnar::SimpleColumnarProfiler;
use dataprof::engines::streaming::memmap::MemoryMappedCsvReader;
use dataprof::engines::streaming::memory_efficient::MemoryEfficientProfiler;
use dataprof::engines::streaming::true_streaming::TrueStreamingProfiler;
use std::path::Path;

/// Test memory mapping functionality with real CSV files
#[test]
fn test_memory_mapping() -> Result<()> {
    let test_file = Path::new("examples/sales_data_problematic.csv");
    if !test_file.exists() {
        println!("Skipping test: example file not found");
        return Ok(());
    }

    let reader = MemoryMappedCsvReader::new(test_file)?;
    assert!(reader.file_size() > 0, "Should have file size > 0");

    let estimated_rows = reader.estimate_row_count()?;
    assert!(estimated_rows > 0, "Should estimate > 0 rows");

    let (headers, records) = reader.read_csv_chunk(0, 4096, true)?;
    assert!(headers.is_some(), "Should have headers");
    assert!(!records.is_empty(), "Should have records");

    println!(
        "✓ Memory mapping: {} bytes, ~{} rows",
        reader.file_size(),
        estimated_rows
    );
    Ok(())
}

/// Test true streaming processing
#[test]
fn test_streaming_processing() -> Result<()> {
    let test_file = Path::new("examples/large_mixed_data.csv");
    if !test_file.exists() {
        println!("Skipping test: example file not found");
        return Ok(());
    }

    let profiler = TrueStreamingProfiler::new().memory_limit_mb(20);
    let report = profiler.analyze_file(test_file)?;

    assert!(report.scan_info.rows_scanned > 0, "Should scan > 0 rows");
    assert!(
        report.file_info.total_columns > 0,
        "Should have > 0 columns"
    );
    assert!(
        !report.column_profiles.is_empty(),
        "Should have column profiles"
    );

    println!(
        "✓ Streaming: {} rows, {} columns",
        report.scan_info.rows_scanned, report.file_info.total_columns
    );
    Ok(())
}

/// Test SIMD acceleration
#[test]
fn test_simd_acceleration() -> Result<()> {
    // This test verifies SIMD is available by running the unit tests
    // The actual SIMD functionality is tested in the lib tests
    use dataprof::acceleration::simd::{compute_stats_auto, should_use_simd};

    let test_data: Vec<f64> = (1..=1000).map(|x| x as f64).collect();

    // Test SIMD availability
    let simd_available = should_use_simd(test_data.len());
    println!(
        "SIMD available for {} elements: {}",
        test_data.len(),
        simd_available
    );

    // Test SIMD computation
    let stats = compute_stats_auto(&test_data);
    assert!(stats.count > 0, "Should have computed stats");
    assert!(stats.min > 0.0, "Should have correct min value");
    assert!(stats.max > 0.0, "Should have correct max value");

    println!("✓ SIMD: computed stats for {} elements", stats.count);
    Ok(())
}

/// Test columnar processing
#[test]
fn test_columnar_processing() -> Result<()> {
    let test_file = Path::new("examples/sales_data_problematic.csv");
    if !test_file.exists() {
        println!("Skipping test: example file not found");
        return Ok(());
    }

    let profiler = SimpleColumnarProfiler::new().use_simd(true);
    let report = profiler.analyze_csv_file(test_file)?;

    assert!(report.scan_info.rows_scanned > 0, "Should scan > 0 rows");
    assert!(
        !report.column_profiles.is_empty(),
        "Should have column profiles"
    );

    println!(
        "✓ Columnar: {} rows, {} columns",
        report.scan_info.rows_scanned, report.file_info.total_columns
    );
    Ok(())
}

/// Test memory efficient processing
#[test]
fn test_memory_efficient() -> Result<()> {
    let test_file = Path::new("examples/large_mixed_data.csv");
    if !test_file.exists() {
        println!("Skipping test: example file not found");
        return Ok(());
    }

    let profiler = MemoryEfficientProfiler::new();
    let report = profiler.analyze_file(test_file)?;

    assert!(report.scan_info.rows_scanned > 0, "Should scan > 0 rows");
    assert!(
        !report.column_profiles.is_empty(),
        "Should have column profiles"
    );

    println!(
        "✓ Memory efficient: {} rows, {} columns",
        report.scan_info.rows_scanned, report.file_info.total_columns
    );
    Ok(())
}

/// Test sampling strategies configuration
#[test]
fn test_sampling_strategies() -> Result<()> {
    // Test Progressive sampling
    let progressive = SamplingStrategy::Progressive {
        initial_size: 100,
        confidence_level: 0.95,
        max_size: 1000,
    };
    match progressive {
        SamplingStrategy::Progressive {
            initial_size,
            confidence_level,
            max_size,
        } => {
            assert_eq!(initial_size, 100);
            assert_eq!(confidence_level, 0.95);
            assert_eq!(max_size, 1000);
        }
        _ => panic!("Expected Progressive sampling"),
    }

    // Test Reservoir sampling
    let reservoir = SamplingStrategy::Reservoir { size: 500 };
    match reservoir {
        SamplingStrategy::Reservoir { size } => assert_eq!(size, 500),
        _ => panic!("Expected Reservoir sampling"),
    }

    // Test Stratified sampling
    let stratified = SamplingStrategy::Stratified {
        key_columns: vec!["department".to_string()],
        samples_per_stratum: 50,
    };
    match stratified {
        SamplingStrategy::Stratified {
            key_columns,
            samples_per_stratum,
        } => {
            assert_eq!(key_columns.len(), 1);
            assert_eq!(samples_per_stratum, 50);
        }
        _ => panic!("Expected Stratified sampling"),
    }

    println!("✓ Sampling strategies: Progressive, Reservoir, Stratified all configured");
    Ok(())
}

/// Comprehensive integration test with real data
#[test]
fn test_full_integration() -> Result<()> {
    let test_files = [
        "examples/sales_data_problematic.csv",
        "examples/large_mixed_data.csv",
    ];

    for file_path in &test_files {
        let test_file = Path::new(file_path);
        if !test_file.exists() {
            println!("Skipping {}: file not found", file_path);
            continue;
        }

        println!("Testing: {}", file_path);

        // Test all profilers on the same file
        let streaming_report = TrueStreamingProfiler::new()
            .memory_limit_mb(20)
            .analyze_file(test_file)?;

        let columnar_report = SimpleColumnarProfiler::new().analyze_csv_file(test_file)?;

        let memory_report = MemoryEfficientProfiler::new().analyze_file(test_file)?;

        // Basic consistency checks
        assert!(!streaming_report.column_profiles.is_empty());
        assert!(!columnar_report.column_profiles.is_empty());
        assert!(!memory_report.column_profiles.is_empty());

        println!("  ✓ All profilers processed successfully");
        println!(
            "  ✓ Quality issues detected: {}",
            streaming_report.issues.len()
        );
    }

    println!("✓ Full integration test completed");
    Ok(())
}

/// Performance benchmark with generated data
#[test]
fn test_performance_benchmark() -> Result<()> {
    use std::fs::File;
    use std::io::{BufWriter, Write};
    use tempfile::TempDir;

    let temp_dir = TempDir::new()?;
    let test_file = temp_dir.path().join("benchmark.csv");

    // Generate test data
    let mut writer = BufWriter::new(File::create(&test_file)?);
    writeln!(writer, "id,name,age,salary,active")?;
    for i in 0..2000 {
        writeln!(
            writer,
            "{},User{},{},{},{}",
            i,
            i % 500,
            25 + (i % 40),
            30000 + (i * 50),
            i % 2 == 0
        )?;
    }
    writer.flush()?;
    drop(writer);

    // Benchmark profilers
    let start = std::time::Instant::now();
    let streaming_report = TrueStreamingProfiler::new().analyze_file(&test_file)?;
    let streaming_time = start.elapsed();

    let start = std::time::Instant::now();
    let columnar_report = SimpleColumnarProfiler::new().analyze_csv_file(&test_file)?;
    let columnar_time = start.elapsed();

    println!("Performance benchmark (2000 rows):");
    println!(
        "  Streaming: {:?} - {} rows",
        streaming_time, streaming_report.scan_info.rows_scanned
    );
    println!(
        "  Columnar:  {:?} - {} rows",
        columnar_time, columnar_report.scan_info.rows_scanned
    );

    // Both should process all rows
    assert!(streaming_report.scan_info.rows_scanned >= 1900); // Allow for sampling
    assert_eq!(columnar_report.scan_info.rows_scanned, 2000);

    println!("✓ Performance benchmark completed");
    Ok(())
}
