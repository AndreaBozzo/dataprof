#[cfg(feature = "arrow")]
use dataprof::DataProfiler;
use std::io::Write;
use std::time::Instant;
use tempfile::NamedTempFile;

/// Generate a large CSV file for testing Arrow performance
#[allow(dead_code)]
fn generate_large_csv(rows: usize) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    let mut temp_file = NamedTempFile::new()?;

    // Write CSV header
    writeln!(
        temp_file,
        "id,name,age,salary,department,start_date,active,score,rating,notes"
    )?;

    // Generate realistic data
    for i in 0..rows {
        writeln!(
            temp_file,
            "{},Employee_{},{},{:.2},Dept_{},2020-01-{:02},{},'{}%',{:.1},\"Notes for employee {}\"",
            i,
            i % 1000,
            25 + (i % 40),
            35000.0 + (i % 50000) as f64,
            i % 10,
            1 + (i % 28),
            i % 2 == 0,
            75 + (i % 25),
            1.0 + (i % 5) as f64 + 0.5,
            i
        )?;
    }

    temp_file.flush()?;
    Ok(temp_file)
}

#[cfg(feature = "arrow")]
#[test]
fn test_arrow_vs_streaming_performance() -> Result<(), Box<dyn std::error::Error>> {
    // Generate a moderately large file (~50MB, ~500k rows)
    println!("Generating test data...");
    let rows = 500_000;
    let temp_file = generate_large_csv(rows)?;

    let file_size = std::fs::metadata(temp_file.path())?.len();
    let file_size_mb = file_size as f64 / 1_048_576.0;

    println!(
        "Created test file: {:.1}MB with {} rows",
        file_size_mb, rows
    );

    // Test streaming profiler
    println!("Testing streaming profiler...");
    let start = Instant::now();
    let streaming_profiler = DataProfiler::streaming();
    let streaming_report = streaming_profiler.analyze_file(temp_file.path())?;
    let streaming_time = start.elapsed();

    // Test Arrow profiler
    println!("Testing Arrow profiler...");
    let start = Instant::now();
    let arrow_profiler = DataProfiler::columnar();
    let arrow_report = arrow_profiler.analyze_csv_file(temp_file.path())?;
    let arrow_time = start.elapsed();

    // Verify both produce similar results
    assert_eq!(
        streaming_report.column_profiles.len(),
        arrow_report.column_profiles.len()
    );
    assert_eq!(
        streaming_report.file_info.total_columns,
        arrow_report.file_info.total_columns
    );

    // Performance comparison
    let speedup = streaming_time.as_secs_f64() / arrow_time.as_secs_f64();

    println!("\n🏁 Performance Results:");
    println!(
        "📊 File: {:.1}MB ({} rows, {} columns)",
        file_size_mb, rows, streaming_report.file_info.total_columns
    );
    println!("🌊 Streaming: {:.2}s", streaming_time.as_secs_f64());
    println!("⚡ Arrow:     {:.2}s", arrow_time.as_secs_f64());
    println!("🚀 Speedup:   {:.2}x", speedup);

    // Assert Arrow is at least competitive (not necessarily faster for medium files)
    // For very large files Arrow should be faster, for smaller files it might be similar
    if file_size_mb > 100.0 {
        assert!(
            speedup > 1.0,
            "Arrow should be faster for files >100MB (got {:.2}x speedup)",
            speedup
        );
    } else {
        // For smaller files, just verify it works without being dramatically slower
        assert!(
            speedup > 0.5,
            "Arrow shouldn't be more than 2x slower (got {:.2}x speedup)",
            speedup
        );
    }

    Ok(())
}

#[cfg(feature = "arrow")]
#[test]
fn test_arrow_memory_efficiency() -> Result<(), Box<dyn std::error::Error>> {
    // Test with a smaller but still meaningful dataset
    let rows = 100_000;
    let temp_file = generate_large_csv(rows)?;

    println!("Testing Arrow memory efficiency with {} rows...", rows);

    let arrow_profiler = DataProfiler::columnar()
        .batch_size(8192)
        .memory_limit_mb(64);

    let start = Instant::now();
    let report = arrow_profiler.analyze_csv_file(temp_file.path())?;
    let duration = start.elapsed();

    println!(
        "✅ Arrow processed {} rows in {:.2}s",
        report.scan_info.rows_scanned,
        duration.as_secs_f64()
    );

    // Verify results are reasonable
    assert_eq!(report.column_profiles.len(), 10); // Our test data has 10 columns
    assert!(report.scan_info.rows_scanned > 90_000); // Should process most rows
    assert!(duration.as_secs() < 30); // Should complete in reasonable time

    Ok(())
}

#[cfg(not(feature = "arrow"))]
#[test]
fn test_large_file_performance_fallback() {
    // When arrow feature is disabled, we can still test that performance tests compile
    println!("✓ Arrow feature disabled - performance tests would use streaming fallback");
}
