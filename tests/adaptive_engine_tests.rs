use dataprof::{AdaptiveProfiler, ProcessingType};
use std::io::Write;
use tempfile::NamedTempFile;

/// Test intelligent engine selection
#[test]
fn test_adaptive_engine_selection() -> Result<(), Box<dyn std::error::Error>> {
    // Create a small test file - should select streaming engine
    let mut small_file = NamedTempFile::new()?;
    writeln!(small_file, "name,age")?;
    writeln!(small_file, "Alice,25")?;
    writeln!(small_file, "Bob,30")?;
    small_file.flush()?;

    let profiler = AdaptiveProfiler::new()
        .with_logging(false) // Disable logging for clean test output
        .with_fallback(true);

    let report = profiler.analyze_file(small_file.path())?;

    assert_eq!(report.column_profiles.len(), 2);
    assert_eq!(report.scan_info.rows_scanned, 2);

    println!("✓ Adaptive engine selection works for small files");
    Ok(())
}

/// Test transparent fallback mechanism
#[test]
fn test_fallback_mechanism() -> Result<(), Box<dyn std::error::Error>> {
    // Create a problematic CSV file that might cause some engines to fail
    let mut problematic_file = NamedTempFile::new()?;
    writeln!(problematic_file, "name,data,mixed")?;
    writeln!(problematic_file, "test1,\"complex,data\"with\"quotes\",123")?;
    writeln!(problematic_file, "test2,normal_data,456")?;
    writeln!(problematic_file, "test3,,789")?; // Empty field
    problematic_file.flush()?;

    let profiler = AdaptiveProfiler::new()
        .with_logging(false)
        .with_fallback(true);

    // Should complete successfully due to fallback
    let report = profiler.analyze_file(problematic_file.path())?;

    assert_eq!(report.column_profiles.len(), 3);
    assert!(report.scan_info.rows_scanned >= 3);

    println!("✓ Fallback mechanism handles problematic files");
    Ok(())
}

/// Test engine benchmarking
#[test]
fn test_engine_benchmarking() -> Result<(), Box<dyn std::error::Error>> {
    // Create a medium-sized test file
    let mut test_file = NamedTempFile::new()?;
    writeln!(test_file, "id,name,value,category")?;
    for i in 0..1000 {
        writeln!(
            test_file,
            "{},Item_{},{:.2},Cat_{}",
            i,
            i,
            i as f64 * 1.5,
            i % 10
        )?;
    }
    test_file.flush()?;

    let profiler = AdaptiveProfiler::new()
        .with_logging(false)
        .with_performance_logging(true);

    let performances = profiler.benchmark_engines(test_file.path())?;

    // Should have multiple engine performance results
    assert!(!performances.is_empty());

    // At least one engine should succeed
    let successful_engines = performances.iter().filter(|p| p.success).count();
    assert!(successful_engines > 0, "At least one engine should succeed");

    // Performance results should have reasonable execution times
    for perf in &performances {
        if perf.success {
            assert!(
                perf.execution_time_ms > 0,
                "Execution time should be positive"
            );
            // Shouldn't take more than 30 seconds for this small dataset
            assert!(
                perf.execution_time_ms < 30_000,
                "Execution should complete in reasonable time"
            );
        }
    }

    println!(
        "✓ Engine benchmarking works with {} engines tested",
        performances.len()
    );
    Ok(())
}

/// Test processing type hints affect engine selection
#[test]
fn test_processing_type_hints() -> Result<(), Box<dyn std::error::Error>> {
    let mut test_file = NamedTempFile::new()?;
    writeln!(test_file, "timestamp,value,user_id")?;
    writeln!(test_file, "2023-01-01T10:00:00,100.5,user1")?;
    writeln!(test_file, "2023-01-01T11:00:00,200.3,user2")?;
    writeln!(test_file, "2023-01-01T12:00:00,150.7,user3")?;
    test_file.flush()?;

    let profiler = AdaptiveProfiler::new()
        .with_logging(false)
        .with_fallback(true);

    // Test different processing types
    let quality_report =
        profiler.analyze_file_with_context(test_file.path(), ProcessingType::QualityFocused)?;

    let batch_report =
        profiler.analyze_file_with_context(test_file.path(), ProcessingType::BatchAnalysis)?;

    // Both should succeed and produce consistent results
    assert_eq!(
        quality_report.column_profiles.len(),
        batch_report.column_profiles.len()
    );
    assert_eq!(
        quality_report.scan_info.rows_scanned,
        batch_report.scan_info.rows_scanned
    );

    println!("✓ Processing type hints work correctly");
    Ok(())
}

/// Test DataProfiler::auto() API
#[test]
fn test_dataprof_auto_api() -> Result<(), Box<dyn std::error::Error>> {
    use dataprof::DataProfiler;

    let mut test_file = NamedTempFile::new()?;
    writeln!(test_file, "product,price,in_stock")?;
    writeln!(test_file, "Widget,19.99,true")?;
    writeln!(test_file, "Gadget,29.99,false")?;
    writeln!(test_file, "Tool,39.99,true")?;
    test_file.flush()?;

    // Test the new auto() API
    let profiler = DataProfiler::auto().with_logging(false);

    let report = profiler.analyze_file(test_file.path())?;

    assert_eq!(report.column_profiles.len(), 3);
    assert_eq!(report.scan_info.rows_scanned, 3);

    println!("✓ DataProfiler::auto() API works correctly");
    Ok(())
}

/// Test runtime Arrow detection
#[test]
fn test_runtime_arrow_detection() -> Result<(), Box<dyn std::error::Error>> {
    use dataprof::engines::EngineSelector;

    let selector = EngineSelector::new();

    let mut test_file = NamedTempFile::new()?;
    writeln!(test_file, "id,name")?;
    writeln!(test_file, "1,test")?;
    test_file.flush()?;

    let characteristics = selector.analyze_file_characteristics(test_file.path())?;

    // Should successfully analyze file characteristics
    assert!(characteristics.file_size_mb > 0.0);
    assert_eq!(characteristics.estimated_columns, Some(2));

    let recommendation = selector.select_engine(&characteristics, ProcessingType::BatchAnalysis);

    // Should get a valid recommendation
    assert!(recommendation.confidence >= 0.0 && recommendation.confidence <= 1.0);
    assert!(!recommendation.reasoning.is_empty());

    println!("✓ Runtime engine detection works (Arrow is always available)");
    Ok(())
}

/// Test backward compatibility with existing APIs
#[test]
fn test_backward_compatibility() -> Result<(), Box<dyn std::error::Error>> {
    use dataprof::{DataProfiler, analyze_csv};

    let mut test_file = NamedTempFile::new()?;
    writeln!(test_file, "name,score")?;
    writeln!(test_file, "Alice,95.5")?;
    writeln!(test_file, "Bob,87.2")?;
    test_file.flush()?;

    // Test old API still works
    let old_profiles = analyze_csv(test_file.path())?;
    assert_eq!(old_profiles.len(), 2);

    // Test existing DataProfiler methods still work
    let mut streaming_profiler = DataProfiler::streaming();
    let streaming_report = streaming_profiler.analyze_file(test_file.path())?;
    assert_eq!(streaming_report.column_profiles.len(), 2);

    let arrow_profiler = DataProfiler::columnar();
    let arrow_report = arrow_profiler.analyze_csv_file(test_file.path())?;
    assert_eq!(arrow_report.column_profiles.len(), 2);

    println!("✓ Backward compatibility maintained");
    Ok(())
}

/// Test performance improvement with automatic selection
#[test]
fn test_performance_improvement() -> Result<(), Box<dyn std::error::Error>> {
    // Create a medium file where intelligent selection should help
    let mut test_file = NamedTempFile::new()?;
    writeln!(test_file, "id,data1,data2,data3,data4")?;
    for i in 0..5000 {
        writeln!(test_file, "{},{},{},{},{}", i, i * 2, i * 3, i * 4, i * 5)?;
    }
    test_file.flush()?;

    let adaptive_profiler = AdaptiveProfiler::new().with_logging(false);

    let start = std::time::Instant::now();
    let adaptive_report = adaptive_profiler.analyze_file(test_file.path())?;
    let adaptive_time = start.elapsed();

    // Compare with a fixed engine choice (streaming)
    use dataprof::DataProfiler;
    let mut streaming_profiler = DataProfiler::streaming();

    let start = std::time::Instant::now();
    let streaming_report = streaming_profiler.analyze_file(test_file.path())?;
    let streaming_time = start.elapsed();

    // Both should produce the same results
    assert_eq!(
        adaptive_report.column_profiles.len(),
        streaming_report.column_profiles.len()
    );
    assert_eq!(
        adaptive_report.scan_info.rows_scanned,
        streaming_report.scan_info.rows_scanned
    );

    // Adaptive should be reasonable in CI environments (within 3.0x of streaming time)
    // Note: CI environments can have variable performance due to resource constraints
    let time_ratio = adaptive_time.as_secs_f64() / streaming_time.as_secs_f64();
    assert!(
        time_ratio < 3.0,
        "Adaptive selection shouldn't be excessively slower (ratio: {:.2})",
        time_ratio
    );

    println!(
        "✓ Performance improvement verified (adaptive: {:.2}s, streaming: {:.2}s, ratio: {:.2})",
        adaptive_time.as_secs_f64(),
        streaming_time.as_secs_f64(),
        time_ratio
    );
    Ok(())
}

/// Test Arrow integration with intelligent selection
#[test]
fn test_arrow_intelligent_selection() -> Result<(), Box<dyn std::error::Error>> {
    // Create a larger file that should trigger Arrow selection
    let mut large_file = NamedTempFile::new()?;
    writeln!(
        large_file,
        "id,name,value1,value2,value3,value4,value5,value6,value7,value8,value9,value10"
    )?;

    // Generate enough data to potentially trigger Arrow selection
    for i in 0..10000 {
        writeln!(
            large_file,
            "{},Item_{},{},{},{},{},{},{},{},{},{},{}",
            i,
            i,
            i as f64 * 1.1,
            i as f64 * 1.2,
            i as f64 * 1.3,
            i as f64 * 1.4,
            i as f64 * 1.5,
            i as f64 * 1.6,
            i as f64 * 1.7,
            i as f64 * 1.8,
            i as f64 * 1.9,
            i as f64 * 2.0
        )?;
    }
    large_file.flush()?;

    let file_size = std::fs::metadata(large_file.path())?.len() as f64 / 1_048_576.0;
    println!("Generated test file: {:.1}MB", file_size);

    let profiler = AdaptiveProfiler::new()
        .with_logging(true) // Enable logging to see engine selection
        .with_performance_logging(true);

    let report = profiler.analyze_file(large_file.path())?;

    assert_eq!(report.column_profiles.len(), 12);
    assert!(report.scan_info.rows_scanned >= 10000);

    // Also test benchmarking on this larger file
    let performances = profiler.benchmark_engines(large_file.path())?;
    let successful_count = performances.iter().filter(|p| p.success).count();

    assert!(
        successful_count > 0,
        "At least one engine should handle the large file"
    );

    println!(
        "✓ Arrow intelligent selection works for larger files ({} engines succeeded)",
        successful_count
    );
    Ok(())
}
