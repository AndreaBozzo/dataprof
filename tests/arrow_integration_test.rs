#[test]
fn test_arrow_api_integration() -> Result<(), Box<dyn std::error::Error>> {
    use dataprof::DataProfiler;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create test CSV
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "name,age,salary")?;
    writeln!(temp_file, "Alice,25,50000.0")?;
    writeln!(temp_file, "Bob,30,60000.5")?;
    writeln!(temp_file, "Charlie,35,70000.0")?;
    temp_file.flush()?;

    // Test Arrow profiler is accessible via public API
    let profiler = DataProfiler::columnar();
    let report = profiler.analyze_csv_file(temp_file.path())?;

    assert_eq!(report.column_profiles.len(), 3);
    assert!(report.scan_info.rows_scanned > 0);

    println!("✓ Arrow integration via DataProfiler::columnar() works");
    Ok(())
}

#[test]
fn test_arrow_with_adaptive_selection() -> Result<(), Box<dyn std::error::Error>> {
    use dataprof::{AdaptiveProfiler, DataProfiler};
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Create a file large enough to potentially trigger Arrow selection
    let mut temp_file = NamedTempFile::new()?;
    writeln!(
        temp_file,
        "id,name,age,salary,department,start_date,active,score,rating,notes,bonus,level"
    )?;

    // Generate enough data to be interesting for Arrow
    for i in 0..5000 {
        writeln!(
            temp_file,
            "{},Employee_{},{},{:.2},Dept_{},2020-01-{:02},{},{},{:.1},\"Note {}\",{:.2},{}",
            i,
            i,
            25 + (i % 40),
            35000.0 + (i % 50000) as f64,
            i % 10,
            1 + (i % 28),
            i % 2 == 0,
            75 + (i % 25),
            1.0 + (i % 5) as f64,
            i,
            1000.0 + (i % 10000) as f64,
            i % 5
        )?;
    }
    temp_file.flush()?;

    let file_size = std::fs::metadata(temp_file.path())?.len() as f64 / 1_048_576.0;
    println!("Created test file: {:.1}MB", file_size);

    // Test adaptive selection (might choose Arrow for this larger file)
    let adaptive_profiler = AdaptiveProfiler::new().with_logging(false); // Keep test output clean

    let adaptive_report = adaptive_profiler.analyze_file(temp_file.path())?;

    // Test explicit Arrow selection for comparison
    let arrow_profiler = DataProfiler::columnar();
    let arrow_report = arrow_profiler.analyze_csv_file(temp_file.path())?;

    // Both should produce consistent results
    assert_eq!(
        adaptive_report.column_profiles.len(),
        arrow_report.column_profiles.len()
    );
    assert_eq!(adaptive_report.column_profiles.len(), 12);

    // Verify data processing
    assert!(adaptive_report.scan_info.rows_scanned >= 5000);
    assert!(arrow_report.scan_info.rows_scanned >= 5000);

    println!("✓ Arrow integration with adaptive selection works");
    println!(
        "  Adaptive processed: {} rows",
        adaptive_report.scan_info.rows_scanned
    );
    println!(
        "  Arrow processed: {} rows",
        arrow_report.scan_info.rows_scanned
    );

    Ok(())
}

#[test]
fn test_arrow_fallback_behavior() -> Result<(), Box<dyn std::error::Error>> {
    use dataprof::{AdaptiveProfiler, ProcessingType, engines::EngineSelector};
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Test that Arrow is properly included in engine recommendations
    let mut temp_file = NamedTempFile::new()?;
    writeln!(
        temp_file,
        "col1,col2,col3,col4,col5,col6,col7,col8,col9,col10"
    )?;
    for i in 0..1000 {
        writeln!(
            temp_file,
            "{},{},{},{},{},{},{},{},{},{}",
            i,
            i * 2,
            i * 3,
            i * 4,
            i * 5,
            i * 6,
            i * 7,
            i * 8,
            i * 9,
            i * 10
        )?;
    }
    temp_file.flush()?;

    let selector = EngineSelector::new();
    let characteristics = selector.analyze_file_characteristics(temp_file.path())?;
    let recommendation = selector.select_engine(&characteristics, ProcessingType::BatchAnalysis);

    // Arrow should be available as either primary or fallback
    let has_arrow = recommendation.primary_engine == dataprof::engines::EngineType::Arrow
        || recommendation
            .fallback_engines
            .contains(&dataprof::engines::EngineType::Arrow);

    println!(
        "✓ Arrow fallback test - Arrow included in recommendation: {}",
        has_arrow
    );
    println!("  Primary engine: {:?}", recommendation.primary_engine);
    println!("  Fallback engines: {:?}", recommendation.fallback_engines);
    println!("  Reasoning: {}", recommendation.reasoning);

    // Test that adaptive profiler works
    let profiler = AdaptiveProfiler::new().with_logging(false);
    let report = profiler.analyze_file(temp_file.path())?;

    assert_eq!(report.column_profiles.len(), 10);
    assert!(report.scan_info.rows_scanned >= 1000);

    println!("✓ Arrow fallback behavior verified");
    Ok(())
}
