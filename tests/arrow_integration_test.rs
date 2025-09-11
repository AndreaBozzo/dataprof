#[cfg(feature = "arrow")]
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

#[cfg(not(feature = "arrow"))]
#[test]
fn test_arrow_feature_disabled() {
    // When arrow feature is disabled, compilation should still work
    // but columnar() method should not be available
    println!("✓ Arrow feature disabled - test passes by compilation");
}
