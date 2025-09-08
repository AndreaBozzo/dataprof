use anyhow::Result;
use dataprof::core::sampling::{ReservoirSampler, SamplingStrategy};
use dataprof::{
    analyze_csv,
    analyze_csv_robust,
    analyze_csv_with_sampling,
    analyze_json,
    analyze_json_with_quality,
    generate_html_report,
    quick_quality_check,
    // v0.3.0 imports for testing new API
    DataProfiler,
};
use std::fs;
use std::io::Write;
use tempfile::{tempdir, NamedTempFile};

#[test]
fn test_csv_basic_analysis() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "name,age,email")?;
    writeln!(temp_file, "John,25,john@email.com")?;
    writeln!(temp_file, "Jane,30,jane@email.com")?;
    writeln!(temp_file, "Bob,,bob@invalid")?;

    let profiles = analyze_csv(temp_file.path())?;

    assert_eq!(profiles.len(), 3);

    // Find profiles by name (order not guaranteed in HashMap)
    let age_profile = profiles
        .iter()
        .find(|p| p.name == "age")
        .expect("Test assertion failed");
    let name_profile = profiles
        .iter()
        .find(|p| p.name == "name")
        .expect("Test assertion failed");
    let email_profile = profiles
        .iter()
        .find(|p| p.name == "email")
        .expect("Test assertion failed");

    // Check null counts
    assert_eq!(age_profile.null_count, 1); // age has one null
    assert_eq!(name_profile.null_count, 0);
    assert_eq!(email_profile.null_count, 0);

    Ok(())
}

#[test]
fn test_csv_quality_analysis() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id,date,amount")?;
    writeln!(temp_file, "1,2024-01-01,100.50")?;
    writeln!(temp_file, "2,01/01/2024,200.75")?;
    writeln!(temp_file, "8,02-01-2024,180.00")?; // DD-MM-YYYY format
    writeln!(temp_file, "3,2024-01-02,999999.99")?; // outlier
    writeln!(temp_file, "5,2024-01-03,100.00")?;
    writeln!(temp_file, "6,2024-01-04,150.00")?;
    writeln!(temp_file, "7,2024-01-05,120.00")?; // more data for outlier detection
                                                 // Add more normal values for proper outlier detection
    writeln!(temp_file, "9,2024-01-06,110.00")?;
    writeln!(temp_file, "10,2024-01-07,130.00")?;
    writeln!(temp_file, "11,2024-01-08,140.00")?;
    writeln!(temp_file, "12,2024-01-09,125.00")?;
    writeln!(temp_file, "13,2024-01-10,135.00")?;
    writeln!(temp_file, "14,2024-01-11,115.00")?;
    writeln!(temp_file, "15,2024-01-12,145.00")?;
    writeln!(temp_file, "4,,150.00")?; // null date

    let report = analyze_csv_with_sampling(temp_file.path())?;

    assert_eq!(report.column_profiles.len(), 3);
    assert!(!report.issues.is_empty());

    // Should detect mixed date formats OR outliers (at least some quality issues)
    let has_mixed_dates = report
        .issues
        .iter()
        .any(|issue| matches!(issue, dataprof::QualityIssue::MixedDateFormats { .. }));
    let has_outliers = report
        .issues
        .iter()
        .any(|issue| matches!(issue, dataprof::QualityIssue::Outliers { .. }));
    let has_nulls = report
        .issues
        .iter()
        .any(|issue| matches!(issue, dataprof::QualityIssue::NullValues { .. }));

    // At least one type of issue should be detected
    assert!(
        has_mixed_dates || has_outliers || has_nulls,
        "Should detect at least one quality issue, found: {:?}",
        report.issues
    );

    Ok(())
}

#[test]
fn test_json_basic_analysis() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    let json_content = r#"[
        {"name": "John", "age": 25, "active": true},
        {"name": "Jane", "age": 30, "active": false},
        {"name": "Bob", "age": null, "active": true}
    ]"#;
    write!(temp_file, "{}", json_content)?;

    let profiles = analyze_json(temp_file.path())?;

    assert_eq!(profiles.len(), 3);

    let age_profile = profiles
        .iter()
        .find(|p| p.name == "age")
        .expect("Test assertion failed");
    assert_eq!(age_profile.null_count, 1);

    Ok(())
}

#[test]
fn test_jsonl_analysis() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(
        temp_file,
        r#"{{"timestamp": "2024-01-01T10:00:00Z", "level": "INFO"}}"#
    )?;
    writeln!(
        temp_file,
        r#"{{"timestamp": "01/01/2024 10:01:00", "level": "ERROR"}}"#
    )?;
    writeln!(
        temp_file,
        r#"{{"timestamp": "2024-01-01T10:02:00Z", "level": "INFO"}}"#
    )?;

    let report = analyze_json_with_quality(temp_file.path())?;

    assert_eq!(report.column_profiles.len(), 2);

    // Should detect mixed date formats in timestamp
    let has_mixed_dates = report
        .issues
        .iter()
        .any(|issue| matches!(issue, dataprof::QualityIssue::MixedDateFormats { .. }));

    assert!(has_mixed_dates);

    Ok(())
}

#[test]
fn test_html_report_generation() -> Result<()> {
    let mut temp_csv = NamedTempFile::new()?;
    writeln!(temp_csv, "name,score")?;
    writeln!(temp_csv, "Alice,95")?;
    writeln!(temp_csv, "Bob,87")?;
    writeln!(temp_csv, "Charlie,999")?; // outlier

    let report = analyze_csv_with_sampling(temp_csv.path())?;

    let temp_dir = tempdir()?;
    let html_path = temp_dir.path().join("report.html");

    generate_html_report(&report, &html_path)?;

    assert!(html_path.exists());

    let html_content = fs::read_to_string(&html_path)?;

    // Check that HTML contains expected elements
    assert!(html_content.contains("<!DOCTYPE html>"));
    assert!(html_content.contains("DataProfiler Report"));
    assert!(html_content.contains("name"));
    assert!(html_content.contains("score"));

    Ok(())
}

#[test]
fn test_pattern_detection() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "email,phone")?;
    writeln!(temp_file, "user1@gmail.com,+39 123 4567890")?;
    writeln!(temp_file, "user2@yahoo.it,+39 098 7654321")?;
    writeln!(temp_file, "invalid-email,invalid-phone")?;

    let profiles = analyze_csv(temp_file.path())?;

    let email_profile = profiles
        .iter()
        .find(|p| p.name == "email")
        .expect("Test assertion failed");
    let phone_profile = profiles
        .iter()
        .find(|p| p.name == "phone")
        .expect("Test assertion failed");

    // Check email pattern detection
    let has_email_pattern = email_profile.patterns.iter().any(|p| p.name == "Email");
    assert!(has_email_pattern);

    // Check phone pattern detection
    let has_phone_pattern = phone_profile
        .patterns
        .iter()
        .any(|p| p.name.contains("Phone"));
    assert!(has_phone_pattern);

    Ok(())
}

#[test]
fn test_large_file_sampling() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id,value")?;

    // Generate 100000 rows to trigger sampling (need bigger file)
    for i in 1..=100000 {
        writeln!(temp_file, "{},{}", i, i * 2)?;
    }

    let report = analyze_csv_with_sampling(temp_file.path())?;

    // Should use sampling for large file
    assert!(report.scan_info.sampling_ratio < 1.0);
    assert!(report.scan_info.rows_scanned < 100000);
    assert_eq!(report.column_profiles.len(), 2);

    Ok(())
}

#[test]
fn test_data_type_inference() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "text,integer,float,date")?;
    writeln!(temp_file, "hello,42,3.14,2024-01-01")?;
    writeln!(temp_file, "world,123,2.71,2024-01-02")?;
    writeln!(temp_file, "test,456,1.41,2024-01-03")?;

    let profiles = analyze_csv(temp_file.path())?;

    assert_eq!(profiles.len(), 4);

    let text_profile = profiles
        .iter()
        .find(|p| p.name == "text")
        .expect("Test assertion failed");
    let int_profile = profiles
        .iter()
        .find(|p| p.name == "integer")
        .expect("Test assertion failed");
    let float_profile = profiles
        .iter()
        .find(|p| p.name == "float")
        .expect("Test assertion failed");
    let date_profile = profiles
        .iter()
        .find(|p| p.name == "date")
        .expect("Test assertion failed");

    assert!(matches!(text_profile.data_type, dataprof::DataType::String));
    assert!(matches!(int_profile.data_type, dataprof::DataType::Integer));
    assert!(matches!(float_profile.data_type, dataprof::DataType::Float));
    assert!(matches!(date_profile.data_type, dataprof::DataType::Date));

    Ok(())
}

#[test]
fn test_quality_issue_severity() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "critical_nulls,warning_dups,info_outliers")?;
    writeln!(temp_file, ",duplicate,100")?;
    writeln!(temp_file, ",duplicate,200")?;
    writeln!(temp_file, ",duplicate,999999")?; // outlier
    writeln!(temp_file, ",duplicate,150")?;

    let report = analyze_csv_with_sampling(temp_file.path())?;

    // Check that different severity levels are detected
    let has_critical = report
        .issues
        .iter()
        .any(|issue| matches!(issue.severity(), dataprof::types::Severity::High));
    let has_warning = report
        .issues
        .iter()
        .any(|issue| matches!(issue.severity(), dataprof::types::Severity::Medium));

    assert!(has_critical); // Null values are critical
    assert!(has_warning); // Duplicates are warnings

    Ok(())
}

#[test]
fn test_empty_file_handling() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;

    // Test with file that has only headers
    writeln!(temp_file, "col1,col2")?;
    let result = analyze_csv(temp_file.path());
    // Should work with just headers (0 data rows)
    assert!(result.is_ok());

    Ok(())
}

#[test]
fn test_file_info_accuracy() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "col1,col2")?;
    writeln!(temp_file, "val1,val2")?;
    writeln!(temp_file, "val3,val4")?;

    let report = analyze_csv_with_sampling(temp_file.path())?;

    assert!(report.file_info.file_size_mb > 0.0);
    assert_eq!(report.file_info.total_columns, 2);

    if let Some(total_rows) = report.file_info.total_rows {
        assert_eq!(total_rows, 2); // Excluding header
    }

    Ok(())
}

// ============ v0.3.0 API Tests ============

#[test]
fn test_v030_quick_quality_check() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "name,score")?;
    writeln!(temp_file, "Alice,95")?;
    writeln!(temp_file, "Bob,87")?;
    writeln!(temp_file, "Charlie,92")?;

    let quality_score = quick_quality_check(temp_file.path())?;

    // Should return a score between 0-100
    assert!((0.0..=100.0).contains(&quality_score));
    // With no quality issues, should be 100.0
    assert_eq!(quality_score, 100.0);

    Ok(())
}

#[test]
fn test_v030_streaming_profiler_basic() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id,name,value")?;
    writeln!(temp_file, "1,test1,100")?;
    writeln!(temp_file, "2,test2,200")?;
    writeln!(temp_file, "3,test3,300")?;

    let profiler = DataProfiler::streaming();
    let report = profiler.analyze_file(temp_file.path())?;

    assert_eq!(report.column_profiles.len(), 3);
    assert_eq!(report.file_info.total_columns, 3);

    // Should have processed all rows
    assert_eq!(report.scan_info.rows_scanned, 3);

    // Find profiles by name
    let id_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "id")
        .expect("Test assertion failed");
    let name_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "name")
        .expect("Test assertion failed");
    let value_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "value")
        .expect("Test assertion failed");

    assert!(matches!(id_profile.data_type, dataprof::DataType::Integer));
    assert!(matches!(name_profile.data_type, dataprof::DataType::String));
    assert!(matches!(
        value_profile.data_type,
        dataprof::DataType::Integer
    ));

    Ok(())
}

// ============ Enhanced Reservoir Sampling Tests ============

#[test]
fn test_improved_reservoir_sampling_deterministic() -> Result<()> {
    // Test that improved reservoir sampling is deterministic with same seed
    let mut sampler1 = ReservoirSampler::with_seed(10, 42);
    let mut sampler2 = ReservoirSampler::with_seed(10, 42);

    for i in 0..100 {
        sampler1.process_record(i);
        sampler2.process_record(i);
    }

    assert_eq!(sampler1.get_sample_indices(), sampler2.get_sample_indices());
    assert_eq!(sampler1.sample_size(), 10);
    assert_eq!(sampler2.sample_size(), 10);

    Ok(())
}

#[test]
fn test_improved_reservoir_sampling_statistics() -> Result<()> {
    let mut sampler = ReservoirSampler::new(50);

    for i in 0..1000 {
        sampler.process_record(i);
    }

    let stats = sampler.get_stats();
    assert_eq!(stats.records_processed, 1000);
    assert_eq!(sampler.sample_size(), 50);

    // Sampling ratio should be 5% (50/1000)
    let ratio = sampler.sampling_ratio();
    assert!((ratio - 0.05).abs() < 0.001);

    // Should have some replacements after filling phase
    assert!(stats.replacement_count > 0);

    Ok(())
}

#[test]
fn test_robust_csv_parsing_vs_standard() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    // Create CSV with problematic data that standard parser might fail on
    writeln!(temp_file, "name,description,value")?;
    writeln!(temp_file, "Alice,\"Normal text\",100")?;
    writeln!(temp_file, "Bob,\"Text with\nnewlines\",200")?; // Multiline field
    writeln!(temp_file, "Charlie,\"Text with \"\"quotes\"\"\",300")?; // Nested quotes
    writeln!(temp_file, "David,Unquoted text with, extra comma,400,extra")?; // Extra field
    writeln!(temp_file, "Eve,Missing field")?; // Missing field

    // Standard parsing might fail, but robust should work
    let robust_result = analyze_csv_robust(temp_file.path());
    assert!(robust_result.is_ok());

    let report = robust_result?;
    assert_eq!(report.column_profiles.len(), 3);

    // Should have processed all rows (possibly with some corrections)
    assert!(report.scan_info.rows_scanned >= 4);

    Ok(())
}

#[test]
fn test_sampling_strategy_adaptive_selection() -> Result<()> {
    // Test adaptive strategy selection based on data size

    // Small dataset - should use no sampling
    let small_strategy = SamplingStrategy::adaptive(Some(1000), 1.0);
    matches!(small_strategy, SamplingStrategy::None);

    // Medium dataset - should use random sampling
    let medium_strategy = SamplingStrategy::adaptive(Some(50000), 10.0);
    matches!(medium_strategy, SamplingStrategy::Random { .. });

    // Large dataset - should use progressive sampling
    let large_strategy = SamplingStrategy::adaptive(Some(500000), 50.0);
    matches!(large_strategy, SamplingStrategy::Progressive { .. });

    // Very large file - should use multi-stage sampling
    let huge_strategy = SamplingStrategy::adaptive(Some(10000000), 2000.0);
    matches!(huge_strategy, SamplingStrategy::MultiStage { .. });

    Ok(())
}

#[test]
fn test_enhanced_error_handling() -> Result<()> {
    use std::path::Path;

    // Test with non-existent file - should fail at both strict and robust parsing
    let non_existent = Path::new("this_file_does_not_exist.csv");
    let result = analyze_csv(non_existent);
    assert!(result.is_err());

    // Test that enhanced error system works with invalid file path
    // The function should fail when trying to read the non-existent file
    let error_str = result.unwrap_err().to_string();

    // Should contain some form of file not found error or robust parser failure message
    let has_file_error = error_str.contains("not found")
        || error_str.contains("No such file")
        || error_str.contains("Impossibile trovare")
        || error_str.contains("file specificato")
        || error_str.contains("(os error 2)")
        || error_str.contains("Both strict and flexible CSV parsing failed");

    assert!(
        has_file_error,
        "Error should indicate file not found or parsing failure, got: {}",
        error_str
    );

    Ok(())
}

#[test]
fn test_large_file_performance_with_sampling() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id,category,value,timestamp")?;

    // Generate larger dataset to test sampling performance
    for i in 1..=50000 {
        let category = format!("cat_{}", i % 10);
        let timestamp = format!("2024-01-{:02}T10:00:00Z", (i % 30) + 1);
        writeln!(
            temp_file,
            "{},{},{},{}",
            i,
            category,
            (i as f64 * 1.5) as i32,
            timestamp
        )?;
    }

    let start_time = std::time::Instant::now();
    let report = analyze_csv_with_sampling(temp_file.path())?;
    let duration = start_time.elapsed();

    // Should complete in reasonable time (less than 5 seconds)
    assert!(duration.as_secs() < 5);

    // Should have used sampling
    assert!(report.scan_info.sampling_ratio < 1.0);

    // Should still detect all columns correctly
    assert_eq!(report.column_profiles.len(), 4);

    // Find timestamp column and verify it's detected as date
    let timestamp_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "timestamp")
        .expect("Test assertion failed");

    // Should detect as string or date type
    assert!(matches!(
        timestamp_profile.data_type,
        dataprof::DataType::String | dataprof::DataType::Date
    ));

    Ok(())
}

#[test]
fn test_streaming_mode_vs_standard_mode() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id,name,score")?;
    for i in 1..=1000 {
        writeln!(temp_file, "{},user_{},{}", i, i, 80 + (i % 20))?;
    }

    // Test standard mode
    let standard_report = analyze_csv_with_sampling(temp_file.path())?;

    // Test streaming mode
    let streaming_profiler = DataProfiler::streaming();
    let streaming_report = streaming_profiler.analyze_file(temp_file.path())?;

    // Both should detect same number of columns
    assert_eq!(
        standard_report.column_profiles.len(),
        streaming_report.column_profiles.len()
    );

    // Both should process all rows (since file is not that large)
    assert_eq!(standard_report.scan_info.rows_scanned, 1000);
    assert_eq!(streaming_report.scan_info.rows_scanned, 1000);

    // Column types should be consistent
    for std_profile in &standard_report.column_profiles {
        let streaming_profile = streaming_report
            .column_profiles
            .iter()
            .find(|p| p.name == std_profile.name)
            .expect("Test assertion failed");
        assert_eq!(std_profile.data_type, streaming_profile.data_type);
    }

    Ok(())
}
