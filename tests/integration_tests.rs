use anyhow::Result;
use dataprof::{
    analyze_csv, analyze_csv_with_sampling, analyze_json, analyze_json_with_quality,
    generate_html_report,
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
    let age_profile = profiles.iter().find(|p| p.name == "age").unwrap();
    let name_profile = profiles.iter().find(|p| p.name == "name").unwrap();
    let email_profile = profiles.iter().find(|p| p.name == "email").unwrap();

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

    let age_profile = profiles.iter().find(|p| p.name == "age").unwrap();
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

    let email_profile = profiles.iter().find(|p| p.name == "email").unwrap();
    let phone_profile = profiles.iter().find(|p| p.name == "phone").unwrap();

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

    let text_profile = profiles.iter().find(|p| p.name == "text").unwrap();
    let int_profile = profiles.iter().find(|p| p.name == "integer").unwrap();
    let float_profile = profiles.iter().find(|p| p.name == "float").unwrap();
    let date_profile = profiles.iter().find(|p| p.name == "date").unwrap();

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
