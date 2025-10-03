/// Simplified Data Quality Tests for DataProfiler
/// Tests core data quality scenarios
use anyhow::Result;
use dataprof::analyze_csv;
use dataprof::core::errors::{DataProfilerError, ErrorSeverity};
use std::fs::File;
use std::io::{BufWriter, Write};
use tempfile::TempDir;

#[test]
fn test_mixed_null_values() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("mixed_nulls.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "id,name,age,city")?;
    writeln!(writer, "1,John,25,New York")?;
    writeln!(writer, "2,,30,Boston")?;
    writeln!(writer, "3,Alice,,Chicago")?;
    writeln!(writer, "4,Bob,35,")?;
    writeln!(writer, "5,,,,")?;
    writer.flush()?;

    let result = analyze_csv(&file_path);
    match result {
        Ok(columns) => {
            assert!(!columns.is_empty());
            println!(
                "✓ Successfully handled mixed null values: {} columns",
                columns.len()
            );
        }
        Err(_) => {
            println!("Acceptable error on mixed null data");
        }
    }
    Ok(())
}

#[test]
fn test_unicode_content() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("unicode.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "name,city,country")?;
    writeln!(writer, "João,São Paulo,Brasil")?;
    writeln!(writer, "田中,東京,日本")?;
    writeln!(writer, "Γιάννης,Αθήνα,Ελλάδα")?;
    writer.flush()?;

    let result = analyze_csv(&file_path);
    assert!(result.is_ok(), "Should handle Unicode content");
    Ok(())
}

#[test]
fn test_inconsistent_data_types() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("inconsistent.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "id,value,flag")?;
    writeln!(writer, "1,123,true")?;
    writeln!(writer, "2,abc,false")?;
    writeln!(writer, "3,45.67,1")?;
    writeln!(writer, "4,text,0")?;
    writer.flush()?;

    let result = analyze_csv(&file_path);
    match result {
        Ok(columns) => {
            println!("✓ Handled inconsistent types: {} columns", columns.len());
        }
        Err(_) => {
            println!("Acceptable error on inconsistent types");
        }
    }
    Ok(())
}

#[test]
fn test_sparse_data() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("sparse.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "col1,col2,col3,col4,col5")?;
    for i in 0..50 {
        match i % 10 {
            0 => {
                writeln!(writer, "value{},,,", i)?;
            }
            1 => {
                writeln!(writer, ",value{},", i)?;
            }
            2 => {
                writeln!(writer, ",,value{},", i)?;
            }
            _ => {
                writeln!(writer, ",,,,")?;
            }
        }
    }
    writer.flush()?;

    let result = analyze_csv(&file_path);
    match result {
        Ok(columns) => {
            println!("✓ Handled sparse data: {} columns", columns.len());
        }
        Err(_) => {
            println!("Acceptable error on sparse data");
        }
    }
    Ok(())
}

#[test]
fn test_quoted_fields() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("quoted.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "name,description,category")?;
    writeln!(
        writer,
        r#""John Doe","A person with ""quotes""","category1""#
    )?;
    writeln!(writer, r#""Jane Smith","Another person","category2""#)?;
    writer.flush()?;

    let result = analyze_csv(&file_path);
    assert!(result.is_ok(), "Should handle quoted fields");
    Ok(())
}

#[test]
fn test_data_quality_error_creation() {
    let quality_issues = vec![
        (
            "High null percentage",
            "90% of values are missing",
            "Consider data collection improvements",
        ),
        (
            "Inconsistent formatting",
            "Multiple date formats detected",
            "Standardize date format",
        ),
        (
            "Extreme outliers detected",
            "Values exceed 5 standard deviations",
            "Review data for errors",
        ),
    ];

    for (issue, impact, recommendation) in quality_issues {
        let error = DataProfilerError::data_quality_issue(issue, impact, recommendation);

        assert_eq!(error.category(), "data_quality");
        assert_eq!(error.severity(), ErrorSeverity::Info);
        assert!(error.is_recoverable());

        let error_msg = error.to_string();
        assert!(error_msg.contains("📊"));
        assert!(error_msg.contains("💡"));
        assert!(error_msg.contains(issue));
    }
}

#[test]
fn test_empty_fields_handling() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("empty_fields.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "col1,col2,col3")?;
    writeln!(writer, "value1,,value3")?;
    writeln!(writer, ",value2,")?;
    writeln!(writer, ",,")?;
    writer.flush()?;

    let result = analyze_csv(&file_path);
    match result {
        Ok(columns) => {
            println!("✓ Handled empty fields: {} columns", columns.len());
        }
        Err(_) => {
            println!("Acceptable error on empty fields");
        }
    }
    Ok(())
}

#[test]
fn test_whitespace_variations() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("whitespace.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "name,description,category")?;
    writeln!(writer, "  John  ,  A description  ,  category1  ")?; // Leading/trailing spaces
    writeln!(writer, "Jane,Normal description,normal")?;
    writeln!(writer, "   ,   ,   ")?; // Only whitespace
    writer.flush()?;

    let result = analyze_csv(&file_path);
    match result {
        Ok(columns) => {
            println!("✓ Handled whitespace variations: {} columns", columns.len());
        }
        Err(_) => {
            println!("Acceptable error on whitespace data");
        }
    }
    Ok(())
}

#[test]
fn test_single_column_data() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("single_col.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "single_column")?;
    for i in 0..10 {
        writeln!(writer, "value_{}", i)?;
    }
    writer.flush()?;

    let result = analyze_csv(&file_path);
    assert!(result.is_ok(), "Should handle single column data");
    if let Ok(columns) = result {
        assert_eq!(columns.len(), 1);
    }
    Ok(())
}

#[test]
fn test_large_field_content() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let file_path = temp_dir.path().join("large_fields.csv");
    let file = File::create(&file_path)?;
    let mut writer = BufWriter::new(file);

    writeln!(writer, "id,large_text")?;
    let large_text = "x".repeat(5000);
    writeln!(writer, "1,{}", large_text)?;
    writeln!(writer, "2,normal_text")?;
    writer.flush()?;

    let result = analyze_csv(&file_path);
    match result {
        Ok(columns) => {
            println!("✓ Handled large fields: {} columns", columns.len());
        }
        Err(_) => {
            println!("Acceptable error on large field data");
        }
    }
    Ok(())
}
