//! Integration tests for boolean column detection and statistics.
//!
//! Verifies that boolean columns are correctly detected and profiled
//! through both the standard CSV parser and the unified Profiler API.

use std::io::Write;

use dataprof::parsers::csv::{CsvParserConfig, analyze_csv_file};
use dataprof::types::{ColumnStats, DataType};
use dataprof::{EngineType, Profiler};
use tempfile::NamedTempFile;

fn create_boolean_csv() -> NamedTempFile {
    let mut f = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(f, "id,active,verified,label").unwrap();
    for i in 0..100 {
        let active = if i % 3 == 0 { "false" } else { "true" };
        let verified = if i % 2 == 0 { "True" } else { "False" };
        writeln!(f, "{},{},{},item{}", i, active, verified, i).unwrap();
    }
    f.flush().unwrap();
    f
}

#[test]
fn test_csv_parser_detects_boolean_columns() {
    let csv = create_boolean_csv();
    let report = analyze_csv_file(csv.path(), &CsvParserConfig::default())
        .expect("CSV analysis should succeed");

    // id should be Integer
    let id_col = report
        .column_profiles
        .iter()
        .find(|c| c.name == "id")
        .unwrap();
    assert_eq!(id_col.data_type, DataType::Integer);

    // active should be Boolean (lowercase true/false)
    let active_col = report
        .column_profiles
        .iter()
        .find(|c| c.name == "active")
        .unwrap();
    assert_eq!(
        active_col.data_type,
        DataType::Boolean,
        "Expected Boolean for 'active' column"
    );
    match &active_col.stats {
        ColumnStats::Boolean(b) => {
            // 34 false (i % 3 == 0 for i in 0..100) and 66 true
            assert_eq!(b.true_count, 66);
            assert_eq!(b.false_count, 34);
            assert!((b.true_ratio - 0.66).abs() < 0.01);
        }
        other => panic!("Expected Boolean stats for 'active', got {:?}", other),
    }

    // verified should be Boolean (Title case True/False)
    let verified_col = report
        .column_profiles
        .iter()
        .find(|c| c.name == "verified")
        .unwrap();
    assert_eq!(
        verified_col.data_type,
        DataType::Boolean,
        "Expected Boolean for 'verified' column"
    );
    match &verified_col.stats {
        ColumnStats::Boolean(b) => {
            assert_eq!(b.true_count, 50);
            assert_eq!(b.false_count, 50);
            assert!((b.true_ratio - 0.5).abs() < 0.01);
        }
        other => panic!("Expected Boolean stats for 'verified', got {:?}", other),
    }

    // label should be String
    let label_col = report
        .column_profiles
        .iter()
        .find(|c| c.name == "label")
        .unwrap();
    assert_eq!(label_col.data_type, DataType::String);
}

#[test]
fn test_incremental_engine_detects_boolean_columns() {
    let csv = create_boolean_csv();
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(csv.path())
        .expect("Incremental engine should succeed");

    let active_col = report
        .column_profiles
        .iter()
        .find(|c| c.name == "active")
        .unwrap();
    assert_eq!(
        active_col.data_type,
        DataType::Boolean,
        "Incremental engine should detect boolean"
    );
    match &active_col.stats {
        ColumnStats::Boolean(b) => {
            assert_eq!(b.true_count + b.false_count, 100);
        }
        other => panic!("Expected Boolean stats, got {:?}", other),
    }
}

#[test]
fn test_columnar_engine_detects_boolean_columns() {
    let csv = create_boolean_csv();
    let report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(csv.path())
        .expect("Columnar engine should succeed");

    let active_col = report
        .column_profiles
        .iter()
        .find(|c| c.name == "active")
        .unwrap();
    assert_eq!(
        active_col.data_type,
        DataType::Boolean,
        "Columnar engine should detect boolean"
    );
    match &active_col.stats {
        ColumnStats::Boolean(b) => {
            assert_eq!(b.true_count + b.false_count, 100);
        }
        other => panic!("Expected Boolean stats, got {:?}", other),
    }
}

#[test]
fn test_boolean_yes_no_detection() {
    let mut f = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(f, "subscribed").unwrap();
    for i in 0..50 {
        writeln!(f, "{}", if i % 2 == 0 { "yes" } else { "no" }).unwrap();
    }
    f.flush().unwrap();

    let report = analyze_csv_file(f.path(), &CsvParserConfig::default())
        .expect("CSV analysis should succeed");

    let col = &report.column_profiles[0];
    assert_eq!(
        col.data_type,
        DataType::Boolean,
        "yes/no values should be detected as Boolean"
    );
    match &col.stats {
        ColumnStats::Boolean(b) => {
            assert_eq!(b.true_count, 25, "yes values should count as true");
            assert_eq!(b.false_count, 25, "no values should count as false");
            assert!(
                (b.true_ratio - 0.5).abs() < 1e-9,
                "expected true_ratio to be 0.5, got {}",
                b.true_ratio
            );
        }
        other => panic!("Expected Boolean stats, got {:?}", other),
    }
}

#[test]
fn test_boolean_json_output_includes_stats() {
    let csv = create_boolean_csv();
    let report = analyze_csv_file(csv.path(), &CsvParserConfig::default())
        .expect("CSV analysis should succeed");

    let json = serde_json::to_string(&report).expect("JSON serialization should succeed");
    let value: serde_json::Value =
        serde_json::from_str(&json).expect("JSON deserialization should succeed");

    let column_profiles = value["column_profiles"]
        .as_array()
        .expect("column_profiles should be an array");
    let active_col = column_profiles
        .iter()
        .find(|col| col["name"].as_str() == Some("active"))
        .expect("JSON output should include the active column");
    // ColumnStats is an externally-tagged serde enum: {"Boolean": {…}}
    let stats = active_col["stats"]["Boolean"]
        .as_object()
        .expect("active column should include Boolean stats object");

    assert!(
        stats.contains_key("true_count"),
        "active column stats should include true_count"
    );
    assert!(
        stats.contains_key("false_count"),
        "active column stats should include false_count"
    );
    assert!(
        stats.contains_key("true_ratio"),
        "active column stats should include true_ratio"
    );
    assert_eq!(
        stats["true_count"].as_u64(),
        Some(66),
        "active column true_count should match the generated data"
    );
    assert_eq!(
        stats["false_count"].as_u64(),
        Some(34),
        "active column false_count should match the generated data"
    );
}
