//! Ten-digit integers are not US phone numbers (#812).
//!
//! The `Phone (US)` regex matches any ten digits, and at 0.525 confidence
//! without a locale it cleared the 0.5 bar of the `sensitive_pattern` finding.
//! Order IDs and Unix epoch seconds were reported as personal contact data.
//! The pattern now validates the North American Numbering Plan, so those
//! columns fall below the bar on every engine and input path, while formatted
//! and bare valid numbers are still reported.
//!
//! `python/tests/test_us_phone_pattern.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CsvParserConfig, EngineType, FindingCode, ProfileReport, Profiler, analyze_column,
    analyze_csv_file,
};
use tempfile::NamedTempFile;

const ROWS: usize = 60;

/// Column name, value for row `i`, and whether it is a phone column.
type Column = (&'static str, fn(usize) -> String, bool);

fn columns() -> Vec<Column> {
    vec![
        ("order_id", |i| (2_000_000_000 + i).to_string(), false),
        (
            "created_epoch",
            |i| (1_705_312_200 + i * 86_400).to_string(),
            false,
        ),
        ("legacy_id", |i| (1_100_000_000 + i).to_string(), false),
        ("phone", |i| format!("(212) 555-{:04}", 100 + i % 100), true),
        (
            "phone_digits",
            |i| format!("312555{:04}", 100 + i % 100),
            true,
        ),
    ]
}

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    let columns = columns();
    let header: Vec<&str> = columns.iter().map(|(name, _, _)| *name).collect();
    writeln!(file, "{}", header.join(",")).unwrap();
    for i in 0..ROWS {
        let row: Vec<String> = columns
            .iter()
            .map(|(_, value, _)| format!("\"{}\"", value(i)))
            .collect();
        writeln!(file, "{}", row.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

fn jsonl_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".jsonl").unwrap();
    for i in 0..ROWS {
        let fields: Vec<String> = columns()
            .iter()
            .map(|(name, value, _)| format!(r#""{name}":"{}""#, value(i)))
            .collect();
        writeln!(file, "{{{}}}", fields.join(",")).unwrap();
    }
    file.flush().unwrap();
    file
}

fn reports() -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture();
    let mut reports = vec![(
        "standard".to_string(),
        analyze_csv_file(csv.path(), &CsvParserConfig::default()).expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }
    let jsonl = jsonl_fixture();
    reports.push((
        "jsonl".to_string(),
        Profiler::new().analyze_file(jsonl.path()).expect("jsonl"),
    ));
    reports
}

#[test]
fn only_phone_columns_are_reported_as_sensitive_on_every_path() {
    for (label, report) in reports() {
        let findings = report.findings();
        let sensitive: Vec<&str> = findings
            .findings
            .iter()
            .filter(|finding| finding.code == FindingCode::SensitivePattern)
            .filter_map(|finding| finding.column.as_deref())
            .collect();
        assert_eq!(sensitive, ["phone", "phone_digits"], "[{label}]");
    }
}

/// The database connectors and in-memory inputs detect patterns through
/// `analyze_column`.
#[test]
fn column_analysis_agrees() {
    for (name, value, is_phone) in columns() {
        let values: Vec<String> = (0..ROWS).map(value).collect();
        let profile = analyze_column(name, &values);
        let confidence = profile
            .patterns
            .as_ref()
            .expect("patterns detected")
            .iter()
            .find(|pattern| pattern.name == "Phone (US)")
            .map_or(0.0, |pattern| pattern.confidence);
        assert_eq!(confidence >= 0.5, is_phone, "{name}: {confidence}");
    }
}
