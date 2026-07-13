use std::io::Write;

use dataprof::{ColumnStats, DataType, EngineType, Profiler};
use tempfile::NamedTempFile;

fn write_csv(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    write!(file, "{content}").unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn positive_columns_drive_negative_values_in_positive() {
    let csv = write_csv("pressure,temperature_delta\n101325,1\n-500,-2\n100900,3\n");

    let without_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(csv.path())
        .expect("profile should succeed");
    assert_eq!(
        without_hint
            .quality
            .as_ref()
            .unwrap()
            .metrics
            .negative_values_in_positive(),
        0
    );

    let with_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .positive_columns(vec!["pressure".to_string()])
        .analyze_file(csv.path())
        .expect("profile should succeed");
    assert_eq!(
        with_hint
            .quality
            .as_ref()
            .unwrap()
            .metrics
            .negative_values_in_positive(),
        1
    );
}

#[test]
fn identifier_columns_are_semantic_strings_and_excluded_from_outliers() {
    let csv = write_csv("order_id\n1\n2\n3\n10000\n");

    let without_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(csv.path())
        .expect("profile should succeed");
    let plain_id = without_hint
        .column_profiles
        .iter()
        .find(|col| col.name == "order_id")
        .unwrap();
    assert_eq!(plain_id.data_type, DataType::Integer);
    assert!(matches!(plain_id.stats, ColumnStats::Numeric(_)));
    assert!(
        without_hint
            .quality
            .as_ref()
            .unwrap()
            .metrics
            .outlier_ratio()
            > 0.0
    );

    let with_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .identifier_columns(vec!["order_id".to_string()])
        .analyze_file(csv.path())
        .expect("profile should succeed");
    let hinted_id = with_hint
        .column_profiles
        .iter()
        .find(|col| col.name == "order_id")
        .unwrap();
    assert_eq!(hinted_id.data_type, DataType::Identifier);
    assert!(matches!(hinted_id.stats, ColumnStats::Text(_)));
    assert_eq!(
        with_hint.quality.as_ref().unwrap().metrics.outlier_ratio(),
        0.0
    );
}

#[test]
fn identifier_columns_drive_key_uniqueness_without_name_guessing() {
    let csv = write_csv("invoice_number\nA\nB\nB\nC\n");

    let without_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(csv.path())
        .expect("profile should succeed");
    let without_uniqueness = without_hint
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.uniqueness.as_ref())
        .expect("uniqueness metrics");
    assert_eq!(without_uniqueness.key_column, None);

    let with_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .identifier_columns(vec!["invoice_number".to_string()])
        .analyze_file(csv.path())
        .expect("profile should succeed");
    let uniqueness = with_hint
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.uniqueness.as_ref())
        .expect("uniqueness metrics");
    assert_eq!(uniqueness.key_column.as_deref(), Some("invoice_number"));
    assert!((uniqueness.key_uniqueness - 75.0).abs() < 0.01);
}

#[test]
fn temporal_columns_gate_timeliness_scoring() {
    let csv = write_csv("observed_on,value\n2020-01-01,1\n2021-01-01,2\n2022-01-01,3\n");

    let without_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(csv.path())
        .expect("profile should succeed");
    assert_eq!(
        without_hint
            .quality
            .as_ref()
            .expect("quality")
            .metrics
            .timeliness_score(),
        None
    );

    let with_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .temporal_columns(vec!["observed_on".to_string()])
        .analyze_file(csv.path())
        .expect("profile should succeed");
    let metrics = &with_hint.quality.as_ref().expect("quality").metrics;
    assert!(metrics.timeliness_score().is_some());
    assert_eq!(
        metrics
            .timeliness
            .as_ref()
            .expect("timeliness metrics")
            .date_values_checked,
        3
    );
}
