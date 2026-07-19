use std::io::Write;

use dataprof::{ColumnStats, DataProfilerError, DataType, EngineType, MetricPack, Profiler};
use dataprof::{SamplingStrategy, StopCondition};
use tempfile::NamedTempFile;

fn write_csv(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    write!(file, "{content}").unwrap();
    file.flush().unwrap();
    file
}

/// Assert a result is an `InvalidSemanticHint` error whose message contains each
/// of `needles`.
#[track_caller]
fn assert_hint_error<T>(result: Result<T, DataProfilerError>, needles: &[&str]) {
    match result {
        Err(DataProfilerError::InvalidSemanticHint {
            message,
            suggestion,
        }) => {
            let text = format!("{message}\n{suggestion}");
            for needle in needles {
                assert!(
                    text.contains(needle),
                    "expected hint error to mention {needle:?}, got: {text}"
                );
            }
        }
        Err(other) => panic!("expected InvalidSemanticHint, got a different error: {other}"),
        Ok(_) => panic!("expected InvalidSemanticHint, but profiling succeeded"),
    }
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
fn inferred_date_columns_are_assessed_without_a_hint() {
    let csv = write_csv("observed_on,value\n2020-01-01,1\n2021-01-01,2\n2022-01-01,3\n");

    let without_hint = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(csv.path())
        .expect("profile should succeed");
    assert!(
        without_hint
            .quality
            .as_ref()
            .expect("quality")
            .metrics
            .timeliness_score()
            .is_some()
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

#[test]
fn invalid_calendar_date_is_visible_and_lowers_timeliness() {
    let csv = write_csv(
        "observed_on\n2024-01-01\n2024-02-02\n2024-03-03\n2024-04-04\n2024-05-05\n2024-06-06\n2024-07-07\n2024-08-08\n2024-13-45\n",
    );

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(csv.path())
        .expect("mostly valid date column should profile");
    let column = report
        .column_profiles
        .iter()
        .find(|column| column.name == "observed_on")
        .expect("date column");
    assert_eq!(column.data_type, DataType::Date);
    assert_eq!(column.invalid_count, Some(1));

    let metrics = &report.quality.as_ref().expect("quality").metrics;
    let timeliness = metrics.timeliness.as_ref().expect("timeliness metrics");
    assert_eq!(timeliness.date_values_checked, 9);
    assert_eq!(timeliness.invalid_date_values, 1);
    assert!(metrics.timeliness_score().expect("timeliness score") < 100.0);
}

#[test]
fn date_invalid_count_covers_values_beyond_stream_reservoir() {
    let mut csv = String::from("observed_on\n2024-13-45\n");
    for _ in 0..10_050 {
        csv.push_str("2024-01-01\n");
    }
    let csv = write_csv(&csv);

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(csv.path())
        .expect("date stream should profile");
    let column = report
        .column_profiles
        .iter()
        .find(|column| column.name == "observed_on")
        .expect("date column");

    assert_eq!(column.data_type, DataType::Date);
    assert_eq!(column.invalid_count, Some(1));
}

// ---------------------------------------------------------------------------
// Hint validation: unknown column names must be loud, not silently dropped.
// ---------------------------------------------------------------------------

#[test]
fn unknown_positive_hint_name_is_rejected() {
    let csv = write_csv("pressure\n101325\n100900\n");
    let result = Profiler::new()
        .engine(EngineType::Incremental)
        .positive_columns(vec!["presure".to_string()])
        .analyze_file(csv.path());
    // Names the typo, the field it came from, and the columns that do exist.
    assert_hint_error(result, &["'presure'", "positive_columns", "'pressure'"]);
}

#[test]
fn unknown_temporal_hint_name_is_rejected() {
    let csv = write_csv("observed_on\n2020-01-01\n2021-01-01\n");
    let result = Profiler::new()
        .engine(EngineType::Incremental)
        .temporal_columns(vec!["not_a_column".to_string()])
        .analyze_file(csv.path());
    assert_hint_error(result, &["'not_a_column'", "temporal_columns"]);
}

#[test]
fn unknown_identifier_hint_name_is_rejected() {
    let csv = write_csv("invoice_number\nA\nB\nC\n");
    let result = Profiler::new()
        .engine(EngineType::Incremental)
        .identifier_columns(vec!["invoice_id".to_string()])
        .analyze_file(csv.path());
    assert_hint_error(result, &["'invoice_id'", "identifier_columns"]);
}

// ---------------------------------------------------------------------------
// Hint validation: a hint that names a real column but binds to nothing over
// the full data is an error, not a silent no-op.
// ---------------------------------------------------------------------------

#[test]
fn positive_hint_on_text_column_is_rejected_as_inert() {
    let csv = write_csv("name\nAlice\nBob\nCharlie\n");
    let result = Profiler::new()
        .engine(EngineType::Incremental)
        .positive_columns(vec!["name".to_string()])
        .analyze_file(csv.path());
    assert_hint_error(result, &["'name'", "positive_columns", "0 of 3"]);
}

#[test]
fn temporal_hint_on_non_date_column_is_rejected_as_inert() {
    let csv = write_csv("name\nAlice\nBob\nCharlie\n");
    let result = Profiler::new()
        .engine(EngineType::Incremental)
        .temporal_columns(vec!["name".to_string()])
        .analyze_file(csv.path());
    assert_hint_error(result, &["'name'", "temporal_columns"]);
}

#[test]
fn temporal_hint_binds_on_mixed_column_and_records_evidence() {
    // Some values parse as dates, some do not: the supported mixed-column case
    // must bind (not error) and record how much it matched.
    let csv = write_csv("event,note\n2020-01-01,a\nnot-a-date,b\n2022-06-15,c\n");
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .temporal_columns(vec!["event".to_string()])
        .analyze_file(csv.path())
        .expect("mixed temporal column should bind, not error");

    let binding = report
        .semantic_hint_bindings
        .iter()
        .find(|b| b.column == "event")
        .expect("temporal binding recorded");
    assert_eq!(binding.checked_values, 3);
    assert_eq!(binding.matched_values, 2);
    assert!(binding.exact);
    assert!(!binding.is_proven_inert());
}

#[test]
fn valid_positive_hint_records_binding_evidence() {
    let csv = write_csv("pressure\n101325\n-500\n100900\n");
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .positive_columns(vec!["pressure".to_string()])
        .analyze_file(csv.path())
        .expect("numeric positive hint should bind");

    let binding = report
        .semantic_hint_bindings
        .iter()
        .find(|b| b.column == "pressure")
        .expect("positive binding recorded");
    assert_eq!(binding.matched_values, 3);
    assert!(!binding.is_proven_inert());
}

#[test]
fn identifier_hint_on_text_column_is_never_inert() {
    // Identifier hints coerce the column's type, so they bind to any existing
    // column and are only rejected for an unknown name.
    let csv = write_csv("code\nX\nY\nZ\n");
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .identifier_columns(vec!["code".to_string()])
        .analyze_file(csv.path())
        .expect("identifier hint on an existing column should succeed");

    let binding = report
        .semantic_hint_bindings
        .iter()
        .find(|b| b.column == "code")
        .expect("identifier binding recorded");
    assert_eq!(binding.matched_values, binding.checked_values);
    assert!(!binding.is_proven_inert());
}

#[test]
fn multiple_unknown_hint_names_are_reported_together() {
    let csv = write_csv("pressure,observed_on\n101325,2020-01-01\n100900,2021-01-01\n");
    let result = Profiler::new()
        .engine(EngineType::Incremental)
        .positive_columns(vec!["nope_a".to_string()])
        .temporal_columns(vec!["nope_b".to_string()])
        .analyze_file(csv.path());
    assert_hint_error(result, &["'nope_a'", "'nope_b'"]);
}

#[test]
fn inert_positive_hint_is_rejected_beyond_stream_reservoir() {
    let mut csv = String::from("value\n");
    for _ in 0..10_050 {
        csv.push_str("not-a-number\n");
    }
    let csv = write_csv(&csv);

    let result = Profiler::new()
        .engine(EngineType::Incremental)
        .positive_columns(vec!["value".to_string()])
        .analyze_file(csv.path());

    assert_hint_error(result, &["'value'", "positive_columns", "0 of 10050"]);
}

#[test]
fn full_stream_match_prevents_false_inert_error() {
    let mut csv = String::from("value\n1\n");
    for _ in 0..10_050 {
        csv.push_str("not-a-number\n");
    }
    let csv = write_csv(&csv);

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .positive_columns(vec!["value".to_string()])
        .analyze_file(csv.path())
        .expect("one full-stream match must make the hint active");

    let binding = report
        .semantic_hint_bindings
        .iter()
        .find(|binding| binding.column == "value")
        .expect("positive binding");
    assert_eq!(binding.checked_values, 10_051);
    assert_eq!(binding.matched_values, 1);
    assert!(binding.exact);
}

#[test]
fn value_driven_hint_without_quality_is_rejected() {
    let csv = write_csv("value\n1\n2\n");
    let result = Profiler::new()
        .engine(EngineType::Incremental)
        .metric_packs(vec![MetricPack::Schema])
        .positive_columns(vec!["value".to_string()])
        .analyze_file(csv.path());

    assert_hint_error(result, &["positive_columns", "Quality metric pack"]);
}

#[test]
fn identifier_hint_remains_usable_without_quality() {
    let csv = write_csv("code\n1\n2\n");
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .metric_packs(vec![MetricPack::Schema])
        .identifier_columns(vec!["code".to_string()])
        .analyze_file(csv.path())
        .expect("identifier hints affect typing without quality");

    assert!(report.quality.is_none());
    assert_eq!(report.column_profiles[0].data_type, DataType::Identifier);
}

#[test]
fn zero_match_on_row_sample_is_not_treated_as_proven_inert() {
    let csv = write_csv("value\ntext\n1\nmore-text\n");
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .sampling(SamplingStrategy::Systematic { interval: 2 })
        .positive_columns(vec!["value".to_string()])
        .analyze_file(csv.path())
        .expect("unseen rows may contain a match");

    let binding = report
        .semantic_hint_bindings
        .iter()
        .find(|binding| binding.column == "value")
        .expect("positive binding");
    assert_eq!(binding.matched_values, 0);
    assert!(!binding.exact);
    assert!(!binding.is_proven_inert());
}

#[test]
fn zero_match_before_early_stop_is_not_treated_as_proven_inert() {
    let csv = write_csv("value\ntext\nmore-text\n1\n");
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .stop_when(StopCondition::MaxRows(2))
        .positive_columns(vec!["value".to_string()])
        .analyze_file(csv.path())
        .expect("unread rows may contain a match");

    let binding = report
        .semantic_hint_bindings
        .iter()
        .find(|binding| binding.column == "value")
        .expect("positive binding");
    assert_eq!(binding.matched_values, 0);
    assert!(!binding.exact);
    assert!(!binding.is_proven_inert());
}
