//! Start/end pairs declared next to `temporal_columns` (#942).
//!
//! Timeliness found pairs only from role words in the names, so
//! `valid_from`/`valid_until`, `checkin`/`checkout` or names in another
//! language were never compared. A declared pair is compared as given on
//! every engine and input format, and an unknown name in one is reported like
//! any other hint.
//!
//! `python/tests/test_timeliness_declared_pairs.py` is the Python twin.

use std::io::Write;

use dataprof::{DataProfilerError, EngineType, ProfileReport, Profiler, SemanticHintKind};
use tempfile::NamedTempFile;

const START: &str = "valid_from";
const END: &str = "valid_until";
/// The start of every row.
const STARTS: [&str; 3] = ["2024-03-15", "2024-04-20", "2024-05-10"];
/// The end, before the start on every row, so the pair counts three violations.
const ENDS: [&str; 3] = ["2024-02-01", "2024-03-01", "2024-03-20"];

fn text_fixture(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

fn csv_fixture() -> NamedTempFile {
    let rows = STARTS
        .iter()
        .zip(ENDS)
        .map(|(start, end)| format!("{start},{end}"));
    let body = std::iter::once(format!("{START},{END}"))
        .chain(rows)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    text_fixture(".csv", body)
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let days = |values: [&str; 3]| -> ArrayRef {
        cast(&StringArray::from(values.to_vec()), &DataType::Date32).unwrap()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new(START, DataType::Date32, false),
        Field::new(END, DataType::Date32, false),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![days(STARTS), days(ENDS)]).unwrap();

    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn declared(start: &str, end: &str) -> Vec<(String, String)> {
    vec![(start.to_string(), end.to_string())]
}

/// Every engine over the CSV, then every other format, each profiled with
/// `pairs` declared.
fn reports(pairs: &[(String, String)]) -> Vec<(String, ProfileReport)> {
    let csv = csv_fixture();
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .temporal_pairs(pairs.to_vec())
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }

    let records: Vec<String> = STARTS
        .iter()
        .zip(ENDS)
        .map(|(start, end)| format!(r#"{{"{START}":"{start}","{END}":"{end}"}}"#))
        .collect();
    let mut files = vec![
        (
            "json",
            text_fixture(".json", format!("[{}]", records.join(","))),
        ),
        ("jsonl", text_fixture(".jsonl", records.join("\n") + "\n")),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in files {
        let report = Profiler::new()
            .temporal_pairs(pairs.to_vec())
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

/// `(temporal_pairs_checked, temporal_violations)`.
fn ordering(report: &ProfileReport, label: &str) -> (usize, usize) {
    let timeliness = report
        .quality
        .as_ref()
        .and_then(|quality| quality.metrics.timeliness.as_ref())
        .unwrap_or_else(|| panic!("[{label}] timeliness assessed"));
    (
        timeliness.temporal_pairs_checked,
        timeliness.temporal_violations,
    )
}

/// Collects every wrong path before failing, so a regression names all the
/// paths it reaches rather than the first.
#[test]
fn a_declared_pair_is_compared_on_every_path() {
    let mut wrong = Vec::new();
    for (pairs, expected) in [(Vec::new(), (0, 0)), (declared(START, END), (3, 3))] {
        for (label, report) in reports(&pairs) {
            let found = ordering(&report, &label);
            if found != expected {
                wrong.push(format!(
                    "[{label}] declared {pairs:?}: (pairs_checked, violations) {found:?}, expected {expected:?}"
                ));
            }
            // Each pair column binds once, as the option the caller used.
            let bound: Vec<(&str, usize)> = report
                .semantic_hint_bindings
                .iter()
                .filter(|binding| binding.kind == SemanticHintKind::TemporalPair)
                .map(|binding| (binding.column.as_str(), binding.matched_values))
                .collect();
            let expected_bound = if pairs.is_empty() {
                vec![]
            } else {
                vec![(START, 3), (END, 3)]
            };
            if bound != expected_bound {
                wrong.push(format!(
                    "[{label}] declared {pairs:?}: bindings {bound:?}, expected {expected_bound:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[track_caller]
fn assert_hint_error(result: Result<ProfileReport, DataProfilerError>, needles: &[&str]) {
    match result {
        Err(DataProfilerError::InvalidSemanticHint {
            message,
            suggestion,
        }) => {
            let text = format!("{message}\n{suggestion}");
            for needle in needles {
                assert!(text.contains(needle), "expected {needle:?} in: {text}");
            }
        }
        Err(other) => panic!("expected InvalidSemanticHint, got: {other}"),
        Ok(_) => panic!("expected InvalidSemanticHint, but profiling succeeded"),
    }
}

#[test]
fn an_unknown_column_in_a_declared_pair_is_rejected() {
    let csv = csv_fixture();
    for engine in [EngineType::Incremental, EngineType::Columnar] {
        let result = Profiler::new()
            .engine(engine)
            .temporal_pairs(declared(START, "valid_untill"))
            .analyze_file(csv.path());
        assert_hint_error(
            result,
            &["'valid_untill'", "temporal_pairs", "'valid_until'"],
        );
    }
}

#[test]
fn a_pair_of_one_column_with_itself_is_rejected() {
    let csv = csv_fixture();
    let result = Profiler::new()
        .temporal_pairs(declared(START, START))
        .analyze_file(csv.path());
    assert_hint_error(result, &["'valid_from'", "temporal_pairs"]);
}

#[test]
fn a_pair_declared_in_both_orders_is_rejected() {
    let csv = csv_fixture();
    let mut pairs = declared(START, END);
    pairs.extend(declared(END, START));
    for engine in [EngineType::Incremental, EngineType::Columnar] {
        let result = Profiler::new()
            .engine(engine)
            .temporal_pairs(pairs.clone())
            .analyze_file(csv.path());
        assert_hint_error(result, &["'valid_from'", "'valid_until'", "temporal_pairs"]);
    }
}

#[test]
fn a_declared_pair_over_columns_without_dates_is_rejected() {
    // The clean check that never happened: two text columns used to profile
    // fine and report `temporal_pairs_checked == 0`.
    let body = "name,city\nAda,Turin\nLinus,Helsinki\nGrace,Arlington\n";
    let csv = text_fixture(".csv", body.to_string());
    let records = r#"{"name":"Ada","city":"Turin"}
{"name":"Linus","city":"Helsinki"}"#;
    let jsonl = text_fixture(".jsonl", records.to_string() + "\n");
    let mut runs = vec![
        ("Auto", Profiler::new().engine(EngineType::Auto), csv.path()),
        (
            "Incremental",
            Profiler::new().engine(EngineType::Incremental),
            csv.path(),
        ),
        (
            "Columnar",
            Profiler::new().engine(EngineType::Columnar),
            csv.path(),
        ),
    ];
    runs.push(("jsonl", Profiler::new(), jsonl.path()));
    for (label, profiler, path) in runs {
        let result = profiler
            .temporal_pairs(declared("name", "city"))
            .analyze_file(path);
        match result {
            Err(DataProfilerError::InvalidSemanticHint { message, .. }) => {
                for needle in ["'name' (temporal_pairs)", "'city' (temporal_pairs)"] {
                    assert!(
                        message.contains(needle),
                        "[{label}] {needle:?} in: {message}"
                    );
                }
            }
            other => panic!("[{label}] expected InvalidSemanticHint, got {other:?}"),
        }
    }
}
