//! The gate's per-column uniqueness rule, `unique_columns` (#904).
//!
//! `max_duplicate_rows` counts whole rows, so a reused business key passes it
//! whenever another column differs, and always when a loader adds a row id.
//! The rule reads each named column's exact distinct count instead.
//!
//! `python/tests/test_unique_columns_gate.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckCode, CheckStatus, CsvParserConfig, EngineType, GateResult, MetricValue, ProfileReport,
    Profiler, QualityPolicy, Verdict, analyze_column, analyze_csv_file,
};
use serde_json::json;

const ROWS: usize = 20;
/// Rows from this index reuse the keys of the first rows.
const REUSED_FROM: usize = 17;
const NULL_NOTES: [usize; 2] = [3, 7];

/// `(row_id, order_id, order_no, note, amount)` for row `i`.
fn row(i: usize) -> (usize, String, usize, Option<String>, usize) {
    let key = if i >= REUSED_FROM { i - REUSED_FROM } else { i };
    let note = (!NULL_NOTES.contains(&i)).then(|| format!("n{i}"));
    (i, format!("O{key}"), key + 100, note, i % 7)
}

/// Each column with the check status and observed repeats the rule must
/// reach on every route. `note` holds two nulls and no repeated value.
const EXPECTED: [(&str, &str, usize); 4] = [
    ("note", "passed", 0),
    ("order_id", "failed", ROWS - REUSED_FROM),
    ("order_no", "failed", ROWS - REUSED_FROM),
    ("row_id", "passed", 0),
];

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn csv_text() -> String {
    let mut text = String::from("row_id,order_id,order_no,note,amount\n");
    for i in 0..ROWS {
        let (id, key, no, note, amount) = row(i);
        text.push_str(&format!(
            "{id},{key},{no},{},{amount}\n",
            note.unwrap_or_default()
        ));
    }
    text
}

fn json_records() -> Vec<String> {
    (0..ROWS)
        .map(|i| {
            let (id, key, no, note, amount) = row(i);
            json!({"row_id": id, "order_id": key, "order_no": no, "note": note, "amount": amount})
                .to_string()
        })
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_file() -> tempfile::NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let rows: Vec<_> = (0..ROWS).map(row).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("row_id", DataType::Int64, false),
        Field::new("order_id", DataType::Utf8, false),
        Field::new("order_no", DataType::Int64, false),
        Field::new("note", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.0 as i64),
            )),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.1.clone()),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.2 as i64),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3.clone()).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.4 as i64),
            )),
        ],
    )
    .unwrap();
    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// Every engine over the CSV, every other file format, and the async routes.
fn reports() -> Vec<(String, ProfileReport)> {
    let csv = file(".csv", &csv_text());
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

    let mut files = vec![
        (
            "json",
            file(".json", &format!("[{}]", json_records().join(","))),
        ),
        ("jsonl", file(".jsonl", &json_records().join("\n"))),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_file()));
    for (label, file) in &files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }

    #[cfg(feature = "async-streaming")]
    {
        use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let bytes = BytesSource::new(
            bytes::Bytes::from(csv_text()),
            AsyncSourceInfo::new("pipe", FileFormat::Csv),
        );
        let stream = rt
            .block_on(Profiler::new().profile_stream(bytes))
            .expect("async stream");
        reports.push(("async stream".to_string(), stream));
        let async_file = rt
            .block_on(Profiler::new().profile_file(csv.path()))
            .expect("async file");
        reports.push(("async file".to_string(), async_file));
    }
    reports
}

fn status(status: &CheckStatus) -> String {
    match status {
        CheckStatus::Passed => "passed".to_string(),
        CheckStatus::Failed => "failed".to_string(),
        CheckStatus::NotEvaluated(reason) => serde_json::to_value(reason).unwrap()["reason"]
            .as_str()
            .unwrap()
            .to_string(),
        _ => unreachable!(),
    }
}

fn unique(columns: &[&str]) -> QualityPolicy {
    QualityPolicy::new().unique_columns(columns.iter().copied())
}

fn evaluate(policy: &QualityPolicy, report: &ProfileReport) -> GateResult {
    policy.evaluate(report).unwrap()
}

/// What each route and the document it writes decide, collected before
/// asserting so one run shows every wrong (route, column).
#[test]
fn a_reused_key_fails_on_every_route_while_whole_rows_stay_distinct() {
    let policy = unique(&["row_id", "order_id", "order_no", "note"]);
    let mut wrong = Vec::new();
    for (route, report) in reports() {
        let document: ProfileReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
        // The case the ticket is about: no whole row repeats.
        let rows = evaluate(&QualityPolicy::new().max_duplicate_rows(0), &report);
        if rows.verdict != Verdict::Pass {
            wrong.push(format!("{route}: max_duplicate_rows {:?}", rows.verdict));
        }
        for (form, report) in [("live", &report), ("document", &document)] {
            let result = evaluate(&policy, report);
            let checks: Vec<_> = result
                .checks
                .iter()
                .map(|check| {
                    (
                        check.code,
                        check.column.as_deref().unwrap_or_default(),
                        status(&check.status),
                        check.observed,
                    )
                })
                .collect();
            let expected: Vec<_> = EXPECTED
                .iter()
                .map(|(column, status, repeated)| {
                    (
                        CheckCode::UniqueColumn,
                        *column,
                        status.to_string(),
                        Some(MetricValue::Count(*repeated)),
                    )
                })
                .collect();
            if checks != expected {
                wrong.push(format!("{route}/{form}: {checks:?}"));
            }
            if result.verdict != Verdict::Fail {
                wrong.push(format!("{route}/{form}: verdict {:?}", result.verdict));
            }
        }
    }
    assert!(wrong.is_empty(), "wrong decisions:\n{}", wrong.join("\n"));
}

/// The database connectors build a column's profile through
/// `analyze_column`, so its distinct count must be exact and recorded so.
#[test]
fn column_analysis_counts_exactly_for_the_rule() {
    let values: Vec<String> = (0..ROWS).map(|i| row(i).1).collect();
    let profile = analyze_column("order_id", &values);
    assert_eq!(profile.unique_count, Some(REUSED_FROM));
    assert_eq!(profile.unique_count_is_approximate, Some(false));
}

/// A distinct count that is an estimate, or a document that does not say
/// whether it is exact, witnesses no repeat and rules none out.
#[test]
fn an_estimated_or_unrecorded_count_is_never_decided() {
    let csv = file(".csv", &csv_text());
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    let mut wrong = Vec::new();
    for (case, flag) in [
        ("estimated", json!(true)),
        ("unrecorded", serde_json::Value::Null),
    ] {
        for column in ["order_id", "row_id"] {
            let mut document = serde_json::to_value(&report).unwrap();
            let profile = document["column_profiles"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|profile| profile["name"] == column)
                .unwrap();
            if flag.is_null() {
                profile
                    .as_object_mut()
                    .unwrap()
                    .remove("unique_count_is_approximate");
            } else {
                profile["unique_count_is_approximate"] = flag.clone();
            }
            let loaded: ProfileReport = serde_json::from_value(document).unwrap();
            let result = evaluate(&unique(&[column]), &loaded);
            let check = &result.checks[0];
            let got = status(&check.status);
            if got != "approximate_count" || check.observed.is_some() {
                wrong.push(format!("{case}/{column}: {got}, {:?}", check.observed));
            }
        }
    }
    assert!(wrong.is_empty(), "wrong decisions:\n{}", wrong.join("\n"));
}

/// A nested column has values but no distinct count.
#[test]
fn a_column_without_a_distinct_count_is_not_assessed() {
    let records: Vec<String> = (0..5)
        .map(|i| json!({"id": i, "payload": {"k": i % 2}}).to_string())
        .collect();
    let jsonl = file(".jsonl", &records.join("\n"));
    let report = Profiler::new().analyze_file(jsonl.path()).unwrap();
    let payload = report
        .column_profiles
        .iter()
        .find(|column| column.name == "payload")
        .expect("payload profiled");
    assert_eq!(payload.unique_count, None, "payload has no distinct count");
    let result = evaluate(&unique(&["payload"]), &report);
    assert_eq!(status(&result.checks[0].status), "not_assessed");
    assert_eq!(result.verdict, Verdict::Inconclusive);
}

/// Every value null: nothing repeats, on a scan that read every row.
#[test]
fn a_column_of_nulls_has_no_repeat() {
    let csv = file(".csv", "id,empty\n1,\n2,\n3,\n");
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    let result = evaluate(&unique(&["empty"]), &report);
    assert_eq!(status(&result.checks[0].status), "passed");
    assert_eq!(result.checks[0].observed, Some(MetricValue::Count(0)));
}

/// The rule is a requirement of its own: naming a column is enough for a
/// policy, and the names are checked once each, in name order.
#[test]
fn named_columns_are_checked_once_in_name_order() {
    let csv = file(".csv", &csv_text());
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    let result = evaluate(&unique(&["row_id", "order_id", "row_id"]), &report);
    let columns: Vec<_> = result
        .checks
        .iter()
        .map(|check| check.column.as_deref().unwrap())
        .collect();
    assert_eq!(columns, ["order_id", "row_id"]);
}
