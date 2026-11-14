//! A fact the report does not record is not a clean result (#1306, #1240).
//!
//! The findings rules read how the source was read: whether the scan reached
//! the end, sampled, counted ragged rows, or ended inside a quoted field. A
//! report that does not record one of these must list the rule as not
//! evaluated, so a gate that forbids the finding is inconclusive instead of
//! passing. A fact that does not apply to the source, such as a quote check
//! on Parquet, still passes, and that is decided from the source format.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

use dataprof::{
    CheckStatus, FindingCode, FindingPolicy, FindingsInput, NotEvaluatedReason, ProfileReport,
    Profiler, QualityPolicy, StopCondition, Verdict,
};
use serde_json::Value;

fn file(suffix: &str, contents: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents).unwrap();
    file.flush().unwrap();
    file
}

fn parquet() -> tempfile::NamedTempFile {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
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

fn document(report: &ProfileReport) -> Value {
    serde_json::to_value(report).unwrap()
}

fn load(document: &Value) -> ProfileReport {
    serde_json::from_value(document.clone()).unwrap()
}

/// The report's document with `execution.unterminated_quote` set, or removed
/// for `None`.
fn with_quote(report: &ProfileReport, value: Option<bool>) -> ProfileReport {
    let mut document = document(report);
    let execution = document["execution"].as_object_mut().unwrap();
    match value {
        Some(value) => execution.insert("unterminated_quote".into(), Value::Bool(value)),
        None => execution.remove("unterminated_quote"),
    };
    load(&document)
}

fn forbid_quote(report: &ProfileReport) -> Verdict {
    QualityPolicy::new()
        .forbid_findings([FindingCode::UnterminatedQuote])
        .evaluate(report)
        .unwrap()
        .verdict
}

fn quote_unrecorded(report: &ProfileReport) -> bool {
    report.findings().not_evaluated.iter().any(|rule| {
        rule.code == FindingCode::UnterminatedQuote && rule.reason == NotEvaluatedReason::Unrecorded
    })
}

#[test]
fn an_unrecorded_quote_check_is_inconclusive_on_csv_and_passes_where_it_does_not_apply() {
    let csv = file(".csv", b"id\n1\n");
    let jsonl = file(".jsonl", b"{\"id\": 1}\n");
    let parquet = parquet();
    let mut wrong = Vec::new();

    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    for (value, expected) in [
        (Some(true), Verdict::Fail),
        (Some(false), Verdict::Pass),
        (None, Verdict::Inconclusive),
    ] {
        let got = forbid_quote(&with_quote(&report, value));
        if got != expected {
            wrong.push(format!("csv {value:?}: {got:?}, expected {expected:?}"));
        }
    }

    // A source with no quoted fields: nothing was checked, and nothing could
    // be. Profiled as it is, and read back from its saved document.
    for (label, path) in [("jsonl", jsonl.path()), ("parquet", parquet.path())] {
        let report = Profiler::new().analyze_file(path).unwrap();
        assert_eq!(report.execution.unterminated_quote, None, "{label}");
        for (form, report) in [
            ("profiled", report.clone()),
            ("loaded", load(&document(&report))),
        ] {
            let got = forbid_quote(&report);
            if got != Verdict::Pass || quote_unrecorded(&report) {
                wrong.push(format!("{label} {form}: {got:?}"));
            }
        }
    }

    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_csv_scan_stopped_before_the_end_does_not_pass_a_quote_check() {
    // The quote is opened on the last record, past the cap.
    let csv = file(".csv", b"id,text\n1,a\n2,b\n3,\"open\n4,x\n");
    let report = Profiler::new()
        .stop_when(StopCondition::MaxRows(1))
        .analyze_file(csv.path())
        .unwrap();
    assert_eq!(report.execution.unterminated_quote, None);
    assert!(quote_unrecorded(&report));
    assert_eq!(forbid_quote(&report), Verdict::Inconclusive);
}

#[test]
fn a_stream_that_records_no_format_cannot_rule_the_quote_check_out() {
    // A stream report written before 0.14 records no format (#1042), so its
    // missing quote state may be a CSV one that was never checked.
    let csv = file(".csv", b"id\n1\n");
    let mut document = document(&Profiler::new().analyze_file(csv.path()).unwrap());
    document["data_source"] = serde_json::json!({
        "type": "stream",
        "topic": "orders",
        "batch_id": "0",
    });
    document["execution"]
        .as_object_mut()
        .unwrap()
        .remove("unterminated_quote");
    let report = load(&document);
    assert!(quote_unrecorded(&report));
    assert_eq!(forbid_quote(&report), Verdict::Inconclusive);
}

/// What `partial_scan` says for the facts a binding reads from a flat summary
/// (#1240): `finding:<reason>`, `unrecorded`, or `clean`.
fn partial_scan(source_exhausted: Value, sampling_applied: Value) -> String {
    let facts = serde_json::json!({
        "rows_processed": 4,
        "source_exhausted": source_exhausted,
        "truncated": false,
        "sampling_applied": sampling_applied,
        "ragged_row_count": 0,
        "unterminated_quote": null,
        "csv_source": false,
        "error_count": 0,
        "quality_status": "not_requested",
        "quality": null,
        "columns": [],
    });
    let input = FindingsInput::from_json(&facts.to_string()).unwrap();
    let result = FindingPolicy::new().evaluate_input(&input).unwrap();
    if let Some(finding) = result
        .findings
        .iter()
        .find(|finding| finding.code == FindingCode::PartialScan)
    {
        return format!("finding:{:?}", finding.evidence["reason"]);
    }
    if result
        .not_evaluated
        .iter()
        .any(|rule| rule.code == FindingCode::PartialScan)
    {
        return "unrecorded".to_string();
    }
    "clean".to_string()
}

#[test]
fn a_summary_that_omits_how_the_source_was_read_does_not_decide_partial_scan() {
    let reason = |text: &str| format!("finding:Text({text:?})");
    let cases = [
        (Value::Null, Value::Bool(false), "unrecorded".to_string()),
        (Value::Bool(true), Value::Null, "unrecorded".to_string()),
        (Value::Null, Value::Null, "unrecorded".to_string()),
        (Value::Bool(true), Value::Bool(false), "clean".to_string()),
        (Value::Bool(false), Value::Null, reason("truncated")),
        (Value::Null, Value::Bool(true), reason("sampled")),
    ];
    let wrong: Vec<_> = cases
        .into_iter()
        .filter_map(|(exhausted, sampled, expected)| {
            let got = partial_scan(exhausted.clone(), sampled.clone());
            (got != expected).then(|| format!("{exhausted} {sampled}: {got}, expected {expected}"))
        })
        .collect();
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Every key path of `value`, objects and array elements alike.
fn key_paths(value: &Value, prefix: &mut Vec<Value>, out: &mut Vec<Vec<Value>>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                prefix.push(Value::String(key.clone()));
                out.push(prefix.clone());
                key_paths(child, prefix, out);
                prefix.pop();
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                prefix.push(Value::from(index));
                key_paths(child, prefix, out);
                prefix.pop();
            }
        }
        _ => {}
    }
}

fn without(document: &Value, path: &[Value]) -> Value {
    let mut document = document.clone();
    let (last, parents) = path.split_last().unwrap();
    let mut node = &mut document;
    for step in parents {
        node = match step {
            Value::String(key) => &mut node[key.as_str()],
            index => &mut node[index.as_u64().unwrap() as usize],
        };
    }
    node.as_object_mut().unwrap().remove(last.as_str().unwrap());
    document
}

/// Every check's status, by what it checks.
fn statuses(report: &ProfileReport) -> BTreeMap<String, CheckStatus> {
    QualityPolicy::new()
        .forbid_findings(FindingCode::ALL)
        .max_duplicate_rows(0)
        .max_null_percentage_any(0.0)
        .min_rows(100)
        .min_quality_score(99.0)
        .evaluate(report)
        .unwrap()
        .checks
        .into_iter()
        .map(|check| {
            let key = format!("{:?} {:?} {:?}", check.code, check.column, check.finding);
            (key, check.status)
        })
        .collect()
}

#[test]
fn deleting_an_optional_field_never_turns_a_forbidden_finding_into_a_pass() {
    // A CSV that holds an unterminated quote, duplicate rows and nulls, and
    // the canonical report, which records no quote state at all.
    let csv = file(".csv", b"id,text\n1,a\n1,a\n2,\n3,\"open\n4,x\n");
    let profiled = document(&Profiler::new().analyze_file(csv.path()).unwrap());
    let canonical: Value =
        serde_json::from_str(include_str!("fixtures/canonical_report.json")).unwrap();
    let mut moved = Vec::new();
    let mut deleted = 0;

    for (label, document) in [("profiled", profiled), ("canonical", canonical)] {
        let before = statuses(&load(&document));
        let mut paths = Vec::new();
        key_paths(&document, &mut Vec::new(), &mut paths);
        for path in paths {
            let changed = without(&document, &path);
            // Only optional fields: a required one makes the document unreadable.
            let Ok(report) = serde_json::from_value::<ProfileReport>(changed) else {
                continue;
            };
            deleted += 1;
            for (key, status) in statuses(&report) {
                let was = before.get(&key);
                if status == CheckStatus::Passed
                    && was.is_some_and(|was| *was != CheckStatus::Passed)
                {
                    moved.push(format!("{label} without {path:?}: {key} was {was:?}"));
                }
            }
        }
    }

    assert!(deleted > 20, "only {deleted} optional fields were deleted");
    assert!(moved.is_empty(), "{moved:#?}");
}

#[test]
fn the_profiled_csv_fails_on_its_quote_and_the_deleted_field_is_inconclusive() {
    // The guard above only means something if the quote check starts out
    // failing on the profiled CSV.
    let csv = file(".csv", b"id,text\n1,a\n1,a\n2,\n3,\"open\n4,x\n");
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    assert_eq!(report.execution.unterminated_quote, Some(true));
    assert_eq!(forbid_quote(&report), Verdict::Fail);
    assert_eq!(
        forbid_quote(&with_quote(&report, None)),
        Verdict::Inconclusive
    );
}
