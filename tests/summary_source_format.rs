//! A flat summary records the format of a file, bytes or stream source, so a
//! summary baseline of a capped CSV or Parquet run shows its header listed
//! every column, as the canonical document does (#1028). A JSONL summary, and
//! one written before the field, still cover only the rows read.
//!
//! `python/tests/test_summary_source_format.py` is the Python twin; it also
//! covers bytes, an Arrow C stream and the `from_dict()` round trip.

use std::io::Write;
use std::sync::Arc;

use dataprof::{CheckCode, ProfileReport, Profiler, QualityPolicy, ReportSide, StopCondition};

const ORDERS_CSV: &str = "id,amount,note\n1,10,a\n2,20,b\n3,30,c\n";
const NO_NOTE_CSV: &str = "id,amount\n1,10\n2,20\n3,30\n";
const ORDERS_JSONL: &str = "{\"id\": 1, \"note\": \"a\"}\n{\"id\": 2, \"note\": \"b\"}\n";
const NO_NOTE_JSONL: &str = "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3, \"note\": \"a\"}\n";

fn file(suffix: &str, contents: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents).unwrap();
    file.flush().unwrap();
    file
}

fn parquet(names: &[&str]) -> tempfile::NamedTempFile {
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(
        names
            .iter()
            .map(|name| Field::new(*name, DataType::Int64, false))
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = names
        .iter()
        .map(|_| Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef)
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn capped() -> Profiler {
    Profiler::new().stop_when(StopCondition::MaxRows(1))
}

fn summary(report: &ProfileReport) -> serde_json::Value {
    serde_json::from_str(&report.summary_json().unwrap()).unwrap()
}

/// The outcome of `forbid_added_columns` for `note` against a baseline read
/// from `document`: `passed`, `failed`, or the reason and gap it was not
/// evaluated.
fn note_outcome(current: &ProfileReport, document: &serde_json::Value) -> String {
    let baseline = ReportSide::from_summary_json(&document.to_string()).unwrap();
    let result = QualityPolicy::new()
        .forbid_added_columns()
        .evaluate_against(current, &baseline)
        .unwrap();
    let check = result
        .checks
        .iter()
        .find(|check| {
            check.code == CheckCode::ForbidAddedColumn && check.column.as_deref() == Some("note")
        })
        .unwrap();
    let document = serde_json::to_value(check).unwrap();
    let text = |key: &str| document[key].as_str().unwrap_or_default().to_string();
    match text("status").as_str() {
        "not_evaluated" => format!("{}:{}", text("reason"), text("gap")),
        status => status.to_string(),
    }
}

/// (route, current report, capped baseline without `note`, expected
/// `source_type`, expected `source_format`, expected outcome)
type Case = (
    &'static str,
    ProfileReport,
    ProfileReport,
    &'static str,
    &'static str,
    &'static str,
);

fn sync_cases() -> Vec<Case> {
    let orders_csv = file(".csv", ORDERS_CSV.as_bytes());
    let no_note_csv = file(".csv", NO_NOTE_CSV.as_bytes());
    let orders_parquet = parquet(&["id", "amount", "note"]);
    let no_note_parquet = parquet(&["id", "amount"]);
    let orders_jsonl = file(".jsonl", ORDERS_JSONL.as_bytes());
    let no_note_jsonl = file(".jsonl", NO_NOTE_JSONL.as_bytes());
    let run = |profiler: Profiler, file: &tempfile::NamedTempFile| {
        profiler.analyze_file(file.path()).unwrap()
    };
    vec![
        (
            "csv file",
            run(Profiler::new(), &orders_csv),
            run(capped(), &no_note_csv),
            "file",
            "csv",
            "failed",
        ),
        (
            "parquet file",
            run(Profiler::new(), &orders_parquet),
            run(capped(), &no_note_parquet),
            "file",
            "parquet",
            "failed",
        ),
        (
            "jsonl file",
            run(Profiler::new(), &orders_jsonl),
            run(capped(), &no_note_jsonl),
            "file",
            "jsonl",
            "evidence_incomplete:truncated",
        ),
    ]
}

#[cfg(feature = "async-streaming")]
fn async_cases() -> Vec<Case> {
    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let orders = file(".csv", ORDERS_CSV.as_bytes());
    let no_note = file(".csv", NO_NOTE_CSV.as_bytes());
    let buffer = |contents: &'static str, profiler: Profiler| {
        rt.block_on(profiler.profile_stream(BytesSource::new(
            bytes::Bytes::from_static(contents.as_bytes()),
            AsyncSourceInfo::new("orders", FileFormat::Csv),
        )))
        .unwrap()
    };
    vec![
        (
            "async csv file",
            rt.block_on(Profiler::new().profile_file(orders.path()))
                .unwrap(),
            rt.block_on(capped().profile_file(no_note.path())).unwrap(),
            "stream",
            "csv",
            "failed",
        ),
        (
            "async csv buffer",
            buffer(ORDERS_CSV, Profiler::new()),
            buffer(NO_NOTE_CSV, capped()),
            "bytes",
            "csv",
            "failed",
        ),
    ]
}

#[cfg(not(feature = "async-streaming"))]
fn async_cases() -> Vec<Case> {
    Vec::new()
}

#[test]
fn a_capped_summary_baseline_declares_its_columns_where_its_format_does() {
    let mut wrong = Vec::new();
    for (route, current, baseline, source_type, format, expected) in
        sync_cases().into_iter().chain(async_cases())
    {
        if baseline.execution.source_exhausted {
            wrong.push(format!("{route}: the cap did not stop the scan"));
        }
        let document = summary(&baseline);
        if document["source_type"] != source_type {
            wrong.push(format!("{route}: source_type {}", document["source_type"]));
        }
        if document["source_format"] != format {
            wrong.push(format!(
                "{route}: source_format {}",
                document["source_format"]
            ));
        }
        let outcome = note_outcome(&current, &document);
        if outcome != expected {
            wrong.push(format!("{route}: {outcome}, expected {expected}"));
        }
        // A summary written before the field covers only the rows read.
        let mut legacy = document.clone();
        legacy.as_object_mut().unwrap().remove("source_format");
        let outcome = note_outcome(&current, &legacy);
        if outcome != "evidence_incomplete:truncated" {
            wrong.push(format!("{route} without source_format: {outcome}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_format_that_declares_nothing_for_its_kind_stays_conservative() {
    let orders = file(".csv", ORDERS_CSV.as_bytes());
    let no_note = file(".csv", NO_NOTE_CSV.as_bytes());
    let current = Profiler::new().analyze_file(orders.path()).unwrap();
    let baseline = summary(&capped().analyze_file(no_note.path()).unwrap());
    let mut wrong = Vec::new();
    for (source_type, format) in [
        // Only the Arrow C stream route records `arrow`.
        ("file", serde_json::json!("arrow")),
        ("bytes", serde_json::json!("arrow")),
        // A format this build does not know declares nothing.
        ("file", serde_json::json!("xlsx")),
        ("stream", serde_json::json!("xlsx")),
        ("stream", serde_json::json!("jsonl")),
        ("stream", serde_json::Value::Null),
        // A kind this build does not know declares nothing either.
        ("lakehouse", serde_json::json!("csv")),
    ] {
        let mut document = baseline.clone();
        document["source_type"] = serde_json::json!(source_type);
        document["source_format"] = format.clone();
        let outcome = note_outcome(&current, &document);
        if outcome != "evidence_incomplete:truncated" {
            wrong.push(format!("{source_type} {format}: {outcome}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");

    let mut malformed = baseline;
    malformed["source_format"] = serde_json::json!(5);
    assert!(ReportSide::from_summary_json(&malformed.to_string()).is_err());
}

#[test]
fn a_query_or_dataframe_summary_records_no_format() {
    // Built from a canonical document, so no database or DataFrame is needed.
    let orders = file(".csv", ORDERS_CSV.as_bytes());
    let mut document =
        serde_json::to_value(Profiler::new().analyze_file(orders.path()).unwrap()).unwrap();
    document["data_source"] = serde_json::json!({
        "type": "dataframe",
        "name": "orders",
        "source_library": "pandas",
        "row_count": 3,
        "column_count": 3,
    });
    let report: ProfileReport = serde_json::from_value(document).unwrap();
    let summary = summary(&report);
    assert_eq!(summary["source_type"], "dataframe");
    assert!(summary.get("source_format").is_none(), "{summary}");
}
