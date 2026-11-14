//! The largest gap between a date column's days, and the gate rule
//! `max_date_gap` that reads it (#1300).
//!
//! A daily file can lose a whole period from the middle of its range while its
//! newest date, its schema and most of its rows stay normal. The report's
//! `largest_gap` is the longest stretch between consecutive distinct days, and
//! the rule fails a column whose gap is longer than allowed.
//!
//! `python/tests/test_date_gap_gate.py` is the Python twin; it also covers the
//! in-memory routes, the `check()` keyword, the command line and the policy
//! file. Parity of the gap past the retained sample is guarded with the other
//! date statistics in `tests/date_statistics.rs`.

use std::io::Write;

use dataprof::{
    CheckCode, CheckStatus, CsvParserConfig, EngineType, GateResult, NaiveDate, PolicyScope,
    ProfileReport, Profiler, QualityPolicy, StopCondition, Verdict, analyze_csv_file,
};
use serde_json::{Value, json};

fn day(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

/// Every day from `start`, `count` of them, leaving out those `skip` names.
fn daily(start: &str, count: u64, skip: impl Fn(NaiveDate) -> bool) -> Vec<String> {
    (0..count)
        .map(|offset| day(start) + chrono::Days::new(offset))
        .filter(|date| !skip(*date))
        .map(|date| date.to_string())
        .collect()
}

/// January to June 2024, contiguous.
fn contiguous() -> Vec<String> {
    daily("2024-01-01", 182, |_| false)
}

/// The same days with March removed: 29 February to 1 April is 32 days.
fn holed() -> Vec<String> {
    daily("2024-01-01", 182, |date| {
        date.format("%m").to_string() == "03"
    })
}

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn csv_text(dates: &[String]) -> String {
    let body: String = dates
        .iter()
        .enumerate()
        .map(|(i, date)| format!("{date},{i}\n"))
        .collect();
    format!("published_at,x\n{body}")
}

fn records(dates: &[String]) -> Vec<Value> {
    dates
        .iter()
        .enumerate()
        .map(|(i, date)| json!({ "published_at": date, "x": i }))
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_file(dates: &[String]) -> tempfile::NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{Date32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let epoch = day("1970-01-01");
    let schema = Arc::new(Schema::new(vec![
        Field::new("published_at", DataType::Date32, false),
        Field::new("x", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Date32Array::from(
                dates
                    .iter()
                    .map(|date| (day(date) - epoch).num_days() as i32)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..dates.len() as i64).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let file = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// The same dates through every file route.
fn routes(dates: &[String]) -> Vec<(String, ProfileReport)> {
    let csv = file(".csv", &csv_text(dates));
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
        reports.push((format!("csv {engine:?}"), report));
    }
    let records = records(dates);
    let jsonl: String = records.iter().map(|record| format!("{record}\n")).collect();
    let mut files = vec![
        ("json", file(".json", &Value::from(records).to_string())),
        ("jsonl", file(".jsonl", &jsonl)),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet date32", parquet_file(dates)));
    for (label, source) in files {
        let report = Profiler::new()
            .analyze_file(source.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    #[cfg(feature = "async-streaming")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = runtime
            .block_on(Profiler::new().profile_file(csv.path()))
            .expect("async CSV");
        reports.push(("async csv".to_string(), report));
    }
    reports
}

fn check_of(result: &GateResult) -> &dataprof::Check {
    result
        .checks
        .iter()
        .find(|check| check.code == CheckCode::MaxDateGap)
        .expect("a max_date_gap check")
}

/// `passed`, `failed`, or the reason the check was not evaluated.
fn status(result: &GateResult) -> String {
    match &check_of(result).status {
        CheckStatus::Passed => "passed".to_string(),
        CheckStatus::Failed => "failed".to_string(),
        CheckStatus::NotEvaluated(reason) => serde_json::to_value(reason).unwrap()["reason"]
            .as_str()
            .unwrap()
            .to_string(),
        _ => unreachable!(),
    }
}

fn observed(result: &GateResult) -> Value {
    serde_json::to_value(check_of(result)).unwrap()["observed"].clone()
}

fn evaluate(report: &ProfileReport, column: &str, days: u32) -> GateResult {
    QualityPolicy::new()
        .max_date_gap(column, days)
        .evaluate(report)
        .unwrap()
}

fn largest_gap(report: &ProfileReport) -> Value {
    let document = serde_json::to_value(report).unwrap();
    document["column_profiles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|column| column["name"] == "published_at")
        .map(|column| column["stats"]["DateTime"]["largest_gap"].clone())
        .unwrap_or(Value::Null)
}

/// The issue's acceptance: a contiguous daily file passes and the same file
/// with an interior month removed fails, with the same gap on every route.
#[test]
fn every_route_reports_the_same_gap_and_decision() {
    let cases = [
        (
            "contiguous",
            contiguous(),
            1,
            "passed",
            json!({ "days": 1, "from": "2024-01-01", "to": "2024-01-02" }),
        ),
        (
            "March removed",
            holed(),
            1,
            "failed",
            json!({ "days": 32, "from": "2024-02-29", "to": "2024-04-01" }),
        ),
        (
            "March removed, at the allowance",
            holed(),
            32,
            "passed",
            json!({ "days": 32, "from": "2024-02-29", "to": "2024-04-01" }),
        ),
        (
            "one day",
            vec!["2024-05-06".to_string(); 3],
            0,
            "passed",
            json!({ "days": 0, "from": "2024-05-06", "to": "2024-05-06" }),
        ),
    ];
    let mut wrong = Vec::new();
    let mut routes_run = 0;
    for (case, dates, days, expected, gap) in cases {
        for (route, report) in routes(&dates) {
            routes_run += 1;
            if largest_gap(&report) != gap {
                wrong.push(format!("{case} [{route}] gap: {}", largest_gap(&report)));
            }
            let result = evaluate(&report, "published_at", days);
            let got = (status(&result), observed(&result));
            if got != (expected.to_string(), gap["days"].clone()) {
                wrong.push(format!("{case} [{route}]: {got:?}"));
            }
        }
    }
    assert!(routes_run >= 4 * 6, "only {routes_run} route runs");
    assert!(wrong.is_empty(), "wrong decisions:\n{}", wrong.join("\n"));
}

/// The hole the ticket describes leaves every other rule passing.
#[test]
fn the_hole_fails_where_the_newest_date_and_rows_pass() {
    let csv = file(".csv", &csv_text(&holed()));
    let report = Profiler::new()
        .reference_date(day("2024-06-30"))
        .analyze_file(csv.path())
        .unwrap();
    let others = QualityPolicy::new()
        .min_rows(100)
        .max_date_age("published_at", 0)
        .max_null_count("published_at", 0)
        .evaluate(&report)
        .unwrap();
    assert_eq!(others.verdict, Verdict::Pass);
    let result = QualityPolicy::new()
        .min_rows(100)
        .max_date_age("published_at", 0)
        .max_date_gap("published_at", 1)
        .evaluate(&report)
        .unwrap();
    assert_eq!(result.verdict, Verdict::Fail);
    assert_eq!(
        check_of(&result).message,
        "this column's dates hold a gap of 32 days from 2024-02-29 to 2024-04-01, longer than \
         allowed"
    );
}

/// A hole past the retained sample is counted over every value, so a
/// complete scan decides it on every file route (#1297).
#[test]
fn a_hole_past_the_sample_fails_on_a_complete_scan() {
    // 30,000 days from 1950; a 100-day hole after the 20,000th.
    let start = day("1950-01-01");
    let hole_from = start + chrono::Days::new(19_999);
    let dates = daily("1950-01-01", 30_100, |date| {
        date > hole_from && date <= hole_from + chrono::Days::new(100)
    });
    let mut wrong = Vec::new();
    for (route, report) in routes(&dates) {
        let sampled = report.column_profiles.iter().any(|column| {
            column
                .type_homogeneity
                .as_ref()
                .is_some_and(|forms| forms.classified_count() < dates.len())
        });
        if !sampled {
            wrong.push(format!("[{route}] held every value in its sample"));
        }
        let result = evaluate(&report, "published_at", 1);
        let got = (status(&result), observed(&result));
        if got != ("failed".to_string(), json!(101)) {
            wrong.push(format!("[{route}]: {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "wrong decisions:\n{}", wrong.join("\n"));
}

/// The ticket's acceptance for a capped scan: inconclusive, whether the rows
/// read show a hole or not, since the rows left may fill it or open one.
#[test]
fn a_capped_scan_is_inconclusive_either_way() {
    let csv = file(".csv", &csv_text(&holed()));
    let incomplete = json!({ "coverage": "incomplete", "reason": "truncated" });
    // 59 rows end on 28 February, before the hole; 70 rows reach past it.
    let cases = [
        (59, PolicyScope::FullSource, "evidence_incomplete", 1),
        (70, PolicyScope::FullSource, "evidence_incomplete", 32),
        (59, PolicyScope::Observed, "passed", 1),
        (70, PolicyScope::Observed, "failed", 32),
    ];
    let mut wrong = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        for (cap, scope, expected, gap) in cases {
            let report = Profiler::new()
                .engine(engine)
                .stop_when(StopCondition::MaxRows(cap))
                .analyze_file(csv.path())
                .unwrap();
            let result = QualityPolicy::new()
                .max_date_gap("published_at", 1)
                .scope(scope)
                .evaluate(&report)
                .unwrap();
            let check = serde_json::to_value(check_of(&result)).unwrap();
            let got = (
                status(&result),
                observed(&result),
                check["evidence"].clone(),
            );
            if got != (expected.to_string(), json!(gap), incomplete.clone()) {
                wrong.push(format!("{engine:?} cap {cap} {scope:?}: {got:?}"));
            }
            if scope == PolicyScope::FullSource && result.verdict != Verdict::Inconclusive {
                wrong.push(format!(
                    "{engine:?} cap {cap}: verdict {:?}",
                    result.verdict
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "wrong decisions:\n{}", wrong.join("\n"));
}

/// A saved report decides as the run did.
#[test]
fn a_saved_report_decides_as_the_run_did() {
    for (route, report) in routes(&holed()) {
        let saved: ProfileReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
        assert_eq!(largest_gap(&saved), largest_gap(&report), "[{route}]");
        assert_eq!(
            status(&evaluate(&saved, "published_at", 1)),
            "failed",
            "[{route}]"
        );
    }
}

/// A column without a recorded gap is never a pass.
#[test]
fn a_column_without_a_gap_is_unevaluated() {
    let csv = file(".csv", &csv_text(&holed()));
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    for (column, expected, message) in [
        (
            "x",
            "not_assessed",
            "this column is not typed as a date, so it has no gap between dates",
        ),
        (
            "missing",
            "column_not_profiled",
            "this column has no profile in the report",
        ),
    ] {
        let result = evaluate(&report, column, 1);
        assert_eq!(result.verdict, Verdict::Inconclusive, "{column}");
        assert_eq!(status(&result), expected, "{column}");
        assert_eq!(check_of(&result).message, message, "{column}");
    }
    // A report written before the field.
    let mut document = serde_json::to_value(&report).unwrap();
    for column in document["column_profiles"].as_array_mut().unwrap() {
        if let Some(stats) = column["stats"]
            .get_mut("DateTime")
            .and_then(Value::as_object_mut)
        {
            stats.remove("largest_gap").expect("a recorded gap");
        }
    }
    let old: ProfileReport = serde_json::from_value(document).unwrap();
    let result = evaluate(&old, "published_at", 1);
    assert_eq!(status(&result), "not_assessed");
    assert!(
        check_of(&result)
            .message
            .starts_with("no largest gap was recorded for this column"),
        "{}",
        check_of(&result).message
    );
}
