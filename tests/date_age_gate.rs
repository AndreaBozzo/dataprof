//! The gate's freshness ceiling, `max_date_age` (#1290).
//!
//! A scheduled extract whose source stopped changing keeps producing the same
//! rows, and every other rule keeps passing them. The rule reads each date
//! column's newest date (`max_datetime`) against the reference date the
//! report's timeliness clock recorded.
//!
//! `python/tests/test_date_age_gate.py` is the Python twin; it also covers the
//! in-memory routes, the `check()` keyword, the command line and the policy
//! file.

use std::io::Write;

use dataprof::{
    CheckCode, CheckStatus, EngineType, GateResult, MetricPack, NaiveDate, PolicyScope,
    ProfileReport, Profiler, QualityPolicy, StopCondition, Verdict,
};
use serde_json::{Value, json};

const DATES: [&str; 3] = ["2025-12-15", "2025-12-16", "2025-12-17"];

fn day(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
}

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn csv_text(dates: &[&str]) -> String {
    let body: String = dates
        .iter()
        .enumerate()
        .map(|(i, date)| format!("{date},{i}\n"))
        .collect();
    format!("published_at,x\n{body}")
}

fn records(dates: &[&str]) -> Vec<Value> {
    dates
        .iter()
        .enumerate()
        .map(|(i, date)| json!({ "published_at": date, "x": i }))
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_file(dates: &[&str]) -> tempfile::NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{Date32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
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

/// The same dates through every file route, measured against `reference`.
fn routes(dates: &[&str], reference: &str) -> Vec<(String, ProfileReport)> {
    let profiler = || Profiler::new().reference_date(day(reference));
    let csv = file(".csv", &csv_text(dates));
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler()
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
        let report = profiler()
            .analyze_file(source.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    reports
}

fn check_of(result: &GateResult) -> &dataprof::Check {
    result
        .checks
        .iter()
        .find(|check| check.code == CheckCode::MaxDateAge)
        .expect("a max_date_age check")
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
        .max_date_age(column, days)
        .evaluate(report)
        .unwrap()
}

/// The issue's case and its neighbours, on every route: the age is counted
/// in calendar days from the newest date to the recorded reference date, a
/// date at the allowance passes, and a date after the reference is fresh.
#[test]
fn every_route_counts_the_same_age_from_the_recorded_reference_date() {
    let cases = [
        ("287 days stale", "2026-09-30", 3, "failed", json!(287)),
        (
            "one day past the allowance",
            "2025-12-21",
            3,
            "failed",
            json!(4),
        ),
        ("at the allowance", "2025-12-20", 3, "passed", json!(3)),
        (
            "same day, zero allowance",
            "2025-12-17",
            0,
            "passed",
            json!(0),
        ),
        (
            "newest date in the future",
            "2025-12-10",
            0,
            "passed",
            json!(-7),
        ),
    ];
    let mut wrong = Vec::new();
    for (case, reference, days, expected, age) in cases {
        for (route, report) in routes(&DATES, reference) {
            let result = evaluate(&report, "published_at", days);
            let got = (status(&result), observed(&result));
            if got != (expected.to_string(), age.clone()) {
                wrong.push(format!("{case} [{route}]: {got:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "wrong decisions:\n{}", wrong.join("\n"));
}

/// A saved report decides as the run did: the reference date comes from the
/// recorded clock, not from the day the policy is evaluated.
#[test]
fn a_saved_report_decides_as_the_run_did() {
    for (route, report) in routes(&DATES, "2026-09-30") {
        let document: ProfileReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
        let live = evaluate(&report, "published_at", 3);
        let saved = evaluate(&document, "published_at", 3);
        assert_eq!(
            serde_json::to_value(&live).unwrap()["checks"],
            serde_json::to_value(&saved).unwrap()["checks"],
            "[{route}]"
        );
    }
}

/// 30,000 dates, past the retained sample the date statistics read.
fn large(newest: &str) -> String {
    let start = day("2020-01-01");
    let mut dates: Vec<String> = (0..29_999)
        .map(|i| (start + chrono::Duration::days(i % 1000)).to_string())
        .collect();
    dates.push(newest.to_string());
    let refs: Vec<&str> = dates.iter().map(String::as_str).collect();
    csv_text(&refs)
}

/// A newest date can only be overtaken by a newer one. So a fresh date read
/// in the sample passes, and a stale one over a sample is a decision only
/// under the observed scope.
#[test]
fn a_sampled_newest_date_decides_only_what_unread_values_cannot_reverse() {
    let reference = day("2026-09-30");
    let stale = file(".csv", &large("2020-06-01"));
    let fresh_values: String = (0..30_000).map(|i| format!("2026-09-29,{i}\n")).collect();
    let fresh = file(".csv", &format!("published_at,x\n{fresh_values}"));
    let mut wrong = Vec::new();
    for engine in [EngineType::Incremental, EngineType::Columnar] {
        let profile = |source: &tempfile::NamedTempFile| {
            Profiler::new()
                .engine(engine)
                .reference_date(reference)
                .analyze_file(source.path())
                .unwrap()
        };
        let stale_report = profile(&stale);
        let fresh_report = profile(&fresh);
        for (case, report, scope, expected) in [
            (
                "stale, full source",
                &stale_report,
                PolicyScope::FullSource,
                "evidence_incomplete",
            ),
            (
                "stale, observed",
                &stale_report,
                PolicyScope::Observed,
                "failed",
            ),
            (
                "fresh, full source",
                &fresh_report,
                PolicyScope::FullSource,
                "passed",
            ),
        ] {
            let result = QualityPolicy::new()
                .max_date_age("published_at", 3)
                .scope(scope)
                .evaluate(report)
                .unwrap();
            let evidence = serde_json::to_value(check_of(&result)).unwrap()["evidence"].clone();
            let got = (status(&result), evidence);
            let sampled = json!({ "coverage": "incomplete", "reason": "quality_sampled" });
            if got != (expected.to_string(), sampled) {
                wrong.push(format!("{case} [{engine:?}]: {got:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "wrong decisions:\n{}", wrong.join("\n"));
}

/// A scan that stopped early: the same rule as the sample, on the scan's gap.
#[test]
fn a_truncated_scan_passes_a_fresh_date_and_leaves_a_stale_one_open() {
    let mut wrong = Vec::new();
    for (case, reference, expected) in [
        ("stale", "2026-09-30", "evidence_incomplete"),
        ("fresh", "2025-12-16", "passed"),
    ] {
        let source = file(".csv", &csv_text(&DATES));
        let report = Profiler::new()
            .reference_date(day(reference))
            .stop_when(StopCondition::MaxRows(2))
            .analyze_file(source.path())
            .unwrap();
        let result = evaluate(&report, "published_at", 3);
        let evidence = serde_json::to_value(check_of(&result)).unwrap()["evidence"].clone();
        let got = (status(&result), evidence);
        let truncated = json!({ "coverage": "incomplete", "reason": "truncated" });
        if got != (expected.to_string(), truncated) {
            wrong.push(format!("{case}: {got:?}"));
        }
    }
    assert!(wrong.is_empty(), "wrong decisions:\n{}", wrong.join("\n"));
}

/// Absence is never a pass: each case names why the age could not be read.
#[test]
fn a_column_without_a_newest_date_or_a_report_without_a_clock_is_unevaluated() {
    let reference = day("2026-09-30");
    let source = file(".csv", &csv_text(&DATES));
    let report = Profiler::new()
        .reference_date(reference)
        .analyze_file(source.path())
        .unwrap();
    let all_null = file(".csv", "published_at,x\n,1\n,2\n");
    let all_null = Profiler::new()
        .reference_date(reference)
        .analyze_file(all_null.path())
        .unwrap();
    let no_quality = Profiler::new()
        .reference_date(reference)
        .metric_packs(vec![MetricPack::Schema, MetricPack::Statistics])
        .analyze_file(source.path())
        .unwrap();
    let mut document = serde_json::to_value(&report).unwrap();
    document["quality"]["metrics"]["timeliness"]
        .as_object_mut()
        .unwrap()
        .remove("clock")
        .expect("a recorded clock");
    let no_clock: ProfileReport = serde_json::from_value(document).unwrap();
    let mut document = serde_json::to_value(&report).unwrap();
    document["column_profiles"][0]["stats"]["DateTime"]["max_datetime"] = json!("17/12/2025");
    let malformed: ProfileReport = serde_json::from_value(document).unwrap();
    let message = serde_json::to_value(check_of(&evaluate(&malformed, "published_at", 3))).unwrap()
        ["message"]
        .clone();
    assert_eq!(
        message,
        "the newest date recorded for this column is not a YYYY-MM-DD date"
    );

    let cases = [
        (
            "a newest date that is not YYYY-MM-DD",
            &malformed,
            "published_at",
            "not_assessed",
        ),
        ("a numeric column", &report, "x", "not_assessed"),
        (
            "a column the report lacks",
            &report,
            "missing",
            "column_not_profiled",
        ),
        (
            "an all-null column",
            &all_null,
            "published_at",
            "not_assessed",
        ),
        (
            "no quality assessment",
            &no_quality,
            "published_at",
            "quality_unavailable",
        ),
        (
            "no recorded clock",
            &no_clock,
            "published_at",
            "setting_unrecorded",
        ),
    ];
    let mut wrong = Vec::new();
    for (case, report, column, expected) in cases {
        let result = evaluate(report, column, 3);
        let got = status(&result);
        if got != expected
            || result.verdict != Verdict::Inconclusive
            || observed(&result) != Value::Null
        {
            wrong.push(format!("{case}: {got} {:?}", result.verdict));
        }
    }
    assert!(wrong.is_empty(), "wrong outcomes:\n{}", wrong.join("\n"));
    let unrecorded =
        serde_json::to_value(check_of(&evaluate(&no_clock, "published_at", 3))).unwrap();
    assert_eq!(unrecorded["setting"], "timeliness_clock");
}

/// The rule alone is a policy, its checks come after the duplicate-row rule
/// and before the findings rules, in column-name order, and the result
/// carries the allowance and the age as whole days.
#[test]
fn the_checks_follow_the_duplicate_rule_in_column_order() {
    let source = file(".csv", "b_date,a_date\n2025-12-17,2025-12-01\n");
    let report = Profiler::new()
        .reference_date(day("2025-12-18"))
        .analyze_file(source.path())
        .unwrap();
    let result = QualityPolicy::new()
        .forbid_findings([dataprof::FindingCode::AllNull])
        .max_date_age("b_date", 1)
        .max_date_age("a_date", 1)
        .max_duplicate_rows(0)
        .evaluate(&report)
        .unwrap();
    let document = serde_json::to_value(&result).unwrap();
    let order: Vec<(String, Value)> = document["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|check| {
            (
                check["code"].as_str().unwrap().to_string(),
                check.get("column").cloned().unwrap_or(Value::Null),
            )
        })
        .collect();
    assert_eq!(
        order,
        vec![
            ("max_duplicate_rows".to_string(), Value::Null),
            ("max_date_age".to_string(), json!("a_date")),
            ("max_date_age".to_string(), json!("b_date")),
            ("forbid_finding".to_string(), Value::Null),
        ]
    );
    assert_eq!(result.verdict, Verdict::Fail);
    let a_date = &document["checks"][1];
    assert_eq!(
        a_date["expected"],
        json!({ "comparison": "at_most", "value": 1 })
    );
    assert_eq!(a_date["observed"], json!(17));
    assert_eq!(a_date["status"], "failed");
    assert_eq!(document["checks"][2]["status"], "passed");

    let alone = QualityPolicy::new()
        .max_date_age("b_date", 1)
        .evaluate(&report);
    assert_eq!(alone.unwrap().verdict, Verdict::Pass);
}
