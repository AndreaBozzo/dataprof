//! Rules about the column set: required columns, and no column removed from
//! or added to a baseline's (#749).
//!
//! `python/tests/test_schema_rules.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckCode, GateResult, PolicyError, PolicyScope, ProfileReport, Profiler, QualityPolicy,
    ReportSide, StopCondition, Verdict,
};
use serde_json::json;

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn profile(
    file: &tempfile::NamedTempFile,
    configure: impl FnOnce(Profiler) -> Profiler,
) -> ProfileReport {
    configure(Profiler::new())
        .analyze_file(file.path())
        .unwrap()
}

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|name| (*name).to_string()).collect()
}

/// `(code, column, outcome)` per check, the outcome being `passed`,
/// `failed`, or the reason a check was not evaluated.
fn outcomes(result: &GateResult) -> Vec<(CheckCode, String, String)> {
    result
        .checks
        .iter()
        .map(|check| {
            let document = serde_json::to_value(check).unwrap();
            let text = |key: &str| document[key].as_str().unwrap_or_default().to_string();
            let outcome = match text("status").as_str() {
                "not_evaluated" => match text("reason").as_str() {
                    reason @ ("setting_changed" | "setting_unrecorded") => {
                        format!("{reason}:{}", text("setting"))
                    }
                    "evidence_incomplete" => format!("evidence_incomplete:{}", text("gap")),
                    reason => reason.to_string(),
                },
                status => status.to_string(),
            };
            (
                check.code,
                check.column.clone().unwrap_or_default(),
                outcome,
            )
        })
        .collect()
}

trait Pick {
    fn pick(self, code: CheckCode, column: &str) -> (CheckCode, String, String);
}

impl Pick for Vec<(CheckCode, String, String)> {
    fn pick(self, code: CheckCode, column: &str) -> (CheckCode, String, String) {
        self.into_iter()
            .find(|(c, name, _)| *c == code && name == column)
            .unwrap_or_else(|| panic!("no {code} check for {column}"))
    }
}

fn outcome(code: CheckCode, column: &str, outcome: &str) -> (CheckCode, String, String) {
    (code, column.to_string(), outcome.to_string())
}

fn without_settings(report: &ProfileReport) -> ProfileReport {
    let mut document = serde_json::to_value(report).unwrap();
    document
        .as_object_mut()
        .unwrap()
        .remove("analysis_settings");
    serde_json::from_value(document).unwrap()
}

const ORDERS: &str = "id,amount,note\n1,10,a\n2,20,b\n3,30,c\n";
const NO_NOTE: &str = "id,amount\n1,10\n2,20\n3,30\n";

#[test]
fn a_required_column_fails_only_when_the_report_shows_its_source_lacks_it() {
    let orders = file(".csv", ORDERS);
    let no_note = file(".csv", NO_NOTE);
    let policy = QualityPolicy::new().require_columns(["note", "id"]);
    let check = |report: &ProfileReport, policy: &QualityPolicy| {
        outcomes(&policy.evaluate(report).unwrap())
    };

    // In name order; no baseline needed.
    assert_eq!(
        check(&profile(&orders, |p| p), &policy),
        [
            outcome(CheckCode::RequireColumn, "id", "passed"),
            outcome(CheckCode::RequireColumn, "note", "passed"),
        ]
    );
    // Every column selected and `note` missing: not in the source.
    let missing = profile(&no_note, |p| p);
    assert_eq!(
        check(&missing, &policy)[1],
        outcome(CheckCode::RequireColumn, "note", "failed")
    );
    // A CSV header lists every column however few rows were read.
    let truncated = profile(&no_note, |p| p.stop_when(StopCondition::MaxRows(1)));
    assert!(!truncated.execution.source_exhausted);
    assert_eq!(
        check(&truncated, &policy)[1],
        outcome(CheckCode::RequireColumn, "note", "failed")
    );
    // Left out of the selection: the run did not profile it.
    let projected = profile(&orders, |p| p.columns(names(&["id", "amount"])));
    assert_eq!(
        check(&projected, &policy)[1],
        outcome(CheckCode::RequireColumn, "note", "column_not_profiled")
    );
    // Saved before the selection was recorded.
    assert_eq!(
        check(&without_settings(&missing), &policy)[1],
        outcome(
            CheckCode::RequireColumn,
            "note",
            "setting_unrecorded:columns"
        )
    );
}

#[test]
fn a_jsonl_column_set_covers_the_source_only_when_every_record_was_read() {
    let records = file(
        ".jsonl",
        "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3, \"note\": \"a\"}\n",
    );
    let policy = QualityPolicy::new().require_columns(["note"]);
    // The cap stops before the only record with `note`.
    let truncated = profile(&records, |p| p.stop_when(StopCondition::MaxRows(1)));
    assert_eq!(
        outcomes(&policy.evaluate(&truncated).unwrap()),
        [outcome(
            CheckCode::RequireColumn,
            "note",
            "evidence_incomplete:truncated"
        )]
    );
    // About what was read, the column is missing.
    assert_eq!(
        outcomes(
            &policy
                .clone()
                .scope(PolicyScope::Observed)
                .evaluate(&truncated)
                .unwrap()
        ),
        [outcome(CheckCode::RequireColumn, "note", "failed")]
    );
    let gone = file(".jsonl", "{\"id\": 1}\n{\"id\": 2}\n");
    assert_eq!(
        outcomes(&policy.evaluate(&profile(&gone, |p| p)).unwrap()),
        [outcome(CheckCode::RequireColumn, "note", "failed")]
    );
}

#[test]
fn removed_and_added_columns_fail_against_a_baseline_that_shows_the_change() {
    let orders = file(".csv", ORDERS);
    let no_note = file(".csv", NO_NOTE);
    let baseline = profile(&orders, |p| p);
    let policy = QualityPolicy::new()
        .forbid_removed_columns()
        .forbid_added_columns();
    let against = |report: &ProfileReport, baseline: &ReportSide| {
        policy.evaluate_against(report, baseline).unwrap()
    };

    // Removed: one check per baseline column, then added: one per report
    // column, each in its side's order.
    let lost = against(
        &profile(&no_note, |p| p),
        &ReportSide::from_report(&baseline),
    );
    assert_eq!(lost.verdict, Verdict::Fail);
    assert_eq!(
        outcomes(&lost),
        [
            outcome(CheckCode::ForbidRemovedColumn, "id", "passed"),
            outcome(CheckCode::ForbidRemovedColumn, "amount", "passed"),
            outcome(CheckCode::ForbidRemovedColumn, "note", "failed"),
            outcome(CheckCode::ForbidAddedColumn, "id", "passed"),
            outcome(CheckCode::ForbidAddedColumn, "amount", "passed"),
        ]
    );
    // The same diagnostic is still listed beside the rule.
    assert_eq!(lost.baseline.as_ref().unwrap().diagnostics.len(), 1);

    let gained = against(
        &baseline,
        &ReportSide::from_report(&profile(&no_note, |p| p)),
    );
    assert_eq!(
        outcomes(&gained).pick(CheckCode::ForbidAddedColumn, "note"),
        outcome(CheckCode::ForbidAddedColumn, "note", "failed")
    );

    // A selection that left the column out is a setting, not the source.
    let projected = profile(&orders, |p| p.columns(names(&["id", "amount"])));
    assert_eq!(
        outcomes(&against(&projected, &ReportSide::from_report(&baseline)))[2],
        outcome(
            CheckCode::ForbidRemovedColumn,
            "note",
            "setting_changed:columns"
        )
    );
    assert_eq!(
        outcomes(&against(&baseline, &ReportSide::from_report(&projected)))
            .pick(CheckCode::ForbidAddedColumn, "note"),
        outcome(
            CheckCode::ForbidAddedColumn,
            "note",
            "setting_changed:columns"
        )
    );
    assert_eq!(
        outcomes(&against(
            &baseline,
            &ReportSide::from_report(&without_settings(&projected))
        ))
        .pick(CheckCode::ForbidAddedColumn, "note"),
        outcome(
            CheckCode::ForbidAddedColumn,
            "note",
            "setting_unrecorded:columns"
        )
    );
}

#[test]
fn a_flat_summary_retains_declared_schema_for_capped_sources() {
    // A CSV header declares every column even when the scan was capped.
    let orders = file(".csv", ORDERS);
    let no_note = file(".csv", NO_NOTE);
    let policy = QualityPolicy::new().forbid_added_columns();
    let flat = |report: &ProfileReport| {
        ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap()
    };
    let report = profile(&orders, |p| p);
    let complete = profile(&no_note, |p| p);
    let truncated = profile(&no_note, |p| p.stop_when(StopCondition::MaxRows(1)));

    let decided = outcomes(&policy.evaluate_against(&report, &flat(&complete)).unwrap());
    assert_eq!(
        decided[2],
        outcome(CheckCode::ForbidAddedColumn, "note", "failed")
    );
    let live = outcomes(
        &policy
            .evaluate_against(&report, &ReportSide::from_report(&truncated))
            .unwrap(),
    );
    assert_eq!(
        live[2],
        outcome(CheckCode::ForbidAddedColumn, "note", "failed")
    );
    let summarized = outcomes(&policy.evaluate_against(&report, &flat(&truncated)).unwrap());
    assert_eq!(
        summarized[2],
        outcome(CheckCode::ForbidAddedColumn, "note", "failed")
    );
}

#[test]
fn the_rules_need_a_baseline_where_they_compare_with_one() {
    let orders = file(".csv", ORDERS);
    let report = profile(&orders, |p| p);
    for policy in [
        QualityPolicy::new().forbid_removed_columns(),
        QualityPolicy::new().forbid_added_columns(),
    ] {
        assert_eq!(
            policy.evaluate(&report).unwrap_err(),
            PolicyError::BaselineRequired
        );
    }
    // A required column is a policy on its own.
    assert!(
        QualityPolicy::new()
            .require_columns(["id"])
            .validate()
            .is_ok()
    );

    let result = QualityPolicy::new()
        .require_columns(["id"])
        .forbid_added_columns()
        .evaluate_against(&report, &ReportSide::from_report(&report))
        .unwrap();
    let document = serde_json::to_value(&result).unwrap();
    assert_eq!(
        document["checks"][0],
        json!({
            "code": "require_column",
            "column": "id",
            "expected": {"comparison": "present"},
            "scope": "full_source",
            "evidence": {"coverage": "complete"},
            "status": "passed",
            "message": "the required column was profiled",
        })
    );
    assert_eq!(
        document["checks"][1],
        json!({
            "code": "forbid_added_column",
            "column": "id",
            "expected": {"comparison": "in_baseline"},
            "scope": "full_source",
            "evidence": {"coverage": "complete"},
            "baseline": {"observed": null, "evidence": {"coverage": "complete"}, "change": null},
            "status": "passed",
            "message": "the column is one the baseline profiled",
        })
    );
}

#[test]
fn a_closed_schema_is_not_passed_by_a_report_that_read_part_of_its_source() {
    // The cap stops before the only record with `extra`, so no per-column
    // check can see it; the full run fails on it.
    let baseline = profile(&file(".jsonl", "{\"id\": 1}\n{\"id\": 2}\n"), |p| p);
    let incoming = file(".jsonl", "{\"id\": 1}\n{\"id\": 2, \"extra\": 5}\n");
    let policy = QualityPolicy::new().forbid_added_columns();
    let against = |report: &ProfileReport, policy: &QualityPolicy| {
        policy
            .evaluate_against(report, &ReportSide::from_report(&baseline))
            .unwrap()
    };

    let capped = profile(&incoming, |p| p.stop_when(StopCondition::MaxRows(1)));
    let result = against(&capped, &policy);
    assert_eq!(
        result.verdict,
        Verdict::Inconclusive,
        "{:#?}",
        result.checks
    );
    assert_eq!(
        outcomes(&result),
        [
            outcome(CheckCode::ForbidAddedColumn, "id", "passed"),
            outcome(
                CheckCode::ForbidAddedColumn,
                "",
                "evidence_incomplete:truncated"
            ),
        ]
    );
    assert!(result.checks[1].column.is_none());
    // About what was read, the one column read is in the baseline.
    let observed = against(&capped, &policy.clone().scope(PolicyScope::Observed));
    assert_eq!(observed.verdict, Verdict::Pass);
    assert_eq!(observed.checks.len(), 1);
    // The full run witnesses the new column.
    let full = against(&profile(&incoming, |p| p), &policy);
    assert_eq!(full.verdict, Verdict::Fail);
    assert_eq!(full.checks.len(), 2);

    // A CSV header lists every column however few rows were read.
    let csv_baseline = profile(&file(".csv", NO_NOTE), |p| p);
    let csv_capped = profile(&file(".csv", NO_NOTE), |p| {
        p.stop_when(StopCondition::MaxRows(1))
    });
    let csv = policy
        .evaluate_against(&csv_capped, &ReportSide::from_report(&csv_baseline))
        .unwrap();
    assert_eq!(csv.verdict, Verdict::Pass);
    assert_eq!(csv.checks.len(), 2);
}

#[test]
fn a_closed_schema_is_not_passed_by_a_report_that_selected_some_columns() {
    // Every row was read and the header lists every column, but the run
    // profiled only `id`, so a new `note` would go unseen.
    let baseline = profile(&file(".csv", NO_NOTE), |p| p.columns(names(&["id"])));
    let orders = file(".csv", ORDERS);
    let policy = QualityPolicy::new().forbid_added_columns();
    let against = |report: &ProfileReport| {
        policy
            .evaluate_against(report, &ReportSide::from_report(&baseline))
            .unwrap()
    };
    let projected = profile(&orders, |p| p.columns(names(&["id"])));
    let result = against(&projected);
    assert_eq!(
        result.verdict,
        Verdict::Inconclusive,
        "{:#?}",
        result.checks
    );
    assert_eq!(
        outcomes(&result),
        [
            outcome(CheckCode::ForbidAddedColumn, "id", "passed"),
            outcome(CheckCode::ForbidAddedColumn, "", "column_not_profiled"),
        ]
    );
    // A column the projection did profile and the baseline lacks is still
    // witnessed as new: the failure decides beside the unevaluated check.
    // Against a baseline that profiled every column of a source without it.
    let widened = policy
        .evaluate_against(
            &profile(&orders, |p| p.columns(names(&["id", "note"]))),
            &ReportSide::from_report(&profile(&file(".csv", NO_NOTE), |p| p)),
        )
        .unwrap();
    assert_eq!(widened.verdict, Verdict::Fail, "{:#?}", widened.checks);
    assert_eq!(
        outcomes(&widened),
        [
            outcome(CheckCode::ForbidAddedColumn, "id", "passed"),
            outcome(CheckCode::ForbidAddedColumn, "note", "failed"),
            outcome(CheckCode::ForbidAddedColumn, "", "column_not_profiled"),
        ]
    );
    // A report saved before the selection was recorded cannot show it
    // selected every column.
    assert_eq!(
        outcomes(&against(&without_settings(&profile(&orders, |p| p))))
            .pick(CheckCode::ForbidAddedColumn, ""),
        outcome(
            CheckCode::ForbidAddedColumn,
            "",
            "setting_unrecorded:columns"
        )
    );
}

#[test]
fn summary_formats_declare_only_file_and_bytes_csv_or_parquet_schemas() {
    let orders = file(".csv", ORDERS);
    let no_note = file(".csv", NO_NOTE);
    let report = profile(&orders, |p| p);
    let capped = profile(&no_note, |p| p.stop_when(StopCondition::MaxRows(1)));
    let original: serde_json::Value =
        serde_json::from_str(&capped.summary_json().unwrap()).unwrap();
    let policy = QualityPolicy::new().forbid_added_columns();

    for (source_type, format, declared) in [
        ("file", Some("csv"), true),
        ("file", Some("parquet"), true),
        ("bytes", Some("csv"), true),
        ("bytes", Some("parquet"), true),
        ("file", None, false),
        ("bytes", None, false),
        ("file", Some("json"), false),
        ("bytes", Some("jsonl"), false),
        ("file", Some("future-format"), false),
        ("stream", Some("csv"), false),
        ("future-source", Some("parquet"), false),
        ("dataframe", None, true),
        ("query", None, true),
    ] {
        let mut summary = original.clone();
        summary["source_type"] = json!(source_type);
        if let Some(format) = format {
            summary["source_format"] = json!(format);
        } else {
            summary.as_object_mut().unwrap().remove("source_format");
        }
        let baseline = ReportSide::from_summary_json(&summary.to_string()).unwrap();
        let result = outcomes(&policy.evaluate_against(&report, &baseline).unwrap());
        assert_eq!(
            result.pick(CheckCode::ForbidAddedColumn, "note"),
            outcome(
                CheckCode::ForbidAddedColumn,
                "note",
                if declared {
                    "failed"
                } else {
                    "evidence_incomplete:truncated"
                }
            ),
            "{source_type} {format:?}",
        );
    }

    for malformed in [json!(null), json!(true), json!(42), json!([]), json!({})] {
        let mut summary = original.clone();
        summary["source_format"] = malformed;
        let error = ReportSide::from_summary_json(&summary.to_string()).unwrap_err();
        assert!(error.to_string().contains("source_format"));
    }
}
