//! The baseline gate's row count rules, `max_row_count_drop` and
//! `max_row_count_growth` (#1291).
//!
//! `python/tests/test_row_count_change_gate.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckCode, CheckStatus, GateResult, PolicyError, PolicyScope, ProfileReport, Profiler,
    QualityPolicy, ReportSide, StopCondition, Verdict,
};
use serde_json::{Value, json};

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn csv(rows: usize) -> String {
    let body: String = (0..rows).map(|i| format!("{i},{}\n", i % 7)).collect();
    format!("id,v\n{body}")
}

fn jsonl(rows: usize) -> String {
    (0..rows)
        .map(|i| format!("{}\n", json!({ "id": i, "v": i % 7 })))
        .collect()
}

/// The same logical rows through each file route.
fn routes(rows: usize) -> Vec<(&'static str, ProfileReport)> {
    let csv_file = file(".csv", &csv(rows));
    let jsonl_file = file(".jsonl", &jsonl(rows));
    vec![
        (
            "csv",
            Profiler::new().analyze_file(csv_file.path()).unwrap(),
        ),
        (
            "jsonl",
            Profiler::new().analyze_file(jsonl_file.path()).unwrap(),
        ),
    ]
}

/// A CSV of `rows` rows, read in full or stopped after `cap` rows.
fn scan(rows: usize, cap: Option<u64>) -> ProfileReport {
    let source = file(".csv", &csv(rows));
    let profiler = match cap {
        Some(cap) => Profiler::new().stop_when(StopCondition::MaxRows(cap)),
        None => Profiler::new(),
    };
    profiler.analyze_file(source.path()).unwrap()
}

/// The three forms a baseline reaches the gate in.
fn baselines(report: &ProfileReport) -> Vec<(&'static str, ReportSide)> {
    let document: ProfileReport =
        serde_json::from_str(&serde_json::to_string(report).unwrap()).unwrap();
    vec![
        ("live", ReportSide::from_report(report)),
        ("document", ReportSide::from_report(&document)),
        (
            "summary",
            ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap(),
        ),
    ]
}

fn policy() -> QualityPolicy {
    QualityPolicy::new()
        .max_row_count_drop(10.0)
        .max_row_count_growth(10.0)
}

fn status_of(result: &GateResult, code: CheckCode) -> String {
    let check = result
        .checks
        .iter()
        .find(|check| check.code == code)
        .unwrap_or_else(|| panic!("a {code} check"));
    match &check.status {
        CheckStatus::Passed => "passed".to_string(),
        CheckStatus::Failed => "failed".to_string(),
        CheckStatus::NotEvaluated(reason) => serde_json::to_value(reason).unwrap()["reason"]
            .as_str()
            .unwrap()
            .to_string(),
        _ => unreachable!(),
    }
}

/// `(drop, growth)` statuses.
fn statuses(result: &GateResult) -> (String, String) {
    (
        status_of(result, CheckCode::MaxRowCountDrop),
        status_of(result, CheckCode::MaxRowCountGrowth),
    )
}

/// Complete scans against a baseline of 100 rows, with a 10% allowance each
/// way, on every route and baseline form.
#[test]
fn a_complete_scan_outside_either_allowance_fails_on_every_route() {
    let cases = [
        ("unchanged", 100, "passed", "passed"),
        ("a 10% drop, at the allowance", 90, "passed", "passed"),
        ("an 11% drop", 89, "failed", "passed"),
        ("a truncation to a fifth", 20, "failed", "passed"),
        ("10% growth, at the allowance", 110, "passed", "passed"),
        ("11% growth", 111, "passed", "failed"),
        ("a doubled file", 200, "passed", "failed"),
    ];
    let reports: Vec<_> = cases
        .iter()
        .map(|(case, rows, drop, growth)| (*case, routes(*rows), *drop, *growth))
        .collect();
    let mut wrong = Vec::new();
    for (index, (route, baseline)) in routes(100).iter().enumerate() {
        for (form, side) in baselines(baseline) {
            for (case, reports, drop, growth) in &reports {
                let result = policy().evaluate_against(&reports[index].1, &side).unwrap();
                let got = statuses(&result);
                if got != (drop.to_string(), growth.to_string()) {
                    wrong.push(format!(
                        "{route}/{form}/{case}: {got:?}, expected ({drop}, {growth})"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// Each cell of #1291's evidence table. A scan that stopped early read a
/// lower bound of its source's rows, so it settles only the outcome its
/// unread rows could not reverse.
#[test]
fn an_incomplete_scan_decides_only_the_outcome_its_unread_rows_cannot_reverse() {
    let exact_baseline = scan(100, None);
    // 400 rows read as 100: the true baseline is larger than it shows.
    let capped_baseline = scan(400, Some(100));
    let cases: [(&str, ProfileReport, &ProfileReport, &str, &str); 12] = [
        // This report stopped early: its true count is larger.
        (
            "current capped, 50 of 200",
            scan(200, Some(50)),
            &exact_baseline,
            "evidence_incomplete",
            "evidence_incomplete",
        ),
        (
            "current capped, 95 of 200",
            scan(200, Some(95)),
            &exact_baseline,
            "passed",
            "evidence_incomplete",
        ),
        (
            "current capped, 150 of 400",
            scan(400, Some(150)),
            &exact_baseline,
            "passed",
            "failed",
        ),
        (
            "current capped, 105 of 400",
            scan(400, Some(105)),
            &exact_baseline,
            "passed",
            "evidence_incomplete",
        ),
        // The baseline stopped early: its true count is larger.
        (
            "baseline capped, 80 now",
            scan(80, None),
            &capped_baseline,
            "failed",
            "passed",
        ),
        (
            "baseline capped, 95 now",
            scan(95, None),
            &capped_baseline,
            "evidence_incomplete",
            "passed",
        ),
        (
            "baseline capped, 200 now",
            scan(200, None),
            &capped_baseline,
            "evidence_incomplete",
            "evidence_incomplete",
        ),
        (
            "baseline capped, 105 now",
            scan(105, None),
            &capped_baseline,
            "evidence_incomplete",
            "passed",
        ),
        // Both stopped early: the unread rows could move either way.
        (
            "both capped, 50",
            scan(400, Some(50)),
            &capped_baseline,
            "evidence_incomplete",
            "evidence_incomplete",
        ),
        (
            "both capped, 100",
            scan(400, Some(100)),
            &capped_baseline,
            "evidence_incomplete",
            "evidence_incomplete",
        ),
        (
            "both capped, 150",
            scan(400, Some(150)),
            &capped_baseline,
            "evidence_incomplete",
            "evidence_incomplete",
        ),
        (
            "both complete, 100",
            scan(100, None),
            &exact_baseline,
            "passed",
            "passed",
        ),
    ];
    let truncated = policy()
        .evaluate_against(&cases[0].1, &ReportSide::from_report(&exact_baseline))
        .unwrap();
    assert_eq!(
        truncated.checks[0].message,
        "the row count fell more than allowed from the baseline, and the rows a scan did not read may reverse that"
    );
    let mut wrong = Vec::new();
    for (case, report, baseline, drop, growth) in &cases {
        let side = ReportSide::from_report(baseline);
        let got = statuses(&policy().evaluate_against(report, &side).unwrap());
        if got != (drop.to_string(), growth.to_string()) {
            wrong.push(format!("{case}: {got:?}, expected ({drop}, {growth})"));
        }
        // Under the observed scope the question is about the rows read, so
        // every cell is decided.
        let observed = policy()
            .scope(PolicyScope::Observed)
            .evaluate_against(report, &side)
            .unwrap();
        let (drop, growth) = statuses(&observed);
        if drop.starts_with("evidence") || growth.starts_with("evidence") {
            wrong.push(format!("{case}, observed scope: ({drop}, {growth})"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn the_check_carries_both_counts_and_the_relative_change() {
    let baseline = ReportSide::from_report(&scan(100, None));
    let result = policy()
        .evaluate_against(&scan(200, None), &baseline)
        .unwrap();
    assert_eq!(result.verdict, Verdict::Fail);
    let checks: Vec<Value> = result
        .checks
        .iter()
        .map(|check| serde_json::to_value(check).unwrap())
        .collect();
    assert_eq!(checks[0]["code"], "max_row_count_drop");
    assert_eq!(
        checks[0]["message"],
        "the row count fell no more than allowed from the baseline"
    );
    assert_eq!(
        checks[1]["message"],
        "the row count grew more than allowed from the baseline"
    );
    assert_eq!(
        checks[0]["expected"],
        json!({"comparison": "relative_change_at_least", "value": -10.0})
    );
    assert_eq!(checks[1]["code"], "max_row_count_growth");
    assert_eq!(
        checks[1]["expected"],
        json!({"comparison": "relative_change_at_most", "value": 10.0})
    );
    for check in &checks {
        assert!(check.get("column").is_none(), "{check}");
        assert_eq!(check["observed"], 200);
        assert_eq!(check["baseline"]["observed"], 100);
        assert_eq!(check["baseline"]["change"], 100.0);
    }
    // A permitted drop of zero serializes as 0.0, not -0.0.
    let zero = QualityPolicy::new()
        .max_row_count_drop(0.0)
        .evaluate_against(&scan(100, None), &baseline)
        .unwrap();
    assert_eq!(
        serde_json::to_string(&zero.checks[0].expected).unwrap(),
        r#"{"comparison":"relative_change_at_least","value":0.0}"#
    );
}

#[test]
fn the_rules_come_after_the_score_rules_and_before_the_column_rules() {
    let baseline = ReportSide::from_report(&scan(100, None));
    let result = QualityPolicy::new()
        .max_null_percentage_increase_any(5.0)
        .max_row_count_growth(10.0)
        .max_quality_score_drop(5.0)
        .max_row_count_drop(10.0)
        .evaluate_against(&scan(100, None), &baseline)
        .unwrap();
    let codes: Vec<CheckCode> = result.checks.iter().map(|check| check.code).collect();
    assert_eq!(
        codes,
        [
            CheckCode::MaxQualityScoreDrop,
            CheckCode::MaxRowCountDrop,
            CheckCode::MaxRowCountGrowth,
            CheckCode::MaxNullPercentageIncrease,
            CheckCode::MaxNullPercentageIncrease,
        ]
    );
}

#[test]
fn a_different_column_set_leaves_both_rules_unevaluated() {
    let baseline = Profiler::new()
        .analyze_file(file(".csv", "id,v\n1,1\n2,2\n3,3\n4,4\n").path())
        .unwrap();
    for (case, contents) in [("added", "id,v,w\n1,1,1\n"), ("removed", "id\n1\n")] {
        let report = Profiler::new()
            .analyze_file(file(".csv", contents).path())
            .unwrap();
        let result = policy()
            .evaluate_against(&report, &ReportSide::from_report(&baseline))
            .unwrap();
        assert_eq!(
            statuses(&result),
            ("schema_changed".to_string(), "schema_changed".to_string()),
            "{case}"
        );
        assert_eq!(result.verdict, Verdict::Inconclusive, "{case}");
    }
}

#[test]
fn a_baseline_without_a_row_count_is_not_in_baseline() {
    let report = scan(100, None);
    let mut wrong = Vec::new();

    let mut summary: Value =
        serde_json::from_str(&scan(100, None).summary_json().unwrap()).unwrap();
    summary["execution"]
        .as_object_mut()
        .unwrap()
        .remove("rows_processed")
        .expect("a summary records rows_processed");
    let unrecorded = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    let got = statuses(&policy().evaluate_against(&report, &unrecorded).unwrap());
    if got != ("not_in_baseline".into(), "not_in_baseline".into()) {
        wrong.push(format!("summary without rows_processed: {got:?}"));
    }

    // A baseline that read no rows has no count to take a share of.
    let empty = ReportSide::from_report(&scan(0, None));
    let got = statuses(&policy().evaluate_against(&report, &empty).unwrap());
    if got != ("not_in_baseline".into(), "not_in_baseline".into()) {
        wrong.push(format!("baseline of no rows: {got:?}"));
    }

    // A report that read no rows against a baseline that read some fell 100%.
    let result = policy()
        .evaluate_against(&scan(0, None), &ReportSide::from_report(&report))
        .unwrap();
    let got = statuses(&result);
    if got != ("failed".into(), "passed".into()) {
        wrong.push(format!("report of no rows: {got:?}"));
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn the_rules_need_a_baseline_and_a_valid_percentage() {
    let report = scan(10, None);
    for policy in [
        QualityPolicy::new().max_row_count_drop(10.0),
        QualityPolicy::new().max_row_count_growth(10.0),
    ] {
        assert_eq!(
            policy.evaluate(&report).unwrap_err(),
            PolicyError::BaselineRequired
        );
    }
    for bad in [-1.0, 100.5, f64::NAN, f64::INFINITY] {
        assert!(
            matches!(
                QualityPolicy::new().max_row_count_drop(bad).validate(),
                Err(PolicyError::ThresholdOutOfRange {
                    code: CheckCode::MaxRowCountDrop,
                    ..
                })
            ),
            "drop {bad}"
        );
    }
    for bad in [-1.0, f64::NAN, f64::INFINITY] {
        let error = QualityPolicy::new()
            .max_row_count_growth(bad)
            .validate()
            .unwrap_err();
        assert!(
            matches!(
                error,
                PolicyError::ThresholdOutOfRange {
                    code: CheckCode::MaxRowCountGrowth,
                    ..
                }
            ),
            "growth {bad}"
        );
        assert!(
            error.to_string().contains("at least 0"),
            "growth {bad}: {error}"
        );
    }
    // A count can more than double, so growth has no upper end.
    QualityPolicy::new()
        .max_row_count_growth(250.0)
        .validate()
        .unwrap();
}

/// A count cannot fall by more than all of it, so a 100% drop allowance, or
/// one that rounds to it, passes whatever the scans missed.
#[test]
fn a_full_drop_allowance_passes_whatever_the_scans_missed() {
    let capped_baseline = ReportSide::from_report(&scan(400, Some(100)));
    let mut wrong = Vec::new();
    for percent in [100.0, 99.999] {
        let policy = QualityPolicy::new().max_row_count_drop(percent);
        for (case, report) in [
            ("baseline capped", scan(80, None)),
            ("both capped", scan(400, Some(50))),
        ] {
            let result = policy.evaluate_against(&report, &capped_baseline).unwrap();
            let got = status_of(&result, CheckCode::MaxRowCountDrop);
            if got != "passed" {
                wrong.push(format!("{percent}, {case}: {got}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
