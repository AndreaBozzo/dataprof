//! The baseline gate's distinct-count collapse rule, `max_distinct_drop`
//! (#1026).
//!
//! `python/tests/test_distinct_drop_gate.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckCode, CheckStatus, EvidenceGap, GateResult, NotEvaluated, PolicyError, PolicyScope,
    ProfileReport, Profiler, QualityPolicy, ReportSide, StopCondition, Verdict,
};
use serde_json::{Value, json};

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

/// One column `k` holding `values`, `None` written as an empty or null cell.
fn csv(values: &[Option<String>]) -> String {
    let rows: String = values
        .iter()
        .map(|value| format!("{}\n", value.as_deref().unwrap_or("")))
        .collect();
    format!("k\n{rows}")
}

fn jsonl(values: &[Option<String>]) -> String {
    values
        .iter()
        .map(|value| format!("{}\n", json!({ "k": value })))
        .collect()
}

/// The routes the rule is held on: each profiles the same logical column.
fn routes(values: &[Option<String>]) -> Vec<(&'static str, ProfileReport)> {
    let csv_file = file(".csv", &csv(values));
    let jsonl_file = file(".jsonl", &jsonl(values));
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

fn keys(rows: usize, distinct: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|i| Some(format!("c{}", i % distinct)))
        .collect()
}

/// The three forms a baseline reaches the gate in: the live report, its
/// canonical document read back, and its flat summary.
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

fn evaluate(policy: &QualityPolicy, report: &ProfileReport, baseline: &ReportSide) -> GateResult {
    policy.evaluate_against(report, baseline).unwrap()
}

fn status(result: &GateResult) -> String {
    let check = result
        .checks
        .iter()
        .find(|check| check.code == CheckCode::MaxDistinctDrop)
        .expect("a max_distinct_drop check");
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

/// Each case against a baseline of 100 distinct keys over 100 rows, with
/// the status the rule must reach on every route and baseline form.
#[test]
fn a_collapse_fails_and_fewer_rows_or_held_counts_pass_on_every_route() {
    let cases: [(&str, Vec<Option<String>>, &str); 6] = [
        ("unchanged", keys(100, 100), "passed"),
        ("a key that repeats", keys(100, 10), "failed"),
        ("fewer rows, all distinct", keys(10, 10), "passed"),
        ("within the allowance", keys(100, 90), "passed"),
        // 89 distinct is an 11% fall, and over the same rows the share
        // falls as far.
        ("just past the allowance", keys(100, 89), "failed"),
        ("every value null", vec![None; 100], "not_assessed"),
    ];
    let policy = QualityPolicy::new().max_distinct_drop("k", 10.0);
    let cases: Vec<_> = cases
        .iter()
        .map(|(case, values, expected)| (*case, routes(values), *expected))
        .collect();
    let mut wrong = Vec::new();
    for (index, (route, baseline)) in routes(&keys(100, 100)).iter().enumerate() {
        for (form, side) in baselines(baseline) {
            for (case, reports, expected) in &cases {
                let got = status(&evaluate(&policy, &reports[index].1, &side));
                if got != *expected {
                    wrong.push(format!("{route}/{form}/{case}: {got}, expected {expected}"));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn the_check_carries_both_counts_and_the_relative_change() {
    let baseline = &routes(&keys(100, 100))[0].1;
    let report = &routes(&keys(100, 10))[0].1;
    let result = evaluate(
        &QualityPolicy::new().max_distinct_drop("k", 10.0),
        report,
        &ReportSide::from_report(baseline),
    );
    assert_eq!(result.verdict, Verdict::Fail);
    let check = serde_json::to_value(&result.checks[0]).unwrap();
    assert_eq!(check["code"], "max_distinct_drop");
    assert_eq!(check["column"], "k");
    assert_eq!(
        check["expected"],
        json!({"comparison": "relative_change_at_least", "value": -10.0})
    );
    assert_eq!(check["observed"], 10);
    assert_eq!(check["baseline"]["observed"], 100);
    assert_eq!(check["baseline"]["change"], -90.0);
    assert_eq!(check["status"], "failed");

    // A permitted drop of zero serializes without a sign.
    let zero = evaluate(
        &QualityPolicy::new().max_distinct_drop("k", 0.0),
        baseline,
        &ReportSide::from_report(baseline),
    );
    assert_eq!(
        serde_json::to_value(&zero.checks[0]).unwrap()["expected"]["value"].to_string(),
        "0.0"
    );
}

/// An estimate falling is not a column collapsing, and a count a document
/// does not record as exact is not one.
#[test]
fn an_estimated_or_unrecorded_count_is_never_decided() {
    let baseline = &routes(&keys(100, 100))[0].1;
    let report = &routes(&keys(100, 10))[0].1;
    let policy = QualityPolicy::new().max_distinct_drop("k", 10.0);
    let mut wrong = Vec::new();

    let mut document = serde_json::to_value(baseline).unwrap();
    document["column_profiles"][0]["unique_count_is_approximate"] = json!(true);
    let approximate: ProfileReport = serde_json::from_value(document.clone()).unwrap();
    let side = ReportSide::from_report(&approximate);
    let got = status(&evaluate(&policy, report, &side));
    if got != "approximate_count" {
        wrong.push(format!("approximate baseline document: {got}"));
    }
    // The evaluated side's estimate counts the same way.
    let got = status(&evaluate(
        &policy,
        &approximate,
        &ReportSide::from_report(report),
    ));
    if got != "approximate_count" {
        wrong.push(format!("approximate report: {got}"));
    }
    document["column_profiles"][0]
        .as_object_mut()
        .unwrap()
        .remove("unique_count_is_approximate");
    let unrecorded: ProfileReport = serde_json::from_value(document).unwrap();
    let got = status(&evaluate(
        &policy,
        report,
        &ReportSide::from_report(&unrecorded),
    ));
    if got != "approximate_count" {
        wrong.push(format!("unrecorded flag, document: {got}"));
    }

    let summary: Value = serde_json::from_str(&baseline.summary_json().unwrap()).unwrap();
    for (label, flag) in [
        ("approximate", Some(json!(true))),
        ("null", Some(Value::Null)),
        ("absent", None),
    ] {
        let mut edited = summary.clone();
        let column = edited["columns"][0].as_object_mut().unwrap();
        match flag {
            Some(value) => column.insert("unique_count_is_approximate".into(), value),
            None => column.remove("unique_count_is_approximate"),
        };
        let side = ReportSide::from_summary_json(&edited.to_string()).unwrap();
        let got = status(&evaluate(&policy, report, &side));
        if got != "approximate_count" {
            wrong.push(format!("summary flag {label}: {got}"));
        }
    }
    // A summary without the count, or without the row counts the share
    // needs, carries no value to measure from.
    for field in ["unique_count", "total_count"] {
        let mut edited = summary.clone();
        edited["columns"][0].as_object_mut().unwrap().remove(field);
        let side = ReportSide::from_summary_json(&edited.to_string()).unwrap();
        let got = status(&evaluate(&policy, report, &side));
        if got != "not_in_baseline" {
            wrong.push(format!("summary without {field}: {got}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn partial_evidence_is_unevaluated_under_full_source_only() {
    let contents = csv(&keys(200, 200));
    let source = file(".csv", &contents);
    let exact = Profiler::new().analyze_file(source.path()).unwrap();
    let truncated = Profiler::new()
        .stop_when(StopCondition::MaxRows(50))
        .analyze_file(source.path())
        .unwrap();
    let policy = QualityPolicy::new().max_distinct_drop("k", 10.0);
    for (report, baseline) in [(&truncated, &exact), (&exact, &truncated)] {
        let result = evaluate(&policy, report, &ReportSide::from_report(baseline));
        assert!(
            matches!(
                result.checks[0].not_evaluated(),
                Some(NotEvaluated::EvidenceIncomplete {
                    gap: EvidenceGap::Truncated,
                    ..
                })
            ),
            "{:#?}",
            result.checks
        );
        // Under the observed scope, 50 rows of 50 distinct keys against 200
        // of 200 is fewer rows, not a collapse.
        let observed = evaluate(
            &policy.clone().scope(PolicyScope::Observed),
            report,
            &ReportSide::from_report(baseline),
        );
        assert_eq!(observed.verdict, Verdict::Pass, "{:#?}", observed.checks);
    }
}

#[test]
fn the_wildcard_covers_shared_columns_and_a_named_column_takes_precedence() {
    let baseline = Profiler::new()
        .analyze_file(file(".csv", "a,b,old\n1,1,1\n2,2,2\n3,3,3\n4,4,4\n").path())
        .unwrap();
    let report = Profiler::new()
        .analyze_file(file(".csv", "a,b,new\n1,1,1\n1,2,1\n1,3,1\n1,4,1\n").path())
        .unwrap();
    let policy = QualityPolicy::new()
        .max_distinct_drop_any(10.0)
        .max_distinct_drop("b", 0.0)
        .max_distinct_drop("a", 100.0)
        .max_distinct_drop("gone", 10.0);
    let result = evaluate(&policy, &report, &ReportSide::from_report(&baseline));
    let got: Vec<(String, String)> = result
        .checks
        .iter()
        .map(|check| {
            let status = match &check.status {
                CheckStatus::Passed => "passed".to_string(),
                CheckStatus::Failed => "failed".to_string(),
                CheckStatus::NotEvaluated(reason) => {
                    serde_json::to_value(reason).unwrap()["reason"]
                        .as_str()
                        .unwrap()
                        .to_string()
                }
                _ => unreachable!(),
            };
            (check.column.clone().unwrap(), status)
        })
        .collect();
    let expected: Vec<(String, String)> = [
        // Named columns in name order: `a` fell 75% within its 100% allowance.
        ("a", "passed"),
        ("b", "passed"),
        ("gone", "column_not_profiled"),
    ]
    .into_iter()
    .map(|(column, status)| (column.to_string(), status.to_string()))
    .collect();
    assert_eq!(got, expected);
    assert_eq!(result.verdict, Verdict::Inconclusive);

    // Only the wildcard: `a` collapses, `b` holds, and neither `old` nor
    // `new` is covered.
    let result = evaluate(
        &QualityPolicy::new().max_distinct_drop_any(10.0),
        &report,
        &ReportSide::from_report(&baseline),
    );
    let columns: Vec<_> = result
        .checks
        .iter()
        .map(|check| (check.column.as_deref().unwrap(), check.status.clone()))
        .collect();
    assert_eq!(
        columns,
        [("a", CheckStatus::Failed), ("b", CheckStatus::Passed)]
    );
}

#[test]
fn the_rule_needs_a_baseline_and_a_percentage() {
    let report = &routes(&keys(10, 10))[0].1;
    assert_eq!(
        QualityPolicy::new()
            .max_distinct_drop("k", 10.0)
            .evaluate(report)
            .unwrap_err(),
        PolicyError::BaselineRequired
    );
    assert_eq!(
        QualityPolicy::new()
            .max_distinct_drop_any(10.0)
            .evaluate(report)
            .unwrap_err(),
        PolicyError::BaselineRequired
    );
    for bad in [-1.0, 100.5, f64::NAN] {
        assert!(matches!(
            QualityPolicy::new().max_distinct_drop("k", bad).validate(),
            Err(PolicyError::ThresholdOutOfRange {
                code: CheckCode::MaxDistinctDrop,
                ..
            })
        ));
        assert!(matches!(
            QualityPolicy::new().max_distinct_drop_any(bad).validate(),
            Err(PolicyError::ThresholdOutOfRange {
                code: CheckCode::MaxDistinctDrop,
                ..
            })
        ));
    }
}

#[test]
fn a_baseline_without_the_null_definition_is_not_comparable() {
    let legacy = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy_0_11_non_ascii.summary.json"),
    )
    .unwrap();
    let legacy: Value = serde_json::from_str(&legacy).unwrap();
    let column = legacy["columns"][0]["name"].as_str().unwrap().to_string();
    let report = Profiler::new()
        .analyze_file(file(".csv", &format!("{column}\nOslo\n")).path())
        .unwrap();
    let result = QualityPolicy::new()
        .max_distinct_drop(column, 10.0)
        .evaluate_against(
            &report,
            &ReportSide::from_summary_json(&legacy.to_string()).unwrap(),
        )
        .unwrap();
    // A 0.11 summary records an exact count but not the null tokens.
    assert_eq!(status(&result), "definition_not_comparable");
}

#[test]
fn a_column_without_a_distinct_count_is_not_assessed() {
    // A nested column holds values but is only counted.
    let nested = |values: &[i64]| {
        let lines: String = values
            .iter()
            .map(|x| format!("{}\n", json!({ "n": { "x": x } })))
            .collect();
        Profiler::new()
            .analyze_file(file(".jsonl", &lines).path())
            .unwrap()
    };
    let baseline = nested(&[1, 2, 3, 4]);
    let report = nested(&[1, 1, 1, 1]);
    let result = evaluate(
        &QualityPolicy::new().max_distinct_drop("n", 10.0),
        &report,
        &ReportSide::from_report(&baseline),
    );
    assert_eq!(status(&result), "not_assessed");
    assert_eq!(
        result.checks[0].message,
        "the report counted no distinct values for this column"
    );
}
