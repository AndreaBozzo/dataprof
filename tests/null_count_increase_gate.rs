//! The baseline gate's null count increase rule, `max_null_count_increase`
//! (#1346).
//!
//! `python/tests/test_null_count_increase_gate.py` is the Python twin.

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

/// A column `k` holding `values`, `None` written as an empty or null cell,
/// beside a row number `i`, so a row whose `k` is empty is not a blank line.
fn csv(values: &[Option<String>]) -> String {
    let rows: String = values
        .iter()
        .enumerate()
        .map(|(i, value)| format!("{},{i}\n", value.as_deref().unwrap_or("")))
        .collect();
    format!("k,i\n{rows}")
}

fn jsonl(values: &[Option<String>]) -> String {
    values
        .iter()
        .enumerate()
        .map(|(i, value)| format!("{}\n", json!({ "k": value, "i": i })))
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

/// `rows` values of which the first `nulls` are null.
fn column(rows: usize, nulls: usize) -> Vec<Option<String>> {
    (0..rows)
        .map(|i| (i >= nulls).then(|| format!("v{i}")))
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

fn status_of(check: &dataprof::Check) -> String {
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

fn status(result: &GateResult) -> String {
    let check = result
        .checks
        .iter()
        .find(|check| check.code == CheckCode::MaxNullCountIncrease)
        .expect("a max_null_count_increase check");
    status_of(check)
}

/// Each case against a baseline of 2 nulls in 100 rows, with an allowance
/// of one more null, and the status the rule must reach on every route and
/// baseline form.
#[test]
fn a_rise_past_the_allowance_fails_on_every_route() {
    let cases: [(&str, Vec<Option<String>>, &str); 6] = [
        ("unchanged", column(100, 2), "passed"),
        (
            "one more null, within the allowance",
            column(100, 3),
            "passed",
        ),
        ("two more nulls", column(100, 4), "failed"),
        ("fewer nulls", column(100, 0), "passed"),
        // A count, not a share: more rows over the same nulls is no rise.
        ("more rows, same nulls", column(200, 2), "passed"),
        ("every value null", column(100, 100), "failed"),
    ];
    let policy = QualityPolicy::new().max_null_count_increase("k", 1);
    let cases: Vec<_> = cases
        .iter()
        .map(|(case, values, expected)| (*case, routes(values), *expected))
        .collect();
    let mut wrong = Vec::new();
    for (index, (route, baseline)) in routes(&column(100, 2)).iter().enumerate() {
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

/// The issue's case: from about 30,000 rows one new null rounds to a
/// percentage change of 0.00, which the percentage rule passes at 0. The
/// count rule fails it at both sizes.
#[test]
fn one_new_null_fails_where_the_rounded_percentage_passes() {
    let mut wrong = Vec::new();
    for rows in [20_000, 30_000] {
        let baselines = routes(&column(rows, 0));
        let reports = routes(&column(rows, 1));
        for ((route, baseline), (_, report)) in baselines.iter().zip(&reports) {
            let side = ReportSide::from_report(baseline);
            let count = evaluate(
                &QualityPolicy::new().max_null_count_increase("k", 0),
                report,
                &side,
            );
            if count.verdict != Verdict::Fail {
                wrong.push(format!("{route}/{rows}: count rule {:?}", count.verdict));
            }
            let percentage = evaluate(
                &QualityPolicy::new().max_null_percentage_increase("k", 0.0),
                report,
                &side,
            );
            let expected = if rows == 20_000 {
                Verdict::Fail
            } else {
                Verdict::Pass
            };
            if percentage.verdict != expected {
                wrong.push(format!(
                    "{route}/{rows}: percentage rule {:?}, expected {expected:?}",
                    percentage.verdict
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn the_check_carries_both_counts_and_a_signed_change() {
    let baseline = &routes(&column(100, 5))[0].1;
    let report = &routes(&column(100, 8))[0].1;
    let result = evaluate(
        &QualityPolicy::new().max_null_count_increase("k", 2),
        report,
        &ReportSide::from_report(baseline),
    );
    assert_eq!(result.verdict, Verdict::Fail);
    let check = serde_json::to_value(&result.checks[0]).unwrap();
    assert_eq!(check["code"], "max_null_count_increase");
    assert_eq!(check["column"], "k");
    assert_eq!(
        check["expected"],
        json!({"comparison": "change_at_most", "value": 2})
    );
    assert_eq!(check["observed"], 8);
    assert_eq!(check["baseline"]["observed"], 5);
    assert_eq!(check["baseline"]["change"], 3);
    assert_eq!(check["status"], "failed");

    // Fewer nulls is a negative change, still a whole number.
    let fell = evaluate(
        &QualityPolicy::new().max_null_count_increase("k", 0),
        baseline,
        &ReportSide::from_report(report),
    );
    let check = serde_json::to_value(&fell.checks[0]).unwrap();
    assert_eq!(check["baseline"]["change"], json!(-3));
    assert_eq!(check["status"], "passed");

    // The allowance is added, never subtracted, so the largest one cannot
    // overflow.
    let widest = evaluate(
        &QualityPolicy::new().max_null_count_increase("k", usize::MAX),
        report,
        &ReportSide::from_report(baseline),
    );
    assert_eq!(widest.verdict, Verdict::Pass);
}

/// A column that read no rows has a null count of zero, as for
/// `max_null_count`, so it decides rather than being `not_assessed`.
#[test]
fn a_column_with_no_rows_read_has_a_null_count_of_zero() {
    let empty = Profiler::new()
        .analyze_file(file(".csv", "k,i\n").path())
        .unwrap();
    let baseline = &routes(&column(10, 2))[0].1;
    let policy = QualityPolicy::new().max_null_count_increase("k", 0);
    let fell = evaluate(&policy, &empty, &ReportSide::from_report(baseline));
    assert_eq!(status(&fell), "passed");
    let rose = evaluate(&policy, baseline, &ReportSide::from_report(&empty));
    assert_eq!(status(&rose), "failed");
}

/// A flat summary that does not record a column's counts carries no null
/// count to measure from.
#[test]
fn a_summary_without_the_counts_is_not_in_baseline() {
    let baseline = &routes(&column(100, 2))[0].1;
    let report = &routes(&column(100, 9))[0].1;
    let policy = QualityPolicy::new().max_null_count_increase("k", 0);
    let summary: Value = serde_json::from_str(&baseline.summary_json().unwrap()).unwrap();
    let mut wrong = Vec::new();
    for field in ["null_count", "total_count"] {
        for value in [None, Some(Value::Null)] {
            let mut edited = summary.clone();
            let entry = edited["columns"][0].as_object_mut().unwrap();
            match &value {
                Some(value) => entry.insert(field.into(), value.clone()),
                None => entry.remove(field),
            };
            let side = ReportSide::from_summary_json(&edited.to_string()).unwrap();
            let result = evaluate(&policy, report, &side);
            let got = status(&result);
            if got != "not_in_baseline" {
                wrong.push(format!("summary with {field} {value:?}: {got}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn partial_evidence_is_unevaluated_under_full_source_only() {
    // Every fourth value null: 50 of 200 over the whole file, 13 of the
    // first 50 rows.
    let values: Vec<Option<String>> = (0..200)
        .map(|i| (i % 4 != 0).then(|| format!("v{i}")))
        .collect();
    let source = file(".csv", &csv(&values));
    let exact = Profiler::new().analyze_file(source.path()).unwrap();
    let truncated = Profiler::new()
        .stop_when(StopCondition::MaxRows(50))
        .analyze_file(source.path())
        .unwrap();
    let policy = QualityPolicy::new().max_null_count_increase("k", 0);
    for (report, baseline, observed) in [
        (&truncated, &exact, Verdict::Pass),
        (&exact, &truncated, Verdict::Fail),
    ] {
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
        // Under the observed scope each side's count is what it read.
        let result = evaluate(
            &policy.clone().scope(PolicyScope::Observed),
            report,
            &ReportSide::from_report(baseline),
        );
        assert_eq!(result.verdict, observed, "{:#?}", result.checks);
    }
}

#[test]
fn the_wildcard_covers_shared_columns_and_a_named_column_takes_precedence() {
    let baseline = Profiler::new()
        .analyze_file(file(".csv", "a,b,old\n1,1,1\n2,2,2\n3,3,3\n4,4,4\n").path())
        .unwrap();
    let report = Profiler::new()
        .analyze_file(file(".csv", "a,b,new\n,1,\n,2,\n3,,\n4,4,4\n").path())
        .unwrap();
    let policy = QualityPolicy::new()
        .max_null_count_increase_any(0)
        .max_null_count_increase("b", 1)
        .max_null_count_increase("a", 2)
        .max_null_count_increase("gone", 0)
        .max_null_count_increase("new", 0);
    let result = evaluate(&policy, &report, &ReportSide::from_report(&baseline));
    let got: Vec<(String, String)> = result
        .checks
        .iter()
        .map(|check| (check.column.clone().unwrap(), status_of(check)))
        .collect();
    let expected: Vec<(String, String)> = [
        // Named columns in name order: `a` gained 2 within its 2.
        ("a", "passed"),
        ("b", "passed"),
        ("gone", "column_not_profiled"),
        ("new", "not_in_baseline"),
    ]
    .into_iter()
    .map(|(column, status)| (column.to_string(), status.to_string()))
    .collect();
    assert_eq!(got, expected);
    assert_eq!(result.verdict, Verdict::Inconclusive);

    // Only the wildcard: `a` and `b` gained nulls, and neither `old` nor
    // `new` is covered.
    let result = evaluate(
        &QualityPolicy::new().max_null_count_increase_any(0),
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
        [("a", CheckStatus::Failed), ("b", CheckStatus::Failed)]
    );
}

#[test]
fn the_rule_needs_a_baseline() {
    let report = &routes(&column(10, 0))[0].1;
    assert_eq!(
        QualityPolicy::new()
            .max_null_count_increase("k", 0)
            .evaluate(report)
            .unwrap_err(),
        PolicyError::BaselineRequired
    );
    assert_eq!(
        QualityPolicy::new()
            .max_null_count_increase_any(0)
            .evaluate(report)
            .unwrap_err(),
        PolicyError::BaselineRequired
    );
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
        .max_null_count_increase(column, 0)
        .evaluate_against(
            &report,
            &ReportSide::from_summary_json(&legacy.to_string()).unwrap(),
        )
        .unwrap();
    // A 0.11 summary records the counts but not the null tokens.
    assert_eq!(status(&result), "definition_not_comparable");
}
