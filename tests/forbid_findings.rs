//! A gate rule that fails on named findings (#1066).
//!
//! `findings()` named defects no gate rule could act on: a column with 10 of
//! 1,000 amounts written `12,50` got a `locale_numbers` finding and a quality
//! score of 99.5, and `check(min_quality_score=95)` passed. `forbid_findings`
//! makes the finding itself a rule. The findings are derived in Rust from the
//! report, so the rule reads the same findings on every route a report reaches
//! the gate by: a profiled file, a report read back from its JSON, and the
//! same report evaluated against a baseline.
//!
//! `python/tests/test_forbid_findings.py` is the Python twin, and the gate's
//! shared fixture (`tests/fixtures/quality_gate_parity.json`) holds the cases
//! on a partial scan.

use std::io::Write;

use dataprof::{
    CheckCode, FindingCode, ProfileReport, Profiler, QualityPolicy, ReportSide, Verdict,
};
use tempfile::NamedTempFile;

/// The issue's data: 990 amounts with a decimal point, 10 with a comma.
fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "amount").unwrap();
    for i in 0..990 {
        writeln!(file, "{i}.50").unwrap();
    }
    for i in 0..10 {
        writeln!(file, "\"{i},50\"").unwrap();
    }
    file.flush().unwrap();
    file
}

fn policy() -> QualityPolicy {
    QualityPolicy::new().forbid_findings([FindingCode::MixedTypes, FindingCode::LocaleNumbers])
}

/// What one route's result says about the two codes, as
/// `(code, status, column of each finding)`.
fn outcome(result: &dataprof::GateResult) -> Vec<(String, String, Vec<Option<String>>)> {
    result
        .checks
        .iter()
        .filter(|check| check.code == CheckCode::ForbidFinding)
        .map(|check| {
            let document = serde_json::to_value(check).unwrap();
            (
                document["finding"].as_str().unwrap().to_string(),
                document["status"].as_str().unwrap().to_string(),
                check
                    .findings
                    .iter()
                    .map(|finding| finding.column.clone())
                    .collect(),
            )
        })
        .collect()
}

#[test]
fn a_forbidden_finding_fails_the_gate_on_every_route() {
    let file = csv_fixture();
    let report = Profiler::new().analyze_file(file.path()).unwrap();

    // The gap the issue names: the score rule cannot see a 1% defect.
    let score_only = QualityPolicy::new()
        .min_quality_score(95.0)
        .evaluate(&report)
        .unwrap();
    assert_eq!(score_only.verdict, Verdict::Pass);

    let reloaded: ProfileReport =
        serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
    let baseline = ReportSide::from_report(&report);
    let routes = [
        ("profiled", policy().evaluate(&report).unwrap()),
        ("reloaded", policy().evaluate(&reloaded).unwrap()),
        (
            "against a baseline",
            policy().evaluate_against(&report, &baseline).unwrap(),
        ),
    ];

    // Codes come out in code order, whatever order the policy named them in.
    let expected = vec![
        (
            "locale_numbers".to_string(),
            "failed".to_string(),
            vec![Some("amount".to_string())],
        ),
        ("mixed_types".to_string(), "passed".to_string(), vec![]),
    ];
    let wrong: Vec<_> = routes
        .iter()
        .filter(|(_, result)| result.verdict != Verdict::Fail || outcome(result) != expected)
        .map(|(route, result)| (route, result.verdict, outcome(result)))
        .collect();
    assert!(wrong.is_empty(), "routes that disagree: {wrong:?}");
}

#[test]
fn a_policy_of_forbidden_findings_alone_is_a_requirement() {
    // The empty-policy error must not fire for a policy that only forbids.
    let file = csv_fixture();
    let report = Profiler::new().analyze_file(file.path()).unwrap();
    let result = QualityPolicy::new()
        .forbid_findings([FindingCode::PartialScan])
        .evaluate(&report)
        .unwrap();
    assert_eq!(result.verdict, Verdict::Pass);
}

#[test]
fn every_findings_code_parses_from_its_name() {
    let wrong: Vec<_> = FindingCode::ALL
        .iter()
        .filter(|code| code.to_string().parse::<FindingCode>().ok() != Some(**code))
        .collect();
    assert!(wrong.is_empty(), "codes that do not round-trip: {wrong:?}");
    // Declared order, which `ALL` must keep for the gate's check order.
    assert!(FindingCode::ALL.windows(2).all(|pair| pair[0] < pair[1]));
    let error = "nope".parse::<FindingCode>().unwrap_err();
    assert!(error.contains("locale_numbers"), "{error}");
}
