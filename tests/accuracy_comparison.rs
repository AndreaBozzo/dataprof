//! A baseline saved before the accuracy score compared exact values (#1199)
//! is not comparable on the rules that read accuracy (#1237).
//!
//! `python/tests/test_accuracy_comparison.py` is the Python twin.

use std::io::Write;

use dataprof::{
    Check, CheckCode, CheckStatus, GateResult, NotEvaluated, ProfileReport, Profiler,
    QualityDimension, QualityPolicy, ReportSide, Verdict,
};

/// The bytes `tests/fixtures/accuracy_0_13_1.report.json` was profiled from
/// with the published 0.13.1 wheel. The first value breaks the percentage
/// range only when compared by its digits, so the current release scores
/// accuracy 75 where 0.13.1 scored 100.
const BATCH: &str = "discount_percent\n100.000000000000000001\n100\n0\n50\n";

fn profile() -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(BATCH.as_bytes()).unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

fn gate(report: &ProfileReport, baseline: &ProfileReport) -> GateResult {
    QualityPolicy::new()
        .max_quality_score_drop(0.0)
        .max_dimension_score_drop(QualityDimension::Accuracy, 0.0)
        .max_dimension_score_drop(QualityDimension::Completeness, 0.0)
        .evaluate_against(report, &ReportSide::from_report(baseline))
        .unwrap()
}

fn check<'a>(result: &'a GateResult, code: CheckCode, dimension: Option<&str>) -> &'a Check {
    result
        .checks
        .iter()
        .find(|check| {
            check.code == code
                && dimension.is_none_or(|name| {
                    check.dimension.map(|d| d.to_string()).as_deref() == Some(name)
                })
        })
        .unwrap_or_else(|| panic!("no {code} check for {dimension:?}"))
}

#[test]
fn a_0_13_1_baseline_leaves_the_accuracy_reading_rules_not_comparable() {
    let baseline: ProfileReport = serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/accuracy_0_13_1.report.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let report = profile();
    // The fixture is what the issue describes: the same bytes, scored higher.
    assert_eq!(
        baseline.quality_score().unwrap(),
        98.44,
        "fixture overall score"
    );
    assert!(report.quality_score().unwrap() < 98.44);

    let result = gate(&report, &baseline);
    let mut wrong = Vec::new();
    for (code, dimension) in [
        (CheckCode::MaxQualityScoreDrop, None),
        (CheckCode::MaxDimensionScoreDrop, Some("accuracy")),
    ] {
        let check = check(&result, code, dimension);
        if !matches!(
            check.not_evaluated(),
            Some(NotEvaluated::DefinitionNotComparable { definition, .. })
                if definition == "accuracy_comparison"
        ) {
            wrong.push(format!("{code:?} {dimension:?}: {check:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    assert_eq!(
        check(
            &result,
            CheckCode::MaxDimensionScoreDrop,
            Some("completeness")
        )
        .status,
        CheckStatus::Passed
    );
    assert_eq!(result.verdict, Verdict::Inconclusive);
}

#[test]
fn a_baseline_from_the_same_build_decides_the_accuracy_reading_rules() {
    let baseline = profile();
    let result = gate(&profile(), &baseline);
    for check in &result.checks {
        assert_eq!(check.status, CheckStatus::Passed, "{check:?}");
    }
    assert_eq!(result.verdict, Verdict::Pass);
}
