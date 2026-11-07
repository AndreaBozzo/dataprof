//! A baseline saved before JSON numbers were read from their literal digits
//! (#1166) is not comparable on the rules that read them (#1243).
//!
//! `python/tests/test_json_number_reading.py` is the Python twin.

use std::io::Write;

use dataprof::{
    Check, CheckCode, CheckStatus, GateResult, NotEvaluated, ProfileReport, Profiler,
    QualityDimension, QualityPolicy, ReportSide, Verdict,
};

/// The bytes `tests/fixtures/json_number_reading_0_13_1.report.json` was
/// profiled from with the published 0.13.1 wheel. 0.13.1 read the three long
/// amounts as one `f64` with no decimals, so it scored precision 100 and found
/// two duplicate rows; their digits score precision 75 and no duplicates.
const BATCH: &str = concat!(
    "{\"amount\":1000000000000000.01,\"n\":1}\n",
    "{\"amount\":1000000000000000.02,\"n\":1}\n",
    "{\"amount\":1000000000000000.03,\"n\":1}\n",
    "{\"amount\":7,\"n\":2}\n",
);
const READING: [QualityDimension; 2] = [QualityDimension::Precision, QualityDimension::Uniqueness];
const DECIDED: [QualityDimension; 2] = [
    QualityDimension::Completeness,
    QualityDimension::Consistency,
];

fn profile() -> ProfileReport {
    let mut file = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .unwrap();
    file.write_all(BATCH.as_bytes()).unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

fn gate(report: &ProfileReport, baseline: &ProfileReport) -> GateResult {
    READING
        .iter()
        .chain(&DECIDED)
        .chain(&[QualityDimension::Accuracy])
        .fold(
            QualityPolicy::new().max_quality_score_drop(0.0),
            |policy, &dimension| policy.max_dimension_score_drop(dimension, 0.0),
        )
        .evaluate_against(report, &ReportSide::from_report(baseline))
        .unwrap()
}

fn check(result: &GateResult, code: CheckCode, dimension: Option<QualityDimension>) -> &Check {
    result
        .checks
        .iter()
        .find(|check| check.code == code && check.dimension == dimension)
        .unwrap_or_else(|| panic!("no {code} check for {dimension:?}"))
}

fn not_comparable_on(check: &Check) -> Option<&str> {
    match check.not_evaluated() {
        Some(NotEvaluated::DefinitionNotComparable { definition, .. }) => Some(definition),
        _ => None,
    }
}

#[test]
fn a_0_13_1_baseline_leaves_the_rules_reading_json_numbers_not_comparable() {
    let baseline: ProfileReport = serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/json_number_reading_0_13_1.report.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let report = profile();
    // The fixture is what the issue describes: the same bytes, scored
    // differently on precision and uniqueness.
    let score = |report: &ProfileReport, dimension: QualityDimension| {
        report
            .quality
            .as_ref()
            .and_then(|quality| quality.scores().dimension_scores[&dimension.to_string()])
    };
    assert_eq!(score(&baseline, QualityDimension::Precision), Some(100.0));
    assert_eq!(score(&report, QualityDimension::Precision), Some(75.0));
    assert_eq!(score(&baseline, QualityDimension::Uniqueness), Some(50.0));
    assert_eq!(score(&report, QualityDimension::Uniqueness), Some(100.0));

    let result = gate(&report, &baseline);
    let mut wrong = Vec::new();
    for dimension in READING {
        let check = check(&result, CheckCode::MaxDimensionScoreDrop, Some(dimension));
        if not_comparable_on(check) != Some("json_number_reading") {
            wrong.push(format!("{dimension}: {check:?}"));
        }
    }
    // 0.13.1 records neither definition, and these rules name the first one
    // they read, accuracy_comparison (#1237).
    for (code, dimension) in [
        (CheckCode::MaxQualityScoreDrop, None),
        (
            CheckCode::MaxDimensionScoreDrop,
            Some(QualityDimension::Accuracy),
        ),
    ] {
        let check = check(&result, code, dimension);
        if not_comparable_on(check).is_none() {
            wrong.push(format!("{code} {dimension:?}: {check:?}"));
        }
    }
    for dimension in DECIDED {
        let check = check(&result, CheckCode::MaxDimensionScoreDrop, Some(dimension));
        if check.status != CheckStatus::Passed {
            wrong.push(format!("{dimension}: {check:?}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    assert_eq!(result.verdict, Verdict::Inconclusive);
}

#[test]
fn a_baseline_from_the_same_build_decides_the_rules_reading_json_numbers() {
    let baseline = profile();
    let result = gate(&profile(), &baseline);
    for check in &result.checks {
        assert_eq!(check.status, CheckStatus::Passed, "{check:?}");
    }
    assert_eq!(result.verdict, Verdict::Pass);
}
