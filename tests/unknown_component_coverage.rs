//! A report can record a sampled quality component this build does not know:
//! a dimension a later release added, whose weight is in the stored overall
//! score. The quality gate and the report delta read that score's coverage
//! through one rule (#749), so a check never certifies a score the delta
//! calls unrecorded.
//!
//! `python/tests/test_unknown_component_coverage.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckCode, EvidenceGap, PolicyScope, ProfileReport, Profiler, QualityDimension, QualityPolicy,
    ReportDelta, Verdict,
};
use serde_json::{Value, json};

const CLEAN: &str = "id,amount\n1,10\n2,20\n3,30\n4,40\n";

/// Every component this build knows, listed as a profiling run lists them
/// when the quality sample feeds every dimension it can.
const EXACT: [&str; 3] = ["completeness", "key_uniqueness", "duplicate_rows"];
const SAMPLED: [&str; 5] = [
    "consistency",
    "accuracy",
    "timeliness",
    "validity",
    "precision",
];

/// The CSV's report, reloaded with every known component listed and the
/// given labels added to each list.
fn reloaded(extra_exact: &[&str], extra_sampled: &[&str]) -> ProfileReport {
    let exact: Vec<&str> = EXACT.iter().chain(extra_exact).copied().collect();
    let sampled: Vec<&str> = SAMPLED.iter().chain(extra_sampled).copied().collect();
    with_confidence(&exact, &sampled)
}

/// The CSV's report, reloaded with exactly these component lists.
fn with_confidence(exact: &[&str], sampled: &[&str]) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(CLEAN.as_bytes()).unwrap();
    let report = Profiler::new().analyze_file(file.path()).unwrap();
    let mut document: Value = serde_json::to_value(&report).unwrap();
    document["quality"]["confidence"] = json!({"Mixed": {
        "exact_dimensions": exact,
        "sample_size": 4,
        "sampled_dimensions": sampled,
    }});
    serde_json::from_value(document).unwrap()
}

/// The gap the gate's check with `code` reports, `None` when it is complete.
fn gate_gap(report: &ProfileReport, policy: QualityPolicy, code: CheckCode) -> Option<EvidenceGap> {
    let result = policy.evaluate(report).unwrap();
    let check = result
        .checks
        .iter()
        .find(|check| check.code == code)
        .expect("the policy asked for this check");
    check.evidence.gap()
}

#[test]
fn the_gate_and_the_delta_resolve_component_coverage_alike() {
    let unrecorded = Some(EvidenceGap::CoverageUnrecorded);
    let sampled = Some(EvidenceGap::QualitySampled);
    // (case, added to exact, added to sampled, overall gap, completeness gap)
    let cases: [(&str, &[&str], &[&str], _, _); 3] = [
        ("as written", &[], &[], sampled, None),
        (
            "unknown sampled",
            &[],
            &["future_dimension"],
            unrecorded,
            None,
        ),
        ("unknown exact", &["future_dimension"], &[], sampled, None),
    ];

    let mut wrong = Vec::new();
    for (case, exact, sampled, overall, completeness) in cases {
        let report = reloaded(exact, sampled);
        let delta = ReportDelta::between(&report, &report);
        let observed = [
            (
                "gate overall",
                gate_gap(
                    &report,
                    QualityPolicy::new().min_quality_score(1.0),
                    CheckCode::MinQualityScore,
                ),
                overall,
            ),
            (
                "delta overall",
                delta.quality_score.evidence.a.gap(),
                overall,
            ),
            (
                "gate completeness",
                gate_gap(
                    &report,
                    QualityPolicy::new().min_dimension_score(QualityDimension::Completeness, 1.0),
                    CheckCode::MinDimensionScore,
                ),
                completeness,
            ),
        ];
        for (reader, got, expected) in observed {
            if got != expected {
                wrong.push(format!(
                    "{case}: {reader} read {got:?}, expected {expected:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The unrecorded overall score leaves a full-source minimum unevaluated, and
/// a policy about what was observed still decides it. Every known component
/// is exact here, so only the unknown one can withhold the verdict.
#[test]
fn an_unknown_sampled_component_makes_a_full_source_minimum_inconclusive() {
    let every_known: Vec<&str> = EXACT.iter().chain(&SAMPLED).copied().collect();
    let report = with_confidence(&every_known, &["future_dimension"]);

    let full = QualityPolicy::new()
        .min_quality_score(10.0)
        .evaluate(&report)
        .unwrap();
    assert_eq!(full.verdict, Verdict::Inconclusive);

    let observed = QualityPolicy::new()
        .min_quality_score(10.0)
        .scope(PolicyScope::Observed)
        .evaluate(&report)
        .unwrap();
    assert_eq!(observed.verdict, Verdict::Pass);
}
