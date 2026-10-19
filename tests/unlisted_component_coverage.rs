//! A quality component computed with signal must be listed as exact or as
//! sampled in the recorded confidence. One listed in neither has no recorded
//! coverage, and the quality gate, the report delta and findings all read it
//! as `coverage_unrecorded` rather than as exact (#749).
//!
//! `python/tests/test_unlisted_component_coverage.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckCode, EvidenceGap, FindingCode, NotEvaluatedReason, ProfileReport, Profiler,
    QualityDimension, QualityPolicy, ReportDelta,
};
use serde_json::{Value, json};

const DATED: &str = "event_date,v\n2026-01-01,1\n2026-01-01,2\n2026-01-01,3\n2026-01-01,4\n";

fn profile(contents: &str) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

fn round_trip(report: &ProfileReport) -> ProfileReport {
    serde_json::from_value(serde_json::to_value(report).unwrap()).unwrap()
}

/// Every report a release writes lists each component it computed, so the
/// rule downgrades none of them. Includes the case where a component is
/// legitimately in neither list: `key_uniqueness` with no identified key.
#[test]
fn a_report_this_build_writes_lists_every_component() {
    let inputs = [
        ("dated", DATED),
        ("clean", "id,amount\n1,10\n2,20\n3,30\n4,40\n"),
        ("a null in every column", "a,b\n1,\n,x\n3,y\n4,z\n"),
    ];
    let mut wrong = Vec::new();
    for (name, contents) in inputs {
        let reloaded = round_trip(&profile(contents));
        let unlisted = reloaded.quality.as_ref().unwrap().unlisted_components();
        if !unlisted.is_empty() {
            wrong.push(format!("{name}: {unlisted:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn a_component_listed_nowhere_reads_unrecorded_in_every_reader() {
    // Every computed component is listed except timeliness, and no label is
    // unknown: the rule does not depend on a later release's labels.
    let mut document: Value = serde_json::to_value(profile(DATED)).unwrap();
    document["quality"]["confidence"] = json!({"Mixed": {
        "exact_dimensions": ["completeness", "duplicate_rows"],
        "sample_size": 4,
        "sampled_dimensions": ["consistency", "accuracy", "validity", "precision"],
    }});
    let report: ProfileReport = serde_json::from_value(document).unwrap();
    assert_eq!(
        report.quality.as_ref().unwrap().unlisted_components(),
        ["timeliness"]
    );

    let unrecorded = Some(EvidenceGap::CoverageUnrecorded);
    let gate = |policy: QualityPolicy, code: CheckCode| {
        let result = policy.evaluate(&report).unwrap();
        let check = result
            .checks
            .iter()
            .find(|check| check.code == code)
            .unwrap();
        check.evidence.gap()
    };
    let delta = ReportDelta::between(&report, &report);
    let delta_gap = |dimension: QualityDimension| {
        delta
            .dimensions
            .iter()
            .find(|change| change.dimension == dimension)
            .unwrap()
            .score
            .evidence
            .a
            .gap()
    };
    let future_dates = report
        .findings()
        .not_evaluated
        .into_iter()
        .find(|rule| rule.code == FindingCode::FutureDates)
        .map(|rule| rule.reason);

    let observed = [
        (
            "gate timeliness",
            gate(
                QualityPolicy::new().min_dimension_score(QualityDimension::Timeliness, 1.0),
                CheckCode::MinDimensionScore,
            ),
            unrecorded,
        ),
        (
            "gate overall",
            gate(
                QualityPolicy::new().min_quality_score(1.0),
                CheckCode::MinQualityScore,
            ),
            unrecorded,
        ),
        (
            "gate completeness",
            gate(
                QualityPolicy::new().min_dimension_score(QualityDimension::Completeness, 1.0),
                CheckCode::MinDimensionScore,
            ),
            None,
        ),
        (
            "delta timeliness",
            delta_gap(QualityDimension::Timeliness),
            unrecorded,
        ),
        (
            "delta overall",
            delta.quality_score.evidence.a.gap(),
            unrecorded,
        ),
        (
            "delta consistency",
            delta_gap(QualityDimension::Consistency),
            Some(EvidenceGap::QualitySampled),
        ),
    ];
    let mut wrong: Vec<String> = observed
        .into_iter()
        .filter(|(_, got, expected)| got != expected)
        .map(|(reader, got, expected)| format!("{reader} read {got:?}, expected {expected:?}"))
        .collect();
    if future_dates != Some(NotEvaluatedReason::Unrecorded) {
        wrong.push(format!(
            "findings future_dates read {future_dates:?}, expected Unrecorded"
        ));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
