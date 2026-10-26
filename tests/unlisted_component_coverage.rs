//! A quality component computed with signal must be listed as exact or as
//! sampled in the recorded confidence. One listed in neither has no recorded
//! coverage, and the quality gate, the report delta and findings all read it
//! as `coverage_unrecorded` rather than as exact (#749).
//!
//! `python/tests/test_unlisted_component_coverage.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckCode, EvidenceGap, FindingCode, NotEvaluatedReason, ProfileReport, Profiler,
    QualityDimension, QualityPolicy, ReportDelta, ReportSide,
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

#[test]
fn flat_summary_records_exact_components_and_keeps_canonical_coverage() {
    let mut document = serde_json::to_value(profile(DATED)).unwrap();
    document["quality"]["confidence"] = json!({"Mixed": {
        "exact_dimensions": ["completeness", "duplicate_rows", "future_component"],
        "sampled_dimensions": ["consistency", "accuracy", "validity", "precision"],
        "sample_size": 4,
    }});
    let report: ProfileReport = serde_json::from_value(document).unwrap();
    let mut summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    assert_eq!(
        summary["quality"]["exact_dimensions"],
        json!(["completeness", "duplicate_rows", "future_component"])
    );
    let side = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    let flat = ReportDelta::between_sides(&side, &side);
    let canonical = ReportDelta::between(&report, &report);
    assert_eq!(
        flat.quality_score.evidence,
        canonical.quality_score.evidence
    );
    for (flat, canonical) in flat.dimensions.iter().zip(&canonical.dimensions) {
        assert_eq!(flat.score.evidence, canonical.score.evidence);
    }

    summary["quality"]
        .as_object_mut()
        .unwrap()
        .remove("exact_dimensions");
    let legacy = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    let delta = ReportDelta::between_sides(&legacy, &legacy);
    assert_eq!(
        delta
            .dimensions
            .iter()
            .find(|d| d.dimension == QualityDimension::Timeliness)
            .unwrap()
            .score
            .evidence
            .a
            .gap(),
        None
    );
}

#[test]
fn flat_summary_distinguishes_exact_sampled_and_unrecorded_components() {
    let original = serde_json::to_value(profile(DATED)).unwrap();
    for confidence in [
        json!("Exact"),
        json!({"Approximate": {"sample_size": 4, "population_size": 10}}),
        json!("Unrecorded"),
    ] {
        let mut document = original.clone();
        document["quality"]["confidence"] = confidence.clone();
        let report: ProfileReport = serde_json::from_value(document).unwrap();
        let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
        let quality = &summary["quality"];
        if confidence == json!("Exact") {
            let labels = quality["exact_dimensions"].as_array().unwrap();
            assert!(labels.contains(&json!("timeliness")));
            assert!(labels.contains(&json!("duplicate_rows")));
            assert!(!labels.contains(&json!("key_uniqueness")));
            assert_eq!(quality["sampled_dimensions"], json!([]));
        } else if confidence == json!("Unrecorded") {
            assert!(quality.get("exact_dimensions").is_none());
            assert!(quality.get("sampled_dimensions").is_none());
        } else {
            assert_eq!(quality["exact_dimensions"], json!([]));
            assert!(
                quality["sampled_dimensions"]
                    .as_array()
                    .unwrap()
                    .contains(&json!("timeliness"))
            );
        }
    }
    let mut report = profile(DATED);
    report.quality = Some(dataprof::QualityAssessment::exact(
        dataprof::QualityMetrics::default(),
    ));
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    assert_eq!(summary["quality"]["exact_dimensions"], json!([]));
}

#[test]
fn exact_components_do_not_include_unassessed_live_metric_defaults() {
    let report = profile("id,amount\n1,10\n2,20\n3,30\n");
    let restored = round_trip(&report);
    let exact = report.quality.as_ref().unwrap().exact_dimensions().unwrap();
    assert_eq!(
        exact,
        restored
            .quality
            .as_ref()
            .unwrap()
            .exact_dimensions()
            .unwrap()
    );
    assert!(!exact.contains(&"timeliness".to_string()));
    assert!(!exact.contains(&"validity".to_string()));
    assert!(!exact.contains(&"precision".to_string()));
}
