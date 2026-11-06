//! A flat summary records `exact_dimensions` beside `sampled_dimensions`, so
//! its reader applies the full document's rule: a component carrying signal
//! that neither list names has unrecorded coverage (#1027, rule 2 of #749).
//! A summary written before the field reads as before, "not sampled" as exact.
//!
//! `python/tests/test_summary_exact_dimensions.py` is the Python twin; it also
//! covers findings and the `from_dict()` round trip.

use std::io::Write;

use dataprof::{
    EvidenceGap, ProfileReport, Profiler, QualityDimension, QualityPolicy, ReportDelta, ReportSide,
};
use serde_json::{Value, json};

const DATED: &str = "event_date,v\n2026-01-01,1\n2026-01-01,2\n2026-01-01,3\n2026-01-01,4\n";

fn profile(contents: &str) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

fn summary(report: &ProfileReport) -> Value {
    serde_json::from_str(&report.summary_json().unwrap()).unwrap()
}

fn side(document: &Value) -> ReportSide {
    ReportSide::from_summary_json(&document.to_string()).unwrap()
}

/// The gap of each dimension's evidence and of the overall score's, as a
/// comparison of the side with itself reads it.
fn delta_gaps(side: &ReportSide) -> Vec<(String, Option<EvidenceGap>)> {
    let delta = ReportDelta::between_sides(side, side);
    let mut gaps = vec![("overall".to_string(), delta.quality_score.evidence.a.gap())];
    gaps.extend(
        delta
            .dimensions
            .iter()
            .map(|change| (change.dimension.to_string(), change.score.evidence.a.gap())),
    );
    gaps
}

/// The status and gap of a `max_dimension_score_drop` on `dimension` with the
/// summary as the baseline.
fn drop_outcome(
    current: &ProfileReport,
    baseline: &ReportSide,
    dimension: QualityDimension,
) -> String {
    let result = QualityPolicy::new()
        .max_dimension_score_drop(dimension, 1.0)
        .evaluate_against(current, baseline)
        .unwrap();
    let [check] = result.checks.as_slice() else {
        panic!("one check expected, got {:?}", result.checks);
    };
    let document = serde_json::to_value(check).unwrap();
    let text = |key: &str| document[key].as_str().unwrap_or_default().to_string();
    match text("status").as_str() {
        "not_evaluated" => format!("{}:{}", text("reason"), text("gap")),
        status => status.to_string(),
    }
}

/// Every summary this build writes records both lists, names every
/// component it computed, and reads with the evidence of the report it came
/// from.
#[test]
fn a_summary_this_build_writes_reads_as_its_report() {
    let inputs = [
        ("dated", DATED),
        ("clean", "id,amount\n1,10\n2,20\n3,30\n4,40\n"),
        // Every column holds a null, so the confidence is Mixed (#788).
        ("a null in every column", "a,b\n1,\n,x\n3,y\n4,z\n"),
        // Text only: accuracy, timeliness and precision assess nothing, and
        // the document drops them.
        ("text only", "name\nada\nbob\n"),
        ("header only", "a,b\n"),
    ];
    let mut wrong = Vec::new();
    for (name, contents) in inputs {
        let report = profile(contents);
        let document = summary(&report);
        if !document["quality"]["exact_dimensions"].is_array() {
            wrong.push(format!(
                "{name}: no exact_dimensions in {}",
                document["quality"]
            ));
            continue;
        }
        // The report read back from its document writes the same list.
        let reloaded: ProfileReport =
            serde_json::from_value(serde_json::to_value(&report).unwrap()).unwrap();
        let reloaded_exact = summary(&reloaded)["quality"]["exact_dimensions"].clone();
        if reloaded_exact != document["quality"]["exact_dimensions"] {
            wrong.push(format!(
                "{name}: report {}, reloaded {reloaded_exact}",
                document["quality"]["exact_dimensions"]
            ));
        }
        let summary_side = side(&document);
        if !summary_side.unlisted_components().is_empty() {
            wrong.push(format!(
                "{name}: unlisted {:?}",
                summary_side.unlisted_components()
            ));
        }
        let from_report = delta_gaps(&ReportSide::from_report(&report));
        let from_summary = delta_gaps(&summary_side);
        if from_report != from_summary {
            wrong.push(format!(
                "{name}: report {from_report:?}, summary {from_summary:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The lists name every computed component except timeliness. With the exact
/// list recorded, every summary reader reads timeliness, and the overall
/// score that weighs it, as unrecorded; without it, as exact.
#[test]
fn a_component_listed_nowhere_in_a_summary_reads_unrecorded() {
    let report = profile(DATED);
    let mut recorded = summary(&report);
    recorded["quality"]["exact_dimensions"] = json!(["completeness", "duplicate_rows"]);
    recorded["quality"]["sampled_dimensions"] =
        json!(["consistency", "accuracy", "validity", "precision"]);
    let mut legacy = recorded.clone();
    legacy["quality"]
        .as_object_mut()
        .unwrap()
        .remove("exact_dimensions");

    let unrecorded = Some(EvidenceGap::CoverageUnrecorded);
    let mut wrong = Vec::new();
    for (name, document, timeliness, overall, drop) in [
        (
            "recorded",
            &recorded,
            unrecorded,
            unrecorded,
            "evidence_incomplete:coverage_unrecorded",
        ),
        (
            "legacy",
            &legacy,
            None,
            Some(EvidenceGap::QualitySampled),
            "passed",
        ),
    ] {
        let baseline = side(document);
        let expected_unlisted: &[&str] = if timeliness.is_some() {
            &["timeliness"]
        } else {
            &[]
        };
        if baseline.unlisted_components() != expected_unlisted {
            wrong.push(format!(
                "{name}: unlisted {:?}",
                baseline.unlisted_components()
            ));
        }
        let gaps = delta_gaps(&baseline);
        let gap = |label: &str| gaps.iter().find(|(name, _)| name == label).unwrap().1;
        for (label, expected) in [
            ("timeliness", timeliness),
            ("overall", overall),
            ("completeness", None),
            ("consistency", Some(EvidenceGap::QualitySampled)),
        ] {
            if gap(label) != expected {
                wrong.push(format!(
                    "{name}: compare {label} read {:?}, expected {expected:?}",
                    gap(label)
                ));
            }
        }
        let got = drop_outcome(&report, &baseline, QualityDimension::Timeliness);
        if got != drop {
            wrong.push(format!(
                "{name}: timeliness drop rule {got}, expected {drop}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Without the weighed dimensions, only "nothing was sampled" settles the
/// overall score; a component listed nowhere leaves it unrecorded too.
#[test]
fn an_unlisted_component_leaves_an_unweighed_overall_score_unrecorded() {
    let mut document = summary(&profile(DATED));
    document["quality"]["assessed_dimensions"] = json!([]);
    document["quality"]["sampled_dimensions"] = json!([]);
    let mut overall = |exact: Value| {
        document["quality"]["exact_dimensions"] = exact;
        delta_gaps(&side(&document))[0].1
    };
    let all = overall(json!([
        "completeness",
        "consistency",
        "duplicate_rows",
        "accuracy",
        "timeliness",
        "validity",
        "precision"
    ]));
    let without_timeliness = overall(json!([
        "completeness",
        "consistency",
        "duplicate_rows",
        "accuracy",
        "validity",
        "precision"
    ]));
    assert_eq!(
        (all, without_timeliness),
        (None, Some(EvidenceGap::CoverageUnrecorded))
    );
}
