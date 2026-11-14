//! The baseline gate's score drop rules on sampled scores (#1035): under
//! `full_source`, a score computed over a retained sample is read as its
//! recorded whole-source interval, and the drop is decided when every change
//! the two sides' intervals allow lies on one side of the allowance.
//!
//! `python/tests/test_score_drop_intervals.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CheckCode, CheckStatus, GateResult, PolicyScope, ProfileReport, Profiler, QualityDimension,
    QualityPolicy, ReportSide, StopCondition,
};
use serde_json::Value;

/// A CSV past the 10,000-value quality reservoir, so its consistency,
/// accuracy, timeliness, validity and precision scores are sampled. Every
/// `bad_every`-th row breaks three columns at once.
fn csv(rows: usize, bad_every: usize) -> String {
    let mut text = String::from("id,amount,code,when\n");
    for i in 0..rows {
        if i % bad_every == 0 {
            text.push_str(&format!("{i},n/a,x,notadate\n"));
        } else {
            text.push_str(&format!(
                "{i},{}.{:02},{:05},2025-01-{:02}\n",
                (i * 37) % 1000,
                i % 100,
                i % 100_000,
                1 + i % 28
            ));
        }
    }
    text
}

fn profile(rows: usize, bad_every: usize, cap: Option<u64>) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(csv(rows, bad_every).as_bytes()).unwrap();
    let profiler = match cap {
        Some(cap) => Profiler::new().stop_when(StopCondition::MaxRows(cap)),
        None => Profiler::new(),
    };
    profiler.analyze_file(file.path()).unwrap()
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

/// The check's status, with the gap when it was not evaluated.
fn status_of(result: &GateResult, code: CheckCode) -> String {
    let check = result
        .checks
        .iter()
        .find(|check| check.code == code)
        .unwrap_or_else(|| panic!("a {code} check"));
    match &check.status {
        CheckStatus::Passed => "passed".to_string(),
        CheckStatus::Failed => "failed".to_string(),
        CheckStatus::NotEvaluated(reason) => {
            let reason = serde_json::to_value(reason).unwrap();
            match reason.get("gap") {
                Some(gap) => format!("{}:{}", reason["reason"].as_str().unwrap(), gap),
                None => reason["reason"].as_str().unwrap().to_string(),
            }
        }
        _ => unreachable!(),
    }
}

fn check_json(result: &GateResult, code: CheckCode) -> Value {
    let check = result
        .checks
        .iter()
        .find(|check| check.code == code)
        .unwrap();
    serde_json::to_value(check).unwrap()
}

const INCONCLUSIVE: &str = "evidence_incomplete:\"quality_sampled\"";

/// Sampled scores on both sides, from every baseline form, for the overall
/// score and one dimension. Before #1035 every one of these was
/// `evidence_incomplete`.
#[test]
fn a_sampled_score_drop_is_decided_on_both_sides_intervals() {
    let baseline = profile(25_000, 50, None);
    let same = profile(25_000, 50, None);
    let worse = profile(25_000, 5, None);
    // (case, report, allowance, overall, consistency)
    let cases = [
        // The intervals overlap, so no allowance of zero is settled.
        (
            "unchanged, no drop allowed",
            &same,
            0.0,
            INCONCLUSIVE,
            INCONCLUSIVE,
        ),
        ("unchanged, 5 points", &same, 5.0, "passed", "passed"),
        (
            "a tenth of the rows broken, 5 points",
            &worse,
            5.0,
            "failed",
            "failed",
        ),
        (
            "a tenth of the rows broken, 30 points",
            &worse,
            30.0,
            "passed",
            "passed",
        ),
    ];
    let mut wrong = Vec::new();
    for (form, side) in baselines(&baseline) {
        for (case, report, points, overall, consistency) in &cases {
            let result = QualityPolicy::new()
                .max_quality_score_drop(*points)
                .max_dimension_score_drop(QualityDimension::Consistency, *points)
                .evaluate_against(report, &side)
                .unwrap();
            let got = (
                status_of(&result, CheckCode::MaxQualityScoreDrop),
                status_of(&result, CheckCode::MaxDimensionScoreDrop),
            );
            if got != (overall.to_string(), consistency.to_string()) {
                wrong.push(format!(
                    "{form}/{case}: {got:?}, expected ({overall}, {consistency})"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The intervals a decision read are in the result: the current side's in
/// `bounds`, the baseline's in `baseline.bounds`. A side read in full is a
/// point and carries none.
#[test]
fn the_result_carries_the_intervals_a_drop_was_decided_on() {
    let sampled = profile(25_000, 50, None);
    let small = profile(2_000, 50, None);
    let mut wrong = Vec::new();
    // (case, report, baseline, current interval, baseline interval)
    let cases = [
        ("both sampled", &sampled, &sampled, true, true),
        ("baseline read in full", &sampled, &small, true, false),
        ("report read in full", &small, &sampled, false, true),
    ];
    for (case, report, baseline, current_bounds, baseline_bounds) in cases {
        for (form, side) in baselines(baseline) {
            let result = QualityPolicy::new()
                .max_quality_score_drop(50.0)
                .evaluate_against(report, &side)
                .unwrap();
            let check = check_json(&result, CheckCode::MaxQualityScoreDrop);
            let got = (
                check["status"].as_str().unwrap().to_string(),
                check.get("bounds").is_some(),
                check["baseline"].get("bounds").is_some(),
            );
            if got != ("passed".to_string(), current_bounds, baseline_bounds) {
                wrong.push(format!(
                    "{case}/{form}: {got:?}, expected (passed, {current_bounds}, \
                     {baseline_bounds})"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The interval stands for a fully read source only: a capped scan keeps its
/// own gap, and an observed scope decides on the sampled scores themselves.
#[test]
fn only_a_full_source_drop_over_fully_read_sources_reads_the_intervals() {
    let baseline = profile(25_000, 50, None);
    let same = profile(25_000, 50, None);
    let capped = profile(25_000, 50, Some(20_000));
    let mut wrong = Vec::new();
    for (form, side) in baselines(&baseline) {
        let truncated = QualityPolicy::new()
            .max_quality_score_drop(5.0)
            .evaluate_against(&capped, &side)
            .unwrap();
        let got = status_of(&truncated, CheckCode::MaxQualityScoreDrop);
        if got != "evidence_incomplete:\"truncated\"" {
            wrong.push(format!("{form}/capped scan: {got}"));
        }
        let observed = QualityPolicy::new()
            .max_quality_score_drop(0.0)
            .scope(PolicyScope::Observed)
            .evaluate_against(&same, &side)
            .unwrap();
        let check = check_json(&observed, CheckCode::MaxQualityScoreDrop);
        if check["status"] != "passed" || check.get("bounds").is_some() {
            wrong.push(format!("{form}/observed: {check}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A summary from a later release can list a sampled component this build
/// does not know. Its overall score's coverage is unrecorded, and its stored
/// bounds are not read, as the minimum score rule does not read them.
#[test]
fn stored_bounds_on_a_score_with_unrecorded_coverage_are_not_read() {
    let baseline = profile(25_000, 50, None);
    let mut summary: Value = serde_json::from_str(&baseline.summary_json().unwrap()).unwrap();
    summary["quality"]["sampled_dimensions"]
        .as_array_mut()
        .unwrap()
        .push(Value::from("a_later_component"));
    assert!(summary["quality"]["score_bounds"]["overall_score"].is_object());
    let side = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    let result = QualityPolicy::new()
        .max_quality_score_drop(5.0)
        .evaluate_against(&profile(25_000, 50, None), &side)
        .unwrap();
    assert_eq!(
        status_of(&result, CheckCode::MaxQualityScoreDrop),
        "evidence_incomplete:\"coverage_unrecorded\""
    );
}

/// Bounds a summary records are refused when they could not be bounds, as
/// the full document's reader refuses them.
#[test]
fn a_summary_with_malformed_score_bounds_is_refused() {
    let baseline = profile(25_000, 50, None);
    let summary: Value = serde_json::from_str(&baseline.summary_json().unwrap()).unwrap();
    let mut wrong = Vec::new();
    for (case, path, value) in [
        (
            "reversed",
            "/quality/score_bounds/overall_score/lower",
            99.5,
        ),
        (
            "above 100",
            "/quality/score_bounds/overall_score/upper",
            100.5,
        ),
        ("certain", "/quality/score_bounds/confidence_level", 1.0),
    ] {
        let mut edited = summary.clone();
        *edited.pointer_mut(path).unwrap() = Value::from(value);
        if ReportSide::from_summary_json(&edited.to_string()).is_ok() {
            wrong.push(case);
        }
    }
    assert!(wrong.is_empty(), "accepted: {wrong:?}");
}
