//! Each file of `tests/fixtures/release_baselines/` gated against the report
//! the last release wrote for it, with no score drop allowed (#1245).
//!
//! A fix that moves a score on these unchanged bytes has to record a
//! `metric_semantics` definition, so that a gate against an older baseline
//! reads `definition_not_comparable` instead of failing on identical data
//! (#1237, #1243 were found by hand after merge). Here that omission fails in
//! the fix's own PR. After each release,
//! `.github/scripts/release_baselines.py <version>` rewrites the reports from
//! the published wheel; see the release section of `docs/CONTRIBUTING.md`.
//!
//! `python/tests/test_release_baselines.py` is the Python twin.

use std::path::{Path, PathBuf};

use dataprof::{
    Check, CheckStatus, NotEvaluated, ProfileReport, Profiler, QualityDimension, QualityPolicy,
    ReportSide,
};

fn corpus() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/release_baselines")
}

/// The corpus inputs, each beside the report the release saved for it. A
/// build without the `parquet` feature cannot read the Parquet input.
fn inputs() -> Vec<PathBuf> {
    let mut inputs: Vec<PathBuf> = std::fs::read_dir(corpus())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name != "RELEASE"
                && !name.ends_with(".report.json")
                && (cfg!(feature = "parquet") || !name.ends_with(".parquet"))
        })
        .collect();
    inputs.sort();
    inputs
}

/// A dimension's score, or the overall score for `None`.
fn score(report: &ProfileReport, dimension: Option<QualityDimension>) -> Option<f64> {
    let scores = report.quality.as_ref()?.scores();
    match dimension {
        Some(dimension) => scores.dimension_scores[&dimension.to_string()],
        None => scores.overall_score,
    }
}

fn shown(score: Option<f64>) -> String {
    score.map_or("-".into(), |score| format!("{score:.2}"))
}

/// Why a check is not a sign of a score moving without a definition, or
/// `None` when it is one.
fn accounted_for(
    check: &Check,
    report: &ProfileReport,
    baseline: &ProfileReport,
) -> Option<&'static str> {
    if check.status == CheckStatus::Passed {
        return Some("passed");
    }
    match check.not_evaluated()? {
        NotEvaluated::DefinitionNotComparable { .. } => Some("definition_not_comparable"),
        NotEvaluated::SettingChanged { .. } | NotEvaluated::SettingUnrecorded { .. } => {
            Some("setting")
        }
        // A dimension neither release scores on this file has nothing to move.
        NotEvaluated::NotAssessed | NotEvaluated::NotInBaseline
            if check.dimension.is_some()
                && score(report, check.dimension).is_none()
                && score(baseline, check.dimension).is_none() =>
        {
            Some("not_assessed")
        }
        _ => None,
    }
}

#[test]
fn no_score_moves_against_the_last_release_without_a_definition() {
    let release = std::fs::read_to_string(corpus().join("RELEASE")).unwrap();
    let release = release.trim();
    let policy = QualityDimension::all().into_iter().fold(
        QualityPolicy::new().max_quality_score_drop(0.0),
        |policy, dimension| policy.max_dimension_score_drop(dimension, 0.0),
    );
    let inputs = inputs();
    let expected = if cfg!(feature = "parquet") { 6 } else { 5 };
    assert!(
        inputs.len() >= expected,
        "corpus has {} inputs",
        inputs.len()
    );

    let mut wrong = Vec::new();
    for input in &inputs {
        let name = input.file_name().unwrap().to_string_lossy();
        let mut stored = input.clone().into_os_string();
        stored.push(".report.json");
        let baseline: ProfileReport =
            serde_json::from_str(&std::fs::read_to_string(&stored).unwrap()).unwrap();
        let report = Profiler::new().analyze_file(input).unwrap();
        let result = policy
            .evaluate_against(&report, &ReportSide::from_report(&baseline))
            .unwrap();
        let mut decided = 0;
        for check in &result.checks {
            match accounted_for(check, &report, &baseline) {
                Some("passed") => decided += 1,
                Some(_) => {}
                None => wrong.push(format!(
                    "{name}: {} {}: {:?}, {} -> {}: {}",
                    check.code,
                    check
                        .dimension
                        .map_or("overall".to_string(), |d| d.to_string()),
                    check.status,
                    shown(score(&baseline, check.dimension)),
                    shown(score(&report, check.dimension)),
                    check.message,
                )),
            }
        }
        eprintln!(
            "{name}: {decided} of {} rules decided against {release}",
            result.checks.len()
        );
    }
    assert!(
        wrong.is_empty(),
        "scores moved against the {release} reports with no metric_semantics definition \
         recording why; add one to MetricSemantics::current() and score_definitions:\n{}",
        wrong.join("\n")
    );
}
