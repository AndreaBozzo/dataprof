//! A report names the weights behind its overall score, default weights
//! included (#749).
//!
//! A stored overall score is kept rather than recomputed, so a report that
//! omits default weights only says "the defaults of the release that reads
//! me". If a later release changed the defaults, the score and its decoded
//! weights would disagree. Both the canonical document and the flat summary
//! now write the full weight vector.
//!
//! `python/tests/test_score_weights_recorded.py` is the Python twin, and also
//! covers re-exporting a flat summary that did not record its weights.

use std::io::Write;

use dataprof::{ProfileReport, Profiler, QualityScoreWeights};
use serde_json::Value;

const CLEAN: &str = "id,amount\n1,10\n2,20\n3,30\n4,40\n";

fn profile() -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(CLEAN.as_bytes()).unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

#[test]
fn default_weights_are_written_in_every_document() {
    let report = profile();
    let defaults = serde_json::to_value(QualityScoreWeights::default()).unwrap();
    let canonical = serde_json::to_value(&report).unwrap();
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    let reloaded: ProfileReport = serde_json::from_value(canonical.clone()).unwrap();
    let rewritten = serde_json::to_value(&reloaded).unwrap();

    let documents = [
        (
            "canonical",
            &canonical["quality"]["metrics"]["score_weights"],
        ),
        ("summary", &summary["quality"]["score_weights"]),
        (
            "canonical after a round trip",
            &rewritten["quality"]["metrics"]["score_weights"],
        ),
    ];
    let wrong: Vec<String> = documents
        .into_iter()
        .filter(|(_, weights)| **weights != defaults)
        .map(|(name, weights)| format!("{name}: {weights}"))
        .collect();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
