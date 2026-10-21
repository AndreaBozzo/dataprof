//! The typed report-to-report delta (#713).
//!
//! `python/tests/test_report_delta.py` is the Python twin, and also checks a
//! report loaded from a flat summary against the report it came from.

use std::io::Write;

use dataprof::{EvidenceGap, ProfileReport, Profiler, ReportDelta, ReportSide, StopCondition};
use serde_json::{Value, json};

fn csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn profile(contents: &str) -> ProfileReport {
    let file = csv(contents);
    Profiler::new().analyze_file(file.path()).unwrap()
}

fn document(delta: &ReportDelta) -> Value {
    serde_json::to_value(delta).unwrap()
}

const CLEAN: &str = "id,amount\n1,10\n2,20\n3,30\n4,40\n";
const WITH_NULLS: &str = "id,amount\n1,10\n2,\n3,\n4,40\n";

#[test]
fn identical_reports_give_explicit_zeros_for_what_was_assessed() {
    let report = profile(CLEAN);
    let delta = document(&ReportDelta::between(&report, &report));

    assert_eq!(delta["quality_score"]["abs"], json!(0.0));
    assert_eq!(delta["quality_score"]["rel_pct"], json!(0.0));
    for (name, change) in delta["dimensions"].as_object().unwrap() {
        if change["a"].is_null() {
            // Unassessed on both sides: no number to subtract.
            assert!(change["abs"].is_null(), "{name}");
        } else {
            assert_eq!(change["abs"], json!(0.0), "{name}");
        }
    }
    for change in delta["columns"].as_object().unwrap().values() {
        assert_eq!(change["null_pct_delta"], json!(0.0));
    }
    assert_eq!(delta["metric_semantics"]["comparable"], json!(true));
    assert_eq!(delta["schema"]["common"], json!(["id", "amount"]));
}

#[test]
fn every_change_is_b_minus_a() {
    let worse = profile(WITH_NULLS);
    let better = profile(CLEAN);
    let delta = document(&ReportDelta::between(&worse, &better));

    let completeness = &delta["dimensions"]["completeness"];
    let (a, b) = (
        completeness["a"].as_f64().unwrap(),
        completeness["b"].as_f64().unwrap(),
    );
    assert!(b > a, "the clean side scores higher");
    assert!(
        completeness["abs"].as_f64().unwrap() > 0.0,
        "improvement is positive"
    );
    assert_eq!(delta["columns"]["amount"]["null_pct_a"], json!(50.0));
    assert_eq!(delta["columns"]["amount"]["null_pct_b"], json!(0.0));
    assert_eq!(
        delta["columns"]["amount"]["null_pct_delta"],
        json!(-50.0),
        "fewer nulls is negative"
    );
}

#[test]
fn an_all_null_column_is_a_share_and_a_null_free_one_is_zero() {
    let empty_column = profile("id,note\n1,\n2,\n");
    let filled = profile("id,note\n1,a\n2,b\n");
    let delta = document(&ReportDelta::between(&empty_column, &filled));

    // The note column is all null: its share is 100%, a real number.
    assert_eq!(delta["columns"]["note"]["null_pct_a"], json!(100.0));
    assert_eq!(delta["columns"]["note"]["null_pct_delta"], json!(-100.0));

    let reversed = document(&ReportDelta::between(&filled, &empty_column));
    assert_eq!(reversed["columns"]["note"]["null_pct_a"], json!(0.0));
    assert_eq!(reversed["columns"]["note"]["null_pct_delta"], json!(100.0));
}

#[test]
fn a_report_with_nothing_assessed_invents_no_number() {
    let header_only = profile("id,amount\n");
    let report = profile(CLEAN);
    for delta in [
        document(&ReportDelta::between(&header_only, &report)),
        document(&ReportDelta::between(&report, &header_only)),
        document(&ReportDelta::between(&header_only, &header_only)),
    ] {
        assert!(delta["quality_score"]["abs"].is_null());
        assert!(delta["quality_score"]["rel_pct"].is_null());
        for change in delta["dimensions"].as_object().unwrap().values() {
            assert!(change["abs"].is_null());
            assert!(change["rel_pct"].is_null());
        }
        for change in delta["columns"].as_object().unwrap().values() {
            assert!(
                change["null_pct_delta"].is_null(),
                "an empty column has no null share"
            );
        }
    }
}

#[test]
fn schema_changes_keep_each_side_order() {
    let a = profile("id,old,shared\n1,x,y\n");
    let b = profile("shared,new,id\ny,z,1\n");
    let delta = document(&ReportDelta::between(&a, &b));

    assert_eq!(delta["schema"]["added"], json!(["new"]));
    assert_eq!(delta["schema"]["removed"], json!(["old"]));
    assert_eq!(delta["schema"]["common"], json!(["id", "shared"]));
    let columns: Vec<&String> = delta["columns"].as_object().unwrap().keys().collect();
    assert_eq!(columns, ["id", "old", "shared", "new"]);
    assert!(delta["columns"]["old"]["null_pct_b"].is_null());
    assert!(delta["columns"]["old"]["null_pct_delta"].is_null());
    assert!(delta["columns"]["new"]["null_pct_a"].is_null());
}

#[test]
fn a_truncated_side_says_so_on_every_number() {
    let rows: String = (0..200).map(|i| format!("{i},{}\n", i * 2)).collect();
    let file = csv(&format!("id,amount\n{rows}"));
    let exact = Profiler::new().analyze_file(file.path()).unwrap();
    let truncated = Profiler::new()
        .stop_when(StopCondition::MaxRows(50))
        .analyze_file(file.path())
        .unwrap();

    let delta = ReportDelta::between(&exact, &truncated);
    assert!(delta.reports.a.evidence.is_complete());
    assert_eq!(delta.reports.b.evidence.gap(), Some(EvidenceGap::Truncated));
    assert_eq!(delta.reports.b.rows_processed, Some(50));
    assert!(delta.quality_score.evidence.a.is_complete());
    assert_eq!(
        delta.quality_score.evidence.b.gap(),
        Some(EvidenceGap::Truncated)
    );
    for change in &delta.dimensions {
        assert_eq!(change.score.evidence.b.gap(), Some(EvidenceGap::Truncated));
    }
}

#[test]
fn a_summary_compares_like_the_report_it_came_from() {
    let a = profile(WITH_NULLS);
    let b = profile(CLEAN);
    let native = document(&ReportDelta::between(&a, &b));
    let side = |report: &ProfileReport| {
        ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap()
    };
    let flat = document(&ReportDelta::between_sides(&side(&a), &side(&b)));

    let mut masked = native.clone();
    for name in ["a", "b"] {
        // A flat summary records neither identity field.
        masked["reports"][name]["id"] = Value::Null;
        masked["reports"][name]["timestamp"] = Value::Null;
    }
    assert_eq!(flat, masked);
    assert_eq!(native["reports"]["a"]["id"], json!(a.id));
}

#[test]
fn a_summary_from_before_0_12_reads_as_unrecorded() {
    let legacy = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy_0_11_non_ascii.summary.json"),
    )
    .unwrap();
    let legacy = ReportSide::from_summary_json(&legacy).unwrap();
    let current = ReportSide::from_report(&profile("city\nOslo\n"));
    let delta = ReportDelta::between_sides(&legacy, &current);

    assert_eq!(delta.metric_semantics.comparable, None);
    assert!(delta.metric_semantics.a.is_none());
    // 0.11 did not record which quality components were sampled.
    assert_eq!(
        delta.quality_score.evidence.a.gap(),
        Some(EvidenceGap::CoverageUnrecorded)
    );
    assert!(delta.quality_score.evidence.b.is_complete());
}

#[test]
fn flat_summary_null_recorded_fields_fail_but_absence_is_legacy() {
    let report = profile(CLEAN);
    let complete: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    for field in ["metric_semantics", "schema_version", "analysis_settings"] {
        let mut malformed = complete.clone();
        malformed[field] = Value::Null;
        let error = ReportSide::from_summary_json(&malformed.to_string()).unwrap_err();
        assert!(
            error.to_string().contains(field),
            "{field}: error must name the null field, got {error}"
        );

        let mut legacy = complete.clone();
        legacy.as_object_mut().unwrap().remove(field);
        ReportSide::from_summary_json(&legacy.to_string())
            .unwrap_or_else(|error| panic!("absent {field} should remain valid: {error}"));
    }
}

#[test]
fn a_summary_without_columns_is_an_error() {
    let report = profile(CLEAN);
    let mut summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    summary.as_object_mut().unwrap().remove("columns");
    let error = ReportSide::from_summary_json(&summary.to_string()).unwrap_err();
    assert!(error.to_string().contains("columns"), "{error}");
}

/// The oldest flat summaries record only some of the execution fields. Python
/// writes the rest as null; a summary from elsewhere can omit them.
#[test]
fn a_summary_that_did_not_record_its_scan_reads_as_unrecorded() {
    let unrecorded = || Some(EvidenceGap::CoverageUnrecorded);
    for execution in [
        json!({"rows_processed": 5}),
        json!({"rows_processed": 5, "source_exhausted": null, "sampling_applied": null, "error_count": null}),
        json!({}),
    ] {
        let summary = json!({"source": "old.csv", "columns": [], "execution": execution});
        let side = ReportSide::from_summary_json(&summary.to_string()).unwrap();
        let delta = ReportDelta::between_sides(&side, &side);
        assert_eq!(delta.reports.a.evidence.gap(), unrecorded(), "{execution}");
        assert_eq!(delta.quality_score.evidence.a.gap(), unrecorded());
        assert!(delta.quality_score.abs.is_none());
    }

    let rows_only = json!({"source": "old.csv", "columns": [], "execution": {"rows_processed": 5}});
    let side = ReportSide::from_summary_json(&rows_only.to_string()).unwrap();
    assert_eq!(
        ReportDelta::between_sides(&side, &side)
            .reports
            .a
            .rows_processed,
        Some(5)
    );

    // A recorded gap is still named over an unrecorded fact.
    let truncated = json!({"source": "x", "columns": [], "execution": {"source_exhausted": false}});
    let side = ReportSide::from_summary_json(&truncated.to_string()).unwrap();
    assert_eq!(
        ReportDelta::between_sides(&side, &side)
            .reports
            .a
            .evidence
            .gap(),
        Some(EvidenceGap::Truncated)
    );
}

/// Without the weights behind the overall score, a sampled component may
/// reach it, so the score is not certified exact.
#[test]
fn an_overall_score_without_its_weights_is_not_certified() {
    let report = profile(CLEAN);
    let mut summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    summary["quality"]["sampled_dimensions"] = json!(["consistency"]);
    for assessed in [None, Some(json!([]))] {
        let quality = summary["quality"].as_object_mut().unwrap();
        match &assessed {
            None => {
                quality.remove("assessed_dimensions");
            }
            Some(list) => {
                quality.insert("assessed_dimensions".into(), list.clone());
            }
        }
        let side = ReportSide::from_summary_json(&summary.to_string()).unwrap();
        let delta = ReportDelta::between_sides(&side, &side);
        assert_eq!(
            delta.quality_score.evidence.a.gap(),
            Some(EvidenceGap::CoverageUnrecorded),
            "assessed_dimensions = {assessed:?}"
        );
    }

    // Nothing sampled settles it without the weights.
    summary["quality"]["sampled_dimensions"] = json!([]);
    let side = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    assert!(
        ReportDelta::between_sides(&side, &side)
            .quality_score
            .evidence
            .a
            .is_complete()
    );
}

/// A rebuilt report can repeat a column name. The delta lists it once and
/// reads its first profile, as `QualityPolicy` does.
#[test]
fn a_repeated_column_name_is_listed_once_with_its_first_profile() {
    let mut report = profile(CLEAN);
    let mut first = report.column_profiles[1].clone();
    let mut second = first.clone();
    first.null_count = 0;
    second.null_count = second.total_count;
    report.column_profiles = vec![report.column_profiles[0].clone(), first, second];

    let delta = document(&ReportDelta::between(&report, &report));
    assert_eq!(delta["schema"]["common"], json!(["id", "amount"]));
    let columns: Vec<&String> = delta["columns"].as_object().unwrap().keys().collect();
    assert_eq!(columns, ["id", "amount"]);
    assert_eq!(delta["columns"]["amount"]["null_pct_a"], json!(0.0));

    let side = ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap();
    let flat = document(&ReportDelta::between_sides(&side, &side));
    assert_eq!(flat["schema"], delta["schema"]);
    assert_eq!(flat["columns"], delta["columns"]);
}

#[test]
fn serialization_is_deterministic_and_ordered() {
    let a = profile(WITH_NULLS);
    let b = profile(CLEAN);
    let first = serde_json::to_string(&ReportDelta::between(&a, &b)).unwrap();
    let second = serde_json::to_string(&ReportDelta::between(&a, &b)).unwrap();
    assert_eq!(first, second);

    let delta: Value = serde_json::from_str(&first).unwrap();
    let keys: Vec<&String> = delta.as_object().unwrap().keys().collect();
    assert_eq!(
        keys,
        [
            "quality_score",
            "dimensions",
            "columns",
            "metric_semantics",
            "configuration",
            "schema",
            "reports"
        ]
    );
    let dimensions: Vec<&String> = delta["dimensions"].as_object().unwrap().keys().collect();
    assert_eq!(
        dimensions,
        [
            "completeness",
            "consistency",
            "uniqueness",
            "accuracy",
            "timeliness",
            "validity",
            "precision"
        ]
    );
}

#[test]
fn a_partly_recorded_side_leaves_comparability_unknown() {
    let report = profile(CLEAN);
    let mut summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    // 0.12 recorded only its text-length unit.
    summary["metric_semantics"] = json!({"text_length_unit": "unicode_scalar"});
    let partial = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    let current = ReportSide::from_report(&report);

    for delta in [
        ReportDelta::between_sides(&partial, &current),
        ReportDelta::between_sides(&current, &partial),
        ReportDelta::between_sides(&partial, &partial),
    ] {
        assert_eq!(delta.metric_semantics.comparable, None);
    }
    let both = ReportDelta::between_sides(&current, &current);
    assert_eq!(both.metric_semantics.comparable, Some(true));
}

/// A dimension a later release added carries weight in the stored overall
/// score. When its component was sampled, nothing this build knows rules out
/// that the sample reaches the score, so the score is not certified exact.
#[test]
fn a_sampled_component_this_build_does_not_know_leaves_the_overall_unrecorded() {
    let report = profile(CLEAN);
    let mut canonical = serde_json::to_value(&report).unwrap();
    canonical["quality"]["confidence"] = json!({"Mixed": {
        "exact_dimensions": ["completeness"],
        "sample_size": 4,
        "sampled_dimensions": ["future_dimension"],
    }});
    let native: ProfileReport = serde_json::from_value(canonical).unwrap();
    let delta = ReportDelta::between(&native, &native);
    assert_eq!(
        delta.quality_score.evidence.a.gap(),
        Some(EvidenceGap::CoverageUnrecorded)
    );
    // A known dimension's own score is unaffected.
    let completeness = &delta.dimensions[0];
    assert!(completeness.score.evidence.a.is_complete());

    let mut summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    summary["quality"]["sampled_dimensions"] = json!(["future_dimension"]);
    summary["quality"]["assessed_dimensions"] = json!(["completeness", "future_dimension"]);
    let side = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    assert_eq!(
        ReportDelta::between_sides(&side, &side)
            .quality_score
            .evidence
            .a
            .gap(),
        Some(EvidenceGap::CoverageUnrecorded)
    );
}

/// A setting that moves a score on unchanged data is listed on both sides,
/// so a policy can tell a configuration change from a change in the data.
#[test]
fn configuration_lists_each_setting_and_whether_the_sides_agree() {
    let file = csv("v\n-3\n-1\n2\n4\n");
    let plain = Profiler::new().analyze_file(file.path()).unwrap();
    let hinted = Profiler::new()
        .positive_columns(vec!["v".to_string()])
        .analyze_file(file.path())
        .unwrap();

    let delta = ReportDelta::between(&plain, &hinted);
    let hints = &delta.configuration.semantic_hints;
    assert_eq!(hints.same, Some(false));
    assert!(hints.a.as_ref().unwrap().positive_columns.is_empty());
    assert_eq!(hints.b.as_ref().unwrap().positive_columns, ["v"]);
    assert_eq!(delta.configuration.score_weights.same, Some(true));
    // The definitions agree, so that check alone would call the drop a change
    // in the data.
    assert_eq!(delta.metric_semantics.comparable, Some(true));
    assert!(delta.quality_score.abs.is_some_and(|abs| abs < 0.0));

    let same = ReportDelta::between(&plain, &plain).configuration;
    assert_eq!(
        (same.score_weights.same, same.semantic_hints.same),
        (Some(true), Some(true))
    );

    // A repeated temporal column counts once (#1004), so freshly profiled
    // reports record the same effective configuration and score.
    let dated = csv("old,new
2001-01-01,2026-10-01
2002-01-01,2026-10-02
");
    let temporal = |columns: &[&str]| {
        Profiler::new()
            .temporal_columns(columns.iter().map(|name| name.to_string()).collect())
            .analyze_file(dated.path())
            .unwrap()
    };
    let repeated = ReportDelta::between(
        &temporal(&["old", "new"]),
        &temporal(&["old", "old", "new"]),
    );
    assert_eq!(repeated.configuration.semantic_hints.same, Some(true));
    assert_eq!(
        repeated
            .configuration
            .semantic_hints
            .b
            .unwrap()
            .temporal_columns,
        ["new", "old"]
    );

    // No public option sets weights; a document written elsewhere can.
    let mut document = serde_json::to_value(&plain).unwrap();
    document["quality"]["metrics"]["score_weights"] = json!({"completeness": 1.0});
    let reweighted: ProfileReport = serde_json::from_value(document).unwrap();
    let weights = ReportDelta::between(&plain, &reweighted)
        .configuration
        .score_weights;
    assert_eq!(weights.same, Some(false));
}

/// What each era of flat summary records, and what it leaves unknown.
#[test]
fn a_summary_records_the_settings_its_release_recorded() {
    let report = profile(CLEAN);
    let current: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    let side = |edit: &dyn Fn(&mut Value)| {
        let mut summary = current.clone();
        edit(&mut summary);
        let side = ReportSide::from_summary_json(&summary.to_string()).unwrap();
        ReportDelta::between_sides(&side, &ReportSide::from_report(&report)).configuration
    };

    let mut wrong = Vec::new();
    // (case, edit to the summary, weights same, hints same)
    type Case<'a> = (&'a str, &'a dyn Fn(&mut Value), Option<bool>, Option<bool>);
    let cases: [Case; 5] = [
        ("as written", &|_| {}, Some(true), Some(true)),
        (
            "custom weights recorded",
            &|summary| summary["quality"]["score_weights"] = json!({"completeness": 1.0}),
            Some(false),
            Some(true),
        ),
        (
            // 0.12 omitted default weights and recorded metric_semantics.
            "0.12: default weights omitted",
            &|summary| {
                summary["quality"]
                    .as_object_mut()
                    .unwrap()
                    .remove("score_weights");
            },
            Some(true),
            Some(true),
        ),
        (
            // 0.10 and 0.11 recorded neither, and could drop custom weights.
            "0.11: no weights, no metric_semantics",
            &|summary| {
                summary["quality"]
                    .as_object_mut()
                    .unwrap()
                    .remove("score_weights");
                summary.as_object_mut().unwrap().remove("metric_semantics");
            },
            None,
            Some(true),
        ),
        (
            "before 0.10: no schema_version",
            &|summary| {
                summary.as_object_mut().unwrap().remove("schema_version");
            },
            Some(true),
            None,
        ),
    ];
    for (case, edit, weights, hints) in cases {
        let configuration = side(edit);
        let got = (
            configuration.score_weights.same,
            configuration.semantic_hints.same,
        );
        if got != (weights, hints) {
            wrong.push(format!(
                "{case}: read {got:?}, expected {:?}",
                (weights, hints)
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A canonical document from before schema versioning recorded no hints.
#[test]
fn a_document_from_before_0_10_leaves_its_hints_unrecorded() {
    let report = profile(CLEAN);
    let mut document = serde_json::to_value(&report).unwrap();
    document.as_object_mut().unwrap().remove("schema_version");
    let legacy: ProfileReport = serde_json::from_value(document).unwrap();
    assert_eq!(legacy.schema_version, 0);

    let hints = ReportDelta::between(&legacy, &report)
        .configuration
        .semantic_hints;
    assert_eq!((hints.a.is_none(), hints.same), (true, None));
}
