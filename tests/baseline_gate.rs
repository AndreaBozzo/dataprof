//! Baseline-relative quality-gate rules (#749).
//!
//! `python/tests/test_baseline_gate.py` is the Python twin.

use std::io::Write;

use dataprof::{
    Check, CheckCode, CheckStatus, EvidenceGap, GateResult, NotEvaluated, PolicyError, PolicyScope,
    ProfileReport, Profiler, QualityDimension, QualityPolicy, ReportSide, StopCondition, Verdict,
};
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

fn against(policy: &QualityPolicy, report: &ProfileReport, baseline: &ProfileReport) -> GateResult {
    policy
        .evaluate_against(report, &ReportSide::from_report(baseline))
        .unwrap()
}

fn check<'a>(result: &'a GateResult, code: CheckCode, subject: Option<&str>) -> &'a Check {
    result
        .checks
        .iter()
        .find(|check| {
            check.code == code
                && subject.is_none_or(|name| {
                    check.column.as_deref() == Some(name)
                        || check.dimension.map(|d| d.to_string()).as_deref() == Some(name)
                })
        })
        .unwrap_or_else(|| panic!("no {code} check for {subject:?}"))
}

fn reason(check: &Check) -> Option<&NotEvaluated> {
    check.not_evaluated()
}

const CLEAN: &str = "id,amount\n1,10\n2,20\n3,30\n4,40\n";
const WITH_NULLS: &str = "id,amount\n1,10\n2,\n3,\n4,40\n";

fn drift_policy() -> QualityPolicy {
    QualityPolicy::new()
        .max_quality_score_drop(0.0)
        .max_dimension_score_drop(QualityDimension::Completeness, 0.0)
        .max_null_percentage_increase_any(0.0)
}

#[test]
fn unchanged_data_passes_every_change_rule() {
    let baseline = profile(CLEAN);
    let report = profile(CLEAN);
    let result = against(&drift_policy(), &report, &baseline);

    assert_eq!(result.verdict, Verdict::Pass, "{:#?}", result.checks);
    let codes: Vec<_> = result
        .checks
        .iter()
        .map(|check| (check.code, check.column.clone()))
        .collect();
    assert_eq!(
        codes,
        [
            (CheckCode::MaxQualityScoreDrop, None),
            (CheckCode::MaxDimensionScoreDrop, None),
            (CheckCode::MaxNullPercentageIncrease, Some("id".to_string())),
            (
                CheckCode::MaxNullPercentageIncrease,
                Some("amount".to_string())
            ),
        ]
    );
    let comparison = result.baseline.as_ref().unwrap();
    assert_eq!(
        comparison.reports.a.id.as_deref(),
        Some(baseline.id.as_str())
    );
    assert_eq!(comparison.reports.b.id.as_deref(), Some(report.id.as_str()));
}

#[test]
fn a_degradation_fails_and_an_improvement_passes() {
    let clean = profile(CLEAN);
    let nulls = profile(WITH_NULLS);
    let policy = QualityPolicy::new()
        .max_dimension_score_drop(QualityDimension::Completeness, 5.0)
        .max_null_percentage_increase("amount", 10.0);

    let worse = against(&policy, &nulls, &clean);
    assert_eq!(worse.verdict, Verdict::Fail);
    let completeness = check(
        &worse,
        CheckCode::MaxDimensionScoreDrop,
        Some("completeness"),
    );
    assert!(completeness.is_violation());
    let amount = check(&worse, CheckCode::MaxNullPercentageIncrease, Some("amount"));
    assert!(amount.is_violation());
    let document = serde_json::to_value(amount).unwrap();
    assert_eq!(document["observed"], json!(50.0));
    assert_eq!(
        document["baseline"],
        json!({"observed": 0.0, "evidence": {"coverage": "complete"}, "change": 50.0})
    );
    assert_eq!(
        document["expected"],
        json!({"comparison": "change_at_most", "value": 10.0})
    );

    let better = against(&policy, &clean, &nulls);
    assert_eq!(better.verdict, Verdict::Pass, "{:#?}", better.checks);
}

#[test]
fn a_change_rule_needs_a_baseline_and_a_percentage() {
    let report = profile(CLEAN);
    assert_eq!(
        drift_policy().evaluate(&report).unwrap_err(),
        PolicyError::BaselineRequired
    );
    let error = QualityPolicy::new()
        .max_null_percentage_increase("amount", 101.0)
        .evaluate_against(&report, &ReportSide::from_report(&report))
        .unwrap_err();
    assert!(
        matches!(
            &error,
            PolicyError::ThresholdOutOfRange { code: CheckCode::MaxNullPercentageIncrease, subject: Some(column), .. }
                if column == "amount"
        ),
        "{error:?}"
    );
    // A baseline alone is not a requirement.
    assert_eq!(
        QualityPolicy::new()
            .evaluate_against(&report, &ReportSide::from_report(&report))
            .unwrap_err(),
        PolicyError::NoRequirements
    );
}

#[test]
fn absolute_and_change_rules_evaluate_together_absolute_first() {
    let baseline = profile(CLEAN);
    let report = profile(WITH_NULLS);
    let result = against(
        &QualityPolicy::new()
            .min_rows(1)
            .max_null_percentage("amount", 100.0)
            .max_null_percentage_increase("amount", 0.0),
        &report,
        &baseline,
    );
    let codes: Vec<_> = result.checks.iter().map(|check| check.code).collect();
    assert_eq!(
        codes,
        [
            CheckCode::MinRows,
            CheckCode::MaxNullPercentage,
            CheckCode::MaxNullPercentageIncrease
        ]
    );
    assert_eq!(
        result
            .checks
            .iter()
            .map(|check| check.baseline.is_some())
            .collect::<Vec<_>>(),
        [false, false, true]
    );
    assert_eq!(result.verdict, Verdict::Fail);
}

#[test]
fn a_changed_column_set_leaves_dataset_rules_unevaluated_and_column_rules_decisive() {
    let baseline = profile(CLEAN);
    let added = profile("id,amount,note\n1,10,\n2,20,\n3,30,\n4,40,x\n");
    let result = against(
        &drift_policy().max_null_percentage_increase("note", 0.0),
        &added,
        &baseline,
    );

    for code in [
        CheckCode::MaxQualityScoreDrop,
        CheckCode::MaxDimensionScoreDrop,
    ] {
        assert_eq!(
            reason(check(&result, code, None)),
            Some(&NotEvaluated::SchemaChanged),
            "{code}"
        );
    }
    // The new column has no baseline: only the rule that names it reads it,
    // and it cannot be decided.
    let note = check(&result, CheckCode::MaxNullPercentageIncrease, Some("note"));
    assert_eq!(reason(note), Some(&NotEvaluated::NotInBaseline));
    let wildcard: Vec<_> = result
        .checks
        .iter()
        .filter(|check| check.code == CheckCode::MaxNullPercentageIncrease)
        .filter_map(|check| check.column.as_deref())
        .collect();
    assert_eq!(wildcard, ["note", "id", "amount"]);
    assert_eq!(result.verdict, Verdict::Inconclusive);
    assert_eq!(
        result.baseline.as_ref().unwrap().schema.added,
        ["note".to_string()]
    );

    // A shared column's rule still decides the verdict: a failure beats the
    // unevaluated dataset-level rules.
    let worse = profile("id,amount,note\n1,10,\n2,,\n3,,\n4,40,x\n");
    let result = against(&drift_policy(), &worse, &baseline);
    assert_eq!(result.verdict, Verdict::Fail);
    // `*` covers the shared columns only; the unnamed new one gets no rule.
    let covered: Vec<_> = result
        .checks
        .iter()
        .filter_map(|check| check.column.as_deref())
        .collect();
    assert_eq!(covered, ["id", "amount"]);
    assert!(
        check(
            &result,
            CheckCode::MaxNullPercentageIncrease,
            Some("amount")
        )
        .is_violation()
    );

    // A removed column has no profile in the evaluated report.
    let removed = profile("id\n1\n2\n3\n4\n");
    let result = against(
        &QualityPolicy::new().max_null_percentage_increase("amount", 0.0),
        &removed,
        &baseline,
    );
    assert_eq!(
        reason(check(
            &result,
            CheckCode::MaxNullPercentageIncrease,
            Some("amount")
        )),
        Some(&NotEvaluated::ColumnNotProfiled)
    );
}

#[test]
fn a_score_either_side_lacks_is_unevaluated() {
    let undated = profile("d\nabc\nxyz\n");
    let dated = profile("d\n2026-01-01\n2026-01-02\n");
    let policy = QualityPolicy::new().max_dimension_score_drop(QualityDimension::Timeliness, 0.0);

    let new_dimension = against(&policy, &dated, &undated);
    assert_eq!(
        reason(check(
            &new_dimension,
            CheckCode::MaxDimensionScoreDrop,
            None
        )),
        Some(&NotEvaluated::NotInBaseline)
    );
    let neither = against(&policy, &undated, &undated);
    assert_eq!(
        reason(check(&neither, CheckCode::MaxDimensionScoreDrop, None)),
        Some(&NotEvaluated::NotAssessed)
    );

    let mut document = serde_json::to_value(&dated).unwrap();
    document["quality"] = Value::Null;
    document["quality_status"] = json!({"state": "not_requested"});
    let unassessed: ProfileReport = serde_json::from_value(document).unwrap();
    let result = against(&policy, &unassessed, &dated);
    assert!(matches!(
        reason(check(&result, CheckCode::MaxDimensionScoreDrop, None)),
        Some(NotEvaluated::QualityUnavailable { quality_status, .. }) if quality_status == "not_requested"
    ));
}

#[test]
fn a_setting_leaves_only_the_scores_it_moves_unevaluated() {
    let file = csv("v\n-3\n-1\n2\n4\n");
    let plain = Profiler::new().analyze_file(file.path()).unwrap();
    let hinted = Profiler::new()
        .positive_columns(vec!["v".to_string()])
        .analyze_file(file.path())
        .unwrap();
    let policy = QualityPolicy::new()
        .max_quality_score_drop(0.0)
        .max_dimension_score_drop(QualityDimension::Accuracy, 0.0)
        .max_dimension_score_drop(QualityDimension::Completeness, 0.0);
    let result = against(&policy, &hinted, &plain);

    let changed = |code, subject| match reason(check(&result, code, subject)) {
        Some(NotEvaluated::SettingChanged { setting, .. }) => Some(setting.clone()),
        _ => None,
    };
    assert_eq!(
        changed(CheckCode::MaxQualityScoreDrop, None).as_deref(),
        Some("positive_columns")
    );
    assert_eq!(
        changed(CheckCode::MaxDimensionScoreDrop, Some("accuracy")).as_deref(),
        Some("positive_columns")
    );
    let completeness = check(
        &result,
        CheckCode::MaxDimensionScoreDrop,
        Some("completeness"),
    );
    assert_eq!(completeness.status, CheckStatus::Passed);

    // Timeliness reads the temporal hints.
    let dates = csv("d\n2026-01-01\n2026-01-02\n");
    let undeclared = Profiler::new().analyze_file(dates.path()).unwrap();
    let declared = Profiler::new()
        .temporal_columns(vec!["d".to_string()])
        .analyze_file(dates.path())
        .unwrap();
    let result = against(
        &QualityPolicy::new()
            .max_dimension_score_drop(QualityDimension::Timeliness, 0.0)
            .max_dimension_score_drop(QualityDimension::Completeness, 0.0),
        &declared,
        &undeclared,
    );
    assert!(matches!(
        reason(check(&result, CheckCode::MaxDimensionScoreDrop, Some("timeliness"))),
        Some(NotEvaluated::SettingChanged { setting, .. }) if setting == "temporal_columns"
    ));
    assert_eq!(
        check(
            &result,
            CheckCode::MaxDimensionScoreDrop,
            Some("completeness")
        )
        .status,
        CheckStatus::Passed
    );

    // No public option sets weights; a document written elsewhere can.
    let mut document = serde_json::to_value(&plain).unwrap();
    document["quality"]["metrics"]["score_weights"] = json!({"completeness": 1.0});
    let reweighted: ProfileReport = serde_json::from_value(document).unwrap();
    let result = against(&policy, &reweighted, &plain);
    assert!(matches!(
        reason(check(&result, CheckCode::MaxQualityScoreDrop, None)),
        Some(NotEvaluated::SettingChanged { setting, .. }) if setting == "score_weights"
    ));
    assert_eq!(
        check(&result, CheckCode::MaxDimensionScoreDrop, Some("accuracy")).status,
        CheckStatus::Passed
    );
}

#[test]
fn a_baseline_that_predates_a_record_leaves_what_depends_on_it_unevaluated() {
    let report = profile(CLEAN);
    let mut summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    // A summary from before 0.10 records no version, and so no hints.
    summary.as_object_mut().unwrap().remove("schema_version");
    let baseline = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    let policy = QualityPolicy::new()
        .max_dimension_score_drop(QualityDimension::Accuracy, 0.0)
        .max_dimension_score_drop(QualityDimension::Completeness, 0.0);
    let result = policy.evaluate_against(&report, &baseline).unwrap();
    assert!(matches!(
        reason(check(&result, CheckCode::MaxDimensionScoreDrop, Some("accuracy"))),
        Some(NotEvaluated::SettingUnrecorded { setting, .. }) if setting == "identifier_columns"
    ));
    assert_eq!(
        check(
            &result,
            CheckCode::MaxDimensionScoreDrop,
            Some("completeness")
        )
        .status,
        CheckStatus::Passed
    );

    // A 0.11 summary records no measurement definitions.
    let legacy = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy_0_11_non_ascii.summary.json"),
    )
    .unwrap();
    let legacy = ReportSide::from_summary_json(&legacy).unwrap();
    let result = QualityPolicy::new()
        .max_null_percentage_increase_any(0.0)
        .max_quality_score_drop(0.0)
        .evaluate_against(&profile("city\nOslo\n"), &legacy)
        .unwrap();
    for check in &result.checks {
        assert!(
            matches!(
                reason(check),
                Some(NotEvaluated::DefinitionNotComparable { definition, .. }) if definition == "null_tokens"
            ),
            "{check:#?}"
        );
    }
    assert_eq!(result.verdict, Verdict::Inconclusive);
}

#[test]
fn partial_evidence_on_either_side_is_unevaluated_under_full_source_only() {
    let rows: String = (0..200).map(|i| format!("{i},{}\n", i * 2)).collect();
    let file = csv(&format!("id,amount\n{rows}"));
    let exact = Profiler::new().analyze_file(file.path()).unwrap();
    let truncated = Profiler::new()
        .stop_when(StopCondition::MaxRows(50))
        .analyze_file(file.path())
        .unwrap();
    let policy = QualityPolicy::new()
        .max_quality_score_drop(100.0)
        .max_null_percentage_increase_any(0.0);

    for (report, baseline) in [(&truncated, &exact), (&exact, &truncated)] {
        let result = against(&policy, report, baseline);
        for check in &result.checks {
            assert!(
                matches!(
                    reason(check),
                    Some(NotEvaluated::EvidenceIncomplete {
                        gap: EvidenceGap::Truncated,
                        ..
                    })
                ),
                "{check:#?}"
            );
        }
        assert_eq!(result.verdict, Verdict::Inconclusive);

        let observed = against(
            &policy.clone().scope(PolicyScope::Observed),
            report,
            baseline,
        );
        assert_eq!(observed.verdict, Verdict::Pass, "{:#?}", observed.checks);
    }
}

#[test]
fn a_saved_baseline_gives_the_verdict_the_live_one_does() {
    let baseline = profile(WITH_NULLS);
    let report = profile(CLEAN);
    let policy = drift_policy().max_dimension_score_drop(QualityDimension::Uniqueness, 0.0);
    let live = serde_json::to_value(against(&policy, &report, &baseline)).unwrap();

    let reloaded: ProfileReport =
        serde_json::from_str(&serde_json::to_string(&baseline).unwrap()).unwrap();
    let canonical = serde_json::to_value(against(&policy, &report, &reloaded)).unwrap();
    assert_eq!(canonical, live);

    let flat = ReportSide::from_summary_json(&baseline.summary_json().unwrap()).unwrap();
    let mut summary =
        serde_json::to_value(policy.evaluate_against(&report, &flat).unwrap()).unwrap();
    // A flat summary records neither identity field.
    assert_eq!(summary["baseline"]["reports"]["a"]["id"], Value::Null);
    summary["baseline"]["reports"]["a"]["id"] = json!(baseline.id);
    summary["baseline"]["reports"]["a"]["timestamp"] = json!(baseline.timestamp);
    assert_eq!(summary, live);
}

#[test]
fn the_document_keeps_the_absolute_shape_and_adds_the_baseline() {
    let report = profile(CLEAN);
    let absolute =
        serde_json::to_value(QualityPolicy::new().min_rows(1).evaluate(&report).unwrap()).unwrap();
    assert!(absolute.get("baseline").is_none());
    assert!(absolute["checks"][0].get("baseline").is_none());

    let relative = serde_json::to_value(against(&drift_policy(), &report, &report)).unwrap();
    let keys: Vec<_> = relative.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, ["verdict", "scope", "evidence", "checks", "baseline"]);
    assert_eq!(
        relative["baseline"]["schema"],
        json!({"added": [], "removed": [], "common": ["id", "amount"]})
    );
    // A permitted drop of zero is 0.0, not -0.0.
    assert_eq!(
        serde_json::to_string(&relative["checks"][0]["expected"]).unwrap(),
        r#"{"comparison":"change_at_least","value":0.0}"#
    );
    let keys: Vec<_> = relative["checks"][0]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        keys,
        [
            "code", "expected", "observed", "scope", "evidence", "baseline", "status", "message"
        ]
    );
}
