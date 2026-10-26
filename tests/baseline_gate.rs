//! Baseline-relative quality-gate rules (#749).
//!
//! `python/tests/test_baseline_gate.py` is the Python twin.

use std::io::Write;

use dataprof::{
    Check, CheckCode, CheckStatus, DiagnosticCode, EvidenceGap, GateResult, NotEvaluated,
    PolicyError, PolicyScope, ProfileReport, Profiler, QualityDimension, QualityPolicy, ReportSide,
    StopCondition, Verdict,
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

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|name| (*name).to_string()).collect()
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
fn a_baseline_without_a_score_definition_leaves_the_scores_reading_it_unevaluated() {
    // A baseline that records every other definition, as a 0.12 report does
    // not, so only the one removed separates it from the report (#1005). The
    // overall score and the dimension reading it go unevaluated; the other
    // dimensions are still decided.
    let report = profile(CLEAN);
    for (definition, reads, other) in [
        ("range_rule_count", "accuracy", "uniqueness"),
        ("key_uniqueness_units", "uniqueness", "accuracy"),
        ("outlier_units", "accuracy", "uniqueness"),
        ("key_column_source", "uniqueness", "accuracy"),
    ] {
        let mut summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
        summary["metric_semantics"]
            .as_object_mut()
            .unwrap()
            .remove(definition)
            .unwrap_or_else(|| panic!("reports record {definition}"));
        let baseline = ReportSide::from_summary_json(&summary.to_string()).unwrap();
        let dimension = |name: &str| match name {
            "accuracy" => QualityDimension::Accuracy,
            _ => QualityDimension::Uniqueness,
        };
        let result = QualityPolicy::new()
            .max_quality_score_drop(0.0)
            .max_dimension_score_drop(dimension(reads), 0.0)
            .max_dimension_score_drop(dimension(other), 0.0)
            .max_dimension_score_drop(QualityDimension::Completeness, 0.0)
            .evaluate_against(&report, &baseline)
            .unwrap();
        for (code, subject) in [
            (CheckCode::MaxQualityScoreDrop, None),
            (CheckCode::MaxDimensionScoreDrop, Some(reads)),
        ] {
            assert!(
                matches!(
                    reason(check(&result, code, subject)),
                    Some(NotEvaluated::DefinitionNotComparable { definition: missing, .. }) if missing == definition
                ),
                "{definition}: {code:?} {subject:?}"
            );
        }
        for decided in [other, "completeness"] {
            assert_eq!(
                check(&result, CheckCode::MaxDimensionScoreDrop, Some(decided)).status,
                CheckStatus::Passed,
                "{definition}: {decided}"
            );
        }
    }
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
        .max_dimension_score_drop(QualityDimension::Completeness, 100.0)
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
    assert_eq!(
        keys,
        [
            "verdict",
            "scope",
            "evidence",
            "checks",
            "baseline",
            "generator"
        ]
    );
    assert_eq!(
        relative["baseline"]["schema"],
        json!({"added": [], "removed": [], "common": ["id", "amount"]})
    );
    // A permitted drop of zero is 0.0, not -0.0.
    assert_eq!(
        serde_json::to_string(&relative["checks"][0]["expected"]).unwrap(),
        r#"{"comparison":"change_at_least","value":0.0}"#
    );
    // The completeness rule, which is decided.
    let keys: Vec<_> = relative["checks"][1]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(
        keys,
        [
            "code",
            "dimension",
            "expected",
            "observed",
            "scope",
            "evidence",
            "baseline",
            "status",
            "message"
        ]
    );
}

#[test]
fn the_overall_score_needs_the_same_assessed_dimensions() {
    // Same column, but only the dated side has anything for timeliness.
    let undated = profile("d\nabc\nxyz\n");
    let dated = profile("d\n2026-01-01\n2026-01-02\n");
    let result = against(
        &QualityPolicy::new().max_quality_score_drop(100.0),
        &dated,
        &undated,
    );
    assert_eq!(
        reason(&result.checks[0]),
        Some(&NotEvaluated::DimensionsChanged)
    );
}

#[test]
fn order_and_pairing_the_reports_cannot_show_are_not_assumed_the_same() {
    let keys = csv("a,b\n1,1\n2,1\n3,2\n");
    let ordered = |columns: &[&str]| {
        Profiler::new()
            .identifier_columns(names(columns))
            .analyze_file(keys.path())
            .unwrap()
    };
    let policy = QualityPolicy::new().max_dimension_score_drop(QualityDimension::Uniqueness, 100.0);
    // The first identifier present is the uniqueness key, so order matters.
    let reordered = against(&policy, &ordered(&["b", "a"]), &ordered(&["a", "b"]));
    assert!(matches!(
        reason(&reordered.checks[0]),
        Some(NotEvaluated::SettingChanged { setting, .. }) if setting == "identifier_columns"
    ));
    let same = against(&policy, &ordered(&["a", "b"]), &ordered(&["a", "b"]));
    assert_eq!(same.checks[0].status, CheckStatus::Passed);

    // A report records which columns the pairs named, not the pairing.
    let dates = csv("start,end\n2026-01-01,2026-01-02\n2026-01-03,2026-01-04\n");
    let paired = Profiler::new()
        .temporal_pairs(vec![("start".to_string(), "end".to_string())])
        .analyze_file(dates.path())
        .unwrap();
    let policy = QualityPolicy::new().max_dimension_score_drop(QualityDimension::Timeliness, 100.0);
    assert!(matches!(
        reason(&against(&policy, &paired, &paired).checks[0]),
        Some(NotEvaluated::SettingUnrecorded { setting, .. }) if setting == "temporal_pairs"
    ));
    let unpaired = Profiler::new().analyze_file(dates.path()).unwrap();
    assert_eq!(
        against(&policy, &unpaired, &unpaired).checks[0].status,
        CheckStatus::Passed
    );

    // Validity reads the pattern locale, which both reports record (#1010);
    // `pattern_locale_record.rs` covers a locale that differs.
    let policy = QualityPolicy::new().max_dimension_score_drop(QualityDimension::Validity, 100.0);
    let emails = profile("email\na@example.com\nb@example.com\n");
    assert_eq!(
        against(&policy, &emails, &emails).checks[0].status,
        CheckStatus::Passed
    );
}

#[test]
fn an_allowance_is_compared_at_the_serialized_precision() {
    // One null in 10,000 values moves the null percentage and completeness
    // by exactly 0.01 points.
    let rows: String = (1..10_000).map(|i| format!("{i}\n")).collect();
    let baseline = profile(&format!("v\n0\n{rows}"));
    let with_null = profile(&format!("v\nNA\n{rows}"));
    let decide = |points: f64| {
        let result = against(
            &QualityPolicy::new()
                .max_dimension_score_drop(QualityDimension::Completeness, points)
                .max_null_percentage_increase("v", points),
            &with_null,
            &baseline,
        );
        serde_json::to_value(&result.checks).unwrap()
    };
    // 0.006 serializes as 0.01, the change it is compared with.
    let checks = decide(0.006);
    for check in checks.as_array().unwrap() {
        assert_eq!(check["status"], json!("passed"), "{check:#}");
        assert_eq!(check["baseline"]["change"].as_f64().unwrap().abs(), 0.01);
        assert_eq!(check["expected"]["value"].as_f64().unwrap().abs(), 0.01);
    }
    for check in decide(0.004).as_array().unwrap() {
        assert_eq!(check["status"], json!("failed"), "{check:#}");
        assert_eq!(check["expected"]["value"].as_f64().unwrap().abs(), 0.0);
    }
}

fn diagnostics(result: &GateResult) -> Vec<(DiagnosticCode, &str)> {
    result
        .baseline
        .as_ref()
        .unwrap()
        .diagnostics
        .iter()
        .map(|diagnostic| (diagnostic.code, diagnostic.column.as_str()))
        .collect()
}

const BEFORE: &str = "id,amount,code,gone\n1,10,a,x\n2,20,b,y\n";
const AFTER: &str = "id,amount,code,new\n1,x1,,z\n2,y2,,w\n";

#[test]
fn a_schema_change_is_a_diagnostic_and_moves_no_verdict() {
    let baseline = profile(BEFORE);
    let report = profile(AFTER);
    let policy = QualityPolicy::new().max_null_percentage_increase("id", 0.0);
    let result = against(&policy, &report, &baseline);

    // The one rule passes; four columns changed, one diagnostic each, in
    // the baseline's column order and then the added ones.
    assert_eq!(result.verdict, Verdict::Pass, "{:#?}", result.checks);
    assert_eq!(
        diagnostics(&result),
        [
            (DiagnosticCode::ColumnTypeChanged, "amount"),
            (DiagnosticCode::ColumnAllNull, "code"),
            (DiagnosticCode::ColumnRemoved, "gone"),
            (DiagnosticCode::ColumnAdded, "new"),
        ]
    );
    let messages: Vec<_> = result
        .baseline
        .as_ref()
        .unwrap()
        .diagnostics
        .iter()
        .map(|diagnostic| diagnostic.message.as_str())
        .collect();
    assert_eq!(
        messages[1..],
        [
            "this column held values in the baseline, and every value read for it in the report \
             is null",
            // Both sides profiled every column, so a missing one was not in
            // the source that side read.
            "the baseline profiled a column the report did not find in its source",
            "the report profiled a column the baseline did not find in its source, so the \
             baseline has nothing to measure its change from",
        ]
    );
    let document = serde_json::to_value(&result).unwrap();
    let retyped = &document["baseline"]["diagnostics"][0];
    assert_eq!(
        retyped,
        &json!({
            "code": "column_type_changed",
            "column": "amount",
            "data_type": {"a": "integer", "b": "string"},
            "message": "this column holds values on both sides, and its inferred type differs",
        })
    );
    let keys: Vec<_> = document["baseline"]["diagnostics"][1]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect();
    assert_eq!(keys, ["code", "column", "message"]);

    // Nothing changed: analyzed, and empty rather than absent.
    let same = serde_json::to_value(against(&policy, &baseline, &baseline)).unwrap();
    assert_eq!(same["baseline"]["diagnostics"], json!([]));
    // No baseline, no comparison: absent.
    let absolute =
        serde_json::to_value(QualityPolicy::new().min_rows(1).evaluate(&report).unwrap()).unwrap();
    assert!(absolute.get("baseline").is_none());
}

#[test]
fn an_all_null_side_establishes_no_type_and_a_rounded_100_is_not_all_null() {
    // The baseline column holds only nulls, so its inferred type says
    // nothing, and a typed column later is not a type change.
    let empty = profile("id,v\n1,\n2,\n");
    let dated = profile("id,v\n1,2026-01-01\n2,2026-01-02\n");
    assert_eq!(diagnostics(&against(&drift_policy(), &dated, &empty)), []);
    // The other way round the column held values and now holds none.
    assert_eq!(
        diagnostics(&against(&drift_policy(), &empty, &dated)),
        [(DiagnosticCode::ColumnAllNull, "v")]
    );

    // One value in 100,000 serializes as 100.00% null, and is still a value.
    // A blank line is no row, so the nulls sit beside an id.
    let nulls: String = (1..100_000).map(|i| format!("{i},\n")).collect();
    let nearly = profile(&format!("id,v\n0,7\n{nulls}"));
    let full = profile("id,v\n1,1\n2,2\n");
    let result = against(&drift_policy(), &nearly, &full);
    let column = &serde_json::to_value(&result.checks).unwrap()[3];
    assert_eq!(column["observed"], json!(100.0), "{column:#}");
    assert_eq!(diagnostics(&result), []);
}

#[test]
fn a_flat_summary_baseline_gives_the_same_diagnostics_and_omits_what_it_lacks() {
    let baseline = profile(BEFORE);
    let report = profile(AFTER);
    let policy = QualityPolicy::new().max_null_percentage_increase("id", 0.0);
    let live = against(&policy, &report, &baseline);

    let summary = baseline.summary_json().unwrap();
    let flat = policy
        .evaluate_against(&report, &ReportSide::from_summary_json(&summary).unwrap())
        .unwrap();
    assert_eq!(
        flat.baseline.as_ref().unwrap().diagnostics,
        live.baseline.as_ref().unwrap().diagnostics
    );

    let edited = |edit: &dyn Fn(&mut serde_json::Map<String, Value>)| {
        let mut document: Value = serde_json::from_str(&summary).unwrap();
        for column in document["columns"].as_array_mut().unwrap() {
            edit(column.as_object_mut().unwrap());
        }
        let side = ReportSide::from_summary_json(&document.to_string()).unwrap();
        policy.evaluate_against(&report, &side).unwrap()
    };
    // A type this build does not know comes from a later release, and reads
    // as unrecorded rather than as a change or a decode error.
    let future = edited(&|column| {
        column.insert("data_type".to_string(), json!("future_type"));
    });
    assert_eq!(
        diagnostics(&future),
        [
            (DiagnosticCode::ColumnAllNull, "code"),
            (DiagnosticCode::ColumnRemoved, "gone"),
            (DiagnosticCode::ColumnAdded, "new"),
        ]
    );
    // Without the counts, neither the type nor the nulls can be read.
    let uncounted = edited(&|column| {
        column.remove("total_count");
        column.remove("null_count");
    });
    assert_eq!(
        diagnostics(&uncounted),
        [
            (DiagnosticCode::ColumnRemoved, "gone"),
            (DiagnosticCode::ColumnAdded, "new"),
        ]
    );
}

#[test]
fn a_type_change_the_identifier_hint_explains_names_the_hint() {
    let keys = csv("id,n\n1,5\n2,6\n");
    let hinted = Profiler::new()
        .identifier_columns(names(&["id"]))
        .analyze_file(keys.path())
        .unwrap();
    let plain = Profiler::new().analyze_file(keys.path()).unwrap();
    let policy = QualityPolicy::new().max_null_percentage_increase("n", 0.0);

    // Same data: only the hint retyped `id`, and the diagnostic says so.
    let result = against(&policy, &hinted, &plain);
    assert_eq!(result.verdict, Verdict::Pass);
    let document = serde_json::to_value(&result).unwrap();
    assert_eq!(
        document["baseline"]["diagnostics"],
        json!([{
            "code": "column_type_changed",
            "column": "id",
            "data_type": {"a": "integer", "b": "identifier"},
            "reason": "setting_changed",
            "setting": "identifier_columns",
            "message": "this column holds values on both sides, and its inferred type differs; \
                        the identifier hint, which sets the type, is not shown to be the same \
                        on both sides",
        }])
    );

    // A baseline from before 0.10 recorded no hints, so the hint cannot be
    // shown to be the same.
    let mut summary: Value = serde_json::from_str(&plain.summary_json().unwrap()).unwrap();
    summary.as_object_mut().unwrap().remove("schema_version");
    let old = ReportSide::from_summary_json(&summary.to_string()).unwrap();
    let document = serde_json::to_value(policy.evaluate_against(&hinted, &old).unwrap()).unwrap();
    assert_eq!(
        document["baseline"]["diagnostics"][0]["reason"],
        json!("setting_unrecorded")
    );
}
