//! The baseline gate reads the timeliness clock (#1024).
//!
//! The timeliness score counts dates later than a reference date and dates
//! older than a maximum age, so two reports measured against different
//! clocks score the same data differently. #903 records the clock in every
//! report; the score drop rules now compare it. A policy declares which
//! difference still measures the same thing: `fixed` (the default) needs the
//! same reference date, `rolling` lets two rolling clocks, each read on its
//! own run day, differ in date. Time zone and maximum age must match under
//! either, and a clock either side did not record leaves the rule
//! unevaluated.
//!
//! `python/tests/test_timeliness_clock_gate.py` is the Python twin; it also
//! covers the `check()` keyword, the command line and the policy file.

use std::io::Write;

use dataprof::{
    CheckStatus, NaiveDate, NotEvaluated, ProfileReport, Profiler, QualityDimension, QualityPolicy,
    ReportSide, TimelinessClockRule,
};
use serde_json::Value;

const CSV: &str = "id,updated_at\n0,2021-06-09\n1,2021-06-10\n2,2026-06-10\n3,2026-06-11\n";

fn date(day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 6, day).unwrap()
}

/// A report measured against a fixed date, with the date column, when the
/// text has one, named as temporal.
fn profiled(text: &str, reference_date: NaiveDate) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(text.as_bytes()).unwrap();
    file.flush().unwrap();
    let mut profiler = Profiler::new().reference_date(reference_date);
    if text.contains("updated_at") {
        profiler = profiler.temporal_columns(vec!["updated_at".to_string()]);
    }
    profiler.analyze_file(file.path()).unwrap()
}

/// A recorded clock: mode, reference day in June 2026, maximum age. `None`
/// writes a report without one, as before 0.14.
type Clock = Option<(&'static str, u32, f64)>;

/// The CSV report with its recorded clock replaced. Only the record changes:
/// the gate decides comparability on what the reports recorded, and a
/// rolling clock cannot be read on a chosen day.
fn with_clock(clock: Clock) -> ProfileReport {
    let mut document = serde_json::to_value(profiled(CSV, date(10))).unwrap();
    let timeliness = document["quality"]["metrics"]["timeliness"]
        .as_object_mut()
        .expect("timeliness assessed");
    match clock {
        Some((mode, day, age)) => {
            let recorded = timeliness["clock"].as_object_mut().expect("clock recorded");
            recorded.insert("mode".into(), Value::from(mode));
            recorded.insert(
                "reference_date".into(),
                Value::from(date(day).format("%Y-%m-%d").to_string()),
            );
            recorded.insert("max_data_age_years".into(), Value::from(age));
        }
        None => {
            timeliness.remove("clock");
        }
    }
    serde_json::from_value(document).unwrap()
}

/// How a score drop rule ended: decided, or the reason it was not.
#[derive(Debug, PartialEq)]
enum Outcome {
    Decided,
    Changed,
    Unrecorded,
}

fn outcome(status: &CheckStatus) -> Result<Outcome, String> {
    match status {
        CheckStatus::Passed | CheckStatus::Failed => Ok(Outcome::Decided),
        CheckStatus::NotEvaluated(NotEvaluated::SettingChanged { setting, .. })
            if setting == "timeliness_clock" =>
        {
            Ok(Outcome::Changed)
        }
        CheckStatus::NotEvaluated(NotEvaluated::SettingUnrecorded { setting, .. })
            if setting == "timeliness_clock" =>
        {
            Ok(Outcome::Unrecorded)
        }
        other => Err(format!("{other:?}")),
    }
}

fn policy(rule: TimelinessClockRule) -> QualityPolicy {
    QualityPolicy::new()
        .max_quality_score_drop(100.0)
        .max_dimension_score_drop(QualityDimension::Timeliness, 100.0)
        .max_dimension_score_drop(QualityDimension::Completeness, 100.0)
        .timeliness_clock(rule)
}

/// The baseline as the gate reads it: the canonical document, or the flat
/// summary `to_dict()` writes.
fn baselines(baseline: &ProfileReport) -> [(&'static str, ReportSide); 2] {
    [
        ("document", ReportSide::from_report(baseline)),
        (
            "summary",
            ReportSide::from_summary_json(&baseline.summary_json().unwrap()).unwrap(),
        ),
    ]
}

#[test]
fn the_score_rules_compare_clocks_as_the_policy_declares() {
    use Outcome::*;
    use TimelinessClockRule::{Fixed, Rolling};

    let cases: [(&str, TimelinessClockRule, Clock, Clock, Outcome); 12] = [
        (
            "same fixed date",
            Fixed,
            Some(("fixed", 10, 5.0)),
            Some(("fixed", 10, 5.0)),
            Decided,
        ),
        (
            "another fixed date",
            Fixed,
            Some(("fixed", 10, 5.0)),
            Some(("fixed", 11, 5.0)),
            Changed,
        ),
        (
            "same date, other mode",
            Fixed,
            Some(("rolling", 10, 5.0)),
            Some(("fixed", 10, 5.0)),
            Decided,
        ),
        (
            "two rolling days, fixed rule",
            Fixed,
            Some(("rolling", 10, 5.0)),
            Some(("rolling", 11, 5.0)),
            Changed,
        ),
        (
            "two rolling days",
            Rolling,
            Some(("rolling", 10, 5.0)),
            Some(("rolling", 11, 5.0)),
            Decided,
        ),
        (
            "same rolling day",
            Rolling,
            Some(("rolling", 10, 5.0)),
            Some(("rolling", 10, 5.0)),
            Decided,
        ),
        (
            "fixed baseline, rolling report",
            Rolling,
            Some(("fixed", 10, 5.0)),
            Some(("rolling", 11, 5.0)),
            Changed,
        ),
        (
            "two fixed dates, rolling rule",
            Rolling,
            Some(("fixed", 10, 5.0)),
            Some(("fixed", 11, 5.0)),
            Changed,
        ),
        (
            "two rolling days, another age",
            Rolling,
            Some(("rolling", 10, 5.0)),
            Some(("rolling", 11, 6.0)),
            Changed,
        ),
        (
            "same fixed date, another age",
            Fixed,
            Some(("fixed", 10, 5.0)),
            Some(("fixed", 10, 6.0)),
            Changed,
        ),
        (
            "baseline before 0.14",
            Fixed,
            None,
            Some(("fixed", 10, 5.0)),
            Unrecorded,
        ),
        (
            "baseline before 0.14, rolling rule",
            Rolling,
            None,
            Some(("rolling", 11, 5.0)),
            Unrecorded,
        ),
    ];

    let mut wrong = Vec::new();
    for (name, rule, baseline_clock, report_clock, expected) in cases {
        let (baseline, report) = (with_clock(baseline_clock), with_clock(report_clock));
        for (route, side) in baselines(&baseline) {
            let result = policy(rule).evaluate_against(&report, &side).unwrap();
            let recorded = result.baseline.as_ref().map(|b| b.timeliness_clock);
            if recorded != Some(rule) {
                wrong.push(format!("{name} [{route}]: result records {recorded:?}"));
            }
            for check in &result.checks {
                let rule_name = check
                    .dimension
                    .map_or("overall".to_string(), |d| d.to_string());
                // Completeness counts nulls, whatever the clock.
                let want = if check.dimension == Some(QualityDimension::Completeness) {
                    &Decided
                } else {
                    &expected
                };
                match outcome(&check.status) {
                    Ok(got) if &got == want => {}
                    got => wrong.push(format!(
                        "{name} [{route}] {rule_name}: {got:?}, expected {want:?}"
                    )),
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Without a timeliness score on either side, the overall score averages no
/// clock, so neither the rule nor an absent clock leaves it unevaluated.
#[test]
fn without_timeliness_the_overall_rule_reads_no_clock() {
    let text = "id,name\n0,a\n1,b\n2,c\n";
    let (baseline, report) = (profiled(text, date(10)), profiled(text, date(11)));
    let assessed = |report: &ProfileReport| {
        report
            .quality
            .as_ref()
            .and_then(|quality| quality.metrics.timeliness.as_ref())
            .is_some_and(|timeliness| timeliness.is_assessed())
    };
    assert!(!assessed(&baseline) && !assessed(&report));

    let mut wrong = Vec::new();
    for rule in [TimelinessClockRule::Fixed, TimelinessClockRule::Rolling] {
        for (route, side) in baselines(&baseline) {
            let result = QualityPolicy::new()
                .max_quality_score_drop(0.0)
                .timeliness_clock(rule)
                .evaluate_against(&report, &side)
                .unwrap();
            if result.checks[0].status != CheckStatus::Passed {
                wrong.push(format!("{rule} [{route}]: {:?}", result.checks[0].status));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Timeliness weighted `0.0` still reports its score but moves no average, so
/// the overall rule reads no clock there, while the timeliness rule still
/// does. No public setter takes the weights yet (#921), so the recorded
/// weights are edited, as the clocks are.
#[test]
fn a_zero_weight_timeliness_leaves_the_overall_rule_without_a_clock() {
    let zero_weight = |clock: Clock| -> ProfileReport {
        let mut document = serde_json::to_value(with_clock(clock)).unwrap();
        document["quality"]["metrics"]["score_weights"]["timeliness"] = Value::from(0.0);
        serde_json::from_value(document).unwrap()
    };
    let baseline = zero_weight(Some(("fixed", 10, 5.0)));
    let report = zero_weight(Some(("fixed", 11, 5.0)));

    let mut wrong = Vec::new();
    for (route, side) in baselines(&baseline) {
        let result = policy(TimelinessClockRule::Fixed)
            .evaluate_against(&report, &side)
            .unwrap();
        for check in &result.checks {
            let want = match check.dimension {
                Some(QualityDimension::Timeliness) => Outcome::Changed,
                _ => Outcome::Decided,
            };
            match outcome(&check.status) {
                Ok(got) if got == want => {}
                got => wrong.push(format!(
                    "[{route}] {:?}: {got:?}, expected {want:?}",
                    check.dimension
                )),
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn the_rule_parses_from_its_name() {
    assert_eq!("fixed".parse(), Ok(TimelinessClockRule::Fixed));
    assert_eq!("rolling".parse(), Ok(TimelinessClockRule::Rolling));
    assert!("daily".parse::<TimelinessClockRule>().is_err());
    assert_eq!(TimelinessClockRule::default(), TimelinessClockRule::Fixed);
    assert_eq!(TimelinessClockRule::Rolling.to_string(), "rolling");
}
