//! Reports record the pattern locale they were profiled with (#1010).
//!
//! `python/tests/test_pattern_locale_record.py` is the Python twin; it also
//! covers the in-memory, DataFrame, Arrow and Parquet routes.

use std::io::Write;

use dataprof::{
    CheckCode, CheckStatus, EngineType, Locale, MetricPack, NotEvaluated, PatternLocale,
    ProfileReport, Profiler, QualityDimension, QualityPolicy, ReportDelta, ReportSide,
    analyze_csv_file,
};
use serde_json::{Value, json};

const EMAILS: &str = "email\na@example.com\nb@example.com\nc@example.com\n";

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn recorded(report: &ProfileReport) -> Option<PatternLocale> {
    report
        .analysis_settings
        .as_ref()
        .map(|settings| settings.pattern_locale)
}

#[test]
fn every_engine_and_file_format_records_the_locale() {
    let csv = file(".csv", EMAILS);
    let json = file(
        ".json",
        r#"[{"email": "a@example.com"}, {"email": "b@example.com"}]"#,
    );
    let jsonl = file(
        ".jsonl",
        "{\"email\": \"a@example.com\"}\n{\"email\": \"b@example.com\"}\n",
    );
    let routes: Vec<(&str, &std::path::Path, Option<EngineType>)> = vec![
        ("csv auto", csv.path(), Some(EngineType::Auto)),
        ("csv incremental", csv.path(), Some(EngineType::Incremental)),
        ("csv columnar", csv.path(), Some(EngineType::Columnar)),
        ("json", json.path(), None),
        ("jsonl", jsonl.path(), None),
    ];
    let mut wrong = Vec::new();
    // Without the quality pack the engines take another branch, which must
    // record the settings too: patterns are still ranked.
    let packs = [None, Some(vec![MetricPack::Schema, MetricPack::Patterns])];
    for ((name, path, engine), packs) in routes
        .iter()
        .flat_map(|route| packs.iter().map(move |packs| (route, packs)))
    {
        for (locale, expected) in [
            (None, PatternLocale::Unset),
            (Some(Locale::It), PatternLocale::It),
        ] {
            let mut profiler = Profiler::new();
            if let Some(packs) = packs {
                profiler = profiler.metric_packs(packs.clone());
            }
            if let Some(engine) = engine {
                profiler = profiler.engine(*engine);
            }
            if let Some(locale) = locale {
                profiler = profiler.locale(locale);
            }
            let got = recorded(&profiler.analyze_file(path).unwrap());
            if got != Some(expected) {
                wrong.push(format!("{name} {packs:?} with {locale:?}: {got:?}"));
            }
        }
    }
    // The legacy entry point takes no options and ranks without a locale.
    let legacy = analyze_csv_file(csv.path(), &Default::default()).unwrap();
    if recorded(&legacy) != Some(PatternLocale::Unset) {
        wrong.push(format!("analyze_csv_file: {:?}", recorded(&legacy)));
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_document_keeps_the_settings_and_their_absence() {
    let csv = file(".csv", EMAILS);
    let report = Profiler::new()
        .locale(Locale::It)
        .analyze_file(csv.path())
        .unwrap();
    let document = serde_json::to_value(&report).unwrap();
    assert_eq!(
        document["analysis_settings"],
        json!({"pattern_locale": "IT", "columns": {"selection": "all"}})
    );
    let reloaded: ProfileReport = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(recorded(&reloaded), Some(PatternLocale::It));

    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    assert_eq!(
        summary["analysis_settings"],
        json!({"pattern_locale": "IT", "columns": {"selection": "all"}})
    );

    // Written before the field: unknown, not the defaults.
    let mut legacy = document.clone();
    legacy.as_object_mut().unwrap().remove("analysis_settings");
    let legacy: ProfileReport = serde_json::from_value(legacy).unwrap();
    assert_eq!(legacy.analysis_settings, None);
    let resaved = serde_json::to_value(&legacy).unwrap();
    assert!(resaved.get("analysis_settings").is_none());
    // Built directly, without the options it was profiled with.
    assert_eq!(
        ProfileReport::new(
            report.data_source.clone(),
            Vec::new(),
            report.execution.clone(),
            None
        )
        .analysis_settings,
        None
    );

    for malformed in [
        Value::Null,
        json!({}),
        json!({"pattern_locale": null}),
        json!({"pattern_locale": "it"}),
        json!({"pattern_locale": "XX"}),
    ] {
        let mut broken = document.clone();
        broken["analysis_settings"] = malformed.clone();
        assert!(
            serde_json::from_value::<ProfileReport>(broken).is_err(),
            "{malformed} decoded"
        );
    }
    // A setting a later release adds is ignored.
    let mut later = document;
    later["analysis_settings"]["metric_packs"] = json!(["schema"]);
    let later: ProfileReport = serde_json::from_value(later).unwrap();
    assert_eq!(recorded(&later), Some(PatternLocale::It));
}

#[test]
fn the_delta_and_the_gate_compare_the_locale() {
    let csv = file(".csv", EMAILS);
    let profile = |locale: Option<Locale>| {
        let mut profiler = Profiler::new();
        if let Some(locale) = locale {
            profiler = profiler.locale(locale);
        }
        profiler.analyze_file(csv.path()).unwrap()
    };
    let unset = profile(None);
    let italian = profile(Some(Locale::It));
    let mut legacy = serde_json::to_value(&unset).unwrap();
    legacy.as_object_mut().unwrap().remove("analysis_settings");
    let legacy: ProfileReport = serde_json::from_value(legacy).unwrap();

    let same = |a: &ProfileReport, b: &ProfileReport| {
        ReportDelta::between(a, b).configuration.pattern_locale.same
    };
    assert_eq!(same(&unset, &profile(None)), Some(true));
    assert_eq!(same(&unset, &italian), Some(false));
    assert_eq!(same(&legacy, &unset), None);
    // A flat summary carries it too, and an explicit null there is as
    // malformed as in the canonical document.
    let flat = ReportSide::from_summary_json(&italian.summary_json().unwrap()).unwrap();
    let mut null: Value = serde_json::from_str(&italian.summary_json().unwrap()).unwrap();
    null["analysis_settings"] = Value::Null;
    assert!(ReportSide::from_summary_json(&null.to_string()).is_err());
    assert_eq!(
        ReportDelta::between_sides(&flat, &ReportSide::from_report(&italian))
            .configuration
            .pattern_locale
            .same,
        Some(true)
    );

    let policy = QualityPolicy::new().max_dimension_score_drop(QualityDimension::Validity, 100.0);
    let validity = |report: &ProfileReport, baseline: &ProfileReport| {
        let result = policy
            .evaluate_against(report, &ReportSide::from_report(baseline))
            .unwrap();
        let check = result
            .checks
            .into_iter()
            .find(|check| check.code == CheckCode::MaxDimensionScoreDrop)
            .unwrap();
        (check.status.clone(), check.not_evaluated().cloned())
    };
    assert_eq!(validity(&profile(None), &unset).0, CheckStatus::Passed);
    assert!(matches!(
        validity(&italian, &unset).1,
        Some(NotEvaluated::SettingChanged { setting, .. }) if setting == "pattern_locale"
    ));
    assert!(matches!(
        validity(&unset, &legacy).1,
        Some(NotEvaluated::SettingUnrecorded { setting, .. }) if setting == "pattern_locale"
    ));
}
