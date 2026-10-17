//! Pattern semantics on every applicable route. Python twin: test_pattern_detection.py.
#[path = "common/profiling_routes.rs"]
mod profiling_routes;
use dataprof::{FindingCode, Locale};
use std::collections::BTreeMap;

#[derive(serde::Deserialize)]
struct Case {
    locale: Option<String>,
    values: Vec<String>,
    expected: BTreeMap<String, usize>,
    #[serde(default)]
    absent: Vec<String>,
    #[serde(default)]
    confidence: BTreeMap<String, f64>,
    sensitive: Option<bool>,
}

fn check(group: &str) {
    let cases: BTreeMap<String, Vec<Case>> =
        serde_json::from_str(include_str!("fixtures/pattern_detection.json")).unwrap();
    let mut wrong = Vec::new();
    for (index, case) in cases[group].iter().enumerate() {
        let locale = Locale::parse_optional(case.locale.as_deref()).unwrap();
        let catalogue = dataprof::list_patterns(locale);
        if group == "complete_iso_datetime"
            && !catalogue
                .iter()
                .find(|p| p.name == "DateTime (ISO)")
                .unwrap()
                .regex
                .ends_with('$')
        {
            wrong.push("ISO datetime catalogue exposes an unanchored regex".to_owned());
        }
        let mut serialized = None;
        for (route, report) in profiling_routes::reports_with_locale(&case.values, locale) {
            let patterns = report.column_profiles[0].patterns.as_ref().unwrap();
            for (name, count) in &case.expected {
                match patterns.iter().find(|p| &p.name == name) {
                    Some(pattern) => {
                        if pattern.regex
                            != catalogue.iter().find(|p| p.name == *name).unwrap().regex
                        {
                            wrong.push(format!(
                                "[{route}] {group}/{index}: catalogue regex differs for {name}"
                            ));
                        }
                        let percentage = *count as f64 / case.values.len() as f64 * 100.0;
                        if pattern.match_count != *count
                            || (pattern.match_percentage - percentage).abs() > 1e-9
                        {
                            wrong.push(format!("[{route}] {group}/{index} {name}: {pattern:?}, expected {count}/{percentage}"));
                        }
                        if let Some(expected) = case.confidence.get(name)
                            && (pattern.confidence - expected).abs() > 1e-12
                        {
                            wrong.push(format!("[{route}] {group}/{index} {name}: confidence {}, expected {expected}",pattern.confidence));
                        }
                    }
                    None => wrong.push(format!(
                        "[{route}] {group}/{index}: missing {name}: {patterns:?}"
                    )),
                }
            }
            for name in &case.absent {
                if patterns.iter().any(|p| &p.name == name) {
                    wrong.push(format!("[{route}] {group}/{index}: unexpected {name}"));
                }
            }
            if let Some(expected) = case.sensitive {
                let actual = report
                    .findings()
                    .findings
                    .iter()
                    .any(|f| f.code == FindingCode::SensitivePattern);
                if actual != expected {
                    wrong.push(format!(
                        "[{route}] {group}/{index}: sensitive={actual}, expected {expected}"
                    ));
                }
            }
            let actual =
                serde_json::to_value(&report).unwrap()["column_profiles"][0]["patterns"].clone();
            if let Some(expected) = &serialized {
                if &actual != expected {
                    wrong.push(format!(
                        "[{route}] {group}/{index}: serialized patterns differ"
                    ));
                }
            } else {
                serialized = Some(actual);
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn configured_locale_patterns_cannot_be_suppressed_by_foreign_locales() {
    check("configured_locale");
}
#[test]
fn unconfigured_postcodes_preserve_their_ambiguity() {
    check("postcode_ambiguity");
}
#[test]
fn uppercase_uuids_match() {
    check("uuid_case");
}
#[test]
fn pattern_counts_use_semantically_validated_values() {
    check("validated_counts");
}
#[test]
fn printed_and_lowercase_ibans_match() {
    check("printed_iban");
}
#[test]
fn domestic_italian_phones_require_formatting() {
    check("domestic_phone");
}
#[test]
fn decimal_numbers_are_not_domestic_italian_phones() {
    check("decimals_are_not_phones");
}
#[test]
fn us_phones_need_separators_between_every_group_or_none() {
    check("us_phone_separators");
}
#[test]
fn coordinates_require_decimal_points_in_both_components() {
    check("coordinate_decimal_points");
}
#[test]
fn iso_datetime_requires_complete_valid_values() {
    check("complete_iso_datetime");
}
#[test]
fn slash_date_labels_do_not_assert_an_order() {
    check("slash_date_label");
}
