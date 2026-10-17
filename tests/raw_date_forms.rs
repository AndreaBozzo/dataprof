//! The date parser fast paths keep every raw date count. Python twin: test_raw_date_forms.py.
#[path = "common/profiling_routes.rs"]
mod profiling_routes;
use dataprof_runtime::StreamingStatistics;
use std::collections::BTreeMap;

#[test]
fn every_accepted_date_form_keeps_its_count_on_every_route() {
    let forms: BTreeMap<String, Vec<String>> =
        serde_json::from_str(include_str!("fixtures/raw_date_forms.json")).unwrap();
    let mut stats = StreamingStatistics::new();
    for value in forms.values().flatten() {
        stats.update(value);
    }
    assert_eq!(stats.date_match_count(), forms["valid"].len());
    let mut wrong = Vec::new();
    for valid in &forms["valid"] {
        for invalid in &forms["invalid"] {
            let mut values = vec![valid.clone(); 20];
            values.push(invalid.clone());
            for (route, report) in profiling_routes::reports(&values) {
                let actual = report
                    .quality
                    .as_ref()
                    .and_then(|q| q.metrics.timeliness.as_ref())
                    .map(|m| m.invalid_date_values);
                if actual != Some(1) {
                    wrong.push(format!(
                        "[{route}] {valid:?}/{invalid:?}: invalid dates={actual:?}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
