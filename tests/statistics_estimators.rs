//! Adjusted sample statistics agree across every input path.
//! `python/tests/test_statistics_estimators.py` is the Python twin.

#[path = "common/profiling_routes.rs"]
mod profiling_routes;
use dataprof::ColumnStats;
use profiling_routes::reports;

type Fixture = (
    &'static str,
    Vec<String>,
    Option<f64>,
    Option<f64>,
    Option<f64>,
);

fn fixtures() -> Vec<Fixture> {
    let values = [1.0, 2.0, 3.0, 10.0, 4.0, 5.0];
    vec![
        (
            "asymmetric",
            values.iter().map(|v| v.to_string()).collect(),
            Some(1.4396),
            Some(2.4377),
            Some(76.52),
        ),
        (
            "small_units",
            values.iter().map(|v| (v * 1e-12).to_string()).collect(),
            Some(1.4396),
            Some(2.4377),
            Some(76.52),
        ),
        (
            "constant",
            vec!["2".into(); 4],
            Some(0.0),
            Some(0.0),
            Some(0.0),
        ),
        ("two", vec!["1".into(), "2".into()], None, None, Some(47.14)),
        (
            "three",
            vec!["1".into(), "2".into(), "3".into()],
            Some(0.0),
            None,
            Some(50.0),
        ),
        (
            "zero_mean",
            vec!["-1".into(), "0".into(), "1".into()],
            Some(0.0),
            None,
            None,
        ),
    ]
}

fn check_metric(field: &str) {
    let mut wrong = Vec::new();
    for (fixture, values, skew, kurt, cv) in fixtures() {
        let expected = match field {
            "skewness" => skew.map(serde_json::Value::from),
            "kurtosis" => kurt.map(serde_json::Value::from),
            "coefficient_of_variation" => cv.map(serde_json::Value::from),
            "is_approximate" => Some(serde_json::Value::Bool(false)),
            _ => unreachable!(),
        };
        for (route, report) in reports(&values) {
            let ColumnStats::Numeric(stats) = &report.column_profiles[0].stats else {
                wrong.push(format!("[{route}] {fixture}: no numeric statistics"));
                continue;
            };
            let serialized = serde_json::to_value(stats).unwrap();
            let actual = serialized.get(field).cloned();
            if actual != expected {
                wrong.push(format!(
                    "[{route}] {fixture}: {field}={actual:?}, expected {expected:?}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn every_path_uses_adjusted_sample_skewness() {
    check_metric("skewness");
}

#[test]
fn every_path_uses_adjusted_sample_excess_kurtosis() {
    check_metric("kurtosis");
}

#[test]
fn every_path_keeps_cv_when_units_are_small() {
    check_metric("coefficient_of_variation");
}

#[test]
fn every_path_marks_exact_numeric_statistics_as_false() {
    check_metric("is_approximate");
}
