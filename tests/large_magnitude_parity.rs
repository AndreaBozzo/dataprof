//! Moment statistics at large magnitudes serialize alike on every route (#1178).
//!
//! `tests/fixtures/large_magnitude_stats.csv` holds 104 floats near 1e20. The
//! routes accumulate them in different orders, so the raw `std_dev` and
//! `variance` differ in their last bits, and 4 decimal places stopped
//! absorbing that above about 1e11: the file routes serialized
//! `2.85136062083506e19` and the in-memory ones `2.8513606208350605e19`.
//! Moment statistics now keep at most 12 significant digits, and every route
//! must serialize the same numbers.
//!
//! `python/tests/test_large_magnitude_parity.py` is the Python twin.

use std::fs;
use std::io::Write;
use std::path::PathBuf;

use dataprof::{EngineType, Profiler, analyze_column};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

const MOMENTS: [&str; 5] = ["mean", "std_dev", "variance", "skewness", "kurtosis"];

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("large_magnitude_stats.csv")
}

fn values() -> Vec<String> {
    fs::read_to_string(fixture())
        .unwrap()
        .lines()
        .skip(1)
        .map(str::to_string)
        .collect()
}

fn temp(suffix: &str, body: String) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write!(file, "{body}").unwrap();
    file.flush().unwrap();
    file
}

/// The serialized moment statistics of the one column a stats document holds.
fn moments(stats: &Value) -> Vec<Value> {
    MOMENTS.iter().map(|name| stats[name].clone()).collect()
}

#[test]
fn large_magnitude_moments_serialize_alike_on_every_route() {
    let mut routes: Vec<(String, Vec<Value>)> = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(fixture())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        let document = serde_json::to_value(&report).unwrap();
        routes.push((
            format!("{engine:?}"),
            moments(&document["column_profiles"][0]["stats"]["Numeric"]),
        ));
    }

    let records: Vec<String> = values()
        .iter()
        .map(|value| json!({ "amount": value.parse::<f64>().unwrap() }).to_string())
        .collect();
    for (label, file) in [
        ("json", temp(".json", format!("[{}]", records.join(",")))),
        ("jsonl", temp(".jsonl", records.join("\n") + "\n")),
    ] {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        let document = serde_json::to_value(&report).unwrap();
        routes.push((
            label.to_string(),
            moments(&document["column_profiles"][0]["stats"]["Numeric"]),
        ));
    }

    let column = serde_json::to_value(analyze_column("amount", &values())).unwrap();
    routes.push((
        "analyze_column".to_string(),
        moments(&column["stats"]["Numeric"]),
    ));

    let (first_label, first) = &routes[0];
    assert!(
        first.iter().all(Value::is_number),
        "[{first_label}] {first:?}"
    );
    let wrong: Vec<String> = routes
        .iter()
        .filter(|(_, observed)| observed != first)
        .map(|(label, observed)| format!("[{label}] {observed:?} != [{first_label}] {first:?}"))
        .collect();
    assert!(wrong.is_empty(), "{wrong:#?}");
}
