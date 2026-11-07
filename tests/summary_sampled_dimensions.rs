//! A flat summary's `quality.sampled_dimensions` is read only as a list of
//! strings (#1235). The Rust reader types the field, so a string, a mapping or
//! a list holding a non-string is refused; the Python loader now refuses the
//! same values instead of reading a string as a list of its characters.
//!
//! `python/tests/test_summary_sampled_dimensions.py` is the Python twin.

use std::io::Write;

use dataprof::{Profiler, ReportSide};
use serde_json::{Value, json};

fn summary() -> Value {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(b"d,v\n2026-01-01,1\n2026-01-01,2\n2026-01-01,3\n2026-01-01,4\n")
        .unwrap();
    file.flush().unwrap();
    let report = Profiler::new().analyze_file(file.path()).unwrap();
    serde_json::from_str(&report.summary_json().unwrap()).unwrap()
}

fn read(sampled: Option<Value>) -> Result<ReportSide, String> {
    let mut document = summary();
    let quality = document["quality"].as_object_mut().unwrap();
    match sampled {
        Some(value) => {
            quality.insert("sampled_dimensions".to_string(), value);
        }
        None => {
            quality.remove("sampled_dimensions");
        }
    }
    ReportSide::from_summary_json(&document.to_string()).map_err(|error| error.to_string())
}

#[test]
fn a_malformed_sampled_list_is_refused() {
    let accepted: Vec<String> = [
        json!("timeliness"),
        json!(""),
        json!(["timeliness", 1]),
        json!({"timeliness": true}),
        json!(3),
    ]
    .into_iter()
    .filter(|value| read(Some(value.clone())).is_ok())
    .map(|value| value.to_string())
    .collect();
    assert!(accepted.is_empty(), "accepted: {accepted:?}");
}

#[test]
fn a_recorded_or_unrecorded_list_is_read() {
    let failed: Vec<(String, String)> = [
        Some(json!(["timeliness"])),
        Some(json!([])),
        Some(Value::Null),
        None,
    ]
    .into_iter()
    .filter_map(|value| {
        let label = format!("{value:?}");
        read(value).err().map(|error| (label, error))
    })
    .collect();
    assert!(failed.is_empty(), "refused: {failed:?}");
}
