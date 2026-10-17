//! Profile the oracle-v1 matrix through the file engines and check it against
//! answers computed without dataprof (#829).
//!
//! `tests/fixtures/oracle-v1.json` holds 20 cases whose expected values were
//! computed with exact rational arithmetic, independently of this code. This
//! runs each case as CSV through the incremental and columnar engines and as
//! JSON and JSONL, compares at the serialized precision, and collects every
//! disagreement before asserting. `KNOWN` lists the ones an open issue owns,
//! strictly: a listed disagreement that stops occurring fails too.
//!
//! `python/tests/test_oracle.py` is the Python twin, and adds the in-memory
//! routes (dicts, records, pandas, polars, Arrow) and Parquet.

use std::path::Path;

use dataprof::{EngineType, ProfileReport, Profiler};
use serde_json::{Value, json};

const ROUTES: [&str; 4] = ["csv_incremental", "csv_columnar", "json", "jsonl"];

/// Half a unit of the 4dp serialized precision, plus room for the binary
/// representation of the rounded value.
const ABS_TOLERANCE: f64 = 0.000051;
/// For magnitudes where 4dp carries no information (around 1e307).
const REL_TOLERANCE: f64 = 1e-12;

/// (case, route, field) and the issue that owns the disagreement. A route of
/// "*" covers every route the case runs on. The same entries as the Python
/// twin, for the routes this file runs.
const KNOWN: &[(&str, &str, &str, &str)] = &[
    // 64-bit integer extrema and order statistics are taken in f64, so values
    // past 2^53 collapse: the extremes round, distinct values tie for a mode,
    // and the quartiles of a narrow spread coincide.
    ("above_2_pow_53", "*", "min", "#877"),
    ("above_2_pow_53", "*", "max", "#877"),
    ("above_2_pow_53", "*", "mode", "#877"),
    ("epoch_nanoseconds", "*", "min", "#877"),
    ("epoch_nanoseconds", "*", "max", "#877"),
    ("epoch_nanoseconds", "*", "mode", "#877"),
    ("epoch_nanoseconds", "*", "quartiles.iqr", "#877"),
    ("uint64_edge", "*", "min", "#877"),
    ("uint64_edge", "*", "max", "#877"),
    ("uint64_edge", "*", "mode", "#877"),
    ("uint64_edge", "*", "quartiles.iqr", "#877"),
    // An all-null text column reports lengths of 0 rather than none.
    ("all_null", "*", "stats", "#983"),
];

#[derive(Debug, Clone, PartialEq)]
struct Mismatch {
    case: String,
    route: &'static str,
    field: String,
    expected: String,
    actual: String,
}

fn fixture() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/oracle-v1.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// The case's values as JSON, typed by its dtype: numeric values are strings
/// in the fixture so no JSON tool can round them before profiling.
fn values(case: &Value) -> Vec<Value> {
    let raw = case["values"].as_array().unwrap();
    if case["kind"] != "numeric" {
        return raw.clone();
    }
    raw.iter()
        .map(|value| {
            let text = value.as_str().unwrap();
            // serde_json keeps an integer exact up to u64; a float parses as f64.
            serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!(text))
        })
        .collect()
}

fn csv_text(values: &[Value]) -> String {
    let mut text = String::from("\"v\"\n");
    for value in values {
        let cell = match value {
            Value::Null => String::new(),
            Value::String(text) => text.replace('"', "\"\""),
            // Python's csv writer prints a float with repr; the fixture's
            // float strings are already that form.
            other => other.to_string(),
        };
        text.push_str(&format!("\"{cell}\"\n"));
    }
    text
}

fn profile(case: &Value, route: &str, dir: &Path) -> Option<ProfileReport> {
    let values = values(case);
    let records: Vec<Value> = values.iter().map(|value| json!({"v": value})).collect();
    let id = case["id"].as_str().unwrap();
    let wanted = case["routes"].as_array();
    let allowed = |name: &str| wanted.is_none_or(|routes| routes.iter().any(|r| r == name));
    let path = match route {
        // A container in a CSV cell is text, not the same logical value.
        "csv_incremental" | "csv_columnar" if case["kind"] == "nested" => return None,
        "csv_incremental" | "csv_columnar" => {
            let path = dir.join(format!("{id}.csv"));
            std::fs::write(&path, csv_text(&values)).unwrap();
            path
        }
        "json" if allowed("json") => {
            let path = dir.join(format!("{id}.json"));
            std::fs::write(&path, serde_json::to_string(&records).unwrap()).unwrap();
            path
        }
        "jsonl" if allowed("jsonl") => {
            let path = dir.join(format!("{id}.jsonl"));
            let lines: String = records.iter().map(|row| format!("{row}\n")).collect();
            std::fs::write(&path, lines).unwrap();
            path
        }
        _ => return None,
    };
    let profiler = match route {
        "csv_incremental" => Profiler::new().engine(EngineType::Incremental),
        "csv_columnar" => Profiler::new().engine(EngineType::Columnar),
        _ => Profiler::new(),
    };
    Some(
        profiler
            .analyze_file(&path)
            .unwrap_or_else(|e| panic!("{id} via {route}: {e}")),
    )
}

/// A fraction such as `"5/3"`, or a decimal, as the nearest f64.
fn number(text: &str) -> f64 {
    match text.split_once('/') {
        Some((num, den)) => num.parse::<f64>().unwrap() / den.parse::<f64>().unwrap(),
        None => text.parse().unwrap(),
    }
}

/// An exact rational over i128, for integer cases past 2^53.
fn rational(text: &str) -> (i128, i128) {
    match text.split_once('/') {
        Some((num, den)) => (num.parse().unwrap(), den.parse().unwrap()),
        None => (text.parse().unwrap(), 1),
    }
}

/// Collects the fields of one (case, route) run that disagree with the oracle.
struct Checker<'a> {
    case: &'a str,
    route: &'static str,
    found: Vec<Mismatch>,
}

impl Checker<'_> {
    fn record(&mut self, field: &str, wanted: impl ToString, actual: &Value) {
        self.found.push(Mismatch {
            case: self.case.to_string(),
            route: self.route,
            field: field.to_string(),
            expected: wanted.to_string(),
            actual: actual.to_string(),
        });
    }

    /// Compare at the serialized precision; `relative` for magnitudes where
    /// 4dp carries no information.
    fn compare(&mut self, field: &str, actual: &Value, wanted: &Value, relative: bool) {
        let good = match wanted {
            Value::Null => actual.is_null(),
            Value::String(text) if field != "data_type" => actual.as_f64().is_some_and(|a| {
                let target = number(text);
                if relative {
                    (a - target).abs() <= REL_TOLERANCE * target.abs()
                } else {
                    (a - target).abs() <= ABS_TOLERANCE
                }
            }),
            other => actual == other,
        };
        if !good {
            self.record(field, wanted, actual);
        }
    }
}

/// The gap from `value` to the next f64 away from zero.
fn ulp(value: f64) -> f64 {
    let magnitude = value.abs();
    f64::from_bits(magnitude.to_bits() + 1) - magnitude
}

fn check(case: &Value, route: &'static str, report: &ProfileReport) -> Vec<Mismatch> {
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    let column = &summary["columns"][0];
    let expected = if case["expected"].is_object() {
        &case["expected"]
    } else {
        &case["expected_common_markers"]
    };
    let id = case["id"].as_str().unwrap();
    let mut checker = Checker {
        case: id,
        route,
        found: Vec::new(),
    };

    for field in [
        "total_count",
        "null_count",
        "unique_count",
        "data_type",
        "stats",
        "null_percentage",
    ] {
        if let Some(wanted) = expected.get(field) {
            checker.compare(field, &column[field], wanted, false);
        }
    }
    if let Some(wanted) = expected.get("duplicate_rows_single_column") {
        checker.compare(
            "duplicate_rows",
            &summary["quality"]["uniqueness"]["duplicate_rows"],
            wanted,
            false,
        );
    }
    if case["kind"] != "numeric" {
        return checker.found;
    }

    let stats = &column["stats"];
    let wide = case["dtype"] != "float64"
        && case["values"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| rational(v.as_str().unwrap()).0.unsigned_abs() > 1 << 53);
    let extreme = id == "variance_fits_m2_does_not";
    let variance = if extreme {
        &expected["sample_variance_binary64"]
    } else {
        &expected["variance_4dp"]
    };
    checker.compare("variance", &stats["variance"], variance, extreme);
    checker.compare(
        "std_dev",
        &stats["std_dev"],
        &expected["std_dev_4dp"],
        extreme,
    );

    if wide {
        // Past 2^53 every f64 is an integer, and an exact mean can fall between
        // two of them; either neighbour is faithful, so the bound is half an ulp.
        let wanted = expected["mean_exact"].as_str().unwrap();
        let (num, den) = rational(wanted);
        let mean = &stats["mean"];
        let faithful = mean
            .as_f64()
            .is_some_and(|m| ((m as i128) * den - num).abs() * 2 <= (ulp(m) as i128) * den);
        if !faithful {
            checker.record("mean", wanted, mean);
        }
        // An exact integer extreme needs an integer-preserving value;
        // comparing after a float conversion would erase the defect (#877).
        for name in ["min", "max"] {
            let wanted = expected[format!("{name}_exact")].as_str().unwrap();
            let actual = &stats[name];
            if !actual
                .as_f64()
                .is_some_and(|a| (a as i128, 1) == rational(wanted))
            {
                checker.record(name, wanted, actual);
            }
        }
    } else {
        checker.compare("mean", &stats["mean"], &expected["mean_4dp"], extreme);
        for name in ["min", "max"] {
            let exact = number(expected[format!("{name}_exact")].as_str().unwrap());
            checker.compare(name, &stats[name], &json!(exact.to_string()), extreme);
        }
    }
    if extreme {
        return checker.found;
    }
    checker.compare(
        "skewness",
        &stats["skewness"],
        &expected["adjusted_sample_skewness_4dp"],
        false,
    );
    checker.compare(
        "kurtosis",
        &stats["kurtosis"],
        &expected["adjusted_sample_excess_kurtosis_4dp"],
        false,
    );
    checker.compare("median", &stats["median"], &expected["median_exact"], wide);
    let mode = stats.get("mode").cloned().unwrap_or(Value::Null);
    checker.compare("mode", &mode, &expected["mode_exact"], wide);
    checker.compare(
        "outlier_count",
        &stats["outlier_count"],
        &expected["outlier_count"],
        false,
    );
    match expected["quartiles_4dp"].as_object() {
        None => checker.compare(
            "quartiles",
            stats.get("quartiles").unwrap_or(&Value::Null),
            &Value::Null,
            false,
        ),
        Some(quartiles) => {
            for (name, wanted) in quartiles {
                checker.compare(
                    &format!("quartiles.{name}"),
                    &stats["quartiles"][name],
                    wanted,
                    wide,
                );
            }
        }
    }
    checker.found
}

fn owner(m: &Mismatch) -> Option<&'static str> {
    KNOWN
        .iter()
        .find(|(case, route, field, _)| {
            *case == m.case && (*route == "*" || *route == m.route) && *field == m.field
        })
        .map(|(.., issue)| *issue)
}

#[test]
fn every_file_engine_matches_the_independent_answer() {
    let dir = tempfile::tempdir().unwrap();
    let mut runs = Vec::new();
    let mut mismatches = Vec::new();
    for case in fixture()["cases"].as_array().unwrap() {
        for route in ROUTES {
            if let Some(report) = profile(case, route, dir.path()) {
                runs.push((case["id"].as_str().unwrap().to_string(), route));
                mismatches.extend(check(case, route, &report));
            }
        }
    }

    let unexplained: Vec<String> = mismatches
        .iter()
        .filter(|m| owner(m).is_none())
        .map(|m| {
            format!(
                "  {} via {}: {} expected {}, got {}",
                m.case, m.route, m.field, m.expected, m.actual
            )
        })
        .collect();
    let stale: Vec<String> = KNOWN
        .iter()
        .filter(|(case, route, field, _)| {
            let ran = runs
                .iter()
                .any(|(c, r)| c == case && (*route == "*" || route == r));
            ran && !mismatches.iter().any(|m| {
                m.case == *case && m.field == *field && (*route == "*" || *route == m.route)
            })
        })
        .map(|(case, route, field, issue)| format!("  {case} via {route}: {field} ({issue})"))
        .collect();

    assert!(
        unexplained.is_empty(),
        "disagreements with the oracle:\n{}",
        unexplained.join("\n")
    );
    assert!(
        stale.is_empty(),
        "known disagreements that no longer occur:\n{}",
        stale.join("\n")
    );
    // Every case ran through at least one route.
    assert_eq!(
        runs.iter()
            .map(|(case, _)| case.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        20
    );
}
