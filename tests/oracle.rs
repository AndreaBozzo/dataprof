//! Profile the oracle matrix through the file engines and check it against
//! answers computed without dataprof (#829).
//!
//! `tests/fixtures/oracle-v2.json` holds cases whose exact expected values
//! `python/tests/_oracle_fixture.py` computes with rational arithmetic,
//! independently of this code. This runs each case that has a text spelling
//! as CSV through the incremental and columnar engines and as JSON and JSONL,
//! rounds the exact answers as the output contract serializes them, and
//! collects every disagreement before asserting. `KNOWN` lists the ones an
//! open issue owns, strictly: a listed disagreement that stops occurring
//! fails too.
//!
//! `python/tests/test_oracle.py` is the Python twin, and adds the in-memory
//! routes (dicts, records, pandas, polars, Arrow tables and streams) and
//! Parquet in each writer layout, which carry the Float32 and decimal cases.

use std::collections::BTreeSet;
use std::path::Path;

use dataprof::{EngineType, ProfileReport, Profiler};
use serde_json::{Value, json};

const ROUTES: [&str; 4] = ["csv_incremental", "csv_columnar", "json", "jsonl"];

/// The float error a binary64 computation may carry, relative to the exact
/// answer: a tie at the serialized precision closer than this may round
/// either way. The same bound compares the statistics of a `relative` case.
const REL_TOLERANCE: f64 = 1e-12;
const ABS_TOLERANCE: f64 = 1e-12;

/// (case, route, field) and the issue that owns the disagreement. A route of
/// "*" covers every route the case runs on. The same entries as the Python
/// twin, for the routes this file runs.
const KNOWN: &[(&str, &str, &str, &str)] = &[
    // 64-bit integer extrema and order statistics are taken in f64, so values
    // past 2^53 collapse: the extremes round, distinct values tie for a mode,
    // and the quartiles of a narrow spread coincide.
    ("above_2_pow_53", "*", "min", "#1038"),
    ("above_2_pow_53", "*", "max", "#1038"),
    ("above_2_pow_53", "*", "mode", "#1038"),
    ("epoch_nanoseconds", "*", "min", "#1038"),
    ("epoch_nanoseconds", "*", "max", "#1038"),
    ("epoch_nanoseconds", "*", "mode", "#1038"),
    ("epoch_nanoseconds", "*", "quartiles.iqr", "#1038"),
    ("uint64_edge", "*", "min", "#1038"),
    ("uint64_edge", "*", "max", "#1038"),
    ("uint64_edge", "*", "mode", "#1038"),
    ("uint64_edge", "*", "quartiles.iqr", "#1038"),
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
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/oracle-v2.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

/// Whether the case's `routes` list, if any, names `route`.
fn allowed(case: &Value, route: &str) -> bool {
    case["routes"]
        .as_array()
        .is_none_or(|routes| routes.iter().any(|r| r == route))
}

/// The case's values as JSON, typed by its dtype: numeric values are strings
/// in the fixture so no JSON tool can round them before profiling.
fn values(case: &Value) -> Vec<Value> {
    let raw = case["values"].as_array().unwrap();
    if case["kind"] != "numeric" {
        return raw.clone();
    }
    raw.iter()
        .map(|value| match value.as_str() {
            // serde_json keeps an integer exact up to u64; a decimal or a
            // float parses as the nearest f64, as any JSON reader holds it.
            Some(text) => serde_json::from_str::<Value>(text).unwrap(),
            None => Value::Null,
        })
        .collect()
}

/// One quoted cell per value: the fixture's literal for a number, so the CSV
/// holds exactly the spelling the case states.
fn csv_text(case: &Value) -> String {
    let mut text = String::from("\"v\"\n");
    for value in case["values"].as_array().unwrap() {
        let cell = match value {
            Value::Null => String::new(),
            Value::String(text) => text.replace('"', "\"\""),
            other => other.to_string(),
        };
        text.push_str(&format!("\"{cell}\"\n"));
    }
    text
}

fn profile(case: &Value, route: &str, dir: &Path) -> Option<ProfileReport> {
    let records: Vec<Value> = values(case)
        .into_iter()
        .map(|value| json!({"v": value}))
        .collect();
    let id = case["id"].as_str().unwrap();
    let path = match route {
        "csv_incremental" | "csv_columnar" if allowed(case, "csv") => {
            let path = dir.join(format!("{id}.csv"));
            std::fs::write(&path, csv_text(case)).unwrap();
            path
        }
        "json" if allowed(case, "json") => {
            let path = dir.join(format!("{id}.json"));
            std::fs::write(&path, serde_json::to_string(&records).unwrap()).unwrap();
            path
        }
        "jsonl" if allowed(case, "jsonl") => {
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

/// An exact rational over i128, for integer cases past 2^53: an integer, a
/// fraction such as `"36893488147419103227/2"`, or a decimal such as
/// `"18446744073709551612.7500"`.
fn rational(text: &str) -> (i128, i128) {
    if let Some((num, den)) = text.split_once('/') {
        return (num.parse().unwrap(), den.parse().unwrap());
    }
    match text.split_once('.') {
        Some((whole, fraction)) => (
            format!("{whole}{fraction}").parse().unwrap(),
            10_i128.pow(fraction.len() as u32),
        ),
        None => (text.parse().unwrap(), 1),
    }
}

/// A reported number as an exact integer when it is one. A JSON integer is
/// read as an integer: `as_f64` would round `9007199254740993` to a
/// neighbour, and a fixed #1038 would then still read as wrong.
fn exact_integer(value: &Value) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
        .or_else(|| {
            value
                .as_f64()
                .filter(|f| f.fract() == 0.0 && f.abs() < 1e38)
                .map(|f| f as i128)
        })
}

/// Whether `actual` is the exact `wanted` as the contract serializes it:
/// rounded at `places`, ties away from zero ([`f64::round`]). A binary64
/// computation of a value within float error of a tie may land on either
/// side of it, so there, and only there, both neighbours are right.
fn serialized(actual: &Value, wanted: &str, places: i32) -> bool {
    let Some(actual) = actual.as_f64() else {
        return false;
    };
    let exact = number(wanted);
    let slack = exact.abs() * REL_TOLERANCE + ABS_TOLERANCE;
    let scale = 10_f64.powi(places);
    let lowest = ((exact - slack) * scale).round();
    let highest = ((exact + slack) * scale).round();
    let scaled = actual * scale;
    let units = scaled.round();
    // A serialized value sits on the grid, up to its binary representation.
    let on_grid = (scaled - units).abs() <= 1e-9 * units.abs().max(1.0);
    on_grid && lowest <= units && units <= highest
}

/// Whether `actual` is a faithful binary64 rendering of the exact `wanted`.
///
/// Past 2^53 adjacent floats are hundreds apart, so the serialized precision
/// says nothing there: the bound is half an ulp of the exact value, the error
/// rounding to the nearest float may carry.
fn faithful(actual: &Value, wanted: &str) -> bool {
    let (num, den) = rational(wanted);
    let target = num as f64 / den as f64;
    match exact_integer(actual) {
        Some(a) => (a * den - num).abs() * 2 <= (ulp(target) as i128) * den,
        None => actual
            .as_f64()
            .is_some_and(|a| (a - target).abs() <= ulp(target)),
    }
}

/// The gap from `value` to the next f64 away from zero.
fn ulp(value: f64) -> f64 {
    let magnitude = value.abs();
    f64::from_bits(magnitude.to_bits() + 1) - magnitude
}

/// Decimal places the output contract serializes for `field`.
fn places(field: &str) -> i32 {
    match field.split('.').next().unwrap() {
        "null_percentage" | "coefficient_of_variation" | "quartiles" => 2,
        _ => 4,
    }
}

#[derive(Clone, Copy, PartialEq)]
enum How {
    Serialized,
    Faithful,
    Relative,
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

    fn compare(&mut self, field: &str, actual: &Value, wanted: &Value, how: How) {
        let good = match wanted {
            Value::Null => actual.is_null(),
            Value::String(text) if field != "data_type" => match how {
                How::Serialized => serialized(actual, text, places(field)),
                How::Faithful => faithful(actual, text),
                How::Relative => actual.as_f64().is_some_and(|a| {
                    (a - number(text)).abs() <= REL_TOLERANCE * number(text).abs()
                }),
            },
            other => actual == other,
        };
        if !good {
            self.record(field, wanted, actual);
        }
    }
}

fn check(case: &Value, route: &'static str, report: &ProfileReport) -> Vec<Mismatch> {
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    let column = &summary["columns"][0];
    let expected = &case["expected"];
    let id = case["id"].as_str().unwrap();
    let mut checker = Checker {
        case: id,
        route,
        found: Vec::new(),
    };

    for field in [
        "total_count",
        "null_count",
        "null_percentage",
        "unique_count",
        "data_type",
        "stats",
    ] {
        if let Some(wanted) = expected.get(field) {
            checker.compare(field, &column[field], wanted, How::Serialized);
        }
    }
    if let Some(wanted) = expected.get("duplicate_rows") {
        checker.compare(
            "duplicate_rows",
            &summary["quality"]["uniqueness"]["duplicate_rows"],
            wanted,
            How::Serialized,
        );
    }
    if case["kind"] != "numeric" {
        return checker.found;
    }

    let stats = &column["stats"];
    let field = |name: &str| stats.get(name).cloned().unwrap_or(Value::Null);
    if case["relative"] == true {
        // 4dp carries no information at this magnitude.
        for name in ["mean", "variance", "std_dev", "min", "max"] {
            checker.compare(name, &field(name), &expected[name], How::Relative);
        }
        return checker.found;
    }
    // Past 2^53 an exact integer can fall between two binary64 values; either
    // neighbour is a faithful float, so the bound is half an ulp.
    let wide = ["int64", "uint64"].contains(&case["dtype"].as_str().unwrap())
        && case["values"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .any(|v| rational(v).0.unsigned_abs() > 1 << 53);

    for name in [
        "variance",
        "std_dev",
        "skewness",
        "kurtosis",
        "coefficient_of_variation",
        "outlier_count",
    ] {
        checker.compare(name, &field(name), &expected[name], How::Serialized);
    }
    for name in ["min", "max"] {
        if wide {
            // An exact integer extreme needs an integer-preserving value;
            // comparing after a float conversion would erase the defect (#1038).
            let wanted = expected[name].as_str().unwrap();
            if exact_integer(&field(name)) != Some(rational(wanted).0) {
                checker.record(name, wanted, &field(name));
            }
        } else {
            checker.compare(name, &field(name), &expected[name], How::Serialized);
        }
    }
    let how = if wide { How::Faithful } else { How::Serialized };
    for name in ["mean", "median", "mode"] {
        checker.compare(name, &field(name), &expected[name], how);
    }
    match expected["quartiles"].as_object() {
        None => checker.compare("quartiles", &field("quartiles"), &Value::Null, how),
        Some(quartiles) => {
            for (name, wanted) in quartiles {
                let actual = &stats["quartiles"][name];
                checker.compare(&format!("quartiles.{name}"), actual, wanted, how);
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
    let cases = fixture()["cases"].as_array().unwrap().clone();
    let mut runs = Vec::new();
    let mut mismatches = Vec::new();
    for case in &cases {
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
    // A "*" entry claims every route, so each route it covers must still
    // disagree: a fix on one route narrows the entry instead of hiding there.
    let stale: Vec<String> = KNOWN
        .iter()
        .flat_map(|(case, route, field, issue)| {
            runs.iter()
                .filter(move |(c, r)| c == case && (*route == "*" || route == r))
                .filter(|(c, r)| {
                    !mismatches
                        .iter()
                        .any(|m| m.case == *c && m.route == *r && m.field == *field)
                })
                .map(move |(_, r)| format!("  {case} via {r}: {field} ({issue})"))
        })
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
    // Every case with a text spelling ran.
    let text_cases: BTreeSet<&str> = cases
        .iter()
        .filter(|case| ["csv", "json", "jsonl"].iter().any(|r| allowed(case, r)))
        .map(|case| case["id"].as_str().unwrap())
        .collect();
    let ran: BTreeSet<&str> = runs.iter().map(|(case, _)| case.as_str()).collect();
    assert_eq!(ran, text_cases);
}

/// The comparison helpers on inputs the current data cannot reach: an exact
/// JSON integer past 2^53 (what a fixed #1038 would report), a median a
/// million away from the truth, and values either side of a 4dp midpoint.
#[test]
fn the_comparison_helpers_are_exact() {
    let exact = json!(9_007_199_254_740_993_u64);
    assert_eq!(exact_integer(&exact), Some(9_007_199_254_740_993));
    assert!(faithful(&exact, "9007199254740993"));
    // Its nearest float is a faithful rendering too; a neighbour two ulps off is not.
    assert!(faithful(
        &json!(9_007_199_254_740_992.0),
        "9007199254740993"
    ));
    assert!(!faithful(
        &json!(9_007_199_254_740_996.0),
        "9007199254740993"
    ));

    let median = "1759700000123456794";
    assert!(faithful(&json!(1.759_700_000_123_456_8e18), median));
    assert!(!faithful(&json!(1.759_700_000_123_456_8e18 + 1e6), median));

    assert_eq!(
        rational("18446744073709551612.7500"),
        (184_467_440_737_095_516_127_500, 10_000)
    );
    assert_eq!(rational("-5/3"), (-5, 3));

    // 1.23495 as binary32 is 1.2349499464...: below the midpoint, so 1.2349
    // and never 1.235 (#1041), on both signs.
    let held = "1.2349499464035034179687500";
    assert!(serialized(&json!(1.2349), held, 4));
    assert!(!serialized(&json!(1.235), held, 4));
    assert!(serialized(
        &json!(-1.2349),
        "-1.2349499464035034179687500",
        4
    ));
    assert!(!serialized(
        &json!(-1.235),
        "-1.2349499464035034179687500",
        4
    ));
    // An exact tie rounds away from zero; a computed one may land either side.
    assert!(serialized(&json!(3.38), "27/8", 2));
    assert!(serialized(&json!(3.37), "27/8", 2));
    assert!(!serialized(&json!(3.36), "27/8", 2));
    // Off the 4dp grid is wrong even when close.
    assert!(!serialized(&json!(1.23495), held, 4));
}
