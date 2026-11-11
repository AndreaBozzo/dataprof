//! `compare()` lists each column's type on both sides and each side's column
//! selection (#1031).
//!
//! The baseline gate reads both and reports a type change or a selection
//! difference as a diagnostic, but the delta carried null percentages only, so
//! a reader of `compare()` could not see either. Each column entry now carries
//! `data_type_a`/`data_type_b` (the inferred type, where the side read a value
//! that is not null) and `native_type_a`/`native_type_b` (the type the source
//! declared, #906), and `configuration.columns` lists the selections.
//!
//! `python/tests/test_compare_column_types.py` is the Python twin.

use std::io::Write;
#[cfg(feature = "parquet")]
use std::sync::Arc;

#[cfg(feature = "parquet")]
use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array};
#[cfg(feature = "parquet")]
use arrow::datatypes::{Field, Schema};
#[cfg(feature = "parquet")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "parquet")]
use dataprof::NativeType;
use dataprof::{DiagnosticCode, ProfileReport, Profiler, QualityPolicy, ReportDelta, ReportSide};
#[cfg(feature = "parquet")]
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use tempfile::NamedTempFile;

fn csv(contents: &str) -> NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn profile_csv(contents: &str, columns: Option<&[&str]>) -> ProfileReport {
    let file = csv(contents);
    let mut profiler = Profiler::new();
    if let Some(columns) = columns {
        profiler = profiler.columns(columns.iter().map(|name| (*name).to_string()).collect());
    }
    profiler.analyze_file(file.path()).unwrap()
}

/// Each report as a native side and as the flat summary it writes, so every
/// pairing of the two forms is compared.
fn routes(a: &ProfileReport, b: &ProfileReport) -> Vec<(&'static str, Value)> {
    let native = |report: &ProfileReport| ReportSide::from_report(report);
    let flat = |report: &ProfileReport| {
        ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap()
    };
    let delta = |a: ReportSide, b: ReportSide| {
        serde_json::to_value(ReportDelta::between_sides(&a, &b)).unwrap()
    };
    vec![
        ("report/report", delta(native(a), native(b))),
        ("summary/summary", delta(flat(a), flat(b))),
        ("report/summary", delta(native(a), flat(b))),
        ("summary/report", delta(flat(a), native(b))),
    ]
}

/// Every (route, column, key) whose value is not the expected one, so one
/// failing run shows which part of the change each wrong entry belongs to.
fn mismatches(
    routes: &[(&'static str, Value)],
    key_a: &str,
    key_b: &str,
    expected: &[(&str, Value, Value)],
) -> Vec<String> {
    let mut wrong = Vec::new();
    for (route, delta) in routes {
        for (column, a, b) in expected {
            let entry = &delta["columns"][column];
            for (key, want) in [(key_a, a), (key_b, b)] {
                if &entry[key] != want {
                    wrong.push(format!("{route}, {column}.{key}: {} != {want}", entry[key]));
                }
            }
        }
    }
    wrong
}

// `blank` is all null in the baseline, so it reads `string` there and
// establishes no type; `gone` and `new` exist on one side only.
const BASELINE: &str = "id,code,blank,gone\n1,10,,x\n2,20,,y\n";
const CURRENT: &str = "id,code,blank,new\n1,a1,3,z\n2,b2,4,w\n";

#[test]
fn each_side_carries_the_type_it_established() {
    let a = profile_csv(BASELINE, None);
    let b = profile_csv(CURRENT, None);
    let expected = [
        ("id", json!("integer"), json!("integer")),
        ("code", json!("integer"), json!("string")),
        ("blank", Value::Null, json!("integer")),
        ("gone", json!("string"), Value::Null),
        ("new", Value::Null, json!("string")),
    ];
    let wrong = mismatches(&routes(&a, &b), "data_type_a", "data_type_b", &expected);
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn two_different_types_are_the_change_the_gate_reports() {
    let a = profile_csv(BASELINE, None);
    let b = profile_csv(CURRENT, None);
    // Diagnostics are listed whatever the policy states; one rule is needed.
    let result = QualityPolicy::new()
        .max_null_percentage_increase("id", 0.0)
        .evaluate_against(&b, &ReportSide::from_report(&a))
        .unwrap();
    let mut retyped_by_gate: Vec<&str> = result
        .baseline
        .as_ref()
        .unwrap()
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.code == DiagnosticCode::ColumnTypeChanged)
        .map(|diagnostic| diagnostic.column.as_str())
        .collect();
    retyped_by_gate.sort();

    let delta = serde_json::to_value(ReportDelta::between(&a, &b)).unwrap();
    let mut retyped_in_delta: Vec<&str> = delta["columns"]
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, entry)| {
            !entry["data_type_a"].is_null()
                && !entry["data_type_b"].is_null()
                && entry["data_type_a"] != entry["data_type_b"]
        })
        .map(|(name, _)| name.as_str())
        .collect();
    retyped_in_delta.sort();
    assert_eq!(retyped_in_delta, retyped_by_gate);
    assert_eq!(retyped_in_delta, ["code"]);
}

#[test]
fn a_summary_carries_a_type_only_with_its_counts_and_a_known_name() {
    let summary = |columns: Value| {
        json!({"source": "s.csv", "execution": {}, "columns": columns}).to_string()
    };
    let a = ReportSide::from_summary_json(&summary(json!([
        // Counts not recorded: whether a value was read is unknown.
        {"name": "uncounted", "null_percentage": 0.0, "data_type": "integer"},
        // No rows read at all.
        {"name": "empty", "null_percentage": null, "data_type": "string",
         "total_count": 0, "null_count": 0},
        // A type name from a later release.
        {"name": "later", "null_percentage": 0.0, "data_type": "duration",
         "total_count": 2, "null_count": 0},
        {"name": "counted", "null_percentage": 50.0, "data_type": "float",
         "total_count": 2, "null_count": 1},
    ])))
    .unwrap();
    let delta = serde_json::to_value(ReportDelta::between_sides(&a, &a)).unwrap();
    let mut wrong = Vec::new();
    for (column, want) in [
        ("uncounted", Value::Null),
        ("empty", Value::Null),
        ("later", Value::Null),
        ("counted", json!("float")),
    ] {
        for key in ["data_type_a", "data_type_b"] {
            let got = &delta["columns"][column][key];
            if got != &want {
                wrong.push(format!("{column}.{key}: {got} != {want}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "parquet")]
fn parquet(amount: ArrayRef) -> NamedTempFile {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, true),
            Field::new("amount", amount.data_type().clone(), true),
        ])),
        vec![Arc::new(Int64Array::from(vec![1, 2])), amount],
    )
    .unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

#[cfg(feature = "parquet")]
#[test]
fn each_side_carries_the_type_its_source_declared() {
    // The same profiled type, `float`, from two declarations; the baseline's
    // decimals are all null, which establishes no inferred type but keeps
    // the declaration. The CSV side declares nothing.
    let decimal = parquet(Arc::new(
        Decimal128Array::from(vec![None::<i128>, None])
            .with_precision_and_scale(12, 2)
            .unwrap(),
    ));
    let double = parquet(Arc::new(Float64Array::from(vec![12.5, 3.0])));
    let a = Profiler::new().analyze_file(decimal.path()).unwrap();
    let b = Profiler::new().analyze_file(double.path()).unwrap();
    let text = profile_csv("id,amount\n1,12.5\n2,3.0\n", None);

    let arrow =
        |name: &str| serde_json::to_value(NativeType::new(NativeType::ARROW, name)).unwrap();
    let mut wrong = mismatches(
        &routes(&a, &b),
        "native_type_a",
        "native_type_b",
        &[
            ("id", arrow("int64"), arrow("int64")),
            ("amount", arrow("decimal128(12, 2)"), arrow("double")),
        ],
    );
    wrong.extend(mismatches(
        &routes(&a, &b),
        "data_type_a",
        "data_type_b",
        &[("amount", Value::Null, json!("float"))],
    ));
    wrong.extend(mismatches(
        &routes(&b, &text),
        "native_type_a",
        "native_type_b",
        &[("amount", arrow("double"), Value::Null)],
    ));
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn configuration_lists_each_sides_column_selection() {
    const DATA: &str = "id,code,note\n1,a,x\n2,b,y\n";
    let all = profile_csv(DATA, None);
    let id_code = profile_csv(DATA, Some(&["id", "code"]));
    let code_id = profile_csv(DATA, Some(&["code", "id"]));
    let id_only = profile_csv(DATA, Some(&["id"]));
    let every = profile_csv(DATA, Some(&["id", "code", "note"]));

    let listed = |names: &[&str]| json!({"selection": "listed", "names": names});
    let all_selection = json!({"selection": "all"});
    let cases = [
        (
            "all/all",
            &all,
            &all,
            &all_selection,
            &all_selection,
            json!(true),
        ),
        // The order a list gives orders the columns and moves no score.
        (
            "listed/reordered",
            &id_code,
            &code_id,
            &listed(&["id", "code"]),
            &listed(&["code", "id"]),
            json!(true),
        ),
        (
            "listed/fewer",
            &id_code,
            &id_only,
            &listed(&["id", "code"]),
            &listed(&["id"]),
            json!(false),
        ),
        // Only `all` follows the source's columns.
        (
            "all/every-listed",
            &all,
            &every,
            &all_selection,
            &listed(&["id", "code", "note"]),
            json!(false),
        ),
    ];
    let mut wrong = Vec::new();
    for (case, a, b, want_a, want_b, want_same) in cases {
        for (route, delta) in routes(a, b) {
            let got = &delta["configuration"]["columns"];
            let want = json!({"a": want_a, "b": want_b, "same": want_same});
            if got != &want {
                wrong.push(format!("{case}, {route}: {got} != {want}"));
            }
        }
    }

    // A summary written before 0.13 recorded no selection.
    let mut legacy: Value = serde_json::from_str(&all.summary_json().unwrap()).unwrap();
    legacy.as_object_mut().unwrap().remove("analysis_settings");
    let legacy = ReportSide::from_summary_json(&legacy.to_string()).unwrap();
    let delta = serde_json::to_value(ReportDelta::between_sides(
        &legacy,
        &ReportSide::from_report(&all),
    ))
    .unwrap();
    let want = json!({"a": null, "b": all_selection, "same": null});
    if delta["configuration"]["columns"] != want {
        wrong.push(format!(
            "pre-0.13 summary: {} != {want}",
            delta["configuration"]["columns"]
        ));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
