//! Delimiter detection reads each file the way its header and records say.
//!
//! Every reader holds a record to the header's width. Detection that chose a
//! delimiter the header did not split, scored a CR-only file as one header
//! record, or counted a trailing empty field against its delimiter, made the
//! readers cut or pad the rows: composite keys `123|456` and `123|789` became
//! two `123` with a fabricated duplicate (#1092, #1097), and a semicolon export
//! with trailing `;` became a comma file with an invented null column (#1096).
//! A candidate that needs trailing fields cut must not tie one that reads every
//! record as it stands, and a UTF-8 BOM is not a header record (#1099 review).
//!
//! A header longer than the 4 KB detection sample was scored as its first
//! 4 KB: no candidate split the fragment, comma won, and the file was read as
//! one column with no error (#1098). Detection now reads on until the header
//! record ends, up to 1 MiB, and refuses a longer one.
//!
//! `python/tests/test_csv_delimiter_header.py` is the Python twin, and adds the
//! bytes and async routes.

use std::io::Write;

use dataprof::{
    CsvParserConfig, DataProfilerError, EngineType, ProfileReport, Profiler, analyze_csv_file,
    analyze_structure, quick_row_count,
};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

/// One file, and what every route must read from it.
struct Case {
    label: &'static str,
    bytes: &'static [u8],
    /// Expected columns as (name, null count).
    columns: &'static [(&'static str, usize)],
    duplicate_rows: usize,
}

const CASES: [Case; 8] = [
    Case {
        label: "header with a quote inside a field",
        bytes: b"id\"x;value\n1;2\n3;4\n",
        columns: &[("id\"x", 0), ("value", 0)],
        duplicate_rows: 0,
    },
    Case {
        label: "composite keys, LF",
        bytes: b"key\n123|456\n123|789\n",
        columns: &[("key", 0)],
        duplicate_rows: 0,
    },
    Case {
        label: "composite keys, CR",
        bytes: b"key\r123|456\r123|789\r",
        columns: &[("key", 0)],
        duplicate_rows: 0,
    },
    Case {
        label: "semicolon rows with a trailing empty field",
        bytes: b"id;amount,EUR\n1;10,50;\n2;20;\n",
        columns: &[("id", 0), ("amount,EUR", 0)],
        duplicate_rows: 0,
    },
    Case {
        label: "semicolon, CR",
        bytes: b"id;value\r1;10\r2;\r",
        columns: &[("id", 0), ("value", 1)],
        duplicate_rows: 0,
    },
    Case {
        label: "semicolon rows that comma reads only after cutting",
        bytes: b"id,part;value\nx;y,z,\nu;v,w,\n",
        columns: &[("id,part", 0), ("value", 0)],
        duplicate_rows: 0,
    },
    Case {
        label: "BOM, then a blank line",
        bytes: b"\xEF\xBB\xBF\nid;value\n1;2\n3;\n",
        columns: &[("id", 0), ("value", 1)],
        duplicate_rows: 0,
    },
    Case {
        label: "semicolon rows with a quoted empty trailing field",
        bytes: b"id;amount,EUR\n1;10,50;\"\"\n2;20;\"\"\n",
        columns: &[("id", 0), ("amount,EUR", 0)],
        duplicate_rows: 0,
    },
];

fn engine(path: &std::path::Path, engine: EngineType) -> Result<ProfileReport, String> {
    Profiler::new()
        .engine(engine)
        .analyze_file(path)
        .map_err(|error| error.to_string())
}

fn shape(report: &ProfileReport) -> Value {
    let serialized = serde_json::to_value(report).expect("serialize report");
    let columns: Vec<Value> = serialized["column_profiles"]
        .as_array()
        .expect("column_profiles")
        .iter()
        .map(|column| json!([column["name"], column["null_count"]]))
        .collect();
    json!({
        "columns": columns,
        "duplicate_rows": serialized["quality"]["metrics"]["uniqueness"]["duplicate_rows"],
    })
}

/// Collects every wrong (case, route) before failing, so one run shows each
/// route and shape the fix reaches.
#[test]
fn detected_delimiter_keeps_every_value_on_every_route() {
    let mut wrong = Vec::new();
    for Case {
        label,
        bytes,
        columns,
        duplicate_rows,
    } in CASES
    {
        let mut file = NamedTempFile::with_suffix(".csv").unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        let path = file.path();

        let expected = json!({
            "columns": columns.iter().map(|(name, nulls)| json!([name, nulls])).collect::<Vec<_>>(),
            "duplicate_rows": duplicate_rows,
        });
        let routes = [
            (
                "standard",
                analyze_csv_file(path, &CsvParserConfig::default()).map_err(|e| e.to_string()),
            ),
            ("auto", engine(path, EngineType::Auto)),
            ("incremental", engine(path, EngineType::Incremental)),
            ("columnar", engine(path, EngineType::Columnar)),
        ];
        for (route, outcome) in routes {
            match outcome {
                Ok(report) if shape(&report) == expected => {}
                Ok(report) => wrong.push(format!("[{label}] {route}: {}", shape(&report))),
                Err(error) => wrong.push(format!("[{label}] {route} failed: {error}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

fn temp_csv(bytes: &[u8]) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();
    file
}

fn file_routes(path: &std::path::Path) -> [(&'static str, Result<ProfileReport, String>); 4] {
    [
        (
            "standard",
            analyze_csv_file(path, &CsvParserConfig::default()).map_err(|e| e.to_string()),
        ),
        ("auto", engine(path, EngineType::Auto)),
        ("incremental", engine(path, EngineType::Incremental)),
        ("columnar", engine(path, EngineType::Columnar)),
    ]
}

/// Collects every wrong (case, route) before failing.
#[test]
fn header_longer_than_the_sample_keeps_its_columns() {
    let long_name = "x".repeat(4094);
    let longer_name = "a".repeat(10_000);
    let cases = [
        (
            // The issue's file: the first `;` is at byte 4096.
            "quoted header ending past 4 KB",
            format!("\"{long_name}\";value\n1;10\n2;20\n").into_bytes(),
            json!({"columns": [[long_name, 0], ["value", 0]], "duplicate_rows": 0}),
        ),
        (
            "BOM, blank line, header past 8 KB",
            format!("\u{feff}\n{longer_name};value\n1;2\n3;\n").into_bytes(),
            json!({"columns": [[longer_name, 0], ["value", 1]], "duplicate_rows": 0}),
        ),
    ];
    let mut wrong = Vec::new();
    for (label, bytes, expected) in cases {
        let file = temp_csv(&bytes);
        for (route, outcome) in file_routes(file.path()) {
            match outcome {
                Ok(report) if shape(&report) == expected => {}
                Ok(report) => {
                    let columns = shape(&report)["columns"].as_array().map(Vec::len);
                    wrong.push(format!("[{label}] {route}: {columns:?} columns"));
                }
                Err(error) => wrong.push(format!("[{label}] {route} failed: {error}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A header past the 1 MiB bound is refused on every route as a CSV structure
/// error naming the setting that reads it, instead of being guessed as comma.
#[test]
fn header_past_the_bound_is_refused_not_guessed() {
    let bytes = format!("{};value\n1;2\n", "h".repeat((1 << 20) + 10)).into_bytes();
    let file = temp_csv(&bytes);
    let path = file.path();
    let refused = |engine: EngineType| Profiler::new().engine(engine).analyze_file(path).err();

    let outcomes = [
        (
            "standard",
            analyze_csv_file(path, &CsvParserConfig::default()).err(),
        ),
        ("auto", refused(EngineType::Auto)),
        ("incremental", refused(EngineType::Incremental)),
        ("columnar", refused(EngineType::Columnar)),
        ("quick_row_count", quick_row_count(path).err()),
        ("analyze_structure", analyze_structure(path, Some(10)).err()),
    ];
    let wrong: Vec<String> = outcomes
        .into_iter()
        .filter_map(|(route, error)| match error {
            Some(error @ DataProfilerError::CsvParsingError { .. })
                if error.to_string().contains("csv_delimiter") =>
            {
                None
            }
            Some(error) => Some(format!("{route}: unexpected error {error:?}")),
            None => Some(format!("{route}: read without an error")),
        })
        .collect();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));

    let explicit = Profiler::new()
        .csv_delimiter(b';')
        .analyze_file(path)
        .expect("an explicit delimiter reads it");
    assert_eq!(explicit.column_profiles.len(), 2);
}
