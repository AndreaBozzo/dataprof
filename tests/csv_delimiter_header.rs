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
//! `python/tests/test_csv_delimiter_header.py` is the Python twin, and adds the
//! bytes and async routes.

use std::io::Write;

use dataprof::{CsvParserConfig, EngineType, ProfileReport, Profiler, analyze_csv_file};
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

const CASES: [Case; 7] = [
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

#[test]
fn incomplete_header_sample_fails_instead_of_inventing_a_schema() {
    let name = "x".repeat(4094);
    let data = format!("\"{name}\";value\n1;10\n2;20\n");
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(data.as_bytes()).unwrap();

    let error = analyze_csv_file(file.path(), &CsvParserConfig::default()).unwrap_err();
    assert!(error.to_string().contains("CSV header"));
    let explicit = CsvParserConfig::default().with_delimiter(b';');
    let report = analyze_csv_file(file.path(), &explicit).unwrap();
    assert_eq!(report.column_profiles.len(), 2);
    assert_eq!(report.column_profiles[1].name, "value");

    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let profiler = Profiler::new().engine(engine);
        let error = profiler.analyze_file(file.path()).unwrap_err();
        assert!(
            error.to_string().contains("CSV header"),
            "{engine:?}: {error}"
        );
        let report = profiler
            .csv_delimiter(b';')
            .analyze_file(file.path())
            .unwrap();
        assert_eq!(report.column_profiles.len(), 2, "{engine:?}");
        assert_eq!(report.column_profiles[1].name, "value");
    }
}
