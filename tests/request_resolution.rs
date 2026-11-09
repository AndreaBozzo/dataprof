//! Every entry point and every route resolves a request the same way (#717).
//!
//! The analysis options used to be forwarded field by field at each layer:
//! the facade copied them into the adaptive selector, which copied them into
//! whichever engine it picked, and each engine rebuilt the selection from its
//! own fields. A new option had to be written into every copy, and one missed
//! copy is a route that silently ignores it (#494, #536). The selection now
//! travels whole, built once from `ProfilerConfig`.
//!
//! This test sets every analysis option at once and profiles the same records
//! through each entry point, engine choice and format, including the adaptive
//! selector's Arrow pick on a wide numeric CSV. Each route must show the effect
//! of every option. `python/tests/test_request_resolution.py` is the Python
//! twin.

use std::io::Write;
use std::path::Path;

use dataprof::{
    ColumnStats, EngineType, Locale, MetricPack, NaiveDate, ProfileReport, Profiler,
    SemanticHintKind, StopCondition,
};
use tempfile::NamedTempFile;

/// Postal codes make the locale observable: without one, both `CAP (IT)` and
/// `ZIP Code (US)` match a five-digit string.
const CAPS: [&str; 5] = ["20121", "00184", "10121", "80132", "50122"];
const CREATED: [&str; 5] = [
    "2026-01-05",
    "2026-02-10",
    "2026-03-15",
    "2026-04-20",
    "2026-05-25",
];
/// Enough numeric columns past `n0` for the adaptive selector to pick Arrow:
/// at least 20 columns, most of them numeric.
const WIDE_EXTRA: usize = 19;
const SELECTED: [&str; 3] = ["cap", "created", "n0"];
const REFERENCE: &str = "2026-06-30";

fn header(wide: bool) -> Vec<String> {
    let mut names = vec![
        "id".to_string(),
        "cap".into(),
        "created".into(),
        "n0".into(),
    ];
    if wide {
        names.extend((1..=WIDE_EXTRA).map(|i| format!("n{i}")));
    }
    names
}

fn csv_text(wide: bool) -> String {
    let names = header(wide);
    let mut text = names.join(",") + "\n";
    for (row, (cap, created)) in CAPS.iter().zip(CREATED).enumerate() {
        let mut fields = vec![
            (row + 1).to_string(),
            cap.to_string(),
            created.to_string(),
            (row * 7 + 3).to_string(),
        ];
        fields.extend((1..=WIDE_EXTRA.min(names.len() - 4)).map(|i| (row * i).to_string()));
        text += &(fields.join(",") + "\n");
    }
    text
}

fn json_records() -> Vec<String> {
    CAPS.iter()
        .zip(CREATED)
        .enumerate()
        .map(|(row, (cap, created))| {
            format!(
                r#"{{"id":{},"cap":"{cap}","created":"{created}","n0":{}}}"#,
                row + 1,
                row * 7 + 3
            )
        })
        .collect()
}

fn json_text() -> String {
    format!("[{}]", json_records().join(","))
}

fn jsonl_text() -> String {
    json_records().join("\n") + "\n"
}

#[cfg(feature = "parquet")]
fn parquet_bytes() -> Vec<u8> {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("cap", DataType::Utf8, false),
        Field::new("created", DataType::Utf8, false),
        Field::new("n0", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((1..=5).collect::<Vec<i64>>())),
            Arc::new(StringArray::from(CAPS.to_vec())),
            Arc::new(StringArray::from(CREATED.to_vec())),
            Arc::new(Int64Array::from(
                (0..5).map(|row| row * 7 + 3).collect::<Vec<i64>>(),
            )),
        ],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    bytes
}

/// Each fixture as a labelled file with the extension its format detects by.
fn fixtures() -> Vec<(&'static str, NamedTempFile)> {
    let mut sources: Vec<(&'static str, &'static str, Vec<u8>)> = vec![
        ("csv", ".csv", csv_text(false).into_bytes()),
        ("csv-wide", ".csv", csv_text(true).into_bytes()),
        ("json", ".json", json_text().into_bytes()),
        ("jsonl", ".jsonl", jsonl_text().into_bytes()),
    ];
    #[cfg(feature = "parquet")]
    sources.push(("parquet", ".parquet", parquet_bytes()));
    sources
        .into_iter()
        .map(|(label, suffix, bytes)| {
            let mut file = NamedTempFile::with_suffix(suffix).unwrap();
            file.write_all(&bytes).unwrap();
            file.flush().unwrap();
            (label, file)
        })
        .collect()
}

/// A profiler with every analysis option set to something other than its
/// default, each one observable in the report.
fn configured() -> Profiler {
    Profiler::new()
        .columns(SELECTED.iter().map(|name| name.to_string()).collect())
        .metric_packs(vec![
            MetricPack::Schema,
            MetricPack::Patterns,
            MetricPack::Quality,
        ])
        .quality_dimensions(vec![
            dataprof::QualityDimension::Consistency,
            dataprof::QualityDimension::Timeliness,
        ])
        .locale(Locale::It)
        .reference_date(NaiveDate::parse_from_str(REFERENCE, "%Y-%m-%d").unwrap())
        .temporal_columns(vec!["created".into()])
        .identifier_columns(vec!["n0".into()])
}

/// Every option the report does not reflect, for one route.
fn missed_options(report: &ProfileReport) -> Vec<String> {
    let mut missed = Vec::new();

    let names: Vec<&str> = report
        .column_profiles
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    if names != SELECTED {
        missed.push(format!("columns: reported {names:?}"));
    }

    let with_stats: Vec<&str> = report
        .column_profiles
        .iter()
        .filter(|c| !matches!(c.stats, ColumnStats::None))
        .map(|c| c.name.as_str())
        .collect();
    if !with_stats.is_empty() {
        missed.push(format!("metric_packs: statistics on {with_stats:?}"));
    }

    let caps: Vec<String> = report
        .column_profiles
        .iter()
        .find(|c| c.name == "cap")
        .and_then(|c| c.patterns.as_ref())
        .map(|patterns| patterns.iter().map(|p| p.name.clone()).collect())
        .unwrap_or_default();
    if !caps.iter().any(|p| p.contains("CAP (IT)")) || caps.iter().any(|p| p.contains("ZIP")) {
        missed.push(format!("locale: cap patterns {caps:?}"));
    }

    let value = serde_json::to_value(report).unwrap();
    let quality = &value["quality"]["metrics"];
    for (dimension, requested) in [
        ("consistency", true),
        ("timeliness", true),
        ("accuracy", false),
    ] {
        if quality[dimension].is_null() == requested {
            missed.push(format!(
                "quality_dimensions: {dimension} {}",
                if requested { "missing" } else { "assessed" }
            ));
        }
    }
    let clock = &quality["timeliness"]["clock"]["reference_date"];
    if clock != REFERENCE {
        missed.push(format!("reference_date: clock {clock}"));
    }

    let mut bindings: Vec<(String, SemanticHintKind)> = report
        .semantic_hint_bindings
        .iter()
        .map(|b| (b.column.clone(), b.kind))
        .collect();
    bindings.sort_by(|a, b| a.0.cmp(&b.0));
    let expected = vec![
        ("created".to_string(), SemanticHintKind::Temporal),
        ("n0".to_string(), SemanticHintKind::Identifier),
    ];
    if bindings != expected {
        missed.push(format!("semantic hints: bindings {bindings:?}"));
    }
    missed
}

fn check(wrong: &mut Vec<String>, route: &str, result: Result<ProfileReport, impl ToString>) {
    match result {
        Ok(report) => {
            for missed in missed_options(&report) {
                wrong.push(format!("{route}: {missed}"));
            }
        }
        Err(error) => wrong.push(format!("{route}: failed: {}", error.to_string())),
    }
}

#[test]
fn every_sync_file_route_honours_every_analysis_option() {
    let mut wrong = Vec::new();
    for (label, file) in fixtures() {
        for (engine_label, engine) in [
            ("auto", EngineType::Auto),
            ("incremental", EngineType::Incremental),
            ("columnar", EngineType::Columnar),
        ] {
            let route = format!("analyze_file[{engine_label}] {label}");
            check(
                &mut wrong,
                &route,
                configured().engine(engine).analyze_file(file.path()),
            );
        }
        // A row cap sends an Auto CSV request to the incremental engine
        // directly, not through the selector.
        check(
            &mut wrong,
            &format!("analyze_file[auto, max_rows] {label}"),
            configured()
                .stop_when(StopCondition::MaxRows(1_000))
                .analyze_file(file.path()),
        );
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The wide CSV exercises the selector's Arrow pick only if it is actually
/// picked; without this the test above could pass on the incremental engine
/// alone.
#[cfg(feature = "arrow")]
#[test]
fn the_wide_csv_reaches_the_arrow_engine_through_the_selector() {
    let wide = fixtures()
        .into_iter()
        .find(|(label, _)| *label == "csv-wide")
        .unwrap()
        .1;
    let narrow = fixtures()
        .into_iter()
        .find(|(label, _)| *label == "csv")
        .unwrap()
        .1;
    let engine = |path: &Path| {
        configured()
            .analyze_file(path)
            .unwrap()
            .execution
            .engine
            .unwrap_or_default()
    };
    assert_eq!(engine(wide.path()), "columnar");
    assert_eq!(engine(narrow.path()), "incremental");
}

#[cfg(feature = "async-streaming")]
#[test]
fn every_async_route_honours_every_analysis_option() {
    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut wrong = Vec::new();
    for (label, file) in fixtures() {
        check(
            &mut wrong,
            &format!("profile_file {label}"),
            runtime.block_on(configured().profile_file(file.path())),
        );
        let format = match label {
            "json" => FileFormat::Json,
            "jsonl" => FileFormat::Jsonl,
            "parquet" => FileFormat::Parquet,
            _ => FileFormat::Csv,
        };
        let bytes = std::fs::read(file.path()).unwrap();
        let source = BytesSource::new(bytes.into(), AsyncSourceInfo::new(label, format));
        check(
            &mut wrong,
            &format!("profile_stream {label}"),
            runtime.block_on(configured().profile_stream(source)),
        );
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
