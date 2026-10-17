//! Binary files bound for a text reader fail with what they are (#893).
//!
//! A gzip file named `data.csv.gz`, or a Parquet file without a `.parquet`
//! extension, used to reach the CSV engines and fail with advice about
//! delimiters and column counts. Every route that reads a file as text now
//! checks its signature bytes first and returns a `binary_input` error naming
//! the format and the way forward.
//!
//! `python/tests/test_binary_input.py` is the Python twin.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType as ArrowType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dataprof::{
    CsvParserConfig, DataProfilerError, EngineType, FileFormat, JsonParserConfig, Profiler,
    analyze_csv_file, analyze_json_file,
};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

/// Signature bytes followed by bytes that are not text either.
fn signed(signature: &[u8]) -> Vec<u8> {
    let mut bytes = signature.to_vec();
    bytes.extend_from_slice(&[0x00, 0xff, 0x8b, 0x02, 0x9c, 0xe3, 0x01, 0x00]);
    bytes
}

fn parquet_bytes() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", ArrowType::Int64, false),
        Field::new("city", ArrowType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![Some("Rome"), None, Some("Milan")])),
        ],
    )
    .unwrap();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buffer
}

/// (file name, bytes, the description the error must carry)
fn binary_files(dir: &TempDir) -> Vec<(PathBuf, &'static str)> {
    let gzip = signed(&[0x1f, 0x8b, 0x08, 0x00]);
    let zstd = signed(&[0x28, 0xb5, 0x2f, 0xfd]);
    let zip = signed(b"PK\x03\x04");
    let parquet = parquet_bytes();
    let files: [(&str, &[u8], &str); 11] = [
        ("data.csv.gz", &gzip, "gzip-compressed"),
        ("gzip.csv", &gzip, "gzip-compressed"),
        ("gzip.json", &gzip, "gzip-compressed"),
        ("gzip.jsonl", &gzip, "gzip-compressed"),
        ("data.csv.zst", &zstd, "zstd-compressed"),
        ("zstd.csv", &zstd, "zstd-compressed"),
        ("data.zip", &zip, "a zip archive"),
        ("zip.csv", &zip, "a zip archive"),
        ("parquet.csv", &parquet, "a Parquet file"),
        ("parquet_no_extension", &parquet, "a Parquet file"),
        ("parquet.dat", &parquet, "a Parquet file"),
    ];
    files
        .into_iter()
        .map(|(name, bytes, detected)| {
            let path = dir.path().join(name);
            std::fs::write(&path, bytes).unwrap();
            (path, detected)
        })
        .collect()
}

type Route = (&'static str, fn(&Path) -> Result<(), DataProfilerError>);

fn routes() -> Vec<Route> {
    vec![
        ("analyze_file auto", |p| {
            Profiler::new()
                .engine(EngineType::Auto)
                .analyze_file(p)
                .map(drop)
        }),
        ("analyze_file incremental", |p| {
            Profiler::new()
                .engine(EngineType::Incremental)
                .analyze_file(p)
                .map(drop)
        }),
        ("analyze_file columnar", |p| {
            Profiler::new()
                .engine(EngineType::Columnar)
                .analyze_file(p)
                .map(drop)
        }),
        ("infer_schema", |p| {
            Profiler::new().infer_schema(p).map(drop)
        }),
        ("quick_row_count", |p| {
            Profiler::new().quick_row_count(p).map(drop)
        }),
        ("analyze_structure", |p| {
            Profiler::new().analyze_structure(p, None).map(drop)
        }),
        ("analyze_csv_file", |p| {
            analyze_csv_file(p, &CsvParserConfig::default()).map(drop)
        }),
        ("analyze_json_file", |p| {
            analyze_json_file(p, &JsonParserConfig::default()).map(drop)
        }),
    ]
}

/// What is wrong with one route's answer for a binary file, if anything.
fn misanswer(result: Result<(), DataProfilerError>, detected: &str) -> Option<String> {
    let err = match result {
        Ok(()) => return Some("profiled a binary file as text".to_string()),
        Err(err) => err,
    };
    let rendered = err.to_string();
    if err.category() != "binary_input" {
        return Some(format!("category {}: {rendered}", err.category()));
    }
    if !rendered.contains(detected) {
        return Some(format!("does not say {detected:?}: {rendered}"));
    }
    if err.suggestion().is_none() {
        return Some("no structured suggestion".to_string());
    }
    None
}

#[test]
fn binary_files_fail_with_what_they_are_on_every_text_route() {
    let dir = TempDir::new().unwrap();
    let mut wrong = Vec::new();
    for (path, detected) in binary_files(&dir) {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        for (route, run) in routes() {
            if let Some(problem) = misanswer(run(&path), detected) {
                wrong.push(format!("{route} / {name}: {problem}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[cfg(feature = "async-streaming")]
#[test]
fn binary_files_fail_with_what_they_are_on_the_async_file_route() {
    let dir = TempDir::new().unwrap();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut wrong = Vec::new();
    for (path, detected) in binary_files(&dir) {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        let result = runtime
            .block_on(Profiler::new().profile_file(&path))
            .map(drop);
        if let Some(problem) = misanswer(result, detected) {
            wrong.push(format!("profile_file / {name}: {problem}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// The Parquet suggestion is the way forward: following it profiles the file.
#[test]
fn a_misnamed_parquet_file_profiles_once_the_format_is_selected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("export.csv");
    std::fs::write(&path, parquet_bytes()).unwrap();

    let err = Profiler::new().analyze_file(&path).unwrap_err();
    let suggestion = err.suggestion().unwrap_or_default();
    assert!(
        suggestion.contains(".format(FileFormat::Parquet)"),
        "{suggestion}"
    );

    let report = Profiler::new()
        .format(FileFormat::Parquet)
        .analyze_file(&path)
        .expect("selecting Parquet reads the file");
    assert_eq!(report.execution.rows_processed, 3);
}

/// Over-correction guard: text that merely starts like a signature, a real
/// Parquet file, and an empty file still go where they went before.
#[test]
fn text_and_correctly_named_parquet_are_not_refused() {
    let dir = TempDir::new().unwrap();
    let write = |name: &str, bytes: &[u8]| {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    };
    let text_files = [
        // A header that starts with the Parquet marker.
        write("par1.csv", b"PAR1,PAR2\n1,2\n3,4\n"),
        // Text that starts with the marker but does not end with it.
        write("short.csv", b"PAR1\n"),
        // One marker that is both the first and the last four bytes.
        write("marker_only.csv", b"PAR1"),
        write("empty.csv", b""),
        write("pk.csv", b"PK,name\n1,a\n"),
        write("events.jsonl", b"{\"a\":1}\n{\"a\":2}\n"),
    ];
    let mut wrong = Vec::new();
    for path in &text_files {
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        for (route, run) in routes() {
            if let Err(err) = run(path)
                && err.category() == "binary_input"
            {
                wrong.push(format!("{route} / {name}: {err}"));
            }
        }
    }
    let parquet = write("real.parquet", &parquet_bytes());
    if let Err(err) = Profiler::new().analyze_file(&parquet) {
        wrong.push(format!("analyze_file / real.parquet: {err}"));
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
