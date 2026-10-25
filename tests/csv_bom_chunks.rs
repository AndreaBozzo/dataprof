//! A UTF-8 BOM never changes what a chunked CSV read finds.
//!
//! The memory-mapped reader extends a first chunk that ends inside the leading
//! blank lines, but its scan for them stopped at the BOM: a chunk of BOM and
//! line end became an empty header and the real header was read as data, so
//! `chunk_size=1` reported no columns and a row too many (#1101). Its record
//! boundary scan also counted the BOM as field content, so a quote right after
//! it did not open a quoted header. Every chunk size must read what the
//! unchunked read does.
//!
//! `python/tests/test_csv_bom_chunks.py` is the Python twin.

use std::io::Write;

use dataprof::{ChunkSize, EngineType, ProfileReport, Profiler};
use serde_json::{Value, json};

/// One file, and what every chunk size must read from it.
struct File {
    label: &'static str,
    bytes: &'static [u8],
    /// Expected columns as (name, null count).
    columns: &'static [(&'static str, usize)],
    rows: usize,
}

const FILES: [File; 4] = [
    File {
        label: "BOM, blank line",
        bytes: b"\xEF\xBB\xBF\na;b\n1;\n",
        columns: &[("a", 0), ("b", 1)],
        rows: 1,
    },
    File {
        label: "BOM, blank lines",
        bytes: b"\xEF\xBB\xBF\n\r\n\na;b\n1;\n2;3\n",
        columns: &[("a", 0), ("b", 1)],
        rows: 2,
    },
    File {
        label: "BOM, quoted multi-line header",
        bytes: b"\xEF\xBB\xBF\"a\nb\";c\n1;\n2;3\n",
        columns: &[("a\nb", 0), ("c", 1)],
        rows: 2,
    },
    File {
        label: "BOM, no blank line",
        bytes: b"\xEF\xBB\xBFa;b\n1;\n",
        columns: &[("a", 0), ("b", 1)],
        rows: 1,
    },
];

fn shape(report: &ProfileReport) -> Value {
    let columns: Vec<Value> = report
        .column_profiles
        .iter()
        .map(|column| json!([column.name, column.null_count]))
        .collect();
    json!({ "columns": columns, "rows": report.execution.rows_processed })
}

/// Collects every wrong (file, engine, chunk size) before failing.
#[test]
fn every_chunk_size_reads_the_whole_header() {
    let mut wrong = Vec::new();
    for File {
        label,
        bytes,
        columns,
        rows,
    } in FILES
    {
        let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        let expected = json!({
            "columns": columns.iter().map(|(name, nulls)| json!([name, nulls])).collect::<Vec<_>>(),
            "rows": rows,
        });
        for engine in [EngineType::Incremental, EngineType::Auto] {
            for size in 1..=bytes.len() + 1 {
                match Profiler::new()
                    .engine(engine)
                    .chunk_size(ChunkSize::Fixed(size))
                    .analyze_file(file.path())
                {
                    Ok(report) if shape(&report) == expected => {}
                    Ok(report) => wrong.push(format!(
                        "[{label}] {engine:?} chunk {size}: {}",
                        shape(&report)
                    )),
                    Err(error) => {
                        wrong.push(format!("[{label}] {engine:?} chunk {size} failed: {error}"))
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
