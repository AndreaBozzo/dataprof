//! Strict CSV parsing refuses a record whose field count differs from the
//! header's wherever a chunk boundary falls (#1202). The incremental engine
//! reads a file in chunks, and the `csv` crate's strict mode compares a record
//! only with the previous record of the same chunk, so a ragged record that
//! started a chunk was profiled. Flexible parsing still recovers and counts
//! every ragged row on the same chunk sizes.
//!
//! `python/tests/test_strict_csv_chunk_boundary.py` is the Python twin.

use std::io::Write;

use dataprof::{ChunkSize, EngineType, Profiler};

/// `(case, source, ragged rows)`: each source holds ragged records.
fn sources() -> Vec<(&'static str, String, usize)> {
    let mut one_width_run = String::from("name,age,city\n");
    for index in 0..40 {
        one_width_run.push_str(&format!("person{index},{index},Rome\n"));
    }
    // A run of records that all share one wrong width: a chunk that starts at
    // the first of them holds nothing the previous-record rule can refuse.
    for index in 0..40 {
        one_width_run.push_str(&format!("person{index},{index}\n"));
    }
    vec![
        (
            "short record",
            "name,age,city\nAlice,25,Rome\nBob,30\nCarl,40,Milan\n".to_string(),
            1,
        ),
        (
            "long record",
            "name,age,city\nAlice,25,Rome\nBob,30,Milan,extra\nCarl,40,Milan\n".to_string(),
            1,
        ),
        (
            "ragged last record",
            "name,age,city\nAlice,25,Rome\nBob,30,Milan\nCarl,40\n".to_string(),
            1,
        ),
        ("run of one wrong width", one_width_run, 40),
    ]
}

/// Incremental chunk sizes: one-record chunks, the issue's 20 bytes, and the
/// adaptive default, which reads these small files in one chunk.
const CHUNK_SIZES: [(&str, ChunkSize); 3] = [
    ("one-record chunks", ChunkSize::Fixed(1)),
    ("20-byte chunks", ChunkSize::Fixed(20)),
    ("adaptive chunks", ChunkSize::Adaptive),
];

fn write_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    write!(file, "{contents}").unwrap();
    file.flush().unwrap();
    file
}

/// Every strict file route, the incremental engine once per chunk size.
fn strict_routes() -> Vec<(String, Profiler)> {
    let mut routes: Vec<(String, Profiler)> = CHUNK_SIZES
        .into_iter()
        .map(|(label, chunk_size)| {
            (
                format!("incremental, {label}"),
                Profiler::new()
                    .csv_flexible(false)
                    .engine(EngineType::Incremental)
                    .chunk_size(chunk_size),
            )
        })
        .collect();
    routes.push((
        "auto".to_string(),
        Profiler::new().csv_flexible(false).engine(EngineType::Auto),
    ));
    routes.push((
        "columnar".to_string(),
        Profiler::new()
            .csv_flexible(false)
            .engine(EngineType::Columnar),
    ));
    routes
}

#[test]
fn strict_parsing_refuses_a_ragged_record_on_every_chunk_size() {
    let mut wrong = Vec::new();
    for (case, source, _) in sources() {
        let csv = write_csv(&source);
        for (route, profiler) in strict_routes() {
            match profiler.analyze_file(csv.path()) {
                Ok(report) => wrong.push(format!(
                    "{route}, {case}: profiled {} rows, {} ragged",
                    report.execution.rows_processed, report.execution.ragged_row_count
                )),
                Err(error) if !error.to_string().contains("fields") => {
                    wrong.push(format!("{route}, {case}: {error}"))
                }
                Err(_) => {}
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn flexible_parsing_counts_every_ragged_record_on_every_chunk_size() {
    let mut wrong = Vec::new();
    for (case, source, ragged) in sources() {
        let csv = write_csv(&source);
        let rows = source.lines().count() - 1;
        for (label, chunk_size) in CHUNK_SIZES {
            let outcome = Profiler::new()
                .csv_flexible(true)
                .engine(EngineType::Incremental)
                .chunk_size(chunk_size)
                .analyze_file(csv.path());
            match outcome {
                Ok(report)
                    if report.execution.rows_processed == rows
                        && report.execution.ragged_row_count == ragged => {}
                Ok(report) => wrong.push(format!(
                    "{label}, {case}: rows {}, ragged {}",
                    report.execution.rows_processed, report.execution.ragged_row_count
                )),
                Err(error) => wrong.push(format!("{label}, {case}: {error}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
