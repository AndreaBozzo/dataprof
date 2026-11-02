//! `max_rows` bounds the analyzed population under strict CSV parsing too
//! (#757). A ragged record past the cap is not part of the profile, so
//! `csv_flexible=false` must not fail over it on any route, as flexible parsing
//! and `analyze_csv_file` with the strict config already did. A ragged record
//! inside the cap still fails. A quote opened past the cap and never closed
//! swallows the rest of the file into one ragged record, so it is not judged
//! either.

use std::io::Write;

use dataprof::{
    ChunkSize, CsvParserConfig, EngineType, ProfileReport, Profiler, StopCondition,
    TruncationReason,
};

/// `(case, cap, source)`: in each source the cap admits only well-formed rows,
/// and a ragged record or one that opens a quote it never closes follows them.
const PAST_THE_CAP: [(&str, u64, &str); 5] = [
    ("short record", 1, "name,age,city\nAlice,25,Rome\nBob,30\n"),
    (
        "long record",
        1,
        "name,age,city\nAlice,25,Rome\nBob,30,Milan,extra\n",
    ),
    (
        "ragged record two past the cap",
        1,
        "name,age,city\nAlice,25,Rome\nBob,30,Milan\nCarl,40\n",
    ),
    (
        "cap of zero, ragged first record",
        0,
        "name,age,city\nBob,30\nCarl,40,Milan\n",
    ),
    (
        "unclosed quote past the cap",
        1,
        "name,age,city\nAlice,25,Rome\nBob,\"30,Milan\nCarl,40,Turin\n",
    ),
];

/// The second row is ragged and a cap of two admits it.
const INSIDE_THE_CAP: &str = "name,age,city\nAlice,25,Rome\nBob,30\nCarl,40,Milan\n";

fn write_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    write!(file, "{contents}").unwrap();
    file.flush().unwrap();
    file
}

fn strict_profiler(cap: u64) -> Profiler {
    Profiler::new()
        .csv_flexible(false)
        .stop_when(StopCondition::MaxRows(cap))
}

/// Every synchronous file route, each a strict profile capped at `cap` rows.
fn file_routes(
    path: &std::path::Path,
    cap: u64,
) -> Vec<(&'static str, Result<ProfileReport, String>)> {
    let engines = [
        ("auto", strict_profiler(cap).engine(EngineType::Auto)),
        (
            "incremental",
            strict_profiler(cap).engine(EngineType::Incremental),
        ),
        (
            "incremental, one-record chunks",
            strict_profiler(cap)
                .engine(EngineType::Incremental)
                .chunk_size(ChunkSize::Fixed(1)),
        ),
        (
            "columnar",
            strict_profiler(cap).engine(EngineType::Columnar),
        ),
    ];
    let mut routes: Vec<_> = engines
        .into_iter()
        .map(|(route, profiler)| {
            (
                route,
                profiler.analyze_file(path).map_err(|e| e.to_string()),
            )
        })
        .collect();
    routes.push((
        "analyze_csv_file",
        dataprof::analyze_csv_file(
            path,
            &CsvParserConfig::strict().max_rows(Some(cap as usize)),
        )
        .map_err(|e| e.to_string()),
    ));
    routes
}

/// The profile holds the `cap` rows, none of them ragged, and says it was cut
/// short by the cap.
fn capped_at(report: &ProfileReport, cap: u64) -> Result<(), String> {
    let execution = &report.execution;
    if execution.rows_processed as u64 != cap
        || execution.ragged_row_count != 0
        || !matches!(
            execution.truncation_reason,
            Some(TruncationReason::MaxRows(limit)) if limit == cap
        )
    {
        return Err(format!(
            "rows {}, ragged {}, truncation {:?}",
            execution.rows_processed, execution.ragged_row_count, execution.truncation_reason
        ));
    }
    Ok(())
}

#[test]
fn strict_parsing_does_not_judge_a_record_past_the_row_cap() {
    let mut wrong = Vec::new();
    for (case, cap, source) in PAST_THE_CAP {
        let csv = write_csv(source);
        for (route, outcome) in file_routes(csv.path(), cap) {
            if let Err(problem) = outcome.and_then(|report| capped_at(&report, cap)) {
                wrong.push(format!("{route}, {case}: {problem}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn strict_parsing_still_rejects_a_ragged_record_inside_the_row_cap() {
    let csv = write_csv(INSIDE_THE_CAP);
    let mut wrong = Vec::new();
    for (route, outcome) in file_routes(csv.path(), 2) {
        // A chunk's first record is not compared with the header, so strict
        // parsing misses a ragged record that starts a chunk: #1202.
        if route == "incremental, one-record chunks" {
            continue;
        }
        match outcome {
            Ok(report) => wrong.push(format!(
                "{route}: profiled {} rows",
                report.execution.rows_processed
            )),
            Err(message) if !message.contains("fields") => {
                wrong.push(format!("{route}: {message}"))
            }
            Err(_) => {}
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "async-streaming")]
mod async_bytes {
    use super::{INSIDE_THE_CAP, PAST_THE_CAP, capped_at, strict_profiler};

    use dataprof::{AsyncSourceInfo, BytesSource, ChunkSize, FileFormat};

    fn source(data: &'static str) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data.as_bytes()),
            AsyncSourceInfo::new("strict-row-cap", FileFormat::Csv)
                .size_hint(Some(data.len() as u64)),
        )
    }

    #[tokio::test]
    async fn strict_parsing_does_not_judge_a_record_past_the_row_cap() {
        let mut wrong = Vec::new();
        for (case, cap, data) in PAST_THE_CAP {
            for (route, chunk_size) in [
                ("async", ChunkSize::Adaptive),
                ("async, one-record chunks", ChunkSize::Fixed(1)),
            ] {
                let outcome = strict_profiler(cap)
                    .chunk_size(chunk_size)
                    .profile_stream(source(data))
                    .await
                    .map_err(|e| e.to_string())
                    .and_then(|report| capped_at(&report, cap));
                if let Err(problem) = outcome {
                    wrong.push(format!("{route}, {case}: {problem}"));
                }
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    #[tokio::test]
    async fn strict_parsing_still_rejects_a_ragged_record_inside_the_row_cap() {
        let mut wrong = Vec::new();
        for (route, chunk_size) in [
            ("async", ChunkSize::Adaptive),
            ("async, one-record chunks", ChunkSize::Fixed(1)),
        ] {
            match strict_profiler(2)
                .chunk_size(chunk_size)
                .profile_stream(source(INSIDE_THE_CAP))
                .await
            {
                Ok(report) => wrong.push(format!(
                    "{route}: profiled {} rows",
                    report.execution.rows_processed
                )),
                Err(error) if !error.to_string().contains("csv_flexible") => {
                    wrong.push(format!("{route}: {error}"))
                }
                Err(_) => {}
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}
