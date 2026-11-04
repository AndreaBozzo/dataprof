//! A CSV report records the delimiter it was split on and what delimiter
//! detection saw, and `findings()` warns when the choice was not clear
//! (#1100). Detection itself does not change: these files read as before.
//!
//! - Two candidates can read a file equally well (`id;amount,EUR` over
//!   `1;10,50;`): the report names the runner-up, and `delimiter_ambiguous`
//!   says so.
//! - No candidate splits the header while a character outside them splits
//!   the header and every record alike (`id^amount` over `1^10`): the report
//!   names it, and `header_not_split` says so. A plain one-column file has no
//!   such character and gets no finding.
//! - A delimiter the caller set records no detection and no finding.
//!
//! `python/tests/test_csv_delimiter_signal.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CsvParserConfig, CsvSourceMetadata, EngineType, FindingCode, FindingPolicy, ProfileReport,
    Profiler,
};

/// `(case, source, delimiter used, header fields, runner-up, unlisted, codes)`.
type Case = (
    &'static str,
    &'static str,
    &'static str,
    usize,
    Option<&'static str>,
    Option<&'static str>,
    &'static [FindingCode],
);

const CASES: [Case; 5] = [
    (
        "comma and semicolon tie",
        "id;amount,EUR\n1;10,50;\n2;20,75;\n",
        ",",
        2,
        Some(";"),
        None,
        &[FindingCode::DelimiterAmbiguous],
    ),
    (
        "the mirror tie",
        "id,part;value\nx;y,z,\nu;v,w,\n",
        ";",
        2,
        Some(","),
        None,
        &[FindingCode::DelimiterAmbiguous],
    ),
    (
        "delimiter outside the candidates",
        "id^amount\n1^10\n2^20\n",
        ",",
        1,
        None,
        Some("^"),
        &[FindingCode::HeaderNotSplit],
    ),
    (
        "one-column file",
        "value\n1\n2\n3\n",
        ",",
        1,
        None,
        None,
        &[],
    ),
    (
        "clear comma",
        "a,b,c\n1,2,3\n4,5,6\n",
        ",",
        3,
        None,
        None,
        &[],
    ),
];

fn write_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    write!(file, "{contents}").unwrap();
    file.flush().unwrap();
    file
}

fn delimiter_codes(report: &ProfileReport) -> Vec<FindingCode> {
    FindingPolicy::new()
        .evaluate(report)
        .unwrap()
        .findings
        .into_iter()
        .map(|finding| finding.code)
        .filter(|code| {
            matches!(
                code,
                FindingCode::DelimiterAmbiguous | FindingCode::HeaderNotSplit
            )
        })
        .collect()
}

/// What is wrong with `report` for `case`, if anything.
fn check(report: &ProfileReport, case: &Case) -> Option<String> {
    let (_, _, delimiter, header_fields, runner_up, unlisted, codes) = *case;
    let Some(metadata) = report.data_source.csv_metadata() else {
        return Some("no csv_metadata".to_string());
    };
    let Some(detection) = &metadata.delimiter_detection else {
        return Some(format!("no delimiter_detection: {metadata:?}"));
    };
    let seen = (
        metadata.delimiter.as_str(),
        detection.header_fields,
        detection.runner_up.as_deref(),
        detection.unlisted_delimiter.as_deref(),
        delimiter_codes(report),
    );
    let wanted = (
        delimiter,
        header_fields,
        runner_up,
        unlisted,
        codes.to_vec(),
    );
    (seen != wanted).then(|| format!("got {seen:?}, want {wanted:?}"))
}

fn file_routes(path: &std::path::Path) -> Vec<(&'static str, Result<ProfileReport, String>)> {
    let mut routes: Vec<_> = [
        ("auto", EngineType::Auto),
        ("incremental", EngineType::Incremental),
        ("columnar", EngineType::Columnar),
    ]
    .into_iter()
    .map(|(route, engine)| {
        (
            route,
            Profiler::new()
                .engine(engine)
                .analyze_file(path)
                .map_err(|e| e.to_string()),
        )
    })
    .collect();
    routes.push((
        "analyze_csv_file",
        dataprof::analyze_csv_file(path, &CsvParserConfig::default()).map_err(|e| e.to_string()),
    ));
    routes
}

#[test]
fn every_file_route_records_what_delimiter_detection_saw() {
    let mut wrong = Vec::new();
    for case in &CASES {
        let csv = write_csv(case.1);
        for (route, outcome) in file_routes(csv.path()) {
            match outcome {
                Ok(report) => {
                    if let Some(problem) = check(&report, case) {
                        wrong.push(format!("{route}, {}: {problem}", case.0));
                    }
                }
                Err(error) => wrong.push(format!("{route}, {}: {error}", case.0)),
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn a_configured_delimiter_records_no_detection_and_no_finding() {
    let csv = write_csv(CASES[0].1);
    let mut wrong = Vec::new();
    let configured = CsvSourceMetadata::configured(b';');
    let mut routes: Vec<_> = [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ]
    .into_iter()
    .map(|engine| {
        (
            format!("{engine:?}"),
            Profiler::new()
                .engine(engine)
                .csv_delimiter(b';')
                .analyze_file(csv.path())
                .unwrap(),
        )
    })
    .collect();
    routes.push((
        "analyze_csv_file".to_string(),
        dataprof::analyze_csv_file(csv.path(), &CsvParserConfig::default().with_delimiter(b';'))
            .unwrap(),
    ));
    for (route, report) in routes {
        let metadata = report.data_source.csv_metadata();
        let codes = delimiter_codes(&report);
        if metadata != Some(&configured) || !codes.is_empty() {
            wrong.push(format!("{route}: {metadata:?}, {codes:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "async-streaming")]
mod async_routes {
    use super::{CASES, check};

    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, Profiler};

    #[tokio::test]
    async fn async_bytes_record_what_delimiter_detection_saw() {
        let mut wrong = Vec::new();
        for case in &CASES {
            let data = case.1;
            let source = BytesSource::new(
                bytes::Bytes::from_static(data.as_bytes()),
                AsyncSourceInfo::new("delimiter-signal", FileFormat::Csv)
                    .size_hint(Some(data.len() as u64)),
            );
            match Profiler::new().profile_stream(source).await {
                Ok(report) => {
                    if let Some(problem) = check(&report, case) {
                        wrong.push(format!("async bytes, {}: {problem}", case.0));
                    }
                }
                Err(error) => wrong.push(format!("async bytes, {}: {error}", case.0)),
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}
