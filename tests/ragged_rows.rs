//! Ragged CSV rows must leave a structural-violation signal in the report.
//!
//! Flexible parsing recovers rows whose field count differs from the header
//! (extra fields dropped, missing fields padded to null), but a recovered row
//! is not a clean row. `execution.ragged_row_count` is the honest answer to
//! "did parsing silently go wrong?" — it must be nonzero for broken input and
//! zero for well-formed input. Guards #418 and #462.

use std::io::Write;

use dataprof::Profiler;

fn write_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    write!(file, "{contents}").unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn ragged_rows_are_counted_not_swallowed() {
    // Row 2 is short (2 fields), row 3 is over-long (4 fields); both differ
    // from the 3-column header and must register as ragged.
    let csv = write_csv("name,age,city\nAlice,25,NYC\nBob,30\nCarol,35,LA,EXTRA\nDave,40,SF\n");

    let report = Profiler::new()
        .analyze_file(csv.path())
        .expect("flexible parsing recovers ragged rows");

    assert_eq!(report.execution.rows_processed, 4);
    assert_eq!(
        report.execution.ragged_row_count, 2,
        "one short and one over-long row must both count as ragged"
    );
}

#[test]
fn well_formed_csv_reports_zero_ragged_rows() {
    let csv = write_csv("name,age,city\nAlice,25,NYC\nBob,30,LA\n");

    let report = Profiler::new()
        .analyze_file(csv.path())
        .expect("a clean file profiles cleanly");

    assert_eq!(
        report.execution.ragged_row_count, 0,
        "a file with no field-count violations is not ragged"
    );
}

#[test]
fn truly_broken_example_is_not_reported_clean() {
    // The dogfooding fixture from #418: a 3-column header with a 5-field row.
    // Before the fix this profiled at error_count 0 / consistency 100.
    let report = Profiler::new()
        .analyze_file("examples/test_datasets/truly_broken.csv")
        .expect("flexible parsing recovers the fixture");

    assert!(
        report.execution.ragged_row_count > 0,
        "truly_broken.csv must surface a structural-violation signal, got {}",
        report.execution.ragged_row_count
    );
}

#[test]
fn strict_file_parsing_keeps_its_own_diagnostic() {
    // Asking for strict parsing opts out of recovery, so the auto engine must
    // not retry under a second parser and replace the actionable CSV error
    // with "all engines failed".
    let csv = write_csv("name,age,city\nAlice,25,NYC\nBob,30\n");

    let err = Profiler::new()
        .csv_flexible(false)
        .analyze_file(csv.path())
        .expect_err("strict parsing must reject a ragged record");

    let message = err.to_string();
    assert!(
        !message.contains("All engines failed"),
        "a deterministic parse rejection must not be buried: {message}"
    );
    assert!(message.contains("3 fields"), "unexpected error: {message}");
}

/// Async byte streams follow the same policy as the default file path: recover
/// the row, count it. The two transports must not disagree about the same
/// bytes, or a caller can launder a broken source through the async API. #462.
#[cfg(feature = "async-streaming")]
mod async_parity {
    use super::write_csv;

    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, Profiler};

    const RAGGED: &[u8] = b"name,age,city\nAlice,25,NYC\nBob,30\nCarol,35,LA,EXTRA\nDave,40,SF\n";
    const CLEAN: &[u8] = b"name,age,city\nAlice,25,NYC\nBob,30,LA\n";

    fn source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo::new("ragged-parity", FileFormat::Csv)
                .size_hint(Some(data.len() as u64)),
        )
    }

    #[tokio::test]
    async fn async_bytes_match_the_file_path_ragged_count() {
        let file = write_csv(std::str::from_utf8(RAGGED).unwrap());
        let from_file = Profiler::new().analyze_file(file.path()).unwrap();
        let from_stream = Profiler::new()
            .profile_stream(source(RAGGED))
            .await
            .unwrap();

        assert_eq!(from_file.execution.ragged_row_count, 2);
        assert_eq!(
            from_stream.execution.ragged_row_count, from_file.execution.ragged_row_count,
            "async bytes and the file path must agree on the same input"
        );
        assert_eq!(
            from_stream.execution.rows_processed,
            from_file.execution.rows_processed
        );
    }

    #[tokio::test]
    async fn async_bytes_report_clean_input_as_clean() {
        let report = Profiler::new().profile_stream(source(CLEAN)).await.unwrap();
        assert_eq!(report.execution.ragged_row_count, 0);
        assert_eq!(report.execution.error_count, 0);
    }

    #[tokio::test]
    async fn async_bytes_honor_csv_flexible_false() {
        // The option was previously accepted and ignored on this path; a caller
        // asking for strict parsing must get a rejection, not a repaired report.
        let err = Profiler::new()
            .csv_flexible(false)
            .profile_stream(source(RAGGED))
            .await
            .expect_err("strict parsing must reject a ragged record");

        assert!(
            err.to_string().contains("csv_flexible"),
            "strict failure must name the recovering option: {err}"
        );
    }
}
