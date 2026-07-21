//! Ragged CSV rows must leave a structural-violation signal in the report.
//!
//! Flexible parsing recovers rows whose field count differs from the header
//! (extra fields dropped, missing fields padded to null), but a recovered row
//! is not a clean row. `execution.ragged_row_count` is the honest answer to
//! "did parsing silently go wrong?" — it must be nonzero for broken input and
//! zero for well-formed input. Guards #418.

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
