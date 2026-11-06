//! CSV delimiter detection must use the quote character configured for parsing (#1094).
//!
//! A pipe inside an apostrophe-quoted header is content, not evidence for a
//! pipe delimiter. Choosing it cuts valid one-column records before profiling.

use std::io::{Cursor, Write};

use dataprof::{CsvParserConfig, analyze_csv_file, analyze_csv_from_reader};
use tempfile::NamedTempFile;

/// A source and the actual fields it contains under apostrophe quoting.
struct Case<'a> {
    name: &'a str,
    bytes: &'a [u8],
    columns: &'a [&'a str],
    values: &'a [&'a [&'a str]],
    delimiter: u8,
    ragged_rows: usize,
}

const CASES: &[Case<'_>] = &[
    Case {
        name: "quoted header pipe is not a delimiter",
        bytes: b"'key|part'\na|b\nc|d\n",
        columns: &["key|part"],
        values: &[&["a|b", "c|d"]],
        delimiter: b',',
        ragged_rows: 0,
    },
    Case {
        name: "doubled apostrophe stays inside the quoted field",
        bytes: b"'key''|part'\na|b\nc|d\n",
        columns: &["key'|part"],
        values: &[&["a|b", "c|d"]],
        delimiter: b',',
        ragged_rows: 0,
    },
    Case {
        name: "an apostrophe within an unquoted field is literal",
        bytes: b"key's;value\nalice's;1\nbob's;2\n",
        columns: &["key's", "value"],
        values: &[&["alice's", "bob's"], &["1", "2"]],
        delimiter: b';',
        ragged_rows: 0,
    },
    Case {
        name: "quoted newline belongs to the header",
        bytes: b"'key\npart';value\n'a;b';1\n'c;d';2\n",
        columns: &["key\npart", "value"],
        values: &[&["a;b", "c;d"], &["1", "2"]],
        delimiter: b';',
        ragged_rows: 0,
    },
    Case {
        name: "apostrophe-quoted empty trailing fields carry no values",
        bytes: b"id;amount,EUR\n1;10,50;''\n2;20;''\n",
        columns: &["id", "amount,EUR"],
        values: &[&["1", "2"], &["10,50", "20"]],
        delimiter: b';',
        ragged_rows: 2,
    },
];

fn config() -> CsvParserConfig {
    CsvParserConfig {
        quote_char: b'\'',
        ..CsvParserConfig::default()
    }
}

/// The public reader exposes retained values, so assert the fields themselves.
fn assert_reader(case: &Case<'_>) {
    let (profiles, statistics, rows, headers) =
        analyze_csv_from_reader(Cursor::new(case.bytes), &config())
            .unwrap_or_else(|error| panic!("{}: {error}", case.name));
    assert_eq!(headers, case.columns, "{}", case.name);
    assert_eq!(rows, 2, "{}", case.name);
    assert_eq!(profiles.len(), case.columns.len(), "{}", case.name);
    for (name, values) in case.columns.iter().zip(case.values) {
        let column = statistics.get_column_stats(name).expect("expected column");
        assert_eq!(column.sample_values(), *values, "{}: {name}", case.name);
    }
    for profile in profiles {
        assert_eq!(profile.total_count, 2, "{}", case.name);
        assert_eq!(profile.null_count, 0, "{}", case.name);
    }
}

/// File reports also expose the selected delimiter and recovered ragged rows.
fn assert_file(case: &Case<'_>) {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(case.bytes).unwrap();
    file.flush().unwrap();
    let report = analyze_csv_file(file.path(), &config())
        .unwrap_or_else(|error| panic!("{}: {error}", case.name));
    assert_eq!(report.execution.rows_processed, 2, "{}", case.name);
    assert_eq!(
        report.execution.ragged_row_count, case.ragged_rows,
        "{}",
        case.name
    );
    assert_eq!(report.execution.error_count, 0, "{}", case.name);
    assert!(report.execution.source_exhausted, "{}", case.name);
    let metadata = report.data_source.csv_metadata().expect("CSV metadata");
    assert_eq!(metadata.delimiter, char::from(case.delimiter).to_string());
    let detection = metadata
        .delimiter_detection
        .as_ref()
        .expect("delimiter was detected");
    assert_eq!(detection.header_fields, case.columns.len(), "{}", case.name);
    assert_eq!(detection.unlisted_delimiter, None, "{}", case.name);

    // Explicitly supplying the expected delimiter bypasses detection and is
    // an independent reference for every serialized column metric.
    let explicit = config().with_delimiter(case.delimiter);
    let (expected, _, _, headers) =
        analyze_csv_from_reader(Cursor::new(case.bytes), &explicit).unwrap();
    assert_eq!(headers, case.columns, "{}", case.name);
    assert_eq!(
        serde_json::to_value(&report.column_profiles).unwrap(),
        serde_json::to_value(expected).unwrap(),
        "{}",
        case.name
    );
}

#[test]
fn file_detection_obeys_the_configured_quote() {
    for case in CASES {
        assert_file(case);
    }
}

#[test]
fn reader_detection_preserves_values_with_the_configured_quote() {
    for case in CASES {
        assert_reader(case);
    }
}

#[test]
fn quoted_header_newline_does_not_end_the_detection_sample() {
    // The newline is inside the first quoted field. Reading only 4 KB sees
    // neither the real end of the header nor its semicolon delimiter.
    let name = format!("key\n{}|part", "x".repeat(5000));
    let bytes = format!("'{name}';value\na|b;1\nc|d;2\n");
    let case = Case {
        name: "quoted header crosses the detection sample",
        bytes: bytes.as_bytes(),
        columns: &[&name, "value"],
        values: &[&["a|b", "c|d"], &["1", "2"]],
        delimiter: b';',
        ragged_rows: 0,
    };
    assert_file(&case);
    assert_reader(&case);
}

#[test]
fn active_quote_is_not_an_unlisted_delimiter() {
    for case in [
        Case {
            name: "quotes surrounding one-column fields are not separators",
            bytes: b"'key'\n'a'\n'b'\n",
            columns: &["key"],
            values: &[&["a", "b"]],
            delimiter: b',',
            ragged_rows: 0,
        },
        Case {
            name: "the active quote is not suggested even when literal",
            bytes: b"key's\nalice's\nbob's\n",
            columns: &["key's"],
            values: &[&["alice's", "bob's"]],
            delimiter: b',',
            ragged_rows: 0,
        },
        Case {
            name: "punctuation inside custom quotes is not a separator",
            bytes: b"'key^part'\n'a^b'\n'c^d'\n",
            columns: &["key^part"],
            values: &[&["a^b", "c^d"]],
            delimiter: b',',
            ragged_rows: 0,
        },
    ] {
        assert_file(&case);
        assert_reader(&case);
    }
}

#[test]
fn an_inactive_quote_can_be_an_unlisted_delimiter() {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(b"key\"part\na\"b\nc\"d\n").unwrap();
    file.flush().unwrap();
    let report = analyze_csv_file(file.path(), &config()).unwrap();
    let metadata = report.data_source.csv_metadata().unwrap();
    assert_eq!(metadata.delimiter, ",");
    let detection = metadata.delimiter_detection.as_ref().unwrap();
    assert_eq!(detection.header_fields, 1);
    assert_eq!(detection.unlisted_delimiter.as_deref(), Some("\""));
}

#[test]
fn leading_record_terminators_take_precedence_over_a_matching_quote_byte() {
    // csv skips terminators at the start of a record before checking for a
    // quote. Configuring that byte must not hide the real header from detection.
    for quote in *b"\n\r" {
        let bytes = b"\nkey;value\na\nb;2\n"
            .iter()
            .map(|&byte| if byte == b'\n' { quote } else { byte })
            .collect::<Vec<_>>();
        let mut file = NamedTempFile::with_suffix(".csv").unwrap();
        file.write_all(&bytes).unwrap();
        file.flush().unwrap();
        for delimiter in [Some(b';'), None] {
            let config = CsvParserConfig {
                quote_char: quote,
                delimiter,
                ..CsvParserConfig::default()
            };
            let report = analyze_csv_file(file.path(), &config).unwrap();
            assert_eq!(report.data_source.csv_metadata().unwrap().delimiter, ";");
            assert_eq!(report.execution.rows_processed, 2);
            assert_eq!(report.execution.ragged_row_count, 1);
            assert_eq!(
                report
                    .column_profiles
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>(),
                ["key", "value"]
            );
            assert_eq!(report.column_profiles[1].null_count, 1);
        }
    }
}

#[test]
fn an_explicit_delimiter_keeps_bypassing_detection() {
    let bytes = b"'key|part'\na|b\nc|d\n";
    let explicit = config().with_delimiter(b',');
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();
    let report = analyze_csv_file(file.path(), &explicit).unwrap();
    let metadata = report.data_source.csv_metadata().expect("CSV metadata");
    assert_eq!(metadata.delimiter, ",");
    assert_eq!(metadata.delimiter_detection, None);
    assert_eq!(report.execution.rows_processed, 2);
    assert_eq!(report.execution.ragged_row_count, 0);
    assert_eq!(report.column_profiles.len(), 1);
    assert_eq!(report.column_profiles[0].name, "key|part");
    let (_, statistics, rows, headers) =
        analyze_csv_from_reader(Cursor::new(bytes), &explicit).unwrap();
    assert_eq!(rows, 2);
    assert_eq!(headers, ["key|part"]);
    assert_eq!(
        statistics
            .get_column_stats("key|part")
            .unwrap()
            .sample_values(),
        ["a|b", "c|d"]
    );
}

#[test]
fn public_detectors_keep_default_double_quote_rules() {
    // These public helpers take no configuration and must retain their
    // existing double-quote grammar; the configured helper stays private.
    for (bytes, delimiter, fields) in [
        (&b"'key|part'\na|b\nc|d\n"[..], b'|', 2),
        (&b"\"key|part\"\na|b\nc|d\n"[..], b',', 1),
        // Byte-based syntax scanning retains Unicode blank-record filtering.
        ("\u{2003}\nid;value\n1;2\n".as_bytes(), b';', 2),
    ] {
        assert_eq!(
            dataprof_csv::detect_delimiter(Cursor::new(bytes)).unwrap(),
            delimiter
        );
        let detection = dataprof_csv::detect_delimiter_detailed(Cursor::new(bytes)).unwrap();
        assert_eq!(detection.delimiter, char::from(delimiter).to_string());
        assert_eq!(detection.header_fields, fields);
        let (with_head, head) =
            dataprof_csv::detect_delimiter_with_head(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(with_head, delimiter);
        assert_eq!(head, bytes);
    }
}

#[test]
fn a_configured_non_utf8_quote_byte_preserves_utf8_fields() {
    // CSV quote characters are bytes. The parser removes 0xff before it
    // validates the decoded fields as UTF-8; detection must not replace it.
    let bytes = b"\xffkey|part\xff\na|b\na|c\n";
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();

    // First establish the existing explicit-delimiter grammar, then require
    // automatic detection to preserve exactly the same records and values.
    for delimiter in [Some(b','), None] {
        let config = CsvParserConfig {
            quote_char: 0xff,
            delimiter,
            ..CsvParserConfig::default()
        };
        let report = analyze_csv_file(file.path(), &config).unwrap();
        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.ragged_row_count, 0);
        assert_eq!(report.execution.error_count, 0);
        assert_eq!(report.column_profiles.len(), 1);
        let column = &report.column_profiles[0];
        assert_eq!(column.name, "key|part");
        assert_eq!(column.total_count, 2);
        assert_eq!(column.null_count, 0);
        assert_eq!(column.unique_count, Some(2));
        let uniqueness = report
            .quality
            .as_ref()
            .unwrap()
            .metrics
            .uniqueness
            .as_ref()
            .unwrap();
        assert_eq!(uniqueness.rows_checked, 2);
        assert_eq!(uniqueness.duplicate_rows, 0);
        let metadata = report.data_source.csv_metadata().unwrap();
        assert_eq!(metadata.delimiter, ",");
        assert_eq!(metadata.delimiter_detection.is_some(), delimiter.is_none());
        if let Some(detection) = &metadata.delimiter_detection {
            assert_eq!(detection.header_fields, 1);
            assert_eq!(detection.unlisted_delimiter, None);
        }

        let (_, statistics, rows, headers) =
            analyze_csv_from_reader(Cursor::new(bytes), &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(headers, ["key|part"]);
        assert_eq!(
            statistics
                .get_column_stats("key|part")
                .unwrap()
                .sample_values(),
            ["a|b", "a|c"]
        );
    }
}
