//! Exact order statistics of integer and decimal columns (#1038).
//!
//! `wide` holds epoch nanoseconds a few units apart and one small value: past
//! 2^53 its `f64` extrema and quartiles round, and `exact` carries them as
//! canonical decimal strings. `ints` repeats small integers, so it has a mode
//! and fractional quartiles. `floats` is a float column and has no `exact`.
//! `sampled` holds more values than any engine's sample, with its one minimum
//! and one maximum far from the rest: `exact.min` and `exact.max` cover every
//! value, as `min` and `max` do, so they come from the stream and not from the
//! sample.
//!
//! `python/tests/test_exact_order_statistics.py` is the Python twin, and
//! adds the decimal columns the typed routes carry.

use std::io::Write;

use dataprof::{
    ColumnProfile, ColumnStats, CsvParserConfig, EngineType, ExactNumericStats, ProfileReport,
    Profiler, analyze_column, analyze_csv_file,
};
use tempfile::NamedTempFile;

const WIDE: [&str; 5] = [
    "1759700000123456789",
    "1759700000123456791",
    "1759700000123456795",
    "1759700000123456800",
    "3",
];
const INTS: [&str; 5] = ["4", "1", "2", "2", "9"];
const FLOATS: [&str; 5] = ["0.5", "1.5", "2.5", "3.5", "4.5"];

/// Rows of the `sampled` file: more than the 10,000 values any sample keeps.
const SAMPLED_ROWS: usize = 60_000;
/// Its minimum and maximum, each in one row, away from the start where a
/// reservoir begins.
const SAMPLED_MIN: &str = "1759700000000000001";
const SAMPLED_MAX: &str = "1759700000999999999";

fn sampled_value(row: usize) -> String {
    match row {
        31_337 => SAMPLED_MIN.to_string(),
        47_111 => SAMPLED_MAX.to_string(),
        _ => (1_759_700_000_123_456_789_u64 + (row % 1000) as u64).to_string(),
    }
}

fn table_fixture(suffix: &str, write: impl Fn(&mut NamedTempFile)) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    write(&mut file);
    file.flush().unwrap();
    file
}

fn records() -> Vec<String> {
    (0..WIDE.len())
        .map(|row| {
            format!(
                r#"{{"wide":{},"ints":{},"floats":{}}}"#,
                WIDE[row], INTS[row], FLOATS[row]
            )
        })
        .collect()
}

/// Every engine over the CSV, then JSON, JSONL and Parquet, then the
/// database connectors' `analyze_column`.
fn profiles() -> Vec<(String, Vec<ColumnProfile>)> {
    let csv = table_fixture(".csv", |file| {
        writeln!(file, "wide,ints,floats").unwrap();
        for row in 0..WIDE.len() {
            writeln!(file, "{},{},{}", WIDE[row], INTS[row], FLOATS[row]).unwrap();
        }
    });
    let mut reports: Vec<(String, ProfileReport)> = vec![(
        "standard".to_string(),
        analyze_csv_file(csv.path(), &CsvParserConfig::default()).expect("standard CSV"),
    )];
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }
    let json = table_fixture(".json", |file| {
        write!(file, "[{}]", records().join(",")).unwrap();
    });
    let jsonl = table_fixture(".jsonl", |file| {
        writeln!(file, "{}", records().join("\n")).unwrap();
    });
    let mut files = vec![("json", json), ("jsonl", jsonl)];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in files {
        let report = Profiler::new()
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }

    let mut profiles: Vec<(String, Vec<ColumnProfile>)> = reports
        .into_iter()
        .map(|(label, report)| (label, report.column_profiles))
        .collect();
    let in_memory = [("wide", WIDE), ("ints", INTS), ("floats", FLOATS)]
        .iter()
        .map(|(name, values)| {
            let values: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            analyze_column(name, &values)
        })
        .collect();
    profiles.push(("analyze_column".to_string(), in_memory));
    profiles
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let ints = |values: [&str; 5]| -> ArrayRef {
        Arc::new(Int64Array::from(
            values
                .iter()
                .map(|v| v.parse::<i64>().unwrap())
                .collect::<Vec<_>>(),
        ))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("wide", DataType::Int64, false),
        Field::new("ints", DataType::Int64, false),
        Field::new("floats", DataType::Float64, false),
    ]));
    let floats: ArrayRef = Arc::new(Float64Array::from(
        FLOATS
            .iter()
            .map(|v| v.parse::<f64>().unwrap())
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(schema.clone(), vec![ints(WIDE), ints(INTS), floats]).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn exact(columns: &[ColumnProfile], name: &str) -> Result<Option<ExactNumericStats>, String> {
    let column = columns
        .iter()
        .find(|column| column.name == name)
        .ok_or_else(|| format!("no column {name}"))?;
    match &column.stats {
        ColumnStats::Numeric(stats) => Ok(stats.exact.clone()),
        other => Err(format!("{name} has no numeric stats: {other:?}")),
    }
}

fn strings(
    min: &str,
    max: &str,
    median: &str,
    quartiles: [&str; 3],
    mode: Option<&str>,
) -> ExactNumericStats {
    ExactNumericStats {
        min: min.to_string(),
        max: max.to_string(),
        median: Some(median.to_string()),
        q1: Some(quartiles[0].to_string()),
        q2: Some(quartiles[1].to_string()),
        q3: Some(quartiles[2].to_string()),
        mode: mode.map(str::to_string),
    }
}

#[test]
fn every_path_reports_exact_order_statistics_of_integer_columns() {
    // Sorted, `wide` is 3 and four values past 2^53; Type 7 quartiles sit on
    // values, and `ints`, sorted 1 2 2 4 9, on values too.
    let wide = strings(
        "3",
        "1759700000123456800",
        "1759700000123456791",
        [
            "1759700000123456789",
            "1759700000123456791",
            "1759700000123456795",
        ],
        None,
    );
    let ints = strings("1", "9", "2", ["2", "2", "4"], Some("2"));
    let mut wrong = Vec::new();
    for (label, columns) in profiles() {
        for (name, expected) in [
            ("wide", Some(&wide)),
            ("ints", Some(&ints)),
            ("floats", None),
        ] {
            match exact(&columns, name) {
                Ok(got) if got.as_ref() == expected => {}
                Ok(got) => wrong.push(format!("[{label}] {name}: {got:?}")),
                Err(error) => wrong.push(format!("[{label}] {error}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A quartile between two values is interpolated in quarters, exactly: the
/// `f64` one of values past 2^53 rounds to a neighbour.
#[test]
fn exact_quartiles_are_interpolated_between_values() {
    let values: Vec<String> = [
        "9007199254740993",
        "9007199254740995",
        "9007199254740997",
        "9007199254740998",
        "9007199254741000",
        "9007199254741003",
    ]
    .iter()
    .map(|v| v.to_string())
    .collect();
    let column = analyze_column("v", &values);
    let ColumnStats::Numeric(stats) = &column.stats else {
        panic!("{:?}", column.stats);
    };
    // Positions 1.25, 2.5 and 3.75 among the six sorted values.
    assert_eq!(
        stats.exact,
        Some(strings(
            "9007199254740993",
            "9007199254741003",
            "9007199254740997.5",
            [
                "9007199254740995.5",
                "9007199254740997.5",
                "9007199254740999.5"
            ],
            None,
        ))
    );
}

/// `exact.min` and `exact.max` cover every value, as `min` and `max` do, on
/// every engine that samples.
#[test]
fn exact_extremes_cover_values_past_the_sample() {
    let csv = table_fixture(".csv", |file| {
        writeln!(file, "sampled").unwrap();
        for row in 0..SAMPLED_ROWS {
            writeln!(file, "{}", sampled_value(row)).unwrap();
        }
    });
    let mut wrong = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        match exact(&report.column_profiles, "sampled") {
            Ok(Some(got)) if got.min == SAMPLED_MIN && got.max == SAMPLED_MAX => {}
            other => wrong.push(format!("[{engine:?}] {other:?}")),
        }
    }
    // In memory there are no stream aggregates: the column's own pass over
    // every value gives the extremes, and the sample only the order
    // statistics.
    let values: Vec<String> = (0..SAMPLED_ROWS).map(sampled_value).collect();
    let column = analyze_column("sampled", &values);
    match exact(std::slice::from_ref(&column), "sampled") {
        Ok(Some(got)) if got.min == SAMPLED_MIN && got.max == SAMPLED_MAX => {}
        other => wrong.push(format!("[analyze_column] {other:?}")),
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

/// A saved report keeps `exact`, and a report saved before it existed loads
/// without it.
#[test]
fn exact_survives_a_report_round_trip() {
    let values: Vec<String> = WIDE.iter().map(|v| v.to_string()).collect();
    let column = analyze_column("wide", &values);
    let json = serde_json::to_value(&column).unwrap();
    assert_eq!(
        json["stats"]["Numeric"]["exact"]["max"],
        "1759700000123456800"
    );
    let restored: ColumnProfile = serde_json::from_value(json.clone()).unwrap();
    let (ColumnStats::Numeric(restored), ColumnStats::Numeric(original)) =
        (&restored.stats, &column.stats)
    else {
        panic!("not numeric");
    };
    assert_eq!(restored.exact, original.exact);

    let mut older = json;
    older["stats"]["Numeric"]
        .as_object_mut()
        .unwrap()
        .remove("exact");
    let restored: ColumnProfile = serde_json::from_value(older).unwrap();
    let ColumnStats::Numeric(stats) = restored.stats else {
        panic!("not numeric");
    };
    assert_eq!(stats.exact, None);
}
