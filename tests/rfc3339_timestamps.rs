//! RFC 3339 is the default rendering for a timestamp in JSON APIs, in export
//! tooling, and in Parquet-to-CSV dumps, so a column of them has to profile as
//! a date column — identically on every engine (#643).

use std::io::Write;

use dataprof::{ColumnStats, DataType, EngineType, Profiler, QualityDimension};
use tempfile::NamedTempFile;

/// Mixed `Z` and numeric offsets, which is what a real export looks like once
/// more than one producer has written to it.
const RFC3339_CSV: &str = "created_at\n\
2024-01-15T10:30:00Z\n\
2024-01-15T11:45:00Z\n\
2024-01-15T12:00:00+02:00\n\
2024-01-16T08:15:00-05:00\n\
2024-01-16T09:00:00Z\n\
2024-01-17T14:20:00.500Z\n";

const EVERY_ENGINE: [EngineType; 3] = [
    EngineType::Auto,
    EngineType::Incremental,
    EngineType::Columnar,
];

fn write_csv(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    write!(file, "{content}").unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn rfc3339_column_infers_as_a_date_column_on_every_engine() {
    let csv = write_csv(RFC3339_CSV);

    for engine in EVERY_ENGINE {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .expect("profile should succeed");

        let column = &report.column_profiles[0];
        assert_eq!(
            column.data_type,
            DataType::Date,
            "{engine:?} profiled an RFC 3339 timestamp column as {:?}",
            column.data_type
        );
        assert_eq!(
            column.invalid_count,
            Some(0),
            "{engine:?} did not read every value as a well-formed instant"
        );
        assert!(
            matches!(column.stats, ColumnStats::DateTime(_)),
            "{engine:?} reported {:?} instead of temporal statistics",
            column.stats
        );
    }
}

#[test]
fn timeliness_assesses_an_rfc3339_column_on_every_engine() {
    let csv = write_csv(RFC3339_CSV);

    for engine in EVERY_ENGINE {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .expect("profile should succeed");

        let quality = report.quality.as_ref().expect("quality assessed");
        assert!(
            quality
                .metrics
                .assessed_dimensions()
                .contains(&QualityDimension::Timeliness),
            "{engine:?}: timeliness assessed nothing, dimensions were {:?}",
            quality.metrics.assessed_dimensions()
        );

        let timeliness = quality
            .metrics
            .timeliness
            .as_ref()
            .expect("timeliness evidence");
        assert_eq!(
            timeliness.date_values_checked, 6,
            "{engine:?}: date_values_checked should equal the non-null count"
        );
        assert_eq!(
            timeliness.invalid_date_values, 0,
            "{engine:?}: no RFC 3339 value should count as unparseable"
        );
    }
}

/// An offset-bearing value names an instant, and dataprof normalizes it to UTC
/// before any statistic sees it. Both values below sit on 2024-03-10 in their
/// own wall clock and on a *different* UTC day, so a profile that kept the
/// local reading would report a zero-day span across the two.
#[test]
fn offsets_are_normalized_to_utc_before_the_statistics() {
    let csv = write_csv(
        "moment\n\
2024-03-10T23:00:00-05:00\n\
2024-03-10T01:00:00+02:00\n",
    );

    for engine in EVERY_ENGINE {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .expect("profile should succeed");

        let ColumnStats::DateTime(stats) = &report.column_profiles[0].stats else {
            panic!("{engine:?}: expected temporal statistics");
        };

        // 2024-03-09T23:00:00Z .. 2024-03-11T04:00:00Z
        assert_eq!(stats.min_datetime, "2024-03-09", "{engine:?}: min");
        assert_eq!(stats.max_datetime, "2024-03-11", "{engine:?}: max");
        assert_eq!(stats.duration_days, 2.0, "{engine:?}: duration");

        let hours = stats
            .hour_distribution
            .as_ref()
            .expect("values carry a time of day");
        assert_eq!(hours.get(&4), Some(&1), "{engine:?}: 23:00-05:00 is 04Z");
        assert_eq!(hours.get(&23), Some(&1), "{engine:?}: 01:00+02:00 is 23Z");
    }
}

/// The grammar must stay narrow enough that a value it calls a date is a value
/// the parser can read. A form that types the column `Date` and then fails
/// every parse is worse than leaving it as text.
#[test]
fn a_timestamp_shaped_value_no_parser_accepts_stays_text() {
    // `+0200` is ISO 8601 basic, which chrono's RFC 3339 parser rejects.
    let csv = write_csv(
        "moment\n\
2024-01-15T10:30:00+0200\n\
2024-01-15T11:30:00+0200\n\
2024-01-15T12:30:00+0200\n",
    );

    for engine in EVERY_ENGINE {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .expect("profile should succeed");

        assert_eq!(
            report.column_profiles[0].data_type,
            DataType::String,
            "{engine:?} typed a column no parser can read as a date"
        );
    }
}
