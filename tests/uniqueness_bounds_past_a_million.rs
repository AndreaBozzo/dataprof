//! A full-source quality gate decides past a million distinct rows (#819).
//!
//! Distinct counts and duplicate rows are exact up to a million (#786) and
//! estimated past it. #789 bounded every sampled dimension but uniqueness,
//! which it left to those exact counts, so past a million rows the overall
//! score had no interval and `--min-quality` was inconclusive on a source read
//! to the end. When an exact set is dropped, the distinct values it held are
//! a floor the true count cannot go under; uniqueness is now bounded from
//! those floors, with certainty, on every engine that estimates.
//!
//! Only this scale reaches the estimates, which is why the test profiles a
//! million rows.

use std::io::Write;

use dataprof::{EngineType, Profiler, QualityPolicy, Verdict};
use tempfile::NamedTempFile;

const ROWS: usize = 1_050_000;
/// Every 1,000th row repeats the one before it.
const DUPLICATE_EVERY: usize = 1_000;

fn value(row: usize) -> (usize, &'static str) {
    let source = if row.is_multiple_of(DUPLICATE_EVERY) && row > 0 {
        row - 1
    } else {
        row
    };
    (source, ["a", "b", "c", "d", "e"][source % 5])
}

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    let mut out = std::io::BufWriter::new(file.as_file_mut());
    writeln!(out, "id,category").unwrap();
    for row in 0..ROWS {
        let (id, category) = value(row);
        writeln!(out, "{id},{category}").unwrap();
    }
    out.flush().unwrap();
    drop(out);
    file
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let (ids, categories): (Vec<i64>, Vec<&str>) = (0..ROWS)
        .map(|row| {
            let (id, category) = value(row);
            (id as i64, category)
        })
        .unzip();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(categories)),
        ],
    )
    .unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

#[test]
fn a_full_source_score_is_decided_past_a_million_rows() {
    let csv = csv_fixture();
    let mut routes = vec![
        (
            "incremental",
            Profiler::new()
                .engine(EngineType::Incremental)
                .analyze_file(csv.path())
                .expect("incremental"),
        ),
        (
            "columnar",
            Profiler::new()
                .engine(EngineType::Columnar)
                .analyze_file(csv.path())
                .expect("columnar"),
        ),
    ];
    #[cfg(feature = "parquet")]
    {
        let parquet = parquet_fixture();
        routes.push((
            "parquet",
            Profiler::new()
                .analyze_file(parquet.path())
                .expect("parquet"),
        ));
    }

    // The duplicate rows' exact set held 1,000,001 distinct rows when it was
    // dropped, and so did the key column's: both floors are that share.
    let floor = 1_000_001.0 / ROWS as f64 * 100.0;
    for (label, report) in routes {
        let quality = report.quality.as_ref().expect("quality assessed");
        let uniqueness = quality.metrics.uniqueness.as_ref().expect("uniqueness");
        assert!(uniqueness.duplicate_rows_approximate, "[{label}] estimated");

        let bounds = quality
            .score_bounds()
            .unwrap_or_else(|| panic!("[{label}] sampled, so bounded"));
        let interval = bounds.dimension_scores["uniqueness"]
            .unwrap_or_else(|| panic!("[{label}] uniqueness unbounded"));
        assert!(
            (interval.lower - floor).abs() < 0.01 && interval.upper == 100.0,
            "[{label}] {interval:?}"
        );
        let overall = bounds
            .overall_score
            .unwrap_or_else(|| panic!("[{label}] overall unbounded"));
        let score = quality.metrics.overall_score().expect("overall score");
        assert!(
            overall.lower <= score && score <= overall.upper,
            "[{label}] {score} outside {overall:?}"
        );

        // Below the interval, the gate decides instead of answering
        // inconclusive, as it did at 900,000 rows before the fix.
        let result = QualityPolicy::new()
            .min_quality_score(overall.lower.floor() - 1.0)
            .evaluate(&report)
            .unwrap();
        assert_eq!(result.verdict, Verdict::Pass, "[{label}] {result:?}");
    }
}
