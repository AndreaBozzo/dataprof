//! A sampled accuracy score's bounds cover the outlier fences of a scan of
//! every value (#1304).
//!
//! A column of zeros and fives with a share of zeros near 75% has its third
//! quartile at 0 or at 5 depending on a handful of values. A full scan draws
//! its fences from the source's quartiles; a route that keeps a sample used
//! to bound the score with the sample's own fences, so a complete scan could
//! pass or fail on the wrong side. A column whose quartiles coincide is no
//! longer tested, so with more than 75% zeros a full scan leaves accuracy
//! unassessed, and with fewer it scores 100.
//!
//! `python/tests/test_sampled_outlier_fences.py` is the Python twin, which
//! also holds each route to the same values profiled from a dict.

use std::io::Write;

use dataprof::{
    EngineType, MetricPack, ProfileReport, Profiler, QualityDimension, QualityPolicy,
    QualityScoreWeights,
};
use tempfile::NamedTempFile;

const ROWS: usize = 40_000;

/// Multipliers that each lay the values out in another order: all prime,
/// so coprime with `ROWS`, and each row maps to a distinct rank.
const ORDERS: [usize; 3] = [7_919, 104_729, 1_299_709];

/// More orders, for a case that needs some sample to land on one side.
const MORE_ORDERS: [usize; 12] = [
    7_919,
    104_729,
    1_299_709,
    15_485_863,
    32_452_843,
    49_979_687,
    67_867_967,
    86_028_121,
    104_395_301,
    122_949_829,
    141_650_939,
    160_481_183,
];

/// One fixture: the number of zeros and of hundreds (the rest are fives),
/// the value added to each, and the verdict of
/// `min_dimension_scores={"accuracy": 90}` on a full scan.
struct Case {
    zeros: usize,
    hundreds: usize,
    base: i128,
    full_scan: &'static str,
}

const CASES: [Case; 8] = [
    // Q3 is 5: the column is tested, no value is an outlier, accuracy 100.
    Case {
        zeros: 29_992,
        hundreds: 0,
        base: 0,
        full_scan: "pass",
    },
    // Q3 is 0, so the IQR is 0: not tested, accuracy unassessed.
    Case {
        zeros: 30_008,
        hundreds: 0,
        base: 0,
        full_scan: "inconclusive",
    },
    // Half zeros, far from the boundary: every route decides.
    Case {
        zeros: 20_000,
        hundreds: 0,
        base: 0,
        full_scan: "pass",
    },
    // The same past 2^53, where the fences are compared exactly.
    Case {
        zeros: 29_992,
        hundreds: 0,
        base: 1 << 60,
        full_scan: "pass",
    },
    Case {
        zeros: 30_008,
        hundreds: 0,
        base: 1 << 60,
        full_scan: "inconclusive",
    },
    Case {
        zeros: 20_000,
        hundreds: 0,
        base: 1 << 60,
        full_scan: "pass",
    },
    // 60% zeros, 25% fives, 15% hundreds: quartiles 0 and 5, far from any
    // boundary, and every hundred an outlier, so accuracy is 85 and the
    // minimum of 90 fails. Every route must decide it (review of #1313).
    Case {
        zeros: 24_000,
        hundreds: 6_000,
        base: 0,
        full_scan: "fail",
    },
    Case {
        zeros: 24_000,
        hundreds: 6_000,
        base: 1 << 60,
        full_scan: "fail",
    },
];

impl Case {
    fn label(&self, order: usize) -> String {
        format!(
            "{} zeros, {} hundreds, base {}, order {order}",
            self.zeros, self.hundreds, self.base
        )
    }

    fn values(&self, order: usize) -> Vec<i128> {
        (0..ROWS)
            .map(|row| {
                let rank = (row * order) % ROWS;
                self.base
                    + if rank < self.zeros {
                        0
                    } else if rank < ROWS - self.hundreds {
                        5
                    } else {
                        100
                    }
            })
            .collect()
    }

    /// Whether a sample of this case can tell the full scan's verdict: near
    /// the boundary it may only report that it cannot.
    fn may_be_inconclusive(&self) -> bool {
        self.zeros != 20_000 && self.hundreds == 0
    }
}

fn file(suffix: &str, text: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(text.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

#[cfg(feature = "parquet")]
fn parquet_file(values: &[i128]) -> Option<NamedTempFile> {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let values: Vec<i64> = values
        .iter()
        .map(|&value| i64::try_from(value).ok())
        .collect::<Option<_>>()?;
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let array = Arc::new(Int64Array::from(values)) as ArrayRef;
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Some(file)
}

/// The values through every engine over a CSV, the other file formats, a
/// quality-only run and the async route.
fn routes(values: &[i128]) -> Vec<(String, ProfileReport)> {
    let mut csv_text = String::from("v\n");
    let mut jsonl = String::new();
    for value in values {
        csv_text.push_str(&format!("{value}\n"));
        jsonl.push_str(&format!("{{\"v\":{value}}}\n"));
    }
    let csv = file(".csv", &csv_text);
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("csv {engine:?}"), report));
    }
    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .metric_packs(vec![MetricPack::Quality])
        .analyze_file(csv.path())
        .expect("quality only");
    reports.push(("csv Incremental quality only".to_string(), report));
    let mut files = vec![("jsonl", Some(file(".jsonl", &jsonl)))];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_file(values)));
    for (label, source) in files {
        let Some(source) = source else { continue };
        let report = Profiler::new()
            .analyze_file(source.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    #[cfg(feature = "async-streaming")]
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let report = runtime
            .block_on(Profiler::new().profile_file(csv.path()))
            .expect("async CSV");
        reports.push(("async csv".to_string(), report));
    }
    reports
}

fn verdict(report: &ProfileReport) -> String {
    let result = QualityPolicy::new()
        .min_dimension_score(QualityDimension::Accuracy, 90.0)
        .evaluate(report)
        .unwrap();
    serde_json::to_value(&result).unwrap()["verdict"]
        .as_str()
        .unwrap()
        .to_string()
}

/// Every route, and the report it saves, gives the full scan's verdict, or
/// says it cannot near the boundary. Never the opposite one.
#[test]
fn every_sampled_route_gives_the_full_scan_verdict_or_none() {
    let mut wrong = Vec::new();
    for case in &CASES {
        for order in ORDERS {
            for (route, report) in routes(&case.values(order)) {
                let saved: ProfileReport =
                    serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
                for (form, report) in [("run", &report), ("saved", &saved)] {
                    let got = verdict(report);
                    let fine = got == case.full_scan
                        || (case.may_be_inconclusive() && got == "inconclusive");
                    if !fine {
                        wrong.push(format!(
                            "{} [{route}, {form}]: {got}, full scan {}",
                            case.label(order),
                            case.full_scan
                        ));
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "wrong verdicts:\n{}", wrong.join("\n"));
}

/// A sample whose quartiles coincide leaves accuracy unassessed, while a
/// scan of every value may assess it and lift the overall score: 10,000
/// nulls beside 29,992 zeros and 10,008 fives score 80 overall on
/// completeness alone, and 87.5 once accuracy joins at 100. A minimum of 85
/// must pass or be inconclusive, never fail (review of #1313).
#[test]
fn an_overall_score_the_full_scan_may_lift_is_never_failed_on_a_sample() {
    let mut wrong = Vec::new();
    let mut flat_samples = 0;
    for order in MORE_ORDERS {
        let case = Case {
            zeros: 29_992,
            hundreds: 0,
            base: 0,
            full_scan: "pass",
        };
        let mut csv_text = String::from("v\n");
        for value in case.values(order) {
            csv_text.push_str(&format!("{value}\n"));
        }
        // Reservoirs hold non-null values, so where the nulls sit is moot.
        csv_text.push_str(&"null\n".repeat(10_000));
        let csv = file(".csv", &csv_text);
        for engine in [
            EngineType::Auto,
            EngineType::Incremental,
            EngineType::Columnar,
        ] {
            let report = Profiler::new()
                .engine(engine)
                .quality_dimensions(vec![
                    QualityDimension::Completeness,
                    QualityDimension::Accuracy,
                ])
                .analyze_file(csv.path())
                .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
            let quality = report.quality.as_ref().expect("quality");
            if quality
                .metrics
                .dimension_score(QualityDimension::Accuracy)
                .is_none()
            {
                flat_samples += 1;
            }
            let result = QualityPolicy::new()
                .min_quality_score(85.0)
                .evaluate(&report)
                .unwrap();
            let got = serde_json::to_value(&result).unwrap()["verdict"]
                .as_str()
                .unwrap()
                .to_string();
            if got == "fail" {
                wrong.push(format!(
                    "{} [{engine:?}]: overall {:?}",
                    case.label(order),
                    quality.score()
                ));
            }
        }
    }
    assert!(flat_samples > 0, "no sample left accuracy unassessed");
    assert!(
        wrong.is_empty(),
        "failed on a sample:\n{}",
        wrong.join("\n")
    );
}

/// A sample that left accuracy open records it, and the overall score stays
/// open under the weights set after profiling: settled when accuracy weighs
/// nothing, open when it weighs anything (review of #1313).
#[test]
fn the_weights_set_after_profiling_decide_whether_an_open_accuracy_reaches_the_overall_score() {
    let flat = MORE_ORDERS
        .into_iter()
        .find_map(|order| {
            let case = Case {
                zeros: 29_992,
                hundreds: 0,
                base: 0,
                full_scan: "pass",
            };
            let mut csv_text = String::from("v\n");
            for value in case.values(order) {
                csv_text.push_str(&format!("{value}\n"));
            }
            csv_text.push_str(&"null\n".repeat(10_000));
            let csv = file(".csv", &csv_text);
            let report = Profiler::new()
                .quality_dimensions(vec![
                    QualityDimension::Completeness,
                    QualityDimension::Accuracy,
                ])
                .analyze_file(csv.path())
                .unwrap();
            let quality = report.quality.as_ref()?;
            quality
                .metrics
                .dimension_score(QualityDimension::Accuracy)
                .is_none()
                .then_some(report)
        })
        .expect("some sample leaves accuracy unassessed");
    let open = &flat
        .quality
        .as_ref()
        .unwrap()
        .recorded_score_bounds()
        .expect("sampled")
        .open_dimensions;
    assert_eq!(open, &["accuracy".to_string()]);

    let verdict = |accuracy: f64, min: f64| {
        let mut report = flat.clone();
        let quality = report.quality.as_mut().unwrap();
        quality.metrics.score_weights = QualityScoreWeights {
            accuracy,
            ..QualityScoreWeights::default()
        };
        let result = QualityPolicy::new()
            .min_quality_score(min)
            .evaluate(&report)
            .unwrap();
        serde_json::to_value(&result).unwrap()["verdict"]
            .as_str()
            .unwrap()
            .to_string()
    };
    // Weightless accuracy cannot move the score: 80 on both sides.
    assert_eq!(verdict(0.0, 75.0), "pass");
    // Weighted as completeness, a full scan may score 90: 85 is open.
    assert_eq!(verdict(0.25, 85.0), "inconclusive");
}

/// The report records the definition the IQR rule is under.
#[test]
fn reports_record_which_columns_the_outlier_test_runs_on() {
    let report = &routes(&CASES[0].values(ORDERS[0]))[0].1;
    let document = serde_json::to_value(report).unwrap();
    assert_eq!(
        document["metric_semantics"]["outlier_spread"],
        "nonzero_iqr"
    );
}
