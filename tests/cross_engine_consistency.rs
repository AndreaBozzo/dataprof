//! Cross-engine consistency test.
//!
//! Profiles the same CSV through the standard CSV engine and Arrow CSV engine,
//! then asserts that numeric stats match within tolerance.

use std::io::Write;
use std::path::Path;

use dataprof::{
    ColumnStats, CsvParserConfig, DataType, MetricConfidence, QualityDimension, analyze_csv_file,
};
use dataprof::{EngineType, Profiler};
use tempfile::NamedTempFile;

/// 30k rows of *sorted* values — larger than the 10k per-column sample
/// reservoirs, so any engine that derives base statistics from its retained
/// sample (or samples a biased prefix) reports wildly wrong min/max/mean.
fn create_sorted_30k_csv() -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,value").unwrap();
    for i in 0..30_000 {
        writeln!(f, "{},{:.2}", i, i as f64 / 2.0).unwrap();
    }
    f.flush().unwrap();
    f
}

/// Base numeric statistics must be exact — computed over every value, not the
/// bounded sample — and identical across engines, even when the data is
/// sorted and larger than the sample capacity (#424).
#[test]
fn test_base_numeric_stats_exact_beyond_sample_capacity() {
    let csv = create_sorted_30k_csv();
    let path = csv.path();

    let std_report = analyze_csv_file(path, &CsvParserConfig::default())
        .expect("standard CSV analysis should succeed");
    let arrow_report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(path)
        .expect("Arrow CSV analysis should succeed");

    for (engine, report) in [("std", &std_report), ("arrow", &arrow_report)] {
        let id = report
            .column_profiles
            .iter()
            .find(|c| c.name == "id")
            .expect("id column");
        let ColumnStats::Numeric(n) = &id.stats else {
            panic!("id should have numeric stats");
        };
        // Exact analytical values for 0..=29999.
        assert_eq!(n.min, 0.0, "[{engine}] min must be exact");
        assert_eq!(n.max, 29_999.0, "[{engine}] max must be exact");
        assert!(
            (n.mean - 14_999.5).abs() < 1e-6,
            "[{engine}] mean must be exact, got {}",
            n.mean
        );
        // Order statistics come from a 10k sample out of 30k values, and the
        // report must say so.
        assert_eq!(
            n.is_approximate,
            Some(true),
            "[{engine}] sampled order statistics must be disclosed as approximate"
        );
        // Every value parsed: analyzed and clean is Some(0), never None —
        // and never a nonzero artifact of comparing against the sample.
        assert_eq!(id.invalid_count, Some(0), "[{engine}] invalid_count");
    }

    // And the two engines must agree with each other exactly on base stats.
    for name in ["id", "value"] {
        let get = |r: &dataprof::ProfileReport| {
            let col = r.column_profiles.iter().find(|c| c.name == name).unwrap();
            match &col.stats {
                ColumnStats::Numeric(n) => (n.min, n.max, n.mean, n.std_dev),
                other => panic!("'{name}' should be numeric, got {other:?}"),
            }
        };
        let (min1, max1, mean1, std1) = get(&std_report);
        let (min2, max2, mean2, std2) = get(&arrow_report);
        assert_eq!(min1, min2, "'{name}' min must match across engines");
        assert_eq!(max1, max2, "'{name}' max must match across engines");
        assert!(
            (mean1 - mean2).abs() < 1e-9,
            "'{name}' mean: {mean1} vs {mean2}"
        );
        assert!(
            (std1 - std2).abs() < 1e-6 * std1.abs().max(1.0),
            "'{name}' std_dev: {std1} vs {std2}"
        );
    }
}

/// A numeric column with an unparseable non-null value must disclose it via
/// `invalid_count` — identically through every engine — so the statistics
/// denominator is auditable instead of silently shrinking (#425).
#[test]
fn test_invalid_count_surfaces_unparseable_numeric_values() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "amount,label").unwrap();
    for i in 0..5 {
        writeln!(f, "{}.5,row{}", i, i).unwrap();
    }
    // Decimal-comma value: non-null, fails the numeric parse.
    writeln!(f, "\"12,50\",rowx").unwrap();
    writeln!(f, ",rownull").unwrap();
    f.flush().unwrap();

    let std_report = analyze_csv_file(f.path(), &CsvParserConfig::default()).unwrap();
    let arrow_report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(f.path())
        .unwrap();

    for (engine, report) in [("std", &std_report), ("arrow", &arrow_report)] {
        let amount = report
            .column_profiles
            .iter()
            .find(|c| c.name == "amount")
            .unwrap();
        assert_eq!(amount.data_type, DataType::Float, "[{engine}]");
        assert_eq!(amount.total_count, 7, "[{engine}]");
        assert_eq!(amount.null_count, 1, "[{engine}]");
        assert_eq!(
            amount.invalid_count,
            Some(1),
            "[{engine}] the unparseable value must be counted, not silently dropped"
        );

        let label = report
            .column_profiles
            .iter()
            .find(|c| c.name == "label")
            .unwrap();
        assert_eq!(
            label.invalid_count, None,
            "[{engine}] non-numeric columns are not checked: None, not 0"
        );
    }
}

/// Small data (fewer values than the sample capacity) keeps exact order
/// statistics and must not be flagged approximate.
#[test]
fn test_small_data_not_flagged_approximate() {
    let csv = create_test_csv();
    let report = analyze_csv_file(csv.path(), &CsvParserConfig::default()).unwrap();
    let age = report
        .column_profiles
        .iter()
        .find(|c| c.name == "age")
        .unwrap();
    let ColumnStats::Numeric(n) = &age.stats else {
        panic!("age should be numeric");
    };
    assert_eq!(
        n.is_approximate, None,
        "full-coverage stats are not approximate"
    );
    assert_eq!(n.min, 20.0);
    assert_eq!(n.max, 69.0);
}

fn create_test_csv() -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "name,age,salary,score").unwrap();
    for i in 0..100 {
        writeln!(
            f,
            "Person{},{},{:.2},{:.1}",
            i,
            20 + i % 50,
            30000.0 + i as f64 * 500.0,
            50.0 + (i % 50) as f64
        )
        .unwrap();
    }
    f.flush().unwrap();
    f
}

#[test]
fn test_standard_vs_arrow_csv_numeric_stats() {
    let csv = create_test_csv();
    let path = csv.path();

    // Standard CSV engine
    let std_report = analyze_csv_file(path, &CsvParserConfig::default())
        .expect("standard CSV analysis should succeed");

    // Arrow CSV engine via the unified Profiler API
    let arrow_report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(path)
        .expect("Arrow CSV analysis should succeed");

    // Same number of columns
    assert_eq!(
        std_report.column_profiles.len(),
        arrow_report.column_profiles.len(),
        "Both engines should detect the same number of columns"
    );

    for std_col in &std_report.column_profiles {
        let arrow_col = arrow_report
            .column_profiles
            .iter()
            .find(|c| c.name == std_col.name)
            .unwrap_or_else(|| panic!("Column '{}' missing from Arrow report", std_col.name));

        // Data type should match
        assert_eq!(
            std_col.data_type, arrow_col.data_type,
            "Type mismatch for column '{}'",
            std_col.name
        );

        // Row counts should match
        assert_eq!(
            std_col.total_count, arrow_col.total_count,
            "total_count mismatch for '{}'",
            std_col.name
        );
        assert_eq!(
            std_col.null_count, arrow_col.null_count,
            "null_count mismatch for '{}'",
            std_col.name
        );

        // Compare numeric stats within tolerance
        if let (ColumnStats::Numeric(n1), ColumnStats::Numeric(n2)) =
            (&std_col.stats, &arrow_col.stats)
        {
            let tol = 0.01;
            assert!(
                (n1.min - n2.min).abs() < tol,
                "'{}' min: {} vs {}",
                std_col.name,
                n1.min,
                n2.min
            );
            assert!(
                (n1.max - n2.max).abs() < tol,
                "'{}' max: {} vs {}",
                std_col.name,
                n1.max,
                n2.max
            );
            assert!(
                (n1.mean - n2.mean).abs() < tol,
                "'{}' mean: {} vs {}",
                std_col.name,
                n1.mean,
                n2.mean
            );
            assert!(
                (n1.std_dev - n2.std_dev).abs() < tol,
                "'{}' std_dev: {} vs {}",
                std_col.name,
                n1.std_dev,
                n2.std_dev
            );
            assert!(
                (n1.variance - n2.variance).abs() < 0.1,
                "'{}' variance: {} vs {}",
                std_col.name,
                n1.variance,
                n2.variance
            );

            // Optional stats: only compare when both are Some
            if let (Some(m1), Some(m2)) = (n1.median, n2.median) {
                assert!(
                    (m1 - m2).abs() < 0.1,
                    "'{}' median: {} vs {}",
                    std_col.name,
                    m1,
                    m2
                );
            }
            if let (Some(s1), Some(s2)) = (n1.skewness, n2.skewness) {
                assert!(
                    (s1 - s2).abs() < 0.1,
                    "'{}' skewness: {} vs {}",
                    std_col.name,
                    s1,
                    s2
                );
            }
            if let (Some(k1), Some(k2)) = (n1.kurtosis, n2.kurtosis) {
                assert!(
                    (k1 - k2).abs() < 0.1,
                    "'{}' kurtosis: {} vs {}",
                    std_col.name,
                    k1,
                    k2
                );
            }
        } else if matches!(std_col.data_type, DataType::Integer | DataType::Float) {
            panic!(
                "Column '{}' is {:?} but one engine produced non-Numeric stats: std={:?}, arrow={:?}",
                std_col.name, std_col.data_type, std_col.stats, arrow_col.stats
            );
        }
    }
}

#[test]
fn test_precision_quality_matches_standard_and_columnar_csv() {
    let mut csv = NamedTempFile::new().unwrap();
    writeln!(csv, "amount").unwrap();
    for value in ["1.25", "2.35", "3.45", "4.5"] {
        writeln!(csv, "{value}").unwrap();
    }
    csv.flush().unwrap();

    let standard = analyze_csv_file(csv.path(), &CsvParserConfig::default()).unwrap();
    let columnar = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(csv.path())
        .unwrap();
    let standard_precision = standard
        .quality
        .unwrap()
        .metrics
        .precision
        .expect("standard precision");
    let columnar_precision = columnar
        .quality
        .unwrap()
        .metrics
        .precision
        .expect("columnar precision");

    assert_eq!(
        standard_precision.numeric_values_checked,
        columnar_precision.numeric_values_checked
    );
    assert_eq!(
        standard_precision.inconsistent_precision_values,
        columnar_precision.inconsistent_precision_values
    );
    assert!(
        (standard_precision.decimal_places_consistency
            - columnar_precision.decimal_places_consistency)
            .abs()
            < 0.01
    );
}

#[test]
fn test_mixed_data_column_type_consistency() {
    // A column with one non-numeric value should be String in both engines
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,value,date_col").unwrap();
    writeln!(f, "1,100,2024-01-01").unwrap();
    writeln!(f, "2,200,2024-01-02").unwrap();
    writeln!(f, "3,N/A,2024-01-03").unwrap();
    writeln!(f, "4,400,2024-01-04").unwrap();
    writeln!(f, "5,500,2024-01-05").unwrap();
    f.flush().unwrap();

    let std_report = analyze_csv_file(f.path(), &CsvParserConfig::default())
        .expect("standard CSV should succeed");
    let arrow_report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(f.path())
        .expect("Arrow CSV should succeed");

    for std_col in &std_report.column_profiles {
        let arrow_col = arrow_report
            .column_profiles
            .iter()
            .find(|c| c.name == std_col.name)
            .unwrap_or_else(|| panic!("Column '{}' missing from Arrow report", std_col.name));

        assert_eq!(
            std_col.data_type, arrow_col.data_type,
            "Type mismatch for column '{}': std={:?}, arrow={:?}",
            std_col.name, std_col.data_type, arrow_col.data_type
        );
    }
}

#[test]
fn test_problematic_date_columns_stay_consistent_across_engines() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/test_datasets");

    for (file_name, column_name) in [
        ("sales_data_problematic.csv", "order_date"),
        ("sensor_data_outliers.csv", "timestamp"),
    ] {
        let path = root.join(file_name);

        let std_report =
            analyze_csv_file(&path, &CsvParserConfig::default()).unwrap_or_else(|err| {
                panic!("standard CSV analysis failed for {}: {}", file_name, err)
            });
        let arrow_report = Profiler::new()
            .engine(EngineType::Columnar)
            .analyze_file(&path)
            .unwrap_or_else(|err| panic!("Arrow CSV analysis failed for {}: {}", file_name, err));

        let std_col = std_report
            .column_profiles
            .iter()
            .find(|c| c.name == column_name)
            .unwrap_or_else(|| {
                panic!(
                    "Column '{}' missing from standard report for {}",
                    column_name, file_name
                )
            });
        let arrow_col = arrow_report
            .column_profiles
            .iter()
            .find(|c| c.name == column_name)
            .unwrap_or_else(|| {
                panic!(
                    "Column '{}' missing from Arrow report for {}",
                    column_name, file_name
                )
            });

        assert_eq!(std_col.data_type, DataType::Date);
        assert_eq!(
            std_col.data_type, arrow_col.data_type,
            "Type mismatch for '{}' in {}: std={:?}, arrow={:?}",
            column_name, file_name, std_col.data_type, arrow_col.data_type
        );
    }
}

/// Test that batch (ArrowProfiler) produces `MetricConfidence::Exact` and that
/// streaming (IncrementalProfiler) detects nulls correctly for a small dataset.
///
/// For small datasets where sample == total rows, both engines use the uniform
/// (non-bifurcated) path. The structural Mixed-confidence test is in
/// `test_streaming_bifurcation_with_large_dataset`.
#[test]
fn test_streaming_vs_batch_quality_confidence() {
    // CSV with known null pattern: empty fields in name/value columns
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,name,value").unwrap();
    for i in 0..200 {
        let name = if i % 10 == 0 { "" } else { "Alice" }; // 10% nulls in name
        let value = if i % 20 == 0 {
            ""
        } else {
            &format!("{}", i * 100)
        }; // 5% nulls in value
        writeln!(f, "{},{},{}", i, name, value).unwrap();
    }
    f.flush().unwrap();

    // Batch engine (ArrowProfiler) — full data, exact confidence
    let batch_report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(f.path())
        .expect("Batch analysis should succeed");

    // Streaming engine (IncrementalProfiler)
    let streaming_report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(f.path())
        .expect("Streaming analysis should succeed");

    // Both should have quality assessments
    let batch_quality = batch_report
        .quality
        .as_ref()
        .expect("Batch report should have quality");
    let streaming_quality = streaming_report
        .quality
        .as_ref()
        .expect("Streaming report should have quality");

    // Batch engine should produce Exact confidence
    assert!(
        matches!(batch_quality.confidence, MetricConfidence::Exact),
        "Batch engine should produce Exact confidence, got {:?}",
        batch_quality.confidence
    );

    // For 200 rows (< 10K reservoir), the streaming engine has sample == total.
    // The bifurcated path only triggers when sample < total, so both engines
    // use the uniform path here. The structural bifurcation test with Mixed
    // confidence is in test_streaming_bifurcation_with_large_dataset.

    // Streaming engine should detect the known null pattern in ColumnProfile.
    // The streaming null_count tracks empty strings as nulls, giving us exact
    // completeness metrics from global counters.
    let streaming_m = &streaming_quality.metrics;
    assert!(
        streaming_m.missing_values_ratio() > 0.0,
        "Streaming engine should detect missing values from empty CSV fields"
    );

    // key_uniqueness should be close (HLL ≤ 3% error for 200 rows)
    let batch_m = &batch_quality.metrics;
    assert!(
        (batch_m.key_uniqueness() - streaming_m.key_uniqueness()).abs() < 5.0,
        "key_uniqueness: batch={:.2} vs streaming={:.2}",
        batch_m.key_uniqueness(),
        streaming_m.key_uniqueness()
    );
}

/// Test bifurcation with a larger dataset where sample < total rows.
/// This forces the streaming engine to produce Mixed confidence.
#[test]
fn test_streaming_bifurcation_with_large_dataset() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,category,amount").unwrap();
    // Write enough rows that reservoir sample (10K) < total rows
    // This triggers the bifurcated path in ReportAssembler
    for i in 0..15_000 {
        let cat = if i % 3 == 0 {
            "A"
        } else if i % 3 == 1 {
            "B"
        } else {
            "C"
        };
        let amount = if i % 100 == 0 {
            ""
        } else {
            &format!("{:.2}", i as f64 * 1.5)
        };
        writeln!(f, "{},{},{}", i, cat, amount).unwrap();
    }
    f.flush().unwrap();

    let report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(f.path())
        .expect("Large streaming analysis should succeed");

    let quality = report
        .quality
        .as_ref()
        .expect("Should have quality assessment");

    // With 15K rows and 10K reservoir, sample < total → Mixed confidence
    match &quality.confidence {
        MetricConfidence::Mixed {
            exact_dimensions,
            sampled_dimensions,
            sample_size,
        } => {
            assert!(
                exact_dimensions.contains(&"completeness".to_string()),
                "completeness should be exact"
            );
            assert!(
                exact_dimensions.contains(&"key_uniqueness".to_string()),
                "key_uniqueness should be exact"
            );
            assert!(
                sampled_dimensions.contains(&"consistency".to_string()),
                "consistency should be sampled"
            );
            assert!(
                sampled_dimensions.contains(&"accuracy".to_string()),
                "accuracy should be sampled"
            );
            assert!(
                *sample_size < 15_000,
                "sample_size ({}) should be less than total rows (15000)",
                sample_size
            );
        }
        other => panic!(
            "Expected Mixed confidence for large streaming dataset, got {:?}",
            other
        ),
    }

    // Completeness should reflect the exact global counters
    // ~1% nulls in amount column (every 100th row), 0 nulls in id/category
    let m = &quality.metrics;
    assert!(
        m.missing_values_ratio() > 0.0,
        "Should detect some missing values"
    );
    assert!(
        m.missing_values_ratio() < 2.0,
        "Missing ratio should be small (~0.33%)"
    );
}

// -- Distinct-count provenance (#383) --

/// On a small dataset every column's distinct count is exact, and both engines
/// must say so: `unique_count_is_approximate == Some(false)`, never `None` or
/// `Some(true)`. An exact-looking count with no provenance is unsafe for key
/// checks, so the flag must be present and truthful.
#[test]
fn test_distinct_count_marked_exact_on_small_data_across_engines() {
    let csv = create_test_csv(); // 100 rows, well under the estimator threshold
    let path = csv.path();

    let std_report = analyze_csv_file(path, &CsvParserConfig::default()).unwrap();
    let arrow_report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(path)
        .unwrap();

    for std_col in &std_report.column_profiles {
        let arrow_col = arrow_report
            .column_profiles
            .iter()
            .find(|c| c.name == std_col.name)
            .unwrap();

        assert_eq!(
            std_col.unique_count_is_approximate,
            Some(false),
            "standard engine should mark '{}' exact",
            std_col.name
        );
        assert_eq!(
            arrow_col.unique_count_is_approximate,
            Some(false),
            "columnar engine should mark '{}' exact",
            std_col.name
        );
        assert_eq!(
            std_col.unique_count, arrow_col.unique_count,
            "exact unique_count should match across engines for '{}'",
            std_col.name
        );
    }
}

/// A high-cardinality column past the estimator threshold is a *legitimate*
/// approximation, not a semantic mismatch: both engines must flag it
/// `Some(true)` and land near the true distinct count.
#[test]
fn test_high_cardinality_marked_approximate_across_engines() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,category").unwrap();
    let rows = 20_000;
    for i in 0..rows {
        // `id` is unique per row (past the 10k exact threshold → HLL estimate);
        // `category` stays tiny.
        writeln!(f, "id-{i},{}", if i % 2 == 0 { "A" } else { "B" }).unwrap();
    }
    f.flush().unwrap();

    let std_report = analyze_csv_file(f.path(), &CsvParserConfig::default()).unwrap();
    let arrow_report = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(f.path())
        .unwrap();

    for report in [&std_report, &arrow_report] {
        let id_col = report
            .column_profiles
            .iter()
            .find(|c| c.name == "id")
            .expect("id column present");

        assert_eq!(
            id_col.unique_count_is_approximate,
            Some(true),
            "high-cardinality 'id' must be flagged approximate"
        );
        let estimate = id_col.unique_count.expect("id unique_count present");
        let error = (estimate as f64 - rows as f64).abs() / rows as f64;
        assert!(
            error < 0.05,
            "approximate estimate {estimate} should be near {rows} (error {error:.4})"
        );
    }
}

// -- Selective dimension computation --

#[test]
fn test_profiler_selective_dimensions_only_completeness() {
    let csv = create_test_csv();
    let report = Profiler::new()
        .quality_dimensions(vec![QualityDimension::Completeness])
        .analyze_file(csv.path())
        .unwrap();

    let quality = report.quality.expect("quality should be present");
    let m = &quality.metrics;

    assert!(m.completeness.is_some(), "completeness should be computed");
    assert!(m.consistency.is_none(), "consistency should be skipped");
    assert!(m.uniqueness.is_none(), "uniqueness should be skipped");
    assert!(m.accuracy.is_none(), "accuracy should be skipped");
    assert!(m.timeliness.is_none(), "timeliness should be skipped");
    assert!(m.validity.is_none(), "validity should be skipped");
    assert!(m.precision.is_none(), "precision should be skipped");

    // Score should re-normalize to completeness alone
    let score = m.overall_score();
    let completeness_score = m.completeness.as_ref().unwrap().complete_records_ratio;
    assert!(
        (score - completeness_score).abs() < 0.01,
        "score {score} should equal completeness {completeness_score}"
    );
}

#[test]
fn test_profiler_selective_dimensions_subset() {
    let csv = create_test_csv();
    let report = Profiler::new()
        .quality_dimensions(vec![
            QualityDimension::Completeness,
            QualityDimension::Uniqueness,
        ])
        .analyze_file(csv.path())
        .unwrap();

    let quality = report.quality.expect("quality should be present");
    let m = &quality.metrics;

    assert!(m.completeness.is_some());
    assert!(m.consistency.is_none());
    assert!(m.uniqueness.is_some());
    assert!(m.accuracy.is_none());
    assert!(m.timeliness.is_none());
    assert!(m.validity.is_none());
    assert!(m.precision.is_none());
}

#[test]
fn test_profiler_all_dimensions_default() {
    let csv = create_test_csv();
    // No quality_dimensions() call → all dimensions computed
    let report = Profiler::new().analyze_file(csv.path()).unwrap();

    let quality = report.quality.expect("quality should be present");
    let m = &quality.metrics;

    assert!(m.completeness.is_some());
    assert!(m.consistency.is_some());
    assert!(m.uniqueness.is_some());
    assert!(m.accuracy.is_some());
    assert!(m.timeliness.is_some());
    assert!(m.validity.is_some());
    assert!(m.precision.is_some());
}

/// Regression for #417: the columnar engine used to skip duplicate-row
/// detection whenever any column contained nulls (misaligned sample
/// reservoirs), silently dropping the duplicate component of the uniqueness
/// dimension and producing a different overall quality score than the
/// incremental engine for the same bytes.
#[test]
fn test_duplicate_rows_with_nulls_match_across_engines() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,name,value,city").unwrap();
    writeln!(f, "1,Alice,10.5,Rome").unwrap();
    writeln!(f, "2,,20.0,Milan").unwrap();
    writeln!(f, "2,,20.0,Milan").unwrap(); // duplicate row containing a null
    writeln!(f, "3,Bob,,Naples").unwrap();
    writeln!(f, "4,Alice,10.5,").unwrap();
    writeln!(f, "1,Alice,10.5,Rome").unwrap(); // duplicate of row 1
    f.flush().unwrap();

    let incremental = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(f.path())
        .expect("incremental analysis should succeed");
    let columnar = Profiler::new()
        .engine(EngineType::Columnar)
        .analyze_file(f.path())
        .expect("columnar analysis should succeed");

    let inc_uniq = incremental
        .quality
        .as_ref()
        .and_then(|q| q.metrics.uniqueness.as_ref())
        .expect("incremental uniqueness should be assessed");
    let col_uniq = columnar
        .quality
        .as_ref()
        .and_then(|q| q.metrics.uniqueness.as_ref())
        .expect("columnar uniqueness should be assessed");

    // Both engines must scan every row and find both duplicates, despite the
    // nulls that break per-column sample alignment.
    assert_eq!(inc_uniq.rows_checked, 6, "incremental rows_checked");
    assert_eq!(col_uniq.rows_checked, 6, "columnar rows_checked");
    assert_eq!(inc_uniq.duplicate_rows, 2, "incremental duplicate_rows");
    assert_eq!(col_uniq.duplicate_rows, 2, "columnar duplicate_rows");
    assert_eq!(
        inc_uniq.key_uniqueness, col_uniq.key_uniqueness,
        "key_uniqueness must match across engines"
    );

    // The contract that actually matters to users: identical overall score.
    let inc_score = incremental.quality_score().expect("incremental score");
    let col_score = columnar.quality_score().expect("columnar score");
    assert!(
        (inc_score - col_score).abs() < 0.01,
        "overall quality must not depend on the engine: incremental={inc_score} columnar={col_score}"
    );
}

/// Duplicate-row detection must also run on clean files (no nulls) — the
/// aligned-sample fallback already handled this, so it guards the new
/// full-stream tracker against regressing the easy case.
#[test]
fn test_duplicate_rows_without_nulls_match_across_engines() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "id,name").unwrap();
    writeln!(f, "1,Alice").unwrap();
    writeln!(f, "2,Bob").unwrap();
    writeln!(f, "2,Bob").unwrap();
    writeln!(f, "3,Carol").unwrap();
    f.flush().unwrap();

    for engine in [EngineType::Incremental, EngineType::Columnar] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(f.path())
            .expect("analysis should succeed");
        let uniq = report
            .quality
            .as_ref()
            .and_then(|q| q.metrics.uniqueness.as_ref())
            .expect("uniqueness should be assessed");
        assert_eq!(uniq.rows_checked, 4, "{engine:?} rows_checked");
        assert_eq!(uniq.duplicate_rows, 1, "{engine:?} duplicate_rows");
    }
}

/// Duplicate column names are a deterministic file property that no engine can
/// resolve; every CSV engine must reject them before profiling rather than
/// silently merging two columns into one profile (#381).
#[test]
fn test_duplicate_headers_rejected_across_csv_engines() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "x,x,y").unwrap();
    writeln!(f, "1,2,a").unwrap();
    writeln!(f, "3,4,b").unwrap();
    f.flush().unwrap();

    // Standard (simple) CSV engine.
    let err = analyze_csv_file(f.path(), &CsvParserConfig::default())
        .expect_err("standard engine must reject duplicate headers");
    assert_eq!(err.category(), "duplicate_column_name");
    let msg = err.to_string();
    assert!(msg.contains("'x'"), "names the offender: {msg}");
    assert!(!msg.contains('1'), "must not echo cell values: {msg}");

    // Auto (incremental) and Columnar (Arrow) engines: a clear, categorized error,
    // never buried under "All engines failed".
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let err = Profiler::new()
            .engine(engine)
            .analyze_file(f.path())
            .expect_err("engine must reject duplicate headers");
        assert_eq!(
            err.category(),
            "duplicate_column_name",
            "{engine:?} should surface the duplicate-column error directly"
        );
    }
}

/// The reason duplicates must be rejected: a rectangular source must yield one
/// profile per column, each with `total_count == rows` — never a merged column
/// whose count exceeds the row count.
#[test]
fn test_unique_headers_preserve_column_count_and_totals() {
    let mut f = NamedTempFile::new().unwrap();
    writeln!(f, "a,b,c").unwrap();
    writeln!(f, "1,2,3").unwrap();
    writeln!(f, "4,5,6").unwrap();
    f.flush().unwrap();

    for engine in [EngineType::Incremental, EngineType::Columnar] {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(f.path())
            .expect("unique headers should profile");
        assert_eq!(report.column_profiles.len(), 3, "{engine:?} column count");
        for col in &report.column_profiles {
            assert_eq!(col.total_count, 2, "{engine:?} {} total_count", col.name);
        }
    }
}
