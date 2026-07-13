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
