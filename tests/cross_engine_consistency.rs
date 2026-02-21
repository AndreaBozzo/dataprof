//! Cross-engine consistency test.
//!
//! Profiles the same CSV through the standard CSV engine and Arrow CSV engine,
//! then asserts that numeric stats match within tolerance.

use std::io::Write;

use dataprof::engines::columnar::ArrowProfiler;
use dataprof::parsers::csv::analyze_csv_robust;
use dataprof::types::{ColumnStats, DataType};
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
    let std_report = analyze_csv_robust(path).expect("standard CSV analysis should succeed");

    // Arrow CSV engine
    let arrow_profiler = ArrowProfiler::new();
    let arrow_report = arrow_profiler
        .analyze_csv_file(path)
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
        if let (
            ColumnStats::Numeric {
                min: min1,
                max: max1,
                mean: mean1,
                std_dev: sd1,
                variance: var1,
                median: med1,
                skewness: sk1,
                kurtosis: ku1,
                ..
            },
            ColumnStats::Numeric {
                min: min2,
                max: max2,
                mean: mean2,
                std_dev: sd2,
                variance: var2,
                median: med2,
                skewness: sk2,
                kurtosis: ku2,
                ..
            },
        ) = (&std_col.stats, &arrow_col.stats)
        {
            let tol = 0.01;
            assert!(
                (min1 - min2).abs() < tol,
                "'{}' min: {} vs {}",
                std_col.name,
                min1,
                min2
            );
            assert!(
                (max1 - max2).abs() < tol,
                "'{}' max: {} vs {}",
                std_col.name,
                max1,
                max2
            );
            assert!(
                (mean1 - mean2).abs() < tol,
                "'{}' mean: {} vs {}",
                std_col.name,
                mean1,
                mean2
            );
            assert!(
                (sd1 - sd2).abs() < tol,
                "'{}' std_dev: {} vs {}",
                std_col.name,
                sd1,
                sd2
            );
            assert!(
                (var1 - var2).abs() < 0.1,
                "'{}' variance: {} vs {}",
                std_col.name,
                var1,
                var2
            );

            // Optional stats: only compare when both are Some
            if let (Some(m1), Some(m2)) = (med1, med2) {
                assert!(
                    (m1 - m2).abs() < 0.1,
                    "'{}' median: {} vs {}",
                    std_col.name,
                    m1,
                    m2
                );
            }
            if let (Some(s1), Some(s2)) = (sk1, sk2) {
                assert!(
                    (s1 - s2).abs() < 0.1,
                    "'{}' skewness: {} vs {}",
                    std_col.name,
                    s1,
                    s2
                );
            }
            if let (Some(k1), Some(k2)) = (ku1, ku2) {
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
