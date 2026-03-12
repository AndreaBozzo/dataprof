//! Cross-engine consistency test.
//!
//! Profiles the same CSV through the standard CSV engine and Arrow CSV engine,
//! then asserts that numeric stats match within tolerance.

use std::io::Write;

use dataprof::parsers::csv::{CsvParserConfig, analyze_csv_file};
use dataprof::types::{ColumnStats, DataType};
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
