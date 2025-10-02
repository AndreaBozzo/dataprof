/// Scientific validation tests for metric calculations
/// Tests verify correctness of statistical methods
use dataprof::analysis::MetricsCalculator;
use dataprof::types::{ColumnProfile, ColumnStats, DataQualityMetrics, DataType};
use std::collections::HashMap;

#[test]
fn test_percentile_calculation_correctness() {
    // Test percentile calculation against known values
    let calculator = MetricsCalculator::new();

    // Create test data with known quartiles
    let mut data = HashMap::new();
    // Dataset: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    // Q1 should be 3.25, Q3 should be 7.75 (using Type 7 interpolation)
    data.insert(
        "numbers".to_string(),
        vec!["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );

    let profiles = vec![ColumnProfile {
        name: "numbers".to_string(),
        data_type: DataType::Integer,
        null_count: 0,
        total_count: 10,
        unique_count: Some(10),
        stats: ColumnStats::Numeric {
            min: 1.0,
            max: 10.0,
            mean: 5.5,
        },
        patterns: vec![],
    }];

    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // With proper percentile calculation, outlier detection should be more accurate
    // IQR = 7.75 - 3.25 = 4.5
    // Lower bound = 3.25 - 1.5*4.5 = -3.5
    // Upper bound = 7.75 + 1.5*4.5 = 14.5
    // No values should be outliers in this uniform distribution
    assert_eq!(
        metrics.outlier_ratio, 0.0,
        "Uniform distribution should have no outliers"
    );
}

#[test]
fn test_outlier_detection_with_actual_outliers() {
    let calculator = MetricsCalculator::new();

    let mut data = HashMap::new();
    // Dataset with clear outlier (needs 30+ samples for statistical validity)
    // Normal values: 1-30, plus one clear outlier: 1000
    let mut values: Vec<String> = (1..=30).map(|n| n.to_string()).collect();
    values.push("1000".to_string());

    data.insert("numbers".to_string(), values);

    let profiles = vec![ColumnProfile {
        name: "numbers".to_string(),
        data_type: DataType::Integer,
        null_count: 0,
        total_count: 31,
        unique_count: Some(31),
        stats: ColumnStats::Numeric {
            min: 1.0,
            max: 1000.0,
            mean: 46.5,
        },
        patterns: vec![],
    }];

    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // 1000 should be detected as an outlier with proper statistical validation
    assert!(
        metrics.outlier_ratio > 0.0,
        "Should detect the outlier value 1000"
    );
    assert!(
        metrics.outlier_ratio <= 5.0,
        "Only one value out of 31 should be outlier (~3.2%)"
    );
}

#[test]
fn test_overall_score_normalization() {
    // Test that overall score is properly normalized to 0-100
    let metrics = DataQualityMetrics {
        missing_values_ratio: 5.0,    // 5% missing
        complete_records_ratio: 95.0, // 95% complete
        null_columns: vec![],
        data_type_consistency: 98.0, // 98% consistent
        format_violations: 0,
        encoding_issues: 0,
        duplicate_rows: 0,
        key_uniqueness: 100.0, // 100% unique
        high_cardinality_warning: false,
        outlier_ratio: 2.0, // 2% outliers
        range_violations: 0,
        negative_values_in_positive: 0,
        future_dates_count: 0,
        stale_data_ratio: 0.0, // 0% stale
        temporal_violations: 0,
    };

    let overall_score = metrics.overall_score();

    // Score calculation:
    // Completeness: 95 * 0.3 = 28.5
    // Consistency: 98 * 0.25 = 24.5
    // Uniqueness: 100 * 0.2 = 20.0
    // Accuracy: (100 - 2) * 0.15 = 14.7
    // Timeliness: (100 - 0) * 0.1 = 10.0
    // Total: 97.7

    assert!(
        (overall_score - 97.7).abs() < 0.1,
        "Overall score should be ~97.7, got {}",
        overall_score
    );
    assert!(
        overall_score >= 0.0 && overall_score <= 100.0,
        "Score must be in 0-100 range"
    );
}

#[test]
fn test_statistical_validation_minimum_sample() {
    // Test that small samples are properly validated
    let validation = MetricsCalculator::validate_sample_size(5, "outlier_detection");

    assert!(
        !validation.sufficient_sample,
        "5 samples should be insufficient for outlier detection"
    );
    assert_eq!(
        validation.min_sample_size, 30,
        "Outlier detection requires 30 samples"
    );
    assert_eq!(validation.actual_sample_size, 5);
    assert_eq!(
        validation.confidence_level, 0.0,
        "Low confidence for small sample"
    );
}

#[test]
fn test_format_violations_accurate_counting() {
    let calculator = MetricsCalculator::new();

    let mut data = HashMap::new();
    // Mixed decimal formats: 3 with dot, 2 with comma (2 violations expected)
    data.insert(
        "prices".to_string(),
        vec!["10.50", "20.75", "30.25", "40,50", "50,75"]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );

    let profiles = vec![ColumnProfile {
        name: "prices".to_string(),
        data_type: DataType::Float,
        null_count: 0,
        total_count: 5,
        unique_count: Some(5),
        stats: ColumnStats::Numeric {
            min: 10.5,
            max: 50.75,
            mean: 30.35,
        },
        patterns: vec![],
    }];

    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Should count the minority format (2 comma formats) as violations
    assert_eq!(
        metrics.format_violations, 2,
        "Should count minority decimal format as violations"
    );
}

#[test]
fn test_complete_records_ratio_correctness() {
    let calculator = MetricsCalculator::new();

    let mut data = HashMap::new();
    data.insert(
        "col1".to_string(),
        vec!["a".to_string(), "b".to_string(), "".to_string()],
    );
    data.insert(
        "col2".to_string(),
        vec!["1".to_string(), "2".to_string(), "3".to_string()],
    );

    let profiles = vec![
        ColumnProfile {
            name: "col1".to_string(),
            data_type: DataType::String,
            null_count: 1,
            total_count: 3,
            unique_count: Some(2),
            stats: ColumnStats::Text {
                min_length: 1,
                max_length: 1,
                avg_length: 1.0,
            },
            patterns: vec![],
        },
        ColumnProfile {
            name: "col2".to_string(),
            data_type: DataType::Integer,
            null_count: 0,
            total_count: 3,
            unique_count: Some(3),
            stats: ColumnStats::Numeric {
                min: 1.0,
                max: 3.0,
                mean: 2.0,
            },
            patterns: vec![],
        },
    ];

    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Only 2 out of 3 rows are complete (row 3 has empty value in col1)
    let expected_ratio = (2.0 / 3.0) * 100.0; // 66.67%
    assert!(
        (metrics.complete_records_ratio - expected_ratio).abs() < 0.1,
        "Complete records ratio should be ~66.67%, got {}",
        metrics.complete_records_ratio
    );
}
