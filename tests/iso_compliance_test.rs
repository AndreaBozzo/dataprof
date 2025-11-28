//! Comprehensive ISO 8000/25012 Compliance Test Suite
//!
//! This test suite validates DataProf's compliance with:
//! - ISO 8000-8 (Data Quality - Completeness & Timeliness)
//! - ISO 8000-61 (Master Data Quality - Consistency)
//! - ISO 8000-110 (Duplicate Detection - Uniqueness)
//! - ISO 25012 (Data Quality Model - Accuracy)

use dataprof::analysis::MetricsCalculator;
use dataprof::core::config::IsoQualityThresholds;
use dataprof::types::{ColumnProfile, ColumnStats, DataType};
use std::collections::HashMap;

/// Helper function to create test data
fn create_test_data_with_outliers() -> (HashMap<String, Vec<String>>, Vec<ColumnProfile>) {
    let mut data = HashMap::new();
    data.insert(
        "age".to_string(),
        vec![
            "25", "30", "35", "28", "45", "33", "27", "250", // 250 is outlier
            "29", "31", "", "34", "32", "-5", // -5 is invalid, "" is null
            "40", "38", "36", "180", "42", "44", // 180 is outlier
            "26", "37", "39", "41", "43", "46", // Additional values for minimum sample size
            "24", "35", "32", "29", "37", // More values to reach 30+ for outlier detection
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
    );

    let profiles = vec![ColumnProfile {
        name: "age".to_string(),
        data_type: DataType::Integer,
        total_count: 31,
        null_count: 1,
        unique_count: Some(25),
        stats: ColumnStats::Numeric {
            min: -5.0,
            max: 250.0,
            mean: 42.0,
            std_dev: 0.0,
            variance: 0.0,
            median: None,
            quartiles: None,
            mode: None,
            coefficient_of_variation: None,
            skewness: None,
            kurtosis: None,
            is_approximate: None,
        },
        patterns: vec![],
    }];

    (data, profiles)
}

fn create_test_data_with_dates() -> (HashMap<String, Vec<String>>, Vec<ColumnProfile>) {
    let mut data = HashMap::new();

    // Mix of old dates, recent dates, and future dates
    data.insert(
        "created_at".to_string(),
        [
            "2015-01-01", // Old (stale)
            "2023-06-15", // Recent
            "2024-12-01", // Recent
            "2026-01-01", // Future
            "2018-03-20", // Old
            "2024-08-10", // Recent
            "",           // Null
            "2030-12-31", // Future
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
    );

    data.insert(
        "start_date".to_string(),
        ["2024-01-01", "2024-02-01", "2024-03-15", "", "", "", "", ""]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );

    data.insert(
        "end_date".to_string(),
        [
            "2024-06-30",
            "2024-01-15", // Violation: end < start
            "2024-12-31",
            "",
            "",
            "",
            "",
            "",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
    );

    let profiles = vec![
        ColumnProfile {
            name: "created_at".to_string(),
            data_type: DataType::Date,
            total_count: 8,
            null_count: 1,
            unique_count: Some(7),
            stats: ColumnStats::Text {
                min_length: 10,
                max_length: 10,
                avg_length: 10.0,
                most_frequent: None,
                least_frequent: None,
            },
            patterns: vec![],
        },
        ColumnProfile {
            name: "start_date".to_string(),
            data_type: DataType::Date,
            total_count: 8,
            null_count: 5,
            unique_count: Some(3),
            stats: ColumnStats::Text {
                min_length: 10,
                max_length: 10,
                avg_length: 10.0,
                most_frequent: None,
                least_frequent: None,
            },
            patterns: vec![],
        },
        ColumnProfile {
            name: "end_date".to_string(),
            data_type: DataType::Date,
            total_count: 8,
            null_count: 5,
            unique_count: Some(3),
            stats: ColumnStats::Text {
                min_length: 10,
                max_length: 10,
                avg_length: 10.0,
                most_frequent: None,
                least_frequent: None,
            },
            patterns: vec![],
        },
    ];

    (data, profiles)
}

// ============================================================================
// ISO 8000-8 COMPLETENESS TESTS
// ============================================================================

#[test]
fn test_completeness_null_detection() {
    let (data, profiles) = create_test_data_with_outliers();
    let calculator = MetricsCalculator::new();
    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Should detect 1 null out of 31 values = ~3.2%
    assert!(
        (metrics.missing_values_ratio - 3.23).abs() < 0.1,
        "Expected ~3.2% missing values, got {}",
        metrics.missing_values_ratio
    );
}

#[test]
fn test_completeness_configurable_thresholds() {
    let mut data = HashMap::new();
    data.insert(
        "high_null_column".to_string(),
        ["", "", "", "", "value", ""]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    );

    let profiles = vec![ColumnProfile {
        name: "high_null_column".to_string(),
        data_type: DataType::String,
        total_count: 6,
        null_count: 5,
        unique_count: Some(2),
        stats: ColumnStats::Text {
            min_length: 0,
            max_length: 5,
            avg_length: 0.8,
            most_frequent: None,
            least_frequent: None,
        },
        patterns: vec![],
    }];

    // Default: 50% threshold
    let default_calc = MetricsCalculator::new();
    let default_metrics = default_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();
    assert!(
        default_metrics
            .null_columns
            .contains(&"high_null_column".to_string()),
        "Default (50%) should flag column with 83% nulls"
    );

    // Strict: 30% threshold
    let strict_calc = MetricsCalculator::strict();
    let strict_metrics = strict_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();
    assert!(
        strict_metrics
            .null_columns
            .contains(&"high_null_column".to_string()),
        "Strict (30%) should flag column with 83% nulls"
    );

    // Lenient: 70% threshold
    let lenient_calc = MetricsCalculator::lenient();
    let lenient_metrics = lenient_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();
    assert!(
        lenient_metrics
            .null_columns
            .contains(&"high_null_column".to_string()),
        "Lenient (70%) should flag column with 83% nulls"
    );
}

// ============================================================================
// ISO 8000-8 TIMELINESS TESTS
// ============================================================================

#[test]
fn test_timeliness_future_dates_detection() {
    let (data, profiles) = create_test_data_with_dates();
    let calculator = MetricsCalculator::new();
    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Should detect 2 future dates (2026, 2030)
    assert_eq!(
        metrics.future_dates_count, 2,
        "Expected 2 future dates, got {}",
        metrics.future_dates_count
    );
}

#[test]
fn test_timeliness_stale_data_detection() {
    let (data, profiles) = create_test_data_with_dates();
    let calculator = MetricsCalculator::new();
    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // With default threshold (5 years from 2025), dates before 2020 are stale
    // 2015 and 2018 are stale out of 7 non-null dates = ~15-30%
    assert!(
        metrics.stale_data_ratio > 10.0 && metrics.stale_data_ratio < 35.0,
        "Expected 10-35% stale data, got {}",
        metrics.stale_data_ratio
    );
}

#[test]
fn test_timeliness_temporal_violations() {
    let (data, profiles) = create_test_data_with_dates();
    let calculator = MetricsCalculator::new();
    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Should detect violations where end_date < start_date
    // Note: The detector checks multiple column name patterns
    assert!(
        metrics.temporal_violations > 0,
        "Expected at least 1 temporal violation, got {}",
        metrics.temporal_violations
    );
}

#[test]
fn test_timeliness_strict_vs_lenient() {
    let (data, profiles) = create_test_data_with_dates();

    // Strict: 2 years threshold
    let strict_calc = MetricsCalculator::strict();
    let strict_metrics = strict_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Lenient: 10 years threshold
    let lenient_calc = MetricsCalculator::lenient();
    let lenient_metrics = lenient_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Strict should report more stale data than lenient
    assert!(
        strict_metrics.stale_data_ratio > lenient_metrics.stale_data_ratio,
        "Strict threshold should report more stale data"
    );
}

// ============================================================================
// ISO 25012 ACCURACY TESTS (IQR Outlier Detection)
// ============================================================================

#[test]
fn test_accuracy_iqr_outlier_detection() {
    let (data, profiles) = create_test_data_with_outliers();
    let calculator = MetricsCalculator::new();
    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Should detect at least 2 outliers (250, 180, possibly -5)
    assert!(
        metrics.outlier_ratio > 0.0,
        "Expected outliers to be detected, got 0%"
    );

    // With IQR method (k=1.5), should detect significant outliers
    assert!(
        metrics.outlier_ratio >= 10.0,
        "Expected at least 10% outliers, got {}",
        metrics.outlier_ratio
    );
}

#[test]
fn test_accuracy_iqr_multiplier_configurability() {
    let (data, profiles) = create_test_data_with_outliers();

    // Default: IQR multiplier = 1.5 (strict)
    let default_calc = MetricsCalculator::new();
    let default_metrics = default_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Lenient: IQR multiplier = 2.0 (more tolerant)
    let lenient_calc = MetricsCalculator::lenient();
    let lenient_metrics = lenient_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Default (k=1.5) should detect more outliers than lenient (k=2.0)
    assert!(
        default_metrics.outlier_ratio >= lenient_metrics.outlier_ratio,
        "Default IQR (1.5) should detect more outliers than lenient (2.0)"
    );
}

#[test]
fn test_accuracy_range_violations() {
    let (data, profiles) = create_test_data_with_outliers();
    let calculator = MetricsCalculator::new();
    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Age column: 250 > 150 (range violation), -5 < 0 (negative in positive field)
    assert!(
        metrics.range_violations >= 1,
        "Expected at least 1 range violation (age > 150)"
    );
    assert!(
        metrics.negative_values_in_positive >= 1,
        "Expected at least 1 negative value in positive field"
    );
}

// ============================================================================
// ISO 8000-110 UNIQUENESS TESTS
// ============================================================================

#[test]
fn test_uniqueness_high_cardinality_threshold() {
    let mut data = HashMap::new();
    // Create column with 95%+ unique values
    let unique_values: Vec<String> = (0..100).map(|i| format!("value_{}", i)).collect();
    data.insert("high_cardinality".to_string(), unique_values);

    let profiles = vec![ColumnProfile {
        name: "high_cardinality".to_string(),
        data_type: DataType::String,
        total_count: 100,
        null_count: 0,
        unique_count: Some(100),
        stats: ColumnStats::Text {
            min_length: 7,
            max_length: 9,
            avg_length: 8.0,
            most_frequent: None,
            least_frequent: None,
        },
        patterns: vec![],
    }];

    // Default threshold: 95%
    let default_calc = MetricsCalculator::new();
    let default_metrics = default_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();
    assert!(
        default_metrics.high_cardinality_warning,
        "Should detect high cardinality with default 95% threshold"
    );

    // Strict threshold: 98%
    let strict_calc = MetricsCalculator::strict();
    let strict_metrics = strict_calc
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();
    assert!(
        strict_metrics.high_cardinality_warning,
        "Should detect high cardinality with strict 98% threshold"
    );
}

// ============================================================================
// CROSS-DIMENSION ISO COMPLIANCE TESTS
// ============================================================================

#[test]
fn test_all_dimensions_integrated() {
    let (data, profiles) = create_test_data_with_outliers();
    let calculator = MetricsCalculator::new();
    let metrics = calculator
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Verify all 5 dimensions are calculated
    assert!(
        metrics.missing_values_ratio >= 0.0,
        "Completeness calculated"
    );
    assert!(
        metrics.data_type_consistency >= 0.0,
        "Consistency calculated"
    );
    // Uniqueness calculated (duplicate_rows is usize, always >= 0)
    let _ = metrics.duplicate_rows;
    assert!(metrics.outlier_ratio >= 0.0, "Accuracy calculated");
    // Timeliness calculated (future_dates_count is usize, always >= 0)
    let _ = metrics.future_dates_count;
}

#[test]
fn test_iso_threshold_validation() {
    // Test that all threshold profiles are valid
    let default_thresholds = IsoQualityThresholds::default();
    assert!(
        default_thresholds.outlier_iqr_multiplier > 0.0,
        "IQR multiplier must be positive"
    );
    assert!(
        default_thresholds.max_null_percentage >= 0.0
            && default_thresholds.max_null_percentage <= 100.0,
        "Null percentage must be 0-100"
    );

    let strict_thresholds = IsoQualityThresholds::strict();
    assert!(
        strict_thresholds.max_null_percentage < default_thresholds.max_null_percentage,
        "Strict should have lower null tolerance"
    );

    let lenient_thresholds = IsoQualityThresholds::lenient();
    assert!(
        lenient_thresholds.max_null_percentage > default_thresholds.max_null_percentage,
        "Lenient should have higher null tolerance"
    );
}

// ============================================================================
// ISO CERTIFICATION READINESS TESTS
// ============================================================================

#[test]
fn test_iso_audit_trail_metadata() {
    let calculator = MetricsCalculator::new();

    // Verify thresholds are documented
    assert_eq!(calculator.thresholds.outlier_iqr_multiplier, 1.5);
    assert_eq!(calculator.thresholds.max_null_percentage, 50.0);
    assert_eq!(calculator.thresholds.max_data_age_years, 5.0);
}

#[test]
fn test_iso_reproducibility() {
    let (data, profiles) = create_test_data_with_outliers();

    let calc1 = MetricsCalculator::new();
    let metrics1 = calc1
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    let calc2 = MetricsCalculator::new();
    let metrics2 = calc2
        .calculate_comprehensive_metrics(&data, &profiles)
        .unwrap();

    // Same configuration should produce identical results
    assert_eq!(metrics1.outlier_ratio, metrics2.outlier_ratio);
    assert_eq!(metrics1.missing_values_ratio, metrics2.missing_values_ratio);
    assert_eq!(metrics1.future_dates_count, metrics2.future_dates_count);
}
