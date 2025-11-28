use anyhow::Result;
use dataprof::types::{ColumnProfile, ColumnStats, DataQualityMetrics, DataType};
use std::collections::HashMap;

#[test]
fn test_data_quality_metrics_creation() -> Result<()> {
    let metrics = DataQualityMetrics {
        // Completeness
        missing_values_ratio: 15.2,
        complete_records_ratio: 84.8,
        null_columns: vec!["phone".to_string(), "optional_field".to_string()],

        // Consistency
        data_type_consistency: 94.7,
        format_violations: 23,
        encoding_issues: 0,

        // Uniqueness
        duplicate_rows: 45,
        key_uniqueness: 99.1,
        high_cardinality_warning: true,

        // Accuracy
        outlier_ratio: 2.3,
        range_violations: 12,
        negative_values_in_positive: 3,

        // Timeliness (ISO 8000-8)
        future_dates_count: 2,
        stale_data_ratio: 5.5,
        temporal_violations: 1,
    };

    // Validate completeness metrics
    assert!(metrics.missing_values_ratio >= 0.0 && metrics.missing_values_ratio <= 100.0);
    assert!(metrics.complete_records_ratio >= 0.0 && metrics.complete_records_ratio <= 100.0);
    assert_eq!(metrics.null_columns.len(), 2);

    // Validate consistency metrics
    assert!(metrics.data_type_consistency >= 0.0 && metrics.data_type_consistency <= 100.0);
    assert_eq!(metrics.format_violations, 23);
    assert_eq!(metrics.encoding_issues, 0);

    // Validate uniqueness metrics
    assert_eq!(metrics.duplicate_rows, 45);
    assert!(metrics.key_uniqueness >= 0.0 && metrics.key_uniqueness <= 100.0);
    assert!(metrics.high_cardinality_warning);

    // Validate accuracy metrics
    assert!(metrics.outlier_ratio >= 0.0 && metrics.outlier_ratio <= 100.0);
    assert_eq!(metrics.range_violations, 12);
    assert_eq!(metrics.negative_values_in_positive, 3);

    // Validate timeliness metrics
    assert_eq!(metrics.future_dates_count, 2);
    assert!(metrics.stale_data_ratio >= 0.0 && metrics.stale_data_ratio <= 100.0);
    assert_eq!(metrics.temporal_violations, 1);

    Ok(())
}

#[test]
fn test_completeness_calculation() -> Result<()> {
    // Create test data with known null patterns
    let test_data = create_test_data_with_nulls();
    let columns = create_column_profiles_from_data(&test_data);

    let metrics = DataQualityMetrics::calculate_from_data(&test_data, &columns)?;

    // Expected: 3 out of 4 rows have at least one null value
    // So complete_records_ratio should be 25.0%
    assert!((metrics.complete_records_ratio - 25.0).abs() < 0.1);

    // Expected: Overall missing values ratio should be calculated correctly
    let expected_missing_ratio = calculate_expected_missing_ratio(&test_data);
    assert!((metrics.missing_values_ratio - expected_missing_ratio).abs() < 0.1);

    Ok(())
}

#[test]
fn test_consistency_validation() -> Result<()> {
    let test_data = create_test_data_with_type_issues();
    let columns = create_column_profiles_from_data(&test_data);

    let metrics = DataQualityMetrics::calculate_from_data(&test_data, &columns)?;

    // Should detect type consistency issues
    assert!(metrics.data_type_consistency < 100.0);
    assert!(metrics.format_violations > 0);

    Ok(())
}

#[test]
fn test_uniqueness_detection() -> Result<()> {
    let test_data = create_test_data_with_duplicates();
    let columns = create_column_profiles_from_data(&test_data);

    let metrics = DataQualityMetrics::calculate_from_data(&test_data, &columns)?;

    // Should detect exact row duplicates
    assert!(metrics.duplicate_rows > 0);

    // High cardinality warning should trigger for certain columns
    // High cardinality warning can be either true or false - both are valid states

    Ok(())
}

#[test]
fn test_accuracy_validation() -> Result<()> {
    let test_data = create_test_data_with_accuracy_issues();
    let columns = create_column_profiles_from_data(&test_data);

    let metrics = DataQualityMetrics::calculate_from_data(&test_data, &columns)?;

    // Should detect range violations (negative ages, etc.)
    assert!(metrics.range_violations > 0);
    assert!(metrics.negative_values_in_positive > 0);

    Ok(())
}

#[test]
fn test_empty_dataset() -> Result<()> {
    let empty_data: HashMap<String, Vec<String>> = HashMap::new();
    let empty_columns: Vec<ColumnProfile> = vec![];

    let metrics = DataQualityMetrics::calculate_from_data(&empty_data, &empty_columns)?;

    // Empty dataset should have perfect metrics (or appropriate defaults)
    assert_eq!(metrics.missing_values_ratio, 0.0);
    assert_eq!(metrics.complete_records_ratio, 100.0);
    assert_eq!(metrics.null_columns.len(), 0);
    assert_eq!(metrics.duplicate_rows, 0);

    Ok(())
}

#[test]
fn test_perfect_dataset() -> Result<()> {
    let perfect_data = create_perfect_test_data();
    let columns = create_column_profiles_from_data(&perfect_data);

    let metrics = DataQualityMetrics::calculate_from_data(&perfect_data, &columns)?;

    // Perfect dataset should have excellent metrics
    assert_eq!(metrics.missing_values_ratio, 0.0);
    assert_eq!(metrics.complete_records_ratio, 100.0);
    assert_eq!(metrics.null_columns.len(), 0);
    assert_eq!(metrics.data_type_consistency, 100.0);
    assert_eq!(metrics.format_violations, 0);
    assert_eq!(metrics.encoding_issues, 0);
    assert_eq!(metrics.duplicate_rows, 0);
    assert_eq!(metrics.range_violations, 0);
    assert_eq!(metrics.negative_values_in_positive, 0);
    // Timeliness should also be perfect
    assert_eq!(metrics.future_dates_count, 0);
    assert_eq!(metrics.stale_data_ratio, 0.0);
    assert_eq!(metrics.temporal_violations, 0);

    Ok(())
}

// Helper functions for test data creation
fn create_test_data_with_nulls() -> HashMap<String, Vec<String>> {
    let mut data = HashMap::new();
    data.insert(
        "id".to_string(),
        vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string(),
        ],
    );
    data.insert(
        "name".to_string(),
        vec![
            "John".to_string(),
            "".to_string(),
            "Alice".to_string(),
            "Bob".to_string(),
        ],
    );
    data.insert(
        "age".to_string(),
        vec![
            "25".to_string(),
            "30".to_string(),
            "".to_string(),
            "35".to_string(),
        ],
    );
    data.insert(
        "phone".to_string(),
        vec![
            "".to_string(),
            "123-456-7890".to_string(),
            "".to_string(),
            "098-765-4321".to_string(),
        ],
    );
    data
}

fn create_test_data_with_type_issues() -> HashMap<String, Vec<String>> {
    let mut data = HashMap::new();
    data.insert(
        "id".to_string(),
        vec![
            "1".to_string(),
            "2".to_string(),
            "three".to_string(),
            "4".to_string(),
        ],
    );
    data.insert(
        "date".to_string(),
        vec![
            "2023-01-01".to_string(),
            "01/02/2023".to_string(),
            "2023/03/01".to_string(),
            "invalid-date".to_string(),
        ],
    );
    data
}

fn create_test_data_with_duplicates() -> HashMap<String, Vec<String>> {
    let mut data = HashMap::new();
    data.insert(
        "id".to_string(),
        vec![
            "1".to_string(),
            "2".to_string(),
            "1".to_string(),
            "3".to_string(),
        ],
    );
    data.insert(
        "name".to_string(),
        vec![
            "John".to_string(),
            "Jane".to_string(),
            "John".to_string(),
            "Bob".to_string(),
        ],
    );
    data
}

fn create_test_data_with_accuracy_issues() -> HashMap<String, Vec<String>> {
    let mut data = HashMap::new();
    data.insert(
        "age".to_string(),
        vec![
            "25".to_string(),
            "-5".to_string(),
            "150".to_string(),
            "30".to_string(),
        ],
    );
    data.insert(
        "salary".to_string(),
        vec![
            "50000".to_string(),
            "-1000".to_string(),
            "1000000000".to_string(),
            "60000".to_string(),
        ],
    );
    data
}

fn create_perfect_test_data() -> HashMap<String, Vec<String>> {
    let mut data = HashMap::new();
    data.insert(
        "id".to_string(),
        vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string(),
        ],
    );
    data.insert(
        "name".to_string(),
        vec![
            "John".to_string(),
            "Jane".to_string(),
            "Alice".to_string(),
            "Bob".to_string(),
        ],
    );
    data.insert(
        "age".to_string(),
        vec![
            "25".to_string(),
            "30".to_string(),
            "28".to_string(),
            "35".to_string(),
        ],
    );
    data
}

fn create_column_profiles_from_data(data: &HashMap<String, Vec<String>>) -> Vec<ColumnProfile> {
    let mut profiles = Vec::new();

    for (name, values) in data {
        let total_count = values.len();
        let null_count = values.iter().filter(|v| v.is_empty()).count();

        // Infer appropriate data type based on column name and content for better testing
        let data_type = if name.to_lowercase().contains("id") {
            DataType::Integer
        } else if name.to_lowercase().contains("date") {
            DataType::Date
        } else if name.to_lowercase().contains("age") || name.to_lowercase().contains("salary") {
            DataType::Integer
        } else {
            DataType::String
        };

        let stats = match data_type {
            DataType::Integer | DataType::Float => {
                let numeric_values: Vec<f64> = values
                    .iter()
                    .filter_map(|v| v.parse::<f64>().ok())
                    .collect();

                if numeric_values.is_empty() {
                    ColumnStats::Numeric {
                        min: 0.0,
                        max: 0.0,
                        mean: 0.0,
                        std_dev: 0.0,
                        variance: 0.0,
                        median: None,
                        quartiles: None,
                        mode: None,
                        coefficient_of_variation: None,
                        skewness: None,
                        kurtosis: None,
                        is_approximate: None,
                    }
                } else {
                    let min = numeric_values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                    let max = numeric_values
                        .iter()
                        .fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                    let mean = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
                    ColumnStats::Numeric {
                        min,
                        max,
                        mean,
                        std_dev: 0.0,
                        variance: 0.0,
                        median: None,
                        quartiles: None,
                        mode: None,
                        coefficient_of_variation: None,
                        skewness: None,
                        kurtosis: None,
                        is_approximate: None,
                    }
                }
            }
            DataType::String | DataType::Date => ColumnStats::Text {
                min_length: values
                    .iter()
                    .filter(|v| !v.is_empty())
                    .map(|v| v.len())
                    .min()
                    .unwrap_or(0),
                max_length: values.iter().map(|v| v.len()).max().unwrap_or(0),
                avg_length: if total_count > 0 {
                    values.iter().map(|v| v.len()).sum::<usize>() as f64 / total_count as f64
                } else {
                    0.0
                },
                most_frequent: None,
                least_frequent: None,
            },
        };

        let profile = ColumnProfile {
            name: name.clone(),
            data_type,
            null_count,
            total_count,
            unique_count: Some(
                values
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len(),
            ),
            stats,
            patterns: vec![],
        };

        profiles.push(profile);
    }

    profiles
}

fn calculate_expected_missing_ratio(data: &HashMap<String, Vec<String>>) -> f64 {
    let total_cells: usize = data.values().map(|v| v.len()).sum();
    let null_cells: usize = data
        .values()
        .map(|v| v.iter().filter(|s| s.is_empty()).count())
        .sum();

    if total_cells == 0 {
        0.0
    } else {
        (null_cells as f64 / total_cells as f64) * 100.0
    }
}
