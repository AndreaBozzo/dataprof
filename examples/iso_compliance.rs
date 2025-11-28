//! Example: Using ISO 8000/25012 Compliant Quality Metrics
//!
//! This example demonstrates how to use DataProf's ISO-compliant quality metrics
//! with configurable thresholds for different industry requirements.
//!
//! Run with: cargo run --example iso_compliance

use dataprof::analysis::MetricsCalculator;
use dataprof::core::config::IsoQualityThresholds;
use dataprof::types::{ColumnProfile, ColumnStats, DataType};
use std::collections::HashMap;

fn main() -> anyhow::Result<()> {
    println!("=== ISO 8000/25012 Compliant Quality Metrics Demo ===\n");

    // Sample data: customer ages with some issues
    let mut data = HashMap::new();
    data.insert(
        "age".to_string(),
        vec![
            "25", "30", "35", "28", "45", "33", "27", "250", // 250 is outlier
            "29", "31", "", "34", "32", "-5", // -5 is invalid, "" is null
            "40", "38", "36", "180", "42", "44", // 180 is outlier
        ]
        .iter()
        .map(|s| s.to_string())
        .collect(),
    );

    // Create column profile
    let profiles = vec![ColumnProfile {
        name: "age".to_string(),
        data_type: DataType::Integer,
        total_count: 20,
        null_count: 1,
        unique_count: Some(18),
        stats: ColumnStats::Numeric {
            min: -5.0,
            max: 250.0,
            mean: 45.5,
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

    // ===== Example 1: Default ISO Thresholds =====
    println!("--- Example 1: Default ISO Thresholds ---");
    let default_calculator = MetricsCalculator::new();
    let default_metrics = default_calculator.calculate_comprehensive_metrics(&data, &profiles)?;

    println!("Outlier ratio: {:.2}%", default_metrics.outlier_ratio);
    println!(
        "Missing values: {:.2}%",
        default_metrics.missing_values_ratio
    );
    println!("Config: IQR multiplier = 1.5 (ISO 25012 standard)\n");

    // ===== Example 2: Strict Thresholds (Finance/Healthcare) =====
    println!("--- Example 2: Strict Thresholds (Finance/Healthcare) ---");
    let strict_calculator = MetricsCalculator::strict();
    let strict_metrics = strict_calculator.calculate_comprehensive_metrics(&data, &profiles)?;

    println!("Outlier ratio: {:.2}%", strict_metrics.outlier_ratio);
    println!(
        "Missing values: {:.2}%",
        strict_metrics.missing_values_ratio
    );
    println!("Null columns (>30%): {:?}", strict_metrics.null_columns);
    println!("Config: Max null = 30%, Min samples = 10\n");

    // ===== Example 3: Lenient Thresholds (Exploratory/Marketing) =====
    println!("--- Example 3: Lenient Thresholds (Marketing/Exploratory) ---");
    let lenient_calculator = MetricsCalculator::lenient();
    let lenient_metrics = lenient_calculator.calculate_comprehensive_metrics(&data, &profiles)?;

    println!("Outlier ratio: {:.2}%", lenient_metrics.outlier_ratio);
    println!(
        "Missing values: {:.2}%",
        lenient_metrics.missing_values_ratio
    );
    println!("Config: IQR multiplier = 2.0 (more tolerant)\n");

    // ===== Example 4: Custom Thresholds =====
    println!("--- Example 4: Custom Thresholds ---");
    let custom_thresholds = IsoQualityThresholds {
        max_null_percentage: 25.0,
        null_report_threshold: 5.0,
        min_type_consistency: 99.0,
        duplicate_report_threshold: 2.0,
        high_cardinality_threshold: 97.0,
        outlier_iqr_multiplier: 1.8, // Custom IQR
        outlier_min_samples: 5,
        max_data_age_years: 3.0,    // Custom age threshold
        stale_data_threshold: 15.0, // Custom stale threshold
    };

    let custom_calculator = MetricsCalculator::with_thresholds(custom_thresholds);
    let custom_metrics = custom_calculator.calculate_comprehensive_metrics(&data, &profiles)?;

    println!("Outlier ratio: {:.2}%", custom_metrics.outlier_ratio);
    println!(
        "Missing values: {:.2}%",
        custom_metrics.missing_values_ratio
    );
    println!("Config: Custom IQR multiplier = 1.8\n");

    // ===== Comparison Table =====
    println!("--- Comparison Table ---");
    println!(
        "{:<20} {:>12} {:>12} {:>12} {:>12}",
        "Metric", "Default", "Strict", "Lenient", "Custom"
    );
    println!("{}", "-".repeat(72));
    println!(
        "{:<20} {:>11.2}% {:>11.2}% {:>11.2}% {:>11.2}%",
        "Outlier Ratio",
        default_metrics.outlier_ratio,
        strict_metrics.outlier_ratio,
        lenient_metrics.outlier_ratio,
        custom_metrics.outlier_ratio
    );
    println!(
        "{:<20} {:>11.2}% {:>11.2}% {:>11.2}% {:>11.2}%",
        "Missing Values",
        default_metrics.missing_values_ratio,
        strict_metrics.missing_values_ratio,
        lenient_metrics.missing_values_ratio,
        custom_metrics.missing_values_ratio
    );
    println!(
        "{:<20} {:>12} {:>12} {:>12} {:>12}",
        "Duplicate Rows",
        default_metrics.duplicate_rows,
        strict_metrics.duplicate_rows,
        lenient_metrics.duplicate_rows,
        custom_metrics.duplicate_rows
    );

    println!("\n=== ISO Compliance Benefits ===");
    println!("✓ Configurable thresholds per industry standards");
    println!("✓ IQR outlier detection (more robust than 3-sigma)");
    println!("✓ Follows ISO 8000-8, ISO 8000-61, ISO 8000-110, ISO 25012");
    println!("✓ Auditable and reproducible quality metrics");
    println!("✓ Support for strict (finance), default (general), lenient (exploratory) modes");

    Ok(())
}
