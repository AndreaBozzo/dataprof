//! Example: Using the ConfigBuilder for fluent configuration
//!
//! This example demonstrates how to use the DataprofConfigBuilder
//! to create configurations with a clear, fluent API.
//!
//! Run with: cargo run --example config_builder_example

use dataprof::core::config::{DataprofConfigBuilder, IsoQualityThresholds};

fn main() -> anyhow::Result<()> {
    println!("=== DataProf Configuration Builder Examples ===\n");

    // Example 1: Basic configuration with defaults
    println!("--- Example 1: Basic Configuration ---");
    let config = DataprofConfigBuilder::new().build()?;
    println!("✓ Created default configuration");
    println!("  Output format: {}", config.output.default_format);
    println!("  Engine: {}", config.engine.default_engine);
    println!();

    // Example 2: Custom configuration for JSON output
    println!("--- Example 2: JSON Output Configuration ---");
    let config = DataprofConfigBuilder::new()
        .output_format("json")
        .verbosity(2)
        .colored(false)
        .show_progress(false)
        .build()?;
    println!("✓ Created JSON configuration");
    println!("  Output format: {}", config.output.default_format);
    println!("  Verbosity: {}", config.output.verbosity);
    println!("  Colored: {}", config.output.colored);
    println!();

    // Example 3: Strict quality profile for finance
    println!("--- Example 3: Strict Quality Profile (Finance/Healthcare) ---");
    let config = DataprofConfigBuilder::new()
        .iso_quality_profile_strict()
        .quality_enabled(true)
        .build()?;
    println!("✓ Created strict quality configuration");
    println!("  Quality enabled: {}", config.quality.enabled);
    println!(
        "  Max null percentage: {}%",
        config.quality.iso_thresholds.max_null_percentage
    );
    println!(
        "  Null report threshold: {}%",
        config.quality.iso_thresholds.null_report_threshold
    );
    println!(
        "  Duplicate threshold: {}%",
        config.quality.iso_thresholds.duplicate_report_threshold
    );
    println!(
        "  Type consistency: {}%",
        config.quality.iso_thresholds.min_type_consistency
    );
    println!();

    // Example 4: Lenient quality profile for exploratory analysis
    println!("--- Example 4: Lenient Quality Profile (Exploratory/Marketing) ---");
    let config = DataprofConfigBuilder::new()
        .iso_quality_profile_lenient()
        .build()?;
    println!("✓ Created lenient quality configuration");
    println!(
        "  Max null percentage: {}%",
        config.quality.iso_thresholds.max_null_percentage
    );
    println!(
        "  Null report threshold: {}%",
        config.quality.iso_thresholds.null_report_threshold
    );
    println!(
        "  Max data age: {} years",
        config.quality.iso_thresholds.max_data_age_years
    );
    println!();

    // Example 5: Custom ISO thresholds
    println!("--- Example 5: Custom ISO Thresholds ---");
    let custom_thresholds = IsoQualityThresholds {
        max_null_percentage: 40.0,
        null_report_threshold: 8.0,
        min_type_consistency: 96.0,
        duplicate_report_threshold: 3.0,
        high_cardinality_threshold: 96.0,
        outlier_iqr_multiplier: 2.0,
        outlier_min_samples: 5,
        max_data_age_years: 3.0,
        stale_data_threshold: 15.0,
    };
    let config = DataprofConfigBuilder::new()
        .iso_quality_thresholds(custom_thresholds)
        .build()?;
    println!("✓ Created custom quality configuration");
    println!(
        "  Max null percentage: {}%",
        config.quality.iso_thresholds.max_null_percentage
    );
    println!(
        "  Type consistency: {}%",
        config.quality.iso_thresholds.min_type_consistency
    );
    println!();

    // Example 6: Engine configuration for streaming
    println!("--- Example 6: Streaming Engine Configuration ---");
    let config = DataprofConfigBuilder::new()
        .engine("streaming")
        .chunk_size(16384)
        .parallel(true)
        .max_concurrent(8)
        .max_memory_mb(256)
        .auto_streaming_threshold_mb(50.0)
        .build()?;
    println!("✓ Created streaming engine configuration");
    println!("  Engine: {}", config.engine.default_engine);
    println!("  Chunk size: {:?}", config.engine.default_chunk_size);
    println!("  Parallel: {}", config.engine.parallel);
    println!("  Max concurrent: {}", config.engine.max_concurrent);
    println!("  Max memory: {} MB", config.engine.memory.max_usage_mb);
    println!();

    // Example 7: CI/CD preset configuration
    println!("--- Example 7: CI/CD Preset ---");
    let config = DataprofConfigBuilder::ci_preset().build()?;
    println!("✓ Created CI/CD configuration");
    println!("  Output format: {}", config.output.default_format);
    println!("  Colored: {}", config.output.colored);
    println!("  Progress: {}", config.output.show_progress);
    println!("  Verbosity: {}", config.output.verbosity);
    println!();

    // Example 8: Interactive preset configuration
    println!("--- Example 8: Interactive Preset ---");
    let config = DataprofConfigBuilder::interactive_preset().build()?;
    println!("✓ Created interactive configuration");
    println!("  Output format: {}", config.output.default_format);
    println!("  Colored: {}", config.output.colored);
    println!("  Progress: {}", config.output.show_progress);
    println!();

    // Example 9: Production quality preset
    println!("--- Example 9: Production Quality Preset ---");
    let config = DataprofConfigBuilder::production_quality_preset().build()?;
    println!("✓ Created production quality configuration");
    println!("  Quality enabled: {}", config.quality.enabled);
    println!(
        "  Max null percentage: {}%",
        config.quality.iso_thresholds.max_null_percentage
    );
    println!("  Max memory: {} MB", config.engine.memory.max_usage_mb);
    println!();

    // Example 10: Validation - invalid configuration
    println!("--- Example 10: Validation Error Handling ---");
    let result = DataprofConfigBuilder::new()
        .output_format("invalid_format")
        .build();
    match result {
        Ok(_) => println!("❌ Unexpected: Invalid config should have failed validation"),
        Err(e) => {
            println!("✓ Validation correctly caught invalid configuration:");
            println!("  Error: {}", e);
        }
    }
    println!();

    // Example 11: Configuration file save/load
    println!("--- Example 11: Save Configuration to File ---");
    let config = DataprofConfigBuilder::new()
        .output_format("json")
        .verbosity(2)
        .iso_quality_profile_strict()
        .engine("streaming")
        .build()?;

    let temp_path = std::env::temp_dir().join("dataprof_example.toml");
    config.save_to_file(&temp_path)?;
    println!("✓ Saved configuration to: {}", temp_path.display());

    // Load it back
    let loaded_config = dataprof::core::config::DataprofConfig::load_from_file(&temp_path)?;
    println!("✓ Loaded configuration from file");
    println!("  Output format: {}", loaded_config.output.default_format);
    println!("  Engine: {}", loaded_config.engine.default_engine);

    // Clean up
    std::fs::remove_file(&temp_path)?;
    println!("✓ Cleaned up temporary file");
    println!();

    println!("=== Summary ===");
    println!("✓ All configuration examples completed successfully!");
    println!("✓ Builder pattern provides:");
    println!("  • Clear, self-documenting configuration");
    println!("  • Type-safe construction");
    println!("  • Validation at build time");
    println!("  • Easy preset configurations");
    println!("  • Fluent API for readability");

    Ok(())
}
