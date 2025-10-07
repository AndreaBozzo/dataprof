/// Example demonstrating Apache Parquet file format support
///
/// This example shows:
/// - How to analyze Parquet files with dataprof
/// - Quality metrics for columnar data
/// - Integration with the unified API
///
/// Run with:
///   cargo run --example parquet_example --features parquet
///
/// Note: This example creates a temporary Parquet file for demonstration.

#[cfg(feature = "parquet")]
fn main() -> anyhow::Result<()> {
    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use dataprof::{analyze_parquet_with_quality, DataProfiler};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    println!("🚀 DataProf Parquet Support Example\n");

    // ========================================
    // Step 1: Create a sample Parquet file
    // ========================================
    println!("=== Step 1: Creating Sample Parquet File ===");

    let temp_file = NamedTempFile::with_suffix(".parquet")?;
    let path = temp_file.path();

    // Define schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("customer_name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
        Field::new("purchase_amount", DataType::Float64, false),
        Field::new("is_premium", DataType::Boolean, false),
    ]));

    // Create sample data simulating e-commerce transactions
    let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let names = StringArray::from(vec![
        "Alice Johnson",
        "Bob Smith",
        "Charlie Davis",
        "Diana Prince",
        "Eve Williams",
        "Frank Miller",
        "Grace Lee",
        "Henry Ford",
        "Iris Chen",
        "Jack Ryan",
    ]);
    let ages = Int32Array::from(vec![
        Some(25),
        Some(30),
        None, // Missing age
        Some(28),
        Some(32),
        Some(45),
        None, // Missing age
        Some(38),
        Some(29),
        Some(41),
    ]);
    let amounts = Float64Array::from(vec![
        129.99, 249.50, 89.99, 449.00, 179.99, 299.99, 59.99, 399.00, 189.50, 279.99,
    ]);
    let premium = BooleanArray::from(vec![
        true, false, false, true, false, true, false, true, false, true,
    ]);

    // Create RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(ages),
            Arc::new(amounts),
            Arc::new(premium),
        ],
    )?;

    // Write to Parquet file
    let file = File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    let file_size = std::fs::metadata(path)?.len();
    println!(
        "✅ Created Parquet file: {:.2} KB",
        file_size as f64 / 1024.0
    );
    println!("   Rows: {}", batch.num_rows());
    println!("   Columns: {}\n", batch.num_columns());

    // ========================================
    // Step 2: Analyze with Parquet Parser
    // ========================================
    println!("=== Step 2: Analyzing with Parquet Parser ===");

    let report = analyze_parquet_with_quality(path)?;

    println!("📊 Analysis Results:");
    println!(
        "   Total Rows: {}",
        report.file_info.total_rows.unwrap_or(0)
    );
    println!("   Total Columns: {}", report.file_info.total_columns);
    println!("   File Size: {:.2} MB", report.file_info.file_size_mb);
    println!("   Scan Time: {} ms\n", report.scan_info.scan_time_ms);

    // ========================================
    // Step 3: Examine Column Profiles
    // ========================================
    println!("=== Step 3: Column Profiles ===");

    for profile in &report.column_profiles {
        println!("📝 Column: {}", profile.name);
        println!("   Type: {:?}", profile.data_type);
        println!("   Total Count: {}", profile.total_count);
        println!(
            "   Null Count: {} ({:.1}%)",
            profile.null_count,
            (profile.null_count as f64 / profile.total_count as f64) * 100.0
        );

        if let Some(unique) = profile.unique_count {
            println!(
                "   Unique Values: {} ({:.1}%)",
                unique,
                (unique as f64 / profile.total_count as f64) * 100.0
            );
        }

        match &profile.stats {
            dataprof::types::ColumnStats::Numeric { min, max, mean } => {
                println!("   Min: {:.2}", min);
                println!("   Max: {:.2}", max);
                println!("   Mean: {:.2}", mean);
            }
            dataprof::types::ColumnStats::Text {
                min_length,
                max_length,
                avg_length,
            } => {
                println!("   Min Length: {}", min_length);
                println!("   Max Length: {}", max_length);
                println!("   Avg Length: {:.1}", avg_length);
            }
        }
        println!();
    }

    // ========================================
    // Step 4: Quality Metrics (ISO 8000/25012)
    // ========================================
    println!("=== Step 4: Data Quality Metrics (ISO 8000/25012) ===");

    let metrics = &report.data_quality_metrics;

    println!("📈 Quality Dimensions:");
    println!("   Completeness: {:.1}%", metrics.complete_records_ratio);
    println!("   Consistency: {:.1}%", metrics.data_type_consistency);
    println!("   Uniqueness: {:.1}%", metrics.key_uniqueness);
    println!("   Accuracy: {:.1}%", metrics.outlier_ratio);
    println!("   Timeliness: {:.1}%", metrics.stale_data_ratio);
    println!();

    let quality_score = report.quality_score();
    println!("⭐ Overall Quality Score: {:.1}%", quality_score);

    if quality_score >= 80.0 {
        println!("   ✅ Excellent data quality!");
    } else if quality_score >= 60.0 {
        println!("   ⚠️  Good, but could be improved");
    } else {
        println!("   ❌ Data quality needs attention");
    }
    println!();

    // ========================================
    // Step 5: Using with Unified API
    // ========================================
    println!("=== Step 5: Using with Unified API ===");

    // The DataProfiler::auto() API automatically detects Parquet files
    let profiler = DataProfiler::auto();
    let auto_report = profiler.analyze_file(path)?;

    println!(" Automatic format detection works!");
    println!(
        "   Detected {} columns in Parquet file",
        auto_report.file_info.total_columns
    );
    println!("   Quality score: {:.1}%\n", auto_report.quality_score());

    // ========================================
    // Step 6: Comparison with CSV
    // ========================================
    println!("=== Step 6: Why Use Parquet? ===");
    println!("✨ Advantages of Parquet:");
    println!("   • Columnar storage = better compression");
    println!("   • Type-safe schema preservation");
    println!("   • Efficient for analytics workloads");
    println!("   • Native support in big data ecosystems");
    println!("   • Faster than CSV for large datasets\n");

    println!("📝 When to use Parquet:");
    println!("   ✅ Storing analytics data");
    println!("   ✅ Data lake architectures");
    println!("   ✅ Machine learning pipelines");
    println!("   ✅ Large datasets (>100MB)");
    println!("   ✅ Integration with Spark, Pandas, etc.\n");

    println!("💡 CLI Usage Examples:");
    println!("   # Analyze a Parquet file");
    println!("   $ dataprof analyze data.parquet --detailed");
    println!();
    println!("   # Generate HTML report");
    println!("   $ dataprof report data.parquet -o report.html");
    println!();
    println!("   # Batch process Parquet files");
    println!("   $ dataprof batch /data/parquet/ --recursive --filter '*.parquet'");
    println!();

    Ok(())
}

#[cfg(not(feature = "parquet"))]
fn main() {
    eprintln!("❌ Parquet feature not enabled!");
    eprintln!();
    eprintln!("This example requires the 'parquet' feature.");
    eprintln!("Please run with:");
    eprintln!();
    eprintln!("  cargo run --example parquet_example --features parquet");
    eprintln!();
    eprintln!("Or to enable both Arrow and Parquet:");
    eprintln!();
    eprintln!("  cargo run --example parquet_example --features arrow,parquet");
    eprintln!();
    std::process::exit(1);
}
