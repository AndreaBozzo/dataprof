//! Integration tests for Parquet file format support
//!
//! These tests verify that Parquet files can be analyzed correctly
//! and that quality metrics are calculated properly.

#[cfg(feature = "parquet")]
mod parquet_tests {
    use anyhow::Result;
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, StringArray, TimestampMillisecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use dataprof::analyze_parquet_with_quality;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    /// Helper function to create a test Parquet file
    fn create_test_parquet_file() -> Result<NamedTempFile> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        // Define schema with various data types
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
            Field::new("salary", DataType::Float64, false),
            Field::new("active", DataType::Boolean, false),
        ]));

        // Create test data
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
        let ages = Int32Array::from(vec![25, 30, 35, 28, 32]);
        let salaries = Float64Array::from(vec![50000.0, 60000.0, 70000.0, 55000.0, 65000.0]);
        let active = BooleanArray::from(vec![true, true, false, true, false]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(ids),
                Arc::new(names),
                Arc::new(ages),
                Arc::new(salaries),
                Arc::new(active),
            ],
        )?;

        // Write to Parquet
        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(temp_file)
    }

    #[test]
    fn test_parquet_basic_analysis() -> Result<()> {
        let temp_file = create_test_parquet_file()?;
        let report = analyze_parquet_with_quality(temp_file.path())?;

        // Verify basic file info
        assert_eq!(report.scan_info.total_rows, 5);
        assert_eq!(report.scan_info.total_columns, 5);
        assert_eq!(report.scan_info.rows_scanned, 5);
        assert_eq!(report.scan_info.sampling_ratio, 1.0);

        // Verify all columns are present
        assert_eq!(report.column_profiles.len(), 5);
        let column_names: Vec<_> = report
            .column_profiles
            .iter()
            .map(|p| p.name.as_str())
            .collect();
        assert!(column_names.contains(&"id"));
        assert!(column_names.contains(&"name"));
        assert!(column_names.contains(&"age"));
        assert!(column_names.contains(&"salary"));
        assert!(column_names.contains(&"active"));

        Ok(())
    }

    #[test]
    fn test_parquet_column_statistics() -> Result<()> {
        let temp_file = create_test_parquet_file()?;
        let report = analyze_parquet_with_quality(temp_file.path())?;

        // Verify age column statistics
        let age_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .expect("Age column should exist");

        assert_eq!(age_profile.total_count, 5);
        assert_eq!(age_profile.null_count, 0);
        assert!(age_profile.unique_count.is_some());

        // Verify salary column (should be detected as Float)
        let salary_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "salary")
            .expect("Salary column should exist");

        assert_eq!(salary_profile.total_count, 5);
        assert_eq!(salary_profile.null_count, 0);

        // Verify name column (should be detected as String)
        let name_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "name")
            .expect("Name column should exist");

        assert_eq!(name_profile.total_count, 5);
        assert_eq!(name_profile.null_count, 0);

        Ok(())
    }

    #[test]
    fn test_parquet_with_nulls() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        // Create schema with nullable fields
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("optional_name", DataType::Utf8, true),
            Field::new("optional_value", DataType::Float64, true),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let names = StringArray::from(vec![
            Some("Alice"),
            None,
            Some("Charlie"),
            None,
            Some("Eve"),
        ]);
        let values = Float64Array::from(vec![Some(10.0), Some(20.0), None, Some(40.0), Some(50.0)]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(names), Arc::new(values)],
        )?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Analyze
        let report = analyze_parquet_with_quality(path)?;

        // Check null counts
        let name_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "optional_name")
            .expect("optional_name should exist");
        assert_eq!(name_profile.total_count, 5);
        assert_eq!(name_profile.null_count, 2);

        let value_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "optional_value")
            .expect("optional_value should exist");
        assert_eq!(value_profile.total_count, 5);
        assert_eq!(value_profile.null_count, 1);

        Ok(())
    }

    #[test]
    fn test_parquet_quality_metrics() -> Result<()> {
        let temp_file = create_test_parquet_file()?;
        let report = analyze_parquet_with_quality(temp_file.path())?;

        // Quality metrics should be calculated
        let metrics = &report.data_quality_metrics;

        // Completeness: all fields are non-null in this test
        assert!(metrics.complete_records_ratio >= 0.0);
        assert!(metrics.complete_records_ratio <= 100.0);

        // Overall quality score should be reasonable
        let quality_score = report.quality_score();
        assert!((0.0..=100.0).contains(&quality_score));

        println!("Quality score: {:.1}%", quality_score);
        println!("Completeness: {:.1}%", metrics.complete_records_ratio);

        Ok(())
    }

    #[test]
    fn test_parquet_multiple_batches() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

        // Write multiple batches
        for batch_num in 0..3 {
            let ids = Int32Array::from(vec![
                batch_num * 10 + 1,
                batch_num * 10 + 2,
                batch_num * 10 + 3,
            ]);
            let values = Float64Array::from(vec![1.0, 2.0, 3.0]);

            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(values)])?;

            writer.write(&batch)?;
        }

        writer.close()?;

        // Analyze
        let report = analyze_parquet_with_quality(path)?;

        // Should process all batches (3 batches × 3 rows = 9 rows)
        assert_eq!(report.scan_info.total_rows, 9);
        assert_eq!(report.scan_info.rows_scanned, 9);

        Ok(())
    }

    #[test]
    fn test_parquet_error_handling() {
        use std::path::Path;

        // Test with non-existent file
        let result = analyze_parquet_with_quality(Path::new("nonexistent.parquet"));
        assert!(result.is_err());

        // Test with invalid file (not Parquet)
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "This is not a Parquet file").unwrap();
        temp_file.flush().unwrap();

        let result = analyze_parquet_with_quality(temp_file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_parquet_large_strings() -> Result<()> {
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("description", DataType::Utf8, false),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3]);
        let descriptions = StringArray::from(vec![
            "Short",
            "This is a much longer description with more text",
            "A",
        ]);

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(descriptions)])?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        let report = analyze_parquet_with_quality(path)?;

        let desc_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "description")
            .expect("description should exist");

        assert_eq!(desc_profile.total_count, 3);
        assert_eq!(desc_profile.null_count, 0);

        // Check text statistics
        if let dataprof::types::ColumnStats::Text {
            min_length,
            max_length,
            avg_length,
            ..
        } = desc_profile.stats
        {
            assert_eq!(min_length, 1); // "A"
            assert!(max_length > 40); // Long description
            assert!(avg_length > 0.0);
        } else {
            panic!("Description should have Text stats");
        }

        Ok(())
    }

    #[test]
    fn test_parquet_extended_types() -> Result<()> {
        // Test with extended Arrow types: UInt, Date, Timestamp, etc.
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        use arrow::datatypes::TimeUnit;

        let schema = Arc::new(Schema::new(vec![
            Field::new("uint32_col", DataType::UInt32, false),
            Field::new("uint64_col", DataType::UInt64, false),
            Field::new("int8_col", DataType::Int8, false),
            Field::new("int16_col", DataType::Int16, false),
            Field::new("date32_col", DataType::Date32, false),
            Field::new(
                "timestamp_col",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        let uint32s = UInt32Array::from(vec![100, 200, 300]);
        let uint64s = UInt64Array::from(vec![1000, 2000, 3000]);
        let int8s = Int8Array::from(vec![1, 2, 3]);
        let int16s = Int16Array::from(vec![10, 20, 30]);
        let date32s = Date32Array::from(vec![18628, 18629, 18630]); // Days since epoch
        let timestamps = TimestampMillisecondArray::from(vec![
            1609459200000, // 2021-01-01
            1609545600000,
            1609632000000,
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(uint32s),
                Arc::new(uint64s),
                Arc::new(int8s),
                Arc::new(int16s),
                Arc::new(date32s),
                Arc::new(timestamps),
            ],
        )?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Analyze
        let report = analyze_parquet_with_quality(path)?;

        // Verify all columns are present
        assert_eq!(report.column_profiles.len(), 6);

        // Verify uint32 column
        let uint32_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "uint32_col")
            .expect("uint32_col should exist");
        assert_eq!(uint32_profile.total_count, 3);
        assert_eq!(uint32_profile.null_count, 0);

        // Verify date32 column - should be detected as Date
        let date_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "date32_col")
            .expect("date32_col should exist");
        assert_eq!(date_profile.total_count, 3);
        assert!(matches!(
            date_profile.data_type,
            dataprof::types::DataType::Date
        ));

        // Verify timestamp column - should be detected as Date
        let ts_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "timestamp_col")
            .expect("timestamp_col should exist");
        assert_eq!(ts_profile.total_count, 3);
        assert!(matches!(
            ts_profile.data_type,
            dataprof::types::DataType::Date
        ));

        Ok(())
    }

    #[test]
    fn test_parquet_binary_and_decimal() -> Result<()> {
        // Test binary and decimal types
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("binary_col", DataType::Binary, false),
            Field::new("decimal_col", DataType::Decimal128(10, 2), false),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3]);
        let binaries = BinaryArray::from(vec![b"hello".as_slice(), b"world", b"test"]);
        let decimals =
            Decimal128Array::from(vec![12345, 67890, 11111]).with_precision_and_scale(10, 2)?;

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(binaries), Arc::new(decimals)],
        )?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Analyze
        let report = analyze_parquet_with_quality(path)?;

        // Verify all columns processed
        assert_eq!(report.column_profiles.len(), 3);

        // Binary column should have been processed
        let binary_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "binary_col")
            .expect("binary_col should exist");
        assert_eq!(binary_profile.total_count, 3);
        assert_eq!(binary_profile.null_count, 0);

        // Decimal column should be processed as numeric
        let decimal_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "decimal_col")
            .expect("decimal_col should exist");
        assert_eq!(decimal_profile.total_count, 3);
        assert!(matches!(
            decimal_profile.data_type,
            dataprof::types::DataType::Float
        ));

        Ok(())
    }

    #[test]
    fn test_parquet_mixed_types_comprehensive() -> Result<()> {
        // Comprehensive test with multiple types in one file
        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path();

        let schema = Arc::new(Schema::new(vec![
            Field::new("int8", DataType::Int8, true),
            Field::new("int16", DataType::Int16, true),
            Field::new("uint8", DataType::UInt8, true),
            Field::new("uint16", DataType::UInt16, true),
            Field::new("uint32", DataType::UInt32, true),
            Field::new("float32", DataType::Float32, true),
            Field::new("bool", DataType::Boolean, true),
        ]));

        let int8s = Int8Array::from(vec![Some(1), None, Some(3)]);
        let int16s = Int16Array::from(vec![Some(100), Some(200), None]);
        let uint8s = UInt8Array::from(vec![Some(10), Some(20), Some(30)]);
        let uint16s = UInt16Array::from(vec![None, Some(1000), Some(2000)]);
        let uint32s = UInt32Array::from(vec![Some(10000), None, Some(30000)]);
        let float32s = Float32Array::from(vec![Some(1.5), Some(2.5), Some(3.5)]);
        let bools = BooleanArray::from(vec![Some(true), None, Some(false)]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(int8s),
                Arc::new(int16s),
                Arc::new(uint8s),
                Arc::new(uint16s),
                Arc::new(uint32s),
                Arc::new(float32s),
                Arc::new(bools),
            ],
        )?;

        let file = File::create(path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        // Analyze
        let report = analyze_parquet_with_quality(path)?;

        // Verify all 7 columns processed
        assert_eq!(report.column_profiles.len(), 7);

        // Verify null handling for each column
        let int8_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "int8")
            .unwrap();
        assert_eq!(int8_profile.null_count, 1);

        let uint16_profile = report
            .column_profiles
            .iter()
            .find(|p| p.name == "uint16")
            .unwrap();
        assert_eq!(uint16_profile.null_count, 1);

        // Quality score should be reasonable
        let quality_score = report.quality_score();
        assert!((0.0..=100.0).contains(&quality_score));

        Ok(())
    }

    #[test]
    fn test_parquet_custom_batch_size() -> Result<()> {
        // Test analysis with custom batch size configuration
        let temp_file = create_test_parquet_file()?;

        // Test with small batch size
        let config = dataprof::ParquetConfig::batch_size(2);
        let report = dataprof::analyze_parquet_with_config(temp_file.path(), &config)?;

        // Should still analyze all data correctly
        assert_eq!(report.scan_info.total_rows, 5);
        assert_eq!(report.column_profiles.len(), 5);

        // Test with large batch size
        let config = dataprof::ParquetConfig::batch_size(10000);
        let report = dataprof::analyze_parquet_with_config(temp_file.path(), &config)?;

        assert_eq!(report.scan_info.total_rows, 5);
        assert_eq!(report.column_profiles.len(), 5);

        Ok(())
    }

    #[test]
    fn test_parquet_adaptive_batch_size() -> Result<()> {
        // Test adaptive batch sizing logic
        use dataprof::ParquetConfig;

        // Small file (< 1MB)
        assert_eq!(ParquetConfig::adaptive_batch_size(500_000), 1024);

        // Medium file (5MB)
        assert_eq!(ParquetConfig::adaptive_batch_size(5_000_000), 4096);

        // Default file (50MB)
        assert_eq!(ParquetConfig::adaptive_batch_size(50_000_000), 8192);

        // Large file (500MB)
        assert_eq!(ParquetConfig::adaptive_batch_size(500_000_000), 16384);

        // Very large file (5GB)
        assert_eq!(ParquetConfig::adaptive_batch_size(5_000_000_000), 32768);

        Ok(())
    }
}
