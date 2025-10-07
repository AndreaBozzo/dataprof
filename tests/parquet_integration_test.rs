//! Integration tests for Parquet file format support
//!
//! These tests verify that Parquet files can be analyzed correctly
//! and that quality metrics are calculated properly.

#[cfg(feature = "parquet")]
mod parquet_tests {
    use anyhow::Result;
    use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
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
        assert_eq!(report.file_info.total_rows, Some(5));
        assert_eq!(report.file_info.total_columns, 5);
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
        assert_eq!(report.file_info.total_rows, Some(9));
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
}
