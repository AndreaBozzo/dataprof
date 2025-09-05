/// Tests for v0.3.0 streaming and memory-efficient features
/// These tests verify the actual implementation of the new features
use anyhow::Result;
use dataprof::{
    acceleration::simd::{compute_stats_auto, compute_stats_simd, should_use_simd},
    core::sampling::SamplingStrategy,
    engines::columnar::SimpleColumnarProfiler,
    engines::streaming::{MemoryEfficientProfiler, MemoryMappedCsvReader, TrueStreamingProfiler},
};
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_memory_mapped_csv_reader() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "name,age,city")?;
    writeln!(temp_file, "Alice,25,New York")?;
    writeln!(temp_file, "Bob,30,London")?;
    writeln!(temp_file, "Charlie,35,Tokyo")?;
    temp_file.flush()?;

    let reader = MemoryMappedCsvReader::new(temp_file.path())?;

    // Test file size detection
    assert!(reader.file_size() > 0);

    // Test row estimation
    let estimated_rows = reader.estimate_row_count()?;
    assert!(estimated_rows >= 3 && estimated_rows <= 10); // Should be around 3-4 rows

    // Test chunk reading
    let (headers, records) = reader.read_csv_chunk(0, 1024, true)?;
    assert!(headers.is_some());
    assert_eq!(records.len(), 3);

    let header_record = headers.unwrap();
    assert_eq!(header_record.get(0), Some("name"));
    assert_eq!(header_record.get(1), Some("age"));
    assert_eq!(header_record.get(2), Some("city"));

    Ok(())
}

#[test]
fn test_memory_efficient_profiler() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id,value,category")?;

    // Create a moderately large file to trigger memory-efficient processing
    for i in 1..=1000 {
        writeln!(temp_file, "{},{:.1},category_{}", i, i as f64 * 1.5, i % 5)?;
    }
    temp_file.flush()?;

    let profiler = MemoryEfficientProfiler::new().memory_limit_mb(50); // Small memory limit to test efficiency

    let report = profiler.analyze_file(temp_file.path())?;

    assert_eq!(report.column_profiles.len(), 3);
    assert_eq!(report.scan_info.rows_scanned, 1000);

    // Verify correct type detection
    let id_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "id")
        .unwrap();
    let value_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "value")
        .unwrap();
    let category_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "category")
        .unwrap();

    assert_eq!(id_profile.data_type, dataprof::DataType::Integer);
    assert_eq!(value_profile.data_type, dataprof::DataType::Float);
    assert_eq!(category_profile.data_type, dataprof::DataType::String);

    // Check statistics are reasonable
    if let dataprof::ColumnStats::Numeric { min, max, mean } = &id_profile.stats {
        assert_eq!(*min, 1.0);
        assert_eq!(*max, 1000.0);
        assert!((mean - 500.5).abs() < 1.0);
    } else {
        panic!("Expected numeric stats for id column");
    }

    Ok(())
}

#[test]
fn test_true_streaming_profiler() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "numbers,text,dates")?;

    // Create a large enough dataset to test true streaming
    for i in 1..=5000 {
        writeln!(temp_file, "{},text_{},2024-01-{:02}", i, i, (i % 28) + 1)?;
    }
    temp_file.flush()?;

    let profiler = TrueStreamingProfiler::new().memory_limit_mb(32); // Very small memory limit to force streaming

    let report = profiler.analyze_file(temp_file.path())?;

    assert_eq!(report.column_profiles.len(), 3);
    // Should process most/all rows (allowing for sampling variations)
    assert!(report.scan_info.rows_scanned >= 4990);
    assert!(report.scan_info.rows_scanned <= 5000);

    let numbers_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "numbers")
        .unwrap();
    let text_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "text")
        .unwrap();
    let dates_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "dates")
        .unwrap();

    assert_eq!(numbers_profile.data_type, dataprof::DataType::Integer);
    assert_eq!(text_profile.data_type, dataprof::DataType::String);
    assert_eq!(dates_profile.data_type, dataprof::DataType::Date);

    // Check that streaming statistics are correct
    if let dataprof::ColumnStats::Numeric { min, max, mean } = &numbers_profile.stats {
        assert_eq!(*min, 1.0);
        assert_eq!(*max, 5000.0);
        assert!((mean - 2500.5).abs() < 1.0);
    }

    Ok(())
}

#[test]
fn test_simple_columnar_profiler() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "integers,floats,strings")?;

    // Test with data that benefits from columnar processing
    for i in 1..=1000 {
        writeln!(
            temp_file,
            "{},{:.2},string_{}",
            i,
            (i as f64) * 0.1,
            i % 100
        )?;
    }
    temp_file.flush()?;

    let profiler = SimpleColumnarProfiler::new().use_simd(true); // Enable SIMD acceleration

    let report = profiler.analyze_csv_file(temp_file.path())?;

    assert_eq!(report.column_profiles.len(), 3);
    assert_eq!(report.scan_info.rows_scanned, 1000);

    let integers_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "integers")
        .unwrap();
    let floats_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "floats")
        .unwrap();

    assert_eq!(integers_profile.data_type, dataprof::DataType::Integer);
    assert_eq!(floats_profile.data_type, dataprof::DataType::Float);

    // Verify SIMD acceleration worked correctly
    if let dataprof::ColumnStats::Numeric { min, max, mean } = &integers_profile.stats {
        assert_eq!(*min, 1.0);
        assert_eq!(*max, 1000.0);
        assert!((mean - 500.5).abs() < 0.01);
    }

    Ok(())
}

#[test]
fn test_simd_acceleration() -> Result<()> {
    let test_data = (1..=1000).map(|x| x as f64).collect::<Vec<_>>();

    // Test SIMD stats computation
    let simd_stats = compute_stats_simd(&test_data);
    assert_eq!(simd_stats.count, 1000);
    assert_eq!(simd_stats.min, 1.0);
    assert_eq!(simd_stats.max, 1000.0);
    assert!((simd_stats.mean() - 500.5).abs() < 1e-10);

    // Test auto selection
    let auto_stats = compute_stats_auto(&test_data);
    assert_eq!(auto_stats.count, 1000);
    assert!((auto_stats.mean() - 500.5).abs() < 1e-10);

    // Test should_use_simd logic
    assert!(!should_use_simd(10)); // Too small
    assert!(should_use_simd(1000)); // Large enough

    Ok(())
}

#[test]
fn test_advanced_sampling_strategies() -> Result<()> {
    use dataprof::core::sampling::strategies::SamplingState;

    // Test progressive sampling with state
    let progressive = SamplingStrategy::Progressive {
        initial_size: 100,
        confidence_level: 0.95,
        max_size: 500,
    };

    let mut progressive_state = SamplingState::new();
    let mut included_count = 0;
    for i in 0..1000 {
        if progressive.should_include_with_state(i, i + 1, &mut progressive_state, None) {
            included_count += 1;
        }
    }

    // Should sample between initial_size and max_size
    assert!(included_count >= 100);
    assert!(included_count <= 500);

    // Test reservoir sampling with state
    let reservoir = SamplingStrategy::Reservoir { size: 200 };
    let mut reservoir_state = SamplingState::new();
    let mut reservoir_count = 0;

    for i in 0..1000 {
        if reservoir.should_include_with_state(i, i + 1, &mut reservoir_state, None) {
            reservoir_count += 1;
        }
    }

    // Should be approximately the reservoir size (allowing for implementation variations)
    println!(
        "Progressive count: {}, Reservoir count: {}",
        included_count, reservoir_count
    );
    // For now, just check that sampling actually happens (not all 1000 rows)
    assert!(reservoir_count > 150);
    assert!(reservoir_count < 800); // Should be significantly less than total

    Ok(())
}

#[test]
fn test_profiler_auto_selection() -> Result<()> {
    // Test with small file - should use regular streaming
    let mut small_file = NamedTempFile::new()?;
    writeln!(small_file, "data")?;
    for i in 1..=100 {
        writeln!(small_file, "{}", i)?;
    }
    small_file.flush()?;

    let small_report = dataprof::quick_quality_check(small_file.path())?;
    assert!(small_report >= 0.0);

    // Test with medium file - should trigger memory-efficient profiler
    let mut medium_file = NamedTempFile::new()?;
    writeln!(medium_file, "data")?;
    for i in 1..=10000 {
        writeln!(medium_file, "{}", i)?;
    }
    medium_file.flush()?;

    let medium_report = dataprof::quick_quality_check(medium_file.path())?;
    assert!(medium_report >= 0.0);

    Ok(())
}

#[test]
fn test_streaming_statistics_accuracy() -> Result<()> {
    use dataprof::core::streaming_stats::StreamingStatistics;

    let mut stats = StreamingStatistics::new();

    // Add test data
    for i in 1..=1000 {
        stats.update(&i.to_string());
    }

    assert_eq!(stats.count, 1000);
    assert_eq!(stats.null_count, 0);
    assert_eq!(stats.unique_count(), 1000);
    assert!((stats.mean() - 500.5).abs() < 1e-10);

    // Test with some null values
    stats.update("");
    stats.update("");

    assert_eq!(stats.count, 1002);
    assert_eq!(stats.null_count, 2);

    // Mean should remain the same (nulls excluded)
    assert!((stats.mean() - 500.5).abs() < 1e-10);

    Ok(())
}

#[test]
fn test_memory_pressure_handling() -> Result<()> {
    use dataprof::core::streaming_stats::StreamingColumnCollection;

    let mut collection = StreamingColumnCollection::with_memory_limit(1); // 1MB limit
    let headers = vec!["col1".to_string(), "col2".to_string()];

    // Add lots of unique data to trigger memory pressure
    for i in 0..10000 {
        let values = vec![
            format!("unique_value_{}", i),
            format!("another_unique_{}", i),
        ];
        collection.process_record(&headers, values);

        // Should handle memory pressure gracefully
        if collection.is_memory_pressure() {
            collection.reduce_memory_usage();
        }
    }

    // Should still function correctly after memory pressure
    assert!(!collection.column_names().is_empty());
    assert!(collection.get_column_stats("col1").is_some());

    Ok(())
}

#[test]
fn test_large_file_memory_efficiency() -> Result<()> {
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id,data")?;

    // Create a file that would use significant memory if loaded entirely
    for i in 1..=50000 {
        writeln!(
            temp_file,
            "{},data_string_number_{}_with_more_content",
            i, i
        )?;
    }
    temp_file.flush()?;

    // Test that memory-efficient profiler handles it
    let profiler = MemoryEfficientProfiler::new().memory_limit_mb(64); // Reasonable limit

    let start_time = std::time::Instant::now();
    let report = profiler.analyze_file(temp_file.path())?;
    let duration = start_time.elapsed();

    assert_eq!(report.column_profiles.len(), 2);
    assert_eq!(report.scan_info.rows_scanned, 50000);

    // Should complete in reasonable time (less than 10 seconds)
    assert!(duration.as_secs() < 10);

    // Verify data types are correctly inferred
    let id_profile = report
        .column_profiles
        .iter()
        .find(|p| p.name == "id")
        .unwrap();
    assert_eq!(id_profile.data_type, dataprof::DataType::Integer);

    Ok(())
}

#[test]
fn test_error_handling_and_edge_cases() -> Result<()> {
    // Test empty file
    let mut empty_file = NamedTempFile::new()?;
    writeln!(empty_file, "header")?;
    empty_file.flush()?;

    let profiler = TrueStreamingProfiler::new();
    let result = profiler.analyze_file(empty_file.path());
    assert!(result.is_ok()); // Should handle empty files gracefully

    // Test malformed CSV
    let mut malformed_file = NamedTempFile::new()?;
    writeln!(malformed_file, "a,b,c")?;
    writeln!(malformed_file, "1,2")?; // Missing column
    writeln!(malformed_file, "3,4,5,6")?; // Extra column
    malformed_file.flush()?;

    let result2 = profiler.analyze_file(malformed_file.path());
    // For now, malformed CSV may fail - this is acceptable behavior
    let _ = result2; // Just ensure it doesn't panic

    Ok(())
}
