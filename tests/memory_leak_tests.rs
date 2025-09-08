use anyhow::Result;
use dataprof::core::MemoryTracker;
use dataprof::engines::streaming::MemoryMappedCsvReader;
use std::io::Write;
use tempfile::NamedTempFile;

/// Test memory leak detection with memory mapped files
#[test]
fn test_mmap_memory_tracking() -> Result<()> {
    let tracker = MemoryTracker::new(1); // 1MB threshold

    // Create test data
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "name,age,city")?;
    for i in 0..1000 {
        writeln!(temp_file, "User{},{},City{}", i, 20 + (i % 50), i % 100)?;
    }
    temp_file.flush()?;

    // Test that mmap is tracked
    {
        let reader = MemoryMappedCsvReader::new_with_tracker(temp_file.path(), tracker.clone())?;

        // Memory should be tracked
        let (count, bytes, mb) = tracker.get_memory_stats();
        assert_eq!(count, 1, "Should track one allocation");
        assert!(bytes > 0, "Should track some bytes");

        println!(
            "Tracked: {} allocations, {} bytes ({} MB)",
            count, bytes, mb
        );

        // Read some data to verify it works
        let (headers, records) = reader.read_csv_chunk(0, 1024, true)?;
        assert!(headers.is_some(), "Should have headers");
        assert!(!records.is_empty(), "Should have records");
    } // reader should be dropped here, triggering deallocation tracking

    // After drop, allocation should be cleaned up
    let (count_after, bytes_after, mb_after) = tracker.get_memory_stats();
    println!(
        "After drop: {} allocations, {} bytes ({} MB)",
        count_after, bytes_after, mb_after
    );

    assert_eq!(count_after, 0, "Should have no allocations after drop");
    assert_eq!(bytes_after, 0, "Should have zero bytes after drop");

    Ok(())
}

/// Test memory leak detection over multiple operations
#[test]
fn test_multiple_mmap_operations() -> Result<()> {
    let tracker = MemoryTracker::new(10); // 10MB threshold

    // Create multiple temp files
    let mut temp_files = Vec::new();
    for i in 0..5 {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id,value")?;
        for j in 0..100 {
            writeln!(temp_file, "{},{}", i * 100 + j, j * i)?;
        }
        temp_file.flush()?;
        temp_files.push(temp_file);
    }

    // Process each file sequentially and verify memory tracking
    for (i, temp_file) in temp_files.iter().enumerate() {
        {
            let reader =
                MemoryMappedCsvReader::new_with_tracker(temp_file.path(), tracker.clone())?;

            let (count, _, _) = tracker.get_memory_stats();
            assert_eq!(count, 1, "Should track 1 allocation at a time");

            // Read some data
            let (_, records) = reader.read_csv_chunk(0, 512, true)?;
            assert!(!records.is_empty(), "File {} should have records", i);
        } // reader drops here automatically

        // Verify cleanup happened
        let (count_after, _, _) = tracker.get_memory_stats();
        assert_eq!(count_after, 0, "Should cleanup after each file");
    }

    // After all operations, should have no leaks
    let (final_count, final_bytes, _) = tracker.get_memory_stats();
    assert_eq!(
        final_count, 0,
        "Should have no allocations after all operations"
    );
    assert_eq!(final_bytes, 0, "Should have zero bytes after cleanup");

    // Verify no leaks detected
    let leak_report = tracker.report_leaks();
    assert!(
        leak_report.contains("No memory leaks detected"),
        "Should report no leaks, got: {}",
        leak_report
    );

    Ok(())
}

/// Test leak detection for large file scenarios
#[test]
fn test_large_file_leak_detection() -> Result<()> {
    let tracker = MemoryTracker::new(0); // 0MB threshold - detect any allocation

    // Create a larger test file (simulating real-world scenario)
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id,name,email,department,salary,years")?;

    // Generate more realistic data
    for i in 0..5000 {
        writeln!(
            temp_file,
            "{},Employee{},emp{}@company.com,Dept{},{},{}",
            i,
            i,
            i,
            i % 10,
            30000 + (i % 50000),
            i % 30
        )?;
    }
    temp_file.flush()?;

    // Test chunked processing (realistic usage)
    {
        let reader = MemoryMappedCsvReader::new_with_tracker(temp_file.path(), tracker.clone())?;

        let file_size = reader.file_size();
        println!("Processing file of {} bytes", file_size);

        // Process file in chunks
        let chunk_size = 8192usize;
        let mut offset = 0u64;
        let mut total_records = 0;

        while offset < file_size {
            let (headers, records) = reader.read_csv_chunk(offset, chunk_size, offset == 0)?;

            if offset == 0 {
                assert!(headers.is_some(), "First chunk should have headers");
            }

            total_records += records.len();
            offset += chunk_size as u64;

            // Memory should remain tracked during processing
            let (count, _, _) = tracker.get_memory_stats();
            assert_eq!(count, 1, "Should maintain one allocation during processing");
        }

        println!("Processed {} total records", total_records);
        assert!(
            total_records > 4000,
            "Should process most records (got {})",
            total_records
        );
    } // Drop reader

    // Verify cleanup after large file processing
    let (count, bytes, _) = tracker.get_memory_stats();
    assert_eq!(count, 0, "Large file should be cleaned up");
    assert_eq!(bytes, 0, "No bytes should remain");

    Ok(())
}

/// Test global memory tracking API
#[test]
fn test_global_memory_api() {
    // Test global API functions
    let initial_report = dataprof::check_memory_leaks();
    assert!(
        initial_report.contains("No memory leaks detected"),
        "Initial state should have no leaks"
    );

    let (count, bytes, mb) = dataprof::get_memory_usage_stats();
    println!(
        "Global stats: {} allocations, {} bytes, {} MB",
        count, bytes, mb
    );

    // Note: Global API creates new tracker instances, so they start empty
    // This is a limitation of the current design but still useful for monitoring
}

/// Test error conditions don't cause memory leaks
#[test]
fn test_error_handling_memory_cleanup() -> Result<()> {
    let tracker = MemoryTracker::new(1); // 1MB threshold

    // Test with invalid file
    let result = MemoryMappedCsvReader::new_with_tracker(
        std::path::Path::new("/nonexistent/file.csv"),
        tracker.clone(),
    );

    assert!(result.is_err(), "Should fail for nonexistent file");

    // Should have no tracked allocations after error
    let (count, bytes, _) = tracker.get_memory_stats();
    assert_eq!(count, 0, "Error case should not leak allocations");
    assert_eq!(bytes, 0, "Error case should not leak bytes");

    Ok(())
}
