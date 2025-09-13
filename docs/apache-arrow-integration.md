# Apache Arrow Integration Guide

DataProfiler leverages Apache Arrow's columnar processing capabilities for high-performance data profiling on large datasets. This guide explains how to use Arrow-based profiling features and when to choose them.

## Overview

Apache Arrow provides:
- **Columnar memory format** for efficient data processing
- **Zero-copy operations** reducing memory overhead
- **SIMD acceleration** for numeric computations
- **Batch processing** for large datasets
- **Memory efficiency** through columnar organization

## Quick Start

### Command Line Usage

```bash
# Automatic Arrow selection for large files (>500MB)
dataprof large_dataset.csv

# Force Arrow profiler
dataprof --engine arrow data.csv

# Arrow with custom batch size
dataprof --engine arrow --batch-size 16384 huge_file.csv
```

### Programmatic Usage

```rust
use dataprof::DataProfiler;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Use Arrow profiler explicitly
    let profiler = DataProfiler::columnar()
        .batch_size(8192)
        .memory_limit_mb(1024);

    let report = profiler.analyze_csv_file("large_data.csv")?;

    println!("Processed {} rows in {}ms",
        report.scan_info.rows_scanned,
        report.scan_info.scan_time_ms
    );

    Ok(())
}
```

### Python Integration

```python
import dataprof

# Arrow profiler automatically selected for large files
profiles = dataprof.analyze_csv_file("large_dataset.csv", engine="arrow")

for profile in profiles:
    print(f"{profile.name}: {profile.data_type} "
          f"(nulls: {profile.null_percentage:.1f}%)")

# Configuration options
report = dataprof.analyze_csv_with_quality(
    "huge_file.csv",
    engine="arrow",
    batch_size=16384,
    memory_limit_mb=2048
)
```

## When to Use Arrow

Arrow profiler is automatically selected when:
- File size > 500MB
- Complex numeric operations needed
- Memory efficiency is critical
- Maximum performance required

### Performance Comparison

| File Size | Standard | Arrow | Speedup |
|-----------|----------|-------|---------|
| 100MB     | 2.1s     | 0.8s  | 2.6x    |
| 500MB     | 12.3s    | 3.2s  | 3.8x    |
| 1GB       | 28.7s    | 5.9s  | 4.9x    |
| 5GB       | 156s     | 24s   | 6.5x    |

## Configuration Options

### ArrowProfiler Configuration

```rust
use dataprof::engines::columnar::ArrowProfiler;

let profiler = ArrowProfiler::new()
    .batch_size(16384)        // Arrow batch size (default: 8192)
    .memory_limit_mb(2048);   // Memory limit (default: 512MB)
```

### Batch Size Guidelines

| Dataset Size | Recommended Batch Size | Memory Usage |
|--------------|----------------------|--------------|
| < 1GB        | 8,192 (default)      | ~256MB       |
| 1-5GB        | 16,384               | ~512MB       |
| 5-20GB       | 32,768               | ~1GB         |
| > 20GB       | 65,536               | ~2GB         |

## Supported Data Types

Arrow profiler supports all Arrow native types with optimized processing:

### Numeric Types
- **Float64/Float32** - SIMD-accelerated statistics
- **Int64/Int32/Int16/Int8** - Fast integer operations
- **Decimal** - High-precision numeric analysis

### String Types
- **Utf8** - Standard string processing
- **LargeUtf8** - For long text fields
- **Binary** - Binary data analysis

### Temporal Types
- **Date32/Date64** - Date analysis with pattern detection
- **Timestamp** - Timezone-aware temporal profiling
- **Time** - Time-of-day analysis

### Complex Types
- **List** - Array/JSON field analysis
- **Struct** - Nested data profiling
- **Dictionary** - Categorical data optimization

## Advanced Features

### Schema Inference

Arrow automatically infers optimal schemas:

```rust
// Arrow detects numeric types automatically
let profiler = DataProfiler::columnar();
let report = profiler.analyze_csv_file("mixed_types.csv")?;

// Access inferred Arrow schema information
for profile in &report.column_profiles {
    match &profile.stats {
        ColumnStats::Numeric { min, max, mean } => {
            println!("Numeric column {}: [{}, {}] avg={}",
                profile.name, min, max, mean);
        }
        ColumnStats::Text { avg_length, .. } => {
            println!("Text column {}: avg_len={}",
                profile.name, avg_length);
        }
    }
}
```

### Memory Management

```rust
use dataprof::engines::columnar::ArrowProfiler;

// Configure for memory-constrained environments
let profiler = ArrowProfiler::new()
    .memory_limit_mb(256)     // Limit to 256MB
    .batch_size(4096);        // Smaller batches

// Monitor memory usage during processing
let report = profiler.analyze_csv_file("large_file.csv")?;
```

### Parallel Processing

Arrow leverages Rust's parallel capabilities:

```rust
use rayon::prelude::*;
use dataprof::DataProfiler;

// Process multiple files in parallel
let files = vec!["data1.csv", "data2.csv", "data3.csv"];

let reports: Result<Vec<_>, _> = files
    .par_iter()
    .map(|file| {
        let profiler = DataProfiler::columnar();
        profiler.analyze_csv_file(file)
    })
    .collect();

match reports {
    Ok(reports) => {
        for (file, report) in files.iter().zip(reports.iter()) {
            println!("{}: {} columns processed", file, report.column_profiles.len());
        }
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## Integration Examples

### Apache Spark Integration

```python
from pyspark.sql import SparkSession
import dataprof

spark = SparkSession.builder.appName("DataProfiling").getOrCreate()

def profile_partition(partition_files):
    """Profile files in a Spark partition using Arrow"""
    results = []
    for file_path in partition_files:
        try:
            report = dataprof.analyze_csv_with_quality(
                file_path,
                engine="arrow",
                batch_size=32768
            )
            results.append({
                'file': file_path,
                'quality_score': report.quality_score(),
                'columns': len(report.column_profiles),
                'rows': report.scan_info.rows_scanned
            })
        except Exception as e:
            results.append({'file': file_path, 'error': str(e)})
    return results

# Process files distributed across cluster
file_rdd = spark.sparkContext.parallelize(file_paths)
results = file_rdd.mapPartitions(profile_partition).collect()
```

### Polars Integration

```python
import polars as pl
import dataprof

# Read with Polars, profile with DataProfiler Arrow engine
df = pl.read_csv("large_dataset.csv")

# Save as Arrow format for fastest profiling
df.write_parquet("temp_data.parquet")

# Profile the Arrow/Parquet data
report = dataprof.analyze_parquet_file(
    "temp_data.parquet",
    engine="arrow"
)

print(f"Quality Score: {report.quality_score():.1f}%")
```

### Delta Lake Integration

```python
from deltalake import DeltaTable
import dataprof

# Read Delta table
delta_table = DeltaTable("path/to/delta/table")

# Profile latest version using Arrow
report = dataprof.analyze_delta_table(
    delta_table,
    engine="arrow",
    version="latest"
)

# Check data quality trends over versions
for version in range(max(0, delta_table.version() - 5), delta_table.version() + 1):
    version_report = dataprof.analyze_delta_table(
        delta_table,
        engine="arrow",
        version=version
    )
    print(f"Version {version}: Quality {version_report.quality_score():.1f}%")
```

## Performance Tuning

### Memory Optimization

```rust
// For memory-constrained environments
let profiler = ArrowProfiler::new()
    .memory_limit_mb(128)      // Very low memory
    .batch_size(2048);         // Small batches

// For high-memory systems
let profiler = ArrowProfiler::new()
    .memory_limit_mb(8192)     // 8GB memory
    .batch_size(131072);       // Large batches
```

### CPU Optimization

```bash
# Set thread count for Arrow operations
export RAYON_NUM_THREADS=16

# Enable SIMD optimizations
export CARGO_CFG_TARGET_FEATURE="+avx2,+fma"

# Profile with optimizations
dataprof --engine arrow --batch-size 65536 huge_dataset.csv
```

### Storage Optimization

```python
import dataprof

# Profile compressed files directly
report = dataprof.analyze_csv_file(
    "compressed_data.csv.gz",
    engine="arrow",
    compression="gzip"
)

# Profile Parquet (optimal for Arrow)
parquet_report = dataprof.analyze_parquet_file(
    "columnar_data.parquet",
    engine="arrow"
)
```

## Monitoring and Observability

### Progress Tracking

```rust
use dataprof::engines::columnar::ArrowProfiler;

let profiler = ArrowProfiler::new()
    .batch_size(16384)
    .with_progress_callback(|progress| {
        println!("Processed batch {}/{}: {:.1}% complete",
            progress.batches_processed,
            progress.total_batches,
            progress.percentage
        );
    });
```

### Memory Monitoring

```python
import dataprof
import psutil
import time

def monitor_memory():
    process = psutil.Process()
    start_memory = process.memory_info().rss / 1024 / 1024  # MB

    report = dataprof.analyze_csv_file(
        "large_file.csv",
        engine="arrow",
        batch_size=16384
    )

    peak_memory = process.memory_info().rss / 1024 / 1024  # MB

    print(f"Memory usage: {start_memory:.1f}MB -> {peak_memory:.1f}MB")
    print(f"Memory efficiency: {peak_memory - start_memory:.1f}MB for {report.scan_info.rows_scanned:,} rows")

monitor_memory()
```

## Troubleshooting

### Common Issues

#### Out of Memory Errors
```
Error: Arrow batch processing failed: out of memory
```
**Solution:**
```bash
# Reduce batch size and memory limit
dataprof --engine arrow --batch-size 4096 --memory-limit-mb 256 large_file.csv
```

#### Schema Inference Errors
```
Error: Failed to infer Arrow schema from CSV headers
```
**Solution:**
```python
# Manually specify schema for complex files
import dataprof

report = dataprof.analyze_csv_file(
    "complex_data.csv",
    engine="arrow",
    schema_hints={
        'numeric_column': 'float64',
        'date_column': 'date32',
        'text_column': 'string'
    }
)
```

#### Performance Issues
```
Warning: Arrow processing slower than expected
```
**Solution:**
```rust
// Enable all optimizations
let profiler = ArrowProfiler::new()
    .batch_size(32768)           // Larger batches
    .memory_limit_mb(4096)       // More memory
    .enable_simd(true)           // SIMD acceleration
    .parallel_processing(true);  // Parallel columns
```

### Debug Mode

```bash
# Enable debug logging for Arrow operations
RUST_LOG=dataprof::engines::columnar=debug dataprof --engine arrow data.csv
```

## Feature Compilation

### Building with Arrow Support

```bash
# Build with Arrow features
cargo build --features arrow

# Build Python wheel with Arrow
maturin build --features python,arrow --release

# Install from source with Arrow
pip install dataprof[arrow]
```

### Feature Flags

| Feature | Description | Dependencies |
|---------|-------------|--------------|
| `arrow` | Core Arrow support | `arrow-rs` |
| `arrow-flight` | Arrow Flight RPC | `arrow-flight` |
| `arrow-parquet` | Parquet support | `parquet` |
| `arrow-json` | JSON Arrow arrays | `arrow-json` |

## Best Practices

1. **Choose appropriate batch sizes** based on available memory
2. **Use Arrow for files > 500MB** for optimal performance
3. **Monitor memory usage** during processing
4. **Enable SIMD** optimizations when available
5. **Profile Parquet files** directly for best performance
6. **Use parallel processing** for multiple files
7. **Set memory limits** to prevent OOM errors

## Migration from Standard Profiler

```python
# Old: Standard profiler
report = dataprof.analyze_csv_file("data.csv")

# New: Arrow profiler (automatic for large files)
report = dataprof.analyze_csv_file("data.csv")  # Arrow auto-selected

# New: Explicit Arrow profiler
report = dataprof.analyze_csv_file("data.csv", engine="arrow")
```

The API remains compatible - Arrow integration is transparent and automatically selected based on file size and system capabilities.
