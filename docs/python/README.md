# DataProf Python Bindings

High-performance Python bindings for DataProf - the blazing-fast data profiling library built in Rust.

## 🚀 Quick Start

### Installation

```bash
pip install dataprof
```

### Basic Usage

```python
import dataprof

# Analyze a CSV file
profiles = dataprof.analyze_csv_file("data.csv")
for profile in profiles:
    print(f"{profile.name}: {profile.data_type} ({profile.null_percentage:.1f}% nulls)")

# Get quality assessment
report = dataprof.analyze_csv_with_quality("data.csv")
print(f"Quality Score: {report.quality_score():.1f}%")

# Access detailed quality metrics (ISO 8000/25012)
metrics = report.data_quality_metrics
print(f"Completeness: {metrics.complete_records_ratio:.1f}%")
print(f"Consistency: {metrics.data_type_consistency:.1f}%")
```

## 📋 Features

- **🔥 High Performance**: Rust-powered analysis with SIMD acceleration
- **📊 Comprehensive Profiling**: Data types, nulls, distributions, quality issues
- **📏 ISO 8000/25012 Compliant**: Industry-standard quality assessment across 5 dimensions
- **🐼 Pandas Integration**: Native DataFrame support when pandas is available
- **⚡ Batch Processing**: Parallel processing for multiple files (CSV, JSON/JSONL, Parquet)
- **📱 Jupyter Support**: Rich HTML displays in notebooks
- **🔍 Quality Assessment**: Automated data quality issue detection
- **🎯 Type Safety**: Complete type hints with mypy compatibility
- **📦 Parquet Support**: Native columnar format analysis (requires `parquet` feature)

## 🎯 Use Cases

### Data Quality Assessment
```python
# Comprehensive quality report
report = dataprof.analyze_csv_with_quality("customer_data.csv")
print(f"Quality Score: {report.quality_score():.1f}%")
print(f"Total rows: {report.total_rows}, columns: {report.total_columns}")
print(f"Scan time: {report.scan_time_ms}ms")

# Access detailed metrics
metrics = report.data_quality_metrics
print(f"Completeness: {metrics.complete_records_ratio:.1f}%")
print(f"Consistency: {metrics.data_type_consistency:.1f}%")
print(f"Uniqueness: {metrics.key_uniqueness:.1f}%")
```

### Data Quality Metrics
```python
# Get detailed quality metrics (ISO 8000/25012 compliant)
metrics = dataprof.calculate_data_quality_metrics("features.csv")
print(f"Overall Quality: {metrics.overall_quality_score():.1f}%")
print(f"Completeness: {metrics.completeness_summary()}")
print(f"Consistency: {metrics.consistency_summary()}")
print(f"Uniqueness: {metrics.uniqueness_summary()}")
print(f"Accuracy: {metrics.accuracy_summary()}")
```

### Batch File Analysis
```python
# Process multiple files in parallel (CSV, JSON/JSONL, Parquet)
result = dataprof.batch_analyze_directory("/data", recursive=True)
print(f"Processed {result.processed_files} files at {result.files_per_second:.1f} files/sec")
print(f"Average quality: {result.average_quality_score:.1f}%")

# Process specific format with glob pattern
result = dataprof.batch_analyze_glob("/data/**/*.parquet")
```

### Pandas Integration
```python
import pandas as pd

# Get analysis as DataFrame for easier manipulation
df = dataprof.analyze_csv_dataframe("sales.csv")
high_null_cols = df[df['null_percentage'] > 20]
print(f"Columns with >20% nulls: {len(high_null_cols)}")
```

## 📚 Documentation

### Core Guides
- **[API Reference](API_REFERENCE.md)** - Complete function and class reference
- **[Integrations](INTEGRATIONS.md)** - Pandas and ecosystem integrations

### Key Functions
- `analyze_csv_file()` - Basic column profiling
- `analyze_csv_with_quality()` - Quality assessment with ISO 8000/25012 metrics
- `calculate_data_quality_metrics()` - Dedicated quality metrics calculation
- `batch_analyze_directory()` - High-performance batch processing
- `analyze_csv_dataframe()` - Pandas DataFrame integration

## ⚡ Performance

DataProf is built for speed:

- **Rust Core**: Memory-safe systems programming language
- **SIMD Acceleration**: Vectorized operations for numeric data
- **Parallel Processing**: Multi-threaded analysis by default
- **Memory Efficient**: Optimized for large datasets
- **Zero-Copy**: Minimal data copying between Rust and Python

### Benchmarks
```
File Size    | Files/sec | Throughput
-------------|-----------|----------
1MB CSV      | 50-100    | 50-100 MB/s
10MB CSV     | 15-25     | 150-250 MB/s
100MB CSV    | 2-5       | 200-500 MB/s
```

## 🔧 Advanced Usage

### Context Managers
```python
# Batch processing with automatic cleanup
with dataprof.PyBatchAnalyzer() as batch:
    batch.add_file("file1.csv")
    batch.add_file("file2.csv")
    results = batch.get_results()

# CSV processing with chunk handling
with dataprof.PyCsvProcessor(chunk_size=10000) as processor:
    processor.open_file("large_file.csv")
    chunks = processor.process_chunks()
```

### Logging Integration
```python
import logging

# Configure DataProf logging
dataprof.configure_logging(level="INFO")

# Analysis with integrated logging
profiles = dataprof.analyze_csv_with_logging("data.csv", log_level="DEBUG")
```

### Error Handling
```python
try:
    profiles = dataprof.analyze_csv_file("data.csv")
except RuntimeError as e:
    print(f"Analysis failed: {e}")
```

## 🛠️ Requirements

- **Python**: 3.8+ (3.9+ recommended)
- **Optional Dependencies**:
  - `pandas` - For DataFrame integration
  - `jupyter` - For rich notebook displays

## 🚀 Getting Started

1. **Install DataProf**:
   ```bash
   pip install dataprof
   ```

2. **Basic Analysis**:
   ```python
   import dataprof
   profiles = dataprof.analyze_csv_file("your_data.csv")
   ```

3. **Quality Assessment**:
   ```python
   report = dataprof.analyze_csv_with_quality("your_data.csv")
   print(f"Quality Score: {report.quality_score():.1f}%")
   ```

4. **Explore Results**: Use the rich objects and DataFrames to explore your data quality in detail.

## 📖 Next Steps

- Read the [API Reference](API_REFERENCE.md) for complete function documentation
- Check [Integrations](INTEGRATIONS.md) for pandas integration examples
- Visit the [main documentation](../../README.md) for CLI usage and Rust API

---

**💡 Tip**: Start with `analyze_csv_with_quality()` for a comprehensive overview of your data quality using ISO 8000/25012 standards.
