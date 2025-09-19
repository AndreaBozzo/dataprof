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

# Check ML readiness
ml_score = dataprof.ml_readiness_score("data.csv")
print(f"ML Ready: {ml_score.is_ml_ready()} ({ml_score.overall_score:.1f}%)")
```

## 📋 Features

- **🔥 High Performance**: Rust-powered analysis with SIMD acceleration
- **📊 Comprehensive Profiling**: Data types, nulls, distributions, quality issues
- **🤖 ML Readiness**: Advanced ML suitability assessment and recommendations
- **🐼 Pandas Integration**: Native DataFrame support when pandas is available
- **⚡ Batch Processing**: Parallel processing for multiple files
- **📱 Jupyter Support**: Rich HTML displays in notebooks
- **🔍 Quality Assessment**: Automated data quality issue detection
- **🎯 Type Safety**: Complete type hints with mypy compatibility

## 🎯 Use Cases

### Data Quality Assessment
```python
# Comprehensive quality report
report = dataprof.analyze_csv_with_quality("customer_data.csv")
print(f"Found {len(report.issues)} quality issues")
for issue in report.issues[:5]:  # Show top 5 issues
    print(f"• {issue.severity}: {issue.description}")
```

### ML Dataset Preparation
```python
# Check if data is ready for machine learning
ml_score = dataprof.ml_readiness_score("features.csv")
if ml_score.is_ml_ready():
    print("✅ Dataset is ML-ready!")
else:
    print("⚠️ Dataset needs preprocessing:")
    for rec in ml_score.recommendations_by_priority("high"):
        print(f"  • {rec.description}")
```

### Batch File Analysis
```python
# Process multiple files in parallel
result = dataprof.batch_analyze_directory("/data", recursive=True)
print(f"Processed {result.processed_files} files at {result.files_per_second:.1f} files/sec")
print(f"Average quality: {result.average_quality_score:.1f}%")
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
- **[ML Features](ML_FEATURES.md)** - ML readiness assessment and recommendations
- **[Integrations](INTEGRATIONS.md)** - Pandas, scikit-learn, and ecosystem integrations

### Key Functions
- `analyze_csv_file()` - Basic column profiling
- `analyze_csv_with_quality()` - Quality assessment with issue detection
- `ml_readiness_score()` - ML suitability analysis
- `batch_analyze_directory()` - High-performance batch processing

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

3. **Check ML Readiness**:
   ```python
   ml_score = dataprof.ml_readiness_score("your_data.csv")
   print(f"ML Ready: {ml_score.is_ml_ready()}")
   ```

4. **Explore Results**: Use the rich objects and DataFrames to explore your data quality and ML readiness in detail.

## 📖 Next Steps

- Read the [API Reference](API_REFERENCE.md) for complete function documentation
- Explore [ML Features](ML_FEATURES.md) for advanced ML workflow integration
- Check [Integrations](INTEGRATIONS.md) for pandas and scikit-learn examples
- Visit the [main documentation](../../README.md) for CLI usage and Rust API

---

**💡 Tip**: Start with `analyze_csv_with_quality()` for a comprehensive overview of your data, then use `ml_readiness_score()` to assess ML suitability.