# DataProf Python API Reference

Complete reference for all DataProf Python functions and classes.

## Core Analysis Functions

### Single File Analysis

#### `analyze_csv_file(path: str) -> List[PyColumnProfile]`
Analyze a single CSV file and return detailed column profiles.

**Parameters:**
- `path` (str): Path to the CSV file

**Returns:**
- List of `PyColumnProfile` objects containing detailed analysis

**Example:**
```python
import dataprof

profiles = dataprof.analyze_csv_file("data.csv")
for profile in profiles:
    print(f"{profile.name}: {profile.data_type} (nulls: {profile.null_percentage:.1f}%)")
```

#### `analyze_csv_with_quality(path: str) -> PyQualityReport`
Analyze CSV file with comprehensive quality assessment and issue detection.

**Parameters:**
- `path` (str): Path to the CSV file

**Returns:**
- `PyQualityReport` object with profiles and detected quality issues

**Example:**
```python
report = dataprof.analyze_csv_with_quality("data.csv")
print(f"Quality Score: {report.quality_score():.1f}%")
metrics = report.data_quality_metrics
print(f"Completeness: {metrics.complete_records_ratio:.1f}%")
print(f"Missing Values: {metrics.missing_values_ratio:.1f}%")
```

#### `analyze_json_file(path: str) -> List[PyColumnProfile]`
Analyze JSON/JSONL file and return column profiles.

**Parameters:**
- `path` (str): Path to the JSON/JSONL file

**Returns:**
- List of `PyColumnProfile` objects

**Example:**
```python
profiles = dataprof.analyze_json_file("data.jsonl")
```

### Batch Processing

#### `batch_analyze_directory(directory: str, recursive: bool = False, parallel: bool = True, max_concurrent: Optional[int] = None) -> PyBatchResult`
Process all supported files (CSV, JSON/JSONL, Parquet) in a directory with high-performance parallel processing.

**Parameters:**
- `directory` (str): Directory path to analyze
- `recursive` (bool): Include subdirectories (default: False)
- `parallel` (bool): Enable parallel processing (default: True)
- `max_concurrent` (Optional[int]): Maximum concurrent files (default: CPU count)

**Returns:**
- `PyBatchResult` object with processing statistics

**Supported Formats:**
- CSV files (`.csv`)
- JSON/JSONL files (`.json`, `.jsonl`)
- Parquet files (`.parquet`) - requires `parquet` feature flag

**Example:**
```python
result = dataprof.batch_analyze_directory("/data", recursive=True)
print(f"Processed {result.processed_files} files at {result.files_per_second:.1f} files/sec")
```

#### `batch_analyze_glob(pattern: str, parallel: bool = True, max_concurrent: Optional[int] = None) -> PyBatchResult`
Process files matching a glob pattern. Supports CSV, JSON/JSONL, and Parquet formats.

**Parameters:**
- `pattern` (str): Glob pattern (e.g., "/data/**/*.csv", "/data/**/*.parquet")
- `parallel` (bool): Enable parallel processing (default: True)
- `max_concurrent` (Optional[int]): Maximum concurrent files (default: CPU count)

**Returns:**
- `PyBatchResult` object with processing statistics

**Supported Formats:**
- CSV files (`.csv`)
- JSON/JSONL files (`.json`, `.jsonl`)
- Parquet files (`.parquet`) - requires `parquet` feature flag

**Example:**
```python
# Process all CSV files
result = dataprof.batch_analyze_glob("/data/**/*.csv")

# Process all Parquet files
result = dataprof.batch_analyze_glob("/data/**/*.parquet")
```

## Python Logging Integration

#### `configure_logging(level: Optional[str] = None, format: Optional[str] = None) -> None`
Configure Python logging integration for DataProf operations.

**Parameters:**
- `level` (Optional[str]): Logging level ("DEBUG", "INFO", "WARNING", "ERROR")
- `format` (Optional[str]): Custom log format string

**Example:**
```python
dataprof.configure_logging(level="INFO")
```

#### Enhanced Analysis with Logging

#### `analyze_csv_with_logging(file_path: str, log_level: Optional[str] = None) -> List[Dict[str, Any]]`
Analyze CSV with integrated Python logging.

## Quality Metrics (ISO 8000/25012 Compliant)

DataProf provides comprehensive quality assessment across 5 ISO standard dimensions.

### `PyDataQualityMetrics`

Comprehensive quality metrics following ISO 8000/25012 standards.

**Attributes:**

#### Completeness (ISO 8000-8)
- `missing_values_ratio: float` - Percentage of missing values (0-100)
- `complete_records_ratio: float` - Percentage of complete rows (0-100)
- `null_columns: List[str]` - Columns with >50% null values

#### Consistency (ISO 8000-61)
- `data_type_consistency: float` - Type consistency percentage (0-100)
- `format_violations: int` - Count of format issues
- `encoding_issues: int` - UTF-8 encoding problems

#### Uniqueness (ISO 8000-110)
- `duplicate_rows: int` - Number of duplicate rows
- `key_uniqueness: float` - Uniqueness in key columns (0-100)
- `high_cardinality_warning: bool` - Flag for excessive unique values

#### Accuracy (ISO 25012)
- `outlier_ratio: float` - Percentage of outliers (0-100)
- `range_violations: int` - Out-of-range values
- `negative_values_in_positive: int` - Invalid negative values

#### Timeliness (ISO 8000-8)
- `future_dates_count: int` - Dates beyond current date
- `stale_data_ratio: float` - Percentage of stale data (0-100)
- `temporal_violations: int` - Temporal ordering issues

### Quality Score Calculation

```python
report = dataprof.analyze_csv_with_quality("data.csv")
metrics = report.data_quality_metrics

# Overall score (0-100)
print(f"Quality Score: {report.quality_score():.1f}%")

# Individual dimensions
print(f"Completeness: {metrics.complete_records_ratio:.1f}%")
print(f"Consistency: {metrics.data_type_consistency:.1f}%")
print(f"Uniqueness: {metrics.key_uniqueness:.1f}%")
```

## Pandas Integration

When pandas is installed, additional DataFrame-returning functions are available:

#### `analyze_csv_dataframe(file_path: str) -> pd.DataFrame`
Return column profiles as a pandas DataFrame for easier data manipulation.

**Returns:**
- DataFrame with columns: `column_name`, `data_type`, `null_count`, `null_percentage`, `unique_count`, etc.

**Example:**
```python
import pandas as pd
import dataprof

df = dataprof.analyze_csv_dataframe("data.csv")
high_null_cols = df[df['null_percentage'] > 10.0]
```

#### `quality_metrics_dataframe(report: PyQualityReport) -> pd.DataFrame`
Convert quality metrics to a pandas DataFrame for easier analysis.

**Returns:**
- DataFrame with quality dimensions and their scores

## Core Data Classes

### `PyColumnProfile`
Detailed column analysis results.

**Attributes:**
- `name: str` - Column name
- `data_type: str` - Detected data type
- `total_count: int` - Total values
- `null_count: int` - Null value count
- `unique_count: Optional[int]` - Unique value count
- `null_percentage: float` - Percentage of null values (0-100)
- `unique_percentage: float` - Percentage of unique values (0-100)

### `PyQualityReport`
Comprehensive quality assessment report.

**Attributes:**
- `file_path: str` - Analyzed file path
- `total_rows: Optional[int]` - Total rows in file
- `total_columns: int` - Total columns
- `column_profiles: List[PyColumnProfile]` - Column analysis results
- `data_quality_metrics: PyDataQualityMetrics` - ISO 8000/25012 compliant quality metrics
- `rows_scanned: int` - Number of rows scanned
- `sampling_ratio: float` - Sampling ratio used (1.0 = full scan)
- `scan_time_ms: int` - Processing time in milliseconds

**Methods:**
- `quality_score() -> float` - Calculate overall quality score (0-100)
- `to_json() -> str` - Export report as JSON string

### `PyBatchResult`
Batch processing results and performance metrics.

**Attributes:**
- `processed_files: int` - Successfully processed files
- `failed_files: int` - Failed file count
- `total_duration_secs: float` - Total processing time
- `average_quality_score: float` - Average quality score across files

## Context Managers

### `PyBatchAnalyzer`
Context manager for batch analysis with automatic resource cleanup.

**Example:**
```python
with dataprof.PyBatchAnalyzer() as batch:
    batch.add_file("file1.csv")
    batch.add_file("file2.csv")
    results = batch.get_results()
```

**Methods:**
- `add_file(path: str) -> None` - Add file to batch
- `add_temp_file(path: str) -> None` - Add temporary file (auto-cleanup)
- `get_results() -> List[Any]` - Get analysis results
- `analyze_batch(paths: List[str]) -> List[Any]` - Analyze multiple files

### `PyCsvProcessor`
Context manager for CSV processing with chunk handling.

**Example:**
```python
with dataprof.PyCsvProcessor(chunk_size=10000) as processor:
    processor.open_file("large_file.csv")
    chunks = processor.process_chunks()
```

**Methods:**
- `open_file(path: str) -> None` - Open CSV file for processing
- `process_chunks() -> List[Any]` - Process file in chunks
- `get_processing_info() -> Dict[str, Any]` - Get processing statistics

## Error Handling

All functions raise `RuntimeError` on failure:

```python
try:
    profiles = dataprof.analyze_csv_file("nonexistent.csv")
except RuntimeError as e:
    print(f"Analysis failed: {e}")
```

## Type Hints

DataProf includes complete type hints and a `py.typed` marker file for full mypy compatibility:

```bash
python -m mypy your_script.py  # No errors!
```

## Performance Notes

- Functions are implemented in Rust for high performance
- Parallel processing is enabled by default for batch operations
- Memory usage is optimized for large files
- SIMD acceleration is used for numeric operations where possible
