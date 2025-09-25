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
print(f"Issues Found: {len(report.issues)}")
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
Process all supported files in a directory with high-performance parallel processing.

**Parameters:**
- `directory` (str): Directory path to analyze
- `recursive` (bool): Include subdirectories (default: False)
- `parallel` (bool): Enable parallel processing (default: True)
- `max_concurrent` (Optional[int]): Maximum concurrent files (default: CPU count)

**Returns:**
- `PyBatchResult` object with processing statistics

**Example:**
```python
result = dataprof.batch_analyze_directory("/data", recursive=True)
print(f"Processed {result.processed_files} files at {result.files_per_second:.1f} files/sec")
```

#### `batch_analyze_glob(pattern: str, parallel: bool = True, max_concurrent: Optional[int] = None) -> PyBatchResult`
Process files matching a glob pattern.

**Parameters:**
- `pattern` (str): Glob pattern (e.g., "/data/**/*.csv")
- `parallel` (bool): Enable parallel processing (default: True)
- `max_concurrent` (Optional[int]): Maximum concurrent files (default: CPU count)

**Returns:**
- `PyBatchResult` object with processing statistics

**Example:**
```python
result = dataprof.batch_analyze_glob("/data/**/*.csv")
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

#### `ml_readiness_score_with_logging(file_path: str, log_level: Optional[str] = None) -> PyMlReadinessScore`
Calculate ML readiness with logging integration.

## Enhanced ML Recommendations (v0.4.6+)

The `PyMlReadinessScore` object now includes enhanced recommendations with **actionable code snippets**.

### Enhanced PyMlRecommendation Properties

Each recommendation in `ml_score.recommendations` now includes:

| Property | Type | Description |
|----------|------|-------------|
| `category` | `str` | Category of the recommendation |
| `priority` | `str` | Priority level: "critical", "high", "medium", "low" |
| `description` | `str` | Human-readable description |
| `expected_impact` | `str` | Expected improvement description |
| `implementation_effort` | `str` | Implementation difficulty |
| **`code_snippet`** | `Optional[str]` | **Ready-to-use Python code** |
| **`framework`** | `Optional[str]` | **Framework used (pandas, sklearn, etc.)** |
| **`imports`** | `List[str]` | **Required import statements** |
| **`variables`** | `Dict[str, str]` | **Variables used in code** |

### Code Snippet Usage Example

```python
import dataprof

ml_score = dataprof.ml_readiness_score("data.csv")

for rec in ml_score.recommendations:
    # Check if code snippet is available
    if rec.code_snippet:
        print(f"Recommendation: {rec.description}")
        print(f"Framework: {rec.framework}")
        print(f"Imports needed: {rec.imports}")
        print(f"Code:\n{rec.code_snippet.replace('\\\\n', '\\n')}")
```

### CLI Script Generation

Use the CLI to generate complete preprocessing scripts:

```bash
# Generate script with all code snippets
dataprof data.csv --ml-score --output-script preprocess.py
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

#### `feature_analysis_dataframe(file_path: str) -> pd.DataFrame`
Return ML feature analysis as a pandas DataFrame.

**Returns:**
- DataFrame with columns: `column_name`, `feature_type`, `ml_suitability`, `importance_potential`, etc.

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
- `issues: List[PyQualityIssue]` - Detected quality issues
- `scan_time_ms: int` - Processing time in milliseconds

**Methods:**
- `quality_score() -> float` - Calculate overall quality score (0-100)
- `issues_by_severity(severity: str) -> List[PyQualityIssue]` - Filter issues by severity

### `PyQualityIssue`
Detected data quality issue.

**Attributes:**
- `issue_type: str` - Type of issue (e.g., "null_values", "duplicates")
- `column: str` - Affected column name
- `severity: str` - Issue severity ("high", "medium", "low")
- `count: Optional[int]` - Number of affected values
- `percentage: Optional[float]` - Percentage of values affected
- `description: str` - Human-readable description

### `PyBatchResult`
Batch processing results and performance metrics.

**Attributes:**
- `processed_files: int` - Successfully processed files
- `failed_files: int` - Failed file count
- `total_duration_secs: float` - Total processing time
- `files_per_second: float` - Processing throughput
- `total_quality_issues: int` - Total issues found across all files
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
