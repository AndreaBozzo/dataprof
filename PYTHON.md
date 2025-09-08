# DataProfiler Python Bindings

Fast, lightweight data profiling and quality assessment library for Python, powered by Rust.

## Installation

### From PyPI (Coming Soon)

```bash
pip install dataprof
```

### From Source (Development)

Requirements:
- Python 3.8+
- Rust 1.70+
- maturin

```bash
# Install maturin
pip install maturin

# Clone and build
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof
maturin develop --features python

# Or build wheel
maturin build --features python --release
pip install target/wheels/dataprof-*.whl
```

## Quick Start

```python
import dataprof

# Analyze CSV file
profiles = dataprof.analyze_csv_file("data.csv")

for profile in profiles:
    print(f"{profile.name}: {profile.data_type} (nulls: {profile.null_percentage:.1f}%)")

# Quality assessment
report = dataprof.analyze_csv_with_quality("data.csv")
print(f"Quality Score: {report.quality_score():.1f}%")
print(f"Issues: {len(report.issues)}")

# Batch processing
result = dataprof.batch_analyze_directory("/data", recursive=True)
print(f"Processed {result.processed_files} files at {result.files_per_second:.1f} files/sec")
```

## API Reference

### Single File Analysis

#### `analyze_csv_file(path: str) -> List[ColumnProfile]`
Analyze a single CSV file and return column profiles.

**Parameters:**
- `path`: Path to CSV file

**Returns:**
- List of `ColumnProfile` objects

#### `analyze_csv_with_quality(path: str) -> QualityReport`
Analyze CSV file with comprehensive quality assessment.

**Parameters:**
- `path`: Path to CSV file

**Returns:**
- `QualityReport` object with profiles and quality issues

#### `analyze_json_file(path: str) -> List[ColumnProfile]`
Analyze JSON/JSONL file and return column profiles.

**Parameters:**
- `path`: Path to JSON/JSONL file

**Returns:**
- List of `ColumnProfile` objects

### Batch Processing

#### `batch_analyze_directory(directory: str, recursive: bool = False, parallel: bool = True, max_concurrent: int = None) -> BatchResult`
Process all supported files in a directory.

**Parameters:**
- `directory`: Directory path
- `recursive`: Include subdirectories
- `parallel`: Enable parallel processing
- `max_concurrent`: Max concurrent files (default: CPU count)

**Returns:**
- `BatchResult` object with processing statistics

#### `batch_analyze_glob(pattern: str, parallel: bool = True, max_concurrent: int = None) -> BatchResult`
Process files matching a glob pattern.

**Parameters:**
- `pattern`: Glob pattern (e.g., "/data/**/*.csv")
- `parallel`: Enable parallel processing
- `max_concurrent`: Max concurrent files (default: CPU count)

**Returns:**
- `BatchResult` object with processing statistics

## Data Classes

### `ColumnProfile`
Column analysis results.

**Attributes:**
- `name: str` - Column name
- `data_type: str` - Detected data type
- `total_count: int` - Total values
- `null_count: int` - Null value count
- `unique_count: Optional[int]` - Unique value count
- `null_percentage: float` - Percentage of null values
- `uniqueness_ratio: float` - Ratio of unique values

### `QualityReport`
Comprehensive quality assessment report.

**Attributes:**
- `file_path: str` - Analyzed file path
- `total_rows: Optional[int]` - Total rows in file
- `total_columns: int` - Total columns
- `column_profiles: List[ColumnProfile]` - Column analysis results
- `issues: List[QualityIssue]` - Detected quality issues
- `rows_scanned: int` - Number of rows processed
- `sampling_ratio: float` - Sampling ratio used
- `scan_time_ms: int` - Processing time in milliseconds

**Methods:**
- `quality_score() -> float` - Calculate overall quality score (0-100)
- `issues_by_severity(severity: str) -> List[QualityIssue]` - Filter issues by severity

### `QualityIssue`
Detected data quality issue.

**Attributes:**
- `issue_type: str` - Type of issue
- `column: str` - Affected column
- `severity: str` - Issue severity (high/medium/low)
- `count: Optional[int]` - Number of affected values
- `percentage: Optional[float]` - Percentage affected
- `description: str` - Human-readable description

### `BatchResult`
Batch processing results.

**Attributes:**
- `processed_files: int` - Successfully processed files
- `failed_files: int` - Failed file count
- `total_duration_secs: float` - Total processing time
- `files_per_second: float` - Processing throughput
- `total_quality_issues: int` - Total issues found
- `average_quality_score: float` - Average quality across files

## Performance

DataProfiler is designed for high performance:

- **10-100x faster** than pandas for basic profiling
- **Parallel processing** for batch operations
- **Memory efficient** streaming for large files
- **SIMD acceleration** for numeric operations
- **Zero-copy** parsing where possible

### Benchmarks

| Dataset Size | DataProfiler | pandas.info() | Speedup |
|--------------|--------------|---------------|---------|
| 1MB CSV      | 12ms         | 150ms         | 12.5x   |
| 10MB CSV     | 85ms         | 800ms         | 9.4x    |
| 100MB CSV    | 650ms        | 6.2s          | 9.5x    |
| 1GB CSV      | 4.2s         | 45s           | 10.7x   |

## Integration Examples

### Apache Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
import dataprof

def quality_gate(**context):
    file_path = context['params']['file_path']
    report = dataprof.analyze_csv_with_quality(file_path)

    if report.quality_score() < 80.0:
        raise ValueError(f"Quality too low: {report.quality_score():.1f}%")

    return report.quality_score()

dag = DAG('data_quality', schedule_interval='@daily')
quality_check = PythonOperator(
    task_id='quality_gate',
    python_callable=quality_gate,
    params={'file_path': '/data/daily.csv'},
    dag=dag
)
```

### dbt pre-hook

```python
# dbt_project.yml
models:
  my_project:
    pre-hook: "{{ run_quality_check('{{ this }}') }}"

# macros/quality_check.sql
{% macro run_quality_check(table_name) %}
  {{ return(adapter.dispatch('run_quality_check', 'my_project')(table_name)) }}
{% endmacro %}

# macros/adapters/default__run_quality_check.sql
{% macro default__run_quality_check(table_name) %}
  {% set python_script %}
import dataprof
report = dataprof.analyze_csv_with_quality('{{ table_name }}.csv')
if report.quality_score() < 85.0:
    raise Exception(f"Quality gate failed: {report.quality_score():.1f}%")
  {% endset %}

  {{ run_python(python_script) }}
{% endmacro %}
```

### Jupyter Notebooks

```python
import dataprof
import matplotlib.pyplot as plt

# Quick quality overview
report = dataprof.analyze_csv_with_quality("dataset.csv")
print(f"📊 Quality Score: {report.quality_score():.1f}%")

# Visualize null percentages
null_percentages = [(p.name, p.null_percentage) for p in report.column_profiles]
columns, percentages = zip(*null_percentages)

plt.figure(figsize=(12, 6))
plt.bar(columns, percentages)
plt.xticks(rotation=45)
plt.ylabel('Null Percentage')
plt.title('Data Completeness by Column')
plt.show()
```

## Issue Types

DataProfiler detects various quality issues:

- **null_values** - Missing values in columns
- **duplicate_values** - Duplicate rows or values
- **outlier_values** - Statistical outliers in numeric columns
- **mixed_date_formats** - Inconsistent date formatting
- **invalid_email_format** - Malformed email addresses
- **inconsistent_casing** - Mixed case in categorical data

## Error Handling

All functions raise `PyRuntimeError` on failure:

```python
try:
    profiles = dataprof.analyze_csv_file("nonexistent.csv")
except RuntimeError as e:
    print(f"Analysis failed: {e}")
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## License

See [LICENSE](LICENSE) for details.
