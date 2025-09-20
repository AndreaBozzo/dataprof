# CLI Usage Guide

A comprehensive guide to using the DataProfiler command-line interface for data analysis, quality assessment, and ML readiness scoring.

## Quick Start

### Installation
```bash
# Install from PyPI (Python bindings)
pip install dataprof

# Or use the CLI directly (Rust binary)
cargo install dataprof
```

### Basic Commands
```bash
# Simple file analysis
dataprof data.csv

# Quality assessment with HTML report
dataprof data.csv --quality --html report.html

# ML readiness scoring
dataprof data.csv --quality --ml-score
```

## Command Reference

### Basic Syntax
```bash
dataprof [FILE|DIRECTORY|PATTERN] [OPTIONS]
```

### Essential Options

| Option | Description | Example |
|--------|-------------|---------|
| `--quality` | Enable comprehensive quality assessment | `dataprof data.csv --quality` |
| `--html` | Generate interactive HTML report | `dataprof data.csv --quality --html report.html` |
| `--ml-score` | Enable ML readiness scoring | `dataprof data.csv --quality --ml-score` |
| `--streaming` | Use streaming mode for large files | `dataprof large_file.csv --streaming` |
| `--progress` | Show progress indicators | `dataprof data.csv --progress --streaming` |
| `--format` | Output format (text, json, csv, plain) | `dataprof data.csv --format json` |

## Output Formats

### Text Output (Default)
Human-readable summary with color coding:
```bash
dataprof data.csv
```

### JSON Output
Machine-readable for integration:
```bash
dataprof data.csv --format json
```

### CSV Output
Tabular format for processing:
```bash
dataprof data.csv --format csv
```

### HTML Reports
Interactive web reports:
```bash
dataprof data.csv --quality --html report.html
```

## Performance & Engine Selection

### Engine Types

| Engine | Best For | Usage |
|--------|----------|-------|
| `auto` | Intelligent selection (recommended) | `dataprof data.csv --engine auto` |
| `streaming` | Small files (<50MB) | `dataprof data.csv --engine streaming` |
| `memory-efficient` | Medium files (50-200MB) | `dataprof data.csv --engine memory-efficient` |
| `true-streaming` | Large files (>200MB) | `dataprof data.csv --engine true-streaming` |
| `arrow` | Very large files (>500MB, requires arrow feature) | `dataprof data.csv --engine arrow` |

### Performance Tools
```bash
# Show system capabilities
dataprof --engine-info

# Compare all engines
dataprof data.csv --benchmark
```

## Advanced Features

### Sampling
```bash
# Process only first 10,000 rows
dataprof huge_file.csv --sample 10000

# Combine with streaming
dataprof huge_file.csv --sample 10000 --streaming
```

### Batch Processing
```bash
# Process directory recursively
dataprof /data/folder --recursive --quality

# Use glob patterns
dataprof --glob "data/**/*.csv" --parallel

# Control parallelism
dataprof /data --recursive --parallel --max-concurrent 4
```

### Database Analysis
```bash
# Profile a database table
dataprof table_name --database "postgresql://user:pass@host:5432/db"

# Custom SQL query
dataprof --database "postgresql://..." --query "SELECT * FROM sales"

# Quality assessment with ML scoring
dataprof table_name --database "..." --quality --ml-score
```

## Configuration

### Configuration Files
Create `.dataprof.toml` in your project:

```toml
[output]
format = "json"
colored = true
show_progress = true
verbosity = 1

[analysis]
auto_quality = true
auto_streaming = true
chunk_size = 8192

[ml]
auto_score = false
score_threshold = 0.7

[batch]
parallel = true
max_concurrent = 4
recursive = true
```

### Usage with Config
```bash
# Auto-discovery
dataprof data.csv

# Specify config file
dataprof data.csv --config .dataprof.toml
```

## Common Use Cases

### 1. Data Quality Gate (CI/CD)
```bash
# Quality check with JSON output for automation
dataprof data.csv --quality --format json --no-color > quality_report.json
echo $?  # Exit code: 0 = success, >0 = issues found
```

### 2. Large File Processing
```bash
# Memory-efficient analysis with progress
dataprof huge_dataset.csv --streaming --progress --sample 100000
```

### 3. ML Pipeline Integration
```bash
# ML readiness assessment
dataprof features.csv --quality --ml-score --format json
```

### 4. Database Monitoring
```bash
# Daily quality report
dataprof user_events --database "$DB_URL" --quality --html daily_report.html
```

### 5. Batch Quality Assessment
```bash
# Warehouse-wide quality check
dataprof /data/warehouse --recursive --parallel --quality --format json
```

## Troubleshooting

### Common Issues

#### File Not Found
```bash
❌ CRITICAL Error: File not found: data.csv
```
**Solution**: Check file path and permissions.

#### Memory Issues
```bash
⚠️ Warning: Low memory detected
```
**Solution**: Use streaming mode:
```bash
dataprof large_file.csv --streaming --chunk-size 1000
```

#### CSV Parsing Errors
```bash
🟠 HIGH Error: CSV parsing failed
```
**Solution**: Verify file format and encoding.

### Getting Help
```bash
# Detailed help
dataprof --help

# System information
dataprof --engine-info

# Debug mode
dataprof data.csv --verbosity 3
```

## Integration Examples

### Airflow DAG
```python
from airflow import DAG
from airflow.operators.bash import BashOperator

quality_check = BashOperator(
    task_id='quality_check',
    bash_command='dataprof {{ params.file }} --quality --format json',
    params={'file': '/data/daily_sales.csv'}
)
```

### GitHub Actions
```yaml
- name: Data Quality Check
  run: |
    dataprof data.csv --quality --format json > quality_report.json
    if [ $? -ne 0 ]; then
      echo "Quality issues found"
      exit 1
    fi
```

### Jupyter Notebook
```python
import subprocess
import json

# Run quality analysis
result = subprocess.run([
    'dataprof', 'dataset.csv',
    '--quality', '--ml-score', '--format', 'json'
], capture_output=True, text=True)

# Parse results
report = json.loads(result.stdout)
print(f"Quality Score: {report['summary']['quality_score']}")
```

## Version Features

### v0.4.1 (Latest)
- ✅ Enhanced CLI with comprehensive testing
- ✅ Progress indicators and validation
- ✅ Security audit integration
- ✅ Production-ready features

### v0.4.0
- ✅ Intelligent engine selection
- ✅ Performance benchmarking
- ✅ Comprehensive testing infrastructure

### v0.3.6
- ✅ Apache Arrow integration
- ✅ Columnar processing
- ✅ Performance optimizations

---

**For more detailed information, see the [full CLI Usage Guide](CLI_USAGE_GUIDE.md)**

**Repository**: https://github.com/AndreaBozzo/dataprof
**Issues**: https://github.com/AndreaBozzo/dataprof/issues
