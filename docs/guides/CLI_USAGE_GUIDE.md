# DataProfiler CLI Usage Guide

A comprehensive guide to using the DataProfiler command-line interface for data analysis, quality assessment, and ML readiness scoring.

## Table of Contents
- [Quick Start](#quick-start)
- [Basic Usage](#basic-usage)
- [Output Formats](#output-formats)
- [Advanced Features](#advanced-features)
- [Performance & Engine Selection](#performance--engine-selection)
- [Database Analysis](#database-analysis)
- [Batch Processing](#batch-processing)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Examples](#examples)

## Quick Start

### Installation
```bash
# Install from PyPI (Python bindings)
pip install dataprof

# Or use the CLI directly (Rust binary)
cargo install dataprof
```

### Basic Analysis
```bash
# Simple file analysis
dataprof data.csv

# Quality assessment with HTML report
dataprof data.csv --quality --html report.html

# ML readiness scoring
dataprof data.csv --quality --ml-score
```

## Basic Usage

### Command Syntax
```bash
dataprof [FILE|DIRECTORY|PATTERN] [OPTIONS]
```

### Essential Options

#### File Input
- `FILE` - Single CSV, JSON, or JSONL file
- `DIRECTORY` - Directory path (use with `--recursive`)
- `PATTERN` - Glob pattern (use with `--glob`)

#### Core Functionality
```bash
# Quality assessment (detailed analysis)
dataprof data.csv --quality

# Generate HTML report
dataprof data.csv --quality --html report.html

# Enable ML readiness scoring
dataprof data.csv --quality --ml-score

# ML scoring with actionable code snippets (NEW v0.4.61)
dataprof data.csv --quality --ml-score --ml-code

# Generate complete preprocessing script (NEW v0.4.61)
dataprof data.csv --quality --ml-score --output-script preprocess.py

# Streaming mode for large files (>100MB)
dataprof large_file.csv --streaming

# Progress indicators for long operations
dataprof data.csv --progress --streaming
```

## Output Formats

### Text Output (Default)
Human-readable summary with color coding and formatting.

```bash
dataprof data.csv
```

### JSON Output
Machine-readable structured data for integration.

```bash
dataprof data.csv --format json
```

### CSV Output
Tabular format for data processing pipelines.

```bash
dataprof data.csv --format csv
```

### Plain Text
Clean text without colors for scripting.

```bash
dataprof data.csv --format plain
```

### HTML Reports
Interactive web-based reports with visualizations.

```bash
dataprof data.csv --quality --html report.html
```

## Advanced Features

### 🔧 Smart Auto-Recovery System (NEW in v0.4.61)

#### Automatic Delimiter Detection
DataProfiler now automatically detects CSV delimiters with enhanced intelligence:

```bash
# Automatic delimiter detection for any CSV format
dataprof data_semicolon.csv     # Detects ';' delimiter
dataprof data_pipe.csv          # Detects '|' delimiter
dataprof data_tab.csv           # Detects tab delimiter
dataprof data_comma.csv         # Detects ',' delimiter (default)
```

**Supported Delimiters:**
- `,` Comma (default)
- `;` Semicolon
- `|` Pipe
- `\t` Tab

**How it works:**
- Analyzes first 5 lines of your file
- Compares field count consistency across delimiters
- Chooses delimiter that produces the most fields consistently
- Falls back gracefully if detection fails

#### Enhanced Error Recovery
```bash
# Robust parsing handles malformed CSV files automatically
dataprof problematic.csv        # Auto-recovers from inconsistent field counts
dataprof mixed_formats.csv      # Handles variable column counts gracefully
```

### 🚀 Performance Intelligence & Benchmarking

#### Engine Benchmarking (NEW)
```bash
# Run comprehensive performance benchmarks
dataprof data.csv --benchmark

# Output example:
# 🏁 DataProfiler Engine Benchmark
# ✅ Streaming: 1.7s, 5912 rows/sec, 0.0MB memory
# 🎯 Best: Use Streaming for optimal performance
```

#### Memory-Aware Processing
```bash
# Enable enhanced progress with memory tracking
dataprof large_file.csv --streaming --progress

# Output shows real-time metrics:
# 🔄 Processing: 45.2% (4,520 rows, 1,250 rows/sec)
```

### 🖥️ Intelligent Terminal Detection (NEW)

#### Adaptive Output Formatting
DataProfiler automatically adapts output based on context:

```bash
# Interactive terminal: Rich output with colors and emojis
dataprof data.csv

# Piped output: Clean, machine-readable format
dataprof data.csv | head -20

# Redirected: Optimized for file output
dataprof data.csv > analysis.txt

# Force plain text in any context
dataprof data.csv --no-color
```

### Sampling for Large Files
```bash
# Process only first 10,000 rows
dataprof huge_file.csv --sample 10000

# Combine with streaming for memory efficiency
dataprof huge_file.csv --sample 10000 --streaming
```

### Custom Chunk Sizes
```bash
# Memory-constrained environments
dataprof data.csv --streaming --chunk-size 1000

# High-memory systems
dataprof data.csv --streaming --chunk-size 10000
```

### Verbosity Levels
```bash
# Quiet mode (errors only)
dataprof data.csv -v 0

# Normal output
dataprof data.csv -v 1

# Verbose with detailed progress
dataprof data.csv -vv

# Debug mode with trace information
dataprof data.csv -vvv
```

### No Color Output
```bash
# Disable colored output
dataprof data.csv --no-color

# Or set environment variable
NO_COLOR=1 dataprof data.csv
```

## Performance & Engine Selection

### Engine Types

#### Auto Mode (Recommended)
Intelligent automatic selection based on file characteristics.

```bash
dataprof data.csv --engine auto  # Default behavior
```

#### Manual Engine Selection
```bash
# Standard streaming (small files <50MB)
dataprof data.csv --engine streaming

# Memory-efficient (medium files 50-200MB)
dataprof data.csv --engine memory-efficient

# True streaming (large files >200MB)
dataprof data.csv --engine true-streaming

# Apache Arrow columnar (requires arrow feature, >500MB)
dataprof data.csv --engine arrow
```

### Performance Tools

#### Engine Information
```bash
# Show system capabilities and recommendations
dataprof --engine-info
```

#### Benchmarking
```bash
# Compare all engines on your data
dataprof data.csv --benchmark
```

#### Performance Monitoring
```bash
# Enable performance logging
dataprof data.csv --engine auto --progress --streaming
```

## Database Analysis

> **Note**: Requires compilation with `--features database` or default build includes postgres, mysql, sqlite support.

### Basic Database Profiling
```bash
# Profile a table
dataprof table_name --database "postgresql://user:pass@host:5432/db"

# Custom SQL query
dataprof --database "postgresql://user:pass@host:5432/db" --query "SELECT * FROM sales WHERE date > '2024-01-01'"
```

### Database Configuration
```bash
# Batch size for large tables
dataprof table_name --database "..." --batch-size 5000

# Quality assessment with HTML report
dataprof table_name --database "..." --quality --html db_report.html

# ML readiness for database tables
dataprof table_name --database "..." --quality --ml-score
```

### Supported Databases
- **PostgreSQL**: `postgresql://user:pass@host:port/database`
- **MySQL**: `mysql://user:pass@host:port/database`
- **SQLite**: `sqlite://path/to/database.db`
- **DuckDB**: `duckdb://path/to/database.duckdb`

## Batch Processing

### Directory Processing
```bash
# Process all files in directory
dataprof /data/folder --quality

# Recursive processing
dataprof /data/folder --recursive --quality

# Parallel processing
dataprof /data/folder --recursive --parallel --progress
```

### Glob Pattern Processing
```bash
# All CSV files in subdirectories
dataprof --glob "data/**/*.csv" --quality

# Multiple file types
dataprof --glob "data/**/*.{csv,json}" --parallel

# Specific pattern matching
dataprof --glob "sales_*_2024.csv" --quality --format json
```

### Batch Configuration
```bash
# Control parallelism (default: auto-detect CPU cores)
dataprof /data --recursive --parallel --max-concurrent 4

# Progress tracking for batch operations
dataprof /data --recursive --progress --quality
```

## Configuration

### Configuration Files
DataProfiler supports TOML configuration files for persistent settings.

#### Auto-discovery
The CLI automatically searches for `.dataprof.toml` in:
1. Current directory
2. Parent directories (up to root)
3. Home directory

#### Manual Configuration
```bash
# Specify config file
dataprof data.csv --config ~/.dataprof.toml

# Example config loading
dataprof data.csv --config .dataprof.toml --verbose
```

#### Sample Configuration
Create `.dataprof.toml`:

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
extensions = ["csv", "json", "jsonl"]
exclude_patterns = ["**/.*", "**/*tmp*"]
```

### Environment Variables
```bash
# Disable colored output
export NO_COLOR=1

# Default configuration directory
export DATAPROF_CONFIG_DIR=~/.config/dataprof
```

## Troubleshooting

### Common Issues

#### File Not Found
```bash
❌ CRITICAL Error: File not found: data.csv
📂 Looking in directory: /current/path
🔍 Similar files found:
   • sample_data.csv
   • test_data.csv
```

**Solution**: Check file path and permissions.

#### Memory Issues
```bash
⚠️ Warning: Low memory detected - streaming engines recommended
```

**Solution**: Use streaming mode with smaller chunk sizes.
```bash
dataprof large_file.csv --streaming --chunk-size 1000
```

#### CSV Parsing Errors
```bash
🟠 HIGH Error: CSV parsing failed - delimiter detection failed
💡 Suggestion: Try specifying delimiter manually or check file encoding
```

**Solution**: Verify file format and encoding.

#### Permission Denied
```bash
❌ Error: Permission denied
```

**Solution**: Check file permissions or run with appropriate privileges.

### Getting Help
```bash
# Show detailed help
dataprof --help

# Show version information
dataprof --version

# System and engine information
dataprof --engine-info

# For additional support
# Visit: https://github.com/AndreaBozzo/dataprof/issues
```

### Debug Mode
```bash
# Enable debug logging
dataprof data.csv --verbosity 3

# Performance debugging
dataprof data.csv --benchmark --verbosity 2
```

## Examples

### 1. NEW: Multi-Delimiter CSV Processing

#### Working with Different CSV Formats
```bash
# European CSV (semicolon-separated)
dataprof european_data.csv
# 📁 european_data.csv | 4 columns
# Column: name, age, salary, city

# Unix/Database export (pipe-separated)
dataprof database_export.csv
# 📁 database_export.csv | 4 columns
# Column: id, product, price, category

# Tab-separated values
dataprof spreadsheet_export.tsv
# 📁 spreadsheet_export.tsv | 4 columns
# Column: date, value, source, notes
```

#### Real-world Example: Mixed Data Sources
```bash
# Analyze multiple files with different formats automatically
dataprof sales_europe.csv      # Semicolon-separated
dataprof sales_usa.csv         # Comma-separated
dataprof sales_asia.csv        # Pipe-separated

# All detected automatically without manual configuration!
```

### 2. Performance Optimization Workflow

#### Step 1: Benchmark Your Data
```bash
# First, understand your data's performance characteristics
dataprof large_dataset.csv --benchmark

# Example output:
# 🏁 DataProfiler Engine Benchmark
# File: large_dataset.csv
# ✅ Streaming: 2.1s, 4,762 rows/sec, 0.0MB memory
# 🎯 Best: Recommendation: Use Streaming for optimal performance
```

#### Step 2: Apply Recommendations
```bash
# Use the recommended engine for production
dataprof large_dataset.csv --engine streaming --progress

# Monitor real-time performance:
# 🔄 Processing: 67.3% (67,300 rows, 4,850 rows/sec)
```

#### Step 3: Quality Analysis with Performance Tracking
```bash
# Full analysis with memory intelligence
dataprof large_dataset.csv --quality --streaming --progress --html report.html

# Generates comprehensive report with performance metrics
```

### 3. Enhanced Error Handling Examples

#### Problematic Files (Auto-Recovery)
```bash
# File with inconsistent field counts
dataprof messy_data.csv
# ⚠️ Strict CSV parsing failed: found record with 4 fields, previous has 3 fields. Trying flexible parsing...
# 📁 messy_data.csv | 3 columns (recovered)

# Mixed line endings or encoding issues
dataprof international_data.csv
# Automatically handles UTF-8, Latin-1, CP1252 encoding detection
```

### 4. Quick Data Assessment
```bash
# Basic file overview
dataprof sales_data.csv

# Output:
# 📊 DataProfiler - Standard Analysis
# 📁 sales_data.csv | 15.3 MB | 8 columns
#
# Column: customer_id
#   Type: Integer
#   Records: 50000
#   Nulls: 0
#   Min: 1.00
#   Max: 10000.00
#   Mean: 5000.50
```

### 2. Quality Gate for CI/CD
```bash
# Quality check with specific format for automation
dataprof data.csv --quality --format json --no-color > quality_report.json

# Exit code indicates quality level
echo $?  # 0 = success, >0 = issues found
```

### 3. Large File Analysis
```bash
# Memory-efficient processing of large files
dataprof huge_dataset.csv --streaming --progress --sample 100000

# Output with progress:
# 🔄 Processing: 45.2% (45,200 rows, 2,341 rows/sec)
```

### 4. ML Pipeline Integration & Code Generation (NEW in v0.4.61)

#### ML Readiness Assessment
```bash
# Basic ML readiness scoring
dataprof features.csv --quality --ml-score

# ML score with actionable code snippets
dataprof features.csv --quality --ml-score --ml-code

# Generate complete preprocessing script
dataprof features.csv --quality --ml-score --output-script preprocess.py
```

#### Code Generation Features
```bash
# Interactive ML recommendations with Python code
dataprof data.csv --ml-score --ml-code

# Example output with code snippets:
# 🐍 Code Snippets (3 recommendations):
# 1. MISSING_VALUES [high] - Handle missing values in salary column
#    📦 Framework: pandas
#    📥 Imports: pandas, sklearn.impute
#    💻 Code:
#    # Fill missing values with median
#    df['salary'] = df['salary'].fillna(df['salary'].median())

# Complete pipeline generation
dataprof large_dataset.csv --ml-score --ml-code --output-script pipeline.py
# 🐍 Preprocessing script saved to: pipeline.py
#    Ready to use with: python pipeline.py
```

#### JSON Output for Integration
```bash
# Machine-readable ML assessment
dataprof features.csv --quality --ml-score --format json

# Example output structure:
# {
#   "summary": {
#     "ml_readiness": {
#       "score": 85.3,
#       "level": "Good",
#       "recommendations": [...]
#     }
#   }
# }
```

### 5. Database Quality Monitoring
```bash
# Regular quality checks on production database
dataprof user_events --database "$DATABASE_URL" --quality --html daily_report.html

# Automated quality monitoring
dataprof --database "$DATABASE_URL" --query "
  SELECT * FROM transactions
  WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
" --quality --format csv
```

### 6. Batch Quality Assessment
```bash
# Quality assessment across entire data warehouse
dataprof /data/warehouse --recursive --parallel --quality --format json > warehouse_quality.json

# Process specific file patterns
dataprof --glob "logs/**/*_2024-*.csv" --parallel --progress --quality
```

### 7. Performance Optimization
```bash
# Find optimal engine for your data
dataprof data.csv --benchmark

# Output:
# 🏁 DataProfiler Engine Benchmark
# 📊 Benchmark Results:
# 🥇 Arrow
#    Time: 2.45s
#    Speed: 20,408 rows/sec
# 🥈 MemoryEfficient
#    Time: 3.12s
#    Speed: 16,025 rows/sec
```

### 8. Configuration-Driven Analysis
```bash
# Using configuration file for consistent analysis
dataprof data.csv --config production.toml

# production.toml enables:
# - Quality assessment
# - Progress indicators
# - JSON output
# - ML scoring
# - Parallel processing
```

---

## Version Information

This guide covers DataProfiler CLI v0.4.61+ with the complete Enhancement #79 feature set.

For the latest updates and documentation, visit: https://github.com/AndreaBozzo/dataprof

### Key Features by Version
- **v0.4.61**: 🎯 **Enhancement #79** - Complete UX & Intelligence Overhaul
  - Intelligent Terminal Detection & Adaptive Output Formatting
  - Smart Auto-Recovery System with Enhanced Delimiter Detection
  - Real-time Performance Intelligence & Memory-Aware Processing
  - Enhanced Progress Indicators with Memory Tracking
  - Comprehensive Engine Benchmarking System
  - API Improvements & Backward Compatibility
- **v0.4.1**: Enhanced CLI, progress indicators, comprehensive testing
- **v0.4.0**: Intelligent engine selection, performance benchmarking
- **v0.3.6**: Apache Arrow integration, columnar processing
- **v0.3.5**: Database connectors, memory safety
- **v0.3.0**: Python bindings, batch processing, streaming architecture
