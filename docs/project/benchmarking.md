# DataProfiler Benchmarking System

## Overview

DataProfiler implements a comprehensive benchmarking infrastructure designed to validate performance claims, ensure statistical rigor, and provide reliable CI/CD integration. The system has evolved from fragmented benchmark files into a unified, scientifically-sound testing framework.

## Architecture

### 🏗️ Benchmark Suites

#### 1. **Unified Benchmarks** (`benches/unified_benchmarks.rs`)
Consolidates previously fragmented benchmark files into a standardized testing framework.

**Purpose**: Core functionality validation with standardized datasets
**Datasets**: 7 realistic patterns (Basic, Mixed, Numeric, Wide, Deep, Unicode, Messy)
**Engines Tested**: Local CSV analysis engine
**Output**: JSON results for CI/CD integration

**Key Features**:
- Standardized dataset generation via `StandardDatasets`
- Precise timing collection with `ResultCollector`
- Memory pattern analysis across different data shapes
- Throughput measurement with row/column processing metrics

#### 2. **Domain-Specific Benchmarks** (`benches/domain_benchmarks.rs`)
Tests real-world data patterns across different use cases.

**Purpose**: Validate performance across domain-specific data patterns
**Domains**:
- **Transactions**: Financial/e-commerce data with high cardinality
- **TimeSeries**: Temporal data with timestamps and metrics
- **Streaming**: Continuous data ingestion patterns

**Key Features**:
- CSV vs JSON vs Database-style data comparison
- Engine selection accuracy validation
- Domain-specific performance characteristics
- Memory efficiency testing for streaming workloads

#### 3. **Statistical Rigor Framework** (`benches/statistical_benchmark.rs`)
Implements scientific methodology for reliable performance measurement.

**Purpose**: Ensure statistical validity of benchmark results
**Statistical Methods**:
- 95% confidence intervals with t-distribution for small samples
- IQR-based outlier detection and removal
- Coefficient of variation measurement (target <5%)
- Regression detection using confidence interval comparison

**Key Features**:
- `StatisticalConfig` for different testing environments (CI/local/regression)
- Engine selection accuracy testing (target 85% accuracy)
- Performance vs accuracy trade-off analysis
- Automated quality criteria validation

## 📊 Statistical Methodology

### Confidence Intervals
- **Small samples** (<30): t-distribution with appropriate degrees of freedom
- **Large samples** (≥30): Normal distribution approximation
- **Target confidence**: 95% for all measurements

### Outlier Detection
- **Method**: Interquartile Range (IQR) with 1.5× threshold
- **Purpose**: Remove measurement artifacts and environmental noise
- **Applied to**: All timing and memory measurements

### Variance Control
- **Metric**: Coefficient of Variation (CV = σ/μ)
- **Target**: <5% for acceptable measurement precision
- **Action**: Increase sample size if CV exceeds threshold

### Engine Selection Accuracy
- **Target**: 85% accuracy for `AdaptiveProfiler` engine selection
- **Method**: Cross-validation across dataset patterns and sizes
- **Metrics**: Precision, recall, F1-score for engine recommendations

## 🚀 CI/CD Integration

### Workflow Architecture

#### **Quick Performance Check** (8 minutes)
**Trigger**: PR/push to code paths
**Purpose**: Fast validation without blocking development
**Datasets**: Micro and Small sizes only
**Sample size**: 10 samples, 2-3 second measurement time

**Features**:
- Rust toolchain reliability with retry logic
- Environment health checks
- Timeout controls for each step
- Separate cache strategy for performance

#### **Comprehensive Performance Suite** (120 minutes)
**Trigger**: Push to main/master branches, scheduled runs
**Purpose**: Full performance validation and regression analysis
**Datasets**: All sizes (Micro, Small, Medium, Large)
**Sample size**: Full statistical rigor

**Features**:
- External tool comparison (pandas, polars, great-expectations)
- Performance regression analysis with baseline comparison
- GitHub Pages dashboard generation
- Artifact retention for trend analysis

### Result Collection

#### **JSON Output Format**
All benchmarks generate structured JSON results for programmatic analysis:

```json
{
  "benchmark_name": "unified_small_performance",
  "dataset_pattern": "Basic",
  "dataset_size": "Small",
  "file_size_mb": 2.1,
  "time_seconds": 0.045,
  "rows_processed": 10000,
  "columns_processed": 5,
  "statistical_summary": {
    "mean": 0.045,
    "std_dev": 0.002,
    "confidence_interval": [0.043, 0.047],
    "coefficient_of_variation": 0.044
  }
}
```

#### **Performance Dashboard**
Automated GitHub Pages deployment provides:
- Real-time performance metrics
- Historical trend analysis
- Regression detection alerts
- Cross-platform comparison results
- Statistical confidence reporting

## 📈 Dataset Specifications

### **Standard Datasets** (`src/testing/standard_datasets.rs`)

| Pattern | Description | Characteristics |
|---------|-------------|-----------------|
| **Basic** | Simple CSV with common data types | Strings, integers, floats |
| **Mixed** | Complex types with nulls | Mixed types, missing values |
| **Numeric** | Number-heavy scientific data | Floats, integers, calculations |
| **Wide** | Many columns, fewer rows | High cardinality, sparse data |
| **Deep** | Many rows, fewer columns | Time series, logs |
| **Unicode** | International character sets | UTF-8, special characters |
| **Messy** | Real-world dirty data | Inconsistent formats, edge cases |

### **Size Categories**

| Size | Rows | Approximate File Size | Use Case |
|------|------|----------------------|----------|
| **Micro** | 100 | ~10KB | Unit testing, CI validation |
| **Small** | 10,000 | ~1MB | Integration testing |
| **Medium** | 100,000 | ~10MB | Performance validation |
| **Large** | 1,000,000+ | ~100MB+ | Scalability testing |

## 🎯 Performance Targets

### **Speed Benchmarks**
- **Small datasets** (<1MB): <100ms processing time
- **Medium datasets** (1-10MB): <1s processing time
- **Large datasets** (10-100MB): <10s processing time
- **Memory efficiency**: <2x input file size peak memory usage

### **Accuracy Targets**
- **Engine selection**: 85% accuracy across all dataset patterns
- **Statistical confidence**: 95% confidence intervals for all measurements
- **Measurement precision**: <5% coefficient of variation

### **CI/CD Performance**
- **Quick benchmarks**: <8 minutes total runtime
- **Build time**: <5 minutes for benchmark compilation
- **Cache hit rate**: >80% for dependency caching

## 🔧 Running Benchmarks

### **Local Development**
```bash
# Run all benchmark suites
cargo bench

# Run specific suite
cargo bench --bench unified_benchmarks
cargo bench --bench domain_benchmarks
cargo bench --bench statistical_benchmark

# Quick validation (reduced samples)
cargo bench -- --sample-size 10 --measurement-time 3
```

### **Manual CI Triggers**
```bash
# Trigger quick performance check
gh workflow run quick-benchmarks.yml --ref staging

# Trigger comprehensive benchmarks
gh workflow run benchmarks.yml --ref main
```

### **Statistical Analysis**
```bash
# Run with statistical rigor
CRITERION_CONFIDENCE_LEVEL=0.95 cargo bench --bench statistical_benchmark

# Generate regression analysis
python scripts/performance_regression_check.py results.json --baseline baseline.json
```

## 📋 Benchmark Results Interpretation

### **Performance Categories**
- **🏆 OUTSTANDING**: Speed and memory both excellent (top 10%)
- **🥇 EXCELLENT**: Very fast with great memory efficiency (top 25%)
- **🥈 COMPETITIVE**: Good speed with excellent memory efficiency (top 50%)
- **💾 MEMORY CHAMPION**: Exceptional memory efficiency regardless of speed
- **⚖️ BALANCED**: Different performance trade-offs, situational advantages

### **Statistical Validation**
- **Green**: CV <3%, high statistical confidence
- **Yellow**: CV 3-5%, acceptable variance
- **Red**: CV >5%, requires investigation

### **Regression Detection**
- **No regression**: Performance within 95% confidence interval of baseline
- **Minor regression**: 5-15% degradation, investigate causes
- **Major regression**: >15% degradation, requires immediate attention

## 🛠️ Troubleshooting

### **Common Issues**

#### Criterion Sample Size Errors
```
assertion failed: num_size >= 10
```
**Solution**: Ensure `--sample-size` is at least 10 for Criterion compatibility.

#### Network Timeouts in CI
```
Connection reset by peer (os error 104)
```
**Solution**: Retry logic and fallback rustup installation implemented in CI.

#### Memory Pressure
```
benchmark exceeded memory limits
```
**Solution**: Use smaller dataset sizes or increase runner resources.

### **Performance Debugging**
1. **Profile with** `cargo bench --bench <suite> -- --profile-time 10`
2. **Memory analysis** via system profiling tools
3. **Statistical validation** with increased sample sizes
4. **Baseline comparison** using regression analysis scripts

## 🔮 Future Enhancements

### **Planned Features**
- **Multi-engine comparison**: Comprehensive testing across all profiling engines
- **Memory profiling integration**: Real-time memory usage tracking
- **Performance prediction**: ML-based performance modeling
- **Custom benchmark definitions**: User-defined benchmark scenarios
- **Real-time monitoring**: Continuous performance tracking in production

### **Statistical Improvements**
- **Bayesian analysis**: More sophisticated statistical modeling
- **Causal inference**: Understanding performance drivers
- **Anomaly detection**: Automated performance anomaly identification
- **Trend analysis**: Long-term performance pattern recognition

---

*This document provides comprehensive coverage of DataProfiler's benchmarking infrastructure. For implementation details, see the source code in `benches/` and `src/testing/`.*