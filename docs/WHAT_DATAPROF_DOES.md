# What DataProf Does: Complete Transparency Guide

> **Last Updated:** 2025-10-02 | **Version:** 0.4.70

## 🔒 TL;DR - Privacy First

**DataProf processes ALL data locally on your machine. Zero telemetry, zero external data transmission.**

- ✅ 100% local processing
- ✅ No data sent to external servers
- ✅ No telemetry or usage tracking
- ✅ No hidden caches or storage
- ✅ Open source and fully auditable

---

## Table of Contents

1. [Overview](#overview)
2. [Column-Level Analysis](#1-column-level-analysis)
3. [Data Quality Scoring](#2-data-quality-scoring-iso-800025012-compliant)
4. [Quality Checks](#3-quality-checks)
5. [Pattern Detection](#4-pattern-detection)
6. [Data Type Inference](#5-data-type-inference)
7. [External Connections](#6-external-connections)
8. [Data Storage & Memory](#7-data-storage--memory)
9. [Privacy Guarantees](#8-privacy-guarantees)

---

## Overview

This document provides **complete transparency** about what DataProf analyzes when you process your data files. Every metric, calculation, and data point is documented here with source code references for independent verification.

**Audit Status:** ✅ Complete codebase audit performed on 2025-10-02

---

## 1. Column-Level Analysis

### 1.1 Numeric Statistics

**Source:** [src/stats/numeric.rs](../src/stats/numeric.rs)
**Function:** `calculate_numeric_stats()`

**Metrics Calculated:**

| Metric | Type | Description        | Data Used                    |
| ------ | ---- | ------------------ | ---------------------------- |
| `min`  | f64  | Minimum value      | All numeric values in column |
| `max`  | f64  | Maximum value      | All numeric values in column |
| `mean` | f64  | Arithmetic average | All numeric values in column |

**How it works:**
- Values are parsed as floating-point numbers
- Empty/null values are excluded
- Calculated on-the-fly during streaming
- **No data stored** - results returned in memory only

---

### 1.2 Text Statistics

**Source:** [src/stats/text.rs](../src/stats/text.rs)
**Function:** `calculate_text_stats()`

**Metrics Calculated:**

| Metric       | Type  | Description          | Data Used                                |
| ------------ | ----- | -------------------- | ---------------------------------------- |
| `min_length` | usize | Shortest text length | Character count of non-empty strings     |
| `max_length` | usize | Longest text length  | Character count of non-empty strings     |
| `avg_length` | f64   | Average text length  | Character count of all non-empty strings |

**How it works:**
- Character length counted (not byte length)
- Empty strings excluded from calculations
- **No actual text content stored** - only length metrics

---

### 1.3 Streaming Statistics

**Source:** [src/core/streaming_stats.rs](../src/core/streaming_stats.rs)
**Class:** `StreamingStatistics`

**Advanced Metrics:**

| Metric         | Description                    | Memory Impact            |
| -------------- | ------------------------------ | ------------------------ |
| `count`        | Total values processed         | Minimal (counter)        |
| `null_count`   | Number of null/empty values    | Minimal (counter)        |
| `sum`          | Sum of numeric values          | Minimal (accumulator)    |
| `sum_squares`  | For variance calculation       | Minimal (accumulator)    |
| `variance`     | Statistical variance           | Calculated from sums     |
| `std_dev`      | Standard deviation             | Calculated from variance |
| `unique_count` | Approximate unique value count | Max 10,000 values cached |

**Memory Safeguards:**
- Max unique values tracked: **10,000** (prevents memory exhaustion)
- Automatic memory pressure detection
- Memory usage capped at **100MB** by default
- Older samples discarded when limit reached

---

## 2. Data Quality Scoring (ISO 8000/25012 Compliant)

### 2.1 Overall Quality Score Calculation

**Source:** [src/types.rs](../src/types.rs)
**Class:** `DataQualityMetrics`

**Formula:**
```
Overall Score = (
    Completeness × 30% +
    Consistency × 25% +
    Uniqueness × 20% +
    Accuracy × 15% +
    Timeliness × 10%
)
```

**All components are percentages (0-100), following ISO 8000/25012 international standards for data quality.**

---

### 2.2 Quality Dimensions (ISO 8000/25012)

#### 2.2.1 Completeness (ISO 8000-8) - Weight: 30%

**Measures:** Presence of all required data

**Metrics Calculated:**

| Metric                   | Description                 | Calculation                        |
| ------------------------ | --------------------------- | ---------------------------------- |
| `missing_values_ratio`   | % of null cells             | (null_cells / total_cells) × 100   |
| `complete_records_ratio` | % of rows with no nulls     | (complete_rows / total_rows) × 100 |
| `null_columns`           | List of mostly-null columns | Columns with > 50% nulls           |

**Quality Thresholds:**

| Missing % | Rating       |
| --------- | ------------ |
| < 5%      | Excellent ✅  |
| 5-10%     | Very Good    |
| 10-20%    | Good         |
| 20-30%    | Fair ⚠️       |
| 30-50%    | Poor ❌       |
| ≥ 50%     | Very Poor ❌❌ |

---

#### 2.2.2 Consistency (ISO 8000-61) - Weight: 25%

**Measures:** Data type and format uniformity

**Metrics Calculated:**

| Metric                  | Description                     | Detection Method          |
| ----------------------- | ------------------------------- | ------------------------- |
| `data_type_consistency` | % values matching inferred type | Type validation per value |
| `format_violations`     | Count of format issues          | Mixed date/number formats |
| `encoding_issues`       | UTF-8 problems                  | Looks for � and artifacts |

**Example Issues:**
- Column has both integers and strings
- Dates in both "YYYY-MM-DD" and "DD/MM/YYYY"
- UTF-8 encoding problems (� characters)

---

#### 2.2.3 Uniqueness (ISO 8000-110) - Weight: 20%

**Measures:** Appropriate level of value uniqueness

**Metrics Calculated:**

| Metric                     | Description                   | Method                     |
| -------------------------- | ----------------------------- | -------------------------- |
| `duplicate_rows`           | Count of exact row duplicates | Hash-based detection       |
| `key_uniqueness`           | % unique in ID columns        | Unique count / total count |
| `high_cardinality_warning` | Flag for > 95% unique         | Boolean flag               |

**Quality Thresholds:**
- **Duplicate rows:** Reports if > 5% duplicate values
- **High cardinality:** Warns if > 95% unique values (possible ID column)

---

#### 2.2.4 Accuracy (ISO 25012) - Weight: 15%

**Measures:** Correctness and validity of data values

**Metrics Calculated:**

| Metric                        | Description                             | Method                |
| ----------------------------- | --------------------------------------- | --------------------- |
| `outlier_ratio`               | % of outlier values                     | IQR method (k=1.5)    |
| `range_violations`            | Count of out-of-range values            | Domain-specific rules |
| `negative_values_in_positive` | Negative values in positive-only fields | Domain knowledge      |

**Outlier Detection:** IQR (Interquartile Range) method - ISO 25012 standard
- Calculate Q1 (25th percentile) and Q3 (75th percentile)
- Calculate IQR = Q3 - Q1
- Values outside [Q1 - 1.5×IQR, Q3 + 1.5×IQR] are outliers

**Domain-Specific Range Rules:**

| Field Type        | Valid Range | Examples                |
| ----------------- | ----------- | ----------------------- |
| `age`             | 0 - 150     | Age in years            |
| `percent`, `rate` | 0 - 100     | Percentages             |
| `count`           | ≥ 0         | Counts must be positive |
| `year`            | 1900 - 2100 | Reasonable year range   |

---

#### 2.2.5 Timeliness (ISO 8000-8) - Weight: 10%

**Measures:** Freshness and temporal validity of data

**Metrics Calculated:**

| Metric                | Description                               | Calculation                    |
| --------------------- | ----------------------------------------- | ------------------------------ |
| `future_dates_count`  | Number of dates beyond current date       | Date > today                   |
| `stale_data_ratio`    | % of dates older than staleness threshold | (stale_dates / total) × 100    |
| `temporal_violations` | Temporal ordering violations              | end_date < start_date detected |

**Staleness Threshold:** Dates older than 5 years from current date (configurable)

**Example Issues:**
- Future dates in historical data
- Records from before expected collection period
- End dates before start dates

---

## 3. Quality Checks

### 3.1 Individual Quality Checks

**Source:** [src/analysis/metrics.rs](../src/analysis/metrics.rs)
**Class:** `MetricsCalculator`

#### 3.1.1 Null Values Detection

**Method:** `check_nulls()`

**Detects:**
- Empty strings (`""`)
- Null values
- Whitespace-only values

**Reports:**
- Count of null values
- Percentage of nulls per column

---

#### 3.1.2 Mixed Date Formats

**Method:** `check_date_formats()`

**Patterns Recognized:**
1. `YYYY-MM-DD` (ISO 8601)
2. `DD/MM/YYYY` (European)
3. `DD-MM-YYYY`
4. `MM/DD/YYYY` (US)
5. `YYYY/MM/DD`

**Detection Logic:**
- Samples first **100 values** per column
- Checks if > 10% match date patterns
- Reports if > 1 format detected in same column

**Example Issue:**
```
Column "date" has mixed formats:
  - 45 values as "2024-01-15"
  - 32 values as "15/01/2024"
```

---

#### 3.1.3 Duplicate Detection

**Detects:** Non-unique values in columns expected to be unique

**Threshold:** Reports if > 5% duplicate values

**Reports:**
- Count of duplicate values
- Percentage of duplicates

**Note:** Does NOT report which specific values are duplicated (privacy-preserving)

---

#### 3.1.4 Outlier Detection (ISO 25012 Compliant)

**Method:** `detect_outliers_in_column()` in MetricsCalculator

**Algorithm:** IQR (Interquartile Range) method - ISO 25012 standard

**Detection:**
1. Calculate Q1 (25th percentile) and Q3 (75th percentile)
2. Calculate IQR = Q3 - Q1
3. Values outside [Q1 - k×IQR, Q3 + k×IQR] are outliers
4. Default k = 1.5 (ISO 25012 standard)
5. Requires ≥ 4 numeric values to operate (configurable)

**Configurable Thresholds:**
- **IQR multiplier (k)**: 1.5 (default), 1.0 (strict), 2.0 (lenient)
- **Minimum samples**: 4 (default), 10 (strict), 4 (lenient)

**Reports:**
- First **10 outlier values** (for privacy)
- Row numbers where outliers found

**Example:**
```
Column "age": 3 outliers detected (IQR method, k=1.5)
  - Row 42: 250 (Q1=30, Q3=40, IQR=10)
  - Row 128: -5
  - Row 201: 180
```

**Why IQR instead of 3-sigma:**
- **More robust**: Not affected by extreme outliers
- **ISO 25012 compliant**: International standard for data quality
- **Configurable**: Industry-specific thresholds (finance: k=1.0, marketing: k=2.0)

---

#### 3.1.5 Mixed Types Detection

**Detects:** Columns with inconsistent data types

**Example:** Column has both `"123"` (numeric) and `"abc"` (text)

**Reports:** Count and examples of type mismatches

---

### 3.2 Configuration & Thresholds

**Source:** [src/core/config.rs](../src/core/config.rs) - `IsoQualityThresholds`

**Available Threshold Profiles:**
- **Default**: Balanced thresholds for general use
- **Strict**: High-compliance industries (finance, healthcare)
- **Lenient**: Exploratory/marketing data analysis

| Threshold        | Default | Strict | Lenient | ISO Standard |
| ---------------- | ------- | ------ | ------- | ------------ |
| Max null %       | 50%     | 30%    | 70%     | ISO 8000-8   |
| IQR multiplier   | 1.5     | 1.5    | 2.0     | ISO 25012    |
| High cardinality | 95%     | 98%    | 90%     | ISO 8000-110 |
| Type consistency | 95%     | 98%    | 90%     | ISO 8000-61  |

**Note:** All dimension metrics and calculations are detailed in Section 2.

---

## 4. Pattern Detection

**Source:** [src/analysis/patterns.rs](../src/analysis/patterns.rs)
**Function:** `detect_patterns()`

### Patterns Detected

| Pattern        | Regex Pattern                                                    | Match Threshold |
| -------------- | ---------------------------------------------------------------- | --------------- |
| **Email**      | `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`               | > 5%            |
| **Phone (US)** | `^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$`     | > 5%            |
| **Phone (IT)** | `^\+39\|0039\|39?[-.\s]?[0-9]{2,4}[-.\s]?[0-9]{5,10}$`           | > 5%            |
| **URL**        | `^https?://[^\s/$.?#].[^\s]*$`                                   | > 5%            |
| **UUID**       | `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$` | > 5%            |

### How It Works

1. **Sampling:** Tests patterns against non-empty column values
2. **Threshold:** Only reports patterns matching > 5% of values
3. **Privacy:** Only stores match **count and percentage**, NOT actual values

**Example Output:**
```
Column "contact": Email pattern detected (87.3% match)
Column "id": UUID pattern detected (100% match)
```

**What is NOT stored:**
- ❌ Actual email addresses
- ❌ Actual phone numbers
- ❌ Actual URLs
- ✅ Only match statistics (count, percentage)

---

## 5. Data Type Inference

**Source:** [src/analysis/inference.rs](../src/analysis/inference.rs)
**Function:** `infer_type()`

### Detection Priority Order

Checks in this order (first match wins):

1. **Date**
   - Patterns: `YYYY-MM-DD`, `DD/MM/YYYY`, `DD-MM-YYYY`
   - Threshold: > 80% of values match date pattern
   - Result: `DataType::Date`

2. **Integer**
   - Test: All non-empty values parse as `i64`
   - Result: `DataType::Integer`

3. **Float**
   - Test: All non-empty values parse as `f64`
   - Result: `DataType::Float`

4. **String** (default)
   - Fallback: If no other type matches
   - Result: `DataType::String`

### What is Stored

**Only the type enum value:**
- ✅ `DataType::Integer`
- ✅ `DataType::Float`
- ✅ `DataType::Date`
- ✅ `DataType::String`

**NOT stored:**
- ❌ Actual data values
- ❌ Sample values
- ❌ Type inference intermediate results

---

## 6. External Connections

### 6.1 Network Library Audit

**Audit Date:** 2025-09-30

**HTTP Client Libraries:**
```bash
$ grep -r "reqwest\|hyper\|ureq\|curl" Cargo.toml src/
# Result: No matches found ✅
```

**Telemetry/Analytics:**
```bash
$ grep -i "telemetry\|analytics\|tracking" Cargo.toml src/
# Result: No matches found ✅
```

**Network Dependencies:**
- `url` crate: Used ONLY for parsing database connection strings (local parsing, zero network calls)

**Conclusion:** ✅ **Zero external data transmission capability**

---

### 6.2 Database Connections (Optional)

**Feature Flags:** `postgres`, `mysql`, `sqlite`

**Purpose:** Read data FROM user's databases for local analysis

**Access Mode:** **READ-ONLY**

**Data Flow:**
```
User's Database → dataprof (local analysis) → Results (local memory/files)
```

**Supported Databases:**
- PostgreSQL (`feature = "postgres"`)
- MySQL (`feature = "mysql"`)
- SQLite (`feature = "sqlite"`)

**What Happens:**
1. User provides connection string
2. dataprof connects to database
3. Reads specified table/query
4. Analyzes data locally
5. Returns results

**What Does NOT Happen:**
- ❌ No data written back to database
- ❌ No data sent to external services
- ❌ No schema modifications
- ❌ No stored procedures created

---

## 7. Data Storage & Memory

### 7.1 Permanent Storage

**What is stored permanently:**

❌ **NOTHING** is stored permanently unless explicitly requested by user.

---

### 7.2 Temporary In-Memory Storage

**During analysis only (cleared after completion):**

| Data Type                | Max Size             | Purpose              | Cleared When      |
| ------------------------ | -------------------- | -------------------- | ----------------- |
| Column samples           | 1,000 values/column  | Pattern detection    | Analysis complete |
| Unique values            | 10,000 values/column | Cardinality analysis | Analysis complete |
| Statistical accumulators | Minimal              | Running sums/counts  | Analysis complete |
| Streaming buffer         | 100MB default        | Chunk processing     | After each chunk  |

**Memory Safeguards:**
- Automatic memory pressure detection
- Adaptive sample size reduction
- Configurable memory limits
- Source: [src/core/memory_tracker.rs](../src/core/memory_tracker.rs)

---

### 7.3 User-Controlled File Outputs

**Files are ONLY written when explicitly requested:**

| CLI Flag                 | Output Type                 | Example                  |
| ------------------------ | --------------------------- | ------------------------ |
| `--html <path>`          | HTML report                 | `report.html`            |
| `--output <path>`        | JSON/CSV export             | `results.json`           |
| `--format json`          | JSON format                 | Combined with `--output` |
| `--output-script <path>` | Python preprocessing script | `preprocess.py`          |

**Default behavior (no flags):** Results printed to terminal only, nothing written to disk.

---

### 7.4 No Hidden Caches

**Explicitly verified:**
- ❌ No `.dataprof/` cache directory
- ❌ No temporary file creation (except system temp for tests)
- ❌ No background processes
- ❌ No persistent state between runs

**Each run is completely independent.**

---

## 8. Privacy Guarantees

### ✅ What We Guarantee

1. **100% Local Processing**
   - All analysis runs entirely on your machine
   - No cloud services, no external APIs
   - Verifiable: No HTTP client libraries in dependencies

2. **Zero External Transmission**
   - No data sent to any external server
   - No telemetry or usage statistics
   - No crash reports or error tracking
   - Verifiable: Audit `Cargo.toml` and source code

3. **No Telemetry**
   - No usage tracking
   - No feature analytics
   - No performance metrics sent externally
   - Verifiable: Search codebase for tracking code

4. **No Hidden Storage**
   - Only writes files you explicitly request (`--html`, `--output`)
   - No hidden caches or temporary data stores
   - No persistent state between runs
   - Verifiable: Monitor file system during execution

5. **Read-Only Database Access** (when using database features)
   - Only reads from your databases
   - Never writes to databases
   - No schema modifications
   - Verifiable: Check SQL query logs

6. **Open Source Transparency**
   - All code publicly available: https://github.com/AndreaBozzo/dataprof
   - MIT License - fully auditable
   - Community can verify all claims

---

### 🔒 Data Flow Diagram

```
┌─────────────────────┐
│  Your Data Source   │
│  (CSV/JSON/DB)      │
└──────────┬──────────┘
           │
           │ Read only
           ▼
┌─────────────────────┐
│   DataProf Engine   │
│  (Local Analysis)   │
│                     │
│  • Statistics       │
│  • ML Scoring       │
│  • Quality Checks   │
│  • Pattern Detection│
└──────────┬──────────┘
           │
           │ Results in memory
           ▼
┌─────────────────────┐
│   Output            │
│  (Your Choice)      │
│                     │
│  • Terminal         │
│  • HTML Report      │
│  • JSON Export      │
│  • Python Script    │
└─────────────────────┘

❌ NO connection to external servers
❌ NO telemetry or tracking
❌ NO cloud storage
```

---

### ❌ What DataProf Does NOT Do

1. **No External Servers**
   - Does not send data to any external service
   - Does not connect to cloud APIs
   - Does not use remote processing

2. **No Tracking**
   - Does not track usage patterns
   - Does not collect analytics
   - Does not report errors externally

3. **No Hidden Behavior**
   - Does not create hidden files or caches
   - Does not run background processes
   - Does not persist state between runs

4. **No Data Modification**
   - Does not modify original files
   - Does not alter database contents
   - Read-only analysis only

5. **No Third-Party Sharing**
   - Does not share data with third parties
   - Does not integrate with external services
   - Completely standalone operation

---

## Verification & Audit

### Independent Verification

You can verify these claims yourself:

```bash
# Clone the repository
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof

# Check for HTTP libraries (should be empty)
grep -r "reqwest\|hyper\|ureq\|curl" Cargo.toml src/

# Check for telemetry (should be empty)
grep -ri "telemetry\|analytics\|tracking" Cargo.toml src/

# Check for external connections (should be empty)
grep -r "http://\|https://" src/ --include="*.rs"

# Monitor network activity during execution
# (Linux/Mac: use `lsof -i` or `netstat`)
# (Windows: use `netstat -ano` or Resource Monitor)
```

**Expected Results:** All searches should return zero matches (except for HTTP URL pattern regex in pattern detection).

---

### Audit Information

**Audit Date:** 2025-10-02
**Version Audited:** 0.4.61
**Repository:** https://github.com/AndreaBozzo/dataprof
**License:** MIT

**Key Files Audited:**
- `Cargo.toml` - No HTTP client dependencies ✅
- `src/lib.rs` - Module structure review ✅
- `src/analysis/` - All analysis code ✅
- `src/stats/` - Statistical calculations ✅
- `src/analysis/metrics.rs` - Comprehensive quality metrics ✅
- `src/database/` - Database connectors (read-only) ✅

**Methodology:**
- Complete source code review
- Dependency tree analysis
- Network library audit
- File system operation verification

---

## Questions or Concerns?

### Contact

For transparency questions or security concerns:

- **GitHub Issues:** https://github.com/AndreaBozzo/dataprof/issues
- **Repository:** https://github.com/AndreaBozzo/dataprof
- **Documentation:** https://github.com/AndreaBozzo/dataprof/tree/master/docs

### Security Issues

For security-related concerns, please email the maintainer directly (see repository).

---

## Summary

DataProf is designed with **privacy first** principles:

- 🔒 **All processing is local** - Your data never leaves your machine
- 📖 **Fully transparent** - Open source code you can audit
- 🚫 **No telemetry** - Zero tracking or external communication
- ✅ **You control outputs** - Only writes files you explicitly request
- 🛡️ **Read-only database access** - Never modifies your data sources

**This document provides a complete, verified inventory of all data collection and analysis performed by DataProf.**

---

**Last Updated:** 2025-10-02 | **Version:** 0.4.61 | [View on GitHub](https://github.com/AndreaBozzo/dataprof)
