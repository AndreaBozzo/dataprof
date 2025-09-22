# Standard Test Datasets

Unified test dataset structure for consistent benchmarking across all engines and use cases.

## Directory Structure

```
tests/fixtures/standard_datasets/
├── micro/           # <1MB - Unit test level (100-5K rows)
├── small/          # 1-10MB - Integration test level (5K-100K rows)
├── medium/         # 10-100MB - Performance test level (100K-1M rows)
├── large/          # 100MB-1GB - Stress test level (1M+ rows)
└── realistic/      # Real-world datasets (anonymized samples)
```

## Dataset Categories

### Data Patterns
- **basic**: Simple structured data (id, name, age, score)
- **mixed**: Mixed data types (strings, numbers, booleans, dates)
- **numeric**: Number-heavy data for SIMD testing
- **wide**: Many columns (50+ cols, few rows)
- **deep**: Few columns, many rows
- **unicode**: International character sets
- **messy**: Real-world inconsistencies (missing values, mixed formats)

### Engine Testing
- **streaming**: Optimized for streaming engines
- **memory**: Memory usage patterns
- **arrow**: Arrow-format compatible data
- **adaptive**: Engine selection decision data

## Usage

```rust
use dataprof_tests::fixtures::StandardDatasets;

let dataset = StandardDatasets::micro("basic")?;
let result = analyze_csv(&dataset.path())?;
```

## Generation

Datasets are generated on-demand using standardized patterns to ensure:
- Consistent schema across size categories
- Reproducible data for regression testing
- Realistic data distributions
- Cross-platform compatibility