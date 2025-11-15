# DataProf Python Bindings Test Suite

Comprehensive test suite for validating the Python bindings of DataProf.

## Test Files

### 1. `test_python_api.py` - Core API Tests
**468 lines** of comprehensive tests covering:
- ✅ **API Contract Validation** - Verifies all declared fields/methods exist
- ✅ **Type Correctness** - Validates return types and field types
- ✅ **Value Ranges** - Ensures metrics are within valid ranges (0-100 for percentages, etc.)
- ✅ **ISO 8000/25012 Compliance** - Tests all 5 quality dimensions
- ✅ **Error Handling** - Tests edge cases and error conditions

**Test Classes:**
- `TestAPIContract` - Validates that type stubs match implementation
- `TestPyColumnProfile` - Tests column profiling functionality
- `TestPyQualityReport` - Tests quality report generation
- `TestPyDataQualityMetrics` - Tests ISO quality metrics
- `TestPyBatchResult` - Tests batch processing
- `TestErrorHandling` - Tests error cases
- `TestJSONAnalysis` - Tests JSON/JSONL support

### 2. `test_context_managers.py` - Context Manager Tests
**263 lines** of tests for resource management:
- ✅ **PyBatchAnalyzer** - Batch analysis context manager
- ✅ **PyCsvProcessor** - CSV processing context manager
- ✅ **Resource Cleanup** - Ensures proper cleanup on exit
- ✅ **Exception Handling** - Tests cleanup during exceptions
- ✅ **Integration** - Tests multiple context managers together

### 3. `test_api_stability.py` - Regression Tests
**336 lines** of tests to prevent breaking changes:
- ✅ **Export Validation** - All `__all__` exports are accessible
- ✅ **Breaking Change Detection** - Catches removed fields/methods
- ✅ **Type Stub Accuracy** - Validates stubs match implementation
- ✅ **Return Type Consistency** - Ensures consistent return types

**CRITICAL TESTS** (would have caught the PyQualityIssue bug):
- `test_pyquality_report_no_issues_field()` - Ensures removed field doesn't exist
- `test_pyquality_issue_class_not_exported()` - Ensures removed class isn't exported
- `test_typepointed_stub_accuracy()` - Validates type stubs match reality

### 4. `test_bindings.py` - Basic Integration Tests (existing)
Simple end-to-end tests for basic functionality.

### 5. `test_timeliness_metrics.py` - Timeliness Tests (existing)
Tests for ISO 8000-8 timeliness dimension.

## Running the Tests

### Prerequisites

1. **Build the Python module first:**
   ```bash
   # From project root
   maturin develop --features python
   ```

2. **Install test dependencies:**
   ```bash
   pip install pytest pytest-cov
   ```

### Run All Tests

```bash
# From python/ directory
cd python
pytest tests/ -v
```

### Run Specific Test Suites

```bash
# API contract and core functionality
pytest tests/test_python_api.py -v

# Context managers
pytest tests/test_context_managers.py -v

# Regression/stability tests
pytest tests/test_api_stability.py -v

# Existing integration tests
pytest tests/test_bindings.py -v
```

### Run Specific Test Classes

```bash
# Only API contract validation
pytest tests/test_python_api.py::TestAPIContract -v

# Only breaking change detection
pytest tests/test_api_stability.py::TestAPIBreakingChanges -v
```

### Run with Coverage

```bash
pytest tests/ --cov=dataprof --cov-report=html
```

### Run in Parallel (faster)

```bash
pip install pytest-xdist
pytest tests/ -n auto
```

## Test Coverage

The test suite covers:

- ✅ **All 4 core classes** (PyColumnProfile, PyQualityReport, PyDataQualityMetrics, PyBatchResult)
- ✅ **All 7 core functions** (analyze_csv_file, analyze_csv_with_quality, etc.)
- ✅ **Both context managers** (PyBatchAnalyzer, PyCsvProcessor)
- ✅ **All ISO 8000/25012 dimensions** (Completeness, Consistency, Uniqueness, Accuracy, Timeliness)
- ✅ **Error handling** and edge cases
- ✅ **JSON/JSONL** analysis
- ✅ **Batch processing** (directory and glob patterns)

## What These Tests Would Have Caught

The comprehensive test suite would have **immediately detected** the following issues found during review:

1. **Missing `issues` field** - `test_pyquality_report_has_all_declared_fields()` checks all fields exist
2. **Removed `PyQualityIssue` class** - `test_pyquality_issue_class_not_exported()` ensures it's not exported
3. **Type stub inaccuracies** - `test_pyquality_report_stub_accuracy()` validates stubs match implementation
4. **Missing `total_quality_issues`** - `test_pybatch_result_stub_accuracy()` validates all fields

## CI/CD Integration

Add to your GitHub Actions workflow:

```yaml
- name: Test Python Bindings
  run: |
    maturin develop --features python
    cd python
    pip install pytest pytest-cov
    pytest tests/ -v --cov=dataprof --cov-report=xml

- name: Upload Coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./python/coverage.xml
```

## Test Philosophy

These tests follow the **"API Contract Testing"** philosophy:

1. **Type stubs are the contract** - Implementation must match type declarations
2. **Breaking changes must be explicit** - Removed fields/methods are tested to ensure they're gone
3. **Exhaustive field validation** - Every attribute is checked for existence and type
4. **Edge cases matter** - Empty files, invalid data, exceptions are all tested
5. **ISO compliance** - Quality metrics must follow ISO 8000/25012 standard

## Maintenance

When adding new features:

1. ✅ Update type stubs first (`.pyi` file)
2. ✅ Implement the feature in Rust
3. ✅ Add tests to validate the new feature
4. ✅ Run full test suite to ensure no regressions

When removing features:

1. ✅ Add regression test to ensure it stays removed
2. ✅ Update type stubs
3. ✅ Update documentation
4. ✅ Run tests to catch any lingering references

## Test Statistics

- **Total test files**: 5
- **Total test lines**: ~1,100+ lines
- **Test classes**: 15+
- **Individual test cases**: 60+
- **Coverage target**: >90% for Python bindings code

## Questions?

See the main project documentation or open an issue on GitHub.
