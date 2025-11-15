#!/usr/bin/env python3
"""
API Stability and Regression Tests for DataProf Python Bindings.

This test suite ensures that:
1. The API surface remains stable across versions
2. No breaking changes are introduced accidentally
3. All exported symbols are actually accessible
4. Type stubs match the actual implementation

These tests would have CAUGHT the PyQualityIssue bug that was discovered.
"""

import pytest
import inspect

pytest.importorskip("dataprof")
import dataprof


class TestExportedAPI:
    """Test that all exported symbols are accessible and correct."""

    def test_all_exports_are_accessible(self):
        """Test that all items in __all__ are actually accessible."""
        if not hasattr(dataprof, '__all__'):
            pytest.skip("Module does not define __all__")

        for name in dataprof.__all__:
            assert hasattr(dataprof, name), \
                f"Symbol '{name}' is in __all__ but not accessible via dataprof.{name}"

            obj = getattr(dataprof, name)
            assert obj is not None, f"Symbol '{name}' exists but is None"

    def test_no_removed_symbols_in_all(self):
        """Critical: Test that removed symbols are NOT in __all__."""
        if not hasattr(dataprof, '__all__'):
            pytest.skip("Module does not define __all__")

        # These symbols were REMOVED and should NOT be in __all__
        removed_symbols = ['PyQualityIssue']

        for symbol in removed_symbols:
            assert symbol not in dataprof.__all__, \
                f"BREAKING: Removed symbol '{symbol}' is still in __all__"

    def test_core_classes_exported(self):
        """Test that core classes are exported."""
        expected_classes = [
            'PyColumnProfile',
            'PyQualityReport',
            'PyDataQualityMetrics',
            'PyBatchResult',
            'PyBatchAnalyzer',
            'PyCsvProcessor',
        ]

        for class_name in expected_classes:
            assert hasattr(dataprof, class_name), \
                f"Core class '{class_name}' is not exported"

            cls = getattr(dataprof, class_name)
            assert inspect.isclass(cls), \
                f"'{class_name}' exists but is not a class"

    def test_core_functions_exported(self):
        """Test that core analysis functions are exported."""
        expected_functions = [
            'analyze_csv_file',
            'analyze_csv_with_quality',
            'analyze_json_file',
            'analyze_json_with_quality',
            'calculate_data_quality_metrics',
            'batch_analyze_glob',
            'batch_analyze_directory',
        ]

        for func_name in expected_functions:
            assert hasattr(dataprof, func_name), \
                f"Core function '{func_name}' is not exported"

            func = getattr(dataprof, func_name)
            assert callable(func), \
                f"'{func_name}' exists but is not callable"


class TestAPIBreakingChanges:
    """Test for breaking changes that should NEVER happen."""

    def test_pyquality_report_no_issues_field(self, sample_csv_file):
        """CRITICAL: Ensure 'issues' field does NOT exist (was removed)."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        assert not hasattr(report, 'issues'), \
            "BREAKING: PyQualityReport should NOT have 'issues' field (removed in API v0.4+)"

    def test_pyquality_report_no_issues_by_severity_method(self, sample_csv_file):
        """CRITICAL: Ensure 'issues_by_severity' method does NOT exist."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        assert not hasattr(report, 'issues_by_severity'), \
            "BREAKING: PyQualityReport should NOT have 'issues_by_severity' method (removed in API v0.4+)"

    def test_pyquality_report_has_data_quality_metrics(self, sample_csv_file):
        """CRITICAL: Ensure 'data_quality_metrics' field EXISTS."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        assert hasattr(report, 'data_quality_metrics'), \
            "CRITICAL: PyQualityReport MUST have 'data_quality_metrics' field"

        assert isinstance(report.data_quality_metrics, dataprof.PyDataQualityMetrics), \
            "data_quality_metrics must be PyDataQualityMetrics instance"

    def test_pybatch_result_no_total_quality_issues(self, sample_csv_directory):
        """CRITICAL: Ensure 'total_quality_issues' field does NOT exist."""
        result = dataprof.batch_analyze_directory(sample_csv_directory)

        assert not hasattr(result, 'total_quality_issues'), \
            "BREAKING: PyBatchResult should NOT have 'total_quality_issues' field (removed)"

    def test_pyquality_issue_class_not_exported(self):
        """CRITICAL: Ensure PyQualityIssue class is NOT exported."""
        assert not hasattr(dataprof, 'PyQualityIssue'), \
            "BREAKING: PyQualityIssue class should NOT be exported (removed from API)"


class TestRequiredMethods:
    """Test that required methods exist on all classes."""

    def test_pyquality_report_required_methods(self, sample_csv_file):
        """Test PyQualityReport has all required methods."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        # Must have quality_score() method
        assert hasattr(report, 'quality_score'), "Missing quality_score() method"
        assert callable(report.quality_score), "quality_score must be callable"

        # Must have to_json() method
        assert hasattr(report, 'to_json'), "Missing to_json() method"
        assert callable(report.to_json), "to_json must be callable"

    def test_pydata_quality_metrics_required_methods(self, sample_csv_file):
        """Test PyDataQualityMetrics has all required methods."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        required_methods = [
            'overall_quality_score',
            'completeness_summary',
            'consistency_summary',
            'uniqueness_summary',
            'accuracy_summary',
            'timeliness_summary',
            'summary_dict',
            '_repr_html_',
            '__str__',
        ]

        for method_name in required_methods:
            assert hasattr(metrics, method_name), \
                f"PyDataQualityMetrics missing required method: {method_name}"
            assert callable(getattr(metrics, method_name)), \
                f"{method_name} must be callable"

    def test_context_managers_have_required_methods(self):
        """Test context managers implement required protocol."""
        # PyBatchAnalyzer
        analyzer = dataprof.PyBatchAnalyzer()
        assert hasattr(analyzer, '__enter__'), "PyBatchAnalyzer missing __enter__"
        assert hasattr(analyzer, '__exit__'), "PyBatchAnalyzer missing __exit__"
        assert callable(analyzer.__enter__)
        assert callable(analyzer.__exit__)

        # PyCsvProcessor (requires chunk_size parameter)
        processor = dataprof.PyCsvProcessor(chunk_size=1000)
        assert hasattr(processor, '__enter__'), "PyCsvProcessor missing __enter__"
        assert hasattr(processor, '__exit__'), "PyCsvProcessor missing __exit__"
        assert callable(processor.__enter__)
        assert callable(processor.__exit__)


class TestTypeStubAccuracy:
    """Test that type stubs accurately reflect the implementation."""

    def test_pycolumn_profile_stub_accuracy(self, sample_csv_file):
        """Test PyColumnProfile attributes match type stub declarations."""
        profiles = dataprof.analyze_csv_file(sample_csv_file)
        profile = profiles[0]

        # All attributes declared in stub must exist
        stub_attributes = [
            'name', 'data_type', 'total_count', 'null_count',
            'unique_count', 'null_percentage', 'uniqueness_ratio'
        ]

        for attr in stub_attributes:
            assert hasattr(profile, attr), \
                f"Type stub declares '{attr}' but not present in implementation"

    def test_pyquality_report_stub_accuracy(self, sample_csv_file):
        """Test PyQualityReport attributes match type stub declarations."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        # Attributes declared in current stub
        stub_attributes = [
            'file_path', 'total_rows', 'total_columns', 'column_profiles',
            'rows_scanned', 'sampling_ratio', 'scan_time_ms', 'data_quality_metrics'
        ]

        for attr in stub_attributes:
            assert hasattr(report, attr), \
                f"Type stub declares '{attr}' but not present in PyQualityReport"

        # Attributes that should NOT exist (removed from stub)
        removed_attributes = ['issues']

        for attr in removed_attributes:
            assert not hasattr(report, attr), \
                f"Type stub removed '{attr}' but still present in implementation"

    def test_pydata_quality_metrics_stub_accuracy(self, sample_csv_file):
        """Test PyDataQualityMetrics attributes match type stub."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        # All ISO 8000/25012 dimensions
        stub_attributes = [
            'overall_quality_score',
            # Completeness
            'missing_values_ratio', 'complete_records_ratio', 'null_columns',
            # Consistency
            'data_type_consistency', 'format_violations', 'encoding_issues',
            # Uniqueness
            'duplicate_rows', 'key_uniqueness', 'high_cardinality_warning',
            # Accuracy
            'outlier_ratio', 'range_violations', 'negative_values_in_positive',
            # Timeliness
            'future_dates_count', 'stale_data_ratio', 'temporal_violations',
        ]

        for attr in stub_attributes:
            assert hasattr(metrics, attr), \
                f"Type stub declares '{attr}' but not present in PyDataQualityMetrics"

    def test_pybatch_result_stub_accuracy(self, sample_csv_directory):
        """Test PyBatchResult attributes match type stub."""
        result = dataprof.batch_analyze_directory(sample_csv_directory)

        # Attributes in current stub
        stub_attributes = [
            'processed_files', 'failed_files',
            'total_duration_secs', 'average_quality_score'
        ]

        for attr in stub_attributes:
            assert hasattr(result, attr), \
                f"Type stub declares '{attr}' but not present in PyBatchResult"

        # Removed attributes
        assert not hasattr(result, 'total_quality_issues'), \
            "Removed attribute 'total_quality_issues' still exists"


class TestReturnTypeConsistency:
    """Test that return types are consistent across calls."""

    def test_analyze_csv_file_always_returns_list(self, sample_csv_file):
        """Test analyze_csv_file always returns list."""
        result1 = dataprof.analyze_csv_file(sample_csv_file)
        result2 = dataprof.analyze_csv_file(sample_csv_file)

        assert isinstance(result1, list)
        assert isinstance(result2, list)
        assert type(result1) == type(result2)

    def test_analyze_csv_with_quality_always_returns_report(self, sample_csv_file):
        """Test analyze_csv_with_quality always returns PyQualityReport."""
        result1 = dataprof.analyze_csv_with_quality(sample_csv_file)
        result2 = dataprof.analyze_csv_with_quality(sample_csv_file)

        assert isinstance(result1, dataprof.PyQualityReport)
        assert isinstance(result2, dataprof.PyQualityReport)
        assert type(result1) == type(result2)

    def test_calculate_data_quality_metrics_always_returns_metrics(self, sample_csv_file):
        """Test calculate_data_quality_metrics always returns PyDataQualityMetrics."""
        result1 = dataprof.calculate_data_quality_metrics(sample_csv_file)
        result2 = dataprof.calculate_data_quality_metrics(sample_csv_file)

        assert isinstance(result1, dataprof.PyDataQualityMetrics)
        assert isinstance(result2, dataprof.PyDataQualityMetrics)
        assert type(result1) == type(result2)


class TestVersionCompatibility:
    """Test version string and compatibility."""

    def test_version_string_exists(self):
        """Test that __version__ is defined."""
        assert hasattr(dataprof, '__version__'), \
            "Module should define __version__"

        version = dataprof.__version__
        assert isinstance(version, str)
        assert len(version) > 0

    def test_version_format(self):
        """Test version string follows semantic versioning."""
        version = dataprof.__version__
        parts = version.split('.')

        # Should have at least major.minor
        assert len(parts) >= 2, \
            f"Version '{version}' should follow semantic versioning (major.minor[.patch])"


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file for testing."""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("""name,age,score
Alice,25,85.5
Bob,30,92.0
Charlie,35,78.3
""")
    return str(csv_file)


@pytest.fixture
def sample_csv_directory(tmp_path):
    """Create a directory with multiple CSV files."""
    csv_dir = tmp_path / "csv_files"
    csv_dir.mkdir()

    for i in range(2):
        csv_file = csv_dir / f"data_{i}.csv"
        csv_file.write_text(f"""name,value
Item{i}_1,{10 + i}
Item{i}_2,{20 + i}
""")

    return str(csv_dir)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
