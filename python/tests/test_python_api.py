#!/usr/bin/env python3
"""
Comprehensive test suite for DataProf Python bindings.

This test suite validates:
- API contract compliance (all declared fields/methods exist)
- Type correctness
- Value ranges and constraints
- Error handling
- Edge cases
"""

import pytest
import os
import tempfile
from typing import List


# Skip all tests if dataprof module is not built
pytest.importorskip("dataprof")
import dataprof


class TestAPIContract:
    """Test that the API contract matches the actual implementation."""

    def test_pycolumn_profile_has_all_declared_fields(self, sample_csv_file):
        """Verify PyColumnProfile has all fields declared in type stubs."""
        profiles = dataprof.analyze_csv_file(sample_csv_file)
        assert len(profiles) > 0, "Should have at least one column profile"

        profile = profiles[0]

        # Check all required attributes exist
        assert hasattr(profile, 'name'), "Missing 'name' attribute"
        assert hasattr(profile, 'data_type'), "Missing 'data_type' attribute"
        assert hasattr(profile, 'total_count'), "Missing 'total_count' attribute"
        assert hasattr(profile, 'null_count'), "Missing 'null_count' attribute"
        assert hasattr(profile, 'unique_count'), "Missing 'unique_count' attribute"
        assert hasattr(profile, 'null_percentage'), "Missing 'null_percentage' attribute"
        assert hasattr(profile, 'uniqueness_ratio'), "Missing 'uniqueness_ratio' attribute"

        # Verify types
        assert isinstance(profile.name, str)
        assert isinstance(profile.data_type, str)
        assert isinstance(profile.total_count, int)
        assert isinstance(profile.null_count, int)
        assert profile.unique_count is None or isinstance(profile.unique_count, int)
        assert isinstance(profile.null_percentage, float)
        assert isinstance(profile.uniqueness_ratio, float)

    def test_pyquality_report_has_all_declared_fields(self, sample_csv_file):
        """Verify PyQualityReport has all fields declared in type stubs."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        # Check all required attributes exist
        assert hasattr(report, 'file_path'), "Missing 'file_path' attribute"
        assert hasattr(report, 'total_rows'), "Missing 'total_rows' attribute"
        assert hasattr(report, 'total_columns'), "Missing 'total_columns' attribute"
        assert hasattr(report, 'column_profiles'), "Missing 'column_profiles' attribute"
        assert hasattr(report, 'rows_scanned'), "Missing 'rows_scanned' attribute"
        assert hasattr(report, 'sampling_ratio'), "Missing 'sampling_ratio' attribute"
        assert hasattr(report, 'scan_time_ms'), "Missing 'scan_time_ms' attribute"
        assert hasattr(report, 'data_quality_metrics'), "Missing 'data_quality_metrics' attribute"

        # CRITICAL: Verify removed fields do NOT exist
        assert not hasattr(report, 'issues'), "BREAKING: 'issues' field should not exist (removed from API)"
        assert not hasattr(report, 'issues_by_severity'), "BREAKING: 'issues_by_severity' method should not exist"

        # Verify types
        assert isinstance(report.file_path, str)
        assert report.total_rows is None or isinstance(report.total_rows, int)
        assert isinstance(report.total_columns, int)
        assert isinstance(report.column_profiles, list)
        assert isinstance(report.rows_scanned, int)
        assert isinstance(report.sampling_ratio, float)
        assert isinstance(report.scan_time_ms, int)
        assert isinstance(report.data_quality_metrics, dataprof.PyDataQualityMetrics)

    def test_pydata_quality_metrics_has_all_declared_fields(self, sample_csv_file):
        """Verify PyDataQualityMetrics has all ISO 8000/25012 fields."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        # Overall score
        assert hasattr(metrics, 'overall_quality_score'), "Missing 'overall_quality_score'"

        # Completeness dimension
        assert hasattr(metrics, 'missing_values_ratio'), "Missing 'missing_values_ratio'"
        assert hasattr(metrics, 'complete_records_ratio'), "Missing 'complete_records_ratio'"
        assert hasattr(metrics, 'null_columns'), "Missing 'null_columns'"

        # Consistency dimension
        assert hasattr(metrics, 'data_type_consistency'), "Missing 'data_type_consistency'"
        assert hasattr(metrics, 'format_violations'), "Missing 'format_violations'"
        assert hasattr(metrics, 'encoding_issues'), "Missing 'encoding_issues'"

        # Uniqueness dimension
        assert hasattr(metrics, 'duplicate_rows'), "Missing 'duplicate_rows'"
        assert hasattr(metrics, 'key_uniqueness'), "Missing 'key_uniqueness'"
        assert hasattr(metrics, 'high_cardinality_warning'), "Missing 'high_cardinality_warning'"

        # Accuracy dimension
        assert hasattr(metrics, 'outlier_ratio'), "Missing 'outlier_ratio'"
        assert hasattr(metrics, 'range_violations'), "Missing 'range_violations'"
        assert hasattr(metrics, 'negative_values_in_positive'), "Missing 'negative_values_in_positive'"

        # Timeliness dimension (ISO 8000-8)
        assert hasattr(metrics, 'future_dates_count'), "Missing 'future_dates_count'"
        assert hasattr(metrics, 'stale_data_ratio'), "Missing 'stale_data_ratio'"
        assert hasattr(metrics, 'temporal_violations'), "Missing 'temporal_violations'"

        # Verify types (overall_quality_score is a method, not attribute)
        assert callable(metrics.overall_quality_score)
        assert isinstance(metrics.overall_quality_score(), float)
        assert isinstance(metrics.missing_values_ratio, float)
        assert isinstance(metrics.complete_records_ratio, float)
        assert isinstance(metrics.null_columns, list)
        assert isinstance(metrics.data_type_consistency, float)
        assert isinstance(metrics.format_violations, int)
        assert isinstance(metrics.encoding_issues, int)
        assert isinstance(metrics.duplicate_rows, int)
        assert isinstance(metrics.key_uniqueness, float)
        assert isinstance(metrics.high_cardinality_warning, bool)
        assert isinstance(metrics.outlier_ratio, float)
        assert isinstance(metrics.range_violations, int)
        assert isinstance(metrics.negative_values_in_positive, int)
        assert isinstance(metrics.future_dates_count, int)
        assert isinstance(metrics.stale_data_ratio, float)
        assert isinstance(metrics.temporal_violations, int)

    def test_pybatch_result_has_all_declared_fields(self, sample_csv_directory):
        """Verify PyBatchResult has all fields declared in type stubs."""
        result = dataprof.batch_analyze_directory(sample_csv_directory, recursive=False)

        # Check all required attributes exist
        assert hasattr(result, 'processed_files'), "Missing 'processed_files' attribute"
        assert hasattr(result, 'failed_files'), "Missing 'failed_files' attribute"
        assert hasattr(result, 'total_duration_secs'), "Missing 'total_duration_secs' attribute"
        assert hasattr(result, 'average_quality_score'), "Missing 'average_quality_score' attribute"

        # CRITICAL: Verify removed field does NOT exist
        assert not hasattr(result, 'total_quality_issues'), "BREAKING: 'total_quality_issues' should not exist"

        # Verify types
        assert isinstance(result.processed_files, int)
        assert isinstance(result.failed_files, int)
        assert isinstance(result.total_duration_secs, float)
        assert isinstance(result.average_quality_score, float)


class TestPyColumnProfile:
    """Test PyColumnProfile functionality."""

    def test_basic_csv_analysis(self, sample_csv_file):
        """Test basic column profiling on CSV file."""
        profiles = dataprof.analyze_csv_file(sample_csv_file)

        assert isinstance(profiles, list)
        assert len(profiles) > 0
        assert all(isinstance(p, dataprof.PyColumnProfile) for p in profiles)

    def test_column_profile_percentages_valid_range(self, sample_csv_file):
        """Test that percentage values are in valid range [0, 100]."""
        profiles = dataprof.analyze_csv_file(sample_csv_file)

        for profile in profiles:
            assert 0.0 <= profile.null_percentage <= 100.0, \
                f"null_percentage {profile.null_percentage} out of range for column {profile.name}"
            assert 0.0 <= profile.uniqueness_ratio <= 1.0, \
                f"uniqueness_ratio {profile.uniqueness_ratio} out of range for column {profile.name}"

    def test_column_profile_counts_consistency(self, sample_csv_file):
        """Test that counts are consistent with each other."""
        profiles = dataprof.analyze_csv_file(sample_csv_file)

        for profile in profiles:
            assert profile.null_count <= profile.total_count, \
                f"null_count cannot exceed total_count for column {profile.name}"

            if profile.unique_count is not None:
                assert profile.unique_count <= profile.total_count, \
                    f"unique_count cannot exceed total_count for column {profile.name}"

    def test_data_type_values(self, sample_csv_file):
        """Test that data_type is one of the expected values."""
        profiles = dataprof.analyze_csv_file(sample_csv_file)

        valid_types = {'integer', 'float', 'string', 'date'}
        for profile in profiles:
            assert profile.data_type in valid_types, \
                f"Invalid data_type '{profile.data_type}' for column {profile.name}"


class TestPyQualityReport:
    """Test PyQualityReport functionality."""

    def test_quality_report_generation(self, sample_csv_file):
        """Test complete quality report generation."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        assert isinstance(report, dataprof.PyQualityReport)
        assert report.file_path == sample_csv_file
        assert report.total_columns > 0
        assert len(report.column_profiles) == report.total_columns

    def test_quality_score_method(self, sample_csv_file):
        """Test quality_score() method returns valid score."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        score = report.quality_score()
        assert isinstance(score, float)
        assert 0.0 <= score <= 100.0, f"Quality score {score} out of valid range [0, 100]"

    def test_quality_score_matches_metrics(self, sample_csv_file):
        """Test that quality_score() matches data_quality_metrics.overall_quality_score."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        report_score = report.quality_score()
        metrics_score = report.data_quality_metrics.overall_quality_score()

        # Should be identical (same source)
        assert abs(report_score - metrics_score) < 0.01, \
            f"Mismatch: report.quality_score()={report_score} vs metrics.overall_quality_score()={metrics_score}"

    def test_to_json_method(self, sample_csv_file):
        """Test JSON serialization."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        json_str = report.to_json()
        assert isinstance(json_str, str)
        assert len(json_str) > 0

        # Verify it's valid JSON
        import json
        data = json.loads(json_str)
        assert 'metadata' in data
        assert 'data_quality_metrics' in data
        assert 'column_profiles' in data

    def test_scan_info_validity(self, sample_csv_file):
        """Test that scan information is valid."""
        report = dataprof.analyze_csv_with_quality(sample_csv_file)

        assert report.rows_scanned > 0
        assert 0.0 < report.sampling_ratio <= 1.0
        assert report.scan_time_ms >= 0


class TestPyDataQualityMetrics:
    """Test PyDataQualityMetrics ISO 8000/25012 compliance."""

    def test_overall_quality_score_calculation(self, sample_csv_file):
        """Test overall quality score is within valid range."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        score = metrics.overall_quality_score()
        assert isinstance(score, float)
        assert 0.0 <= score <= 100.0, f"Overall quality score {score} out of range"

    def test_quality_score_method_consistency(self, sample_csv_file):
        """Test overall_quality_score() method is consistent across calls."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        score1 = metrics.overall_quality_score()
        score2 = metrics.overall_quality_score()

        assert abs(score1 - score2) < 0.01, \
            "Method should return consistent score across calls"

    def test_completeness_metrics_valid(self, sample_csv_file):
        """Test completeness dimension metrics are valid."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        assert 0.0 <= metrics.missing_values_ratio <= 100.0
        assert 0.0 <= metrics.complete_records_ratio <= 100.0
        assert isinstance(metrics.null_columns, list)

        # Note: ratios are independent metrics, don't necessarily sum to 100%
        # missing_values_ratio = % of all values that are null
        # complete_records_ratio = % of rows with no nulls

    def test_consistency_metrics_valid(self, sample_csv_file):
        """Test consistency dimension metrics are valid."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        assert 0.0 <= metrics.data_type_consistency <= 100.0
        assert metrics.format_violations >= 0
        assert metrics.encoding_issues >= 0

    def test_uniqueness_metrics_valid(self, sample_csv_file):
        """Test uniqueness dimension metrics are valid."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        assert metrics.duplicate_rows >= 0
        assert 0.0 <= metrics.key_uniqueness <= 100.0
        assert isinstance(metrics.high_cardinality_warning, bool)

    def test_accuracy_metrics_valid(self, sample_csv_file):
        """Test accuracy dimension metrics are valid."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        assert 0.0 <= metrics.outlier_ratio <= 100.0
        assert metrics.range_violations >= 0
        assert metrics.negative_values_in_positive >= 0

    def test_timeliness_metrics_valid(self, sample_csv_file):
        """Test timeliness (ISO 8000-8) dimension metrics are valid."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        assert metrics.future_dates_count >= 0
        assert 0.0 <= metrics.stale_data_ratio <= 100.0
        assert metrics.temporal_violations >= 0

    def test_summary_methods_exist(self, sample_csv_file):
        """Test that all summary methods exist and return strings."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        summaries = [
            metrics.completeness_summary(),
            metrics.consistency_summary(),
            metrics.uniqueness_summary(),
            metrics.accuracy_summary(),
            metrics.timeliness_summary(),
        ]

        for summary in summaries:
            assert isinstance(summary, str)
            assert len(summary) > 0

    def test_summary_dict_method(self, sample_csv_file):
        """Test summary_dict() returns proper dictionary."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        summary = metrics.summary_dict()
        assert isinstance(summary, dict)
        assert len(summary) > 0

        # Check some expected keys
        expected_keys = [
            'missing_values_ratio',
            'complete_records_ratio',
            'data_type_consistency',
            'duplicate_rows',
            'key_uniqueness'
        ]
        for key in expected_keys:
            assert key in summary, f"Missing key '{key}' in summary_dict"

    def test_repr_html_for_jupyter(self, sample_csv_file):
        """Test _repr_html_() for Jupyter notebook integration."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        html = metrics._repr_html_()
        assert isinstance(html, str)
        assert len(html) > 0
        assert '<div' in html  # Should contain HTML
        assert 'Quality' in html or 'quality' in html

    def test_str_representation(self, sample_csv_file):
        """Test __str__() method."""
        metrics = dataprof.calculate_data_quality_metrics(sample_csv_file)

        str_repr = str(metrics)
        assert isinstance(str_repr, str)
        assert len(str_repr) > 0
        assert 'DataQualityMetrics' in str_repr


class TestPyBatchResult:
    """Test batch processing functionality."""

    def test_batch_analyze_directory(self, sample_csv_directory):
        """Test batch directory analysis."""
        result = dataprof.batch_analyze_directory(sample_csv_directory, recursive=False)

        assert isinstance(result, dataprof.PyBatchResult)
        assert result.processed_files >= 0
        assert result.failed_files >= 0
        assert result.total_duration_secs >= 0.0

    def test_batch_analyze_glob(self, sample_csv_directory):
        """Test batch glob pattern analysis."""
        import os
        pattern = os.path.join(sample_csv_directory, "*.csv")
        result = dataprof.batch_analyze_glob(pattern)

        assert isinstance(result, dataprof.PyBatchResult)
        assert result.processed_files > 0

    def test_batch_parallel_processing(self, sample_csv_directory):
        """Test parallel batch processing."""
        result = dataprof.batch_analyze_directory(
            sample_csv_directory,
            recursive=False,
            parallel=True,
            max_concurrent=2
        )

        assert result.processed_files > 0

    def test_average_quality_score_valid(self, sample_csv_directory):
        """Test average quality score is in valid range."""
        result = dataprof.batch_analyze_directory(sample_csv_directory)

        if result.processed_files > 0:
            assert 0.0 <= result.average_quality_score <= 100.0


class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_nonexistent_file_raises_error(self):
        """Test that analyzing non-existent file raises appropriate error."""
        with pytest.raises(Exception):  # Should raise PyRuntimeError or similar
            dataprof.analyze_csv_file("/nonexistent/file.csv")

    def test_invalid_csv_format(self):
        """Test handling of invalid CSV format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("invalid\x00binary\x00data")
            invalid_file = f.name

        try:
            # Should handle gracefully or raise clear error
            dataprof.analyze_csv_file(invalid_file)
        except Exception as e:
            # Should be a clear error message
            assert len(str(e)) > 0
        finally:
            os.unlink(invalid_file)

    def test_empty_csv_file(self):
        """Test handling of empty CSV file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("")  # Empty file
            empty_file = f.name

        try:
            # Should handle empty files gracefully
            result = dataprof.analyze_csv_file(empty_file)
            assert isinstance(result, list)
        except Exception:
            pass  # Or raise clear error - either is acceptable
        finally:
            os.unlink(empty_file)

    def test_csv_with_only_headers(self):
        """Test CSV with headers but no data rows."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col1,col2,col3\n")  # Only headers
            headers_only_file = f.name

        try:
            profiles = dataprof.analyze_csv_file(headers_only_file)
            # Should return column profiles even with no data
            assert isinstance(profiles, list)
        finally:
            os.unlink(headers_only_file)


class TestJSONAnalysis:
    """Test JSON/JSONL analysis functionality."""

    def test_json_file_analysis(self, sample_json_file):
        """Test basic JSON file analysis."""
        profiles = dataprof.analyze_json_file(sample_json_file)

        assert isinstance(profiles, list)
        assert len(profiles) > 0
        assert all(isinstance(p, dataprof.PyColumnProfile) for p in profiles)

    def test_json_quality_analysis(self, sample_json_file):
        """Test JSON quality analysis."""
        report = dataprof.analyze_json_with_quality(sample_json_file)

        assert isinstance(report, dataprof.PyQualityReport)
        assert isinstance(report.data_quality_metrics, dataprof.PyDataQualityMetrics)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a sample CSV file for testing."""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text("""name,age,email,score,city
Alice,25,alice@example.com,85.5,New York
Bob,30,bob@example.com,92.0,San Francisco
Charlie,35,charlie@example.com,78.3,Boston
David,28,david@example.com,88.7,Seattle
Eve,32,eve@example.com,95.2,Austin
Frank,29,,82.1,Denver
Grace,31,grace@example.com,90.4,Portland
Henry,27,henry@example.com,,Miami
Ivy,33,ivy@example.com,87.9,Chicago
Jack,26,jack@example.com,91.5,Philadelphia
""")
    return str(csv_file)


@pytest.fixture
def sample_csv_directory(tmp_path):
    """Create a directory with multiple CSV files for batch testing."""
    csv_dir = tmp_path / "csv_files"
    csv_dir.mkdir()

    # Create multiple CSV files
    for i in range(3):
        csv_file = csv_dir / f"data_{i}.csv"
        csv_file.write_text(f"""name,value,category
Item{i}_1,{10 + i},A
Item{i}_2,{20 + i},B
Item{i}_3,{30 + i},C
""")

    return str(csv_dir)


@pytest.fixture
def sample_json_file(tmp_path):
    """Create a sample JSON file for testing."""
    json_file = tmp_path / "sample.json"
    json_file.write_text("""[
        {"name": "Alice", "age": 25, "active": true},
        {"name": "Bob", "age": 30, "active": false},
        {"name": "Charlie", "age": 35, "active": true}
    ]""")
    return str(json_file)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
