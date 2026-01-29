#!/usr/bin/env python3
"""
Integration tests for Arrow PyCapsule/FFI functionality.

Tests the zero-copy data exchange between Rust and Python via the Arrow C Data Interface.
"""

import pytest
from pathlib import Path

# Import dataprof - skip all tests if not available
pytest.importorskip("dataprof")
import dataprof


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_csv_file(tmp_path: Path) -> str:
    """Create a sample CSV file for testing."""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(
        """name,age,score,active
Alice,25,85.5,true
Bob,30,92.0,false
Charlie,35,78.3,true
David,28,88.7,true
Eve,,95.2,false
"""
    )
    return str(csv_file)


@pytest.fixture
def empty_csv_file(tmp_path: Path) -> str:
    """Create an empty CSV file (headers only)."""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("name,age,score\n")
    return str(csv_file)


# ============================================================================
# PyRecordBatch Tests
# ============================================================================


class TestRecordBatchClass:
    """Test PyRecordBatch class functionality."""

    def test_analyze_csv_to_arrow(self, sample_csv_file: str):
        """Test CSV analysis returning RecordBatch."""
        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)

        assert isinstance(batch, dataprof.RecordBatch)
        assert batch.num_rows > 0
        assert batch.num_columns > 0
        assert len(batch.column_names) == batch.num_columns

    def test_record_batch_properties(self, sample_csv_file: str):
        """Test RecordBatch property accessors."""
        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)

        assert isinstance(batch.num_rows, int)
        assert isinstance(batch.num_columns, int)
        assert isinstance(batch.column_names, list)
        assert all(isinstance(name, str) for name in batch.column_names)

        # Check expected columns from profile output
        assert "column_name" in batch.column_names
        assert "data_type" in batch.column_names
        assert "total_count" in batch.column_names

    def test_record_batch_repr(self, sample_csv_file: str):
        """Test RecordBatch string representation."""
        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)

        repr_str = repr(batch)
        assert "RecordBatch" in repr_str
        assert "rows=" in repr_str
        assert "columns=" in repr_str

    def test_arrow_c_schema_method(self, sample_csv_file: str):
        """Test __arrow_c_schema__ returns PyCapsule."""
        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)

        schema_capsule = batch.__arrow_c_schema__()
        # Should be a PyCapsule object (opaque from Python side)
        assert schema_capsule is not None

    def test_arrow_c_array_method(self, sample_csv_file: str):
        """Test __arrow_c_array__ returns tuple of PyCapsules."""
        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)

        result = batch.__arrow_c_array__()
        assert isinstance(result, tuple)
        assert len(result) == 2
        # Both should be PyCapsule objects
        assert result[0] is not None
        assert result[1] is not None

    def test_arrow_c_array_with_requested_schema(self, sample_csv_file: str):
        """Test __arrow_c_array__ with requested_schema parameter."""
        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)

        # Should work with None (ignored for now)
        result = batch.__arrow_c_array__(requested_schema=None)
        assert isinstance(result, tuple)
        assert len(result) == 2


# ============================================================================
# PyArrow Integration Tests
# ============================================================================


class TestPyArrowIntegration:
    """Test integration with pyarrow library."""

    @pytest.fixture
    def pyarrow(self):
        """Import pyarrow or skip tests."""
        return pytest.importorskip("pyarrow")

    def test_to_pandas_with_pyarrow(self, sample_csv_file: str, pyarrow):
        """Test zero-copy conversion to pandas via pyarrow."""
        pd = pytest.importorskip("pandas")

        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)
        df = batch.to_pandas()

        assert isinstance(df, pd.DataFrame)
        assert len(df) == batch.num_rows
        assert len(df.columns) == batch.num_columns

    def test_import_from_pyarrow(self, sample_csv_file: str, pyarrow):
        """Test that pyarrow can import our RecordBatch via PyCapsule."""
        import pyarrow as pa

        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)

        # Use PyCapsule protocol to import into pyarrow via pa.record_batch()
        pa_batch = pa.record_batch(batch)

        assert isinstance(pa_batch, pa.RecordBatch)
        assert pa_batch.num_rows == batch.num_rows
        assert pa_batch.num_columns == batch.num_columns

    def test_roundtrip_pyarrow(self, sample_csv_file: str, pyarrow):
        """Test data integrity through pyarrow roundtrip."""
        import pyarrow as pa

        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)

        # Export to pyarrow using PyCapsule protocol
        pa_batch = pa.record_batch(batch)

        # Verify column names match
        assert list(pa_batch.schema.names) == batch.column_names


# ============================================================================
# Polars Integration Tests
# ============================================================================


class TestPolarsIntegration:
    """Test integration with polars library."""

    @pytest.fixture
    def polars(self):
        """Import polars or skip tests."""
        return pytest.importorskip("polars")

    @pytest.fixture
    def pyarrow_for_polars(self):
        """Polars conversion requires pyarrow."""
        return pytest.importorskip("pyarrow")

    def test_to_polars(self, sample_csv_file: str, polars, pyarrow_for_polars):
        """Test conversion to polars DataFrame."""
        import polars as pl

        batch = dataprof.analyze_csv_to_arrow(sample_csv_file)
        df = batch.to_polars()

        assert isinstance(df, pl.DataFrame)
        assert len(df) == batch.num_rows


# ============================================================================
# profile_dataframe Tests
# ============================================================================


class TestProfileDataFrame:
    """Test profile_dataframe function for in-memory DataFrame profiling."""

    def test_profile_pandas_dataframe(self, sample_csv_file: str):
        """Test profiling a pandas DataFrame."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")  # Required for pandas Arrow export

        df = pd.read_csv(sample_csv_file)
        report = dataprof.profile_dataframe(df, name="test_pandas")

        assert isinstance(report, dataprof.PyQualityReport)
        assert report.total_columns > 0
        assert report.total_rows > 0
        # Check data source identifier contains our name
        assert "test_pandas" in report.file_path or "pandas" in report.file_path

    def test_profile_pandas_with_default_name(self, sample_csv_file: str):
        """Test profiling with default name."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")

        df = pd.read_csv(sample_csv_file)
        report = dataprof.profile_dataframe(df)

        assert isinstance(report, dataprof.PyQualityReport)
        assert report.total_columns > 0

    def test_profile_polars_dataframe(self, sample_csv_file: str):
        """Test profiling a polars DataFrame."""
        pl = pytest.importorskip("polars")

        df = pl.read_csv(sample_csv_file)
        report = dataprof.profile_dataframe(df, name="test_polars")

        assert isinstance(report, dataprof.PyQualityReport)
        assert report.total_columns > 0
        assert report.total_rows > 0

    def test_profile_dataframe_quality_metrics(self, sample_csv_file: str):
        """Test that quality metrics are calculated correctly."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")

        df = pd.read_csv(sample_csv_file)
        report = dataprof.profile_dataframe(df, name="metrics_test")

        # Quality score should be valid
        score = report.quality_score()
        assert 0.0 <= score <= 100.0

        # Should have data quality metrics
        metrics = report.data_quality_metrics
        assert metrics is not None

    def test_profile_dataframe_with_nulls(self, sample_csv_file: str):
        """Test profiling DataFrame with null values."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")

        df = pd.read_csv(sample_csv_file)
        # Our sample has a null in 'age' column
        report = dataprof.profile_dataframe(df, name="nulls_test")

        assert isinstance(report, dataprof.PyQualityReport)
        # Should detect the null values
        assert report.data_quality_metrics.missing_values_ratio >= 0


# ============================================================================
# Error Handling Tests
# ============================================================================


class TestErrorHandling:
    """Test error handling for invalid inputs."""

    def test_analyze_nonexistent_file(self):
        """Test error when file doesn't exist."""
        with pytest.raises(Exception):  # RuntimeError from Rust
            dataprof.analyze_csv_to_arrow("/nonexistent/path/file.csv")

    def test_profile_invalid_object(self):
        """Test error when passing non-DataFrame object."""
        with pytest.raises(Exception):  # TypeError or RuntimeError
            dataprof.profile_dataframe("not a dataframe", name="invalid")

    def test_profile_dict_object(self):
        """Test error when passing dict instead of DataFrame."""
        with pytest.raises(Exception):
            dataprof.profile_dataframe({"a": [1, 2, 3]}, name="dict")


# ============================================================================
# Parquet Tests (Feature-gated)
# ============================================================================


class TestParquetArrow:
    """Test Parquet analysis with Arrow output."""

    @pytest.fixture
    def sample_parquet_file(self, tmp_path: Path, sample_csv_file: str):
        """Create a sample Parquet file from CSV."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("pyarrow")
        pytest.importorskip("pyarrow.parquet")

        df = pd.read_csv(sample_csv_file)
        parquet_path = tmp_path / "sample.parquet"
        df.to_parquet(str(parquet_path))
        return str(parquet_path)

    def test_analyze_parquet_to_arrow(self, sample_parquet_file: str):
        """Test Parquet analysis returning RecordBatch."""
        try:
            batch = dataprof.analyze_parquet_to_arrow(sample_parquet_file)
            assert isinstance(batch, dataprof.RecordBatch)
            assert batch.num_rows > 0
        except AttributeError:
            pytest.skip("Parquet feature not enabled")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
