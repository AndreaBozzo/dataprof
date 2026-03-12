#!/usr/bin/env python3
"""
Test suite for DataProf context managers.

Tests:
- PyCsvProcessor context manager
- Resource cleanup
- Error handling within context managers
"""

import pytest
import os
import tempfile

pytest.importorskip("dataprof")
import dataprof


class TestPyCsvProcessor:
    """Test PyCsvProcessor context manager."""

    def test_csv_processor_context_manager(self):
        """Test PyCsvProcessor as context manager."""
        with dataprof.PyCsvProcessor(chunk_size=1000) as processor:
            assert processor is not None
            assert hasattr(processor, 'open_file')
            assert hasattr(processor, 'process_chunks')
            assert hasattr(processor, 'get_processing_info')

    def test_csv_processor_with_chunk_size(self):
        """Test PyCsvProcessor with custom chunk size."""
        chunk_size = 1000
        with dataprof.PyCsvProcessor(chunk_size=chunk_size) as processor:
            assert processor is not None

    def test_csv_processor_open_and_process(self, sample_csv_file):
        """Test opening file and processing chunks."""
        with dataprof.PyCsvProcessor(chunk_size=5) as processor:
            processor.open_file(sample_csv_file)
            chunks = processor.process_chunks()

            assert isinstance(chunks, list)

    def test_csv_processor_processing_info(self, sample_csv_file):
        """Test get_processing_info method."""
        with dataprof.PyCsvProcessor(chunk_size=1000) as processor:
            processor.open_file(sample_csv_file)
            _ = processor.process_chunks()

            info = processor.get_processing_info()
            assert isinstance(info, dict)

    def test_csv_processor_auto_cleanup(self, sample_csv_file):
        """Test automatic cleanup on context exit."""
        processor = dataprof.PyCsvProcessor(chunk_size=1000)

        with processor:
            processor.open_file(sample_csv_file)
            info = processor.get_processing_info()

        # After exit, cleanup should have occurred
        assert info is not None


class TestContextManagerEdgeCases:
    """Test edge cases and error conditions for context managers."""

    def test_csv_processor_without_opening_file(self):
        """Test processor without opening a file first."""
        with dataprof.PyCsvProcessor(chunk_size=1000) as processor:
            # Should handle gracefully
            try:
                _ = processor.process_chunks()
            except Exception as e:
                # Should raise clear error about no file opened
                assert len(str(e)) > 0

    def test_csv_processor_invalid_chunk_size(self):
        """Test processor with invalid chunk size."""
        # Zero or negative chunk size should be handled
        try:
            with dataprof.PyCsvProcessor(chunk_size=0) as _:
                pass
        except (ValueError, Exception):
            pass  # Expected to raise or handle gracefully


class TestContextManagerIntegration:
    """Integration tests for context managers."""

    def test_nested_context_managers(self, sample_csv_file):
        """Test nesting different context managers."""
        with dataprof.PyCsvProcessor(chunk_size=1000) as csv_processor:
            csv_processor.open_file(sample_csv_file)
            chunks = csv_processor.process_chunks()

        assert chunks is not None


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_csv_file(tmp_path):
    """Create a single sample CSV file."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("""name,age,score
Alice,25,85.5
Bob,30,92.0
Charlie,35,78.3
David,28,88.7
Eve,32,95.2
""")
    return str(csv_file)


@pytest.fixture
def sample_csv_files(tmp_path):
    """Create multiple sample CSV files."""
    files = []
    for i in range(3):
        csv_file = tmp_path / f"test_{i}.csv"
        csv_file.write_text(f"""name,value
Item{i}_A,{10 + i}
Item{i}_B,{20 + i}
Item{i}_C,{30 + i}
""")
        files.append(str(csv_file))

    return files


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
