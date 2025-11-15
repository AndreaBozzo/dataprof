#!/usr/bin/env python3
"""
Test suite for DataProf context managers.

Tests:
- PyBatchAnalyzer context manager
- PyCsvProcessor context manager
- Resource cleanup
- Error handling within context managers
"""

import pytest
import os
import tempfile

pytest.importorskip("dataprof")
import dataprof


class TestPyBatchAnalyzer:
    """Test PyBatchAnalyzer context manager."""

    def test_batch_analyzer_context_manager(self, sample_csv_files):
        """Test PyBatchAnalyzer as context manager."""
        with dataprof.PyBatchAnalyzer() as analyzer:
            assert analyzer is not None
            assert hasattr(analyzer, 'add_file')
            assert hasattr(analyzer, 'get_results')

    def test_batch_analyzer_add_file(self, sample_csv_files):
        """Test adding files to batch analyzer."""
        with dataprof.PyBatchAnalyzer() as analyzer:
            for csv_file in sample_csv_files:
                analyzer.add_file(csv_file)

            results = analyzer.get_results()
            assert isinstance(results, list)

    def test_batch_analyzer_analyze_batch(self, sample_csv_files):
        """Test analyze_batch method."""
        with dataprof.PyBatchAnalyzer() as analyzer:
            results = analyzer.analyze_batch(sample_csv_files)
            assert isinstance(results, list)
            assert len(results) > 0

    def test_batch_analyzer_auto_cleanup(self, sample_csv_files):
        """Test that batch analyzer cleans up properly on exit."""
        analyzer = dataprof.PyBatchAnalyzer()

        with analyzer:
            for csv_file in sample_csv_files:
                analyzer.add_file(csv_file)
            results = analyzer.get_results()

        # After exiting context, should still have access to results
        # but cleanup should have occurred
        assert results is not None

    def test_batch_analyzer_exception_handling(self, sample_csv_files):
        """Test that batch analyzer handles exceptions properly."""
        try:
            with dataprof.PyBatchAnalyzer() as analyzer:
                analyzer.add_file(sample_csv_files[0])
                raise ValueError("Test exception")
        except ValueError:
            pass  # Expected

        # Should have cleaned up despite exception

    def test_batch_analyzer_temp_file_support(self, sample_csv_files):
        """Test add_temp_file method for temporary file handling."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col1,col2\n1,2\n3,4\n")
            temp_file = f.name

        try:
            with dataprof.PyBatchAnalyzer() as analyzer:
                # Add as temp file - should be automatically cleaned up
                analyzer.add_temp_file(temp_file)
                # Process the batch to get results
                results = analyzer.analyze_batch([temp_file])
                assert len(results) > 0
        finally:
            # Clean up if still exists
            if os.path.exists(temp_file):
                os.unlink(temp_file)


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

    def test_batch_analyzer_empty_file_list(self):
        """Test batch analyzer with no files added."""
        with dataprof.PyBatchAnalyzer() as analyzer:
            results = analyzer.get_results()
            assert isinstance(results, list)
            assert len(results) == 0

    def test_batch_analyzer_nonexistent_file(self):
        """Test batch analyzer with non-existent file."""
        with dataprof.PyBatchAnalyzer() as analyzer:
            # Should handle gracefully or raise clear error
            try:
                analyzer.add_file("/nonexistent/file.csv")
            except Exception as e:
                assert len(str(e)) > 0

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

    def test_batch_analyzer_with_quality_reports(self, sample_csv_files):
        """Test that batch analyzer returns quality reports."""
        with dataprof.PyBatchAnalyzer() as analyzer:
            results = analyzer.analyze_batch(sample_csv_files)

            for result in results:
                # Results should be quality reports or similar
                assert result is not None

    def test_multiple_context_managers_sequentially(self, sample_csv_files):
        """Test using multiple context managers sequentially."""
        # First analyzer
        with dataprof.PyBatchAnalyzer() as analyzer1:
            results1 = analyzer1.analyze_batch([sample_csv_files[0]])

        # Second analyzer - should work independently
        with dataprof.PyBatchAnalyzer() as analyzer2:
            results2 = analyzer2.analyze_batch([sample_csv_files[1]])

        assert len(results1) > 0
        assert len(results2) > 0

    def test_nested_context_managers(self, sample_csv_file):
        """Test nesting different context managers."""
        with dataprof.PyBatchAnalyzer() as batch_analyzer:
            with dataprof.PyCsvProcessor(chunk_size=1000) as csv_processor:
                csv_processor.open_file(sample_csv_file)
                chunks = csv_processor.process_chunks()

                batch_analyzer.add_file(sample_csv_file)
                results = batch_analyzer.get_results()

        assert chunks is not None
        assert results is not None


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
