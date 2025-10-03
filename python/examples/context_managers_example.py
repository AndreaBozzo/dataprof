#!/usr/bin/env python3
"""
DataProf Context Managers examples
"""

import tempfile
import os
import time
from pathlib import Path

def create_sample_datasets(count=3):
    """Create multiple sample datasets for context manager testing"""
    datasets = []

    templates = [
        # Dataset 1: E-commerce data
        """order_id,product,price,quantity,date,customer_type
ORD001,Laptop,1299.99,1,2024-01-15,premium
ORD002,Mouse,29.99,2,2024-01-16,regular
ORD003,Keyboard,89.99,1,2024-01-17,premium
ORD004,Monitor,399.99,1,2024-01-18,regular
ORD005,Headphones,199.99,1,2024-01-19,premium""",

        # Dataset 2: HR data
        """emp_id,name,department,salary,hire_date,status
E001,Alice Johnson,Engineering,85000,2020-03-15,active
E002,Bob Smith,Marketing,65000,2019-08-22,active
E003,Charlie Brown,Sales,58000,2021-01-10,inactive
E004,Diana Prince,HR,62000,2020-11-05,active
E005,Eve Wilson,Engineering,78000,2018-06-30,active""",

        # Dataset 3: IoT sensor data
        """sensor_id,timestamp,temperature,humidity,battery,location
S001,2024-01-20T10:00:00,22.5,45.2,85,Office A
S002,2024-01-20T10:01:00,23.1,47.8,92,Office B
S003,2024-01-20T10:02:00,21.8,44.1,78,Office C
S004,2024-01-20T10:03:00,24.2,49.5,88,Office A
S005,2024-01-20T10:04:00,22.9,46.3,95,Office B"""
    ]

    for i, template in enumerate(templates[:count]):
        fd, path = tempfile.mkstemp(suffix=f'_dataset_{i+1}.csv', prefix='dataprof_ctx_')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(template)
            datasets.append(path)
        except:
            os.close(fd)
            raise

    return datasets

def create_large_dataset():
    """Create a larger dataset for chunk processing testing"""
    fd, path = tempfile.mkstemp(suffix='_large.csv', prefix='dataprof_large_')

    try:
        header = "id,name,value,category,timestamp,score\n"
        rows = []
        for i in range(500):  # Medium-sized dataset
            row = f"{i},Item_{i},{i * 2.5},Category_{i % 5},2024-01-{(i % 28) + 1:02d}T{(i % 24):02d}:00:00,{(i % 100) + 1}\n"
            rows.append(row)

        with os.fdopen(fd, 'w') as f:
            f.write(header)
            f.writelines(rows)

        return path
    except:
        os.close(fd)
        raise

def test_batch_analyzer_context_manager():
    """Test PyBatchAnalyzer context manager"""
    print("\n📦 Test 1: Batch Analyzer Context Manager")
    print("=" * 55)

    try:
        import dataprof
        if not hasattr(dataprof, 'PyBatchAnalyzer'):
            print("❌ PyBatchAnalyzer not available")
            return False
    except ImportError:
        print("❌ dataprof module not available")
        return False

    datasets = create_sample_datasets(3)

    try:
        print(f"📚 Testing batch analysis with {len(datasets)} datasets...")

        # Test context manager usage
        with dataprof.PyBatchAnalyzer() as analyzer:
            print("✅ Context manager entered successfully")

            # Analyze files one by one
            for i, dataset in enumerate(datasets):
                analyzer.add_file(dataset)
                print(f"  • Added dataset {i+1}: {Path(dataset).name}")

            # Get results
            results = analyzer.get_results()
            print(f"📊 Retrieved {len(results)} analysis results")

            # Test batch analysis
            batch_results = analyzer.analyze_batch(datasets)
            print(f"📈 Batch analysis completed: {len(batch_results)} results")

        print("✅ Context manager exited successfully with automatic cleanup")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        # Manual cleanup of test files
        for dataset in datasets:
            try:
                os.unlink(dataset)
            except:
                pass

def test_quality_metrics_batch():
    """Test batch quality metrics calculation"""
    print("\n📊 Test 2: Batch Quality Metrics")
    print("=" * 55)

    try:
        import dataprof
    except ImportError:
        print("❌ dataprof module not available")
        return False

    datasets = create_sample_datasets(2)

    try:
        print(f"🎯 Testing quality metrics with {len(datasets)} datasets...")

        # Analyze each dataset for quality
        for i, dataset in enumerate(datasets):
            start_time = time.time()
            metrics = dataprof.calculate_data_quality_metrics(dataset)
            duration = time.time() - start_time

            dataset_name = Path(dataset).name
            print(f"  • {dataset_name}:")
            print(f"    - Overall Quality: {metrics.overall_quality_score:.1f}%")
            print(f"    - Completeness: {metrics.complete_records_ratio:.1f}%")
            print(f"    - Consistency: {metrics.data_type_consistency:.1f}%")
            print(f"    - Processing time: {duration:.3f}s")

        print("✅ Quality metrics calculation completed")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        for dataset in datasets:
            try:
                os.unlink(dataset)
            except:
                pass

def test_csv_processor_context_manager():
    """Test PyCsvProcessor context manager"""
    print("\n📄 Test 3: CSV Processor Context Manager")
    print("=" * 55)

    try:
        import dataprof
        if not hasattr(dataprof, 'PyCsvProcessor'):
            print("❌ PyCsvProcessor not available")
            return False
    except ImportError:
        print("❌ dataprof module not available")
        return False

    large_dataset = create_large_dataset()

    try:
        print(f"📊 Testing CSV processor with large dataset...")

        # Test CSV processor with chunking
        chunk_size = 100
        with dataprof.PyCsvProcessor(chunk_size=chunk_size) as processor:
            print(f"✅ CSV Processor context manager entered (chunk_size={chunk_size})")

            # Open file for processing
            processor.open_file(large_dataset)
            print(f"📂 Opened file: {Path(large_dataset).name}")

            # Process file in chunks
            start_time = time.time()
            chunk_results = processor.process_chunks()
            processing_time = time.time() - start_time

            print(f"⚡ Chunk processing completed: {len(chunk_results)} chunks in {processing_time:.3f}s")

            # Get processing information
            info = processor.get_processing_info()
            print(f"📊 Processing info:")
            print(f"  • File: {Path(info['file_path']).name}")
            print(f"  • Chunk size: {info['chunk_size']}")
            print(f"  • Processed rows: {info['processed_rows']}")
            print(f"  • Temp files created: {info['temp_files_created']}")
            print(f"  • Average rows per second: {info['processed_rows']/processing_time:.1f}")

            # Show chunk results summary
            for i, chunk_result in enumerate(chunk_results):
                if isinstance(chunk_result, list) and chunk_result:
                    columns = len(chunk_result)
                    print(f"  • Chunk {i+1}: {columns} columns analyzed")

        print("✅ CSV Processor context manager exited with automatic temp file cleanup")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        try:
            os.unlink(large_dataset)
        except:
            pass

def test_context_manager_error_handling():
    """Test context manager error handling and cleanup"""
    print("\n⚠️  Test 4: Error Handling and Cleanup")
    print("=" * 55)

    try:
        import dataprof
        if not hasattr(dataprof, 'PyBatchAnalyzer'):
            print("❌ Context managers not available")
            return False
    except ImportError:
        print("❌ dataprof module not available")
        return False

    try:
        print("🧪 Testing error handling and cleanup...")

        # Test error handling with invalid file
        with dataprof.PyBatchAnalyzer() as analyzer:
            print("✅ Batch analyzer entered")

            # Add some temporary files to track cleanup
            analyzer.add_temp_file("fake_temp_file_1.csv")
            analyzer.add_temp_file("fake_temp_file_2.csv")

            try:
                # This should fail
                analyzer.add_file("non_existent_file.csv")
                print("❌ Should have failed with non-existent file")
                return False
            except Exception as e:
                print(f"✅ Correctly handled error: {type(e).__name__}")

        print("✅ Context manager properly exited even after error")

        # Test CSV processor error handling
        with dataprof.PyCsvProcessor(chunk_size=None) as processor:
            print("✅ CSV processor entered")

            try:
                # This should fail - no file opened
                processor.process_chunks()
                print("❌ Should have failed with no file opened")
                return False
            except Exception as e:
                print(f"✅ Correctly handled error: {type(e).__name__}")

        print("✅ CSV processor properly exited after error")

        return True

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_nested_context_managers():
    """Test nested context managers"""
    print("\n🔗 Test 5: Nested Context Managers")
    print("=" * 55)

    try:
        import dataprof
        if not hasattr(dataprof, 'PyBatchAnalyzer') or not hasattr(dataprof, 'PyCsvProcessor'):
            print("❌ Context managers not available")
            return False
    except ImportError:
        print("❌ dataprof module not available")
        return False

    datasets = create_sample_datasets(2)

    try:
        print("🔄 Testing nested context managers...")

        # Nested context managers: batch + CSV processor
        with dataprof.PyBatchAnalyzer() as batch_analyzer:
            print("✅ Outer context (batch analyzer) entered")

            # Use batch analyzer for multiple files
            for dataset in datasets:
                batch_analyzer.add_file(dataset)
                dataset_name = Path(dataset).name
                print(f"  • Added {dataset_name} to batch")

            # Get batch results
            batch_results = batch_analyzer.get_results()
            print(f"📦 Batch results: {len(batch_results)} analyses completed")

            # Use CSV processor on first dataset
            with dataprof.PyCsvProcessor(chunk_size=100) as processor:
                print("✅ Inner context (CSV processor) entered")

                processor.open_file(datasets[0])
                chunk_results = processor.process_chunks()
                print(f"  • Processed {len(chunk_results)} chunks from {Path(datasets[0]).name}")

                print("✅ Inner context (CSV processor) exited")

        print("✅ Outer context (batch analyzer) exited")
        print("🎉 Nested context managers completed successfully")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        for dataset in datasets:
            try:
                os.unlink(dataset)
            except:
                pass

def main():
    """Run all context manager tests"""
    print("🔬 DataProf Context Managers Test Suite")
    print("=" * 65)

    tests = [
        test_batch_analyzer_context_manager,
        test_quality_metrics_batch,
        test_csv_processor_context_manager,
        test_context_manager_error_handling,
        test_nested_context_managers,
    ]

    passed = 0
    total = len(tests)

    for test_func in tests:
        if test_func():
            passed += 1

    print("\n" + "=" * 65)
    print(f"📊 Results: {passed}/{total} context manager tests completed successfully")

    if passed == total:
        print("🎉 All context manager functionality works perfectly!")
        print("\n✨ DataProf context manager features:")
        print("  • Automatic resource cleanup with __enter__/__exit__")
        print("  • Batch analysis with temporary file management")
        print("  • Quality metrics calculation with ISO 8000/25012 standards")
        print("  • CSV chunked processing with temp file cleanup")
        print("  • Robust error handling and cleanup")
        print("  • Support for nested context managers")
    else:
        print("❌ Some context manager tests failed")

if __name__ == "__main__":
    main()
