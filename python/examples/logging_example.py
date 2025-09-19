#!/usr/bin/env python3
"""
DataProf Python Logging Integration Examples
"""

import tempfile
import os
import sys
from pathlib import Path

def create_sample_csv():
    """Create a sample CSV file for testing"""
    fd, path = tempfile.mkstemp(suffix='_logging_test.csv', prefix='dataprof_log_')

    sample_data = """name,age,salary,department,hire_date
Alice Johnson,28,85000,Engineering,2020-03-15
Bob Smith,35,65000,Marketing,2019-08-22
Charlie Brown,42,58000,Sales,2021-01-10
Diana Prince,31,62000,HR,2020-11-05
Eve Wilson,29,78000,Engineering,2018-06-30
Frank Miller,45,72000,Marketing,2017-02-14
Grace Lee,33,68000,HR,2019-05-20
Henry Kim,38,82000,Engineering,2016-11-08
Iris Chen,26,55000,Sales,2022-01-03
Jack Wong,40,76000,Engineering,2015-07-12"""

    try:
        with os.fdopen(fd, 'w') as f:
            f.write(sample_data)
        return path
    except:
        os.close(fd)
        raise

def test_basic_logging():
    """Test basic logging configuration and functions"""
    print("\n🔧 Test 1: Basic Logging Configuration")
    print("=" * 55)

    try:
        import dataprof

        # Configure logging
        print("📝 Configuring logging...")
        dataprof.configure_logging(level="INFO")

        # Test different log levels
        print("📋 Testing log levels...")
        dataprof.log_info("This is an info message from DataProf")
        dataprof.log_debug("This is a debug message (might not show if level is INFO)")
        dataprof.log_warning("This is a warning message")
        dataprof.log_error("This is an error message")

        # Get logger instance
        logger = dataprof.get_logger("dataprof.test")
        print(f"📊 Got logger instance: {type(logger)}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_enhanced_analysis_with_logging():
    """Test enhanced analysis functions with logging"""
    print("\n📊 Test 2: Enhanced Analysis with Logging")
    print("=" * 55)

    sample_file = None

    try:
        import dataprof

        # Create sample data
        sample_file = create_sample_csv()
        print(f"📁 Created sample file: {Path(sample_file).name}")

        # Test CSV analysis with logging
        print("\n📈 Testing CSV analysis with logging...")
        profiles = dataprof.analyze_csv_with_logging(sample_file, log_level="INFO")
        print(f"✅ CSV analysis completed: {len(profiles)} columns analyzed")

        # Show some results
        print("📋 First few columns:")
        for i, profile in enumerate(profiles[:3]):
            print(f"  • {profile.name}: {profile.data_type} ({profile.null_count} nulls)")

        # Test ML readiness analysis with logging
        print("\n🧠 Testing ML readiness analysis with logging...")
        ml_score = dataprof.ml_readiness_score_with_logging(sample_file, log_level="INFO")
        print(f"✅ ML analysis completed: {ml_score.overall_score:.1f}% ({ml_score.readiness_level})")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        if sample_file:
            try:
                os.unlink(sample_file)
            except:
                pass

def test_custom_logger():
    """Test custom logger names and configurations"""
    print("\n🎯 Test 3: Custom Logger Configuration")
    print("=" * 55)

    try:
        import dataprof

        # Configure with DEBUG level and custom format
        print("🔧 Configuring custom logging...")
        custom_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        dataprof.configure_logging(level="DEBUG", format=custom_format)

        # Test with custom logger names
        dataprof.log_info("Message from default logger")
        dataprof.log_info("Message from custom logger", logger_name="dataprof.custom")
        dataprof.log_debug("Debug message (should now be visible)", logger_name="dataprof.debug")
        dataprof.log_warning("Warning from analysis module", logger_name="dataprof.analysis")

        # Get custom loggers
        default_logger = dataprof.get_logger()
        custom_logger = dataprof.get_logger("dataprof.ml")
        analysis_logger = dataprof.get_logger("dataprof.analysis")

        print(f"📊 Got {3} different logger instances")
        print(f"✅ Custom logging configuration successful")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_integration_with_python_logging():
    """Test integration with standard Python logging"""
    print("\n🔗 Test 4: Integration with Python Logging")
    print("=" * 55)

    try:
        import logging
        import dataprof

        # Set up Python logging first
        logging.basicConfig(
            level=logging.INFO,
            format='[PYTHON] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        python_logger = logging.getLogger("test.python")
        python_logger.info("Message from Python logging")

        # Use DataProf logging
        dataprof.configure_logging(level="INFO", format="[DATAPROF] %(asctime)s - %(name)s - %(levelname)s - %(message)s")
        dataprof.log_info("Message from DataProf logging")

        # Both should work together
        python_logger.warning("Warning from Python")
        dataprof.log_warning("Warning from DataProf")

        print("✅ Python and DataProf logging integration successful")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_performance_logging():
    """Test performance measurement logging"""
    print("\n⚡ Test 5: Performance Logging")
    print("=" * 55)

    sample_files = []

    try:
        import dataprof
        import time

        # Configure logging for performance tracking
        dataprof.configure_logging(level="INFO")

        # Create multiple sample files
        print("📁 Creating test files...")
        for i in range(3):
            sample_file = create_sample_csv()
            sample_files.append(sample_file)

        # Test batch analysis with logging
        print("📊 Running batch analysis with logging...")
        start_time = time.time()

        for i, sample_file in enumerate(sample_files):
            dataprof.log_info(f"Processing file {i+1}/{len(sample_files)}: {Path(sample_file).name}")

            # Analyze with logging
            profiles = dataprof.analyze_csv_with_logging(sample_file)
            ml_score = dataprof.ml_readiness_score_with_logging(sample_file)

            dataprof.log_info(f"File {i+1} completed: {len(profiles)} columns, ML score {ml_score.overall_score:.1f}%")

        total_time = time.time() - start_time
        dataprof.log_info(f"Batch processing completed in {total_time:.3f}s")

        print(f"✅ Performance logging test completed")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        for sample_file in sample_files:
            try:
                os.unlink(sample_file)
            except:
                pass

def main():
    """Run all logging tests"""
    print("🔬 DataProf Python Logging Integration Test Suite")
    print("=" * 65)

    tests = [
        test_basic_logging,
        test_enhanced_analysis_with_logging,
        test_custom_logger,
        test_integration_with_python_logging,
        test_performance_logging,
    ]

    passed = 0
    total = len(tests)

    for test_func in tests:
        if test_func():
            passed += 1

    print("\n" + "=" * 65)
    print(f"📊 Results: {passed}/{total} logging tests completed successfully")

    if passed == total:
        print("🎉 All logging functionality works perfectly!")
        print("\n✨ DataProf logging features:")
        print("  • Python logging integration with configurable levels")
        print("  • Custom logger names and formats")
        print("  • Enhanced analysis functions with automatic logging")
        print("  • Performance tracking and timing")
        print("  • Integration with existing Python logging setups")
        print("  • Standard log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL")
    else:
        print("❌ Some logging tests failed")

if __name__ == "__main__":
    main()