#!/usr/bin/env python3
"""
Test script for DataProfiler Python bindings
"""

import dataprof
import os
import sys

def test_csv_analysis():
    """Test basic CSV analysis functionality"""
    print("🧪 Testing CSV analysis...")

    # Use test files from tests/data directory
    test_file = os.path.join("..", "..", "tests", "data", "customer_data_clean.csv")
    if not os.path.exists(test_file):
        test_file = os.path.join("tests", "data", "customer_data_clean.csv")

    if not os.path.exists(test_file):
        print("❌ Test file not found")
        return False

    try:
        # Test basic analysis
        profiles = dataprof.analyze_csv_file(test_file)
        print(f"✅ Found {len(profiles)} columns")

        for profile in profiles:
            print(f"  📊 {profile.name}: {profile.data_type} (null: {profile.null_percentage:.1f}%)")

        # Test quality analysis
        quality_report = dataprof.analyze_csv_with_quality(test_file)
        print(f"✅ Quality report - {quality_report.total_rows} rows, {quality_report.total_columns} columns")
        print(f"📈 Quality score: {quality_report.quality_score():.1f}%")
        print(f"⚠️ Issues found: {len(quality_report.issues)}")

        for issue in quality_report.issues:
            print(f"  🔍 {issue.issue_type}: {issue.description}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def test_json_analysis():
    """Test JSON analysis functionality"""
    print("\n🧪 Testing JSON analysis...")

    # Create a simple test JSON file
    test_data = """[
        {"name": "John", "age": 25, "email": "john@example.com"},
        {"name": "Jane", "age": 30, "email": "jane@example.com"},
        {"name": "Bob", "age": 35}
    ]"""

    test_file = "test_data.json"

    try:
        with open(test_file, 'w') as f:
            f.write(test_data)

        profiles = dataprof.analyze_json_file(test_file)
        print(f"✅ Found {len(profiles)} columns")

        for profile in profiles:
            print(f"  📊 {profile.name}: {profile.data_type} (null: {profile.null_percentage:.1f}%)")

        # Cleanup
        os.remove(test_file)
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        # Cleanup on error
        if os.path.exists(test_file):
            os.remove(test_file)
        return False

def test_batch_analysis():
    """Test batch analysis functionality"""
    print("\n🧪 Testing batch analysis...")

    # Create a temporary test directory with CSV files
    test_dir = "temp_test_batch"
    try:
        os.makedirs(test_dir, exist_ok=True)

        # Create test CSV files
        test_files = []
        for i in range(2):
            test_file = os.path.join(test_dir, f"test_data_{i}.csv")
            test_files.append(test_file)
            with open(test_file, 'w') as f:
                f.write("name,age,city\n")
                f.write(f"Person{i},2{i},City{i}\n")
                f.write(f"User{i},3{i},Town{i}\n")

        # Test batch processing
        result = dataprof.batch_analyze_directory(test_dir, recursive=False, parallel=True)
        print(f"✅ Processed {result.processed_files} files in {result.total_duration_secs:.2f}s")
        print(f"📊 Average quality: {result.average_quality_score:.1f}%")
        print(f"⚠️ Total issues: {result.total_quality_issues}")

        # Cleanup
        for test_file in test_files:
            try:
                os.remove(test_file)
            except:
                pass
        try:
            os.rmdir(test_dir)
        except:
            pass

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        # Cleanup on error
        for test_file in test_files if 'test_files' in locals() else []:
            try:
                os.remove(test_file)
            except:
                pass
        try:
            os.rmdir(test_dir)
        except:
            pass
        return False

def test_data_quality_metrics():
    """Test data quality metrics functionality"""
    print("\n🧪 Testing DataQualityMetrics...")

    # Use test files from tests/data directory
    test_file = os.path.join("..", "..", "tests", "data", "customer_data_clean.csv")
    if not os.path.exists(test_file):
        test_file = os.path.join("tests", "data", "customer_data_clean.csv")

    if not os.path.exists(test_file):
        print("❌ Test file not found")
        return False

    try:
        # Test dedicated data quality metrics function
        metrics = dataprof.calculate_data_quality_metrics(test_file)

        if metrics is None:
            print("❌ No data quality metrics returned")
            return False

        print(f"✅ Data Quality Metrics calculated")
        print(f"📊 Overall quality score: {metrics.overall_quality_score():.1f}%")
        print(f"📋 Completeness: {metrics.completeness_summary()}")
        print(f"🔧 Consistency: {metrics.consistency_summary()}")
        print(f"🔑 Uniqueness: {metrics.uniqueness_summary()}")
        print(f"🎯 Accuracy: {metrics.accuracy_summary()}")

        # Test that metrics are included in quality report
        quality_report = dataprof.analyze_csv_with_quality(test_file)
        if quality_report.data_quality_metrics is not None:
            report_metrics = quality_report.data_quality_metrics
            print(f"✅ Quality report includes metrics")
            print(f"📈 Report metrics score: {report_metrics.overall_quality_score():.1f}%")

            # Verify consistency between standalone metrics and report metrics
            standalone_score = metrics.overall_quality_score()
            report_score = report_metrics.overall_quality_score()

            if abs(standalone_score - report_score) < 0.1:
                print("✅ Metrics consistency verified")
            else:
                print(f"⚠️ Metrics inconsistency: standalone={standalone_score:.1f}%, report={report_score:.1f}%")
        else:
            print("⚠️ Quality report missing data_quality_metrics")

        # Test string representation
        metrics_str = str(metrics)
        print(f"✅ String representation: {metrics_str}")

        # Test summary dict
        summary = metrics.summary_dict()
        print(f"✅ Summary dict has {len(summary)} entries")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test runner"""
    print("🚀 DataProfiler Python Bindings Test Suite")
    print("=" * 50)

    tests = [
        test_csv_analysis,
        test_json_analysis,
        test_batch_analysis,
        test_data_quality_metrics
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1

    print("\n" + "=" * 50)
    print(f"📊 Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed!")
        sys.exit(0)
    else:
        print("❌ Some tests failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
