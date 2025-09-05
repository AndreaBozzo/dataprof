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
    
    # Use test files from test_batch directory
    test_file = os.path.join("..", "test_batch", "customer_data_clean.csv")
    if not os.path.exists(test_file):
        test_file = os.path.join("test_batch", "customer_data_clean.csv")
    
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
    
    test_dir = "test_batch"
    if not os.path.exists(test_dir):
        test_dir = os.path.join("..", "test_batch")
    
    if not os.path.exists(test_dir):
        print("❌ Test directory not found")
        return False
    
    try:
        result = dataprof.batch_analyze_directory(test_dir, recursive=False, parallel=True)
        print(f"✅ Processed {result.processed_files} files in {result.total_duration_secs:.2f}s")
        print(f"📊 Performance: {result.files_per_second:.1f} files/sec")
        print(f"⚠️ Total issues: {result.total_quality_issues}")
        print(f"📈 Average quality: {result.average_quality_score:.1f}%")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Main test runner"""
    print("🚀 DataProfiler Python Bindings Test Suite")
    print("=" * 50)
    
    tests = [
        test_csv_analysis,
        test_json_analysis, 
        test_batch_analysis
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