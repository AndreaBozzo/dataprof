#!/usr/bin/env python3
"""
Test ML readiness functionality
"""

import dataprof
import os
import sys
import tempfile

def create_test_csv(content: str) -> str:
    """Create a temporary CSV file with given content"""
    fd, path = tempfile.mkstemp(suffix='.csv')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(content)
        return path
    except:
        os.close(fd)
        raise

def test_ml_readiness_basic():
    """Test basic ML readiness functionality"""
    print("🧪 Testing ML readiness basic functionality...")

    # Create test data with various ML characteristics
    test_data = """name,age,salary,city,email,date_joined
Alice,25,50000.0,New York,alice@example.com,2023-01-15
Bob,30,75000.0,London,bob@example.com,2023-02-20
Charlie,35,,Paris,charlie@invalid-email,2023-03-10
Diana,28,65000.0,Tokyo,diana@example.com,2023-04-05
Eve,32,80000.0,New York,eve@example.com,2023-05-12
Frank,45,95000.0,London,,2023-06-18
Grace,29,55000.0,Paris,grace@example.com,2023-07-22"""

    test_file = create_test_csv(test_data)

    try:
        # Test ml_readiness_score function
        ml_score = dataprof.ml_readiness_score(test_file)

        print(f"✅ ML Readiness Score calculated")
        print(f"  📊 Overall Score: {ml_score.overall_score:.1f}%")
        print(f"  📈 Readiness Level: {ml_score.readiness_level}")
        print(f"  🔍 Features: {len(ml_score.feature_analysis)}")
        print(f"  💡 Recommendations: {len(ml_score.recommendations)}")
        print(f"  ⚠️ Blocking Issues: {len(ml_score.blocking_issues)}")

        # Test helper methods
        print(f"  🚦 Is ML Ready: {ml_score.is_ml_ready()}")
        print(f"  📝 Summary: {ml_score.summary()}")

        # Test filtering methods
        high_priority_recs = ml_score.recommendations_by_priority("high")
        print(f"  🔥 High Priority Recommendations: {len(high_priority_recs)}")

        good_features = ml_score.features_by_suitability(0.7)
        print(f"  ⭐ Good Features (>70%): {len(good_features)}")

        critical_preprocessing = ml_score.preprocessing_by_priority("critical")
        print(f"  🚨 Critical Preprocessing: {len(critical_preprocessing)}")

        # Test feature analysis details
        for feature in ml_score.feature_analysis[:3]:  # Show first 3
            print(f"  📊 {feature.column_name}: {feature.feature_type} ({feature.ml_suitability:.2f})")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        try:
            os.unlink(test_file)
        except:
            pass

def test_analyze_csv_for_ml():
    """Test comprehensive CSV analysis for ML"""
    print("\n🧪 Testing comprehensive ML analysis...")

    # Create test data with ML challenges
    test_data = """product_id,price,category,description,stock,rating,mixed_types
P001,29.99,Electronics,Great gadget,100,4.5,123
P002,19.99,Books,Nice book,50,3.8,abc
P003,,Clothing,Comfy shirt,0,4.2,45.6
P004,39.99,Electronics,Cool device,,5.0,mixed
P005,9.99,Books,Good read,25,invalid,true"""

    test_file = create_test_csv(test_data)

    try:
        # Test combined analysis
        quality_report, ml_score = dataprof.analyze_csv_for_ml(test_file)

        print(f"✅ Combined analysis completed")
        print(f"  📊 Quality Score: {quality_report.quality_score():.1f}%")
        print(f"  🤖 ML Score: {ml_score.overall_score:.1f}%")
        print(f"  📈 Data Quality Issues: {len(quality_report.issues)}")
        print(f"  🚫 ML Blocking Issues: {len(ml_score.blocking_issues)}")

        # Show specific recommendations
        if ml_score.recommendations:
            print("  💡 Top ML Recommendations:")
            for rec in ml_score.recommendations[:3]:
                print(f"    • {rec.category}: {rec.description[:50]}...")

        # Show blocking issues
        if ml_score.blocking_issues:
            print("  🚫 Blocking Issues:")
            for issue in ml_score.blocking_issues:
                print(f"    • {issue.issue_type}: {issue.description}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        try:
            os.unlink(test_file)
        except:
            pass

def test_ml_edge_cases():
    """Test ML readiness with edge cases"""
    print("\n🧪 Testing ML readiness edge cases...")

    # Test with constant values (should get low variance warning)
    constant_data = """id,constant_col,varying_col
1,SAME,10
2,SAME,20
3,SAME,30
4,SAME,40"""

    test_file = create_test_csv(constant_data)

    try:
        ml_score = dataprof.ml_readiness_score(test_file)

        print(f"✅ Edge case analysis completed")
        print(f"  📊 Overall Score: {ml_score.overall_score:.1f}%")

        # Look for low variance features
        low_variance_features = [f for f in ml_score.feature_analysis
                               if f.feature_type == "low_variance"]
        print(f"  ⚠️ Low Variance Features: {len(low_variance_features)}")

        if low_variance_features:
            for feature in low_variance_features:
                print(f"    • {feature.column_name}: {feature.potential_issues}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        try:
            os.unlink(test_file)
        except:
            pass

def main():
    """Main test runner"""
    print("🚀 DataProfiler ML Readiness Test Suite")
    print("=" * 50)

    tests = [
        test_ml_readiness_basic,
        test_analyze_csv_for_ml,
        test_ml_edge_cases,
    ]

    passed = 0
    total = len(tests)

    for test in tests:
        if test():
            passed += 1

    print("\n" + "=" * 50)
    print(f"📊 Results: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All ML readiness tests passed!")
        sys.exit(0)
    else:
        print("❌ Some ML readiness tests failed")
        sys.exit(1)

if __name__ == "__main__":
    main()