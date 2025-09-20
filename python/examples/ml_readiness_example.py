#!/usr/bin/env python3
"""
ML Readiness Example - Shows how to use the new ML readiness features
"""

import dataprof
import tempfile
import os

def create_sample_ml_dataset():
    """Create a sample dataset for ML readiness testing"""
    sample_data = """customer_id,age,income,education,city,purchase_amount,has_purchased,signup_date,email,mixed_column
CUST001,25,50000,Bachelor,New York,129.99,yes,2023-01-15,alice@email.com,123
CUST002,34,75000,Master,London,89.50,no,2023-02-20,bob@email.com,abc
CUST003,28,,High School,Paris,199.99,yes,2023-03-10,charlie@invalid,45.6
CUST004,45,95000,PhD,Tokyo,0,no,2023-04-05,diana@email.com,mixed
CUST005,32,65000,Bachelor,Berlin,250.00,yes,15/05/2023,eve@email.com,true
CUST006,29,55000,Master,Sydney,,yes,2023-06-18,,false
CUST007,38,85000,Bachelor,Toronto,175.25,no,2023-07-22,frank@email.com,12.34"""

    # Create temporary file
    fd, path = tempfile.mkstemp(suffix='.csv')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(sample_data)
        return path
    except:
        os.close(fd)
        raise

def ml_readiness_basic_example():
    """Basic ML readiness assessment example"""
    print("🤖 ML Readiness Assessment Example")
    print("=" * 50)

    csv_file = create_sample_ml_dataset()

    try:
        # Calculate ML readiness score
        ml_score = dataprof.ml_readiness_score(csv_file)

        print(f"📊 Overall ML Readiness: {ml_score.overall_score:.1f}%")
        print(f"🎯 Readiness Level: {ml_score.readiness_level}")
        print(f"✅ Is ML Ready: {ml_score.is_ml_ready()}")
        print()

        # Component scores
        print("📈 Component Scores:")
        print(f"  • Completeness: {ml_score.completeness_score:.1f}%")
        print(f"  • Consistency: {ml_score.consistency_score:.1f}%")
        print(f"  • Type Suitability: {ml_score.type_suitability_score:.1f}%")
        print(f"  • Feature Quality: {ml_score.feature_quality_score:.1f}%")
        print()

        # Blocking issues
        if ml_score.blocking_issues:
            print("🚫 Blocking Issues (must be resolved):")
            for issue in ml_score.blocking_issues:
                print(f"  • {issue.issue_type}: {issue.description}")
                print(f"    Resolution: {issue.resolution_required}")
        else:
            print("✅ No blocking issues found!")
        print()

        # High priority recommendations
        high_priority = ml_score.recommendations_by_priority("high")
        if high_priority:
            print("🔥 High Priority Recommendations:")
            for rec in high_priority:
                print(f"  • {rec.category}: {rec.description}")
                print(f"    Impact: {rec.expected_impact}")
                print(f"    Effort: {rec.implementation_effort}")
        print()

        # Feature analysis
        print("🔍 Feature Analysis:")
        good_features = ml_score.features_by_suitability(0.7)
        print(f"  • {len(good_features)} features ready for ML (>70% suitability)")

        for feature in ml_score.feature_analysis:
            suitability_emoji = "🟢" if feature.ml_suitability > 0.7 else "🟡" if feature.ml_suitability > 0.4 else "🔴"
            print(f"  {suitability_emoji} {feature.column_name}: {feature.feature_type} ({feature.ml_suitability:.2f})")

            if feature.potential_issues:
                print(f"    Issues: {', '.join(feature.potential_issues)}")
            if feature.encoding_suggestions:
                print(f"    Suggestions: {', '.join(feature.encoding_suggestions[:2])}")
        print()

        # Preprocessing pipeline
        print("🔧 Preprocessing Pipeline:")
        for step in ml_score.preprocessing_suggestions:
            priority_emoji = "🔴" if step.priority == "critical" else "🟡" if step.priority == "high" else "🟢"
            print(f"  {priority_emoji} {step.step}")
            print(f"    Description: {step.description}")
            print(f"    Columns: {', '.join(step.columns_affected[:3])}{'...' if len(step.columns_affected) > 3 else ''}")
            print(f"    Tools: {', '.join(step.tools_frameworks[:2])}")

    except Exception as e:
        print(f"❌ Error: {e}")

    finally:
        try:
            os.unlink(csv_file)
        except:
            pass

def comprehensive_ml_analysis_example():
    """Comprehensive analysis combining data quality and ML readiness"""
    print("\n🔬 Comprehensive ML Analysis Example")
    print("=" * 50)

    csv_file = create_sample_ml_dataset()

    try:
        # Get both quality report and ML score
        quality_report, ml_score = dataprof.analyze_csv_for_ml(csv_file)

        print(f"📊 Dataset Overview:")
        print(f"  • Rows: {quality_report.total_rows}")
        print(f"  • Columns: {quality_report.total_columns}")
        print(f"  • Scan time: {quality_report.scan_time_ms}ms")
        print()

        print(f"📈 Assessment Scores:")
        print(f"  • Data Quality: {quality_report.quality_score():.1f}%")
        print(f"  • ML Readiness: {ml_score.overall_score:.1f}%")
        print()

        # Quick summary
        print(f"📝 Quick Summary:")
        print(f"  • {ml_score.summary()}")

        # Data quality issues
        if quality_report.issues:
            print(f"\n⚠️ Data Quality Issues ({len(quality_report.issues)}):")
            for issue in quality_report.issues:
                print(f"  • {issue.issue_type}: {issue.description}")

        # ML recommendations
        if ml_score.recommendations:
            print(f"\n💡 ML Recommendations ({len(ml_score.recommendations)}):")
            for rec in ml_score.recommendations[:3]:  # Show top 3
                print(f"  • {rec.category} ({rec.priority}): {rec.description[:60]}...")

    except Exception as e:
        print(f"❌ Error: {e}")

    finally:
        try:
            os.unlink(csv_file)
        except:
            pass

def ml_workflow_integration_example():
    """Example showing how to integrate ML readiness in a typical workflow"""
    print("\n🔄 ML Workflow Integration Example")
    print("=" * 50)

    csv_file = create_sample_ml_dataset()

    try:
        # Step 1: Check ML readiness
        ml_score = dataprof.ml_readiness_score(csv_file)

        print("Step 1: ML Readiness Check")
        if ml_score.is_ml_ready():
            print("✅ Data is ready for ML!")
        else:
            print(f"⚠️ Data needs work (score: {ml_score.overall_score:.1f}%)")

            # Show what needs to be fixed
            if ml_score.blocking_issues:
                print("🚫 Must fix these issues first:")
                for issue in ml_score.blocking_issues:
                    print(f"  • {issue.description}")

        print("\nStep 2: Feature Preparation Guide")
        features_ready = 0
        features_need_work = 0

        for feature in ml_score.feature_analysis:
            if feature.ml_suitability > 0.7:
                features_ready += 1
            else:
                features_need_work += 1

        print(f"✅ {features_ready} features ready")
        print(f"🔧 {features_need_work} features need preprocessing")

        print("\nStep 3: Preprocessing Checklist")
        critical_steps = ml_score.preprocessing_by_priority("critical")
        high_steps = ml_score.preprocessing_by_priority("high")

        print(f"🔴 Critical steps: {len(critical_steps)}")
        print(f"🟡 High priority steps: {len(high_steps)}")

        # Generate a simple checklist
        print("\n📋 Action Items:")
        all_steps = critical_steps + high_steps
        for i, step in enumerate(all_steps, 1):
            print(f"  {i}. {step.step}")
            print(f"     → {step.description}")
            print(f"     → Columns: {', '.join(step.columns_affected)}")
            print()

    except Exception as e:
        print(f"❌ Error: {e}")

    finally:
        try:
            os.unlink(csv_file)
        except:
            pass

if __name__ == "__main__":
    print("🚀 DataProf ML Readiness Examples")
    print("This demonstrates the new ML readiness assessment features")
    print()

    try:
        ml_readiness_basic_example()
        comprehensive_ml_analysis_example()
        ml_workflow_integration_example()

        print("\n🎉 All examples completed successfully!")
        print("\n💡 Next steps:")
        print("  • Install with: pip install dataprof")
        print("  • Try with your own datasets")
        print("  • Integrate into your ML pipeline")

    except ImportError:
        print("❌ DataProf not installed. Please install the Python package first.")
        print("   Build with: maturin develop --features python")
    except Exception as e:
        print(f"❌ Error running examples: {e}")
