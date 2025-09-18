#!/usr/bin/env python3
"""
DataProf + scikit-learn integration examples
"""

import dataprof
import tempfile
import os

def create_sample_dataset():
    """Create a sample dataset for ML workflow demonstration"""
    content = """customer_id,age,income,credit_score,account_balance,loan_amount,employment_years,education,city,risk_category
1,25,45000.0,650,1200.50,5000.0,2,bachelor,New York,low
2,35,,720,8500.75,,8,masters,London,medium
3,45,85000.0,580,2100.00,15000.0,15,high_school,Paris,high
4,29,52000.0,690,3400.25,8000.0,4,bachelor,Tokyo,low
5,38,,,4200.80,12000.0,10,masters,Berlin,medium
6,52,95000.0,750,7800.00,20000.0,20,phd,Sydney,low
7,31,48000.0,620,1800.75,6000.0,6,bachelor,,medium
8,,62000.0,700,5600.50,10000.0,12,masters,Toronto,low
9,27,35000.0,600,900.25,3000.0,1,high_school,Madrid,high
10,42,78000.0,680,6200.00,18000.0,16,bachelor,Amsterdam,medium"""

    fd, path = tempfile.mkstemp(suffix='.csv')
    try:
        with os.fdopen(fd, 'w') as f:
            f.write(content)
        return path
    except:
        os.close(fd)
        raise

def example_basic_ml_workflow():
    """Example 1: Basic ML workflow with DataProf preprocessing guidance"""
    print("🚀 Example 1: Basic ML Workflow with DataProf")
    print("=" * 60)

    dataset_path = create_sample_dataset()

    try:
        # Step 1: Get ML readiness assessment
        ml_score = dataprof.ml_readiness_score(dataset_path)

        print(f"📊 ML Readiness: {ml_score.readiness_level} ({ml_score.overall_score:.1f}%)")
        print(f"🎯 Completeness: {ml_score.completeness_score:.1f}%")
        print(f"🔄 Consistency: {ml_score.consistency_score:.1f}%")
        print(f"📐 Type Suitability: {ml_score.type_suitability_score:.1f}%")
        print(f"⭐ Feature Quality: {ml_score.feature_quality_score:.1f}%")

        # Step 2: Get detailed feature analysis (get DataFrame for easier handling)
        features_df = dataprof.feature_analysis_dataframe(dataset_path)

        print(f"\n🔍 Feature Analysis ({len(features_df)} features):")

        # Categorize features by ML suitability
        high_suitability = features_df[features_df['ml_suitability'] > 0.7]
        medium_suitability = features_df[(features_df['ml_suitability'] >= 0.4) & (features_df['ml_suitability'] <= 0.7)]
        low_suitability = features_df[features_df['ml_suitability'] < 0.4]

        print(f"  ✅ High suitability (>70%): {len(high_suitability)} features")
        for _, row in high_suitability.iterrows():
            print(f"     • {row['column_name']}: {row['ml_suitability']:.1%} ({row['feature_type']})")

        print(f"  ⚠️  Medium suitability (40-70%): {len(medium_suitability)} features")
        for _, row in medium_suitability.iterrows():
            print(f"     • {row['column_name']}: {row['ml_suitability']:.1%} ({row['feature_type']}) - needs preprocessing")

        print(f"  ❌ Low suitability (<40%): {len(low_suitability)} features")
        for _, row in low_suitability.iterrows():
            print(f"     • {row['column_name']}: {row['ml_suitability']:.1%} ({row['feature_type']}) - consider dropping")

        # Step 3: Show preprocessing recommendations
        print(f"\n💡 Preprocessing Recommendations:")
        if ml_score.recommendations:
            for rec in ml_score.recommendations:
                priority_emoji = {"HIGH": "🔥", "MEDIUM": "⚡", "LOW": "📝"}.get(rec.priority, "•")
                print(f"  {priority_emoji} [{rec.priority}] {rec.category}: {rec.description}")
        else:
            print(f"  ✅ No recommendations needed - data is ready for ML!")

        # Step 4: Show blocking issues
        if ml_score.blocking_issues:
            print(f"\n🚫 Blocking Issues ({len(ml_score.blocking_issues)}):")
            for issue in ml_score.blocking_issues:
                severity_emoji = {"HIGH": "❌", "MEDIUM": "⚠️", "LOW": "ℹ️"}.get(getattr(issue, 'severity', 'UNKNOWN'), "•")
                print(f"  {severity_emoji} [{getattr(issue, 'severity', 'UNKNOWN')}] {getattr(issue, 'issue_type', 'Issue')}: {getattr(issue, 'description', 'No description')}")
        else:
            print(f"\n✅ No blocking issues found!")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        try:
            os.unlink(dataset_path)
        except:
            pass

def example_sklearn_preprocessing_workflow():
    """Example 2: DataProf-guided scikit-learn preprocessing pipeline"""
    print("\n🔬 Example 2: DataProf-Guided Scikit-learn Pipeline")
    print("=" * 60)

    dataset_path = create_sample_dataset()

    try:
        # Check for scikit-learn availability
        try:
            from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
            from sklearn.impute import SimpleImputer
            from sklearn.compose import ColumnTransformer
            from sklearn.pipeline import Pipeline
            import pandas as pd
            print("✅ scikit-learn and pandas available")
        except ImportError as e:
            print(f"⚠️ scikit-learn or pandas not available: {e}")
            print("💡 Install with: pip install scikit-learn pandas")
            return False

        # Step 1: Load data and get DataProf analysis
        df = pd.read_csv(dataset_path)
        print(f"📊 Dataset shape: {df.shape}")

        ml_score = dataprof.ml_readiness_score(dataset_path)
        features = dataprof.analyze_csv_for_ml(dataset_path)

        # Step 2: Categorize columns based on DataProf analysis
        features_df = dataprof.feature_analysis_dataframe(dataset_path)

        numeric_features = []
        categorical_features = []
        text_features = []
        drop_features = []

        for _, row in features_df.iterrows():
            col_name = row['column_name']
            if col_name == 'customer_id':  # Skip ID columns
                drop_features.append(col_name)
                continue

            if row['feature_type'] in ['numeric', 'continuous']:
                numeric_features.append(col_name)
            elif row['feature_type'] in ['categorical', 'ordinal']:
                categorical_features.append(col_name)
            elif row['feature_type'] in ['text', 'high_cardinality']:
                if row['ml_suitability'] < 0.3:
                    drop_features.append(col_name)
                else:
                    text_features.append(col_name)
            else:
                # Let DataProf ML suitability decide
                if row['ml_suitability'] > 0.5:
                    categorical_features.append(col_name)
                else:
                    drop_features.append(col_name)

        print(f"📈 Feature categorization:")
        print(f"  • Numeric features: {numeric_features}")
        print(f"  • Categorical features: {categorical_features}")
        print(f"  • Text features: {text_features}")
        print(f"  • Features to drop: {drop_features}")

        # Step 3: Build preprocessing pipeline based on DataProf recommendations
        preprocessor_steps = []

        # Numeric pipeline
        if numeric_features:
            numeric_transformer = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='median')),
                ('scaler', StandardScaler())
            ])
            preprocessor_steps.append(('num', numeric_transformer, numeric_features))
            print(f"  ✅ Numeric pipeline: imputation (median) + standardization")

        # Categorical pipeline
        if categorical_features:
            categorical_transformer = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy='most_frequent')),
                ('onehot', OneHotEncoder(drop='first', sparse_output=False))
            ])
            preprocessor_steps.append(('cat', categorical_transformer, categorical_features))
            print(f"  ✅ Categorical pipeline: imputation (most_frequent) + one-hot encoding")

        # Create the preprocessor
        if preprocessor_steps:
            preprocessor = ColumnTransformer(
                transformers=preprocessor_steps,
                remainder='drop'  # Drop other columns
            )

            # Step 4: Apply preprocessing
            print(f"\n🔄 Applying preprocessing pipeline...")

            # Separate features and target
            X = df.drop(columns=['risk_category'] + drop_features)
            y = df['risk_category']

            print(f"📊 Input shape: {X.shape}")
            print(f"🎯 Target distribution: {y.value_counts().to_dict()}")

            # Transform features
            X_transformed = preprocessor.fit_transform(X)
            print(f"📊 Transformed shape: {X_transformed.shape}")

            # Encode target
            label_encoder = LabelEncoder()
            y_encoded = label_encoder.fit_transform(y)
            print(f"🏷️ Target classes: {dict(enumerate(label_encoder.classes_))}")

            print(f"✅ Preprocessing pipeline created successfully!")
            print(f"💡 Ready for ML model training with clean, encoded data")

        else:
            print(f"⚠️ No suitable features found for ML pipeline")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            os.unlink(dataset_path)
        except:
            pass

def example_pandas_dataframe_workflow():
    """Example 3: Using DataProf with pandas DataFrames"""
    print("\n🐼 Example 3: Pandas DataFrame Workflow")
    print("=" * 60)

    dataset_path = create_sample_dataset()

    try:
        try:
            import pandas as pd
            print("✅ pandas available")
        except ImportError:
            print("⚠️ pandas not available - install with: pip install pandas")
            return False

        # Step 1: Get DataProf analysis as DataFrames
        profiles_df = dataprof.analyze_csv_dataframe(dataset_path)
        features_df = dataprof.feature_analysis_dataframe(dataset_path)

        print(f"📊 Column Profiles DataFrame: {profiles_df.shape}")
        print(f"🤖 ML Features DataFrame: {features_df.shape}")

        # Step 2: Data quality analysis
        print(f"\n🔍 Data Quality Analysis:")
        quality_issues = profiles_df[profiles_df['null_percentage'] > 10.0]
        print(f"  • Columns with >10% nulls: {len(quality_issues)}")

        if not quality_issues.empty:
            print("  📋 Quality issues:")
            for _, row in quality_issues.iterrows():
                print(f"    - {row['column_name']}: {row['null_percentage']:.1f}% nulls")

        # Step 3: ML readiness analysis
        print(f"\n🤖 ML Readiness Analysis:")
        ml_ready = features_df[features_df['ml_suitability'] > 0.7]
        needs_work = features_df[(features_df['ml_suitability'] >= 0.4) & (features_df['ml_suitability'] <= 0.7)]
        poor_quality = features_df[features_df['ml_suitability'] < 0.4]

        print(f"  ✅ Ready for ML (>70%): {len(ml_ready)} features")
        if not ml_ready.empty:
            for _, row in ml_ready.iterrows():
                print(f"    - {row['column_name']}: {row['ml_suitability']:.1%} ({row['feature_type']})")

        print(f"  ⚡ Needs preprocessing (40-70%): {len(needs_work)} features")
        if not needs_work.empty:
            for _, row in needs_work.iterrows():
                print(f"    - {row['column_name']}: {row['ml_suitability']:.1%} ({row['feature_type']})")

        print(f"  ❌ Poor quality (<40%): {len(poor_quality)} features")
        if not poor_quality.empty:
            for _, row in poor_quality.iterrows():
                print(f"    - {row['column_name']}: {row['ml_suitability']:.1%} ({row['feature_type']})")

        # Step 4: Feature type distribution
        print(f"\n📊 Feature Type Distribution:")
        feature_types = features_df['feature_type'].value_counts()
        for ftype, count in feature_types.items():
            print(f"  • {ftype}: {count}")

        # Step 5: Importance potential analysis
        print(f"\n⭐ Importance Potential:")
        importance_dist = features_df['importance_potential'].value_counts()
        for importance, count in importance_dist.items():
            print(f"  • {importance}: {count}")

        print(f"\n✅ Pandas DataFrame workflow completed!")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            os.unlink(dataset_path)
        except:
            pass

def main():
    """Run all scikit-learn integration examples"""
    print("🔬 DataProf + Scikit-learn Integration Examples")
    print("=" * 70)

    examples = [
        example_basic_ml_workflow,
        example_sklearn_preprocessing_workflow,
        example_pandas_dataframe_workflow,
    ]

    passed = 0
    total = len(examples)

    for example in examples:
        if example():
            passed += 1

    print("\n" + "=" * 70)
    print(f"📊 Results: {passed}/{total} examples completed successfully")

    if passed == total:
        print("🎉 All scikit-learn integration examples work perfectly!")
        print("\n✨ DataProf + scikit-learn integration features:")
        print("  • ML readiness assessment guides preprocessing decisions")
        print("  • Feature analysis categorizes columns for pipeline building")
        print("  • Automated recommendations for encoding, scaling, imputation")
        print("  • Pandas DataFrame integration for seamless data workflows")
        print("  • Blocking issues detection prevents common ML pitfalls")
    else:
        print("❌ Some examples failed - check dependencies and data")

if __name__ == "__main__":
    main()