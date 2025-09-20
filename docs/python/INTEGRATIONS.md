# DataProf Ecosystem Integrations

Complete guide to integrating DataProf with popular data science and ML tools.

## 🐼 Pandas Integration

### DataFrame Output

Get DataProf analysis results as pandas DataFrames for seamless integration:

```python
import pandas as pd
import dataprof

# Column profiles as DataFrame
profiles_df = dataprof.analyze_csv_dataframe("data.csv")
print(profiles_df.head())

# ML feature analysis as DataFrame
features_df = dataprof.feature_analysis_dataframe("data.csv")
print(features_df[['column_name', 'feature_type', 'ml_suitability']])
```

### Data Manipulation Workflows

```python
# Filter columns by quality metrics
high_quality = profiles_df[profiles_df['null_percentage'] < 5.0]
low_unique = profiles_df[profiles_df['unique_percentage'] < 1.0]

# Group by data type
type_summary = profiles_df.groupby('data_type').agg({
    'null_percentage': 'mean',
    'unique_count': 'sum'
})

# Find problematic columns
problematic = profiles_df[
    (profiles_df['null_percentage'] > 50) |
    (profiles_df['unique_percentage'] < 0.1)
]['name'].tolist()
```

### Visualization with Pandas

```python
import matplotlib.pyplot as plt

# Plot null percentage distribution
plt.figure(figsize=(10, 6))
profiles_df['null_percentage'].hist(bins=20)
plt.title('Null Percentage Distribution')
plt.xlabel('Null Percentage')
plt.ylabel('Column Count')
plt.show()

# Data type distribution
type_counts = profiles_df['data_type'].value_counts()
type_counts.plot(kind='bar', title='Data Type Distribution')
plt.show()
```

## 🔬 Scikit-learn Integration

### Automated Pipeline Building

Use DataProf analysis to build scikit-learn preprocessing pipelines:

```python
from sklearn.preprocessing import StandardScaler, OneHotEncoder, LabelEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import VarianceThreshold

def build_sklearn_pipeline_from_dataprof(file_path, target_column=None):
    """Build sklearn pipeline based on DataProf analysis."""

    # Get DataProf analysis
    features_df = dataprof.feature_analysis_dataframe(file_path)
    ml_score = dataprof.ml_readiness_score(file_path)

    # Categorize features
    numeric_features = features_df[
        features_df['feature_type'] == 'numeric_ready'
    ]['column_name'].tolist()

    categorical_features = features_df[
        features_df['feature_type'] == 'categorical_needs_encoding'
    ]['column_name'].tolist()

    # Remove target column if specified
    if target_column:
        numeric_features = [f for f in numeric_features if f != target_column]
        categorical_features = [f for f in categorical_features if f != target_column]

    # Remove low-quality features
    low_quality = features_df[
        features_df['ml_suitability'] < 0.3
    ]['column_name'].tolist()

    numeric_features = [f for f in numeric_features if f not in low_quality]
    categorical_features = [f for f in categorical_features if f not in low_quality]

    # Build preprocessing pipeline
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', Pipeline([
                ('imputer', SimpleImputer(strategy='median')),
                ('scaler', StandardScaler())
            ]), numeric_features),
            ('cat', Pipeline([
                ('imputer', SimpleImputer(strategy='most_frequent')),
                ('onehot', OneHotEncoder(drop='first', handle_unknown='ignore'))
            ]), categorical_features)
        ],
        remainder='drop'  # Drop features not in either list
    )

    # Add variance threshold for feature selection
    full_pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('variance_selector', VarianceThreshold(threshold=0.01))
    ])

    return full_pipeline, {
        'numeric_features': numeric_features,
        'categorical_features': categorical_features,
        'dropped_features': low_quality,
        'ml_score': ml_score
    }

# Usage
pipeline, info = build_sklearn_pipeline_from_dataprof("data.csv", "target")
print(f"Pipeline includes {len(info['numeric_features'])} numeric and {len(info['categorical_features'])} categorical features")
```

### Feature Selection Integration

```python
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.ensemble import RandomForestClassifier

def select_features_with_dataprof(X, y, file_path, k=10):
    """Combine DataProf analysis with sklearn feature selection."""

    # Get DataProf feature analysis
    features_df = dataprof.feature_analysis_dataframe(file_path)

    # Create feature quality scores
    feature_scores = dict(zip(
        features_df['column_name'],
        features_df['ml_suitability']
    ))

    # Combine with statistical feature selection
    selector = SelectKBest(score_func=f_classif, k=k)
    X_selected = selector.fit_transform(X, y)

    # Get selected feature names
    selected_features = X.columns[selector.get_support()].tolist()

    # Rank by DataProf quality
    selected_with_quality = [
        (feat, feature_scores.get(feat, 0.0))
        for feat in selected_features
    ]
    selected_with_quality.sort(key=lambda x: x[1], reverse=True)

    return X_selected, selected_with_quality
```

### Model Performance Correlation

```python
def correlate_quality_with_performance(datasets_info):
    """Analyze correlation between DataProf scores and model performance."""

    results = []
    for dataset_path, model_score in datasets_info:
        ml_score = dataprof.ml_readiness_score(dataset_path)
        results.append({
            'dataset': dataset_path,
            'ml_readiness': ml_score.overall_score,
            'completeness': ml_score.completeness_score,
            'consistency': ml_score.consistency_score,
            'model_accuracy': model_score
        })

    results_df = pd.DataFrame(results)

    # Calculate correlations
    correlations = results_df[['ml_readiness', 'completeness', 'consistency']].corrwith(
        results_df['model_accuracy']
    )

    return results_df, correlations
```

## 📊 Data Visualization Libraries

### Matplotlib Integration

```python
import matplotlib.pyplot as plt
import seaborn as sns

def plot_dataprof_analysis(file_path):
    """Create comprehensive visualization of DataProf analysis."""

    # Get analysis data
    profiles_df = dataprof.analyze_csv_dataframe(file_path)
    features_df = dataprof.feature_analysis_dataframe(file_path)
    ml_score = dataprof.ml_readiness_score(file_path)

    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle(f'DataProf Analysis: {file_path}', fontsize=16)

    # 1. Null percentage distribution
    axes[0, 0].hist(profiles_df['null_percentage'], bins=20, alpha=0.7)
    axes[0, 0].set_title('Null Percentage Distribution')
    axes[0, 0].set_xlabel('Null Percentage')
    axes[0, 0].set_ylabel('Column Count')

    # 2. Data type distribution
    type_counts = profiles_df['data_type'].value_counts()
    axes[0, 1].pie(type_counts.values, labels=type_counts.index, autopct='%1.1f%%')
    axes[0, 1].set_title('Data Type Distribution')

    # 3. ML suitability distribution
    axes[0, 2].hist(features_df['ml_suitability'], bins=20, alpha=0.7, color='green')
    axes[0, 2].set_title('ML Suitability Distribution')
    axes[0, 2].set_xlabel('ML Suitability Score')
    axes[0, 2].set_ylabel('Feature Count')

    # 4. Feature type distribution
    feature_counts = features_df['feature_type'].value_counts()
    axes[1, 0].bar(range(len(feature_counts)), feature_counts.values)
    axes[1, 0].set_xticks(range(len(feature_counts)))
    axes[1, 0].set_xticklabels(feature_counts.index, rotation=45)
    axes[1, 0].set_title('Feature Type Distribution')

    # 5. ML readiness components
    components = [
        ml_score.completeness_score,
        ml_score.consistency_score,
        ml_score.type_suitability_score,
        ml_score.feature_quality_score
    ]
    component_names = ['Completeness', 'Consistency', 'Type Suitability', 'Feature Quality']

    bars = axes[1, 1].bar(component_names, components)
    axes[1, 1].set_title('ML Readiness Components')
    axes[1, 1].set_ylabel('Score')
    axes[1, 1].set_ylim(0, 100)

    # Color bars based on score
    for bar, score in zip(bars, components):
        if score >= 80:
            bar.set_color('green')
        elif score >= 60:
            bar.set_color('orange')
        else:
            bar.set_color('red')

    # 6. Quality vs ML suitability scatter
    merged_df = profiles_df.merge(
        features_df[['column_name', 'ml_suitability']],
        left_on='name',
        right_on='column_name',
        how='inner'
    )

    axes[1, 2].scatter(
        100 - merged_df['null_percentage'],  # Quality proxy
        merged_df['ml_suitability'],
        alpha=0.6
    )
    axes[1, 2].set_xlabel('Data Completeness (%)')
    axes[1, 2].set_ylabel('ML Suitability')
    axes[1, 2].set_title('Quality vs ML Suitability')

    plt.tight_layout()
    plt.show()

    return fig

# Usage
plot_dataprof_analysis("customer_data.csv")
```

### Seaborn Integration

```python
def create_seaborn_dashboard(file_path):
    """Create advanced visualizations with seaborn."""

    profiles_df = dataprof.analyze_csv_dataframe(file_path)
    features_df = dataprof.feature_analysis_dataframe(file_path)

    # Combine DataFrames
    combined_df = profiles_df.merge(
        features_df[['column_name', 'feature_type', 'ml_suitability']],
        left_on='name',
        right_on='column_name'
    )

    # Set style
    sns.set_style("whitegrid")

    # Create subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # Heatmap of correlations
    numeric_cols = ['null_percentage', 'unique_percentage', 'ml_suitability']
    corr_matrix = combined_df[numeric_cols].corr()
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, ax=axes[0, 0])
    axes[0, 0].set_title('Feature Correlation Matrix')

    # Box plot by feature type
    sns.boxplot(data=combined_df, x='feature_type', y='ml_suitability', ax=axes[0, 1])
    axes[0, 1].set_xticklabels(axes[0, 1].get_xticklabels(), rotation=45)
    axes[0, 1].set_title('ML Suitability by Feature Type')

    # Violin plot of null percentages
    sns.violinplot(data=combined_df, y='null_percentage', ax=axes[1, 0])
    axes[1, 0].set_title('Null Percentage Distribution')

    # Scatter plot with regression
    sns.scatterplot(
        data=combined_df,
        x='unique_percentage',
        y='ml_suitability',
        hue='feature_type',
        ax=axes[1, 1]
    )
    sns.regplot(
        data=combined_df,
        x='unique_percentage',
        y='ml_suitability',
        scatter=False,
        ax=axes[1, 1]
    )
    axes[1, 1].set_title('Uniqueness vs ML Suitability')

    plt.tight_layout()
    plt.show()
```

## 📱 Jupyter Notebook Integration

### Rich Display Objects

DataProf objects automatically display rich HTML in Jupyter:

```python
# In Jupyter notebook
import dataprof

# Rich display of ML readiness
ml_score = dataprof.ml_readiness_score("data.csv")
ml_score  # Automatically shows rich HTML visualization

# Rich display of quality report
report = dataprof.analyze_csv_with_quality("data.csv")
report  # Shows quality dashboard
```

### Interactive Widgets

```python
import ipywidgets as widgets
from IPython.display import display

def interactive_dataprof_analysis():
    """Create interactive DataProf analysis widget."""

    file_input = widgets.Text(
        value='data.csv',
        placeholder='Enter file path',
        description='File:',
    )

    analyze_button = widgets.Button(
        description='Analyze',
        button_style='success'
    )

    output = widgets.Output()

    def on_analyze_click(b):
        with output:
            output.clear_output()
            try:
                # Perform analysis
                profiles_df = dataprof.analyze_csv_dataframe(file_input.value)
                ml_score = dataprof.ml_readiness_score(file_input.value)

                print(f"📊 Analysis Results for: {file_input.value}")
                print(f"📈 ML Readiness: {ml_score.overall_score:.1f}% ({ml_score.readiness_level})")
                print(f"📋 Columns: {len(profiles_df)}")
                print(f"🎯 High-quality features: {len(profiles_df[profiles_df['null_percentage'] < 5])}")

                # Display top issues if any
                if hasattr(ml_score, 'blocking_issues') and ml_score.blocking_issues:
                    print("\n🚫 Blocking Issues:")
                    for issue in ml_score.blocking_issues[:3]:
                        print(f"  • {issue.description}")

            except Exception as e:
                print(f"❌ Error: {e}")

    analyze_button.on_click(on_analyze_click)

    display(widgets.VBox([file_input, analyze_button, output]))

# Usage in Jupyter
interactive_dataprof_analysis()
```

### Progress Tracking

```python
from tqdm.notebook import tqdm
import time

def batch_analyze_with_progress(file_paths):
    """Batch analysis with progress bar."""

    results = []

    for file_path in tqdm(file_paths, desc="Analyzing files"):
        try:
            ml_score = dataprof.ml_readiness_score(file_path)
            results.append({
                'file': file_path,
                'ml_readiness': ml_score.overall_score,
                'is_ready': ml_score.is_ml_ready()
            })
        except Exception as e:
            results.append({
                'file': file_path,
                'ml_readiness': 0,
                'is_ready': False,
                'error': str(e)
            })

        # Small delay to show progress
        time.sleep(0.1)

    return pd.DataFrame(results)
```

## 🌊 Apache Airflow Integration

### DataProf DAG Tasks

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import dataprof

def dataprof_quality_check(**context):
    """Airflow task for data quality checking."""

    file_path = context['params']['file_path']
    quality_threshold = context['params']['quality_threshold']

    # Perform quality check
    report = dataprof.analyze_csv_with_quality(file_path)
    quality_score = report.quality_score()

    # Log results
    print(f"Quality score: {quality_score:.1f}%")
    print(f"Issues found: {len(report.issues)}")

    # Check threshold
    if quality_score < quality_threshold:
        raise ValueError(f"Quality score {quality_score:.1f}% below threshold {quality_threshold}%")

    # Store results in XCom
    return {
        'quality_score': quality_score,
        'issues_count': len(report.issues),
        'file_path': file_path
    }

def dataprof_ml_readiness_check(**context):
    """Airflow task for ML readiness checking."""

    file_path = context['params']['file_path']

    ml_score = dataprof.ml_readiness_score(file_path)

    return {
        'ml_readiness': ml_score.overall_score,
        'is_ready': ml_score.is_ml_ready(),
        'blocking_issues': len(ml_score.blocking_issues) if hasattr(ml_score, 'blocking_issues') else 0
    }

# DAG definition
dag = DAG(
    'dataprof_pipeline',
    default_args={
        'owner': 'data-team',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='DataProf quality and ML readiness pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# Tasks
quality_task = PythonOperator(
    task_id='quality_check',
    python_callable=dataprof_quality_check,
    params={
        'file_path': '/data/daily_export.csv',
        'quality_threshold': 80.0
    },
    dag=dag
)

ml_readiness_task = PythonOperator(
    task_id='ml_readiness_check',
    python_callable=dataprof_ml_readiness_check,
    params={
        'file_path': '/data/daily_export.csv'
    },
    dag=dag
)

# Dependencies
quality_task >> ml_readiness_task
```

## 🔧 dbt Integration

### dbt Tests with DataProf

```python
# macros/dataprof_tests.sql
{% macro dataprof_quality_test(model_name, quality_threshold=80) %}
    {{ return(adapter.dispatch('dataprof_quality_test', 'dataprof')(model_name, quality_threshold)) }}
{% endmacro %}

{% macro default__dataprof_quality_test(model_name, quality_threshold) %}
    -- This would call a Python script that uses DataProf
    select case
        when quality_score >= {{ quality_threshold }} then 1
        else 0
    end as quality_check
    from (
        select {{ dataprof_quality_score(model_name) }} as quality_score
    )
{% endmacro %}
```

### dbt Post-Hook Integration

```python
# scripts/dataprof_post_hook.py
import sys
import dataprof

def run_dataprof_analysis(model_path):
    """Post-hook script for dbt models."""

    try:
        # Analyze the generated model
        report = dataprof.analyze_csv_with_quality(model_path)
        ml_score = dataprof.ml_readiness_score(model_path)

        # Log results
        print(f"dbt model analysis: {model_path}")
        print(f"Quality: {report.quality_score():.1f}%")
        print(f"ML Readiness: {ml_score.overall_score:.1f}%")

        # Write results to metadata
        with open(f"{model_path}.dataprof.json", "w") as f:
            import json
            json.dump({
                'quality_score': report.quality_score(),
                'ml_readiness': ml_score.overall_score,
                'issues_count': len(report.issues)
            }, f)

    except Exception as e:
        print(f"DataProf analysis failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_dataprof_analysis(sys.argv[1])
```

## 📈 Performance Benchmarks

### Comparative Analysis

```python
import time
import pandas as pd

def benchmark_dataprof_vs_alternatives(file_path):
    """Compare DataProf performance with alternative tools."""

    results = {}

    # DataProf timing
    start = time.time()
    dataprof_profiles = dataprof.analyze_csv_file(file_path)
    dataprof_time = time.time() - start
    results['dataprof'] = dataprof_time

    # Pandas profiling (if available)
    try:
        import pandas_profiling
        df = pd.read_csv(file_path)
        start = time.time()
        pandas_profile = pandas_profiling.ProfileReport(df, minimal=True)
        pandas_time = time.time() - start
        results['pandas_profiling'] = pandas_time
    except ImportError:
        results['pandas_profiling'] = None

    # Basic pandas analysis
    start = time.time()
    df = pd.read_csv(file_path)
    basic_info = df.describe()
    basic_nulls = df.isnull().sum()
    basic_time = time.time() - start
    results['pandas_basic'] = basic_time

    return results

# Usage
benchmark_results = benchmark_dataprof_vs_alternatives("large_dataset.csv")
print("Performance comparison:")
for tool, time_taken in benchmark_results.items():
    if time_taken:
        print(f"{tool}: {time_taken:.2f}s")
```

### Memory Usage Analysis

```python
import psutil
import os

def analyze_memory_usage(file_path):
    """Analyze memory usage during DataProf analysis."""

    process = psutil.Process(os.getpid())

    # Baseline memory
    baseline_memory = process.memory_info().rss / 1024 / 1024  # MB

    # During analysis
    profiles = dataprof.analyze_csv_file(file_path)
    peak_memory = process.memory_info().rss / 1024 / 1024  # MB

    # After analysis
    del profiles
    final_memory = process.memory_info().rss / 1024 / 1024  # MB

    return {
        'baseline_mb': baseline_memory,
        'peak_mb': peak_memory,
        'final_mb': final_memory,
        'peak_usage_mb': peak_memory - baseline_memory
    }
```

## 🔗 API Integration Examples

### REST API with FastAPI

```python
from fastapi import FastAPI, UploadFile, HTTPException
from typing import Dict, Any
import dataprof
import tempfile
import os

app = FastAPI(title="DataProf API")

@app.post("/analyze/quality")
async def analyze_quality(file: UploadFile) -> Dict[str, Any]:
    """Endpoint for data quality analysis."""

    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files supported")

    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
        content = await file.read()
        tmp_file.write(content)
        tmp_file_path = tmp_file.name

    try:
        # Analyze with DataProf
        report = dataprof.analyze_csv_with_quality(tmp_file_path)

        result = {
            'filename': file.filename,
            'quality_score': report.quality_score(),
            'total_columns': report.total_columns,
            'total_rows': report.total_rows,
            'issues_count': len(report.issues),
            'scan_time_ms': report.scan_time_ms
        }

        return result

    finally:
        # Cleanup
        os.unlink(tmp_file_path)

@app.post("/analyze/ml-readiness")
async def analyze_ml_readiness(file: UploadFile) -> Dict[str, Any]:
    """Endpoint for ML readiness analysis."""

    # Similar implementation for ML analysis
    pass
```

## 📚 Related Documentation

- [API Reference](API_REFERENCE.md) - Complete function reference
- [ML Features](ML_FEATURES.md) - ML readiness assessment guide
- [Python Overview](README.md) - Getting started guide
