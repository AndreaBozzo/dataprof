# DataProf ML Features Guide

Complete guide to DataProf's machine learning readiness assessment and ecosystem integration features.

## 🤖 ML Readiness Assessment

DataProf provides comprehensive ML readiness assessment to help you prepare datasets for machine learning workflows.

### Quick Start

```python
import dataprof

# Get ML readiness score
ml_score = dataprof.ml_readiness_score("dataset.csv")

print(f"ML Readiness: {ml_score.readiness_level} ({ml_score.overall_score:.1f}%)")
print(f"Ready for ML: {ml_score.is_ml_ready()}")
```

### ML Readiness Score Components

The ML readiness score consists of four key components:

- **Completeness Score** (25%): Measures missing data impact
- **Consistency Score** (25%): Evaluates data format consistency
- **Type Suitability Score** (25%): Assesses data type appropriateness for ML
- **Feature Quality Score** (25%): Analyzes feature engineering potential

### ML Readiness Levels

- **🟢 ready** (80-100%): Dataset is ML-ready with minimal preprocessing
- **🟡 needs_work** (60-79%): Requires moderate preprocessing
- **🔴 poor** (0-59%): Significant work needed before ML use

## 🐍 Enhanced ML Recommendations with Code Snippets (v0.4.6+)

DataProf now generates **actionable code snippets** for each ML recommendation, making it easier to immediately implement preprocessing steps.

### New Features

- **Ready-to-use Python code** for each recommendation
- **Framework-specific implementations** (pandas, scikit-learn)
- **Required imports** automatically included
- **Context-aware code generation** based on your data
- **Complete preprocessing script generation**

### Code Snippet Example

```python
ml_score = dataprof.ml_readiness_score("data.csv")

for rec in ml_score.recommendations:
    print(f"📋 {rec.category}: {rec.description}")

    # NEW: Access code snippets
    if rec.code_snippet:
        print(f"📦 Framework: {rec.framework}")
        print(f"📥 Imports: {', '.join(rec.imports)}")
        print(f"💻 Code:\n{rec.code_snippet}")
```

### CLI Integration

Generate complete preprocessing scripts directly from the command line:

```bash
# Show code snippets in terminal
dataprof data.csv --ml-score --ml-code

# Generate complete preprocessing script
dataprof data.csv --ml-score --output-script preprocess.py
```

The generated script includes:
- All required imports
- Step-by-step preprocessing pipeline
- Progress indicators
- Error handling
- Data validation

### Supported Preprocessing Patterns

| Issue Type | Framework | Code Generated |
|------------|-----------|----------------|
| Missing Values | pandas | `df['col'].fillna(strategy)` |
| Categorical Encoding | pandas/sklearn | `pd.get_dummies()`, `LabelEncoder()` |
| Feature Scaling | sklearn | `StandardScaler()`, `MinMaxScaler()` |
| Date Engineering | pandas | Extract year, month, day, etc. |
| Outlier Handling | scipy/pandas | IQR capping, z-score filtering |
| Text Processing | sklearn | TF-IDF, tokenization |
| Mixed Types | pandas | Data type standardization |

### Enhanced PyMlRecommendation Properties

The `PyMlRecommendation` object now includes these **new properties**:

```python
class PyMlRecommendation:
    # Existing properties
    category: str              # "Missing Data", "Feature Encoding", etc.
    priority: str             # "critical", "high", "medium", "low"
    description: str          # Human-readable description
    expected_impact: str      # Expected ML improvement
    implementation_effort: str # "trivial", "easy", "moderate", etc.

    # NEW: Code snippet properties
    code_snippet: Optional[str]        # Ready-to-use Python code
    framework: Optional[str]           # "pandas", "scikit-learn", etc.
    imports: List[str]                # Required import statements
    variables: Dict[str, str]         # Variables used in code (column names, etc.)
```

**Example usage:**

```python
ml_score = dataprof.ml_readiness_score("data.csv")

for rec in ml_score.recommendations:
    if rec.code_snippet:
        # Display recommendation info
        print(f"🔧 {rec.category} [{rec.priority}]")
        print(f"📝 {rec.description}")
        print(f"🎯 Impact: {rec.expected_impact}")

        # Display code snippet info
        print(f"📦 Framework: {rec.framework}")
        print(f"📥 Required imports:")
        for imp in rec.imports:
            print(f"   {imp}")

        # Show variables used in the code
        if rec.variables:
            print(f"🔧 Variables:")
            for key, value in rec.variables.items():
                print(f"   {key}: {value}")

        # Display the actual code
        print(f"💻 Code:")
        print(rec.code_snippet.replace('\\n', '\n'))
```

## 🔄 Batch ML Processing (v0.4.61+)

DataProf now supports comprehensive batch processing for ML analysis across multiple files with enhanced dashboard and script generation capabilities.

### Batch ML Analysis

```bash
# Batch ML analysis across multiple files
dataprof /data/folder --ml-score --recursive

# Generate interactive HTML dashboard
dataprof /data/folder --quality --ml-score --html batch_dashboard.html --recursive

# Complete batch pipeline with script generation
dataprof /data/folder --quality --ml-score --ml-code --output-script batch_pipeline.py --recursive
```

### Key Batch Features

- **Multi-file ML Analysis**: Unified ML readiness assessment across file collections
- **Aggregated Recommendations**: Smart consolidation of common ML patterns and suggestions
- **Interactive HTML Dashboard**: Comprehensive batch analysis with per-file drill-down
- **Automated Script Generation**: Production-ready Python preprocessing scripts
- **Parallel Processing**: Configurable concurrency for improved performance

### Batch HTML Dashboard

The enhanced HTML dashboard provides:

- **Batch Summary**: Overall statistics, success rates, and processing performance
- **ML Readiness Overview**: Aggregated ML scores with distribution analysis
- **File Details Table**: Per-file quality and ML scores with interactive expansion
- **Aggregated Issues**: Common quality issues across all files
- **Performance Metrics**: Processing speed and resource utilization

### Batch Script Generation

Generated Python scripts include:

- **Aggregated Recommendations**: Consolidated preprocessing steps from all files
- **Parallel Processing Template**: ThreadPoolExecutor-based batch processing
- **Error Handling**: Robust error recovery and logging
- **Configuration Management**: Flexible batch processing parameters

```python
# Example of generated batch script structure
def process_file_batch(file_paths: List[str], output_dir: str):
    """
    Process multiple files with aggregated ML recommendations
    """
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for file_path in file_paths:
            future = executor.submit(preprocess_single_file, file_path, output_dir)
            futures.append(future)

        # Collect results with error handling
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                logger.info(f"Processed: {result}")
            except Exception as e:
                logger.error(f"Processing failed: {e}")
```

### Performance Considerations

- **Parallel Execution**: Use `--parallel` flag for multi-core processing
- **Memory Management**: Automatic resource optimization for large batch operations
- **Progress Tracking**: Real-time progress with `--progress` flag
- **Configurable Concurrency**: Tune performance with `--max-concurrent N`

## 📊 ML Functions

### `ml_readiness_score(file_path: str) -> PyMlReadinessScore`

Calculate comprehensive ML readiness assessment with actionable code snippets.

**Example:**
```python
ml_score = dataprof.ml_readiness_score("customer_data.csv")

# Overall assessment
print(f"Score: {ml_score.overall_score:.1f}%")
print(f"Level: {ml_score.readiness_level}")

# Component scores
print(f"Completeness: {ml_score.completeness_score:.1f}%")
print(f"Consistency: {ml_score.consistency_score:.1f}%")
print(f"Type Suitability: {ml_score.type_suitability_score:.1f}%")
print(f"Feature Quality: {ml_score.feature_quality_score:.1f}%")
```

### `analyze_csv_for_ml(file_path: str) -> Tuple[PyQualityReport, PyMlReadinessScore]`

Combined data quality and ML readiness analysis.

**Example:**
```python
quality_report, ml_score = dataprof.analyze_csv_for_ml("data.csv")

print(f"Data Quality: {quality_report.quality_score():.1f}%")
print(f"ML Readiness: {ml_score.overall_score:.1f}%")
print(f"Quality Issues: {len(quality_report.issues)}")
print(f"ML Blocking Issues: {len(ml_score.blocking_issues)}")
```

## 🔍 Feature Analysis

### Feature Types

DataProf categorizes features into ML-relevant types:

- **`numeric_ready`**: Numeric features ready for ML algorithms
- **`categorical_needs_encoding`**: Categorical data requiring encoding
- **`temporal_needs_engineering`**: Date/time features needing feature engineering
- **`high_cardinality_risky`**: High-cardinality features that may hurt performance
- **`text_needs_nlp`**: Text data requiring NLP preprocessing
- **`low_variance`**: Features with very low variance (potential removal candidates)

### Feature Suitability Scoring

Each feature gets an ML suitability score (0-1):

- **0.8-1.0**: Excellent for ML (ready to use)
- **0.6-0.79**: Good (minor preprocessing needed)
- **0.4-0.59**: Fair (moderate preprocessing required)
- **0.2-0.39**: Poor (significant work needed)
- **0.0-0.19**: Very poor (consider removal)

**Example:**
```python
# Get features by suitability
good_features = ml_score.features_by_suitability(0.7)
print(f"Features ready for ML: {len(good_features)}")

for feature in ml_score.feature_analysis:
    print(f"{feature.column_name}: {feature.ml_suitability:.2f} ({feature.feature_type})")
```

## 🚫 Blocking Issues

Blocking issues are critical problems that must be resolved before ML training:

**Common Blocking Issues:**
- **missing_target**: Target column is missing or empty
- **all_null_features**: Columns with 100% null values
- **duplicate_rows**: Excessive duplicate data
- **data_leakage**: Features that leak future information
- **constant_features**: Features with only one unique value

**Example:**
```python
if ml_score.blocking_issues:
    print("🚫 Critical issues found:")
    for issue in ml_score.blocking_issues:
        print(f"  • {issue.issue_type}: {issue.description}")
        print(f"    Affected: {', '.join(issue.affected_columns)}")
else:
    print("✅ No blocking issues!")
```

## 💡 ML Recommendations

DataProf provides actionable recommendations categorized by priority:

### Recommendation Categories

- **Feature Encoding**: Categorical variable encoding strategies
- **Feature Scaling**: Normalization and standardization suggestions
- **Feature Engineering**: Date/time and text feature extraction
- **Data Cleaning**: Missing value and outlier handling
- **Feature Selection**: Variance and correlation-based selection

### Recommendation Priorities

- **🔴 critical**: Must be addressed (affects model training)
- **🟡 high**: Strongly recommended (significant impact)
- **🟢 medium**: Good to have (moderate impact)
- **⚪ low**: Optional (minor improvements)

**Example:**
```python
# Get high-priority recommendations
high_priority = ml_score.recommendations_by_priority("high")

for rec in high_priority:
    print(f"📋 {rec.category} ({rec.priority})")
    print(f"   {rec.description}")
    print(f"   Impact: {rec.expected_impact}")
    print(f"   Effort: {rec.implementation_effort}")
```

## 🔧 Preprocessing Guidance

### Preprocessing Suggestions

DataProf provides specific preprocessing suggestions for each feature:

**Example:**
```python
# Get preprocessing suggestions by priority
critical_steps = ml_score.preprocessing_by_priority("critical")

for step in critical_steps:
    print(f"🔧 {step.step}")
    print(f"   Description: {step.description}")
    print(f"   Columns: {', '.join(step.columns_affected)}")
    print(f"   Tools: {', '.join(step.tools_frameworks)}")
```

### Preprocessing Pipeline Generation

Generate scikit-learn compatible preprocessing recommendations:

```python
# Categorize features for sklearn pipeline
for feature in ml_score.feature_analysis:
    if feature.feature_type == 'numeric_ready':
        # Add to numeric pipeline (scaling)
        numeric_features.append(feature.column_name)
    elif feature.feature_type == 'categorical_needs_encoding':
        # Add to categorical pipeline (encoding)
        categorical_features.append(feature.column_name)
    elif feature.ml_suitability < 0.3:
        # Consider dropping low-quality features
        drop_features.append(feature.column_name)
```

## 🐼 Pandas Integration

### DataFrame Output

Get ML analysis results as pandas DataFrames for easy manipulation:

```python
import pandas as pd

# Get feature analysis as DataFrame
features_df = dataprof.feature_analysis_dataframe("data.csv")

# Analyze feature quality distribution
print(features_df['feature_type'].value_counts())
print(features_df['importance_potential'].value_counts())

# Filter by ML suitability
ready_features = features_df[features_df['ml_suitability'] > 0.7]
needs_work = features_df[(features_df['ml_suitability'] >= 0.4) &
                        (features_df['ml_suitability'] <= 0.7)]
```

## 🔬 Scikit-learn Integration

### Automated Pipeline Building

Use DataProf analysis to build scikit-learn pipelines:

```python
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

# Get DataProf analysis
features_df = dataprof.feature_analysis_dataframe("data.csv")
ml_score = dataprof.ml_readiness_score("data.csv")

# Categorize features based on DataProf analysis
numeric_features = features_df[
    features_df['feature_type'] == 'numeric_ready'
]['column_name'].tolist()

categorical_features = features_df[
    features_df['feature_type'] == 'categorical_needs_encoding'
]['column_name'].tolist()

# Build preprocessing pipeline
preprocessor = ColumnTransformer(
    transformers=[
        ('num', Pipeline([
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ]), numeric_features),
        ('cat', Pipeline([
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', OneHotEncoder(drop='first'))
        ]), categorical_features)
    ]
)
```

### Feature Engineering Recommendations

DataProf suggests specific feature engineering techniques:

```python
for feature in ml_score.feature_analysis:
    if feature.feature_type == 'temporal_needs_engineering':
        print(f"📅 {feature.column_name}: Extract date components")
        print(f"   Suggestions: {', '.join(feature.encoding_suggestions)}")
    elif feature.feature_type == 'text_needs_nlp':
        print(f"📝 {feature.column_name}: Apply NLP preprocessing")
        print(f"   Suggestions: {', '.join(feature.encoding_suggestions)}")
```

## 📱 Jupyter Notebook Support

### Rich Display

DataProf ML objects support rich HTML display in Jupyter notebooks:

```python
# In Jupyter notebook
ml_score = dataprof.ml_readiness_score("data.csv")
ml_score  # Displays rich HTML visualization
```

The rich display includes:
- Overall readiness score with color coding
- Component score breakdown
- Feature analysis summary
- Top recommendations
- Blocking issues (if any)

### Visualization Integration

Combine with matplotlib/seaborn for custom visualizations:

```python
import matplotlib.pyplot as plt
import seaborn as sns

# Get feature analysis
features_df = dataprof.feature_analysis_dataframe("data.csv")

# Plot ML suitability distribution
plt.figure(figsize=(10, 6))
sns.histplot(data=features_df, x='ml_suitability', bins=20)
plt.title('ML Suitability Distribution')
plt.xlabel('ML Suitability Score')
plt.ylabel('Feature Count')
plt.show()

# Plot feature types
plt.figure(figsize=(12, 6))
feature_counts = features_df['feature_type'].value_counts()
sns.barplot(x=feature_counts.index, y=feature_counts.values)
plt.title('Feature Type Distribution')
plt.xticks(rotation=45)
plt.show()
```

## 🔄 ML Workflow Examples

### Complete ML Preparation Workflow

```python
def prepare_ml_dataset(file_path, target_column):
    """Complete ML dataset preparation using DataProf."""

    # Step 1: Check ML readiness
    ml_score = dataprof.ml_readiness_score(file_path)

    if not ml_score.is_ml_ready():
        print(f"⚠️ Dataset needs work (score: {ml_score.overall_score:.1f}%)")

        # Show blocking issues
        if ml_score.blocking_issues:
            print("🚫 Blocking issues:")
            for issue in ml_score.blocking_issues:
                print(f"  • {issue.description}")
            return None

    # Step 2: Get feature categorization
    features_df = dataprof.feature_analysis_dataframe(file_path)

    ready_features = features_df[features_df['ml_suitability'] > 0.7]
    print(f"✅ {len(ready_features)} features ready for ML")

    # Step 3: Build preprocessing pipeline
    # (pipeline building code here)

    # Step 4: Apply preprocessing
    # (data transformation code here)

    return processed_data, feature_names

# Usage
data, features = prepare_ml_dataset("customer_data.csv", "target")
```

### Model Performance Prediction

Use DataProf scores to predict model performance:

```python
def predict_model_performance(ml_score):
    """Predict expected model performance based on data quality."""

    if ml_score.overall_score >= 90:
        return "Excellent performance expected"
    elif ml_score.overall_score >= 75:
        return "Good performance expected"
    elif ml_score.overall_score >= 60:
        return "Fair performance, consider more preprocessing"
    else:
        return "Poor performance expected, significant preprocessing needed"

performance_prediction = predict_model_performance(ml_score)
print(f"📈 {performance_prediction}")
```

## 🎯 Best Practices

### 1. Always Check Blocking Issues First
```python
ml_score = dataprof.ml_readiness_score("data.csv")
if ml_score.blocking_issues:
    # Fix blocking issues before proceeding
    for issue in ml_score.blocking_issues:
        print(f"Fix: {issue.description}")
```

### 2. Use Feature Suitability for Feature Selection
```python
# Keep only high-quality features
good_features = [f.column_name for f in ml_score.feature_analysis
                if f.ml_suitability > 0.6]
```

### 3. Prioritize Recommendations by Impact
```python
# Focus on high-impact recommendations first
high_impact = ml_score.recommendations_by_priority("high")
```

### 4. Validate After Preprocessing
```python
# Re-check ML readiness after preprocessing
preprocessed_score = dataprof.ml_readiness_score("preprocessed_data.csv")
print(f"Improvement: {preprocessed_score.overall_score - original_score.overall_score:.1f} points")
```

## 📚 Related Documentation

- [API Reference](API_REFERENCE.md) - Complete function reference
- [Python Overview](../PYTHON.md) - General Python bindings guide
- [Integrations Guide](INTEGRATIONS.md) - Ecosystem integrations
