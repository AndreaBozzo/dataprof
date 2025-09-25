use crate::types::DataType;
use std::collections::HashMap;

/// Code generation engine for ML preprocessing tasks
#[derive(Debug, Clone)]
pub struct CodeGenerator {
    /// Default framework preference (pandas, scikit-learn, etc.)
    pub default_framework: String,
}

impl Default for CodeGenerator {
    fn default() -> Self {
        Self {
            default_framework: "pandas".to_string(),
        }
    }
}

/// Generated code snippet with metadata
#[derive(Debug, Clone)]
pub struct CodeSnippet {
    pub code: String,
    pub framework: String,
    pub imports: Vec<String>,
    pub variables: HashMap<String, String>,
}

impl CodeGenerator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_framework(framework: String) -> Self {
        Self {
            default_framework: framework,
        }
    }

    /// Generate code snippet for missing value handling
    pub fn generate_missing_value_code(
        &self,
        column_name: &str,
        data_type: &DataType,
        null_percentage: f64,
    ) -> CodeSnippet {
        let mut variables = HashMap::new();
        variables.insert("column_name".to_string(), column_name.to_string());
        variables.insert(
            "null_percentage".to_string(),
            format!("{:.1}", null_percentage),
        );

        match data_type {
            DataType::Integer | DataType::Float => {
                let strategy = if null_percentage > 30.0 { "median" } else { "mean" };
                variables.insert("strategy".to_string(), strategy.to_string());

                CodeSnippet {
                    code: format!("# Handle missing values in numeric column '{}'\\ndf['{}'].fillna(df['{}'].{}(), inplace=True)",
                                 column_name, column_name, column_name, strategy),
                    framework: "pandas".to_string(),
                    imports: vec!["import pandas as pd".to_string()],
                    variables,
                }
            }
            DataType::String => {
                CodeSnippet {
                    code: format!("# Handle missing values in categorical column '{}'\\ndf['{}'].fillna(df['{}'].mode()[0], inplace=True)",
                                 column_name, column_name, column_name),
                    framework: "pandas".to_string(),
                    imports: vec!["import pandas as pd".to_string()],
                    variables,
                }
            }
            DataType::Date => {
                CodeSnippet {
                    code: format!("# Handle missing values in date column '{}'\\n# Option 1: Forward fill\\ndf['{}'].fillna(method='ffill', inplace=True)\\n# Option 2: Use a default date\\n# df['{}'].fillna(pd.to_datetime('1900-01-01'), inplace=True)",
                                 column_name, column_name, column_name),
                    framework: "pandas".to_string(),
                    imports: vec!["import pandas as pd".to_string()],
                    variables,
                }
            }
        }
    }

    /// Generate code snippet for categorical encoding
    pub fn generate_categorical_encoding_code(
        &self,
        column_name: &str,
        unique_ratio: f64,
    ) -> CodeSnippet {
        let mut variables = HashMap::new();
        variables.insert("column_name".to_string(), column_name.to_string());
        variables.insert("unique_ratio".to_string(), format!("{:.3}", unique_ratio));

        if unique_ratio < 0.1 {
            // Low cardinality - one-hot encoding
            CodeSnippet {
                code: format!("# One-hot encode low cardinality categorical column '{}'\\ndf_encoded = pd.get_dummies(df, columns=['{}'], prefix='{}')\\n# Or using scikit-learn:\\n# from sklearn.preprocessing import OneHotEncoder\\n# encoder = OneHotEncoder(sparse_output=False)\\n# encoded = encoder.fit_transform(df[['{}']].values)",
                             column_name, column_name, column_name, column_name),
                framework: "pandas".to_string(),
                imports: vec!["import pandas as pd".to_string()],
                variables,
            }
        } else {
            // Medium/high cardinality - label encoding
            CodeSnippet {
                code: format!("# Label encode categorical column '{}'\\nfrom sklearn.preprocessing import LabelEncoder\\nlabel_encoder = LabelEncoder()\\ndf['{}_encoded'] = label_encoder.fit_transform(df['{}'])\\n# Keep original column or drop it:\\n# df.drop('{}', axis=1, inplace=True)",
                             column_name, column_name, column_name, column_name),
                framework: "scikit-learn".to_string(),
                imports: vec![
                    "import pandas as pd".to_string(),
                    "from sklearn.preprocessing import LabelEncoder".to_string()
                ],
                variables,
            }
        }
    }

    /// Generate code snippet for feature scaling
    pub fn generate_scaling_code(&self, column_names: &[String]) -> CodeSnippet {
        let mut variables = HashMap::new();
        let columns_str = format!("{:?}", column_names);
        variables.insert("columns".to_string(), columns_str.clone());

        CodeSnippet {
            code: format!("# Standardize numeric features\\nfrom sklearn.preprocessing import StandardScaler\\nscaler = StandardScaler()\\ncolumns_to_scale = {}\\ndf[columns_to_scale] = scaler.fit_transform(df[columns_to_scale])\\n\\n# Alternative: Min-Max scaling (0-1 range)\\n# from sklearn.preprocessing import MinMaxScaler\\n# scaler = MinMaxScaler()\\n# df[columns_to_scale] = scaler.fit_transform(df[columns_to_scale])",
                         columns_str),
            framework: "scikit-learn".to_string(),
            imports: vec![
                "import pandas as pd".to_string(),
                "from sklearn.preprocessing import StandardScaler".to_string()
            ],
            variables,
        }
    }

    /// Generate code snippet for date feature engineering
    pub fn generate_date_engineering_code(&self, column_name: &str) -> CodeSnippet {
        let mut variables = HashMap::new();
        variables.insert("column_name".to_string(), column_name.to_string());

        CodeSnippet {
            code: format!("# Extract date features from column '{}'\\n# Ensure column is datetime\\ndf['{}'] = pd.to_datetime(df['{}'])\\n\\n# Extract date components\\ndf['{}_year'] = df['{}'].dt.year\\ndf['{}_month'] = df['{}'].dt.month\\ndf['{}_day'] = df['{}'].dt.day\\ndf['{}_weekday'] = df['{}'].dt.dayofweek\\ndf['{}_quarter'] = df['{}'].dt.quarter\\n\\n# Calculate days since reference date\\nreference_date = pd.to_datetime('2020-01-01')\\ndf['{}_days_since_ref'] = (df['{}'] - reference_date).dt.days\\n\\n# Drop original date column if not needed\\n# df.drop('{}', axis=1, inplace=True)",
                         column_name, column_name, column_name,
                         column_name, column_name, column_name, column_name,
                         column_name, column_name, column_name, column_name,
                         column_name, column_name, column_name, column_name, column_name),
            framework: "pandas".to_string(),
            imports: vec!["import pandas as pd".to_string()],
            variables,
        }
    }

    /// Generate code snippet for outlier handling
    pub fn generate_outlier_handling_code(&self, column_name: &str) -> CodeSnippet {
        let mut variables = HashMap::new();
        variables.insert("column_name".to_string(), column_name.to_string());

        CodeSnippet {
            code: format!("# Handle outliers in column '{}'\\n# Method 1: IQR-based capping\\nQ1 = df['{}'].quantile(0.25)\\nQ3 = df['{}'].quantile(0.75)\\nIQR = Q3 - Q1\\nlower_bound = Q1 - 1.5 * IQR\\nupper_bound = Q3 + 1.5 * IQR\\n\\n# Cap outliers\\ndf['{}'] = df['{}'].clip(lower=lower_bound, upper=upper_bound)\\n\\n# Method 2: Z-score based removal (alternative)\\n# from scipy import stats\\n# z_scores = np.abs(stats.zscore(df['{}']))\\n# df = df[z_scores < 3]  # Keep rows with z-score < 3",
                         column_name, column_name, column_name, column_name, column_name, column_name),
            framework: "pandas".to_string(),
            imports: vec!["import pandas as pd".to_string(), "import numpy as np".to_string()],
            variables,
        }
    }

    /// Generate code snippet for mixed type cleaning
    pub fn generate_mixed_type_cleaning_code(&self, column_name: &str) -> CodeSnippet {
        let mut variables = HashMap::new();
        variables.insert("column_name".to_string(), column_name.to_string());

        CodeSnippet {
            code: format!("# Clean mixed data types in column '{}'\\n# Step 1: Examine the different types\\nprint(\"Data types in column '{}':\")\\nprint(df['{}'].apply(type).value_counts())\\n\\n# Step 2: Convert to string and clean\\ndf['{}'] = df['{}'].astype(str)\\n\\n# Step 3: Handle specific cases (customize as needed)\\n# Remove non-numeric characters if should be numeric\\n# df['{}'] = pd.to_numeric(df['{}'].str.replace(r'[^0-9.-]', '', regex=True), errors='coerce')\\n\\n# Step 4: Verify the result\\nprint(\"After cleaning:\")\\nprint(df['{}'].dtype)",
                         column_name, column_name, column_name, column_name, column_name, column_name, column_name, column_name),
            framework: "pandas".to_string(),
            imports: vec!["import pandas as pd".to_string()],
            variables,
        }
    }

    /// Generate code snippet for text preprocessing
    pub fn generate_text_preprocessing_code(&self, column_name: &str) -> CodeSnippet {
        let mut variables = HashMap::new();
        variables.insert("column_name".to_string(), column_name.to_string());

        CodeSnippet {
            code: format!("# Text preprocessing for column '{}'\\n# Basic text cleaning\\ndf['{}'] = df['{}'].str.lower()  # Convert to lowercase\\ndf['{}'] = df['{}'].str.replace(r'[^\\w\\s]', '', regex=True)  # Remove punctuation\\n\\n# Option 1: TF-IDF Vectorization\\nfrom sklearn.feature_extraction.text import TfidfVectorizer\\nvectorizer = TfidfVectorizer(max_features=1000, stop_words='english')\\ntfidf_features = vectorizer.fit_transform(df['{}'])\\ntfidf_df = pd.DataFrame(tfidf_features.toarray(), \\n                       columns=[f'{}_tfidf_{{i}}' for i in range(tfidf_features.shape[1])])\\ndf = pd.concat([df, tfidf_df], axis=1)\\n\\n# Option 2: Simple word count features (alternative)\\n# df['{}_word_count'] = df['{}'].str.split().str.len()\\n# df['{}_char_count'] = df['{}'].str.len()",
                         column_name, column_name, column_name, column_name, column_name, column_name, column_name, column_name, column_name, column_name, column_name),
            framework: "scikit-learn".to_string(),
            imports: vec![
                "import pandas as pd".to_string(),
                "from sklearn.feature_extraction.text import TfidfVectorizer".to_string()
            ],
            variables,
        }
    }
}
