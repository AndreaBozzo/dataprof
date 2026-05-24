/// User-supplied semantic hints that affect profiling and quality metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SemanticHints {
    pub positive_columns: Vec<String>,
    pub identifier_columns: Vec<String>,
}

impl SemanticHints {
    pub fn new(positive_columns: Vec<String>, identifier_columns: Vec<String>) -> Self {
        Self {
            positive_columns,
            identifier_columns,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.positive_columns.is_empty() && self.identifier_columns.is_empty()
    }

    pub fn is_identifier_column(&self, column_name: &str) -> bool {
        self.identifier_columns
            .iter()
            .any(|candidate| candidate == column_name)
    }

    pub fn is_positive_column(&self, column_name: &str) -> bool {
        self.positive_columns
            .iter()
            .any(|candidate| candidate == column_name)
    }
}
