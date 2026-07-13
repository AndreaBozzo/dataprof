/// User-supplied semantic hints that affect profiling and quality metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SemanticHints {
    pub positive_columns: Vec<String>,
    pub identifier_columns: Vec<String>,
    pub temporal_columns: Vec<String>,
}

impl SemanticHints {
    pub fn new(positive_columns: Vec<String>, identifier_columns: Vec<String>) -> Self {
        Self {
            positive_columns,
            identifier_columns,
            temporal_columns: Vec::new(),
        }
    }

    pub fn with_temporal_columns(mut self, temporal_columns: Vec<String>) -> Self {
        self.temporal_columns = temporal_columns;
        self
    }

    pub fn is_empty(&self) -> bool {
        self.positive_columns.is_empty()
            && self.identifier_columns.is_empty()
            && self.temporal_columns.is_empty()
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

    pub fn is_temporal_column(&self, column_name: &str) -> bool {
        self.temporal_columns
            .iter()
            .any(|candidate| candidate == column_name)
    }
}
