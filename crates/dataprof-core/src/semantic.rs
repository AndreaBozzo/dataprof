use serde::{Deserialize, Serialize};

use crate::errors::DataProfilerError;

/// The kind of a semantic hint, used for diagnostics and binding evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SemanticHintKind {
    /// A [`SemanticHints::positive_columns`] entry.
    Positive,
    /// An [`SemanticHints::identifier_columns`] entry.
    Identifier,
    /// A [`SemanticHints::temporal_columns`] entry.
    Temporal,
}

impl SemanticHintKind {
    /// The public configuration field name that carries this hint kind.
    pub fn field_name(self) -> &'static str {
        match self {
            Self::Positive => "positive_columns",
            Self::Identifier => "identifier_columns",
            Self::Temporal => "temporal_columns",
        }
    }
}

/// Per-column evidence of whether a semantic hint actually bound to any value.
///
/// A hint expresses user domain knowledge about a column; this records how much
/// of the column that knowledge matched. `matched_values` counts the values of
/// the kind the hint expects (numeric for `positive`, date-parseable for
/// `temporal`, every non-null value for `identifier`, which coerces the column).
///
/// `exact` distinguishes a full-data count from a sampled one: a hint that
/// matched nothing (`matched_values == 0`) is only *proven* inert when the count
/// covers every row. A sampled zero is recorded but never treated as an error,
/// because absence in a sample is not proof of absence in the data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticHintBinding {
    /// The hinted column this evidence is for.
    pub column: String,
    /// Which hint list named the column.
    pub kind: SemanticHintKind,
    /// Non-null values considered.
    pub checked_values: usize,
    /// Values that matched the hint's expected kind.
    pub matched_values: usize,
    /// Whether the counts cover every row (`true`) or a sample (`false`).
    pub exact: bool,
}

impl SemanticHintBinding {
    /// Whether this hint is *proven* inert: it was assessed over the full data
    /// (`exact`), had values to consider (`checked_values > 0`), yet matched
    /// none of them.
    pub fn is_proven_inert(&self) -> bool {
        self.exact && self.checked_values > 0 && self.matched_values == 0
    }
}

/// User-supplied semantic hints that affect profiling and quality metrics.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
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

    /// Every `(kind, column_name)` pair across all three hint lists.
    pub fn iter(&self) -> impl Iterator<Item = (SemanticHintKind, &str)> {
        let positive = self
            .positive_columns
            .iter()
            .map(|c| (SemanticHintKind::Positive, c.as_str()));
        let identifier = self
            .identifier_columns
            .iter()
            .map(|c| (SemanticHintKind::Identifier, c.as_str()));
        let temporal = self
            .temporal_columns
            .iter()
            .map(|c| (SemanticHintKind::Temporal, c.as_str()));
        positive.chain(identifier).chain(temporal)
    }

    /// Fail loudly when a hint names a column that is not in the schema.
    ///
    /// A hint is the user's chosen alternative to overconfident inference, so a
    /// hint that cannot bind must not vanish: a typo'd or stale column name is a
    /// mistake the profiler should surface, not silently discard.
    pub fn validate_names(&self, column_names: &[&str]) -> Result<(), DataProfilerError> {
        let mut unknown: Vec<(SemanticHintKind, &str)> = self
            .iter()
            .filter(|(_, name)| !column_names.contains(name))
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        // Stable, readable ordering regardless of hint-list order.
        unknown.sort_unstable_by(|a, b| a.1.cmp(b.1).then(a.0.field_name().cmp(b.0.field_name())));

        let details = unknown
            .iter()
            .map(|(kind, name)| format!("'{}' ({})", name, kind.field_name()))
            .collect::<Vec<_>>()
            .join(", ");
        let mut available: Vec<&str> = column_names.to_vec();
        available.sort_unstable();
        let available = available
            .iter()
            .map(|c| format!("'{c}'"))
            .collect::<Vec<_>>()
            .join(", ");

        Err(DataProfilerError::InvalidSemanticHint {
            message: format!("semantic hint names not found in the data: {details}"),
            suggestion: format!(
                "Available columns: [{available}]. Check spelling and case, or remove the hint."
            ),
        })
    }

    /// Reject value-driven hints when the Quality metric pack did not run.
    ///
    /// Positive and temporal hints only affect quality calculators. Accepting
    /// them without a quality assessment would make the configuration look
    /// effective while producing neither metrics nor binding evidence.
    pub fn validate_quality_usage(&self, quality_computed: bool) -> Result<(), DataProfilerError> {
        if quality_computed
            || (self.positive_columns.is_empty() && self.temporal_columns.is_empty())
        {
            return Ok(());
        }

        Err(DataProfilerError::InvalidSemanticHint {
            message: "positive_columns and temporal_columns require the Quality metric pack"
                .to_string(),
            suggestion: "Include MetricPack::Quality (\"quality\" in Python), or remove the value-driven hint. Identifier hints remain usable without quality because they affect column typing."
                .to_string(),
        })
    }

    /// Fail loudly when a hint names a real column but bound to nothing.
    ///
    /// Only bindings that were assessed over the full data
    /// ([`SemanticHintBinding::is_proven_inert`]) raise; sampled evidence is
    /// carried in the report but never treated as proof of absence.
    pub fn validate_bindings(
        &self,
        bindings: &[SemanticHintBinding],
    ) -> Result<(), DataProfilerError> {
        let inert: Vec<&SemanticHintBinding> =
            bindings.iter().filter(|b| b.is_proven_inert()).collect();
        if inert.is_empty() {
            return Ok(());
        }

        let details = inert
            .iter()
            .map(|b| {
                format!(
                    "'{}' ({}): 0 of {} value(s) matched",
                    b.column,
                    b.kind.field_name(),
                    b.checked_values
                )
            })
            .collect::<Vec<_>>()
            .join("; ");

        Err(DataProfilerError::InvalidSemanticHint {
            message: format!("semantic hint(s) bound to no matching value: {details}"),
            suggestion: "positive_columns need numeric values; temporal_columns need date values. \
                 Fix the column choice or remove the hint."
                .to_string(),
        })
    }
}
