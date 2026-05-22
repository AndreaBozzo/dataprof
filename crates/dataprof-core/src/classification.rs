/// Inferred column data type.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DataType {
    /// Text or string values.
    String,
    /// Whole numbers in the i64 range.
    Integer,
    /// Floating-point numbers.
    Float,
    /// Date or datetime values.
    Date,
    /// Boolean values.
    Boolean,
}

/// Semantic category for a detected pattern.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PatternCategory {
    /// Email addresses, phone numbers.
    Contact,
    /// UUIDs, fiscal codes, tax IDs.
    Identifier,
    /// IPv4, IPv6, MAC addresses, URLs.
    Network,
    /// Coordinates and postal codes.
    Geographic,
    /// IBANs, credit cards, SWIFT/BIC.
    Financial,
    /// Unix or Windows file paths.
    FilePath,
    /// Uncategorized patterns.
    Other,
}

impl std::fmt::Display for PatternCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contact => write!(f, "contact"),
            Self::Identifier => write!(f, "identifier"),
            Self::Network => write!(f, "network"),
            Self::Geographic => write!(f, "geographic"),
            Self::Financial => write!(f, "financial"),
            Self::FilePath => write!(f, "file_path"),
            Self::Other => write!(f, "other"),
        }
    }
}
