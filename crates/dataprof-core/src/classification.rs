/// Inferred column data type.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum DataType {
    /// Text or string values.
    String,
    /// Identifier values that should be treated as semantic strings.
    Identifier,
    /// Whole numbers in the i64 range.
    Integer,
    /// Floating-point numbers.
    Float,
    /// Date or datetime values.
    Date,
    /// Boolean values.
    Boolean,
}

/// Mutually exclusive lexical form of a single non-null value.
///
/// The variants partition non-null values — every value belongs to exactly one —
/// which is what lets the share held by the largest class describe a column
/// whose inferred [`DataType`] says nothing about the mixture inside it. A
/// `String` column of names and a `String` column that is 60% numbers are the
/// same type; they are not the same class distribution.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum LexicalClass {
    /// Whole numbers and fractions alike: `["1.5", "2"]` is one numeric column,
    /// not a two-class mixture.
    Numeric,
    /// Any date or datetime form the profiler recognizes.
    Date,
    /// Strict boolean tokens.
    Boolean,
    /// Everything else.
    Text,
}

impl std::fmt::Display for LexicalClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Numeric => write!(f, "numeric"),
            Self::Date => write!(f, "date"),
            Self::Boolean => write!(f, "boolean"),
            Self::Text => write!(f, "text"),
        }
    }
}

/// How a column's non-null values distribute across [`LexicalClass`]es.
///
/// Raw counts rather than a share, so that a reader can state the mixture it
/// actually found ("60% numeric, 40% text") instead of the dominant class and
/// an unnamed remainder, and so a serialized report carries no rounded
/// derivative of a number it does not also carry exactly.
///
/// All four counts are always present. Every field zero means the column was
/// classified and had no non-null values to classify — "analyzed, found
/// nothing", which is not the same as an absent `type_homogeneity`.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct TypeHomogeneity {
    pub numeric: usize,
    pub date: usize,
    pub boolean: usize,
    pub text: usize,
}

impl TypeHomogeneity {
    /// Counts in declaration order, which is also the order ties resolve in.
    fn counts(&self) -> [(LexicalClass, usize); 4] {
        [
            (LexicalClass::Numeric, self.numeric),
            (LexicalClass::Date, self.date),
            (LexicalClass::Boolean, self.boolean),
            (LexicalClass::Text, self.text),
        ]
    }

    /// Add one value of `class`.
    pub fn record(&mut self, class: LexicalClass) {
        match class {
            LexicalClass::Numeric => self.numeric += 1,
            LexicalClass::Date => self.date += 1,
            LexicalClass::Boolean => self.boolean += 1,
            LexicalClass::Text => self.text += 1,
        }
    }

    /// Non-null values classified — the denominator of every share below.
    pub fn classified_count(&self) -> usize {
        self.numeric + self.date + self.boolean + self.text
    }

    /// The class holding the most values and its count, or `None` when nothing
    /// was classified.
    ///
    /// A tie is held by the earliest-declared class. Which class wins does not
    /// change the reported share — both hold the same count — it only keeps the
    /// answer stable across runs.
    pub fn dominant(&self) -> Option<(LexicalClass, usize)> {
        self.counts()
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .fold(None, |best, candidate| match best {
                Some((_, best_count)) if best_count >= candidate.1 => best,
                _ => Some(candidate),
            })
    }

    /// Share of classified values held by [`Self::dominant`], in `0.0..=1.0`.
    /// `None` when nothing was classified, where the ratio is undefined.
    pub fn dominant_share(&self) -> Option<f64> {
        let (_, count) = self.dominant()?;
        Some(count as f64 / self.classified_count() as f64)
    }

    /// Every class that holds at least one value, largest first, with its share
    /// of the classified values. Empty when nothing was classified.
    ///
    /// Ties keep declaration order, so the sequence is stable across runs.
    pub fn mixture(&self) -> Vec<(LexicalClass, usize, f64)> {
        let total = self.classified_count();
        if total == 0 {
            return Vec::new();
        }
        let mut present: Vec<(LexicalClass, usize)> = self
            .counts()
            .into_iter()
            .filter(|(_, count)| *count > 0)
            .collect();
        // Stable sort on the descending count keeps declaration order for ties.
        present.sort_by_key(|(_, count)| std::cmp::Reverse(*count));
        present
            .into_iter()
            .map(|(class, count)| (class, count, count as f64 / total as f64))
            .collect()
    }
}

/// Semantic category for a detected pattern.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
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

#[cfg(test)]
mod type_homogeneity_tests {
    use super::*;

    fn homogeneity(numeric: usize, date: usize, boolean: usize, text: usize) -> TypeHomogeneity {
        TypeHomogeneity {
            numeric,
            date,
            boolean,
            text,
        }
    }

    #[test]
    fn recording_values_counts_them_by_class() {
        let mut counts = TypeHomogeneity::default();
        counts.record(LexicalClass::Numeric);
        counts.record(LexicalClass::Text);
        counts.record(LexicalClass::Numeric);

        assert_eq!(counts, homogeneity(2, 0, 0, 1));
        assert_eq!(counts.classified_count(), 3);
        assert_eq!(counts.dominant(), Some((LexicalClass::Numeric, 2)));
    }

    #[test]
    fn nothing_classified_has_no_dominant_class_and_no_share() {
        // The all-zero value is "classified, nothing to classify" — every
        // derived answer is absent rather than a number invented from a zero
        // denominator.
        let empty = TypeHomogeneity::default();

        assert_eq!(empty.classified_count(), 0);
        assert_eq!(empty.dominant(), None);
        assert_eq!(empty.dominant_share(), None);
        assert!(empty.mixture().is_empty());
    }

    #[test]
    fn a_tie_is_held_by_the_earliest_declared_class() {
        // Which class wins a tie does not change the share — both hold the same
        // count — but it must be the same class on every run, and the same one
        // the consistency dimension scores against.
        let tied = homogeneity(50, 50, 0, 0);

        assert_eq!(tied.dominant(), Some((LexicalClass::Numeric, 50)));
        assert_eq!(tied.dominant_share(), Some(0.5));
        assert_eq!(
            tied.mixture(),
            vec![
                (LexicalClass::Numeric, 50, 0.5),
                (LexicalClass::Date, 50, 0.5)
            ]
        );
    }

    #[test]
    fn mixture_reports_every_present_class_largest_first() {
        let mixed = homogeneity(60, 10, 0, 30);

        assert_eq!(mixed.dominant_share(), Some(0.6));
        assert_eq!(
            mixed.mixture(),
            vec![
                (LexicalClass::Numeric, 60, 0.6),
                (LexicalClass::Text, 30, 0.3),
                (LexicalClass::Date, 10, 0.1),
            ],
            "an absent class must not appear with a 0% share"
        );
    }

    #[test]
    fn counts_survive_a_json_round_trip() {
        let counts = homogeneity(600, 0, 0, 400);
        let json = serde_json::to_string(&counts).expect("counts should serialize");

        assert_eq!(json, r#"{"numeric":600,"date":0,"boolean":0,"text":400}"#);
        assert_eq!(
            serde_json::from_str::<TypeHomogeneity>(&json).expect("counts should deserialize"),
            counts
        );
    }

    #[test]
    fn lexical_classes_serialize_as_the_names_reports_use() {
        for (class, name) in [
            (LexicalClass::Numeric, "numeric"),
            (LexicalClass::Date, "date"),
            (LexicalClass::Boolean, "boolean"),
            (LexicalClass::Text, "text"),
        ] {
            assert_eq!(class.to_string(), name);
            assert_eq!(
                serde_json::to_string(&class).expect("class should serialize"),
                format!("\"{name}\"")
            );
        }
    }
}
