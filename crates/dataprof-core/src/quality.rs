/// ISO 25012 quality dimensions that can be selectively requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QualityDimension {
    Completeness,
    Consistency,
    Uniqueness,
    Accuracy,
    Timeliness,
    Validity,
    Precision,
}

impl QualityDimension {
    /// All currently implemented dimensions.
    pub fn all() -> Vec<Self> {
        vec![
            Self::Completeness,
            Self::Consistency,
            Self::Uniqueness,
            Self::Accuracy,
            Self::Timeliness,
            Self::Validity,
            Self::Precision,
        ]
    }
}

impl std::str::FromStr for QualityDimension {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "completeness" => Ok(Self::Completeness),
            "consistency" => Ok(Self::Consistency),
            "uniqueness" => Ok(Self::Uniqueness),
            "accuracy" => Ok(Self::Accuracy),
            "timeliness" => Ok(Self::Timeliness),
            "validity" => Ok(Self::Validity),
            "precision" => Ok(Self::Precision),
            _ => Err(format!("Unknown quality dimension: {s}")),
        }
    }
}

impl std::fmt::Display for QualityDimension {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completeness => write!(f, "completeness"),
            Self::Consistency => write!(f, "consistency"),
            Self::Uniqueness => write!(f, "uniqueness"),
            Self::Accuracy => write!(f, "accuracy"),
            Self::Timeliness => write!(f, "timeliness"),
            Self::Validity => write!(f, "validity"),
            Self::Precision => write!(f, "precision"),
        }
    }
}

/// High-level categories of analysis that can be selectively enabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricPack {
    /// Column names, data types, null counts — always included.
    Schema,
    /// Numeric stats (min/max/mean/median/std_dev/quartiles), text lengths.
    Statistics,
    /// Regex pattern detection (email, phone, UUID, etc.).
    Patterns,
    /// ISO 25012 quality dimensions (completeness, consistency, etc.).
    Quality,
}

impl MetricPack {
    /// All available metric packs.
    pub fn all() -> Vec<Self> {
        vec![
            Self::Schema,
            Self::Statistics,
            Self::Patterns,
            Self::Quality,
        ]
    }

    /// Whether statistics should be computed given the selected packs.
    pub fn include_statistics(packs: Option<&[Self]>) -> bool {
        match packs {
            None => true,
            Some(p) => p.contains(&Self::Statistics),
        }
    }

    /// Whether pattern detection should run given the selected packs.
    pub fn include_patterns(packs: Option<&[Self]>) -> bool {
        match packs {
            None => true,
            Some(p) => p.contains(&Self::Patterns),
        }
    }

    /// Whether quality metrics should be computed given the selected packs.
    pub fn include_quality(packs: Option<&[Self]>) -> bool {
        match packs {
            None => true,
            Some(p) => p.contains(&Self::Quality),
        }
    }

    /// Resolve the packs to compute, folding in an explicit quality-dimension
    /// selection.
    ///
    /// Requesting an empty set of dimensions (`Some(&[])`) says "assess no
    /// dimension", which is the same statement as not selecting the quality
    /// pack: nothing is analyzed, so the report carries no quality. Without
    /// this, an empty selection produces a quality object whose dimensions are
    /// all absent — an artifact that reads like a measured result.
    ///
    /// This is deliberately about what was *requested*. A caller that asks for
    /// dimensions and gets none back (an empty dataset, no date column) was
    /// still analyzed, and keeps a quality object reporting exactly that.
    pub fn resolve_with_dimensions(
        packs: Option<&[Self]>,
        dimensions: Option<&[QualityDimension]>,
    ) -> Option<Vec<Self>> {
        if !dimensions.is_some_and(<[QualityDimension]>::is_empty) {
            return packs.map(<[Self]>::to_vec);
        }
        // `None` packs means "all packs"; make that explicit before removing
        // Quality, so the result still selects everything else.
        let selected = packs.map_or_else(Self::all, <[Self]>::to_vec);
        Some(
            selected
                .into_iter()
                .filter(|pack| *pack != Self::Quality)
                .collect(),
        )
    }
}

impl std::str::FromStr for MetricPack {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "schema" => Ok(Self::Schema),
            "statistics" => Ok(Self::Statistics),
            "patterns" => Ok(Self::Patterns),
            "quality" => Ok(Self::Quality),
            _ => Err(format!(
                "Unknown metric pack: {s}. Valid packs: schema, statistics, patterns, quality"
            )),
        }
    }
}

impl std::fmt::Display for MetricPack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Schema => write!(f, "schema"),
            Self::Statistics => write!(f, "statistics"),
            Self::Patterns => write!(f, "patterns"),
            Self::Quality => write!(f, "quality"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_pack_include_helpers_none_means_all() {
        assert!(MetricPack::include_statistics(None));
        assert!(MetricPack::include_patterns(None));
        assert!(MetricPack::include_quality(None));
    }

    #[test]
    fn test_resolve_with_dimensions_empty_selection_drops_quality() {
        // Requesting no dimension is the same statement as not asking for
        // quality at all, so the pack must fall out and the report carry no
        // quality object.
        let resolved = MetricPack::resolve_with_dimensions(None, Some(&[])).unwrap();
        assert!(!resolved.contains(&MetricPack::Quality));
        assert!(!MetricPack::include_quality(Some(&resolved)));
        // The other packs survive: only quality was deselected.
        assert!(resolved.contains(&MetricPack::Schema));
        assert!(resolved.contains(&MetricPack::Statistics));
        assert!(resolved.contains(&MetricPack::Patterns));
    }

    #[test]
    fn test_resolve_with_dimensions_empty_selection_keeps_explicit_packs() {
        let packs = vec![MetricPack::Schema, MetricPack::Quality];
        let resolved = MetricPack::resolve_with_dimensions(Some(&packs), Some(&[])).unwrap();
        assert_eq!(resolved, vec![MetricPack::Schema]);
    }

    #[test]
    fn test_resolve_with_dimensions_non_empty_selection_is_untouched() {
        // A real selection says which dimensions to assess, not whether to.
        let dims = vec![QualityDimension::Completeness];
        assert_eq!(MetricPack::resolve_with_dimensions(None, Some(&dims)), None);
        assert!(MetricPack::include_quality(None));

        let packs = vec![MetricPack::Quality];
        let resolved = MetricPack::resolve_with_dimensions(Some(&packs), Some(&dims)).unwrap();
        assert!(MetricPack::include_quality(Some(&resolved)));
    }

    #[test]
    fn test_resolve_with_dimensions_absent_selection_is_untouched() {
        // `None` means "all dimensions", which is not the same as "none".
        assert_eq!(MetricPack::resolve_with_dimensions(None, None), None);
        let packs = vec![MetricPack::Quality];
        assert_eq!(
            MetricPack::resolve_with_dimensions(Some(&packs), None),
            Some(packs)
        );
    }

    #[test]
    fn test_metric_pack_include_helpers_selective() {
        let packs = vec![MetricPack::Schema, MetricPack::Quality];
        assert!(!MetricPack::include_statistics(Some(&packs)));
        assert!(!MetricPack::include_patterns(Some(&packs)));
        assert!(MetricPack::include_quality(Some(&packs)));
    }

    #[test]
    fn test_metric_pack_from_str() {
        assert_eq!(
            "statistics".parse::<MetricPack>().unwrap(),
            MetricPack::Statistics
        );
        assert_eq!(
            "QUALITY".parse::<MetricPack>().unwrap(),
            MetricPack::Quality
        );
        assert!("invalid".parse::<MetricPack>().is_err());
    }
}
