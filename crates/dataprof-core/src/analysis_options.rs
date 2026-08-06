//! The analysis selection every parser and engine must honour.
//!
//! Metric packs, quality dimensions, locale, and semantic hints are all
//! *requests about what to compute*, and the report they produce has to be the
//! same on every input path. Passing them as loose parameters made that easy to
//! get wrong: a parser that took dimensions and hints but not packs or locale
//! compiled fine and silently computed work the caller had deselected.
//! [`AnalysisOptions`] bundles them so a path either carries the whole selection
//! or does not compile.

use crate::quality::{MetricPack, QualityDimension};
use crate::semantic::SemanticHints;

/// What to analyze, and how, for a single profiling run.
///
/// Construct with [`AnalysisOptions::default`] (analyze everything) and narrow
/// with the builder methods.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnalysisOptions {
    metric_packs: Option<Vec<MetricPack>>,
    quality_dimensions: Option<Vec<QualityDimension>>,
    locale: Option<String>,
    semantic_hints: SemanticHints,
}

impl AnalysisOptions {
    /// Select the metric packs to compute. `None` (the default) means all.
    pub fn with_metric_packs(mut self, packs: Option<Vec<MetricPack>>) -> Self {
        self.metric_packs = packs;
        self
    }

    /// Select the quality dimensions to assess. `None` (the default) means all.
    pub fn with_quality_dimensions(mut self, dimensions: Option<Vec<QualityDimension>>) -> Self {
        self.quality_dimensions = dimensions;
        self
    }

    /// Set the ISO 3166-1 alpha-2 locale used to rank detected patterns.
    pub fn with_locale(mut self, locale: Option<String>) -> Self {
        self.locale = locale;
        self
    }

    /// Set the user's semantic hints.
    pub fn with_semantic_hints(mut self, hints: SemanticHints) -> Self {
        self.semantic_hints = hints;
        self
    }

    /// The packs to compute, with an empty quality-dimension selection folded in.
    ///
    /// Resolved on read rather than in the setters so the outcome does not
    /// depend on the order the selections were made in.
    pub fn effective_metric_packs(&self) -> Option<Vec<MetricPack>> {
        MetricPack::resolve_with_dimensions(
            self.metric_packs.as_deref(),
            self.quality_dimensions.as_deref(),
        )
    }

    /// Whether per-column statistics should be computed.
    pub fn include_statistics(&self) -> bool {
        MetricPack::include_statistics(self.effective_metric_packs().as_deref())
    }

    /// Whether pattern detection should run.
    pub fn include_patterns(&self) -> bool {
        MetricPack::include_patterns(self.effective_metric_packs().as_deref())
    }

    /// Whether quality metrics should be computed.
    ///
    /// `false` means the report carries no quality at all — absent, not an
    /// empty assessment — because nothing was analyzed.
    pub fn include_quality(&self) -> bool {
        MetricPack::include_quality(self.effective_metric_packs().as_deref())
    }

    /// The locale used to rank detected patterns.
    ///
    /// A locale only ranks patterns, so it has no effect of its own when
    /// [`include_patterns`](Self::include_patterns) is false and detection never
    /// runs.
    pub fn locale(&self) -> Option<&str> {
        self.locale.as_deref()
    }

    /// The requested quality dimensions, if the caller narrowed them.
    pub fn quality_dimensions(&self) -> Option<&[QualityDimension]> {
        self.quality_dimensions.as_deref()
    }

    /// The user's semantic hints.
    pub fn semantic_hints(&self) -> &SemanticHints {
        &self.semantic_hints
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_analyzes_everything() {
        let options = AnalysisOptions::default();
        assert!(options.include_statistics());
        assert!(options.include_patterns());
        assert!(options.include_quality());
        assert_eq!(options.locale(), None);
        assert_eq!(options.quality_dimensions(), None);
    }

    #[test]
    fn schema_only_deselects_every_other_pack() {
        let options = AnalysisOptions::default().with_metric_packs(Some(vec![MetricPack::Schema]));
        assert!(!options.include_statistics());
        assert!(!options.include_patterns());
        assert!(!options.include_quality());
    }

    #[test]
    fn empty_dimension_selection_removes_the_quality_pack() {
        let options = AnalysisOptions::default().with_quality_dimensions(Some(vec![]));
        assert!(!options.include_quality());
        // Deselecting quality says nothing about the other packs.
        assert!(options.include_statistics());
        assert!(options.include_patterns());
    }

    #[test]
    fn resolution_does_not_depend_on_setter_order() {
        let packs = vec![MetricPack::Schema, MetricPack::Quality];
        let dims_first = AnalysisOptions::default()
            .with_quality_dimensions(Some(vec![]))
            .with_metric_packs(Some(packs.clone()));
        let packs_first = AnalysisOptions::default()
            .with_metric_packs(Some(packs))
            .with_quality_dimensions(Some(vec![]));
        assert_eq!(
            dims_first.effective_metric_packs(),
            packs_first.effective_metric_packs()
        );
        assert!(!dims_first.include_quality());
    }

    #[test]
    fn a_locale_alone_does_not_re_enable_pattern_detection() {
        let options = AnalysisOptions::default()
            .with_locale(Some("IT".to_string()))
            .with_metric_packs(Some(vec![MetricPack::Schema]));
        assert_eq!(options.locale(), Some("IT"));
        assert!(!options.include_patterns());
    }
}
