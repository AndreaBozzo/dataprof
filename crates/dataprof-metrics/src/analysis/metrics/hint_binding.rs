//! Per-column evidence of whether value-driven semantic hints bound to any data.
//!
//! The `positive` and `temporal` hints are consumed by the accuracy and
//! timeliness calculators, which parse raw cell values regardless of a column's
//! inferred type. Binding evidence therefore has to be measured the same way:
//! by scanning the values with the exact predicates those calculators use.
//!
//! Identifier binding is structural (the hint coerces the column's type), so it
//! is derived from the column profiles by the report assembler, not here.

use std::collections::HashMap;

use super::accuracy::rule_value;
use super::utils::extract_year;
use crate::analysis::inference::LexicalTally;
use dataprof_core::{SemanticHintBinding, SemanticHintKind, SemanticHints};

/// Compute binding evidence for the value-driven hints (`positive_columns`,
/// `temporal_columns` and the columns of `temporal_pairs`) from per-column
/// [`LexicalTally`] counters accumulated over every scanned value (#788 step 3).
///
/// This is the single implementation behind every profiling route: CSV, JSON,
/// JSONL, async streams, Parquet, Arrow, DataFrames, Python containers and
/// databases all feed [`LexicalTally`] and derive their binding evidence here.
///
/// `exact` states whether the tallies cover every row of the source (`true`)
/// or only a row-sampled / truncated subset (`false`). Columns named by a hint
/// but absent from `tallies` are skipped — unknown names are the name
/// validator's job.
pub fn compute_value_hint_bindings_from_tallies(
    tallies: &HashMap<String, LexicalTally>,
    hints: &SemanticHints,
    exact: bool,
) -> Vec<SemanticHintBinding> {
    let mut bindings = Vec::new();

    for (kind, column) in hints
        .iter()
        .filter(|(kind, _)| *kind != SemanticHintKind::Identifier)
    {
        if let Some(tally) = tallies.get(column) {
            let checked = tally.values_checked();
            let matched = match kind {
                SemanticHintKind::Positive => tally.rule_numeric_count(),
                SemanticHintKind::Temporal | SemanticHintKind::TemporalPair => {
                    tally.temporal_match_count()
                }
                SemanticHintKind::Identifier => checked,
            };
            bindings.push(binding(column, kind, checked, matched, exact));
        }
    }

    bindings
}

/// Compute binding evidence for the value-driven hints (`positive_columns`,
/// `temporal_columns` and the columns of `temporal_pairs`) over `data`.
///
/// Delegates to [`compute_value_hint_bindings_from_tallies`] over per-column
/// [`LexicalTally`] counts so callers without precomputed tallies apply the
/// exact same predicates as every streaming route.
pub fn compute_value_hint_bindings(
    data: &HashMap<String, Vec<String>>,
    hints: &SemanticHints,
    exact: bool,
) -> Vec<SemanticHintBinding> {
    let mut tallies = HashMap::new();
    for (_, column) in hints
        .iter()
        .filter(|(kind, _)| *kind != SemanticHintKind::Identifier)
    {
        if !tallies.contains_key(column)
            && let Some(values) = data.get(column)
        {
            tallies.insert(column.to_string(), LexicalTally::from_values(values));
        }
    }
    compute_value_hint_bindings_from_tallies(&tallies, hints, exact)
}

fn binding(
    column: &str,
    kind: SemanticHintKind,
    checked_values: usize,
    matched_values: usize,
    exact: bool,
) -> SemanticHintBinding {
    SemanticHintBinding {
        column: column.to_string(),
        kind,
        checked_values,
        matched_values,
        exact,
    }
}

/// Apply the exact value predicate used by the quality calculator for `kind`.
///
/// Mirrors the quality calculators exactly: both numeric rules and temporal
/// predicates accept surrounding whitespace, matching type inference and
/// `DateTimeAccumulator` (#788 step 4).
pub fn value_matches_hint(value: &str, kind: SemanticHintKind) -> bool {
    match kind {
        // The sign rule's own reading of a cell, surrounding whitespace
        // accepted.
        SemanticHintKind::Positive => rule_value(value.trim()).is_some(),
        // Mirror timeliness and DateTimeAccumulator: parse through the shared
        // date reader.
        SemanticHintKind::Temporal | SemanticHintKind::TemporalPair => {
            extract_year(value).is_some()
        }
        // Identifier binding is structural and is not value-driven.
        SemanticHintKind::Identifier => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(name, vals)| {
                (
                    name.to_string(),
                    vals.iter().map(|v| v.to_string()).collect(),
                )
            })
            .collect()
    }

    #[test]
    fn positive_hint_on_numeric_column_binds() {
        let d = data(&[("pressure", &["101325", "-500", "100900"])]);
        let hints = SemanticHints::new(vec!["pressure".to_string()], vec![]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0].kind, SemanticHintKind::Positive);
        assert_eq!(bindings[0].checked_values, 3);
        assert_eq!(bindings[0].matched_values, 3);
        assert!(!bindings[0].is_proven_inert());
    }

    #[test]
    fn positive_hint_on_text_column_is_proven_inert_when_exact() {
        let d = data(&[("name", &["Alice", "Bob"])]);
        let hints = SemanticHints::new(vec!["name".to_string()], vec![]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);
        assert_eq!(bindings[0].matched_values, 0);
        assert!(bindings[0].is_proven_inert());
    }

    #[test]
    fn sampled_zero_match_is_not_proven_inert() {
        let d = data(&[("name", &["Alice", "Bob"])]);
        let hints = SemanticHints::new(vec!["name".to_string()], vec![]);
        let bindings = compute_value_hint_bindings(&d, &hints, false);
        assert_eq!(bindings[0].matched_values, 0);
        assert!(!bindings[0].is_proven_inert());
    }

    #[test]
    fn temporal_hint_binds_on_parseable_values_in_mixed_column() {
        // The supported mixed-column case: some values are dates, some are not.
        let d = data(&[("event", &["2020-01-01", "not-a-date"])]);
        let hints = SemanticHints::default().with_temporal_columns(vec!["event".to_string()]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);
        assert_eq!(bindings[0].kind, SemanticHintKind::Temporal);
        assert_eq!(bindings[0].checked_values, 2);
        assert_eq!(bindings[0].matched_values, 1);
        assert!(!bindings[0].is_proven_inert());
    }

    #[test]
    fn temporal_hint_on_non_date_column_is_proven_inert() {
        let d = data(&[("name", &["Alice", "Bob"])]);
        let hints = SemanticHints::default().with_temporal_columns(vec!["name".to_string()]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);
        assert!(bindings[0].is_proven_inert());
    }

    #[test]
    fn whitespace_padded_values_match_each_calculators_normalization() {
        // Both numeric rules and temporal predicates trim surrounding
        // whitespace, matching type inference and `DateTimeAccumulator`
        // (#788 step 4).
        let d = data(&[
            ("pressure", &[" 101325", "100900", " -inf "]),
            ("event", &[" 2020-01-01", "2022-06-15"]),
        ]);
        let hints = SemanticHints::new(vec!["pressure".to_string()], vec![])
            .with_temporal_columns(vec!["event".to_string()]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);

        let positive = bindings
            .iter()
            .find(|b| b.column == "pressure")
            .expect("positive binding");
        assert_eq!(positive.checked_values, 3);
        assert_eq!(positive.matched_values, 2);

        let temporal = bindings
            .iter()
            .find(|b| b.column == "event")
            .expect("temporal binding");
        assert_eq!(temporal.checked_values, 2);
        assert_eq!(temporal.matched_values, 2);
    }

    /// A literal past the `f64` range is a value the sign rule checks
    /// (#1181), so the hint binds on it.
    #[test]
    fn positive_hint_binds_on_the_values_the_sign_rule_checks() {
        // `NaN` is a null token and is not checked; `inf` holds no number.
        let d = data(&[("balance", &["-1e400", "1e-400", "inf", "NaN"])]);
        let hints = SemanticHints::new(vec!["balance".to_string()], vec![]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);
        assert_eq!(
            (bindings[0].checked_values, bindings[0].matched_values),
            (3, 2)
        );
    }

    #[test]
    fn all_null_column_has_no_checked_values_and_is_not_inert() {
        let d = data(&[("pressure", &["", "NULL", "  "])]);
        let hints = SemanticHints::new(vec!["pressure".to_string()], vec![]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);
        assert_eq!(bindings[0].checked_values, 0);
        assert!(!bindings[0].is_proven_inert());
    }

    #[test]
    fn tally_bindings_match_direct_value_scan() {
        let d = data(&[
            ("pressure", &[" 101325", "-500", "-1e400", " -inf ", "NULL"]),
            (
                "event",
                &["2020-01-01", " 2021-01-01", "not-a-date", "2024-02-29"],
            ),
            ("ended", &["2022-05-01", "bad", ""]),
        ]);
        let tallies: HashMap<String, LexicalTally> = d
            .iter()
            .map(|(name, vals)| (name.clone(), LexicalTally::from_values(vals)))
            .collect();
        let hints = SemanticHints::new(
            vec!["pressure".to_string(), "missing".to_string()],
            vec!["pressure".to_string()],
        )
        .with_temporal_columns(vec!["event".to_string()])
        .with_temporal_pairs(vec![("event".to_string(), "ended".to_string())]);

        for exact in [true, false] {
            assert_eq!(
                compute_value_hint_bindings_from_tallies(&tallies, &hints, exact),
                compute_value_hint_bindings(&d, &hints, exact),
                "exact={exact}"
            );
        }
    }
}
