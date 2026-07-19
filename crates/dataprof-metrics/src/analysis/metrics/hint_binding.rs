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

use dataprof_core::{SemanticHintBinding, SemanticHintKind, SemanticHints};

use super::utils::extract_year;
use crate::analysis::inference::is_null_like_token;

/// Compute binding evidence for the value-driven hints (`positive_columns` and
/// `temporal_columns`) over `data`.
///
/// `exact` states whether `data` covers every row (`true`) or is a sample
/// (`false`); it is copied onto each binding so a proven-inert hint can be told
/// apart from one that merely matched nothing in a sample. Columns named by a
/// hint but absent from `data` are skipped — unknown names are the name
/// validator's job.
pub fn compute_value_hint_bindings(
    data: &HashMap<String, Vec<String>>,
    hints: &SemanticHints,
    exact: bool,
) -> Vec<SemanticHintBinding> {
    let mut bindings = Vec::new();

    for column in &hints.positive_columns {
        if let Some(values) = data.get(column) {
            // Mirror accuracy.rs: a value counts if it parses as f64.
            let (checked, matched) = count_matches(values, |v| v.parse::<f64>().is_ok());
            bindings.push(binding(
                column,
                SemanticHintKind::Positive,
                checked,
                matched,
                exact,
            ));
        }
    }

    for column in &hints.temporal_columns {
        if let Some(values) = data.get(column) {
            // Mirror timeliness.rs: a value counts as a date if a year parses.
            let (checked, matched) = count_matches(values, |v| extract_year(v).is_some());
            bindings.push(binding(
                column,
                SemanticHintKind::Temporal,
                checked,
                matched,
                exact,
            ));
        }
    }

    bindings
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

/// Count `(non-null values, values satisfying `pred`)`.
///
/// Mirrors the quality calculators exactly: the null-like check runs on the
/// trimmed token, but `pred` sees the *original* value. `accuracy.rs` and
/// `timeliness.rs` parse the raw string (`v.parse::<f64>()`,
/// `extract_year(value)`), and neither `parse::<f64>()` nor `extract_year`
/// tolerates surrounding whitespace, so trimming here would let this evidence
/// accept values the calculators reject (e.g. `" 1"`, `" 2020-01-01"`).
fn count_matches(values: &[String], pred: impl Fn(&str) -> bool) -> (usize, usize) {
    let mut checked = 0;
    let mut matched = 0;
    for value in values {
        if is_null_like_token(value.trim()) {
            continue;
        }
        checked += 1;
        if pred(value) {
            matched += 1;
        }
    }
    (checked, matched)
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
    fn whitespace_padded_values_match_the_calculators_not_a_trimmed_view() {
        // The calculators parse the raw string, and neither `parse::<f64>()` nor
        // `extract_year` accepts surrounding whitespace. Binding evidence must
        // agree, so a padded value is counted as considered but not matched.
        let d = data(&[
            ("pressure", &[" 101325", "100900"]),
            ("event", &[" 2020-01-01", "2022-06-15"]),
        ]);
        let hints = SemanticHints::new(vec!["pressure".to_string()], vec![])
            .with_temporal_columns(vec!["event".to_string()]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);

        let positive = bindings
            .iter()
            .find(|b| b.column == "pressure")
            .expect("positive binding");
        assert_eq!(positive.checked_values, 2);
        assert_eq!(positive.matched_values, 1);

        let temporal = bindings
            .iter()
            .find(|b| b.column == "event")
            .expect("temporal binding");
        assert_eq!(temporal.checked_values, 2);
        assert_eq!(temporal.matched_values, 1);
    }

    #[test]
    fn all_null_column_has_no_checked_values_and_is_not_inert() {
        let d = data(&[("pressure", &["", "NULL", "  "])]);
        let hints = SemanticHints::new(vec!["pressure".to_string()], vec![]);
        let bindings = compute_value_hint_bindings(&d, &hints, true);
        assert_eq!(bindings[0].checked_values, 0);
        assert!(!bindings[0].is_proven_inert());
    }
}
