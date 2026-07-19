//! Exact, bounded-memory semantic-hint binding counts for streaming engines.

use dataprof_core::{SemanticHintBinding, SemanticHintKind, SemanticHints};
use dataprof_metrics::{is_null_like_token, value_matches_hint};

#[derive(Debug, Clone)]
struct BindingCounter {
    binding: SemanticHintBinding,
}

/// Accumulates value-driven hint evidence while an engine scans the full data.
///
/// The number of configured hints bounds memory use; no cell values are kept.
#[derive(Debug, Clone, Default)]
pub struct ValueHintBindingAccumulator {
    counters: Vec<BindingCounter>,
}

impl ValueHintBindingAccumulator {
    pub fn new(hints: &SemanticHints) -> Self {
        let positive = hints.positive_columns.iter().map(|column| BindingCounter {
            binding: SemanticHintBinding {
                column: column.clone(),
                kind: SemanticHintKind::Positive,
                checked_values: 0,
                matched_values: 0,
                exact: true,
            },
        });
        let temporal = hints.temporal_columns.iter().map(|column| BindingCounter {
            binding: SemanticHintBinding {
                column: column.clone(),
                kind: SemanticHintKind::Temporal,
                checked_values: 0,
                matched_values: 0,
                exact: true,
            },
        });
        Self {
            counters: positive.chain(temporal).collect(),
        }
    }

    /// Observe one cell using the same null and match predicates as the quality
    /// calculators and their sample-based binding evidence.
    pub fn observe(&mut self, column: &str, value: &str) {
        if is_null_like_token(value.trim()) {
            return;
        }
        for counter in self
            .counters
            .iter_mut()
            .filter(|counter| counter.binding.column == column)
        {
            counter.binding.checked_values += 1;
            if value_matches_hint(value, counter.binding.kind) {
                counter.binding.matched_values += 1;
            }
        }
    }

    /// Return exact evidence for hints whose columns exist in the source.
    /// Unknown names remain the schema validator's responsibility.
    pub fn bindings<'a>(
        &self,
        column_names: impl IntoIterator<Item = &'a str>,
    ) -> Vec<SemanticHintBinding> {
        let names: std::collections::HashSet<&str> = column_names.into_iter().collect();
        self.counters
            .iter()
            .filter(|counter| names.contains(counter.binding.column.as_str()))
            .map(|counter| counter.binding.clone())
            .collect()
    }

    pub fn merge(&mut self, other: &Self) {
        for other_counter in &other.counters {
            if let Some(counter) = self.counters.iter_mut().find(|candidate| {
                candidate.binding.column == other_counter.binding.column
                    && candidate.binding.kind == other_counter.binding.kind
            }) {
                counter.binding.checked_values += other_counter.binding.checked_values;
                counter.binding.matched_values += other_counter.binding.matched_values;
            } else {
                self.counters.push(other_counter.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_exact_counts_with_calculator_semantics() {
        let hints = SemanticHints::new(vec!["amount".to_string()], vec![])
            .with_temporal_columns(vec!["event".to_string()]);
        let mut accumulator = ValueHintBindingAccumulator::new(&hints);
        for value in ["1", " 2", "NULL"] {
            accumulator.observe("amount", value);
        }
        for value in ["2020-01-01", "not-a-date", " 2021-01-01"] {
            accumulator.observe("event", value);
        }

        let bindings = accumulator.bindings(["amount", "event"]);
        assert_eq!(bindings[0].checked_values, 2);
        assert_eq!(bindings[0].matched_values, 1);
        assert!(bindings[0].exact);
        assert_eq!(bindings[1].checked_values, 3);
        assert_eq!(bindings[1].matched_values, 1);
        assert!(bindings[1].exact);
    }

    #[test]
    fn omits_unknown_columns() {
        let hints = SemanticHints::new(vec!["missing".to_string()], vec![]);
        let accumulator = ValueHintBindingAccumulator::new(&hints);
        assert!(accumulator.bindings(["present"]).is_empty());
    }
}
