//! Duplicate hints must not weight metrics or duplicate binding evidence (#1004).
//! `python/tests/test_semantic_hint_duplicates.py` is the Python twin.

use std::io::Write;

use dataprof::{EngineType, Profiler, SemanticHintKind, SemanticHints};
use tempfile::NamedTempFile;

const CSV: &str = "z_old,a_new,value,code\n2001-01-01,2026-10-01,1,A\n2002-01-01,2026-10-02,-2,B\n2003-01-01,2026-10-03,3,C\n2026-10-01,2099-01-01,4,D\n";

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|name| (*name).to_string()).collect()
}

#[test]
fn public_hint_lists_iterate_once_per_kind_in_first_seen_order() {
    let mut hints = SemanticHints::default();
    hints.positive_columns = names(&["z", "a", "z", "a"]);
    hints.identifier_columns = names(&["z", "z"]);
    hints.temporal_columns = names(&["a", "z", "a"]);
    hints.temporal_pairs = vec![("z".into(), "a".into()), ("z".into(), "a".into())];
    assert_eq!(
        hints.iter().collect::<Vec<_>>(),
        vec![
            (SemanticHintKind::Positive, "z"),
            (SemanticHintKind::Positive, "a"),
            (SemanticHintKind::Identifier, "z"),
            (SemanticHintKind::Temporal, "a"),
            (SemanticHintKind::Temporal, "z"),
            (SemanticHintKind::TemporalPair, "z"),
            (SemanticHintKind::TemporalPair, "a"),
        ]
    );
    assert!(SemanticHints::default().iter().next().is_none());
}

#[test]
fn repeated_hints_preserve_scores_and_bindings_on_every_csv_engine() {
    let mut csv = NamedTempFile::with_suffix(".csv").unwrap();
    write!(csv, "{CSV}").unwrap();
    csv.flush().unwrap();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let profile = |temporal: &[&str], copies: usize| {
            Profiler::new()
                .engine(engine)
                .temporal_columns(names(temporal))
                .positive_columns(vec!["value".to_string(); copies])
                .identifier_columns(vec!["code".to_string(); copies])
                .analyze_file(csv.path())
                .unwrap()
        };
        let base = profile(&["z_old", "a_new"], 1);
        for temporal in [
            vec!["z_old", "z_old", "a_new"],
            vec!["z_old", "a_new", "a_new"],
            vec!["z_old", "z_old", "a_new", "z_old", "a_new", "a_new"],
        ] {
            let repeated = profile(&temporal, 3);
            assert_eq!(
                serde_json::to_value(&repeated.quality).unwrap(),
                serde_json::to_value(&base.quality).unwrap(),
                "{engine:?}: {temporal:?}"
            );
            assert_eq!(repeated.semantic_hint_bindings, base.semantic_hint_bindings);
            let temporal: Vec<_> = repeated
                .semantic_hint_bindings
                .iter()
                .filter(|binding| binding.kind == SemanticHintKind::Temporal)
                .map(|binding| binding.column.as_str())
                .collect();
            assert_eq!(temporal, ["z_old", "a_new"]);
        }
    }
}

#[test]
fn repeated_unknown_temporal_names_still_fail_validation() {
    let mut csv = NamedTempFile::with_suffix(".csv").unwrap();
    write!(csv, "{CSV}").unwrap();
    let error = Profiler::new()
        .temporal_columns(names(&["missing", "missing"]))
        .analyze_file(csv.path())
        .unwrap_err()
        .to_string();
    assert!(error.contains("missing"), "{error}");
    assert!(error.contains("temporal_columns"), "{error}");
}
