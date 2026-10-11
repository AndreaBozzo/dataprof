use crate::errors::DataProfilerError;
use std::collections::HashSet;

/// Reject repeated names in an ordered column-name list, before any per-column
/// state is built.
///
/// Every engine keys its per-column accumulators by name, so a repeated name is
/// otherwise silently merged (file/Arrow paths) or shadowed at mapping access
/// (columnar path), yielding a report where `total_count` can exceed the row
/// count or a column simply disappears. Rejecting up front is the only behavior
/// all engines can share.
///
/// `names` is the final list each engine uses as profile keys, so the check runs
/// on already-normalized names (the caller's stringification is the normalization
/// step). `source` is a short transport label — e.g. `"CSV header"`, `"CSV bytes"`,
/// `"Arrow/Parquet schema"` — surfaced to the user. Duplicates are reported in
/// first-seen order, deduped; no cell values are ever embedded.
pub fn validate_unique_column_names(
    names: &[String],
    source: &str,
) -> Result<(), DataProfilerError> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(names.len());
    let mut reported: HashSet<&str> = HashSet::new();
    let mut duplicates: Vec<String> = Vec::new();
    for name in names {
        if !seen.insert(name.as_str()) && reported.insert(name.as_str()) {
            duplicates.push(name.clone());
        }
    }
    if duplicates.is_empty() {
        Ok(())
    } else {
        Err(DataProfilerError::duplicate_column_name(
            &duplicates,
            source,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn unique_column_names_pass() {
        assert!(validate_unique_column_names(&names(&["x", "y", "z"]), "CSV header").is_ok());
        assert!(validate_unique_column_names(&[], "CSV header").is_ok());
    }

    #[test]
    fn duplicate_first_middle_last_rejected() {
        for cols in [
            names(&["x", "x", "y"]), // first
            names(&["a", "x", "x"]), // last
            names(&["a", "x", "b", "x", "c"]),
        ] {
            let err = validate_unique_column_names(&cols, "CSV header").unwrap_err();
            assert!(matches!(err, DataProfilerError::DuplicateColumnName { .. }));
        }
    }

    #[test]
    fn duplicate_error_names_offender_and_source_only() {
        let cols = names(&["id", "amount", "id", "amount", "note"]);
        let err = validate_unique_column_names(&cols, "CSV bytes").unwrap_err();
        let msg = err.to_string();
        // Each offender named once, source present, no cell values.
        assert!(msg.contains("'id'"));
        assert!(msg.contains("'amount'"));
        assert_eq!(msg.matches("'id'").count(), 1);
        assert!(msg.contains("CSV bytes"));
        assert!(msg.contains("Duplicate column names:")); // plural "s"
    }
}
