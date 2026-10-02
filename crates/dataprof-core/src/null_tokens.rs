//! The text values every input path counts as null.
//!
//! A cell is null when it is empty or whitespace, `null` or `nan` in any case,
//! or one of [`NULL_MARKERS`] exactly: `NA`, `N/A`, `n/a`, `#N/A`, `\N` and
//! `None`. Typed nulls (an Arrow null, a SQL `NULL`, Python `None`, a float
//! NaN) are null before any text is involved.
//!
//! Through 0.12 only empty, `null` and `nan` were nulls, so the markers the
//! tools that write files use for a missing value were counted as values, and
//! completeness read 100% on columns that were mostly missing (#813): `NA` is
//! R's, `#N/A` Excel's, `\N` MySQL `SELECT INTO OUTFILE` and PostgreSQL
//! `COPY` text format's, `None` Python's `str(None)`, `N/A` and `n/a` those of
//! hand-maintained sheets.
//!
//! The markers match exactly, unlike `null` and `nan`. Each is a missing-value
//! marker only in the spelling its writer uses: `Na` is sodium and `none` an
//! answer, and a case-insensitive `na` would take both. The set leaves out
//! tokens that are data in one file and a marker in the next, such as `-`,
//! `?` and `n.d.`.

/// The null vocabulary a report was measured with, as recorded in its metric
/// semantics.
///
/// Only the current vocabulary has a value. Releases that knew only empty,
/// `null` and `nan` recorded none, and a report from one reads back with the
/// vocabulary unknown rather than as one it never declared.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum NullTokenSet {
    /// Empty, `null` and `nan` in any case, and [`NULL_MARKERS`] exactly, as
    /// matched by [`is_null_like_token`].
    CommonMarkers,
}

/// Missing-value markers counted as null in exactly this spelling.
pub const NULL_MARKERS: &[&str] = &["NA", "N/A", "n/a", "#N/A", "\\N", "None"];

/// Whether a text value is null under [`NullTokenSet::CommonMarkers`].
///
/// This is the single definition behind every `null_count`, so the CSV, JSON,
/// Parquet, Arrow, database and in-memory paths cannot drift apart on what
/// they count as missing. Surrounding whitespace is ignored.
pub fn is_null_like_token(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("null")
        || trimmed.eq_ignore_ascii_case("nan")
        || NULL_MARKERS.contains(&trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_marker_is_null_with_surrounding_whitespace() {
        for marker in NULL_MARKERS {
            assert!(is_null_like_token(marker), "{marker:?}");
            assert!(is_null_like_token(&format!(" {marker}\t")), "{marker:?}");
        }
    }

    #[test]
    fn null_and_nan_match_in_any_case() {
        for token in ["", "  ", "null", "NULL", "Null", "nan", "NaN", "NAN"] {
            assert!(is_null_like_token(token), "{token:?}");
        }
    }

    #[test]
    fn markers_match_only_in_their_writers_spelling() {
        // Sodium, an answer, and the lower-case spellings the markers' writers
        // never produce stay values.
        for value in [
            "Na", "na", "none", "NONE", "#n/a", "\\n", "N/a", "NA1", "N/A/",
        ] {
            assert!(!is_null_like_token(value), "{value:?}");
        }
    }

    #[test]
    fn ambiguous_placeholders_stay_values() {
        for value in ["-", "?", "n.d.", "nil", "(null)", "0"] {
            assert!(!is_null_like_token(value), "{value:?}");
        }
    }
}
