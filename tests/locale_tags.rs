//! Locale tags are a closed set (#545).
//!
//! `locale=` is strict: setting one suppresses every pattern belonging to a
//! different locale. Applied to a tag the catalogue does not know, that made a
//! typo produce a strictly worse report than passing nothing at all, silently —
//! `"it-IT"`, the BCP 47 spelling a user is most likely to reach for, returned
//! no patterns where `"IT"` returned a confident match.
//!
//! These tests hold the two halves of the fix at the public API: the common
//! spellings all mean the same locale, and anything left over is an error that
//! names the supported set.

use std::io::Write;

use dataprof::{Locale, ProfileReport, Profiler};
use tempfile::NamedTempFile;

/// Italian postal codes. A five-digit string matches both `CAP (IT)` and
/// `ZIP Code (US)`, so the column shows what a locale did: `IT` keeps CAP and
/// suppresses ZIP, and a tag that resolves to nothing keeps neither.
const CAPS: [&str; 5] = ["20121", "00184", "10121", "80132", "50122"];

fn csv_fixture() -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(file, "cap").unwrap();
    for cap in CAPS {
        writeln!(file, "{cap}").unwrap();
    }
    file.flush().unwrap();
    file
}

fn pattern_names(report: &ProfileReport) -> Vec<String> {
    let mut names: Vec<String> = report
        .column_profiles
        .iter()
        .find(|profile| profile.name == "cap")
        .expect("the fixture has a cap column")
        .patterns
        .as_ref()
        .expect("pattern detection runs by default")
        .iter()
        .map(|pattern| pattern.name.clone())
        .collect();
    names.sort();
    names
}

#[test]
fn every_spelling_of_a_locale_profiles_alike() {
    let file = csv_fixture();
    let reference = Profiler::new()
        .locale(Locale::It)
        .analyze_file(file.path())
        .expect("profiling with a locale should succeed");
    let expected = pattern_names(&reference);
    assert!(
        expected.iter().any(|name| name == "CAP (IT)"),
        "the fixture should detect CAP under the IT locale, got {expected:?}"
    );
    assert!(
        !expected.iter().any(|name| name == "ZIP Code (US)"),
        "the IT locale should suppress the US pattern, got {expected:?}"
    );

    for tag in ["IT", "it", "It", " it ", "ITA", "ita", "it-IT", "it_IT"] {
        let locale = Locale::parse_optional(Some(tag))
            .unwrap_or_else(|error| panic!("tag {tag:?} was rejected: {error}"))
            .unwrap_or_else(|| panic!("tag {tag:?} resolved to no locale"));
        let report = Profiler::new()
            .locale(locale)
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{tag}] profiling failed: {e}"));

        assert_eq!(
            pattern_names(&report),
            expected,
            "tag {tag:?} profiled differently from Locale::It"
        );
    }
}

#[test]
fn an_unrecognised_tag_is_rejected_naming_the_supported_set() {
    for tag in ["XX", "ZZZZ", "de-CH", "en", "italiano"] {
        let error = Locale::parse_optional(Some(tag))
            .expect_err(&format!("tag {tag:?} should not be accepted"));

        assert!(
            error.contains(tag),
            "the error should quote the rejected tag, got {error:?}"
        );
        for locale in Locale::all() {
            assert!(
                error.contains(locale.as_str()),
                "the error should name {locale}, got {error:?}"
            );
        }
    }
}

#[test]
fn a_blank_tag_means_no_locale_rather_than_an_unmatched_one() {
    let file = csv_fixture();
    let unset = Profiler::new()
        .analyze_file(file.path())
        .expect("profiling without a locale should succeed");
    let unset_patterns = pattern_names(&unset);

    // Both locale-specific candidates survive without a locale preference; the
    // old behaviour for a blank tag was to leave the column with none.
    assert!(
        unset_patterns.iter().any(|name| name == "CAP (IT)")
            && unset_patterns.iter().any(|name| name == "ZIP Code (US)"),
        "without a locale both candidates should remain as evidence, got {unset_patterns:?}"
    );

    for tag in [Some(""), Some("   "), None] {
        assert_eq!(
            Locale::parse_optional(tag),
            Ok(None),
            "tag {tag:?} should mean no locale"
        );
    }
}
