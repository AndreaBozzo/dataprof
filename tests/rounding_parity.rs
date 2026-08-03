//! The rounding convention, asserted against the shared fixture (#513).
//!
//! Rust and Python each serialize report floats through their own code, and
//! they used to disagree in three ways: different precisions for six fields,
//! quality dimension floats rounded on one side only, and different
//! tie-breaking. `tests/fixtures/rounding_parity.json` states the expected
//! output for one shared set of inputs; this file asserts the Rust serializers
//! against it and `python/tests/test_rounding_parity.py` asserts the Python
//! helpers against the same file, so changing either implementation alone
//! fails that layer's test.
//!
//! The inputs are weighted towards count-derived percentages (`k / n * 100`),
//! because those are what produce exact decimal ties in a real report; random
//! floats essentially never do.

use std::fs;
use std::path::PathBuf;

use dataprof_core::serde_helpers::{round_2, round_2_opt, round_4, round_4_opt};
use serde_json::Value;
use serde_json::value::Serializer;

const CONVENTION: &str = "round the stored f64 at n decimal places, ties away from zero";

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("rounding_parity.json");
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_json::from_str(&text).expect("fixture is valid JSON")
}

fn cases(fixture: &Value) -> &Vec<Value> {
    fixture["cases"].as_array().expect("fixture has cases")
}

fn number(value: &Value, key: &str) -> f64 {
    value[key]
        .as_f64()
        .unwrap_or_else(|| panic!("fixture case field {key} is not a number: {value}"))
}

/// Round through the real serializer and read the number it emitted.
fn emitted(rounded: Value) -> f64 {
    rounded
        .as_f64()
        .unwrap_or_else(|| panic!("serializer emitted a non-number: {rounded}"))
}

#[test]
fn rust_serializers_match_the_shared_rounding_fixture() {
    let fixture = fixture();
    assert_eq!(
        fixture["convention"].as_str().unwrap_or_default(),
        CONVENTION,
        "the fixture states a convention this test was not written against"
    );

    let mut failures = Vec::new();
    for case in cases(&fixture) {
        let value = number(case, "value");
        let checks = [
            (
                "round_2",
                number(case, "r2"),
                emitted(round_2(&value, Serializer).unwrap()),
            ),
            (
                "round_4",
                number(case, "r4"),
                emitted(round_4(&value, Serializer).unwrap()),
            ),
            // The Option variants duplicate the arithmetic, so they are pinned
            // too rather than assumed to agree with their non-Option twins.
            (
                "round_2_opt",
                number(case, "r2"),
                emitted(round_2_opt(&Some(value), Serializer).unwrap()),
            ),
            (
                "round_4_opt",
                number(case, "r4"),
                emitted(round_4_opt(&Some(value), Serializer).unwrap()),
            ),
        ];
        for (name, expected, actual) in checks {
            if actual != expected {
                failures.push(format!(
                    "{name}({value:?}): expected {expected:?}, serializer produced {actual:?}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} disagreements across {} fixture cases:\n{}",
        failures.len(),
        cases(&fixture).len(),
        failures.join("\n")
    );
}

#[test]
fn fixture_covers_the_ties_that_discriminate_the_rule() {
    // A fixture of only well-behaved values would pass under either
    // tie-breaking rule and so would guard nothing. Ties are the whole point,
    // so assert the fixture still holds them: values whose shortest decimal
    // representation is an exact tie at the second decimal, where rounding the
    // printed string and rounding the stored f64 can disagree.
    let fixture = fixture();
    let ties = cases(&fixture)
        .iter()
        .filter(|case| {
            let printed = format!("{}", number(case, "value"));
            printed
                .split_once('.')
                .is_some_and(|(_, frac)| frac.len() == 3 && frac.ends_with('5'))
        })
        .count();
    assert!(
        ties >= 10,
        "fixture holds only {ties} exact-tie cases; it no longer discriminates \
         the tie-breaking rule it exists to pin"
    );
}
