# Reading dataprof output honestly

dataprof emits deterministic signals. It does not tell you what to do about
them, and neither should you pretend it did. This file is what each signal
means and — more importantly — what it does not.

## Contents

- The absence rule
- Quality dimensions, one by one
- Approximation provenance
- Patterns and sensitive data
- Comparisons
- Things dataprof deliberately does not do

## The absence rule

**`None` means "not analyzed". Empty means "analyzed, found nothing."**

This is the single most important interpretation rule in the library, and it
holds for every metric, not just quality dimensions.

- A `None` quality score is not a perfect score. It is no score.
- Never substitute zero for `None` to make arithmetic work. A zero you invented
  is a number a user will act on.
- `assessed_dimensions` is the authority on what was computed. If a dimension is
  not in it, do not report a value for it.
- A metric pack you narrowed away with `metrics=[...]` produces absence, not
  failure. Say "not requested", not "not available".

## Quality dimensions, one by one

**Completeness** — presence of values. `missing_values_ratio` is per-value;
`complete_records_ratio` is per-row and drops sharply when any single optional
column is sparse. Check both before describing a dataset as incomplete.

**Consistency** — whether values in a column agree on type and format.
`data_type_consistency` and `format_violations` are the evidence.

**Uniqueness** — duplicate rows and key behavior. `key_uniqueness` and
`duplicate_rows` are the evidence. A high-cardinality column is not a key, and
`high_cardinality_warning` is not a defect.

**Accuracy** — range and sign violations against declared expectations.
`negative_values_in_positive` only means something if you passed
`positive_columns`; without that hint dataprof cannot know a column should be
positive.

**Timeliness** — `future_dates_count`, `stale_data_ratio`,
`temporal_violations`. Only meaningful for columns you named in
`temporal_columns`, or ones dataprof confidently detected as temporal.

**Validity** — assessed **only** for columns with a confidently detected
pattern. A column with no detected pattern is not invalid; it is unassessed.
Reporting "0% valid" for an unpatterned column is wrong.

**Precision** — consistency of effective decimal scale across values. It does
not know how many decimals your business requires, and a low score is not
evidence of rounding errors.

The overall score is a weighted combination (`score_weights`). Report what drove
it, not just the number.

## Approximation provenance

`is_approximate`, `unique_count_is_approximate`, and the structure report's
`provenance` field mark numbers that came from an estimator rather than an exact
count. This is deliberate honesty, not a warning to suppress. When a distinct
count is approximate, say "approximately", and do not compare it against an
exact count from another run as if the difference were drift.

## Patterns and sensitive data

`report["col"].patterns` gives detected pattern names, not values. When a
pattern is classified sensitive — email, phone, identifier, financial,
geographic, network, file path — `to_llm_context()` reports the pattern name and
counts and withholds the values, and `include_samples=True` does not override
that.

A detected pattern is a detection, not a guarantee. Locale matters: without a
`locale`, patterns from other locales can fire on ordinary identifiers. If a
pattern result looks surprising, check whether a locale should have been set
before reporting it as a finding.

## Comparisons

`before.compare(after)` returns deltas. Two caveats:

- Comparing a sampled profile to a full one measures your sampling, not drift.
  Check `sampling_applied` on both sides first.
- Comparing profiles with different `metrics` selections produces absence, not
  change. Narrow both sides the same way.

## Things dataprof deliberately does not do

It does not transform, clean, deduplicate, or move data. It does not recommend
fixes, generate explanations, or score a dataset as pass/fail. It makes no
network calls and sends no telemetry.

If a user wants a recommendation, that interpretation is yours to make and yours
to own — cite the specific metric that drove it, and say what dataprof measured
versus what you inferred.
