# Reading dataprof output honestly

dataprof emits deterministic signals. It does not tell you what to do about
them, and neither should you pretend it did. This file is what each signal
means and — more importantly — what it does not.

## Contents

- The absence rule
- Quality dimensions, one by one
- Type homogeneity
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

Each dimension is a dict of its own evidence — `report.quality.completeness`,
`report.quality.uniqueness`, and so on. The keys named below live inside it.

**Completeness** — presence of values. `missing_values_ratio` is per-value;
`complete_records_ratio` is per-row and treats every column as required, so it
drops sharply when any single optional column is sparse. Check both, and name
the columns in `null_columns`, before describing a dataset as incomplete.

**Consistency** — whether values in a column agree on type and format.
`data_type_consistency` and `format_violations` are the evidence.

Read it against the column's `data_type`, because that is what it is measured
against. A column with an inferred type is scored on conformance to that type. A
`string` column is scored differently, because every value conforms to `string`:
each value is assigned to one lexical class — numeric, date, boolean, or text —
and the score is the share held by the largest class.

| non-numeric share | inferred type | data_type_consistency | overall |
| --- | --- | --- | --- |
| 10% | float | 90.0 | 97.5 |
| 18% | float | 82.0 | 95.5 |
| 19% | float | 81.0 | 95.25 |
| 20% | string | 80.0 | 94.67 |
| 50% | string | 50.0 | 86.67 |
| 100% | string | 100.0 | 100.0 |

So a low consistency score on a `string` column means the column holds more than
one kind of value, and the score is roughly the share of the dominant kind. A
perfect score means one kind — which for a `string` column is ordinary text, not
a guarantee the text is useful.

The score is symmetric around a 50/50 mix: 20% junk and 80% junk both report 80,
because the minority is what costs the column its consistency in each case. A
column that is 80% junk is typed `string` because that is what it mostly is.

The dataset-level score cannot tell you *which* column is mixed; the per-column
evidence is `type_homogeneity` (see below). Read the two together.

Two kinds of column keep the plain type check rather than the dominant-class
rule, so a low score there means something different: columns declared through
`identifier_columns` (mixed forms are intended in an ID scheme), and columns
whose name announces dates, which are held to date formats however their values
look.

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

The overall score is a weighted combination (`score_weights()`). Report what
drove it, not just the number.

## Type homogeneity

`report["col"].type_homogeneity` counts the column's non-null values by lexical
class: `{"numeric": 600, "date": 0, "boolean": 0, "text": 400}`. It answers the
question `data_type` cannot — a column of names and a column that is 60% numbers
are both `string` — and it is the only per-column evidence that a column defeated
type inference, because `invalid_count` is absent on string columns by contract.

The absence rule applies with a twist worth stating twice:

- `None` — the classification did not run (a report reloaded from a document
  written before the field existed, or a hand-edited one).
- all four counts `0` — classified, and there was nothing to classify: the
  column is all-null or has no rows.

Never read the second as the first, or either as "one uniform class".

The counts cover the values the profiler retained, which on a source larger than
the 10k per-column reservoir is a sample. Sum them and compare against
`total_count - null_count`: short means the shares are sampled, and you should
say so. `to_llm_context()` does this for you and appends `sampled N of M values`
to the flag.

A `mixed types` flag appears in `to_llm_context()` once at least 5% of a
column's classified values fall outside its dominant class. That threshold is
display only — it changes no score — and `identifier` columns are exempt,
because an ID scheme mixing `A1` and `123` is intended rather than a defect.

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

The supported locales are `CA`, `DE`, `FR`, `GB`, `IT`, `US`. Case, the alpha-3
code (`ITA`) and the BCP 47 / POSIX forms (`it-IT`, `it_IT`) all normalise to
the same locale; any other tag raises `ValueError` instead of returning a report
with every locale-specific pattern suppressed.

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
