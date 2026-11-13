# Profile report JSON Schema

The versioned
[`profile-report.v1.schema.json`](profile-report.v1.schema.json) document is the
machine-readable contract for serialized `ProfileReport` values. It uses JSON
Schema 2020-12, carries a stable project-owned `$id`, and keeps every `$ref`
self-contained under `$defs`.

Schema versions are independent of package versions. The filename and `$id`
must use the same version as Rust's `REPORT_SCHEMA_VERSION` and Python's
`dataprof.REPORT_SCHEMA_VERSION`.

Starting in 0.12, the canonical persisted document is Rust's complete runtime
report: `data_source`, `column_profiles`, report `id` and `timestamp`, and the
confidence-wrapped quality assessment. Python `to_json()` and JSON `save()` use
the same Rust serializer. `ProfileReport::to_json()` sorts object keys recursively
for byte-stable output; arrays retain their order. Ordinary Rust Serde output
has the same document values, without a promise about map-key byte order.

### The v1 compatibility decision (#714)

Version 1 deliberately continues to accept two shapes:

- Rust's complete runtime document (`data_source`, `column_profiles`, and the
  confidence-wrapped quality assessment).
- The historical Python summary (`source`, `source_type`, `columns`, and its
  flattened quality summary), still available through Python `to_dict()`.

Both producers now live in Rust, and the summary's producer uses the very types
that generate its schema. Python no longer builds a parallel serialized report.
The summary is a convenience projection, not a lossless persistence format;
`json.loads(report.to_json())` is the full document as a Python dictionary.

Python `from_dict()`, `from_json()` and `load()` accept either shape. A loaded
flat summary stays flat when resaved: it never recorded report identity, full
source metadata or quality confidence, so manufacturing those fields would
misrepresent the original run. Missing provenance markers stay missing; the
resave does add `schema_version`, because it is written by a v1 build. Rust's `ProfileReport` reader reads the canonical shape; the flat
compatibility loader remains a Python API. `check()` refuses a loaded flat summary,
because the gate runs on Rust's report.

This preserves the published v1 validation contract instead of silently
narrowing it. New profiles save only the canonical shape. Consumers of the old
JSON layout should use `to_dict()` for its summary keys, or migrate to the
canonical paths above. A future schema v2 may remove the summary branch from
the persistence schema; that requires an explicit migration for incomplete
legacy provenance, not relabeling a flat document as a complete runtime report.

The canonical quality block now also retains `scores.overall_score` and
`scores.dimension_scores`, at the existing two-decimal score precision. These
are the aggregate values the Python summary already persisted. They must travel
with the rounded input metrics: deriving them again from rounded ratios can
change the saved score beyond its documented rounding. Older canonical documents without `scores`
remain readable and derive scores from the metrics they recorded. Rust callers
that edit a restored assessment's public metrics invalidate its saved scores;
subsequent score access and serialization use the edited metrics.

Both dialects accept unknown additive object properties. This matches the v1
reader policy and lets compatible fields be added without invalidating stored
reports. Required fields, schema version, known enum values, and primitive
types remain enforced.

`execution.sampled_row_ranges` is an optional additive v1 field in both
dialects. Each `[start, end]` pair identifies a zero-based, half-open source
row interval selected for analysis, in source order. Capped Parquet reads
record at most 32 ranges. Absence means the selection was not recorded, while
`[]` means a recorded selection of zero rows. Existing prefix-capped reports
remain readable and do not gain inferred ranges. The existing truncation reason
and sampling fields describe the cap and coverage; these ranges identify its
analyzed population exactly.

## Execution recovery provenance

`execution.recovery_events` is optional execution provenance in both v1
dialects. New reports record an ordered array; `[]` means no recovery occurred.
An absent field means history is unknown, including when loading older reports,
and remains absent on reserialization. It does not change metric semantics or
participate in cross-engine metric comparisons.

Each event contains `kind` (`engine_fallback` or `csv_auto_recovery`),
`attempted` (the engine or parse strategy that failed), `retry` (the next engine
or strategy), and `error` (diagnostic text). Engine names are `columnar` and
`incremental`, matching `execution.engine`, which still identifies the engine
that produced the report. Strategy names and error messages are diagnostics,
not stable codes. The last retry in a successful result succeeded; preceding
retries may have failed and led to further events. Row-level error and ragged-row
counts retain their existing meaning and are not incremented for failed attempts.

Python exposes the same history through `report.recovery_events`. No current
engine records `csv_auto_recovery`: it came from the standalone CSV recovery
parser, removed before 1.0, and stays in the schema so reports written by earlier
versions still load.

## Numeric equality contract

`skewness` uses adjusted Fisher-Pearson sample skewness and requires at least
three finite numeric values. `kurtosis` uses adjusted Fisher sample excess
kurtosis and requires at least four. These match pandas
[`Series.skew`](https://pandas.pydata.org/docs/reference/api/pandas.Series.skew.html)
and [`Series.kurt`](https://pandas.pydata.org/docs/reference/api/pandas.Series.kurt.html).
A constant column reports zero once the minimum count is met. When a reservoir
is used, its mean, sample standard deviation and higher moments all describe
that same retained sample. `is_approximate: false` records exact numeric
statistics; `true` records sampling, and absence means the approximation status
was not analyzed or is missing from an older report.

The coefficient of variation is `100 * std_dev / abs(mean)`. Its zero-mean
guard is relative to the spread (`abs(mean) <= f64::EPSILON * abs(std_dev)`),
so changing measurement units does not make the statistic disappear.

Timeliness compares whole dates against the freshness cutoff. A configured
`max_data_age_years` uses the Gregorian mean year of 365.2425 days, including
fractional years; the age budget is rounded down to whole days before it is
subtracted from the reference date. A date equal to the cutoff remains fresh.

For numeric columns, `mean` is always a finite number, including when the sum
of the input values overflows `f64`: the mean of finite values lies between
their minimum and maximum. `std_dev` and `variance` remain required fields in the
canonical report. They are `null` when the spread of finite input values cannot
be represented as a finite `f64`; a true constant column reports `0.0`. A
variance too large for `f64`, as for values near ±1e155, is `null` while its
standard deviation, about the size of the values, fits: it is taken without
forming the variance and reported with the skewness and kurtosis that follow
from it, and with the coefficient of variation wherever the mean allows one;
values balanced around a mean of 0 have none (#1150). Only when the sum of
squared deviations passes about 2e609, a standard deviation past about 4e304
divided by the square root of the row count, is the standard deviation `null`
too. A variance too small for `f64`, as for values near 1e-200, is `null` too
rather than `0.0`, and its standard deviation is reported the same way (#1088).
Values below about 1e-306, at the bottom of `f64`'s range, square to products
that lose their digits even so: their `std_dev` is
`null` as well rather than a number with too few digits. Statistics derived
from an unrepresentable standard deviation, including the coefficient of
variation, skewness and kurtosis, are absent. The same rule
covers `quartiles.iqr`: it stays required and is `null` when `q3 - q1` cannot be
represented, while `q1`, `q2` and `q3` are still reported. `outlier_count` is
then `0`, which is exact: fences beyond the `f64` range leave no finite value
outside them. This distinction survives report save/load and applies to the
Python summary as well. A document that omits any of the three keys is
malformed and fails to load; it is not read as `null`.

Integers of 2^53 or more in magnitude, up to the 64-bit range, are not exact as
`f64`. `variance`, `std_dev`, `skewness` and `kurtosis` are computed from the
exact integers on every input path, so epoch nanoseconds a few units apart keep
their spread (#877). `mean` sums the exact integers in a compensated `f64`
sum. `mode` is counted over the exact values, so distinct values that round
alike do not tie for it, and is reported as the nearest `f64` of the exact
mode. `quartiles.iqr` and the fences `outlier_count` uses come from exact
differences between neighbouring values, so a spread of a few units has an IQR
of a few units (#1038). `min`, `max`, `median`, `q1`, `q2` and `q3` are taken from
the nearest `f64` of each value, so `iqr` can differ from `q3 - q1` as
reported.

`stats.exact` carries the order statistics themselves, as canonical decimal
strings (no exponent, no trailing zeros in a fraction): `min`, `max`, `median`,
`q1`, `q2`, `q3` and `mode`, each present when the `f64` field of the same name
is, over the same values. It is present on every integer column and every
decimal-typed column (an Arrow decimal read from Arrow, Parquet, pandas or
polars, and a PostgreSQL `NUMERIC` or MySQL `DECIMAL` column, whose declared
type selects the decimal reading of its text, #1268), and absent on a float
column, whose values are not held exactly. A CSV or JSON column of long decimal
text is typed `float` and has no `exact` although its spread is exact. A median
or quartile between two integers is a decimal, such as `"9007199254740997.5"`. `exact` is absent on a report written before it
existed (#1038).

Decimals with more digits than `f64` keeps get the same treatment (#1149):
`variance`, `std_dev`, `skewness` and `kurtosis` come from their exact
deviations, so cents on 10^15 keep their spread, `mode`, `quartiles.iqr` and
`outlier_count` are exact as above, `mean` sums each value's remainder beyond
its nearest `f64` in the compensated sum, as it does for integers (#1262), and
the other statistics are taken in `f64`. A column typed as a decimal (an Arrow decimal of any
width, from `Decimal32` to `Decimal256`, read from Arrow, Parquet, pandas or
polars, or a PostgreSQL `NUMERIC` or MySQL `DECIMAL` column) is held exactly
from 11 significant digits. Text is held exactly
from 18: the shortest form of an `f64`, what Python and Rust write, has at most 17 digits, so shorter text reads
as the float it may be, as the same column typed `Float64` does. A CSV of a
decimal column therefore serializes what the typed column does when its values
have at most 10 significant digits or at least 18, and may differ in between. A
decimal too wide to hold exactly, past 38 digits, keeps a spread only where it
stands well above the rounding of its nearest `f64` (2^-32 of the magnitude);
below that, a constant column of such values included, the spread statistics
are `null`.

### Numeric domain

Exact handling covers the values profiling meets in real data: integers up to
the 64-bit range, decimals of any width a source declares, float literals with
more digits than `f64` keeps, and finite `f64` values whose arithmetic stays
inside `f64`'s range. Inside this domain every route serializes the same
values, and a difference is a parity defect.

Values near the limits of `f64` form an approximate domain: magnitudes within a
few orders of `f64::MAX` (about 1.8e308), where a fence, a difference or a sum
can overflow, and magnitudes below about 1e-300, including literals such as
`1e-400` that underflow. The guarantees above still hold there: `mean` is
finite, and a spread `f64` cannot represent is `null`. Other results that
depend on arithmetic at those magnitudes may differ from exact arithmetic and
between routes, such as `outlier_count` and the accuracy outlier ratio when a
Tukey fence would overflow (#1238). A difference confined to this domain is
documented behaviour, and it is not fixed case by case.

The cross-engine identical-numbers contract governs **serialized, rounded
metric values** (#547): Rust's Serde report and Python's `to_dict()`,
`to_json()`, and JSON `save()` output. JSON persistence now uses the same document
layout; `to_dict()` retains the summary projection described above.

For the same logical values, schema semantics, analysis options and analyzed
population, serialized metrics must compare exactly, with no additional
tolerance or rounding by the consumer. Counts, types and absence must also
agree: `None`/missing means not analyzed, while empty means analyzed with
nothing found. Source names, engine identifiers, timestamps, elapsed time,
throughput, memory and input-byte measurements describe execution and are not
cross-path equality targets. Whole report documents need not be identical.

| Metric kind | Serialized precision |
| --- | --- |
| 0–100 percentages, including coefficient of variation | 2 decimal places |
| Moment statistics: mean, standard deviation, variance, skewness, kurtosis | 4 decimal places, at most 12 significant digits |
| Data values: extrema, median, mode | 4 decimal places |
| 0–1 ratios and average text length | 4 decimal places |
| Quartiles | 2 decimal places |

Rounding uses the stored binary float, with ties away from zero. The Rust
`serde_helpers` and Python `_rounding` modules implement the same convention.
Native Rust fields and Python `ColumnProfile` attributes retain full precision;
their final digits may depend on accumulation order. Raw values are not
required to be bit-identical across engines, or to equal the rounded values
restored from JSON. Parity tests supplement exact serialized comparisons with
raw diagnostics using relative tolerance `1e-9` and absolute tolerance `1e-12`;
those tolerances do not weaken the serialized contract.

Moment statistics are accumulated, so their last bits depend on the order the
values were added in, which differs between routes. Four decimal places absorb
that drift only while they are finer than it, below about 1e11, so moment
statistics also keep at most 12 significant digits (#1178). Below 1e8 that is
the 4dp value; above it the digits where the drift lives are dropped, and the
rounded value is the float nearest its decimal form. Data values are read, not
accumulated, and keep 4 decimal places at any magnitude.
Rounding alone cannot mathematically guarantee equality for every possible
floating-point input, especially near rounding boundaries. Inside the
[numeric domain](#numeric-domain), a difference that survives serialization
remains a parity defect to investigate, rather than an allowed raw-precision
difference. The known
nested JSON/Arrow representation mismatch (#637) is one such defect.

Text frequencies are unassessed in newly computed profiles (#709):
`most_frequent` and `least_frequent` are `None` and omitted by Rust Serde on
every path, including `analyze_column` and the database connectors. Python
exports already omit these fields. This keeps absence consistent without
presenting retained-sample frequencies as full-column measurements. The fields
remain optional in schema v1; Rust deserialization and reserialization preserve
historical frequency measurements, including measured-empty lists. Callers that
need explicit frequency analysis can use
`dataprof_metrics::stats::text::{calculate_most_frequent, calculate_least_frequent}`
over their chosen population; these helpers count exactly the supplied values,
so callers choose which null-like values to exclude.

Sampling and approximate statistics must retain their provenance. Different
row selections or retained samples are different analyzed populations; compare
like with like rather than treating sampling differences as numeric drift.
Producer-side conversions also matter: pandas widening nullable integers to
floats changes the schema before dataprof sees it.

Derived threshold decisions should use serialized precision so that saving and
loading cannot change the verdict, as established for `to_llm_context()` in
#526. That is not yet true of every consumer: `_dominant_pattern` compares the
native `confidence` against its 0.5 threshold while serialization rounds that
value to four decimals, so a confidence just under the boundary can be
suppressed natively and emitted after a round trip.
Structural claims remain exact: `all-null` is based on null and total counts,
not a percentage rounded to 100. This decision does not define historical
metric-unit compatibility, tracked separately in #675.

## Regenerate and verify

Run from the repository root:

```bash
cargo run --example generate_profile_schema
cargo test --test profile_report_schema
```

The generator explicitly selects `SchemaSettings::draft2020_12()` and the
Serde serialization contract. CI reruns it and fails if the committed artifact
drifts. Rust tests validate the artifact against the bundled Draft 2020-12
meta-schema, compile it without network resolution, and validate representative
reports. Python tests validate `to_dict()`, `to_json()`, and JSON `save()`
output against the same file.

When changing a serialized report field, review compatibility first. Additive
fields with reader defaults stay in the current schema version; an incompatible
change requires incrementing `REPORT_SCHEMA_VERSION` and committing a new
versioned schema without deleting schemas that supported releases still read.

## Validate a saved report

The schema is also useful to consumers that store reports outside dataprof. The
published URL is stable across package releases:

```text
https://andreabozzo.github.io/dataprof/schema/profile-report.v1.schema.json
```

For example, create a full JSON report and validate it with the optional
`jsonschema` package. The package is a validation-tool dependency only; it is
not required by the base `dataprof` wheel.

Because the schema allows unknown additive properties, a successful validation
confirms required fields and primitive types but does not reject extra keys.
New Python JSON saves write the `ProfileReport` shape, so the examples
below pin validation to that branch of the versioned schema. This keeps errors
focused on the fields that matter to Python consumers instead of reporting the
whole document as failing a top-level `anyOf`.

```bash
python -m pip install jsonschema
python - <<'PY'
import copy
import json
import os
from pathlib import Path
from urllib.request import urlopen

from jsonschema import Draft202012Validator

SCHEMA_URL = (
    "https://andreabozzo.github.io/dataprof/schema/"
    "profile-report.v1.schema.json"
)
REPORT_PATH = Path("report.json")
schema_source = os.environ.get("DATAPROF_SCHEMA", SCHEMA_URL)

if schema_source.startswith(("http://", "https://")):
    with urlopen(schema_source, timeout=30) as response:  # noqa: S310 - project default or caller-supplied URL
        schema = json.load(response)
else:
    schema = json.loads(Path(schema_source).read_text(encoding="utf-8"))

document_schema = {key: value for key, value in schema.items() if key != "anyOf"}
document_schema["$ref"] = "#/$defs/ProfileReport"
Draft202012Validator.check_schema(document_schema)
report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
validator = Draft202012Validator(document_schema)
errors = sorted(
    validator.iter_errors(report),
    key=lambda error: [str(path_part) for path_part in error.path],
)
if errors:
    for error in errors:
        print(f"{REPORT_PATH}: {error.json_path}: {error.message}")
    raise SystemExit(1)
print(f"{REPORT_PATH}: valid")

# A deliberately invalid document fails for a useful reason, rather than
# merely failing because the file could not be read.
invalid_report = copy.deepcopy(report)
invalid_report["schema_version"] = "not-an-integer"
invalid_errors = list(validator.iter_errors(invalid_report))
assert invalid_errors, "the invalid schema_version should be rejected"
print(
    "invalid example: rejected "
    f"({invalid_errors[0].json_path}: {invalid_errors[0].message})"
)
PY
```

The `report.json` input above can be produced by the normal Python API:

```python
import dataprof as dp

dp.profile("data.csv").save("report.json")
```

For offline or project CI validation, save the Python block above as
`validate_report.py` and use the checked-in artifact instead of the network URL:

```bash
DATAPROF_SCHEMA=docs/schema/profile-report.v1.schema.json python validate_report.py
```

A minimal GitHub Actions job can validate every report committed under
`reports/` without adding anything to the runtime package:

```yaml
name: Validate dataprof reports

on: [push, pull_request]

jobs:
  reports:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.x"
      - run: python -m pip install jsonschema
      - name: Validate saved reports
        run: |
          python - <<'PY'
          import json
          from pathlib import Path

          from jsonschema import Draft202012Validator

          schema_path = Path("docs/schema/profile-report.v1.schema.json")
          schema = json.loads(schema_path.read_text(encoding="utf-8"))
          document_schema = {
              key: value for key, value in schema.items() if key != "anyOf"
          }
          document_schema["$ref"] = "#/$defs/ProfileReport"
          Draft202012Validator.check_schema(document_schema)
          validator = Draft202012Validator(document_schema)
          report_paths = sorted(Path("reports").rglob("*.json"))
          if not report_paths:
              raise SystemExit("no JSON reports found under reports/")

          failures = []
          for report_path in report_paths:
              report = json.loads(report_path.read_text(encoding="utf-8"))
              failures.extend(
                  f"{report_path}: {error.json_path}: {error.message}"
                  for error in validator.iter_errors(report)
              )
          if failures:
              raise SystemExit("\n".join(failures))
          print(f"validated {len(report_paths)} report(s)")
          PY
```

## v1 additive change: `bytes` source type

Version 1.1 (additive) introduced the `"bytes"` value in `PythonSourceType` for
byte-buffer inputs (CSV/JSON/JSONL/Parquet bytes). Previously they were labeled
`"dataframe"`. Consumers matching on `"dataframe"` must also accept `"bytes"`
for byte inputs; stored v1 reports are unchanged.

## v1 additive change: nullable `overall_score`, `NotAssessed` confidence

Version 1.2 (additive) widened two value domains for reports where no quality
dimension had anything to assess — a header-only file, or one whose every
dimension has a zero denominator:

- `overall_score` in the Python dialect accepts `null` as well as a number.
  Previously the empty set of dimension scores was averaged to `0.0`, which
  reads as "this data is terrible" rather than "there was nothing to assess",
  and contradicted `report.quality_score`, which already returned `None`.
- `MetricConfidence` accepts a `"NotAssessed"` value in the Rust dialect's
  `quality.confidence`. `"Exact"` claimed certainty about a score that was
  never computed.

Both are widenings, so stored v1 reports remain valid. Consumers that read
`overall_score` as a number must now handle `null`, and consumers matching on
`MetricConfidence` must accept `"NotAssessed"`; in both cases the signal is
"not assessed", never zero. `assessed_dimensions` is empty for exactly these
reports and is the authority to branch on.

## v1 behaviour change: text lengths count Unicode scalar values

`min_length`, `max_length` and `avg_length` on a text column counted UTF-8 bytes
through 0.11, under names that disclose no encoding. They now count Unicode
scalar values, so a column of CJK or emoji values reports smaller numbers than a
stored 0.11 report does for the same data.

No property was added, removed or retyped, and ASCII values are unchanged, so
the schema's validation shape and version are unchanged and stored reports
remain valid. The document itself does change: both dialects' `min_length`,
`max_length` and `avg_length` gain `description` annotations naming the unit.

A consumer comparing a report across the 0.11/0.12 boundary is comparing two
units for non-ASCII text, which `schema_version` cannot signal because nothing
about validation changed. Compare within a release, or re-profile.

## v1 behaviour change: unassessed dimensions are omitted

The seven quality dimension objects (`completeness`, `consistency`,
`uniqueness`, `accuracy`, `timeliness`, `validity`, `precision`) were already
optional in both dialects, and are now genuinely absent when the dimension
assessed nothing. Previously a dimension was emitted whenever its metric struct
existed, which is whenever it was requested, so a file with no pattern-bearing
column published `validity.valid_values_ratio: 100.0` beside
`values_checked: 0`.

No property was added, removed or retyped, so the schema document is unchanged
and stored reports remain valid. What changed is which optional properties
appear: a consumer that assumed every dimension key was present must read it as
optional. A dimension key is present exactly when that dimension had a positive
denominator, which is exactly when its `dimension_scores` entry is a number.

That is a slightly wider set than `assessed_dimensions`, which additionally
filters on a positive score weight. Under the default weights the two agree; a
dimension configured with a weight of `0.0` is assessed and serialized while
contributing nothing to `overall_score`, so it is absent from
`assessed_dimensions`. Read a dimension's presence as "this was measured", not
as "this is behind the overall score".

## v1 additive change: `quality_status`

Both dialects now carry a top-level `quality_status` object naming what
happened to the quality computation. It has a `state` string and, on `failed`
only, an `error` message:

| `state` | meaning |
| --- | --- |
| `computed` | quality was computed; `quality` holds the assessment |
| `not_requested` | the quality pack was deselected for this run |
| `no_data` | requested, but no quality sample was supplied to the assembler |
| `withheld_by_projection` | requested, but every requested dimension measures whole rows and the run profiled a subset of columns |
| `failed` | requested and attempted; the computation failed, and `error` says how |
| `unrecorded` | read back from a document written before this field existed |

`quality` alone could not answer this. A run that never asked for quality and a
run whose quality computation failed both left it absent, so absence itself was
the plausible value that hid the failure, and the only record of the failure was
a `log::warn!` line that no consumer reads back.

The field is additive and not `required`: stored v1 reports remain valid. A
document without it reads back as `computed` when it carries an assessment and
`unrecorded` when it does not — a stored assessment proves the computation ran,
and nothing else about an older document is knowable.

`quality_status` is provenance, not a metric, so it is outside the numeric
equality contract above. It is assigned in one place, `ReportAssembler`, which
every input path builds through, and agrees across engines and paths for a
source with the same analysis options, including an empty source.

### 0.12 empty-source quality parity (#723)

A successfully analyzed zero-row source is **analyzed, nothing found**, whether
it declares columns or has no columns at all. With quality enabled, CSV, JSON,
JSONL, Parquet, queries and Arrow/dataframe inputs report `computed` and carry
an assessment with no assessable dimensions. Python serializes
`overall_score: null`, `assessed_dimensions: []` and null dimension scores;
Rust serializes the corresponding empty quality metrics. File, byte-buffer,
async and HTTP transports follow the same rule. Header-only CSV and zero-row
Arrow/Parquet schemas retain their columns.
Empty CSV streams and zero-column Arrow RecordBatches, which previously raised
errors, now produce reports too.

Previously, empty CSV files on the direct parser, incremental and columnar
paths withheld quality and reported `no_data`, while CSV buffers and most
other inputs emitted an empty assessment. Those CSV file paths, and the
database assembler's no-column branch, now emit the assessment too. This is a
behavior correction within schema v1: both shapes were already valid. Stored
reports keep their original status and quality presence when loaded; consumers
comparing empty extracts should regenerate older baselines for parity.

Deselecting quality still yields absent `quality` and `not_requested`, and
projection withholding still applies to the requested dimensions. `no_data`
remains available for an assembler that receives no quality sample, distinct
from an explicitly supplied empty sample. It no longer denotes a successfully
analyzed empty source.

## v1 additive change: `score_weights` in the Python dialect

The Python dialect's `quality` object now carries `score_weights`, the relative
weights behind `overall_score`, with the same rule the Rust dialect applies to
`quality.metrics.score_weights`: custom weights are written and default weights
are omitted (#760). The next section supersedes the omission: since 0.13 both
dialects write default weights too.

Previously the summary dropped them, so a report assessed with custom weights
came back from `ProfileReport.from_dict(report.to_dict())` with the default
weights next to an `overall_score` those defaults did not produce.

The field is additive and not `required`: stored v1 reports remain valid.
Summaries of reports that used the default weights were unchanged by #760. A summary
written before this change omits the field whatever weights were used, so there
its absence does not show that the defaults applied; the loader still reads an
absent field as the defaults, as it did before. The canonical document from
`to_json()` or JSON `save()` has kept custom weights since #714.

## v1 additive change: default `score_weights` are written too

Both dialects now write `score_weights` whatever the weights are, defaults
included (`quality.metrics.score_weights` in the canonical document,
`quality.score_weights` in the Python dialect). A stored `overall_score` is kept
rather than recomputed, so a document that omits default weights only names
the defaults of whichever release reads it; if a later release changed them,
the score and its decoded weights would disagree (#749).

The field stays optional in the schema, so stored v1 reports remain valid, and
an absent field is still read as the defaults. The default weights have not
changed since weights became configurable in 0.10, so on a canonical document
an absent field did mean the defaults. On a flat summary written before 0.12 it
does not, because those dropped custom weights; re-exporting such a summary
with `to_dict()` keeps the field absent rather than claim the defaults.

## v1 additive change: `metric_semantics`

Both dialects now carry a top-level `metric_semantics` object naming how the
report's measurements were defined. It has one field per intentionally changed
definition:

| field | values | meaning |
| --- | --- | --- |
| `text_length_unit` | `"unicode_scalar"` | unit of `min_length`, `max_length` and `avg_length` on text columns |

`schema_version` answers whether a document validates. It cannot answer whether
two valid documents measured the same way. Text lengths are the case in point:
they counted UTF-8 bytes through 0.11 and count Unicode scalar values since
(see above), with no change to the document's shape (#675).

Every report this release produces records the object. It is additive and not
`required`, so stored v1 reports remain valid. A document without it was written
before dataprof recorded it: its definitions are **unknown**, not the current
ones, and loading and saving it keeps the object absent rather than stamping the
reader's definitions on it. An explicit `null`, for the object or for a
definition inside it, is malformed: it fails to load in both dialects and the
schema rejects it. Definition names this build does not know are ignored, so a later
release can add one; a value this build does not know fails to load.

Python's `compare()` reports both sides and `comparable`: `True` when both
record every definition this release knows, with the same values, and `None`
otherwise, including when either side records an empty object. Only `True` makes a difference
in a measurement such as `max_length` a difference in the data.

The object is provenance, not a metric, so it is outside the numeric equality
contract above, and every engine and input path records the same value.

## v1 additive change: `Nested` data type

A column of structs, lists or maps is now typed `Nested` in the Rust dialect and
`"nested"` in the Python dialect, and reports its counts only: `total_count`,
`null_count` and `null_percentage`. `unique_count`, `invalid_count`,
`type_homogeneity`, `stats` and `patterns` are absent, meaning not analyzed
(#637).

Through 0.11 such a column was typed `String` and measured on a text rendering
of its values. That rendering was Arrow's display string on the Parquet and
Arrow paths and compact JSON on the JSON paths, so the same records reported
different lengths, distinct counts and patterns depending on the file format.
A JSON column is `Nested` when every non-null value is an object or an array; a
column mixing containers with scalars has no typed counterpart and stays text.

The enum gains a value, so stored v1 reports remain valid. A reader built
before this change rejects a report that contains the new value, as it would
any enum value it does not know. Consumers matching on `data_type` must accept
`nested`. A nested column typed `String` in a 0.11 report and `nested` in a
0.12 report is the same data measured under a different definition. Compare
within a release, or re-profile.

## v1 additive change: `execution.unterminated_quote`

Both dialects carry an optional boolean `execution.unterminated_quote` for CSV
input (#782). `true` means the source ended inside a quoted field: a quote was
opened and never closed, and the CSV parser read every following row into the
last field of one record. The report then counts one row where the source held
several, and nothing else in it shows the difference. `false` means the scan
reached the end of the source outside any quoted field.

Absence means the check did not run: non-CSV input, a scan that stopped before
the end of its source (a row cap, a byte budget), or a report written before
0.12. The quote that swallows the rest of the file is on the last record, so a
scan that did not reach the end did not read it.

Under `csv_flexible=False` such a source is refused instead, as a ragged row is.
The field is additive and not `required`, so stored v1 reports remain valid.
It is provenance about the source, not a metric, and every CSV engine and input
path (file, bytes, async) reports the same value for the same source.

## v1 additive change: `slash_date_order`

The Rust dialect's `DateTime` statistics carry an optional `slash_date_order`
(#811). It records how the column's `NN/NN/YYYY` values were read, decided once
per column from its own values:

- `month_first`: some values only parse month-first (`12/31/2024`) and none
  only day-first, so `01/02/2024` is 2 January.
- `day_first`: the reverse.
- `assumed_day_first`: every value parses both ways, so nothing decides it; the
  values were read day-first.
- `mixed`: values that only parse day-first and values that only parse
  month-first are both present. Each was read in the order it parses, and the
  dates cannot all be right.

Absence means the column holds no slash dates, or the report predates 0.12.

Through 0.11 each value was parsed on its own: month-first when day-first
failed, day-first otherwise. A US column therefore read its ambiguous values
day-first and reported wrong minimums, month distributions and timeliness
counts. Dates, statistics and timeliness checks derived from slash dates in
0.12 reports can differ from 0.11 reports of the same data. The field is
additive and not `required`, so stored v1 reports remain valid.

## v1 additive change: `locale_number_count`

Both dialects carry an optional integer `locale_number_count` on each column
(#433). Of the values `type_homogeneity` counted as `text`, it is how many are
numbers written with a decimal comma or digit-group separators: `10,50`,
`1.234,56`, `1,234.56`, `12,34,567`, `1'234.56`, `1 234,56`. dataprof does not
parse these as numbers, so they are in no numeric statistic. A column made of
them is typed `string`, and a numeric column counts them in `invalid_count`.
The count does not decide which convention a value follows: `1,234` is a
thousand in one locale and a fraction in another.

It is counted over the same values as `type_homogeneity` and is present
whenever that field is. Absence means the count did not run: a `nested` column,
or a report written before 0.12. `0` means the column was checked and none were
found. The `locale_numbers` finding reads it, and lists a column from an older
report as `unrecorded` rather than clean.

The field is additive and not `required`, so stored v1 reports remain valid.
Every engine and input path reports the same count for the same values.

## v1 additive change: `metric_semantics.null_tokens`

`metric_semantics` gains a second definition (#813):

| field | values | meaning |
| --- | --- | --- |
| `null_tokens` | `"common_markers"` | the text values counted as null |

Under `common_markers` a value is null when it is empty or whitespace, `null`
or `nan` in any case, or exactly one of `NA`, `N/A`, `n/a`, `#N/A`, `\N` and
`None`, ignoring surrounding whitespace. The markers match only in that
spelling, so `Na` and `none` stay values. Typed nulls (an Arrow null, SQL
`NULL`, Python `None`, a float NaN) are null on every path, as before.

Through 0.12 only empty, `null` and `nan` were nulls, and the markers were
counted as values: completeness read 100% on columns that were mostly missing,
and a numeric column with markers in it could fall below the inference
threshold and be typed `string`. Null counts, completeness, inferred types and
every statistic of a column holding these markers can differ from a 0.12 report
of the same data.

A 0.12 report records `text_length_unit` and not `null_tokens`, so its null
vocabulary reads back as unknown and Python's `compare()` reports
`comparable: None` against a current report. Every engine and input path
records the same value. The field is additive and not `required`, so stored v1
reports remain valid; an explicit `null` is malformed, as for
`text_length_unit`.

## v1 additive change: `metric_semantics.pattern_match_count` and `shape_moments`

`metric_semantics` gains two definitions:

| field | values | meaning |
| --- | --- | --- |
| `pattern_match_count` | `"validated"` | what every pattern's `match_count` and `match_percentage` count |
| `shape_moments` | `"adjusted_sample"` | the estimator behind `skewness` and `kurtosis` |

Under `validated` a pattern match is a value that passes the pattern's regular
expression and, where the pattern has one, its semantic validator (checksum,
range or calendar check). Through 0.12 every regular expression match counted,
so random 16-digit ids were 100% `Credit Card`.

Under `adjusted_sample` skewness is the adjusted Fisher-Pearson sample skewness,
from three values, and kurtosis the adjusted Fisher sample excess kurtosis, from
four, as pandas computes them. Through 0.12 both divided population moments by
the sample standard deviation.

A report that records neither reads back with both unknown, and Python's
`compare()` reports `comparable: None` against a current report. Every engine
and input path records the same values. The fields are additive and not
`required`, so stored v1 reports remain valid; an explicit `null` is malformed,
as for `text_length_unit`.

## v1 additive change: `temporal_pair` hint kind

`SemanticHintKind`, the `kind` of a `semantic_hint_bindings` entry, gains the
value `temporal_pair` in both dialects. Each column named by a `temporal_pairs`
entry is recorded once with this kind.

The enum gains a value, so stored v1 reports remain valid. A reader built
before this change rejects a report that contains the new value, as it would
any enum value it does not know. Consumers matching on `kind` must accept
`temporal_pair`.

## v1 additive change: `analysis_settings`

Both dialects gain a top-level `analysis_settings` object recording the
settings a report was profiled with that move its scores on unchanged data and
are recorded nowhere else in it (#1010):

| field | values | meaning |
| --- | --- | --- |
| `pattern_locale` | `"none"`, `"CA"`, `"DE"`, `"FR"`, `"GB"`, `"IT"`, `"US"` | the locale detected patterns were ranked with, which the validity score reads; `"none"` when no locale was given |
| `columns` | `{"selection": "all"}` or `{"selection": "listed", "names": [...]}` | the columns the run profiled: every column, or a `columns=` projection in the order given. A projection that names a missing column is refused, so with `"all"` a column without a profile was not in what the run read |

`metric_semantics` records how a release defines its measurements;
`analysis_settings` records what the caller chose. Score weights live in the
quality assessment and semantic hints in the hint bindings, so neither is
repeated here.

The object is absent from documents written before 0.13, whose settings are
unknown rather than the defaults, and from a report built directly with
`ProfileReport::new` rather than through a profiling run. An explicit `null`,
an object without `pattern_locale` or `columns`, an unknown locale value, and a
`columns` object of another shape are malformed.
A field a later release adds is ignored by this reader. Stored v1 reports
remain valid.

## v1 additive change: `metric_semantics.range_rule_count` and the accuracy rule units

`metric_semantics` gains one definition:

| field | values | meaning |
| --- | --- | --- |
| `range_rule_count` | `"violating_values"` | what the range and sign rules behind the accuracy score count |

`quality.metrics.accuracy` gains the two counts it defines (#1005):

| field | meaning |
| --- | --- |
| `rule_values_checked` | numeric values, a literal past the `f64` range included, of the columns a range rule (`age`, percentage, `count`, `year` names) or the sign rule (`positive_columns`) applies to |
| `rule_violating_values` | of those, the values breaking at least one rule, each counted once |

The accuracy score subtracts `100 * rule_violating_values / rule_values_checked`
from `100 - outlier_ratio` (9 violating values of 20 take off 45 points);
columns under no rule add nothing to either side,
and with no such column the rules take nothing off. Through 0.12 every rule
failure counted, a negative percentage hinted positive twice, and the failures
were divided by `numeric_values_checked`, the numeric values of every column,
so clean columns beside a broken one raised its score. `range_violations` and
`negative_values_in_positive` keep counting failures per rule, as diagnostics.

A report that records no `range_rule_count` reads back with it unknown, and a
quality gate leaves the accuracy and overall score rules over it
`definition_not_comparable`. Both counts default to 0 when absent, and such a
document still scores its range and sign failures over
`numeric_values_checked`, as the release that wrote it did. The fields are
additive and not `required`, so stored v1 reports remain valid; an explicit
`null` for `range_rule_count` is malformed, as for `text_length_unit`.

## v1 additive change: `metric_semantics.key_uniqueness_units` and `key_values_checked`

`metric_semantics` gains one definition:

| field | values | meaning |
| --- | --- | --- |
| `key_uniqueness_units` | `"non_null_values"` | what `key_uniqueness` divides the key column's distinct values by |

`quality.metrics.uniqueness` gains `key_values_checked`, the non-null values of
`key_column` (#1005). `key_uniqueness` is the key's distinct values over them,
so a nullable key no longer loses uniqueness for the nulls completeness already
counts: 100 distinct ids and 100 nulls score 100, where 0.12 reported 50. A key
column holding no value is still named, with `key_values_checked` 0, and its
`key_uniqueness` carries no signal: the uniqueness score then rests on the
duplicate-row component alone, or the dimension is unassessed without one.

A report that records no `key_uniqueness_units` reads back with it unknown, and
a quality gate leaves the uniqueness and overall score rules over it
`definition_not_comparable`. `key_values_checked` is absent when no key column
was identified, and on documents written before it, whose `key_uniqueness`
divided by every row and still scores as recorded. Both fields are additive and
not `required`, so stored v1 reports remain valid; an explicit `null` for
`key_uniqueness_units` is malformed, as for `text_length_unit`.

## v1 additive change: `metric_semantics.outlier_units` and `outlier_values_checked`

`metric_semantics` gains one definition:

| field | values | meaning |
| --- | --- | --- |
| `outlier_units` | `"tested_values"` | what `outlier_ratio` is measured over |

`quality.metrics.accuracy` gains `outlier_values_checked`, the finite numeric
values of the columns that reached `outlier_min_samples` (#1005). With none, no
outlier test ran: `outlier_ratio` is 0 and carries no signal, and the component
takes nothing off the score. Through 0.12 that 0 read as a measurement.

The accuracy dimension is now assessed when one of its components had units: an
outlier test ran, or a range or sign rule applied to a value
(`rule_values_checked`). Numeric values neither component examined no longer
assess it, so a source whose only numeric columns are shorter than
`outlier_min_samples` and fall under no rule reports accuracy as unassessed,
where 0.12 scored it 100. The overall score then renormalizes over the other
dimensions, as for any unassessed one.

A report that records no `outlier_units` reads back with it unknown, and a
quality gate leaves the accuracy and overall score rules over it
`definition_not_comparable`. `outlier_values_checked` is absent on documents
written before it; those are assessed and scored as their release did. The
field is additive and not `required`, so stored v1 reports remain valid; an
explicit `null` for `outlier_units` is malformed, as for `text_length_unit`.

## v1 additive change: `metric_semantics.key_column_source`

`metric_semantics` gains one definition:

| field | values | meaning |
| --- | --- | --- |
| `key_column_source` | `"declared"` | which column `key_uniqueness` may measure |

`key_uniqueness` now measures only a declared key: the first `identifier_columns`
entry the source has (#1065). Through 0.12, with none declared, the first column
whose name had an `id`, `key` or `pk` word was the key. A name cannot tell a
table's key from a foreign key, so a `customer_id` listed before `order_id` was
scored as the table key, and the same clean orders table scored 100 or 94.38
depending on column order. Without a declared key, `key_column` and
`key_values_checked` are absent, `key_uniqueness` carries no signal, and the
uniqueness score rests on the duplicate-row component alone, or the dimension
is unassessed without one. Declare the key to have it scored:
`profile(..., identifier_columns=["order_id"])`.

A report that records no `key_column_source` reads back with it unknown, and a
quality gate leaves the uniqueness and overall score rules over it
`definition_not_comparable`. `findings()` lists its `key_duplicates` rule as
`unrecorded` rather than judging a key that may be a guess from column names.
Documents written before it keep their key and score as recorded. The field is additive and not `required`, so stored v1
reports remain valid; an explicit `null` for `key_column_source` is malformed,
as for `text_length_unit`.

## v1 additive change: `metric_semantics.statistic_rounding`

`metric_semantics` gains `statistic_rounding`, with the one value
`"four_decimals_twelve_significant"`: every column's `mean`, `std_dev`,
`variance`, `skewness` and `kurtosis` are serialized at 4 decimal places and at
most 12 significant digits (#1178). Through 0.13 they kept 4 decimal places at
any magnitude, where the order values were accumulated in could change the last
serialized digits between input routes.

A report that does not record it reads back with the definition unknown, and
Python's `compare()` reports `comparable: None` against a current report. Every
engine and input path records the same value. The field is additive and not
`required`, so stored v1 reports remain valid; an explicit `null` is malformed,
as for `text_length_unit`.

## v1 additive change: `metric_semantics.accuracy_comparison`

`metric_semantics` gains one definition:

| field | values | meaning |
| --- | --- | --- |
| `accuracy_comparison` | `"exact_values"` | how the accuracy score compares a value with its outlier fences and its range and sign bounds |

The accuracy score now compares each value as written (#1199): outliers are
counted with the exact Tukey fences a column's `outlier_count` uses, and a
value whose nearest `f64` equals a range or sign bound is compared with it by
its digits. Through 0.13 both comparisons went through the value's nearest
`f64`, so `100.000000000000000001` in a percentage column broke no rule, and a
64-bit integer past 2^53 could land on the wrong side of a fence. A column
whose source declared a decimal (`native_type`) is read as a decimal with or
without statistics (#1268); a pandas `object` column of `Decimal` values
declares none and is read as text when statistics are not requested. The same
bytes can therefore score lower on accuracy, and overall, than they did.

A report that records no `accuracy_comparison` reads back with it unknown, and
a quality gate leaves the accuracy and overall score rules over it
`definition_not_comparable` (#1237), so a baseline saved by 0.13 does not fail
the gate on unchanged data. Regenerate the baseline with the current release
to have those rules decided again. The field is additive and not `required`,
so stored v1 reports remain valid; an explicit `null` for
`accuracy_comparison` is malformed, as for `text_length_unit`.

## v1 additive change: `metric_semantics.json_number_reading`

`metric_semantics` gains one definition:

| field | values | meaning |
| --- | --- | --- |
| `json_number_reading` | `"literal_digits"` | how a JSON number whose value its nearest `f64` does not hold is read |

A JSON number with more digits than an `f64` holds, like
`1000000000000000.01`, or one below its range, like `1e-400`, is now read from
its literal digits, as a CSV cell with the same text is (#1166). That covers a
field of a JSON or JSONL record on every route and a database JSON cell
(#1168); a number nested in an array or object keeps its `f64`. Through 0.13
such a number was read as its nearest `f64`, so distinct literals could count
as one value and duplicate rows, and lost their decimal places; a literal
written as `1e+20` now keeps its decimal point, which a column of
comma-decimal text counts as a mixed separator. The same bytes can therefore
score differently on consistency, precision, uniqueness, accuracy and overall
than they did.

A report that records no `json_number_reading` reads back with it unknown, and
a quality gate leaves the consistency, precision, uniqueness, accuracy and
overall score rules over it `definition_not_comparable` (#1243), so a baseline saved by 0.13
does not fail the gate on unchanged data. The definition is recorded for every
source format, so this applies to a 0.13 baseline of any format. Regenerate
the baseline with the current release to have those rules decided again. The
field is additive and not `required`, so stored v1 reports remain valid; an
explicit `null` for `json_number_reading` is malformed, as for
`text_length_unit`.

## v1 additive change: `metric_semantics.sampled_score_estimator`

`metric_semantics` gains one definition:

| field | values | meaning |
| --- | --- | --- |
| `sampled_score_estimator` | `"population_weighted"` | how a score computed over a retained sample estimates the score over every scanned value |

A column keeps at most 10,000 of its non-null values for the sampled
dimensions: consistency, accuracy, timeliness, validity and precision.
Through 0.13 their scores pooled those samples, so a column counted as many
values as its sample kept, and a sparse column read in full weighed as much
as a dense one sampled from many more values. The score could then fall
outside its own `score_bounds` (#1293). Each column's sampled counts are now
scaled to its non-null values in the source, which is the score the bounds
cover, so the score lies within its interval. A source whose samples hold
every value scores as before. Only the scores change: the metric values
beside them, such as `data_type_consistency` and `format_violations`, still
describe the sample.

A report that records no `sampled_score_estimator` reads back with it
unknown, and a quality gate leaves the consistency, accuracy, timeliness,
validity, precision and overall score rules over it
`definition_not_comparable`, so a baseline saved by 0.13 does not fail the
gate on unchanged data. Regenerate the baseline with the current release to
have those rules decided again. The field is additive and not `required`, so
stored v1 reports remain valid; an explicit `null` for
`sampled_score_estimator` is malformed, as for `text_length_unit`.

## v1 additive change: `generator`

Both dialects gain a top-level `generator` object naming the dataprof build
that produced the report (#907):

| field | values | meaning |
| --- | --- | --- |
| `version` | string | the dataprof package version |
| `commit` | string | the full git hash the build was compiled from |
| `dirty` | boolean | whether the compiled Rust sources (`Cargo.toml`, `Cargo.lock`, `crates/`) had uncommitted changes when it was built |

A development build carries the version of the release it follows, so
`version` alone cannot tell them apart; `commit` and `dirty` can. Both are
absent where the build did not record them: a crate built from a package, an
sdist, or a build without a usable `git`. The Python extension records them;
a Rust program built against the crates records `version` alone.

A stored report keeps the generator it was written with. The object is absent
from documents written before 0.13. An explicit `null`, for the object or for
`commit` or `dirty`, a missing `version`, and a value of the wrong type are
malformed. A field a later release adds is ignored by this reader. Stored v1
reports remain valid.

## v1 additive change: `source_identity`

Both dialects gain a top-level `source_identity` object recording which data
the report describes (#866). `data_source` and `source` name a location; the
data behind a path or a table name can change between the check and the
promotion, so a pipeline needs an identity of the data itself to compare with
what it promotes.

| field | values | meaning |
| --- | --- | --- |
| `version` | string | the version of the source the caller supplied (`source_version=`, `--source-version`), recorded verbatim: a table snapshot ID, a commit version, a load or batch ID |

dataprof neither interprets nor verifies `version`. No profiling path records
an identity on its own, so the object is absent unless the caller supplied
one, and it is absent from every document written before 0.14. Each member
names its kind of identity; a later release may add others, such as a digest
of the bytes read, and this reader ignores members it does not know, so an
object holding none it knows reads back as `{}`. An explicit `null`, for the
object or for `version`, and a value of the wrong type are malformed.

The identity is provenance, not a metric: two input paths over the same
logical data carry different identities, so like `data_source` it stays
outside the serialized-value parity comparison. Stored v1 reports remain
valid.

## v1 additive change: `csv_metadata`

A CSV report records how its source was split into fields (#1100). The Rust
dialect carries it as `data_source.csv_metadata` on a `file`, `bytes` or
`stream` source; the Python dialect carries the same object as a top-level
`csv_metadata`.

| field | values | meaning |
| --- | --- | --- |
| `delimiter` | one character | the delimiter the reader split records on |
| `delimiter_detection` | object | present when detection chose the delimiter; absent when the caller set it (`csv_delimiter`) |
| `delimiter_detection.delimiter` | one character | the delimiter detection chose |
| `delimiter_detection.header_fields` | integer | fields the chosen delimiter splits the header into: 1 when no candidate splits it, 0 when the sampled source holds no header record |
| `delimiter_detection.runner_up` | one character | another candidate that splits the header and keeps as many sampled records at the header's width, so structure alone does not tell the two apart; absent when the choice was clear |
| `delimiter_detection.unlisted_delimiter` | one character | when no candidate splits the header, a punctuation character outside the candidates that occurs as often in the header as in every sampled record; absent for an ordinary one-column source |

Detection tries comma, semicolon, tab and pipe, and these fields do not change
which one it picks. `findings()` reports `delimiter_ambiguous` when
`runner_up` is present and `header_not_split` when `unlisted_delimiter` is.

The object is absent for a source that is not CSV, and from every document
written before 0.14. For a CSV `file`, `bytes` or `stream` source without it,
`findings()` lists both rules as not evaluated (`unrecorded`) rather than
passing them. Like the rest of `data_source`, it is provenance and stays
outside the serialized-value parity comparison, although every CSV engine and
input path records the same object for the same source. Stored v1 reports
remain valid.

## v1 widening: optional stream `source_system`

`data_source.source_system` on a `stream` source is no longer required (#1219).
It is present when the caller declared the system the stream came from (`file`
for `profile_file`, `http` for `profile_url`, `arrow_c_stream` for an Arrow
stream) and absent when it declared none. Report assembly used to write `http`
for every undeclared source, so a report claimed a network read for data that
never crossed one.

In the same change, an async source that is a buffer already in memory and
declares no system records a `bytes` source, with the buffer's length as
`size_bytes`, as the synchronous bytes route does. `dataprof.asyncio.profile_bytes`
now records the same `bytes` source as `dataprof.profile` for the same input.

This is a widening: stored v1 reports remain valid, and a stored `http` value
reads back unchanged. A consumer that read `source_system` as always present
must treat it as optional; absent means "not recorded", never `http`.

## v1 additive change: stream `format`

A `stream` source records the format it was parsed as in
`data_source.format` (#1042): `csv`, `json` or `jsonl` for the async routes
(`profile_file`, `profile_url`, `profile_stream`), and `arrow` for an Arrow C
stream (#1230). It is absent in every document written before 0.14. `arrow` is
a new `FileFormat` value: no file, bytes or URL route records it, and a reader
built before 0.14 reads it as an unknown format.

The baseline gate reads a CSV stream as declaring its columns, as it reads a
CSV `file` or `bytes` source: its header lists every column however few rows
were read, so a capped async CSV run can fail a missing required column or an
added one rather than leaving it `evidence_incomplete`. It reads an Arrow
stream the same way, since the stream's schema lists every column before the
first batch. A stream without `format`, and a JSON or JSONL one, covers only
the records read, as before.
Like the rest of `data_source`, it is provenance and stays outside the
serialized-value parity comparison. Stored v1 reports remain valid.

## v1 additive change: summary `source_format`

The Python dialect, the flat `to_dict()` summary, records the format of a
`file`, `bytes` or `stream` source as a top-level `source_format` (#1028): the
value the Rust dialect carries in `data_source.format`. It is absent for a
`query` or `dataframe` source, for a stream that recorded no format, and in
every summary written before 0.14.

The baseline gate reads a summary's column set with the rule it applies to the
Rust dialect: a CSV or Parquet `file` or `bytes` source, and a CSV, Parquet or
Arrow `stream`, declare every column however few rows were read. A capped CSV
run kept as a summary baseline can then fail a removed or added column rather
than leave it `evidence_incomplete`. A summary without `source_format` covers
only the rows read, as before. Like `data_source`, it is provenance and stays
outside the serialized-value parity comparison. Stored v1 reports remain
valid.

## v1 additive change: summary `exact_dimensions`

The Python dialect, the flat `to_dict()` summary, records
`quality.exact_dimensions` beside `quality.sampled_dimensions` (#1027): the
components computed from every scanned row, in the same labels. For a `Mixed`
confidence it is the Rust dialect's `exact_dimensions`; an exact assessment
lists every component carrying signal, and an approximate one none. It is
absent when the coverage is not recorded, and in every summary written before
0.14.

With both lists recorded, a summary's readers (`compare()`, a summary
baseline in the gate, and findings) apply the rule the Rust dialect already
follows: a component carrying signal that neither list names has
`coverage_unrecorded` coverage, and so has any score that weighs it. A summary
without `exact_dimensions` keeps reading a component that is not listed as
sampled as exact. Stored v1 reports remain valid.

## v1 additive change: column `native_type`

Both dialects carry an optional `native_type` object on each column (#906):
the type the source declared for the column, which `data_type` does not keep.
Several declared types profile as one `data_type`, so an Arrow
`decimal128(12, 2)` column is `Float` and a `timestamp[us, tz=UTC]` column is
`Date`; `native_type` says which one the source declared.

| field | meaning |
| --- | --- |
| `system` | the type system the declaration belongs to |
| `name` | the type as that system spells it |

| `system` | sources | `name` |
| --- | --- | --- |
| `arrow` | Arrow tables, record batches and streams, polars frames, ADBC results, Parquet files and buffers, local or remote | the name pyarrow prints for the type: `decimal128(12, 2)`, `timestamp[us, tz=UTC]`, `dictionary<values=string, indices=int32, ordered=0>` |
| `pandas` | pandas DataFrames | the column's dtype as pandas prints it: `int64`, `object`, `datetime64[ns, UTC]`, `Int64`, `category` |
| `postgresql`, `mysql` | the database connectors | the server's type name as the driver reports it: `NUMERIC`, `INT4`, `TIMESTAMPTZ`, `DECIMAL`, `BIGINT UNSIGNED` |

A field that declares an Arrow extension type is named as the extension,
`extension<arrow.uuid>` rather than the `fixed_size_binary[16]` that stores it,
also as the child of a list, struct or map. pyarrow appends the parameters of a
parameterized extension (`arrow.fixed_shape_tensor[value_type=int32, ...]`);
the report keeps the name only. A Parquet column carries the Arrow type its reader produced from the file's
schema, which keeps the Arrow schema the writer stored with the file. A pandas
column records its dtype rather than the Arrow type the profiler read, because
an `object` column declares no type for its values: pyarrow infers one from
them. An index that pandas stores as columns takes the dtype of its level.

The field is absent when the source declares no type: CSV, JSON and JSONL in
every engine, Python dicts and lists of records, and SQLite, whose driver reads
a declared type into a few classes and reports the first value's storage class
for an expression or a declaration it does not know. It is also absent in every
report written before 0.14. Both values are strings, so a report that names a
system this build does not know still loads.

The declaration is provenance, not a metric. Two input paths over the same
logical data declare different types or none, so it stays outside the
serialized-value parity comparison. Stored v1 reports remain valid.

## v1 additive change: `analysis_settings.metric_packs`

`analysis_settings` gains an optional `metric_packs` list (#1030): the metric
packs the run computed, from `"schema"`, `"statistics"`, `"patterns"` and
`"quality"`. `"schema"` is always listed, since every run computes it, and the
packs follow that fixed order. An empty `quality_dimensions` selection drops
`"quality"`, as the run then computes no quality.

The packs move a score on unchanged data. Without `"patterns"` no column has
detected patterns, so validity is not assessed and the overall score averages
the other dimensions. `compare()` lists the packs under `configuration`. The
baseline gate reads them for the validity score, and for the overall score
when validity is assessed on one side only: when both sides record their
packs and only one ran `"patterns"`, the rule is `setting_changed` on
`metric_packs`.

The field is absent from every report written before 0.14, whose packs are
unknown rather than all four. An explicit `null` and an unknown pack name are
malformed. Stored v1 reports remain valid.

## v1 additive change: `analysis_settings.temporal_pairs`

`analysis_settings` gains an optional `temporal_pairs` list (#1029): the
start/end column pairs the run declared through `temporal_pairs`, each as
`{"start": "...", "end": "..."}`. Timeliness counts a row whose start date
falls after its end date, so pairing the same columns differently moves the
score on unchanged data. The `temporal_pair` hint bindings already named the
columns; this records which column pairs with which.

The writer lists each pair once, sorted by start then end, since the
calculator compares a repeated pair once and in any order. Direction is kept:
a reversed pair counts the opposite rows. The list is empty when no pairs were
declared.

`compare()` lists the pairs under `configuration`, comparing them as a set of
directed pairs. The baseline gate compares them for the timeliness and the
overall score when both sides record them. Against a side that does not, it
keeps the earlier rule: two empty pair-column lists agree, and anything else
is `setting_unrecorded` on `temporal_pairs`.

The field is absent from every report written before 0.14, whose pairs are
unknown. An explicit `null` or a pair without a string `start` and `end` is
malformed. Stored v1 reports remain valid.

## v1 additive change: `quality.columns_without_value_checks`

The quality block gains an optional `columns_without_value_checks` list in both
dialects (#1134): the columns whose values no value-level check read, each as
`{"column": "...", "reason": "..."}`, in column order. The one reason is
`"nested"`: a struct, list or map column is counted and nothing more (#637).
Its nulls count toward completeness and its values toward duplicate rows, but
consistency, validity, accuracy, precision, timeliness and key uniqueness
never read them, so those dimensions say nothing about their values. Before
this, a source whose payload sat in such columns scored 100 with nothing in the
report saying so.

The list is empty when every column was checked. Every engine and input path
records the same list for the same columns. The text and HTML renderings in
Python (`repr()`, `to_markdown()`, `to_html()`, `to_llm_context()`) print a
note next to the score when it is not empty.

The field is absent from every report written before 0.14, which did not
record it. An explicit `null` and an unknown reason are malformed. Stored v1
reports remain valid.
