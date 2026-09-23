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
compatibility loader remains a Python API.

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

Python exposes the same history through `report.recovery_events`. The standalone
Rust `dataprof_csv::RobustCsvParser` does not produce a `ProfileReport`; its
`parse_csv()` and `parse_csv_with_recovery()` methods return `CsvParseOutput`
with `headers`, `records`, and `recovery_events`. This includes the ordinary
strict-to-flexible retry and the outer auto-recovery strategies. Callers building
their own report should transfer this history to its execution metadata.
The existing encoding-recovery placeholder only retries flexible parsing; its
history therefore records `flexible`, without claiming an encoding conversion.

## Numeric equality contract

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
| Statistics and data values, including mean, standard deviation and extrema | 4 decimal places |
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

This decision changes no rounding rule and no schema version.
Rounding alone cannot mathematically guarantee equality for every possible
floating-point input, especially near rounding boundaries or at large
magnitudes. A difference that survives serialization remains a parity defect
to investigate, rather than an allowed raw-precision difference. The known
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
are omitted (#760).

Previously the summary dropped them, so a report assessed with custom weights
came back from `ProfileReport.from_dict(report.to_dict())` with the default
weights next to an `overall_score` those defaults did not produce.

The field is additive and not `required`: stored v1 reports remain valid.
Summaries of reports that use the default weights are unchanged. A summary
written before this change omits the field whatever weights were used, so there
its absence does not show that the defaults applied; the loader still reads an
absent field as the defaults, as it did before. The canonical document from
`to_json()` or JSON `save()` has kept custom weights since #714.

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
