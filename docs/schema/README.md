# Profile report JSON Schema

The versioned
[`profile-report.v1.schema.json`](profile-report.v1.schema.json) document is the
machine-readable contract for serialized `ProfileReport` values. It uses JSON
Schema 2020-12, carries a stable project-owned `$id`, and keeps every `$ref`
self-contained under `$defs`.

Schema versions are independent of package versions. The filename and `$id`
must use the same version as Rust's `REPORT_SCHEMA_VERSION` and Python's
`dataprof.REPORT_SCHEMA_VERSION`.

Version 1 covers both existing serialization dialects:

- Rust's complete runtime document (`data_source`, `column_profiles`, and the
  confidence-wrapped quality assessment).
- Python's high-level export document (`source`, `source_type`, `columns`, and
  its flattened quality summary).

Both dialects accept unknown additive object properties. This matches the v1
reader policy and lets compatible fields be added without invalidating stored
reports. Required fields, schema version, known enum values, and primitive
types remain enforced.

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
The Python API writes the `PythonProfileReportDocument` shape, so the examples
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
document_schema["$ref"] = "#/$defs/PythonProfileReportDocument"
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
          document_schema["$ref"] = "#/$defs/PythonProfileReportDocument"
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
