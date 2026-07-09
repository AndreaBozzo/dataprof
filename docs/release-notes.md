# DataProf 0.9.0 — Release Notes

0.9.0 focuses on the Python surface: a report object that behaves like a report
object, a first-class path for handing profiles to LLM agents, and a correctness
fix in numeric statistics that will change numbers you have seen before.

Read [Numeric statistics on nullable columns](#numeric-statistics-on-nullable-columns)
first. It is the only change in this release that can silently alter results you
have already recorded.

## Upgrading

```bash
pip install --upgrade dataprof
```

Requires Python 3.10 or newer. Rust users need toolchain 1.96 or newer.

---

## Numeric statistics on nullable columns

**Any nullable numeric column read through Parquet, pandas, polars, or Arrow had
incorrect statistics before 0.9.0.**

Ten numeric array handlers guarded their null check with
`if let Some(value) = array.value(index).into()`. Because `impl<T> From<T> for
Option<T>` exists, that arm is always taken — the guard never rejected anything.
The physical value stored underneath a null slot (`0.0`) was folded into the
column's `min`, `mean`, `std_dev`, and `unique_count` as though it were real data.

For the column `[100.0, null, 1.0, null, 2.0]`:

| Statistic | 0.8.x (wrong) | 0.9.0 (correct) |
| --- | --- | --- |
| `mean` | 20.8 | 34.33 |
| `min` | 0.0 | 1.0 |
| `unique_count` | 4 | 3 |

The string, boolean, and timestamp handlers already used `!array.is_null(index)`
and were never affected. CSV columns parsed as text were not affected.

**What to do:** if you have stored profiles, quality baselines, or drift
thresholds derived from nullable numeric columns in Parquet or DataFrame inputs,
regenerate them. Baselines computed under 0.8.x encode the null-contaminated
values, and comparing a 0.9.0 profile against them will report drift that is not
there. There is no compatibility flag to restore the old behavior; it was a bug.

Fixed in #358.

---

## Breaking changes

### Python 3.10 is the minimum

Python 3.8 and 3.9 are past end-of-life and are no longer supported.
`requires-python` is now `>=3.10`. Wheels are no longer built for 3.9.

If you are pinned to 3.9, stay on `dataprof==0.8.1` until you can upgrade the
interpreter.

### Rust MSRV is 1.96

Declared as `rust-version = "1.96"` across the workspace.

### `panic = "abort"` removed from the release profile

Previously, a panic inside the Rust core aborted the process, taking the Python
interpreter down with it — no traceback, no `except` clause, no chance to
recover. Panics now unwind to the PyO3 boundary and surface as ordinary Python
exceptions.

If you have supervisor logic that treats a hard interpreter crash as the signal
that profiling failed, it will now see a raised exception instead. This is the
outcome you wanted; it just arrives through a different channel.

### Flat `DataQualityMetrics` accessors now warn

The flat accessors are **deprecated, not removed.** They still return the same
values they returned in 0.8.x for the whole 0.9 line. Reading one now emits a
`DeprecationWarning`:

```python
quality = report.quality
quality.missing_values_ratio   # DeprecationWarning; still returns 20.0
```

This only breaks you if you run with `-W error` or
`filterwarnings = ["error"]` in pytest. In that case the read raises instead of
warning, and you need to migrate now rather than later. See
[Migrating off the flat quality accessors](#migrating-off-the-flat-quality-accessors)
below.

Fifteen accessors are affected — the full list, and where each one moved to, is
in [the mapping table below](#migrating-off-the-flat-quality-accessors).

`low_sample_warning` and `overall_quality_score()` are **not** deprecated. They
are report-level signals rather than per-dimension metrics, and they keep working
unchanged.

Deprecated in #326 / #350.

---

## Added

### `to_llm_context()` — token-bounded summaries for agents

```python
report = dp.profile("orders.csv")
print(report.to_llm_context(max_tokens=1000))
```

Renders a structured, agent-oriented summary — shape, provenance caveats,
derived quality flags, schema, and pattern names. Structured signals only, no
prose narration. Tokens are estimated as `ceil(len / 4)`, so there is no
tokenizer dependency and the output is deterministic. Sections are budgeted by
priority: a starved high-priority section suppresses the lower-priority ones
beneath it, so the summary can never show patterns while hiding the quality
flags that give them meaning.

Raw cell values are withheld unless you pass `include_samples=True`. See
[Sample redaction](#sample-redaction-fails-closed) for the conditions under
which samples are withheld anyway.

Added in #331 / #353.

### `analyze_structure()` — cheap structural profiling

```python
structure = dp.analyze_structure("big.parquet", max_rows=10_000)
```

A lightweight pass that answers "what is in this file" without paying for full
quality metrics. Returns a `StructureReport` of `StructureColumnSummary` entries.
Joins `infer_schema()` and `quick_row_count()` as the third member of the partial
analysis surface.

Added in #319 / #348.

### `ProfileReport.load(path)`

```python
report = dp.ProfileReport.load("profile.json")
```

The path-based counterpart to `from_json()` and `from_dict()`, reloading a report
written by `save()`. Passing a `.csv` or `.parquet` path raises a clear
`ValueError` — those exports store column profiles only and cannot rebuild a full
report.

Added in #325 / #347.

### Report ergonomics

`to_html()`, `to_markdown()`, `compare()`, `from_dict()`, and `from_json()`.

`compare(other)` returns a dict of quality, schema, and null deltas between two
reports. Its shape is **provisional** — it will be aligned with the Rust
`QualityDelta` type (#310) in a later release. Do not build a stable contract on
its keys yet.

`from_dict()` and `from_json()` return a full read-only view backed by proxy
classes, so every export method works on a reloaded report.

Added in #316 / #323.

### Ad-hoc inputs without pandas

```python
dp.profile({"amount": [1, 2, None], "region": ["EU", "EU", "US"]})   # columns
dp.profile([{"amount": 1}, {"amount": 2}])                           # rows
dp.profile(csv_bytes, format="csv")                                  # bytes
```

Bytes and `BytesIO` sources require an explicit `format=` (`"csv"`, `"json"`,
`"jsonl"`, or `"parquet"`) — there is no content sniffing.

The README has promised these on the base wheel for some time, but all three were
adapters over `pd.DataFrame(source)` and needed pandas installed. They now go
straight to the Rust core through a new `profile_columns` entry point. The native
path is unconditional rather than a fallback, on the principle that an output
which changes with the installed environment is the same class of bug as a
redaction gate that changes with an unrelated flag.

Two consequences fall out of dropping the pandas round-trip:

- Column order follows the input rather than a `HashMap` iteration order.
- An integer column containing a null stays an integer instead of being widened
  to float.

A dict whose columns have differing lengths now raises `ValueError` instead of
panicking:

```python
dp.profile({"a": [1, 2, 3], "b": [1]})
# ValueError: dict input: columns have differing lengths (a=3, b=1).
```

A list of row-dicts with differing keys is still accepted — the column set is the
union of all keys, and rows missing a key are recorded as null for that column.

Added in #360.

---

## Fixed

### Sample redaction fails closed

`to_llm_context(include_samples=True)` printed raw credit-card numbers whenever
pattern detection had not run. Any `metrics=` selection that omitted the
`"patterns"` pack, and any use of `fast_mode`, silently disabled the redaction
gate.

The cause was representational. `ColumnProfile.patterns` was a `Vec`, so
"detection was skipped" and "detection ran and matched nothing" were the same
empty value, and the PyO3 boundary collapsed both to `None`. The Python gate read
that absence of evidence as evidence of absence.

`patterns` is now `Option<Vec<Pattern>>` end to end. In Python:

- `col.patterns is None` — detection did not run. **Nothing is known about this
  column's sensitivity**, so samples are withheld.
- `col.patterns == []` — detection ran and found nothing. Samples may be shown.
- `col.patterns == [...]` — detection ran and matched. Sensitive matches are
  redacted.

The type annotation was already `list[Pattern] | None` in 0.8.x, so this does not
break type checking. The *meaning* of `None` changed, and it changed in the
direction of withholding more. If you profile with a narrowed `metrics=` selection
and expected samples in your agent context, add the `"patterns"` pack.

Fixed in #355 / #359.

### Other fixes

- `crossbeam-epoch` bumped to 0.9.20 for RUSTSEC-2026-0204 (#346).
- `arrow` and `parquet` bumped from 57.3.0 to 58.x (#324).

---

## Migrating off the flat quality accessors

The nested dimension properties return `None` when a dimension was not computed,
which is the distinction the flat accessors could not express — they returned
`0.0` both for "perfectly complete" and for "completeness was never evaluated."

`quality` is a property, not a method. Note also that these values are
**percentages on a 0–100 scale**, not ratios on 0–1: a column with two nulls out
of ten cells reports `missing_values_ratio == 20.0`. This is unchanged from
0.8.x, but it is easy to reintroduce as a bug while rewriting a threshold.

```python
# 0.8.x — indistinguishable from a dimension that was never computed
if report.quality.missing_values_ratio > 5.0:
    raise QualityGateFailure(...)

# 0.9.0 — absence is explicit
completeness = report.quality.completeness
if completeness is None:
    raise QualityGateFailure("completeness was not evaluated")
if completeness["missing_values_ratio"] > 5.0:
    raise QualityGateFailure(...)
```

The five dimensions are `completeness`, `consistency`, `uniqueness`, `accuracy`,
and `timeliness`. Each is a `dict[str, Any] | None`, keyed by the flat accessor
names in the table below. `overall_quality_score()` is unchanged and is not
deprecated.

Flat accessor → nested location:

| Flat accessor | Dimension | Key |
| --- | --- | --- |
| `missing_values_ratio` | `completeness` | `missing_values_ratio` |
| `complete_records_ratio` | `completeness` | `complete_records_ratio` |
| `null_columns` | `completeness` | `null_columns` |
| `data_type_consistency` | `consistency` | `data_type_consistency` |
| `format_violations` | `consistency` | `format_violations` |
| `encoding_issues` | `consistency` | `encoding_issues` |
| `duplicate_rows` | `uniqueness` | `duplicate_rows` |
| `key_uniqueness` | `uniqueness` | `key_uniqueness` |
| `high_cardinality_warning` | `uniqueness` | `high_cardinality_warning` |
| `outlier_ratio` | `accuracy` | `outlier_ratio` |
| `range_violations` | `accuracy` | `range_violations` |
| `negative_values_in_positive` | `accuracy` | `negative_values_in_positive` |
| `future_dates_count` | `timeliness` | `future_dates_count` |
| `stale_data_ratio` | `timeliness` | `stale_data_ratio` |
| `temporal_violations` | `timeliness` | `temporal_violations` |

To find every call site before upgrading, run your test suite against 0.9.0 with
warnings escalated:

```bash
python -W error::DeprecationWarning -m pytest
```

---

## Acknowledgements

Thanks to @JHON12091986 for taking on #329 and drafting the first version of this
document.
