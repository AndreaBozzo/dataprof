# dataprof 0.12.0 · Profile, then gate

<!-- release-body:start -->

0.12.0 turns dataprof from something you run once on unfamiliar data into a
checkpoint that stays in the pipeline. A report can now say what deserves
attention (`findings()`), and a declared policy turns it into a verdict
(`check()`, `QualityPolicy`, and `python -m dataprof.check` for CI). The verdict
has three values, and `inconclusive` is a real answer: a sampled or partial scan
never passes a claim about the whole source. On a large file read in full,
sampled quality scores now carry an interval, and the gate decides on it.

A gate is only as good as the numbers under it, so most of this release is
about the numbers. Of more than a hundred commits, 48 are fixes, many found by profiling real
files and checking the output against something independent: a CSV of the same
values, the previous release, or the planted truth. Several were silent and
plausible, which is the worst failure for a profiler: polars and Arrow-backed
pandas frames profiled from their first chunk only, Parquet decimal columns
reported 10^scale times too large, a small spread on a large offset reported
the wrong standard deviation, and a US date column was read day-first value by
value. Each is fixed, and the same data now gives the same serialized numbers on
every engine and input path.

Numbers move because of that, and some APIs changed shape. The upgrade
checklist below says where, and what to re-baseline.

## Install

```bash
# Python
pip install --upgrade dataprof==0.12.0

# Rust
cargo add dataprof@0.12.0
```

Wheels cover standard CPython 3.10 to 3.14 on Linux, macOS and Windows, with no
Python dependencies. Rust 1.96 remains the minimum supported version. dataprof
ships libraries and Python packages; there is no CLI binary.

## Highlights

- **A quality gate, in Rust, Python and CI** (#725, #732). State a policy as
  data and get `pass`, `fail` or `inconclusive`, with the evidence behind each
  check. `python -m dataprof.check data.csv --min-quality 90` exits 0, 1 or 2.
- **Findings** (#770). `report.findings()` lists what deserves attention, each
  with a stable code, a severity and its evidence, never a raw value, and lists
  the rules it could not evaluate instead of reading them as clean.
- **Gates that decide on large files** (#789, #820). When quality was computed
  over the retained sample, the report carries a 99.9% interval per score, and
  the gate passes or fails on it. Past a million distinct rows, uniqueness is
  bounded with certainty from what the exact counts held.
- **Column projection** (#655). Profile only the columns you name, on every
  input path; Parquet skips the unselected column chunks entirely.
- **Arrow C Stream inputs** (#706). `profile()` accepts PyArrow
  `RecordBatchReader` objects and DuckDB relations without collecting a table.
- **Reports that record how they were made.** `quality_status` says why a
  report has no quality assessment (#722), `recovery_events` records engine
  fallback (#735), `metric_semantics` records the definitions in force (#765),
  and JSON saves carry the complete report in both languages (#759).
- **Honest about what it cannot read.** Numbers written as `1.234,56` or
  `10,50` are counted and reported (#807), a CSV that ends inside an open quote
  is reported (#791), and nested columns report their counts instead of
  statistics about a display string (#769).

## Upgrade checklist

| If you rely on… | What changed | What to do |
| --- | --- | --- |
| flat Python quality accessors (`quality.missing_values_ratio`, …) | The 16 accessors deprecated in 0.9 are removed (#733). Reading one raises `AttributeError` naming its nested replacement. | Read the nested dimension, after checking it is not `None`: `quality.completeness["missing_values_ratio"]`. |
| the layout of `to_json()` / JSON `save()` | Both write the complete canonical report, the same document Rust serializes: `data_source`, `column_profiles`, quality under `quality.metrics` (#759). `to_dict()` keeps the flat summary. | Parse `to_dict()` if you consumed the flat layout. Loaders accept both layouts. |
| `overall_quality_score()` / `overall_score` | A report where nothing was assessed returns `None` (serialized `null`), not `0.0` (#629). Rust `overall_score()` and `QualityAssessment::score()` return `Option<f64>`; `MetricConfidence` gains `NotAssessed`. | Decide what an unassessable report means for you; branch on `assessed_dimensions()`. |
| dimension evidence dicts (`quality.validity`, …) | A dimension that assessed nothing returns `None` and is omitted from serialized output, instead of ratios computed from zero inputs (#640). | Check for `None` before reading keys. |
| `variance`, `std_dev`, `quartiles.iqr` | A spread that overflows `f64` is `null`, not `0.0` or `inf` (#800, #803). Rust fields are `Option<f64>`. | Treat `null` as "too large to represent", not "no spread". |
| text lengths | `min_length`, `max_length`, `avg_length` count Unicode scalar values, not UTF-8 bytes (#641): `"東京"` is 2, not 6. ASCII is unchanged. Saved reports record this as `metric_semantics.text_length_unit` (#765). | Re-profile non-ASCII text rather than comparing across the 0.11/0.12 boundary. |
| struct, list and map columns | Typed `nested` (`DataType::Nested`) with counts only; no length statistics, distinct count or patterns (#769). | Add a `Nested` arm to exhaustive matches; compare nested columns within a release. |
| text `most_frequent` / `least_frequent` (Rust) | Newly computed profiles leave them `None` on every path (#758). | Compute them explicitly with `dataprof_metrics::stats::text::{calculate_most_frequent, calculate_least_frequent}`. |
| Rust struct literals | `QualityAssessment` is built with `new`, `exact` or `approximate` (#759). Public structs gain public fields: `ProfileReport` gains `quality_status` (#722) and `metric_semantics` (#765); `ExecutionMetadata` gains `recovery_events` (#735), `sampled_row_ranges` (#721) and `unterminated_quote` (#791); `ColumnProfile` gains `locale_number_count` (#807) and `unique_count_lower_bound` (#820); `DateTimeStats` gains `slash_date_order` (#817); `RowDuplicateSummary` gains `max_duplicate_rows` (#820). Lower-level `BifurcatedResult` and `ColumnProfileInput` gain `score_bounds` and `unique_count_lower_bound`. | Use the constructors (`ProfileReport::new`, `ExecutionMetadata::new`), or add the new fields to literals. |
| `RobustCsvParser::parse_csv()` / `parse_csv_with_recovery()` (Rust) | Return `CsvParseOutput { headers, records, recovery_events }` instead of a tuple (#735). | Read the named fields. |
| Python interpreters | Wheels are published for standard, GIL-enabled CPython 3.10 to 3.14 only (#707). PyPy, preview and free-threaded wheels are no longer built. | Use a supported CPython. |
| quoted policy thresholds | `check()` and `findings()` reject strings and booleans as thresholds; a quoted number in a policy file is an input error (#772). | Write thresholds as numbers. |
| hand-edited flat documents | `from_dict()` rejects a malformed `quality.score_weights` instead of substituting defaults (#764). | Fix the document. |

## Numbers that move

Re-baseline stored reports and gates that read these.

- **Rows and everything after them, on chunked DataFrames** (#662). Multi-chunk
  polars frames and Arrow-backed pandas frames profiled their first chunk only.
- **Parquet and Arrow decimals** (#828): statistics were `10^scale` times too
  large, and `decimal256` columns had no statistics and a fabricated distinct
  count.
- **Variance and standard deviation** (#677, #806): values on a large offset
  (IDs, epoch timestamps, `1e12 + x`) lost precision; the mean of values whose
  sum overflows is now reported (#804).
- **Distinct counts** are exact up to a million values on every engine, and an
  estimate never exceeds the values seen (#786, #779). Duplicate rows are exact
  up to a million distinct rows, including JSON records whose keys appear late
  (#658).
- **Timeliness**: start/end pairs are compared only within the same row (#792),
  and a US date column is read month-first as a whole (#817). Offsets in RFC 3339
  timestamps normalize to UTC (#660), and timestamps in named time zones profile
  instead of failing (#669).
- **Patterns**: ten-digit IDs and epoch seconds are no longer reported as US
  phone numbers or flagged sensitive (#818).
- **Whitespace-padded numbers** enter numeric statistics (#628); columns with no
  parsed value report no statistics rather than zeros (#682).
- **Capped Parquet profiles** (`max_rows`) sample 32 ranges across the file
  instead of its prefix (#721), recorded in `execution.sampled_row_ranges`.
- **Empty inputs** report an empty quality assessment on every path (#724).
- **In-memory profiles** of the same data are deterministic (#774).
- **Parquet schema inference** (`infer_schema()`, `analyze_structure()`) types
  text columns from their values, as `profile()` does (#699).

## Known limitations

Shipping with these; each has an issue.

- **Common null markers are values** (#813). Only empty, `null` and `nan` are
  null, so `NA`, `N/A`, `#N/A`, `\N` and `None` count as values and completeness
  can read 100% on data that is mostly missing.
- **Zero-padded codes are numbers** (#814). `00123` and `20240115` are typed
  `integer` and averaged.
- **Locale-formatted numbers are reported, not parsed** (#433). `locale=` still
  tunes pattern detection only. `10,50` can also match the `Geographic
  Coordinates` pattern (#808), and amounts with a currency or percent sign are
  not counted (#809).
- **Very large sources can stay inconclusive** (#830). The certain uniqueness
  bound widens with row count; past about ten million rows a high
  `min_quality_score` may not be decidable.
- **Sensitive-data coverage is narrow** (#790): two national IDs and two phone
  formats. International and E.164 phone numbers are not detected.
- **`--baseline` is not supported yet** (#749); the flag exits 2.
- **Database connectors need a source build** (#588); they are not in the wheel.
- **Durations** report statistics in their storage unit, which the report does
  not name (#823).

<!-- release-body:end -->

## Detailed changes

The generated [changelog](CHANGELOG.md) lists every commit. This section groups
the user-visible ones.

### Quality gate and findings

- `ProfileReport.check()` (Python) and `QualityPolicy` (Rust) evaluate minimum
  quality and dimension scores, per-column null limits, a duplicate-row limit
  and required metrics, with `scope="full_source"` (default) or `"observed"`.
  Each check records what it expected, what it observed, the evidence and why it
  could not be evaluated when it could not (#725).
- `python -m dataprof.check` wraps the gate for CI: JSON policy files and
  threshold flags, `--json` to stdout, human summaries to stderr, exit codes
  0/1/2 (#732). See the
  [CI entrypoint guide](python/README.md#python--m-dataprofcheck----ci-entrypoint-012).
- `findings()` reports `all_null`, `null_heavy`, `mixed_types`,
  `duplicate_rows`, `future_dates`, `temporal_order_violations`, `ragged_rows`,
  `unterminated_quote`, `records_skipped`, `locale_numbers`, `constant_column`,
  `sensitive_pattern` and `partial_scan`, identically in Rust and Python (#770,
  #791, #807).
- `quality.score_bounds` / `report.quality_score_bounds`: a 99.9% interval per
  sampled score, and the gate's decision rule on it (#789). Uniqueness gets a
  certain interval past a million distinct rows (#820).

### Correctness

- One stable numeric accumulator for every path: Welford on shifted values and a
  compensated sum (#677, #806); overflow reported as `null` (#800, #803); the
  mean of an overflowing sum (#804).
- Exact distinct counts to a million on every engine, using 64-bit fingerprints;
  exact sets spill under memory pressure on the incremental, columnar and Arrow
  batch paths, and the columnar engine honours `memory_limit_mb` (#786, #792).
- Accumulator merge produces the single-pass profile, including one-sided
  columns, completeness and reservoir weighting (#650).
- Parquet columns are profiled by value, not physical encoding (dictionary,
  run-end and view types), and an unreadable column is an error rather than a
  fabricated full-cardinality profile (#661).
- Memory-mapped CSV chunks end on record boundaries; headerless configuration is
  honoured by both CSV engines (#628). CSV decoding stops at the row cap (#755),
  and `max_bytes` stops at the first record past the budget (#776).
- Slash dates are read in one day/month order per column, recorded as
  `DateTimeStats::slash_date_order` (#817).

### Python

- Every chunk of a pandas or polars DataFrame is profiled (#662); zero-row Arrow
  sources profile over their declared columns (#666).
- Arrow C Stream producers, including DuckDB relations, are accepted (#706).
- Native and restored reports share one set of read-only views (#746), and the
  bindings are split into private modules behind a small facade (#705).
- The async database helpers are coroutine functions (#778); engine aliases are
  documented (#680); progress callbacks hear from every file route (#777).

### Removed flat quality accessors

The 16 flat `DataQualityMetrics` accessors deprecated in 0.9 are gone (#733).
Reading one raises `AttributeError` naming its replacement; the names no longer
appear in `dir()` or the type stubs. Read the nested dimension only after
checking it is not `None`, since an unassessed dimension must not become a
fabricated zero or perfect score:

```python
quality = report.quality
completeness = quality.completeness if quality is not None else None
missing_ratio = completeness["missing_values_ratio"] if completeness is not None else None
```

| Dimension | Removed flat names (replacement keys) |
| --- | --- |
| `completeness` | `missing_values_ratio`, `complete_records_ratio`, `null_columns` |
| `consistency` | `data_type_consistency`, `format_violations`, `encoding_issues` |
| `uniqueness` | `duplicate_rows`, `key_uniqueness`, `high_cardinality_warning` |
| `accuracy` | `outlier_ratio`, `range_violations`, `negative_values_in_positive` |
| `timeliness` | `future_dates_count`, `stale_data_ratio`, `temporal_violations`, `invalid_date_values` |

Nested values, report serialization and the schema version are unchanged, and
saved reports still load through the nested API.

### Reports and schema

- JSON saves write the canonical report; `to_dict()` stays the flat summary
  (#759). The report schema stays v1: every new field is optional.
- New fields: `quality_status` (#722), `execution.recovery_events` (#735),
  `metric_semantics` (#765), `execution.sampled_row_ranges` (#721),
  `execution.unterminated_quote` (#791), `quality.score_bounds` (#789),
  `locale_number_count` (#807), `DateTimeStats::slash_date_order` (#817), and the
  `nested` data type (#769). See the [schema guide](schema/README.md).
- The cross-engine numeric equality contract is scoped to serialized, rounded
  metrics and enforced by comparing complete serialized column documents (#708).

### Structure, Parquet and databases

- `analyze_structure()` reports whole-file counters for Parquet from the footer
  (#702), types text columns from their values (#699), and refuses duplicate
  column names (#701).
- Database connectors decode temporal, decimal, UUID and unsigned columns
  (#642) and MySQL `TIME` as a time of day (#646).
- `AgentGuard.llm_context()` redacts host paths (#703).

### Performance and benchmarks

- Peak-memory sampling is throttled, so small chunk sizes no longer pay a
  process-table walk per chunk (#775); the columnar engine reads its ragged-row
  count from arrow-csv instead of pre-scanning (#754).
- The benchmark suites share one home, one CI artifact and one page, with a
  locked four-tool comparison (dataprof, pandas, polars, ydata-profiling),
  energy and peak-memory collection, and Python/Arrow boundary costs (#736,
  #747, #748, #750). The page reports what each run measured and does not claim
  a general ranking.
