# dataprof 0.13.1 · Same bytes, same answer

<!-- release-body:start -->

0.13.0 shipped with a confession in its known limitations: some JSON bytes behaved differently from the same bytes in a file. Three days later that list is shorter. This is a patch release: nothing removed, one rule added, and the wheels rebuilt on patched zstd.

The 0.13 audits profiled the same bytes through every route and compared the answers. The routes that still disagreed were on the synchronous bytes path, which decodes JSON with Python's `json` module, while files, async reads and streams decode in Rust. Each fix here teaches the Python side a rule the Rust side already applied.

## What changed

- **Bytes read like files.** On the synchronous bytes route, a JSON number that overflows a float (`1e400`) is a decode error instead of an infinity (#1144), and a lone surrogate escape (`"\ud800"`) is a decode error instead of a `UnicodeEncodeError` crash under either error policy (#1145). JSONL bytes split records at `\n` only, so a U+2028 inside a string no longer cuts a record in two, and two records joined by CR, NEL, LS or PS on one line are refused, as the file reader refuses them (#1147).
- **Your Arrow stream no longer waits on itself.** `profile()` hung on an Arrow stream whose producer needs the GIL on another thread, such as a PyArrow scanner over a filesystem written in Python. The stream callbacks now run with the GIL released (#1139).
- **Tiny values keep their spread.** Near 1e-200 the squared deviations underflowed, and a column reported a variance of 0.0 with the CV and kurtosis of a constant. The accumulator now holds them at a raised scale; a spread that already fitted reports the same bits as before (#1151).
- **Exact no-null rules.** `check(max_null_count={"customer_id": 0})`, `--max-null-count customer_id=0` and `QualityPolicy::max_null_count` decide on the exact count. `max_null_percentage=0` decides on the two-decimal value, so one null in 20,001 rows passes it (#1141).
- **Summaries and errors say more.** `python -m dataprof.check --baseline` lists the baseline's schema-change diagnostics in its text summary, not only under `--json` (#1129). A polars `LazyFrame` or a PyArrow `Scanner` passed to `profile()` gets an error naming the call that makes it readable, `.collect()` or `.to_reader()` (#1136).

The wheels and the locked Rust build carry zstd past RUSTSEC-2026-0391, 0398, 0403 and 0410 (#1131). Rust projects pick the patched crates up with `cargo update`.

## Install

```bash
# Python
pip install --upgrade dataprof==0.13.1

# Rust
cargo add dataprof@0.13.1
```

Wheels cover standard CPython 3.11 to 3.15 on Linux, macOS and Windows, as for 0.13.0. Rust 1.96 remains the minimum supported version.

## Numbers that move

- **JSON bytes with an overflowing number or a lone surrogate escape**: under `strict` they raise; under `skip` a JSONL record is dropped and counted in `error_count`, and a JSON document is refused whole. Before, the number profiled as an invalid float and the surrogate crashed the run.
- **JSONL bytes holding CR, VT, FF, U+001C to U+001E, NEL, LS or PS**: record and error counts now match the file reader.
- **Numeric columns of very small values**, from about 1e-160 down: variance, CV, skewness and kurtosis were zero or drifted, and now match the same values at unit scale. Other columns report the same numbers as 0.13.0.

For a no-null rule past 20,000 rows, move from `max_null_percentage=0` to `max_null_count=0`.

## Still known

The 0.13.0 limitations below still hold, except the JSON bytes differences (#1086, #1087) and the spread of underflowing values, fixed here. Decimals whose fraction lies below f64 spacing at a large magnitude still report a variance of zero (#1149).

<!-- release-body:end -->

---

## 0.13.0 · Against yesterday

0.12 put a gate in the pipeline. 0.13 gives it a memory. A checkpoint can now compare today's batch with the report an earlier run saved: the overall score may fall by so many points, a column's nulls may rise by so many, no column may disappear and none may appear unannounced. `python -m dataprof.check --baseline yesterday.json` turns that into an exit code, and `--report` saves the profile that the next run compares against.

Comparing two reports is only fair when both were made the same way, so every report now records how it was made: the dataprof build that wrote it, the score weights, the column selection, the null vocabulary, the pattern locale and the definitions behind each score. When a baseline was scored under a different definition, the gate says `definition_not_comparable` instead of reading the difference as a change in the data.

A gate that compares two numbers inherits every error in both, so most of the cycle went into the numbers again: 75 of the 133 commits are fixes. Null markers like `NA` and `#N/A` now count as nulls, zero-padded codes stay text, and key uniqueness and accuracy score what their names say. Before the tag, the release candidate went through five rounds of external audit. Each round profiled the same bytes through every route and checked the answers against each other and against independently computed values. The last high finding was a zero with a sign on it: `-0` in a JSON file was a float on one route and an integer on another, so a duplicate check passed or failed depending on how the bytes arrived.

Sixteen people besides the maintainer contributed code, tests or docs to this release. Thank you.

Numbers move, and some APIs changed shape. The upgrade checklist below says where, and what to re-baseline.

### Install

```bash
# Python
pip install --upgrade dataprof==0.13.0

# Rust
cargo add dataprof@0.13.0
```

Wheels cover standard CPython 3.11 to 3.15 on Linux, macOS and Windows, with no Python dependencies. Python 3.10 is no longer supported, now that it reached end of life. Rust 1.96 remains the minimum supported version. dataprof ships libraries and Python packages; there is no CLI binary.

### Highlights

- **A gate that remembers yesterday** (#1008, #1016, #1023, #1057). `check(baseline=...)` and `QualityPolicy::evaluate_against` add rules about change: `max_quality_score_drop`, `max_dimension_score_drops`, `max_null_percentage_increase`, `require_columns`, `forbid_removed_columns` and `forbid_added_columns`. Schema changes (a column added, removed, retyped or gone all-null) are listed as diagnostics whatever the policy says. On the command line: `--baseline`, `--max-quality-drop`, `--max-null-increase`, `--forbid-removed-columns`, and `--report` to save the next baseline (#870).
- **Uniqueness and accuracy that score what they claim.** Key uniqueness measures a declared key, the first of `identifier_columns`, over its non-null values, instead of whichever column first had `id` in its name (#1049, #1069). Accuracy counts a range or sign violation once, over the columns the rule applies to (#1048), applies range rules only where the name pins the unit (#881, #948), and leaves an outlier test that never ran out of the score (#1051).
- **Decoding that refuses instead of guessing.** JSON integers outside 64 bits are a decode error rather than a rounded float that merges distinct IDs (#1105). Database columns of a type no decoder reads fail the query instead of reading as all null, and PostgreSQL enums decode (#1059). Gzip, zstd, zip and Parquet files read as text are named, not parsed (#901). Remote Parquet is pinned to one object version by ETag (#947).
- **Identity in every report.** A top-level `generator` names the dataprof version, commit and dirty state that wrote it (#1064). Score weights are written even when they are the defaults (#1002), and the column selection (#1019), the pattern locale (#1011) and the null vocabulary (#847) are recorded beside `metric_semantics`.
- **Typed comparison and a row floor.** `compare()` is computed by a Rust `ReportDelta` with a stated contract: changes are `b - a`, inputs are taken at their serialized precision, and a number either side lacks leaves the change `None` (#981). `check(min_rows=N)` fails a truncated batch that every other rule would pass (#959).

### Upgrade checklist

| If you rely on… | What changed | What to do |
| --- | --- | --- |
| Python 3.10 | Dropped (#1056). `requires-python` is `>=3.11,<3.16`, so pip on 3.10 resolves to 0.12.0. | Move to CPython 3.11 or later. |
| exit code 2 from `python -m dataprof.check` | Argument, policy and input errors exit 3; 2 now means inconclusive only. With `--json`, an error is a JSON object on stdout (#843). | Treat 3 as "the check could not run". |
| `check()` on a report loaded from a flat `to_dict()` summary | `check()` evaluates through the Rust gate and refuses a flat summary with a `ValueError` (#910). `compare()` still accepts one. | Gate on a report from `profile()`, or one loaded from `save()` / `to_json()`. |
| `max_null_percentage=0` or `min_quality_score=100` | Absolute rules decide on the two-decimal value a check shows (#1090). One null in a million rounds to `0.0` and passes a 0% rule. | For an exact no-null rule, read `null_count == 0`. |
| completeness on files that write `NA`, `N/A`, `#N/A`, `\N` or `None` | These count as nulls on every route (#847). Matching is exact: `Na` and `none` stay values. | Re-baseline completeness; `compare()` across the change reports `comparable: None`. |
| zero-padded codes and `YYYYMMDD` dates | A column holding `08001`-style codes, or only real `YYYYMMDD` dates, is typed `string`, not `integer` (#868). | Re-baseline type and statistics for those columns. |
| the uniqueness score | Without `identifier_columns`, key uniqueness is not scored (#1069). | Declare the key: `identifier_columns=["order_id", ...]`. |
| pattern names and regexes | `Date (EU slash)` is `Date (slash)`; the `UUID`, `IBAN`, `Phone (IT)`, `Geographic Coordinates` and `DateTime (ISO)` regexes changed, and validated patterns drop matches that fail validation (#924). | Match patterns by the new names; re-baseline validity. |
| skewness, kurtosis and CV | Adjusted sample estimators (the pandas convention), with three and four values as the minimum; CV keeps its percentage scale (#922). | Re-baseline shape statistics. |
| JSON integers beyond 64 bits | Refused as a decode error: strict raises, skip drops and counts the record (#1105). PostgreSQL `json` columns keep the digits. | Write such integers as strings. |
| PostgreSQL `interval`, `inet`, `money`, ranges, geometric types and others; MySQL `GEOMETRY` | A non-null value fails the query with an error naming the column (#1059). Binary columns still read as null. | Cast to text in the query, or leave the column out. |
| synchronous CSV bytes | The delimiter is detected by the Rust detector, as for files, instead of Python's `csv.Sniffer` (#1099). Detection judges candidates by the header (#1093). | Pass `csv_delimiter` where a file is ambiguous. |
| `quick_quality_check()` (Rust) | Returns `Result<Option<f64>, _>`: `None` when nothing was assessable (#899). | Handle `None` rather than reading a 0.0 score. |
| quality-gate types (Rust) | All 15 public types are `#[non_exhaustive]` (#900). | Match with a wildcard arm; build policies with the constructors. |
| `JsonRecord` (Rust) | Gains the `WideInteger` variant (#1105). | Add an arm to exhaustive matches. |
| removed legacy surface (Rust) | `RobustCsvParser`, `DataprofConfig` and its builder, `InputValidator`, `MemoryTracker`, the unused reservoir samplers, seven never-constructed `DataProfilerError` variants, and `dataprof-db` sampling and retry, which never worked (#919). `patterns::looks_like_date` is gone; call `is_inferred_date_token` (#930). `StreamReservoirSampler::offer` takes `&str` (#924). | Use `ProfilerConfig` and the profiler entry points. |

### Numbers that move

Re-baseline stored reports and gates that read these.

- **Completeness**: common null markers are nulls (#847), and a column with no non-null value reports no statistics (#985).
- **Types**: zero-padded codes and `YYYYMMDD` dates are text (#868); binary columns count distinct values by their bytes and report no length statistics or patterns (#853, #855).
- **Uniqueness**: key uniqueness only for a declared key, over its non-null values (#1049, #1069). Duplicate-row signatures compare nested values by value (#944), Arrow maps by their entries (#973), and tell a JSON string from a container that spells it (#977).
- **Accuracy**: range rules by name and unit (#881, #948), one violation per value over ruled columns (#1048), no score from an outlier test that did not run (#1051).
- **Timeliness**: date columns pair by the words of their names (#914, #940, #956), or explicitly through `temporal_pairs` (#968); mixed date formats are counted over every value (#916).
- **Statistics**: adjusted skewness and kurtosis (#922); the spread of 64-bit integers past 2^53 from their exact values (#957); a variance that fits when its sum of squares overflows (#958).
- **Patterns**: rebuilt detection with validators (#924); decimals no longer match `Phone (US)` (#931).
- **CSV delimiters**: detection reads the header, and synchronous bytes use the same detector as files (#1093, #1099). A file detection used to split wrongly now profiles its real columns.

### Known limitations

Shipping with these; each has an issue.

- **Spread can collapse to zero at the edges of `f64`**: values small enough to underflow, and decimals whose fraction lies below f64 spacing at a large magnitude, report a variance of zero (#1088).
- **The null vocabulary is fixed** (#846). There is no `null_values=` option to add `-`, `n.d.` or a source's own marker.
- **Some JSON bytes behave differently from files**: a number that overflows a float becomes infinity on synchronous bytes (#1086), and a Unicode line separator inside a JSONL string splits the record there (#1087).
- **Delimiter detection gives no signal when two candidates read a file equally well** (#1100), ignores a configured `quote_char` (#1094), and misjudges a header longer than its 4 KB sample (#1098).
- **64-bit integers past 2^53** still take extrema, mode and quartiles through `f64` (#1038).
- **Database connectors need a source build** (#588), and the PostgreSQL types they now refuse are not decoded yet (#1060).
- **Durations** report statistics in their storage unit, which the report does not name (#823).
- **Very large sources can stay inconclusive** (#830): past about ten million rows a high `min_quality_score` may not be decidable.

### Detailed changes

The generated [changelog](CHANGELOG.md) lists every commit. This section groups the user-visible ones.

#### Baseline gate and CI

- `QualityPolicy::evaluate_against` and `ProfileReport.check(baseline=...)`: score-drop and null-increase rules (#1008), schema-change diagnostics (`column_added`, `column_removed`, `column_type_changed`, `column_all_null`, #1016), and the column-set rules `require_columns`, `forbid_removed_columns` and `forbid_added_columns` (#1023). A baseline whose definitions differ reads `definition_not_comparable`.
- A closed schema is not passed by a report that read only part of its source (#1037). A score-drop rule reads point scores, so a quality-sampled score on either side leaves it inconclusive for now (#1035).
- `python -m dataprof.check`: `--baseline` with every change rule as a flag or policy key (#1057), `--report` to save the checked profile (#870), `--min-rows` (#959), `--require-column`, `--version` with the build (#856, #1064), and error exit code 3 with JSON errors under `--json` (#843). Policy errors name the file they come from, also for non-UTF-8 files (#863, #954, #955).
- `python/examples/baseline_checkpoint.py` promotes a new baseline only on a pass (#711, #1082).

#### Reports and comparison

- `generator` in every report, and `commit` / `dirty` in `capabilities()` (#1064).
- Recorded settings: score weights (#1002), column selection (#1019), pattern locale (#1011), null vocabulary (#847), and the score definitions the gate compares (#1048, #1049, #1051, #1069).
- `compare()` from a typed Rust `ReportDelta`, with provenance for both sides; flat summaries still compare (#981).
- Read-only report `id` and `timestamp` in Python (#1014); saving is atomic (#978); `from_dict()` refuses malformed flat fields, `quality` and `type_homogeneity` instead of defaulting them (#951, #980, #1013).

#### Correctness

- JSON: integers beyond 64 bits refused (#1105); an array read that stopped at a fault reports the source as not exhausted (#1107); `-0` reads as the integer 0 on every route, and async reads stop at the row cap (#1109); keys first seen after the first chunk are profiled and announced to progress consumers on the async path (#1022, #1046); leading fieldless records are counted (#1053).
- CSV: a UTF-8 BOM before a blank line no longer loses the header in small chunks (#1105); quoted empty trailing fields, trailing delimiters and CR-only line ends are read alike by detection and the readers (#1099, #1105); invalid UTF-8 is refused on the columnar engine as on every other route (#965); a file of only blank lines profiles as empty (#971).
- Row counts, composed stop conditions and statistical estimators corrected across engines (#922); capped chunk rows are counted in the finished progress event (#967); the terminal progress event always has room in the channel (#975).
- Parquet: Float32 samples come from the promoted value (#1045); remote reads are pinned by ETag (#947) and a server that ignores `Range` gets advice to download the file (#879).

#### Python

- CPython 3.11 to 3.15 (#1056).
- pandas object columns that mix types profile without Arrow errors and keep container identity (#979, #987).
- Errors are classified: remote transport failures and database connection failures raise `OSError`, a malformed connection string `ValueError`, and Arrow producer messages reach the top-level error (#988, #989, #999, #1043, #1078).
- `quick_quality_check` returns `None` when nothing was assessable (#899).

#### Databases

- PostgreSQL enums, JSON, JSONB and arrays decode (#953, #1059); undecodable values fail the query with the column named (#1059), including after NULL batches on the streaming path (#1084).
- A profiled query is fetched in one pass instead of `LIMIT`/`OFFSET` pages (#895).
- SQLite keeps its URI options and refuses URLs with credentials or a port (#990, #1040).

#### Testing

- An oracle suite checks every input route against values computed without dataprof, in Python and Rust (#829, #984, #1054).
- A fuzz target compares the CSV engines on the same bytes (#966), which found undefined behaviour in arrow-csv 60.0.0's UTF-8 handling, fixed here by #965 and reported upstream.
- The release wheel and sdist build is rehearsed in CI without publishing (#1079).
