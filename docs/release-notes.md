# dataprof 0.14.1 · Every source, one verdict

<!-- release-body:start -->

0.14 gave the checkpoint rules for the quiet failures. Running them still took a wrapper script: one call per file, a loop to combine the answers, and a special case for the first run, when there is no accepted report to compare against yet. This release moves that script into `python -m dataprof.check`. One command checks a whole delivery, decides one verdict for it, passes a first run on its absolute rules and saves the report the next run compares against.

This is a patch release: nothing is removed, and no common-case number moves. It also ships with a rebuilt GitHub Action, below.

## What changed

- **Gather a whole delivery into one run.** `python -m dataprof.check` takes several files and directories, with `-r` to recurse. Every source is checked against the same policy, and the run gets one verdict: fail if any source failed, otherwise inconclusive if any was, otherwise pass. The same rule is in the library as `dataprof.combine_verdicts()`. An unreadable source is reported without stopping the others and makes the run an error (exit 3); a file whose format cannot be inferred from its extension is listed as skipped. With `--json`, the run prints one document with each source's result (#836).
- **A first run that needs no yesterday.** `--allow-missing-baseline` lets the `--baseline` file be absent: the rules about change are recorded as `not_applicable`, the absolute rules decide, and the result says `"baseline_missing": true`. `--promote PATH` saves the report only on a pass, with the same atomic write as `save()`, so a failed batch never becomes the next baseline; the path may be the baseline itself. Both are policy keys too, and `ProfileReport.check(allow_missing_baseline=True)` gives the library the same decision (#1081).
- **Tight null drift, on exact counts.** `max_null_count_increase` limits how many nulls a column may gain over the baseline, decided on both reports' exact null counts. `max_null_percentage_increase` compares percentages at two decimals, so from about 30,000 rows one new null rounds to a change of 0.00 and passes; an allowance of 0 nulls now fails it at any size. In Rust as `QualityPolicy::max_null_count_increase`, in Python as `check(max_null_count_increase=...)`, on the command line as `--max-null-count-increase COLUMN=N` (#1346).
- **Edges, fixed.** A number literal too small for a float, such as `1e-400`, is held as written instead of reading as 0.0, so `1e-400` and `2e-400` no longer tie for a mode of 0 (#1182). The exact median and quartiles of a column holding a zero beside a decimal with 39 or more fractional digits are reported instead of missing (#1376). `profile()` accepts the stream `polars.LazyFrame.collect_batches()` returns (#1362). A full-source duplicate-row limit met by an estimated count is inconclusive with the cause `approximate_count` (#1336). Two error messages name what went wrong: a format override on a Parquet path (#1356) and a leftover option argument (#1284). The `dataprof.asyncio` profile functions have typed keyword arguments (#1281).

## The checkpoint in a workflow: dataprof-action v2

[dataprof-action](https://github.com/AndreaBozzo/dataprof-action) v2 runs this release in GitHub Actions. It installs the wheel into its own environment, runs one `python -m dataprof.check` over the files, directories or globs you give it, and turns the result into step outputs and a job summary with one row per check. It computes no verdict of its own: the step passes, fails or stops on what dataprof decided, and an unevaluated check shows as unevaluated, never as 0. The policy can sit inline in the workflow, so a checkpoint needs no extra file:

```yaml
- uses: AndreaBozzo/dataprof-action@v2
  with:
    file: exports/orders.csv
    baseline: baselines/orders.json
    allow-missing-baseline: 'true'
    promote: baselines/orders.json
    policy: |
      {"min_rows": 1000, "unique_columns": ["order_id"],
       "max_null_count_increase": {"*": 0}, "max_date_age": {"ordered_at": 2}}
```

Each job starts on a fresh runner, so keep `baselines/` between runs; the action's README shows it with a cache step. v1 called a CLI binary that dataprof no longer ships and scored the data in shell; v2 replaces it, and its README lists every changed input and output.

Four people besides the maintainer contributed to this release: @majidninja and @bfs2021 the two number fixes, @inverteum the two error messages, the async typing and the CI check of the project site's figures, and @KingEmma7 the Rust quickstart. Thank you.

## Install

```bash
# Python
pip install --upgrade dataprof==0.14.1

# Rust
cargo add dataprof@0.14.1
```

Wheels cover standard CPython 3.11 to 3.15 on Linux, macOS and Windows, as for 0.14.0. Rust 1.96 remains the minimum supported version.

## Numbers that move

- **Columns holding literals below the smallest float**, such as `1e-400`: they no longer read as 0.0, so such values stop tying with a real zero or with each other for the mode. Every other column reports the same numbers as 0.14.0.
- **Exact median and quartiles** of a column mixing a zero with a decimal of 39 or more fractional digits: present where 0.14.0 left them out.
- **Gate results** of a full-source `max_duplicate_rows` check over an estimated count: the not-evaluated cause is `approximate_count`.

Profiling 211 test files through every input route with 0.14.0 and 0.14.1 showed no other serialized difference. Baselines saved by 0.14.0 stay comparable.

## Still known

The 0.14.0 limitations below still hold, except the estimated duplicate count on a fully read file (#1336), fixed here.

<!-- release-body:end -->

---

## 0.14.0 · Quiet failures

The incidents that hurt most are the quiet ones. A scheduled job runs green every day while the file it publishes stopped changing months ago. A refresh drops a whole year of rows and keeps its header, its columns and its null share. A key repeats in one release in ten, and every whole-row duplicate check passes because the other fields differ. None of these moves a quality score much, so a score threshold lets them all through.

0.14 adds rules for each of them. A checkpoint can now fail a batch whose newest date is too old, whose dates hide a gap, whose row count fell or grew too far against the accepted run, or whose key repeats a value. Before the tag we replayed real incidents of these kinds from public data repositories, on the owners' own published file versions: every incident fails the rule written for it, and the healthy versions around it pass.

The other half of the cycle went into numbers that hold across routes. Integers past 2^53 and declared decimals keep their exact digits through the median, quartiles, mode and outlier fences, so a CSV, a Parquet file, an Arrow table and a database query of the same values agree in every serialized digit. Database connections now apply the TLS settings they are given. Four rounds of external audit went through the release candidate. Every critical finding from them is fixed here, and so is every high finding except two near the `decimal128` maximum, listed under known limitations.

Three people besides the maintainer contributed code to this release. Thank you.

### Install

```bash
# Python
pip install --upgrade dataprof==0.14.0

# Rust
cargo add dataprof@0.14.0
```

Wheels cover standard CPython 3.11 to 3.15 on Linux, macOS and Windows, with no Python dependencies. Rust 1.96 remains the minimum supported version. The Rust crates need `arrow` and `parquet` 60.1.0 or later in the 60 line (RUSTSEC-2026-0442).

### Highlights

- **Freshness and missing periods** (#1290, #1300). `check(max_date_age={"published_at": 3})` fails a column whose newest date is more than three days before the report's reference date, and `max_date_gap={"published_at": 1}` fails a hole between consecutive distinct dates inside the range. Date statistics now cover every value read, not the retained sample (#1297), and `reference_date=` fixes the clock (#903). On the command line: `--max-date-age` and `--max-date-gap`.
- **Row counts against the accepted run** (#1291). `max_row_count_drop` and `max_row_count_growth` limit the percentage change from the baseline's row count, and catch a truncated extract that every per-column rule passes. A column whose distinct count collapses fails `max_distinct_drop` (#1026).
- **Exact numbers past f64** (#1038, #1268). Integer and decimal columns report their order statistics exactly in `stats.exact`, take the mode, IQR and fences from exact values, and read a declared decimal as a decimal from Arrow, Parquet, PostgreSQL `NUMERIC` and MySQL `DECIMAL`. Each column records its source type as `native_type` (#906).
- **Spotting repeated keys** (#904, #1068, #1292). `unique_columns=["order_id"]` and `--unique-column order_id` fail a key that repeats a non-null value. `key_duplicates` reports it as a finding, and `forbid_findings` turns any finding into a gate failure (#1066). The exact duplicate-row count holds past its default 1,000,000 distinct rows when you raise `duplicate_rows_exact_limit`.
- **Hardened database connections.** PostgreSQL and MySQL connections apply the configured TLS settings, from the URL, `ssl_config` or the environment, with the URL's setting first (#1350, #1354). Queries run in a read-only transaction and accept `WITH` (#1171).

### Upgrade checklist

| If you rely on… | What changed | What to do |
| --- | --- | --- |
| a baseline saved by 0.13 | Reports record new score definitions in `metric_semantics` (`accuracy_comparison`, `json_number_reading`, `statistic_rounding`, `date_statistics`, `sampled_score_estimator`, `outlier_spread`, `date_forms`). A rule that reads one of them is `definition_not_comparable` against a 0.13 baseline on runs the definition can move (#1317). Completeness decides as before. | Run once with 0.14 and promote the report that passes your absolute rules. |
| a daily checkpoint that scores timeliness | A baseline check decides timeliness and overall drops only when both reports' clocks agree. Without `timeliness_clock`, two runs on different days give `setting_changed` (#1024). | Pass `timeliness_clock="rolling"` (`--timeliness-clock rolling`), or profile both runs with the same `reference_date`. |
| `max_date_age` on a capped or sampled read | Under `full_source`, a fresh inferred date on a partial read of CSV, JSON or strings is `evidence_incomplete` (#1333). | Scan the whole source, or use `scope="observed"`. |
| strict CSV parsing on the incremental engine | A ragged record at the start of a chunk is refused, as on the other engines (#1202). A header longer than 1 MiB is an error unless `csv_delimiter` is set (#1098). | Use `csv_flexible=True`, or set the delimiter. |
| async bytes reports | `report.source_type` is `"bytes"`, not `"stream"`, and an undeclared stream system is no longer recorded as HTTP (#1224). | Match on the new values. |
| hand-built flat summaries | `quality.sampled_dimensions` must be a list of strings or null; a summary without `execution.sampling_applied` keeps it absent, so a gate reads its coverage as unrecorded (#1235, #1236). | Load reports that dataprof wrote. |
| `dataprof-db` `ConnectionInfo` (Rust) | `query_params` is a `Vec<(String, String)>` in written order, with `query_param` and `set_query_param`; `username`, `password` and `database` hold decoded values (#1350). A non-default `ssl_config` wins over the SSL environment variables when the connection string is empty (#1354). | Read parameters through `query_param`; pass literal credentials. |
| report structs (Rust) | `DataSource` variants gain `csv_metadata` and, for streams, `format`; `source_system` is an `Option`. `FileFormat` gains `Arrow` and is `#[non_exhaustive]`. `ColumnProfile` gains `native_type`, `TimelinessMetrics` `clock`, `DateTimeStats` `largest_gap`. `ReportDelta.reports` is a `ReportPair` (#1215, #1224, #1231, #1232, #1251, #1267, #1275, #1300). | Add the fields to struct literals and a wildcard arm to matches. |
| `NotEvaluatedReason::QualityUnavailable`, `FindingPolicyError::ThresholdOutOfRange` (Rust) | `#[non_exhaustive]` at the variant level (#1200). | Destructure with `..`. |

### Numbers that move

Re-baseline stored reports and gates that read these.

- **Large magnitudes**: moment statistics at or above 1e8 serialize at 12 significant digits, so every route writes the same value (#1201).
- **Wide integers and decimals**: exact median, quartiles, mode, IQR and outlier counts (#1038, #1334, #1341); accuracy counts outliers and rule violations on exact values (#1199, #1304); the mean sums every value's remainder (#1262).
- **Declared decimals**: read as decimals with or without the statistics pack, and from databases at their declared digits (#1268, #1342).
- **High-precision JSON floats**: a literal with more digits than an `f64` holds keeps them, so distinct amounts stay distinct, on files and in database JSON cells (#1166, #1168).
- **Dates**: statistics over every value read (#1297); spaced datetimes with an offset are typed `date` (#1335); timeliness reads slash dates in the order the date statistics settled (#1305).
- **Sampled scores**: estimated over every scanned value, weighting columns by size (#1293); a column whose quartiles coincide is not tested for outliers (#1304).
- **A variance past `f64::MAX`** now reports `std_dev`, CV, skewness and kurtosis (#1150).

### Known limitations

Shipping with these; each has an issue.

- **Exact decimal arithmetic stops at the `i128` difference limit**: near the `decimal128` maximum, a mean of values that cancel can lose its fraction (#1352), and Tukey fences decide on rounded values (#1353). Decimals that cannot share one scale in `i128` leave the exact median and quartiles out.
- **A multi-part Parquet dataset is not one source** (#1355): merge the parts into one file to profile them together.
- **`compare()` still flags a whole report** as not comparable against a 0.13 report, even where the gate decides a rule (#712).
- **An estimated duplicate count on a fully read file** reports the gap as `quality_sampled` (#1336).
- **The null vocabulary is fixed** (#846), **database connectors need a source build** (#588), the PostgreSQL types they refuse are not decoded yet (#1060), and **durations** report statistics in their storage unit (#823).



### Detailed changes

The generated [changelog](CHANGELOG.md) lists every commit. This section groups the user-visible ones.

#### Gate and checkpoint

- Freshness (`max_date_age`, #1290), the largest date gap (`max_date_gap`, `stats.largest_gap`, #1300), row-count drop and growth (#1291), distinct-count collapse (#1026), per-column uniqueness (#904), and `forbid_findings` (#1066), each as a `check()` keyword, a `QualityPolicy` field, a CLI flag and a policy-file key.
- A timeliness reference date recorded in the report (#903) and a declared clock for baseline comparisons, fixed or rolling (#1024).
- Score definitions in `metric_semantics`, compared only on runs a definition can move (#1237, #1243, #1293, #1297, #1304, #1317, #1340).
- Sampled score drops are decided on both sides' intervals (#1035); a sampled accuracy interval covers the score of a full scan (#1304).
- Capped CSV, async CSV and Arrow C stream reports, and their flat summaries, declare their columns, so schema rules give a verdict (#1028, #1042, #1230); a flat summary records `exact_dimensions` (#1027).
- `findings()` runs in Rust for every binding (#1200), lists an unrecorded scan fact as not evaluated (#1306), and adds `key_duplicates` (#1068).

#### Reports and comparison

- `native_type` for every column (#906), `csv_metadata` with the delimiter choice and `delimiter_ambiguous` / `header_not_split` findings (#1100), the computed metric packs (#1030), `temporal_pairs` (#1029), the columns whose values the quality score did not check (#1134), and the caller's `source_version` (#866).
- `compare()` lists inferred and declared types and the column selection (#1031), and names each report's build and source version, as the gate result does (#1063).

#### Correctness

- Exact order statistics, mode and fences for integer and decimal columns (#1038, #1185, #1334, #1341); Decimal32 and Decimal64 read as decimals (#1160).
- Async routes: in-memory bytes recorded as a bytes source (#1224), `bytes_consumed` counted at the reader (#1223), row caps judged consistently for ragged CSV records and malformed JSON records (#1203, #1206, #1208).
- JSON: Unicode white space around an array (#1146); strict JSONL errors name the decoder's reason (#1143).
- CSV: configured quotes honored in detection (#1239), fields past Python's 131,072-character limit on bytes (#1222), a header past the 4 KB detection sample (#1098).

#### Databases

- TLS settings reach the driver from the URL, `ssl_config` and the environment, with every URL part encoded and the query order kept (#1350, #1354).
- PostgreSQL `NUMERIC` read at its display scale and from its plain digits (#1342); JSON cells with long float literals read from their text (#1168).
- Read-only transactions, `WITH` queries, and keyword checks that match whole words, so `created_at` passes (#1170, #1171).

#### Python

- Argument errors name the parameter and the accepted form (#1283); `python -m dataprof.check` exits 3 on a symlink loop (#1204).

#### Testing and release

- A release-baseline corpus gates every PR against the last release's reports (#1245), and a test holds sampled routes to the verdict of a full read (#1307).
- Release wheels pin maturin and their build images, and are tested on older CPUs in baseline and optimized builds (#1332).
