# Unreleased

## Security auditing covers every published feature graph

`cargo deny` ran against the default build only. Optional features are still
published features — the database connectors pull in the whole SQLx client
stack, and the async and Parquet features pull in a TLS stack — so CI reported
green while a shipped feature graph carried an advisory nobody had reviewed. The
security workflow now audits both the default and the complete feature graph,
and the two disagreed on more than advisories: the licence check had also never
seen `ISC` or `CDLA-Permissive-2.0`, both reached only through the TLS stack and
both permissive. They are now explicitly allowed.

Dependency updates that came with it:

- `arrow` and `parquet` 58 → 59, which removes the `thrift` dependency entirely
  and with it [GHSA-2f9f-gq7v-9h6m](https://github.com/advisories/GHSA-2f9f-gq7v-9h6m)
  (excessive allocation size). No API changes were required.
- `rand` 0.8.5 → 0.8.7 (transitive, via SQLx), clearing
  [GHSA-cq8v-f236-94qc](https://github.com/advisories/GHSA-cq8v-f236-94qc).
- `spin` 0.9.8 → 0.9.9, which was a yanked release.
- `Pygments` 2.19.2 → 2.20.0 in the Python environment, clearing
  [GHSA-5239-wwwm-4pmq](https://github.com/advisories/GHSA-5239-wwwm-4pmq).
  Dependabot was not watching the Python dependencies at all, so nothing would
  have proposed this; it now covers `uv.lock` alongside Cargo and Actions.

**RUSTSEC-2023-0071 (Marvin attack in `rsa`) is recorded as not applicable**,
with the reasoning and a review date in
[docs/SECURITY.md](SECURITY.md#accepted-advisories). In short: dataprof reaches
`rsa` only through `sqlx-mysql`, whose sole use of it encrypts a password with
the *server's* public key. It never performs a private-key operation and holds
no private key, so there is nothing for a private-key timing attack to target —
and the code path is skipped entirely over TLS, which is how dataprof builds
SQLx. No fixed release of `rsa` exists. The disposition is recorded in both
`deny.toml` and `.cargo/audit.toml`, so `cargo deny` and `cargo audit` agree.

## Sampling strategies actually sample

`sampling=` was undependable across engines and inputs. The documented default
call, `dp.profile(path, sampling=...)`, dropped the strategy and profiled
everything; forcing the incremental or async engine returned all rows, no rows,
or an arbitrary subset depending on which strategy was chosen. The cause was
that both engines asked a *stateless* helper about each row, which built fresh
state every time and never passed the row's values — so stateful strategies
behaved as if every row were the first, and row-aware ones could never match.

Every strategy is now applied by one shared sampler that holds its state for the
whole scan and can read the row it is deciding on.

**Fixed-size samples are drawn at the end of the source.** Whether a row belongs
in a uniform sample of *n* is not known until the stream ends — a row selected
early can be evicted later — and streaming statistics cannot be retracted. So
`random(n)` and `reservoir(n)` now hold their candidate rows and compute the
profile from the surviving sample, at a cost of *n* rows of memory. They give
the same guarantee (a uniform sample of exactly `n` rows, or the whole source if
it is shorter); both names are kept because both are familiar. Every other
strategy decides per row and adds no memory.

**Two strategies were redefined, because their names promised more than their
arithmetic delivered.**

- `progressive(initial, confidence_level, max)` measured "confidence" as
  `1 - 1/sqrt(n)`, which ignores the data entirely and cannot reach 0.95 below
  400 rows — so for any smaller `max_size` the parameter did nothing and the
  strategy just took `max_size` rows. It now measures the *relative standard
  error* of each numeric column's mean and stops once every one is within
  `1 - confidence_level` of its mean. Low-variance data stops early; volatile
  data runs to `max_size`. A source with no numeric columns has no measurable
  precision and samples `max_size` rows.
- `importance(weight_threshold)` scored rows with a built-in heuristic under
  which any complete row passed a threshold of 0.5, and applied no
  inverse-probability correction. It now takes the column to weigh on:
  `importance(weight_column, weight_threshold)` keeps rows whose value in that
  column is a number at or above the threshold. **This is a breaking signature
  change.** It is a filter, so the resulting profile describes the rows that met
  the threshold, not the source as a whole.

**Multi-stage composition is now defined.** Streaming stages act as filters in
sequence, and at most one fixed-size stage may appear, last — it draws its
sample from whatever the filters passed. Two fixed-size stages, or a filter
after one, are refused before reading rather than silently dropping stages.

**No path ignores the option any more.** `engine="auto"` routes a sampled CSV
run to the engine that can honour it. The columnar engine, the JSON and Parquet
readers, and synchronous bytes input raise instead of returning a full profile
under a sampling request.

`sampling_applied` and `sampling_ratio` now describe the rows that reached the
profile, and a strategy that happened to keep every row no longer reports itself
as sampling. Sampling also no longer marks a fully read source as unexhausted —
that field answers whether the source ran out, which is what `truncation_reason`
accompanies.

Note that sampling bounds the cost of *analysis*, not of reading: a uniform
sample requires seeing the whole source. Pair it with a `StopCondition` to bound
I/O.

## Execution controls take effect, and execution metadata tells the truth

`chunk_size` and `memory_limit_mb` were accepted and then dropped on the paths
that document them, so a resource control could be silently ineffective. Both
now reach the engine that does the work:

- The incremental engine honors an explicitly configured `chunk_size` instead of
  always deriving one from the memory limit and file size.
- `memory_limit_mb` is forwarded on the incremental and default (`auto`) paths,
  not only on the explicit columnar and async ones.

**`chunk_size` is measured in bytes, on every engine and in every binding.** The
type documented rows while the async reader treated the value as bytes and the
incremental engine ignored it; bytes is now the single unit, because it is what
actually bounds the working set. Chunk size never changes the result of a
complete scan — only read granularity, progress cadence, and the points at which
chunk-level stop conditions are evaluated. Async callers who passed
`ChunkSize::Fixed` see effectively no change, since that path already divided
the value by an assumed row width.

Stop conditions and the metadata they produce were also inconsistent:

- **Row caps are hard caps.** `dataprof.asyncio` evaluated `max_rows` per chunk,
  so a request for 123 rows could return 200 while the report named the limit of
  123. Async CSV, JSON and JSONL now stop exactly at the cap.
- **A condition met on the final chunk is not a truncation.** A confidence
  threshold or `quality_sample()` preset satisfied by the last chunk of a fully
  read file reported `source_exhausted: false` with a truncation reason, making
  a complete profile indistinguishable from a bounded one. The same holds for a
  row cap equal to the row count.
- **Schema stability keeps its counters.** Stopping on `schema_stable` reset the
  stop evaluator to suppress a duplicate reason, discarding the accumulated byte
  count with it and reporting `bytes_consumed: 0` on a scan that had read
  thousands of bytes.
- **Async byte counts are no longer short.** They were summed from parsed
  fields, which exclude delimiters, quotes and line endings, so even a complete
  scan reported fewer bytes than the source held. CSV now counts from the
  parser's own byte position, and a scan that reaches the end of a source of
  known length reports that source's full size.

Byte caps remain chunk-boundary caps: `bytes_consumed` may exceed `MaxBytes` by
at most one chunk, a bound the caller controls through `chunk_size`. Row caps
have no such allowance.

`rows_processed`, `bytes_consumed`, `source_exhausted` and `truncation_reason`
are now covered by invariant tests on both the sync and async paths — a
truncated scan is exactly a non-exhausted one, and an exhausted scan accounts
for every byte of its source.

## Ragged CSV rows leave a signal instead of vanishing

A CSV row whose field count differs from the header — extra trailing fields, or
missing ones — is still recovered (extra fields dropped, missing fields padded
to null) so profiling continues, but that recovery is no longer silent.
`execution.ragged_row_count` reports how many rows were ragged; it is `0` for a
cleanly parsed file. Previously such files reported `error_count: 0` and a
perfect consistency score, answering "did parsing silently go wrong?" with a
confident and wrong "no".

The count is exposed on the Python `ProfileReport` as `report.ragged_row_count`
and in `to_dict()["execution"]["ragged_row_count"]`. It is an additive report
field: reports written before this release deserialize with `ragged_row_count`
of `0`.

The async reader follows the same policy
([#462](https://github.com/AndreaBozzo/dataprof/issues/462)): byte streams and
URLs recover ragged rows and report the same count, so the transport can no
longer launder a broken source into a clean-looking report. `csv_flexible=False`
now also reaches that path — previously it was accepted and ignored there — and
rejects the first ragged record instead of repairing it.

Scope: the count is surfaced by the incremental engine, which drives the default
CSV path, and by the async reader. Byte inputs to the synchronous `profile()`
still reject rather than recover, since they are read without the flexible
engine; write the data to a file to profile it leniently.

The explicitly selected columnar (Arrow) engine is not yet covered. It rejects a
row with *extra* fields, but pads a row with *missing* fields to null and still
reports `ragged_row_count: 0` — so `engine="columnar"` remains a silent-clean
path for short rows. Prefer the default `engine="auto"` when a source may be
structurally broken; the gap is tracked in
[#470](https://github.com/AndreaBozzo/dataprof/issues/470).

Ragged rows do not yet influence the consistency dimension's score.

## Async CSV detects its delimiter instead of assuming a comma

`csv_delimiter` was accepted and ignored on every async path, which always
parsed on commas. A semicolon- or tab-separated stream therefore collapsed into
a single column and profiled as perfectly clean. The async reader now honors an
explicit `csv_delimiter`, and when none is given it detects one from the head of
the stream using the same sample size and scoring as the file path — so the same
bytes yield the same columns whether they are read from disk, from memory, or
off a URL.

## Malformed CSV raises `ValueError`, like malformed JSON

A CSV parse failure reached Python as a `RuntimeError` while the equivalent JSON
failure raised `ValueError`, so callers could not catch bad input as one
category. Malformed data of either format is now a `ValueError`.

Relatedly, a rejection under `csv_flexible=False` keeps its own diagnostic. The
auto engine used to retry the file under its fallback parser and report
`All engines failed: ...` with both parsers' messages; asking for strict parsing
means opting out of recovery, so the original error — naming the row and the
expected field count — is returned directly.

## Locale patterns require locale evidence before becoming report claims

Ambiguous locale-specific shapes remain available in each column's detailed
`patterns` evidence, but no longer appear as a top pattern in text, Markdown,
HTML, tabular, or LLM-oriented summaries unless their confidence is at least
0.5. For example, a five-digit order ID column without a configured locale can
still show both Italian CAP and US ZIP as low-confidence candidates, but neither
is presented as the column's semantic type.

An explicit `locale=` is now case-insensitive and strict: its matching patterns
receive enough evidence to be reportable at a strong match rate, while patterns
for other locales are suppressed even when the broad regex matches every row.
Coordinate validation also distinguishes compact latitude/longitude pairs from
decimal-comma numbers such as `1.234,56`. More generally, a pattern is no longer
returned when its semantic validator rejects every regex match, and such a
candidate cannot suppress another pattern during overlap resolution. This
resolves [#429](https://github.com/AndreaBozzo/dataprof/issues/429).

## Errors preserve source context and map to idiomatic Python exceptions

Failure diagnostics are now consistent about what failed, where, and what to do
next. This is **compatibility-sensitive**: error messages and, on the Python
side, exception *types* have changed.

- **Real paths, never `'unknown'`.** The context-free `From<io::Error>`,
  `From<anyhow::Error>`, and `From<csv::Error>` conversions no longer fabricate
  a `FileNotFound { path: "unknown" }`; they keep the original message and let
  call sites that hold the path attach it. `Profiler::analyze_file` now fails
  fast with a `FileNotFound` naming the real path when the file is missing.
- **Honest supported-format claims.** `UnsupportedFormat` lists only the formats
  the running build can actually read — Parquet appears only when the `parquet`
  feature is enabled — instead of the fixed `CSV, JSON, JSONL` string that
  omitted Parquet.
- **Structured suggestions.** `DataProfilerError::suggestion()` exposes the
  actionable next step as data, so callers no longer have to parse it out of the
  formatted message.
- **Credential redaction.** Database connection and query errors scrub
  `scheme://user:password@host` userinfo before the message is stored, and the
  "invalid connection string" errors report the detected scheme instead of
  echoing the raw string.
- **Idiomatic Python exceptions.** File-based entry points now raise
  `FileNotFoundError` (missing file), `ValueError` (unsupported format, invalid
  config, unbindable semantic hints), and `PermissionError` / `OSError` (I/O),
  instead of wrapping every failure in a generic `RuntimeError`. `except
  RuntimeError:` blocks that relied on the old behavior need updating.

## Installed capabilities are discoverable

`dataprof.capabilities()` returns an immutable snapshot of the current build:
local formats, compiled pandas/polars/Arrow interoperability, installed optional
Python packages, async and URL support, remote Parquet support, database
availability, compiled connectors, and the package version. Discovery imports
no heavyweight optional dependency and performs no file, database, or network
operation.

## Validity and Precision join the quality model

Two new selectively requestable dimensions are available in Rust and Python:

- **Validity** measures conformance to a confidently detected semantic pattern.
  It stays unassessed when pattern detection did not run or no pattern has
  enough confidence, rather than assuming values are valid.
- **Precision** measures consistency of effective decimal places within each
  floating-point column. It reports deviation from the observed modal scale;
  it does not infer a business-required number of decimal places.

Both dimensions participate in `assessed_dimensions()`, per-dimension scores,
streaming provenance, report serialization, and Python nested quality dicts.
Adding them changes the default score weights to 0.25 completeness, 0.20
consistency, 0.15 uniqueness, 0.15 accuracy, 0.10 timeliness, 0.10 validity,
and 0.05 precision. Re-baseline aggregate-score gates; underlying facts remain
individually inspectable.

## Quality score weights are configurable

The overall score's relative dimension weights now live in
`IsoQualityConfig::score_weights`. `MetricsCalculator::with_thresholds(...)`
copies them into each `QualityMetrics` result, so custom scores remain
reproducible after report serialization. Weights renormalize over the
dimensions that were actually assessed.

The documentation now describes ISO 8000 and ISO/IEC 25012 as sources for the
quality-dimension concepts. Dataprof's aggregate score and its weights are a
configurable project formula, not an ISO-mandated or certified score.

## The quality score now only scores what was actually assessed

**`quality_score` changes value for almost every dataset.** The facts
(per-dimension counts and ratios) are unchanged; how they aggregate is not.

Previously, a dimension with nothing to assess counted as perfect: no numeric
columns meant accuracy 100, no date columns meant timeliness 100, no id-named
column meant uniqueness 100. A text-only CSV could not score below 70 no matter
how bad it was. And the violation counts (duplicate rows, format violations,
encoding issues, range violations, future dates, temporal violations) never
influenced the score at all.

Now:

- Each dimension records how much data it examined (`total_cells`,
  `values_checked`, `rows_checked`, `numeric_values_checked`,
  `date_values_checked`). A dimension that examined nothing is **excluded**
  and the weights renormalize over the assessed dimensions
  (`QualityMetrics::assessed_dimensions()`, Python
  `quality.assessed_dimensions()` / `quality.dimension_scores()`).
- Every sub-metric now feeds its dimension score: duplicate rows drive
  uniqueness, format violations and encoding issues drive consistency, range
  violations drive accuracy, future dates and temporal violations drive
  timeliness. Completeness is the mean of cell-level and row-level
  completeness.
- Timeliness scoring assesses confidently inferred date columns by default.
  Explicit `temporal_columns` hints add columns that inference cannot identify,
  such as mixed-format strings. Non-null values that fail strict calendar
  parsing are reported as `invalid_date_values` and reduce the timeliness score
  instead of silently disappearing from its denominator.
- `UniquenessMetrics.key_column` names the column `key_uniqueness` describes;
  when no key column is identified, key uniqueness carries no signal instead
  of "assume perfect".
- Explicit `identifier_columns` hints now select the column used by
  `key_uniqueness`. Without a hint, key-name inference matches complete words
  such as `id`, `key`, and `pk` (including snake/kebab/camel case) instead of
  substring false positives such as `paid`, `valid`, or `monkey`.
- The duplicate-row scan refuses per-column samples it cannot prove
  row-aligned (null-stripped or reservoir-evicted columns) instead of
  comparing unrelated values.
- `report.quality_score()` returns `None` when nothing was assessable (empty
  dataset, or a report serialized by an older version) instead of a
  fabricated 100. `overall_score()` on raw metrics returns 0.0 in that case —
  check `assessed_dimensions()` to tell the difference.

**What to do:** re-baseline any thresholds built on `quality_score`. Scores
move down or stay; datasets whose old score leaned on vacuous-perfect
dimensions drop the most. At that stage the five-dimension weights remained
0.30 / 0.25 / 0.20 / 0.15 / 0.10; the Validity and Precision change above
subsequently expands and rebalances them. The formula remains dataprof's own,
not an ISO-mandated one.

## Duplicate rows are now counted over the full stream

The CSV, JSON, and streaming engines track row identity while they read:
every record's fields fold into a length-prefixed signature fed to the same
exact-then-HLL distinct estimator that backs `unique_count`. As a result:

- `duplicate_rows` covers **every row of the source** — including rows with
  null values, which the old sample-based scan could never see — with
  `rows_checked` reporting the full row count.
- Below ~10,000 distinct rows the count is exact. Beyond that the distinct
  estimator spills to its HLL sketch and the derived duplicate count is an
  estimate (~1% relative error on distincts), flagged by the new
  `UniquenessMetrics.duplicate_rows_approximate` field and filed as
  non-exact provenance.
- Engines without a row tracker (Parquet record batches, DataFrames, Arrow)
  keep the alignment-guarded sample scan; when the sample cannot be proven
  row-aligned, `duplicate_rows` stays not-assessed rather than wrong.

`MetricsCalculator::calculate_{comprehensive,bifurcated}_metrics_with_positive_columns`
gained a `row_duplicates: Option<RowDuplicateSummary>` parameter; pass `None`
to keep the previous behavior.

## Semantic hints are validated, not silently dropped

A semantic hint (`positive_columns`, `identifier_columns`, `temporal_columns`)
is the user's chosen alternative to overconfident inference, so a hint that
cannot bind is now an error instead of a silent no-op:

- A hint that names a column not in the schema raises, listing the unmatched
  names and the available columns. In Python this is a `ValueError`.
- A hint that names a real column but binds to no value over the full data —
  a `positive` hint on a column with no numeric values, a `temporal` hint on a
  column with no dates — also raises. Identifier hints coerce the column's type,
  so they bind to any existing column and are only rejected for an unknown name.
- Mixed columns still bind: a `temporal` hint on a column where only some values
  parse as dates is assessed, not rejected.

Reports gained `semantic_hint_bindings`, per-column evidence of how each hint
bound (`column`, `kind`, `checked_values`, `matched_values`, `exact`).
Value-driven hints are counted by bounded-memory accumulators over the full
processed stream, even when quality metrics use a reservoir sample. Their
evidence is therefore exact, and an inert hint is rejected consistently on
large streamed sources without risking a false positive when a match exists
outside the retained sample. Supplying `positive_columns` or
`temporal_columns` without the Quality metric pack is also an error because
those hints would have no consumer; `identifier_columns` remains usable because
it affects column typing. The field is additive; older readers ignore it.

---

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

### pyo3 upgraded to 0.29 (security)

`pyo3` and `pyo3-async-runtimes` move from 0.27 to 0.29, clearing a high-severity
out-of-bounds read in the `nth`/`nth_back` iterator implementations for `PyList`
and `PyTuple` (GHSA-36hh-v3qg-5jq4), plus a missing `Sync` bound on
`PyCFunction::new_closure` (GHSA-chgr-c6px-7xpp).

dataprof never called the affected iterator methods, so no released version was
exploitable through the public API — but the vulnerable code was compiled into
the wheels we publish, and 0.9.0 does not ship it.

While adapting to the 0.29 API, the Arrow PyCapsule import path also became
stricter. It now validates the capsule names (`arrow_schema`, `arrow_array`)
before dereferencing, using `pointer_checked()` in place of pyo3's removed
unchecked `PyCapsule::pointer()`. A capsule carrying an unexpected payload is
rejected with a `TypeError` rather than reinterpreted as an Arrow struct.

This is a Rust-side dependency change with no Python API impact.

### Database helpers were never actually exported

The database guide has always told you to call `dp.analyze_database_async(...)`
after a source build. That name never existed on the `dataprof` package — the
functions were only ever registered on the private `dataprof._dataprof` module,
so the documented call raised `AttributeError` even when you built with the
right features.

They are now exported from `dataprof`, and `analyze_database_async()` returns a
wrapped `ProfileReport` instead of the raw core report, so `report.rows` and the
rest of the wrapper surface behave like every other report:

```python
report = await dp.analyze_database_async(
    "postgres://user:pass@localhost/mydb",
    "SELECT * FROM users",
    batch_size=10000,
    calculate_quality=True,
)
print(f"{report.rows} rows, quality: {report.quality_score}")
```

On the published wheels, which carry no database support, these names now resolve
to stubs that raise `ImportError` telling you how to rebuild, rather than
disappearing with an `AttributeError`.

The type stubs for these four functions were also wrong and have been corrected:
`analyze_database_async` takes `batch_size` and `calculate_quality` rather than a
`config`, `get_table_schema_async` returns `list[str]`, and
`count_table_rows_async` returns `int`.

### Database queries returned all-null numeric columns

Profiling a database query read every column as text. The connectors called
`row.try_get::<String>(i)` and dropped the result with `.ok()`, so when sqlx
failed to decode an `INTEGER` or `REAL` into a `String`, the **decode error
became a `None`** — indistinguishable from SQL NULL. Every non-text column, on
all three connectors, profiled as an all-null string column: `null_count` equal
to the row count, no minimum, no mean. Text columns were unaffected, which is why
this survived.

This is the same failure as the Parquet bug above — an error silently reinterpreted
as missing data — and it has been present since 0.8.0.

Integer, float, and boolean columns now decode correctly, and a database profile
agrees with profiling the same rows exported to CSV, down to the inferred column
type. A `REAL` column of whole numbers still reports as a float rather than
collapsing to an integer.

**What to do:** any quality baseline taken from a database query is wrong for
every non-text column and should be regenerated.

> **Still unsupported.** `NUMERIC`/`DECIMAL`, date and time types, and `BLOB`
> continue to profile as nulls: decoding them requires sqlx features this crate
> does not enable. Profile an export if you need statistics for those.

### Other fixes

- `crossbeam-epoch` bumped to 0.9.20 for RUSTSEC-2026-0204 (#346).
- `arrow` and `parquet` bumped from 57.3.0 to 58.x (#324).
- `quinn-proto` bumped to 0.11.15 for RUSTSEC-2026-0185.
- Two clippy lints introduced by Rust 1.97 resolved (`question_mark`,
  `byte_char_slices`).

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
