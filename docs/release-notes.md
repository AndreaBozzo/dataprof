# dataprof 0.11.0 — The documented API, and metrics that earn their scores

<!-- release-body:start -->

0.11.0 closes two gaps that had the same shape: something dataprof published
did not match what it actually did.

The first was distribution. `dataprof.asyncio`, `profile_url_async` and remote
Parquet were documented, tested, and absent from every published wheel, because
the release job built `--features python` and that feature was empty. `pip
install dataprof` now reaches the async API, URL profiling and remote Parquet
without a Rust toolchain.

The second was measurement. Four quality metrics returned confident numbers
that were wrong in ways no error surfaced: a file dated entirely in the future
scored a perfect timeliness, a column of clean ISO datetimes scored zero
consistency, adding junk to a numeric column *raised* its score, and record
completeness collapsed to 0% whenever nulls co-occurred. Each is now measured
the way it is described.

Both strands change numbers. That is the point, and the upgrade checklist below
says where.

## Install

```bash
# Python
pip install --upgrade dataprof==0.11.0

# Rust
cargo add dataprof@0.11.0
```

Python 3.10+ and Rust 1.96+ remain the supported minimums. dataprof 0.11.0
ships libraries and Python packages; there is no CLI binary.

## Upgrade checklist

| If you rely on… | What changed | What to do |
| --- | --- | --- |
| a source build for async or URL profiling | `python-async`, `async-streaming` and `parquet-async` now ship in the published wheel. It grows 3.72 → 5.25 MiB on Linux x86_64, still with no Python dependencies. | Drop the source-build step. `capabilities()` reports what the installed build supports. |
| database helpers from a wheel | Database connectors stay out of the wheel deliberately, not accidentally: `#365` records DECIMAL, temporal, UUID and BLOB columns as null. | Keep the source build with `--features database,<driver>`, and track `#365` before trusting those columns. |
| `timeliness` counts or scores | `future_dates_count` compared calendar years, so nothing inside the current year was ever future — a blind window up to 364 days wide. Ordering compared raw strings, which only holds for ISO, so day-first inputs hid real inversions and invented others. Overlapping role patterns counted every pair twice. | Re-baseline timeliness gates. Files with near-future dates, non-ISO date formats, or `start_date`/`end_date` pairs move the most. |
| `complete_records_ratio` | It was derived from per-column null totals, which assumes no two nulls share a record. It understated completeness whenever nulls co-occurred and collapsed to 0% once null cells outnumbered rows. It is now counted on rows. | Expect this figure to rise on data with correlated nulls. Re-baseline any gate built on it. |
| `data_type_consistency` on `string` columns | A column that fell below the inference thresholds became `string` and reported a perfect 100 however mixed it was, so adding junk raised the score. Columns with no inferred type are now scored on the share held by their largest lexical class. | Mixed columns score lower and continuously across the old boundary. Read `type_homogeneity` beside the score for the direction of the mix. |
| `data_type_consistency` on `date` columns | Inference and validation used different regex sets, so a column typed `Date` on a form the validating set rejected failed every value in it. Clean ISO datetimes and dotted dates scored 0.0. | Date columns that were wrongly penalised now score correctly. Re-baseline. |
| `locale=` tags | An unrecognised tag was accepted and silently suppressed every locale-specific pattern, so `locale="it-IT"` returned fewer patterns than passing nothing. The tag is now a closed set. | Handle `ValueError` for an unknown tag. `IT`, `ITA`, `it-IT` and `it_IT` all normalise to the same locale. |
| names reachable from `dataprof.*` | The package declared 28 names in `__all__` while 40 were reachable, including `dataprof.os`, `dataprof.json` and `dataprof.pathlib`. Internal imports are now private. | Import standard-library modules directly rather than through `dataprof`. |
| exact float values in reports | Rust and Python rounded the same fields to different precisions. Precision now follows what the number is: 2dp for 0–100 percentages, 4dp for statistics, data values and 0–1 ratios, quartiles at 2dp. | `min`, `max`, `median`, `mode` and `avg_length` gain precision in Rust. Compare reports across languages rather than assuming either was canonical. |
| `Error::source()` on `DataProfilerError` | Six variants now retain the originating error instead of flattening it into a string, and the cause reaches Python as `__cause__`. | Walk the source chain instead of parsing message text. |

## Release highlights

- **The wheel is the product.** Async, URL profiling and remote Parquet ship by
  default. `pyproject.toml` is the single declaration of the shipped feature
  set, and a test fails if the release job disagrees with it.
- **Four quality metrics stopped flattering the data.** Timeliness, record
  completeness, string-column consistency, and date-column validity each
  returned a confident wrong number; each now measures what it documents.
- **Arrow interoperability is honest about what it accepts.** Chunked Tables
  profile every batch, any Arrow PyCapsule producer is accepted, and data
  imported over the C Data Interface is validated instead of trusted.
- **Reports carry a versioned, published JSON schema.** Mapping fields
  serialize in a deterministic order, and a byte-buffer input reports a `bytes`
  source type rather than pretending to be a file.
- **Errors are walkable.** A decode failure keeps the error that caused it,
  in Rust and in Python.

## Known limitations

Shipping with these, tracked for 0.12:

- **An unassessable report's aggregate is `0.0`, not `None`** (`#571`). Every
  dimension correctly reports `None`, and `report.quality_score` returns `None`,
  but `overall_quality_score()` and the serialized `overall_score` aggregate the
  empty set to zero. A zero-row input reads as "terrible" rather than "nothing
  to assess".
- **Dimension evidence accessors report ratios from zero inputs** (`#622`).
  `quality.validity` returns `valid_values_ratio: 100.0` with
  `values_checked: 0` when validity was never assessed. `assessed_dimensions()`,
  `dimension_scores()`, `quality_summary()` and `to_llm_context()` are all
  correct; only the raw evidence dicts fabricate.
- **Text lengths are UTF-8 byte counts under names that say nothing about
  encoding** (`#627`). `min_length`, `max_length` and `avg_length` measure
  encoded bytes on every engine, so `"東京"` reports 6 and `"🙂"` reports 4.
  0.12 changes the unit to Unicode scalar values, which reports 2 and 1 for the
  same values. **ASCII is unaffected; a non-ASCII text column profiled under
  0.11 and under 0.12 is not comparable**, and the report schema version does
  not move because nothing about validation changes. Re-profile rather than
  comparing across the boundary.
- **`std_dev` differs in the last ULP between engines** (`#547`). The
  incremental accumulator and the Arrow path compute it differently. Serialized
  reports round to 4dp and agree; the raw attribute does not.
- **Locale-aware value parsing is not implemented** (`#433`). `locale=` tunes
  pattern detection, not number or date parsing, so a European export with
  decimal commas still profiles those columns as text.

<!-- release-body:end -->
