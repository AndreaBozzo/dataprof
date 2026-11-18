<div align="center">
  <img src="https://raw.githubusercontent.com/AndreaBozzo/dataprof/HEAD/assets/images/logo.webp" alt="dataprof logo" width="800" />
  <h1>dataprof</h1>
  <p><strong>Know what's in your data before it ships.</strong></p>

  [![PyPI](https://img.shields.io/pypi/v/dataprof.svg)](https://pypi.org/project/dataprof/)
  [![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
  [![docs.rs](https://docs.rs/dataprof/badge.svg)](https://docs.rs/dataprof)
  [![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](https://github.com/AndreaBozzo/dataprof/blob/HEAD/LICENSE)

  [Website](https://andreabozzo.github.io/dataprof/) · [Getting started](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/getting-started.md) · [Python API](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/python/README.md) · [Release notes](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/release-notes.md)

</div>

dataprof profiles CSV, JSON, JSONL, Parquet, DataFrames and Arrow tables in one call: types, nulls, distributions, patterns and a quality score. Then it can gate your pipeline on what it found, including changes from a saved baseline. It runs locally and records when the evidence cannot settle a check.

A Rust library with a Python package on top. Local file profiling needs no
server, account or network access.

## Install

```bash
pip install dataprof          # or: uv pip install dataprof
```

Wheels for CPython 3.11 to 3.15 on Linux, macOS and Windows, with no Python dependencies.

## Profile a file

```python
import dataprof as dp

report = dp.profile("orders.csv")        # also .json, .jsonl, .parquet, DataFrames, dicts
print(report.rows, "rows,", report.columns, "columns, quality", report.quality_score)

amount = report["amount"]
print(amount.data_type, amount.mean, amount.null_percentage)
```

Ask it what deserves attention. Each finding has a stable code and the evidence behind it, never a raw value:

```python
for finding in report.findings():
    print(finding.severity, finding.code, finding.column)
# warning locale_numbers price       ("10,50"-style numbers, left out of the stats)
# warning null_heavy email
# info sensitive_pattern email
```

## Gate a pipeline

State what "good enough" means and get a verdict: `pass`, `fail`, or `inconclusive` when the data can't prove it either way.

```python
result = report.check(
    min_quality_score=90, max_null_count={"customer_id": 0}, max_null_percentage={"*": 20}
)
if not result.passed:
    for check in result.violations:
        print(check.code, check.column, check.message)
```

Or from CI, with no code at all:

```bash
python -m dataprof.check orders.csv --min-quality 90 --max-null "*=20"
# exit 0 = pass, 1 = fail, 2 = inconclusive, 3 = an argument, policy, input, baseline or output error

# Or against the last accepted run: fail if quality drops more than 5 points.
# A rolling clock lets each run judge its dates against its own day.
python -m dataprof.check orders.csv --baseline accepted.json --max-quality-drop 5 --timeliness-clock rolling
```

Keep the candidate report with `--report candidate.json`. Promote it to the
baseline only on a pass, including the first run, which uses absolute rules
alone. A changed or unrecorded metric definition can leave a comparison
`definition_not_comparable`; inspect the reason and regenerate the baseline
deliberately. See the [checkpoint recipe](https://github.com/AndreaBozzo/dataprof/blob/HEAD/examples/README.md#4-baseline-checkpoint).

0.14 adds these checks:

- **Freshness and missing periods:** `check(max_date_age={"published_at": 3}, max_date_gap={"published_at": 1})` limits the age of the newest date and the largest gap between consecutive distinct days. `dp.profile("orders.csv", reference_date="2026-06-10")` fixes the timeliness clock.
- **Volume:** `check(baseline=baseline, max_row_count_drop=10, max_row_count_growth=20)` limits percentage changes in row count; `min_rows=1` sets an absolute floor.
- **Keys:** `check(unique_columns=["order_id"], max_null_count={"order_id": 0})` requires distinct non-null keys and no nulls. From CI, use `--unique-column order_id --max-null-count order_id=0`. `identifier_columns=["order_id"]` selects the key for the uniqueness score and `key_duplicates` finding.
- **Schema drift:** `forbid_removed_columns=True` rejects removed columns. `compare()` and baseline diagnostics also report inferred type changes; these diagnostics alone do not fail a gate. A column's `native_type` records its source's declared type where available.

See the [cookbook](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/examples.md#freshness-and-missing-days) for complete examples.

## Hand it to an agent

A token-bounded summary for an LLM. Values that look like personal data are never echoed:

```python
print(report.to_llm_context(max_tokens=500))
```

## Use it from Rust

```bash
cargo add dataprof
```

```rust
use dataprof::{Profiler, QualityPolicy, Verdict};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let report = Profiler::new().analyze_file("orders.csv")?;
    let result = QualityPolicy::new().min_quality_score(90.0).evaluate(&report)?;
    assert_eq!(result.verdict, Verdict::Pass);
    Ok(())
}
```

Minimum supported Rust: 1.96.

## What you can count on

- **Same data, same serialized numbers.** Rust Serde output and Python `to_dict()` / `to_json()` agree across routes and engines for the same logical data, schema semantics, options and analyzed population, within the [numeric contract](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/schema/README.md#numeric-equality-contract). Source and execution provenance are compared separately. Raw floats may differ in their final digits; extreme values near the limits of `f64` belong to an approximate domain. A retained sample and a full scan need not agree. Reports flag sampled numeric statistics with `is_approximate`, estimated distinct counts with `unique_count_is_approximate`, and sampled quality components with `quality_sampled_dimensions`.
- **Bounded memory.** Files larger than RAM stream through fixed-size accumulators. The exact duplicate-row count holds up to 1,000,000 distinct rows by default, about 10 to 20 bytes each, inside the memory budget, and becomes an estimate under memory pressure. A `duplicate_rows_exact_limit` you set yourself is the exception: those rows stay exact and are kept outside the budget.
- **Honest verdicts.** A sampled or partial scan passes a claim about the whole file only when what it read already proves it, such as a row minimum it reached; otherwise the check is inconclusive. A sampled score carries an interval in `score_bounds` whenever it can be bounded.
- **Absence is not zero.** A metric that wasn't computed is `None`, never a plausible default.
- **Local by default.** Profiling runs on your machine. Reports carry statistics, including numeric minima, maxima and modes; `to_llm_context()` leaves raw values out by default.

The Python wheels include file and bytes profiling, Arrow C array and stream
inputs, and `dataprof.asyncio` file, bytes and HTTP helpers. Install the table
library you use, plus pyarrow for pandas and polars conversion; native Parquet file and bytes
profiling needs none of them. PostgreSQL, MySQL and SQLite require a source
build with connector features. See [input routes](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/getting-started.md#working-with-different-formats).

## Status

Beta, and moving fast toward a stable 1.0 contract. Some things are still narrow, and we'd rather tell you than have you find out: numbers written with a decimal comma are detected and flagged but not yet parsed, and beyond empty, `null` and `nan`, only a fixed set of markers (`NA`, `N/A`, `n/a`, `#N/A`, `\N`, `None`, matched exactly, ignoring surrounding whitespace) counts as null, so a sentinel like `-` or `-999` is still a value. The [release notes](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/release-notes.md) list what changed and what's known.

Found something wrong? [Open an issue](https://github.com/AndreaBozzo/dataprof/issues). A file that profiles badly is the most useful bug report there is.

## Learn more

- [Why dataprof?](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/why-dataprof.md): profiling and quality gates compared with summaries, visual reports and validation tools, including when to pick each
- [Getting started](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/getting-started.md): the metrics and how to read them
- [Python API](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/python/README.md): every function, the gate and the CI entrypoint
- [Examples](https://github.com/AndreaBozzo/dataprof/blob/HEAD/examples/README.md): a messy CSV, an ETL gate, before and after cleaning, a checkpoint against the last accepted run, a collapsing key column, runnable from a clean checkout
- [Agent workflows](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/agent-workflows.md): AGENTS.md, Cursor rules and Claude Code skills
- [Report schema](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/schema/README.md): the saved document and its guarantees
- [Database connectors](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/database-connectors.md) and [feature flags](https://docs.rs/dataprof): Rust-side options
- [Changelog](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/CHANGELOG.md) and [contributing](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/CONTRIBUTING.md)

## Citing dataprof

Use **Cite this repository** in the GitHub sidebar, which builds APA and BibTeX from [CITATION.cff](https://github.com/AndreaBozzo/dataprof/blob/HEAD/CITATION.cff). The citation is for the software; benchmark material lives in [scalcom2026-dataprof](https://github.com/AndreaBozzo/scalcom2026-dataprof).

## License

Either the [MIT License](https://github.com/AndreaBozzo/dataprof/blob/HEAD/LICENSE) or the [Apache License, Version 2.0](https://github.com/AndreaBozzo/dataprof/blob/HEAD/LICENSE-APACHE), at your option.
