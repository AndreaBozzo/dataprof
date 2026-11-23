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

dataprof profiles CSV, JSON, JSONL, Parquet, DataFrames and Arrow tables in one call: types, nulls, distributions, patterns and a quality score. Then it gates your pipeline on what it found, including changes from a saved baseline. A Rust library with a Python package on top; local profiling needs no server, account or network access.

## Install

```bash
pip install dataprof          # or: uv pip install dataprof
```

Wheels for CPython 3.11 to 3.15 on Linux, macOS and Windows, with no Python dependencies. pandas and polars inputs need pyarrow; databases need a source build. See [input routes](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/getting-started.md#working-with-different-formats).

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
print(result)  # the verdict, then each failed or unevaluated check
if result.verdict != "pass":
    raise SystemExit(f"quality gate: {result.verdict}")
```

Other checks cover freshness (`max_date_age`, `max_date_gap`), volume (`min_rows`, `max_row_count_drop`), keys (`unique_columns`) and schema drift (`forbid_removed_columns`). See the [cookbook](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/examples.md#freshness-and-missing-days).

Or from CI, with no code at all:

```bash
python -m dataprof.check orders.csv --min-quality 90 --max-null "*=20"
# exit 0 = pass, 1 = fail, 2 = inconclusive, 3 = an argument, policy, input, baseline or output error

# Or against the last accepted run: fail if quality drops more than 5 points.
# A rolling clock lets each run judge its dates against its own day.
python -m dataprof.check orders.csv --min-quality 90 --baseline accepted.json \
  --allow-missing-baseline --max-quality-drop 5 --timeliness-clock rolling --promote accepted.json
```

`--promote` writes the baseline only on a pass, and `--allow-missing-baseline` lets the first run go without one. See the [checkpoint recipe](https://github.com/AndreaBozzo/dataprof/blob/HEAD/examples/README.md#4-baseline-checkpoint). In GitHub Actions, use [dataprof-action](https://github.com/AndreaBozzo/dataprof-action): `uses: AndreaBozzo/dataprof-action@v2`.

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

- **Same data, same numbers.** Serialized reports from Rust and Python agree across input routes and engines, within the [numeric contract](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/schema/README.md#numeric-equality-contract). Sampled values say so: `is_approximate`, `unique_count_is_approximate`, `quality_sampled_dimensions`.
- **Bounded memory.** Files larger than RAM stream through fixed-size accumulators. Duplicate rows are counted exactly up to 1,000,000 distinct rows by default, and estimated beyond that or under memory pressure.
- **Honest verdicts.** A sampled or partial scan passes a claim about the whole file only when what it read already proves it; otherwise the check is inconclusive. A sampled score carries an interval in `score_bounds` when it can be bounded.
- **Absence is not zero.** A metric that wasn't computed is `None`, never a plausible default.
- **Local by default.** Reports carry statistics such as minima, maxima and modes; `to_llm_context()` leaves raw values out by default.

## Status

Beta, moving toward a stable 1.0 contract. Known narrow spots: numbers with a decimal comma are flagged but not parsed, and only empty, `null`, `nan` and a fixed set of markers such as `NA`, `N/A` and `\N` count as null, so `-` or `-999` is still a value. The [release notes](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/release-notes.md) list what changed.

Found something wrong? [Open an issue](https://github.com/AndreaBozzo/dataprof/issues). A file that profiles badly is the most useful bug report there is.

## Learn more

- [Why dataprof?](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/why-dataprof.md): how it compares with summaries, visual reports and validation tools
- [Getting started](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/getting-started.md): the metrics and how to read them
- [Python API](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/python/README.md): every function, the gate and the CI entrypoint
- [Examples](https://github.com/AndreaBozzo/dataprof/blob/HEAD/examples/README.md): runnable recipes, from a messy CSV to a checkpoint against the last accepted run
- [Agent workflows](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/agent-workflows.md): AGENTS.md, Cursor rules and Claude Code skills
- [Report schema](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/schema/README.md): the saved document and its guarantees
- [Database connectors](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/database-connectors.md) and [feature flags](https://docs.rs/dataprof): Rust-side options
- [Changelog](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/CHANGELOG.md) and [contributing](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/CONTRIBUTING.md)

## Citing dataprof

Use **Cite this repository** in the GitHub sidebar, which builds APA and BibTeX from [CITATION.cff](https://github.com/AndreaBozzo/dataprof/blob/HEAD/CITATION.cff). The citation is for the software; benchmark material lives in [scalcom2026-dataprof](https://github.com/AndreaBozzo/scalcom2026-dataprof).

## License

Either the [MIT License](https://github.com/AndreaBozzo/dataprof/blob/HEAD/LICENSE) or the [Apache License, Version 2.0](https://github.com/AndreaBozzo/dataprof/blob/HEAD/LICENSE-APACHE), at your option.
