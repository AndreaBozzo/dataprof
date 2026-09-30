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

dataprof profiles CSV, JSON, Parquet, DataFrames and Arrow tables in one call: types, nulls, distributions, patterns and a quality score. Then it can gate your pipeline on what it found. It runs locally, gives the same numbers for the same data whichever way you load it, and says "can't tell" instead of guessing.

A Rust library with a Python package on top. No server, no account, no network.

## Install

```bash
pip install dataprof          # or: uv pip install dataprof
```

Wheels for CPython 3.10 to 3.14 on Linux, macOS and Windows, with no Python dependencies.

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
result = report.check(min_quality_score=90, max_null_percentage={"customer_id": 0, "*": 20})
if not result.passed:
    for check in result.violations:
        print(check.code, check.column, check.message)
```

Or from CI, with no code at all:

```bash
python -m dataprof.check orders.csv --min-quality 90 --max-null "*=20"
# exit 0 = pass, 1 = fail, 2 = inconclusive or bad input
```

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

let report = Profiler::new().analyze_file("orders.csv")?;
let result = QualityPolicy::new().min_quality_score(90.0).evaluate(&report)?;
assert_eq!(result.verdict, Verdict::Pass);
```

Minimum supported Rust: 1.96.

## What you can count on

- **Same data, same numbers.** CSV, Parquet, pandas, polars and Arrow of the same values produce the same profile, on every engine. CI checks it.
- **Bounded memory.** Files larger than RAM stream through fixed-size accumulators.
- **Honest verdicts.** A sampled or partial scan never passes a claim about the whole file; scores carry their confidence interval.
- **Absence is not zero.** A metric that wasn't computed is `None`, never a plausible default.
- **Private by default.** Reports and summaries carry counts and patterns, not your values.

## Status

Beta, and moving fast toward a stable 1.0 contract. Some things are still narrow, and we'd rather tell you than have you find out: numbers written with a decimal comma are detected and flagged but not yet parsed, and markers like `NA` or `N/A` aren't treated as nulls yet. The [release notes](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/release-notes.md) list what changed and what's known.

Found something wrong? [Open an issue](https://github.com/AndreaBozzo/dataprof/issues). A file that profiles badly is the most useful bug report there is.

## Learn more

- [Why dataprof?](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/why-dataprof.md): an honest comparison with pandas, polars and ydata-profiling, including when to pick them instead
- [Getting started](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/getting-started.md): the metrics and how to read them
- [Python API](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/python/README.md): every function, the gate and the CI entrypoint
- [Examples](https://github.com/AndreaBozzo/dataprof/blob/HEAD/examples/README.md): a messy CSV, an ETL gate, before and after cleaning, runnable from a clean checkout
- [Agent workflows](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/agent-workflows.md): AGENTS.md, Cursor rules and Claude Code skills
- [Report schema](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/schema/README.md): the saved document and its guarantees
- [Database connectors](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/guides/database-connectors.md) and [feature flags](https://docs.rs/dataprof): Rust-side options
- [Changelog](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/CHANGELOG.md) and [contributing](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/CONTRIBUTING.md)

## Citing dataprof

Use **Cite this repository** in the GitHub sidebar, which builds APA and BibTeX from [CITATION.cff](https://github.com/AndreaBozzo/dataprof/blob/HEAD/CITATION.cff). The citation is for the software; benchmark material lives in [scalcom2026-dataprof](https://github.com/AndreaBozzo/scalcom2026-dataprof).

## License

Either the [MIT License](https://github.com/AndreaBozzo/dataprof/blob/HEAD/LICENSE) or the [Apache License, Version 2.0](https://github.com/AndreaBozzo/dataprof/blob/HEAD/LICENSE-APACHE), at your option.
