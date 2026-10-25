# Why dataprof?

dataprof has two jobs: **profile what the data is doing**, then **gate a
pipeline on explicit thresholds over that profile**. A summary helps you
investigate unfamiliar data; a gate states which measured properties a batch
must satisfy before it proceeds.

You may already have the right tool. pandas, Polars and DuckDB summarize data;
ydata-profiling produces visual reports; Great Expectations, Soda, pandera and
dbt tests enforce expectations you define. This page explains when to use each
and where dataprof fits. It makes no performance comparisons.

## What dataprof adds

- **Measurements and findings together.** Types, nulls, distributions and
  quality dimensions describe the data. `findings()` identifies conditions
  worth investigating, with codes and evidence you can inspect. A finding is
  a reason to investigate, not proof that a business requirement was violated.
- **Profile-level gates.** Python's `report.check()`, Rust's `QualityPolicy`
  and the `python -m dataprof.check` CI entrypoint evaluate policies over
  profile metrics, including null percentages and quality dimensions. The
  result distinguishes `pass`, `fail` and `inconclusive`.
- **A report contract across inputs.** Files, supported DataFrames and Arrow
  inputs share a report shape. The [numeric equality contract](../schema/README.md#numeric-equality-contract)
  requires the same serialized metrics across engines and input paths for the
  same logical data, options and analyzed population. It does not equate a
  sample with a full scan or promise parity with another profiling tool.
- **Reports you can retain.** `save()`, `load()` and `compare()` support
  inspecting changes between runs without re-profiling the earlier input.
- **A summary for an agent.** `to_llm_context()` emits shape, provenance
  caveats, quality flags, schema and detected pattern names. Raw cell values
  are redacted by default; detected sensitive-pattern values are never echoed.
  Its token budget is approximate, and the dataset header is always emitted
  even when it exceeds that budget.
- **File profiling without a DataFrame dependency.** The base Python install has no
  Python runtime dependencies. Profiling local files does not require pandas
  or Polars. This is an installation choice, not a memory-use comparison.

Use scores to **triage**, then choose explicit rules for the gate. An overall
score, or a small change in that score, cannot establish that order amounts
are nonnegative or that status values belong to your business vocabulary.
A newly empty optional column can affect a score while a business-critical
defect needs its own rule. Inspect the relevant metrics and use a validation
tool when the rule cannot be expressed by dataprof's profile thresholds.

## What an inconclusive gate means

A partial scan can describe the rows it observed without establishing a null
percentage for the whole source. This self-contained example runs with the
released `dataprof==0.12.0` wheel:

```python
from pathlib import Path
from tempfile import TemporaryDirectory

import dataprof as dp

with TemporaryDirectory() as directory:
    source = Path(directory) / "orders.csv"
    source.write_text("customer_id,amount\n1,10\n2,20\n,30\n", encoding="utf-8")

    partial = dp.profile(source, max_rows=2)
    whole_source = partial.check(
        max_null_percentage={"customer_id": 0}, scope="full_source"
    )
    observed_rows = partial.check(
        max_null_percentage={"customer_id": 0}, scope="observed"
    )
    complete = dp.profile(source).check(max_null_percentage={"customer_id": 0})

    print(whole_source.verdict, whole_source.passed)
    print(whole_source.unevaluated[0].reason)
    print(observed_rows.verdict)
    print(complete.verdict)
```

Output:

```text
inconclusive False
{'reason': 'evidence_incomplete', 'gap': 'truncated'}
pass
fail
```

The first two rows have no missing customer ID. The full-source check explains
why that is insufficient evidence; `passed` remains false. Choosing `observed`
narrows the claim to those two rows. It does not establish that the file meets
the requirement. Scanning all three rows finds the missing ID and fails.

Inspect the verdict and individual check reasons: unavailable metrics and
estimated evidence can also prevent a policy from being evaluated. A witnessed
violation takes precedence over an unevaluated check. See the
[quality-gate reference](../python/README.md#check----quality-gates) for scope
and evidence semantics.

Inconclusive outcomes are not unique to dataprof. Soda v4 has a per-check
[`NOT_EVALUATED` outcome](https://github.com/sodadata/soda-core/blob/v4.28.0/soda-core/src/soda_core/contracts/contract_verification.py#L114).
The point of this example is the explanation attached to dataprof's verdict,
not an exclusive feature claim.

## Compared with summaries and visual reports

| Tool | Prefer it when | Where dataprof fits |
|---|---|---|
| [pandas `describe()`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html) / [`info()`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.info.html) | You already have a DataFrame and want column statistics, dtypes or non-null counts. | Add findings, quality dimensions and an explicit profile-threshold verdict when a summary alone does not answer the acceptance question. |
| [Polars `describe()`](https://docs.pola.rs/api/python/stable/reference/dataframe/api/polars.DataFrame.describe.html) | You are inspecting a Polars DataFrame and its summary statistics answer the question. | Use dataprof's report contract when persisting profiles or gating on them. Polars documents `describe()` output as unstable and advises against depending on it programmatically. |
| [DuckDB `SUMMARIZE`](https://duckdb.org/docs/2.0/guides/meta/summarize) | You work in SQL and want statistics over a table or query: null percentages, ranges, approximate distinct counts and quantiles. | Add quality dimensions, findings, saved reports and the cross-engine metric contract described above. This does not imply that DuckDB cannot express a check in SQL. |
| [ydata-profiling](https://docs.profiling.ydata.ai/latest/) | The deliverable is a visual exploratory report to read or share as HTML or in a notebook. | Choose dataprof for its profile-threshold gates and evidence semantics. ydata-profiling also exposes JSON metrics and supports pandas and Spark DataFrames; machine-readable output is not exclusive to dataprof. |

These tools can be enough on their own. A summary showing a column's minimum
does not decide whether negative values are acceptable: that decision comes
from your domain requirements.

## Compared with validation tools

Profiling and validation compose: a profile can reveal where to investigate
and which expectations to write. A rule captures a requirement that cannot be
learned just by observing today's data. Prefer an existing validation workflow
when it already expresses the requirements you need to enforce.

| Tool | What its documented workflow provides | Prefer it when |
|---|---|---|
| [Great Expectations](https://docs.greatexpectations.io/docs/core/introduction/gx_overview/) | Expectations grouped into Suites, Validation Definitions connecting data and suites, and Checkpoints running validations and actions. | You want an expectation-based validation workflow with reusable suites and actions. |
| [Soda](https://docs.soda.io/soda-documentation/reference/contract-language-reference.md) | YAML data contracts with dataset and column checks, thresholds and schema rules. | You want acceptance criteria maintained as data contracts and checked through that workflow. |
| [pandera](https://pandera.readthedocs.io/en/stable/dataframe_schemas.html) | DataFrame schemas plus [checks](https://pandera.readthedocs.io/en/stable/checks.html) on columns and whole frames. | Your requirements belong in Python schemas and validation checks within a DataFrame workflow. |
| [dbt data tests](https://docs.getdbt.com/docs/build/data-tests?version=1.12) | SQL assertions expressed as queries for violating records, with reusable generic tests and custom singular tests. [Severity and thresholds](https://docs.getdbt.com/reference/resource-configs/severity) configure their outcomes. | The data already belongs to a dbt project and tests should live beside its models, sources, seeds or snapshots. |

dataprof fits when the requirement is a threshold over the profile it already
produces, and you want the decision tied to the available evidence. It does
not replace arbitrary row-level expectations or business logic in these tools.
In particular, `check()` has no rule for repeated key values, per-column value
bounds, allowed-value sets or declared column types. Use a validation tool for
those requirements. `max_duplicate_rows` checks repeated whole rows, which
does not establish that a business key is unique.

## Rule of thumb

| Situation | Reach for |
|---|---|
| Inspect statistics in an existing DataFrame or SQL workflow | pandas / Polars `describe()` or DuckDB `SUMMARIZE` |
| Share a visual exploratory report | ydata-profiling |
| Profile a file and investigate findings or changes between saved reports | dataprof |
| Gate on profile metrics with an explicit evidence scope | dataprof |
| Enforce domain rules, schemas or custom assertions | Great Expectations, Soda, pandera or dbt tests, according to your workflow |
| Discover useful checks, then enforce business requirements | Profiling plus explicit validation |

## What dataprof deliberately does not do

- **Transform or clean data.** It reports on data; it does not repair it.
- **Replace a visual analysis report.** Its structured reports and exports
  are not a charting or dashboard system.
- **Provide an arbitrary expectations language.** `QualityPolicy` and
  `check()` are real policies over profile metrics, not a general-purpose
  language for row-level predicates or business rules.
- **Infer your acceptance criteria.** Findings and scores guide investigation.
  A pass means the requested policy was satisfied within its scope; it is not
  a certificate that every possible defect has been ruled out.

Start with [Getting Started](getting-started.md), then choose the
[gate policy](../python/README.md#check----quality-gates) that matches the
decision you need to make.
