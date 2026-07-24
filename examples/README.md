# Examples

Three scenarios, each written twice — once in Rust, once in Python — so you can
read whichever matches the codebase you are working in. Both versions profile the
same data and agree on every number they both report. The Python versions show a
little more, because the Python API has `quality_summary()` and `compare()` where
the Rust side leaves the diffing to you.

Every example generates its own dataset in a temporary directory, so they run
from a clean checkout with no fixtures to download and nothing left behind.

## The scenarios

### 1. Messy CSV inspection

A partner sends you `orders.csv`. Is `order_id` really unique? Is `customer_email`
populated? Why are the finance team's totals wrong? One pass answers all three:
null-heavy columns, a duplicated key, a negative amount in a positive-only column,
a cell that does not match its column's type, and PII that gets *flagged* but
never printed.

```bash
cargo run --example messy_csv_inspection
uv run python python/examples/messy_csv_inspection.py
```

### 2. ETL quality gate

A daily drop lands in a staging bucket. Stop the bad file before it reaches the
warehouse, and put the reason in the log. The gate is a plain function over a
`ProfileReport` — required columns, a minimum quality score, a missing-cell
allowance, key uniqueness, and no negative values where negatives are impossible —
so it composes into Airflow, Dagster, or a shell script.

Both examples profile a good file and a bad one and print both verdicts, so they
exit 0. A real gate would exit non-zero on rejection.

```bash
cargo run --example etl_quality_gate
uv run python python/examples/etl_quality_gate.py
```

### 3. Before/after cleaning comparison

You wrote a cleaning script. Did it help? Profile the raw file, persist that
report as JSON, profile the cleaned file, and diff the two. The saved baseline is
the point: weeks later you can still answer "what did this data look like when we
onboarded it?" without keeping the raw file around.

Watch the `accuracy` dimension go *down* while the overall score goes up — a
reminder that a single number can hide a regression, and that you should assert on
the specific defects you set out to fix.

```bash
cargo run --example before_after_cleaning
uv run python python/examples/before_after_cleaning.py
```

## Running them

The Rust examples need nothing beyond the default feature set:

```bash
cargo run --example messy_csv_inspection
```

The Python examples need the extension module built into your environment:

```bash
uv sync
uv run maturin develop
uv run python python/examples/messy_csv_inspection.py
```

They use only the base wheel API, so `pip install dataprof` is enough outside a
development checkout — no pandas, no source build.

## A note on the numbers

These examples print real profiler output, not illustrative constants. If you
change a metric's semantics, the examples change with it. CI runs all six on every
push for exactly that reason.

Two results surprise people, so both examples call them out explicitly:

- **`completeness` scores complete *records***, not populated cells. A file where
  22% of cells are missing can score 0 if almost every row is missing something.
  Optional-column semantics are tracked in
  [#436](https://github.com/AndreaBozzo/dataprof/issues/436); inspect
  `missing_values_ratio` and `null_columns` beside the strict record ratio.
- **`null_columns` lists columns past the null threshold** (50% by default), not
  only columns that are entirely null.

## Other data in this directory

`test_data/` and `test_datasets/` hold fixtures used by the test suite. They are
not part of the examples.
