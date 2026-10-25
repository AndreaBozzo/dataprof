# Examples

Four scenarios. The first three are written twice — once in Rust, once in
Python — so you can read whichever matches the codebase you are working in. Both
versions profile the same data and agree on every number they both report. The
Python versions show a little more, because the Python API has
`quality_summary()` and `compare()` where the Rust side leaves the diffing to
you. The fourth, a checkpoint against the last accepted run, is in Python only.

Every example generates its own dataset in a temporary directory, so they run
from a clean checkout with nothing to download and nothing left behind. The one
committed file is the report an older release wrote, which the fourth example
reads as a baseline.

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
warehouse, and put the reason in the log. The policy is declared as data — a
minimum quality score, per-column null allowances, no duplicate rows, and the
metrics that must have been analyzed at all — and evaluating it returns a
structured result rather than printing or exiting, so it composes into Airflow,
Dagster, or a shell script.

Watch the fourth drop: it is the third one read under a row cap, so the scan
never reaches the duplicate. The verdict is `inconclusive`, not `pass`. A clean
prefix is not a clean source, and a gate that cannot tell should not say yes.

Both examples profile four drops and print all three verdicts, so they exit 0.
A real gate would exit non-zero on anything but a pass.

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

### 4. Baseline checkpoint

The same daily drop, now judged against the last report the pipeline accepted
as well as against absolute thresholds. Absolute limits catch a broken file;
only a comparison catches a slow slide, such as a key column going from 0% to
15% empty under a 20% limit. Each run writes `candidate.json` whatever the
verdict, and only a pass renames it onto `baseline.json`, so a failing or
inconclusive run leaves the accepted baseline as it was and its own evidence
beside the verdict.

Seven drops cover the cases a pipeline meets:

1. a broken first drop, which must not become the baseline;
2. the first clean drop, judged on absolute rules alone because no baseline
   exists yet, which creates it;
3. a clean drop judged against it;
4. the slow slide, which fails only the rules about change;
5. a new column: the score now measures a different column set, so its rule is
   unevaluated (`schema_changed`) and the run is inconclusive, until a person
   accepts the new shape and refreshes the baseline on purpose;
6. a run that did not analyze quality, whose score rules are unevaluated rather
   than passing;
7. a baseline written by 0.12.0, committed in `python/examples/data/`: the data
   has not changed, yet each rule about change is unevaluated
   (`definition_not_comparable`), because that release did not record how it
   read null markers.

```bash
uv run python python/examples/baseline_checkpoint.py
```

#### The same checkpoint as a CI job

Where the baseline lives decides how it is refreshed. In a pipeline that keeps
its own state, promote the candidate on a pass, as the example does. In a
repository, keep it at `quality/baseline.json` beside the policy, so that
changing it is a commit: the job below never writes it. To accept a run,
download its `candidate.json` and commit it as `quality/baseline.json` in a pull
request, whose review is the refresh. The first run has no baseline, and a rule
about change without one is a policy error (exit 3), so the job applies the
absolute rules alone until the first baseline is committed.

```yaml
name: Data quality
on:
  schedule:
    - cron: '0 6 * * *'
  workflow_dispatch:
jobs:
  checkpoint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7.0.1
        with:
          persist-credentials: false
      - uses: actions/setup-python@v7
        with:
          python-version: '3.12'
      - run: python -m pip install dataprof==0.13.0
      - run: ./fetch_drop.sh orders.csv  # your extract step
      - name: Check against the accepted baseline
        run: |
          if [ -f quality/baseline.json ]; then
            python -m dataprof.check orders.csv --policy quality/policy.json \
              --baseline quality/baseline.json --max-quality-drop 2 \
              --max-null-increase '*=5' --forbid-removed-columns \
              --report candidate.json --json > verdict.json
          else
            python -m dataprof.check orders.csv --policy quality/policy.json \
              --report candidate.json --json > verdict.json
          fi
      - uses: actions/upload-artifact@v7
        if: always()
        with:
          name: quality-evidence
          path: |
            verdict.json
            candidate.json
```

The rules about change need dataprof 0.13 or later.
`quality/policy.json` holds the absolute rules, with the same keys as
`check()`; the example's `ABSOLUTE` dictionary written as JSON is a working
one. The step exits 0 on a pass, 1 on a fail and 2 when inconclusive, so the job
fails on anything but a pass and still uploads its evidence.

For a full pipeline around the same idea, with dlt loading into DuckDB and a
batch published only when accepted, see
[dataprof-pipeline-reference](https://github.com/AndreaBozzo/dataprof-pipeline-reference).

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
change a metric's semantics, the examples change with it. CI runs all seven on
every push for exactly that reason.

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
