# Benchmark suites

`benches/` is the home for benchmark definitions and their optional dependencies.
The Rust and Python runners measure different boundaries, but share the
**Benchmarks** workflow, the `benchmark-results` artifact, and the website's
`/benchmarks/` page. `.github/scripts/` owns execution and publishing automation.
There is no second benchmark source directory or separate publishing workflow.

| Suite | Definition / runner | Raw results | Website |
| --- | --- | --- | --- |
| Rust profiling | `benchmarks.rs`, `cargo bench --bench benchmarks` | `target/criterion/` | Existing Criterion group URLs |
| Python tool comparison (#401) | `.github/scripts/benchmark_comparison.py`, environment in `benches/pyproject.toml` and `uv.lock` | `benchmark-results/comparison/` | `/benchmarks/#comparison`, downloadable JSON and table |
| Python/Arrow boundaries (#698) | `.github/scripts/benchmark_boundaries.py`, same optional environment | `benchmark-results/boundaries/` | `/benchmarks/#boundaries`, stage table and raw evidence |

`benchmarks.rs` only registers the Criterion groups. `scenarios/csv.rs` owns CSV
scan, report assembly and row-count estimation; `scenarios/scaling.rs` owns scaling
and larger inputs. `support/mod.rs` owns deterministic fixtures and reusable
operations. Add a module under `scenarios/` for a new workload family and register
its function in the entry point. No new framework or workspace crate is needed.
Use `CsvFixture::with_rows(n)` for new sizes without changing existing size IDs;
add separate fixture builders under `support/` for other formats or data shapes.
Fixtures are generated before timing into private temporary files, flushed, and
removed when the owning scenario finishes. They never reuse a shared filename
from a previous run, and filesystem errors cannot become zero-byte throughput.
The generator's contents and existing Criterion IDs are unchanged.

For a fast correctness smoke check of every registered Rust case:

```bash
cargo bench --bench benchmarks -- --test
cargo test --test benchmark_fixtures
```

The existing Rust scenarios stay `csv_parsing`, `full_analysis`,
`throughput_metrics`, `scaling_behavior`, and `large_scale`. Routine CI retains
the `csv_parsing|full_analysis` filter and adds a bounded comparison run (1,000
rows, three samples, one warmup). The workflow's existing `full_benchmark` input
enables every Rust scenario and the default comparison run.

## Run the comparison

From the repository root, with uv and the repository's Rust toolchain installed:

```bash
uv run --project benches --locked --reinstall-package dataprof python .github/scripts/benchmark_comparison.py
```

This builds the current checkout as a release wheel, installs only into
`benches/.venv`, generates a deterministic CSV, runs all four tools, prints a
Markdown table, and writes `results.json` and `comparison.md` under
`benchmark-results/comparison/`. The fixture is retained beside them. The
explicit reinstall avoids uv reusing a local-project wheel after Rust source
changes. Cargo still reuses compiled dependencies. Build/install time is outside
the benchmark. No benchmark dependencies enter the published wheel.

Use a new output directory for each run. To assess repeatability on the same
idle machine:

```bash
uv run --project benches --locked python .github/scripts/benchmark_comparison.py --output benchmark-results/repeat --compare benchmark-results/comparison/results.json
```

The second command deliberately reuses the same installed binary. `--compare`
requires matching fixture, environment fingerprints (including the recorded Git
commit and working-tree status), and configuration, then
reports whether each pair of interquartile ranges overlaps. A disjoint result
is evidence of noise or drift, not a performance conclusion. It retains all
outliers; IQR is dispersion, not a confidence interval. No harness can guarantee
overlap on a busy or thermally changing host. Increase `--iterations` and inspect
raw samples before drawing conclusions. Shared CI runners provide smoke evidence,
not a controlled performance baseline.

### Publication experiment budget

Use `--publication --host-description "host identity; load, power and thermal controls"`
for a larger experiment: by default, 21 samples per condition in each of three
serial process blocks. Each block creates fresh cold workers and a fresh warm
worker per tool. `--iterations` and `--blocks` can tune that budget (publication
mode requires at least seven samples per block and two blocks). Seven or 21
samples do not guarantee precision. Operations within one warm block share a
process; blocks share the machine and OS caches, so they are not independent
hosts. Raw observations and per-block summaries are retained alongside pooled
summaries. Routine CI remains at three samples in one block.

```bash
uv run --project benches --locked python .github/scripts/benchmark_comparison.py --publication --host-description "lab-01; idle; AC power; fixed power profile" --output benchmark-results/publication-a
uv run --project benches --locked python .github/scripts/benchmark_comparison.py --publication --host-description "lab-01; idle; AC power; fixed power profile" --output benchmark-results/publication-b --compare benchmark-results/publication-a/results.json
```

Keep the checkout, installed binaries, fixture, configuration and host controls
unchanged between repeats. `--compare` rejects unknown or architecture-only CPU
identifiers even when both runs contain the same placeholder. CPU detection uses
the OS model identifier when `platform.processor()` supplies only an architecture.
The website labels all automated results **diagnostic, no established baseline**,
including publication experiments. Establishing a baseline requires reviewing
ordered samples and across-run stability on the declared host; IQR overlap is
not a significance test and is never promoted automatically to baseline status.

See the [#738 repeat-run demonstration](evidence/738/README.md) for two complete
four-tool runs, downloadable raw artifacts and an interpretation of their limits.

## Measurement contract

- **Cold** means a fresh process for each sample. The parent measures interpreter
  startup, tool imports, CSV read, summary computation, observation export, IPC,
  and process exit. The worker's operation-only timing is also retained.
- **Import/setup** is measured directly in each worker around the adapter import
  and callable construction. It excludes interpreter and harness imports; any
  deferred import inside the callable remains in operation time. Preflight records
  the first adapter import after this harness's environment preparation. Build,
  install or earlier workflow preflights can already have warmed library pages.
  The first fixture operation after preflight is labeled separately from subsequent
  fresh processes. All observations retain block, round and execution order; the
  first sample is never discarded.
- **First operation** is the first CSV read, summary and observation in every
  worker, including first-use initialization. For warm workers this is the first
  warmup; warmup timings are retained separately from measured steady-state samples.
- **Minimal-worker control** runs before each cold round with the same interpreter,
  thread environment and parent timer. It imports only JSON/OS/system support and
  measures interpreter startup, minimal IPC and exit, without the benchmark
  harness or tools. It is a separate observation, not subtracted from independent
  medians to manufacture an import estimate. Unseparated process costs remain
  labeled as combined costs.
- **Warm** uses a fresh worker per tool per block, imports once, performs warmups,
  then times repeated CSV reads and fresh summaries. Inputs and reports are never
  reused. Garbage collection occurs before each sample outside the operation
  timer; automatic GC stays enabled.
- **File cache** is primed before each worker by default. Fresh-process results
  therefore do not claim cold storage. On supporting POSIX hosts,
  `--cold-cache evict` requests per-file eviction with `POSIX_FADV_DONTNEED` after
  syncing the fixture. This is advisory and explicitly recorded as unverified.
  Unsupported hosts fail instead of silently changing conditions. Neither mode
  evicts shared library pages or changes system-wide cache policy. Warm runs
  always prime the fixture and perform warmups.
- **Scheduling** is serial, with cold tools shuffled within each round using a
  recorded seed. Requested thread limits default to one, are passed before tool
  imports, and are recorded; they are not a promise that every native dependency
  obeys the same limit.
- **Statistics** retain every sample and report median, inclusive Q1/Q3, IQR,
  minimum, and maximum. Ratios use the named `--reference` (pandas by default);
  ratios below one are displayed as slowdowns.
- **Provenance** includes all installed distribution versions, the environment
  lock hash, fixture bytes and SHA-256, host CPU/RAM/OS/architecture, Python,
  available Rust compiler, checkout commit/status, script hash, Cargo lock hash,
  native extension hash, and build environment overrides. The compiler field
  identifies the available toolchain; the native hash identifies the binary.

| Tool | Measured workload |
| --- | --- |
| dataprof | `profile(engine="auto", metrics=["schema", "statistics"])` |
| pandas | `read_csv`, `describe(include="all")`, null counts |
| polars | `read_csv`, `describe`, null counts |
| ydata-profiling | pandas `read_csv`, fresh `ProfileReport(minimal=True).description_set` |

Every operation verifies row count, column order, and null counts against the
generated fixture. Failures and timeouts abort without publishing a successful
table. These checks establish basic input integrity, **not equivalent metric
coverage**. The tools compute different summaries; their ratios compare these
named workloads, not interchangeable profilers. The
[ydata minimal configuration](https://docs.profiling.ydata.ai/4.6/features/big_data/)
disables expensive computations, and
[`description_set`](https://docs.profiling.ydata.ai/4.7/features/profile_values/)
forces the otherwise lazy summary. HTML report rendering is outside all workloads.

## Energy and peak memory

Add `--resources` to the same runner. Dependencies remain in the optional
benchmark environment; this adds nothing to the dataprof library or wheel.
For a bounded collection smoke test (all four tools, 100 rows, two repeats):

```bash
uv run --project benches --locked python .github/scripts/benchmark_comparison.py --resources --rows 100 --iterations 2 --blocks 2 --warmups 1 --output benchmark-results/resources-smoke
```

The command produces the existing timing artifacts plus `runs[].resources`
and `resource_results` in `results.json`, and separate resource tables in
`comparison.md`. The additive `config.resources.protocol_version = 1`
identifies this protocol; absent resource fields in older artifacts mean
not collected. The website links the raw evidence and resource tables and
labels the collection boundary alongside its timing charts.

Publishable measurements require controlled local hardware, repeated runs and
review of their dispersion. For example, replace the host descriptions with
the controls actually applied on your machine:

```bash
uv run --project benches --locked python .github/scripts/benchmark_comparison.py --resources --publication --host-description "lab-01; fixed performance profile; thermally settled" --power-source AC --background-load-policy "dedicated idle host; scheduled tasks disabled" --idle-seconds 1 --output benchmark-results/resources-publication
```

The publication budget retains 21 fresh-process samples and three warm worker
blocks per tool by default. Every warm block includes two warmups followed by
21 timed operations. Energy/RSS observations are **per worker**, so there are
63 cold resource samples and only three warm resource samples per tool.
Increasing `--iterations` does not create more independent warm resource samples;
increase `--blocks` for that. One warm block yields `insufficient_samples`,
not a dispersion estimate. The harness never divides warm worker totals by
iteration count to imply an operation-only resource measurement.

### Energy boundary and counters

- On Linux, discover readable `energy_uj` counters through
  `/sys/class/powercap`, resolving class symlinks and deduplicating aliases.
  `--powercap-root` can select another mounted sysfs root. Record every zone's
  canonical counter path, name, unit and `max_energy_range_uj`. Package, core,
  DRAM and platform zones remain separate: parent and child zones can overlap
  and are **never summed**. Counter scope is whatever that hardware zone covers,
  not the benchmark process, and package energy is not whole-system energy.
- A parent thread reads each zone immediately before worker launch, every
  `--energy-poll-seconds` (default 0.05), and after process exit. Every integer
  reading and its monotonic read-start/read-end timestamps are retained. The
  boundary includes interpreter startup, imports, all operations (including
  warmups), IPC, exit, and counter-reading overhead. Host background activity
  and the collector consume energy within the same zones. The timing timer
  excludes the paired idle wait; collector overhead still perturbs an opted-in
  run, so compare runs with matching resource settings.
- Sum `(after - before) modulo max_energy_range_uj` over consecutive readings.
  This handles repeated wraps across a long run only when each interval is
  short enough to exclude an unseen full cycle. The declared
  `--max-zone-watts` upper bound (default **10,000 W per zone**) is an assumption,
  not a discovered hardware limit. An interval whose duration times that bound
  can cover a full counter range is unavailable; so is a delta exceeding the
  bound. The check uses actual timestamps, including delayed polls, rather than
  assuming the polling thread ran on schedule. Choose a conservative bound for
  the hardware and a short enough polling interval. No counter reset may occur
  during measurement: the ABI cannot always distinguish resets from wraps.
  In particular, the defaults are deliberately conservative and do not guarantee
  usable readings on every counter. At 10,000 W, a 0.05-second interval needs a
  range greater than 500,000,000 microjoules (500 J). For an illustrative 262 J
  range, the interval must be below 0.0262 seconds: try
  `--energy-poll-seconds 0.01`, allowing margin for scheduling delays. Inspect
  the actual range and rejected intervals in the raw artifact; decrease the
  power bound only with a hardware-justified upper bound, never merely to make
  a rejected reading pass. The same check applies to idle-baseline polling.
- Before each worker, collect a paired idle interval using the same counters
  and polling method (`--idle-seconds`, default 0.25). This is an observation of
  the host while the benchmark worker is absent, not a guarantee the host was
  idle. Keep gross energy and compute the signed estimate
  `gross_uj - (idle_uj / idle_seconds) * measured_seconds`, using actual per-zone
  durations. Negative estimates remain negative; subtraction is not proof of
  process attribution. The raw paired baseline remains available for audit.
- Unavailable, inaccessible or malformed counters and read failures have
  explicit status/reason fields, never zero energy. Without readable counters,
  the command still succeeds and skips the idle wait. A failed interval
  invalidates that zone's entire worker measurement; no partial total is
  published. Any missing worker measurement suppresses that zone's aggregate,
  while retaining the successful raw samples and the expected sample count.

The source interface is the Linux kernel's
[Power Capping Framework](https://docs.kernel.org/power/powercap/powercap.html).
Short operations may be below counter update resolution; a measured zero is
possible and must not be promoted to a zero-energy claim. Longer repeated
workloads and controlled-host validation are needed before interpretation.

### Peak resident memory and provenance

For **every tool**, collect the OS worker-process high-water mark after its last
operation: Linux `getrusage(RUSAGE_SELF).ru_maxrss` in KiB converted to bytes,
macOS `ru_maxrss` already in bytes, Windows `PeakWorkingSetSize` through the
pinned psutil `peak_wset` field in bytes. This includes native Arrow buffers,
interpreter, imports, collector setup, and retained allocator memory. It excludes
child processes and final JSON serialization/exit; it is neither Python-only
allocation accounting nor a process-tree peak. Warm-worker peaks include
warmups and all iterations and are not reset between operations. Unsupported
platforms report unavailable rather than substituting a sampled RSS maximum.
The OS definitions have platform differences: compare tools on the same host.

Raw samples and complete repeated-worker aggregates retain median, inclusive
Q1/Q3, IQR, minimum and maximum. Gross and idle-adjusted energy remain separate,
as do cold and warm worker conditions. Existing fixture, binary, dependency,
CPU, architecture and OS fingerprints apply. The collector source is hashed;
resource provenance also records kernel version, observed Linux power-supply
online state and governor/minimum/maximum frequency policy where accessible,
plus the user's declared power source and background-load policy. Empty policy
mappings mean unknown. Publication mode requires explicit power/load declarations
and a host description; the harness records but does not enforce those controls.

GitHub-hosted CI runs only the bounded collection smoke test. Its observations
remain diagnostic even if counters happen to be readable. Run the synthetic
counter/unit tests and real-worker unavailable-counter test locally with:

```bash
uv run --no-sync pytest python/tests/test_benchmark_resources.py -q
```

## Import preflight and failed runs

Every comparison checks the pinned environment and imports each selected adapter
in a disposable subprocess before creating the fixture or taking measurements.
Preflight resolves the same imports as the measured worker, without invoking its
operation. To check the environment alone:

```bash
uv run --project benches --locked python .github/scripts/benchmark_comparison.py --preflight-only --output benchmark-results/preflight
```

CI installs the environment and runs this check before building or measuring the
Rust benchmark suite. The comparison repeats preflight immediately before its
own measurements. Neither check is timed as a benchmark sample. Subprocesses
isolate Python module state, but imports can warm OS library pages and persistent
library caches. Those caches are **not evicted**. Fresh-process samples are
therefore subsequent to environment preparation and preflight, not the first
invocation on an untouched host. This policy is recorded in `config.preflight`,
and each successful preflight worker's PID and diagnostics are retained. Fixture
cache policy is separate and unchanged. The import/setup timer described above
separates adapter setup from the overall preflight process duration; neither
should be interpreted as a pure import measurement.

`setuptools==81.0.0` remains pinned because ydata-profiling 4.18.4 imports
`pkg_resources`, which [setuptools removed in 82.0.0](https://setuptools.pypa.io/en/latest/deprecated/pkg_resources.html).
Dependabot continues maintaining `/benches`, but ignores setuptools 82 and newer.
Remove that bound and upgrade the pin together with a ydata version that no
longer requires `pkg_resources`, verified by the import preflight and comparison
smoke run. Do not merge a standalone setuptools upgrade across this boundary.

`progress.json` is an atomic checkpoint after each completed worker. On failure
it keeps completed raw observations, environment and fixture metadata when
available, an explicit `incomplete` status, and the failed tool/stage/iteration.
Failure diagnostics include the traceback and worker stdout/stderr/exit code;
timeouts retain output captured before termination. An interrupted process leaves
the last checkpoint incomplete with its active stage. Observations from a worker
that did not complete are not promoted to successful measurements. No failed or
missing tool gets a zero-time row. Successful runs still export `results.json`
and `comparison.md`; incomplete runs do not produce a successful comparison.

CI always attempts to upload `benchmark-results/` and `target/criterion/`, even
after failure. `run-status.json` identifies failed or skipped workflow stages;
logs retain dependency, build, Rust and Python diagnostics. A Python failure
therefore preserves completed Criterion output. The job stays failed. Pages
only selects successful workflow runs, and the page builder also rejects
incomplete status files or missing requested tools. Earlier successful artifacts
without status files remain supported.

Exercise real workers, broken imports, partial-run retention and publication
filtering without rebuilding Rust:

```bash
uv run --no-sync pytest python/tests/test_benchmark_comparison.py python/tests/test_benchmark_pages.py python/tests/test_benchmark_workflow.py -q
```

## Python/Arrow boundary experiment

This suite attributes the cost of a Python profiling call across producer
construction, the public profiling call, and report export. It uses the same
locked environment, subprocess isolation, thread controls, fingerprints,
inclusive quartiles and failure checkpoints as the comparison harness. It does
not compare different tools' statistical implementations: all producers feed
dataprof with `metrics=["schema", "statistics"]`.

```bash
uv run --project benches --locked --reinstall-package dataprof python .github/scripts/benchmark_boundaries.py
# Bounded CI-sized run, reusing that installed wheel:
uv run --project benches --no-sync python .github/scripts/benchmark_boundaries.py --rows 1000 --chunks 250 --iterations 2 --output benchmark-results/boundaries-smoke
```

Use a new output directory per run. The default budget is 8,192 rows, chunks of
256, 2,048 and 8,192 rows, offsets zero and three, three fresh-process samples
and three measured operations after one warmup in a separate warm worker per
case. One stream case grows total input fourfold with the smallest batch size
held fixed. The runner enforces a 600-second total worker budget, a 60-second
worker timeout and at most one million rows including the stream scale. Every
budget and host-control declaration is retained in `config`. CI uses the smaller
command above with a 300-second budget; no cloud service or large file is needed.

The versioned fixture contains int64 values above 2^53, quarter-step floats,
Unicode strings and nulls. Its logical rows are hashed independently of chunking.
The matrix includes C Array RecordBatches, chunked PyArrow Tables, Arrow-backed
pandas DataFrames, Polars DataFrames without rechunking, and lazy C Streams.
Pandas preparation starts from typed Arrow batches and calls
`to_pandas(types_mapper=pandas.ArrowDtype)`; Polars calls
`from_arrow(rechunk=False)`. These are declared producer constructions, not a
claim about NumPy-backed pandas or arbitrary object columns. PyArrow slices
retain nonzero array offsets; pandas/Polars may normalize their representation.
C Array cells with multiple chunks are explicit skips: that protocol exports
one RecordBatch. Nested types and unsupported protocols are outside this primitive
matrix; an unexpected import, decode or validation error fails the run, never
becomes a zero-duration success or an automatic skip.

Every supported cell passes a disposable preflight before measurement. It checks
producer values (including exact int64 preservation), nulls, order and Arrow
offsets, then compares **all serialized column metrics, including absence**
against a single-batch reference. The reference additionally checks counts and
exact distinct counts from the fixture. Metrics are compared exactly, with no
tolerance or missing-field substitution. This establishes path parity, not
independent correctness of every statistical formula. Quality is unrequested
and must remain absent. Each measured operation repeats the report checks outside
the operation timer; JSON and dict exports must agree. Reference/preflight
workers cannot inflate the measured stream workers' RSS.

| Field in each sample's `seconds` | Measured boundary |
| --- | --- |
| `prepare` | Generate Arrow batches and construct the selected producer; streams construct only a reader |
| `import_profile` | `dataprof.profile()`: Python adapter, consumer import and profiling together |
| `export_dict` | `report.to_dict()` |
| `export_json` | `report.to_json()`, including its independent document construction |
| `end_to_end` | Preparation through both exports, excluding imports, GC and validation |

There is no public import-only operation that would preserve this behavior, so
`import_profile` must not be called profiler-core time. For streams,
`producer.batch_preparation_seconds` records lazy batch construction **inside**
that combined timer. It is nested evidence, not another additive stage; no
independent medians are subtracted to invent a consumer-import estimate. Deferred
library imports inside preparation/profiling stay in those timings. Direct
imports are recorded as `import_setup_seconds` in each worker.

The parent also records `process_seconds`, which includes interpreter startup,
imports, validation, IPC and exit. Warm process time covers warmups and all
iterations. Fresh workers are fresh processes, not cold storage or an untouched
host: imports and preflight can warm library pages. The serial schedule is shuffled
with a recorded seed; every sample, warmup, worker PID, execution order and
diagnostic survives in the artifact. A single warm worker per case gives
within-process dispersion, not across-process stability or independent hosts.

`peak_rss` reuses the OS high-water collector, including native buffers, imports
and all worker operations. `producer.observed_arrow_pool_bytes` samples the Arrow
pool after each generated batch; it is neither a continuous peak nor a measurement
of all native allocations. Batch count, maximum rows/bytes and produced rows are
also retained. Streams generate one batch on demand and keep no full table;
their buffer observations can be compared as total rows grow. RSS cannot isolate
profiler accumulators, and allocator observations do not establish an asymptotic
memory bound. Python allocation counters are not used as a substitute for native
memory evidence.

Schema-v1 `results.json` retains configuration, logical fixture fingerprints,
installed versions/native hashes, harness hashes, references, preflights, raw
worker runs, and per-case fresh/warm stage summaries (median, Q1/Q3, IQR, min/max).
`boundaries.md` presents the scope and stage table. Incomplete runs retain
`progress.json` and diagnostics without publishing results. Output files are staged
and promoted before the checkpoint is marked complete; failed publication removes
partial outputs. The publisher recomputes every summary statistic from its raw
samples and rejects missing or contradictory fields. The existing workflow
and Pages renderer retain this suite alongside the other artifacts; older runs
without it remain valid. Invalid present results fail publication.

See the [#698 recorded experiment](evidence/698/README.md) for raw evidence and
the bounded attribution result. Tests:

```bash
uv run --no-sync pytest python/tests/test_benchmark_boundaries.py python/tests/test_benchmark_pages.py python/tests/test_benchmark_workflow.py -q
```

## Expansion and publishing rules

Extend this inventory when adding a scenario; do not introduce another root
directory, workflow, or website. Keep Criterion results in their native format
and preserve their existing deep links. Additional measurement suites write to
`benchmark-results/<suite>/` and join the same artifact. Extend
`build_benchmark_pages.py` with a renderer for their documented result schema;
do not pretend a different statistic is a Criterion mean. Missing suites in older
artifacts remain absent, not fabricated zeros. Invalid present results fail the
page build rather than publishing misleading numbers.

Planned work stays in its tickets:

| Ticket | Extension point |
| --- | --- |
| #404 cross-tool metric parity | Workload definitions and correctness checks before fair speed comparisons |
| #402 ablation | Additional named workloads with the same repeated-run provenance |
| #697 Parquet physical reads | A measurement suite for physical I/O and accumulator work |
| #405 auto-engine investigation | Controlled workloads and before/after evidence |

Do not merge their data into one unlabeled timing table. Add result schema versions
when measurement meanings change, and test the artifact-to-page path whenever a
new suite becomes publishable.
