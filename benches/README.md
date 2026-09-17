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

## Measurement contract

- **Cold** means a fresh process for each sample. The parent measures interpreter
  startup, tool imports, CSV read, summary computation, observation export, IPC,
  and process exit. The worker's operation-only timing is also retained.
- **Warm** uses a fresh worker per tool, imports once, performs unmeasured warmups,
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
| #698 Python/Arrow boundaries | Stage timings, chunk/batch matrix, and optional producers |
| #440 energy and peak memory | Documented instrumentation and units alongside timing samples |
| #405 auto-engine investigation | Controlled workloads and before/after evidence |

Do not merge their data into one unlabeled timing table. Add result schema versions
when measurement meanings change, and test the artifact-to-page path whenever a
new suite becomes publishable.
