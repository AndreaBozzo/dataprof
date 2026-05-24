# Crate Redesign

This note explains the workspace architecture behind dataprof 0.8. The goal is
not to split crates for its own sake. The goal is to make the package easier
to install, understand, test, and embed.

## Current Diagnosis

dataprof grew into several products inside one crate:

- a Rust library API
- a Python package
- a retired CLI lineage for profiling files from the terminal
- format readers for CSV, JSON, JSONL, Parquet, Arrow, and databases
- streaming and async ingestion
- ISO 8000/25012 quality metrics
- benchmark and academic-paper support

That breadth is useful, but it created adoption friction:

- new users do not immediately know whether dataprof is primarily a Rust
   library, a Python package, or a research artifact with tool history attached
- default Rust builds pull in heavy format dependencies even when the user only
  wants simple CSV profiling
- public APIs, Python bindings, retired CLI history, and internal engines used
   to live close together, so changes felt riskier than they should
- the project has many valuable features, but the first-use path is not sharp
  enough

## Design Goals

1. Make the Rust library embeddable without CLI, Python, database, or heavy
   columnar dependencies unless requested.
2. Keep the retired CLI out of the shipped facade so the package reads clearly
   as a library product.
3. Keep Python ergonomics independent from Rust internals where possible.
4. Let format support grow through small, testable crates instead of one
   increasingly crowded parser layer.
5. Keep the public Rust facade compact and stable at the top level while
   treating the old broad module tree and CLI as non-goals for 0.8.
6. Make benchmarks and paper artifacts reproducible without making the main
   package feel research-only.

## Status After The Workspace Split

The workspace now has clear ownership lines:

- `dataprof-core` owns shared classification, execution, source,
   partial-analysis, output, progress, config, and column/profile model types
- `dataprof-metrics` owns `analysis/`, `stats/`, pattern detection,
   validators, the SIMD numeric helper, and the quality metric result types
- `dataprof-runtime` owns `ProfileReport`, report assembly, streaming stats,
   and profile-building helpers that used to live under `src/core/`
- `dataprof-runtime` also owns shared engine-facing runtime helpers such as
   `MemoryConfig` and the async source abstractions used by async streaming
- `dataprof-csv` now owns the standard CSV parser, delimiter detection, and
   CSV report-building flow
- `dataprof-csv` also owns the memory-mapped CSV reader used by the
   incremental engine
- `dataprof-json` now owns both the low-level JSON scanner and the higher-level
   JSON/JSONL report-building flow
- `dataprof-parquet` now owns the synchronous Parquet parser plus the shared
  `RecordBatchAnalyzer` used by Parquet-backed and Arrow-backed batch analysis
- `dataprof-parquet` now also owns async HTTP Range Parquet profiling
- `dataprof-engines` now owns adaptive engine selection plus the incremental
   and async streaming profilers
- `dataprof-partial` owns fast schema inference and quick row-count APIs
- `dataprof-parquet` still owns the Arrow-backed profiler implementation used
   by the columnar engine path
- `dataprof-db` now owns database configuration, connector traits, SQL
  validation/security helpers, retry/sampling/streaming helpers, and the
  feature-gated PostgreSQL, MySQL, and SQLite connectors
- `dataprof-core` now owns `MemoryTracker`
- `dataprof-python` now owns the PyO3 extension module and Python-facing report
  wrappers; `pyproject.toml` points maturin at that crate
- the CLI implementation and CLI-only output module have been removed from the
   facade crate, and the facade no longer carries CLI compatibility feature
   names
- the top-level `dataprof` package now uses `crates/dataprof/src` for its
   facade source and exposes common APIs through top-level re-exports
- the former top-level `src/` shell has been removed instead of carried
   forward as a compatibility layer
- parser, partial-analysis, database, and engine implementation modules are
  accessed through their owning workspace crates when callers need low-level
  control

The important outcome for users is simpler than the crate graph: the top-level
crate is smaller, the Python package has its own home, and optional behavior is
exposed through sharper feature boundaries instead of dead compatibility
surface.

## Current Boundary Status

The current layout is concrete enough to talk about the remaining work in terms
of real boundaries instead of hypothetical ones:

| Area | What is already true | What is still coupled |
| --- | --- | --- |
| Packaging | The repo is now a workspace with `dataprof-core`, `dataprof-metrics`, `dataprof-runtime`, `dataprof-csv`, `dataprof-json`, `dataprof-parquet`, `dataprof-engines`, `dataprof-partial`, `dataprof-db`, and `dataprof-python`, and the facade crate supports meaningful library builds | The facade still owns the high-level `Profiler` orchestration API |
| Shared model | Core report and profiling DTOs live in `dataprof-core`, `MemoryTracker` lives in `dataprof-core`, and `ProfileReport` plus report assembly live in `dataprof-runtime` | The facade still owns top-level API wiring by design |
| Parser boundary | Standard CSV, JSON/JSONL, synchronous Parquet parsing, async HTTP Parquet, and the memory-mapped CSV reader now live outside the facade crate | New format behavior should start in owning crates |
| Metrics boundary | `analysis/`, `stats/`, quality result types, pattern detection, and validators now live in `dataprof-metrics` | Parser and engine code still reach metrics through runtime or facade re-exports rather than a smaller explicit adapter boundary |
| Public API | Existing top-level exports remain available through facade re-exports, including async source types and partial-analysis APIs | `crates/dataprof/src/profiler.rs` remains the facade-owned builder/orchestration surface |
| Database boundary | Database config, connector traits, SQL validation/security helpers, retry/sampling/streaming helpers, and concrete SQL connectors now live in `dataprof-db` | `dataprof-python` depends on the facade/database surface for Python async database bindings |
| Python boundary | PyO3 classes, functions, and extension registration now live in `dataprof-python`; the root facade no longer exposes Python alias features | Python wrappers still depend on the facade API rather than smaller leaf crates directly |
| Product surfaces | The facade crate is now a compact top-level API over workspace crates; engine implementations live in `dataprof-engines` | The high-level `Profiler` builder remains facade-owned by design |

## End-State Workspace

The likely workspace shape is:

| Crate | Role |
| --- | --- |
| `dataprof-core` | Core report types, errors, data model, profiler traits, metric selection, sampling, stop conditions |
| `dataprof-metrics` | ISO 8000/25012 quality metrics, pattern detection, validators, statistical summaries |
| `dataprof-runtime` | Shared runtime composition helpers, report assembly, streaming stats, async source abstractions, and cross-crate profiling adapters |
| `dataprof-csv` | Standard CSV parsing, delimiter detection, robust CSV recovery helpers, diagnostics, memory-mapped CSV reading, and CSV report assembly |
| `dataprof-json` | JSON and JSONL scanning plus JSON report assembly |
| `dataprof-parquet` | Synchronous Parquet profiling plus shared Arrow `RecordBatchAnalyzer` and `ArrowProfiler` support used by Parquet-adjacent paths |
| `dataprof-engines` | Adaptive selection plus incremental and async streaming engine implementations |
| `dataprof-partial` | Fast schema inference and quick row-count APIs for files and async byte streams |
| `dataprof-db` | PostgreSQL, MySQL, SQLite connectors and SQL validation |
| `dataprof-python` | PyO3 extension and Python report wrappers |
| `dataprof` | Compact facade crate for common Rust users, re-exporting stable top-level APIs behind features |

The facade crate should remain the main package people discover. Internally,
the workspace can become more modular without forcing users to understand every
crate on day one.

## Recommended Execution Order

The implemented sequence ended up looking like this:

| Step | Deliverable | Why this order is safer |
| --- | --- | --- |
| 1 | `dataprof-core` | Completed. Shared report types, progress/error/config primitives, and profiling model types now have a stable internal home |
| 2 | `dataprof-metrics` | Completed. `analysis/` and `stats/` proved isolated enough to move behind the new core boundary |
| 3 | `dataprof-runtime`, `dataprof-csv`, and full `dataprof-json` ownership | Completed. Shared report-building helpers moved first, then standard CSV and JSON parser ownership followed |
| 4 | `dataprof-parquet` | Completed. The synchronous Parquet reader, async HTTP Parquet, and shared `RecordBatchAnalyzer` moved behind a new crate |
| 5 | `dataprof-db`, `dataprof-python`, `dataprof-engines`, `dataprof-partial`, facade polish | Completed for this redesign slice. Database, Python, engine, partial-analysis, async HTTP Parquet, CLI retirement, and removal of the old `src/` shell are in place; remaining work is release validation |

## Feature Boundary Proposal

In the current layout, `--no-default-features` no longer pulls in Arrow,
Parquet, engine optional dependencies, or database connector dependencies. The
main product boundaries now live at crate level; the remaining work is release
validation:

| Feature | Intended dependency boundary |
| --- | --- |
| default | Library-friendly default feature set; no CLI binary |
| always-on | CSV plus JSON/JSONL parsing through lightweight workspace crates |
| `arrow` | Columnar engine internals, not the whole public facade |
| `parquet` | Parquet reader plus Arrow-backed dependencies |
| `async-streaming` | Tokio-based async profiling and streaming runtime support |
| `parquet-async` | Async HTTP Parquet support layered on `parquet` and `async-streaming` |
| `database` | `dataprof-db` facade dependency without concrete SQL backend dependencies |
| `postgres`, `mysql`, `sqlite` | Individual SQL backends through `dataprof-db` features |
| `all-db` | Convenience bundle for all three SQL backends |

The facade crate now optimizes for `dataprof = "0.8"` as a Rust dependency. A
CLI could return later as a separate product if it becomes useful, but it is
not part of the shipped 0.8 surface.

## Migration Plan

### Phase 0: Packaging Baseline

Completed in the 0.8 workspace:

- Turn Arrow and Parquet into optional dependencies.
- Make unsupported backend paths fail explicitly at runtime.
- Remove the former CLI binary and command modules from the facade crate.
- Remove old `cli`/`full-cli` compatibility aliases from the facade crate.

### Phase 1: Extract `dataprof-core`

Completed in the current workspace:

- shared classification, execution, output, partial-analysis, source, and
   column/profile model types now live in `dataprof-core`
- stable config primitives (`IsoQualityConfig`) and progress/error types are
   available through the same core boundary
- the facade crate re-exports those items as top-level API

### Phase 2: Move Metrics Behind The Core Boundary

Completed in the current workspace:

- `analysis/`, `stats/`, pattern detection, validators, and quality metric
   result types now live in `dataprof-metrics`
- the facade crate exposes common analysis/statistics helpers as top-level
   re-exports rather than owning the implementations directly
- the workspace has been revalidated against the facade library build matrix after
   the split, though broader public-API compile-test coverage still needs to be
   formalized

### Phase 3: Split Reader Crates

Completed for the extracted reader crates in the current workspace:

- `dataprof-runtime` now owns the shared report-building and streaming helpers
   that used to block parser extraction
- `dataprof-csv` owns the standard CSV parser, delimiter detection flow,
  robust CSV recovery helpers, and `CsvDiagnostics`
- `dataprof-json` owns both the JSON scanner and the higher-level JSON/JSONL
   report-building API
- `dataprof-parquet` owns the synchronous Parquet parser and the shared
  `RecordBatchAnalyzer` used by Parquet-backed batch analysis
- the facade exposes the common parser entry points as top-level re-exports
- the facade no longer owns `robust_csv`

### Phase 4: Heavy Optional Crates

- Database connectors have moved to `dataprof-db`, with common entry points
   re-exported at the facade top level.
- Python bindings have moved to `dataprof-python`, with maturin configured to
   build the extension crate directly.
- The first engine-helper follow-up is now complete: `ArrowProfiler` moved into
   `dataprof-parquet`, `MemoryMappedCsvReader` moved into `dataprof-csv`, and
   `MemoryConfig` plus async source types moved into `dataprof-runtime`.
- The remaining streaming and adaptive engine implementations have moved into
   `dataprof-engines`.
- Async HTTP Parquet handling moved into `dataprof-parquet`.
- Fast schema inference and quick row-count behavior moved into
   `dataprof-partial`.
- The remaining `src/` tree has been removed. The published `dataprof` crate
   now builds from `crates/dataprof/src`, with a compact facade and no broad
   compatibility module shell.
- Run hygiene and architectural review of the new crate graph, new repository
   structure, fix any issues, and update documentation to reflect the new
   structure.
- Keep `dataprof` as the stable discovery package and dependency facade.

### Phase 5: Release Story

- Publish a release that is explicitly about usability and packaging, with
   `dataprof` positioned as the compact Rust facade users discover first.
- Document the 0.8 top-level facade through the README, changelog, and public
   API inventory rather than reviving the retired CLI surface or broad module
   shell.
- Keep benchmark results separate from the main README, linked but not
   dominant. The benchmark workflow now runs the quick suite by default on free
   GitHub runners, with the full suite available through manual dispatch, and
   the Pages deploy has a navigable landing page.
- Finish the work started on release.yml. The release workflow now validates
   the workspace, publishes Rust crates in dependency order, builds Python
   wheels, publishes PyPI wheels, and only then makes the GitHub release public.
- Version bump 0.8.0 across workspace crates and README installation snippets.
- Run explicit facade feature checks before release:
   `cargo check --no-default-features`,
   `cargo check --no-default-features --features async-streaming`,
   `cargo test --test public_api_facade --no-default-features`, and
   `cargo test --test public_api_facade --all-features`.

## What Not To Do Yet

- Do not move every module at once.
- Do not reintroduce the old broad module shell unless a concrete user-facing
   migration need appears.
- Do not make Python wrappers depend on unstable private facade internals.
- Do not add more format support until the existing format boundary is smaller.
- Do not let academic framing be the first thing a practical user sees.

## Near-Term Next Steps

1. Keep tightening the `dataprof-db` and `dataprof-python` split by moving
   Python internals toward owning crates where it is practical.
2. Review the full branch diff against `master`, fix PR-readiness issues, and
   open the release-oriented redesign PR.
