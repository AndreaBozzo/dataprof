# Crate Redesign

This note captures the direction for a deeper dataprof architecture redesign.
The goal is not to split crates for its own sake. The goal is to make the
project easier to install, understand, test, and embed.

## Current Diagnosis

dataprof has historically grown into several products inside one crate:

- a Rust library API
- a Python package
- a legacy CLI for profiling files from the terminal
- format readers for CSV, JSON, JSONL, Parquet, Arrow, and databases
- streaming and async ingestion
- ISO 8000/25012 quality metrics
- benchmark and academic-paper support

That breadth is useful, but it creates adoption friction:

- new users do not immediately know whether dataprof is primarily a Rust
  library, a Python package, a CLI, or an academic benchmark artifact
- default Rust builds pull in heavy format dependencies even when the user only
  wants simple CSV profiling
- public APIs, Python bindings, legacy CLI behavior, and internal engines live
  close together, so changes feel riskier than they should
- the project has many valuable features, but the first-use path is not sharp
  enough

## Design Goals

1. Make the Rust library embeddable without CLI, Python, database, or heavy
   columnar dependencies unless requested.
2. Retire the CLI implementation from this redesign branch and keep any old CLI
   feature names only as compatibility aliases while the project moves
   library-first.
3. Keep Python ergonomics independent from Rust internals where possible.
4. Let format support grow through small, testable crates instead of one
   increasingly crowded parser layer.
5. Keep the public Rust facade stable while treating the old CLI as
   non-goal/deprecated code.
6. Make benchmarks and paper artifacts reproducible without making the main
   package feel research-only.

## Status After The Core, Metrics, Runtime, Parser, Parquet, DB, Python, And Engine Split

The redesign is now past the packaging-only stage and into a real workspace
layout:

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
- `dataprof-engines` now owns adaptive engine selection plus the incremental
   and async streaming profilers, while preserving old facade module paths
- `dataprof-parquet` still owns the Arrow-backed profiler implementation used
   by the columnar engine path
- `dataprof-db` now owns database configuration, connector traits, SQL
  validation/security helpers, retry/sampling/streaming helpers, and the
  feature-gated PostgreSQL, MySQL, and SQLite connectors
- `dataprof-core` now owns `MemoryTracker`, leaving the facade path as a
  compatibility shim for streaming callers
- `dataprof-python` now owns the PyO3 extension module and Python-facing report
  wrappers; `pyproject.toml` points maturin at that crate
- the legacy CLI implementation and CLI-only output module have been removed
  from the facade crate; `cli` and `full-cli` remain as deprecated no-op
  compatibility feature names
- the top-level `dataprof` crate now acts as a facade and compatibility layer:
   it re-exports the moved APIs and keeps the public surface stable while the
   internal crate graph changes
- `src/parsers/csv`, `src/parsers/json`, and `src/parsers/parquet` no longer
  own implementations; parser compatibility is now provided through lightweight
  shim modules
- `src/database` no longer owns implementations; database compatibility is now
  provided through a lightweight shim module
- `src/engines/common`, `src/engines/adaptive`,
   `src/engines/streaming/incremental`, `src/engines/streaming/async_reader`,
   `src/engines/streaming/memmap`, `src/engines/streaming/async_source`, and
   the old `src/engines/columnar/arrow_profiler` path no longer own
   implementations; these are now compatibility shims over workspace crates

That is a meaningful architectural milestone. The remaining work is no longer
"create a place for shared code". The remaining work is to finish release
validation, decide the async HTTP Parquet ownership slice, and keep the facade
stable while smaller crates own behavior.

## Current Boundary Status

The branch now has enough structure to talk about the remaining work in terms
of real boundaries instead of hypothetical ones:

| Area | What is already true | What is still coupled |
| --- | --- | --- |
| Packaging | The repo is now a workspace with `dataprof-core`, `dataprof-metrics`, `dataprof-runtime`, `dataprof-csv`, `dataprof-json`, `dataprof-parquet`, `dataprof-engines`, `dataprof-db`, and `dataprof-python`, and the facade crate supports meaningful library builds | The facade still owns the high-level `Profiler` orchestration API |
| Shared model | Core report and profiling DTOs live in `dataprof-core`, `MemoryTracker` lives in `dataprof-core`, and `ProfileReport` plus report assembly live in `dataprof-runtime` | Async HTTP Parquet still crosses parser/runtime/facade boundaries |
| Parser boundary | Standard CSV, JSON/JSONL, synchronous Parquet parsing, and the memory-mapped CSV reader now live outside the facade crate, and the facade parser/reader modules are shims | Async Parquet HTTP handling, parser dispatch, and non-parser product code are still in `src/` |
| Metrics boundary | `analysis/`, `stats/`, quality result types, pattern detection, and validators now live in `dataprof-metrics` | Parser and engine code still reach metrics through runtime or facade re-exports rather than a smaller explicit adapter boundary |
| Public API | Existing top-level exports remain available through facade re-exports and compatibility shims, including async source types and columnar engine entry points | `src/api/partial.rs` and async HTTP Parquet still contain facade-owned format glue |
| Database boundary | Database config, connector traits, SQL validation/security helpers, retry/sampling/streaming helpers, and concrete SQL connectors now live in `dataprof-db`; `src/database` is a shim | `dataprof-python` depends on the facade/database surface for Python async database bindings |
| Python boundary | PyO3 classes, functions, and extension registration now live in `dataprof-python`; the root `python` feature is a deprecated alias | Python wrappers still depend on the facade API rather than smaller leaf crates directly |
| Product surfaces | The facade crate is increasingly a shell around internal crates; engine implementations now live in `dataprof-engines` with old paths as shims | The high-level `Profiler` builder remains facade-owned by design |

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
| `dataprof-db` | PostgreSQL, MySQL, SQLite connectors and SQL validation |
| `dataprof-python` | PyO3 extension and Python report wrappers |
| `dataprof` | Facade crate for common Rust users, re-exporting stable APIs behind features |

The facade crate should remain the main package people discover. Internally,
the workspace can become more modular without forcing users to understand every
crate on day one.

## Recommended Execution Order

The safest order is no longer "metrics or CSV, whichever feels smaller". The
branch now gives enough evidence to sequence the work more clearly:

| Step | Deliverable | Why this order is safer |
| --- | --- | --- |
| 1 | `dataprof-core` | Completed. Shared report types, progress/error/config primitives, and profiling model types now have a stable internal home |
| 2 | `dataprof-metrics` | Completed. `analysis/` and `stats/` proved isolated enough to move behind the new core boundary with facade shims |
| 3 | `dataprof-runtime`, `dataprof-csv`, and full `dataprof-json` ownership | Completed. Shared report-building helpers moved first, then standard CSV and JSON parser ownership followed behind facade shims |
| 4 | `dataprof-parquet` | Completed. The synchronous Parquet reader and shared `RecordBatchAnalyzer` moved behind a new crate while facade module paths stayed stable |
| 5 | `dataprof-db`, `dataprof-python`, `dataprof-engines`, facade polish | Completed for this redesign slice. Database, Python, engine implementations, and legacy CLI removal are behind stable facade shims; remaining work is release validation and the async HTTP Parquet ownership decision |

## Feature Boundary Proposal

As of the current redesign branch, `minimal = []` no longer pulls in Arrow,
Parquet, engine optional dependencies, or database connector dependencies. The
main product boundaries now live at crate level; the remaining work is release
validation and any future async HTTP Parquet ownership slice:

| Feature | Intended dependency boundary |
| --- | --- |
| default | Library-friendly default feature set; no CLI binary |
| `csv` | CSV parsing and CSV-specific config only |
| `json` | JSON and JSONL support only |
| `arrow` | Columnar engine internals, not the whole public facade |
| `parquet` | Parquet reader plus Arrow-backed dependencies |
| `database` | `dataprof-db` facade dependency without concrete SQL backend dependencies |
| `postgres`, `mysql`, `sqlite` | Individual SQL backends through `dataprof-db` features |
| `python`, `python-async` | Deprecated facade aliases; real PyO3 features live on `dataprof-python` |
| `cli`, `full-cli` | Deprecated compatibility aliases; no CLI implementation is built |

The facade crate now optimizes for `dataprof = "0.8"` as a Rust dependency.
CLI packaging can return later as a separate product if it becomes useful, but
it is not part of the current library-first redesign.

## Migration Plan

### Phase 0: Packaging Baseline

Completed or intentionally changed on this branch:

- Turn Arrow and Parquet into optional dependencies.
- Make unsupported backend paths fail explicitly at runtime.
- Remove the legacy CLI binary and command modules from the facade crate.
- Keep old `cli`/`full-cli` feature names as deprecated compatibility aliases.

### Phase 1: Extract `dataprof-core`

Completed on this branch:

- shared classification, execution, output, partial-analysis, source, and
   column/profile model types now live in `dataprof-core`
- stable config primitives (`IsoQualityConfig`) and progress/error types are
   available through the same core boundary
- the facade crate re-exports those items so downstream paths remain stable

### Phase 2: Move Metrics Behind The Core Boundary

Completed on this branch:

- `analysis/`, `stats/`, pattern detection, validators, and quality metric
   result types now live in `dataprof-metrics`
- the facade crate exposes compatibility shims for `analysis` and `stats`
   rather than owning the implementations directly
- the branch has been revalidated against the facade library build matrix after
   the split, though broader public-API compile-test coverage still needs to be
   formalized

### Phase 3: Split Reader Crates

Completed for the extracted reader crates on this branch:

- `dataprof-runtime` now owns the shared report-building and streaming helpers
   that used to block parser extraction
- `dataprof-csv` owns the standard CSV parser, delimiter detection flow,
  robust CSV recovery helpers, and `CsvDiagnostics`
- `dataprof-json` owns both the JSON scanner and the higher-level JSON/JSONL
   report-building API
- `dataprof-parquet` owns the synchronous Parquet parser and the shared
  `RecordBatchAnalyzer` used by Parquet-backed batch analysis
- the facade crate still re-exports the old parser entry points through shim
   modules so downstream module paths remain stable during transition
- the facade no longer owns `robust_csv`; only lightweight re-exports remain

### Phase 4: Heavy Optional Crates

- Database connectors have moved to `dataprof-db` behind the facade shim.
- Python bindings have moved to `dataprof-python`, with maturin configured to
   build the extension crate directly.
- The first engine-helper follow-up is now complete: `ArrowProfiler` moved into
   `dataprof-parquet`, `MemoryMappedCsvReader` moved into `dataprof-csv`, and
   `MemoryConfig` plus async source types moved into `dataprof-runtime`.
- The remaining streaming and adaptive engine implementations have moved into
   `dataprof-engines`.
- Fully evaluate what's left of the src/ folder and move any remaining product code into workspace crates behind facade shims. ( es. parquet async HTTP handling, parser dispatch, and non-parser product code )
- Run hygiene and architectural review of the new crate graph, new repository structure, fix any issues, and update documentation to reflect the new structure.
- Keep `dataprof` as the stable discovery package and dependency facade.

### Phase 5: Release Story

- Publish a release that is explicitly about usability and packaging, with
   `dataprof` positioned as a library-first facade.
- Document old command/API compatibility through the README, changelog, and
   public API inventory rather than reviving the legacy CLI implementation.
- Keep benchmark results separate from the main README, linked but not
   dominant, improve the current benches (must still run on free gh runners) and enhance the page deploy to make it more navigable and user friendly.
- Finish the work started on release.yml.
- Version bump 0.8.0.
- Run explicit facade feature checks before release:
   `cargo check --no-default-features`,
   `cargo check --no-default-features --features async-streaming`,
   `cargo test --test public_api_facade --no-default-features`, and
   `cargo test --test public_api_facade --all-features`.

## What Not To Do Yet

- Do not move every module at once.
- Do not make the first redesign PR mostly mechanical file churn.
- Do not make Python wrappers depend on unstable private facade internals.
- Do not add more format support until the existing format boundary is smaller.
- Do not let academic framing be the first thing a practical user sees.

## Near-Term Next Steps

1. Keep tightening the `dataprof-db` and `dataprof-python` split by removing
   facade-only assumptions from Python callers where it is practical.
2. Decide whether the async HTTP Parquet path should stay facade-owned for a
   while longer or move behind `dataprof-parquet` in a later network-aware
   slice.
3. Review the full branch diff against `master`, fix PR-readiness issues, and
   open the release-oriented redesign PR.
