# Crate Redesign

This note captures the direction for a deeper dataprof architecture redesign.
The goal is not to split crates for its own sake. The goal is to make the
project easier to install, understand, test, and embed.

## Current Diagnosis

dataprof has grown into several products inside one crate:

- a CLI for profiling files from the terminal
- a Rust library API
- a Python package
- format readers for CSV, JSON, JSONL, Parquet, Arrow, and databases
- streaming and async ingestion
- ISO 8000/25012 quality metrics
- benchmark and academic-paper support

That breadth is useful, but it creates adoption friction:

- new users do not immediately know whether dataprof is primarily a CLI, a
  Python package, a Rust library, or an academic benchmark artifact
- default Rust builds pull in heavy format dependencies even when the user only
  wants simple CSV profiling
- public APIs, Python bindings, CLI behavior, and internal engines live close
  together, so changes feel riskier than they should
- the project has many valuable features, but the first-use path is not sharp
  enough

## Design Goals

1. Keep the first user experience simple: `cargo install dataprof`,
   `dataprof analyze data.csv`, useful output.
2. Make the Rust library embeddable without CLI, Python, database, or heavy
   columnar dependencies unless requested.
3. Keep Python ergonomics independent from Rust internals where possible.
4. Let format support grow through small, testable crates instead of one
   increasingly crowded parser layer.
5. Preserve the current CLI command surface during the migration unless a
   breaking change is explicitly planned.
6. Make benchmarks and paper artifacts reproducible without making the main
   package feel research-only.

## Status After The Core, Metrics, Runtime, Parser, And Parquet Split

The redesign is now past the packaging-only stage and into a real workspace
layout:

- `dataprof-core` owns shared classification, execution, source,
   partial-analysis, output, progress, config, and column/profile model types
- `dataprof-metrics` owns `analysis/`, `stats/`, pattern detection,
   validators, the SIMD numeric helper, and the quality metric result types
- `dataprof-runtime` owns `ProfileReport`, report assembly, streaming stats,
   and profile-building helpers that used to live under `src/core/`
- `dataprof-csv` now owns the standard CSV parser, delimiter detection, and
   CSV report-building flow
- `dataprof-json` now owns both the low-level JSON scanner and the higher-level
   JSON/JSONL report-building flow
- `dataprof-parquet` now owns the synchronous Parquet parser plus the shared
  `RecordBatchAnalyzer` used by Parquet-backed and Arrow-backed batch analysis
- the top-level `dataprof` crate now acts as a facade and compatibility layer:
   it re-exports the moved APIs and keeps the public surface stable while the
   internal crate graph changes
- `src/parsers/csv`, `src/parsers/json`, and `src/parsers/parquet` no longer
  own implementations; parser compatibility is now provided through lightweight
  shim modules

That is a meaningful architectural milestone. The remaining work is no longer
"create a place for shared code". The remaining work is to keep moving the
heavier parser, engine, CLI, Python, and database ownership behind the same
facade pattern.

## Current Boundary Status

The branch now has enough structure to talk about the remaining work in terms
of real boundaries instead of hypothetical ones:

| Area | What is already true | What is still coupled |
| --- | --- | --- |
| Packaging | The repo is now a workspace with `dataprof-core`, `dataprof-metrics`, `dataprof-runtime`, `dataprof-csv`, `dataprof-json`, and `dataprof-parquet`, and the facade crate still supports meaningful minimal/default builds | Database and product-specific crates do not exist yet |
| Shared model | Core report and profiling DTOs live in `dataprof-core`, and `ProfileReport` plus report assembly live in `dataprof-runtime` | Some engine code still reaches runtime helpers through facade module shims instead of depending on smaller leaf crates directly |
| Parser boundary | Standard CSV, JSON/JSONL, and synchronous Parquet parsing now live outside the facade crate, and the facade parser modules are shims | Async Parquet HTTP handling, parser dispatch, and non-parser product code are still in `src/` |
| Metrics boundary | `analysis/`, `stats/`, quality result types, pattern detection, and validators now live in `dataprof-metrics` | Parser and engine code still reach metrics through runtime or facade re-exports rather than a smaller explicit adapter boundary |
| Public API | Existing top-level exports remain available through facade re-exports and compatibility shims | `src/api/mod.rs` still dispatches through facade parser and engine modules instead of thinner crate-specific adapters |
| Product surfaces | The facade crate is increasingly a shell around internal crates | CLI, Python, database, and engine orchestration still ship from the same facade crate |

## End-State Workspace

The likely workspace shape is:

| Crate | Role |
| --- | --- |
| `dataprof-core` | Core report types, errors, data model, profiler traits, metric selection, sampling, stop conditions |
| `dataprof-metrics` | ISO 8000/25012 quality metrics, pattern detection, validators, statistical summaries |
| `dataprof-runtime` | Shared runtime composition helpers, report assembly, streaming stats, and cross-crate profiling adapters |
| `dataprof-csv` | Standard CSV parsing, delimiter detection, robust CSV recovery helpers, diagnostics, and CSV report assembly |
| `dataprof-json` | JSON and JSONL scanning plus JSON report assembly |
| `dataprof-parquet` | Synchronous Parquet profiling plus shared Arrow `RecordBatchAnalyzer` support used by Parquet-adjacent paths |
| `dataprof-db` | PostgreSQL, MySQL, SQLite connectors and SQL validation |
| `dataprof-cli` | CLI application crate that builds the `dataprof` binary |
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
| 5 | `dataprof-db`, `dataprof-cli`, `dataprof-python`, facade polish | Leave the user-facing and connector-heavy leaf crates for last, after the reader and runtime contracts stop moving |

## Feature Boundary Proposal

As of the current redesign branch, `minimal = []` no longer pulls in Arrow or
Parquet. The remaining work is to move those boundaries from module-level `cfg`
checks into crate-level dependencies with clearer ownership:

| Feature | Intended dependency boundary |
| --- | --- |
| default | Keep `cargo install dataprof` working: CLI plus common local formats |
| `csv` | CSV parsing and CSV-specific config only |
| `json` | JSON and JSONL support only |
| `arrow` | Columnar engine internals, not the whole public facade |
| `parquet` | Parquet reader plus Arrow-backed dependencies |
| `database` | Shared database config, connector traits, and SQL validation |
| `postgres`, `mysql`, `sqlite` | Individual SQL backends |
| `python` | PyO3 only in the Python crate or extension build |
| `datafusion` | DataFusion integration only |
| `full-cli` | CLI plus all production connectors and heavy backends |

Open decision: decide whether the facade crate default should optimize for
`cargo install dataprof` or `dataprof = "0.8"` as a dependency. If one package
must serve both, the CLI can stay default while library docs recommend
`default-features = false`.

## Migration Plan

### Phase 0: Packaging Baseline

Completed or largely completed on this branch:

- Make the installed command match the README: `dataprof`.
- Turn Arrow and Parquet into optional dependencies.
- Make unsupported backend paths fail explicitly at runtime.
- Keep the common CLI path on by default.

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

- Move database connectors, remaining Arrow-heavy paths, and Python bindings to
   dedicated crates after the reader crates validate the approach.
- Keep `dataprof` as the stable discovery package and dependency facade.

### Phase 5: Release Story

- Publish a release that is explicitly about usability and packaging.
- Document old command/API compatibility.
- Publish benchmark results separately from the main README, linked but not
  dominant.

## What Not To Do Yet

- Do not move every module at once.
- Do not make the first redesign PR mostly mechanical file churn.
- Do not split Python before the Rust public API boundaries are clear.
- Do not add more format support until the existing format boundary is smaller.
- Do not let academic framing be the first thing a practical user sees.

## Near-Term Next Steps

1. Add a public API inventory document from `src/lib.rs`, grouped into
   "must stay stable", "re-export during migration", and "internal-only".
2. Turn the current manual validation workflow into explicit compile-test
   coverage for facade re-exports and feature combinations.
3. Split the database readers now that the lighter parser crates, runtime
   boundary, and synchronous Parquet path are proven.
4. Decide whether the async HTTP Parquet path should stay facade-owned for a
   while longer or move behind `dataprof-parquet` in a later network-aware
   slice.
