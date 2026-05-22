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

## Status After The First Two Slices

The redesign has started, but it has not reached a multi-crate workspace yet.
The first two slices did useful prerequisite work:

- the installed binary is now consistently named `dataprof`
- `arrow` and `parquet` are now optional dependencies instead of always-on
   library weight
- `default = ["cli", "csv", "json", "parquet"]` keeps the common CLI path
   working, while `minimal = []` now means a real library-only build with no
   CLI, no Python, no databases, and no Arrow/Parquet stack
- public APIs in `src/api/mod.rs` and `src/api/partial.rs` now return explicit
   unsupported-feature errors when Parquet or Arrow-backed paths are disabled,
   instead of assuming those backends are present
- `parsers::parquet`, `engines::columnar`, and related re-exports are now
   behind feature gates, and the adaptive engine treats Parquet as a dedicated
   parser path rather than a generic engine decision

That is good progress because it turns dependency weight into an explicit build
choice. It also shows what is still missing: the package graph is cleaner, but
the code graph is still mostly one crate.

## Current Boundary Status

The branch now has enough structure to talk about the remaining work in terms
of real boundaries instead of hypothetical ones:

| Area | What is already true | What is still coupled |
| --- | --- | --- |
| Packaging | Optional `arrow`/`parquet` dependencies and a meaningful `minimal` build exist | There are still no workspace members; all code ships from one package |
| Public API | `Profiler` and partial-analysis entry points now degrade cleanly when features are off | `src/api/mod.rs` still dispatches directly into concrete parser and engine modules |
| Engine selection | `engines::columnar` is behind `arrow`, and adaptive selection already branches on feature availability | Engine choice, CSV config translation, and parser invocation still live in the same crate |
| Format readers | `parsers::parquet` and async Parquet paths are feature-gated | CSV and JSON parsing still depend on in-crate report assembly and streaming stats machinery |
| Product surfaces | CLI, Python, and database support are already mostly feature-scoped | CLI, Python, and database code still share internal types and orchestration logic from the same crate |

One important consequence follows from the current tree: `analysis/` and
`stats/` do not depend on parsers or engines, but they do depend on shared
types and core configuration/error types. That makes a small `dataprof-core`
crate the next necessary boundary, not an optional future cleanup.

## End-State Workspace

The likely workspace shape is:

| Crate | Role |
| --- | --- |
| `dataprof-core` | Core report types, errors, data model, profiler traits, metric selection, sampling, stop conditions |
| `dataprof-metrics` | ISO 8000/25012 quality metrics, pattern detection, validators, statistical summaries |
| `dataprof-csv` | CSV and delimiter detection, including robust parsing behavior |
| `dataprof-json` | JSON and JSONL readers |
| `dataprof-parquet` | Parquet and Arrow-backed profiling |
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
| 1 | `dataprof-core` | Extract shared report types, quality enums, errors, progress types, and stable config primitives so every later crate has somewhere to depend |
| 2 | `dataprof-metrics` | Move `analysis/` and `stats/` after `dataprof-core` exists; these modules are comparatively isolated from parsers and engines |
| 3 | `dataprof-csv` and `dataprof-json` | Split lighter readers once report-building adapters are no longer tied to the facade crate |
| 4 | `dataprof-parquet` and `dataprof-db` | Keep heavier optional dependencies isolated until the lighter crates prove the packaging model |
| 5 | `dataprof-cli`, `dataprof-python`, facade polish | Leave the user-facing leaf crates for last, after the library contracts stop moving |

Avoid splitting CSV first. The current CSV path still pulls on
`core::profile_builder`, `core::report_assembler`, and streaming stats internals,
so it would force adapter churn before the shared core types are stable.

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

- Move `types.rs`, stable error types, progress types, and the configuration
   pieces that do not depend on CLI, Python, or database code into a new core
   crate.
- Re-export those items from the facade crate so downstream code does not break.
- Keep this step intentionally boring: type moves, import rewrites, and tests.

### Phase 2: Move Metrics Behind The Core Boundary

- Move `analysis/` and `stats/` into `dataprof-metrics` once `dataprof-core`
   exists.
- Keep parser crates consuming a metrics API instead of reaching into metrics
   internals.
- Add compile tests for the public helpers re-exported from `src/lib.rs`.

### Phase 3: Split Reader Crates

- Split `dataprof-csv` and `dataprof-json` after report assembly stops being a
   private monolith detail.
- Keep the facade crate re-exporting the old entry points during transition.
- Add deprecation notes only after the replacement paths are proven.

### Phase 4: Heavy Optional Crates

- Move Parquet, Arrow-heavy paths, database connectors, and Python bindings to
   dedicated crates after the lighter crates validate the approach.
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

1. Add a public API inventory document generated from `src/lib.rs`, grouped
   into "must stay stable", "re-export during migration", and "internal-only".
2. Define the exact contents of `dataprof-core` before creating any new crate:
   shared report types, errors, progress types, and stable config primitives.
3. Add a small compatibility test matrix for `default`, `minimal`, and
   `default-features = false` so packaging regressions are visible early.
4. Move `analysis/` and `stats/` only after `dataprof-core` compiles as an
   internal dependency.
