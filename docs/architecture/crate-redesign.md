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

## Target Workspace

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

## Feature Boundary Proposal

As of the 0.8 development branch, `minimal = []` no longer pulls in Arrow or
Parquet. The remaining redesign work is to move these feature boundaries from
module-level `cfg`s into cleaner crate boundaries:

| Feature | Intended dependency boundary |
| --- | --- |
| default | CLI plus common local formats, unless release strategy chooses library-only |
| `csv` | CSV parsing and basic profiling |
| `json` | JSON and JSONL support |
| `parquet` | Parquet plus Arrow |
| `database` | Shared database config and connector traits |
| `postgres`, `mysql`, `sqlite` | Individual SQL backends |
| `python` | PyO3 only in the Python crate or extension build |
| `datafusion` | DataFusion integration only |
| `full-cli` | CLI plus all production connectors |

Open decision: decide whether the facade crate default should optimize for
`cargo install dataprof` or `dataprof = "0.8"` as a dependency. If one package
must serve both, the CLI can stay default while library docs recommend
`default-features = false`.

## Migration Plan

### Phase 0: Trust and Funnel

- Make the installed command match the README: `dataprof`.
- Remove stale version references in docs.
- Clarify beta/stability language without scaring away users.
- Add a short "which interface should I use?" section to the README.
- Add one real-world example that demonstrates clear value.

### Phase 1: API Inventory

- List all public exports from `src/lib.rs`.
- Classify each export as stable, unstable, internal-but-public, or deprecated.
- Add compile tests for the public examples before moving code.
- Decide which report fields are part of the 1.0 contract.

### Phase 2: Internal Module Boundaries

- Separate metrics from format readers inside the current crate first.
- Move engine-independent types into a smaller core module.
- Reduce direct dependencies from Python and CLI modules on engine internals.
- Add narrow adapter functions where the CLI and Python APIs cross into Rust.

### Phase 3: Workspace Split

- Introduce workspace members one at a time.
- Start with the lowest-risk split: metrics or CSV.
- Keep the facade crate re-exporting the old paths during transition.
- Add deprecation notes only after the replacement paths are proven.

### Phase 4: Release Story

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

1. Add an examples index with one e-commerce or telemetry quality workflow.
2. Add a public API inventory document generated from `src/lib.rs`.
3. Move Arrow and Parquet boundaries from feature-gated modules toward
   workspace crates.
4. Add issue labels/milestones around adoption, architecture, and paper-blocked
   work so contributors can see what is safe to touch.
