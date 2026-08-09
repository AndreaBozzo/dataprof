---
name: code-review
description: Repository-specific review checklist for dataprof pull requests. Use when reviewing a diff in this repository, especially one touching an engine, a parser, a metric, type inference, or the quality score. Directs review effort at the defect class that actually ships here — a change that is correct where it was made and inconsistent with a parallel path, a paired predicate, or a documented number that was not touched.
---

# Reviewing dataprof

`dataprof` is a local, deterministic data profiler: a Rust workspace with PyO3
Python bindings. It reports on data and never transforms, cleans, or moves it.
The release surface is Rust library crates plus Python wheels; there is no CLI
binary. Standing repository conventions live in
[AGENTS.md](../../../AGENTS.md) — this file is only about what to look for when
reviewing a change.

The bug class that costs most here is rarely a local mistake inside the diff. It
is a change that is correct where it was made and inconsistent with something
that was not touched: a sibling input path, a second predicate for the same
concept, or a number quoted in documentation. Read the diff, then go look at the
places it should have changed too.

## 1. The same input must produce the same numbers everywhere

Profile numbers must be identical regardless of which engine or input path
produced them. A diff that changes one path and leaves its siblings alone is the
most common real defect, and it passes every test that only exercises the path it
touched.

When a diff touches any of these, check the others for the same behaviour:

| path | file |
| --- | --- |
| CSV parser (shared by file and reader entry points) | `crates/dataprof-csv/src/lib.rs` |
| JSON / JSONL parser | `crates/dataprof-json/src/lib.rs` |
| Parquet | `crates/dataprof-parquet/src/arrow_profiler.rs`, `record_batch_analyzer.rs` |
| incremental streaming engine | `crates/dataprof-engines/src/streaming/incremental.rs` |
| async streaming reader | `crates/dataprof-engines/src/streaming/async_reader.rs` |
| memory-mapped reader | `crates/dataprof-engines/src/streaming/memmap.rs` |
| columnar (Arrow) engine | `crates/dataprof-engines/src/columnar/mod.rs` |
| partial analysis (`infer_schema`, `analyze_structure`, `quick_row_count`) | `crates/dataprof-partial/src/lib.rs` |
| database connectors | `crates/dataprof-db/` |
| shared profile construction | `crates/dataprof-runtime/src/profile_builder.rs` |

Flag it when a fix lands in one of these and the same input would still be
answered differently by another. Check both raw attributes and the serialized
report, since a diff can bring one into line and leave the other out.

**Example of the miss.** A fix made `infer_schema` preserve the declared columns
of a header-only CSV, and `profile` and `analyze_structure` already did. The
shared CSV parser underneath still dropped them, so `analyze_csv_file` — public
API — reported zero columns and no quality block where every engine reported two
columns and a full one. One line in the parser would have fixed all of them
(issue #558). A review that only read the diff could not see this; a review that
asked "which other path produces this number?" could.

## 2. `None` means "not analyzed"; empty means "analyzed, found nothing"

These are not interchangeable anywhere in the codebase, including the Python
bindings and the serialized report. Flag:

- a `None` collapsed into a default that reads as a real measurement — `0`,
  `100.0`, `true`, an empty `Vec` presented as "nothing found";
- `unwrap_or_default()` or `.ok()` on a decode or parse path. A silent error that
  becomes a plausible number is the worst defect class in a profiler;
- a new optional field without a doc comment saying what its `None` means.

## 3. Two predicates for one concept will drift

The codebase carries several pairs of functions that answer the same question in
different places. When a diff touches one side, check whether the other should
move too, and whether the diff has quietly made them disagree:

- `infer_type` (`crates/dataprof-metrics/src/analysis/inference.rs`), batch, over
  `&[String]` — and `infer_data_type_streaming`
  (`crates/dataprof-runtime/src/profile_builder.rs`), streaming, over
  `StreamingStatistics`. Their thresholds must stay in step.
- the date forms inference recognizes and the date forms consistency validation
  accepts — two regex sets, neither a superset of the other.
- anything that both types a value and later checks that value against its type.

**Example of the catch.** Classifying values by lexical form used the validation
regex set, which does not recognize dotted `DD.MM.YYYY` or either datetime form
that inference accepts. Those values were classified as text alongside genuine
junk, so a column of 70% ISO datetimes and 30% junk again scored a perfect 100.
The same divergence, read the other way, means a column of clean ISO datetimes is
typed `date` and then scores 0.0 consistency (issue #562). Whenever a value is
typed by one predicate and validated by another, check that they are the same
predicate.

## 4. A changed number invalidates every place that quotes it

Published quality numbers are quoted in prose, in agent-facing instructions, and
in evaluation rubrics. A diff that changes a score and not its documentation
leaves the repository asserting something false. When a diff changes any
published metric value, require the corresponding update in:

- `docs/guides/getting-started.md` — the public definition of each metric;
- `docs/release-notes.md` — the real state of the current release;
- `.claude/skills/dataprof/SKILL.md` and `reference/interpretation.md` —
  agent-facing instructions, which may carry a *warning about the very bug being
  fixed* and will otherwise keep telling agents the tool is broken;
- `.claude/skills/dataprof/evals/` — rubrics grade against concrete numbers, and
  a rubric quoting a number dataprof no longer produces makes a passing eval
  meaningless;
- `docs/schema/profile-report.v1.schema.json` when a field's shape or meaning
  changes, plus `REPORT_SCHEMA_VERSION`.

## 5. A regression test that has never failed is not a regression test

For each test a diff adds, ask whether it would fail against the unfixed code.
Flag tests that would pass either way unless they are explicitly guarding
against over-correction, in which case they should say so.

The failure mode to watch for is an assertion that is technically true of the old
behaviour. A monotonicity test asserting a sequence is *non-increasing* passes
against a constant sequence, so it guards nothing when the bug being fixed is
"this value is always 100". Such a test needs a strict comparison, or a separate
assertion that the value moves at all.

Also flag a multi-part fix covered by a single test that cannot distinguish the
parts — it proves the test catches something, not that each part is needed.

## 6. Numbers computed over samples must not be presented as exact

Quality metrics are computed over bounded reservoir samples (10,000 values per
column), and distinct counts fall back to a HyperLogLog sketch. Flag any new
number that is sampled or estimated but carries no provenance for it, and any
comparison that treats an estimate and an exact count as the same kind of value.
`unique_count_is_approximate` is the existing pattern: `Some(true)` means "do not
use this as an exact integer".

## 7. Identity of the project

Flag additions that make dataprof transform, clean, deduplicate, or move data,
that add a CLI binary, or that introduce a network call or telemetry outside the
explicitly optional URL and database features. It profiles data and reports on
it; that is the whole surface.

## What not to comment on

Precision matters more than volume here. These are already enforced mechanically
and a comment about them is noise:

- formatting and lint (`cargo fmt`, `cargo clippy -D warnings`, `ruff`, `ty` all
  gate CI);
- commit subject conventions and changelog generation (checked in CI);
- missing AI attribution trailers — they are forbidden here, not required;
- test coverage of a path the diff did not change;
- restating the diff back as a summary comment.

Prefer one finding that names a concrete failing input over several stylistic
observations. If a finding is a guess, say which part is uncertain rather than
stating it flatly.
