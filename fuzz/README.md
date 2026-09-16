# Bounded parser and report fuzzing

This unpublished, isolated Cargo project exercises public Rust boundaries. It
adds no dependency, feature, or public API to the shipped crates. Ordinary
contributor builds do not compile libFuzzer.

## Deterministic smoke test (including Windows)

From the repository root, using the pinned contributor toolchain (Rust 1.98):

```sh
cargo test --manifest-path fuzz/Cargo.toml --locked --test smoke
```

This replays every committed seed, in filename order, through the **same
functions** used by the fuzz targets, plus assertions that known malformed
inputs fail or are explicitly counted as skipped. No random campaign, nightly
compiler, network service, or sanitizer is needed. Seed bytes are not subject
to Git newline conversion. A failing replay prints the seed path; repeat with
`-- --nocapture` to see all paths.

The harness rejects inputs larger than 4,096 bytes and caps parsed records at
32. Input size also bounds field counts and field lengths; it does not claim
that allocations equal input size. JSON retains the parser's recursion limit.
CI additionally imposes a job timeout; libFuzzer enforces 10 seconds per input
and a 1 GiB RSS limit. Time and memory limits are failure detectors, not proofs
against every allocation or hang.

## Targets and invariants

| Target | Boundary and checks |
| --- | --- |
| `csv` | Delimiter detection and reader profiling; header/headerless, strict/flexible, detected/explicit delimiter. Successful results have unique ordered names, bounded row counts, coherent null/distinct counts, and serializable columns. Invalid UTF-8 and duplicate headers must return typed errors; ragged rows fail strictly or follow documented padding/truncation. |
| `json` | JSON document, JSONL, and format sniffing, under strict and skip policies. Scanner and profiler agree on success, ordered columns, rows, skipped records, and format. Empty objects count as records. Strict success has no skipped records. |
| `report` | Rust `ProfileReport` deserialization, including legacy and current versions. Accepted reports reserialize to valid JSON and reload with their version, quality status, and whole emitted document unchanged. Missing, null, and empty values are compared distinctly, and a recorded row selection of zero rows stays distinct from no selection. Rejected outright: non-JSON input, future versions, an explicit null `quality_status`, and a status contradicting the presence of the assessment it describes. |

Parser errors are valid outcomes, not crashes. Panics and invariant violations
are never caught or turned into defaults. Report deserialization uses its
existing `serde_json::Error` boundary. The report target covers the Rust report
dialect; the separate Python report dialect remains covered by Python tests.

Facade format dispatch is deferred: that surface also accepts encoded Parquet,
where a tiny compressed input need not imply bounded decoded allocations.
These initial targets accept only local bytes and use no database or network.

## Pinned libFuzzer invocation

The supported sanitizer campaign platform is Linux x86_64, with
`nightly-2026-09-01`, `cargo-fuzz 0.13.2`, `libfuzzer-sys 0.4.13`, and the
committed `fuzz/Cargo.lock`. On Windows, use stable corpus replay, or run these
commands inside Linux/WSL. Native Windows sanitizer campaigns are not part of
this project's verified configuration.

```sh
rustup toolchain install nightly-2026-09-01 --profile minimal
cargo +nightly-2026-09-01 install cargo-fuzz --version 0.13.2 --locked
cargo +nightly-2026-09-01 fetch --manifest-path fuzz/Cargo.toml --locked
mkdir -p fuzz/corpus/csv
cp fuzz/seeds/csv/* fuzz/corpus/csv/
CARGO_NET_OFFLINE=true cargo +nightly-2026-09-01 fuzz run csv --features fuzzing -- \
  -max_total_time=300 -max_len=4096 -timeout=10 -rss_limit_mb=1024 -seed=274
git diff --exit-code -- fuzz/Cargo.lock
```

Substitute `json` or `report` in all corpus/target locations to run the
other targets. AddressSanitizer, debug assertions and overflow checks are
enabled by cargo-fuzz's defaults. For a deterministic sanitizer replay, replace
`-max_total_time=300` with `-runs=0` (initial corpus only). A fixed seed makes
the mutation sequence reproducible for the same build and corpus; a time
budget does not promise identical coverage or iteration counts across hosts.

`cargo-fuzz 0.13.2` does not expose `--locked` for `run`: fetch with `--locked`
first, run offline, and check the lockfile afterward. When updating dependencies,
refresh the harness lockfile deliberately along with the workspace lockfile.

See the upstream [cargo-fuzz guide](https://rust-fuzz.github.io/book/cargo-fuzz/guide.html)
and [CLI source for the pinned version](https://github.com/rust-fuzz/cargo-fuzz/blob/0.13.2/src/options.rs).

## CI and retained failures

`.github/workflows/fuzz.yml` runs stable replay on Linux and Windows, and builds
all three sanitizer targets to replay their seeds on Linux for relevant PRs and
pushes. Weekly and manual runs mutate each corpus for five minutes. Each job
has a wall-clock timeout and uploads its crash artifacts and evolved corpus
for 14 days, even after a failure. Corpus growth is kept in ignored
`fuzz/corpus/`; review and minimize discoveries before adding them to `seeds/`.

Reproduce an uploaded input with the same tool versions and bounds:

```sh
cargo +nightly-2026-09-01 fuzz run csv --features fuzzing fuzz/artifacts/csv/crash-HASH -- \
  -runs=1 -max_len=4096 -timeout=10 -rss_limit_mb=1024
cargo +nightly-2026-09-01 fuzz tmin csv --features fuzzing fuzz/artifacts/csv/crash-HASH -- \
  -max_total_time=60 -timeout=10 -rss_limit_mb=1024
```

Every discovered crash or contract violation must get a minimized ordinary
regression test in its owning crate **before closing the finding**, as well as
a retained seed here. Investigate timeouts and OOM artifacts with the same
bounds. Do not weaken an invariant merely to make a crash disappear.

## Seed provenance

Seeds are small synthetic reproductions of already documented boundaries:

- CSV ragged rows (#462), duplicate headers, invalid UTF-8, multiline quoted
  records, explicit/detected delimiters, empty input/header-only input, extreme
  numbers, and the row cap (#459/#460).
- JSON zero-field records (#463), first-seen column order (#465), nested nulls,
  pretty documents versus JSONL, non-object/malformed records, excessive
  nesting, invalid UTF-8/numbers, Unicode, and the row cap.
- Report legacy/current/future versions (#374), absent/null/empty quality and
  row-range evidence, contradictory/null quality status (#715), and truncated
  non-JSON bytes.

#461 (Parquet/Python dependency routing) and #464 (dependency security policy)
are outside these parser and report targets. No exhaustive correctness or
coverage claim is implied by this initial corpus.
