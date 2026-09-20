# Python/Arrow boundary attribution (#698)

On this host and fixture, **the combined consumer import/profiling call was the
largest warm stage in every supported case**. For the 8,192-row cells its median
was 14.47–28.51 ms, versus 3.33–12.45 ms for materialized-producer preparation.
Dict export medians were 0.23–0.42 ms and JSON export medians 0.27–0.52 ms.
These exports did not dominate the measured workload. Consumer import is not
separately instrumented, so this result does not identify an engine optimization.

The experiment stops at that attribution. It adds no profiling optimization.
The producer-dependent differences and fresh-process timings need controlled
repeats before interpretation as regressions or comparative speed claims.

## Evidence and reproduction

[`run.zip`](run.zip) contains `results.json` (raw observations, summaries,
fingerprints and reference profiles), `boundaries.md` (all stage medians/IQR),
and the three measured harness sources. No samples or outliers were dropped.

Measured on 2026-09-20, Windows x86-64, Intel Core Ultra 7 258V, CPython 3.12.13.
The installed release extension was rebuilt from checkout
`11172bc` (working tree modified for this experiment), dataprof 0.11.0,
PyArrow 25.0.1, pandas 2.3.3 and Polars 1.44.1. The archive includes full
distribution versions, the native extension hash and lock/source hashes.
The measurement script SHA-256 was
`4f27c6506fc38f580ce0a57b384215cdfbd5abea15f97e9fc31f5199a9140c2d`.

This was a development workstation with foreground development active and no
power, thermal or background-load controls. It is diagnostic evidence, not an
established performance baseline. Fresh-process totals were 0.81–1.63 seconds,
including imports, validation, IPC and exit. These are not operation-only costs;
warm stage medians must not be subtracted from them to infer startup overhead.

From the repository root:

```bash
uv sync --project benches --locked --reinstall-package dataprof
uv run --project benches --locked --no-sync python .github/scripts/benchmark_boundaries.py --rows 8192 --chunks 256 2048 --iterations 3 --warmups 1 --budget-seconds 600 --host-description "Windows development workstation; foreground development active; no power or thermal controls; diagnostic only" --output benchmark-results/boundaries-698
```

The declared budget was 600 seconds total, 60 seconds per worker, three measured
samples per condition, one warmup and one requested thread. The matrix completed
27 supported cells and four explicit C Array/multiple-chunk skips. It retained
108 measured worker runs (81 fresh, 27 warm), 27 preflight runs and two reference
profiles. Each warm worker contains three measured operations and its warmup.
Warm dispersion describes a single process per cell; no independent-process
repeatability is established.

All supported cells matched the single-batch reference's serialized column
metrics **exactly**, including absence. Preflight additionally verified int64
values above 2^53, float/string/null values, source column order and requested
Arrow slice offsets. Statistics equality is path parity, not a proof that each
statistical formula is numerically exact. No tolerance hid a mismatch.

## Stream growth at a fixed batch size

Both cells used offset three and 256-row batches:

| Observation | 8,192 rows | 32,768 rows |
| --- | ---: | ---: |
| Batches generated | 32 | 128 |
| Largest logical batch | 8,504 bytes | 8,504 bytes |
| Largest observed Arrow pool allocation | 8,832 bytes | 8,832 bytes |
| Warm worker peak RSS | 89.01 MiB | 88.93 MiB |
| Fresh worker peak RSS range | 88.58–88.84 MiB | 89.12–89.98 MiB |
| Warm import/profiling median | 26.19 ms | 83.01 ms |

The reader constructed no full table; each batch was generated on demand.
Lazy batch preparation took about 6.4–6.8 ms and 23.4–26.8 ms respectively in the
warm samples, **inside** the import/profiling timer. It cannot be added again to
that timer or interpreted as consumer import.

This provides evidence of bounded producer-buffer use at the two tested sizes.
It does not prove total native memory stays flat for arbitrary row counts or
cardinalities: RSS includes imports and allocator retention, Arrow pool samples
exclude other native allocators, and short-lived allocations between samples may
be missed. The complete repeated-worker memory observations remain in the JSON.

The [suite protocol](../../README.md#pythonarrow-boundary-experiment) defines all
boundaries and limitations, including pandas' Arrow-backed representation,
Polars preparation, deferred imports and uncontrollable OS library caches.
