# Timing protocol demonstration (#738)

[repeat-runs.zip](repeat-runs.zip) retains both complete, unmodified output
directories: fixture CSV, ordered worker observations, import preflights,
minimal-worker controls, block summaries, environment fingerprints, progress
checkpoints and Markdown tables. This is diagnostic evidence from a development
workstation, not an established performance baseline.

The same raw [first-run JSON](first-run.json) and [repeat-run JSON](repeat-run.json)
are also available as reviewable text. Their bytes match the corresponding
`results.json` entries in the archive.

## Experiment

On 2026-09-18, both runs used the same installed benchmark environment and native
binary on Windows, Intel Core Ultra 7 258V, with one requested thread. The tools
were the committed pins: pandas 2.3.3, Polars 1.44.1, ydata-profiling 4.18.4 and
dataprof 0.11.0. The pinned environment validation and every row/column/null check
passed. The existing binary was reused, without reinstalling between runs.

Each run used two serial process blocks, seven fresh-process observations per
tool per block, and seven measured operations after one warmup in a new warm
worker per tool per block. There are 14 observations per tool and condition,
64 tool workers, four import preflights and 14 minimal controls in each run.
The host description records AC power, the normal power profile, and uncontrolled
desktop/background activity. Validation and browser checks also ran during this
development session; these are deliberately not controlled-host claims.

Both runs used `mixed-csv-v1`, 1,000 rows, 23,446 bytes, fixture SHA-256
`1d766b0c21f3a89d78c31e76c9196471a2b5c7a87e6c39bdcff87e44b8e42272`.
Files were pre-read before workers. Library pages and other caches were not
evicted. The second run inherited host/cache history from the first.

Run from the repository root, repeating the first command with `--output`
changed to `benchmark-results/738-repeat-b` and adding
`--compare benchmark-results/738-repeat-a/results.json`:

```console
uv run --project benches --no-sync python .github/scripts/benchmark_comparison.py --publication --iterations 7 --blocks 2 --rows 1000 --warmups 1 --host-description "local Windows workstation; AC; normal power profile; background desktop and CI polling uncontrolled; diagnostic only" --output benchmark-results/738-repeat-a
```

The checkout was `5a9fe836ac13d1d931a7c901d90077de7c40c98b` plus the recorded
working changes for #738, committed in `e72447a` before review fixes.
The exact executed script SHA-256 was
`eb51fc744af58230ceddebc3126bc0261f95bb665876925f8eabb7724548e39a`.
The native extension SHA-256 was
`f6f55e3e952164754a9406a11a5167bdb2d0afe51768163d921e9adf79372188`.
Full installed versions, binary paths and host information are in each JSON.

## Repeat-run interpretation

Seconds, pooled medians; each tool performs its own documented workload:

| Tool | First run process | Repeat process | First run warm operation | Repeat warm operation |
| --- | ---: | ---: | ---: | ---: |
| dataprof | 0.331729 | 0.295872 | 0.046951 | 0.038788 |
| pandas | 0.917569 | 0.808065 | 0.005757 | 0.006944 |
| Polars | 0.609879 | 0.590418 | 0.001451 | 0.001869 |
| ydata-profiling | 5.474194 | 5.105104 | 0.213707 | 0.244650 |

All eight repeat-run IQR pairs overlap. This is not a significance test, proof
of stability, or evidence that the tools compute equivalent metrics. The
ordered samples and per-block summaries remain necessary: pooling does not
make warm operations independent processes or process blocks independent hosts.

For example, the first run's pandas import/setup preflight took 4.066779 seconds
and ydata's took 14.440017 seconds. Later fresh-process totals have much lower
medians. These are different invocation positions and boundaries, not a pure
import-cost subtraction. Environment preparation, library pages, runtime caches
and host load can all contribute; this experiment does not isolate their causes.

Inspect the archive without installing benchmark dependencies:

```console
python -m zipfile -e benches/evidence/738/repeat-runs.zip benchmark-results/738-evidence
```

The normal benchmark page generator can consume either extracted run as its
`comparison/` directory alongside Criterion artifacts. It exposes every worker
in execution order, import/setup and first-operation boundaries, controls,
sample/block counts, cache treatment and the diagnostic evidence status.
