"""Bounded Python/Arrow boundary experiment (#698); see benches/README.md."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import os
import random
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import benchmark_comparison as common

PRODUCERS = ("arrow_array", "arrow_table", "pandas", "polars", "arrow_stream")
METRICS = ["schema", "statistics"]
STAGES = ("prepare", "import_profile", "export_dict", "export_json", "end_to_end")
SCOPE = {
    "prepare": "generate typed Arrow batches and construct producer; streams construct only reader",
    "import_profile": "public profile(): adapter, consumer import and profiling combined",
    "export_dict": "report.to_dict()",
    "export_json": "report.to_json(); includes its own document construction",
    "end_to_end": "preparation through both exports; excludes imports, GC and validation",
    "lazy_prepare": "batch generation inside import_profile; nested, never add to stage totals",
    "process": "interpreter, imports, operations, validation, IPC and exit; warm includes warmups",
}


def values(start: int, count: int) -> dict:
    """Versioned logical data; nullable int64 never travels through float64."""
    return {
        "id": [2**53 + i % 97 for i in range(start, start + count)],
        "amount": [
            None if i % 11 == 0 else (i % 101 - 50) / 4 for i in range(start, start + count)
        ],
        "category": [
            None if i % 13 == 0 else f"group_{i % 7}_東京" for i in range(start, start + count)
        ],
    }


def fixture_identity(rows: int) -> dict:
    # Hash the logical fixture incrementally, independently of batch boundaries.
    digest = hashlib.sha256()
    for start in range(0, rows, 1024):
        data = values(start, min(1024, rows - start))
        for row in zip(*data.values(), strict=True):
            digest.update((json.dumps(row, ensure_ascii=False) + "\n").encode())
    return {"generator": "python-boundaries-v1", "rows": rows, "sha256": digest.hexdigest()}


def batch(pa, start: int, count: int, offset: int):
    data = values(start - offset, count + offset)
    types = (pa.int64(), pa.float64(), pa.string())
    arrays = [pa.array(v, type=t) for v, t in zip(data.values(), types, strict=True)]
    return pa.RecordBatch.from_arrays(arrays, list(data)).slice(offset, count)


def producer(pa, case: dict, evidence: dict):
    rows, chunk, offset = case["rows"], case["chunk_size"], case["offset"]

    def batches():
        for start in range(0, rows, chunk):
            begin = time.perf_counter_ns()
            item = batch(pa, start, min(chunk, rows - start), offset)
            evidence["batch_preparation_seconds"] += (time.perf_counter_ns() - begin) / 1e9
            evidence["batches"] += 1
            evidence["rows"] += item.num_rows
            evidence["max_batch_rows"] = max(evidence["max_batch_rows"], item.num_rows)
            evidence["max_batch_bytes"] = max(evidence["max_batch_bytes"], item.nbytes)
            evidence["observed_arrow_pool_bytes"] = max(
                evidence["observed_arrow_pool_bytes"], pa.total_allocated_bytes()
            )
            yield item
            # Do not retain yielded batches in the producer.
            del item

    kind = case["producer"]
    if kind == "arrow_stream":
        schema = pa.schema(
            [("id", pa.int64()), ("amount", pa.float64()), ("category", pa.string())]
        )
        return pa.RecordBatchReader.from_batches(schema, batches())
    if kind == "arrow_array":
        return next(batches())
    table = pa.Table.from_batches(batches())
    if kind == "arrow_table":
        return table
    if kind == "pandas":
        import pandas as pd

        return table.to_pandas(types_mapper=pd.ArrowDtype)
    if kind == "polars":
        import polars as pl

        return pl.from_arrow(table, rechunk=False)
    raise ValueError(f"unknown producer {kind}")


def validate(document: dict, expected: list[dict], rows: int) -> None:
    if document["execution"]["rows_processed"] != rows:
        raise ValueError("row count mismatch")
    if document["columns"] != expected:
        raise ValueError("serialized column metrics/absence/order mismatch")
    if document["quality"] is not None or document["quality_status"]["state"] != "not_requested":
        raise ValueError("unrequested quality must remain absent")


def worker(request: dict) -> dict:
    begin = time.perf_counter_ns()
    import dataprof
    import pyarrow as pa

    case = request["case"]
    if case["producer"] in ("pandas", "polars"):
        __import__(case["producer"])
    import_seconds = (time.perf_counter_ns() - begin) / 1e9
    if request.get("reference"):
        source = batch(pa, 0, case["rows"], 0)
        report = dataprof.profile(source, metrics=METRICS)
        document = report.to_dict()
        data = values(0, case["rows"])
        for column, (name, cells) in zip(document["columns"], data.items(), strict=True):
            assert column["name"] == name
            assert column["total_count"] == len(cells)
            assert column["null_count"] == cells.count(None)
            assert column["unique_count"] == len(set(cells) - {None})
        return {"columns": document["columns"], "status": "complete"}

    samples, warmups = [], []
    for index in range(request["warmups"] + request["iterations"]):
        gc.collect()
        evidence = dict.fromkeys(
            ("batches", "rows", "max_batch_rows", "max_batch_bytes", "observed_arrow_pool_bytes"), 0
        )
        evidence["batch_preparation_seconds"] = 0.0
        begin = time.perf_counter_ns()
        source = producer(pa, case, evidence)
        prepared = time.perf_counter_ns()
        report = dataprof.profile(source, metrics=METRICS)
        profiled = time.perf_counter_ns()
        document = report.to_dict()
        exported_dict = time.perf_counter_ns()
        encoded = report.to_json()
        exported_json = time.perf_counter_ns()
        validate(document, request["expected"], case["rows"])
        if json.loads(encoded) != document:
            raise ValueError("JSON export differs from dict export")
        if evidence["rows"] != case["rows"]:
            raise ValueError("producer was not fully consumed")
        sample = {
            "seconds": dict(
                zip(
                    STAGES,
                    [
                        (prepared - begin) / 1e9,
                        (profiled - prepared) / 1e9,
                        (exported_dict - profiled) / 1e9,
                        (exported_json - exported_dict) / 1e9,
                        (exported_json - begin) / 1e9,
                    ],
                    strict=True,
                )
            ),
            "producer": evidence,
        }
        (warmups if index < request["warmups"] else samples).append(sample)
        del source, report, document, encoded
    return {
        "status": "complete",
        "pid": os.getpid(),
        "import_setup_seconds": import_seconds,
        "warmups": warmups,
        "samples": samples,
        "peak_rss": common.resource_collector().peak_rss(),
    }


def preflight(request: dict) -> dict:
    """Check physical values, offsets and chunks without contaminating timed workers."""
    import pyarrow as pa

    case = request["case"]
    evidence = dict.fromkeys(
        (
            "batches",
            "rows",
            "max_batch_rows",
            "max_batch_bytes",
            "observed_arrow_pool_bytes",
            "batch_preparation_seconds",
        ),
        0,
    )
    source = producer(pa, case, evidence)
    kind = case["producer"]
    if kind == "pandas":
        source = pa.Table.from_pandas(source, preserve_index=False)
    elif kind == "polars":
        source = source.to_arrow()
    observed = (
        [source]
        if kind == "arrow_array"
        else source
        if kind == "arrow_stream"
        else source.to_batches()
    )
    start = 0
    for item in observed:
        if item.to_pydict() != values(start, item.num_rows):
            raise ValueError("producer changed integers, values, nulls or column order")
        if kind in ("arrow_array", "arrow_table", "arrow_stream"):
            if any(column.offset != case["offset"] for column in item.columns):
                raise ValueError("requested non-zero offset was lost")
        start += item.num_rows
    if start != case["rows"]:
        raise ValueError("preflight row count mismatch")
    # The consumer itself must also pass before taking any measured sample.
    return worker({**request, "warmups": 0, "iterations": 1})


def cases(rows: int, chunks: list[int], scale: int) -> list[dict]:
    result = []
    for kind in PRODUCERS:
        for chunk in sorted(set([*chunks, rows])):
            for offset in (0, 3):
                case = {"producer": kind, "rows": rows, "chunk_size": chunk, "offset": offset}
                if kind == "arrow_array" and chunk != rows:
                    case["skip_reason"] = (
                        "C Array exports one batch; chunked input uses Table or C Stream"
                    )
                result.append(case)
    # Hold batch size fixed as total stream input grows; never create a full table.
    result.append(
        {"producer": "arrow_stream", "rows": rows * scale, "chunk_size": min(chunks), "offset": 3}
    )
    for case in result:
        case["id"] = "{producer}/{rows}/{chunk_size}/offset-{offset}".format(**case)
    return result


def summarize_runs(runs: list[dict]) -> dict:
    result = {}
    for condition in ("fresh", "warm"):
        selected = [run for run in runs if run["condition"] == condition]
        samples = [sample for run in selected for sample in run["samples"]]
        result[condition] = {
            stage: common.summarize([sample["seconds"][stage] for sample in samples])
            for stage in STAGES
        }
    return result


def render(document: dict) -> str:
    lines = [
        "# Python/Arrow boundary experiment",
        "",
        "Diagnostic evidence; no established baseline. Seconds: median [IQR].",
        "Consumer import and profiling are combined. Lazy stream preparation is inside that timer.",
        "End-to-end includes both independent exports. RSS is a process lifetime high-water mark,",
        "including imports and warmups, not per-stage allocation or a Python allocation count.",
        "",
        "| Case (producer/rows/chunk/offset) | Condition | Prepare | Import + profile | "
        "Dict | JSON | End-to-end |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for case in document["cases"]:
        if case["status"] == "skipped":
            continue
        for condition, summary in case["summary"].items():
            cells = [
                f"{summary[s]['median_seconds']:.6f} [{summary[s]['iqr_seconds']:.6f}]"
                for s in STAGES
            ]
            lines.append(f"| {case['id']} | {condition} | " + " | ".join(cells) + " |")
    lines += [
        "",
        *[
            f"Skipped `{c['id']}`: {c['skip_reason']}"
            for c in document["cases"]
            if c["status"] == "skipped"
        ],
        "",
        "All serialized column metrics and absence match a single-batch reference exactly.",
        "Producer preflight also verifies exact int64 values above 2^53, nulls and order.",
        "Raw samples, warmups, preflights, memory observations and fingerprints: results.json.",
    ]
    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=common.ROOT / "benchmark-results/boundaries")
    parser.add_argument("--rows", type=int, default=8192)
    parser.add_argument("--chunks", type=int, nargs="+", default=[256, 2048])
    parser.add_argument("--stream-scale", type=int, default=4)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=60)
    parser.add_argument("--budget-seconds", type=float, default=600)
    parser.add_argument("--host-description", default="unspecified host controls; diagnostic only")
    args = parser.parse_args(argv)
    if (
        args.rows < 100
        or args.rows * args.stream_scale > 1_000_000
        or args.stream_scale < 2
        or min(args.chunks) < 1
        or max(args.chunks) > args.rows
        or args.iterations < 2
        or args.warmups < 1
        or args.threads < 1
        or not all(math.isfinite(x) and x > 0 for x in (args.timeout, args.budget_seconds))
    ):
        parser.error(
            "require rows >= 100, scaled rows <= 1M, scale >= 2, chunks in 1..rows, "
            "repeats >= 2, warmups/threads >= 1 and positive finite time budgets"
        )
    args.output.mkdir(parents=True, exist_ok=False)
    document = {
        "schema_version": 1,
        "status": "incomplete",
        "suite": "python-boundaries",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            **{k: v for k, v in vars(args).items() if k != "output"},
            "metrics": METRICS,
            "scope": SCOPE,
            "seed": 698,
            "cache": "OS/library caches uncontrolled; fresh means a new process, not cold storage",
            "producer_conversion": "typed Arrow batches; pandas ArrowDtype / polars rechunk=False",
            "memory": "OS peak RSS plus sampled Arrow pool bytes; "
            "neither isolates profiler native buffers",
        },
        "cases": [],
        "runs": [],
        "preflights": [],
        "references": {},
    }
    start = time.monotonic()

    def run(request):
        remaining = args.budget_seconds - (time.monotonic() - start)
        if remaining <= 0:
            raise TimeoutError("experiment runtime budget exhausted")
        return common.run_worker(
            {**request, "tool": request["case"]["id"], "threads": args.threads},
            min(args.timeout, remaining),
            script=Path(__file__).resolve(),
        )

    try:
        common.checkpoint(args.output, document)
        document["environment"] = common.environment_metadata()
        document["environment"]["boundary_script_sha256"] = common.fingerprint(Path(__file__))
        matrix = cases(args.rows, args.chunks, args.stream_scale)
        document["fixtures"] = [
            fixture_identity(n) for n in (args.rows, args.rows * args.stream_scale)
        ]
        for case in matrix:
            if "skip_reason" in case:
                document["cases"].append({**case, "status": "skipped"})
                continue
            document["active"] = {"stage": "preflight", "case": case["id"]}
            common.checkpoint(args.output, document)
            key = str(case["rows"])
            if key not in document["references"]:
                document["references"][key] = run({"case": case, "reference": True})
            expected = document["references"][key]["columns"]
            checked = run({"case": case, "preflight": True, "expected": expected})
            document["preflights"].append({"case": case["id"], **checked})
            document["cases"].append({**case, "status": "validated"})
            common.checkpoint(args.output, document)
        # Every cell validates before timings are compared. Interleave fresh workers.
        supported = [case for case in matrix if "skip_reason" not in case]
        schedule = [(case, "fresh", i) for i in range(args.iterations) for case in supported]
        schedule += [(case, "warm", 0) for case in supported]
        random.Random(698).shuffle(schedule)
        for case, condition, repeat in schedule:
            document["active"] = {"stage": condition, "case": case["id"], "repeat": repeat}
            common.checkpoint(args.output, document)
            result = run(
                {
                    "case": case,
                    "expected": document["references"][str(case["rows"])]["columns"],
                    "warmups": args.warmups if condition == "warm" else 0,
                    "iterations": args.iterations if condition == "warm" else 1,
                }
            )
            document["runs"].append(
                {"case": case["id"], "condition": condition, "repeat": repeat, **result}
            )
            common.checkpoint(args.output, document)
        for case in document["cases"]:
            if case["status"] != "skipped":
                case["summary"] = summarize_runs(
                    [r for r in document["runs"] if r["case"] == case["id"]]
                )
                case["status"] = "complete"
        document.pop("active", None)
        document["status"] = "complete"
        common.checkpoint(args.output, document)
        (args.output / "results.json").write_text(
            json.dumps(document, indent=2, allow_nan=False) + "\n", encoding="utf-8"
        )
        (args.output / "boundaries.md").write_text(render(document), encoding="utf-8")
        print(f"Completed {len(supported)} cases; evidence in {args.output}")
        return 0
    except Exception as exc:
        document["failure"] = {
            "error": str(exc),
            "traceback": traceback.format_exc(),
            **getattr(exc, "diagnostics", {}),
        }
        common.checkpoint(args.output, document)
        print(f"boundary experiment failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    if sys.argv[1:] == ["--worker"]:
        request = json.load(sys.stdin)
        print(
            json.dumps(
                preflight(request) if request.get("preflight") else worker(request), allow_nan=False
            )
        )
    else:
        raise SystemExit(main())
