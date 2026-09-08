# Python bindings architecture

The Python package presents one public API over the native PyO3 extension.
`python/dataprof/__init__.py` and its stub are re-export facades: callers keep
the same public names and signatures, while implementation ownership lives in
private modules. The native extension remains `dataprof._dataprof`, implemented by
`crates/dataprof-python`.

## Module ownership

| Module | Responsibility |
| --- | --- |
| `_api.py` | Dispatch `profile()` inputs, construct native options, and wrap native reports; expose the lightweight file and pattern helpers. |
| `_inputs.py` | Decode dependency-free Python inputs and byte buffers into named columns, preserving parse errors and field order. |
| `_paths.py` | Normalize path-like values and validate existing files. |
| `_profiler.py` | Accumulate builder options, then call the same profiling entry point. |
| `_capabilities.py` | Read native feature metadata and discover optional Python dependencies without importing them. |
| `_database.py` | Wrap optional native database profiling and provide actionable errors when database support is unavailable. |
| `_report.py` | Own `ProfileReport`: accessors, exports, comparison, persistence, and bounded model context. |
| `_report_backing.py` | Reconstruct read-only report, column, pattern, and quality objects from serialized documents. |
| `_columns.py` | Build serialized column records and interpret shared column evidence, including dominant patterns. |
| `_render.py` | Format statistics, quality flags, redacted samples, and token-budgeted summary sections. |
| `_rounding.py` | Apply the existing numeric rounding convention. |
| `_report_schema.py` | Declare the report schema version and quality-dimension order. |

## Dependency direction

The facade imports implementations. Implementations import their dependencies
directly, never back through the facade. The builder depends on profiling
dispatch; dispatch and the async/database wrappers depend on the public report
wrapper. Input decoding does not depend on report rendering.

The report wrapper uses the restored-report adapters, column conversion, and
rendering helpers. Both restored columns and rendering depend on column evidence
helpers. The column module's reference to the restored pattern type is only a
type-checking import, so it does not create a runtime cycle.

`dataprof.asyncio` returns the same high-level `ProfileReport` as synchronous
profiling. `dataprof.interop` continues to expose the raw native types for
advanced callers. `dataprof.agent` applies its policy around the shared profiling
and report implementations. These entry points add neither a second profiler
nor a dependency from the core reporting modules back to the facade.

Modules that declare public Python objects have matching `.pyi` files. The
facade stub re-exports those declarations and the native extension's types.
Private helpers without separate stubs retain their inline annotations.
Python-owned public classes and functions retain `dataprof` as their module
identity so the implementation move does not change their public pickle paths.

## Report backing follow-up

This split establishes the boundary for #516. Native reports and restored
reports still have their existing implementations; `_report_backing.py` isolates
the restored side so a shared accessor protocol can be introduced separately.
That follow-up must preserve the distinction between absent and empty evidence,
the report schema contract, and the parity covered by
`python/tests/test_report_roundtrip_parity.py`.

The split in #515 changes ownership and imports. It does not change metric
semantics, input handling, public signatures, or the deprecation schedule, and
it makes no import-performance claim.
