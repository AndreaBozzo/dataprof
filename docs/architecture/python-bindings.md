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
| `_accessors.py` | Define the shared read-only column, pattern, quality, and report views over native and mapping accessors. |
| `_report_backing.py` | Normalize saved document layout and legacy defaults into mapping accessors. |
| `_columns.py` | Build serialized column records and interpret shared column evidence, including dominant patterns. |
| `_render.py` | Format statistics, quality flags, redacted samples, and token-budgeted summary sections. |
| `_rounding.py` | Apply the existing numeric rounding convention. |
| `_report_schema.py` | Declare the report schema version and quality-dimension order. |

## Dependency direction

The facade imports implementations. Implementations import their dependencies
directly, never back through the facade. The builder depends on profiling
dispatch; dispatch and the async/database wrappers depend on the public report
wrapper. Input decoding does not depend on report rendering.

The report wrapper uses shared views, saved-document normalization, column
conversion, and rendering helpers. Column conversion and rendering depend on
the shared views; saved-document normalization also uses column evidence
helpers. The accessor module depends only on typing and the native extension,
so these imports do not create a cycle.

`dataprof.asyncio` returns the same high-level `ProfileReport` as synchronous
profiling. `dataprof.interop` continues to expose the raw native types for
advanced callers. `dataprof.agent` applies its policy around the shared profiling
and report implementations. These entry points add neither a second profiler
nor a dependency from the core reporting modules back to the facade.

Most modules that declare public Python objects have matching `.pyi` files;
the shared views use typed descriptors and inline annotations. The facade stub
re-exports these declarations and the native extension's remaining public types.
Private helpers without separate stubs also retain their inline annotations.
Python-owned public classes and functions retain `dataprof` as their module
identity so the implementation move does not change their public pickle paths.

## Shared report accessors

#516 replaces the separate restored stand-ins with one set of public views.
Both backings satisfy `_Accessor.get(name)`: the native accessor reads raw
extension values without serializing or rounding, while the mapping accessor
reads normalized saved values. Mapping accessors detach collection values on
construction and retrieval, so neither caller-owned input dictionaries nor
returned dictionaries can mutate the saved report. Nested accessors are retained.

Column fields declare their persisted stats section next to the accessor.
The saved-document normalizer owns only layout and legacy defaults; public
accessors, representations, and removed-name handling are shared. Native and
restored views preserve absent versus empty evidence and the existing schema.
`test_report_roundtrip_parity.py` checks both backings, and
`test_report_accessor_protocol.py` checks exact raw native values and the shared
surface. The two persisted document dialects are still separate; #714 owns that
contract decision.

The split in #515 changes ownership and imports. It does not change metric
semantics, input handling, public signatures, or the deprecation schedule, and
it makes no import-performance claim.
