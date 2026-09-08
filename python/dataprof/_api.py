"""Profiling dispatch and lightweight wrappers over the native extension."""

from __future__ import annotations as _annotations

import io as _io
import json as _json
import os as _os
import pathlib as _pathlib
import warnings as _warnings
from collections.abc import Callable as _Callable
from typing import Any as _Any

from ._dataprof import (
    ProfilerConfig,
    ProgressEvent,
    RowCountEstimate,
    SamplingStrategy,
    SchemaResult,
    StopCondition,
    StructureReport,
    analyze_file as _analyze_file,
    analyze_structure as _analyze_structure,
    infer_schema as _infer_schema,
    list_patterns as _list_patterns,
    profile_arrow as _profile_arrow,
    profile_columns as _profile_columns,
    profile_dataframe as _profile_dataframe,
    profile_parquet_bytes as _profile_parquet_bytes,
    quick_row_count as _quick_row_count,
)
from ._inputs import (
    _bytes_buffer,
    _Column,
    _columns_from_csv_bytes,
    _columns_from_dict,
    _columns_from_records,
    _is_list_of_dicts,
    _NonStandardJsonConstant,
    _scan_json_array_records,
    _scan_jsonl_records,
    _strict_json_loads,
)
from ._paths import _normalize_existing_file
from ._report import ProfileReport


def infer_schema(path: str | _os.PathLike[str]) -> SchemaResult:
    """Infer a file schema from a string path or path-like object."""
    return _infer_schema(_normalize_existing_file(path))


def quick_row_count(path: str | _os.PathLike[str]) -> RowCountEstimate:
    """Estimate or count rows from a string path or path-like object."""
    return _quick_row_count(_normalize_existing_file(path))


def analyze_structure(
    path: str | _os.PathLike[str],
    max_rows: int | None = None,
) -> StructureReport:
    """Analyze file structure with a bounded, lightweight pass."""
    return _analyze_structure(_normalize_existing_file(path), max_rows)


def list_patterns(locale: str | None = None) -> list[dict[str, _Any]]:
    """List supported pattern detectors.

    Args:
        locale: Optional locale. When provided, the result includes universal
            patterns plus the patterns specific to that locale. Supported:
            "CA", "DE", "FR", "GB", "IT", "US". Case, the ISO 3166-1 alpha-3
            spelling ("ITA") and the BCP 47 / POSIX forms ("it-IT", "it_IT")
            all normalise; an empty string means no locale.

    Returns:
        A list of dicts with ``name``, ``regex``, ``category``, ``locale``, and
        ``min_threshold`` keys, in detector order.

    Raises:
        ValueError: If ``locale`` names no supported locale.
    """
    return _list_patterns(locale)


def profile_file(
    path: str | _os.PathLike[str],
    *,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    format: str | None = None,
    max_rows: int | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    jsonl_on_error: str = "skip",
    sampling: SamplingStrategy | None = None,
    stop_condition: StopCondition | None = None,
    on_progress: _Callable[[ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    columns: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
    temporal_columns: list[str] | None = None,
) -> ProfileReport:
    """Profile a file path and return a report.

    This is the explicit file-oriented counterpart to :func:`profile`, with
    all file-only options kept in one place.

    Args:
        path: File path to profile.
        engine: File profiling engine ("auto", "incremental", "columnar").
            "streaming" aliases "incremental"; "arrow" aliases "columnar".
            Names are case-insensitive. CSV execution metadata reports the
            selected canonical engine ("incremental" or "columnar").
        chunk_size: Bytes to read per streaming chunk (None = adaptive).
            Bounds the working set and the granularity at which progress
            and stop conditions are evaluated; it never changes the
            result of a complete scan.
        memory_limit_mb: Memory limit in MB, applied by the incremental,
            columnar and async engines. Bounds retained per-column state;
            it does not drop rows.
        format: Override format detection ("csv", "json", "jsonl", "parquet").
            "json" reads one standard JSON document — an array of objects, or
            one object as a single record — which may be pretty-printed across
            lines. "jsonl" reads one record per physical line: a record may not
            span lines, and a line may not hold more than one value. Byte
            sources have no extension to read the grammar from, so they require
            this argument.
        max_rows: Maximum rows to process before stopping.
        csv_delimiter: Single-character CSV delimiter (default: detected
            from the data).
        csv_flexible: Allow variable-length CSV records. Applies to file and
            async inputs, where True (default) recovers a row whose field count
            differs from the header and counts it in
            ``report.ragged_row_count``, and False raises ValueError on the
            first such row. CSV bytes are always parsed strictly, and
            ``engine="columnar"`` does not report the count.
        jsonl_on_error: How to handle a JSON/JSONL record that cannot become a
            row — malformed JSON, or valid JSON that is not an object and so
            has no fields to profile: "skip" (default) skips it, counts it in
            ``report.execution.error_count``, and returns a partial profile;
            "strict" raises ValueError on the first such record. A file with no
            valid records always fails.
        sampling: Sampling strategy (e.g. SamplingStrategy.random(1000)).
        stop_condition: Early stop condition (e.g. StopCondition.max_rows(5000)).
            Cannot be used together with max_rows.
        on_progress: Callable receiving ProgressEvent objects during profiling.
            Only effective with engine="incremental". Files that finish faster
            than ``progress_interval_ms`` may emit only start and finish events.
        progress_interval_ms: Minimum interval between progress events in ms
            (default: 500).
        quality_dimensions: List of quality dimensions to evaluate.
            Valid values: "completeness", "consistency", "uniqueness",
            "accuracy", "timeliness", "validity", "precision". None = all
            dimensions (default). An empty list requests no dimension, which
            means quality is not analyzed at all: ``quality`` and
            ``quality_score`` are None, the same as omitting "quality" from
            ``metrics``.
        metrics: List of metric packs to compute. Valid values: "schema"
            (always included), "statistics", "patterns", "quality".
            None = all packs (default).
        columns: Top-level columns to profile. None selects every column;
            an empty list returns a report with no profiled columns. Reports
            preserve source order rather than the order supplied here. Any
            explicit selection withholds row-level completeness and uniqueness.
        locale: Locale for pattern detection. Supported: "CA", "DE", "FR",
            "GB", "IT", "US"; "it", "ITA" and "it-IT" all mean "IT", and an
            unsupported tag raises ValueError rather than silently suppressing
            every locale-specific pattern.
        positive_columns: Columns whose numeric values are expected to be
            non-negative.
        identifier_columns: Numeric-looking columns to treat as semantic
            identifiers instead of measures.
        temporal_columns: Columns whose values should contribute to timeliness
            quality metrics.

    Returns:
        ProfileReport with analysis results and quality metrics.
    """
    if jsonl_on_error not in ("skip", "strict"):
        raise ValueError(f"jsonl_on_error must be 'skip' or 'strict', got {jsonl_on_error!r}.")
    normalized_path = _normalize_existing_file(path, arg_name="path")
    config = ProfilerConfig(
        engine=engine,
        chunk_size=chunk_size,
        memory_limit_mb=memory_limit_mb,
        format=format,
        max_rows=max_rows,
        csv_delimiter=csv_delimiter,
        csv_flexible=csv_flexible,
        jsonl_on_error=jsonl_on_error,
        sampling=sampling,
        stop_condition=stop_condition,
        on_progress=on_progress,
        progress_interval_ms=progress_interval_ms,
        quality_dimensions=quality_dimensions,
        metrics=metrics,
        columns=columns,
        locale=locale,
        positive_columns=positive_columns,
        identifier_columns=identifier_columns,
        temporal_columns=temporal_columns,
    )
    rust_report = _analyze_file(normalized_path, config)
    return ProfileReport(rust_report)


def profile(
    source: _Any,
    *,
    engine: str = "auto",
    chunk_size: int | None = None,
    memory_limit_mb: int | None = None,
    format: str | None = None,
    max_rows: int | None = None,
    name: str | None = None,
    csv_delimiter: str | None = None,
    csv_flexible: bool | None = None,
    jsonl_on_error: str = "skip",
    sampling: SamplingStrategy | None = None,
    stop_condition: StopCondition | None = None,
    on_progress: _Callable[[ProgressEvent], None] | None = None,
    progress_interval_ms: int | None = None,
    quality_dimensions: list[str] | None = None,
    metrics: list[str] | None = None,
    columns: list[str] | None = None,
    locale: str | None = None,
    positive_columns: list[str] | None = None,
    identifier_columns: list[str] | None = None,
    temporal_columns: list[str] | None = None,
) -> ProfileReport:
    """Profile a data source and return a report.

    Accepts file paths (str/Path), pandas DataFrames, polars DataFrames,
    Arrow PyCapsule-compatible objects, dict/list-of-dicts, and bytes-like
    file contents when ``format=`` is provided. Synchronous bytes use the
    in-memory columnar path and reject streaming-only controls such as
    ``chunk_size``, ``memory_limit_mb``, ``stop_condition``, and progress
    callbacks; use ``dataprof.asyncio.profile_bytes()`` for those.

    Args:
        source: Data source to profile.
        engine: File profiling engine ("auto", "incremental", "columnar").
            "streaming" aliases "incremental"; "arrow" aliases "columnar".
            Names are case-insensitive. CSV execution metadata reports the
            selected canonical engine ("incremental" or "columnar").
        chunk_size: Bytes to read per streaming chunk (None = adaptive).
            Bounds the working set and the granularity at which progress
            and stop conditions are evaluated; it never changes the
            result of a complete scan.
        memory_limit_mb: Memory limit in MB, applied by the incremental,
            columnar and async engines. Bounds retained per-column state;
            it does not drop rows.
        format: Override format detection ("csv", "json", "jsonl", "parquet").
            "json" reads one standard JSON document — an array of objects, or
            one object as a single record — which may be pretty-printed across
            lines. "jsonl" reads one record per physical line: a record may not
            span lines, and a line may not hold more than one value. Byte
            sources have no extension to read the grammar from, so they require
            this argument.
        max_rows: Maximum rows to process before stopping.
        name: Name for DataFrame sources in the report.
        csv_delimiter: Single-character CSV delimiter (default: detected
            from the data).
        csv_flexible: Allow variable-length CSV records. Applies to file and
            async inputs, where True (default) recovers a row whose field count
            differs from the header and counts it in
            ``report.ragged_row_count``, and False raises ValueError on the
            first such row. CSV bytes are always parsed strictly, and
            ``engine="columnar"`` does not report the count.
        jsonl_on_error: How to handle a JSON/JSONL record that cannot become a
            row — malformed JSON, or valid JSON that is not an object and so has
            no fields to profile — applied identically to file, bytes, and async
            byte inputs: "skip" (default) skips it, counts it in
            ``report.execution.error_count``, and returns a partial profile;
            "strict" raises ValueError on the first such record. Input with no
            valid records always raises.
        sampling: Sampling strategy (e.g. SamplingStrategy.random(1000)).
        stop_condition: Early stop condition (e.g. StopCondition.max_rows(5000)).
            Cannot be used together with max_rows.
        on_progress: Callable receiving ProgressEvent objects during profiling.
            Only effective with engine="incremental". Files that finish faster
            than ``progress_interval_ms`` may emit only start and finish events.
        progress_interval_ms: Minimum interval between progress events in ms
            (default: 500).
        quality_dimensions: List of quality dimensions to evaluate.
            Valid values: "completeness", "consistency", "uniqueness",
            "accuracy", "timeliness", "validity", "precision". None = all
            dimensions (default). An empty list requests no dimension, which
            means quality is not analyzed at all: ``quality`` and
            ``quality_score`` are None, the same as omitting "quality" from
            ``metrics``.
        metrics: List of metric packs to compute. Valid values: "schema"
            (always included), "statistics", "patterns", "quality".
            None = all packs (default). Omitting a pack skips that
            category of computation entirely.
        columns: Top-level columns to profile. None selects every column;
            an empty list returns a report with no profiled columns. Reports
            preserve source order rather than the order supplied here. Any
            explicit selection withholds row-level completeness and uniqueness.
        locale: Locale for pattern detection. Boosts confidence for
            locale-matching patterns and suppresses non-matching locale
            patterns. None = no preference. Supported: "CA", "DE", "FR", "GB",
            "IT", "US"; "it", "ITA" and "it-IT" all mean "IT", and an
            unsupported tag raises ValueError rather than silently suppressing
            every locale-specific pattern.
        positive_columns: Columns whose numeric values are expected to be
            non-negative.
        identifier_columns: Numeric-looking columns to treat as semantic
            identifiers instead of measures.
        temporal_columns: Columns whose values should contribute to timeliness
            quality metrics.

    Returns:
        ProfileReport with analysis results and quality metrics.
    """
    if jsonl_on_error not in ("skip", "strict"):
        raise ValueError(f"jsonl_on_error must be 'skip' or 'strict', got {jsonl_on_error!r}.")

    # File path — build config and delegate to Rust
    if isinstance(source, (str, _pathlib.PurePath)):
        return profile_file(
            source,
            engine=engine,
            chunk_size=chunk_size,
            memory_limit_mb=memory_limit_mb,
            format=format,
            max_rows=max_rows,
            csv_delimiter=csv_delimiter,
            csv_flexible=csv_flexible,
            jsonl_on_error=jsonl_on_error,
            sampling=sampling,
            stop_condition=stop_condition,
            on_progress=on_progress,
            progress_interval_ms=progress_interval_ms,
            quality_dimensions=quality_dimensions,
            metrics=metrics,
            columns=columns,
            locale=locale,
            positive_columns=positive_columns,
            identifier_columns=identifier_columns,
            temporal_columns=temporal_columns,
        )

    # DataFrame/Arrow paths — build config for metric packs + quality dims + locale
    def _df_config() -> ProfilerConfig | None:
        """Build a ProfilerConfig if any DataFrame-relevant options are set."""
        if any(
            v is not None
            for v in (
                max_rows,
                quality_dimensions,
                metrics,
                columns,
                locale,
                positive_columns,
                identifier_columns,
                temporal_columns,
            )
        ):
            return ProfilerConfig(
                max_rows=max_rows,
                quality_dimensions=quality_dimensions,
                metrics=metrics,
                columns=columns,
                locale=locale,
                positive_columns=positive_columns,
                identifier_columns=identifier_columns,
                temporal_columns=temporal_columns,
            )
        return None

    # Warn about file-only kwargs that are ignored for DataFrame/Arrow sources
    _file_only_kwargs = {
        "engine": engine != "auto",
        "chunk_size": chunk_size is not None,
        "memory_limit_mb": memory_limit_mb is not None,
        "format": format is not None,
        "csv_delimiter": csv_delimiter is not None,
        "csv_flexible": csv_flexible is not None,
        "sampling": sampling is not None,
        "stop_condition": stop_condition is not None,
        "on_progress": on_progress is not None,
        "progress_interval_ms": progress_interval_ms is not None,
    }

    def _warn_if_config_ignored():
        ignored = [k for k, v in _file_only_kwargs.items() if v]
        if ignored:
            _warnings.warn(
                f"Config kwargs {ignored} are ignored for DataFrame/Arrow sources. "
                "These options only apply to file paths.",
                stacklevel=3,
            )

    def _profile_python_dataframe(df: object, default_name: str) -> ProfileReport:
        rust_report = _profile_dataframe(df, name or default_name, max_rows, _df_config())
        return ProfileReport(rust_report)

    def _profile_python_columns(
        columns: list[_Column],
        default_name: str,
        error_count: int = 0,
        row_count: int | None = None,
        source_type: str = "dataframe",
        source_format: str | None = None,
        source_bytes: int | None = None,
    ) -> ProfileReport:
        rust_report = _profile_columns(
            columns,
            name or default_name,
            max_rows,
            _df_config(),
            error_count,
            row_count,
            source_type,
            source_format,
            source_bytes,
        )
        return ProfileReport(rust_report)

    if isinstance(source, dict):
        # A dict is a mapping of column name to cells, so an empty one is a
        # source with no columns and therefore no rows. That is a different
        # input from a record with no fields (`[{}]`), which is one row.
        _warn_if_config_ignored()
        return _profile_python_columns(_columns_from_dict(source), "dataframe")

    if _is_list_of_dicts(source):
        _warn_if_config_ignored()
        record_columns, record_rows = _columns_from_records(source, max_rows)
        return _profile_python_columns(record_columns, "dataframe", row_count=record_rows)

    if isinstance(source, (bytes, bytearray, memoryview, _io.BytesIO)):
        if format is None:
            raise ValueError(
                "bytes and BytesIO sources require format='csv', 'json', 'jsonl', or 'parquet'. "
                "For async byte streams, use dataprof.asyncio.profile_bytes(data, format='csv')."
            )
        unsupported: list[str] = []
        if engine not in ("auto", "columnar"):
            unsupported.append("engine")
        if chunk_size is not None:
            unsupported.append("chunk_size")
        if memory_limit_mb is not None:
            unsupported.append("memory_limit_mb")
        if stop_condition is not None:
            unsupported.append("stop_condition")
        if on_progress is not None:
            unsupported.append("on_progress")
        if progress_interval_ms is not None:
            unsupported.append("progress_interval_ms")
        if csv_flexible is True:
            unsupported.append("csv_flexible=True")
        if unsupported:
            joined = ", ".join(unsupported)
            raise ValueError(
                f"Synchronous bytes input cannot apply: {joined}. "
                "Use max_rows for a row cap, or dataprof.asyncio.profile_bytes() "
                "for streaming controls."
            )
        if sampling is not None and not getattr(sampling, "is_noop", False):
            # Synchronous byte input is read by the pure-Python reader, which has
            # no row-by-row sampling stage. Returning a full profile while the
            # caller asked for a sample would misreport what was analyzed. An
            # explicit SamplingStrategy.none() asks for nothing, so it passes.
            raise ValueError(
                "sampling is not applied to synchronous bytes input. Write the data to a "
                "file, or use dataprof.asyncio.profile_bytes(data, format=..., "
                "sampling=...), which samples while streaming."
            )
        fmt = format.lower()
        buffer = _bytes_buffer(source)

        skipped = 0
        row_count: int | None = None
        if fmt == "csv":
            decoded_columns = _columns_from_csv_bytes(buffer, csv_delimiter)
        elif fmt == "jsonl":
            text = buffer.getvalue().decode("utf-8-sig")
            rows, skipped = _scan_jsonl_records(text, jsonl_on_error)
            decoded_columns, row_count = _columns_from_records(rows, max_rows)
        elif fmt == "json":
            text = buffer.getvalue().decode("utf-8-sig")
            try:
                rows = _strict_json_loads(text)
            except _json.JSONDecodeError as exc:
                # Surface a dataprof error category, not a bare decoder exception.
                raise ValueError(
                    f"json bytes: malformed JSON (line {exc.lineno}, column {exc.colno})."
                ) from None
            except _NonStandardJsonConstant as exc:
                raise ValueError(
                    f"json bytes: malformed JSON (non-standard numeric constant {exc.value!r})."
                ) from None
            if isinstance(rows, dict):
                # `all(...)` is vacuously true for `{}`, which would read an
                # empty object as a column-oriented document holding no rows.
                # The file scanner reads a root `{}` as one record instead, so
                # require at least one column before taking that branch.
                if rows and all(isinstance(values, (list, tuple)) for values in rows.values()):
                    decoded_columns = _columns_from_dict(rows)
                else:
                    decoded_columns, row_count = _columns_from_records([rows], max_rows)
            elif isinstance(rows, list):
                # An array is a document of records: elements that are not
                # objects follow the same policy the file and async scanners
                # apply, instead of rejecting the whole array.
                records, skipped = _scan_json_array_records(rows, jsonl_on_error)
                decoded_columns, row_count = _columns_from_records(records, max_rows)
            else:
                raise ValueError(
                    f"{fmt} bytes must decode to an object of columns or an array of "
                    f"row objects, got {type(rows).__name__}."
                )
        elif fmt == "parquet":
            # Read by the compiled Arrow/Parquet stack, the same one the file
            # path uses. Routing through pandas would put an optional
            # dependency behind an API documented as needing none, and would
            # change types and column order on the way through.
            return ProfileReport(
                _profile_parquet_bytes(
                    buffer.getvalue(),
                    name or "parquet_bytes",
                    max_rows,
                    _df_config(),
                )
            )
        else:
            raise ValueError("Unsupported bytes format. Use 'csv', 'json', 'jsonl', or 'parquet'.")

        # The buffer's own length, not the decoded cells: `size_bytes` on the
        # report is a statement about the input the caller handed over.
        return _profile_python_columns(
            decoded_columns,
            f"{fmt}_bytes",
            skipped,
            row_count,
            "bytes",
            fmt,
            len(buffer.getvalue()),
        )

    # DataFrame detection via module name
    source_module = type(source).__module__ or ""

    if source_module.startswith("pandas"):
        _warn_if_config_ignored()
        return _profile_python_dataframe(source, "dataframe")

    if source_module.startswith("polars"):
        _warn_if_config_ignored()
        return _profile_python_dataframe(source, "dataframe")

    # PyArrow objects (Table, RecordBatch) or any Arrow PyCapsule-compatible object
    if source_module.startswith("pyarrow") or hasattr(source, "__arrow_c_array__"):
        _warn_if_config_ignored()
        rust_report = _profile_arrow(source, name or "arrow_data", max_rows, _df_config())
        return ProfileReport(rust_report)

    raise TypeError(
        f"Unsupported source type: {type(source).__module__}.{type(source).__name__}. "
        "Expected a file path (str/Path), pandas DataFrame, polars DataFrame, "
        "an object implementing the Arrow PyCapsule protocol, dict/list-of-dicts, "
        "or bytes/BytesIO with format=."
    )


# Preserve the public import path for introspection and pickled API objects.
infer_schema.__module__ = "dataprof"
quick_row_count.__module__ = "dataprof"
analyze_structure.__module__ = "dataprof"
list_patterns.__module__ = "dataprof"
profile_file.__module__ = "dataprof"
profile.__module__ = "dataprof"
