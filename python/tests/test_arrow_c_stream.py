"""One-shot Arrow streams are consumed incrementally through their capsule."""

import subprocess
import sys

import dataprof
import pytest

pa = pytest.importorskip("pyarrow")


def _quality_values(report):
    return {key: value for key, value in report.quality_summary().items() if key != "source"}


class StreamProducer:
    def __init__(self, schema, batches):
        self.reader = pa.RecordBatchReader.from_batches(schema, batches)
        self.exports = 0

    def __arrow_c_stream__(self, requested_schema=None):
        self.exports += 1
        return self.reader.__arrow_c_stream__(requested_schema)

    def to_arrow_table(self):
        raise AssertionError("must not materialize the stream")

    def to_batches(self):
        raise AssertionError("must not collect the stream")


def _batches():
    return [
        pa.record_batch({"z": [2**60, None], "a": ["Roma", None]}),
        pa.record_batch({"z": [2**60 + 1, 4], "a": ["東京", "Roma"]}),
    ]


@pytest.mark.parametrize("custom", [False, True])
def test_stream_matches_table(custom):
    batches = _batches()
    source = (
        StreamProducer(batches[0].schema, iter(batches))
        if custom
        else pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    )
    report = dataprof.profile(source)
    reference = dataprof.profile(pa.Table.from_batches(batches))
    assert report.to_dict()["columns"] == reference.to_dict()["columns"]
    assert report.rows == 4
    assert report.source_type == "stream"
    assert report.engine == "columnar"
    assert report.truncation_reason is None


@pytest.mark.parametrize("limit, reads", [(0, 0), (1, 1), (2, 1), (3, 2), (4, 2)])
def test_row_cap_does_not_fetch_remaining_batches(limit, reads):
    batches = _batches()
    seen = []

    def produce():
        for batch in batches:
            seen.append(1)
            yield batch
        raise AssertionError("read past the requested row cap")

    report = dataprof.profile(StreamProducer(batches[0].schema, produce()), max_rows=limit)
    assert report.rows == limit
    assert len(seen) == reads
    assert report.truncation_reason is not None


def test_empty_stream_preserves_schema_and_absence():
    schema = _batches()[0].schema
    report = dataprof.profile(StreamProducer(schema, []))
    reference = dataprof.profile(pa.Table.from_batches([], schema=schema))
    assert report.to_dict()["columns"] == reference.to_dict()["columns"]
    assert report.rows == 0
    assert report.quality_score is None


def test_duplicate_schema_fails_before_reading_even_with_projection():
    schema = pa.schema([("x", pa.int64()), ("x", pa.int64())])

    def produce():
        raise AssertionError("schema must be checked before reading")
        yield

    with pytest.raises(ValueError, match="[Dd]uplicate"):
        dataprof.profile(StreamProducer(schema, produce()), columns=[])


@pytest.mark.parametrize(
    "nested",
    [
        pa.struct([("x", pa.int64())]),
        pa.list_(pa.int64()),
        pa.list_(pa.float32(), 3),
        pa.map_(pa.string(), pa.int64()),
    ],
)
def test_nested_stream_types_are_rejected(nested):
    with pytest.raises(TypeError, match="nested.*column|column.*nested"):
        dataprof.profile(StreamProducer(pa.schema([("nested", nested)]), []))


def test_producer_failure_does_not_return_partial_report():
    batches = _batches()

    def produce():
        yield batches[0]
        raise ValueError("producer broke after first batch")

    with pytest.raises(RuntimeError, match="Arrow stream") as caught:
        dataprof.profile(StreamProducer(batches[0].schema, produce()))
    assert "producer broke after first batch" in str(caught.value)
    assert caught.value.__cause__ is not None
    assert "producer broke after first batch" in str(caught.value.__cause__)


def test_producer_first_line_is_visible_without_losing_full_cause():
    batches = _batches()

    def produce():
        yield batches[0]
        raise OSError("connection reset by peer\nretry might be required")

    with pytest.raises(RuntimeError, match="Arrow stream batch failed") as caught:
        dataprof.profile(StreamProducer(batches[0].schema, produce()))

    assert "connection reset by peer" in str(caught.value)
    assert "retry might be required" not in str(caught.value)
    assert caught.value.__cause__ is not None
    assert "retry might be required" in str(caught.value.__cause__)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"engine": "streaming"},
        {"chunk_size": 10},
        {"metrics": ["bogus"]},
        {"locale": "bogus"},
        {"stop_condition": dataprof.StopCondition.schema_stable(1)},
        {"sampling": dataprof.SamplingStrategy.random(1)},
        {"max_rows": 1, "stop_condition": dataprof.StopCondition.max_rows(1)},
    ],
)
def test_invalid_controls_fail_before_export(kwargs):
    source = StreamProducer(_batches()[0].schema, [])
    with pytest.raises(ValueError):
        dataprof.profile(source, **kwargs)
    assert source.exports == 0


@pytest.mark.parametrize("engine", ["auto", "AUTO", "columnar", "COLUMNAR", "arrow", "ArRoW"])
def test_stream_accepts_documented_engine_spellings(engine):
    source = StreamProducer(_batches()[0].schema, _batches())
    report = dataprof.profile(source, engine=engine)
    assert report.rows == 4
    assert report.engine == "columnar"


@pytest.mark.parametrize(
    "engine", ["incremental", "INCREMENTAL", "streaming", "STREAMING", "bogus"]
)
def test_stream_rejects_unsupported_engines_before_export(engine):
    source = StreamProducer(_batches()[0].schema, _batches())
    with pytest.raises(ValueError, match="engine"):
        dataprof.profile(source, engine=engine)
    assert source.exports == 0


def test_stream_option_parity():
    batches = _batches()

    def profile(source):
        return dataprof.profile(
            source,
            columns=["a", "z"],
            metrics=["schema", "quality"],
            quality_dimensions=["completeness"],
            locale="IT",
        )

    report = profile(StreamProducer(batches[0].schema, batches))
    reference = profile(pa.Table.from_batches(batches))
    assert report.to_dict()["columns"] == reference.to_dict()["columns"]
    assert report.quality_score == reference.quality_score


def test_duckdb_relation():
    duckdb = pytest.importorskip("duckdb")
    relation = duckdb.sql("select * from (values (1, 'Rome'), (2, NULL)) t(id, city)")
    report = dataprof.profile(relation)
    reference = dataprof.profile(relation.to_arrow_table())
    assert report.to_dict()["columns"] == reference.to_dict()["columns"]


def test_batches_are_released_during_consumption():
    schema = pa.schema([("n", pa.int64())])
    before = pa.total_allocated_bytes()
    observations = []

    def produce():
        for _ in range(12):
            batch = pa.record_batch([pa.array(range(4096), type=pa.int64())], schema=schema)
            observations.append(pa.total_allocated_bytes() - before)
            yield batch

    assert dataprof.profile(StreamProducer(schema, produce())).rows == 12 * 4096
    # Allow the generator and C exporter to hold the previous/current batch.
    # Retaining all twelve imported batches exceeds this bound by a wide margin.
    assert max(observations) <= 3 * 4096 * 8


# A PyArrow scanner reads on its own I/O threads, and a filesystem written in
# Python needs the GIL on them. Run in a subprocess so a deadlock fails the test
# instead of hanging the suite.
_PYTHON_FILESYSTEM_SCAN = r"""
import sys

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs

import dataprof


class PythonFiles(pafs.FileSystemHandler):
    def __init__(self, fail_reads):
        self.local = pafs.LocalFileSystem()
        self.fail_reads = fail_reads

    def __eq__(self, other):
        return isinstance(other, PythonFiles)

    def __ne__(self, other):
        return not self == other

    def get_type_name(self):
        return "python-files"

    def equals(self, other):
        return self == other

    def normalize_path(self, path):
        return path

    def get_file_info(self, paths):
        return self.local.get_file_info(paths)

    def get_file_info_selector(self, selector):
        return self.local.get_file_info(selector)

    def open_input_file(self, path):
        # Dataset discovery reads part0's schema; fail only the scan's reads.
        if self.fail_reads and not path.endswith("part0.parquet"):
            raise OSError("object store unavailable")
        return pa.PythonFile(open(path, "rb"), mode="r")

    open_input_stream = open_input_file

    def _unsupported(self, *args):
        raise NotImplementedError

    create_dir = delete_dir = delete_dir_contents = delete_root_dir_contents = _unsupported
    delete_file = move = copy_file = open_output_stream = open_append_stream = _unsupported


root, case = sys.argv[1:]
filesystem = pafs.PyFileSystem(PythonFiles(fail_reads=case == "error"))
reader = ds.dataset(root, filesystem=filesystem, format="parquet").scanner().to_reader()
try:
    report = dataprof.profile(reader, max_rows=1 if case == "cap" else None)
except RuntimeError as error:
    print(f"error: {error}")
else:
    print(f"rows: {report.rows}")
"""


@pytest.mark.parametrize(
    "case, expected",
    [
        ("full", "rows: 8000"),
        ("cap", "rows: 1"),
        ("error", "error: Arrow stream batch failed"),
    ],
)
def test_producer_threads_that_need_the_gil_do_not_deadlock(tmp_path, case, expected):
    pq = pytest.importorskip("pyarrow.parquet")
    pytest.importorskip("pyarrow.dataset")
    for part in range(8):
        values = list(range(part * 1000, (part + 1) * 1000))
        pq.write_table(
            pa.table({"x": values}), tmp_path / f"part{part}.parquet", row_group_size=100
        )
    try:
        result = subprocess.run(
            [sys.executable, "-c", _PYTHON_FILESYSTEM_SCAN, tmp_path.as_posix(), case],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(f"profile() deadlocked on a scanner over a Python filesystem ({case})")
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith(expected), result.stdout
    if case == "error":
        assert "object store unavailable" in result.stdout


def test_export_exception_keeps_original_cause():
    class Broken:
        def __arrow_c_stream__(self, requested_schema=None):
            raise ValueError("export failed") from OSError("producer unavailable")

    with pytest.raises(ValueError, match="export failed") as caught:
        dataprof.profile(Broken())
    assert isinstance(caught.value.__cause__, OSError)


def test_capsule_is_consumed_only_once():
    reader = pa.RecordBatchReader.from_batches(_batches()[0].schema, _batches())

    class ReusedCapsule:
        capsule = reader.__arrow_c_stream__()

        def __arrow_c_stream__(self, requested_schema=None):
            return self.capsule

    source = ReusedCapsule()
    assert dataprof.profile(source).rows == 4
    with pytest.raises(ValueError, match="consumed or released"):
        dataprof.profile(source)


def test_wrong_capsule_type_is_rejected():
    class WrongCapsule:
        def __arrow_c_stream__(self, requested_schema=None):
            return pa.schema([]).__arrow_c_schema__()

    with pytest.raises(TypeError, match="arrow_array_stream"):
        dataprof.profile(WrongCapsule())


@pytest.mark.parametrize("columns", [[], ["a"]])
def test_projection_preserves_rows_and_withholds_whole_row_metrics(columns):
    batches = _batches()
    report = dataprof.profile(StreamProducer(batches[0].schema, batches), columns=columns)
    reference = dataprof.profile(pa.Table.from_batches(batches), columns=columns)
    assert report.rows == reference.rows == 4
    assert report.to_dict()["columns"] == reference.to_dict()["columns"]
    assert _quality_values(report) == _quality_values(reference)


def test_semantic_hints_match_table():
    batches = [pa.record_batch({"id": [101, 102], "amount": [-1, 2]})]

    def profile(source):
        return dataprof.profile(source, identifier_columns=["id"], positive_columns=["amount"])

    report = profile(StreamProducer(batches[0].schema, batches))
    reference = profile(pa.Table.from_batches(batches))
    assert report.to_dict()["columns"] == reference.to_dict()["columns"]
    assert _quality_values(report) == _quality_values(reference)


def test_validation_respects_row_cap_and_nonzero_offsets():
    column = pa.DictionaryArray.from_arrays(
        pa.array([1, 0, 99], type=pa.int8()), pa.array(["a", "b"]), safe=False
    )
    batch = pa.record_batch({"c": column}).slice(1)
    report = dataprof.profile(StreamProducer(batch.schema, [batch]), max_rows=1)
    reference = dataprof.profile(batch, max_rows=1)
    assert report.to_dict()["columns"] == reference.to_dict()["columns"]
    with pytest.raises(ValueError, match="Arrow columnar spec"):
        dataprof.profile(StreamProducer(batch.schema, [batch]))


@pytest.mark.parametrize("entrypoint", ["profile_arrow", "profile_dataframe"])
def test_direct_interop_stream_entrypoints(entrypoint):
    from dataprof import interop

    source = StreamProducer(_batches()[0].schema, _batches())
    report = getattr(interop, entrypoint)(source, max_rows=3)
    assert report.rows_processed == 3
    assert report.source_type == "stream"


def test_stream_provenance_and_cap_survive_serialization():
    import json
    from pathlib import Path

    from jsonschema import Draft202012Validator

    source = StreamProducer(_batches()[0].schema, _batches())
    report = dataprof.profile(source, name="events", max_rows=3)
    document = report.to_dict()
    schema_path = Path(__file__).resolve().parents[2] / "docs/schema/profile-report.v1.schema.json"
    Draft202012Validator(json.loads(schema_path.read_text(encoding="utf-8"))).validate(document)
    restored = dataprof.ProfileReport.from_json(report.to_json())
    assert restored.to_dict() == document
    assert restored.source_type == "stream"
    assert restored.truncation_reason == report.truncation_reason


@pytest.mark.parametrize("stage", ["schema", "batch", "end", "cap"])
@pytest.mark.parametrize("error_text", [None, b"producer diagnostic"])
def test_c_callbacks_release_once_and_allow_missing_error_text(stage, error_text):
    """A minimal C producer checks ownership and C errors independently of PyArrow's reader."""
    import ctypes as ct

    class CStream(ct.Structure):
        _fields_ = [
            (name, ct.c_void_p)
            for name in ("get_schema", "get_next", "get_last_error", "release", "private_data")
        ]

    fetch = ct.CFUNCTYPE(ct.c_int, ct.c_void_p, ct.c_void_p)
    release_callback = ct.CFUNCTYPE(None, ct.c_void_p)
    last_error_callback = ct.CFUNCTYPE(ct.c_void_p, ct.c_void_p)
    released = []
    fetched = []
    message = ct.create_string_buffer(error_text) if error_text is not None else None

    @fetch
    def get_schema(_stream, out):
        if stage == "schema":
            return 5
        pa.schema([("x", pa.int64())])._export_to_c(out)
        return 0

    @fetch
    def get_next(_stream, _out):
        fetched.append(1)
        # The initialized output remains released, signalling end-of-stream.
        return 5 if stage == "batch" else 0

    @last_error_callback
    def get_last_error(_stream):
        return ct.addressof(message) if message is not None else None

    @release_callback
    def release(stream):
        released.append(1)
        ct.cast(stream, ct.POINTER(CStream)).contents.release = None

    stream = CStream(
        *[
            ct.cast(callback, ct.c_void_p).value
            for callback in (get_schema, get_next, get_last_error, release)
        ],
        None,
    )
    # Also cover a producer that omits get_last_error entirely.
    if error_text is None:
        stream.get_last_error = None
    new_capsule = ct.pythonapi.PyCapsule_New
    new_capsule.argtypes = [ct.c_void_p, ct.c_char_p, ct.c_void_p]
    new_capsule.restype = ct.py_object

    class Producer:
        def __arrow_c_stream__(self, requested_schema=None):
            return new_capsule(ct.addressof(stream), b"arrow_array_stream", None)

    if stage in ("schema", "batch"):
        with pytest.raises(RuntimeError, match=f"Arrow stream {stage} failed") as caught:
            dataprof.profile(Producer())
        if error_text is not None:
            assert str(caught.value.__cause__) == error_text.decode()
        else:
            assert caught.value.__cause__ is None
    else:
        report = dataprof.profile(Producer(), max_rows=0 if stage == "cap" else None)
        assert report.rows == 0
    assert fetched == ([1] if stage in ("batch", "end") else [])
    assert released == [1]
    assert stream.release is None  # ownership moved out of the capsule
