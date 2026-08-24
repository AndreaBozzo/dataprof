"""The originating error must survive into Python as ``__cause__``.

Rust-side, ``DataProfilerError`` retains the error it was built from (see the
``ErrorSource`` type in ``dataprof-core``). These tests assert the Python half
of that contract: a caller reading a traceback sees the decoder or OS error
that actually failed, not only dataprof's summary of it.

The distinction the suite has to hold onto is the project's usual one: absent
means "no cause was recorded", and it must stay different from a cause that is
recorded but empty.
"""

from __future__ import annotations

import dataprof
import pytest


class TestErrorCauseChain:
    def test_corrupt_parquet_surfaces_the_reader_error_as_cause(self, tmp_path):
        # The parquet reader's own diagnostic ("Corrupt footer") is the detail a
        # user needs and the part that used to be flattened into a string.
        bogus = tmp_path / "broken.parquet"
        bogus.write_bytes(b"NOTPARQUET" * 16)

        with pytest.raises(Exception) as excinfo:
            dataprof.profile(str(bogus))

        cause = excinfo.value.__cause__
        assert cause is not None, "the parquet reader error must be retained as __cause__"
        assert "Parquet" in str(cause)

    def test_parquet_bytes_path_agrees_with_the_file_path(self, tmp_path):
        # Output parity: the two input paths must not disagree about whether a
        # cause was recorded.
        payload = b"NOTPARQUET" * 16
        bogus = tmp_path / "broken.parquet"
        bogus.write_bytes(payload)

        with pytest.raises(Exception) as from_file:
            dataprof.profile(str(bogus))
        with pytest.raises(Exception) as from_bytes:
            dataprof.profile(payload, format="parquet")

        assert (from_file.value.__cause__ is None) == (from_bytes.value.__cause__ is None)
        assert str(from_file.value.__cause__) == str(from_bytes.value.__cause__)

    def test_missing_file_records_no_cause(self, tmp_path):
        # Nothing failed underneath here: dataprof checked for the file and it
        # was not there. A fabricated __cause__ would be noise.
        with pytest.raises(FileNotFoundError) as excinfo:
            dataprof.profile(str(tmp_path / "absent.csv"))
        assert excinfo.value.__cause__ is None

    def test_cause_is_not_the_exception_itself(self, tmp_path):
        # Guards against wiring __cause__ to the wrapper, which would render as
        # a plausible-looking but circular chain.
        bogus = tmp_path / "broken.parquet"
        bogus.write_bytes(b"NOTPARQUET" * 16)

        with pytest.raises(Exception) as excinfo:
            dataprof.profile(str(bogus))

        err = excinfo.value
        # Assert the cause exists first: without this the checks below pass
        # trivially on None and the test guards nothing.
        assert err.__cause__ is not None
        assert err.__cause__ is not err
        assert str(err.__cause__) != str(err)
