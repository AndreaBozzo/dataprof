"""UTF-8 BOM parity for JSON and JSONL transports (#497)."""

from __future__ import annotations

import asyncio
import codecs

import dataprof
import pytest
from dataprof.asyncio import profile_bytes, profile_file

PAYLOADS = {
    "json": b'[{"id":1,"label":"alpha"},{"id":2,"label":"beta"}]',
    "jsonl": b'{"id":1,"label":"alpha"}\n{"id":2,"label":"beta"}\n',
}


def _signature(report) -> tuple:
    return (
        report.rows,
        report.columns,
        tuple(
            (
                name,
                report[name].data_type,
                report[name].total_count,
                report[name].null_count,
            )
            for name in report.column_profiles
        ),
    )


def _run(awaitable):
    return asyncio.run(awaitable)


@pytest.mark.parametrize("fmt", ["json", "jsonl"])
def test_sync_bytes_accept_one_leading_utf8_bom(fmt):
    plain = dataprof.profile(PAYLOADS[fmt], format=fmt)
    prefixed = dataprof.profile(codecs.BOM_UTF8 + PAYLOADS[fmt], format=fmt)

    assert _signature(prefixed) == _signature(plain)


@pytest.mark.parametrize("fmt", ["json", "jsonl"])
def test_sync_file_accepts_one_leading_utf8_bom(tmp_path, fmt):
    plain_path = tmp_path / f"plain.{fmt}"
    bom_path = tmp_path / f"bom.{fmt}"
    plain_path.write_bytes(PAYLOADS[fmt])
    bom_path.write_bytes(codecs.BOM_UTF8 + PAYLOADS[fmt])

    plain = dataprof.profile(plain_path, format=fmt)
    prefixed = dataprof.profile(bom_path, format=fmt)

    assert _signature(prefixed) == _signature(plain)


@pytest.mark.parametrize("fmt", ["json", "jsonl"])
def test_async_bytes_and_file_accept_bom_and_count_source_bytes(tmp_path, fmt):
    data = codecs.BOM_UTF8 + PAYLOADS[fmt]
    path = tmp_path / f"bom.{fmt}"
    path.write_bytes(data)

    bytes_report = _run(profile_bytes(data, format=fmt))
    file_report = _run(profile_file(path, format=fmt))

    assert _signature(bytes_report) == _signature(file_report)
    assert bytes_report.to_dict()["execution"]["bytes_consumed"] == len(data)
    assert file_report.to_dict()["execution"]["bytes_consumed"] == len(data)


@pytest.mark.parametrize("fmt", ["json", "jsonl"])
def test_nonleading_and_second_bom_remain_malformed(fmt):
    payload = PAYLOADS[fmt]
    for data in (b" " + codecs.BOM_UTF8 + payload, codecs.BOM_UTF8 * 2 + payload):
        with pytest.raises(ValueError, match="malformed"):
            dataprof.profile(data, format=fmt, jsonl_on_error="strict")


@pytest.mark.parametrize("fmt", ["json", "jsonl"])
def test_bom_only_matches_empty_input_policy(fmt):
    for data in (b"", codecs.BOM_UTF8):
        try:
            report = dataprof.profile(data, format=fmt)
        except ValueError as error:
            outcome = ("error", str(error))
        else:
            outcome = ("report", _signature(report))

        if data == b"":
            empty_outcome = outcome
        else:
            assert outcome == empty_outcome
