"""Locale tags are a closed set, normalised then rejected (#545).

``locale=`` is strict: setting one suppresses every pattern belonging to a
different locale. Applied to a tag the catalogue does not know, that made a typo
produce a strictly worse report than passing nothing at all, silently.
``locale="it-IT"`` — the BCP 47 spelling a user is most likely to reach for, and
the one an agent passing a locale through from user text is most likely to
produce — returned zero patterns where ``locale="IT"`` returned a confident
match, with nothing in the report saying the tag was not understood.

The common spellings now normalise to the same locale, and anything left over
raises ``ValueError`` naming the supported set. These tests cover every surface a
tag can enter through: ``profile()``, ``ProfilerConfig``, the ``Profiler``
builder, and ``list_patterns()``.
"""

from __future__ import annotations

import io

import dataprof as dp
import pytest

SUPPORTED = ("CA", "DE", "FR", "GB", "IT", "US")

# Ten Italian CAP values. A five-digit string matches both ``CAP (IT)`` and
# ``ZIP Code (US)``, so the column shows what a locale did.
CAP_CSV = b"cap\n20121\n00184\n10121\n50123\n80133\n35100\n40121\n16121\n70121\n90133\n"

# Every spelling of Italy that has to mean the same thing.
IT_SPELLINGS = ("IT", "it", "It", " it ", "ITA", "ita", "it-IT", "it_IT")

# Tags that name no supported locale. "de-CH" is the interesting one: falling
# back to its language subtag would answer a Swiss request with Germany's
# patterns, which is a guess.
UNKNOWN_TAGS = ("XX", "ZZZZ", "de-CH", "de-CH-x-IT", "en", "italiano", "-", "0")


def _patterns(locale: str | None) -> list[tuple[str, float]]:
    report = dp.profile(io.BytesIO(CAP_CSV), format="csv", locale=locale)
    detected = report.column_profiles["cap"].patterns or []
    return sorted((p.name, round(p.match_percentage, 1)) for p in detected)


def test_every_spelling_of_a_locale_profiles_alike():
    expected = _patterns("IT")
    assert ("CAP (IT)", 100.0) in expected
    assert not any(name == "ZIP Code (US)" for name, _ in expected)

    for tag in IT_SPELLINGS:
        assert _patterns(tag) == expected, f"tag {tag!r} profiled differently from 'IT'"


def test_an_unknown_tag_raises_naming_the_supported_set():
    for tag in UNKNOWN_TAGS:
        with pytest.raises(ValueError) as excinfo:
            dp.profile(io.BytesIO(CAP_CSV), format="csv", locale=tag)

        message = str(excinfo.value)
        assert tag in message, f"the error should quote the rejected tag: {message}"
        for locale in SUPPORTED:
            assert locale in message, f"the error should name {locale}: {message}"


def test_an_unknown_tag_never_returns_an_emptier_report():
    """The failure this issue is about: a silent, plausible, emptier answer."""
    without_locale = _patterns(None)
    assert without_locale, "the fixture detects patterns with no locale set"

    for tag in UNKNOWN_TAGS:
        with pytest.raises(ValueError):
            dp.profile(io.BytesIO(CAP_CSV), format="csv", locale=tag)


def test_a_stray_separator_is_tolerated():
    """A trailing separator is a typo for the tag, not a different one."""
    assert dp.ProfilerConfig(locale="IT-").locale == "IT"
    assert dp.ProfilerConfig(locale="_it_").locale == "IT"


def test_a_blank_tag_means_no_locale():
    without_locale = _patterns(None)
    assert ("CAP (IT)", 100.0) in without_locale
    assert ("ZIP Code (US)", 100.0) in without_locale

    for tag in ("", "   "):
        assert _patterns(tag) == without_locale, f"tag {tag!r} should mean no locale"


def test_config_rejects_and_normalises_at_construction():
    with pytest.raises(ValueError, match="Unknown locale"):
        dp.ProfilerConfig(locale="it-IT-u-ca-gregory")

    assert dp.ProfilerConfig(locale="it-IT").locale == "IT"
    assert dp.ProfilerConfig(locale="ita").locale == "IT"
    assert dp.ProfilerConfig(locale="").locale is None
    assert dp.ProfilerConfig().locale is None


def test_builder_rejects_an_unknown_tag():
    with pytest.raises(ValueError, match="Unknown locale"):
        dp.Profiler().format("csv").locale("XX").profile(io.BytesIO(CAP_CSV))

    report = dp.Profiler().format("csv").locale("it-IT").profile(io.BytesIO(CAP_CSV))
    names = {p.name for p in report.column_profiles["cap"].patterns or []}
    assert "CAP (IT)" in names
    assert "ZIP Code (US)" not in names


def test_list_patterns_normalises_and_rejects():
    catalogue = dp.list_patterns()
    assert {p["locale"] for p in catalogue if p["locale"]} == set(SUPPORTED)

    reference = dp.list_patterns("IT")
    assert all(p["locale"] in {None, "IT"} for p in reference)
    for tag in IT_SPELLINGS:
        assert dp.list_patterns(tag) == reference, f"tag {tag!r} listed a different catalogue"

    assert dp.list_patterns("") == catalogue
    assert dp.list_patterns(None) == catalogue

    for tag in UNKNOWN_TAGS:
        with pytest.raises(ValueError, match="Unknown locale"):
            dp.list_patterns(tag)


def test_every_supported_locale_selects_its_own_patterns():
    for locale in SUPPORTED:
        specific = [p for p in dp.list_patterns(locale) if p["locale"] == locale]
        assert specific, f"{locale} is accepted but selects no patterns"
