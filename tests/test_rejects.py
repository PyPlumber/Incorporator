"""Unit tests for :class:`incorporator.RejectEntry` and
:class:`IncorporatorList`'s structured rejects list.

Covers entry construction, frozen invariants, ``__str__`` shape, the
back-compat ``failed_sources`` property, the mutually-exclusive
constructor kwargs, and the auto-wrap path that maps legacy
``List[str]`` into a list of minimal entries.
"""

from __future__ import annotations

from typing import Any, Type

import pytest
from pydantic import ValidationError

from incorporator import IncorporatorList, RejectEntry


# ---------------------------------------------------------------------------
# RejectEntry construction
# ---------------------------------------------------------------------------


def test_entry_minimum_construction() -> None:
    """A bare ``RejectEntry(source=...)`` fills defaults."""
    entry = RejectEntry(source="https://x")
    assert entry.source == "https://x"
    assert entry.error_kind == "Unknown"
    assert entry.message == ""
    assert entry.retry_after is None
    assert entry.wave_index is None


def test_entry_full_construction() -> None:
    """All fields populated round-trip cleanly."""
    entry = RejectEntry(
        source="https://api.example.com/users",
        error_kind="HTTPStatusError",
        message="429 Too Many Requests",
        retry_after=60.0,
        wave_index=3,
    )
    assert entry.source == "https://api.example.com/users"
    assert entry.error_kind == "HTTPStatusError"
    assert entry.message == "429 Too Many Requests"
    assert entry.retry_after == 60.0
    assert entry.wave_index == 3


def test_entry_is_frozen() -> None:
    """Assigning to a field after construction raises."""
    entry = RejectEntry(source="x")
    with pytest.raises(ValidationError):
        entry.source = "mutated"  # type: ignore[misc]


def test_entry_requires_source() -> None:
    """``source`` is the one required field."""
    with pytest.raises(ValidationError, match="source"):
        RejectEntry()  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# __str__ back-compat string form
# ---------------------------------------------------------------------------


def test_str_with_error_kind_and_message() -> None:
    """``str(entry)`` renders ``"{error_kind}: {source} - {message}"`` when both are set and message differs from source."""
    entry = RejectEntry(source="x", error_kind="RequestError", message="connection refused")
    assert str(entry) == "RequestError: x — connection refused"


def test_str_with_error_kind_only() -> None:
    """``str(entry)`` renders ``"{error_kind}: {source}"`` when message is empty."""
    entry = RejectEntry(source="x", error_kind="RequestError")
    assert str(entry) == "RequestError: x"


def test_str_unknown_falls_back_to_source() -> None:
    """``str(entry)`` renders ``"Unknown: {source}"`` when no message is set."""
    entry = RejectEntry(source="https://x")
    assert str(entry) == "Unknown: https://x"


def test_str_unknown_with_message_falls_back_to_message() -> None:
    """``str(entry)`` renders ``"Unknown: {source} — {message}"`` when message is distinct from source."""
    entry = RejectEntry(source="x", message="some descriptive error")
    assert str(entry) == "Unknown: x — some descriptive error"


# ---------------------------------------------------------------------------
# IncorporatorList round-trip
# ---------------------------------------------------------------------------


class _Model:
    """Minimal stand-in for an Incorporator subclass — IncorporatorList's
    ``model_class`` is only used at constructor time for the typed-list
    discriminator; the actual fields don't matter for these tests."""

    pass


def test_list_with_structured_rejects_round_trip() -> None:
    """Constructing with ``rejects=[entry]`` exposes both views."""
    entry = RejectEntry(source="https://x", error_kind="HTTPStatusError")
    lst: IncorporatorList[Any] = IncorporatorList(_Model, [], rejects=[entry])
    assert lst.rejects == [entry]
    assert lst.failed_sources == ["https://x"]


def test_list_with_legacy_failed_sources_auto_wraps() -> None:
    """Legacy ``failed_sources=[...]`` auto-wraps each string in a minimal entry."""
    lst: IncorporatorList[Any] = IncorporatorList(_Model, [], failed_sources=["https://a", "https://b"])
    entries = lst.rejects
    assert len(entries) == 2
    assert entries[0].source == "https://a"
    assert entries[0].error_kind == "Unknown"
    assert entries[1].source == "https://b"
    # Legacy view still works.
    assert lst.failed_sources == ["https://a", "https://b"]


def test_list_default_has_empty_rejects() -> None:
    """Without either kwarg, ``rejects`` is empty and ``failed_sources`` is ``[]``."""
    lst: IncorporatorList[Any] = IncorporatorList(_Model, [])
    assert lst.rejects == []
    assert lst.failed_sources == []


def test_list_rejects_both_kwargs() -> None:
    """Passing both ``failed_sources`` and ``rejects`` raises."""
    entry = RejectEntry(source="x")
    with pytest.raises(ValueError, match="not both"):
        IncorporatorList(_Model, [], failed_sources=["y"], rejects=[entry])


def test_rejects_returns_defensive_copy() -> None:
    """Caller mutations on the returned list don't affect the framework's state."""
    entry = RejectEntry(source="x")
    lst: IncorporatorList[Any] = IncorporatorList(_Model, [], rejects=[entry])
    snapshot = lst.rejects
    snapshot.append(RejectEntry(source="mutated"))
    # Original list is unchanged.
    assert lst.rejects == [entry]


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------


def test_top_level_import() -> None:
    """``from incorporator import RejectEntry`` works."""
    import incorporator

    assert hasattr(incorporator, "RejectEntry")
    assert incorporator.RejectEntry is RejectEntry
    assert "RejectEntry" in incorporator.__all__


def test_failed_sources_is_property_not_attribute() -> None:
    """``failed_sources`` is a derived @property, not a writable instance attr."""
    lst: IncorporatorList[Any] = IncorporatorList(_Model, [], failed_sources=["x"])
    # Confirm the descriptor lookup goes via the property — assigning raises.
    with pytest.raises(AttributeError):
        lst.failed_sources = ["mutated"]  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Integration: structured entries from the fetch path
# ---------------------------------------------------------------------------


def test_fetch_reject_entry_carries_error_kind() -> None:
    """Verify :func:`incorporator.io.fetch._build_reject_entry` populates ``error_kind``."""
    from httpx import HTTPStatusError, Request, Response

    from incorporator.io.fetch import _build_reject_entry

    req = Request("GET", "https://api.example.com/users")
    resp = Response(429, headers={"Retry-After": "30"}, request=req)
    exc = HTTPStatusError("rate limited", request=req, response=resp)

    entry = _build_reject_entry("https://api.example.com/users", exc)
    assert entry.source == "https://api.example.com/users"
    assert entry.error_kind == "HTTPStatusError"
    assert entry.retry_after == 30.0


def test_fetch_reject_entry_no_retry_after() -> None:
    """No ``Retry-After`` header → ``entry.retry_after is None``."""
    from httpx import RequestError

    from incorporator.io.fetch import _build_reject_entry

    exc = RequestError("connection refused")
    entry = _build_reject_entry("https://x", exc)
    assert entry.error_kind == "RequestError"
    assert entry.retry_after is None


# ---------------------------------------------------------------------------
# New fields — back-compat and new-field coverage
# ---------------------------------------------------------------------------


def test_new_fields_default_to_none() -> None:
    """The seven new RejectEntry fields all default to None — back-compat for existing consumers."""
    entry = RejectEntry(source="https://x")
    assert entry.from_name is None
    assert entry.to_name is None
    assert entry.host is None
    assert entry.status_code is None
    assert entry.attempt_number is None
    assert entry.duration_sec is None
    assert entry.cooldown_sec is None


def test_retry_after_still_works() -> None:
    """retry_after is unchanged — back-compat for existing retry-loop consumers."""
    entry = RejectEntry(source="https://x", retry_after=45.0)
    assert entry.retry_after == 45.0


def test_cooldown_sec_coexists_with_retry_after() -> None:
    """cooldown_sec and retry_after are distinct fields that coexist — not a rename."""
    entry = RejectEntry(source="https://x", retry_after=30.0, cooldown_sec=30.0)
    assert entry.retry_after == 30.0
    assert entry.cooldown_sec == 30.0


def test_str_fully_decorated_with_all_optional_fields() -> None:
    """``str(entry)`` renders the fully decorated form: kind, source, edge, HTTP status with phrase, and message."""
    entry = RejectEntry(
        source="https://x",
        error_kind="HTTPStatusError",
        message="429 Too Many Requests",
        host="x.com",
        status_code=429,
        from_name="SourceA",
        to_name="DestB",
        cooldown_sec=15.0,
    )
    assert (
        str(entry) == "HTTPStatusError: https://x (SourceA->DestB) [HTTP 429 Too Many Requests] — 429 Too Many Requests"
    )


def test_build_reject_entry_populates_host_and_status_code() -> None:
    """fetch._build_reject_entry populates host and status_code alongside retry_after."""
    from httpx import HTTPStatusError, Request, Response

    from incorporator.io.fetch import _build_reject_entry

    req = Request("GET", "https://api.example.com/users")
    resp = Response(429, headers={"Retry-After": "30"}, request=req)
    exc = HTTPStatusError("rate limited", request=req, response=resp)

    entry = _build_reject_entry("https://api.example.com/users", exc)
    assert entry.host == "api.example.com"
    assert entry.status_code == 429
    assert entry.cooldown_sec == 30.0


def test_build_reject_entry_no_header_sets_cooldown_none() -> None:
    """No Retry-After header → cooldown_sec is also None alongside retry_after."""
    from httpx import RequestError

    from incorporator.io.fetch import _build_reject_entry

    exc = RequestError("connection refused")
    entry = _build_reject_entry("https://api.example.com/data", exc)
    assert entry.retry_after is None
    assert entry.cooldown_sec is None
    assert entry.host == "api.example.com"


# ---------------------------------------------------------------------------
# _format_reject_warning
# ---------------------------------------------------------------------------


def test_format_reject_warning_count_headline() -> None:
    """First line of ``_format_reject_warning`` is the count headline."""
    from incorporator.rejects import _format_reject_warning

    rejects = [RejectEntry(source=f"s{i}", error_kind="RequestError") for i in range(3)]
    msg = _format_reject_warning(rejects)
    lines = msg.splitlines()
    assert lines[0] == "3 source(s) returned partial data."


def test_format_reject_warning_entries_rendered() -> None:
    """Each of the first ``cap`` entries is rendered via ``str(r)``."""
    from incorporator.rejects import _format_reject_warning

    rejects = [RejectEntry(source=f"s{i}", error_kind="E") for i in range(3)]
    msg = _format_reject_warning(rejects)
    lines = msg.splitlines()
    assert lines[1] == str(rejects[0])
    assert lines[2] == str(rejects[1])
    assert lines[3] == str(rejects[2])


def test_format_reject_warning_overflow_line() -> None:
    """When rejects exceed cap, an overflow line '... and N more.' is appended."""
    from incorporator.rejects import _format_reject_warning

    rejects = [RejectEntry(source=f"s{i}", error_kind="E") for i in range(8)]
    msg = _format_reject_warning(rejects, cap=5)
    lines = msg.splitlines()
    # headline + 5 entries + overflow
    assert len(lines) == 7
    assert lines[-1] == "... and 3 more."


def test_format_reject_warning_no_overflow_when_at_cap() -> None:
    """Exactly ``cap`` entries produces no overflow line."""
    from incorporator.rejects import _format_reject_warning

    rejects = [RejectEntry(source=f"s{i}", error_kind="E") for i in range(5)]
    msg = _format_reject_warning(rejects, cap=5)
    lines = msg.splitlines()
    assert len(lines) == 6  # headline + 5 entries
    assert not lines[-1].startswith("...")


def test_str_renderer_is_cp1252_safe() -> None:
    """``str(entry)`` is encodable as cp1252 — no emoji or characters outside the Windows Western code page."""
    entry = RejectEntry(
        source="https://api.example.com/users",
        error_kind="HTTPStatusError",
        message="429 Too Many Requests",
        status_code=429,
        from_name="SourceA",
        to_name="DestB",
    )
    rendered = str(entry)
    # The phrase "Too Many Requests" is ASCII/cp1252-safe.
    assert "Too Many Requests" in rendered
    try:
        rendered.encode("cp1252")
    except UnicodeEncodeError as exc:
        raise AssertionError(f"str(RejectEntry) is not cp1252-safe: {exc}\nRendered: {rendered!r}") from exc


# ---------------------------------------------------------------------------
# Commit D' — reason_phrase enrichment
# ---------------------------------------------------------------------------


def test_str_known_phrase_included() -> None:
    """``str(entry)`` includes the httpx reason phrase for a known status code."""
    entry = RejectEntry(source="https://x", status_code=500)
    rendered = str(entry)
    assert "[HTTP 500 Internal Server Error]" in rendered


def test_str_unknown_code_graceful_fallback() -> None:
    """``str(entry)`` omits the phrase for an unknown code (e.g. Cloudflare 522)."""
    entry = RejectEntry(source="https://x", status_code=522)
    rendered = str(entry)
    # No phrase available; render code only.
    assert "[HTTP 522]" in rendered
    # Must not contain an empty trailing space before the bracket.
    assert "[HTTP 522 ]" not in rendered


# Suppress an unused-import lint when typing checkers narrow Type.
_ = Type
