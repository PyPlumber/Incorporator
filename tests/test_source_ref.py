"""Unit tests for :class:`incorporator.io.source_ref.SourceRef`.

Covers the named factories, auto-dispatch via :meth:`SourceRef.parse`,
the back-compat ``as_str()`` flattening, and frozen/immutable invariants.
Integration with ``_normalize_source_list`` is exercised by the
existing ``tests/test_io_fetch.py`` suite — those tests stay green
under the SourceRef-backed implementation.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from incorporator.io.source_ref import SourceKind, SourceRef


# ---------------------------------------------------------------------------
# Named factories
# ---------------------------------------------------------------------------


def test_from_url_round_trip() -> None:
    """``from_url`` produces ``kind="url"`` and preserves the string."""
    ref = SourceRef.from_url("https://api.example.com/users")
    assert ref.kind == "url"
    assert ref.value == "https://api.example.com/users"


def test_from_file_preserves_string() -> None:
    """``from_file`` stores a string path verbatim (no Path coercion).

    Callers' literal text is preserved so the public ``inc_file``
    kwarg carries the same string the user passed in.  See the
    Windows-path-fidelity rationale in the docstring.
    """
    ref = SourceRef.from_file("./data.json")
    assert ref.kind == "file"
    assert ref.value == "./data.json"
    assert isinstance(ref.value, str)


def test_from_file_accepts_path_directly() -> None:
    """``from_file`` accepts a :class:`Path` instance unchanged in shape."""
    ref = SourceRef.from_file(Path("./data.json"))
    assert ref.kind == "file"
    assert isinstance(ref.value, Path)


def test_from_parent_wraps_arbitrary_object() -> None:
    """``from_parent`` accepts any object without type-checking."""
    sentinel = object()
    ref = SourceRef.from_parent(sentinel)
    assert ref.kind == "parent"
    assert ref.value is sentinel


def test_from_payload_defensively_copies() -> None:
    """``from_payload`` copies the list so caller mutations don't reach the frozen ref."""
    original = [{"id": 1}, {"id": 2}]
    ref = SourceRef.from_payload(original)
    assert ref.kind == "payload"
    assert ref.value == original
    original.append({"id": 99})
    assert ref.value == [{"id": 1}, {"id": 2}]  # ref's copy is untouched


def test_from_kwargs_defensively_copies() -> None:
    """``from_kwargs`` copies the mapping into a fresh dict."""
    original = {"inc_url": "https://x", "verb": "fjord"}
    ref = SourceRef.from_kwargs(original)
    assert ref.kind == "kwargs"
    assert ref.value == original
    original["mutated"] = True
    assert "mutated" not in ref.value


# ---------------------------------------------------------------------------
# Auto-dispatch via SourceRef.parse()
# ---------------------------------------------------------------------------


def test_parse_http_url() -> None:
    """``http://`` URL classifies as ``url``."""
    assert SourceRef.parse("http://api.example.com/x").kind == "url"


def test_parse_https_url() -> None:
    """``https://`` URL classifies as ``url``."""
    assert SourceRef.parse("https://api.example.com/x").kind == "url"


def test_parse_relative_path_string() -> None:
    """A non-URL string is classified as a file path; literal preserved."""
    ref = SourceRef.parse("./data.json")
    assert ref.kind == "file"
    assert ref.value == "./data.json"
    assert isinstance(ref.value, str)


def test_parse_pathlike_keeps_path_instance() -> None:
    """A :class:`Path` input keeps its Path type in ``ref.value``."""
    ref = SourceRef.parse(Path("./data.json"))
    assert ref.kind == "file"
    assert isinstance(ref.value, Path)


def test_parse_pathlike() -> None:
    """A :class:`Path` instance classifies as a file."""
    assert SourceRef.parse(Path("./x.json")).kind == "file"


def test_parse_mapping_input() -> None:
    """A mapping classifies as ``kwargs`` (architect's escape hatch)."""
    ref = SourceRef.parse({"inc_url": "https://x"})
    assert ref.kind == "kwargs"
    assert ref.value == {"inc_url": "https://x"}


def test_parse_rejects_list() -> None:
    """Lists can't be auto-classified — caller uses an explicit factory."""
    with pytest.raises(ValueError, match="cannot auto-detect"):
        SourceRef.parse([{"id": 1}])


def test_parse_rejects_arbitrary_object() -> None:
    """Generic objects (e.g. Incorporator instances) need ``from_parent`` explicitly."""

    class _Probe:
        pass

    with pytest.raises(ValueError, match="cannot auto-detect"):
        SourceRef.parse(_Probe())


# ---------------------------------------------------------------------------
# as_str() flattening (back-compat with the List[str] contract)
# ---------------------------------------------------------------------------


def test_as_str_url() -> None:
    """``as_str`` returns the URL string for ``url`` kind."""
    assert SourceRef.from_url("https://x").as_str() == "https://x"


def test_as_str_file_returns_fspath() -> None:
    """``as_str`` returns the ``os.fspath`` form for ``file`` kind.

    String inputs round-trip verbatim (``os.fspath`` of a string is the
    string itself).  :class:`Path` inputs round-trip through the OS
    representation (which differs by platform — Windows uses
    backslashes).
    """
    assert SourceRef.from_file("./data.json").as_str() == "./data.json"


def test_as_str_parent_returns_empty() -> None:
    """``as_str`` returns ``""`` for ``parent`` kind (string-list bypass marker)."""
    assert SourceRef.from_parent(object()).as_str() == ""


def test_as_str_payload_returns_empty() -> None:
    """``as_str`` returns ``""`` for ``payload`` kind."""
    assert SourceRef.from_payload([{"id": 1}]).as_str() == ""


def test_as_str_kwargs_returns_empty() -> None:
    """``as_str`` returns ``""`` for ``kwargs`` kind."""
    assert SourceRef.from_kwargs({"inc_url": "x"}).as_str() == ""


# ---------------------------------------------------------------------------
# Frozen invariants
# ---------------------------------------------------------------------------


def test_source_ref_is_frozen() -> None:
    """Setting ``kind`` or ``value`` on a SourceRef raises (frozen=True)."""
    ref = SourceRef.from_url("https://x")
    with pytest.raises(Exception):  # FrozenInstanceError; broad catch for stability
        ref.kind = "file"  # type: ignore[misc]


def test_source_ref_equality() -> None:
    """Two SourceRefs with the same (kind, value) compare equal."""
    a = SourceRef.from_url("https://x")
    b = SourceRef.from_url("https://x")
    assert a == b


def test_source_ref_inequality_across_kinds() -> None:
    """Same string value but different kinds compare unequal."""
    a = SourceRef.from_url("file.json")  # forced URL kind via factory
    b = SourceRef.from_file("file.json")  # forced file kind
    assert a != b


# ---------------------------------------------------------------------------
# SourceKind Literal alias
# ---------------------------------------------------------------------------


def test_source_kind_literal_values() -> None:
    """Verify the Literal alias accepts each of the five expected kinds."""
    # SourceKind is a typing.Literal — we can verify by constructing SourceRefs
    # with each kind and confirming no validation errors.
    for kind, factory_value in [
        ("url", "https://x"),
        ("file", "./x"),
        ("parent", object()),
        ("payload", [{}]),
        ("kwargs", {"k": "v"}),
    ]:
        # Construct directly to test the literal range; factories also verified above.
        ref = SourceRef(kind=kind, value=factory_value)  # type: ignore[arg-type]
        assert ref.kind == kind
    # mypy-time check via assignment — SourceKind isn't introspectable at runtime.
    _: SourceKind = "url"  # noqa: F841
