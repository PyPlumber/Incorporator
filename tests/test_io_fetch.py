"""Unit tests for ``incorporator.io.fetch`` helpers."""

import json
from pathlib import Path
from typing import Any

import pytest

from incorporator import Incorporator
from incorporator.io.fetch import _normalize_source_list


# ----------------------------------------------------------------------
# _normalize_source_list — type handling
# ----------------------------------------------------------------------


def test_normalize_source_list_handles_str() -> None:
    assert _normalize_source_list("https://example.com/x", None) == ["https://example.com/x"]


def test_normalize_source_list_handles_pathlib_path(tmp_path: Path) -> None:
    """A ``pathlib.Path`` argument used to drop through to ``return []``.

    Before the fix, ``_normalize_source_list(Path("foo.ndjson"), None)`` saw
    ``isinstance(source, list) == False`` and ``isinstance(source, str) ==
    False``, falling through to the empty-list branch.  ``incorp()`` then
    silently returned an empty IncorporatorList with no diagnostic — the
    file was never opened.  The ``os.PathLike`` branch fixes this.
    """
    p = tmp_path / "x.ndjson"
    result = _normalize_source_list(p, None)
    assert result == [str(p)]
    assert isinstance(result[0], str)


def test_normalize_source_list_handles_list_of_paths(tmp_path: Path) -> None:
    """Mixed list of str + Path elements should all coerce to str."""
    paths = [tmp_path / "a.ndjson", str(tmp_path / "b.ndjson")]
    result = _normalize_source_list(paths, None)
    assert result == [str(tmp_path / "a.ndjson"), str(tmp_path / "b.ndjson")]


def test_normalize_source_list_handles_list_with_none() -> None:
    """``None`` entries inside the list are dropped."""
    result = _normalize_source_list(["a", None, "b"], None)
    assert result == ["a", "b"]


def test_normalize_source_list_payload_fallback() -> None:
    """No source but ``payload_list`` set → placeholder list matches its length."""
    result = _normalize_source_list(None, [{}, {}, {}])
    assert result == ["", "", ""]


def test_normalize_source_list_empty_when_unrecognised() -> None:
    """An unsupported type returns empty list (caller is responsible for the
    diagnostic via the ``source`` falsiness check in ``base.py``)."""
    assert _normalize_source_list(42, None) == []  # type: ignore[arg-type]
    assert _normalize_source_list(None, None) == []


# ----------------------------------------------------------------------
# End-to-end: incorp(inc_file=Path) round-trip
# ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_incorp_inc_file_accepts_pathlib_path(tmp_path: Path) -> None:
    """``incorp(inc_file=Path(...))`` reads the file (was silently empty pre-fix).

    Tutorial 2 (universal-formats) uses ``data_dir / "coins_log.ndjson"`` —
    a ``Path`` object — for the round-trip read.  Before the fix this read
    returned an empty IncorporatorList and the tutorial died with a
    ``KeyError`` on the first ``inc_dict["bitcoin"]`` lookup.
    """

    class _Coin(Incorporator):
        inc_code: Any = None
        symbol: str = ""
        name: str = ""

    src = tmp_path / "coins.ndjson"
    src.write_text(
        json.dumps({"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"}) + "\n"
        + json.dumps({"id": "ethereum", "symbol": "eth", "name": "Ethereum"}) + "\n",
        encoding="utf-8",
    )

    # Pass the Path directly — no manual str() wrap.
    coins = await _Coin.incorp(inc_file=src, inc_code="id")
    assert len(coins) == 2
    assert "bitcoin" in coins.inc_dict
    assert coins.inc_dict["bitcoin"].name == "Bitcoin"
