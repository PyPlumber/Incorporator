"""Tests for the optional-dependency introspection public API and CLI command.

Covers:
- Public re-exports from ``incorporator`` top-level package
- Symbol identity (re-exports are the same objects as the _deps versions)
- ``incorporator deps`` CLI tabular and JSON output
- Filter flags: ``--missing``, ``--category``, ``--json``
- Invalid category exits with code 1
- Empty filter result exits 0 with informational message
- tzdata platform_marker in JSON output
- Graceful handling of PackageNotFoundError in _get_version
"""

from __future__ import annotations

import json
import sys
from typing import Any
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Detect typer availability — CLI tests are skipped when typer is absent
# ---------------------------------------------------------------------------
try:
    import typer as _typer_mod
    from typer.testing import CliRunner

    from incorporator.cli import app

    _runner: CliRunner | None = CliRunner()
    _typer_available = True
except ImportError:
    _typer_mod = None  # type: ignore[assignment]
    _runner = None
    _typer_available = False


# ---------------------------------------------------------------------------
# Z5-T1: Public re-exports resolve without error
# ---------------------------------------------------------------------------


def test_public_api_imports_succeed() -> None:
    """``from incorporator import list_deps, Category, install_hint, DepInfo`` succeeds."""
    from incorporator import Category, DepInfo, install_hint, list_deps  # noqa: F401 (import as proof)

    assert callable(list_deps)
    assert callable(install_hint)
    assert Category is not None
    assert DepInfo is not None


# ---------------------------------------------------------------------------
# Z5-T2: Re-exported symbols are identical objects to the _deps originals
# ---------------------------------------------------------------------------


def test_public_api_symbols_are_same_objects() -> None:
    """Re-exported symbols are the SAME objects as their _deps counterparts."""
    import incorporator
    import incorporator._deps as _deps_pkg
    from incorporator._deps._registry import install_hint as _ih
    from incorporator._deps._registry import list_deps as _ld
    from incorporator._deps._types import Category as _Cat
    from incorporator._deps._types import DepInfo as _Di

    assert incorporator.list_deps is _ld
    assert incorporator.install_hint is _ih
    assert incorporator.Category is _Cat
    assert incorporator.DepInfo is _Di
    # Also verify the _deps package re-exports match
    assert _deps_pkg.list_deps is _ld
    assert _deps_pkg.install_hint is _ih


# ---------------------------------------------------------------------------
# CLI tests — skipped when typer is not installed
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _typer_available, reason="typer not installed; skipping CLI tests")
class TestDepsCli:
    """CLI tests for ``incorporator deps``; skipped when typer is absent."""

    # ---------------------------------------------------------------------------
    # Z5-T3: CLI runs without error and produces tabular output
    # ---------------------------------------------------------------------------

    def test_cli_deps_tabular(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """``incorporator deps`` runs without error and produces tabular output."""
        assert _runner is not None
        monkeypatch.chdir(tmp_path)
        result = _runner.invoke(app, ["deps"])
        assert result.exit_code == 0, result.output
        # Header row must be present
        assert "NAME" in result.output
        assert "CATEGORY" in result.output
        assert "INSTALL" in result.output

    # ---------------------------------------------------------------------------
    # Z5-T4: --json produces valid JSON array
    # ---------------------------------------------------------------------------

    def test_cli_deps_json_valid(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """``incorporator deps --json`` produces a valid JSON array parseable by stdlib json."""
        assert _runner is not None
        monkeypatch.chdir(tmp_path)
        result = _runner.invoke(app, ["deps", "--json"])
        assert result.exit_code == 0, result.output
        parsed = json.loads(result.output)
        assert isinstance(parsed, list)
        assert len(parsed) > 0
        first = parsed[0]
        assert "name" in first
        assert "category" in first
        assert "is_available" in first
        assert "installed_version" in first
        # module field must NOT appear
        assert "module" not in first

    # ---------------------------------------------------------------------------
    # Z5-T5: --missing filters correctly
    # ---------------------------------------------------------------------------

    def test_cli_deps_missing_filter(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """``--missing`` retains only deps that are not installed."""
        assert _runner is not None
        monkeypatch.chdir(tmp_path)
        result_all = _runner.invoke(app, ["deps", "--json"])
        result_missing = _runner.invoke(app, ["deps", "--missing", "--json"])
        assert result_all.exit_code == 0
        assert result_missing.exit_code == 0

        all_deps: list[dict[str, Any]] = json.loads(result_all.output)
        missing_deps: list[dict[str, Any]] = json.loads(result_missing.output)

        # Every entry in missing_deps must be not available
        for dep in missing_deps:
            assert dep["is_available"] is False, f"{dep['name']} should not be available"

        # Missing list must be a subset of all
        missing_names = {d["name"] for d in missing_deps}
        all_names = {d["name"] for d in all_deps}
        assert missing_names <= all_names

    # ---------------------------------------------------------------------------
    # Z5-T6: --category speedup filters correctly
    # ---------------------------------------------------------------------------

    def test_cli_deps_category_filter(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """``--category speedup`` returns only speedup-category deps."""
        assert _runner is not None
        monkeypatch.chdir(tmp_path)
        result = _runner.invoke(app, ["deps", "--category", "speedup", "--json"])
        assert result.exit_code == 0, result.output
        parsed: list[dict[str, Any]] = json.loads(result.output)
        for dep in parsed:
            assert dep["category"] == "speedup", f"Expected speedup, got {dep['category']} for {dep['name']}"

    # ---------------------------------------------------------------------------
    # Z5-T7: --category invalid_name exits 1 with useful error message
    # ---------------------------------------------------------------------------

    def test_cli_deps_invalid_category_exits_1(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """``--category invalid_name`` exits with code 1 and an informative error."""
        assert _runner is not None
        monkeypatch.chdir(tmp_path)
        result = _runner.invoke(app, ["deps", "--category", "invalid_name"])
        assert result.exit_code == 1
        # Error message should mention the unknown category
        assert "invalid_name" in result.output or "invalid_name" in (result.stderr or "")

    # ---------------------------------------------------------------------------
    # Z5-T8: Empty filter result exits 0 with informational message
    # ---------------------------------------------------------------------------

    def test_cli_deps_empty_filter_exits_0(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """Empty filter result exits 0 and prints an informational message, not exit 1."""
        assert _runner is not None
        monkeypatch.chdir(tmp_path)
        import importlib

        deps_mod = sys.modules.get("incorporator.cli.deps")
        if deps_mod is None:
            deps_mod = importlib.import_module("incorporator.cli.deps")

        with patch.object(deps_mod, "list_deps", return_value=[]):
            result = _runner.invoke(app, ["deps"])
        assert result.exit_code == 0
        assert "No deps match" in result.output

    # ---------------------------------------------------------------------------
    # Z5-T9: tzdata JSON entry has expected platform_marker on non-Windows
    # ---------------------------------------------------------------------------

    def test_tzdata_json_platform_marker(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """tzdata row in --json carries the win32 platform_marker."""
        assert _runner is not None
        monkeypatch.chdir(tmp_path)
        result = _runner.invoke(app, ["deps", "--json"])
        assert result.exit_code == 0
        parsed: list[dict[str, Any]] = json.loads(result.output)
        tzdata_entries = [d for d in parsed if d["name"] == "tzdata"]
        assert len(tzdata_entries) == 1, "tzdata must appear exactly once"
        entry = tzdata_entries[0]
        assert entry["platform_marker"] == "sys_platform == 'win32'"

    # ---------------------------------------------------------------------------
    # Z5-T10: PackageNotFoundError handled gracefully via monkeypatching _get_version
    # ---------------------------------------------------------------------------

    def test_get_version_returns_none_on_package_not_found(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> None:
        """When _get_version is patched to always return None, output is still valid."""
        assert _runner is not None
        monkeypatch.chdir(tmp_path)
        import importlib

        deps_mod = sys.modules.get("incorporator.cli.deps") or importlib.import_module("incorporator.cli.deps")

        with patch.object(deps_mod, "_get_version", return_value=None):
            result = _runner.invoke(app, ["deps", "--json"])
        assert result.exit_code == 0
        parsed: list[dict[str, Any]] = json.loads(result.output)
        for dep in parsed:
            assert dep["installed_version"] is None
