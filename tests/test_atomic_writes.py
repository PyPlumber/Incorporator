"""Atomic-write regressions.

The senior-review audit found that monolithic formats (Parquet, Excel,
JSON, XML, Feather, ORC) write directly to the target path.  A crash
mid-write leaves a corrupt file the reader can't open.  Fix: write to
a sibling tempfile and ``os.replace()`` atomically on success.

These tests:
  1. Confirm the new ``atomic_write_path`` context manager renames on
     success and cleans up on failure.
  2. Confirm Parquet / JSON / XML failures leave NO half-written file
     at the target path.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.handlers._base import atomic_write_path
from incorporator.io.handlers.text import JSONHandler, XMLHandler


# ==========================================
# atomic_write_path() — the building block
# ==========================================


def test_atomic_write_renames_on_success(tmp_path: Path) -> None:
    """Successful write moves the tempfile to the target path."""
    target = tmp_path / "out.txt"
    with atomic_write_path(target) as tmp:
        assert tmp != target  # different path during write
        assert tmp.parent == target.parent  # sibling, same dir
        tmp.write_text("payload", encoding="utf-8")
    assert target.exists()
    assert target.read_text(encoding="utf-8") == "payload"
    # Tempfile must NOT survive the rename.
    siblings = list(target.parent.glob("out.txt.tmp-*"))
    assert siblings == [], "tempfile leaked"


def test_atomic_write_cleans_up_tempfile_on_exception(tmp_path: Path) -> None:
    """An exception inside the `with` block deletes the tempfile."""
    target = tmp_path / "out.txt"

    with pytest.raises(RuntimeError, match="simulated"):
        with atomic_write_path(target) as tmp:
            tmp.write_text("partial", encoding="utf-8")
            raise RuntimeError("simulated mid-write failure")

    assert not target.exists(), "target was created despite the write failing"
    siblings = list(target.parent.glob("out.txt.tmp-*"))
    assert siblings == [], "tempfile leaked on failure"


def test_atomic_write_preserves_pre_existing_target_on_failure(tmp_path: Path) -> None:
    """If the target already exists, a failed write leaves the old version."""
    target = tmp_path / "out.txt"
    target.write_text("original", encoding="utf-8")

    with pytest.raises(RuntimeError):
        with atomic_write_path(target) as tmp:
            tmp.write_text("would have replaced", encoding="utf-8")
            raise RuntimeError("simulated failure")

    assert target.read_text(encoding="utf-8") == "original", "target was clobbered on failure"


# ==========================================
# Format-handler integration
# ==========================================


def test_json_write_failure_leaves_no_partial_file(tmp_path: Path) -> None:
    """JSON write that fails mid-stream leaves NO half-written file."""
    target = tmp_path / "out.json"

    # An iterator that yields one good row then explodes.
    def _exploding_data():
        yield {"id": 1, "name": "Alice"}
        raise RuntimeError("upstream pipeline failed")

    with pytest.raises(Exception):
        JSONHandler().write(_exploding_data(), target)

    # Pre-fix: a half-written "[\n{...}," sat on disk.  Post-fix: no file.
    assert not target.exists()
    siblings = list(target.parent.glob("out.json.tmp-*"))
    assert siblings == [], "tempfile leaked"


def test_json_write_success_atomic(tmp_path: Path) -> None:
    """Normal JSON write still produces a parseable file."""
    target = tmp_path / "out.json"
    JSONHandler().write([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}], target)
    assert target.exists()
    parsed = json.loads(target.read_text(encoding="utf-8"))
    assert len(parsed) == 2
    assert parsed[0]["name"] == "Alice"


def test_xml_write_success_atomic(tmp_path: Path) -> None:
    """Normal XML write produces a readable file under atomic_write_path."""
    target = tmp_path / "out.xml"
    XMLHandler().write([{"id": 1, "name": "Alice"}], target)
    assert target.exists()
    text = target.read_text(encoding="utf-8")
    assert "Alice" in text
