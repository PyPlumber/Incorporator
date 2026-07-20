#!/usr/bin/env python3
"""Sync [project.optional-dependencies] in pyproject.toml from _deps META objects.

Usage::

    python tools/sync_pyproject.py          # regenerate in-place
    python tools/sync_pyproject.py --check  # exit 1 if file would change
    python tools/sync_pyproject.py --diff   # print unified diff

The block between the AUTO-GENERATED markers is replaced wholesale on each
run.  Everything outside the markers is left exactly as-is (line-based, no
TOML parser needed).

Marker lines (must appear verbatim in pyproject.toml)::

    # >>> AUTO-GENERATED OPTIONAL DEPENDENCIES (sync via tools/sync_pyproject.py)
    # <<< END AUTO-GENERATED
"""

from __future__ import annotations

import argparse
import difflib
import importlib
import sys
from collections import defaultdict
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_PYPROJECT = _REPO_ROOT / "pyproject.toml"

_MARKER_START = "# >>> AUTO-GENERATED OPTIONAL DEPENDENCIES (sync via tools/sync_pyproject.py)"
_MARKER_END = "# <<< END AUTO-GENERATED"

_DEP_MODULE_NAMES: list[str] = [
    "orjson",
    "lxml",
    "cramjam",
    "fastavro",
    "pyarrow",
    "openpyxl",
    "typer",
    "prefect",
    "tzdata",
]

# Canonical extra ordering in pyproject.toml.
# Extras not listed here (or not in _deps) are appended after in declaration order.
_EXTRA_ORDER: list[str] = ["dev", "speedups", "avro", "xlsx", "parquet", "cli", "orchestrate", "docs", "all"]

# Hard-coded comments per extra (mirrors existing pyproject.toml style exactly)
_EXTRA_COMMENTS: dict[str, str] = {
    "dev": "# Development / CI tooling",
    "speedups": "# The GIL-releasing C/Rust upgrades",
    "avro": "# Big Data Formats",
    "xlsx": "# Business spreadsheets — pure Python, ~250 KB, fits the microclient identity",
    "parquet": (
        "# Columnar format for data lakes / warehouses. Pyarrow is ~30 MB — deliberately\n"
        '# NOT included in [all] to keep the "everything reasonable" install lean.\n'
        "# tzdata is required on Windows for pyarrow's ORC reader, which hardcodes\n"
        "# /usr/share/zoneinfo lookups regardless of platform. Linux/macOS ship the\n"
        "# IANA data natively, so the marker keeps the wheel weight off non-Windows\n"
        "# installs."
    ),
    "cli": "# The bare CLI entry point (typer only, no Prefect) — see [orchestrate] for the Prefect @flow wrapper",
    "orchestrate": "# The v2.0 Pipeline Orchestration Upgrades",
    "docs": "# Documentation generation (contributor-only — NOT added to [all])",
    "all": (
        '# The developer "install everything reasonable" flag.\n'
        "# NOTE: heavyweight deps (e.g. pyarrow for Parquet, ~50 MB) are intentionally\n"
        "# excluded — users opt into those explicitly via incorporator[parquet] etc."
    ),
}

# Extras whose content is fixed (not derived from _deps modules)
_FIXED_EXTRAS: dict[str, list[str]] = {
    "dev": [
        '"pytest>=7.0"',
        '"pytest-asyncio>=0.21.0"',
        '"pytest-cov>=4.0.0"',
        '"pytest-randomly>=3.15.0"',
        '"pytest-xdist>=3.5.0"',
        '"mypy>=1.0.0"',
        '"ruff>=0.1.0"',
        '"black>=24.0"',
    ],
    "docs": [
        '"pdoc>=14.0"',
    ],
    # Matches typer's version_spec in incorporator/_deps/typer.py's META verbatim.
    # Fixed here (rather than a new _deps module) because typer already has a
    # `_deps` entry keyed to `extra="orchestrate"` for [all] inclusion, and a single
    # DepInfo can't participate in two extras at once.
    "cli": [
        '"typer>=0.9.0"',
    ],
}


def _load_metas() -> list[object]:
    """Import each _deps module and collect its META object."""
    sys.path.insert(0, str(_REPO_ROOT))
    metas = []
    for name in _DEP_MODULE_NAMES:
        mod = importlib.import_module(f"incorporator._deps.{name}")
        metas.append(mod.META)
    return metas


def _generate_block(metas: list[object]) -> list[str]:
    """Produce TOML lines for the optional-dependencies block (no markers).

    Groups deps by extra in ``_EXTRA_ORDER`` order.  Fixed extras (dev, docs)
    use ``_FIXED_EXTRAS``.  The ``[all]`` extra is assembled from deps where
    ``include_in_all=True``.
    """
    # Collect dep entries per extra from _deps META
    by_extra: dict[str, list[tuple[str, str, str | None]]] = defaultdict(list)
    all_entries: list[tuple[str, str, str | None]] = []

    for meta in metas:
        name: str = meta.name  # type: ignore[attr-defined]
        extra: str = meta.extra  # type: ignore[attr-defined]
        version_spec: str = meta.version_spec  # type: ignore[attr-defined]
        platform_marker: str | None = meta.platform_marker  # type: ignore[attr-defined]
        include_in_all: bool = meta.include_in_all  # type: ignore[attr-defined]

        spec = f'"{name}{version_spec}"'
        by_extra[extra].append((spec, version_spec, platform_marker))
        if include_in_all:
            all_entries.append((spec, version_spec, None))

    lines: list[str] = []

    for extra in _EXTRA_ORDER:
        if extra == "all":
            continue  # handled last

        if extra in _FIXED_EXTRAS:
            # dev, docs — static content
            comment = _EXTRA_COMMENTS.get(extra, "")
            if comment:
                for comment_line in comment.splitlines():
                    lines.append(comment_line + "\n")
            lines.append(f"{extra} =[\n")
            fixed = _FIXED_EXTRAS[extra]
            for idx, spec in enumerate(fixed):
                comma = "," if idx < len(fixed) - 1 else ""
                lines.append(f"    {spec}{comma}\n")
            lines.append("]\n")
        elif extra in by_extra:
            entries = by_extra[extra]
            comment = _EXTRA_COMMENTS.get(extra, "")
            if comment:
                for comment_line in comment.splitlines():
                    lines.append(comment_line + "\n")
            lines.append(f"{extra} =[\n")
            for idx, (spec, _, platform_marker) in enumerate(entries):
                comma = "," if idx < len(entries) - 1 else ""
                if platform_marker:
                    # Strip the trailing quote from spec, add marker, re-close
                    bare = spec[:-1]  # remove trailing "
                    lines.append(f'    {bare}; {platform_marker}"{comma}\n')
                else:
                    lines.append(f"    {spec}{comma}\n")
            lines.append("]\n")

    # [all] — assembled from include_in_all=True deps
    comment = _EXTRA_COMMENTS.get("all", "")
    if comment:
        for comment_line in comment.splitlines():
            lines.append(comment_line + "\n")
    lines.append("all =[\n")
    for idx, (spec, _, _pm) in enumerate(all_entries):
        comma = "," if idx < len(all_entries) - 1 else ""
        lines.append(f"    {spec}{comma}\n")
    lines.append("]\n")

    return lines


def _splice(original_lines: list[str], new_block_lines: list[str]) -> list[str] | None:
    """Replace the content between the AUTO-GENERATED markers.

    Returns the new full file as a list of lines, or ``None`` if markers not
    found (caller should abort with an error).
    """
    start_idx: int | None = None
    end_idx: int | None = None

    for i, line in enumerate(original_lines):
        stripped = line.rstrip("\n").rstrip()
        if stripped == _MARKER_START:
            start_idx = i
        elif stripped == _MARKER_END:
            end_idx = i
            break

    if start_idx is None or end_idx is None:
        return None

    result = original_lines[: start_idx + 1] + new_block_lines + original_lines[end_idx:]
    return result


def main() -> None:
    """Entry point for ``python tools/sync_pyproject.py``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Exit 1 if pyproject.toml would change.")
    parser.add_argument("--diff", action="store_true", help="Print unified diff to stdout.")
    args = parser.parse_args()

    original_text = _PYPROJECT.read_text(encoding="utf-8")
    original_lines = original_text.splitlines(keepends=True)

    metas = _load_metas()
    new_block = _generate_block(metas)
    new_lines = _splice(original_lines, new_block)

    if new_lines is None:
        print(f"ERROR: AUTO-GENERATED markers not found in {_PYPROJECT}", file=sys.stderr)
        sys.exit(2)

    new_text = "".join(new_lines)

    if args.diff:
        diff = difflib.unified_diff(
            original_lines,
            new_lines,
            fromfile="pyproject.toml (current)",
            tofile="pyproject.toml (generated)",
        )
        sys.stdout.writelines(diff)
        if new_text != original_text:
            sys.exit(1)
        return

    if args.check:
        if new_text != original_text:
            print(
                "ERROR: pyproject.toml optional-dependencies are out of sync.\nRun: python tools/sync_pyproject.py",
                file=sys.stderr,
            )
            sys.exit(1)
        print("pyproject.toml optional-dependencies are in sync.")
        return

    _PYPROJECT.write_text(new_text, encoding="utf-8")
    print(f"Updated {_PYPROJECT}")


if __name__ == "__main__":
    main()
