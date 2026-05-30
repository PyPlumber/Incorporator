"""incorporator deps — list optional dependencies and install status."""

from __future__ import annotations

import importlib.metadata
import json
import sys
from typing import Any

from incorporator._deps import Category, DepInfo, install_hint, list_deps
from incorporator._deps.typer import TYPER

# Module-level alias (typer may be None if not installed)
_typer = TYPER


def _get_version(name: str) -> str | None:
    """Return installed version of a package or None if not installed.

    Args:
        name: PyPI package name.

    Returns:
        Version string such as ``"3.9.1"``, or ``None`` when not installed.
    """
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _status_text(dep: DepInfo, version: str | None) -> tuple[str, str]:
    """Return (display_text, color) for the dep's status column.

    Args:
        dep: The dependency record to evaluate.
        version: Installed version string, or ``None``.

    Returns:
        A two-tuple of ``(display_text, typer_color_constant)``.
    """
    if not dep.is_available:
        if dep.platform_marker is not None:
            return ("n/a (platform)", _typer.colors.YELLOW)
        return ("✗ not installed", _typer.colors.RED)
    return (f"✓ {version or 'available'}", _typer.colors.GREEN)


def _apply_filters(deps: list[DepInfo], *, missing: bool, category: str | None) -> list[DepInfo]:
    """Filter deps by missing flag + category. Exits on unknown category.

    Args:
        deps: Full list of dependency records.
        missing: When ``True``, keep only deps that are not installed.
        category: Category value string to filter on, or ``None`` for all.

    Returns:
        Filtered list of :class:`~incorporator._deps.DepInfo` records.
    """
    if category is not None:
        valid = {c.value for c in Category}
        if category.lower() not in valid:
            _typer.secho(
                f"Unknown category '{category}'. Valid: {sorted(valid)}",
                fg=_typer.colors.RED,
                err=True,
            )
            sys.exit(1)
    result = []
    for dep in deps:
        if missing and dep.is_available:
            continue
        if category is not None and dep.category.value.lower() != category.lower():
            continue
        result.append(dep)
    return result


def _render_json(deps: list[DepInfo]) -> None:
    """Emit JSON array of dep dicts (excluding module field).

    Args:
        deps: Filtered list of dependency records to serialise.
    """
    payload: list[dict[str, Any]] = []
    for dep in deps:
        payload.append(
            {
                "name": dep.name,
                "extra": dep.extra,
                "category": dep.category.value,
                "description": dep.description,
                "version_spec": dep.version_spec,
                "is_available": dep.is_available,
                "installed_version": _get_version(dep.name),
                "platform_marker": dep.platform_marker,
                "include_in_all": dep.include_in_all,
            }
        )
    _typer.echo(json.dumps(payload, indent=2))


def _render_table(deps: list[DepInfo]) -> None:
    """Hand-format a tabular view with colored status + install hints.

    Args:
        deps: Filtered list of dependency records to display.
    """
    if not deps:
        _typer.echo("No deps match the given filters.")
        return

    # Compute column widths
    headers = ["NAME", "CATEGORY", "EXTRA", "STATUS", "INSTALL"]
    rows: list[tuple[str, str, str, str, str, str]] = []  # +color for status
    for dep in deps:
        version = _get_version(dep.name)
        status_text, status_color = _status_text(dep, version)
        rows.append(
            (
                dep.name,
                dep.category.value,
                dep.extra,
                status_text,
                install_hint(dep.name),
                status_color,
            )
        )

    widths = [
        max(len(headers[0]), max(len(r[0]) for r in rows)),
        max(len(headers[1]), max(len(r[1]) for r in rows)),
        max(len(headers[2]), max(len(r[2]) for r in rows)),
        max(len(headers[3]), max(len(r[3]) for r in rows)),
        max(len(headers[4]), max(len(r[4]) for r in rows)),
    ]

    # Header row
    header_line = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    _typer.echo(header_line)
    _typer.echo("  ".join("-" * w for w in widths))

    # Data rows
    for name, cat, extra, status, hint, color in rows:
        # Print prefix + status + hint separately so only the status column gets color
        prefix = f"{name.ljust(widths[0])}  {cat.ljust(widths[1])}  {extra.ljust(widths[2])}  "
        _typer.secho(prefix, nl=False)
        _typer.secho(status.ljust(widths[3]), fg=color, nl=False)
        _typer.echo(f"  {hint.ljust(widths[4])}")


def render_deps(*, missing: bool, category: str | None, as_json: bool) -> None:
    """Main entry point for the ``incorporator deps`` CLI command.

    Args:
        missing: When ``True``, show only deps that are not installed.
        category: Filter output to a single category value (e.g. ``"speedup"``),
            or ``None`` to show all categories.
        as_json: When ``True``, emit a JSON array suitable for scripting instead
            of the human-readable table.

    Raises:
        RuntimeError: When typer is not installed (the CLI cannot function).
    """
    if _typer is None:
        raise RuntimeError("typer not installed. Run: pip install incorporator[orchestrate]")
    deps = list_deps()
    filtered = _apply_filters(deps, missing=missing, category=category)
    if as_json:
        _render_json(filtered)
    else:
        _render_table(filtered)
