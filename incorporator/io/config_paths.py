"""Path-resolution toolkit for CLI-loaded JSON configs.

Two public functions cover the INPUT vs OUTPUT resolution policy agreed
on for Docker/CLI deployments:

- :func:`resolve_config_paths` — rebase known INPUT fields (inflow,
  outflow, inc_file, inc_files, new_file) relative to the config file's
  directory.  Leave OUTPUT fields (export_params.file_path,
  archive_target) and URL fields (inc_url, new_url) at CWD-relative.
  Idempotent: absolute paths pass through unchanged.

- :func:`resolve_output_path` — resolve + auto-mkdir for output paths
  (heartbeat file, log directory, etc.) that must land in the writable
  runtime directory (CWD/WORKDIR), not alongside the config file.

These helpers are called ONLY by CLI/JSON loaders
(:func:`incorporator.cli.runners._load_pipeline_config` and
:func:`incorporator.tideweaver.config.build_watershed`).
The in-process Python API (``Incorporator.incorp`` / ``export`` /
``refresh``) stays CWD-relative and does NOT call these functions.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Field sets
# ---------------------------------------------------------------------------

# Top-level INPUT sidecar/source fields — resolved relative to config dir.
_TOP_LEVEL_INPUT_FIELDS: frozenset[str] = frozenset({"inflow", "outflow"})

# incorp_params / refresh_params INPUT sub-fields.
_INCORP_INPUT_FIELDS: frozenset[str] = frozenset({"inc_file", "inc_files"})
_REFRESH_INPUT_FIELDS: frozenset[str] = frozenset({"new_file"})

# Per-current top-level sidecar fields (watershed.json current entries).
_CURRENT_SIDECAR_FIELDS: frozenset[str] = frozenset({"inflow", "outflow"})


def resolve_config_paths(config: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Rebase known INPUT file fields in *config* so they are relative to *base_dir*.

    Produces a shallow-copy of *config* — the original dict is never mutated.
    Nested dicts (``incorp_params``, ``refresh_params``, per-current entries,
    ``stream_params`` entries) are also shallow-copied before mutation.

    Resolution rules
    ----------------
    * Relative string path → ``str((base_dir / p).resolve())``.
    * Absolute string path → unchanged (already fully qualified).
    * Non-string value (``None``, list, dict, …) → unchanged.
    * URL fields (``inc_url``, ``new_url``) → **never** touched.
    * OUTPUT fields (``export_params.file_path``, ``archive_target``) → **never** touched.

    The function is idempotent: calling it twice on the same dict with the
    same *base_dir* produces the same result because an already-absolute path
    is left unchanged on the second pass.

    INPUT fields handled
    --------------------
    * Top level: ``inflow``, ``outflow``
    * ``incorp_params``: ``inc_file``, ``inc_files`` (each entry in the list)
    * ``refresh_params``: ``new_file``
    * ``stream_params[]`` entries (fjord): same ``incorp_params`` + ``refresh_params`` rules
    * Watershed current entries (``currents``, ``head``, ``tail``, ``middle``,
      ``source``, ``sinks``): ``inflow``, ``outflow``, ``incorp_params``,
      ``refresh_params``

    Args:
        config: The fully env-expanded + token-resolved config dict.
        base_dir: The resolved directory of the JSON config file
            (typically ``config_path.parent.resolve()``).

    Returns:
        A new dict with INPUT paths rebased.  OUTPUT fields and URLs are
        untouched.
    """
    result = dict(config)

    # Top-level inflow / outflow.
    for field in _TOP_LEVEL_INPUT_FIELDS:
        if field in result:
            result[field] = _rebase(result[field], base_dir)

    # Top-level incorp_params.
    if isinstance(result.get("incorp_params"), dict):
        result["incorp_params"] = _rebase_incorp_params(result["incorp_params"], base_dir)

    # Top-level refresh_params.
    if isinstance(result.get("refresh_params"), dict):
        result["refresh_params"] = _rebase_refresh_params(result["refresh_params"], base_dir)

    # stream_params[] — fjord-style list of per-source entries.
    if isinstance(result.get("stream_params"), list):
        result["stream_params"] = [
            _rebase_stream_entry(entry, base_dir) if isinstance(entry, dict) else entry
            for entry in result["stream_params"]
        ]

    # Watershed current container keys (diamond / chain / fanout / parallel / custom).
    for single_key in ("head", "tail", "source"):
        if isinstance(result.get(single_key), dict):
            result[single_key] = _rebase_current_entry(result[single_key], base_dir)

    for list_key in ("middle", "sinks", "currents"):
        if isinstance(result.get(list_key), list):
            result[list_key] = [
                _rebase_current_entry(entry, base_dir) if isinstance(entry, dict) else entry
                for entry in result[list_key]
            ]

    return result


def resolve_output_path(p: str | Path) -> Path:
    """Resolve an OUTPUT path and ensure its parent directory exists.

    OUTPUT paths (heartbeat file, disk-log directory, etc.) are CWD/WORKDIR-
    relative — they must NOT be rebased to the config file's directory.
    This helper consolidates the ``Path.resolve() + mkdir(parents=True,
    exist_ok=True)`` pattern used at every output-write site.

    Args:
        p: A relative or absolute path string or :class:`~pathlib.Path`.

    Returns:
        The resolved (absolute) :class:`~pathlib.Path`.  The parent
        directory is created if it does not exist.
    """
    resolved = Path(p).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _rebase(value: Any, base_dir: Path) -> Any:
    """Rebase a single value if it is a relative string path; otherwise return as-is."""
    if not isinstance(value, str) or not value:
        return value
    p = Path(value)
    if p.is_absolute():
        return value
    return str((base_dir / p).resolve())


def _rebase_list(values: Any, base_dir: Path) -> Any:
    """Rebase each string entry in a list; non-list values pass through."""
    if not isinstance(values, list):
        return values
    return [_rebase(v, base_dir) if isinstance(v, str) else v for v in values]


def _rebase_incorp_params(params: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Shallow-copy *params* and rebase inc_file / inc_files fields."""
    result = dict(params)
    if "inc_file" in result:
        result["inc_file"] = _rebase(result["inc_file"], base_dir)
    if "inc_files" in result:
        result["inc_files"] = _rebase_list(result["inc_files"], base_dir)
    return result


def _rebase_refresh_params(params: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Shallow-copy *params* and rebase new_file field."""
    result = dict(params)
    if "new_file" in result:
        result["new_file"] = _rebase(result["new_file"], base_dir)
    return result


def _rebase_stream_entry(entry: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Rebase the INPUT fields of one fjord stream_params[] entry."""
    result = dict(entry)
    if isinstance(result.get("incorp_params"), dict):
        result["incorp_params"] = _rebase_incorp_params(result["incorp_params"], base_dir)
    if isinstance(result.get("refresh_params"), dict):
        result["refresh_params"] = _rebase_refresh_params(result["refresh_params"], base_dir)
    return result


def _rebase_current_entry(entry: dict[str, Any], base_dir: Path) -> dict[str, Any]:
    """Rebase the INPUT fields of one watershed current entry."""
    result = dict(entry)
    for field in _CURRENT_SIDECAR_FIELDS:
        if field in result:
            result[field] = _rebase(result[field], base_dir)
    if isinstance(result.get("incorp_params"), dict):
        result["incorp_params"] = _rebase_incorp_params(result["incorp_params"], base_dir)
    if isinstance(result.get("refresh_params"), dict):
        result["refresh_params"] = _rebase_refresh_params(result["refresh_params"], base_dir)
    return result
