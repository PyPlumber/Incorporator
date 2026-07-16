"""Integration tests: Ex / Nm / Pk tokens in pipeline.json + watershed.json.

Verifies that the wrapper directives flow through:
- resolve_tokens() — JSON-string -> Python wrapper instance
- Existing template regression — all in-repo templates still validate
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from incorporator.cli.envexpand import expand_env
from incorporator.cli.tokens import TokenResolutionError, resolve_tokens
from incorporator.cli.validate import (
    autodetect_type,
    validate_fjord_config,
    validate_stream_config,
    validate_watershed_config,
)
from incorporator.io.config_paths import resolve_config_paths
from incorporator.schema.directives import Ex, Nm, Pk, _normalize_etl_kwargs
from incorporator.tideweaver.config import load_watershed
from incorporator.usercode import merge_sidecar_extra_names

# ---------------------------------------------------------------------------
# resolve_tokens() — mixed bare + wrapped shapes
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).parent.parent


def test_excl_lst_mixed_bare_and_wrapped() -> None:
    """Bare string and Ex() token coexist in excl_lst without interfering."""
    out: dict[str, Any] = resolve_tokens({"excl_lst": ["foo", "Ex('nested.field')"]})
    assert out["excl_lst"][0] == "foo"
    assert isinstance(out["excl_lst"][1], Ex)
    assert out["excl_lst"][1].field == "nested.field"


def test_name_chg_mixed_bare_and_wrapped() -> None:
    """Bare list and Nm() token coexist in name_chg without interfering."""
    out: dict[str, Any] = resolve_tokens({"name_chg": [["a", "b"], "Nm('c', 'd')"]})
    assert out["name_chg"][0] == ["a", "b"]
    assert isinstance(out["name_chg"][1], Nm)
    assert out["name_chg"][1].old == "c"
    assert out["name_chg"][1].new == "d"


def test_pk_kwarg_form() -> None:
    """Pk with a keyword argument resolves to the correct Pk instance."""
    out: dict[str, Any] = resolve_tokens({"x": "Pk('id', target='code')"})
    assert isinstance(out["x"], Pk)
    assert out["x"].source == "id"
    assert out["x"].target == "code"


def test_pk_positional_form() -> None:
    """Pk with two positional arguments resolves to the correct Pk instance."""
    out: dict[str, Any] = resolve_tokens({"x": "Pk('league.name', 'name')"})
    assert isinstance(out["x"], Pk)
    assert out["x"].source == "league.name"
    assert out["x"].target == "name"


# ---------------------------------------------------------------------------
# Template regression — every in-repo config must still validate
# ---------------------------------------------------------------------------

# Stream and fjord configs: validate structure from raw JSON (env-var strings
# in headers / params are valid string content for Pydantic; no env-expand needed).
# Watershed configs: env-expand first so window timestamps parse as ISO 8601
# (the :-default form resolves cleanly without any env vars set).

# These lists are DISCOVERED, not hardcoded.  They used to be three literal
# lists, which made coverage opt-in: a config was only exercised if someone
# remembered to add it here.  That is the same hand-maintained-list failure
# mode that let `_run_fjord` drop `inflow` (fixed in 25627d9) survive ~40 days
# after the identical `_run_stream` bug was fixed in 24b65bd.  Globbing means
# every config a tutorial ships is covered the moment it lands.
#
# SCOPE — read before trusting this: these tests prove a config is STRUCTURALLY
# VALID.  They do NOT prove it produces correct output.  The `_run_fjord`
# `inflow` bug passed validation cleanly and still emitted zero rows.  A config
# passing here means "the CLI will accept it", not "the pipeline works".


def _discover_configs() -> list[str]:
    """Every CLI config under examples/, repo-root-relative and sorted.

    Excludes ``fixtures/`` — those are sample DATA the tutorials read, not
    configs, and would fail validation as though they were broken pipelines.
    """
    configs: list[str] = []
    for path in sorted((_REPO_ROOT / "examples").rglob("*.json")):
        rel = path.relative_to(_REPO_ROOT)
        if "fixtures" in rel.parts:
            continue
        configs.append(rel.as_posix())
    return configs


def _load_raw(rel_path: str) -> tuple[dict[str, Any], Path]:
    """Load a config JSON file; return (raw_dict, config_dir)."""
    path = _REPO_ROOT / rel_path
    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return raw, path.parent.resolve()


def _configs_of_kind(kind: str) -> list[str]:
    """Discovered configs whose autodetected type is ``kind``."""
    return [p for p in _ALL_CONFIGS if autodetect_type(_load_raw(p)[0]) == kind]


_ALL_CONFIGS = _discover_configs()
_STREAM_TEMPLATES = _configs_of_kind("stream")
_FJORD_TEMPLATES = _configs_of_kind("fjord")
_WATERSHED_TEMPLATES = _configs_of_kind("tideweaver")


def test_config_discovery_is_not_silently_empty() -> None:
    """Guard the glob itself.

    ``@pytest.mark.parametrize`` over an empty list PASSES silently, so a
    broken glob would turn every regression below into a no-op that still
    reports green.  Assert discovery actually found configs, that each one is
    classified into exactly one kind, and that the known-good baseline is
    present.
    """
    assert _ALL_CONFIGS, "config discovery found nothing — the glob is broken"
    assert len(_STREAM_TEMPLATES) + len(_FJORD_TEMPLATES) + len(_WATERSHED_TEMPLATES) == len(_ALL_CONFIGS)
    # Baseline that predates the glob; if these vanish, discovery regressed.
    assert "examples/cli-templates/stream-basic.json" in _STREAM_TEMPLATES
    assert "examples/cli-templates/fjord-basic.json" in _FJORD_TEMPLATES
    assert "examples/11-tideweaver/watershed.json" in _WATERSHED_TEMPLATES


@pytest.mark.parametrize("rel_path", _STREAM_TEMPLATES)
def test_stream_template_validates(rel_path: str) -> None:
    """Every in-repo stream config — template or tutorial — passes structural validation."""
    raw, config_dir = _load_raw(rel_path)
    errors = validate_stream_config(raw, config_dir)
    assert errors == [], f"Config {rel_path} failed: {errors}"


@pytest.mark.parametrize("rel_path", _FJORD_TEMPLATES)
def test_fjord_template_validates(rel_path: str) -> None:
    """Every in-repo fjord config passes structural + sidecar validation.

    Sidecar validation is the load-bearing part for tutorials: it resolves
    ``inflow`` / ``outflow`` against the config's own directory, so a config
    carrying repo-root-relative sidecar paths (the bug T10's README shipped
    for months) fails here instead of at a reader's terminal.
    """
    raw, config_dir = _load_raw(rel_path)
    errors = validate_fjord_config(raw, config_dir)
    assert errors == [], f"Config {rel_path} failed: {errors}"


@pytest.mark.parametrize("rel_path", _WATERSHED_TEMPLATES)
def test_watershed_template_validates(rel_path: str) -> None:
    """Every in-repo watershed config passes full build_watershed validation.

    Mirrors ``_load_pipeline_config``'s exact sequence (``cli/runners.py:72-152``)
    rather than calling ``validate_watershed_config`` on the raw/env-expanded
    dict directly: ``build_watershed`` (unlike ``load_watershed``) does NOT
    resolve ``@name`` sidecar tokens itself (that step lives in
    ``load_watershed``, one layer up), so a config whose ``window`` uses the
    dateless ``"@window_start"``/``"@window_end"`` sigil form would otherwise
    reach ``_parse_window`` as a literal, unresolved string and fail. Every
    REAL CLI entry point (``incorporator validate``, ``incorporator tideweaver
    validate/run``) already resolves tokens via ``_load_pipeline_config``
    before validation ever runs — this test previously skipped that step,
    which is a test-fidelity bug, not evidence the mechanism doesn't work.
    """
    raw, config_dir = _load_raw(rel_path)
    # Watershed window timestamps may be ISO-8601 strings with :-defaults, or a
    # dateless "@name" sigil resolved from an inflow/outflow sidecar; env-expand
    # resolves the ${VAR:-default} form without requiring any env vars to be set.
    expanded: dict[str, Any] = expand_env(raw)
    rebased = resolve_config_paths(expanded, config_dir)
    inflow_field = rebased.get("inflow")
    outflow_field = rebased.get("outflow")
    extra_names = merge_sidecar_extra_names(
        Path(str(inflow_field)) if inflow_field else None,
        Path(str(outflow_field)) if outflow_field else None,
        strict_outflow=False,
    )
    resolved = resolve_tokens(rebased, extra_names=extra_names or None)
    errors = validate_watershed_config(resolved, config_dir)
    assert errors == [], f"Config {rel_path} failed: {errors}"


# ---------------------------------------------------------------------------
# Dateless watershed windows — pin the sidecar-datetime-token mechanism
# ---------------------------------------------------------------------------


def test_watershed_window_resolves_sidecar_datetime_token(tmp_path: Path) -> None:
    """Pins the dateless-window mechanism: a public sidecar ``datetime``
    resolves through ``load_watershed``'s token pipeline into
    ``Watershed.window``.

    If ``resolve_tokens`` is ever narrowed to a key-allowlist that excludes
    ``"window"``, this goes from an equality assertion to a ``ValueError``
    raised by ``_parse_dt`` when it receives the literal string
    ``"@window_start"`` — i.e. it fails loudly, not silently.
    """
    outflow_body = (
        "from incorporator import Incorporator\n"
        "import datetime as _dt\n"
        "window_start = _dt.datetime(2099, 1, 1, tzinfo=_dt.timezone.utc)\n"
        "window_end = window_start + _dt.timedelta(seconds=60)\n"
        "class LapData(Incorporator):\n    pass\n"
        "def outflow(state):\n    return []\n"
    )
    (tmp_path / "outflow.py").write_text(outflow_body, encoding="utf-8")

    body: dict[str, Any] = {
        "window": {"start": "@window_start", "end": "@window_end"},
        "shape": "chain",
        "outflow": "outflow.py",
        "drain_timeout": 5,
        "gate_mode": "hard",
        "currents": [
            {"name": "laps", "class": "LapData", "verb": "stream", "interval": 30, "incorp_params": {}},
        ],
    }
    cfg = tmp_path / "ws.json"
    cfg.write_text(json.dumps(body), encoding="utf-8")

    ws = load_watershed(cfg)

    expected_start = datetime(2099, 1, 1, tzinfo=timezone.utc)
    expected_end = expected_start + timedelta(seconds=60)
    assert ws.window == (expected_start, expected_end)


def test_watershed_window_token_requires_sidecar_merge_not_resolve_tokens_alone() -> None:
    """Proves the sidecar merge, not ``resolve_tokens``/``_parse_window``
    themselves, is what makes the dateless mechanism work.

    Calling ``resolve_tokens`` on a ``"window"`` block with NO ``extra_names``
    (i.e. no sidecar merge) must raise ``TokenResolutionError`` — the
    ``"@window_start"`` name is not in the framework's own ``_ALLOWED_NAMES``.
    """
    with pytest.raises(TokenResolutionError):
        resolve_tokens({"window": {"start": "@window_start", "end": "@window_end"}}, extra_names=None)


# ---------------------------------------------------------------------------
# Nested-path Nm token + bare-tuple forms
# ---------------------------------------------------------------------------


def test_resolve_nm_directive_nested_paths() -> None:
    """Nm token with dotted-path args resolves to an Nm with nested DataPaths."""
    out: dict[str, Any] = resolve_tokens({"name_chg": ["Nm('user.email', 'contact.email')"]})
    assert len(out["name_chg"]) == 1
    nm = out["name_chg"][0]
    assert isinstance(nm, Nm)
    assert nm.old == "user.email"
    assert nm.new == "contact.email"


def test_name_chg_bare_tuple_nested_paths() -> None:
    """Bare ("a.b","c.d") tuples normalize to nested-aware Nm via _normalize_etl_kwargs."""
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=[("user.email", "contact.email")],
        code_attr=None,
        name_attr=None,
    )
    assert len(result.nm_tuple) == 1
    nm = result.nm_tuple[0]
    assert nm.old == "user.email"
    assert nm.new == "contact.email"
    # The cached DataPaths must have multiple segments (proves nested-aware).
    assert len(nm._old_path.segments) == 2
    assert len(nm._new_path.segments) == 2
