"""Integration tests: Ex / Nm / Pk tokens in pipeline.json + watershed.json.

Verifies that the wrapper directives flow through:
- resolve_tokens() — JSON-string -> Python wrapper instance
- Existing template regression — all in-repo templates still validate
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from incorporator.cli.envexpand import expand_env
from incorporator.cli.tokens import resolve_tokens
from incorporator.cli.validate import (
    autodetect_type,
    validate_fjord_config,
    validate_stream_config,
    validate_watershed_config,
)
from incorporator.schema.directives import Ex, Nm, Pk

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
# Template regression — all in-repo templates must still validate
# ---------------------------------------------------------------------------

# Stream and fjord templates: validate structure from raw JSON (env-var strings
# in headers / params are valid string content for Pydantic; no env-expand needed).
# Watershed templates: env-expand first so window timestamps parse as ISO 8601
# (the :-default form resolves cleanly without any env vars set).

_STREAM_TEMPLATES = [
    "examples/cli-templates/stream-basic.json",
    "examples/cli-templates/daemon-mode.json",
    "examples/cli-templates/with-auth.json",
]

_FJORD_TEMPLATES = [
    "examples/cli-templates/fjord-basic.json",
]

_WATERSHED_TEMPLATES = [
    "examples/11-tideweaver/watershed.json",
    "examples/appendix/mlb-pulse/watershed.json",
    "examples/appendix/nascar-tideweaver/watershed.json",
]


def _load_raw(rel_path: str) -> tuple[dict[str, Any], Path]:
    """Load a template JSON file; return (raw_dict, config_dir)."""
    path = _REPO_ROOT / rel_path
    raw: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return raw, path.parent.resolve()


@pytest.mark.parametrize("rel_path", _STREAM_TEMPLATES)
def test_stream_template_validates(rel_path: str) -> None:
    """All stream CLI templates pass structural validation after the Ex/Nm/Pk additions."""
    raw, config_dir = _load_raw(rel_path)
    errors = validate_stream_config(raw, config_dir)
    assert errors == [], f"Template {rel_path} failed: {errors}"


@pytest.mark.parametrize("rel_path", _FJORD_TEMPLATES)
def test_fjord_template_validates(rel_path: str) -> None:
    """All fjord CLI templates pass structural + sidecar validation after the Ex/Nm/Pk additions."""
    raw, config_dir = _load_raw(rel_path)
    errors = validate_fjord_config(raw, config_dir)
    assert errors == [], f"Template {rel_path} failed: {errors}"


@pytest.mark.parametrize("rel_path", _WATERSHED_TEMPLATES)
def test_watershed_template_validates(rel_path: str) -> None:
    """All watershed templates pass full build_watershed validation after the Ex/Nm/Pk additions."""
    raw, config_dir = _load_raw(rel_path)
    # Watershed window timestamps are ISO-8601 strings with :-defaults; env-expand
    # resolves the ${VAR:-default} form without requiring any env vars to be set.
    expanded: dict[str, Any] = expand_env(raw)
    errors = validate_watershed_config(expanded, config_dir)
    assert errors == [], f"Template {rel_path} failed: {errors}"


def test_existing_templates_autodetect_correctly() -> None:
    """autodetect_type() correctly classifies every in-repo template file."""
    for rel_path in _STREAM_TEMPLATES:
        raw, _ = _load_raw(rel_path)
        assert autodetect_type(raw) == "stream", f"{rel_path} should be detected as stream"
    for rel_path in _FJORD_TEMPLATES:
        raw, _ = _load_raw(rel_path)
        assert autodetect_type(raw) == "fjord", f"{rel_path} should be detected as fjord"
    for rel_path in _WATERSHED_TEMPLATES:
        raw, _ = _load_raw(rel_path)
        assert autodetect_type(raw) == "tideweaver", f"{rel_path} should be detected as tideweaver"
