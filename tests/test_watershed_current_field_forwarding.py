"""Structural regression guard: every Current-model field reaches ``_build_current``.

The bug class this guards against (see AGENTS.md / commits ``24b65bd``,
``25627d9``): a config schema accepts a key, but the loader/runner that
consumes it never forwards the value to the engine call. ``_build_current``
in ``incorporator/tideweaver/config.py`` forwards fields via an explicit
per-field allow-list rather than a generic loop over ``model_fields`` — so a
new field added to ``Current``/``Stream``/``Fjord``/``Export`` without also
being wired into that allow-list would be silently dropped at JSON-load time.

This test derives its expectations from ``model_fields`` directly, so it
fails the day a new field is added and not wired up — instead of relying on
a human remembering to write a matching per-key test (the approach that let
the ``inflow``/``outflow`` forwarding bug recur twice, once per CLI verb).
"""

from __future__ import annotations

import types
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from incorporator.base import Incorporator
from incorporator.tideweaver.config import _build_current
from incorporator.tideweaver.current import Export, Fjord, Stream

# name/cls/interval are always set unconditionally inside _build_current's
# `common` dict and are never candidates for being silently dropped — they
# don't need a generic sentinel builder.
_ALWAYS_HANDLED = {"name", "cls", "interval"}

_FIELD_SENTINEL_BUILDERS: dict[str, Callable[[], Any]] = {
    "depends_on": lambda: ["upstream_marker"],
    # Default is "restart" — pick a different literal so a dropped forward
    # reads back as the default instead of the sentinel.
    "on_error": lambda: "isolate",
    "phase_offset_sec": lambda: 12.5,
    "inflow": lambda: "sentinel_inflow.py",
    "outflow": lambda: "sentinel_outflow.py",
    "incorp_params": lambda: {"marker": "stream_incorp_sentinel"},
    "refresh_params": lambda: {"marker": "refresh_sentinel"},
    # file_path satisfies Export._require_export_destination too, so the
    # same builder works for Stream/Fjord/Export without a per-verb variant.
    "export_params": lambda: {"file_path": "sentinel_export.ndjson", "marker": "export_sentinel"},
    # Must NOT carry an "inc_parent" key in incorp_params — that would trip
    # Stream._validate_parent_current's mutual-exclusion check.
    "parent_current": lambda: "upstream_a",
    "parent_currents": lambda: ["upstream_a", "upstream_b"],
}


@pytest.mark.parametrize("verb, model_cls", [("stream", Stream), ("fjord", Fjord), ("export", Export)])
def test_build_current_forwards_every_current_field(
    verb: str, model_cls: type[Stream] | type[Fjord] | type[Export], tmp_path: Path
) -> None:
    """Every non-exempt ``model_fields`` key on Stream/Fjord/Export reaches ``_build_current``'s output.

    Derives the expected field set from ``model_cls.model_fields`` rather
    than a hand-maintained list, so a field added to any of the three
    verb-typed Currents without also being wired into ``_build_current``'s
    allow-list fails this test immediately.
    """
    fields = set(model_cls.model_fields) - _ALWAYS_HANDLED
    uncovered = fields - set(_FIELD_SENTINEL_BUILDERS)
    assert not uncovered, (
        f"{model_cls.__name__} has field(s) {sorted(uncovered)} with no sentinel builder — "
        "add a builder above (or an explicit exemption) before this guard can pass."
    )

    class TestCls(Incorporator):
        pass

    fake_outflow = types.ModuleType("fake_outflow_sidecar")
    fake_outflow.TestCls = TestCls  # type: ignore[attr-defined]

    entry: dict[str, Any] = {
        "name": "under_test",
        "class": "TestCls",
        "interval": 5.0,
        "verb": verb,
        **{key: builder() for key, builder in _FIELD_SENTINEL_BUILDERS.items() if key in fields},
    }

    result = _build_current(entry, outflow_module=fake_outflow, inflow_module=None, base_dir=tmp_path)

    for field in fields:
        expected = entry[field]
        actual = getattr(result, field)
        if field in ("inflow", "outflow"):
            expected = Path(expected)
        assert actual == expected, f"{model_cls.__name__}.{field}: expected {expected!r}, got {actual!r}"
