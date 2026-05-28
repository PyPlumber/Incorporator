"""Unit tests for ``CustomCurrent.auto_park_snapshot`` and ``_run_tick``.

Three tests verify the auto-park semantics introduced in v1.1.3:

1. Default auto-park fires when user ``tick()`` populates ``inc_dict``
   without manually assigning ``_tideweaver_snapshot``.
2. Manual assignment inside ``tick()`` wins — auto-park is skipped.
3. ``auto_park_snapshot = False`` opts out — snapshot is never touched.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from incorporator import Incorporator
from incorporator.observability.tideweaver import CustomCurrent


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class Target(Incorporator):
    """Minimal Incorporator subclass used as the current's ``cls``."""


def _reset_registries(*classes: type[Incorporator]) -> None:
    """Wipe per-class inc_dict + parked snapshot between tests."""
    for cls in classes:
        cls.inc_dict.clear()
        if "_tideweaver_snapshot" in cls.__dict__:
            try:
                delattr(cls, "_tideweaver_snapshot")
            except AttributeError:
                pass


# ---------------------------------------------------------------------------
# Test 1 — default auto-park fires
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_autopark_fires_when_no_manual_park(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Auto-park populates ``_tideweaver_snapshot`` when tick() does not.

    Proves that after ``_run_tick`` returns, ``Target._tideweaver_snapshot``
    equals ``list(Target.inc_dict.values())`` and is non-empty when the
    tick body populated ``inc_dict`` directly without assigning the snapshot.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(Target)

    instance = Target(inc_code=1)
    Target.inc_dict[instance.inc_code] = instance

    class Drill(CustomCurrent):
        async def tick(self, scheduler: Any) -> None:
            # Populate inc_dict but do NOT touch _tideweaver_snapshot.
            pass

    drill = Drill(name="drill", cls=Target, interval=1.0)
    await drill._run_tick(object())

    snapshot = getattr(Target, "_tideweaver_snapshot", None)
    assert snapshot is not None, "_tideweaver_snapshot must be parked after _run_tick"
    assert snapshot == [instance], f"auto-parked snapshot must equal inc_dict values, got {snapshot}"


# ---------------------------------------------------------------------------
# Test 2 — manual assignment wins
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_manual_park_wins_over_autopark(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Manual ``_tideweaver_snapshot`` assignment inside ``tick()`` is preserved.

    Proves that when ``tick()`` assigns ``Target._tideweaver_snapshot`` to a
    new list object, the identity check (``is pre``) detects the new object
    and auto-park is skipped, leaving the manually-assigned value intact.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(Target)

    class Drill(CustomCurrent):
        async def tick(self, scheduler: Any) -> None:
            Target._tideweaver_snapshot = ["sentinel"]  # type: ignore[attr-defined]

    drill = Drill(name="drill", cls=Target, interval=1.0)
    await drill._run_tick(object())

    snapshot = getattr(Target, "_tideweaver_snapshot", None)
    assert snapshot == ["sentinel"], (
        f"manual park must be preserved; auto-park must not overwrite it, got {snapshot}"
    )


# ---------------------------------------------------------------------------
# Test 3 — auto_park_snapshot = False opts out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_autopark_disabled_leaves_snapshot_untouched(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``auto_park_snapshot = False`` prevents any snapshot assignment.

    Proves that a subclass with ``auto_park_snapshot = False`` and an empty
    ``tick()`` body leaves ``Target._tideweaver_snapshot`` completely unset
    after ``_run_tick``.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(Target)

    class Drill(CustomCurrent):
        auto_park_snapshot: ClassVar[bool] = False

        async def tick(self, scheduler: Any) -> None:
            pass

    drill = Drill(name="drill", cls=Target, interval=1.0)
    await drill._run_tick(object())

    assert getattr(Target, "_tideweaver_snapshot", None) is None, (
        "auto_park_snapshot=False must leave _tideweaver_snapshot unset"
    )
