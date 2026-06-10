"""Unit tests for ``CustomCurrent.auto_park_snapshot`` and ``_run_tick``.

Four tests verify the auto-park semantics introduced in v1.1.3:

1. Default auto-park fires when user ``tick()`` populates ``inc_dict``
   without manually assigning ``_tideweaver_snapshot``.
2. Manual assignment inside ``tick()`` wins — auto-park is skipped.
3. ``auto_park_snapshot = False`` opts out — snapshot is never touched.
4. Re-assigning the *same* list object inside ``tick()`` defeats the
   identity check — auto-park overwrites. Documents the limit at
   ``CustomCurrent._run_tick``'s docstring; the test's assertion can
   be flipped if a future change tightens the check.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from incorporator import Incorporator
from incorporator.tideweaver import CustomCurrent

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
    assert snapshot == ["sentinel"], f"manual park must be preserved; auto-park must not overwrite it, got {snapshot}"


# ---------------------------------------------------------------------------
# Test 3 — auto_park_snapshot = False opts out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_autopark_disabled_leaves_snapshot_untouched(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
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


# ---------------------------------------------------------------------------
# Test 4 — same-list-object reassignment defeats the identity check
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_same_list_reassignment_defeats_identity_check(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Re-assigning the same list object inside ``tick()`` is silently overwritten.

    The auto-park guard at ``CustomCurrent._run_tick`` uses ``is pre`` (object
    identity) to detect a manual override. If ``tick()`` re-binds the
    pre-existing list to ``cls._tideweaver_snapshot`` instead of assigning a
    new list, ``is pre`` evaluates to ``True`` and auto-park overwrites the
    manual value with ``list(cls.inc_dict.values())``.

    This test locks the current behavior so a future fix that tightens the
    identity check (e.g. by snapshotting ``id(pre)`` AND a value hash) can
    flip the assertion. Documents the limit named in the ``_run_tick``
    docstring: "assign a NEW list to opt out."
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(Target)

    instance = Target(inc_code=42)
    Target.inc_dict[instance.inc_code] = instance

    pre_list = ["pre-existing"]
    Target._tideweaver_snapshot = pre_list  # type: ignore[attr-defined]

    class Drill(CustomCurrent):
        async def tick(self, scheduler: Any) -> None:
            # Identity rebinding: the same list object, NOT a new list.
            Target._tideweaver_snapshot = Target._tideweaver_snapshot  # type: ignore[attr-defined]

    drill = Drill(name="drill", cls=Target, interval=1.0)
    await drill._run_tick(object())

    snapshot = getattr(Target, "_tideweaver_snapshot", None)
    assert snapshot == [instance], (
        f"identity rebinding defeats the `is pre` check; auto-park is expected to overwrite "
        f"pre_list with list(inc_dict.values()); got {snapshot}"
    )
    assert snapshot is not pre_list, (
        "auto-park must replace the manual list with a fresh list(inc_dict.values()); "
        "the pre-existing list object must NOT be preserved"
    )
