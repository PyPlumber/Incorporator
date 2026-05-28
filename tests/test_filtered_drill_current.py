"""Unit tests for ``FilteredDrillCurrent``.

Seven tests (five functions, one parametrized over two cases) verify the declarative
drill-with-filter primitive:

1. Tuple predicate filters the parent snapshot and calls incorp() for matching rows only.
2. Callable (lambda) predicate applies the same filter-then-drill logic.
3. Empty filtered list short-circuits: incorp() is never called when no rows match.
4. Upstream snapshot is None or empty: tick() silently skips, no incorp() call.
5. Non-callable second element raises ValueError at construction time.
6. Wrong-length tuple predicate (too short / too long) raises ValidationError at
   construction time (parametrized over both cases).
"""

from __future__ import annotations

import operator
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import ConfigDict

from incorporator import Incorporator
from incorporator.observability.tideweaver import FilteredDrillCurrent


# ---------------------------------------------------------------------------
# Module-level Incorporator subclasses
# ---------------------------------------------------------------------------


class Parent(Incorporator):
    """Upstream class whose _tideweaver_snapshot FilteredDrillCurrent reads."""

    model_config = ConfigDict(extra="allow")


class Target(Incorporator):
    """Downstream class that FilteredDrillCurrent drives via incorp()."""

    model_config = ConfigDict(extra="allow")


# ---------------------------------------------------------------------------
# Scheduler stub — tick() receives it but doesn't call any methods on it
# ---------------------------------------------------------------------------


class _StubScheduler:
    """Minimal scheduler stand-in; FilteredDrillCurrent.tick() never calls it."""


# ---------------------------------------------------------------------------
# Reset helper — mirrors tests/test_tideweaver_routing_diamond.py:102-110
# ---------------------------------------------------------------------------


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
# Test 1 — tuple predicate filters correctly
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tuple_predicate_filters_and_drills(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Tuple predicate passes only matching rows to incorp().

    Proves that when the parent snapshot contains two rows with different
    ``division`` values, a structured-tuple predicate ``("division",
    operator.eq, 201)`` passes exactly the row with division==201 to
    ``Target.incorp(inc_parent=filtered, ...)`` and that auto-park fires
    afterwards, setting ``Target._tideweaver_snapshot`` to a non-None value.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(Parent, Target)

    row_a = Parent(inc_code=1, division=201)  # type: ignore[call-arg]
    row_b = Parent(inc_code=2, division=200)  # type: ignore[call-arg]
    Parent._tideweaver_snapshot = [row_a, row_b]  # type: ignore[attr-defined]

    drilled_rows = [Target(inc_code=1)]
    mock_incorp = AsyncMock(return_value=drilled_rows)
    monkeypatch.setattr(Target, "incorp", mock_incorp)

    drill = FilteredDrillCurrent(
        name="d",
        cls=Target,
        interval=1.0,
        parent=Parent,
        predicate=("division", operator.eq, 201),
        inc_url="http://x/{}",
        inc_child="inc_code",
    )
    await drill._run_tick(_StubScheduler())

    mock_incorp.assert_called_once()
    call_kwargs = mock_incorp.call_args.kwargs
    assert call_kwargs["inc_parent"] == [row_a], (
        f"incorp() must be called with only the matching row; got {call_kwargs['inc_parent']}"
    )
    snapshot = getattr(Target, "_tideweaver_snapshot", None)
    assert snapshot is not None, "_tideweaver_snapshot must be auto-parked after _run_tick"


# ---------------------------------------------------------------------------
# Test 2 — callable (lambda) predicate
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_callable_predicate_filters_and_drills(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Callable predicate passes only matching rows to incorp().

    Proves that a ``lambda r: r.division == 201`` predicate produces the
    same filtering behaviour as the structured-tuple form: only the row
    with division==201 is forwarded to incorp(), and auto-park fires.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(Parent, Target)

    row_a = Parent(inc_code=1, division=201)  # type: ignore[call-arg]
    row_b = Parent(inc_code=2, division=200)  # type: ignore[call-arg]
    Parent._tideweaver_snapshot = [row_a, row_b]  # type: ignore[attr-defined]

    drilled_rows = [Target(inc_code=1)]
    mock_incorp = AsyncMock(return_value=drilled_rows)
    monkeypatch.setattr(Target, "incorp", mock_incorp)

    drill = FilteredDrillCurrent(
        name="d",
        cls=Target,
        interval=1.0,
        parent=Parent,
        predicate=lambda r: r.division == 201,  # type: ignore[union-attr]
        inc_url="http://x/{}",
        inc_child="inc_code",
    )
    await drill._run_tick(_StubScheduler())

    mock_incorp.assert_called_once()
    call_kwargs = mock_incorp.call_args.kwargs
    assert call_kwargs["inc_parent"] == [row_a], (
        f"incorp() must be called with only the matching row; got {call_kwargs['inc_parent']}"
    )
    snapshot = getattr(Target, "_tideweaver_snapshot", None)
    assert snapshot is not None, "_tideweaver_snapshot must be auto-parked after _run_tick"


# ---------------------------------------------------------------------------
# Test 3 — empty filtered list short-circuits
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_filtered_list_short_circuits(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """When the predicate rejects every row, incorp() is never called.

    Proves that a predicate matching no rows (``operator.eq, 999``) causes
    tick() to return early without invoking incorp(), preventing unnecessary
    network traffic.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(Parent, Target)

    row_a = Parent(inc_code=1, division=201)  # type: ignore[call-arg]
    row_b = Parent(inc_code=2, division=200)  # type: ignore[call-arg]
    Parent._tideweaver_snapshot = [row_a, row_b]  # type: ignore[attr-defined]

    mock_incorp = AsyncMock(return_value=[])
    monkeypatch.setattr(Target, "incorp", mock_incorp)

    drill = FilteredDrillCurrent(
        name="d",
        cls=Target,
        interval=1.0,
        parent=Parent,
        predicate=("division", operator.eq, 999),
        inc_url="http://x/{}",
        inc_child="inc_code",
    )
    await drill._run_tick(_StubScheduler())

    mock_incorp.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4 — upstream snapshot is None / empty → silent skip
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_none_or_empty_upstream_snapshot_silently_skips(
    tmp_path: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the parent snapshot is None or [], tick() exits without calling incorp().

    Proves first-tick safety: when Tideweaver hasn't reached the upstream
    current yet (``_tideweaver_snapshot`` is absent / None) or the upstream
    ran but produced no rows (``[]``), FilteredDrillCurrent silently returns
    and Target._tideweaver_snapshot remains None.
    """
    monkeypatch.chdir(tmp_path)
    _reset_registries(Parent, Target)

    mock_incorp = AsyncMock(return_value=[])
    monkeypatch.setattr(Target, "incorp", mock_incorp)

    drill = FilteredDrillCurrent(
        name="d",
        cls=Target,
        interval=1.0,
        parent=Parent,
        predicate=("division", operator.eq, 201),
        inc_url="http://x/{}",
        inc_child="inc_code",
    )

    # Case A: snapshot is None (attribute absent)
    await drill._run_tick(_StubScheduler())
    mock_incorp.assert_not_called()
    # Auto-park fires but inc_dict is empty so it parks []; both None and [] mean "no data"
    assert not getattr(Target, "_tideweaver_snapshot", None), (
        "Target snapshot must be absent or empty when upstream had no data"
    )

    # Case B: snapshot is empty list
    _reset_registries(Parent, Target)
    Parent._tideweaver_snapshot = []  # type: ignore[attr-defined]
    await drill._run_tick(_StubScheduler())
    mock_incorp.assert_not_called()
    assert not getattr(Target, "_tideweaver_snapshot", None), (
        "Target snapshot must be absent or empty when upstream had no data"
    )


# ---------------------------------------------------------------------------
# Tests 5–6 — malformed tuple predicates raise at construction
# ---------------------------------------------------------------------------


def test_malformed_tuple_predicate_raises(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Non-callable second element raises ValueError at FilteredDrillCurrent construction.

    Proves that passing a string instead of a callable as the operator
    (second element of the tuple) is caught by ``_validate_predicate`` and
    raises a ``ValueError`` whose message mentions "predicate tuple".
    """
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError, match="predicate tuple"):
        FilteredDrillCurrent(
            name="d",
            cls=Target,
            interval=1,
            parent=Parent,
            predicate=("division", "not_a_callable", 201),  # type: ignore[arg-type]
            inc_url="http://x/{}",
        )


@pytest.mark.parametrize(
    "bad_predicate",
    [
        ("division", operator.eq),  # length != 3 (too short)
        ("division", operator.eq, 201, "extra"),  # length != 3 (too long)
    ],
)
def test_wrong_length_tuple_predicate_raises(bad_predicate: Any) -> None:
    """Wrong-length tuple predicate raises ValidationError at construction.

    Proves that tuples with length != 3 are rejected by Pydantic's union
    coercion before the model_validator runs, raising ``ValidationError``
    rather than passing through to ``_validate_predicate``.
    """
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        FilteredDrillCurrent(
            name="d",
            cls=Target,
            interval=1,
            parent=Parent,
            predicate=bad_predicate,  # type: ignore[arg-type]
            inc_url="http://x/{}",
        )
