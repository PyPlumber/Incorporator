"""Inflow sidecar for the NASCAR fantasy-league fjord pipeline.

Provides incoming-data manipulation: the ``inflow(state)`` callable that
wires Race's foreign-key fields against already-loaded Track + Driver
registries, plus the conv_dict helpers and constants those wires depend on.

* ``_DATE_FIELDS`` — tuple of Race fields that carry raw timestamp strings
  and must be coerced to ``datetime`` objects.
* ``_driver_id_or_none`` — sentinel guard for NASCAR's ``0``-as-missing
  pattern on driver-ID fields; lets ``link_to`` short-circuit cleanly.
* ``_speed_or_none`` — sentinel guard for NASCAR's ``0.0``-as-missing
  pattern on ``pole_winner_speed``; same shape as ``_driver_id_or_none``
  but for a float field consumed by ``calc()`` instead of ``link_to()``.
* ``_mfg_from_logo_url`` — parses a NASCAR CDN logo URL into the make name
  (``'Chevrolet'``, ``'Ford'``, ``'Toyota'``, ``'Ram'``); used as the
  ``calc()`` converter for the Driver source's ``Manufacturer`` field.
* ``inflow(state)`` — fjord seed hook called before each source refresh;
  emits Race conv_dict overrides once Track + Driver registries are live.

Output shaping (``outflow(state)``, source classes, ``OWNER_SCORED``) lives
in the sibling ``outflow.py``.
"""

from datetime import datetime
from typing import Any

from incorporator import calc, inc, link_to

# ── Constants ──────────────────────────────────────────────────────

_DATE_FIELDS = ("date_scheduled", "race_date", "qualifying_date", "tunein_date")


# ── Sentinel filter for link_to ────────────────────────────────────


def _driver_id_or_none(raw: Any) -> Any:
    """NASCAR returns ``0`` for any driver-ID field whose underlying
    event hasn't happened yet (qualifying not held, race not run,
    rain-out).  Driver ID 0 coincidentally resolves to a real driver
    in the registry, so without this filter every future race's
    pole/winner column would show the same incidental name.  Mapping
    falsy values (``0``, ``None``, ``""``) to ``None`` lets ``link_to``
    short-circuit and downstream consumers see ``None``.
    """
    return raw if raw else None


def _speed_or_none(raw: Any) -> float | None:
    """NASCAR returns ``0.0`` for ``pole_winner_speed`` on races whose pole
    hasn't been set yet (same sentinel pattern as the driver-ID fields
    above).  Mapping ``0.0`` to ``None`` at build time means outflow reads
    ``race.pole_winner_speed`` directly — no ``if pole else None`` guard
    needed against the magic-number sentinel.  Casts to ``float`` inline
    (rather than via ``calc()``'s ``target_type=``) so a genuine ``None``
    result doesn't hit ``float(None)`` and log a per-row coercion warning.
    """
    return float(raw) if raw else None


# ── Helpers ────────────────────────────────────────────────────────


def _mfg_from_logo_url(url: str) -> str:
    """Parse a NASCAR manufacturer logo URL into the make name.

    'https://www.nascar.com/.../Chevrolet_2025-330x140.png' -> 'Chevrolet'
    'https://www.nascar.com/.../Ford-Logo-1-320x180.png'   -> 'Ford'
    'https://www.nascar.com/.../Toyota-180x180.png'         -> 'Toyota'
    'https://www.nascar.com/.../Ram-330x115.png'            -> 'Ram'

    Splits the basename on underscores and hyphens; first token is the make.
    is_garbage_value pre-handles empty / None inputs — no defensive guard needed.
    """
    basename = url.rsplit("/", 1)[-1]  # 'Chevrolet_2025-330x140.png'
    stem = basename.split(".")[0]  # 'Chevrolet_2025-330x140'
    token = stem.replace("-", "_").split("_")[0]  # 'Chevrolet'
    return token


# ── State-aware inflow — wires Race.conv_dict against live peers ────


def inflow(state: dict[str, Any]) -> dict[str, Any]:
    """Build per-source ``conv_dict`` overrides from sibling registries.

    Inflow is called before each source's ``incorp()``.  On the early
    calls (Track / Driver / Standings / LeagueRoster) ``state`` is
    empty or partial, so we only emit Race's override once its peers
    exist — fjord then re-applies it on every refresh wave so Race's
    ``track_id``, ``pole_winner_driver_id``, and ``winner_driver_id``
    resolve to live ``Track`` / ``Driver`` instances rather than raw
    integers.
    """
    overrides: dict[str, Any] = {}
    if "Track" in state and "Driver" in state:
        overrides["Race"] = {
            "conv_dict": {
                "track_id": link_to(state["Track"]),
                "pole_winner_driver_id": link_to(state["Driver"], extractor=_driver_id_or_none),
                "winner_driver_id": link_to(state["Driver"], extractor=_driver_id_or_none),
                **{key: inc(datetime) for key in _DATE_FIELDS},
                # Build-time coercion so outflow.py reads these as plain
                # attributes instead of getattr(race, "...", default).
                "number_of_cars_in_field": inc(int, default=0),
                "television_broadcaster": inc(str, default="TBD"),
                "playoff_round": inc(int, default=0),
                # 0.0-as-missing sentinel, same shape as the driver-ID
                # fields above but for calc() since there's no dataset to
                # join against — just a float re-mapped to None.
                "pole_winner_speed": calc(_speed_or_none, "pole_winner_speed"),
            }
        }
    return overrides
