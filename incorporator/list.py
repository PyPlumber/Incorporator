"""IncorporatorList: the typed list wrapper and deduplication utility.

Lives at the package root (rather than under ``schema/``) because it is
a runtime collection of Incorporator instances — public API exported in
``incorporator.__all__`` — not a schema-compilation artifact.  ``schema/``
is reserved for the modules that build Pydantic classes from raw data
(``builder``, ``router``, ``factory``).
"""

from __future__ import annotations

import logging
import weakref
from typing import Any, TypeVar, cast

from .rejects import RejectEntry

logger = logging.getLogger(__name__)

T = TypeVar("T")
TIncorporator = TypeVar("TIncorporator")  # kept for any external callers


def _deduplicate_extracted(data: list[Any]) -> list[Any]:
    """Deduplicate extracted parent data preserving insertion order.

    Falls back gracefully when non-hashable items (dicts, objects) are present:
    deduplicates the hashable subset and appends non-hashable items as-is.
    """
    try:
        return list(dict.fromkeys(data))
    except TypeError:
        hashable = [x for x in data if isinstance(x, str | int | float | bool)]
        non_hashable = [x for x in data if not isinstance(x, str | int | float | bool)]
        if non_hashable:
            logger.warning(
                "extracted_data contains %d non-hashable item(s) that cannot be "
                "deduplicated and will be included as-is. Consider extracting scalar IDs.",
                len(non_hashable),
            )
        return list(dict.fromkeys(hashable)) + non_hashable


class IncorporatorList(list[T]):
    """Typed-list-plus-O(1)-registry — what :meth:`Incorporator.incorp` hands back for multi-record sources.

    Use it like any Python list (iterate, slice, ``len()``, pass to
    ``link_to`` joins) and reach for two extras when you need them:
    :attr:`inc_dict` for point lookups by primary key, and
    :attr:`failed_sources` / :attr:`rejects` as the failure surface for
    retry orchestrators.

    Example::

        coins = await Coin.incorp(inc_url="...", inc_code="id")
        for coin in coins:                       # IncorporatorList behaves as list
            print(coin.name)
        btc = coins.inc_dict["bitcoin"]          # O(1) primary-key lookup
        if coins.rejects:                        # structured failures
            for entry in coins.rejects:
                schedule_retry(entry.source, after=entry.retry_after)

    The same instance can be used wherever ``List[Incorporator]`` is
    accepted — including ``Class.export(instance=this_list)`` and any
    ``link_to(this_list)`` join from another class.  ``rejects``
    collects a structured :class:`RejectEntry` per URL or file path
    that hit a permanent error (HTTP 4xx-other-than-429, network
    failure, unparseable payload); the legacy ``failed_sources``
    derives from it as ``[entry.source for entry in rejects]``.
    """

    def __init__(
        self,
        model_class: type[Any],
        items: list[Any],
        failed_sources: list[str] | None = None,
        rejects: list[RejectEntry] | None = None,
    ):
        super().__init__(items)
        self._model_class = model_class
        # Structured rejects.  Accept EITHER the legacy
        # ``failed_sources`` (List[str] — auto-wrap each string in a
        # minimal :class:`RejectEntry`) OR the new ``rejects``
        # (List[RejectEntry] — preferred).  Passing both raises so
        # callers don't accidentally double-fill.
        if failed_sources is not None and rejects is not None:
            raise ValueError(
                "IncorporatorList: pass `failed_sources` (legacy List[str]) OR `rejects` (List[RejectEntry]), not both."
            )
        if rejects is not None:
            self._rejects: list[RejectEntry] = list(rejects)
        elif failed_sources is not None:
            self._rejects = [RejectEntry(source=s, error_kind="Unknown", message=s) for s in failed_sources]
        else:
            self._rejects = []

        # Per-list cache slot for graph-drilling routes (set by incorp()).
        self.inc_child_path: str | None = None

    @property
    def rejects(self) -> list[RejectEntry]:
        """Structured view of failed sources — preferred over ``failed_sources``.

        Returns a defensive copy of the list (the underlying storage is
        kept private so callers can't mutate framework state — entries
        are frozen Pydantic models so the entries themselves are safe).
        """
        return list(self._rejects)

    @property
    def failed_sources(self) -> list[str]:
        """Legacy string-list view of the failure surface.

        Equivalent to ``[entry.source for entry in self.rejects]``;
        kept as a derived view so every existing user/test/example that
        reads ``IncorporatorList.failed_sources`` continues working
        unchanged.  Reach for :attr:`rejects` when you need structured
        access to ``error_kind`` / ``message`` / ``retry_after`` /
        ``wave_index``.
        """
        return [entry.source for entry in self._rejects]

    def __del__(self) -> None:
        """Diagnostic hook — emits a DEBUG log if a non-empty list is GC'd.

        Helps catch the "forgot to assign ``incorp()`` to a variable" gotcha
        where the list (and therefore the registry entries) disappear
        before the user can query ``.inc_dict``.  No-op unless
        ``_warn_on_gc`` is set on the instance.
        """
        if not self:
            return
        if getattr(self, "_warn_on_gc", False):
            logger.debug(
                "INCORPORATOR GC ALERT: A built list was just garbage collected. "
                "Ensure you assign incorp() to a variable if you need to use .inc_dict."
            )

    @property
    def inc_dict(self) -> "weakref.WeakValueDictionary[Any, Any]":
        """The O(1) registry mapping primary key → instance — no list-scanning, no manual dict-building.

        Reach for it whenever a list-walk would be quadratic: parent-child
        drilling, dedup checks, point queries by ID.  The registry is
        already built (every ``incorp()`` populates it as instances are
        constructed), so the lookup is a single dict hit.

        Example::

            launches = await Launch.incorp(inc_url="...", inc_code="id")
            launch = launches.inc_dict["abc123"]      # O(1) lookup

        ``inc_dict.get(key)`` returns ``None`` for missing keys rather than
        raising — the same way Python's ``dict.get()`` does.  The registry
        is shared with the class itself, so ``Launch.inc_dict[key]`` and
        ``launches.inc_dict[key]`` return the same instance.

        **The registry is a ``weakref.WeakValueDictionary`` — keep the
        ``IncorporatorList`` alive.** The list returned by ``incorp()`` is
        the only strong-ref holder for the instances it registered.  If
        you discard it (``await Class.incorp(...)`` without binding to a
        variable), Python's garbage collector reaps the instances and the
        registry will be empty at the next read.  Always bind the return
        into a local variable for as long as you need
        ``Class.inc_dict[key]`` to resolve.
        """
        return cast("weakref.WeakValueDictionary[Any, Any]", self._model_class.inc_dict)
