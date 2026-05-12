"""
IncorporatorList: the typed list wrapper and deduplication utility.

Kept separate from base.py so it can be imported by factory.py without
creating a circular dependency chain.
"""

import logging
import weakref
from typing import Any, Generic, List, Optional, Type, TypeVar, cast

logger = logging.getLogger(__name__)

T = TypeVar("T")
TIncorporator = TypeVar("TIncorporator")  # kept for any external callers


def _deduplicate_extracted(data: List[Any]) -> List[Any]:
    """Deduplicate extracted parent data preserving insertion order.

    Falls back gracefully when non-hashable items (dicts, objects) are present:
    deduplicates the hashable subset and appends non-hashable items as-is.
    """
    try:
        return list(dict.fromkeys(data))
    except TypeError:
        hashable = [x for x in data if isinstance(x, (str, int, float, bool))]
        non_hashable = [x for x in data if not isinstance(x, (str, int, float, bool))]
        if non_hashable:
            logger.warning(
                f"extracted_data contains {len(non_hashable)} non-hashable item(s) that cannot be "
                "deduplicated and will be included as-is. Consider extracting scalar IDs."
            )
        return list(dict.fromkeys(hashable)) + non_hashable


class IncorporatorList(List[T]):
    """
    A specialized list providing direct access to the dynamic class registry.

    When `incorp()` returns multiple items, this wrapper allows users to run
    `dataset.inc_dict.get(id)` seamlessly against the dynamically generated class
    without needing to manually inspect `type(dataset[0])`.
    """

    failed_sources: List[str]

    def __init__(
        self,
        model_class: Type[Any],
        items: List[Any],
        failed_sources: Optional[List[str]] = None,
    ):
        super().__init__(items)
        self._model_class = model_class
        # Exposes HTTP 429 failed URLs/Paths for programmatic Dead Letter Queue retries
        self.failed_sources = failed_sources if failed_sources is not None else []

        # Protects schema_builder.py's cache from cross-contamination during graph drilling
        self.inc_child_path: Optional[str] = None

    def __del__(self) -> None:
        """Memory-leak sentinel alerting users to immediate Garbage Collection."""
        if not self:
            return
        if getattr(self, "_warn_on_gc", False):
            logger.debug(
                "🧹 INCORPORATOR GC ALERT: A built list was just garbage collected. "
                "Ensure you assign `.incorp()` to a variable if you need to use `.inc_dict`!"
            )

    @property
    def inc_dict(self) -> "weakref.WeakValueDictionary[Any, Any]":
        """Provides O(1) direct access to the class-level weakref registry."""
        return cast("weakref.WeakValueDictionary[Any, Any]", self._model_class.inc_dict)
