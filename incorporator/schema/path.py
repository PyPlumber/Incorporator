"""DataPath value type for dot-notation record traversal.

Parsed once from a dotted string, reused across N rows.  Per-row hot loop
calls ``resolve(record)``; never re-splits the string.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DataPath:
    """Parsed traversal recipe for reaching a value in a nested record.

    Constructed once from a dotted string (``"a.b.0.c"``), validated at parse
    time, hashable, reusable across N rows.  Per-row hot loop calls
    ``resolve(record)``; never re-splits the string.

    Same value-type shape as CurrentOutcome (frozen dataclass + slots —
    construction is hot-path so we accept zero serialisation surface
    in exchange for ~5x cheaper construction vs Pydantic).

    Attributes:
        segments: Pre-parsed tuple of path segments.  Each segment is either
            a ``str`` (dict key lookup) or an ``int`` (list index lookup).
        source: Original dotted string, preserved for debug messages and error
            reporting.
    """

    segments: tuple[str | int, ...]
    source: str

    @classmethod
    def parse(cls, dotted: str) -> DataPath:
        """Parse a dotted path string into a reusable ``DataPath``.

        Args:
            dotted: Dot-separated path string (e.g. ``"a.b.0.c"``).  Digit-only
                segments are converted to ``int`` for list-index access; all
                other segments stay as ``str``.

        Returns:
            A frozen, hashable ``DataPath`` instance.

        Raises:
            ValueError: if ``dotted`` is empty. The empty-path case is a
                deliberate stricter contract; earlier internal helpers returned
                the node silently.
        """
        if not dotted:
            raise ValueError("DataPath.parse: empty path string")
        parts = dotted.split(".")
        segments = tuple(int(p) if p.isdigit() else p for p in parts)
        return cls(segments=segments, source=dotted)

    def resolve(self, record: Any) -> Any:
        """Walk ``record`` segment-by-segment and return the value at this path.

        Mirrors ``_drill_path`` semantics exactly:

        - **dict**: ``int`` segment is coerced to ``str`` for the key lookup
          (preserves the original behaviour where dict keys are always strings
          after JSON parsing; list-index intent uses the ``int`` segment type
          for the ``isinstance(current, list)`` branch below).
        - **list**: requires an ``int`` segment; bounds-checked, returns
          ``None`` on out-of-range rather than raising.
        - **anything else**: returns ``None`` immediately.

        Args:
            record: Raw parsed JSON-like value (typically ``dict`` or
                ``list``).

        Returns:
            The value at the path, or ``None`` if any segment cannot be
            navigated (missing key, non-int segment on list, out-of-range
            index, scalar mid-walk).
        """
        current: Any = record
        for seg in self.segments:
            if current is None:
                return None
            if isinstance(current, dict):
                # int seg → coerce to str key (JSON dicts always have str keys;
                # the original _drill_path split on "." and never converted to
                # int, so dict lookups always used the raw string form).
                key = str(seg) if isinstance(seg, int) else seg
                current = current.get(key)
            elif isinstance(current, list) and isinstance(seg, int):
                current = current[seg] if 0 <= seg < len(current) else None
            else:
                return None
        return current
