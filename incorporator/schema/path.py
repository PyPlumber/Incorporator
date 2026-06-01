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
        if len(self.segments) == 1:
            seg = self.segments[0]
            if current is None:
                return None
            if isinstance(current, dict):
                key = str(seg) if isinstance(seg, int) else seg
                return current.get(key)
            if isinstance(current, list) and isinstance(seg, int):
                return current[seg] if 0 <= seg < len(current) else None
            return None
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

    def pop(self, record: Any) -> None:
        """Remove the leaf at this path from *record* in-place.

        Mirrors ``resolve()`` resilience: missing keys, missing intermediates,
        and non-dict nodes at any point are silent no-ops.

        Args:
            record: Raw parsed JSON-like value to mutate (typically ``dict``).
        """
        if not self.segments:
            return
        if len(self.segments) == 1:
            seg = self.segments[0]
            if isinstance(record, dict):
                key = str(seg) if isinstance(seg, int) else seg
                record.pop(key, None)
            return
        parent: Any = record
        for seg in self.segments[:-1]:
            if parent is None:
                return
            if isinstance(parent, dict):
                key = str(seg) if isinstance(seg, int) else seg
                parent = parent.get(key)
            elif isinstance(parent, list) and isinstance(seg, int):
                parent = parent[seg] if 0 <= seg < len(parent) else None
            else:
                return
        if isinstance(parent, dict):
            leaf = self.segments[-1]
            key = str(leaf) if isinstance(leaf, int) else leaf
            parent.pop(key, None)

    def set(self, record: Any, value: Any, *, create_parents: bool = False) -> None:
        """Assign ``value`` at this path in ``record`` in place.

        Top-level segment: ``record[seg] = value`` when ``record`` is a dict.
        Nested: walks to the parent and assigns when the parent is a dict.
        With ``create_parents=False`` (default), missing intermediate dicts are a
        silent no-op.  With ``create_parents=True``, missing intermediate dicts
        are created as empty dicts on the way down — useful for cross-parent
        moves in Nm.apply_rename.  Integer segments and non-dict intermediates
        are always a silent no-op (auto-extending lists is not safe).

        Args:
            record: Raw parsed JSON-like value to mutate (typically ``dict``).
            value: Value to assign at the leaf.
            create_parents: When True, auto-create missing intermediate dicts
                (str-keyed segments only; int segments refuse silently).
        """
        if not self.segments:
            return
        if len(self.segments) == 1:
            if isinstance(record, dict):
                seg = self.segments[0]
                key = str(seg) if isinstance(seg, int) else seg
                record[key] = value
            return
        current: Any = record
        for seg in self.segments[:-1]:
            if current is None:
                return
            if isinstance(current, dict):
                key = str(seg) if isinstance(seg, int) else seg
                nxt = current.get(key)
                if nxt is None:
                    if create_parents and isinstance(seg, str):
                        nxt = {}
                        current[key] = nxt
                    else:
                        return
                elif not isinstance(nxt, (dict, list)):
                    # intermediate exists but isn't a dict/list — refuse silently
                    return
                current = nxt
            elif isinstance(current, list) and isinstance(seg, int):
                current = current[seg] if 0 <= seg < len(current) else None
            else:
                return
        if isinstance(current, dict):
            last = self.segments[-1]
            last_key = str(last) if isinstance(last, int) else last
            current[last_key] = value

    def has(self, record: Any) -> bool:
        """Return True iff this path resolves to a present key in ``record``.

        Distinct from ``resolve``: ``resolve`` returns None for both "key absent"
        and "key present with None value".  ``has`` returns True for the
        present-with-None case and False for the absent case.  Used by callers
        that need to distinguish missing data from explicit None (e.g.,
        Nm.apply_rename's silent no-op contract on missing source keys).

        Walks the same parent chain as ``pop`` and ``set`` but reads — never
        mutates.  Non-dict intermediates and missing intermediates return False.

        Args:
            record: Raw parsed JSON-like value to inspect (typically ``dict``).

        Returns:
            True if the path is present (even if the value is None); False
            if any segment along the path is absent or non-traversable.
        """
        if not self.segments:
            return False
        if len(self.segments) == 1:
            if isinstance(record, dict):
                seg = self.segments[0]
                key = str(seg) if isinstance(seg, int) else seg
                return key in record
            return False
        current: Any = record
        for seg in self.segments[:-1]:
            if current is None:
                return False
            if isinstance(current, dict):
                key = str(seg) if isinstance(seg, int) else seg
                current = current.get(key)
            elif isinstance(current, list) and isinstance(seg, int):
                current = current[seg] if 0 <= seg < len(current) else None
            else:
                return False
        if isinstance(current, dict):
            last = self.segments[-1]
            last_key = str(last) if isinstance(last, int) else last
            return last_key in current
        return False
