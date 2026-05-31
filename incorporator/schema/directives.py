"""Typed frozen-dataclass wrappers for DATA-SHAPE pipeline directives.

Three wrappers — ``Ex``, ``Nm``, ``Pk`` — carry the DROP / RENAME /
PK-BIND intent that today travels as bare strings and tuples alongside
``conv_dict``.  They are constructed once at ``incorp()`` callsite and
replayed safely under ``_incorp_kwargs`` reference-sharing because they
are frozen and hashable.

These classes sit dormant until Chain 2 wires the normalizer and
dispatcher.  No existing API surface is altered here.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any, Literal, Protocol

from incorporator.schema.path import DataPath


class FieldDirective(Protocol):
    """Structural marker for drop / rename / PK-bind directives.

    Declaring a ``Protocol`` rather than an ABC keeps the three wrappers
    independent (no shared base, no MRO cost).  Later chains use this as
    a type-hint bound in dispatcher signatures.
    """


@dataclass(frozen=True, slots=True)
class Ex:
    """Drop directive — removes a field (or nested leaf) from a record.

    Constructed once from a dotted path string; the ``DataPath`` is cached
    in ``_path`` so the per-row ``apply_drop`` call only walks segments,
    never re-splits the string.

    Attributes:
        field: Dot-notation path to the field to drop (e.g. ``"a.b.c"``).

    Example::

        Ex("status").apply_drop(record)          # drops top-level key
        Ex("meta.internal").apply_drop(record)   # drops nested leaf

    """

    field: str
    _path: DataPath = dc_field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_path", DataPath.parse(self.field))

    def apply_drop(self, record: dict[str, Any]) -> None:
        """Remove the field at ``self.field`` from *record* in-place.

        Missing keys and missing intermediates are silent no-ops.

        Args:
            record: Raw record dict to mutate.
        """
        self._path.pop(record)

    def __repr__(self) -> str:
        return f"Ex({self.field!r})"


@dataclass(frozen=True, slots=True)
class Nm:
    """Rename directive — moves a top-level key to a new name in a record.

    Semantics mirror ``builder.py:340-345`` exactly: when both ``old`` and
    ``new`` already exist in the record, ``new`` is clobbered with ``old``'s
    value and ``old`` is removed.  Missing ``old`` is a silent no-op.

    Attributes:
        old: Source key name.
        new: Destination key name.

    Example::

        Nm("external_id", "id").apply_rename(record)

    """

    old: str
    new: str
    _old_path: DataPath = dc_field(init=False, repr=False, compare=False)
    _new_key: str = dc_field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_old_path", DataPath.parse(self.old))
        object.__setattr__(self, "_new_key", self.new)

    def apply_rename(self, record: dict[str, Any]) -> None:
        """Rename ``self.old`` to ``self.new`` in *record* in-place.

        When ``self.old`` is absent the record is left untouched.

        Args:
            record: Raw record dict to mutate.
        """
        if self.old in record:
            record[self._new_key] = record.pop(self.old)

    def __repr__(self) -> str:
        return f"Nm({self.old!r}, {self.new!r})"


@dataclass(frozen=True, slots=True)
class Pk:
    """PK-bind directive — resolves a source path and writes ``inc_code`` / ``inc_name``.

    Mirrors the null-skip at ``builder.py:303``: when the resolved value is
    ``None`` the target key is NOT written, preserving any prior value.

    Attributes:
        source: Dot-notation path to the source field.
        target: Either ``"code"`` (writes ``inc_code``) or ``"name"`` (writes
            ``inc_name``).

    Example::

        Pk("id", target="code").apply_bind(record)
        Pk("league.name", target="name").apply_bind(record)

    """

    source: str
    target: Literal["code", "name"]
    _path: DataPath = dc_field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_path", DataPath.parse(self.source))

    def apply_bind(self, record: dict[str, Any]) -> None:
        """Resolve ``self.source`` and write it to ``inc_code`` or ``inc_name``.

        When the resolved value is ``None`` the target key is not touched.

        Args:
            record: Raw record dict to mutate.
        """
        val = self._path.resolve(record)
        if val is None:
            return
        dest = "inc_code" if self.target == "code" else "inc_name"
        record[dest] = val

    def __repr__(self) -> str:
        return f"Pk({self.source!r}, target={self.target!r})"
