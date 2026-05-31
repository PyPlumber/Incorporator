"""Typed frozen-dataclass wrappers for DATA-SHAPE pipeline directives.

Three wrappers — ``Ex``, ``Nm``, ``Pk`` — carry the DROP / RENAME /
PK-BIND intent that today travels as bare strings and tuples alongside
``conv_dict``.  They are constructed once at ``incorp()`` callsite and
replayed safely under ``_incorp_kwargs`` reference-sharing because they
are frozen and hashable.

``NormalizedKwargs`` is the canonical wrapped container produced by
``_normalize_etl_kwargs``.  It is stored under ``_incorp_kwargs`` and
replayed by ``refresh()`` via pass-by-reference.
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

    def apply_rename(self, record: dict[str, Any]) -> None:
        """Rename ``self.old`` to ``self.new`` in *record* in-place.

        When ``self.old`` is absent the record is left untouched.

        Args:
            record: Raw record dict to mutate.
        """
        if self.old in record:
            record[self.new] = record.pop(self.old)

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


@dataclass(frozen=True, slots=True)
class NormalizedKwargs:
    """Canonical wrapped form of the DATA-SHAPE pipeline parameters.

    Frozen because it is stored under ``_incorp_kwargs`` and replayed by
    ``refresh()`` via pass-by-reference.  The contained ``conv_map`` is a
    regular dict (mutable internally) — the container itself is frozen, not
    its dict value.

    Attributes:
        ex_tuple: Drop directives derived from ``excl_lst``.
        conv_map: Per-field converter mapping (pass-through from
            ``conv_dict``).
        nm_tuple: Rename directives derived from ``name_chg``.
        pk_tuple: PK-bind directives derived from ``code_attr`` /
            ``name_attr``, with sources already rewritten through the
            first-hit rename map.
    """

    ex_tuple: tuple[Ex, ...]
    conv_map: dict[str, Any]
    nm_tuple: tuple[Nm, ...]
    pk_tuple: tuple[Pk, ...]


def _normalize_etl_kwargs(
    *,
    excl_lst: list[str] | tuple[Ex, ...] | None,
    conv_dict: dict[str, Any] | None,
    name_chg: list[tuple[str, str]] | tuple[Nm, ...] | None,
    code_attr: str | None,
    name_attr: str | None,
) -> NormalizedKwargs:
    """Convert bare-shape DATA-SHAPE kwargs into a ``NormalizedKwargs`` container.

    Idempotent: re-normalizing an already-normalized input (i.e., passing
    tuples of ``Ex`` / ``Nm`` / ``Pk`` objects) yields an equivalent
    container.  The Pk-source rewrite (Case A fix) applies the first-hit
    rule against the ``nm_tuple`` rename map at config time, mirroring
    ``_splice_pk_binding``'s non-chained behaviour.

    Args:
        excl_lst: Field names to drop, as bare strings or already-wrapped
            ``Ex`` instances (mixed sequences are accepted).
        conv_dict: Per-field converter mapping; passed through unchanged.
        name_chg: Rename pairs as ``(old, new)`` 2-tuples or already-wrapped
            ``Nm`` instances.
        code_attr: Source field name to alias as ``inc_code``.  Skipped when
            ``conv_dict`` already contains an explicit ``"inc_code"`` entry.
        name_attr: Source field name to alias as ``inc_name``.  Skipped when
            ``conv_dict`` already contains an explicit ``"inc_name"`` entry.

    Returns:
        A frozen ``NormalizedKwargs`` container ready for storage and replay.
    """
    # ex_tuple: bare str → Ex(field); existing Ex instances pass through.
    ex_tuple: tuple[Ex, ...]
    if excl_lst:
        ex_tuple = tuple(item if isinstance(item, Ex) else Ex(item) for item in excl_lst)
    else:
        ex_tuple = ()

    # nm_tuple: 2-tuple → Nm(old, new); existing Nm instances pass through.
    nm_tuple: tuple[Nm, ...]
    if name_chg:
        nm_tuple = tuple(item if isinstance(item, Nm) else Nm(item[0], item[1]) for item in name_chg)
    else:
        nm_tuple = ()

    # pk_tuple: synthesise from code_attr / name_attr; user conv_dict wins.
    user_owns_code = bool(conv_dict and "inc_code" in conv_dict)
    user_owns_name = bool(conv_dict and "inc_name" in conv_dict)
    pk_list: list[Pk] = []
    if code_attr and not user_owns_code:
        pk_list.append(Pk(code_attr, target="code"))
    if name_attr and not user_owns_name:
        pk_list.append(Pk(name_attr, target="name"))

    # Rewrite Pk.source through the rename map (first hit only — mirrors
    # _splice_pk_binding's non-chained behaviour).  This fixes Case A: when
    # code_attr names a field that name_chg renames, inc_code binding must
    # follow the field to its new name before the rename pass runs.
    if pk_list and nm_tuple:
        rename_map = {nm.old: nm.new for nm in nm_tuple}
        pk_list = [Pk(rename_map[pk.source], target=pk.target) if pk.source in rename_map else pk for pk in pk_list]

    return NormalizedKwargs(
        ex_tuple=ex_tuple,
        conv_map=conv_dict or {},
        nm_tuple=nm_tuple,
        pk_tuple=tuple(pk_list),
    )
