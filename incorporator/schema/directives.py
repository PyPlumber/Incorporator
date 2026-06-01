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

# Sentinel distinguishing "key absent" from "key present with None value" in apply_rename.
_ABSENT: Any = object()


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
    """Rename directive — moves a key (top-level or nested) to a new name in a record.

    Top-level renames (``Nm("a", "b")``) behave identically to the previous
    single-segment shape.  Nested renames (``Nm("user.email", "contact.email")``)
    drill via ``DataPath`` and may cross parent dicts.  Cross-parent moves
    auto-create the target parent dict via ``DataPath.set(create_parents=True)``.

    Source resolution preserves explicit-None fidelity: ``Nm("a", "b")`` on
    ``{"a": None}`` writes ``b == None``; on ``{}`` it is a silent no-op.

    Attributes:
        old: Source path (top-level key or dotted path).
        new: Destination path (top-level key or dotted path).

    Example::

        Nm("external_id", "id").apply_rename(record)
        Nm("user.email", "contact.email").apply_rename(record)

    """

    old: str
    new: str
    _old_path: DataPath = dc_field(init=False, repr=False, compare=False)
    _new_path: DataPath = dc_field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_old_path", DataPath.parse(self.old))
        object.__setattr__(self, "_new_path", DataPath.parse(self.new))

    def apply_rename(self, record: dict[str, Any]) -> None:
        """Move the value at ``self.old`` to ``self.new`` in *record* in-place.

        When the source key is absent, the record is left untouched.  When the
        source value is present (including explicit None), it is moved to the
        target path; missing target parent dicts are auto-created.

        Args:
            record: Raw record dict to mutate.
        """
        segs = self._old_path.segments
        # Fast path: single-segment top-level rename preserves bit-for-bit
        # behaviour of the pre-Stage-3 implementation including explicit-None.
        if len(segs) == 1:
            seg = segs[0]
            key = str(seg) if isinstance(seg, int) else seg
            value = record.pop(key, _ABSENT) if isinstance(record, dict) else _ABSENT
            if value is _ABSENT:
                return
            self._new_path.set(record, value, create_parents=True)
            return
        # Multi-segment: has() distinguishes absent from explicit-None, then
        # pop removes from the source parent, set writes to the target with
        # auto-create.
        if not self._old_path.has(record):
            return
        value = self._old_path.resolve(record)
        self._old_path.pop(record)
        self._new_path.set(record, value, create_parents=True)

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

    Note::

        Internal-only.  ``Pk`` is synthesized from ``code_attr`` / ``name_attr``
        by ``_normalize_etl_kwargs`` and is not part of the public construction
        API.  Direct instantiation by config code is unsupported:

        - ``code_attr=Pk(...)`` — ``code_attr`` is typed ``str | None``; the
          wrapping ``Pk(code_attr, target="code")`` at line 257 ends up with
          ``Pk.source`` holding a ``Pk`` instance, and ``__post_init__``'s
          ``DataPath.parse(self.source)`` raises on the non-string.
        - ``conv_dict={"x": Pk(...)}`` — falls into the converter dispatcher's
          else branch at ``builder.py:256-261``; ``Pk`` is not callable, so
          ``operation(d.get(key, None))`` raises ``TypeError``, the try/except
          catches it, ``logger.warning`` fires per row, and the key is never
          written.

        Use bare-string ``code_attr=`` / ``name_attr=`` instead.

    Internal example::

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
    rule against the ``nm_tuple`` rename map at config time, applying each
    rename at most once (no chained rewrites: if ``A → B`` and ``B → C`` both
    appear, a ``Pk`` on ``A`` binds to ``B``, not ``C``).

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

    # Rewrite Pk.source through the rename map (first hit only — no chained
    # rewrites: if A→B and B→C both appear in nm_tuple, Pk.source=A becomes
    # B, not C).  This fixes Case A: when code_attr names a field that
    # name_chg renames, inc_code binding must follow the field to its new
    # name before the rename pass runs.
    if pk_list and nm_tuple:
        rename_map = {nm.old: nm.new for nm in nm_tuple}
        pk_list = [Pk(rename_map[pk.source], target=pk.target) if pk.source in rename_map else pk for pk in pk_list]

    return NormalizedKwargs(
        ex_tuple=ex_tuple,
        conv_map=conv_dict or {},
        nm_tuple=nm_tuple,
        pk_tuple=tuple(pk_list),
    )
