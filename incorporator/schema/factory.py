"""Schema-driven instance assembly: Transform, Compile, Instantiate.

Module-level factory functions for the ``incorp()`` pipeline. Each function
receives ``cls`` explicitly so this module stays import-time independent of
``base.py`` — eliminating the circular-import risk.

Dependency direction: ``base.py → schema/factory.py → schema/{builder,router}.py``
(never the reverse).
"""

import logging
import warnings
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Tuple, Type, Union, cast

from pydantic import TypeAdapter

from ..list import IncorporatorList, _deduplicate_extracted
from . import builder as schema_builder
from . import converters, router

if TYPE_CHECKING:
    from ..base import Incorporator
    from ..rejects import RejectEntry

logger = logging.getLogger(__name__)


# JSON-Schema "type" string → Python type.  Mirrors the table at
# ``incorporator/io/formats.py``.  Deliberately omits ``"string"`` —
# coercing values to ``str`` is either a no-op (real strings) or actively
# wrong (would cast numeric / boolean values to strings if a previous
# typeless-format read populated _schema_union with ``"string"``).  See
# ``_expand_conv_dict_with_schema_union`` below.
_JSON_SCHEMA_TYPE_TO_PYTHON: Dict[str, type] = {
    "integer": int,
    "number": float,
    "boolean": bool,
}


def _target_type_from_schema_info(schema_info: Mapping[str, Any]) -> Optional[type]:
    """Pick a Python coercion target from a JSON-Schema dict.

    Handles both flat-schema shapes (``{"type": "integer"}``) and the
    ``anyOf`` shape Pydantic emits for ``Optional[...]`` / ``Union[...]``
    fields (``{"anyOf": [{"type": "integer"}, {"type": "null"}]}``).

    Returns ``None`` for:
      * Plain ``"string"`` entries (see asymmetry note on
        ``_expand_conv_dict_with_schema_union``).
      * Unrecognised types.
      * Schemas with no ``"type"`` or ``"anyOf"`` key.

    The left-most non-null union member wins — mirrors Pydantic's own
    union resolution order.
    """

    def _from_member(member: Mapping[str, Any]) -> Optional[type]:
        mt = member.get("type")
        # Datetime carries ``{"type": "string", "format": "date-time"}``.
        if mt == "string" and member.get("format") == "date-time":
            return datetime
        if isinstance(mt, str):
            return _JSON_SCHEMA_TYPE_TO_PYTHON.get(mt)
        return None

    # Flat schema first.
    if isinstance(schema_info.get("type"), str) or "type" in schema_info:
        flat = _from_member(schema_info)
        if flat is not None:
            return flat
    # anyOf union — pick the first non-null member that maps to a target type.
    any_of = schema_info.get("anyOf")
    if isinstance(any_of, list):
        for member in any_of:
            if not isinstance(member, dict):
                continue
            if member.get("type") == "null":
                continue
            target = _from_member(member)
            if target is not None:
                return target
    return None


def _effective_conv_cache_key(
    conv_dict: Optional[Dict[str, Any]],
    schema_union: Mapping[str, Any],
    declared_field_names: Optional[frozenset[str]],
) -> Tuple[int, int, Optional[frozenset[str]]]:
    """Cheap cache key for the per-class ``_cached_effective_conv`` slot.

    Captures the three inputs to :func:`_expand_conv_dict_with_schema_union`
    that can change between ``incorp()`` calls:

    * ``id(conv_dict)`` — caller-supplied identity.  Long-running daemons
      (chunked ``stream()``, ``fjord()``, Tideweaver) reuse the same
      ``conv_dict`` instance per-tick — cache hits.  Callers passing a
      fresh dict each call will see misses (cache check is O(1)).
    * ``len(schema_union)`` — captures growth.  Every new field bumps
      the union; when stable, the size doesn't change.  Stable steady
      state on a long-running daemon → cache hits.
    * ``declared_field_names`` — stable per class identity.  Included
      defensively so subclass-with-different-fields cases don't collide.

    Returns a tuple to feed an ``==`` comparison against a stored key.
    """
    return (
        id(conv_dict) if conv_dict is not None else 0,
        len(schema_union),
        declared_field_names,
    )


def _expand_conv_dict_with_schema_union(
    conv_dict: Optional[Dict[str, Any]],
    schema_union: Mapping[str, Any],
    declared_field_names: Optional[frozenset[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Backfill ``inc()`` converters for fields the caller didn't name.

    Bridges the gap between ``_schema_union`` (which records the JSON-Schema
    type per field observed across prior incorps) and the ranked-converter
    engine (``incorporator.schema.converters``).  Without this bridge,
    typeless formats — CSV / TSV / PSV (and XML / HTML text nodes) —
    surface every cell as ``str`` and the converter chain never runs
    unless the caller manually writes ``conv_dict={"current_price":
    inc(int), "market_cap": inc(int), ...}`` for every field.  With this
    bridge, a tutorial-2-style round-trip (CoinGecko JSON → CSV → re-incorp)
    preserves types automatically.

    Three skip rules:

    1. **User wins.**  Fields already in ``conv_dict`` are left alone —
       caller's explicit choice always trumps.
    2. **Base-class fields stay with Pydantic.**  Fields declared on the
       calling class (``cls.model_fields`` — includes the inherited
       ``inc_code`` / ``inc_name`` / ``last_rcd`` from ``Incorporator``)
       get coerced by Pydantic itself via their declared annotations
       (``datetime``, ``str``, etc.).  Synthesising an ``inc()`` here
       would return ``default=None`` for garbage inputs and then fail
       Pydantic's strict-typed validation; skipping leaves Pydantic in
       charge of the decision.
    3. **Asymmetry on string.**  Only coerce TOWARDS richer types
       (``int`` / ``float`` / ``bool`` / ``datetime``).  Skip plain
       ``"string"`` entries — coercing values TO ``str`` is either a
       no-op or actively wrong (would cast numeric values back to
       strings when a typeless first-read populated ``_schema_union``
       with ``"string"``).

    Args:
        conv_dict: The user's explicit converter mapping (may be ``None``).
        schema_union: ``cls._schema_union`` — JSON-Schema-shaped dict
            of ``{field_name: {"type": ..., "format": ...}}`` entries.
        declared_field_names: Names already declared on the class
            hierarchy.  Pass ``frozenset(cls.model_fields.keys())`` to
            cede those fields to Pydantic's own coercion.

    Returns:
        An expanded conv_dict (or ``None`` if both inputs are empty).
    """
    if not schema_union:
        return conv_dict
    effective = dict(conv_dict or {})
    declared = declared_field_names or frozenset()
    for field, schema_info in schema_union.items():
        if field in effective or field in declared or not isinstance(schema_info, dict):
            continue
        target = _target_type_from_schema_info(schema_info)
        if target is not None:
            effective[field] = converters.inc(target)
    return effective or None


async def child_incorp(
    cls: "Type[Incorporator]",
    inc_parent: Any,
    **kwargs: Any,
) -> Union["Incorporator", "IncorporatorList[Any]"]:
    """Drive a parent-to-child ``incorp()`` call for deeply nested RESTful graphs.

    Resolves ``inc_child`` paths via BFS drill-down on the parent dataset,
    deduplicates the extracted URLs / IDs, builds the correct request shape
    (GET ``{}``-template or declarative POST / PUT / PATCH), then delegates
    to ``cls.incorp(**kwargs)``.

    Args:
        cls: The child :class:`Incorporator` subclass to instantiate.
        inc_parent: The parent dataset (list of instances or single instance).
        **kwargs: Forwarded to ``cls.incorp()`` — ``inc_child``, ``inc_url``,
            ``http_method``, ``json_payload``, ``form_payload``, etc.

    Returns:
        A single instance or an :class:`IncorporatorList` of child instances.
    """
    child_path = kwargs.get("inc_child") or getattr(inc_parent, "inc_child_path", None)
    if not child_path and inc_parent:
        parent_class = inc_parent[0].__class__ if isinstance(inc_parent, list) else inc_parent.__class__
        child_path = getattr(parent_class, "inc_child", None)

    extracted_data = (
        router.extract_parent_data(inc_parent, child_path)
        if child_path
        else (inc_parent if isinstance(inc_parent, list) else [inc_parent])
    )

    # Deduplicate paths to prevent duplicate HTTP requests for identical parent IDs.
    if extracted_data and child_path:
        extracted_data = _deduplicate_extracted(extracted_data)

    raw_method = kwargs.pop("method", kwargs.pop("http_method", "GET"))
    kwargs["http_method"] = raw_method.upper() if isinstance(raw_method, str) else "GET"

    inc_url = kwargs.get("inc_url")
    source_urls = [inc_url] if isinstance(inc_url, str) else (inc_url or [])

    if extracted_data:
        kwargs = router.resolve_declarative_routing(cls.__name__, extracted_data, source_urls, **kwargs)

    return await cls.incorp(**kwargs)


def build_instances(
    cls: "Type[Incorporator]",
    parsed_data: List[Any],
    rejects: List["RejectEntry"],
    is_single: bool,
    target_class: Optional["Type[Incorporator]"] = None,
    inc_code: Optional[str] = None,
    inc_name: Optional[str] = None,
    excl_lst: Optional[List[str]] = None,
    conv_dict: Optional[Dict[str, Any]] = None,
    name_chg: Optional[List[Tuple[str, str]]] = None,
) -> Union["Incorporator", "IncorporatorList[Any]"]:
    """Transform, compile, and instantiate the parsed payload into Incorporator objects.

    Three sequential phases:

    1. **Transform** — applies ``conv_dict``, ``excl_lst``, ``name_chg``, and
       columnar ``calc`` / ``calc_all`` operations via
       :func:`schema_builder.apply_etl_transformations`.
    2. **Compile** — resolves or builds the Pydantic model class via
       :func:`schema_builder.infer_dynamic_schema`.
    3. **Instantiate** — batch-validates rows with ``model_validate``
       (1 000 rows per batch for predictable memory) and wraps the result in
       an :class:`IncorporatorList`.

    Args:
        cls: The calling :class:`Incorporator` subclass.
        parsed_data: Raw dicts from the format handler.
        rejects: Structured failure entries accumulated upstream
            (surfaced as a ``UserWarning`` and forwarded to
            :class:`IncorporatorList`).  See
            :class:`incorporator.RejectEntry`.
        is_single: When ``True`` and ``parsed_data`` has exactly one item,
            returns a single instance rather than a list.
        target_class: Override the compiled model class (e.g. for
            ``refresh()``).
        inc_code: Field name used as the ``IncorporatorList`` primary key.
        inc_name: Field name used as the display name.
        excl_lst: Field names to exclude before instantiation.
        conv_dict: Per-field converter mapping.
        name_chg: ``[(old_name, new_name), ...]`` field renames.

    Returns:
        A single :class:`Incorporator` instance or an
        :class:`IncorporatorList`.
    """
    if rejects:
        warnings.warn(
            f"Incorporator partial data returned: {len(rejects)} source(s) failed.",
            stacklevel=2,
        )

    if not parsed_data:
        # Generate a safe empty class if an API returns 200 OK but 0 records
        EmptyClass = cast(
            "Type[Incorporator]",
            schema_builder.infer_dynamic_schema("DynamicModel", [{}], cls),
        )
        return IncorporatorList(EmptyClass, [], rejects=rejects)

    if is_single and len(parsed_data) == 1:
        parsed_data = parsed_data[0]

    # Auto-coerce based on previously-observed types.  ``_schema_union``
    # carries the JSON-Schema type per field from prior incorps; the helper
    # synthesises ``inc()`` converters for any field the caller didn't name
    # in ``conv_dict``.  This is what makes a CSV / TSV / PSV round-trip
    # preserve types automatically after a typed source (JSON / NDJSON /
    # SQLite / Parquet / Avro) has populated ``_schema_union``.  Caller
    # overrides always win, and fields declared on the base class (e.g.
    # ``last_rcd: datetime`` on ``Incorporator``) are left to Pydantic.
    #
    # A-F-4: cache the expansion on the class.  Long-running daemons
    # (chunked ``stream()``, ``fjord()``, Tideweaver) reuse the same
    # ``conv_dict`` and the schema union stabilises after a few waves,
    # so the cache hits every tick after warm-up.  Invalidates correctly
    # when (a) schema_union grows (new field appears) or (b) caller
    # passes a different ``conv_dict`` instance.  See
    # ``_effective_conv_cache_key`` above for the key shape.
    schema_union = getattr(cls, "_schema_union", {})
    declared_field_names = frozenset(cls.model_fields.keys())
    cache_key = _effective_conv_cache_key(conv_dict, schema_union, declared_field_names)
    cached = getattr(cls, "_cached_effective_conv", None)
    if cached is not None and cached[0] == cache_key:
        effective_conv = cached[1]
    else:
        effective_conv = _expand_conv_dict_with_schema_union(
            conv_dict,
            schema_union,
            declared_field_names=declared_field_names,
        )
        # setattr keeps mypy strict happy — ``_cached_effective_conv`` is a
        # dynamic cache attribute, not a declared class field.
        setattr(cls, "_cached_effective_conv", (cache_key, effective_conv))  # noqa: B010

    transformed_data = schema_builder.apply_etl_transformations(
        parsed_data=parsed_data,
        code_attr=inc_code,
        name_attr=inc_name,
        excl_lst=excl_lst,
        conv_dict=effective_conv,
        name_chg=name_chg,
    )

    ActualClass = target_class or cast(
        "Type[Incorporator]",
        schema_builder.infer_dynamic_schema("DynamicModel", transformed_data, cls),
    )

    if isinstance(transformed_data, list):
        # Populate the superset schema from raw dicts before Pydantic absorbs extra keys.
        # Fork a per-class dict on first write so subclasses don't share the base
        # Incorporator's empty default (see ``test_schema_union_sibling_class_isolation``).
        cls._ensure_schema_union()
        # Writes only on first-seen keys — O(1) miss per key, zero writes after stabilization.
        # ``ActualClass.model_json_schema()`` is stable for the lifetime of the
        # SCHEMA_REGISTRY-cached dynamic class; cache its ``properties`` dict
        # on the class itself so long-running daemons don't pay the rebuild
        # cost on every wave.
        declared = getattr(ActualClass, "_cached_json_properties", None)
        if declared is None:
            declared = ActualClass.model_json_schema().get("properties", {})
            # setattr keeps mypy strict happy — ``_cached_json_properties`` is a
            # dynamic cache attribute, not a declared class field.
            setattr(ActualClass, "_cached_json_properties", declared)  # noqa: B010
        for item in transformed_data:
            for k in item:
                if k not in cls._schema_union:
                    cls._schema_union[k] = declared.get(k, {"type": "string"})

        # A-F-4: batch-validate the full payload through a per-class-cached
        # ``TypeAdapter(List[Cls])``.  A-F-3 (see
        # ``tests/benchmarks/test_validate_batch_vs_per_row.py``) measured
        # this at 1.3-2.0× faster than the per-row ``model_validate`` loop
        # across realistic shapes (10k × 20 mixed: 1.34× worst-case).  The
        # cache mirrors ``_cached_json_properties`` above — built once per
        # dynamic-class lifetime so the schema-binding work doesn't repeat
        # on every ``incorp()`` call.
        #
        # Trade-off vs. the old per-row 1000-chunk loop: peak memory per
        # ``incorp()`` call is now O(N) instead of O(1000).  High-row-count
        # callers who need O(chunk_size) memory should use chunking-mode
        # ``stream(stateful_polling=False)`` which preserves the bound at
        # the stream layer regardless of ``incorp()``'s internal shape.
        # Error semantics changed too: validation errors now accumulate
        # across all rows in a single ``ValidationError`` rather than
        # raising on the first bad row.
        adapter = getattr(ActualClass, "_cached_type_adapter", None)
        if adapter is None:
            adapter = TypeAdapter(List[ActualClass])  # type: ignore[valid-type]
            # setattr keeps mypy strict happy — ``_cached_type_adapter`` is a
            # dynamic cache attribute, not a declared class field.
            setattr(ActualClass, "_cached_type_adapter", adapter)  # noqa: B010
        instances: List[Any] = adapter.validate_python(transformed_data)
        return IncorporatorList(ActualClass, instances, rejects=rejects)

    return ActualClass(**transformed_data)
