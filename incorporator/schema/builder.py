"""Dynamic Pydantic model generation and declarative ETL engine.

Provides two public entry points: :func:`infer_dynamic_schema` (builds a
Pydantic V2 model class from raw data samples) and
:func:`apply_etl_transformations` (applies ``excl_lst``, ``conv_dict``, and
``name_chg`` passes in columnar order).  Both are called exclusively from
:mod:`incorporator.schema.factory`.
"""

from __future__ import annotations

import keyword
import logging
import re
import weakref
from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, create_model

from ..exceptions import IncorporatorSchemaError
from .converters import CalcAllOp, CalcOp, is_garbage_value
from .directives import NormalizedKwargs, _normalize_etl_kwargs
from .path import DataPath

logger = logging.getLogger(__name__)

# LRU cache keyed by (model_name, frozenset(field_types), id(base_class)).
# OrderedDict + move_to_end on hit gives O(1) LRU eviction — older entries
# fall off the front while hot schemas stay at the end.
SCHEMA_REGISTRY: OrderedDict[tuple[str, frozenset[Any], int], type[BaseModel]] = OrderedDict()
MAX_REGISTRY_SIZE = 1000  # Hard boundary to prevent OOM leaks

PYDANTIC_RESERVED = {
    "model_config",
    "model_fields",
    "model_computed_fields",
    "model_extra",
    "model_fields_set",
    "model_dump",
    "model_dump_json",
    "model_validate",
    "model_validate_json",
    "fields",
    "copy",
    "dict",
    "json",
}

# Mutable-container ClassVars on Incorporator that need per-subclass isolation.
# Each entry: (attr_name, factory(seed) -> fresh instance).  ``seed`` is the
# base-class value (walked via MRO with getattr) so dict-typed entries can
# inherit any seeds populated on the base before forking off a shallow copy
# for the dynamic subclass.  WeakValueDictionary always starts empty per
# subclass — fjord_snapshot owns the strong references during outflow.
_MISSING: Any = object()
_PER_SUBCLASS_CONTAINERS: tuple[tuple[str, Callable[[Any], Any]], ...] = (
    ("inc_dict", lambda _seed: weakref.WeakValueDictionary[Any, Any]()),
    ("_schema_union", lambda seed: dict(seed) if isinstance(seed, dict) else {}),
    ("_incorp_kwargs", lambda seed: dict(seed) if isinstance(seed, dict) else {}),
)

# Pre-compile regex to save C-level CPU cycles
_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]")


def sanitize_json_key(key: str) -> str:
    """Convert a raw JSON key to a safe Python identifier.

    Replaces non-alphanumeric characters with ``_``, prefixes digit-leading
    names, appends ``_`` to Python keywords, and prefixes Pydantic reserved
    names with ``safe_`` to prevent ``model_dump`` and friends from colliding.
    """
    clean_key = _SANITIZE_RE.sub("_", str(key))

    if not clean_key:
        clean_key = "empty_key"
    if clean_key[0].isdigit():
        clean_key = f"_{clean_key}"
    if keyword.iskeyword(clean_key):
        clean_key = f"{clean_key}_"
    if clean_key in PYDANTIC_RESERVED:
        clean_key = f"safe_{clean_key}"

    return clean_key


def apply_etl_transformations(
    parsed_data: dict[str, Any] | list[dict[str, Any]],
    code_attr: str | None = None,
    name_attr: str | None = None,
    excl_lst: list[str] | None = None,
    conv_dict: dict[str, Any] | None = None,
    name_chg: list[tuple[str, str]] | None = None,
    *,
    normalized: NormalizedKwargs | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Apply excl_lst, conv_dict, name_chg, and PK-binding transforms in-place.

    Processes ``parsed_data`` through four ordered passes:

    1. **Ex (drop)** — remove fields named in ``excl_lst`` / ``ex_tuple``.
    2. **conv_dict (Op family)** — columnar converter dispatch (op-outer /
       row-inner) for ``CalcOp``, ``CalcAllOp``, whole-row ops, and plain
       callables.
    3. **Nm (rename)** — apply ``name_chg`` / ``nm_tuple`` renames.
    4. **Pk (PK-bind)** — resolve source paths and write ``inc_code`` /
       ``inc_name``.  Runs LAST so renames applied in pass 3 are visible
       (fixes Case A and Case B bugs where a pre-rename source or a
       rename-created target was not yet present when PK binding ran).

    Args:
        parsed_data: A single record dict or a list of record dicts from the
            format handler.
        code_attr: Source field name to alias as ``inc_code``.
        name_attr: Source field name to alias as ``inc_name``.
        excl_lst: Field names to drop before Pydantic compilation.
        conv_dict: Mapping of field name → converter (``inc``, ``calc``,
            ``calc_all``, ``link_to``, ``pluck``, etc.).
        name_chg: ``[(old_name, new_name), ...]`` rename pairs applied before
            PK binding.
        normalized: Optional pre-built ``NormalizedKwargs`` container from
            ``_normalize_etl_kwargs``.  When ``None`` the function synthesises
            a transient container from the bare kwargs — this preserves
            backward-compat for direct test calls that don't pass
            ``normalized``.  The ``conv_map`` field of the container is
            intentionally NOT used for Op dispatch: ``factory.build_instances``
            expands the user ``conv_dict`` through ``_schema_union`` before
            calling here, and that expanded ``effective_conv`` must not be
            clobbered by the raw user mapping.

    Returns:
        The same structure as ``parsed_data`` (dict or list), mutated in
        place.  Callers may discard the return value.
    """
    if normalized is None:
        normalized = _normalize_etl_kwargs(
            excl_lst=excl_lst,
            conv_dict=conv_dict,
            name_chg=name_chg,
            code_attr=code_attr,
            name_attr=name_attr,
        )

    items = parsed_data if isinstance(parsed_data, list) else [parsed_data]
    if not items:
        return parsed_data

    dict_items: list[dict[str, Any]] = [i for i in items if isinstance(i, dict)]
    if not dict_items:
        return items if isinstance(parsed_data, list) else items[0]

    # Pass 1 — Ex (drop): rows outer, keys inner keeps each dict warm in CPU
    # cache, avoiding thrashing on large datasets.
    if normalized.ex_tuple:
        for d in dict_items:
            for ex in normalized.ex_tuple:
                ex.apply_drop(d)

    if conv_dict:
        # Pass 2 — conv_dict (Op family): op-outer / row-inner so per-key
        # metadata is resolved once, then rows iterated inside.  Insertion
        # order is preserved so calc A → calc B sequencing (where B reads
        # A's output) keeps working unchanged.
        for key, operation in conv_dict.items():
            if isinstance(operation, CalcOp):
                func = operation.func
                default = operation.default
                target_type = operation.target_type if callable(operation.target_type) else None
                raw_inputs: list[DataPath] = operation.input_list if operation.input_list else [DataPath.parse(key)]
                for d in dict_items:
                    args = [dep.resolve(d) for dep in raw_inputs]
                    # Align with inc()'s null-handling contract: when EVERY
                    # input value is garbage (None/""/n-a/null/unknown/nan/
                    # undefined), skip the user-supplied func and silently
                    # fall back to default.  Saves one Python exception raise
                    # + one logger.warning call per garbage row — the per-
                    # row pre-check is ~50ns, the exception path it replaces
                    # is ~30µs.  Material on garbage-heavy datasets.  See H3 reshape's efficiency analysis.
                    if all(is_garbage_value(a) for a in args):
                        val = default
                    else:
                        try:
                            val = func(*args)
                        except TypeError:
                            # TypeError sources here: (1) lru_cache rejecting an unhashable
                            # arg (list/dict/etc); (2) the func body itself raising TypeError
                            # on the args.  Both route through the same unwrap-and-retry path
                            # — case (1) succeeds on the bare callable, case (2) re-raises
                            # and falls through to the inner Exception handler that logs +
                            # uses default.  Same observable behaviour as any other func-
                            # body failure.
                            raw_func = getattr(func, "__wrapped__", func)
                            try:
                                val = raw_func(*args)
                            except Exception as e:
                                logger.warning("calc failed for key '%s' with args %s: %s", key, args, e)
                                val = default
                        except Exception as e:
                            logger.warning("calc failed for key '%s' with args %s: %s", key, args, e)
                            val = default

                    if target_type is not None:
                        try:
                            val = target_type(val)
                        except Exception as e:
                            logger.warning("calc type coercion failed for key '%s' value %r: %s", key, val, e)
                            val = default
                    d[key] = val

            elif isinstance(operation, CalcAllOp):
                func = operation.func
                default = operation.default
                target_type = operation.target_type if callable(operation.target_type) else None
                all_inputs: list[DataPath] = operation.input_list if operation.input_list else [DataPath.parse(key)]
                col_args = [[dep.resolve(d) for d in dict_items] for dep in all_inputs]
                # Symmetric to CalcOp's pre-check, applied across the full
                # column matrix: if every cell of every input column is
                # garbage, skip the func call and default every output row.
                if all(is_garbage_value(v) for col in col_args for v in col):
                    results = [default] * len(dict_items)
                else:
                    try:
                        results = func(*col_args)
                    except Exception as e:
                        logger.warning("calc_all failed for key '%s': %s", key, e)
                        results = [default] * len(dict_items)

                for idx, d in enumerate(dict_items):
                    try:
                        val = results[idx]
                    except IndexError:
                        logger.warning(
                            "calc_all returned fewer results than expected for key '%s' (needed index %d)",
                            key,
                            idx,
                        )
                        val = default

                    if target_type is not None:
                        try:
                            val = target_type(val)
                        except Exception as e:
                            logger.warning("calc_all type coercion failed for key '%s' value %r: %s", key, val, e)
                            val = default
                    d[key] = val

            elif getattr(operation, "whole_row", False):
                for d in dict_items:
                    try:
                        d[key] = operation(d)
                    except Exception as e:
                        logger.warning("conv_dict failed on key '%s': %s", key, e)

            else:
                for d in dict_items:
                    try:
                        d[key] = operation(d.get(key, None))
                    except Exception as e:
                        logger.warning("conv_dict failed on key '%s': %s", key, e)

    # Pass 3 — Nm (rename): rows outer, directives inner.
    if normalized.nm_tuple:
        for d in dict_items:
            for nm in normalized.nm_tuple:
                nm.apply_rename(d)

    # Pass 4 — Pk (PK-bind): runs LAST so pass-3 renames are visible.
    # This fixes Case A (source renamed away) and Case B (rename creates target).
    if normalized.pk_tuple:
        for d in dict_items:
            for pk in normalized.pk_tuple:
                pk.apply_bind(d)

    return items if isinstance(parsed_data, list) else items[0]


def infer_dynamic_schema(
    model_name: str, data: dict[str, Any] | list[dict[str, Any]], base_class: type[BaseModel]
) -> type[BaseModel]:
    """Recursively build a Pydantic V2 model class from a raw data sample.

    Samples up to 50 records from ``data`` (Python's native list slicing
    won't raise on shorter inputs), merges every observed key into a
    composite ``sample_dict``, and recursively invokes itself on nested
    dicts and lists so deeply nested schemas are fully typed.

    Args:
        model_name: Name to assign the generated class (e.g. ``"DynamicModel"``).
        data: A single record (``dict``) or a list of records. Lists drive
            the field-union behaviour: any key that appears in ANY record
            becomes part of the model.
        base_class: The :class:`Incorporator` subclass that the new model
            should inherit from. Determines the registry the resulting
            instances will live in.

    Returns:
        A new :class:`pydantic.BaseModel` subclass created via
        :func:`pydantic.create_model`, inheriting from ``base_class``.

        Tolerance contract — every inferred field is wrapped in
        ``... | None`` so missing keys never trigger ``ValidationError``,
        and all numeric fields use ``int | float`` so APIs that
        sometimes return ``42`` and sometimes ``42.0`` for the same field
        validate cleanly.

        The class is cached in :data:`SCHEMA_REGISTRY` keyed by
        ``(model_name, frozenset((k, type(v).__name__) for k, v in sample_dict.items()), id(base_class))``
        so re-invoking with the same shape returns the cached class
        rather than rebuilding.
    """

    sample_dict: dict[str, Any] = {}

    # Stratified sampling: evenly spaced indices up to 100 records so rare
    # field types that appear later in large datasets are more likely to be
    # discovered.  On short lists (< 100) this degenerates to `data[:n]`.
    if isinstance(data, list):
        n = len(data)
        if n <= 100:
            items_to_sample = data
        else:
            step = n // 100
            items_to_sample = data[::step][:100]
    else:
        items_to_sample = [data]

    for item in items_to_sample:
        if isinstance(item, dict):
            for k, v in item.items():
                current_val = sample_dict.get(k)

                if current_val is None or (isinstance(current_val, list | dict) and not current_val):
                    # Shallow copy prevents mutating the original data via reference.
                    if isinstance(v, list):
                        sample_dict[k] = list(v)
                    elif isinstance(v, dict):
                        sample_dict[k] = dict(v)
                    else:
                        sample_dict[k] = v

                elif isinstance(current_val, list) and isinstance(v, list):
                    # Merge so recursive calls see all nested keys; cap at 50 to bound memory.
                    if len(current_val) < 50:
                        current_val.extend(v[: 50 - len(current_val)])

                elif isinstance(current_val, dict) and isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        if current_val.get(sub_k) is None:
                            # Shallow copy to prevent recursive reference loops.
                            if isinstance(sub_v, list):
                                current_val[sub_k] = list(sub_v)
                            elif isinstance(sub_v, dict):
                                current_val[sub_k] = dict(sub_v)
                            else:
                                current_val[sub_k] = sub_v

    # Include types in the key to prevent cross-dataset collisions when two
    # datasets have identical field names but different value types.
    cache_key = (
        model_name,
        frozenset((k, type(v).__name__) for k, v in sample_dict.items()),
        id(base_class),
    )
    if cache_key in SCHEMA_REGISTRY:
        # LRU: promote this key to the most-recently-used position so hot
        # schemas are never evicted while cold ones age off the front.
        SCHEMA_REGISTRY.move_to_end(cache_key)
        return SCHEMA_REGISTRY[cache_key]

    fields: dict[str, Any] = {}
    for raw_key, value in sample_dict.items():
        safe_key = sanitize_json_key(raw_key)

        if safe_key in base_class.model_fields:
            continue

        if isinstance(value, dict):
            nested_model: Any = infer_dynamic_schema(f"{model_name}_{safe_key}", value, BaseModel)
            fields[safe_key] = (nested_model | None, Field(alias=raw_key, default=None))

        elif isinstance(value, list) and value and any(isinstance(x, dict) for x in value):
            # Filter to dicts only — value[0] might be None, which would break recursive inference.
            dict_vals = [x for x in value if isinstance(x, dict)]
            nested_model_list: Any = infer_dynamic_schema(f"{model_name}_{safe_key}Item", dict_vals, BaseModel)
            fields[safe_key] = (
                list[nested_model_list] | None,
                Field(alias=raw_key, default_factory=list),
            )
        else:
            field_type: Any = type(value) if value is not None else Any
            if field_type is int:
                field_type = int | float
            fields[safe_key] = (field_type | None, Field(alias=raw_key, default=None))

    try:
        DynamicModel = create_model(
            model_name,
            __config__=ConfigDict(extra="allow", populate_by_name=True),
            __base__=base_class,
            **fields,
        )

        # Mirror per-subclass mutable-container state from the base.
        # Replaces the older ``dir(base_class)`` scan (~200 names) with an
        # explicit allow-list — see _PER_SUBCLASS_CONTAINERS.  ``getattr``
        # walks the MRO, so user-defined intermediate subclasses (those that
        # subclass Incorporator without overriding these attrs) still resolve
        # to the grandparent's seed.  Shallow copy for dict-typed entries
        # so the child owns its mapping but shares value references.
        for attr_name, factory in _PER_SUBCLASS_CONTAINERS:
            seed = getattr(base_class, attr_name, _MISSING)
            if seed is not _MISSING:
                setattr(DynamicModel, attr_name, factory(seed))

        # LRU cache insert: evict the least-recently-used entry when the
        # registry is full.  OrderedDict.popitem(last=False) removes the
        # oldest (front) entry in O(1) — guaranteed by CPython's dict impl.
        if len(SCHEMA_REGISTRY) >= MAX_REGISTRY_SIZE:
            SCHEMA_REGISTRY.popitem(last=False)
        SCHEMA_REGISTRY[cache_key] = DynamicModel

        return DynamicModel
    except Exception as e:
        raise IncorporatorSchemaError(f"Failed to compile dynamic schema {model_name}: {e}") from e
