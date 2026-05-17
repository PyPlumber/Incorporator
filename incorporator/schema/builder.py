"""Dynamic Pydantic model generation and declarative ETL engine.

Provides two public entry points: :func:`infer_dynamic_schema` (builds a
Pydantic V2 model class from raw data samples) and
:func:`apply_etl_transformations` (applies ``excl_lst``, ``conv_dict``, and
``name_chg`` passes in columnar order).  Both are called exclusively from
:mod:`incorporator.schema.factory`.
"""

import keyword
import logging
import re
import weakref
from collections import OrderedDict
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple, Type, Union

from pydantic import BaseModel, ConfigDict, Field, create_model

from ..exceptions import IncorporatorSchemaError

logger = logging.getLogger(__name__)

# LRU cache keyed by (model_name, frozenset(field_types), id(base_class)).
# OrderedDict + move_to_end on hit gives O(1) LRU eviction — older entries
# fall off the front while hot schemas stay at the end.
SCHEMA_REGISTRY: "OrderedDict[Tuple[str, FrozenSet[Any], int], Type[BaseModel]]" = OrderedDict()
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
_PER_SUBCLASS_CONTAINERS: Tuple[Tuple[str, Callable[[Any], Any]], ...] = (
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
    parsed_data: Union[Dict[str, Any], List[Dict[str, Any]]],
    code_attr: Optional[str] = None,
    name_attr: Optional[str] = None,
    excl_lst: Optional[List[str]] = None,
    conv_dict: Optional[Dict[str, Any]] = None,
    name_chg: Optional[List[Tuple[str, str]]] = None,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """Apply exc_lst, conv_dict, name_chg, and PK-binding transforms in-place.

    Processes ``parsed_data`` through four ordered columnar passes: drop
    (``excl_lst``), convert (``conv_dict`` — handles :func:`calc`,
    :func:`calc_all`, and standard converters), rename (``name_chg``), and
    PK binding (``code_attr`` / ``name_attr`` → ``inc_code`` / ``inc_name``).

    Args:
        parsed_data: A single record dict or a list of record dicts from the
            format handler.
        code_attr: Source field name to alias as ``inc_code``.
        name_attr: Source field name to alias as ``inc_name``.
        excl_lst: Field names to drop before Pydantic compilation.
        conv_dict: Mapping of field name → converter (``inc``, ``calc``,
            ``calc_all``, ``link_to``, ``pluck``, etc.).
        name_chg: ``[(old_name, new_name), ...]`` rename pairs applied after
            conversions.

    Returns:
        The same structure as ``parsed_data`` (dict or list), mutated in
        place.  Callers may discard the return value.
    """

    items = parsed_data if isinstance(parsed_data, list) else [parsed_data]
    if not items:
        return parsed_data

    dict_items: List[Dict[str, Any]] = [i for i in items if isinstance(i, dict)]
    if not dict_items:
        return items if isinstance(parsed_data, list) else items[0]

    # Rows outer, keys inner — keeps each dict warm in the CPU cache during
    # the inner loop, avoiding cache thrashing on large datasets.
    if excl_lst:
        excl_set = frozenset(excl_lst)
        for d in dict_items:
            for key in excl_set:
                d.pop(key, None)

    if conv_dict:
        for key, operation in conv_dict.items():
            op_type = type(operation).__name__

            if "Calc" in op_type and "All" not in op_type:  # calc sentinel
                inputs = operation.input_list if operation.input_list else [key]
                for d in dict_items:
                    args = [d.get(dep) for dep in inputs]
                    try:
                        val = operation.func(*args)
                    except Exception as e:
                        logger.warning("calc failed for key '%s' with args %s: %s", key, args, e)
                        val = operation.default

                    if callable(operation.target_type):
                        try:
                            val = operation.target_type(val)
                        except Exception as e:
                            logger.warning("calc type coercion failed for key '%s' value %r: %s", key, val, e)
                            val = operation.default
                    d[key] = val

            elif "CalcAll" in op_type:  # calc_all sentinel
                inputs = operation.input_list if operation.input_list else [key]
                col_args = [[d.get(dep) for d in dict_items] for dep in inputs]
                try:
                    results = operation.func(*col_args)
                except Exception as e:
                    logger.warning("calc_all failed for key '%s': %s", key, e)
                    results = [operation.default] * len(dict_items)

                for idx, d in enumerate(dict_items):
                    try:
                        val = results[idx]
                    except IndexError:
                        logger.warning(
                            "calc_all returned fewer results than expected for key '%s' (needed index %d)",
                            key,
                            idx,
                        )
                        val = operation.default

                    if callable(operation.target_type):
                        try:
                            val = operation.target_type(val)
                        except Exception as e:
                            logger.warning("calc_all type coercion failed for key '%s' value %r: %s", key, val, e)
                            val = operation.default
                    d[key] = val

            else:  # standard converter: inc, link_to, pluck, etc.
                for d in dict_items:
                    try:
                        d[key] = operation(d.get(key, None))
                    except Exception as e:
                        logger.warning("conv_dict failed on key '%s': %s", key, e)

    if name_chg:
        name_map = dict(name_chg)  # preserves insertion order so chained renames apply in sequence
        for d in dict_items:
            for old_key, new_key in name_map.items():
                if old_key in d:
                    d[new_key] = d.pop(old_key)

    if code_attr:
        for d in dict_items:
            if code_attr in d:
                d["inc_code"] = d[code_attr]
    if name_attr:
        for d in dict_items:
            if name_attr in d:
                d["inc_name"] = d[name_attr]

    return items if isinstance(parsed_data, list) else items[0]


def infer_dynamic_schema(
    model_name: str, data: Union[Dict[str, Any], List[Dict[str, Any]]], base_class: Type[BaseModel]
) -> Type[BaseModel]:
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
        ``Optional[...]`` so missing keys never trigger ``ValidationError``,
        and all numeric fields use ``Union[int, float]`` so APIs that
        sometimes return ``42`` and sometimes ``42.0`` for the same field
        validate cleanly.

        The class is cached in :data:`SCHEMA_REGISTRY` keyed by
        ``(model_name, frozenset((k, type(v).__name__) for k, v in sample_dict.items()), id(base_class))``
        so re-invoking with the same shape returns the cached class
        rather than rebuilding.
    """

    sample_dict: Dict[str, Any] = {}

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

                if current_val is None or (isinstance(current_val, (list, dict)) and not current_val):
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

    fields: Dict[str, Any] = {}
    for raw_key, value in sample_dict.items():
        safe_key = sanitize_json_key(raw_key)

        if safe_key in base_class.model_fields:
            continue

        if isinstance(value, dict):
            nested_model: Any = infer_dynamic_schema(f"{model_name}_{safe_key}", value, BaseModel)
            fields[safe_key] = (Optional[nested_model], Field(alias=raw_key, default=None))

        elif isinstance(value, list) and value and any(isinstance(x, dict) for x in value):
            # Filter to dicts only — value[0] might be None, which would break recursive inference.
            dict_vals = [x for x in value if isinstance(x, dict)]
            nested_model_list: Any = infer_dynamic_schema(f"{model_name}_{safe_key}Item", dict_vals, BaseModel)
            fields[safe_key] = (
                Optional[List[nested_model_list]],
                Field(alias=raw_key, default_factory=list),
            )
        else:
            field_type: Any = type(value) if value is not None else Any
            if field_type is int:
                field_type = Union[int, float]
            fields[safe_key] = (Optional[field_type], Field(alias=raw_key, default=None))

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
