"""Dynamic Pydantic model generation and Declarative ETL engine."""

import copy
import keyword
import logging
import re
import weakref
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from pydantic import BaseModel, ConfigDict, Field, create_model

from .exceptions import IncorporatorSchemaError

logger = logging.getLogger(__name__)

# Cache to prevent recompiling identical schemas during deep nesting
SCHEMA_REGISTRY: Dict[Tuple[str, frozenset[str]], Type[BaseModel]] = {}
MAX_REGISTRY_SIZE = 1000  # Hard boundary to prevent OOM leaks

# The protective shield for Pydantic internals
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

# Pre-compile regex to save C-level CPU cycles
_SANITIZE_RE = re.compile(r"[^a-zA-Z0-9_]")


def sanitize_json_key(key: str) -> str:
    """Sanitizes keys to PEP 8 standards and prevents Python/Pydantic collisions."""
    clean_key = _SANITIZE_RE.sub("_", key)

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
    """Applies Declarative ETL rules utilizing Attribute-Based (Columnar) Processing."""

    items = parsed_data if isinstance(parsed_data, list) else [parsed_data]
    if not items:
        return parsed_data

    # Extract guaranteed dict references once.
    dict_items = [i for i in items if isinstance(i, dict)]
    if not dict_items:
        return items if isinstance(parsed_data, list) else items[0]

    # 1. COLUMNAR PASS: Exclusions (Drop)
    if excl_lst:
        for key in excl_lst:
            for d in dict_items:
                d.pop(key, None)

    # 2. COLUMNAR PASS: Conversions (Mutate)
    if conv_dict:
        for key, operation in conv_dict.items():
            op_type = type(operation).__name__

            # --- SCENARIO A: Multi-Input Row Calculation (calc) ---
            if "Calc" in op_type and "All" not in op_type:
                inputs = operation.input_list if operation.input_list else [key]
                for d in dict_items:
                    args = [d.get(dep) for dep in inputs]
                    try:
                        val = operation.func(*args)
                    except Exception:
                        val = operation.default

                    if callable(operation.target_type):
                        try:
                            val = operation.target_type(val)
                        except Exception:
                            val = operation.default
                    d[key] = val

            # --- SCENARIO B: Batch Array Calculation (calc_all) ---
            elif "CalcAll" in op_type:
                inputs = operation.input_list if operation.input_list else [key]
                # Zip-like column extraction using our pre-filtered dict_items
                col_args = [[d.get(dep) for d in dict_items] for dep in inputs]
                try:
                    results = operation.func(*col_args)
                except Exception:
                    results = [operation.default] * len(dict_items)

                for idx, d in enumerate(dict_items):
                    try:
                        val = results[idx]
                    except IndexError:
                        val = operation.default

                    if callable(operation.target_type):
                        try:
                            val = operation.target_type(val)
                        except Exception:
                            val = operation.default
                    d[key] = val

            # --- SCENARIO C: Standard Extraction (inc, link_to) ---
            else:
                for d in dict_items:
                    try:
                        d[key] = operation(d.get(key, None))
                    except Exception as e:
                        logger.warning(f"Standard conv_dict failed on key '{key}': {e}")

    # 3. COLUMNAR PASS: Renaming (Alias)
    if name_chg:
        name_map = dict(name_chg)  # Dict conversion preserves chronological insertion order!
        for old_key, new_key in name_map.items():
            for d in dict_items:
                if old_key in d:
                    d[new_key] = d.pop(old_key)

    # 4. COLUMNAR PASS: PK Binding (Index)
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
    """Recursively builds a Pydantic subclass based on the data's comprehensive shape."""

    sample_dict: Dict[str, Any] = {}

    # Python natively handles lists smaller than 50 without throwing an IndexError
    items_to_sample = data[:50] if isinstance(data, list) else [data]

    for item in items_to_sample:
        if isinstance(item, dict):
            for k, v in item.items():
                current_val = sample_dict.get(k)

                # Condition A: Key doesn't exist, is None, or is an empty List/Dict.
                if current_val is None or (
                    isinstance(current_val, (list, dict)) and not current_val
                ):
                    # Use shallow copies to prevent mutating the original data via reference!
                    if isinstance(v, list):
                        sample_dict[k] = list(v)
                    elif isinstance(v, dict):
                        sample_dict[k] = dict(v)
                    else:
                        sample_dict[k] = v

                # Condition B: Both are lists. Combine them so recursive calls see ALL nested keys!
                elif isinstance(current_val, list) and isinstance(v, list):
                    # Cap the combined sample list at 50 to prevent recursive memory bloat
                    if len(current_val) < 50:
                        current_val.extend(v[: 50 - len(current_val)])

                # Condition C: Both are dicts. Merge keys to discover missing attributes.
                elif isinstance(current_val, dict) and isinstance(v, dict):
                    for sub_k, sub_v in v.items():
                        if current_val.get(sub_k) is None:
                            # Enforce shallow copies to prevent recursive reference loops!
                            if isinstance(sub_v, list):
                                current_val[sub_k] = list(sub_v)
                            elif isinstance(sub_v, dict):
                                current_val[sub_k] = dict(sub_v)
                            else:
                                current_val[sub_k] = sub_v

    cache_key = (model_name, frozenset(sample_dict.keys()))
    if cache_key in SCHEMA_REGISTRY:
        return SCHEMA_REGISTRY[cache_key]

    fields: Dict[str, Any] = {}
    for raw_key, value in sample_dict.items():
        safe_key = sanitize_json_key(raw_key)

        if safe_key in base_class.model_fields:
            continue

        if isinstance(value, dict):
            nested_model: Any = infer_dynamic_schema(f"{model_name}_{safe_key}", value, BaseModel)
            fields[safe_key] = (Optional[nested_model], Field(alias=raw_key, default=None))
        elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
            # Pass the entire combined `value` list to ensure nested sampling works!
            nested_model_list: Any = infer_dynamic_schema(
                f"{model_name}_{safe_key}Item", value, BaseModel
            )
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

        for attr_name in dir(base_class):
            if not attr_name.startswith("__") and attr_name not in PYDANTIC_RESERVED:
                attr_val = getattr(base_class, attr_name)
                if isinstance(attr_val, dict):
                    setattr(DynamicModel, attr_name, copy.deepcopy(attr_val))
                elif isinstance(attr_val, weakref.WeakValueDictionary):
                    setattr(DynamicModel, attr_name, weakref.WeakValueDictionary())

        if base_class is BaseModel:
            if len(SCHEMA_REGISTRY) >= MAX_REGISTRY_SIZE:
                SCHEMA_REGISTRY.clear()
            SCHEMA_REGISTRY[cache_key] = DynamicModel

        return DynamicModel
    except Exception as e:
        raise IncorporatorSchemaError(f"Failed to compile dynamic schema {model_name}: {e}") from e
