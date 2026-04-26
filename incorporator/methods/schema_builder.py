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
    "model_config", "model_fields", "model_computed_fields", "model_extra",
    "model_fields_set", "model_dump", "model_dump_json", "model_validate",
    "model_validate_json", "fields", "copy", "dict", "json"
}


def sanitize_json_key(key: str) -> str:
    """Sanitizes keys to PEP 8 standards and prevents Python/Pydantic collisions."""
    clean_key = re.sub(r'[^a-zA-Z0-9_]', '_', key)

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

    # 1. FAST PASS: Row-Based Data Cleaning
    if excl_lst:
        for item in items:
            if isinstance(item, dict):
                for key in excl_lst:
                    item.pop(key, None)

    # 2. COLUMNAR PIVOT: Attribute-Based Execution Order
    if conv_dict:
        for key, operation in conv_dict.items():
            op_type = type(operation).__name__

            # --- SCENARIO A: Multi-Input Row Calculation (calc) ---
            if "Calc" in op_type and "All" not in op_type:
                inputs = operation.input_list if operation.input_list else [key]
                for item in items:
                    if not isinstance(item, dict): continue
                    args = [item.get(dep) for dep in inputs]
                    try:
                        val = operation.func(*args)
                    except Exception as e:
                        val = operation.default

                    if callable(operation.target_type):
                        try:
                            val = operation.target_type(val)
                        except Exception:
                            val = operation.default
                    item[key] = val

            # --- SCENARIO B: Batch Array Calculation (calc_all) ---
            elif "CalcAll" in op_type:
                inputs = operation.input_list if operation.input_list else [key]
                col_args = [[item.get(dep) for item in items if isinstance(item, dict)] for dep in inputs]
                try:
                    results = operation.func(*col_args)
                except Exception as e:
                    results = [operation.default] * len(items)

                dict_items = [i for i in items if isinstance(i, dict)]
                for idx, item in enumerate(dict_items):
                    try:
                        val = results[idx]
                    except IndexError:
                        val = operation.default

                    if callable(operation.target_type):
                        try:
                            val = operation.target_type(val)
                        except Exception:
                            val = operation.default
                    item[key] = val

            # --- SCENARIO C: Standard Extraction (inc, link_to) ---
            else:
                for item in items:
                    if not isinstance(item, dict): continue
                    try:
                        item[key] = operation(item.get(key, None))
                    except Exception as e:
                        logger.warning(f"Standard conv_dict failed on key '{key}': {e}")

    # 3. FAST PASS: Row-Based Structure Finalization
    for item in items:
        if not isinstance(item, dict): continue
        if name_chg:
            for old_key, new_key in name_chg:
                if old_key in item:
                    item[new_key] = item.pop(old_key)
        if code_attr and code_attr in item:
            item['inc_code'] = item[code_attr]
        if name_attr and name_attr in item:
            item['inc_name'] = item[name_attr]

    return items if isinstance(parsed_data, list) else items[0]


def infer_dynamic_schema(
        model_name: str,
        data: Union[Dict[str, Any], List[Dict[str, Any]]],
        base_class: Type[BaseModel]
) -> Type[BaseModel]:
    """Recursively builds a Pydantic subclass based on the data's shape."""
    sample_dict = data[0] if isinstance(data, list) and data else data
    if not isinstance(sample_dict, dict):
        sample_dict = {}

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
            nested_model_list: Any = infer_dynamic_schema(f"{model_name}_{safe_key}Item", value[0], BaseModel)
            fields[safe_key] = (Optional[List[nested_model_list]], Field(alias=raw_key, default_factory=list))
        else:
            field_type: Any = type(value) if value is not None else Any
            if field_type is int:
                field_type = Union[int, float]
            fields[safe_key] = (Optional[field_type], Field(alias=raw_key, default=None))

    try:
        DynamicModel = create_model(
            model_name,
            __config__=ConfigDict(extra='allow', populate_by_name=True),
            __base__=base_class,
            **fields
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