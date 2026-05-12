"""
Incorporator Factory Module
============================
Module-level factory functions extracted from Incorporator classmethod internals.
Each function receives `cls` explicitly so this module stays independent of
base.py at import time — eliminating the circular-import risk.

Dependency direction: factory.py → schema/, list.py  (never → base.py at runtime)
"""

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union, cast

from .list import IncorporatorList, _deduplicate_extracted
from .schema import builder as schema_builder
from .schema import router

if TYPE_CHECKING:
    from .base import Incorporator

logger = logging.getLogger(__name__)


async def child_incorp(
    cls: "Type[Incorporator]",
    inc_parent: Any,
    **kwargs: Any,
) -> Union["Incorporator", IncorporatorList]:
    """Unified Parent-to-Child API proxy for deeply nested RESTful graphs.

    Extracted from Incorporator._child_incorp so the logic lives outside base.py
    while still operating on the live `cls` passed in at call time.
    """
    child_path = kwargs.get("inc_child") or getattr(inc_parent, "inc_child_path", None)
    if not child_path and inc_parent:
        parent_class = inc_parent[0].__class__ if isinstance(inc_parent, list) else inc_parent.__class__
        child_path = getattr(parent_class, "inc_child", None)

    # Use the Router to perform BFS drill-down
    extracted_data = (
        router.extract_parent_data(inc_parent, child_path)
        if child_path
        else (inc_parent if isinstance(inc_parent, list) else [inc_parent])
    )

    # Deduplicate paths to prevent duplicate HTTP requests
    if extracted_data and child_path:
        extracted_data = _deduplicate_extracted(extracted_data)

    # Enforce REST canonical methods
    raw_method = kwargs.pop("method", kwargs.pop("http_method", "GET"))
    kwargs["http_method"] = raw_method.upper() if isinstance(raw_method, str) else "GET"

    inc_url = kwargs.get("inc_url")
    source_urls = [inc_url] if isinstance(inc_url, str) else (inc_url or [])

    # Use the Router to build POST payloads or GET queries
    if extracted_data:
        kwargs = router.resolve_declarative_routing(cls.__name__, extracted_data, source_urls, **kwargs)

    return await cls.incorp(**kwargs)


def build_instances(
    cls: "Type[Incorporator]",
    parsed_data: List[Any],
    failed_sources: List[str],
    is_single: bool,
    target_class: Optional["Type[Incorporator]"] = None,
    inc_code: Optional[str] = None,
    inc_name: Optional[str] = None,
    excl_lst: Optional[List[str]] = None,
    conv_dict: Optional[Dict[str, Any]] = None,
    name_chg: Optional[List[Tuple[str, str]]] = None,
) -> Union["Incorporator", IncorporatorList]:
    """
    The Factory Assembler:
    1. Transforms data via Columnar processing.
    2. Compiles the optimal Pydantic schema cache.
    3. Instantiates the Python objects at C-speed.

    Extracted from Incorporator._build_instances so the heavy lifting lives
    outside base.py and can be unit-tested independently.
    """
    if failed_sources:
        warnings.warn(
            f"Incorporator partial data returned: {len(failed_sources)} source(s) failed.",
            stacklevel=2,
        )

    if not parsed_data:
        # Generate a safe empty class if an API returns 200 OK but 0 records
        EmptyClass = cast(
            "Type[Incorporator]",
            schema_builder.infer_dynamic_schema("DynamicModel", [{}], cls),
        )
        return IncorporatorList(EmptyClass, [], failed_sources=failed_sources)

    if is_single and len(parsed_data) == 1:
        parsed_data = parsed_data[0]

    # 1. Transform Phase
    transformed_data = schema_builder.apply_etl_transformations(
        parsed_data=parsed_data,
        code_attr=inc_code,
        name_attr=inc_name,
        excl_lst=excl_lst,
        conv_dict=conv_dict,
        name_chg=name_chg,
    )

    # 2. Metaprogramming Compile Phase
    ActualClass = target_class or cast(
        "Type[Incorporator]",
        schema_builder.infer_dynamic_schema("DynamicModel", transformed_data, cls),
    )

    # 3. Final Instantiation Phase
    if isinstance(transformed_data, list):
        # Populate the superset schema from raw dicts before Pydantic absorbs extra keys.
        # Guard: create a per-class dict so subclasses don't share the base-class instance.
        if "_schema_union" not in cls.__dict__:
            cls._schema_union = {}
        # Writes only on first-seen keys — O(1) miss per key, zero writes after stabilization.
        declared = ActualClass.model_json_schema().get("properties", {})
        for item in transformed_data:
            for k in item:
                if k not in cls._schema_union:
                    cls._schema_union[k] = declared.get(k, {"type": "string"})

        instances = [ActualClass(**item) for item in transformed_data]
        return IncorporatorList(ActualClass, instances, failed_sources=failed_sources)

    return ActualClass(**transformed_data)
