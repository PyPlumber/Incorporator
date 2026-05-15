"""Schema-driven instance assembly: Transform, Compile, Instantiate.

Module-level factory functions for the ``incorp()`` pipeline. Each function
receives ``cls`` explicitly so this module stays import-time independent of
``base.py`` — eliminating the circular-import risk.

Dependency direction: ``base.py → schema/factory.py → schema/{builder,router}.py``
(never the reverse).
"""

import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, Union, cast

from ..list import IncorporatorList, _deduplicate_extracted
from . import builder as schema_builder
from . import router

if TYPE_CHECKING:
    from ..base import Incorporator

logger = logging.getLogger(__name__)


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
    failed_sources: List[str],
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
        failed_sources: Any fetch failures accumulated upstream (surfaced as a
            ``UserWarning`` and forwarded to :class:`IncorporatorList`).
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

    transformed_data = schema_builder.apply_etl_transformations(
        parsed_data=parsed_data,
        code_attr=inc_code,
        name_attr=inc_name,
        excl_lst=excl_lst,
        conv_dict=conv_dict,
        name_chg=name_chg,
    )

    ActualClass = target_class or cast(
        "Type[Incorporator]",
        schema_builder.infer_dynamic_schema("DynamicModel", transformed_data, cls),
    )

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

        # model_validate avoids a redundant **kwargs unpack per row and allows
        # Pydantic's Rust core to amortise field-offset lookups across calls.
        # Batching in 1000-row chunks keeps peak memory predictable and gives
        # Pydantic's internal schema cache the best hit rate.
        _BATCH = 1000
        instances: List[Any] = []
        for i in range(0, len(transformed_data), _BATCH):
            instances.extend(ActualClass.model_validate(row) for row in transformed_data[i : i + _BATCH])
        return IncorporatorList(ActualClass, instances, failed_sources=failed_sources)

    return ActualClass(**transformed_data)
