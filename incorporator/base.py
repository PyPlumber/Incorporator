"""
Incorporator Base Module
========================
The core orchestrator and declarative factory for the Incorporator framework.

This file intentionally contains NO data parsing, network looping, or schema
compilation logic. It acts purely as a Domain-Driven orchestrator, routing
kwargs to the `methods/` directory and assembling the resulting Pydantic objects.
"""

import logging
import warnings
import weakref
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Type, TypeVar, Union, cast

from pydantic import BaseModel, Field

from .methods import format_parsers, network, schema_builder
from .methods.converters import _EachSentinel
from .methods.format_parsers import FormatType, infer_format
from .methods.paginate import AsyncPaginator

TIncorporator = TypeVar("TIncorporator", bound="Incorporator")
logger = logging.getLogger(__name__)


# ==========================================
# 1. LIST WRAPPER & REGISTRY ACCESS
# ==========================================
class IncorporatorList(list[TIncorporator]):
    """
    A specialized list providing direct access to the dynamic class registry.

    When `incorp()` returns multiple items, this wrapper allows users to run
    `dataset.inc_dict.get(id)` seamlessly against the dynamically generated class.
    """

    # Exposes failed HTTP 429 URLs for programmatic Dead Letter Queue retries
    failed_sources: List[str]

    def __init__(
        self,
        model_class: Type[TIncorporator],
        items: List[TIncorporator],
        failed_sources: Optional[List[str]] = None,
    ):
        super().__init__(items)
        self._model_class = model_class
        self.failed_sources = failed_sources if failed_sources is not None else []

        # Protects schema_builder.py's cache from concurrent cross-contamination.
        self.inc_child_path: Optional[str] = None

    def __del__(self) -> None:
        """Alert on immediate Garbage Collection."""
        if not self:
            return  # Ignore empty lists

        # If this list is destroyed almost immediately after creation, warn the user!
        if getattr(self, "_warn_on_gc", False):
            logger.debug(
                "🧹 INCORPORATOR GC ALERT: A built list was just garbage collected. "
                "Ensure you assign `.incorp()` to a variable if you need to use `.inc_dict`!"
            )

    @property
    def inc_dict(self) -> "weakref.WeakValueDictionary[Any, TIncorporator]":
        """Provides direct access to the class-level weakref registry."""
        return cast("weakref.WeakValueDictionary[Any, TIncorporator]", self._model_class.inc_dict)


# ==========================================
# 2. THE INCORPORATOR ENGINE
# ==========================================
class Incorporator(BaseModel):
    """
    The Incorporator Super Class.
    Inherits from Pydantic V2 BaseModel to leverage metaprogramming.
    """

    # --- Class-Level Registries & Origin Tracking ---
    inc_dict: ClassVar[weakref.WeakValueDictionary[Any, "Incorporator"]] = (
        weakref.WeakValueDictionary()
    )
    _auto_counter: ClassVar[int] = 1

    inc_url: ClassVar[Optional[str]] = None
    inc_file: ClassVar[Optional[str]] = None

    # --- Universal Instance Attributes ---
    inc_code: Any = Field(default=None, description="Primary key for cls.inc_dict.")
    inc_name: Optional[str] = Field(default=None, description="Optional readable name.")
    last_rcd: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="The exact UTC timestamp this object was instantiated.",
    )

    def display(self) -> None:
        """Utility method to quickly print core instance identity."""
        cls_name = getattr(self.__class__, "__name__", "UnknownClass")
        print(
            f'class:"{cls_name}", inc_code:"{self.inc_code}", '
            f'inc_name:"{self.inc_name}", last_rcd:"{self.last_rcd}"'
        )

    def model_post_init(self, __context: Any) -> None:
        """
        Pydantic Lifecycle Hook: Runs immediately after object instantiation.
        Handles the crucial 'Bubble-Up' registration to protect against Schema Splintering.
        """
        cls = self.__class__

        if self.inc_code is None:
            self.inc_code = cls._auto_counter
            cls._auto_counter += 1

        cls.inc_dict[self.inc_code] = self

        for base in cls.__bases__:
            if issubclass(base, Incorporator) and base is not Incorporator:
                base.inc_dict[self.inc_code] = self

    # ==========================================
    # 3. INTERNAL ROUTERS & FACTORIES
    # ==========================================
    @classmethod
    def _resolve_declarative_routing(
        cls, extracted_data: List[Any], source_urls: List[str], **kwargs: Any
    ) -> Dict[str, Any]:
        """Unified resolver for {} GET injections and Declarative POST tokens."""
        method = kwargs.get("http_method", "GET")
        if method in ("POST", "PUT", "PATCH"):
            target_payload = kwargs.get("form_payload") or kwargs.get("json_payload")

            if target_payload and isinstance(target_payload, dict):
                is_iterative = any(isinstance(v, _EachSentinel) for v in target_payload.values())

                if is_iterative:
                    payload_list = []
                    for item in extracted_data:
                        p = {}
                        for k, v in target_payload.items():
                            p[k] = item if isinstance(v, _EachSentinel) else v
                        payload_list.append(p)
                    kwargs["payload_list"] = payload_list

                    if len(source_urls) == 1:
                        kwargs["inc_url"] = source_urls * len(extracted_data)
                else:
                    built_payload = {}
                    for k, v in target_payload.items():
                        built_payload[k] = v(extracted_data) if callable(v) else v

                    if source_urls:
                        kwargs["payload_list"] = [built_payload] * len(source_urls)
                    else:
                        raise ValueError(
                            f"[{cls.__name__}] Missing Target URL. "
                            f"You must explicitly provide `inc_url='...'` when executing a POST request "
                            f"via `inc_parent` and declarative tokens."
                        )
            else:
                if source_urls:
                    kwargs["inc_url"] = source_urls

        elif method == "GET":
            extracted_strs = [str(x) for x in extracted_data if x is not None]

            if len(source_urls) == 1 and "{}" in source_urls[0]:
                base_url = source_urls[0]
                kwargs["inc_url"] = [base_url.format(x) for x in extracted_strs]
            else:
                valid_urls = [
                    x for x in extracted_strs if x.startswith("http") or x.startswith("/")
                ]
                if valid_urls:
                    kwargs["inc_url"] = source_urls + valid_urls
                elif not source_urls:
                    valid_items = [
                        item
                        for item in extracted_data
                        if isinstance(getattr(item, "detail_url", getattr(item, "url", None)), str)
                    ]
                    legacy_urls = [
                        getattr(item, "detail_url", getattr(item, "url", None))
                        for item in valid_items
                    ]

                    if legacy_urls:
                        logger.warning(
                            f"[{cls.__name__}] Deprecation Warning: Relying on implicit '.url' or '.detail_url' "
                            f"attributes for HATEOAS routing is deprecated. "
                            f"Tip: Explicitly pass `inc_child='url'` (or your target JSON key) to your .incorp() call."
                        )
                        kwargs["inc_url"] = legacy_urls
                    else:
                        raise ValueError("inc_parent extraction yielded no valid URLs.")

        return kwargs

    @classmethod
    def _extract_parent_data(cls, parents: Any, child_path: str) -> List[Any]:
        """
        Safely drills into dynamic structures iteratively (Columnar BFS).
        """
        # 1. Start with our baseline array
        current_layer = parents if isinstance(parents, list) else [parents]

        # 2. Drill level-by-level across the ENTIRE array (Breadth-First)
        for part in child_path.split("."):
            next_layer: List[Any] = []

            for node in current_layer:
                if node is None:
                    continue

                # Splintering Defense: If the node itself is a list, flatten it into the layer
                if isinstance(node, list):
                    # We process the list items in the SAME pass
                    for item in node:
                        val = (
                            item.get(part) if isinstance(item, dict) else getattr(item, part, None)
                        )
                        if val is not None:
                            next_layer.append(val)
                else:
                    val = node.get(part) if isinstance(node, dict) else getattr(node, part, None)
                    if val is not None:
                        next_layer.append(val)

            # The next layer becomes the current layer for the next dot-notation part
            current_layer = next_layer

            # Fast-exit: If a column drill yields absolutely nothing, stop processing.
            if not current_layer:
                break

        return current_layer

    @classmethod
    async def _child_incorp(
        cls: Type[TIncorporator], inc_parent: Any, **kwargs: Any
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """Unified Parent-to-Child routing supporting Declarative POSTs and GET injections."""

        child_path = kwargs.get("inc_child") or getattr(inc_parent, "inc_child_path", None)
        if not child_path and inc_parent:
            parent_class = (
                inc_parent[0].__class__ if isinstance(inc_parent, list) else inc_parent.__class__
            )
            child_path = getattr(parent_class, "inc_child", None)

        extracted_data = (
            cls._extract_parent_data(inc_parent, child_path)
            if child_path
            else (inc_parent if isinstance(inc_parent, list) else [inc_parent])
        )

        # Deduplicate extracted child paths (URLs/IDs)
        if extracted_data and child_path:
            hashable_data = [x for x in extracted_data if isinstance(x, (str, int, float, bool))]
            extracted_data = list(dict.fromkeys(hashable_data))

        # Safely clear aliases and enforce canonical 'http_method'
        raw_method = kwargs.pop("method", kwargs.pop("http_method", "GET"))
        kwargs["http_method"] = raw_method.upper() if isinstance(raw_method, str) else "GET"

        inc_url = kwargs.get("inc_url")
        source_urls = [inc_url] if isinstance(inc_url, str) else (inc_url or [])

        # DELEGATE TO MODULAR ROUTER
        if extracted_data:
            kwargs = cls._resolve_declarative_routing(extracted_data, source_urls, **kwargs)

        return await cls.incorp(**kwargs)

    @classmethod
    def _build_instances(
        cls: Type[TIncorporator],
        parsed_data: List[Any],
        failed_sources: List[str],
        is_single: bool,
        target_class: Optional[Type[TIncorporator]] = None,
        inc_code: Optional[str] = None,
        inc_name: Optional[str] = None,
        excl_lst: Optional[List[str]] = None,
        conv_dict: Optional[Dict[str, Any]] = None,
        name_chg: Optional[List[Tuple[str, str]]] = None,
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """
        The Factory Assembler: Applies Columnar ETL transformations,
        compiles the dynamic Pydantic schema, and instantiates final objects.
        """
        if failed_sources:
            warnings.warn(
                f"Incorporator partial data returned: {len(failed_sources)} source(s) failed with HTTP 429.",
                UserWarning,
                stacklevel=2,
            )

        if not parsed_data:
            # Warn on empty valid payloads
            logger.info(
                f"ℹ️ INCORPORATOR INFO:[{cls.__name__}] The API returned a valid response, "
                f"but 0 records were mapped. "
                f"Tip: Verify your `rec_path='...'` accurately matches the nested JSON structure, "
                f"or check your API query parameters."
            )
            EmptyClass = cast(
                Type[TIncorporator], schema_builder.infer_dynamic_schema("DynamicModel", [{}], cls)
            )
            return IncorporatorList(EmptyClass, [], failed_sources=failed_sources)

        if is_single and len(parsed_data) == 1:
            parsed_data = parsed_data[0]

        # 1. Declarative ETL Transformation Phase (Columnar)
        transformed_data = schema_builder.apply_etl_transformations(
            parsed_data=parsed_data,
            code_attr=inc_code,
            name_attr=inc_name,
            excl_lst=excl_lst,
            conv_dict=conv_dict,
            name_chg=name_chg,
        )

        # 2. Metaprogramming Compilation Phase
        ActualClass = target_class or cast(
            Type[TIncorporator],
            schema_builder.infer_dynamic_schema("DynamicModel", transformed_data, cls),
        )

        # 3. Final Object Instantiation Phase
        if isinstance(transformed_data, list):
            instances = [ActualClass(**item) for item in transformed_data]
            return IncorporatorList(ActualClass, instances, failed_sources=failed_sources)

        return ActualClass(**transformed_data)

    # ==========================================
    # 4. PUBLIC "HOLY TRINITY" API
    # ==========================================
    @classmethod
    async def incorp(
        cls: Type[TIncorporator],
        inc_url: Optional[Union[str, List[str]]] = None,
        inc_file: Optional[Union[str, List[str]]] = None,
        inc_parent: Optional[Union[TIncorporator, "IncorporatorList[TIncorporator]"]] = None,
        inc_child: Optional[str] = None,
        inc_code: Optional[str] = None,
        inc_name: Optional[str] = None,
        excl_lst: Optional[List[str]] = None,
        conv_dict: Optional[Dict[str, Any]] = None,
        name_chg: Optional[List[Tuple[str, str]]] = None,
        inc_page: Optional[AsyncPaginator] = None,
        **kwargs: Any,
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """
        Extracts data from an API or File and returns dynamically generated Python objects.

        Args:
            inc_url: Single URL or List of URLs to fetch concurrently.
            inc_file: Single File path or List of file paths to parse.
            inc_parent: A parent Incorporator object to setup shallow and deep enrichment.
            inc_child: API URLs or Incorp. Unique ID to create sub-class for deep enrichment.
            inc_code: The attribute name in the API to bind to the primary key (`cls.inc_dict`).
            inc_name: The attribute name in the API to bind to the readable name.
            excl_lst: A list of keys to completely drop from the API response before building.
            conv_dict: Dictionary utilizing `inc()`, `calc()`, and `calc_all()` for Declarative ETL.
            name_chg: List of tuples to rename API keys e.g., `[("old_key", "new_key")]`.
            inc_page: A Paginator instance (e.g., NextUrlPaginator(), OffsetPaginator(limit=50))
            **kwargs: Configs passed to `network.py` (e.g., `http_method="POST"`, `payload_builder`).
        """
        if inc_parent is not None:
            # We explicitly pass inc_url, inc_file, and inc_child down
            kwargs.update(
                {
                    "inc_url": inc_url,
                    "inc_file": inc_file,
                    "inc_child": inc_child,
                    "inc_code": inc_code,
                    "inc_name": inc_name,
                    "excl_lst": excl_lst,
                    "conv_dict": conv_dict,
                    "name_chg": name_chg,
                    "inc_page": inc_page,
                }
            )
            return await cls._child_incorp(inc_parent=inc_parent, **kwargs)

        source = inc_file if inc_file else inc_url
        if not source and not kwargs.get("payload_list"):
            raise ValueError(
                f"[{cls.__name__}] Either 'inc_url', 'inc_file', or a valid 'inc_parent' must be provided."
            )

        is_file_mode = bool(inc_file)
        source_list: List[str] = []
        if isinstance(source, list):
            source_list = [str(s) for s in source if s is not None]
        elif isinstance(source, str):
            source_list = [source]
        elif kwargs.get("payload_list"):
            # Fallback constraint for dynamic payloads
            source_list = [""] * len(cast(List[Any], kwargs["payload_list"]))

        is_single = not isinstance(source, list) and inc_page is None

        if is_single and isinstance(source, str):
            if is_file_mode:
                cls.inc_file = source
            else:
                cls.inc_url = source

        # Extract parallel payload list if injected by _child_incorp
        payload_list = kwargs.pop("payload_list", None)

        # Concurrency & I/O Phase (Delegated to network.py)
        parsed_data, failed_sources = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=is_file_mode,
            inc_page=inc_page,
            payload_list=payload_list,  # Passes the parallel POST bodies!
            **kwargs,
        )

        # Pass Explicit ETL Parameters to the Builder
        result = cls._build_instances(
            parsed_data,
            failed_sources,
            is_single,
            inc_code=inc_code,
            inc_name=inc_name,
            excl_lst=excl_lst,
            conv_dict=conv_dict,
            name_chg=name_chg,
        )

        if inc_child is not None and isinstance(result, IncorporatorList):
            result.inc_child_path = inc_child

        return result

    @classmethod
    async def refresh(
        cls: Type[TIncorporator],
        instance: Union[TIncorporator, List[TIncorporator]],
        new_url: Optional[Union[str, List[str]]] = None,
        new_file: Optional[Union[str, List[str]]] = None,
        inc_child: Optional[str] = None,
        inc_code: Optional[str] = None,
        inc_name: Optional[str] = None,
        excl_lst: Optional[List[str]] = None,
        conv_dict: Optional[Dict[str, Any]] = None,
        name_chg: Optional[List[Tuple[str, str]]] = None,
        inc_page: Optional[AsyncPaginator] = None,
        **kwargs: Any,
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:

        inst_list = instance if isinstance(instance, list) else [instance]
        if not inst_list:
            raise ValueError("Cannot refresh an empty list.")
        TargetClass = inst_list[0].__class__

        raw_method = kwargs.pop("method", kwargs.pop("http_method", "GET"))
        kwargs["http_method"] = raw_method.upper() if isinstance(raw_method, str) else "GET"

        child_path = inc_child or getattr(instance, "inc_child_path", None)
        extracted_data = (
            cls._extract_parent_data(inst_list, child_path) if child_path else inst_list
        )

        target = new_url or new_file
        source_urls: List[str] = []
        if isinstance(target, list):
            source_urls = [str(x) for x in target if x is not None]
        elif isinstance(target, str):
            source_urls = [target]

        # DELEGATE TO MODULAR ROUTER
        if not new_file and extracted_data:
            kwargs = cls._resolve_declarative_routing(extracted_data, source_urls, **kwargs)
            raw_url = kwargs.pop("inc_url", source_urls)

            source_list: List[str] = []
            if isinstance(raw_url, list):
                source_list = [str(x) for x in raw_url if x is not None]
            elif isinstance(raw_url, str):
                source_list = [raw_url]

            payload_list = kwargs.pop("payload_list", None)
        else:
            source_list = source_urls
            payload_list = kwargs.pop("payload_list", None)

        if not source_list and not new_file:
            raw_sources = [
                getattr(inst, "inc_url", getattr(inst, "inc_file", "")) for inst in inst_list
            ]
            source_list = [str(u) for u in raw_sources if u]

            # Deduplicate refresh URLs
            if source_list:
                source_list = list(dict.fromkeys(source_list))

            if not source_list:
                raise ValueError(
                    "Instances contain no origin URLs to refresh from, and no new_url was provided."
                )

        parsed_data, failed_sources = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=bool(new_file)
            or (not new_url and getattr(inst_list[0], "inc_file", None) is not None),
            inc_page=inc_page,
            payload_list=payload_list,
            **kwargs,
        )

        # DELEGATE TO MODULAR BUILDER
        result = cls._build_instances(
            parsed_data,
            failed_sources,
            is_single=(len(source_list) <= 1 and inc_page is None),
            target_class=TargetClass,
            inc_code=inc_code,
            inc_name=inc_name,
            excl_lst=excl_lst,
            conv_dict=conv_dict,
            name_chg=name_chg,
        )

        if inc_child is not None and isinstance(result, IncorporatorList):
            result.inc_child_path = inc_child

        return result

    @classmethod
    async def export(
        cls: Type[TIncorporator],
        instance: Union[TIncorporator, List[TIncorporator]],
        file_path: str,
        format_type: Optional[FormatType] = None,
    ) -> None:
        """Serializes current Incorporator states out to physical JSON/CSV/XML files."""
        active_format = format_type or infer_format(file_path)
        instances = instance if isinstance(instance, list) else [instance]

        data_dicts = [obj.model_dump(by_alias=True, mode="json") for obj in instances]
        await format_parsers.write_destination_data(data_dicts, file_path, active_format)
