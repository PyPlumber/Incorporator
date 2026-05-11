"""
Incorporator Base Module
========================
The core orchestrator and declarative factory for the Incorporator framework.

This file acts purely as a Domain-Driven orchestrator. It contains NO data parsing,
network looping, or schema compilation logic. It maps Developer kwargs to the
`methods/` directory and assembles the resulting dynamic Pydantic object graphs.
"""

import asyncio
import logging
import warnings
import weakref
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    ClassVar,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel, Field

from .methods import format_parsers, network, router, schema_builder
from .methods.format_parsers import FormatType, infer_format
from .methods.paginate import AsyncPaginator

if TYPE_CHECKING:
    from .methods.logger import AuditResult

# Type variable for strict IDE hinting on subclass generation
TIncorporator = TypeVar("TIncorporator", bound="Incorporator")
logger = logging.getLogger(__name__)


# ==========================================
# 1. LIST WRAPPER & REGISTRY ACCESS
# ==========================================
class IncorporatorList(list[TIncorporator]):
    """
    A specialized list providing direct access to the dynamic class registry.

    When `incorp()` returns multiple items, this wrapper allows users to run
    `dataset.inc_dict.get(id)` seamlessly against the dynamically generated class
    without needing to manually inspect `type(dataset[0])`.
    """

    failed_sources: List[str]

    def __init__(
        self,
        model_class: Type[TIncorporator],
        items: List[TIncorporator],
        failed_sources: Optional[List[str]] = None,
    ):
        super().__init__(items)
        self._model_class = model_class
        # Exposes HTTP 429 failed URLs/Paths for programmatic Dead Letter Queue retries
        self.failed_sources = failed_sources if failed_sources is not None else []

        # Protects schema_builder.py's cache from cross-contamination during graph drilling
        self.inc_child_path: Optional[str] = None

    def __del__(self) -> None:
        """Memory-leak sentinel alerting users to immediate Garbage Collection."""
        if not self:
            return
        if getattr(self, "_warn_on_gc", False):
            logger.debug(
                "🧹 INCORPORATOR GC ALERT: A built list was just garbage collected. "
                "Ensure you assign `.incorp()` to a variable if you need to use `.inc_dict`!"
            )

    @property
    def inc_dict(self) -> "weakref.WeakValueDictionary[Any, TIncorporator]":
        """Provides O(1) direct access to the class-level weakref registry."""
        return cast("weakref.WeakValueDictionary[Any, TIncorporator]", self._model_class.inc_dict)


# ==========================================
# 2. THE INCORPORATOR ENGINE
# ==========================================
class Incorporator(BaseModel):
    """
    The Incorporator Super Class.
    Inherits from Pydantic V2 BaseModel to leverage blazing-fast Rust validation.
    """

    # --- Class-Level Memory Registries ---
    # WeakValueDictionary ensures objects are garbage-collected when the user's lists
    # go out of scope, absolutely preventing Out-Of-Memory (OOM) leaks.
    inc_dict: ClassVar[weakref.WeakValueDictionary[Any, "Incorporator"]] = weakref.WeakValueDictionary()
    _auto_counter: ClassVar[int] = 1

    # Origin Tracking
    inc_url: ClassVar[Optional[str]] = None
    inc_file: ClassVar[Optional[str]] = None

    # --- Universal Instance Attributes ---
    inc_code: Any = Field(default=None, description="Primary key for cls.inc_dict.")
    inc_name: Optional[str] = Field(default=None, description="Optional readable name.")
    last_rcd: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Exact UTC timestamp this object was instantiated.",
    )

    def display(self) -> None:
        """Utility method to quickly print core instance identity to stdout."""
        cls_name = getattr(self.__class__, "__name__", "UnknownClass")
        print(f'class:"{cls_name}", inc_code:"{self.inc_code}", inc_name:"{self.inc_name}", last_rcd:"{self.last_rcd}"')

    def model_post_init(self, __context: Any) -> None:
        """
        Pydantic Lifecycle Hook: Runs immediately after Rust instantiation.
        Handles the crucial 'Bubble-Up' registration to protect against Schema Splintering.
        """
        cls = self.__class__

        # Auto-increment if the API provided no unique identifier
        if self.inc_code is None:
            self.inc_code = cls._auto_counter
            cls._auto_counter += 1

        cls.inc_dict[self.inc_code] = self

        # DSA OPTIMIZATION: Fast-path C-tuple evaluation.
        # Only iterate bases if this is a deeply nested dynamic subclass.
        if cls.__bases__ and cls.__bases__[0] is not Incorporator:
            for base in cls.__bases__:
                if issubclass(base, Incorporator) and base is not Incorporator:
                    base.inc_dict[self.inc_code] = self

    # ==========================================
    # 3. INTERNAL FACTORIES
    # ==========================================
    @classmethod
    async def _child_incorp(
        cls: Type[TIncorporator], inc_parent: Any, **kwargs: Any
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """Unified Parent-to-Child API proxy for deeply nested RESTful graphs."""

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

        # Deduplicate paths (O(N) preserve-order hash trick) to prevent duplicate HTTP requests
        if extracted_data and child_path:
            hashable_data = [x for x in extracted_data if isinstance(x, (str, int, float, bool))]
            extracted_data = list(dict.fromkeys(hashable_data))

        # Enforce REST canonical methods
        raw_method = kwargs.pop("method", kwargs.pop("http_method", "GET"))
        kwargs["http_method"] = raw_method.upper() if isinstance(raw_method, str) else "GET"

        inc_url = kwargs.get("inc_url")
        source_urls = [inc_url] if isinstance(inc_url, str) else (inc_url or [])

        # Use the Router to build POST payloads or GET queries
        if extracted_data:
            kwargs = router.resolve_declarative_routing(cls.__name__, extracted_data, source_urls, **kwargs)

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
        The Factory Assembler:
        1. Transforms data via Columnar processing.
        2. Compiles the optimal Pydantic schema cache.
        3. Instantiates the Python objects at C-speed.
        """
        if failed_sources:
            warnings.warn(
                f"Incorporator partial data returned: {len(failed_sources)} source(s) failed.",
                stacklevel=2,
            )

        if not parsed_data:
            # Generate a safe empty class if an API returns 200 OK but 0 records
            EmptyClass = cast(Type[TIncorporator], schema_builder.infer_dynamic_schema("DynamicModel", [{}], cls))
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
            Type[TIncorporator],
            schema_builder.infer_dynamic_schema("DynamicModel", transformed_data, cls),
        )

        # 3. Final Instantiation Phase
        if isinstance(transformed_data, list):
            instances = [ActualClass(**item) for item in transformed_data]
            return IncorporatorList(ActualClass, instances, failed_sources=failed_sources)

        return ActualClass(**transformed_data)

    @classmethod
    async def test(
        cls: Type[TIncorporator],
        **kwargs: Any,
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator], List[Any]]:
        """
        DX Helper: Wraps incorp() to help developers map out unknown APIs.
        Prints a tree-view of the payload, suggests optimal kwargs, and
        returns a maximum of 3 records to prevent console spam.
        """
        kwargs["__inspect"] = True

        if "inc_page" in kwargs and not kwargs.get("call_lim"):
            kwargs["call_lim"] = 1

        if "timeout" not in kwargs:
            kwargs["timeout"] = 5.0  # Fail fast!

        try:
            result = await cls.incorp(**kwargs)

        except Exception as e:
            # Defer DX logging to the Inspector module
            from .methods.inspector import analyze_error

            analyze_error(e)
            return IncorporatorList(cls, [])

        if isinstance(result, IncorporatorList):
            sliced = result[:3]
            new_list = IncorporatorList(result._model_class, sliced, result.failed_sources)
            new_list.inc_child_path = result.inc_child_path
            return new_list
        elif isinstance(result, list):
            return result[:3]

        return result

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
        """Extracts data from an API or File and returns dynamically generated Python objects."""

        # Route to Parent Execution if triggered
        if inc_parent is not None:
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

        # Auto-Infer SQLite Queries
        if source:
            sample_source = source[0] if isinstance(source, list) else source
            if infer_format(str(sample_source)) == FormatType.SQLITE and not kwargs.get("sql_query"):
                table_name = kwargs.get("sql_table") or cls.__name__.lower()
                kwargs["sql_query"] = f'SELECT * FROM "{table_name}"'  # noqa: S608

        # Unrolled for traceability (Eliminated dense one-liners)
        is_file_mode = bool(inc_file)
        source_list: List[str] = []
        if isinstance(source, list):
            source_list = [str(s) for s in source if s is not None]
        elif isinstance(source, str):
            source_list = [source]
        elif kwargs.get("payload_list"):
            source_list = [""] * len(cast(List[Any], kwargs["payload_list"]))

        is_single = not isinstance(source, list) and inc_page is None

        # Lock Root Class Origin Context
        if is_single and isinstance(source, str):
            if is_file_mode:
                cls.inc_file = source
            else:
                cls.inc_url = source

        # Extract control flags before network call so they don't pollute handlers
        __inspect = kwargs.pop("__inspect", False)
        payload_list = kwargs.pop("payload_list", None)

        # I/O Network Phase
        parsed_data, failed_sources = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=is_file_mode,
            inc_page=inc_page,
            payload_list=payload_list,
            __inspect=__inspect,
            **kwargs,
        )

        # Routes raw data to the Inspector if triggered
        if __inspect:
            from .methods.inspector import analyze_data

            analyze_data(parsed_data, {"rec_path": kwargs.get("rec_path")})

        # Build Phase
        result = await asyncio.to_thread(
            cls._build_instances,
            parsed_data,
            failed_sources,
            is_single,
            inc_code=inc_code,
            inc_name=inc_name,
            excl_lst=excl_lst,
            conv_dict=conv_dict,
            name_chg=name_chg,
        )

        # Retain parent linking instructions for potential nested refreshes
        if inc_child is not None and isinstance(result, IncorporatorList):
            result.inc_child_path = inc_child

        return result

    @classmethod
    async def refresh(
        cls: Type[TIncorporator],
        instance: Optional[Union[str, Path, TIncorporator, List[TIncorporator]]] = None,
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
        """Hydrates existing instances with new data, deduplicating HTTP execution via weakrefs."""

        target_url = new_url
        target_file = new_file

        # Unrolled instance resolution for DX traceability
        if instance is None:
            inst_list = cast(List[TIncorporator], list(cls.inc_dict.values()))
        elif isinstance(instance, (str, Path)):
            inst_list = cast(List[TIncorporator], list(cls.inc_dict.values()))
            target_str = str(instance)
            if target_str.startswith("http"):
                target_url = target_str
            else:
                target_file = target_str
        else:
            inst_list = cast(List[TIncorporator], instance if isinstance(instance, list) else [instance])

        if not inst_list:
            return IncorporatorList(cls, [])

        TargetClass = inst_list[0].__class__
        kwargs["http_method"] = kwargs.pop("method", kwargs.pop("http_method", "GET")).upper()

        child_path = inc_child or getattr(inst_list[0], "inc_child_path", None)

        # Use the Router to drill the Graph
        extracted_data = router.extract_parent_data(inst_list, child_path) if child_path else inst_list

        if child_path and extracted_data:
            hashable_data = [x for x in extracted_data if isinstance(x, (str, int, float, bool))]
            extracted_data = list(dict.fromkeys(hashable_data))

        # Target Resolution
        target = target_url or target_file
        source_urls: List[str] = []
        if isinstance(target, list):
            source_urls = [str(x) for x in target if x is not None]
        elif isinstance(target, str):
            source_urls = [target]

        if not target_file and extracted_data:
            # Use the Router to build declarative payloads
            kwargs = router.resolve_declarative_routing(cls.__name__, extracted_data, source_urls, **kwargs)
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

        # Fallback to origin URLs from memory if no new URLs were explicitly passed
        if not source_list and not target_file:
            raw_sources = [getattr(inst, "inc_url", getattr(inst, "inc_file", "")) for inst in inst_list]
            source_list = list(dict.fromkeys([str(u) for u in raw_sources if u]))
            if not source_list:
                raise ValueError(f"[{cls.__name__}] Instances contain no origin URLs to refresh from.")

        parsed_data, failed_sources = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=bool(target_file) or (not target_url and getattr(inst_list[0], "inc_file", None) is not None),
            inc_page=inc_page,
            payload_list=payload_list,
            **kwargs,
        )

        result = await asyncio.to_thread(
            cls._build_instances,
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
        instance: Union[str, Path, TIncorporator, List[TIncorporator]],
        file_path: Optional[Union[str, Path]] = None,
        format_type: Optional[FormatType] = None,
        compression: Optional[str] = None,
        sql_table: Optional[str] = None,
        if_exists: str = "replace",
        **kwargs: Any,
    ) -> None:
        """Serializes current Incorporator states out to physical files natively."""

        # Unrolled instance resolution for DX traceability
        if file_path is None:
            actual_path = str(instance)
            instances = cast(List[TIncorporator], list(cls.inc_dict.values()))
        else:
            actual_path = str(file_path)
            instances = cast(List[TIncorporator], instance if isinstance(instance, list) else [instance])

        if not instances:
            return

        active_format = format_type or infer_format(actual_path)
        kwargs.update(
            {
                "sql_table": sql_table or (cls.__name__.lower() if active_format == FormatType.SQLITE else None),
                "if_exists": if_exists,
                "pydantic_schema": instances[0].model_json_schema(),
            }
        )

        # Replaced anonymous lambda with named func for CPU Profiling visibility
        def _dump_all_to_dict() -> List[Dict[str, Any]]:
            return [obj.model_dump(by_alias=True, mode="json") for obj in instances]

        data_dicts = await asyncio.to_thread(_dump_all_to_dict)

        await format_parsers.write_destination_data(data_dicts, actual_path, active_format, **kwargs)

        if compression:
            from .methods.compression import compress_file

            await asyncio.to_thread(compress_file, actual_path, compression)

    @classmethod
    async def stream(
        cls: Type[TIncorporator],
        incorp_params: Dict[str, Any],
        refresh_params: Optional[Dict[str, Any]] = None,
        export_params: Optional[Dict[str, Any]] = None,
        poll_interval: Optional[float] = None,
        stateful_polling: bool = False,
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
    ) -> AsyncGenerator["AuditResult", None]:
        """
        Autonomous Pipeline Controller.
        Dual-Engine design supports both O(1) Memory Chunking and Stateful Graph Polling.
        """
        from .methods.pipeline import run_pipeline

        async for audit in run_pipeline(
            cls=cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            stateful_polling=stateful_polling,
            refresh_interval=refresh_interval,
            export_interval=export_interval,
        ):
            yield audit
