"""
Incorporator Base Module
========================
The core orchestrator and declarative factory for the Incorporator framework.

This file acts purely as a Domain-Driven orchestrator. It contains NO data parsing,
network looping, or schema compilation logic. It delegates to `io/`, `schema/`,
`observability/`, `factory.py`, and `list.py`, then assembles the resulting
dynamic Pydantic object graphs.
"""

import asyncio
import logging
import threading
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
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel, Field

from .io import fetch as network
from .io import handlers as format_parsers
from .io.formats import FormatType, infer_format
from .io.pagination.base import AsyncPaginator
from .schema import router
from .list import IncorporatorList, _deduplicate_extracted
from . import factory as _factory

if TYPE_CHECKING:
    from .observability.logger import AuditResult

# Type variable for strict IDE hinting on subclass generation
TIncorporator = TypeVar("TIncorporator", bound="Incorporator")
logger = logging.getLogger(__name__)

_INSPECTION_LIMIT = 3
_counter_lock = threading.Lock()


# ==========================================
# THE INCORPORATOR ENGINE
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

    # Superset schema: union of all field→JSON-schema-property-dicts seen across incorp() calls.
    # Updated lazily from raw transformed_data before Pydantic absorbs extra fields.
    # Consumed by export() for Avro (pydantic_schema) and CSV/SQLite (all_field_names).
    _schema_union: ClassVar[Dict[str, Any]] = {}

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
            with _counter_lock:
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
    # PUBLIC "HOLY TRINITY" API
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
    ) -> Union[TIncorporator, "IncorporatorList[TIncorporator]"]:
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
            return await _factory.child_incorp(cls, inc_parent=inc_parent, **kwargs)

        source = inc_file if inc_file else inc_url
        if not source and not kwargs.get("payload_list"):
            raise ValueError(
                f"[{cls.__name__}] Either 'inc_url', 'inc_file', or a valid 'inc_parent' must be provided."
            )

        # Auto-Infer SQLite Queries
        if source:
            network._inject_sqlite_query(source, kwargs.get("sql_table") or cls.__name__.lower(), kwargs)

        is_file_mode = bool(inc_file)
        source_list: List[str] = network._normalize_source_list(source, kwargs.get("payload_list"))

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
            from .inspector import analyze_data

            analyze_data(parsed_data, {"rec_path": kwargs.get("rec_path")})

        # Build Phase — runs in a thread pool to keep the event loop free
        result = await asyncio.to_thread(
            _factory.build_instances,
            cls,
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
    ) -> Union[TIncorporator, "IncorporatorList[TIncorporator]"]:
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
            logger.warning(
                "[%s] refresh() called but no instances are loaded in inc_dict — "
                "returning empty list. Did you forget to call incorp() first?",
                cls.__name__,
            )
            return IncorporatorList(cls, [])

        TargetClass = inst_list[0].__class__
        kwargs["http_method"] = kwargs.pop("method", kwargs.pop("http_method", "GET")).upper()

        child_path = inc_child or getattr(inst_list[0], "inc_child_path", None)

        # Use the Router to drill the Graph
        extracted_data = router.extract_parent_data(inst_list, child_path) if child_path else inst_list

        if child_path and extracted_data:
            extracted_data = _deduplicate_extracted(extracted_data)

        # Target Resolution
        target = target_url or target_file
        source_urls = network._normalize_source_list(target, None)

        if not target_file and extracted_data:
            # Use the Router to build declarative payloads
            kwargs = router.resolve_declarative_routing(cls.__name__, extracted_data, source_urls, **kwargs)
            raw_url = kwargs.pop("inc_url", source_urls)
            source_list: List[str] = network._normalize_source_list(raw_url, None)
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
            _factory.build_instances,
            cls,
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

    @staticmethod
    def _apply_code_transform(
        instances: List[Any],
        code_file: Union[str, Path],
    ) -> List[Any]:
        """Loads a Python file and calls its top-level ``transform(instances)`` function.

        The file must define::

            def transform(instances):
                # filter, sort, add computed fields, etc.
                return modified_instances

        If no ``transform`` function is found, ``instances`` is returned unchanged.
        Runs synchronously — callers should wrap in ``asyncio.to_thread`` for CPU-heavy transforms.

        Args:
            instances: The list of Incorporator objects to transform.
            code_file: Absolute or relative path to a ``.py`` file.

        Raises:
            FileNotFoundError: If ``code_file`` does not exist.
            ImportError: If the file cannot be loaded as a Python module.
        """
        import importlib.util

        code_path = Path(code_file).resolve()
        if not code_path.exists():
            raise FileNotFoundError(f"[Incorporator] code_file not found: {code_path}")

        spec = importlib.util.spec_from_file_location("_inc_code_transform", code_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"[Incorporator] Cannot load module spec from: {code_path}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[union-attr]

        transform_fn = getattr(module, "transform", None)
        if transform_fn is None:
            return instances

        import inspect

        sig = inspect.signature(transform_fn)
        params = list(sig.parameters)
        if len(params) != 1:
            raise ValueError(
                f"[Incorporator] transform() must accept exactly 1 parameter (instances), "
                f"got {len(params)}: {params}"
            )

        result = transform_fn(instances)
        return result if result is not None else instances

    @classmethod
    async def export(
        cls: Type[TIncorporator],
        *,
        instance: Union[str, Path, TIncorporator, List[TIncorporator]],
        file_path: Optional[Union[str, Path]] = None,
        format_type: Optional[FormatType] = None,
        compression: Optional[str] = None,
        sql_table: Optional[str] = None,
        if_exists: str = "replace",
        code_file: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        """Serializes current Incorporator states out to physical files natively.

        All parameters after ``cls`` are keyword-only (enforced by the bare ``*``).
        Supports all formats registered in ``FormatType`` and scales to 10M+ rows
        via a lazy generator: each record is serialized only when the handler
        requests the next item.

        Args:
            instance: Either a file path (when ``file_path`` is omitted, uses
                ``cls.inc_dict`` as data source) or a list of Incorporator instances.
            file_path: Destination file path. When provided, ``instance`` is the
                data source. When omitted, ``instance`` is treated as the file path.
            format_type: Override the format inferred from the file extension.
            compression: Optional compression to apply after writing (e.g. ``"gz"``).
            sql_table: Table name for SQLite exports. Defaults to the class name.
            if_exists: ``"replace"`` (default), ``"append"``, or ``"fail"``.
            code_file: Path to a ``.py`` file defining ``transform(instances)``.
                Called with the in-state object list before serialization; the
                return value is exported instead. Passes through ``export_params``
                in ``stream()`` naturally.
            **kwargs: Forwarded to the format handler.
        """
        # Unrolled instance resolution for DX traceability
        if file_path is None:
            actual_path = str(instance)
            instances: List[TIncorporator] = cast(List[TIncorporator], list(cls.inc_dict.values()))
        else:
            actual_path = str(file_path)
            if not isinstance(instance, list):
                from pydantic import BaseModel

                if not isinstance(instance, BaseModel):
                    raise TypeError(
                        f"export() 'instance' must be an Incorporator instance or a list, "
                        f"got {type(instance).__name__!r}. "
                        "Pass a list returned by incorp(), or omit file_path to use in-state export."
                    )
            instances = cast(
                List[TIncorporator],
                instance if isinstance(instance, list) else [instance],
            )

        if not instances:
            return

        # Optional code transform — runs in a thread (user code may be CPU-bound).
        transform_source: Iterable[Any] = instances
        code_file_field_names: Optional[List[str]] = None
        if code_file is not None:
            transform_source = await asyncio.to_thread(cls._apply_code_transform, instances, code_file)
            # Peek at the first transformed row so we can rebuild all_field_names from the
            # *actual* output schema — code_file may add, remove, or rename fields.
            import itertools as _itertools

            _transform_iter = iter(transform_source)
            _first_row = next(_transform_iter, None)
            if _first_row is not None:
                if isinstance(_first_row, dict):
                    code_file_field_names = list(_first_row.keys())
                elif hasattr(_first_row, "model_dump"):
                    code_file_field_names = list(
                        _first_row.model_dump(by_alias=True, mode="json").keys()
                    )
                transform_source = _itertools.chain([_first_row], _transform_iter)

        active_format = format_type or infer_format(actual_path)
        kwargs.update(
            {
                "sql_table": sql_table or (cls.__name__.lower() if active_format == FormatType.SQLITE else None),
                "if_exists": if_exists,
                "pydantic_schema": {"properties": cls._schema_union},
                "all_field_names": code_file_field_names or list(cls._schema_union.keys()) or None,
            }
        )

        # Lazy serialization generator: model_dump() is called per-object only when
        # the handler requests the next row — no full-list copy in RAM.
        # Handles both Incorporator objects (model_dump) and raw dicts from code_file transforms.
        def _make_lazy_iter(source: Iterable[Any]) -> Iterable[Dict[str, Any]]:
            for obj in source:
                if isinstance(obj, Incorporator):
                    yield obj.model_dump(by_alias=True, mode="json")
                else:
                    yield dict(obj)

        await format_parsers.write_destination_data(
            _make_lazy_iter(transform_source), actual_path, active_format, **kwargs
        )

        if compression:
            from .io.compression import compress_file

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
        from .observability.pipeline import run_pipeline

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

    @classmethod
    async def test(
        cls: Type[TIncorporator],
        **kwargs: Any,
    ) -> Union[TIncorporator, "IncorporatorList[TIncorporator]", List[Any]]:
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
            from .inspector import analyze_error

            analyze_error(e)
            return IncorporatorList(cls, [])

        if isinstance(result, IncorporatorList):
            sliced = result[:_INSPECTION_LIMIT]
            new_list = IncorporatorList(result._model_class, sliced, result.failed_sources)
            new_list.inc_child_path = result.inc_child_path
            return new_list
        elif isinstance(result, list):
            return result[:_INSPECTION_LIMIT]

        return result
