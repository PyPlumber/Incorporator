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
from typing import (
    Any, ClassVar, Dict, List, Optional, Type, TypeVar, Union, cast
)

from pydantic import BaseModel, Field

from .methods import format_parsers, network, schema_builder
from .methods.format_parsers import FormatType, infer_format

TIncorporator = TypeVar("TIncorporator", bound="Incorporator")
logger = logging.getLogger(__name__)


# ==========================================
# 1. LIST WRAPPER & REGISTRY ACCESS
# ==========================================
class IncorporatorList(list[TIncorporator]):
    """
    A specialized list providing direct access to the dynamic class registry.

    When `incorp()` returns multiple items, this wrapper allows users to run
    `dataset.codeDict.get(id)` seamlessly against the dynamically generated class.
    """

    # Exposes failed HTTP 429 URLs for programmatic Dead Letter Queue retries
    failed_sources: List[str]

    def __init__(
            self,
            model_class: Type[TIncorporator],
            items: List[TIncorporator],
            failed_sources: Optional[List[str]] = None
    ):
        super().__init__(items)
        self._model_class = model_class
        self.failed_sources = failed_sources if failed_sources is not None else []

    @property
    def codeDict(self) -> "weakref.WeakValueDictionary[Any, TIncorporator]":
        """Provides direct access to the class-level weakref registry."""
        return cast("weakref.WeakValueDictionary[Any, TIncorporator]", self._model_class.codeDict)


# ==========================================
# 2. THE INCORPORATOR ENGINE
# ==========================================
class Incorporator(BaseModel):
    """
    The Incorporator Super Class.
    Inherits from Pydantic V2 BaseModel to leverage metaprogramming.
    """

    # --- Class-Level Registries & Origin Tracking ---
    codeDict: ClassVar[weakref.WeakValueDictionary[Any, "Incorporator"]] = weakref.WeakValueDictionary()
    _auto_counter: ClassVar[int] = 1

    inc_url: ClassVar[Optional[str]] = None
    inc_file: ClassVar[Optional[str]] = None

    # --- Universal Instance Attributes ---
    inc_code: Any = Field(default=None, description="Primary key for cls.codeDict.")
    inc_name: Optional[str] = Field(default=None, description="Optional readable name.")
    last_rcd: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="The exact UTC timestamp this object was instantiated."
    )

    def display(self) -> None:
        """Utility method to quickly print core instance identity."""
        cls_name = getattr(self.__class__, '__name__', 'UnknownClass')
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

        # 1. Assign auto-incrementing ID if none was provided by the API
        if self.inc_code is None:
            self.inc_code = cls._auto_counter
            cls._auto_counter += 1

        # 2. Register the instance in its own dynamically generated Subclass registry
        cls.codeDict[self.inc_code] = self

        # 3. Bubble up to explicitly defined parent classes (e.g. NHTSARecord.codeDict)
        for base in cls.__bases__:
            if issubclass(base, Incorporator) and base is not Incorporator:
                base.codeDict[self.inc_code] = self

    # ==========================================
    # 3. INTERNAL ROUTERS & FACTORIES
    # ==========================================
    @classmethod
    async def _child_incorp(
            cls: Type[TIncorporator],
            inc_parent: Any,
            **kwargs: Any
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """
        Handles HATEOAS REST architectures by extracting nested URLs
        from parent Incorporator objects and dynamically routing them.
        """
        parent_items = inc_parent if isinstance(inc_parent, list) else [inc_parent]
        discovered_urls = [
            url_val for item in parent_items
            if (url_val := getattr(item, 'detail_url', getattr(item, 'url', None))) and isinstance(url_val, str)
        ]

        if not discovered_urls:
            raise ValueError("The 'inc_parent' object did not contain a valid 'url' or 'detail_url' attribute.")

        # Re-route the discovered URLs back into the main pipeline
        kwargs['inc_url'] = discovered_urls
        kwargs.pop('inc_parent', None)
        return await cls.incorp(**kwargs)

    @classmethod
    def _build_instances(
            cls: Type[TIncorporator],
            parsed_data: List[Any],
            failed_sources: List[str],
            is_single: bool,
            **kwargs: Any
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """
        The Factory Assembler: Applies ETL transformations to raw dictionaries,
        compiles the dynamic Pydantic schema, and instantiates the final objects.
        """
        # Warn developers if a massive concurrent batch encountered 429 Rate Limits
        if failed_sources:
            warnings.warn(
                f"Incorporator partial data returned: {len(failed_sources)} source(s) failed with HTTP 429.",
                UserWarning, stacklevel=2
            )

        # Graceful degradation for completely empty responses
        if not parsed_data:
            EmptyClass = cast(Type[TIncorporator], schema_builder.infer_dynamic_schema("DynamicModel", [{}], cls))
            return IncorporatorList(EmptyClass, [], failed_sources=failed_sources)

        if is_single and len(parsed_data) == 1:
            parsed_data = parsed_data[0]

        # 1. Declarative ETL Transformation Phase
        transformed_data = schema_builder.apply_etl_transformations(
            parsed_data=parsed_data,
            code_attr=kwargs.get('inc_code'),
            name_attr=kwargs.get('inc_name'),
            excl_lst=kwargs.get('excl_lst'),
            conv_dict=kwargs.get('conv_dict'),
            name_chg=kwargs.get('name_chg')
        )

        # 2. Metaprogramming Compilation Phase
        ActualClass = cast(
            Type[TIncorporator],
            schema_builder.infer_dynamic_schema("DynamicModel", transformed_data, cls)
        )

        # 3. Final Object Instantiation Phase
        if isinstance(transformed_data, list):
            instances = [ActualClass(**cast(Dict[str, Any], item)) for item in transformed_data]
            return IncorporatorList(ActualClass, instances, failed_sources=failed_sources)

        return ActualClass(**cast(Dict[str, Any], transformed_data))

    # ==========================================
    # 4. PUBLIC "HOLY TRINITY" API
    # ==========================================
    @classmethod
    async def incorp(
            cls: Type[TIncorporator],
            inc_url: Optional[Union[str, List[str]]] = None,
            inc_file: Optional[Union[str, List[str]]] = None,
            inc_parent: Optional[Union[TIncorporator, "IncorporatorList[TIncorporator]"]] = None,
            **kwargs: Any
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """Extracts data from an API or File and returns dynamically generated Python objects."""

        # 1. HATEOAS Extraction Intercept
        if inc_parent is not None:
            return await cls._child_incorp(inc_parent=inc_parent, **kwargs)

        source = inc_file if inc_file else inc_url
        if not source:
            raise ValueError("Either 'inc_url' or 'inc_file' must be provided.")

        is_file_mode = bool(inc_file)
        source_list = source if isinstance(source, list) else [source]
        is_single = not isinstance(source, list) and not kwargs.get("paginate", False)

        # Track the active origin class-wide for single operations
        if is_single and isinstance(source, str):
            if is_file_mode:
                cls.inc_file = source
            else:
                cls.inc_url = source

        # 2. Concurrency & I/O Phase (Delegated to network.py)
        parsed_data, failed_sources = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=is_file_mode,
            **kwargs
        )

        # 3. Metaprogramming & Instantiation Phase
        return cls._build_instances(parsed_data, failed_sources, is_single, **kwargs)

    @classmethod
    async def refresh(
            cls: Type[TIncorporator],
            instance: Union[TIncorporator, List[TIncorporator]],
            new_url: Optional[Union[str, List[str]]] = None,
            new_file: Optional[Union[str, List[str]]] = None,
            **kwargs: Any
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """Re-hydrates existing instances with fresh API data using their origin tracking."""

        inst_list = instance if isinstance(instance, list) else [instance]
        if not inst_list:
            raise ValueError("Cannot refresh an empty list of Incorporator instances.")

        # Resolve target source (Fallback to the origin URL tracked by the instance)
        target = new_url if new_url else new_file if new_file else None
        if not target:
            target = [getattr(inst, "inc_url", getattr(inst, "inc_file", "")) for inst in inst_list]

        is_file_mode = new_file is not None or (not new_url and getattr(inst_list[0], "inc_file", None) is not None)
        source_list = target if isinstance(target, list) else [target]
        is_single = not isinstance(target, list) and not kwargs.get("paginate", False)

        # 1. Concurrency & I/O Phase
        parsed_data, failed_sources = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=is_file_mode,
            **kwargs
        )

        TargetClass = inst_list[0].__class__
        if failed_sources:
            warnings.warn(f"Refresh partial data: {len(failed_sources)} source(s) failed.", UserWarning, stacklevel=2)

        if is_single and len(parsed_data) == 1:
            parsed_data = parsed_data[0]

        # 2. Hydration Phase
        if isinstance(parsed_data, list):
            instances = [TargetClass(**cast(Dict[str, Any], item)) for item in parsed_data]
            return IncorporatorList(TargetClass, instances, failed_sources=failed_sources)

        return TargetClass(**cast(Dict[str, Any], parsed_data))

    @classmethod
    async def export(
            cls: Type[TIncorporator],
            instance: Union[TIncorporator, List[TIncorporator]],
            file_path: str,
            format_type: Optional[FormatType] = None
    ) -> None:
        """Serializes current Incorporator states out to physical JSON/CSV/XML files."""
        active_format = format_type or infer_format(file_path)
        instances = instance if isinstance(instance, list) else [instance]

        # Dump state utilizing Pydantic's optimized JSON alias tracking
        data_dicts = [obj.model_dump(by_alias=True, mode='json') for obj in instances]

        await format_parsers.write_destination_data(data_dicts, file_path, active_format)