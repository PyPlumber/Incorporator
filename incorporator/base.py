"""The core super class and declarative factory for Incorporator."""

import asyncio
import weakref
from datetime import datetime, timezone
from typing import (
    Any, Callable, ClassVar, Dict, Generic, List, Optional,
    Tuple, Type, TypeVar, Union, cast
)

from pydantic import BaseModel, Field

from .methods import network, format_parsers, schema_builder
from .methods.format_parsers import FormatType

TIncorporator = TypeVar("TIncorporator", bound="Incorporator")


class IncorporatorList(list[TIncorporator]):
    """A specialized list that provides direct access to the dynamic class registry."""

    def __init__(self, model_class: Type[TIncorporator], items: List[TIncorporator]):
        super().__init__(items)
        self._model_class = model_class

    @property
    def codeDict(self) -> "weakref.WeakValueDictionary[Any, TIncorporator]":
        """Provides direct access to the class-level weakref registry."""
        return cast("weakref.WeakValueDictionary[Any, TIncorporator]", self._model_class.codeDict)


def _infer_format(path_or_url: str) -> FormatType:
    """Helper to auto-detect format from a file extension or URL."""
    path_lower = path_or_url.lower()
    if path_lower.endswith(".csv"):
        return FormatType.CSV
    if path_lower.endswith(".xml"):
        return FormatType.XML
    return FormatType.JSON


class Incorporator(BaseModel):
    """The Incorporator Super Class and Dynamic Class Building Engine."""

    # --- Class-Level Registries & Origin Tracking ---
    codeDict: ClassVar[weakref.WeakValueDictionary[Any, "Incorporator"]] = weakref.WeakValueDictionary()
    _auto_counter: ClassVar[int] = 1

    url: ClassVar[Optional[str]] = None
    file: ClassVar[Optional[str]] = None

    # --- Universal Instance Attributes ---
    code: Any = Field(default=None, description="Simple key for cls.codeDict.")
    name: Optional[str] = Field(default=None, description="Optional name for the instance.")
    last_rcd: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="The exact timestamp this instance was instantiated."
    )

    # --- Inherited Instance Methods ---
    def display(self) -> None:
        """Prints a structured, meta-like representation of the instance."""
        cls_name = getattr(self.__class__, '__name__', 'UnknownClass')
        print(
            f'class:"{cls_name}", '
            f'code:"{getattr(self, "code", None)}", '
            f'name:"{getattr(self, "name", None)}", '
            f'last_rcd:"{getattr(self, "last_rcd", None)}"'
        )

    # --- Lifecycle Hooks ---
    def model_post_init(self, __context: Any) -> None:
        """Registers instance to codeDict and increments auto_counter if code is missing."""
        cls = self.__class__
        if self.code is None:
            self.code = cls._auto_counter
            cls._auto_counter += 1
        cls.codeDict[self.code] = self

    # --- The Holy Trinity API (Declarative Factories) ---
    @classmethod
    async def incorp(
            cls: Type[TIncorporator],
            url: Optional[Union[str, List[str]]] = None,
            file: Optional[Union[str, List[str]]] = None,
            rPath: Optional[str] = None,
            code: Optional[str] = None,
            name: Optional[str] = None,
            static_dct: Optional[Dict[str, Any]] = None,
            excl_lst: Optional[List[str]] = None,
            conv_dict: Optional[Dict[str, Callable[[Any], Any]]] = None,
            name_chg: Optional[List[Tuple[str, str]]] = None,
            format_type: Optional[FormatType] = None,
            paginate: bool = False,
            next_url_extractor: Optional[Callable[[str], Optional[str]]] = None
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """Declarative factory to fetch data and generate a mapped Incorporator subclass."""

        # ==========================================
        # NATIVE CONCURRENCY ENGINE
        # ==========================================
        if isinstance(url, list) or isinstance(file, list):
            tasks = []
            if isinstance(url, list):
                tasks = [
                    cls.incorp(
                        url=u, rPath=rPath, code=code, name=name, static_dct=static_dct,
                        excl_lst=excl_lst, conv_dict=conv_dict, name_chg=name_chg,
                        format_type=format_type, paginate=paginate, next_url_extractor=next_url_extractor
                    ) for u in url
                ]
            elif isinstance(file, list):
                tasks = [
                    cls.incorp(
                        file=f, rPath=rPath, code=code, name=name, static_dct=static_dct,
                        excl_lst=excl_lst, conv_dict=conv_dict, name_chg=name_chg,
                        format_type=format_type, paginate=paginate, next_url_extractor=next_url_extractor
                    ) for f in file
                ]

            results = await asyncio.gather(*tasks)

            flat_results: List[TIncorporator] = []
            for res in results:
                if isinstance(res, list):
                    flat_results.extend(res)
                else:
                    flat_results.append(res)  # Mypy correctly narrows this to TIncorporator!

            if not flat_results:
                raise ValueError("No data returned from concurrent execution.")

            ActualClass = flat_results[0].__class__
            return IncorporatorList(ActualClass, flat_results)

        # ==========================================
        # STANDARD PIPELINE
        # ==========================================
        source = file if file else url
        if not source:
            raise ValueError("Either 'url' or 'file' must be provided.")

        if not isinstance(source, str):
            raise ValueError("Source must be a string at this point in the pipeline.")

        if file:
            cls.file = source
        else:
            cls.url = source

        active_format = format_type or _infer_format(source)

        # 1. FETCH & PARSE
        accumulated_data: List[Any] = []
        is_single_object = False

        async for raw_text in network.stream_raw_data(
                source=source,
                is_file=bool(file),
                paginate=paginate,
                next_url_extractor=next_url_extractor
        ):
            parsed_chunk = await format_parsers.parse_source_data(raw_text, active_format)

            if rPath:
                for part in rPath.split('.'):
                    if isinstance(parsed_chunk, dict) and part in parsed_chunk:
                        parsed_chunk = parsed_chunk[part]
                    else:
                        break

            if isinstance(parsed_chunk, list):
                accumulated_data.extend(parsed_chunk)
            else:
                accumulated_data.append(parsed_chunk)
                if not paginate:
                    is_single_object = True

        parsed_data = accumulated_data[0] if is_single_object and len(accumulated_data) == 1 else accumulated_data

        if not parsed_data:
            raise ValueError("No data could be extracted from the source.")

        # 2. TRANSFORM (The ETL Engine)
        transformed_data = schema_builder.apply_etl_transformations(
            parsed_data=parsed_data,
            code_attr=code,
            name_attr=name,
            static_dct=static_dct,
            excl_lst=excl_lst,
            conv_dict=conv_dict,
            name_chg=name_chg
        )

        # 3. BUILD SCHEMA
        # We cast the returned Type[BaseModel] back into our expected Incorporator Subclass Type
        ActualClass = cast(Type[TIncorporator],
                           schema_builder.infer_dynamic_schema("DynamicModel", transformed_data, cls))

        # 4. INSTANTIATE AND RETURN
        if isinstance(transformed_data, list):
            instances = [ActualClass(**item) for item in transformed_data]
            return IncorporatorList(ActualClass, instances)

        return ActualClass(**transformed_data)

    @classmethod
    async def refresh(
            cls: Type[TIncorporator],
            instance: Union[TIncorporator, List[TIncorporator]],
            new_url: Optional[Union[str, List[str]]] = None,
            new_file: Optional[Union[str, List[str]]] = None,
            format_type: Optional[FormatType] = None,
            rPath: Optional[str] = None,
            paginate: bool = False,
            next_url_extractor: Optional[Callable[[str], Optional[str]]] = None
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """Hydrates an existing Incorporator subclass instance with new data."""

        # ==========================================
        # NATIVE CONCURRENCY ENGINE
        # ==========================================
        if isinstance(new_url, list) or isinstance(new_file, list) or (
                isinstance(instance, list) and not new_url and not new_file):
            tasks = []
            inst_list = instance if isinstance(instance, list) else [instance]

            if isinstance(new_url, list):
                tasks = [
                    cls.refresh(
                        instance=inst, new_url=u, format_type=format_type, rPath=rPath,
                        paginate=paginate, next_url_extractor=next_url_extractor
                    ) for inst, u in zip(inst_list * len(new_url), new_url)
                ]
            elif isinstance(new_file, list):
                tasks = [
                    cls.refresh(
                        instance=inst, new_file=f, format_type=format_type, rPath=rPath,
                        paginate=paginate, next_url_extractor=next_url_extractor
                    ) for inst, f in zip(inst_list * len(new_file), new_file)
                ]
            else:
                tasks = [
                    cls.refresh(
                        instance=inst, format_type=format_type, rPath=rPath,
                        paginate=paginate, next_url_extractor=next_url_extractor
                    ) for inst in inst_list
                ]

            results = await asyncio.gather(*tasks)
            flat_results: List[TIncorporator] = []
            for res in results:
                if isinstance(res, list):
                    flat_results.extend(res)
                else:
                    flat_results.append(res)

            if not flat_results:
                raise ValueError("No data returned from concurrent refresh.")

            ActualClass = flat_results[0].__class__
            return IncorporatorList(ActualClass, flat_results)

        # ==========================================
        # STANDARD PIPELINE
        # ==========================================
        TargetClass = instance[0].__class__ if isinstance(instance, list) else instance.__class__
        active_url = new_url or getattr(TargetClass, "url", None)
        active_file = new_file or getattr(TargetClass, "file", None)

        source = active_file if active_file else active_url
        if not source:
            raise ValueError("No valid origin to refresh from. Provide new_url or new_file.")

        if not isinstance(source, str):
            raise ValueError("Source must be a string at this point in the pipeline.")

        active_format = format_type or _infer_format(source)

        # 1. FETCH & PARSE
        accumulated_data: List[Any] = []
        is_single_object = False

        async for raw_text in network.stream_raw_data(
                source=source,
                is_file=bool(active_file),
                paginate=paginate,
                next_url_extractor=next_url_extractor
        ):
            parsed_chunk = await format_parsers.parse_source_data(raw_text, active_format)

            if rPath:
                for part in rPath.split('.'):
                    if isinstance(parsed_chunk, dict) and part in parsed_chunk:
                        parsed_chunk = parsed_chunk[part]
                    else:
                        break

            if isinstance(parsed_chunk, list):
                accumulated_data.extend(parsed_chunk)
            else:
                accumulated_data.append(parsed_chunk)
                if not paginate:
                    is_single_object = True

        parsed_data = accumulated_data[0] if is_single_object and len(accumulated_data) == 1 else accumulated_data

        # 2. REHYDRATE SCHEMA
        if isinstance(parsed_data, list):
            instances = [TargetClass(**item) for item in parsed_data]
            return IncorporatorList(TargetClass, instances)

        return TargetClass(**parsed_data)

    @classmethod
    async def export(
            cls: Type[TIncorporator],
            instance: Union[TIncorporator, List[TIncorporator]],
            file_path: str,
            format_type: Optional[FormatType] = None
    ) -> None:
        """Exports Incorporator instances out to a local JSON, CSV, or XML file."""

        active_format = format_type or _infer_format(file_path)
        instances = instance if isinstance(instance, list) else [instance]

        # Extract dictionaries using Pydantic's native dump, respecting our dynamic aliases
        data_dicts = [obj.model_dump(by_alias=True, mode='json') for obj in instances]

        # Await the background-threaded writer
        await format_parsers.write_destination_data(data_dicts, file_path, active_format)