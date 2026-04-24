"""The core super class and declarative factory for Incorporator."""

import asyncio
import logging
import re
import warnings
import weakref
from datetime import datetime, timezone
from typing import (
    Any, Callable, ClassVar, Coroutine, Dict, Generic, List, Optional,
    Tuple, Type, TypeVar, Union, cast
)

import httpx
from pydantic import BaseModel, Field

from .methods import format_parsers, network, schema_builder
from .methods.exceptions import IncorporatorNetworkError
from .methods.format_parsers import FormatType
from .methods.network import RateLimiter

TIncorporator = TypeVar("TIncorporator", bound="Incorporator")
logger = logging.getLogger(__name__)


class IncorporatorList(list[TIncorporator]):
    """A specialized list providing direct access to the dynamic class registry and error tracking."""

    # Expose failed sources programmatically for automated Dead Letter Queues
    failed_sources: List[str]

    def __init__(self, model_class: Type[TIncorporator], items: List[TIncorporator],
                 failed_sources: Optional[List[str]] = None):
        super().__init__(items)
        self._model_class = model_class
        self.failed_sources = failed_sources if failed_sources is not None else []

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


class _AutoURLPaginator:
    """Stateful heuristic paginator that invisibly increments page/offset counters in URLs."""

    def __init__(self, start_url: str):
        self._last_url = start_url

    def __call__(self, raw_text: str) -> Optional[str]:
        cleaned = raw_text.strip()
        if not cleaned or cleaned in ("[]", "{}"):
            return None

        if re.search(r'"(?:results|data|items|response|items)"\s*:\s*\[\s*\]', cleaned, re.IGNORECASE):
            return None

        match = re.search(r'([?&])(page|p|offset|start)=(\d+)', self._last_url, re.IGNORECASE)
        if not match:
            return None

        param = match.group(2)
        val = int(match.group(3))

        increment = 1
        if param.lower() in ('offset', 'start'):
            limit_match = re.search(r'[?&](limit|per_page|count)=(\d+)', self._last_url, re.IGNORECASE)
            if limit_match:
                increment = int(limit_match.group(2))
            else:
                increment = 5

        new_val = val + increment
        new_url = self._last_url[:match.start(3)] + str(new_val) + self._last_url[match.end(3):]
        self._last_url = new_url
        return new_url


class Incorporator(BaseModel):
    """The Incorporator Super Class and Dynamic Class Building Engine."""

    # --- Class-Level Registries & Origin Tracking ---
    codeDict: ClassVar[weakref.WeakValueDictionary[Any, "Incorporator"]] = weakref.WeakValueDictionary()
    _auto_counter: ClassVar[int] = 1

    inc_url: ClassVar[Optional[str]] = None
    inc_file: ClassVar[Optional[str]] = None

    # --- Universal Instance Attributes ---
    inc_code: Any = Field(default=None, description="Simple key for cls.codeDict.")
    inc_name: Optional[str] = Field(default=None, description="Optional name for the instance.")
    last_rcd: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="The exact timestamp this instance was instantiated."
    )

    def display(self) -> None:
        cls_name = getattr(self.__class__, '__name__', 'UnknownClass')
        print(
            f'class:"{cls_name}", '
            f'inc_code:"{getattr(self, "inc_code", None)}", '
            f'inc_name:"{getattr(self, "inc_name", None)}", '
            f'last_rcd:"{getattr(self, "last_rcd", None)}"'
        )

    def model_post_init(self, __context: Any) -> None:
        cls = self.__class__
        if self.inc_code is None:
            self.inc_code = cls._auto_counter
            cls._auto_counter += 1
        cls.codeDict[self.inc_code] = self

    @classmethod
    async def incorp(
            cls: Type[TIncorporator],
            inc_url: Optional[Union[str, List[str]]] = None,
            inc_file: Optional[Union[str, List[str]]] = None,
            inc_parent: Optional[Union[TIncorporator, "IncorporatorList[TIncorporator]"]] = None,
            rec_path: Optional[str] = None,
            inc_code: Optional[str] = None,
            inc_name: Optional[str] = None,
            excl_lst: Optional[List[str]] = None,
            conv_dict: Optional[Dict[str, Callable[[Any], Any]]] = None,
            name_chg: Optional[List[Tuple[str, str]]] = None,
            format_type: Optional[FormatType] = None,
            paginate: bool = False,
            next_url_extractor: Optional[Callable[[str], Optional[str]]] = None,
            call_lim: Optional[int] = None,
            concurrency_limit: Optional[int] = 25,
            delay_between_batches: float = 0.0,
            requests_per_second: float = 15.0,
            ignore_ssl: bool = False,
            _client: Optional[httpx.AsyncClient] = None,
            _rate_limiter: Optional[RateLimiter] = None
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:

        if inc_parent is not None:
            parent_items = inc_parent if isinstance(inc_parent, list) else [inc_parent]
            discovered_urls = [
                url_val for item in parent_items
                if (url_val := getattr(item, 'detail_url', getattr(item, 'url', None))) and isinstance(url_val, str)
            ]

            if not discovered_urls:
                raise ValueError("The 'inc_parent' object did not contain a valid 'url' or 'detail_url' attribute.")

            return await cls.incorp(
                inc_url=discovered_urls, rec_path=rec_path, inc_code=inc_code, inc_name=inc_name,
                excl_lst=excl_lst, conv_dict=conv_dict, name_chg=name_chg, format_type=format_type,
                paginate=paginate, next_url_extractor=next_url_extractor, call_lim=call_lim,
                concurrency_limit=concurrency_limit, delay_between_batches=delay_between_batches,
                requests_per_second=requests_per_second, ignore_ssl=ignore_ssl,
                _client=_client, _rate_limiter=_rate_limiter
            )

        # ==========================================
        # NATIVE CONCURRENCY ENGINE
        # ==========================================
        if isinstance(inc_url, list) or isinstance(inc_file, list):

            failed_sources: List[str] = []

            async def run_with_semaphore(semaphore: asyncio.Semaphore, coro: Coroutine[Any, Any, Any]) -> Any:
                async with semaphore:
                    return await coro

            async def _fetch_resiliently(coro: Coroutine[Any, Any, Any], source_id: str) -> Optional[Any]:
                try:
                    return await coro
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        logger.warning(f"Request failed with status 429. Skipping: {source_id}")
                        failed_sources.append(source_id)
                        return None
                    raise IncorporatorNetworkError(f"HTTP error {e.response.status_code}") from e

            source_list = inc_url if isinstance(inc_url, list) else cast(List[str], inc_file)
            is_file_mode = isinstance(inc_file, list)

            limit = concurrency_limit if concurrency_limit is not None else 50
            semaphore = asyncio.Semaphore(limit)

            should_close_client = False
            if not is_file_mode and _client is None:
                client_limits = httpx.Limits(max_keepalive_connections=limit, max_connections=limit)
                _client = httpx.AsyncClient(follow_redirects=True, timeout=15.0, limits=client_limits,
                                            verify=not ignore_ssl)
                _rate_limiter = RateLimiter(requests_per_second)
                should_close_client = True

            try:
                accumulated_results: List[TIncorporator] = []
                chunks = [source_list[i:i + limit] for i in range(0, len(source_list), limit)]

                for i, chunk in enumerate(chunks):
                    resilient_tasks: List[Coroutine[Any, Any, Any]] = []

                    if is_file_mode:
                        for f in chunk:
                            task = cls.incorp(
                                inc_file=f, rec_path=rec_path, inc_code=inc_code, inc_name=inc_name,
                                excl_lst=excl_lst, conv_dict=conv_dict, name_chg=name_chg,
                                format_type=format_type, paginate=paginate, next_url_extractor=next_url_extractor,
                                call_lim=call_lim, concurrency_limit=1, _client=_client,
                                _rate_limiter=_rate_limiter, ignore_ssl=ignore_ssl
                            )
                            resilient_tasks.append(_fetch_resiliently(run_with_semaphore(semaphore, task), str(f)))
                    else:
                        for u in chunk:
                            task = cls.incorp(
                                inc_url=u, rec_path=rec_path, inc_code=inc_code, inc_name=inc_name,
                                excl_lst=excl_lst, conv_dict=conv_dict, name_chg=name_chg,
                                format_type=format_type, paginate=paginate, next_url_extractor=next_url_extractor,
                                call_lim=call_lim, concurrency_limit=1, _client=_client,
                                _rate_limiter=_rate_limiter, ignore_ssl=ignore_ssl
                            )
                            resilient_tasks.append(_fetch_resiliently(run_with_semaphore(semaphore, task), str(u)))

                    chunk_results = await asyncio.gather(*resilient_tasks)

                    for res in chunk_results:
                        if res is None:
                            continue
                        if isinstance(res, list):
                            accumulated_results.extend(res)
                        else:
                            accumulated_results.append(res)

                    if delay_between_batches > 0.0 and i < len(chunks) - 1:
                        await asyncio.sleep(delay_between_batches)

                # TRIGGER SYSTEMIC AND TERMINAL WARNINGS FOR PARTIAL DATA
                if failed_sources:
                    warnings.warn(
                        f"Incorporator partial data returned: {len(failed_sources)} source(s) failed with HTTP 429 (Too Many Requests). "
                        f"Check the '.failed_sources' attribute on the returned list.",
                        UserWarning, stacklevel=2
                    )

                if not accumulated_results:
                    ActualClass = cast(Type[TIncorporator],
                                       schema_builder.infer_dynamic_schema("DynamicModel", [{}], cls))
                    return IncorporatorList(ActualClass, [], failed_sources=failed_sources)

                ActualClass = accumulated_results[0].__class__
                return IncorporatorList(ActualClass, accumulated_results, failed_sources=failed_sources)
            finally:
                if should_close_client and _client is not None:
                    await _client.aclose()

        # ==========================================
        # STANDARD PIPELINE
        # ==========================================
        source = inc_file if inc_file else inc_url
        if not source:
            raise ValueError("Either 'inc_url' or 'inc_file' must be provided.")

        if not isinstance(source, str):
            raise ValueError("Source must be a string at this point in the pipeline.")

        if inc_file:
            cls.inc_file = source
        else:
            cls.inc_url = source

        active_format = format_type or _infer_format(source)

        active_extractor = next_url_extractor
        if paginate and not active_extractor and not inc_file:
            active_extractor = _AutoURLPaginator(source)

        accumulated_data: List[Any] = []
        is_single_object = False

        async for raw_text in network.stream_raw_data(
                source=source, is_file=bool(inc_file), paginate=paginate,
                next_url_extractor=active_extractor, call_lim=call_lim,
                client=_client, rate_limiter=_rate_limiter, ignore_ssl=ignore_ssl
        ):
            parsed_chunk = await format_parsers.parse_source_data(raw_text, active_format)

            if rec_path:
                for part in rec_path.split('.'):
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

        transformed_data = schema_builder.apply_etl_transformations(
            parsed_data=parsed_data, code_attr=inc_code, name_attr=inc_name,
            excl_lst=excl_lst, conv_dict=conv_dict, name_chg=name_chg
        )

        ActualClass = cast(Type[TIncorporator],
                           schema_builder.infer_dynamic_schema("DynamicModel", transformed_data, cls))

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
            rec_path: Optional[str] = None,
            paginate: bool = False,
            next_url_extractor: Optional[Callable[[str], Optional[str]]] = None,
            call_lim: Optional[int] = None,
            concurrency_limit: Optional[int] = 25,
            delay_between_batches: float = 0.0,
            requests_per_second: float = 15.0,
            ignore_ssl: bool = False,
            _client: Optional[httpx.AsyncClient] = None,
            _rate_limiter: Optional[RateLimiter] = None
    ) -> Union[TIncorporator, IncorporatorList[TIncorporator]]:
        """Hydrates an existing Incorporator subclass instance with new data."""

        if isinstance(new_url, list) or isinstance(new_file, list) or (
                isinstance(instance, list) and not new_url and not new_file):

            failed_sources: List[str] = []

            async def run_with_semaphore(semaphore: asyncio.Semaphore, coro: Coroutine[Any, Any, Any]) -> Any:
                async with semaphore:
                    return await coro

            async def _fetch_resiliently(coro: Coroutine[Any, Any, Any], source_id: str) -> Optional[Any]:
                try:
                    return await coro
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        logger.warning(f"Refresh failed with status 429. Skipping: {source_id}")
                        failed_sources.append(source_id)
                        return None
                    raise IncorporatorNetworkError(f"HTTP error {e.response.status_code}") from e

            inst_list = instance if isinstance(instance, list) else [instance]
            source_list = new_url if isinstance(new_url, list) else new_file if isinstance(new_file, list) else None

            limit = concurrency_limit if concurrency_limit is not None else 50
            semaphore = asyncio.Semaphore(limit)

            should_close_client = False
            if not isinstance(new_file, list) and _client is None:
                client_limits = httpx.Limits(max_keepalive_connections=limit, max_connections=limit)
                _client = httpx.AsyncClient(follow_redirects=True, timeout=15.0, limits=client_limits,
                                            verify=not ignore_ssl)
                _rate_limiter = RateLimiter(requests_per_second)
                should_close_client = True

            try:
                accumulated_results: List[TIncorporator] = []
                target_list_for_chunks = source_list if source_list else inst_list
                if not target_list_for_chunks:
                    return IncorporatorList(cls, [])

                chunks = [target_list_for_chunks[i:i + limit] for i in range(0, len(target_list_for_chunks), limit)]

                for i, chunk_data in enumerate(chunks):
                    resilient_tasks: List[Coroutine[Any, Any, Any]] = []

                    if isinstance(new_url, list):
                        chunk_instances = inst_list * len(chunk_data)
                        for inst, u in zip(chunk_instances, chunk_data):
                            task = cls.refresh(
                                instance=inst, new_url=cast(str, u), format_type=format_type, rec_path=rec_path,
                                paginate=paginate, next_url_extractor=next_url_extractor, call_lim=call_lim,
                                concurrency_limit=1, _client=_client, _rate_limiter=_rate_limiter, ignore_ssl=ignore_ssl
                            )
                            resilient_tasks.append(_fetch_resiliently(run_with_semaphore(semaphore, task), str(u)))

                    elif isinstance(new_file, list):
                        chunk_instances = inst_list * len(chunk_data)
                        for inst, f in zip(chunk_instances, chunk_data):
                            task = cls.refresh(
                                instance=inst, new_file=cast(str, f), format_type=format_type, rec_path=rec_path,
                                paginate=paginate, next_url_extractor=next_url_extractor, call_lim=call_lim,
                                concurrency_limit=1, _client=_client, _rate_limiter=_rate_limiter, ignore_ssl=ignore_ssl
                            )
                            resilient_tasks.append(_fetch_resiliently(run_with_semaphore(semaphore, task), str(f)))
                    else:
                        for inst in cast(List[TIncorporator], chunk_data):
                            task = cls.refresh(
                                instance=inst, format_type=format_type, rec_path=rec_path,
                                paginate=paginate, next_url_extractor=next_url_extractor, call_lim=call_lim,
                                concurrency_limit=1, _client=_client, _rate_limiter=_rate_limiter, ignore_ssl=ignore_ssl
                            )
                            src_id = getattr(inst, "inc_url",
                                             getattr(inst, "inc_file", str(getattr(inst, "inc_code", "Unknown"))))
                            resilient_tasks.append(_fetch_resiliently(run_with_semaphore(semaphore, task), str(src_id)))

                    chunk_results = await asyncio.gather(*resilient_tasks)

                    for res in chunk_results:
                        if res is None:
                            continue
                        if isinstance(res, list):
                            accumulated_results.extend(res)
                        else:
                            accumulated_results.append(res)

                    if delay_between_batches > 0.0 and i < len(chunks) - 1:
                        await asyncio.sleep(delay_between_batches)

                # TRIGGER SYSTEMIC AND TERMINAL WARNINGS FOR PARTIAL DATA
                if failed_sources:
                    warnings.warn(
                        f"Incorporator partial data returned: {len(failed_sources)} source(s) failed with HTTP 429 (Too Many Requests). "
                        f"Check the '.failed_sources' attribute on the returned list.",
                        UserWarning, stacklevel=2
                    )

                if not accumulated_results:
                    ActualClass = cast(Type[TIncorporator],
                                       schema_builder.infer_dynamic_schema("DynamicModel", [{}], cls))
                    return IncorporatorList(ActualClass, [], failed_sources=failed_sources)

                ActualClass = accumulated_results[0].__class__
                return IncorporatorList(ActualClass, accumulated_results, failed_sources=failed_sources)
            finally:
                if should_close_client and _client is not None:
                    await _client.aclose()

        # ==========================================
        # STANDARD PIPELINE
        # ==========================================
        TargetClass = instance[0].__class__ if isinstance(instance, list) else instance.__class__
        active_url = new_url or getattr(TargetClass, "inc_url", None)
        active_file = new_file or getattr(TargetClass, "inc_file", None)

        source = active_file if active_file else active_url
        if not source:
            raise ValueError("No valid origin to refresh from. Provide new_url or new_file.")

        if not isinstance(source, str):
            raise ValueError("Source must be a string at this point in the pipeline.")

        active_format = format_type or _infer_format(source)

        active_extractor = next_url_extractor
        if paginate and not active_extractor and not active_file:
            active_extractor = _AutoURLPaginator(source)

        accumulated_data: List[Any] = []
        is_single_object = False

        async for raw_text in network.stream_raw_data(
                source=source, is_file=bool(active_file), paginate=paginate,
                next_url_extractor=active_extractor, call_lim=call_lim,
                client=_client, rate_limiter=_rate_limiter, ignore_ssl=ignore_ssl
        ):
            parsed_chunk = await format_parsers.parse_source_data(raw_text, active_format)

            if rec_path:
                for part in rec_path.split('.'):
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
        data_dicts = [obj.model_dump(by_alias=True, mode='json') for obj in instances]
        await format_parsers.write_destination_data(data_dicts, file_path, active_format)