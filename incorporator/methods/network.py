"""
incorporator/methods/network.py

Modular Network Engine for the Incorporator Framework.
Handles HTTPX client generation, dynamic request execution, resilience,
and asynchronous connection-pooling for maximum throughput.
"""

import asyncio
import logging
import re
from pathlib import Path
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from . import format_parsers
from .exceptions import IncorporatorNetworkError
from .format_parsers import infer_format

logger = logging.getLogger(__name__)


# ==========================================
# 1. THROTTLING & RESILIENCE
# ==========================================
class RateLimiter:
    """Standard token-bucket lock for requests-per-second throttling to prevent 429 bans."""

    def __init__(self, requests_per_second: float) -> None:
        self.rate = requests_per_second
        self.interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self.lock = asyncio.Lock()
        self.last_call = 0.0

    async def wait(self) -> None:
        """Yields execution only for the exact delta needed to maintain the rate limit."""
        if self.rate <= 0:
            return

        async with self.lock:
            now = asyncio.get_running_loop().time()
            elapsed = now - self.last_call

            if elapsed < self.interval:
                await asyncio.sleep(self.interval - elapsed)

            self.last_call = asyncio.get_running_loop().time()


# ==========================================
# 2. HTTPX CONFIGURATION & FACTORY
# ==========================================
class HTTPClientBuilder:
    """
    Centralizes all httpx client configuration.
    Isolates connection limits, headers, and timeouts from the execution loops.
    """

    @staticmethod
    def build_client(
            concurrency_limit: int = 50,
            ignore_ssl: bool = False,
            timeout: float = 15.0,
            headers: Optional[Dict[str, str]] = None
    ) -> httpx.AsyncClient:
        """Returns a tuned httpx.AsyncClient ready for scoped concurrency."""

        client_limits = httpx.Limits(max_keepalive_connections=concurrency_limit, max_connections=concurrency_limit)

        return httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            limits=client_limits,
            verify=not ignore_ssl,
            headers=headers
        )




def _validate_url(url: str) -> None:
    """Guards against SSRF and unsupported protocol injections (e.g. file://, ftp://)."""
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise IncorporatorNetworkError(
            f"Security Policy Violation: Unsupported URL scheme '{parsed.scheme}'. "
            f"Only http/https are allowed. URL: {url}"
        )


# ==========================================
# 3. LOCAL FILE I/O
# ==========================================
def _sync_read(file_path: str) -> str:
    """Synchronous file reader with rigid path traversal mitigation."""
    try:
        path = Path(file_path).resolve()
        if not path.is_file():
            raise IncorporatorNetworkError(f"Security/IO Error: Path is not a valid file: {path}")
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except OSError as e:
        raise IncorporatorNetworkError(f"Failed to read file {file_path}: {e}") from e


async def _read_file(file_path: str) -> str:
    """Reads raw text from a local file via threadpool to protect the async event loop."""
    return await asyncio.to_thread(_sync_read, file_path)


# ==========================================
# 4. PAGINATION ENGINE
# ==========================================
def _extract_rfc5988_next_link(link_header: str) -> Optional[str]:
    """Parses standard API Link headers to find the 'next' page URL."""
    links = link_header.split(",")
    for link in links:
        if 'rel="next"' in link:
            match = re.search(r'<(.*?)>', link)
            if match:
                return match.group(1)
    return None


class AutoURLPaginator:
    """Stateful heuristic paginator that invisibly increments page/offset counters in URLs."""

    def __init__(self, start_url: str):
        self._last_url = start_url

    def __call__(self, raw_text: str) -> Optional[str]:
        # 1. Event-Loop Defense: Prevent massive string copies on 50MB+ JSON payloads
        if not raw_text or raw_text.isspace():
            return None

        # 2. Fast-path for small empty arrays/objects
        if len(raw_text) < 20 and raw_text.strip() in ("[]", "{}"):
            return None

        # 3. Only scan the tail to prevent ReDoS / Event-Loop blocking
        tail_text = raw_text[-250:]
        if re.search(r'"(?:results|data|items|response|items)"\s*:\s*\[\s*\]', tail_text, re.IGNORECASE):
            return None

        match = re.search(r'([?&])(page|p|offset|start)=(\d+)', self._last_url, re.IGNORECASE)
        if not match:
            return None

        param, val = match.group(2), int(match.group(3))
        increment = 1
        if param.lower() in ('offset', 'start'):
            limit_match = re.search(r'[?&](limit|per_page|count)=(\d+)', self._last_url, re.IGNORECASE)
            increment = int(limit_match.group(2)) if limit_match else 5

        new_val = val + increment
        new_url = self._last_url[:match.start(3)] + str(new_val) + self._last_url[match.end(3):]
        self._last_url = new_url
        return new_url


# ==========================================
# 5. CORE HTTP EXECUTION WORKERS
# ==========================================
@retry(
    stop=stop_after_attempt(8),
    wait=wait_random_exponential(multiplier=1.5, min=2, max=30),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    reraise=True
)
async def execute_request(
    url: str,
    client: httpx.AsyncClient,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    form_payload: Optional[Dict[str, Any]] = None,
    rate_limiter: Optional[RateLimiter] = None
) -> httpx.Response:
    """
    Executes a resilient, jittered HTTP request supporting GET/POST, query strings, and bodies.
    """
    _validate_url(url)

    if rate_limiter:
        await rate_limiter.wait()

    # Dynamically build kwargs for httpx
    req_kwargs: Dict[str, Any] = {}
    if params:
        req_kwargs["params"] = params
    if json_payload:
        req_kwargs["json"] = json_payload
    if form_payload:
        req_kwargs["data"] = form_payload

    # Support dynamic execution of POST, GET, PUT, etc.
    response = await client.request(method.upper(), url, **req_kwargs)

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status < 500 and status != 429:
            raise IncorporatorNetworkError(f"Fatal client error {status} for URL {url}: {e}") from e
        raise e

    return response


async def stream_raw_data(
    source: str,
    is_file: bool = False,
    paginate: bool = False,
    next_url_extractor: Optional[Callable[[str], Optional[str]]] = None,
    call_lim: Optional[int] = None,
    client: Optional[httpx.AsyncClient] = None,
    rate_limiter: Optional[RateLimiter] = None,
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    json_payload: Optional[Dict[str, Any]] = None,
    form_payload: Optional[Dict[str, Any]] = None,
    ignore_ssl: bool = False
) -> AsyncGenerator[str, None]:
    """Advanced router that yields data payloads and securely manages connection scope streams."""
    if is_file:
        yield await _read_file(source)
        return

    async def _run_stream(c: httpx.AsyncClient) -> AsyncGenerator[str, None]:
        current_url: Optional[str] = source
        calls_made = 0

        while current_url:
            if call_lim is not None and calls_made >= call_lim:
                break

            try:
                response = await execute_request(
                    url=current_url,
                    client=c,
                    method=method,
                    params=params,
                    json_payload=json_payload,
                    form_payload=form_payload,
                    rate_limiter=rate_limiter
                )
            except httpx.HTTPStatusError as e:
                raise e
            except Exception as e:
                raise IncorporatorNetworkError(f"Failed to fetch data from {current_url}: {e}") from e

            yield response.text
            calls_made += 1

            if not paginate:
                break

            next_url: Optional[str] = None
            if "link" in response.headers:
                next_url = _extract_rfc5988_next_link(response.headers["link"])

            if not next_url and next_url_extractor:
                try:
                    next_url = next_url_extractor(response.text)
                except Exception as e:
                    raise IncorporatorNetworkError(f"Pagination extractor failed on {current_url}: {e}") from e

            if next_url:
                try:
                    base_url = str(response.url)
                except RuntimeError:
                    base_url = str(current_url)
                current_url = urljoin(base_url, next_url)
            else:
                current_url = None

    if client:
        async for data in _run_stream(client):
            yield data
    else:
        # Fallback if stream_raw_data is called without a parent orchestrator
        async with HTTPClientBuilder.build_client(ignore_ssl=ignore_ssl) as c:
            async for data in _run_stream(c):
                yield data

async def _process_single_source(
        source_val: str,
        is_file_mode: bool,
        client: Optional[httpx.AsyncClient],
        rate_limiter: Optional[RateLimiter],
        **kwargs: Any
) -> List[Any]:
    """Isolates stream processing, format parsing, and rec_path drill-down into a pure worker."""
    format_type = kwargs.get('format_type')
    paginate = kwargs.get('paginate', False)
    next_url_extractor = kwargs.get('next_url_extractor')
    call_lim = kwargs.get('call_lim')
    rec_path = kwargs.get('rec_path')
    ignore_ssl = kwargs.get('ignore_ssl', False)

    # Advanced HTTP features
    method = kwargs.get('method', 'GET')
    params = kwargs.get('params')
    json_payload = kwargs.get('json_payload')
    form_payload = kwargs.get('form_payload')

    active_format = format_type or infer_format(source_val)
    active_extractor = next_url_extractor

    if paginate and not active_extractor and not is_file_mode:
        active_extractor = AutoURLPaginator(source_val)

    accumulated: List[Any] = []
    async for raw_text in stream_raw_data(
            source=source_val,
            is_file=is_file_mode,
            paginate=paginate,
            next_url_extractor=active_extractor,
            call_lim=call_lim,
            client=client,
            rate_limiter=rate_limiter,
            method=method,
            params=params,
            json_payload=json_payload,
            form_payload=form_payload,
            ignore_ssl=ignore_ssl
    ):
        # 1. Thread-safe parsing
        parsed_chunk = await format_parsers.parse_source_data(raw_text, active_format)

        # 2. Path drilling
        if rec_path:
            for part in rec_path.split('.'):
                if isinstance(parsed_chunk, dict) and part in parsed_chunk:
                    parsed_chunk = parsed_chunk[part]
                else:
                    break

        # 3. Accumulation
        if isinstance(parsed_chunk, list):
            accumulated.extend(parsed_chunk)
        else:
            accumulated.append(parsed_chunk)

    return accumulated


# ==========================================
# 6. CONCURRENT ORCHESTRATOR
# ==========================================
async def fetch_concurrent_payloads(
    source_list: List[str],
    is_file_mode: bool,
    **kwargs: Any
) -> Tuple[List[Any], List[str]]:
    """Unified Orchestrator: Exclusively manages semaphores, chunks, and concurrent batching."""
    failed_sources: List[str] =[]
    all_parsed_data: List[Any] =[]

    concurrency_limit = kwargs.get('concurrency_limit', 25)
    delay_between_batches = kwargs.get('delay_between_batches', 0.0)
    requests_per_second = kwargs.get('requests_per_second', 15.0)
    ignore_ssl = kwargs.get('ignore_ssl', False)

    # Advanced HTTP Client configurations
    headers = kwargs.get('headers')
    timeout = kwargs.get('timeout', 15.0)
    form_payload = kwargs.get('form_payload')

    _client = kwargs.get('_client')
    _rate_limiter = kwargs.get('_rate_limiter')

    limit = concurrency_limit if concurrency_limit is not None else 50
    semaphore = asyncio.Semaphore(limit)
    should_close_client = False

    # Instantiate the HTTP Client once for the entire batch
    if not is_file_mode and _client is None:
        _client = HTTPClientBuilder.build_client(
            concurrency_limit=limit,
            ignore_ssl=ignore_ssl,
            timeout=timeout,
            headers=headers
        )
        _rate_limiter = RateLimiter(requests_per_second)
        should_close_client = True

    async def _task_wrapper(src: str) -> List[Any]:
        """Wraps the worker in a Semaphore and traps 429 Rate Limits cleanly."""
        async with semaphore:
            try:
                return await _process_single_source(src, is_file_mode, _client, _rate_limiter, **kwargs)
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    logger.warning(f"Request failed with status 429. Skipping: {src}")
                    failed_sources.append(src)
                    return[]
                raise IncorporatorNetworkError(f"HTTP error {e.response.status_code}") from e

    try:
        # Segment into batches
        chunks = [source_list[i:i + limit] for i in range(0, len(source_list), limit)]

        for i, chunk in enumerate(chunks):
            tasks = [_task_wrapper(str(src)) for src in chunk]
            chunk_results = await asyncio.gather(*tasks)

            for res in chunk_results:
                if res:
                    all_parsed_data.extend(res)

            if delay_between_batches > 0.0 and i < len(chunks) - 1:
                await asyncio.sleep(delay_between_batches)

        return all_parsed_data, failed_sources
    finally:
        # Gracefully cleanup connections
        if should_close_client and _client is not None:
            await _client.aclose()