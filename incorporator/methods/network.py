"""Network and file I/O operations for Incorporator.

Manages HTTP connection pooling via httpx, transparent retry resilience via tenacity,
and zero-boilerplate API pagination.
"""

import asyncio
import re
from typing import AsyncGenerator, Callable, Optional

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

from .exceptions import IncorporatorNetworkError


def _sync_read(file_path: str) -> str:
    """Synchronous file reader."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        raise IncorporatorNetworkError(f"Failed to read file {file_path}: {e}")


async def _read_file(file_path: str) -> str:
    """Reads raw text from a local file without blocking the async event loop."""
    return await asyncio.to_thread(_sync_read, file_path)


# --- LIVE NETWORK ENGINE ---

# Retry attempts to 8, and max wait to 30 seconds for strict 429 APIs.
@retry(
    stop=stop_after_attempt(8),
    wait=wait_random_exponential(multiplier=1.5, min=2, max=30),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    reraise=True
)
async def _execute_get(client: httpx.AsyncClient, url: str) -> httpx.Response:
    """Executes a single GET request using an existing connection pool, with jittered retries."""
    response = await client.get(url)
    # Raise an exception for 4xx/5xx status codes so Tenacity can catch and retry them
    response.raise_for_status()
    return response


def _extract_rfc5988_next_link(link_header: str) -> Optional[str]:
    """Parses standard API Link headers to find the 'next' page URL."""
    links = link_header.split(",")
    for link in links:
        if 'rel="next"' in link:
            match = re.search(r'<(.*?)>', link)
            if match:
                return match.group(1)
    return None


async def stream_raw_data(
        source: str,
        is_file: bool = False,
        paginate: bool = False,
        next_url_extractor: Optional[Callable[[str], Optional[str]]] = None
) -> AsyncGenerator[str, None]:
    """
    Advanced router that yields data payloads.
    Handles local files, single requests, and automatic API pagination.
    """
    if is_file:
        yield await _read_file(source)
        return

    # Instantiating the client here ensures connection pooling across all paginated requests
    async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
        current_url: Optional[str] = source

        while current_url:
            try:
                response = await _execute_get(client, current_url)
            except Exception as e:
                raise IncorporatorNetworkError(f"Failed to fetch data from {current_url} after all retries: {e}")

            # Yield the current page's raw text to the downstream parser
            yield response.text

            if not paginate:
                break

            next_url: Optional[str] = None

            # 1. Zero-Boilerplate Auto-Pagination (RFC 5988 Link Headers)
            if "link" in response.headers:
                next_url = _extract_rfc5988_next_link(response.headers["link"])

            # 2. Custom JSON Body Pagination (If header pagination isn't used)
            if not next_url and next_url_extractor:
                try:
                    next_url = next_url_extractor(response.text)
                except Exception as e:
                    raise IncorporatorNetworkError(f"Pagination extractor failed on {current_url}: {e}")

            current_url = next_url

            # If there is another page to fetch, wait 0.5s to evade IP tracking/bans.
            if current_url:
                await asyncio.sleep(0.5)


async def get_raw_data(source: str, is_file: bool = False) -> str:
    """Legacy/Minimal router for fetching a single page of source data."""
    async for page in stream_raw_data(source, is_file=is_file, paginate=False):
        return page
    return ""