"""DX Inspector: analyse raw API payloads and suggest optimal Incorporator kwargs.

Probes a fetched payload for schema shape, datetime columns, numeric columns,
pagination signals, and heavy asset fields. Surface via
:func:`incorporator.test` — the ``test()`` verb of the public API.
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from ..exceptions import IncorporatorFormatError, IncorporatorNetworkError
from ..schema.converters import parses_as_datetime, parses_as_float, parses_as_int

logger = logging.getLogger(__name__)

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_HEX_HASH_RE = re.compile(r"^[0-9a-fA-F]{24,64}$")

# Heavy-field detection patterns. Matches asset CDN URLs the user almost
# certainly wants to drop from the payload via excl_lst.
_ASSET_URL_RE = re.compile(
    r"^https?://.*\.(?:jpg|jpeg|png|gif|webp|svg|bmp|ico|mp4|mov|webm|avi|mp3|wav|pdf|zip|tar|gz)(?:\?.*)?$",
    re.I,
)
_HEAVY_FIELD_BYTES = 2048  # strings larger than this look like blobs
_ASSET_URL_FIELD_BYTES = 200  # _url / _uri suffix + long value heuristic

# Pagination signal keys. Mapped to the matching paginator class name
# exported from incorporator/__init__.py.
_NEXT_URL_KEYS = ("next", "next_url", "next_page", "next_page_url")
_CURSOR_KEYS = ("cursor", "next_cursor", "page_token")
_OFFSET_PAIRS = (("offset", "limit"), ("page", "per_page"))
_PAGE_META_KEYS = ("has_more", "total", "total_pages", "page_count")


def _print_tree(data: Any, prefix: str = "", depth: int = 0, max_depth: int = 3) -> None:
    """Recursively prints a visual tree of the data dictionary."""
    if depth > max_depth:
        print(f"{prefix}...")
        return

    if isinstance(data, dict):
        for i, (k, v) in enumerate(data.items()):
            is_last = i == len(data) - 1
            connector = "└── " if is_last else "├── "

            if isinstance(v, dict):
                print(f"{prefix}{connector}{k} (dict)")
                extension = "    " if is_last else "│   "
                _print_tree(v, prefix + extension, depth + 1, max_depth)
            elif isinstance(v, list):
                print(f"{prefix}{connector}{k} (list, len={len(v)})")
                extension = "    " if is_last else "│   "
                if v:
                    _print_tree(v[0], prefix + extension, depth + 1, max_depth)
            else:
                type_name = type(v).__name__
                val_str = str(v).replace("\n", " ")
                val_str = val_str[:30] + "..." if len(val_str) > 30 else val_str
                print(f"{prefix}{connector}{k}: {type_name} = {val_str}")

    elif isinstance(data, list) and data:
        print(f"{prefix}Array of {type(data[0]).__name__} (len={len(data)})")
        _print_tree(data[0], prefix, depth, max_depth)
    else:
        val_str = str(data)[:30] + "..." if len(str(data)) > 30 else str(data)
        print(f"{prefix}{val_str}")


def analyze_data(parsed_data: List[Any], provided_kwargs: Dict[str, Any]) -> None:
    """Print the DX Inspector report for a freshly-fetched payload.

    Five sections, each actionable:

    1. **Payload structure** — tree-view of every key Python sees, with
       types and sample values.
    2. **Identity mapping** — scored candidates for ``inc_code``
       (UUIDs, integer IDs, hashes) and ``inc_name`` (display strings)
       evaluated against the **top-level** record.
    3. **Type-casting suggestions** — strings that the framework's own
       runtime converters (:func:`incorporator.schema.converters.inc`)
       would successfully coerce to ``datetime``, ``int``, or ``float``
       become ``conv_dict`` candidates. Routing through the same parser
       means inspector advice is structurally aligned with what an
       actual ``incorp()`` call would accept.
    4. **Pagination hints** — when sample keys look like ``next`` /
       ``cursor`` / ``offset+limit`` / ``page+per_page``, suggest the
       matching paginator from :mod:`incorporator.io.pagination`.
    5. **Heavy-field hints** — base64 blobs, asset-CDN URLs, and
       oversized strings get nominated for ``excl_lst``.

    Called by :meth:`Incorporator.test` after the safe single-page
    fetch.  Prints directly to stdout; returns ``None``.
    """
    if not parsed_data:
        print("\n🔍 INCORPORATOR INSPECTOR: No data returned to inspect.\n")
        return

    print("\n" + "=" * 70)
    print("🕵️‍♂️  INCORPORATOR DX INSPECTOR")
    print("=" * 70)

    sample = parsed_data[0]

    # 1. Structure & rec_path suggestion
    print("\n📦 1. PAYLOAD STRUCTURE:")
    _print_tree(sample, "   ")

    # Decide what record-shaped object to analyze.  Key DX fix: never
    # silently re-target into a nested list-of-dicts.  When the top-level
    # sample is itself a list, the first row IS the record.  When it's a
    # dict, that dict is the record — full stop.  Nested arrays just get
    # surfaced as drill-down candidates.
    target_obj: Any
    list_children: List[Tuple[str, int]] = []  # [(key, len), ...] for the drill hint

    if isinstance(sample, list) and sample:
        target_obj = sample[0]
    elif isinstance(sample, dict):
        target_obj = sample
        if not provided_kwargs.get("rec_path"):
            list_children = [
                (k, len(v)) for k, v in sample.items() if isinstance(v, list) and v and isinstance(v[0], dict)
            ]
            if list_children:
                # Sort by size desc so the biggest list reads first.
                list_children.sort(key=lambda pair: pair[1], reverse=True)
                biggest = list_children[0][0]
                inventory = ", ".join(f"{k} ({n})" for k, n in list_children)
                print(f"\n   ⚠️  The root object also contains nested arrays:  {inventory}")
                print("   💡 To map one of those instead, add `rec_path` and re-run test():")
                print(f"      await YourClass.test(inc_url=..., rec_path='{biggest}')")
    else:
        target_obj = sample

    if not isinstance(target_obj, dict):
        print("\n   ℹ️  Data is not a dictionary. No further attribute suggestions can be made.")
        print("=" * 70 + "\n")
        return

    # 2. Identity mapping — analyzed against the top-level record.
    _print_identity_mapping(target_obj)

    # 3. ETL / type casting — route through the framework's own parsers.
    _print_type_casting(target_obj)

    # 4. Pagination hints — read the un-drifted top-level sample.
    if isinstance(sample, dict):
        _print_pagination_hints(sample)

    # 5. Heavy-field hints — drop bloaty payloads.
    _print_heavy_field_hints(target_obj)

    print("=" * 70 + "\n")


def _print_identity_mapping(target_obj: Dict[str, Any]) -> None:
    """Score inc_code / inc_name candidates and print the top picks."""
    print("\n🔑 2. IDENTITY MAPPING:")

    best_code, best_code_score = None, -1
    best_name, best_name_score = None, -1

    for k, v in target_obj.items():
        k_lower = str(k).lower()

        # --- Primary Key Heuristic ---
        c_score = 0
        if k_lower in ("id", "uuid", "guid", "pk", "code", "key", "hash"):
            c_score += 50
        elif k_lower.endswith("_id"):
            c_score += 30

        if isinstance(v, int):
            c_score += 10
        elif isinstance(v, str):
            if _UUID_RE.match(v):
                c_score += 40
            elif _HEX_HASH_RE.match(v):
                c_score += 20

        if c_score > best_code_score and c_score > 0:
            best_code_score, best_code = c_score, k

        # --- Display Name Heuristic ---
        n_score = 0
        if k_lower in ("name", "title", "label", "display", "headline", "full_name", "username"):
            n_score += 50
        elif k_lower in ("description", "desc", "summary", "slug"):
            n_score += 20

        if isinstance(v, str) and v:
            if 2 <= len(v) <= 100:
                n_score += 10
            if " " in v:
                n_score += 10  # Titles/Names usually have spaces
            if v.istitle() or v.isupper():
                n_score += 10

        if n_score > best_name_score and n_score > 0:
            best_name_score, best_name = n_score, k

    print("   Recommended kwargs for O(1) Memory Registry:")
    if best_code:
        print(f"   ✅ inc_code='{best_code}'")
    else:
        print("   ❓ inc_code=None (Could not accurately identify a unique primary key)")

    if best_name:
        print(f"   ✅ inc_name='{best_name}'")
    else:
        print("   ❓ inc_name=None (Could not accurately identify a display name)")


def _print_type_casting(target_obj: Dict[str, Any]) -> None:
    """Suggest conv_dict entries by asking the framework's own parsers.

    Routes through :func:`incorporator.schema.converters.parses_as_datetime`
    and siblings, so the inspector only suggests conversions the runtime
    would actually accept.  No more guessing with positional regex.

    Precedence: datetime > int > float (a numeric ISO date wouldn't reach
    here, but if a string parses as both int and datetime, datetime wins).
    """
    print("\n🛠️  3. ETL / TYPE CASTING SUGGESTIONS:")

    date_candidates: List[str] = []
    int_candidates: List[str] = []
    float_candidates: List[str] = []

    for k, v in target_obj.items():
        if not isinstance(v, str) or not v:
            continue
        k_lower = str(k).lower()

        if parses_as_datetime(v):
            date_candidates.append(k)
            continue

        # Don't suggest numeric coercion for hash/uuid-shaped strings.
        if _UUID_RE.match(v) or _HEX_HASH_RE.match(v):
            continue

        # Field-name confidence booster: only suggest numeric coercion when
        # the key name hints at a quantity OR the string is purely numeric.
        looks_numeric_key = any(
            tok in k_lower
            for tok in ("count", "total", "amount", "qty", "quantity", "price", "rate", "score", "size", "len")
        )

        if parses_as_int(v):
            # Skip identifier-shaped ints (leading zeros, very long)
            if len(v) > 18 or (len(v) > 1 and v.lstrip("-").startswith("0")):
                continue
            if looks_numeric_key or k_lower.endswith(("_num", "_count", "_total")):
                int_candidates.append(k)
                continue

        if parses_as_float(v):
            if looks_numeric_key:
                float_candidates.append(k)

    any_suggestion = bool(date_candidates or int_candidates or float_candidates)
    if any_suggestion:
        print("   💡 The framework's runtime parsers would coerce these. Consider:")
        print("      conv_dict={")
        for c in date_candidates:
            print(f"          '{c}': inc(datetime),")
        for c in int_candidates:
            print(f"          '{c}': inc(int),")
        for c in float_candidates:
            print(f"          '{c}': inc(float),")
        print("      }")
    else:
        print("   ✅ No string fields look like dates or numbers requiring conversion.")


def _print_pagination_hints(sample: Dict[str, Any]) -> None:
    """Detect pagination signals in the top-level sample and suggest a paginator.

    Picks the highest-confidence signal (next-url > cursor > offset-pair).
    Class names match the public exports from :mod:`incorporator`.
    """
    suggestion: Optional[str] = None
    description: str = ""

    # 1. Next-URL paginator (highest confidence — a literal URL string).
    for key in _NEXT_URL_KEYS:
        val = sample.get(key)
        if isinstance(val, str) and val.startswith(("http://", "https://")):
            suggestion = f"NextUrlPaginator('{key}')"
            description = f"the response contains a `{key}` URL pointing to the next page"
            break

    # _links.next is the JSON:API / HAL convention — nested one level deep.
    if not suggestion and isinstance(sample.get("_links"), dict):
        links = sample["_links"]
        next_link = links.get("next")
        if isinstance(next_link, str) and next_link.startswith(("http://", "https://")):
            suggestion = "NextUrlPaginator('_links', 'next')"
            description = "the response uses the JSON:API `_links.next` convention"
        elif isinstance(next_link, dict) and isinstance(next_link.get("href"), str):
            suggestion = "NextUrlPaginator('_links', 'next', 'href')"
            description = "the response uses the HAL `_links.next.href` convention"

    # 2. Cursor / page-token paginator.
    if not suggestion:
        for key in _CURSOR_KEYS:
            if key in sample:
                suggestion = f"CursorPaginator(cursor_param='{key}')"
                description = f"the response carries a `{key}` token for the next page"
                break

    # 3. Offset/page-number pairs.
    if not suggestion:
        for low_key, high_key in _OFFSET_PAIRS:
            if low_key in sample and high_key in sample:
                if low_key == "offset":
                    suggestion = f"OffsetPaginator(limit={sample.get(high_key) or 100})"
                else:
                    suggestion = "PageNumberPaginator(page_param='page')"
                description = f"the response uses an `{low_key}` + `{high_key}` window"
                break

    # 4. Bare metadata fallback — likely paginated but ambiguous which style.
    if not suggestion and any(k in sample for k in _PAGE_META_KEYS):
        present = [k for k in _PAGE_META_KEYS if k in sample]
        print("\n📑 4. PAGINATION HINTS:")
        print(f"   ⚠️  Found pagination metadata ({', '.join(present)}) but no clear cursor.")
        print("   💡 Re-fetch the next page manually and check which kwarg the API expects:")
        print("      `page=` / `offset=` / `cursor=` — then wrap with the matching paginator.")
        return

    if suggestion:
        print("\n📑 4. PAGINATION HINTS:")
        print(f"   💡 This endpoint looks paginated — {description}. Consider:")
        print(f"      inc_page={suggestion}")


def _print_heavy_field_hints(target_obj: Dict[str, Any]) -> None:
    """Suggest excl_lst entries for fields that bloat the payload.

    Catches:
      * Base64-encoded image strings (`data:image/...`).
      * Asset-CDN URLs (image / video / archive extensions).
      * String values larger than ``_HEAVY_FIELD_BYTES``.
      * `*_url` / `*_uri` fields with long values (CDN heuristic).
    """
    heavy: List[str] = []

    for k, v in target_obj.items():
        if not isinstance(v, str) or not v:
            continue
        k_lower = str(k).lower()

        if v.startswith("data:image/"):
            heavy.append(k)
        elif _ASSET_URL_RE.match(v):
            heavy.append(k)
        elif len(v) > _HEAVY_FIELD_BYTES:
            heavy.append(k)
        elif (k_lower.endswith("_url") or k_lower.endswith("_uri")) and len(v) > _ASSET_URL_FIELD_BYTES:
            heavy.append(k)

    if heavy:
        print("\n🗑️  5. HEAVY-FIELD HINTS:")
        print("   💡 Fields likely to bloat the payload — consider excluding:")
        formatted = ", ".join(f"'{k}'" for k in heavy)
        print(f"      excl_lst=[{formatted}]")


def analyze_error(e: Exception) -> None:
    """Centralized DX Error Inspector providing actionable fixes for modern formats."""

    def p(text: str) -> None:
        """print() that survives consoles that can't encode the inspector's emojis.

        Windows cp1252 console (the default for many users) can't encode
        🚨 / 💡 / 👉.  Without this fallback, the inspector itself crashes on
        ``UnicodeEncodeError`` and the actionable hint never reaches the user.
        ASCII-replace and re-emit so the diagnosis still lands.
        """
        try:
            print(text)
        except UnicodeEncodeError:
            print(text.encode("ascii", errors="replace").decode("ascii"))

    p("\n" + "=" * 70)
    p("🚨 INCORPORATOR DX INSPECTOR: EXECUTION FAILED")
    p("=" * 70)
    p(f"[{e.__class__.__name__}] {e}")
    p("\n💡 QUICK FIX SUGGESTIONS:")

    if isinstance(e, IncorporatorNetworkError):
        cause = getattr(e, "__cause__", None)
        cause_name = type(cause).__name__ if cause else ""

        if cause_name == "HTTPStatusError":
            status = getattr(getattr(cause, "response", None), "status_code", 0)
            if status in (401, 403):
                p(f"   👉 Auth Blocked (HTTP {status}): Pass `headers={{'Authorization': 'Bearer ...'}}`.")
            elif status == 406:
                p("   👉 Format Rejected (HTTP 406): Try `headers={{'Accept': 'application/json'}}`.")
            else:
                p(f"   👉 Server returned HTTP {status}. Verify the endpoint requirements.")

        elif cause_name == "ConnectError":
            cause_str = str(cause).upper()
            if "SSL" in cause_str or "CERTIFICATE" in cause_str:
                p("   👉 SSL Verification Failed: Add `ignore_ssl=True` to bypass proxies.")
            else:
                p("   👉 Connection Refused: Verify the URL or check if a VPN is required.")

        elif cause_name in ("TimeoutException", "ReadTimeout", "ConnectTimeout"):
            p("   👉 Connection Timed Out: The server is unresponsive. Add `timeout=30.0` if it's just slow.")
        else:
            p("   👉 Check your URL or network connection.")

    elif isinstance(e, IncorporatorFormatError):
        e_str = str(e).lower()

        # Modern Format Hints
        if "avro" in e_str or "fastavro" in e_str:
            p(
                "   👉 Missing Dependency: Apache Avro requires `fastavro`. Run `pip install incorporator[orchestrate]`."  # noqa: E501
            )
        elif "sqlite" in e_str or "sql" in e_str:
            p("   👉 SQLite Execution: Ensure you provide `sql_query='SELECT * FROM ...'`.")
            p("      Or check that the file path resolves to a valid `.db` / `.sqlite` file.")
        elif "json" in e_str and ("decode" in e_str or "invalid" in e_str):
            p("   👉 JSON Decode Failed: If the file is JSON Lines, specify `format_type=FormatType.NDJSON`.")
            p("      If it's an API, it might be returning a text/html firewall or CAPTCHA.")
        elif "xml" in e_str:
            p(
                "   👉 XML Parsing Failed: Check if the XML declaration is malformed, or if DTDs are blocked by security policy."  # noqa: E501
            )
        elif "delimited" in e_str or "csv" in e_str:
            p(
                "   👉 CSV Parsing Failed: Check the delimiter. Use `format_type=FormatType.TSV` or `PSV` if it's not a comma."  # noqa: E501
            )
        elif "cramjam" in e_str:
            p("   👉 Missing Dependency: Rust compression requires Cramjam. Run `pip install incorporator[cramjam]`.")
        else:
            p("   👉 HTML Firewall/Login Page: If the parser choked, the API likely returned HTML.")
            p("      Open the URL in your browser to check for captchas or login portals.")
            p("   👉 Local File: Check `format_type=...` if relying on a file without an extension.")

    else:
        p("   👉 A schema or configuration error occurred. Check your payload syntax.")

    p("=" * 70 + "\n")
