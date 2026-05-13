"""
Developer Experience (DX) Inspector Module.
Analyzes raw API payloads and suggests the optimal Incorporator kwargs.
"""

import logging
import re
from typing import Any, Dict, List

from .exceptions import IncorporatorFormatError, IncorporatorNetworkError

logger = logging.getLogger(__name__)

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_HEX_HASH_RE = re.compile(r"^[0-9a-fA-F]{24,64}$")


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
    if not parsed_data:
        print("\n🔍 INCORPORATOR INSPECTOR: No data returned to inspect.\n")
        return

    print("\n" + "=" * 70)
    print("🕵️‍♂️  INCORPORATOR DX INSPECTOR")
    print("=" * 70)

    sample = parsed_data[0]

    # 1. Structure & rec_path suggestion (Keep as is)
    print("\n📦 1. PAYLOAD STRUCTURE:")
    _print_tree(sample, "   ")

    target_obj = sample
    if isinstance(sample, dict) and not provided_kwargs.get("rec_path"):
        list_keys = [k for k, v in sample.items() if isinstance(v, list) and v and isinstance(v[0], dict)]
        if list_keys:
            biggest_list = max(list_keys, key=lambda k: len(sample[k]))
            target_obj = sample[biggest_list][0]
            print("\n   ⚠️  WARNING: The root object is a dictionary, but it contains arrays.")
            print(f"   💡 SUGGESTION: You probably want to add `rec_path='{biggest_list}'` to your incorp() call.")

    elif isinstance(sample, list) and sample:
        target_obj = sample[0]

    if not isinstance(target_obj, dict):
        print("\n   ℹ️  Data is not a dictionary. No further attribute suggestions can be made.")
        print("=" * 70 + "\n")
        return

    # Advanced Value-Based Scoring Heuristics
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

    # 3. ETL Suggestions (Dates)
    print("\n🛠️  3. ETL / TYPE CASTING SUGGESTIONS:")
    date_candidates = []
    for k, v in target_obj.items():
        k_lower = str(k).lower()
        if isinstance(v, str):
            # Basic ISO check or keyword match
            if any(x in k_lower for x in ("date", "time", "_at")):
                date_candidates.append(k)
            elif len(v) >= 10 and v[4] == "-" and v[7] == "-":
                date_candidates.append(k)

    if date_candidates:
        print("   💡 We detected string-based timestamps. Consider passing:")
        print("      conv_dict={")
        for c in date_candidates:
            print(f"          '{c}': inc(datetime),")
        print("      }")
    else:
        print("   ✅ No obvious string-dates found requiring conversion.")

    print("=" * 70 + "\n")


def analyze_error(e: Exception) -> None:
    """Centralized DX Error Inspector providing actionable fixes for modern formats."""
    print("\n" + "=" * 70)
    print("🚨 INCORPORATOR DX INSPECTOR: EXECUTION FAILED")
    print("=" * 70)
    print(f"[{e.__class__.__name__}] {e}")
    print("\n💡 QUICK FIX SUGGESTIONS:")

    if isinstance(e, IncorporatorNetworkError):
        cause = getattr(e, "__cause__", None)
        cause_name = type(cause).__name__ if cause else ""

        if cause_name == "HTTPStatusError":
            status = getattr(getattr(cause, "response", None), "status_code", 0)
            if status in (401, 403):
                print(f"   👉 Auth Blocked (HTTP {status}): Pass `headers={{'Authorization': 'Bearer ...'}}`.")
            elif status == 406:
                print("   👉 Format Rejected (HTTP 406): Try `headers={{'Accept': 'application/json'}}`.")
            else:
                print(f"   👉 Server returned HTTP {status}. Verify the endpoint requirements.")

        elif cause_name == "ConnectError":
            cause_str = str(cause).upper()
            if "SSL" in cause_str or "CERTIFICATE" in cause_str:
                print("   👉 SSL Verification Failed: Add `ignore_ssl=True` to bypass proxies.")
            else:
                print("   👉 Connection Refused: Verify the URL or check if a VPN is required.")

        elif cause_name in ("TimeoutException", "ReadTimeout", "ConnectTimeout"):
            print("   👉 Connection Timed Out: The server is unresponsive. Add `timeout=30.0` if it's just slow.")
        else:
            print("   👉 Check your URL or network connection.")

    elif isinstance(e, IncorporatorFormatError):
        e_str = str(e).lower()

        # Modern Format Hints
        if "avro" in e_str or "fastavro" in e_str:
            print(
                "   👉 Missing Dependency: Apache Avro requires `fastavro`. Run `pip install incorporator[orchestrate]`."  # noqa: E501
            )
        elif "sqlite" in e_str or "sql" in e_str:
            print("   👉 SQLite Execution: Ensure you provide `sql_query='SELECT * FROM ...'`.")
            print("      Or check that the file path resolves to a valid `.db` / `.sqlite` file.")
        elif "json" in e_str and ("decode" in e_str or "invalid" in e_str):
            print("   👉 JSON Decode Failed: If the file is JSON Lines, specify `format_type=FormatType.NDJSON`.")
            print("      If it's an API, it might be returning a text/html firewall or CAPTCHA.")
        elif "xml" in e_str:
            print(
                "   👉 XML Parsing Failed: Check if the XML declaration is malformed, or if DTDs are blocked by security policy."  # noqa: E501
            )
        elif "delimited" in e_str or "csv" in e_str:
            print(
                "   👉 CSV Parsing Failed: Check the delimiter. Use `format_type=FormatType.TSV` or `PSV` if it's not a comma."  # noqa: E501
            )
        elif "cramjam" in e_str:
            print(
                "   👉 Missing Dependency: Rust compression requires Cramjam. Run `pip install incorporator[cramjam]`."
            )
        else:
            print("   👉 HTML Firewall/Login Page: If the parser choked, the API likely returned HTML.")
            print("      Open the URL in your browser to check for captchas or login portals.")
            print("   👉 Local File: Check `format_type=...` if relying on a file without an extension.")

    else:
        print("   👉 A schema or configuration error occurred. Check your payload syntax.")

    print("=" * 70 + "\n")
