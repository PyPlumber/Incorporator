"""
HATEOAS & Declarative Payload Router for the Incorporator Framework.
Handles iterative graph-drilling and dynamic HTTP Request generation.
"""

import logging
from typing import Any, Dict, List

from .converters import _EachSentinel

logger = logging.getLogger(__name__)


def extract_parent_data(parents: Any, child_path: str) -> List[Any]:
    """Iterative BFS to safely drill into dynamic structures without recursion."""
    current_layer = parents if isinstance(parents, list) else [parents]

    for part in child_path.split("."):
        next_layer: List[Any] = []

        for node in current_layer:
            if node is None:
                continue

            if isinstance(node, list):
                for item in node:
                    val = item.get(part) if isinstance(item, dict) else getattr(item, part, None)
                    if val is not None:
                        next_layer.append(val)
            else:
                val = node.get(part) if isinstance(node, dict) else getattr(node, part, None)
                if val is not None:
                    next_layer.append(val)

        current_layer = next_layer
        if not current_layer:
            break

    return current_layer


def resolve_declarative_routing(
    caller_name: str, extracted_data: List[Any], source_urls: List[str], **kwargs: Any
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
                        f"[{caller_name}] Missing Target URL. "
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
            valid_urls = [x for x in extracted_strs if x.startswith("http") or x.startswith("/")]
            if valid_urls:
                kwargs["inc_url"] = source_urls + valid_urls
            elif not source_urls:
                valid_items = [
                    item
                    for item in extracted_data
                    if isinstance(getattr(item, "detail_url", getattr(item, "url", None)), str)
                ]
                legacy_urls = [getattr(item, "detail_url", getattr(item, "url", None)) for item in valid_items]

                if legacy_urls:
                    logger.warning(
                        f"[{caller_name}] Deprecation Warning: Relying on implicit '.url' or '.detail_url' "
                        f"attributes for HATEOAS routing is deprecated. "
                        f"Tip: Explicitly pass `inc_child='url'` (or your target JSON key) to your .incorp() call."
                    )
                    kwargs["inc_url"] = legacy_urls
                else:
                    raise ValueError(f"[{caller_name}] inc_parent extraction yielded no valid URLs.")

    return kwargs
