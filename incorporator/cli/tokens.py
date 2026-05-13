"""JSON-text → Python-callable token resolution for the CLI.

Users write Incorporator pipelines as ``pipeline.json``. JSON can carry
strings, numbers, lists, and dicts — but not Python callables. The
framework's `incorp()` / `refresh()` / `export()` API accepts several
fields whose values *are* callables or instances:

* ``incorp_params.inc_page`` — an ``AsyncPaginator`` instance
* ``incorp_params.conv_dict`` — values are converter closures (``inc(datetime)``,
  ``as_list()``, ``join_all(';')``, ``split_and_get(',', 0)``)
* ``export_params.transform`` — a closure
* ``export_params.conv_dict`` — same as incorp's

This module resolves the **text form** of those callables into real
Python objects at config-load time, so users don't need a ``code_file``
just to express ``"inc_page": "NextUrlPaginator(\"next\")"``.

Tokens that require user Python (``calc(my_function)``, ``link_to(MyClass)``,
``each(MyClass)``) still need a ``code_file`` — there's no way to fit a
user-defined function into a JSON string. Those are the fjord scenarios.

## Syntax

A string is treated as a token candidate if it matches a Python-call shape
(``^[A-Za-z_][A-Za-z0-9_]*\\(.*\\)$``). The string is parsed via
:func:`ast.parse` in ``eval`` mode, then walked under a strict allow-list:

* ``ast.Constant`` — string / number / bool / None literals pass through
* ``ast.Name`` — only resolved if the identifier is in :data:`_ALLOWED_NAMES`
* ``ast.Call`` — only resolved if the ``func`` is an allow-listed name
* ``ast.List`` / ``ast.Tuple`` — recursively resolved
* anything else — rejected with a clear error

This is the standard safe-eval pattern. Attribute access, subscripts,
imports, lambdas, comprehensions, and binary operators are all rejected
before any Python code runs.

## False-match guard

A plain string that *happens* to look like a call (rare — URLs don't
match, ordinary text doesn't match) but contains an unknown identifier
won't be substituted: the resolver leaves it as a string and emits a
warning to ``logger`` so users get a diagnostic if they intended a token.
"""

from __future__ import annotations

import ast
import logging
import re
from datetime import date, datetime, time
from typing import Any, Dict

from ..io.pagination import (
    AvroPaginator,
    CSVPaginator,
    CursorPaginator,
    LinkHeaderPaginator,
    NextUrlPaginator,
    OffsetPaginator,
    PageNumberPaginator,
    SQLitePaginator,
)
from ..schema.converters import inc, new
from ..schema.extractors import (
    as_list,
    join_all,
    pluck,
    split_and_get,
    sum_attributes,
)

logger = logging.getLogger(__name__)

# Names safe to resolve from a JSON-text token. Anything outside this dict
# is rejected before any code runs.
_ALLOWED_NAMES: Dict[str, Any] = {
    # Paginators — class constructors with literal args (URL, key, etc.)
    "NextUrlPaginator": NextUrlPaginator,
    "CursorPaginator": CursorPaginator,
    "OffsetPaginator": OffsetPaginator,
    "PageNumberPaginator": PageNumberPaginator,
    "LinkHeaderPaginator": LinkHeaderPaginator,
    "SQLitePaginator": SQLitePaginator,
    "CSVPaginator": CSVPaginator,
    "AvroPaginator": AvroPaginator,
    # Converter / extractor tokens whose arguments are all literals or
    # well-known type names (so they don't need a user-defined function).
    "inc": inc,
    "as_list": as_list,
    "join_all": join_all,
    "split_and_get": split_and_get,
    "pluck": pluck,
    "sum_attributes": sum_attributes,
    # Type / sentinel names that show up as bare identifiers inside calls
    # like ``inc(datetime)`` or ``inc(int)``.
    "datetime": datetime,
    "date": date,
    "time": time,
    "int": int,
    "float": float,
    "bool": bool,
    "str": str,
    "list": list,
    "dict": dict,
    "tuple": tuple,
    "set": set,
    "bytes": bytes,
    "None": None,
    "True": True,
    "False": False,
    "new": new,  # incorporator schema "any-type" sentinel
}

# A string looks like a token only if it matches a Python call grammar.
# Dotted names (``datetime.datetime(...)``) match here intentionally — the AST
# walker rejects them with a clear error, which is friendlier than silent
# pass-through that fails opaquely downstream.  URLs, plain strings, file
# paths, and headers still don't match (no top-level parens).
_TOKEN_SHAPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*\s*\(.*\)\s*$", re.DOTALL)


class TokenResolutionError(ValueError):
    """Raised when a JSON-text token references an unsafe / unknown symbol."""


def _eval_node(node: ast.AST, *, origin: str) -> Any:
    """Walk an AST under the safe-eval allow-list and return the Python value.

    ``origin`` is the original token string — surfaced in error messages.
    """
    if isinstance(node, ast.Expression):
        return _eval_node(node.body, origin=origin)

    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        if node.id not in _ALLOWED_NAMES:
            raise TokenResolutionError(
                f"Token {origin!r} references unknown identifier {node.id!r}. "
                f"Allow-listed names: {sorted(_ALLOWED_NAMES)}. "
                "User-defined functions / classes must live in a code_file."
            )
        return _ALLOWED_NAMES[node.id]

    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise TokenResolutionError(
                f"Token {origin!r} uses an unsupported call form. "
                "Only top-level allow-listed function names may be called."
            )
        func = _eval_node(node.func, origin=origin)
        args = [_eval_node(a, origin=origin) for a in node.args]
        kwargs = {kw.arg: _eval_node(kw.value, origin=origin) for kw in node.keywords if kw.arg}
        return func(*args, **kwargs)

    if isinstance(node, (ast.List, ast.Tuple)):
        elts = [_eval_node(e, origin=origin) for e in node.elts]
        return list(elts) if isinstance(node, ast.List) else tuple(elts)

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        # Allow negative number literals: -1, -3.14.  Not arbitrary unary ops.
        value = node.operand.value
        if isinstance(value, (int, float)):
            return -value

    raise TokenResolutionError(
        f"Token {origin!r} uses an unsupported AST node ({type(node).__name__}). "
        "Only literals, allow-listed names, and calls on allow-listed names are permitted."
    )


def _resolve_string(text: str) -> Any:
    """Attempt to resolve ``text`` as a token. Returns the original string on
    a structural non-match; raises :class:`TokenResolutionError` on a
    structural match with an unsafe payload.

    The two-tier behaviour is deliberate:

    * Shape mismatch → silent pass-through. URLs and ordinary strings never
      try to parse, so they stay strings.
    * Shape match, allow-list miss → loud error. The user clearly *meant* a
      token; tell them why it didn't resolve instead of letting the engine
      receive a raw string and fail later with a confusing error.
    """
    if not _TOKEN_SHAPE.match(text):
        return text
    try:
        tree = ast.parse(text, mode="eval")
    except SyntaxError:
        # Looked like a call but isn't valid Python. Leave it alone.
        logger.debug("Token shape matched but ast.parse failed: %r", text)
        return text
    return _eval_node(tree, origin=text)


def resolve_tokens(obj: Any) -> Any:
    """Recursively walk ``obj`` and resolve every JSON-text token in place.

    ``obj`` is typically the dict returned by :func:`json.load` after
    :func:`expand_env` has expanded environment-variable references.

    Returns a new structure with strings substituted by their resolved
    Python values where applicable. The input is not mutated.
    """
    if isinstance(obj, dict):
        return {k: resolve_tokens(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [resolve_tokens(v) for v in obj]
    if isinstance(obj, str):
        return _resolve_string(obj)
    return obj


__all__ = [
    "TokenResolutionError",
    "resolve_tokens",
]
