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
Python objects at config-load time, so users don't need a sidecar
``.py`` file just to express ``"inc_page": "NextUrlPaginator('next')"``.

Tokens that require user Python (``calc(my_function)``, ``link_to(MyClass)``)
need an ``inflow.py`` sidecar whose public symbols extend the allow-list.
The CLI loader handles this automatically when a top-level ``inflow``
key is present in ``pipeline.json``.

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
import operator
import re
from datetime import date, datetime, time
from typing import Any

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
from ..schema.converters import calc, calc_all, inc, new
from ..schema.extractors import (
    as_list,
    join_all,
    link_to,
    link_to_list,
    pluck,
    split_and_get,
    sum_attributes,
)

logger = logging.getLogger(__name__)

# Names safe to resolve from a JSON-text token. Anything outside this dict
# is rejected before any code runs.
_ALLOWED_NAMES: dict[str, Any] = {
    # Paginators — class constructors with literal args (URL, key, etc.)
    "NextUrlPaginator": NextUrlPaginator,
    "CursorPaginator": CursorPaginator,
    "OffsetPaginator": OffsetPaginator,
    "PageNumberPaginator": PageNumberPaginator,
    "LinkHeaderPaginator": LinkHeaderPaginator,
    "SQLitePaginator": SQLitePaginator,
    "CSVPaginator": CSVPaginator,
    "AvroPaginator": AvroPaginator,
    # Converter / extractor tokens.  `inc`, `as_list`, `join_all`,
    # `split_and_get`, `pluck`, `sum_attributes` take all-literal args and
    # work standalone.  `calc`, `calc_all`, `link_to`, `link_to_list` take
    # a user-defined callable or registry — they only resolve when the
    # caller also supplies an `inflow.py` whose public names include the
    # referenced helper.
    "inc": inc,
    "as_list": as_list,
    "join_all": join_all,
    "split_and_get": split_and_get,
    "pluck": pluck,
    "sum_attributes": sum_attributes,
    "calc": calc,
    "calc_all": calc_all,
    "link_to": link_to,
    "link_to_list": link_to_list,
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
    # Comparison operators for parent_filter 3-tuple form.
    # Use via @-sigil in JSON: ["division_id", "@operator_eq", 201]
    "operator_eq": operator.eq,
    "operator_ne": operator.ne,
    "operator_lt": operator.lt,
    "operator_le": operator.le,
    "operator_gt": operator.gt,
    "operator_ge": operator.ge,
    "operator_contains": operator.contains,
}

# A string looks like a call-grammar token only if it matches a Python call.
# Dotted names (``datetime.datetime(...)``) match here intentionally — the AST
# walker rejects them with a clear error, which is friendlier than silent
# pass-through that fails opaquely downstream.  URLs, plain strings, file
# paths, and headers still don't match (no top-level parens).
_TOKEN_SHAPE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*\s*\(.*\)\s*$", re.DOTALL)

# The ``@name`` sigil for bare-name references into the inflow module's
# public symbols.  Deliberately strict: a single ``@`` prefix followed by a
# valid Python identifier and nothing else.  No dots, no parens, no slashes
# — so the grammar can't be confused with email addresses, URLs, decorators,
# or ``npm``-style scopes.
_AT_SIGIL_SHAPE = re.compile(r"^@([A-Za-z_][A-Za-z0-9_]*)$")


class TokenResolutionError(ValueError):
    """Raised when a JSON-text token references an unsafe / unknown symbol."""


def _eval_node(node: ast.AST, *, origin: str, allowed: dict[str, Any]) -> Any:
    """Walk an AST under the safe-eval ``allowed`` map and return the Python value.

    ``origin`` is the original token string — surfaced in error messages.
    ``allowed`` is the effective allow-list (framework names + any
    ``extra_names`` supplied by the caller).
    """
    if isinstance(node, ast.Expression):
        return _eval_node(node.body, origin=origin, allowed=allowed)

    if isinstance(node, ast.Constant):
        return node.value

    if isinstance(node, ast.Name):
        if node.id not in allowed:
            raise TokenResolutionError(
                f"Token {origin!r} references unknown identifier {node.id!r}. "
                f"Allow-listed names: {sorted(allowed)}. "
                "User-defined functions / classes must live in an inflow.py."
            )
        return allowed[node.id]

    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise TokenResolutionError(
                f"Token {origin!r} uses an unsupported call form. "
                "Only top-level allow-listed function names may be called."
            )
        func = _eval_node(node.func, origin=origin, allowed=allowed)
        args = [_eval_node(a, origin=origin, allowed=allowed) for a in node.args]
        kwargs = {kw.arg: _eval_node(kw.value, origin=origin, allowed=allowed) for kw in node.keywords if kw.arg}
        return func(*args, **kwargs)

    if isinstance(node, ast.List | ast.Tuple):
        elts = [_eval_node(e, origin=origin, allowed=allowed) for e in node.elts]
        return list(elts) if isinstance(node, ast.List) else tuple(elts)

    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub) and isinstance(node.operand, ast.Constant):
        # Allow negative number literals: -1, -3.14.  Not arbitrary unary ops.
        value = node.operand.value
        if isinstance(value, int | float):
            return -value

    # Friendly per-shape rejections for the AST nodes users most commonly
    # try.  Falls through to the generic message for anything else.
    if isinstance(node, ast.Lambda):
        raise TokenResolutionError(
            f"Token {origin!r} contains a lambda, which is not allowed in the JSON "
            "token grammar.  Define a named function in your inflow.py sidecar and "
            'reference it by name from the JSON (e.g. ``"@my_helper"`` or '
            "``\"calc(my_helper, 'field')\"``)."
        )
    if isinstance(node, ast.ListComp | ast.DictComp | ast.SetComp | ast.GeneratorExp):
        raise TokenResolutionError(
            f"Token {origin!r} contains a comprehension, which is not allowed in the "
            "JSON token grammar.  Move the comprehension into a named function in "
            "inflow.py and reference it from the JSON."
        )

    raise TokenResolutionError(
        f"Token {origin!r} uses an unsupported AST node ({type(node).__name__}). "
        "Only literals, allow-listed names, and calls on allow-listed names are permitted."
    )


def _resolve_string(text: str, *, allowed: dict[str, Any]) -> Any:
    """Attempt to resolve ``text`` as a token. Returns the original string on
    a structural non-match; raises :class:`TokenResolutionError` on a
    structural match with an unsafe payload.

    Two resolution paths:

    * **``@name`` sigil** — strict bare-name reference. Looks ``name`` up
      in ``allowed`` and returns the object. The cleanest pattern for
      anything pre-built in ``inflow.py``: ``"@my_pager"``.
    * **Call-grammar** — ``Name(args)`` form. AST-parsed against the
      safe-eval allow-list. Useful for trivial framework cases that
      don't justify an inflow file: ``"inc(datetime)"``.

    Shape mismatch on both paths → silent pass-through. URLs and ordinary
    strings stay as strings.

    Shape match + allow-list miss → loud error. The user clearly *meant*
    a reference; tell them why it didn't resolve.
    """
    # @name sigil — cheapest check, run first.
    sigil_match = _AT_SIGIL_SHAPE.match(text)
    if sigil_match:
        name = sigil_match.group(1)
        if name not in allowed:
            user_names = sorted(n for n in allowed if n not in _ALLOWED_NAMES)
            user_hint = f" inflow.py public names: {user_names}." if user_names else " No inflow.py is loaded."
            raise TokenResolutionError(f"Token {text!r} references unknown name {name!r}.{user_hint}")
        return allowed[name]

    # Call-grammar path.
    if not _TOKEN_SHAPE.match(text):
        return text
    try:
        tree = ast.parse(text, mode="eval")
    except SyntaxError:
        # Looked like a call but isn't valid Python. Leave it alone.
        logger.debug("Token shape matched but ast.parse failed: %r", text)
        return text
    return _eval_node(tree, origin=text, allowed=allowed)


def resolve_tokens(obj: Any, extra_names: dict[str, Any] | None = None) -> Any:
    """Recursively walk ``obj`` and resolve every JSON-text token in place.

    ``obj`` is typically the dict returned by :func:`json.load` after
    :func:`expand_env` has expanded environment-variable references.

    ``extra_names`` is an optional mapping of additional allow-list
    entries — typically the public symbols of an ``inflow.py`` module
    extracted via :func:`incorporator.usercode.extract_public_names`.
    Framework names take precedence on conflict (defensive against a
    user accidentally shadowing ``inc``, ``as_list``, etc.).

    Returns a new structure with strings substituted by their resolved
    Python values where applicable. The input is not mutated.
    """
    # Build the merged allow-list once at the top of the walk.  User-supplied
    # names go FIRST so framework names win on conflict via the second update.
    if extra_names:
        allowed: dict[str, Any] = {**extra_names, **_ALLOWED_NAMES}
    else:
        allowed = _ALLOWED_NAMES

    def _walk(node: Any) -> Any:
        if isinstance(node, dict):
            return {k: _walk(v) for k, v in node.items()}
        if isinstance(node, list):
            return [_walk(v) for v in node]
        if isinstance(node, str):
            return _resolve_string(node, allowed=allowed)
        return node

    return _walk(obj)


__all__ = [
    "TokenResolutionError",
    "resolve_tokens",
]
