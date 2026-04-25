"""Built-in data converters and lambda wrappers for Incorporator.

These functions abstract away messy lambda syntax and are 100% "Null-Safe".
They gracefully handle None or empty strings to prevent ETL pipeline crashes.
Designed to be passed into the 'conv_dict' parameter during Dynamic Class Building.
"""

import ast
import collections.abc
import json
import operator
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional

# ==========================================
# SAFE MATH EVALUATOR (Security Fix)
# ==========================================
_ALLOWED_BIN_OPS: Dict[type, Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow
}

_ALLOWED_UNARY_OPS: Dict[type, Callable[[Any], Any]] = {
    ast.USub: operator.neg
}

_ALLOWED_FUNCS: Dict[str, Callable[..., Any]] = {
    'abs': abs, 'round': round, 'min': min, 'max': max
}


def _safe_eval_ast(node: ast.AST, env: Dict[str, float]) -> float:
    """Safely evaluates a mathematical AST, preventing Arbitrary Code Execution."""
    if isinstance(node, ast.Expression):
        return _safe_eval_ast(node.body, env)
    elif isinstance(node, ast.Constant):
        if isinstance(node.value, (int, float)):
            return float(node.value)
        raise ValueError("Only numbers allowed in math expressions.")
    elif isinstance(node, ast.Name):
        if node.id in env:
            return float(env[node.id])
        if node.id in _ALLOWED_FUNCS:
            raise ValueError(f"Function {node.id} must be called, not referenced.")
        raise ValueError(f"Unknown variable: {node.id}")
    elif isinstance(node, ast.BinOp):
        left = _safe_eval_ast(node.left, env)
        right = _safe_eval_ast(node.right, env)
        bin_op = _ALLOWED_BIN_OPS.get(type(node.op))
        if bin_op:
            return float(bin_op(left, right))
    elif isinstance(node, ast.UnaryOp):
        operand = _safe_eval_ast(node.operand, env)
        un_op = _ALLOWED_UNARY_OPS.get(type(node.op))
        if un_op:
            return float(un_op(operand))
    elif isinstance(node, ast.Call):
        if isinstance(node.func, ast.Name) and node.func.id in _ALLOWED_FUNCS:
            func = _ALLOWED_FUNCS[node.func.id]
            args =[_safe_eval_ast(arg, env) for arg in node.args]
            return float(func(*args))
    raise ValueError(f"Unsupported math operation: {type(node)}")


def cast_callable_unary(op: Any) -> Callable[[Any], Any]:
    """Helper to appease mypy for unary operators."""
    return op  # type: ignore


# ==========================================
# DIRECT CASTERS (Usage in conv_dict: {'key': to_bool})
# ==========================================
def to_bool(value: Any) -> bool:
    """Safely converts strings ('true', '1', 'yes') to booleans. Returns False if empty."""
    if isinstance(value, bool):
        return value
    if not value:
        return False

    truthy_values = {'true', '1', 'yes', 'y', 't', 'on'}
    return str(value).strip().lower() in truthy_values


def to_date(value: Any) -> Optional[datetime]:
    """Parses standard ISO-8601 and various common date strings into datetime objects."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value

    safe_str = str(value).strip().replace('Z', '+00:00')

    try:
        return datetime.fromisoformat(safe_str)
    except ValueError:
        pass

    fallback_formats =[
        "%B %d, %Y",                 # Long: December 2, 2013
        "%Y-%m-%d %H:%M:%S",         # SQL Timestamps: 2026-04-22 23:59:59
        "%m/%d/%Y",                  # US Short: 04/22/2026
        "%d/%m/%Y",                  # EU Short: 22/04/2026
        "%Y/%m/%d",                  # Asian Short: 2026/04/22
        "%d %b %Y",                  # 22 Apr 2026
        "%b %d, %Y",                 # Apr 22, 2026
        "%Y-%m-%dT%H:%M:%S.%f",      # ISO with truncated timezone
        "%a, %d %b %Y %H:%M:%S %Z",  # RFC 2822 / HTTP headers
    ]

    for fmt in fallback_formats:
        try:
            return datetime.strptime(safe_str, fmt)
        except ValueError:
            continue

    raise ValueError(f"Could not parse '{value}' into a datetime object.")


def to_int(
        value: Any = "__INCORP_FACTORY__",
        *,
        math: Optional[str] = None,
        default: Optional[int] = None
) -> Any:
    """Safely converts strings/floats to ints, supporting AST-based math scaling."""
    # DSA OPTIMIZATION: Compile AST once at factory init
    math_ast = ast.parse(math, mode="eval") if math else None

    if value == "__INCORP_FACTORY__":
        def _factory(val: Any) -> Optional[int]:
            if val is None or val == "":
                return default

            clean_val = str(val).strip().lower() if isinstance(val, str) else val
            if isinstance(clean_val, str):
                if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
                    return default
                clean_val = clean_val.replace(",", "")

            try:
                result = float(clean_val)
                if math_ast:
                    result = _safe_eval_ast(math_ast, {"x": result})
                return int(result)
            except Exception:
                return default

        return _factory

    # Direct Execution
    if value is None or value == "":
        return default

    clean_val = str(value).strip().lower() if isinstance(value, str) else value
    if isinstance(clean_val, str):
        if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
            return default
        clean_val = clean_val.replace(",", "")

    try:
        result = float(clean_val)
        if math_ast:
            result = _safe_eval_ast(math_ast, {"x": result})
        return int(result)
    except Exception:
        return default


def to_float(
        value: Any = "__INCORP_FACTORY__",
        *,
        math: Optional[str] = None,
        default: Optional[float] = None
) -> Any:
    """Safely converts strings to floats, supporting AST-based math scaling."""
    math_ast = ast.parse(math, mode="eval") if math else None

    if value == "__INCORP_FACTORY__":
        def _factory(val: Any) -> Optional[float]:
            if val is None or val == "":
                return default

            clean_val = str(val).strip().lower() if isinstance(val, str) else val
            if isinstance(clean_val, str):
                if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
                    return default
                clean_val = clean_val.replace(",", "")

            try:
                result = float(clean_val)
                if math_ast:
                    result = _safe_eval_ast(math_ast, {"x": result})
                return result
            except Exception:
                return default

        return _factory

    # Direct Execution
    if value is None or value == "":
        return default

    clean_val = str(value).strip().lower() if isinstance(value, str) else value
    if isinstance(clean_val, str):
        if clean_val in {"unknown", "n/a", "none", "null", "undefined"}:
            return default
        clean_val = clean_val.replace(",", "")

    try:
        result = float(clean_val)
        if math_ast:
            result = _safe_eval_ast(math_ast, {"x": result})
        return result
    except Exception:
        return default


# ==========================================
# WRAPPERS (Usage in conv_dict: {'key': split_and_get('/')})
# ==========================================

def split_and_get(
        delimiter: str = '/',
        index: int = -1,
        cast_type: Optional[Callable[[Any], Any]] = None
) -> Callable[[Any], Any]:
    def _splitter(value: Any) -> Any:
        if not value:
            return None
        try:
            result = str(value).strip(delimiter).split(delimiter)[index]
            return cast_type(result) if cast_type is not None else result
        except (IndexError, ValueError, TypeError):
            return None
    return _splitter


def cast_list_items(cast_type: Callable[[Any], Any]) -> Callable[[Any], List[Any]]:
    def _caster(lst: Any) -> List[Any]:
        if not lst:
            return []
        if not isinstance(lst, list):
            return[cast_type(lst)]
        return[cast_type(item) for item in lst if item is not None and item != ""]
    return _caster


def default_if_null(default_value: Any) -> Callable[[Any], Any]:
    def _defaulter(value: Any) -> Any:
        return default_value if value is None or value == "" else value
    return _defaulter


def link_to(dataset: Any, extractor: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    if isinstance(dataset, list):
        registry: Mapping[Any, Any] = {
            getattr(item, 'inc_code'): item
            for item in dataset
            if getattr(item, 'inc_code', None) is not None
        }
    else:
        registry = getattr(dataset, "codeDict", {})

    if not isinstance(registry, collections.abc.Mapping):
        registry = {}

    def _mapper(val: Any) -> Any:
        key = extractor(val) if extractor is not None else val
        if key is None:
            return None
        if key in registry:
            return registry[key]
        try:
            return registry.get(int(key))
        except (ValueError, TypeError):
            return None

    return _mapper



def link_to_list(dataset: Any, extractor: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], List[Any]]:
    base_linker = link_to(dataset, extractor)
    def _mapper(val_list: Any) -> List[Any]:
        if not isinstance(val_list, list):
            return []
        return[obj for v in val_list if (obj := base_linker(v)) is not None]
    return _mapper


# ==========================================
# URL & NESTED DATA TOOLS
# ==========================================

def json_path_extractor(*keys: str) -> Callable[[str], Optional[str]]:
    def _extractor(raw_json_str: str) -> Optional[str]:
        try:
            data = json.loads(raw_json_str)
            for key in keys:
                if isinstance(data, dict):
                    data = data.get(key)
                else:
                    return None
            return str(data) if data else None
        except Exception:
            return None
    return _extractor


def extract_url_id(cast_type: Callable[[Any], Any] = int) -> Callable[[Any], Any]:
    def _extractor(url_str: Any) -> Any:
        if not isinstance(url_str, str) or not url_str:
            return None
        try:
            clean_str = url_str.strip('/')
            result = clean_str.split('/')[-1]
            return cast_type(result) if cast_type is not None else result
        except (ValueError, TypeError, IndexError):
            return None
    return _extractor


def pluck(key: str, chain: Optional[Callable[[Any], Any]] = None) -> Callable[[Any], Any]:
    def _plucker(val: Any) -> Any:
        extracted = val.get(key) if isinstance(val, dict) else val
        if chain:
            return chain(extracted)
        return extracted
    return _plucker