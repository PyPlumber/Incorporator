"""Filesystem-based loaders for user-supplied Python code.

This module hosts three small utilities that don't belong on the
``Incorporator`` class — they don't touch ``cls``/``self`` and share
exactly one job: read a ``.py`` off disk, validate a named function's
arity, and either run it or return the callable.

- :func:`apply_code_transform` — load and run an optional
  ``transform(instances)`` hook (used by :meth:`Incorporator.export`).
- :func:`load_outflow_function` — load fjord's required
  ``outflow(state)`` function (used by :meth:`Incorporator.fjord`).
- :func:`pascal_case_from_stem` — derive a PascalCase class name from
  a filename stem (used by fjord to name its dynamic output class).

Keeping these out of ``base.py`` makes the import graph one-directional::

    base.py → usercode.py        (never the reverse)
"""

import importlib.util
import inspect as _inspect
import re
from pathlib import Path
from typing import Any, Callable, List, Union


def apply_code_transform(
    instances: List[Any],
    code_file: Union[str, Path],
) -> List[Any]:
    """Load a Python file and call its top-level ``transform(instances)`` function.

    The file must define::

        def transform(instances):
            # filter, sort, add computed fields, etc.
            return modified_instances

    If no ``transform`` function is found, ``instances`` is returned unchanged.
    Runs synchronously — callers should wrap in ``asyncio.to_thread`` for
    CPU-heavy transforms.

    Args:
        instances: The list of Incorporator objects to transform.
        code_file: Absolute or relative path to a ``.py`` file.

    Raises:
        FileNotFoundError: If ``code_file`` does not exist.
        ImportError: If the file cannot be loaded as a Python module.
        ValueError: If ``transform`` is defined but takes the wrong number
            of parameters.
    """
    code_path = Path(code_file).resolve()
    if not code_path.exists():
        raise FileNotFoundError(f"[Incorporator] code_file not found: {code_path}")

    spec = importlib.util.spec_from_file_location("_inc_code_transform", code_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"[Incorporator] Cannot load module spec from: {code_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    transform_fn = getattr(module, "transform", None)
    if transform_fn is None:
        return instances

    sig = _inspect.signature(transform_fn)
    params = list(sig.parameters)
    if len(params) != 1:
        raise ValueError(
            f"[Incorporator] transform() must accept exactly 1 parameter (instances), " f"got {len(params)}: {params}"
        )

    result = transform_fn(instances)
    return result if result is not None else instances


def load_outflow_function(code_file: Union[str, Path]) -> Callable[[Any], Any]:
    """Load a top-level ``outflow(state)`` function from a Python file.

    Mirrors :func:`apply_code_transform`'s importlib pattern but for the
    fjord engine — the file must define a function named ``outflow`` that
    accepts exactly one parameter (the state dict mapping class names to
    ``IncorporatorList`` snapshots) and returns a ``list[dict]`` (or a
    single ``dict``, which fjord auto-wraps).  The returned rows are fed
    into the dynamic-schema-inference path the same way ``incorp()``
    treats parsed payloads.

    Returns:
        The loaded callable.

    Raises:
        FileNotFoundError: ``code_file`` does not exist.
        ImportError: The file cannot be loaded as a Python module.
        ValueError: ``outflow`` is missing or has the wrong arity.
    """
    code_path = Path(code_file).resolve()
    if not code_path.exists():
        raise FileNotFoundError(f"[Incorporator] code_file not found: {code_path}")

    spec = importlib.util.spec_from_file_location("_inc_fjord_outflow", code_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"[Incorporator] Cannot load module spec from: {code_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    outflow_fn = getattr(module, "outflow", None)
    if outflow_fn is None:
        raise ValueError(f"[Incorporator] code_file must define a top-level outflow(state) function: {code_path}")

    sig = _inspect.signature(outflow_fn)
    params = list(sig.parameters)
    if len(params) != 1:
        raise ValueError(
            f"[Incorporator] outflow() must accept exactly 1 parameter (state), " f"got {len(params)}: {params}"
        )

    return outflow_fn  # type: ignore[no-any-return]


def pascal_case_from_stem(code_file: Union[str, Path]) -> str:
    """Derive a Pydantic-class-friendly name from a code_file's filename.

    ``coin_market.py`` → ``"CoinMarket"``;
    ``crypto-spread.py`` → ``"CryptoSpread"``.  Used by
    :meth:`Incorporator.fjord` to name the dynamic output class — the
    developer never has to declare it.

    Raises:
        ValueError: If the stem produces an invalid Python identifier
            (empty, leading digit, or contains nothing alphabetic).
    """
    stem = Path(code_file).stem
    parts = re.split(r"[_\-\s]+", stem)
    name = "".join(p.capitalize() for p in parts if p)
    if not name or not name[0].isalpha():
        raise ValueError(
            f"[Incorporator] Cannot derive a valid Python class name from code_file stem "
            f"{stem!r}. Use a filename like 'coin_market.py'."
        )
    return name
