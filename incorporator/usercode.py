"""Filesystem-based loaders for user-supplied Python code.

This module hosts the small utilities that don't belong on the
``Incorporator`` class — they don't touch ``cls``/``self`` and share
exactly one job: read a ``.py`` off disk and surface its symbols.

- :func:`load_user_module` — import a path-anchored ``.py`` file once,
  return its module object. Backs every CLI/Python flow that needs a
  user sidecar (inflow.py for incorp/refresh/stream; outflow.py for
  fjord's classes; export's optional transform hook).
- :func:`extract_public_names` — return ``{name: obj}`` for every
  top-level non-underscore name in a loaded module. Used by the token
  resolver to extend its allow-list with the inflow module's symbols.
- :func:`merge_sidecar_extra_names` — union public names from the
  outflow and inflow sidecars into one token-resolver allow-list
  (inflow wins on name collision). Shared by
  :func:`incorporator.tideweaver.config.load_watershed` and the CLI's
  ``_load_pipeline_config`` so both paths resolve ``conv_dict`` tokens
  against the same sidecar symbols.
- :func:`apply_inflow_resolution` — load the inflow sidecar and
  resolve string-form tokens in ``conv_dict`` / ``inc_page`` against
  its public symbols (shared by :meth:`Incorporator.incorp` and
  :meth:`Incorporator.refresh`).
- :func:`apply_code_transform` — load and run an optional
  ``transform(instances)`` hook (used by :meth:`Incorporator.export`).
- :func:`load_outflow_module` — load fjord's required ``outflow(state)``
  function AND its module (used by :meth:`Incorporator.fjord` and the
  Tideweaver Fjord current).
- :func:`load_inflow_callable` — load the optional state-aware
  ``inflow(state)`` callable from an inflow sidecar (used by
  :meth:`Incorporator.fjord`).
- :func:`pascal_case_from_stem` — derive a PascalCase class name from
  a filename stem (used by fjord/stream to name a dynamic output
  class, or to look up the user-defined class in an outflow.py).

Keeping these out of ``base.py`` makes the import graph one-directional::

    base.py → usercode.py        (never the reverse)

All loaders rely on Python's ``importlib.util.spec_from_file_location``
+ ``exec_module``, which registers the module in ``sys.modules`` before
``exec_module`` returns.  Subsequent calls with the same path therefore
**don't re-execute the file** — Python's import cache absorbs the
repeat.  Callers can safely call these helpers per-wave without paying
re-import cost.
"""

from __future__ import annotations

import importlib.util
import inspect as _inspect
import re
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any, cast

from .io.pagination.base import AsyncPaginator


def load_user_module(path: str | Path) -> ModuleType:
    """Import a path-anchored ``.py`` file and return its module object.

    Single-source loader for every sidecar file the framework accepts:
    inflow.py (helpers for trinity calls) and outflow.py (Incorporator
    subclasses + fjord ``outflow(state)`` plus optional ``transform()``
    hook for export).

    **Per-path caching, keyed on identity alone.** The loader registers
    each module under ``sys.modules`` keyed SOLELY by a digest of the
    resolved absolute path — no caller-supplied name enters the key — so
    the SAME physical file loaded through two different framework entry
    points (e.g. :func:`load_outflow_module` and then
    :func:`apply_code_transform` against the same ``outflow.py``) returns
    the IDENTICAL module object with IDENTICAL class objects. This matters
    because downstream consumers (the Tideweaver Fjord current, the token
    resolver, class-name lookups) all resolve classes by getattr-ing a
    loaded sidecar module; two module copies of one file would mean two
    non-interchangeable copies of every class it defines.

    **``__main__`` short-circuit.** When ``path`` is the SAME file already
    running as ``__main__`` (a user's direct ``python entry.py`` run that
    also calls into the framework, e.g. via ``asyncio.run(main())``), this
    returns that ``__main__`` module directly instead of ``exec``-ing a
    second copy — so the classes the framework resolves from the file ARE
    the classes the user's own top-level code declared, and share the
    same (non-weak-ref-orphaned) ``inc_dict``.

    **Sidecar-relative imports.** Before executing a freshly loaded file
    (first load only — cached and ``__main__`` returns skip this), its own
    parent directory is inserted at ``sys.path[0]`` if not already present,
    so a bare top-level ``import sibling`` inside the sidecar resolves
    without the file having to hand-roll its own
    ``sys.path.insert(0, str(Path(__file__).parent))`` guard.

    Args:
        path: Absolute or relative path to a ``.py`` file.

    Returns:
        The loaded module object.

    Raises:
        FileNotFoundError: ``path`` does not resolve to an existing file.
        ImportError: The file cannot be loaded as a Python module.
    """
    import sys

    code_path = Path(path).resolve()
    if not code_path.is_file():
        raise FileNotFoundError(f"[Incorporator] sidecar file not found: {code_path}")

    cache_key = f"_inc_user_module_{abs(hash(str(code_path)))}"
    cached = sys.modules.get(cache_key)
    if cached is not None:
        return cached

    main_module = sys.modules.get("__main__")
    if main_module is not None:
        main_file = getattr(main_module, "__file__", None)
        if main_file is not None and Path(main_file).resolve() == code_path:
            # Same file as the running entry point — share its class
            # objects rather than exec-ing a disconnected second copy.
            sys.modules[cache_key] = main_module
            return main_module

    spec = importlib.util.spec_from_file_location(cache_key, code_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"[Incorporator] Cannot load module spec from: {code_path}")

    module = importlib.util.module_from_spec(spec)
    # Register BEFORE exec_module — covers re-entrant imports inside the
    # user file and ensures the cache check above sees the module on the
    # next call.
    sys.modules[cache_key] = module

    # Mirror how `python entry.py` itself prepends the script's own
    # directory to sys.path for the life of the process — a sidecar's
    # bare `import sibling` should work the same way, without the user
    # hand-writing a sys.path guard.
    sidecar_dir = str(code_path.parent)
    if sidecar_dir not in sys.path:
        sys.path.insert(0, sidecar_dir)

    try:
        spec.loader.exec_module(module)
    except Exception:
        # Roll back the cache entry so a broken file can be fixed and
        # reloaded in the same process without a stale half-initialised
        # module sitting in sys.modules.
        sys.modules.pop(cache_key, None)
        raise
    return module


def extract_public_names(module: ModuleType) -> dict[str, Any]:
    """Return ``{name: obj}`` for every public (non-underscore) name in ``module``.

    Used by the token resolver to extend its allow-list with whatever
    the user's ``inflow.py`` exposes.  No ``__all__`` is required — the
    "public by default" convention follows Python's standard rules.
    """
    return {n: getattr(module, n) for n in dir(module) if not n.startswith("_")}


def merge_sidecar_extra_names(
    inflow: str | Path | None,
    outflow: str | Path | None,
    *,
    strict_outflow: bool = True,
) -> dict[str, Any]:
    """Union public names from the outflow and inflow sidecar modules.

    Single source of truth for "which sidecar symbols may a JSON config's
    ``conv_dict`` (or other token-resolved field) reference by name" — shared
    by :func:`incorporator.tideweaver.config.load_watershed` and
    :func:`incorporator.cli.runners._load_pipeline_config` so the Tideweaver
    CLI path (``incorporator tideweaver run``) and the direct Python
    ``load_watershed`` path resolve tokens against the exact same allow-list.

    Loads ``outflow`` first, then ``inflow``, and updates outflow's dict with
    inflow's — an inflow helper wins over an outflow helper of the same name.

    Args:
        inflow: Path to an ``inflow.py`` sidecar, or ``None`` to skip it.
        outflow: Path to an ``outflow.py`` sidecar, or ``None`` to skip it.
        strict_outflow: When ``True`` (default), a missing/broken ``outflow``
            sidecar raises immediately, same as ``inflow``.  Callers that defer
            outflow existence/import errors to a later, friendlier aggregated
            validator (the CLI's ``_load_pipeline_config`` does, ahead of
            ``_run_validation``) should pass ``False`` — the outflow sidecar is
            then skipped silently on ``FileNotFoundError`` / ``ImportError`` /
            ``SyntaxError`` instead of propagating.  ``inflow`` errors always
            propagate regardless of this flag.

    Returns:
        ``{name: obj}`` for every public name across both sidecars (``{}``
        when both are ``None``, or when a non-strict outflow load fails).

    Raises:
        FileNotFoundError: ``inflow`` (always) or ``outflow`` (when
            ``strict_outflow=True``) does not resolve to a file.
        ImportError: ``inflow`` (always) or ``outflow`` (when
            ``strict_outflow=True``) cannot be loaded as a Python module.
    """
    extra_names: dict[str, Any] = {}
    if outflow is not None:
        try:
            extra_names.update(extract_public_names(load_user_module(outflow)))
        except (FileNotFoundError, ImportError, SyntaxError):
            if strict_outflow:
                raise
    if inflow is not None:
        extra_names.update(extract_public_names(load_user_module(inflow)))
    return extra_names


def _extract_user_callable(
    module: ModuleType,
    *,
    name: str,
    required: bool,
    param_label: str,
    source_path: str | Path,
    arity: int = 1,
) -> Callable[..., Any] | None:
    """Pull a top-level callable off a sidecar module and validate its arity.

    Args:
        module: The already-loaded sidecar module.
        name: The top-level symbol to extract (``"transform"`` / ``"outflow"``
            / ``"inflow"``).
        required: Raise ``ValueError`` when the symbol is missing.  When
            ``False``, returns ``None`` for missing.
        param_label: Human-readable parameter name used in arity-mismatch
            errors (``"instances"`` / ``"state"``) so the message matches the
            domain of the caller.
        source_path: The user's sidecar path; included in error messages so
            failures point at the offending file.
        arity: Expected number of positional parameters (default 1).

    Returns:
        The callable on success, ``None`` when ``required=False`` and the
        symbol is absent.

    Raises:
        ValueError: For missing-and-required, non-callable target, or
            wrong arity.
    """
    fn = getattr(module, name, None)
    if fn is None:
        if required:
            raise ValueError(
                f"[Incorporator] outflow file must define a top-level {name}(state) function: {source_path}"
                if name == "outflow"
                else f"[Incorporator] sidecar file must define a top-level {name}() function: {source_path}"
            )
        return None
    if not callable(fn):
        raise ValueError(f"[Incorporator] sidecar file's top-level {name!r} attribute must be callable: {source_path}")
    sig = _inspect.signature(fn)
    params = list(sig.parameters)
    if len(params) != arity:
        raise ValueError(
            f"[Incorporator] {name}() must accept exactly {arity} parameter ({param_label}), "
            f"got {len(params)}: {params}"
        )
    return fn  # type: ignore[no-any-return]


def apply_inflow_resolution(
    inflow: str | Path,
    conv_dict: dict[str, Any] | None,
    inc_page: AsyncPaginator | None,
) -> tuple[dict[str, Any] | None, AsyncPaginator | None]:
    """Load the inflow module and resolve string-form tokens in trinity kwargs.

    Shared by :meth:`Incorporator.incorp` and :meth:`Incorporator.refresh`.
    When ``inflow`` is set, imports the module (cached via ``sys.modules``,
    so the first call pays the import cost and all subsequent calls are
    free) and resolves any string-form tokens in ``conv_dict`` and
    ``inc_page`` against the module's public symbols.

    Real Python callables already present in ``conv_dict`` pass through
    unchanged — the resolver only touches strings.
    """
    from .cli.tokens import resolve_tokens

    module = load_user_module(inflow)
    extra_names = extract_public_names(module)
    resolved_conv = cast(
        dict[str, Any] | None,
        resolve_tokens(conv_dict, extra_names=extra_names) if conv_dict else conv_dict,
    )
    resolved_page = inc_page
    if isinstance(inc_page, str):
        resolved_page = cast(
            AsyncPaginator | None,
            resolve_tokens(inc_page, extra_names=extra_names),
        )
    return resolved_conv, resolved_page


def apply_code_transform(
    instances: list[Any],
    outflow: str | Path,
) -> list[Any]:
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
        outflow: Absolute or relative path to a ``.py`` outflow file.

    Raises:
        FileNotFoundError: If ``outflow`` does not exist.
        ImportError: If the file cannot be loaded as a Python module.
        ValueError: If ``transform`` is defined but takes the wrong number
            of parameters.
    """
    module = load_user_module(outflow)
    transform_fn = _extract_user_callable(
        module, name="transform", required=False, param_label="instances", source_path=outflow
    )
    if transform_fn is None:
        return instances
    result = transform_fn(instances)
    return result if result is not None else instances


def load_outflow_module(outflow: str | Path) -> tuple[Callable[[Any], Any], ModuleType]:
    """Load ``outflow.py``; return both its ``outflow`` callable AND its module.

    The engine needs the module object so it can probe for user-pre-declared
    Incorporator subclasses whose names match the keys returned by
    ``outflow(state)``.  When such a subclass exists, the engine uses it as
    the derived class instead of building one via
    :func:`infer_dynamic_schema`.

    Reuses :func:`load_user_module`'s ``sys.modules`` cache so repeated
    loads of the same path cost one file-read total; the arity check
    delegates to :func:`_extract_user_callable`.

    Raises:
        FileNotFoundError: ``outflow`` does not exist.
        ImportError: The file cannot be loaded as a Python module.
        ValueError: ``outflow`` is missing the top-level ``outflow(state)``
            function or it has the wrong arity.
    """
    module = load_user_module(outflow)
    outflow_fn = _extract_user_callable(module, name="outflow", required=True, param_label="state", source_path=outflow)
    # ``required=True`` guarantees a non-None return; mypy needs the cast.
    return cast(Callable[[Any], Any], outflow_fn), module


def load_inflow_callable(inflow: str | Path) -> Callable[[Any], Any] | None:
    """Return the optional state-aware ``inflow(state)`` callable from ``inflow.py``.

    When ``inflow.py`` defines a top-level callable named ``inflow`` accepting
    one parameter (``state``), the fjord engine runs it before each source's
    ``incorp()`` to obtain per-source ``conv_dict`` overrides.  Without this
    callable (``inflow.py`` used as a passive name-bag for reducer helpers),
    the engine keeps the parallel ``asyncio.gather`` seed path.

    Sync OR async callables are both accepted — the caller is
    responsible for detecting and awaiting if needed via
    ``inspect.iscoroutinefunction``.

    Args:
        inflow: Path to an ``inflow.py`` file.

    Returns:
        The ``inflow`` callable when present; ``None`` when the
        file exists but defines no such function.

    Raises:
        FileNotFoundError: ``inflow`` does not exist.
        ImportError: The file cannot be loaded as a Python module.
        ValueError: ``inflow`` is defined but has the wrong arity.
    """
    module = load_user_module(inflow)
    return _extract_user_callable(module, name="inflow", required=False, param_label="state", source_path=inflow)


def pascal_case_from_stem(outflow: str | Path) -> str:
    """Derive a Pydantic-class-friendly name from an outflow file's filename.

    ``coin_market.py`` → ``"CoinMarket"``;
    ``crypto-spread.py`` → ``"CryptoSpread"``.  Used by
    :meth:`Incorporator.fjord` to name the dynamic output class — the
    developer never has to declare it.

    Raises:
        ValueError: If the stem produces an invalid Python identifier
            (empty, leading digit, or contains nothing alphabetic).
    """
    stem = Path(outflow).stem
    parts = re.split(r"[_\-\s]+", stem)
    name = "".join(p.capitalize() for p in parts if p)
    if not name or not name[0].isalpha():
        raise ValueError(
            f"[Incorporator] Cannot derive a valid Python class name from outflow stem "
            f"{stem!r}. Use a filename like 'coin_market.py'."
        )
    return name
