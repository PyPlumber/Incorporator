"""Shared sidecar-union + token-resolve step for pipeline/watershed configs.

Both the library entry point (:func:`incorporator.tideweaver.config.load_watershed`)
and the CLI entry point (:func:`incorporator.cli.runners._load_pipeline_config`)
need to run the exact same sequence — extract the top-level ``inflow``/
``outflow`` sidecar paths, union their public names into one token-resolver
allow-list, then resolve every JSON-text token in the config against that
allow-list. Historically each caller hand-rolled its own copy of this
sequence and had to be kept in lockstep by hand (see commit ``221b16c``,
where the two copies drifted). :func:`resolve_sidecar_tokens` is now the
single implementation both callers route through.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

from ..usercode import merge_sidecar_extra_names
from .tokens import resolve_tokens


def resolve_sidecar_tokens(rebased: dict[str, Any], *, strict_outflow: bool = True) -> dict[str, Any]:
    """Union the inflow/outflow sidecar names and resolve every JSON-text token.

    Args:
        rebased: The config dict after env-expansion and config-dir path
            rebasing (``resolve_config_paths``) have already run, so any
            ``inflow``/``outflow`` fields are config-dir-absolute.
        strict_outflow: Forwarded to :func:`~incorporator.usercode.merge_sidecar_extra_names`.
            When ``True`` (the default, matching :func:`~incorporator.tideweaver.config.load_watershed`),
            a missing/broken outflow sidecar raises immediately, same as
            inflow. Callers that defer outflow errors to a later, friendlier
            aggregated validator should pass ``False``.

    Returns:
        ``rebased`` with every JSON-text token resolved to its Python value.

    Raises:
        FileNotFoundError: The inflow sidecar (always) or outflow sidecar
            (when ``strict_outflow=True``) does not resolve to a file.
        ImportError: The inflow sidecar (always) or outflow sidecar (when
            ``strict_outflow=True``) cannot be loaded as a Python module.
        TokenResolutionError: A JSON-text token references an unsafe or
            unknown symbol.
    """
    inflow_val = rebased.get("inflow")
    outflow_val = rebased.get("outflow")
    inflow = Path(inflow_val) if isinstance(inflow_val, str) and inflow_val else None
    outflow = Path(outflow_val) if isinstance(outflow_val, str) and outflow_val else None

    extra_names = merge_sidecar_extra_names(inflow, outflow, strict_outflow=strict_outflow)
    return cast(dict[str, Any], resolve_tokens(rebased, extra_names=extra_names))


__all__ = ["resolve_sidecar_tokens"]
