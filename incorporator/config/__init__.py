"""Config-layer utilities: env/secret expansion and JSON-token resolution.

Shared by the verb layer (:func:`incorporator.usercode.apply_inflow_resolution`),
the watershed layer (:func:`incorporator.tideweaver.config.load_watershed`), and
the CLI (:func:`incorporator.cli.runners._load_pipeline_config`) — none of
these three consumers depends on the others through this package.
"""

from __future__ import annotations

from .envexpand import EnvExpansionError, expand_env
from .tokens import TokenResolutionError, resolve_tokens

__all__ = [
    "EnvExpansionError",
    "TokenResolutionError",
    "expand_env",
    "resolve_tokens",
]
