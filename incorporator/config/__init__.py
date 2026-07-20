"""Config-layer utilities: env/secret expansion and JSON-token resolution.

Shared by the verb layer (:func:`incorporator.usercode.apply_inflow_resolution`),
the watershed layer (:func:`incorporator.tideweaver.config.load_watershed`), and
the CLI (:func:`incorporator.cli.runners._load_pipeline_config`). The latter
two both route their sidecar-union + token-resolve step through
:func:`incorporator.config.pipeline.resolve_sidecar_tokens`, which itself
depends on :mod:`incorporator.usercode` — the only edge back out of this
package.
"""

from __future__ import annotations

from .envexpand import EnvExpansionError, expand_env
from .pipeline import resolve_sidecar_tokens
from .tokens import TokenResolutionError, resolve_tokens

__all__ = [
    "EnvExpansionError",
    "TokenResolutionError",
    "expand_env",
    "resolve_sidecar_tokens",
    "resolve_tokens",
]
