"""Pydantic schema models for ``incorporator stream`` / ``fjord`` configs.

These models mirror the field-level rules enforced by
:mod:`incorporator.cli.validate`'s ``validate_stream_config`` and
``validate_fjord_config``.

Runtime concerns (sidecar import, ``outflow(state)`` arity check, class
lookup against the loaded user module) deliberately stay OUT of these
models — they remain on the CLI side where the user-code execution risk
already lives.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Source keys recognised by ``Incorporator.incorp()`` — at least one
# must be present in ``incorp_params`` for a stream config to be valid.
# Kept in sync with ``incorporator.cli.validate._STREAM_SOURCE_KEYS``.
_STREAM_SOURCE_KEYS = {"inc_url", "inc_file", "inc_parent", "payload_list"}

IntervalSpec = float | dict[str, float]
"""A refresh / export interval may be a scalar (seconds, applied to
every source) or a dict keyed by class name with numeric seconds
values."""


class FjordStreamEntry(BaseModel):
    """One entry in a fjord config's ``stream_params`` list.

    Attributes:
        cls_name: Symbol name of the :class:`Incorporator` subclass
            declared in the outflow sidecar.  Class resolution against
            the loaded module is a CLI-runtime concern and lives in
            :mod:`incorporator.cli.runners`.
        incorp_params: Source kwargs forwarded to
            :meth:`Incorporator.incorp`.
        refresh_params: Optional per-source refresh-stage overrides.
        export_params: Optional per-source export overrides.
    """

    model_config = ConfigDict(frozen=True, extra="allow")

    cls_name: str = Field(..., min_length=1)
    incorp_params: dict[str, Any]
    refresh_params: dict[str, Any] | None = None
    export_params: dict[str, Any] | None = None


class StreamConfig(BaseModel):
    """Schema for an ``incorporator stream`` pipeline.json.

    Field set + cross-field invariants mirror
    :func:`incorporator.cli.validate.validate_stream_config` 1:1.
    Sidecar-file existence checks are intentionally out of scope.
    """

    model_config = ConfigDict(frozen=True, extra="allow")

    incorp_params: dict[str, Any]
    refresh_params: dict[str, Any] | None = None
    export_params: dict[str, Any] | None = None
    poll_interval: float | None = None
    refresh_interval: IntervalSpec | None = None
    export_interval: IntervalSpec | None = None
    stateful_polling: bool = False
    inflow: str | None = None
    outflow: str | None = None

    @model_validator(mode="after")
    def _require_source_key(self) -> StreamConfig:
        """``incorp_params`` must contain at least one recognised source key."""
        if not (_STREAM_SOURCE_KEYS & set(self.incorp_params)):
            raise ValueError(f"'incorp_params' must contain at least one source key: {sorted(_STREAM_SOURCE_KEYS)}.")
        return self

    @model_validator(mode="after")
    def _outflow_requires_stateful_polling(self) -> StreamConfig:
        """``outflow`` on stream requires opting into stateful polling."""
        if self.outflow and not self.stateful_polling:
            raise ValueError(
                "'outflow' requires 'stateful_polling': true.  Chunking-mode "
                "streams release per-chunk state and have no persistent "
                "registry for a user-defined Incorporator subclass to attach "
                "to.  Drop 'outflow', or switch to stateful polling."
            )
        return self


class FjordConfig(BaseModel):
    """Schema for an ``incorporator fjord`` pipeline.json.

    Field set + cross-field invariants mirror
    :func:`incorporator.cli.validate.validate_fjord_config` 1:1.
    Sidecar import, class resolution, and ``outflow(state)`` arity
    checks remain on the CLI side.
    """

    model_config = ConfigDict(frozen=True, extra="allow")

    outflow: str = Field(..., min_length=1)
    stream_params: list[FjordStreamEntry] = Field(..., min_length=1)
    export_params: dict[str, Any]
    inflow: str | None = None
    refresh_interval: IntervalSpec | None = None
    export_interval: IntervalSpec | None = None


PipelineKind = Literal["stream", "fjord"]


def parse_pipeline_config(
    data: dict[str, Any],
    *,
    kind: PipelineKind,
) -> StreamConfig | FjordConfig:
    """Validate an env-expanded pipeline config dict against the matching model.

    Args:
        data: Result of
            :func:`incorporator.cli.runners._load_pipeline_config` —
            already env-expanded and token-resolved.
        kind: The pipeline verb the runner is about to execute.
            Tideweaver configs are NOT covered here; they have their
            own loader in
            :mod:`incorporator.tideweaver.config`.

    Returns:
        A validated, frozen :class:`StreamConfig` or :class:`FjordConfig`.

    Raises:
        pydantic.ValidationError: When ``data`` violates the schema.
    """
    if kind == "stream":
        return StreamConfig.model_validate(data)
    return FjordConfig.model_validate(data)
