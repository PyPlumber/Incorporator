"""Schema and transformation layer for Incorporator."""

from __future__ import annotations

from typing import Any, TypedDict


class JsonSchemaProperty(TypedDict, total=False):
    """Pydantic V2 JSON Schema property descriptor.

    Mirrors one entry in ``BaseModel.model_json_schema()["properties"]``.
    All keys are optional (``total=False``) because different Python types
    produce different key combinations (``int`` → ``{"type": "integer"}``,
    ``int | None`` → ``{"anyOf": [...]}``, etc.).

    Used as the value type of
    :attr:`incorporator.Incorporator._schema_union`; readers use defensive
    ``.get(key)`` so no key is required at runtime.
    """

    type: str
    format: str
    anyOf: list[JsonSchemaProperty]
    items: JsonSchemaProperty
    properties: dict[str, JsonSchemaProperty]
    description: str
    title: str
    default: Any


__all__ = ["JsonSchemaProperty"]
