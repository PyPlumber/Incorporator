"""Schema and transformation layer for Incorporator."""

from __future__ import annotations

from typing import Any, Dict, List, TypedDict


class JsonSchemaProperty(TypedDict, total=False):
    """Pydantic V2 JSON Schema property descriptor.

    Mirrors the shape of an entry in
    ``BaseModel.model_json_schema()["properties"]``.  All keys are
    optional (``total=False``) because different Python types produce
    different key combinations:

    - ``int`` → ``{"type": "integer"}``
    - ``Optional[int]`` → ``{"anyOf": [{"type": "integer"}, {"type": "null"}]}``
    - ``datetime`` → ``{"type": "string", "format": "date-time"}``
    - ``List[str]`` → ``{"type": "array", "items": {"type": "string"}}``
    - ``str`` (fallback for typeless fields) → ``{"type": "string"}``

    Used as the value type of
    :attr:`incorporator.Incorporator._schema_union` — the per-class
    superset of every field's JSON-Schema property descriptor observed
    across all :meth:`Incorporator.incorp` calls.  Readers
    (``incorporator.schema.factory._expand_conv_dict_with_schema_union``,
    ``incorporator.schema.factory._target_type_from_schema_info``,
    and :meth:`Incorporator.export`'s Avro path) all use defensive
    ``.get(key)`` access, so no key is required at runtime — the
    ``total=False`` matches that contract precisely.

    The recursive references (``anyOf``, ``items``, ``properties``)
    use string forward references so the TypedDict resolves cleanly
    under ``from __future__ import annotations``.
    """

    type: str
    format: str
    anyOf: List["JsonSchemaProperty"]
    items: "JsonSchemaProperty"
    properties: Dict[str, "JsonSchemaProperty"]
    description: str
    title: str
    default: Any


__all__ = ["JsonSchemaProperty"]
