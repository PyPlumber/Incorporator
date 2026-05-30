"""Centralised probe + metadata for fastavro."""

from __future__ import annotations

from typing import Any

from ._types import Category, DepInfo


def _probe() -> Any:
    try:
        import fastavro  # type: ignore[import-not-found, import-untyped, unused-ignore]

        return fastavro
    except ImportError:
        return None


FASTAVRO = _probe()

META = DepInfo(
    name="fastavro",
    extra="avro",
    category=Category.FORMAT,
    description="Avro reader/writer for Apache Avro columnar format",
    version_spec=">=1.8",
    is_available=FASTAVRO is not None,
    module=FASTAVRO,
)
