"""Abstract base handler and shared utilities for format I/O."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Union

from ...exceptions import IncorporatorFormatError


def _raise_if_append_unsupported(kwargs: Dict[str, Any], format_name: str) -> None:
    if kwargs.get("if_exists") == "append":
        raise IncorporatorFormatError(
            f"Monolithic formats ({format_name}) do not support O(1) streaming appends. "
            "Please stream to NDJSON, CSV, SQLite, or Avro instead."
        )


class BaseFormatHandler(ABC):
    """Abstract Strategy for parsing and writing different data formats."""

    @abstractmethod
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        pass

    @abstractmethod
    def write(self, data: List[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        pass
