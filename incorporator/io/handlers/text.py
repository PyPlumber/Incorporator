"""Text-based format handlers: JSON, NDJSON, and XML."""

import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, TextIO, Union, cast

from ._base import BaseFormatHandler, _raise_if_append_unsupported
from ...exceptions import IncorporatorFormatError
from ..formats import check_xml_security, ensure_string, serialize_nested, xml_to_dict

logger = logging.getLogger(__name__)


class JSONHandler(BaseFormatHandler):
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        try:
            import orjson  # type: ignore[import-untyped, import-not-found, unused-ignore]

            if isinstance(source, Path):
                raw_data = source.read_bytes()
            elif isinstance(source, str):
                raw_data = source.encode("utf-8")
            else:
                raw_data = source

            try:
                return cast(Union[Dict[str, Any], List[Dict[str, Any]]], orjson.loads(raw_data))
            except Exception as e:
                raise IncorporatorFormatError(f"Invalid JSON: {e}") from e

        except ImportError:
            import json

            raw_text = ensure_string(source)
            try:
                return cast(Union[Dict[str, Any], List[Dict[str, Any]]], json.loads(raw_text))
            except json.JSONDecodeError as e:
                raise IncorporatorFormatError(f"Invalid JSON: {e}") from e

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        _raise_if_append_unsupported(kwargs, "JSON")
        path = Path(file_path).resolve()
        try:
            import orjson  # type: ignore[import-untyped, import-not-found, unused-ignore]

            # Streaming JSON array: write one record at a time — no full-list materialization.
            try:
                with open(path, "wb") as f:
                    f.write(b"[\n")
                    first = True
                    for item in data:
                        if not first:
                            f.write(b",\n")
                        f.write(orjson.dumps(item, option=orjson.OPT_INDENT_2))
                        first = False
                    f.write(b"\n]")
            except OSError as e:
                raise IncorporatorFormatError(f"JSON File IO Error on {file_path}: {e}") from e

        except ImportError:
            import json

            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write("[\n")
                    first = True
                    for item in data:
                        if not first:
                            f.write(",\n")
                        f.write(json.dumps(item, indent=4))
                        first = False
                    f.write("\n]")
            except OSError as e:
                raise IncorporatorFormatError(f"JSON File IO Error on {file_path}: {e}") from e


class NDJSONHandler(BaseFormatHandler):
    def _parse_stream(self, stream: Union[TextIO, List[str]]) -> List[Dict[str, Any]]:
        import json

        rows: List[Dict[str, Any]] = []
        for line_num, line in enumerate(stream, start=1):
            clean_line = line.strip()
            if not clean_line:
                continue
            try:
                rows.append(json.loads(clean_line))
            except json.JSONDecodeError as e:
                raise IncorporatorFormatError(f"Invalid NDJSON on line {line_num}: {e}") from e
        return rows

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        if isinstance(source, Path):
            with open(source, "rt", encoding="utf-8") as f:
                return self._parse_stream(f)
        else:
            raw_data = ensure_string(source)
            return self._parse_stream(raw_data.splitlines())

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        import json

        try:
            path = Path(file_path).resolve()
            mode = "a" if kwargs.get("if_exists") == "append" else "w"
            with open(path, mode, encoding="utf-8") as f:
                for item in data:
                    f.write(json.dumps(item) + "\n")
        except OSError as e:
            raise IncorporatorFormatError(f"NDJSON File IO Error on {file_path}: {e}") from e


def _build_xml_root(data: List[Dict[str, Any]], ET: Any) -> Any:
    """Builds an XML root element from a list of dicts using any ElementTree-compatible module."""
    root = ET.Element("root")
    for item in data:
        item_el = ET.SubElement(root, "item")
        for key, val in item.items():
            clean_key = str(key).replace(" ", "_")
            if clean_key and clean_key[0].isdigit():
                clean_key = f"_{clean_key}"
            child = ET.SubElement(item_el, clean_key)
            safe_val = serialize_nested(val)
            child.text = str(safe_val) if safe_val is not None else ""
    return root


class XMLHandler(BaseFormatHandler):
    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        try:
            import lxml.etree as lxml_ET  # type: ignore[import-untyped, import-not-found, unused-ignore]

            raw_bytes = (
                source.read_bytes()
                if isinstance(source, Path)
                else source.encode("utf-8")
                if isinstance(source, str)
                else source
            )
            parser = lxml_ET.XMLParser(resolve_entities=False, no_network=True)

            try:
                root = lxml_ET.fromstring(raw_bytes, parser=parser)
                return xml_to_dict(root)
            except lxml_ET.ParseError:
                root = lxml_ET.fromstring(raw_bytes.strip(), parser=parser)
                return xml_to_dict(root)

        except ImportError:
            import xml.etree.ElementTree as ET

            raw_str = ensure_string(source)
            check_xml_security(raw_str)

            try:
                root = ET.fromstring(raw_str)  # noqa: S314
                return xml_to_dict(root)
            except ET.ParseError:
                try:
                    root = ET.fromstring(raw_str.strip())  # noqa: S314
                    return xml_to_dict(root)
                except ET.ParseError as e:
                    raise IncorporatorFormatError(f"Invalid XML: {e}") from e

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        _raise_if_append_unsupported(kwargs, "XML")
        # XML requires a full DOM in memory — intentionally materialize here.
        # ElementTree cannot write a streaming element tree incrementally.
        data_list: List[Dict[str, Any]] = list(data)
        path = Path(file_path).resolve()
        try:
            import lxml.etree as lxml_ET  # type: ignore[import-untyped, import-not-found, unused-ignore]

            root = _build_xml_root(data_list, lxml_ET)
            lxml_ET.ElementTree(root).write(str(path), encoding="utf-8", xml_declaration=True, pretty_print=True)

        except ImportError:
            import xml.etree.ElementTree as ET

            try:
                with open(path, "w", encoding="utf-8") as f:
                    root = _build_xml_root(data_list, ET)
                    ET.ElementTree(root).write(f, encoding="unicode")
            except OSError as e:
                raise IncorporatorFormatError(f"XML File IO Error on {file_path}: {e}") from e
