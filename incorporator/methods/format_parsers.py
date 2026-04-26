"""Native zero-bloat format converters and handlers for Incorporator."""

import asyncio
import csv
import io
import json
import re
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Union

from .exceptions import IncorporatorFormatError


class FormatType(str, Enum):
    """Strict enumeration of supported data formats."""
    JSON = "json"
    CSV = "csv"
    XML = "xml"


def infer_format(path_or_url: str) -> FormatType:
    """Helper to auto-detect format from a file extension or URL."""
    path_lower = path_or_url.lower()
    if path_lower.endswith(".csv"):
        return FormatType.CSV
    if path_lower.endswith(".xml"):
        return FormatType.XML
    return FormatType.JSON

def _serialize_nested(val: Any) -> Any:
    """Safely serializes nested lists/dicts to JSON strings for flat format exports."""
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return val

def _xml_to_dict(element: ET.Element) -> Dict[str, Any]:
    """Recursively converts an XML ElementTree into a Python dictionary."""
    result: Dict[str, Any] = {element.tag: {} if element.attrib else None}
    children = list(element)

    if children:
        child_dict: Dict[str, Any] = {}
        for child in children:
            child_result = _xml_to_dict(child)
            for key, val in child_result.items():
                if key in child_dict:
                    if not isinstance(child_dict[key], list):
                        child_dict[key] = [child_dict[key]]
                    child_dict[key].append(val)
                else:
                    child_dict[key] = val
        if element.attrib:
            child_dict.update(element.attrib)
        result = {element.tag: child_dict}
    elif element.text and element.text.strip():
        val_text = element.text.strip()
        if element.attrib:
            leaf_dict: Dict[str, Any] = dict(element.attrib)
            leaf_dict['text'] = val_text
            result = {element.tag: leaf_dict}
        else:
            result = {element.tag: val_text}

    return result


def _check_xml_security(raw_data: str) -> None:
    """Pre-flight check to block DTDs and Entities (XXE) without external dependencies."""
    if re.search(r'<!(?:DOCTYPE|ENTITY)', raw_data, re.IGNORECASE):
        raise IncorporatorFormatError(
            "Security Policy Violation: XML DTDs and External Entities (XXE) are strictly "
            "blocked to prevent 'Billion Laughs' memory exhaustion attacks."
        )


class BaseFormatHandler(ABC):
    """Abstract Strategy for parsing and writing different data formats."""

    @abstractmethod
    def parse(self, raw_data: str) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Parses raw string data into dictionaries."""
        pass

    @abstractmethod
    def write(self, data: List[Dict[str, Any]], file_path: str) -> None:
        """Writes dictionary data to disk in the target format."""
        pass


class JSONHandler(BaseFormatHandler):
    def parse(self, raw_data: str) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        try:
            result: Union[Dict[str, Any], List[Dict[str, Any]]] = json.loads(raw_data)
            return result
        except json.JSONDecodeError as e:
            raise IncorporatorFormatError(f"Invalid JSON: {e}")

    def write(self, data: List[Dict[str, Any]], file_path: str) -> None:
        try:
            path = Path(file_path).resolve()
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
        except OSError as e:
            raise IncorporatorFormatError(f"JSON File IO Error on {file_path}: {e}") from e


class CSVHandler(BaseFormatHandler):
    def parse(self, raw_data: str) -> List[Dict[str, Any]]:
        try:
            # DSA: Passed raw_data directly to avoid O(N) memory duplication from .strip()
            reader = csv.DictReader(io.StringIO(raw_data))
            rows: List[Dict[str, Any]] = []

            for row in reader:
                parsed_row: Dict[str, Any] = {}
                for k, v in row.items():
                    safe_k = str(k) if k is not None else "unknown_column"

                    # DSA: Replaced string method calls with O(1) index lookups for tight loop efficiency
                    if v and (
                            (v[0] == '{' and v[-1] == '}') or
                            (v[0] == '[' and v[-1] == ']')
                    ):
                        try:
                            parsed_row[safe_k] = json.loads(v)
                        except json.JSONDecodeError:
                            parsed_row[safe_k] = v
                    else:
                        parsed_row[safe_k] = v

                rows.append(parsed_row)
            return rows
        except csv.Error as e:
            raise IncorporatorFormatError(f"Invalid CSV: {e}")

    def write(self, data: List[Dict[str, Any]], file_path: str) -> None:
        if not data:
            return

        try:
            path = Path(file_path).resolve()
            with open(path, 'w', encoding='utf-8', newline='') as f:
                processed_data =[{k: _serialize_nested(v) for k, v in row.items()} for row in data]

                writer = csv.DictWriter(f, fieldnames=list(processed_data[0].keys()))
                writer.writeheader()
                writer.writerows(processed_data)
        except OSError as e:
            raise IncorporatorFormatError(f"CSV File IO Error on {file_path}: {e}") from e


class XMLHandler(BaseFormatHandler):
    def parse(self, raw_data: str) -> Dict[str, Any]:
        _check_xml_security(raw_data)
        try:
            # DSA: Try O(1) memory parsing first without .strip() string duplication
            root = ET.fromstring(raw_data)
            return _xml_to_dict(root)
        except ET.ParseError:
            try:
                # Fallback: Only duplicate memory if strict parser chokes on leading whitespace
                root = ET.fromstring(raw_data.strip())
                return _xml_to_dict(root)
            except ET.ParseError as e:
                raise IncorporatorFormatError(f"Invalid XML: {e}")

    def write(self, data: List[Dict[str, Any]], file_path: str) -> None:
        try:
            path = Path(file_path).resolve()
            with open(path, 'w', encoding='utf-8') as f:
                root = ET.Element("root")
                for item in data:
                    item_el = ET.SubElement(root, "item")
                    for key, val in item.items():
                        clean_key = str(key).replace(" ", "_")
                        # Tag names must not start with a number in XML
                        if clean_key and clean_key[0].isdigit():
                            clean_key = f"_{clean_key}"

                        child = ET.SubElement(item_el, clean_key)
                        safe_val = _serialize_nested(val)
                        child.text = str(safe_val) if safe_val is not None else ""

                tree = ET.ElementTree(root)
                tree.write(f, encoding='unicode')
        except OSError as e:
            raise IncorporatorFormatError(f"XML File IO Error on {file_path}: {e}") from e


# Registry mapping Enums to their respective Handler instances
_HANDLERS: Dict[FormatType, BaseFormatHandler] = {
    FormatType.JSON: JSONHandler(),
    FormatType.CSV: CSVHandler(),
    FormatType.XML: XMLHandler(),
}


async def parse_source_data(raw_data: str, format_type: FormatType) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    """Asynchronously routes raw string data to the correct parser strategy."""
    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported format: '{format_type}'.")

    # Offload CPU-bound parsing to a background thread to protect the event loop
    return await asyncio.to_thread(handler.parse, raw_data)


async def write_destination_data(data: List[Dict[str, Any]], file_path: str, format_type: FormatType) -> None:
    """Asynchronously routes dictionary data to the correct writer strategy."""
    handler = _HANDLERS.get(format_type)
    if not handler:
        raise IncorporatorFormatError(f"Unsupported export format: '{format_type}'.")

    # Offload I/O-bound writing to a background thread to protect the event loop
    await asyncio.to_thread(handler.write, data, file_path)