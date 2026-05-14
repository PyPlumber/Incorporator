"""Text-based format handlers: JSON, NDJSON, and XML."""

import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, TextIO, Union, cast

from ...exceptions import IncorporatorFormatError
from ..formats import check_xml_security, ensure_string, serialize_nested, xml_to_dict
from ._base import BaseFormatHandler, _raise_if_append_unsupported, atomic_write_path

logger = logging.getLogger(__name__)


class JSONHandler(BaseFormatHandler):
    """Parse and write standard JSON files.

    Prefers ``orjson`` (installed via the ``[speedups]`` extra) for GIL-free
    parsing and writes; transparently falls back to the stdlib ``json``
    module when ``orjson`` is missing. Append mode is rejected — JSON is a
    monolithic format with no safe O(1) append. Use NDJSON for streaming.
    """

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Read a JSON file or byte buffer and return the decoded structure."""
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
        """Stream rows into a JSON array file one record at a time.

        Writes ``[\\n``, then yields each record's serialised bytes, then
        ``\\n]`` — no full-list materialisation, so memory stays O(1) for
        arbitrarily large input streams. Append mode is rejected.
        """
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        _raise_if_append_unsupported(kwargs, "JSON")
        path = Path(file_path).resolve()
        try:
            import orjson  # type: ignore[import-untyped, import-not-found, unused-ignore]

            # Streaming JSON array: write one record at a time — no full-list materialization.
            # Atomic write: build to a sibling tempfile then rename on success.
            try:
                with atomic_write_path(path) as tmp_path:
                    with open(tmp_path, "wb") as f:
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
                with atomic_write_path(path) as tmp_path:
                    with open(tmp_path, "w", encoding="utf-8") as f:
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
    """Parse and write newline-delimited JSON (one JSON object per line).

    NDJSON is the streaming-native JSON format — each line is an
    independent record, so reads and writes are both O(1) memory.
    Append mode is supported natively.
    """

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
        """Read an NDJSON file or byte buffer line-by-line and return parsed rows.

        Empty / whitespace-only lines are skipped. Invalid JSON on any line
        raises :class:`IncorporatorFormatError` with the offending line number.
        """
        if isinstance(source, Path):
            with open(source, "rt", encoding="utf-8") as f:
                return self._parse_stream(f)
        else:
            raw_data = ensure_string(source)
            return self._parse_stream(raw_data.splitlines())

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        """Stream rows to an NDJSON file, one ``json.dumps()`` line per row.

        Append mode is supported natively — set ``if_exists="append"`` to
        extend an existing file rather than overwrite.
        """
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
    """Builds an XML root element from a list of dicts using any ElementTree-compatible module.

    Key cleaning (space → underscore, digit-prefix guard) is cached on first
    occurrence — the mapping from row keys to clean XML element names is
    fixed per schema, so subsequent rows hit a single dict lookup instead of
    repeating string ops per row × per key.
    """
    root = ET.Element("root")
    clean_key_cache: Dict[str, str] = {}
    for item in data:
        item_el = ET.SubElement(root, "item")
        for key, val in item.items():
            clean_key = clean_key_cache.get(key)
            if clean_key is None:
                clean_key = str(key).replace(" ", "_")
                if clean_key and clean_key[0].isdigit():
                    clean_key = f"_{clean_key}"
                clean_key_cache[key] = clean_key
            child = ET.SubElement(item_el, clean_key)
            safe_val = serialize_nested(val)
            child.text = str(safe_val) if safe_val is not None else ""
    return root


class XMLHandler(BaseFormatHandler):
    """Parse and write XML files with built-in XXE protection.

    Prefers ``lxml`` (installed via ``[speedups]``) for performance; falls
    back to the stdlib ``xml.etree.ElementTree`` when missing. Every payload
    runs through ``check_xml_security`` before parsing — defense-in-depth
    against XXE / billion-laughs / external-entity attacks regardless of
    which parser is active. Append mode is rejected — XML requires a full
    DOM in memory for safe writes.
    """

    def parse(self, source: Union[str, bytes, Path], **kwargs: Any) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Read an XML file or byte buffer and return its tree as nested dicts.

        Always runs ``check_xml_security`` first — even with lxml's
        ``resolve_entities=False``, the framework needs a consistent
        rejection point so attacks never silently no-op.

        Pass ``xml_force_list=["item", "row"]`` to force those tag names
        to always be lists in the result — useful when the same tag is
        sometimes single and sometimes multiple across documents, which
        otherwise causes downstream schema-inference shape drift.
        """
        # Defense-in-depth: run check_xml_security BEFORE either parser path.
        # lxml's resolve_entities=False silently drops XXE entities (good!) but
        # also silently returns success — so the framework would never know an
        # attack was attempted. Stdlib ElementTree has no XXE protection at
        # all. Centralizing the security check here gives us a single,
        # consistent rejection point regardless of which parser is installed.
        raw_str_for_check = source.read_text(encoding="utf-8") if isinstance(source, Path) else ensure_string(source)
        check_xml_security(raw_str_for_check)

        force_list_kwarg = kwargs.get("xml_force_list") or []
        force_list_set = set(force_list_kwarg) if force_list_kwarg else None

        try:
            import lxml.etree as lxml_ET  # type: ignore[import-untyped, import-not-found, unused-ignore]

            if isinstance(source, Path):
                raw_bytes = source.read_bytes()
            elif isinstance(source, str):
                raw_bytes = source.encode("utf-8")
            else:
                raw_bytes = source
            parser = lxml_ET.XMLParser(resolve_entities=False, no_network=True)

            try:
                root = lxml_ET.fromstring(raw_bytes, parser=parser)
                return xml_to_dict(root, force_list=force_list_set)
            except lxml_ET.ParseError:
                root = lxml_ET.fromstring(raw_bytes.strip(), parser=parser)
                return xml_to_dict(root, force_list=force_list_set)

        except ImportError:
            import xml.etree.ElementTree as ET

            try:
                root = ET.fromstring(raw_str_for_check)  # noqa: S314
                return xml_to_dict(root, force_list=force_list_set)
            except ET.ParseError:
                try:
                    root = ET.fromstring(raw_str_for_check.strip())  # noqa: S314
                    return xml_to_dict(root, force_list=force_list_set)
                except ET.ParseError as e:
                    raise IncorporatorFormatError(f"Invalid XML: {e}") from e

    def write(self, data: Iterable[Dict[str, Any]], file_path: Union[str, Path], **kwargs: Any) -> None:
        """Build an XML DOM from the row iterable and write it to disk.

        XML cannot be streamed safely — ElementTree has no incremental
        writer, so the full DOM is materialised before flushing. Append
        mode is rejected. Element names are sanitised (spaces → underscores,
        digit prefixes guarded) and cached for O(1) per-row reuse.
        """
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        _raise_if_append_unsupported(kwargs, "XML")
        # XML requires a full DOM in memory — intentionally materialize here.
        # ElementTree cannot write a streaming element tree incrementally.
        data_list: List[Dict[str, Any]] = list(data)
        path = Path(file_path).resolve()
        try:
            import lxml.etree as lxml_ET  # type: ignore[import-untyped, import-not-found, unused-ignore]

            root = _build_xml_root(data_list, lxml_ET)
            # Atomic write: build to tempfile, rename on success so an
            # interrupted write doesn't leave a malformed XML on disk.
            with atomic_write_path(path) as tmp_path:
                lxml_ET.ElementTree(root).write(
                    str(tmp_path), encoding="utf-8", xml_declaration=True, pretty_print=True
                )

        except ImportError:
            import xml.etree.ElementTree as ET

            try:
                with atomic_write_path(path) as tmp_path:
                    with open(tmp_path, "w", encoding="utf-8") as f:
                        root = _build_xml_root(data_list, ET)
                        ET.ElementTree(root).write(f, encoding="unicode")
            except OSError as e:
                raise IncorporatorFormatError(f"XML File IO Error on {file_path}: {e}") from e
