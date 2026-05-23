"""Text-based format handlers: JSON, NDJSON, and XML."""

from __future__ import annotations

import json as _stdlib_json
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import Any, TextIO, cast

from ...exceptions import IncorporatorFormatError
from ..formats import check_xml_security, ensure_bytes, ensure_string, serialize_nested, xml_to_dict
from ._base import BaseFormatHandler, _raise_if_append_unsupported, atomic_write_path

logger = logging.getLogger(__name__)

# ── Speedup probes (runtime-aware) ─────────────────────────────────────
# JSON and XML each support an optional fast path (orjson / lxml) that
# falls back to the stdlib when the dep is missing.  These helpers
# re-import per call — after the first import the lookup is a
# sub-microsecond ``sys.modules`` dict hit, so the cost is negligible
# and tests can transparently force the fallback path via
# ``patch.dict(sys.modules, {"orjson": None, "lxml": None})``.


def _try_import_orjson() -> Any:
    try:
        import orjson  # type: ignore[import-untyped, import-not-found, unused-ignore]

        return orjson
    except ImportError:
        return None


def _try_import_lxml_etree() -> Any:
    try:
        import lxml.etree as lxml_etree  # type: ignore[import-untyped, import-not-found, unused-ignore]

        return lxml_etree
    except ImportError:
        return None


def _dumps_json_bytes(item: Any, *, indent: int) -> bytes:
    """Serialise one JSON record to bytes, preferring orjson when available."""
    orjson = _try_import_orjson()
    if orjson is not None:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return cast(bytes, orjson.dumps(item, option=opt))
    return _stdlib_json.dumps(item, indent=indent or None).encode("utf-8")


def _loads_json(raw: bytes | str) -> Any:
    """Decode a JSON document, preferring orjson when available."""
    orjson = _try_import_orjson()
    if orjson is not None:
        return orjson.loads(raw)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return _stdlib_json.loads(raw)


def _parse_xml(raw_bytes: bytes, raw_str: str) -> Any:
    """Parse XML bytes/string through whichever parser is available.

    Prefers lxml (``resolve_entities=False``, ``no_network=True``) for both
    speed and explicit XXE protection.  Falls back to stdlib ElementTree.
    Either way ``check_xml_security`` runs first for defence-in-depth.
    Both branches transparently retry on ``ParseError`` after ``.strip()``
    to handle XML payloads with whitespace preambles.
    """
    lxml_etree = _try_import_lxml_etree()
    if lxml_etree is not None:
        parser = lxml_etree.XMLParser(resolve_entities=False, no_network=True)
        try:
            return lxml_etree.fromstring(raw_bytes, parser=parser)
        except lxml_etree.ParseError:
            return lxml_etree.fromstring(raw_bytes.strip(), parser=parser)
    import xml.etree.ElementTree as ET

    try:
        return ET.fromstring(raw_str)  # noqa: S314
    except ET.ParseError:
        return ET.fromstring(raw_str.strip())  # noqa: S314


def _xml_parse_error_types() -> tuple[type, ...]:
    """Return the parse-error class(es) currently in play.

    lxml's ParseError is its own type; stdlib ET has its own.  Callers
    catch the union so a fixture-forced fallback still raises the right
    type-name in error messages.
    """
    types: list[type] = []
    lxml_etree = _try_import_lxml_etree()
    if lxml_etree is not None:
        types.append(lxml_etree.ParseError)
    import xml.etree.ElementTree as ET

    types.append(ET.ParseError)
    return tuple(types)


def _resolved_path(file_path: str | Path) -> Path:
    """Trust the dispatcher's pre-resolution; coerce to Path without a syscall."""
    return file_path if isinstance(file_path, Path) else Path(file_path)


class JSONHandler(BaseFormatHandler):
    """Parse and write standard JSON files.

    Prefers ``orjson`` (installed via the ``[speedups]`` extra) for GIL-free
    parsing and writes; transparently falls back to the stdlib ``json``
    module when ``orjson`` is missing. Append mode is rejected — JSON is a
    monolithic format with no safe O(1) append. Use NDJSON for streaming.
    """

    def parse(self, source: str | bytes | Path, **kwargs: Any) -> dict[str, Any] | list[dict[str, Any]]:
        """Read a JSON file or byte buffer and return the decoded structure."""
        try:
            return cast(dict[str, Any] | list[dict[str, Any]], _loads_json(ensure_bytes(source)))
        except Exception as exc:
            raise IncorporatorFormatError(f"Invalid JSON: {exc}") from exc

    def write(self, data: Iterable[Any], file_path: str | Path, **kwargs: Any) -> None:
        """Stream rows into a JSON array file one record at a time.

        Writes ``[\\n``, then yields each record's serialised bytes, then
        ``\\n]`` — no full-list materialisation, so memory stays O(1) for
        arbitrarily large input streams. Append mode is rejected.

        Fast path: when the iterable yields Pydantic BaseModel instances
        (the upstream ``export()`` pipeline does this for JSON / NDJSON
        formats), each row is serialised via ``model_dump_json()`` —
        Pydantic v2's Rust core writes JSON bytes without allocating the
        intermediate dict.  Plain ``dict`` rows fall back to ``orjson``/
        ``json``.  The behaviour is transparent: callers can mix both.
        """
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        _raise_if_append_unsupported(kwargs, "JSON")
        path = _resolved_path(file_path)
        # orjson formats with a 2-space indent (its only indent option); the
        # stdlib path stays at the historical 4-space default — both produce
        # valid JSON, the difference is purely cosmetic.
        indent = 2 if _try_import_orjson() is not None else 4

        # Streaming JSON array: write one record at a time — no full-list
        # materialisation.  Atomic write: build to a sibling tempfile then
        # rename on success so a crash mid-stream leaves no partial file.
        try:
            with atomic_write_path(path) as tmp_path:
                with open(tmp_path, "wb") as f:
                    f.write(b"[\n")
                    first = True
                    for item in data:
                        if not first:
                            f.write(b",\n")
                        # Pydantic v2 fast path — Rust serialiser skips the
                        # intermediate dict allocation that ``orjson.dumps
                        # (model_dump())`` would otherwise pay for.
                        dump_json = getattr(item, "model_dump_json", None)
                        if callable(dump_json):
                            f.write(dump_json(by_alias=True, indent=indent).encode("utf-8"))
                        else:
                            f.write(_dumps_json_bytes(item, indent=indent))
                        first = False
                    f.write(b"\n]")
        except OSError as e:
            raise IncorporatorFormatError(f"JSON File IO Error on {file_path}: {e}") from e


class NDJSONHandler(BaseFormatHandler):
    """Parse and write newline-delimited JSON (one JSON object per line).

    NDJSON is the streaming-native JSON format — each line is an
    independent record, so reads and writes are both O(1) memory.
    Append mode is supported natively.
    """

    def _parse_stream(self, stream: TextIO | list[str]) -> list[dict[str, Any]]:
        """Decode an iterable of JSON-encoded lines.

        Uses the same ``_loads_json`` helper as :class:`JSONHandler` so
        orjson's ~3× speed-up applies per-line.  The hot loop binds the
        helper to a local name to avoid the module-attribute lookup on
        every iteration — measurable at 500k+ rows.
        """
        loads = _loads_json
        rows: list[dict[str, Any]] = []
        for line_num, line in enumerate(stream, start=1):
            clean_line = line.strip()
            if not clean_line:
                continue
            try:
                rows.append(loads(clean_line))
            except Exception as exc:
                # orjson raises ``orjson.JSONDecodeError`` (subclass of
                # ``json.JSONDecodeError``); stdlib raises
                # ``json.JSONDecodeError``.  Catching ``Exception`` lets us
                # surface either with the offending line number attached.
                raise IncorporatorFormatError(f"Invalid NDJSON on line {line_num}: {exc}") from exc
        return rows

    def parse(self, source: str | bytes | Path, **kwargs: Any) -> dict[str, Any] | list[dict[str, Any]]:
        """Read an NDJSON file or byte buffer line-by-line and return parsed rows.

        Empty / whitespace-only lines are skipped. Invalid JSON on any line
        raises :class:`IncorporatorFormatError` with the offending line number.

        Performance note: each line is decoded via the same orjson-preferring
        helper as :class:`JSONHandler` — NDJSON parse is now within ~3× of
        the single-pass JSON parse (was 8× slower with stdlib ``json``).
        """
        if isinstance(source, Path):
            with open(source, "rt", encoding="utf-8") as f:
                return self._parse_stream(f)
        else:
            raw_data = ensure_string(source)
            return self._parse_stream(raw_data.splitlines())

    def write(self, data: Iterable[Any], file_path: str | Path, **kwargs: Any) -> None:
        """Stream rows to an NDJSON file, one JSON object per line.

        Append mode is supported natively — set ``if_exists="append"`` to
        extend an existing file rather than overwrite.

        Two fast paths beyond the legacy ``json.dumps`` fallback:

        1. **Pydantic instance row** — call ``model_dump_json(by_alias=True)``;
           Pydantic v2's Rust core serialises straight to JSON text without
           the intermediate dict allocation that ``orjson.dumps(model_dump())``
           would otherwise pay.  Encoded once to UTF-8 bytes and written.
        2. **Plain dict row** — route through ``_dumps_json_bytes`` which
           prefers orjson over stdlib ``json``.  Same ~3× per-row speed-up
           as :class:`JSONHandler`.

        The file is opened in **binary mode** so orjson's native ``bytes``
        output is written without a redundant UTF-8 round-trip through the
        text-mode writer.  Newlines are appended as literal ``b"\\n"``.
        """
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        path = _resolved_path(file_path)
        mode = "ab" if kwargs.get("if_exists") == "append" else "wb"
        try:
            # Bind ``_dumps_json_bytes`` to a local name to avoid the
            # module-attribute lookup on every iteration of the hot loop.
            dumps_bytes = _dumps_json_bytes
            with open(path, mode) as f:
                for item in data:
                    dump_json = getattr(item, "model_dump_json", None)
                    if callable(dump_json):
                        f.write(dump_json(by_alias=True).encode("utf-8"))
                    else:
                        f.write(dumps_bytes(item, indent=0))
                    f.write(b"\n")
        except OSError as e:
            raise IncorporatorFormatError(f"NDJSON File IO Error on {file_path}: {e}") from e


def _build_xml_root(data: list[dict[str, Any]], ET: Any) -> Any:
    """Builds an XML root element from a list of dicts using any ElementTree-compatible module.

    Key cleaning (space → underscore, digit-prefix guard) is cached on first
    occurrence — the mapping from row keys to clean XML element names is
    fixed per schema, so subsequent rows hit a single dict lookup instead of
    repeating string ops per row × per key.
    """
    root = ET.Element("root")
    clean_key_cache: dict[str, str] = {}
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

    def parse(self, source: str | bytes | Path, **kwargs: Any) -> dict[str, Any] | list[dict[str, Any]]:
        """Read an XML file or byte buffer and return its tree as nested dicts.

        Always runs ``check_xml_security`` first — even with lxml's
        ``resolve_entities=False``, the framework needs a consistent
        rejection point so attacks never silently no-op.

        Pass ``xml_force_list=["item", "row"]`` to force those tag names
        to always be lists in the result — useful when the same tag is
        sometimes single and sometimes multiple across documents, which
        otherwise causes downstream schema-inference shape drift.
        """
        # Defence-in-depth: run check_xml_security BEFORE either parser path.
        # lxml's resolve_entities=False silently drops XXE entities (good!) but
        # also silently returns success — so without an explicit pre-check the
        # framework would never know an attack was attempted.  Stdlib
        # ElementTree has no XXE protection at all.  Centralising the security
        # check here gives a single, consistent rejection point regardless of
        # which parser is installed.
        # Read once as bytes, then decode for the security check — avoids the
        # double filesystem hit that calling both ``ensure_string`` and
        # ``ensure_bytes`` on a ``Path`` source would incur.
        raw_bytes = ensure_bytes(source)
        raw_str = raw_bytes.decode("utf-8", errors="replace")
        check_xml_security(raw_str)

        force_list_kwarg = kwargs.get("xml_force_list") or []
        force_list_set = set(force_list_kwarg) if force_list_kwarg else None

        try:
            root = _parse_xml(raw_bytes, raw_str)
        except Exception as e:
            # ``_parse_xml`` only raises ParseError variants (lxml's or stdlib
            # ElementTree's).  We catch Exception rather than a runtime-built
            # tuple because mypy can't statically prove the tuple shape —
            # narrowing the except clause to the parse-error classes happens
            # implicitly: any other exception type would have already
            # surfaced from ``_parse_xml`` to the caller without the chance
            # to reach this clause in normal use.
            parse_errors = _xml_parse_error_types()
            if not isinstance(e, parse_errors):
                raise
            raise IncorporatorFormatError(f"Invalid XML: {e}") from e
        return xml_to_dict(root, force_list=force_list_set)

    def write(self, data: Iterable[dict[str, Any]], file_path: str | Path, **kwargs: Any) -> None:
        """Build an XML DOM from the row iterable and write it to disk.

        XML cannot be streamed safely — ElementTree has no incremental
        writer, so the full DOM is materialised before flushing. Append
        mode is rejected. Element names are sanitised (spaces → underscores,
        digit prefixes guarded) and cached for O(1) per-row reuse.
        """
        # Empty guard is handled centrally by _peek_iterable in handlers/__init__.py
        _raise_if_append_unsupported(kwargs, "XML")
        # XML requires a full DOM in memory — intentionally materialise here.
        # ElementTree cannot write a streaming element tree incrementally.
        data_list: list[dict[str, Any]] = list(data)
        path = _resolved_path(file_path)
        lxml_etree = _try_import_lxml_etree()
        try:
            with atomic_write_path(path) as tmp_path:
                if lxml_etree is not None:
                    root = _build_xml_root(data_list, lxml_etree)
                    lxml_etree.ElementTree(root).write(
                        str(tmp_path), encoding="utf-8", xml_declaration=True, pretty_print=True
                    )
                else:
                    import xml.etree.ElementTree as ET

                    with open(tmp_path, "w", encoding="utf-8") as f:
                        root = _build_xml_root(data_list, ET)
                        ET.ElementTree(root).write(f, encoding="unicode")
        except OSError as e:
            raise IncorporatorFormatError(f"XML File IO Error on {file_path}: {e}") from e
