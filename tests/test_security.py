"""Security hardening tests: XXE injection and TAR path traversal blocking."""

import io
import tarfile
from pathlib import Path

import pytest

from incorporator.exceptions import IncorporatorFormatError
from incorporator.io.formats import check_xml_security
from incorporator.io.handlers.text import XMLHandler


# ==========================================
# 1. XML XXE / INJECTION TESTS
# ==========================================


def test_xxe_classic_entity_blocked() -> None:
    """Classic XXE: <!DOCTYPE> with an <!ENTITY> declaration must be blocked."""
    xxe_payload = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        "<root><item><name>&xxe;</name></item></root>"
    )
    with pytest.raises(IncorporatorFormatError, match="Security Policy Violation"):
        check_xml_security(xxe_payload)


def test_xxe_doctype_only_blocked() -> None:
    """DOCTYPE alone (without entity) must still be blocked."""
    doctype_payload = '<?xml version="1.0"?><!DOCTYPE foo><root/>'
    with pytest.raises(IncorporatorFormatError, match="Security Policy Violation"):
        check_xml_security(doctype_payload)


def test_xxe_parameter_entity_blocked() -> None:
    """Parameter entity references (%xxe;) must be caught by the updated regex."""
    # Parameter entity in XML body — triggers the %[a-zA-Z_][\w.-]*; pattern
    param_entity_payload = "<?xml version='1.0'?><root>%xxe;</root>"
    with pytest.raises(IncorporatorFormatError, match="Security Policy Violation"):
        check_xml_security(param_entity_payload)


def test_xxe_case_insensitive_blocked() -> None:
    """Case-insensitive matching must catch <!doctype> and <!ENTITY> variants."""
    lower_doctype = "<?xml version='1.0'?><!doctype foo><root/>"
    with pytest.raises(IncorporatorFormatError, match="Security Policy Violation"):
        check_xml_security(lower_doctype)


def test_safe_xml_passes_check() -> None:
    """Well-formed, safe XML must NOT raise."""
    safe_xml = "<root><item><id>1</id><name>Alice</name></item></root>"
    check_xml_security(safe_xml)  # Should not raise


def test_xml_handler_parse_raises_on_xxe(tmp_path: Path) -> None:
    """XMLHandler.parse() must block XXE at the file level too."""
    xxe_file = tmp_path / "evil.xml"
    xxe_file.write_text(
        '<?xml version="1.0"?><!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>'
        "<root><item><name>&xxe;</name></item></root>",
        encoding="utf-8",
    )
    handler = XMLHandler()
    with pytest.raises(IncorporatorFormatError):
        handler.parse(xxe_file)


# ==========================================
# 2. TAR PATH TRAVERSAL TESTS
# ==========================================


def _make_tar_with_member(member_name: str, content: bytes = b"pwned") -> bytes:
    """Helper: Creates an in-memory TAR archive containing a single member at the given path."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        info = tarfile.TarInfo(name=member_name)
        info.size = len(content)
        tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def test_tar_dotdot_traversal_blocked() -> None:
    """TAR archive with ../../etc/passwd member must raise IncorporatorFormatError."""
    from incorporator.io.compression import decompress_data
    from incorporator.io.formats import FormatType

    tar_bytes = _make_tar_with_member("../../etc/passwd")
    with pytest.raises(IncorporatorFormatError, match="path traversal blocked"):
        decompress_data(tar_bytes, "archive.tar", FormatType.JSON)


def test_tar_absolute_path_blocked() -> None:
    """TAR archive with an absolute-path member (/etc/passwd) must raise IncorporatorFormatError."""
    from incorporator.io.compression import decompress_data
    from incorporator.io.formats import FormatType

    tar_bytes = _make_tar_with_member("/etc/passwd")
    with pytest.raises(IncorporatorFormatError, match="path traversal blocked|Archive path traversal"):
        decompress_data(tar_bytes, "archive.tar", FormatType.JSON)


def test_tar_safe_member_passes(tmp_path: Path) -> None:
    """TAR archive with a safe, flat member name must extract successfully."""
    import json

    from incorporator.io.compression import decompress_data
    from incorporator.io.formats import FormatType

    safe_json = json.dumps([{"id": 1}]).encode()
    tar_bytes = _make_tar_with_member("data.json", safe_json)

    # Should not raise — returns the JSON string
    result = decompress_data(tar_bytes, "archive.tar", FormatType.JSON)
    assert '"id"' in result
