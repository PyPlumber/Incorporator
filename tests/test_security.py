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


# ==========================================
# 3. SSRF REDIRECT-HOOK TESTS (async DNS path)
# ==========================================


@pytest.mark.asyncio
async def test_host_is_internal_metadata_host_fast_path() -> None:
    """Known cloud-metadata host must resolve internal without any DNS round-trip."""
    from incorporator.io.fetch import _host_is_internal

    assert await _host_is_internal("169.254.169.254") is True
    assert await _host_is_internal("metadata.google.internal") is True


@pytest.mark.asyncio
async def test_host_is_internal_external_ip_literal() -> None:
    """Public IP literal must resolve external without any DNS round-trip."""
    from incorporator.io.fetch import _host_is_internal

    assert await _host_is_internal("8.8.8.8") is False


@pytest.mark.asyncio
async def test_host_is_internal_uses_async_dns(monkeypatch: pytest.MonkeyPatch) -> None:
    """DNS resolution must go through ``loop.getaddrinfo``, never sync ``socket.getaddrinfo``.

    Regression test for the blocking-DNS bug: previously ``_host_is_internal``
    called ``socket.getaddrinfo`` synchronously inside an async hook, stalling
    the event loop on every redirect.  This test asserts the async path is
    used by patching it; the sync path is patched to raise so a regression
    would fail loudly.
    """
    import asyncio

    from incorporator.io import fetch

    # Sync socket.getaddrinfo must not be called — regression guard.
    def _fail_sync(*args: object, **kwargs: object) -> object:
        raise AssertionError("synchronous socket.getaddrinfo called from async path")

    monkeypatch.setattr("socket.getaddrinfo", _fail_sync)

    # Async path returns a fake addrinfo pointing at an internal IP.
    fake_internal = [(0, 0, 0, "", ("10.0.0.1", 0))]

    async def _fake_async_getaddrinfo(host: str, port: object, **kw: object) -> object:
        return fake_internal

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", _fake_async_getaddrinfo)

    assert await fetch._host_is_internal("my-internal.local") is True


# ==========================================
# 4. TOKEN ALLOW-LIST BOUNDARY TESTS (Ex / Nm / Pk)
# ==========================================


def test_token_rejects_ex_class_access() -> None:
    """Attribute-access forms on Ex are rejected; the new allow-list entry does not
    expand the attack surface beyond direct construction."""
    from incorporator.cli.tokens import TokenResolutionError, resolve_tokens

    with pytest.raises(TokenResolutionError, match="unsupported call form"):
        resolve_tokens({"x": "Ex('a').__init__('b')"})


def test_token_rejects_nm_attribute_access() -> None:
    """Dotted-name call on Nm is rejected by the AST walker before any code runs."""
    from incorporator.cli.tokens import TokenResolutionError, resolve_tokens

    with pytest.raises(TokenResolutionError, match="unsupported call form"):
        resolve_tokens({"x": "Nm.__class__('c', 'd')"})


def test_token_rejects_pk_attribute_access() -> None:
    """Dotted-name call on Pk is rejected by the AST walker before any code runs."""
    from incorporator.cli.tokens import TokenResolutionError, resolve_tokens

    with pytest.raises(TokenResolutionError, match="unsupported call form"):
        resolve_tokens({"x": "Pk.__class__('id', 'code')"})


@pytest.mark.asyncio
async def test_redirect_hook_blocks_internal_target(monkeypatch: pytest.MonkeyPatch) -> None:
    """End-to-end: the response hook must raise on a 302 → internal-host redirect."""
    import asyncio

    import httpx

    from incorporator.exceptions import IncorporatorNetworkError
    from incorporator.io import fetch

    fake_internal = [(0, 0, 0, "", ("127.0.0.1", 0))]

    async def _fake_async_getaddrinfo(host: str, port: object, **kw: object) -> object:
        return fake_internal

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", _fake_async_getaddrinfo)

    # Synthesize a 302 response whose Location points at an internal-resolving hostname.
    request = httpx.Request("GET", "https://example.com/")
    response = httpx.Response(
        status_code=302,
        headers={"Location": "https://my-internal.local/admin"},
        request=request,
    )

    with pytest.raises(IncorporatorNetworkError, match="blocked redirect to internal host"):
        await fetch._block_internal_redirect_hook(response)
