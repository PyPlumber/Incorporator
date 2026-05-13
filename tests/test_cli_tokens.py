"""Unit tests for ``incorporator.cli.tokens.resolve_tokens``.

The resolver turns JSON-text Python-call expressions (e.g.
``"NextUrlPaginator('next')"``, ``"inc(datetime)"``) into real callables /
instances at config-load time, so users don't need a sidecar file for
the common cases. Tokens needing user-defined functions (``calc``,
``link_to``) resolve when an ``inflow.py`` extends the allow-list via
the ``extra_names=`` parameter; otherwise they raise
:class:`TokenResolutionError` here with a clear allow-list message.
"""

from datetime import datetime
from typing import Any

import pytest

from incorporator.cli.tokens import TokenResolutionError, resolve_tokens
from incorporator.io.pagination import (
    AvroPaginator,
    NextUrlPaginator,
    OffsetPaginator,
    PageNumberPaginator,
)

# ---------- happy path: shape match + allow-listed names ----------


def test_resolve_next_url_paginator() -> None:
    out = resolve_tokens({"inc_page": 'NextUrlPaginator("next")'})
    assert isinstance(out["inc_page"], NextUrlPaginator)


def test_resolve_offset_paginator_with_int_arg() -> None:
    out = resolve_tokens({"inc_page": 'OffsetPaginator(100, "offset", "limit")'})
    assert isinstance(out["inc_page"], OffsetPaginator)


def test_resolve_page_number_paginator_with_kwargs() -> None:
    out = resolve_tokens({"inc_page": 'PageNumberPaginator(page_param="page", start_page=1)'})
    assert isinstance(out["inc_page"], PageNumberPaginator)


def test_resolve_avro_paginator_with_chunk_size() -> None:
    out = resolve_tokens({"inc_page": 'AvroPaginator("data.avro", 1000)'})
    assert isinstance(out["inc_page"], AvroPaginator)


def test_resolve_inc_datetime() -> None:
    out = resolve_tokens({"conv_dict": {"created_at": "inc(datetime)"}})
    converter = out["conv_dict"]["created_at"]
    assert callable(converter)
    # Round-trip a real ISO-8601 string through the converter.
    result = converter("2026-05-12T14:32:00+00:00")
    assert isinstance(result, datetime)


def test_resolve_inc_int() -> None:
    out = resolve_tokens({"conv_dict": {"age": "inc(int)"}})
    converter = out["conv_dict"]["age"]
    assert converter("42") == 42


def test_resolve_as_list_no_args() -> None:
    out = resolve_tokens({"conv_dict": {"tags": "as_list()"}})
    assert callable(out["conv_dict"]["tags"])


def test_resolve_join_all_with_delimiter() -> None:
    out = resolve_tokens({"form_payload": {"data": 'join_all(";")'}})
    assert callable(out["form_payload"]["data"])


def test_resolve_split_and_get_positional_args() -> None:
    out = resolve_tokens({"conv_dict": {"first": 'split_and_get(",", 0)'}})
    assert callable(out["conv_dict"]["first"])


def test_resolve_nested_inside_lists() -> None:
    out = resolve_tokens(
        {
            "stream_params": [
                {"incorp_params": {"inc_page": 'NextUrlPaginator("next")'}},
                {"incorp_params": {"inc_page": 'PageNumberPaginator(page_param="p")'}},
            ]
        }
    )
    assert isinstance(out["stream_params"][0]["incorp_params"]["inc_page"], NextUrlPaginator)
    assert isinstance(out["stream_params"][1]["incorp_params"]["inc_page"], PageNumberPaginator)


# ---------- pass-through: non-token strings stay strings ----------


@pytest.mark.parametrize(
    "value",
    [
        "https://api.example.com/v1/users",  # URL: no top-level parens
        "Bearer abc123def456",  # auth header text
        "/path/to/file.json",  # file path
        "this is a plain sentence with no parens",  # English prose
        "results",  # bare word (rec_path value)
        "",  # empty string
        "POST",  # HTTP method
    ],
)
def test_non_token_strings_unchanged(value: str) -> None:
    out = resolve_tokens({"x": value})
    assert out["x"] == value


def test_lists_of_plain_strings_unchanged() -> None:
    out = resolve_tokens({"excl_lst": ["image", "moves", "game_indices"]})
    assert out["excl_lst"] == ["image", "moves", "game_indices"]


def test_nested_plain_dict_unchanged() -> None:
    headers = {"Authorization": "Bearer x", "Accept": "application/json"}
    out = resolve_tokens({"headers": headers})
    assert out["headers"] == headers


# ---------- error path: shape match + allow-list miss → loud error ----------


def test_unknown_identifier_raises() -> None:
    with pytest.raises(TokenResolutionError, match="EvilPaginator"):
        resolve_tokens({"x": "EvilPaginator()"})


def test_attribute_access_call_raises() -> None:
    """``datetime.datetime(2024, 1, 1)`` matches the shape regex but the AST
    walker rejects the attribute-access call form."""
    with pytest.raises(TokenResolutionError, match="unsupported call form"):
        resolve_tokens({"x": "datetime.datetime(2024, 1, 1)"})


def test_import_attempt_raises() -> None:
    with pytest.raises(TokenResolutionError, match="__import__"):
        resolve_tokens({"x": 'inc(__import__("os"))'})


def test_lambda_rejected() -> None:
    with pytest.raises(TokenResolutionError, match="unsupported"):
        resolve_tokens({"x": "inc(lambda x: x)"})


def test_subscript_passes_through_as_string() -> None:
    # ``as_list()[0]`` ends with ``]`` not ``)`` so the shape regex doesn't
    # match — the string passes through unchanged.  Safe: no code runs.
    # Downstream the engine sees a string in a callable slot and fails with
    # its usual type error, which is acceptable for this rare misuse.
    out = resolve_tokens({"x": "as_list()[0]"})
    assert out["x"] == "as_list()[0]"


# ---------- env-expand + token-resolve interaction (no conflict) ----------


def test_url_with_query_params_stays_string() -> None:
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&per_page=100"
    out = resolve_tokens({"inc_url": url})
    assert out["inc_url"] == url


# ---------- non-dict/list scalars pass through ----------


@pytest.mark.parametrize("value", [None, True, False, 42, 3.14, 0, ""])
def test_scalar_passthrough(value: Any) -> None:
    out = resolve_tokens({"x": value})
    assert out["x"] == value


# ---------- extra_names extension (the inflow hook) ----------


def test_extra_names_resolves_user_function() -> None:
    """Pass a user reducer via extra_names; calc(user_fn, 'field') should resolve."""
    from incorporator.schema.converters import CalcOp

    def my_reducer(values: Any) -> int:
        return sum(values) if isinstance(values, list) else 0

    out = resolve_tokens(
        {"x": "calc(my_reducer, 'field')"},
        extra_names={"my_reducer": my_reducer},
    )
    assert isinstance(out["x"], CalcOp)


def test_framework_name_wins_over_user_shadow() -> None:
    """A user trying to shadow ``inc`` doesn't break framework semantics."""
    sneaky = lambda *_: "hijacked"  # noqa: E731
    out = resolve_tokens(
        {"x": "inc(int)"},
        extra_names={"inc": sneaky},
    )
    # Framework `inc` still wins — out["x"] is the real converter callable.
    assert callable(out["x"])
    assert out["x"]("42") == 42


# ---------- @name sigil ----------


def test_at_sigil_resolves_inflow_reference() -> None:
    from incorporator.io.pagination import NextUrlPaginator

    my_pager = NextUrlPaginator("next")
    out = resolve_tokens({"inc_page": "@my_pager"}, extra_names={"my_pager": my_pager})
    assert out["inc_page"] is my_pager


def test_at_sigil_unknown_name_raises_with_user_hint() -> None:
    with pytest.raises(TokenResolutionError, match="unknown name"):
        resolve_tokens({"x": "@nope"}, extra_names={"my_pager": object()})


def test_at_sigil_rejects_dotted_or_call() -> None:
    """@foo.bar and @foo() don't match the strict sigil grammar — pass through."""
    out = resolve_tokens({"x": "@foo.bar", "y": "@foo()"})
    assert out["x"] == "@foo.bar"
    assert out["y"] == "@foo()"


def test_at_sigil_empty_pass_through() -> None:
    """Bare ``@`` is not a valid sigil; stays a literal string."""
    out = resolve_tokens({"x": "@"})
    assert out["x"] == "@"


def test_single_quote_inside_call_token() -> None:
    """JSON-friendly form: NextUrlPaginator('next') — no escape needed."""
    from incorporator.io.pagination import NextUrlPaginator

    out = resolve_tokens({"inc_page": "NextUrlPaginator('next')"})
    assert isinstance(out["inc_page"], NextUrlPaginator)
