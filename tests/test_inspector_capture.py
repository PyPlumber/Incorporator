"""Tests for ``incorporator.tools.inspector.capture_signals()``.

The pure-detection counterpart to ``analyze_data()`` — same fixtures, but
asserting on the returned :class:`SourceProfile` instead of captured stdout.

Print-output preservation is covered indirectly by the existing test
suite (the 712 tests that already exercise ``cls.test()`` continue to
pass after the refactor).  This file pins the structured signal bundle
that the cross-source orchestration analyzer in
``incorporator.observability.tideweaver.architect`` consumes.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from incorporator.tools.inspector import (
    ResponseMeta,
    SourceProfile,
    capture_signals,
)


def test_capture_signals_empty_data_returns_default_profile() -> None:
    """No data → profile with empty/default fields, no detection ran."""
    profile = capture_signals([], {})
    assert isinstance(profile, SourceProfile)
    assert profile.parsed_data == []
    assert profile.sample is None
    assert profile.target_obj is None
    assert profile.is_dict_shaped is False
    assert profile.primary_key_field is None
    assert profile.display_name_field is None
    assert profile.datetime_fields == []
    assert profile.pagination_kind is None
    assert profile.heavy_fields == []
    assert profile.top_level_fields == set()


def test_capture_signals_records_top_level_fields_for_overlap() -> None:
    """``top_level_fields`` is the key-name set of the record-shaped object.

    The cross-source orchestration analyzer in architect uses this set for
    pairwise-overlap analysis when picking a Watershed shape.
    """
    profile = capture_signals([{"user_id": 1, "name": "Ada", "email": "a@x"}], {})
    assert profile.top_level_fields == {"user_id", "name", "email"}


def test_capture_signals_detects_uuid_primary_key() -> None:
    """UUID-shaped ``id`` field scores high enough to win inc_code."""
    sample = {"id": "550e8400-e29b-41d4-a716-446655440000", "name": "Test"}
    profile = capture_signals([sample], {})
    assert profile.primary_key_field == "id"
    assert profile.primary_key_score >= 50  # 50 (id name) + 40 (UUID)


def test_capture_signals_detects_underscore_id_field() -> None:
    """``*_id`` field name pattern wins inc_code when no UUID/id is present."""
    sample = {"user_id": 42, "title": "Hello World"}
    profile = capture_signals([sample], {})
    assert profile.primary_key_field == "user_id"


def test_capture_signals_detects_display_name() -> None:
    """``name`` / ``title`` field scores high for inc_name."""
    sample = {"id": 1, "title": "Hello World", "summary": "x"}
    profile = capture_signals([sample], {})
    assert profile.display_name_field == "title"


def test_capture_signals_finds_datetime_fields() -> None:
    """ISO-8601 timestamp strings land in ``datetime_fields``."""
    sample = {
        "id": 1,
        "created_at": "2026-05-21T10:30:00+00:00",
        "updated_at": "2026-05-22T11:00:00+00:00",
        "label": "Test",
    }
    profile = capture_signals([sample], {})
    assert "created_at" in profile.datetime_fields
    assert "updated_at" in profile.datetime_fields


def test_capture_signals_finds_int_fields_with_numeric_key_hint() -> None:
    """Numeric strings with quantity-shaped key names land in ``int_fields``."""
    sample = {"id": 1, "view_count": "1234", "total_amount": "5678"}
    profile = capture_signals([sample], {})
    # Both have numeric-key hints (count, total/amount) and parse as int.
    assert "view_count" in profile.int_fields
    assert "total_amount" in profile.int_fields


def test_capture_signals_decimal_numeric_keys_land_in_int_fields() -> None:
    """Decimal strings with quantity-shaped key names land in ``int_fields``.

    Two existing inspector quirks preserved by the capture-mode refactor:

    1. ``parses_as_int`` is permissive — it truncates ``"19.99"`` to ``19``
       and returns True, so the int branch wins over the float branch.
    2. Leading-zero strings (``"0.05"``) hit the identifier-shape skip
       and don't land in either bucket.  Use a leading-digit decimal
       (``"4.5"``) to bypass that filter.
    """
    sample = {"id": 1, "price": "19.99", "rate": "4.5"}
    profile = capture_signals([sample], {})
    assert "price" in profile.int_fields
    assert "rate" in profile.int_fields


def test_capture_signals_inf_lands_in_float_fields() -> None:
    """Strings the int parser rejects but float accepts fall through to float_fields."""
    sample = {"id": 1, "rate": "inf"}
    profile = capture_signals([sample], {})
    assert "rate" in profile.float_fields


def test_capture_signals_pagination_cursor() -> None:
    """``cursor`` key on the top-level dict → pagination_kind='cursor'."""
    sample = {"items": [{"id": 1}], "cursor": "abc123"}
    profile = capture_signals([sample], {})
    assert profile.pagination_kind == "cursor"
    assert profile.pagination_suggestion == "CursorPaginator(cursor_param='cursor')"
    assert profile.pagination_description is not None
    assert "cursor" in profile.pagination_description


def test_capture_signals_pagination_next_url() -> None:
    """``next`` URL string → pagination_kind='next_url'."""
    sample = {"items": [{"id": 1}], "next": "https://api.example.com/v1/items?page=2"}
    profile = capture_signals([sample], {})
    assert profile.pagination_kind == "next_url"
    assert "NextUrlPaginator" in (profile.pagination_suggestion or "")


def test_capture_signals_pagination_offset() -> None:
    """``offset`` + ``limit`` pair → pagination_kind='offset'."""
    sample = {"items": [{"id": 1}], "offset": 0, "limit": 100}
    profile = capture_signals([sample], {})
    assert profile.pagination_kind == "offset"
    assert "OffsetPaginator" in (profile.pagination_suggestion or "")


def test_capture_signals_pagination_page() -> None:
    """``page`` + ``per_page`` pair → pagination_kind='page'."""
    sample = {"items": [{"id": 1}], "page": 1, "per_page": 25}
    profile = capture_signals([sample], {})
    assert profile.pagination_kind == "page"
    assert "PageNumberPaginator" in (profile.pagination_suggestion or "")


def test_capture_signals_pagination_ambiguous_when_only_metadata() -> None:
    """Bare ``has_more`` / ``total_pages`` without a cursor → 'ambiguous'."""
    sample = {"items": [{"id": 1}], "has_more": True, "total": 100}
    profile = capture_signals([sample], {})
    assert profile.pagination_kind == "ambiguous"
    assert "has_more" in profile.pagination_meta_keys_present
    assert "total" in profile.pagination_meta_keys_present


def test_capture_signals_no_pagination_signal() -> None:
    """A flat dict with no pagination keys → pagination_kind=None."""
    sample = {"id": 1, "name": "Test"}
    profile = capture_signals([sample], {})
    assert profile.pagination_kind is None
    assert profile.pagination_suggestion is None


def test_capture_signals_heavy_data_image() -> None:
    """``data:image/...`` base64 blob → heavy_fields entry."""
    sample = {"id": 1, "avatar": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAA..."}
    profile = capture_signals([sample], {})
    assert "avatar" in profile.heavy_fields


def test_capture_signals_heavy_asset_url() -> None:
    """Asset-CDN URL → heavy_fields entry."""
    sample = {"id": 1, "image_url": "https://cdn.example.com/x.jpg"}
    profile = capture_signals([sample], {})
    assert "image_url" in profile.heavy_fields


def test_capture_signals_heavy_oversized_string() -> None:
    """String > _HEAVY_FIELD_BYTES (2048) → heavy_fields entry."""
    big = "x" * 3000
    sample = {"id": 1, "blob": big}
    profile = capture_signals([sample], {})
    assert "blob" in profile.heavy_fields


def test_capture_signals_rec_path_candidates_sorted_largest_first() -> None:
    """Nested arrays in a dict sample → rec_path_candidates ranked by size desc."""
    sample = {
        "meta": "x",
        "small_list": [{"a": 1}],
        "big_list": [{"a": 1}, {"a": 2}, {"a": 3}, {"a": 4}, {"a": 5}],
    }
    profile = capture_signals([sample], {})
    # Two nested arrays of records, big_list first.
    assert [name for name, _n in profile.rec_path_candidates] == ["big_list", "small_list"]
    assert profile.rec_path_candidates[0] == ("big_list", 5)


def test_capture_signals_rec_path_skipped_when_user_provided_one() -> None:
    """When the user already supplied rec_path=, don't suggest more."""
    sample = {"items": [{"a": 1}, {"a": 2}], "other": [{"b": 3}]}
    profile = capture_signals([sample], {"rec_path": "items"})
    assert profile.rec_path_candidates == []


def test_capture_signals_list_sample_treats_first_row_as_record() -> None:
    """A list-of-records sample uses sample[0] as the target object."""
    parsed: List[Any] = [[{"id": 1, "name": "Ada"}, {"id": 2, "name": "Bob"}]]
    profile = capture_signals(parsed, {})
    assert profile.is_dict_shaped is True
    assert profile.target_obj == {"id": 1, "name": "Ada"}
    assert profile.primary_key_field == "id"


def test_capture_signals_non_dict_target_flags_not_dict_shaped() -> None:
    """A scalar or string sample → is_dict_shaped=False, no detection ran."""
    profile = capture_signals(["just a string"], {})
    assert profile.is_dict_shaped is False
    assert profile.primary_key_field is None
    assert profile.datetime_fields == []


def test_capture_signals_passes_response_meta_through() -> None:
    """response_meta is stored verbatim — architect reads it for Penstock decisions."""
    meta = ResponseMeta(host="api.example.com", status_code=200, response_time_ms=120.5)
    profile = capture_signals([{"id": 1}], {}, response_meta=meta)
    assert profile.response_meta is meta
    assert profile.response_meta.host == "api.example.com"


def test_capture_signals_response_meta_defaults_to_none() -> None:
    """File-mode probes and legacy callers leave response_meta unset."""
    profile = capture_signals([{"id": 1}], {})
    assert profile.response_meta is None


def test_response_meta_dataclass_defaults() -> None:
    """ResponseMeta is constructible with zero kwargs (all fields default)."""
    meta = ResponseMeta()
    assert meta.host is None
    assert meta.status_code is None
    assert meta.rate_limited is False
    assert meta.retry_after_sec is None
    assert meta.response_time_ms is None
    assert meta.content_type is None


def test_source_profile_dataclass_default_factories_independent() -> None:
    """Mutable defaults must not share state across instances."""
    p1 = SourceProfile(parsed_data=[], provided_kwargs={})
    p2 = SourceProfile(parsed_data=[], provided_kwargs={})
    p1.heavy_fields.append("x")
    p1.datetime_fields.append("y")
    p1.top_level_fields.add("z")
    assert p2.heavy_fields == []
    assert p2.datetime_fields == []
    assert p2.top_level_fields == set()


def test_capture_signals_does_not_mutate_provided_kwargs() -> None:
    """provided_kwargs is copied — architect's source-loop must not be polluted."""
    kwargs: Dict[str, Any] = {"rec_path": "data"}
    capture_signals([{"id": 1}], kwargs)
    assert kwargs == {"rec_path": "data"}  # unchanged
