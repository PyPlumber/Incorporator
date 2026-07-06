"""Unit tests for _normalize_etl_kwargs and NormalizedKwargs."""

import pytest

from incorporator.schema.directives import Ex, Nm, NormalizedKwargs, Pk, _normalize_etl_kwargs


def test_empty_inputs_yield_empty_container() -> None:
    """All-None inputs produce an empty NormalizedKwargs with no directives."""
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=None,
        code_attr=None,
        name_attr=None,
    )
    assert isinstance(result, NormalizedKwargs)
    assert result.ex_tuple == ()
    assert result.conv_map == {}
    assert result.nm_tuple == ()
    assert result.pk_tuple == ()


def test_bare_excl_lst_becomes_ex_tuple() -> None:
    """Bare string excl_lst entries are wrapped into Ex instances."""
    result = _normalize_etl_kwargs(
        excl_lst=["a", "b"],
        conv_dict=None,
        name_chg=None,
        code_attr=None,
        name_attr=None,
    )
    assert result.ex_tuple == (Ex("a"), Ex("b"))


def test_bare_name_chg_becomes_nm_tuple() -> None:
    """Bare (old, new) name_chg entries are wrapped into Nm instances."""
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=[("a", "b")],
        code_attr=None,
        name_attr=None,
    )
    assert result.nm_tuple == (Nm("a", "b"),)


def test_code_attr_and_name_attr_become_pk_tuple() -> None:
    """Bare code_attr and name_attr produce Pk directives with correct targets."""
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=None,
        code_attr="id",
        name_attr="nm",
    )
    assert result.pk_tuple == (Pk("id", target="code"), Pk("nm", target="name"))


def test_mixed_bare_and_wrapped_excl_lst() -> None:
    """excl_lst accepts a mix of bare strings and already-wrapped Ex instances."""
    result = _normalize_etl_kwargs(
        excl_lst=["a", Ex("b")],
        conv_dict=None,
        name_chg=None,
        code_attr=None,
        name_attr=None,
    )
    assert result.ex_tuple == (Ex("a"), Ex("b"))


def test_mixed_bare_and_wrapped_name_chg() -> None:
    """name_chg accepts a mix of bare 2-tuples and already-wrapped Nm instances."""
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=[("x", "y"), Nm("a", "b")],
        code_attr=None,
        name_attr=None,
    )
    assert result.nm_tuple == (Nm("x", "y"), Nm("a", "b"))


def test_idempotency_bare_then_rewrapped() -> None:
    """Re-normalizing an already-normalized input yields an equivalent container."""
    first = _normalize_etl_kwargs(
        excl_lst=["drop_me"],
        conv_dict={"val": lambda x: x},
        name_chg=[("old", "new")],
        code_attr="id",
        name_attr="label",
    )
    # Feed the wrapped output back in as if it had been deserialized.
    second = _normalize_etl_kwargs(
        excl_lst=list(first.ex_tuple),
        conv_dict=first.conv_map if first.conv_map else None,
        name_chg=list(first.nm_tuple),
        code_attr=None,
        name_attr=None,
    )
    assert second.ex_tuple == first.ex_tuple
    assert second.nm_tuple == first.nm_tuple


def test_idempotency_pk_tuple_after_case_a_rewrite() -> None:
    """Re-normalizing replays code_attr/name_attr and reproduces the same Pk rewrite.

    The replay path used by ``refresh()`` reads persisted ``Nm`` tuples back
    in alongside the original (un-rewritten) ``code_attr`` / ``name_attr``
    that live on ``_inc_code_attr`` / ``_inc_name_attr``.  This test pins
    that the rewrite is deterministic across calls: feeding the wrapped
    ``nm_tuple`` back in with the same un-rewritten attrs must produce a
    ``pk_tuple`` identical to the first call's.
    """
    first = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=[("teamid", "tid")],
        code_attr="teamid",
        name_attr="teamname",
    )
    assert first.pk_tuple == (Pk("tid", target="code"), Pk("teamname", target="name"))

    second = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=list(first.nm_tuple),
        code_attr="teamid",
        name_attr="teamname",
    )
    assert second.pk_tuple == first.pk_tuple


def test_user_conv_dict_wins_over_code_attr() -> None:
    """When conv_dict already has inc_code, code_attr is suppressed from pk_tuple."""
    op = lambda x: x  # noqa: E731
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict={"inc_code": op},
        name_chg=None,
        code_attr="id",
        name_attr=None,
    )
    assert all(pk.target != "code" for pk in result.pk_tuple)


def test_user_conv_dict_wins_over_name_attr() -> None:
    """When conv_dict already has inc_name, name_attr is suppressed from pk_tuple."""
    op = lambda x: x  # noqa: E731
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict={"inc_name": op},
        name_chg=None,
        code_attr=None,
        name_attr="label",
    )
    assert all(pk.target != "name" for pk in result.pk_tuple)


def test_pk_source_rewrite_case_a() -> None:
    """Case A: code_attr names a field that name_chg renames — Pk.source follows the rename."""
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=[("teamid", "tid")],
        code_attr="teamid",
        name_attr=None,
    )
    assert result.pk_tuple == (Pk("tid", target="code"),)


def test_pk_source_no_rewrite_case_b() -> None:
    """Case B: code_attr does not appear in the rename map — Pk.source is unchanged."""
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=[("user_id", "id")],
        code_attr="id",
        name_attr=None,
    )
    assert result.pk_tuple == (Pk("id", target="code"),)


def test_pk_source_rewrite_first_hit_only() -> None:
    """First-hit rule: chained renames a→b→c only rewrite code_attr='a' to 'b', not 'c'.

    D2-03 pin (distinct-key case): 'a' and 'b' are distinct old keys, so the
    rename_map construction (dict comprehension pre-fix, first-hit scan
    post-fix) agrees either way — this test passes both before and after
    the fix. It pins the docstring's no-chained-rewrites contract.
    """
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=[("a", "b"), ("b", "c")],
        code_attr="a",
        name_attr=None,
    )
    # 'a' maps to 'b' in the rename_map; 'b' maps to 'c' but that's a separate
    # entry — first-hit stops at 'b'.
    assert result.pk_tuple == (Pk("b", target="code"),)


def test_pk_source_rewrite_duplicate_old_key_first_hit() -> None:
    """D2-03: duplicate old key in name_chg — Pk.source rewrites to the FIRST match.

    name_chg=[("a", "b"), ("a", "c")] has the SAME old key 'a' twice. Pre-fix,
    the dict comprehension `{nm.old: nm.new for nm in nm_tuple}` resolves
    last-hit, binding Pk('a') to 'c' — disagreeing with the runtime
    apply_rename pass, which applies renames sequentially: 'a' moves to 'b'
    on the first Nm, so the second Nm('a', 'c') is a no-op (there is no 'a'
    left to rename). Post-fix, the first-hit scan binds Pk('a') to 'b',
    matching both the docstring and the runtime rename pass.
    """
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=[("a", "b"), ("a", "c")],
        code_attr="a",
        name_attr=None,
    )
    assert result.pk_tuple == (Pk("b", target="code"),)


def test_conv_map_passthrough() -> None:
    """conv_dict is passed through unchanged into conv_map."""
    op = lambda x: int(x)  # noqa: E731
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict={"price": op},
        name_chg=None,
        code_attr=None,
        name_attr=None,
    )
    assert result.conv_map is not None
    assert result.conv_map["price"] is op


def test_empty_excl_lst_yields_empty_ex_tuple() -> None:
    """An empty list for excl_lst yields an empty ex_tuple (not a tuple with one empty Ex)."""
    result = _normalize_etl_kwargs(
        excl_lst=[],
        conv_dict=None,
        name_chg=None,
        code_attr=None,
        name_attr=None,
    )
    assert result.ex_tuple == ()


def test_normalized_kwargs_is_frozen() -> None:
    """NormalizedKwargs instances are frozen (attribute assignment raises FrozenInstanceError)."""
    result = _normalize_etl_kwargs(
        excl_lst=None,
        conv_dict=None,
        name_chg=None,
        code_attr=None,
        name_attr=None,
    )
    with pytest.raises(Exception):
        result.ex_tuple = (Ex("x"),)  # type: ignore[misc]
