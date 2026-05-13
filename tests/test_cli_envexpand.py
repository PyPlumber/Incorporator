"""Unit tests for incorporator.cli.envexpand.expand_env."""

from pathlib import Path

import pytest

from incorporator.cli.envexpand import EnvExpansionError, expand_env


def test_expand_env_simple(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_API_KEY", "abc123")
    out = expand_env({"headers": {"Authorization": "Bearer ${TEST_API_KEY}"}})
    assert out == {"headers": {"Authorization": "Bearer abc123"}}


def test_expand_env_default_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)
    out = expand_env({"value": "${MISSING_VAR:-fallback_value}"})
    assert out == {"value": "fallback_value"}


def test_expand_env_default_skipped_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRESENT_VAR", "real_value")
    out = expand_env({"value": "${PRESENT_VAR:-fallback}"})
    assert out == {"value": "real_value"}


def test_expand_env_required_raises_with_clear_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REQUIRED_THING", raising=False)
    with pytest.raises(EnvExpansionError, match="REQUIRED_THING"):
        expand_env({"x": "${REQUIRED_THING}"})


def test_expand_env_error_op_uses_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OOPS", raising=False)
    with pytest.raises(EnvExpansionError, match="Set OOPS in your environment"):
        expand_env({"x": "${OOPS:?Set OOPS in your environment}"})


def test_expand_env_double_dollar_escape(monkeypatch: pytest.MonkeyPatch) -> None:
    """$${VAR} is preserved literally as ${VAR} — the env lookup is skipped."""
    monkeypatch.setenv("VAR", "should_not_appear")
    out = expand_env({"x": "literal: $${VAR}"})
    assert out == {"x": "literal: ${VAR}"}


def test_expand_env_file_reference(tmp_path: Path) -> None:
    """${file:/path} reads the file and substitutes its contents (rstripped)."""
    secret = tmp_path / "api_key"
    secret.write_text("very-secret-token\n", encoding="utf-8")
    out = expand_env({"headers": {"Authorization": f"Bearer ${{file:{secret}}}"}})
    assert out == {"headers": {"Authorization": "Bearer very-secret-token"}}


def test_expand_env_file_missing_raises(tmp_path: Path) -> None:
    missing = tmp_path / "ghost"
    with pytest.raises(EnvExpansionError, match="not found"):
        expand_env({"x": f"${{file:{missing}}}"})


def test_expand_env_walks_lists_and_nested_dicts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("A", "alpha")
    monkeypatch.setenv("B", "beta")
    obj = {"top": [{"k": "${A}"}, "${B}", 42, True, None]}
    out = expand_env(obj)
    assert out == {"top": [{"k": "alpha"}, "beta", 42, True, None]}


def test_expand_env_preserves_non_string_leaves() -> None:
    """Numbers, bools, None pass through. Walker must not crash on them."""
    obj = {"int": 42, "float": 1.5, "bool": True, "none": None, "list": [1, 2, 3]}
    assert expand_env(obj) == obj
