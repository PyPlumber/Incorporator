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


# ---------------------------------------------------------------------------
# Senior-review CC1: INCORPORATOR_SECRETS_ROOT sandbox.
#
# The sandbox is the framework's only path-traversal protection for
# ``${file:...}`` references.  Without these regression tests, a future
# refactor could silently break the protection and CI would pass.
# ---------------------------------------------------------------------------


def test_secrets_root_rejects_path_outside_sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Sandbox active + ``${file:...}`` points OUTSIDE root → ``EnvExpansionError``.

    The check happens BEFORE any file open, so a hostile config that uses
    ``${file:/etc/passwd}`` to exfiltrate a sensitive host file at startup
    is rejected with a clear message rather than silently succeeding.
    """
    sandbox = tmp_path / "secrets"
    sandbox.mkdir()
    outside = tmp_path / "elsewhere" / "exfil.txt"
    outside.parent.mkdir()
    outside.write_text("would-be-stolen", encoding="utf-8")

    monkeypatch.setenv("INCORPORATOR_SECRETS_ROOT", str(sandbox))

    with pytest.raises(EnvExpansionError, match="outside the configured INCORPORATOR_SECRETS_ROOT"):
        expand_env({"x": f"${{file:{outside}}}"})


def test_secrets_root_accepts_path_inside_sandbox(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Sandbox active + ``${file:...}`` points INSIDE root → resolves normally."""
    sandbox = tmp_path / "secrets"
    sandbox.mkdir()
    inside = sandbox / "api_key"
    inside.write_text("legitimate-token\n", encoding="utf-8")

    monkeypatch.setenv("INCORPORATOR_SECRETS_ROOT", str(sandbox))

    out = expand_env({"headers": {"Authorization": f"Bearer ${{file:{inside}}}"}})
    assert out == {"headers": {"Authorization": "Bearer legitimate-token"}}


def test_secrets_root_unset_falls_back_to_permissive(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Sandbox env-var unset → no path-traversal check; any readable file works.

    Preserves the legacy permissive behaviour for users who haven't opted in
    to the sandbox.  The file still has to exist and be readable; the only
    relaxation is that it doesn't have to resolve under any specific root.
    """
    monkeypatch.delenv("INCORPORATOR_SECRETS_ROOT", raising=False)

    secret = tmp_path / "loose_secret"
    secret.write_text("permissive-token\n", encoding="utf-8")

    out = expand_env({"x": f"${{file:{secret}}}"})
    assert out == {"x": "permissive-token"}


def test_drain_timeout_resolver_precedence_chain(monkeypatch: pytest.MonkeyPatch) -> None:
    """``incorporator.cli.tideweaver._resolve_drain_timeout`` honours CLI > env > JSON.

    Senior-review pass-2 finding CC2 — the drain-timeout knob has three
    sources of truth and the precedence MUST be (1) explicit ``--drain-timeout``
    CLI flag, (2) ``INCORPORATOR_DRAIN_TIMEOUT`` env-var, (3) ``None`` (fall
    through to watershed.json's value).
    """
    from incorporator.cli.tideweaver import _resolve_drain_timeout

    # 1. CLI override wins over env-var.
    monkeypatch.setenv("INCORPORATOR_DRAIN_TIMEOUT", "45")
    assert _resolve_drain_timeout(60.0) == 60.0

    # 2. Env-var consumed when CLI override is None.
    monkeypatch.setenv("INCORPORATOR_DRAIN_TIMEOUT", "45")
    assert _resolve_drain_timeout(None) == 45.0

    # 3. Both absent → None (caller falls back to watershed.json).
    monkeypatch.delenv("INCORPORATOR_DRAIN_TIMEOUT", raising=False)
    assert _resolve_drain_timeout(None) is None

    # 4. Malformed env-var → None + warning (don't crash the CLI).
    monkeypatch.setenv("INCORPORATOR_DRAIN_TIMEOUT", "not-a-float")
    assert _resolve_drain_timeout(None) is None


def test_secrets_root_rejects_directory_traversal_attempt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``../`` traversal from inside the sandbox to outside is rejected.

    Uses an absolute path that contains traversal segments resolving to a
    location OUTSIDE the sandbox.  Path.resolve() collapses ``..`` segments,
    so the relative_to check correctly detects the escape regardless of
    whether the attacker uses literal absolute paths or traversal tricks.
    """
    sandbox = tmp_path / "secrets"
    sandbox.mkdir()
    sibling = tmp_path / "outside.txt"
    sibling.write_text("not-yours", encoding="utf-8")

    monkeypatch.setenv("INCORPORATOR_SECRETS_ROOT", str(sandbox))

    # Path constructed to look like it's "under" the sandbox but resolves out.
    traversal = sandbox / ".." / "outside.txt"

    with pytest.raises(EnvExpansionError, match="outside the configured INCORPORATOR_SECRETS_ROOT"):
        expand_env({"x": f"${{file:{traversal}}}"})
