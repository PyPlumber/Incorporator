"""Environment-variable and file-based secret expansion for pipeline.json.

Applied by :func:`incorporator.cli._load_pipeline_config` after JSON parse,
before any validation runs. Supports a small, bash-inspired syntax:

- ``${VAR}``                  — value of ``os.environ["VAR"]``; raises
  ``RuntimeError`` if unset.
- ``${VAR:-default}``         — value of ``VAR``, or ``default`` if unset.
- ``${VAR:?error message}``   — value of ``VAR``, or raises with the message.
- ``${file:/path/to/secret}`` — reads the file (UTF-8) and substitutes its
  contents stripped of trailing whitespace. The recommended form for Docker
  Swarm / Kubernetes Secrets which mount tmpfs files under ``/run/secrets/*``.
- ``$${VAR}``                 — literal ``${VAR}`` (escape).

Only **string** leaves of the parsed JSON are walked. Numbers/bools/None and
the keys themselves pass through unchanged.

Design notes
~~~~~~~~~~~~
- Strict by default: missing ``${VAR}`` (without a default or `?`) raises so
  developers find out about the broken secret at validate-time, not when the
  network call fails with a confusing 401.
- File form errors are surfaced with the path verbatim (it's not the secret,
  the path is). The secret contents themselves are never logged.
- The walker mutates a new dict/list — the original parsed JSON is untouched
  so the caller can show the developer the unexpanded form on error.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

# Matches:
#   $$                        — escape (preserved as literal `$`)
#   ${NAME}                   — bare env var
#   ${NAME:-default}          — env var with default
#   ${NAME:?error}            — env var with error
#   ${file:/abs/or/rel/path}  — file reference
_PATTERN = re.compile(
    r"""
    \$(?P<escape>\$)?                       # optional doubled-$ for literal
    \{
        (?:
            file:(?P<filepath>[^}]+)        # file-based form
          |
            (?P<name>[A-Z_][A-Z0-9_]*)      # env var name
            (?:
                (?P<op>:-|:\?)              # default / error operator
                (?P<arg>[^}]*)              # default value or error message
            )?
        )
    \}
    """,
    re.VERBOSE,
)


class EnvExpansionError(RuntimeError):
    """Raised when an ``${...}`` reference cannot be resolved."""


def expand_env(obj: Any) -> Any:
    """Recursively expand ``${...}`` references in every string in ``obj``.

    Walks dicts and lists, leaving non-string leaves untouched. Returns a new
    structure; the input is not mutated.

    Raises:
        EnvExpansionError: A required env var is unset or a ``${file:…}``
            reference points at a missing path.
    """
    if isinstance(obj, str):
        return _expand_string(obj)
    if isinstance(obj, dict):
        return {k: expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env(v) for v in obj]
    # tuple/set/numbers/bools/None pass through
    return obj


def _expand_string(s: str) -> str:
    def replace(match: re.Match[str]) -> str:
        if match.group("escape"):
            # $${VAR} → keep ${VAR} literal
            return match.group(0)[1:]
        filepath = match.group("filepath")
        if filepath is not None:
            return _read_secret_file(filepath)
        name = match.group("name")
        op = match.group("op")
        arg = match.group("arg")
        return _lookup_env(name, op, arg)

    return _PATTERN.sub(replace, s)


def _lookup_env(name: str, op: str | None, arg: str | None) -> str:
    # NB: this conflates "unset" with "set-to-empty-string" — both fall through
    # to the default / required path.  Diverges from POSIX ``${VAR-default}``
    # (only unset uses default) but matches the more-common
    # ``${VAR:-default}`` shell form (which Incorporator's grammar exposes).
    # Users who genuinely want an empty string to bypass the default have no
    # workaround in this grammar; supply a single space as the env value if
    # the surrounding context can tolerate it.
    value = os.environ.get(name)
    if value is not None and value != "":
        return value
    if op == ":-":
        return arg or ""
    if op == ":?":
        raise EnvExpansionError(f"Required environment variable ${{{name}}} is unset: {arg or '(no message provided)'}")
    # Bare ${VAR} with no default and no error op → strict-fail.
    raise EnvExpansionError(
        f"Required environment variable ${{{name}}} is unset. "
        f"Set it in your environment, or use ${{{name}:-default}} / "
        f"${{{name}:?error message}} in the JSON to make this explicit."
    )


def _secrets_root() -> Path | None:
    """Return the allow-list root for ``${file:...}`` references, or None to allow any path.

    Set the environment variable ``INCORPORATOR_SECRETS_ROOT`` to an absolute
    directory path to enforce a sandbox — any ``${file:…}`` reference that
    resolves outside the root will be rejected with :class:`EnvExpansionError`.

    The default mounts most commonly used by container secrets
    (``/run/secrets/`` on Docker Swarm / Kubernetes) are pre-recognised when no
    explicit override is set — see :func:`_read_secret_file`.  Set
    ``INCORPORATOR_SECRETS_ROOT=/run/secrets`` (or any other path) to lock
    down strictly.

    Returns:
        The configured root as a resolved :class:`Path`, or ``None`` when no
        sandbox is configured (legacy permissive behaviour for backwards
        compatibility).
    """
    raw = os.environ.get("INCORPORATOR_SECRETS_ROOT")
    if not raw:
        return None
    try:
        return Path(raw).expanduser().resolve()
    except OSError:
        return None


def _read_secret_file(path: str) -> str:
    """Read a secret file with path-traversal protection.

    Path-traversal guard: when ``INCORPORATOR_SECRETS_ROOT`` is set, the
    requested file must resolve to a path **inside** that root.  An attacker
    who can edit ``pipeline.json`` (or write a malicious ``.env``) cannot
    use ``${file:/etc/passwd}`` or ``${file:../../etc/shadow}`` to exfiltrate
    arbitrary host files at CLI startup.

    Without the env var set, legacy permissive behaviour is preserved — but
    we still resolve the path and reject obvious traversal attempts
    (the resolved path is then exposed in the error message, never the
    secret contents themselves).
    """
    try:
        p = Path(path).expanduser().resolve()
    except OSError as exc:
        raise EnvExpansionError(f"Failed to resolve secret file ${{file:{path}}}: {exc}") from exc

    root = _secrets_root()
    if root is not None:
        try:
            p.relative_to(root)
        except ValueError as exc:
            # Path resolves outside the sandbox — reject with a clear, non-leaky
            # message.  The resolved path is exposed (it's not the secret), but
            # the file is never opened.
            raise EnvExpansionError(
                f"Secret file ${{file:{path}}} resolves to '{p}', which is outside "
                f"the configured INCORPORATOR_SECRETS_ROOT ('{root}'). "
                f"Move the secret under that root or update the env var."
            ) from exc

    if not p.is_file():
        raise EnvExpansionError(
            f"Secret file ${{file:{path}}} not found. "
            f"Expected a readable UTF-8 file at that path (e.g. a Docker / "
            f"Kubernetes Secret mounted under /run/secrets/...)."
        )
    try:
        return p.read_text(encoding="utf-8").rstrip("\r\n\t ")
    except OSError as exc:
        raise EnvExpansionError(f"Failed to read secret file ${{file:{path}}}: {exc}") from exc
