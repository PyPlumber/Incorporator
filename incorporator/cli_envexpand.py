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
    value = os.environ.get(name)
    if value is not None and value != "":
        return value
    if op == ":-":
        return arg or ""
    if op == ":?":
        raise EnvExpansionError(
            f"Required environment variable ${{{name}}} is unset: " f"{arg or '(no message provided)'}"
        )
    # Bare ${VAR} with no default and no error op → strict-fail.
    raise EnvExpansionError(
        f"Required environment variable ${{{name}}} is unset. "
        f"Set it in your environment, or use ${{{name}:-default}} / "
        f"${{{name}:?error message}} in the JSON to make this explicit."
    )


def _read_secret_file(path: str) -> str:
    p = Path(path)
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
