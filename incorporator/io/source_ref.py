"""SourceRef — tagged value type for ``incorp()`` source kwargs.

Unifies URL / file / Incorporator-parent / payload-list / kwargs-dict
source forms into one shape.  Public verbs keep their existing kwarg
signatures; :class:`SourceRef` is an opt-in type for callers that want
explicit source typing.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Union

SourceKind = Literal["url", "file", "parent", "payload", "kwargs"]


@dataclass(frozen=True)
class SourceRef:
    """A typed reference to one source for :meth:`Incorporator.incorp`.

    Five kinds (extensible):

    - ``"url"`` — an HTTP(S) endpoint; ``value`` is the URL string.
    - ``"file"`` — a local filesystem path; ``value`` is a :class:`Path`.
    - ``"parent"`` — a previous Incorporator graph; ``value`` is an
      :class:`Incorporator` or :class:`IncorporatorList`.
    - ``"payload"`` — a per-request POST body list; ``value`` is
      ``List[Dict[str, Any]]``.
    - ``"kwargs"`` — escape hatch carrying a raw ``incorp()`` kwargs
      dict (used by architect's per-source dispatch); ``value`` is a
      dict.

    Construct via the named factories or :meth:`parse` for auto-detect.

    Examples::

        SourceRef.from_url("https://api.example.com/users")
        SourceRef.from_file(Path("./data.json"))
        SourceRef.parse("./data.json")  # kind="file"
        SourceRef.parse("https://x")    # kind="url"
        SourceRef.parse({"inc_url": "https://x", "verb": "fjord"})  # kind="kwargs"
    """

    kind: SourceKind
    value: Any

    @classmethod
    def from_url(cls, url: str) -> "SourceRef":
        """Construct a URL source reference.

        Args:
            url: HTTP(S) endpoint string.
        """
        return cls(kind="url", value=url)

    @classmethod
    def from_file(cls, path: Union[str, "os.PathLike[str]"]) -> "SourceRef":
        """Construct a file source reference.

        The ``path`` is stored verbatim (strings stay strings, ``Path``
        instances stay Paths) so Windows-vs-POSIX formatting survives
        round-trips through error messages.  Use :meth:`as_str` to flatten.
        """
        return cls(kind="file", value=path)

    @classmethod
    def from_parent(cls, parent: Any) -> "SourceRef":
        """Construct a parent source reference (Incorporator or IncorporatorList).

        Args:
            parent: Previous :class:`Incorporator` graph the caller wants
                to drill from.  Not type-checked here because importing
                ``Incorporator`` would cycle; callers know what they
                pass.
        """
        return cls(kind="parent", value=parent)

    @classmethod
    def from_payload(cls, payload: list[dict[str, Any]]) -> "SourceRef":
        """Construct a payload-list source reference (bulk POST dispatch).

        Args:
            payload: List of per-request body dicts.  Defensively copied
                so the caller's list mutations don't reach into the
                frozen :class:`SourceRef`.
        """
        return cls(kind="payload", value=list(payload))

    @classmethod
    def from_kwargs(cls, kwargs: Mapping[str, Any]) -> "SourceRef":
        """Construct a raw-kwargs source reference (architect's dispatch path).

        Args:
            kwargs: Mapping spread verbatim as :meth:`Incorporator.incorp`
                kwargs.  Defensively copied into a fresh dict.
        """
        return cls(kind="kwargs", value=dict(kwargs))

    @classmethod
    def parse(cls, value: Any) -> "SourceRef":
        """Auto-detect kind from the shape of ``value``.

        Recognises:

        - ``str`` starting with ``"http://"`` / ``"https://"`` → url
        - other ``str`` or :class:`os.PathLike` → file
        - :class:`Mapping` → kwargs (architect's per-source dispatch path)

        Args:
            value: Source specification of unknown type.

        Returns:
            A :class:`SourceRef` with the auto-detected kind.

        Raises:
            ValueError: When the value's shape can't be auto-classified.
                ``parent`` and ``payload`` forms can't be distinguished
                from generic objects / lists, so callers use the
                explicit :meth:`from_parent` / :meth:`from_payload`
                factories instead.
        """
        if isinstance(value, str):
            if value.startswith(("http://", "https://")):
                return cls.from_url(value)
            return cls.from_file(value)
        if isinstance(value, os.PathLike):
            return cls.from_file(value)
        if isinstance(value, Mapping):
            return cls.from_kwargs(value)
        raise ValueError(
            f"SourceRef.parse() cannot auto-detect kind for {type(value).__name__}; "
            "use SourceRef.from_parent() / .from_payload() for those forms."
        )

    def as_str(self) -> str:
        """Flat string representation for back-compat with the legacy ``List[str]`` contract.

        URL and file kinds return their string form; parent / payload /
        kwargs kinds return the empty string (callers that route these
        through the fetch layer's string list don't need a stable
        identifier — they bypass the URL dispatcher entirely).
        """
        if self.kind == "url":
            return str(self.value)
        if self.kind == "file":
            return str(os.fspath(self.value))
        return ""
