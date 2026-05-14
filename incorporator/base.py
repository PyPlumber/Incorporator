"""
Incorporator Base Module
========================
The core orchestrator and declarative factory for the Incorporator framework.

This file acts purely as a Domain-Driven orchestrator. It contains NO data parsing,
network looping, or schema compilation logic. It delegates to `io/`, `schema/`,
`observability/`, `tools/`, `usercode.py`, and `list.py`, then assembles the
resulting dynamic Pydantic object graphs.
"""

import asyncio
import logging
import threading
import weakref
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    ClassVar,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel, Field

from .io import fetch as network
from .io import handlers as format_parsers
from .io.formats import FormatType, infer_format
from .io.pagination.base import AsyncPaginator
from .list import IncorporatorList, _deduplicate_extracted
from .schema import factory as _factory
from .schema import router
from .usercode import apply_code_transform, load_outflow_module, pascal_case_from_stem

if TYPE_CHECKING:
    from .observability.logger import Wave

# Type variable for strict IDE hinting on subclass generation
TIncorporator = TypeVar("TIncorporator", bound="Incorporator")
logger = logging.getLogger(__name__)

_INSPECTION_LIMIT = 3
_counter_lock = threading.Lock()

# Sentinel that distinguishes "kwarg never passed" from "kwarg passed as None".
# Used by stream() so the default behaviour is "refresh runs with no kwargs"
# (the common case) while still letting callers opt out via explicit
# ``refresh_params=None``.  Module-private; treat any external comparison
# against this object as undefined behaviour.
_UNSET: Any = object()


def _apply_inflow_resolution(
    inflow: Union[str, Path],
    conv_dict: Optional[Dict[str, Any]],
    inc_page: Optional[AsyncPaginator],
) -> Tuple[Optional[Dict[str, Any]], Optional[AsyncPaginator]]:
    """Load the inflow module and resolve string-form tokens in trinity kwargs.

    Shared by :meth:`Incorporator.incorp` and :meth:`Incorporator.refresh`.
    When ``inflow`` is set, imports the module (cached via ``sys.modules``,
    so the first call pays the import cost and all subsequent calls are
    free) and resolves any string-form tokens in ``conv_dict`` and
    ``inc_page`` against the module's public symbols.

    Real Python callables already present in ``conv_dict`` pass through
    unchanged — the resolver only touches strings.
    """
    from .cli.tokens import resolve_tokens
    from .usercode import extract_public_names, load_user_module

    module = load_user_module(inflow, name_hint="_inc_trinity_inflow")
    extra_names = extract_public_names(module)
    resolved_conv = cast(
        Optional[Dict[str, Any]],
        resolve_tokens(conv_dict, extra_names=extra_names) if conv_dict else conv_dict,
    )
    resolved_page = inc_page
    if isinstance(inc_page, str):
        resolved_page = cast(
            Optional[AsyncPaginator],
            resolve_tokens(inc_page, extra_names=extra_names),
        )
    return resolved_conv, resolved_page


# ==========================================
# THE INCORPORATOR ENGINE
# ==========================================
class Incorporator(BaseModel):
    """The Incorporator base class — subclass it to build an async ETL pipeline.

    Subclassing (not instantiation) is the primary user interaction.  Every
    subclass automatically gets its own dynamically generated Pydantic V2
    model and its own instance registry, so unrelated data sources never
    share state.

    Public verbs you call on a subclass:

    - :meth:`incorp` — fetch + parse + build the object graph.
    - :meth:`test` — let the framework write your ``incorp()`` kwargs for
      you by inspecting an unknown endpoint.
    - :meth:`refresh` — re-fetch live data into existing instances,
      deduplicated by origin URL/file.
    - :meth:`export` — serialise the object graph out to any supported
      format (JSON, NDJSON, CSV, Parquet, SQLite, Avro, Feather, ORC,
      Excel, XML, …).
    - :meth:`stream` — run a long-running pipeline as a daemon; yield one
      :class:`Wave` per tick.
    - :meth:`fjord` — fuse multiple sources through a user-supplied
      ``outflow(state)`` function and export the combined output.
    - :meth:`display` — REPL identity print for ad-hoc debugging.

    Design contract:

    - Inherits from :class:`pydantic.BaseModel` with ``extra='allow'`` so
      unexpected fields from messy APIs never raise ``ValidationError``.
    - Every instance auto-registers into its subclass's :attr:`inc_dict`
      via :meth:`model_post_init`, **and** into every parent class's
      registry up the MRO (the "Bubble-Up" pattern — see that method).
      Together with ``weakref``-backed storage this means 10M-row
      ingestion stays O(1) in memory: entries vanish from the registry
      as soon as the holding list goes out of scope.

    Example:
        Minimal subclass + fetch::

            from incorporator import Incorporator

            class User(Incorporator):
                pass

            users = await User.incorp("https://api.example.com/users", inc_code="id")
            print(users.inc_dict[42].name)         # O(1) lookup by id

        Concurrent fan-out (list source triggers ``asyncio.gather``)::

            users = await User.incorp([
                "https://api.example.com/users/1",
                "https://api.example.com/users/2",
            ])

        Subclasses do **not** share registries::

            class A(Incorporator): pass
            class B(Incorporator): pass
            await A.incorp(...)
            await B.incorp(...)
            assert A.inc_dict is not B.inc_dict      # Always true.
    """

    # ------------------------------------------------------------------
    # Class-level state — every subclass gets its own copy of each ClassVar.
    # ------------------------------------------------------------------

    #: Per-class instance registry. Auto-populated by :meth:`model_post_init`
    #: on every successful ``__init__``. Typed as
    #: ``weakref.WeakValueDictionary`` so entries are reclaimed automatically
    #: when the holding list goes out of scope — this is what prevents OOM
    #: crashes on 10M+ row ingestion. Sibling subclasses get isolated
    #: registries to prevent cross-contamination during graph drilling.
    inc_dict: ClassVar[weakref.WeakValueDictionary[Any, "Incorporator"]] = weakref.WeakValueDictionary()

    #: Module-scoped auto-increment counter used to synthesise unique
    #: ``inc_code`` values when the API omits an identity field. Guarded by
    #: ``_counter_lock`` so concurrent workers spawned via
    #: ``asyncio.to_thread`` never produce duplicate keys.
    _auto_counter: ClassVar[int] = 1

    #: Per-class superset of every (field_name → JSON-schema-property) entry
    #: ever observed across all :meth:`incorp` calls on this class.  Updated
    #: lazily from the raw ``transformed_data`` **before** Pydantic absorbs
    #: extra fields, so it sees the unfiltered API shape.  Consumed by
    #: :meth:`export` to generate the destination schema for Avro
    #: (``pydantic_schema``) and to seed ``all_field_names`` for CSV/SQLite.
    #: Concurrent writes are guarded by a per-class ``threading.Lock`` held
    #: in :mod:`incorporator.schema.factory`.
    _schema_union: ClassVar[Dict[str, Any]] = {}

    #: Origin tracking — the URL the subclass was first populated from.
    #: Populated on the first :meth:`incorp` call; :meth:`refresh` falls back
    #: to this when called without an explicit ``new_url``.
    inc_url: ClassVar[Optional[str]] = None

    #: Origin tracking — the local file path the subclass was first populated
    #: from. Same fallback semantics as :attr:`inc_url`.
    inc_file: ClassVar[Optional[str]] = None

    #: Identity-mapping memory — the ``inc_code`` field name passed to the
    #: first :meth:`incorp` call.  :meth:`refresh` defaults to this when the
    #: caller doesn't re-pass ``inc_code=`` so the in-state refresh contract
    #: ("re-hit the source and update existing records by their primary key")
    #: actually works on a no-args call.
    _inc_code_attr: ClassVar[Optional[str]] = None

    #: Identity-mapping memory — same as :attr:`_inc_code_attr` but for the
    #: ``inc_name`` display field.
    _inc_name_attr: ClassVar[Optional[str]] = None

    # ------------------------------------------------------------------
    # Universal instance attributes — present on every Incorporator object.
    # ------------------------------------------------------------------

    inc_code: Any = Field(
        default=None,
        description="Primary key used to register this instance in ``cls.inc_dict``. "
        "Auto-synthesised from ``_auto_counter`` if the source data has no "
        "identity field — pass ``inc_code='id'`` (or similar) to incorp() to "
        "use a real field as the key.",
    )
    inc_name: Optional[str] = Field(
        default=None,
        description="Optional human-readable name. Used by :meth:`display` for "
        "REPL inspection and by some converters as a label.",
    )
    last_rcd: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp of when this instance was constructed. "
        "Useful for staleness checks in stateful-polling pipelines.",
    )

    def display(self) -> None:
        """Print this instance's identity fields to stdout.

        Emits a compact line containing ``class``, ``inc_code``, ``inc_name``,
        and ``last_rcd`` — useful for REPL inspection and ad-hoc debugging.
        For structured output use :meth:`pydantic.BaseModel.model_dump_json`.
        """
        cls_name = getattr(self.__class__, "__name__", "UnknownClass")
        print(f'class:"{cls_name}", inc_code:"{self.inc_code}", inc_name:"{self.inc_name}", last_rcd:"{self.last_rcd}"')

    def model_post_init(self, __context: Any) -> None:
        """Pydantic V2 lifecycle hook — register this instance into ``inc_dict``.

        Runs once, synchronously, immediately after Pydantic's Rust core
        finishes ``__init__``.  Performs two jobs:

        1. **Auto-key**: if ``self.inc_code`` is ``None`` (the API gave no
           identity field), assigns the next value of ``cls._auto_counter``
           under ``_counter_lock`` so concurrent ``asyncio.to_thread`` workers
           cannot collide on the same synthetic key.

        2. **Bubble-Up registration**: registers ``self`` into ``cls.inc_dict``
           AND into every parent class's ``inc_dict`` up the MRO (stopping
           at :class:`Incorporator`).  This is what makes ``link_to()``
           lookups work regardless of whether the user holds the dynamic
           subclass or a hand-defined base class — a defence against the
           "Schema Splintering" bug where two API endpoints yield slightly
           different schemas for the same logical entity.
        """
        cls = self.__class__

        # Auto-increment if the API provided no unique identifier
        if self.inc_code is None:
            with _counter_lock:
                self.inc_code = cls._auto_counter
                cls._auto_counter += 1

        cls.inc_dict[self.inc_code] = self

        # DSA OPTIMIZATION: Fast-path C-tuple evaluation.
        # Only iterate bases if this is a deeply nested dynamic subclass.
        if cls.__bases__ and cls.__bases__[0] is not Incorporator:
            for base in cls.__bases__:
                if issubclass(base, Incorporator) and base is not Incorporator:
                    base.inc_dict[self.inc_code] = self

    # ==========================================
    # PUBLIC VERBS
    # ==========================================
    @classmethod
    async def incorp(
        cls: Type[TIncorporator],
        inc_url: Optional[Union[str, List[str]]] = None,
        inc_file: Optional[Union[str, List[str]]] = None,
        inc_parent: Optional[Union[TIncorporator, "IncorporatorList[TIncorporator]"]] = None,
        inc_child: Optional[str] = None,
        inc_code: Optional[str] = None,
        inc_name: Optional[str] = None,
        excl_lst: Optional[List[str]] = None,
        conv_dict: Optional[Dict[str, Any]] = None,
        name_chg: Optional[List[Tuple[str, str]]] = None,
        inc_page: Optional[AsyncPaginator] = None,
        inflow: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> Union[TIncorporator, "IncorporatorList[TIncorporator]"]:
        """Fetch data from an HTTP API or local file and return Pydantic objects.

        The single entry point for Extract + Transform.  Builds a dynamic
        Pydantic V2 model from the raw payload, coerces every field through
        the converter pipeline (``conv_dict``), registers each instance in
        ``cls.inc_dict``, and returns either a single instance (when the
        source resolves to one record) or an :class:`IncorporatorList`
        wrapping multiple records.

        Concurrency: if ``inc_url`` or ``inc_file`` is a ``List[str]``, the
        sources are fetched concurrently via an ``asyncio.gather`` sliding
        window (size ``concurrency_limit``, default 50) with built-in
        per-source rate limiting and exponential-backoff retries.

        Args:
            inc_url: HTTP source. ``str`` for a single endpoint;
                ``List[str]`` triggers concurrent fan-out.
            inc_file: Local file path. Same ``str`` / ``List[str]``
                polymorphism. Compressed archives (``.gz``, ``.bz2``,
                ``.xz``, ``.zip``, ``.tar``) are auto-detected and
                transparently decompressed.
            inc_parent: Pass a previous ``incorp()`` result to enable the
                Parent-Child routing pattern: child URLs are extracted from
                ``inc_parent`` via ``inc_child`` (dot-notation JSONPath),
                deduplicated, and fanned out concurrently. Fully abstracts
                the HATEOAS "Discovery & Enrichment" workflow.
            inc_child: Dot-notation path on parent objects to extract child
                IDs or URLs (e.g. ``"results.url"`` or ``"Vehicle.VIN"``).
                Only meaningful with ``inc_parent``.
            inc_code: Source-field name to use as the primary key for
                ``cls.inc_dict`` registration. If omitted, instances are
                keyed by an auto-incremented integer from ``_auto_counter``.
            inc_name: Source-field name used as the human-readable label.
                Stored on each instance as ``self.inc_name``.
            excl_lst: List of field names to **drop** before Pydantic
                compilation — useful for stripping heavy keys like
                ``"image_data"`` or ``"raw_html"``.
            conv_dict: Mapping of ``field_name → converter`` applied before
                validation. Converters include :func:`inc` (type coercion),
                :func:`calc` (computed fields), :func:`link_to` (cross-class
                joins), :func:`pluck` (nested extraction), :func:`split_and_get`,
                :func:`each`, :func:`join_all`, and :func:`as_list`.
            name_chg: List of ``(old_name, new_name)`` renames applied
                before validation — useful for normalising field names
                across heterogeneous sources.
            inc_page: An :class:`AsyncPaginator` instance for streaming
                pagination. Supported paginators include
                :class:`NextUrlPaginator`, :class:`CursorPaginator`,
                :class:`OffsetPaginator`, :class:`PageNumberPaginator`,
                :class:`LinkHeaderPaginator`, :class:`SQLitePaginator`,
                :class:`CSVPaginator`, and :class:`AvroPaginator`.
            **kwargs: Forwarded to the format handler **and** the HTTP
                client. Common keys: ``rec_path`` (dot-notation drill-down
                to extract a list from a wrapper response), ``http_method``
                (``"GET"`` / ``"POST"`` / ``"PUT"`` / ``"PATCH"``),
                ``json_payload`` or ``form_payload`` (POST body),
                ``payload_list`` (per-request POST bodies for bulk
                dispatch), ``payload_type`` (``"json"`` or ``"form"``),
                ``call_lim`` (cap pagination at N pages), ``sql_query``
                (custom SELECT for SQLite sources), ``archive_target``
                (file inside a ZIP/TAR to extract), ``concurrency_limit``
                (sliding-window worker count, default 50),
                ``requests_per_second`` (rate limit, default 15),
                ``timeout`` (HTTP timeout, default 15s), ``headers``
                (custom HTTP headers), ``ignore_ssl`` (disable TLS
                verification — use with care).

        Returns:
            ``TIncorporator``: A single instance when the source resolved
            to exactly one record and ``inc_page`` was not used.

            ``IncorporatorList[TIncorporator]``: A list wrapper for multiple
            records. The list also carries ``.failed_sources`` containing
            URLs/paths that hit permanent 429 / network errors — the Dead
            Letter Queue for programmatic retry.

        Raises:
            ValueError: When no source is provided (no ``inc_url``,
                ``inc_file``, ``inc_parent``, or ``payload_list``).
            IncorporatorFormatError: When parsing fails — the underlying
                :class:`json.JSONDecodeError`, :class:`csv.Error`,
                :class:`xml.etree.ElementTree.ParseError`, etc., are
                trapped and recast.
            IncorporatorNetworkError: For permanent 4xx (except 429) or
                URL-scheme violations.

        Examples:
            Basic JSON URL::

                class User(Incorporator):
                    pass

                users = await User.incorp(
                    "https://api.example.com/users",
                    inc_code="id",
                )
                print(users.inc_dict[42].name)

            Concurrent fan-out across multiple URLs::

                users = await User.incorp([
                    "https://api.example.com/users/1",
                    "https://api.example.com/users/2",
                    "https://api.example.com/users/3",
                ])

            Parent-Child enrichment (HATEOAS)::

                class Nav(Incorporator): pass
                class Pokemon(Incorporator): pass

                nav = await Nav.incorp(
                    "https://pokeapi.co/api/v2/pokemon?limit=20",
                    rec_path="results",
                    inc_child="url",
                )
                pokemon = await Pokemon.incorp(
                    inc_parent=nav,
                    inc_code="id",
                )

            Pagination with type casting::

                from datetime import datetime
                from incorporator import inc, NextUrlPaginator

                launches = await Launch.incorp(
                    inc_url="https://ll.thespacedevs.com/2.2.0/launch/upcoming/",
                    rec_path="results",
                    inc_page=NextUrlPaginator("next"),
                    call_lim=5,
                    conv_dict={"net": inc(datetime)},
                )

            Bulk POST with declarative payload tokens::

                from incorporator import join_all

                results = await NHTSA.incorp(
                    inc_url="https://vpic.nhtsa.dot.gov/api/.../DecodeVINValuesBatch/",
                    inc_parent=invoices,
                    http_method="POST",
                    payload_type="form",
                    form_payload={"format": "json", "data": join_all(";")},
                    rec_path="Results",
                )
        """

        # inflow= sidecar: load any user-defined helpers and resolve string-
        # form tokens in conv_dict / inc_page against the module's public
        # symbols.  Cheap when not set; module imports are cached via
        # sys.modules so per-call cost is one dict lookup after first load.
        if inflow is not None:
            conv_dict, inc_page = _apply_inflow_resolution(inflow, conv_dict, inc_page)

        # Route to Parent Execution if triggered
        if inc_parent is not None:
            kwargs.update(
                {
                    "inc_url": inc_url,
                    "inc_file": inc_file,
                    "inc_child": inc_child,
                    "inc_code": inc_code,
                    "inc_name": inc_name,
                    "excl_lst": excl_lst,
                    "conv_dict": conv_dict,
                    "name_chg": name_chg,
                    "inc_page": inc_page,
                }
            )
            return cast(
                Union[TIncorporator, "IncorporatorList[TIncorporator]"],
                await _factory.child_incorp(cls, inc_parent=inc_parent, **kwargs),
            )

        source = inc_file if inc_file else inc_url
        if not source and not kwargs.get("payload_list"):
            raise ValueError(
                f"[{cls.__name__}] Either 'inc_url', 'inc_file', or a valid 'inc_parent' must be provided."
            )

        # Auto-Infer SQLite Queries
        if source:
            network._inject_sqlite_query(source, kwargs.get("sql_table") or cls.__name__.lower(), kwargs)

        is_file_mode = bool(inc_file)
        source_list: List[str] = network._normalize_source_list(source, kwargs.get("payload_list"))

        is_single = not isinstance(source, list) and inc_page is None

        # Lock Root Class Origin Context
        if is_single and isinstance(source, str):
            if is_file_mode:
                cls.inc_file = source
            else:
                cls.inc_url = source

        # Remember identity-mapping kwargs so refresh() can reuse them without
        # forcing the caller to re-pass inc_code= / inc_name= on every tick.
        if inc_code is not None:
            cls._inc_code_attr = inc_code
        if inc_name is not None:
            cls._inc_name_attr = inc_name

        # Persist the FULL network / format-handler context so refresh()
        # can replay the same fetch without forcing the caller to re-declare
        # ``params`` / ``headers`` / ``conv_dict`` / ``rec_path`` on every
        # tick.  Without this, the stateful-daemon refresh would hit the
        # bare URL — e.g. CoinGecko returns 422 when ``?vs_currency=usd``
        # is dropped.  User-supplied ``refresh_params`` still win on key
        # conflict (handled in ``refresh()`` below).  ``__inspect`` is a
        # one-shot inspector flag and is intentionally not replayed.
        cls._incorp_kwargs = {
            "conv_dict": conv_dict,
            "excl_lst": excl_lst,
            "name_chg": name_chg,
            **{k: v for k, v in kwargs.items() if k != "__inspect"},
        }

        # Extract control flags before network call so they don't pollute handlers
        __inspect = kwargs.pop("__inspect", False)
        payload_list = kwargs.pop("payload_list", None)

        # I/O Network Phase
        parsed_data, failed_sources = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=is_file_mode,
            inc_page=inc_page,
            payload_list=payload_list,
            __inspect=__inspect,
            **kwargs,
        )

        # Routes raw data to the Inspector if triggered
        if __inspect:
            from .tools.inspector import analyze_data

            analyze_data(parsed_data, {"rec_path": kwargs.get("rec_path")})

        # Build Phase — runs in a thread pool to keep the event loop free
        result = await asyncio.to_thread(
            _factory.build_instances,
            cls,
            parsed_data,
            failed_sources,
            is_single,
            inc_code=inc_code,
            inc_name=inc_name,
            excl_lst=excl_lst,
            conv_dict=conv_dict,
            name_chg=name_chg,
        )

        # Retain parent linking instructions for potential nested refreshes
        if inc_child is not None and isinstance(result, IncorporatorList):
            result.inc_child_path = inc_child

        return cast(Union[TIncorporator, "IncorporatorList[TIncorporator]"], result)

    @classmethod
    async def refresh(
        cls: Type[TIncorporator],
        instance: Optional[Union[str, Path, TIncorporator, List[TIncorporator]]] = None,
        new_url: Optional[Union[str, List[str]]] = None,
        new_file: Optional[Union[str, List[str]]] = None,
        inc_child: Optional[str] = None,
        inc_code: Optional[str] = None,
        inc_name: Optional[str] = None,
        excl_lst: Optional[List[str]] = None,
        conv_dict: Optional[Dict[str, Any]] = None,
        name_chg: Optional[List[Tuple[str, str]]] = None,
        inc_page: Optional[AsyncPaginator] = None,
        inflow: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> Union[TIncorporator, "IncorporatorList[TIncorporator]"]:
        """Re-fetch live data and hydrate existing instances in-place.

        ``refresh()`` is the stateful-update verb of the framework.  It is
        designed for long-running pipelines that need to keep an in-memory
        object graph synchronised with a changing remote source without
        rebuilding from scratch.

        Instance resolution has three modes, chosen by the ``instance`` arg:

        - **In-state (``instance=None``)**: refresh every object currently
          in ``cls.inc_dict``. Origin URLs/files are read from each
          instance's stored ``inc_url`` / ``inc_file`` attributes.
        - **Re-source (``instance=str | Path``)**: re-fetch ``cls.inc_dict``
          from a brand-new URL or file. If the string starts with ``http``
          it is treated as a URL; otherwise as a local file path.
        - **Targeted (``instance=List[obj] | obj``)**: refresh only the
          listed instances. Useful for partial updates.

        Deduplication: HTTP requests are deduplicated across the resolved
        instance set via origin URL/file, so 1000 instances sharing
        20 source URLs trigger 20 fetches, not 1000.

        Args:
            instance: Resolution mode selector (see above). Defaults to
                ``None`` (refresh everything in ``cls.inc_dict``).
            new_url: Optional override URL(s) — equivalent to passing
                a string to ``instance`` but typed explicitly.
            new_file: Optional override file path(s).
            inc_child: Same as in :meth:`incorp` — drill into nested
                child URLs for re-enrichment.
            inc_code: Override the registry key field on this refresh.
            inc_name: Override the display name field.
            excl_lst: Same as in :meth:`incorp`.
            conv_dict: Same as in :meth:`incorp` — re-apply or change
                type coercion on the refreshed data.
            name_chg: Same as in :meth:`incorp`.
            inc_page: Optional paginator for streaming refresh.
            **kwargs: Forwarded to the network engine and format handlers
                (see :meth:`incorp` for the full kwarg list).

        Returns:
            ``IncorporatorList[TIncorporator]``: A new list wrapping the
            refreshed instances. Existing Python references are mutated
            in-place via Pydantic field updates, so callers holding the
            old list will see updated values without reassigning.

        Raises:
            ValueError: When neither a new source nor stored origin URLs
                are available (e.g. ``refresh()`` called before any
                ``incorp()`` and with no override).

        Examples:
            Simple in-state refresh::

                users = await User.incorp("https://api.example.com/users")
                # ... some time later ...
                refreshed = await User.refresh()      # uses User.inc_url
                # `users[0]` now has the latest field values.

            Refresh from a new source::

                refreshed = await User.refresh("https://api.example.com/users-v2")

            Partial refresh of specific instances::

                stale = [users.inc_dict[i] for i in (1, 2, 3)]
                refreshed = await User.refresh(instance=stale)
        """

        target_url = new_url
        target_file = new_file

        # Fall back to the identity-mapping kwargs the class was first loaded
        # with — so callers can use ``await Cls.refresh()`` without re-passing
        # the same inc_code / inc_name they passed to ``incorp()``.
        if inc_code is None:
            inc_code = getattr(cls, "_inc_code_attr", None)
        if inc_name is None:
            inc_name = getattr(cls, "_inc_name_attr", None)

        # Replay the persisted network / format-handler context from the
        # original ``incorp()`` call.  This is what makes stateful polling
        # "just work" — the seed call's ``params={"vs_currency": "usd"}``,
        # ``headers``, ``rec_path``, ``conv_dict``, etc. are re-applied on
        # every refresh tick so the same endpoint returns the same shape.
        # User-supplied ``refresh_params`` win on key conflict: caller's
        # explicit kwargs are appended LAST in the merge below.
        persisted: Dict[str, Any] = getattr(cls, "_incorp_kwargs", None) or {}
        if conv_dict is None:
            conv_dict = persisted.get("conv_dict")
        if excl_lst is None:
            excl_lst = persisted.get("excl_lst")
        if name_chg is None:
            name_chg = persisted.get("name_chg")
        # Network / handler kwargs (params, headers, rec_path, sql_query,
        # parquet_* etc.).  Filter out the three explicit-param slots we
        # already handled so they don't double-feed.
        persisted_net = {
            k: v for k, v in persisted.items()
            if k not in ("conv_dict", "excl_lst", "name_chg")
        }
        kwargs = {**persisted_net, **kwargs}

        # Unrolled instance resolution for DX traceability
        if instance is None:
            inst_list = cast(List[TIncorporator], list(cls.inc_dict.values()))
        elif isinstance(instance, (str, Path)):
            inst_list = cast(List[TIncorporator], list(cls.inc_dict.values()))
            target_str = str(instance)
            if target_str.startswith("http"):
                target_url = target_str
            else:
                target_file = target_str
        else:
            inst_list = cast(List[TIncorporator], instance if isinstance(instance, list) else [instance])

        # Re-source mode: refresh("new_url") or refresh(new_url=...) /
        # refresh(new_file=...) should update the class's origin tracking
        # so subsequent in-state refreshes hit the new source, not the old one.
        if target_url:
            cls.inc_url = target_url
        elif target_file:
            cls.inc_file = target_file

        if not inst_list:
            logger.warning(
                "[%s] refresh() called but no instances are loaded in inc_dict — "
                "returning empty list. Did you forget to call incorp() first?",
                cls.__name__,
            )
            return IncorporatorList(cls, [])

        # inflow= sidecar: same DX as incorp() — resolve string-form tokens
        # in conv_dict / inc_page against the module's public symbols.
        if inflow is not None:
            conv_dict, inc_page = _apply_inflow_resolution(inflow, conv_dict, inc_page)

        TargetClass = inst_list[0].__class__
        kwargs["http_method"] = kwargs.pop("method", kwargs.pop("http_method", "GET")).upper()

        child_path = inc_child or getattr(inst_list[0], "inc_child_path", None)

        # Use the Router to drill the Graph
        extracted_data = router.extract_parent_data(inst_list, child_path) if child_path else inst_list

        if child_path and extracted_data:
            extracted_data = _deduplicate_extracted(extracted_data)

        # Target Resolution
        target = target_url or target_file
        source_urls = network._normalize_source_list(target, None)

        # Declarative routing only fires when there's something declarative to
        # route — a `new_url` (URL-template / POST-token injection) or a
        # `child_path` (parent → child drill).  Pure in-state refresh (no
        # args, no inc_child) skips this and falls through to the origin-URL
        # fallback below, which reads each instance's stored `inc_url`.
        if not target_file and extracted_data and (target_url or child_path):
            # Use the Router to build declarative payloads
            kwargs = router.resolve_declarative_routing(cls.__name__, extracted_data, source_urls, **kwargs)
            raw_url = kwargs.pop("inc_url", source_urls)
            source_list: List[str] = network._normalize_source_list(raw_url, None)
            payload_list = kwargs.pop("payload_list", None)
        else:
            source_list = source_urls
            payload_list = kwargs.pop("payload_list", None)

        # Fallback to origin URLs from memory if no new URLs were explicitly passed
        if not source_list and not target_file:
            raw_sources = [getattr(inst, "inc_url", getattr(inst, "inc_file", "")) for inst in inst_list]
            source_list = list(dict.fromkeys([str(u) for u in raw_sources if u]))
            if not source_list:
                raise ValueError(f"[{cls.__name__}] Instances contain no origin URLs to refresh from.")

        parsed_data, failed_sources = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=bool(target_file) or (not target_url and getattr(inst_list[0], "inc_file", None) is not None),
            inc_page=inc_page,
            payload_list=payload_list,
            **kwargs,
        )

        result = await asyncio.to_thread(
            _factory.build_instances,
            cls,
            parsed_data,
            failed_sources,
            is_single=(len(source_list) <= 1 and inc_page is None),
            target_class=TargetClass,
            inc_code=inc_code,
            inc_name=inc_name,
            excl_lst=excl_lst,
            conv_dict=conv_dict,
            name_chg=name_chg,
        )

        if inc_child is not None and isinstance(result, IncorporatorList):
            result.inc_child_path = inc_child

        return cast(Union[TIncorporator, "IncorporatorList[TIncorporator]"], result)

    @classmethod
    async def export(
        cls: Type[TIncorporator],
        *,
        instance: Union[str, Path, TIncorporator, List[TIncorporator]],
        file_path: Optional[Union[str, Path]] = None,
        format_type: Optional[FormatType] = None,
        compression: Optional[str] = None,
        sql_table: Optional[str] = None,
        if_exists: str = "replace",
        outflow: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        """Serialise Incorporator instances out to a file in any supported format.

        Streaming-first: records flow through a lazy generator and
        ``model_dump()`` is called per-row only when the handler asks for the
        next item.  No full-list copy is materialised in RAM, so 10M-row
        exports stay flat on RSS.

        All parameters after ``cls`` are keyword-only (enforced by the bare ``*``).

        The ``instance`` argument is polymorphic:

        - **In-state mode** (``file_path=None``, default): ``instance`` is
          interpreted as the **output path** and the data source is
          ``cls.inc_dict.values()``.  Convenient one-liner for "dump my
          current state to disk."
        - **Explicit mode** (``file_path`` provided): ``instance`` is the
          data source (a single instance or a list) and ``file_path`` is
          the destination.

        Args:
            instance: Either the output path (in-state mode) or the data
                source (explicit mode). When ``file_path`` is provided
                this must be a ``list`` or a Pydantic ``BaseModel``;
                a plain string here raises ``TypeError``.
            file_path: Destination file path. Omit to use in-state mode.
            format_type: Override the format inferred from the destination
                file extension.  Supported values: ``JSON``, ``NDJSON``,
                ``CSV``, ``TSV``, ``PSV``, ``XML``, ``SQLITE``, ``AVRO``,
                ``PARQUET``, ``FEATHER``, ``ORC``, ``XLSX`` (some require
                opt-in extras — see :doc:`/docs/formats_and_compression`).
            compression: Optional compression to apply **after** writing
                (e.g. ``"gz"``, ``"bz2"``, ``"xz"``, ``"zip"``, ``"tar"``,
                ``"zstd"``, ``"lz4"``, ``"snappy"``, ``"brotli"``).  Runs
                in a background thread via ``asyncio.to_thread``.
            sql_table: Table name for SQLite exports. Defaults to
                ``cls.__name__.lower()``.  Sanitised against SQL injection.
            if_exists: How to handle an existing table/file:
                ``"replace"`` (default), ``"append"``, or ``"fail"``.
            outflow: Path to a Python file defining a top-level
                ``transform(instances) -> Iterable``.  Called once with
                the in-state object list **before** serialisation; the
                return value is exported instead.  The function must
                accept **exactly one** parameter (enforced via
                :func:`inspect.signature`).  Records may be dicts or
                Pydantic models; new fields added by the transform are
                detected via first-row peek and become CSV columns
                automatically.
            **kwargs: Forwarded to the format handler — e.g. ``delimiter``
                (CSV/TSV/PSV), ``xml_root`` (XML), ``json_indent`` (JSON).

        Returns:
            ``None``. The file is written as a side effect.  In-progress
            failures bubble up as :class:`IncorporatorFormatError`.

        Raises:
            TypeError: When ``file_path`` is provided but ``instance`` is
                neither a list nor a ``BaseModel`` (e.g. a plain string).
            ValueError: When ``outflow`` is provided but its
                ``transform()`` function has more or fewer than one
                parameter.
            IncorporatorFormatError: On unsupported format / unwritable path.

        Examples:
            Explicit mode (list of instances → file)::

                users = await User.incorp("https://api.example.com/users")
                await User.export(instance=users, file_path="users.csv")

            In-state mode (registry → file)::

                await User.export("users.json")     # uses cls.inc_dict

            Cross-format pivot (JSON API → SQLite warehouse)::

                users = await User.incorp("https://api.example.com/users")
                await User.export(instance=users, file_path="warehouse.db")

            With outflow transform::

                # transform.py
                def transform(instances):
                    return [
                        {"id": u.id, "name": u.name.upper()}
                        for u in instances
                    ]

                await User.export(
                    instance=users,
                    file_path="upper.csv",
                    outflow="transform.py",
                )
        """
        # Unrolled instance resolution for DX traceability
        if file_path is None:
            actual_path = str(instance)
            instances: List[TIncorporator] = cast(List[TIncorporator], list(cls.inc_dict.values()))
        else:
            actual_path = str(file_path)
            if not isinstance(instance, list):
                from pydantic import BaseModel

                if not isinstance(instance, BaseModel):
                    raise TypeError(
                        f"export() 'instance' must be an Incorporator instance or a list, "
                        f"got {type(instance).__name__!r}. "
                        "Pass a list returned by incorp(), or omit file_path to use in-state export."
                    )
            instances = cast(
                List[TIncorporator],
                instance if isinstance(instance, list) else [instance],
            )

        if not instances:
            return

        # Optional code transform — runs in a thread (user code may be CPU-bound).
        transform_source: Iterable[Any] = instances
        outflow_field_names: Optional[List[str]] = None
        if outflow is not None:
            transform_source = await asyncio.to_thread(apply_code_transform, instances, outflow)
            # Peek at the first transformed row so we can rebuild all_field_names from the
            # *actual* output schema — outflow may add, remove, or rename fields.
            import itertools as _itertools

            _transform_iter = iter(transform_source)
            _first_row = next(_transform_iter, None)
            if _first_row is not None:
                if isinstance(_first_row, dict):
                    outflow_field_names = list(_first_row.keys())
                elif hasattr(_first_row, "model_dump"):
                    outflow_field_names = list(_first_row.model_dump(by_alias=True, mode="json").keys())
                transform_source = _itertools.chain([_first_row], _transform_iter)

        active_format = format_type or infer_format(actual_path)
        kwargs.update(
            {
                "sql_table": sql_table or (cls.__name__.lower() if active_format == FormatType.SQLITE else None),
                "if_exists": if_exists,
                "pydantic_schema": {"properties": cls._schema_union},
                "all_field_names": outflow_field_names or list(cls._schema_union.keys()) or None,
            }
        )

        # Lazy serialization generator: model_dump() is called per-object only when
        # the handler requests the next row — no full-list copy in RAM.
        # Handles both Incorporator objects (model_dump) and raw dicts from outflow transforms.
        #
        # Fast path for text formats (JSON / NDJSON): yield the Pydantic instance
        # directly so the handler can call ``model_dump_json()`` — Pydantic v2's
        # Rust core serialises straight to JSON bytes without allocating the
        # intermediate dict.  ~15–25 % throughput win on realistic payloads.
        # All other formats need columnar access to keyed values, so they still
        # receive plain dicts.
        is_json_text_format = active_format in (FormatType.JSON, FormatType.NDJSON)

        def _make_lazy_iter(source: Iterable[Any]) -> Iterable[Any]:
            for obj in source:
                if isinstance(obj, Incorporator):
                    if is_json_text_format:
                        # The text-format handlers accept Incorporator instances
                        # and skip the dict round-trip via model_dump_json().
                        yield obj
                    else:
                        yield obj.model_dump(by_alias=True, mode="json")
                else:
                    yield dict(obj)

        await format_parsers.write_destination_data(
            _make_lazy_iter(transform_source), actual_path, active_format, **kwargs
        )

        if compression:
            from .io.compression import compress_file

            await asyncio.to_thread(compress_file, actual_path, compression)

    @classmethod
    async def stream(
        cls: Type[TIncorporator],
        incorp_params: Dict[str, Any],
        refresh_params: Optional[Dict[str, Any]] = _UNSET,  # type: ignore[assignment]
        export_params: Optional[Dict[str, Any]] = None,
        poll_interval: Optional[float] = None,
        stateful_polling: bool = False,
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
        inflow: Optional[Union[str, Path]] = None,
        outflow: Optional[Union[str, Path]] = None,
    ) -> AsyncGenerator["Wave", None]:
        """Run a long-running pipeline; yield one :class:`Wave` per tick.

        ``stream()`` is the production verb for daemons that keep fetching,
        transforming, and exporting until exhausted or cancelled.  Two modes,
        selected by ``stateful_polling``:

        - **Chunking mode** (default, ``stateful_polling=False``): every
          tick calls ``incorp(**incorp_params)`` for the next chunk, then
          optionally ``refresh()`` and ``export()``, then yields one Wave.
          Memory stays O(1) because each chunk is released before the next
          one fetches.  Use this for paginated sources where you want a
          steady throughput trace and an exit when the API runs out.

        - **Stateful daemon mode** (``stateful_polling=True``): seeds the
          dataset with one ``incorp()`` call, then runs refresh and export
          on independent schedules until cancelled.  Use this to keep a
          live in-memory object graph synchronised against an upstream
          API while exporting snapshots at a different cadence.

        Interval cascade: ``refresh_interval`` and ``export_interval`` each
        fall back to ``poll_interval`` when not set, so a single
        ``poll_interval=60.0`` ticks both rhythms identically.

        Args:
            incorp_params: kwargs forwarded to :meth:`incorp` every tick.
            refresh_params: kwargs for :meth:`refresh`.  Omit to skip
                refresh entirely.
            export_params: kwargs for :meth:`export`.  Omit to skip
                export entirely.  In chunking mode, ``if_exists`` is
                forced to ``"append"`` so successive chunks accumulate
                into one output file.
            poll_interval: Default sleep between ticks.  ``None`` means
                "one-shot then exit" — useful for testing.
            stateful_polling: Mode selector — see the two modes above.
            refresh_interval: Override the refresh tick period in stateful
                mode.  Falls back to ``poll_interval`` when omitted.
            export_interval: Override the export tick period in stateful
                mode.  Falls back to ``poll_interval`` when omitted.
            inflow: Optional path to a Python sidecar (``inflow.py``)
                holding user-defined helper functions referenced from
                ``incorp_params["conv_dict"]`` text tokens — calc reducers,
                custom converters, pre-built paginator instances.  Loaded
                once per process; symbols become available to the CLI
                token resolver.  See :doc:`/docs/cli_and_configuration`.
            outflow: Optional path to a Python sidecar (``outflow.py``)
                defining the :class:`Incorporator` subclass whose instances
                the stream should produce.  **Stateful daemon mode only** —
                chunking mode raises ``ValueError`` if ``outflow`` is set,
                since per-chunk state has no persistent registry for a
                user-defined class to attach to.  The subclass is named
                by the file stem in PascalCase (e.g. ``coin_market.py``
                → ``CoinMarket``).

        Yields:
            :class:`Wave`: One per chunk (chunking) or per daemon
            iteration (stateful). Fields:

            - ``chunk_index`` (int): Sequential index within the engine.
            - ``operation`` (str): ``"chunk"``, ``"incorp"``, ``"refresh"``,
              or ``"export"``.
            - ``rows_processed`` (int): Row count for this iteration.
            - ``failed_sources`` (List[str]): URLs/paths that failed.
            - ``processing_time_sec`` (float): Wall-clock duration.
            - ``timestamp`` (datetime): UTC instant of completion.

        Examples:
            Simple chunked stream with paginator::

                async for wave in User.stream(
                    incorp_params={
                        "inc_url": "https://api.example.com/users",
                        "inc_page": NextUrlPaginator("next"),
                    },
                    export_params={"file_path": "users.ndjson"},
                ):
                    print(f"Chunk {wave.chunk_index}: {wave.rows_processed} rows")

            Stateful polling — refresh every 5 min, export every 30 s::

                async for wave in User.stream(
                    incorp_params={"inc_url": "https://api.example.com/users"},
                    refresh_params={},
                    export_params={"file_path": "snapshot.json"},
                    stateful_polling=True,
                    refresh_interval=300.0,
                    export_interval=30.0,
                ):
                    handle(wave)
        """
        from .observability.pipeline import run_pipeline

        # outflow= is a stateful-daemon-only hook: chunking releases per-chunk
        # state and has no persistent registry for a user-defined class to
        # attach to.  Fail loud here rather than silently accepting the
        # kwarg for chunking pipelines.
        if outflow is not None and not stateful_polling:
            raise ValueError(
                "[Incorporator.stream] outflow= requires stateful_polling=True. "
                "Chunking mode releases per-chunk state; a user-defined subclass "
                "has no persistent registry to attach to. Use outflow only with "
                "the stateful daemon engine, or drop the outflow= kwarg."
            )

        # Receiver class swap: when outflow= is set, prefer the user-defined
        # Incorporator subclass over `cls`.  By convention the class is named
        # after the file stem in PascalCase (`outflow.py` -> `Outflow`);
        # if that name is absent the first Incorporator subclass found in
        # the module wins.
        receiver_cls: Type[Incorporator] = cast(Type[Incorporator], cls)
        if outflow is not None:
            from .usercode import load_user_module, pascal_case_from_stem

            module = load_user_module(outflow, name_hint="_inc_stream_outflow")
            preferred_name = pascal_case_from_stem(outflow)
            candidate = getattr(module, preferred_name, None)
            if not (isinstance(candidate, type) and issubclass(candidate, Incorporator)):
                # Fall back: first Incorporator subclass in the module.
                candidate = next(
                    (
                        getattr(module, n)
                        for n in dir(module)
                        if isinstance(getattr(module, n, None), type)
                        and issubclass(getattr(module, n), Incorporator)
                        and getattr(module, n) is not Incorporator
                    ),
                    None,
                )
            if candidate is None:
                raise ValueError(
                    f"[Incorporator.stream] outflow={outflow!r} must define an "
                    f"Incorporator subclass (preferred name {preferred_name!r})."
                )
            receiver_cls = candidate

        # inflow= is a CLI/JSON convenience — when the trinity is driven by
        # the engine directly (this path), the module is loaded once via
        # importlib's sys.modules cache so per-chunk operations are free.
        # Python users passing callables directly into conv_dict don't need
        # this hook at all.
        if inflow is not None:
            from .usercode import load_user_module

            load_user_module(inflow, name_hint="_inc_stream_inflow")

        # Translate the _UNSET sentinel to "{}" (run refresh with defaults).
        # Callers that explicitly want to skip refresh pass refresh_params=None,
        # which propagates through unchanged.  This makes the common case
        # ("just refresh the same source") the default rather than requiring
        # boilerplate refresh_params={} on every stream() call.
        if refresh_params is _UNSET:
            refresh_params = {}

        async for wave in run_pipeline(
            cls=receiver_cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            stateful_polling=stateful_polling,
            refresh_interval=refresh_interval,
            export_interval=export_interval,
        ):
            yield wave

    @classmethod
    async def fjord(
        cls,
        stream_params: List[Dict[str, Any]],
        outflow: Union[str, Path],
        export_params: Dict[str, Any],
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
        inflow: Optional[Union[str, Path]] = None,
    ) -> AsyncGenerator["Wave", None]:
        """Run a multi-source pipeline; fuse the live sources through your ``outflow``.

        ``fjord()`` is the multi-source production verb.  It fetches N
        independent Incorporator subclasses concurrently, keeps each one
        refreshed on its own schedule, hands a live snapshot of all of
        them to your ``outflow(state)`` function on every export tick,
        and writes whatever ``outflow()`` returns to a single combined
        file.  Think of it as: N rivers (streams) → one fjord (combined
        body) → one exported output.

        Two things ``fjord()`` does that :meth:`stream` does not:

        - **Multi-source orchestration.**  Per-source refresh and export
          cadences are independent — define them on each
          ``stream_params`` entry and the engine schedules them in
          parallel.
        - **Dynamic output class.**  You don't declare the type of the
          combined row; the engine infers it from whatever
          ``outflow(state)`` returns and names it after the outflow
          file's stem in PascalCase (``coin_market.py`` → ``CoinMarket``).
          The dynamic class behaves like any other :class:`Incorporator`
          subclass — it has its own ``inc_dict`` and ``export()``.

        Stateful by design.  For single-source streaming, use :meth:`stream`.

        Args:
            stream_params: List of source-stream config dicts. Each entry
                **must** contain:

                - ``cls`` (Type[Incorporator]): the source Incorporator subclass.
                - ``incorp_params`` (dict): kwargs forwarded to
                  ``cls.incorp()`` for the seed phase.

                Each entry **may** contain:

                - ``refresh_params`` (dict): kwargs for ``cls.refresh()``.
                  Omit to skip refresh for that source.
                - ``export_params`` (dict): kwargs for a per-source export.
                  Omit to skip per-source export.
                - ``refresh_interval`` (float): per-entry override of the
                  top-level ``refresh_interval``.
                - ``export_interval`` (float): per-entry override of the
                  top-level ``export_interval``.
            outflow: Path to a Python file defining the top-level
                ``outflow(state)`` function the engine calls on every
                export tick.  ``state`` is a dict mapping each source
                class's ``__name__`` to its current
                :class:`IncorporatorList` snapshot (with ``inc_dict``
                available for O(1) joins).  ``outflow(state)`` must
                return ``list[dict]`` (or a single ``dict``, auto-wrapped);
                each row becomes one instance of the dynamic output
                class.  Returning ``[]`` yields a zero-row Wave and
                skips the export — useful for gating output on a join
                condition.
            export_params: kwargs forwarded to the dynamic output class's
                ``export()`` for the combined output.  Required — the joined
                output must have a destination.
            inflow: Optional path to a sidecar ``inflow.py`` whose public
                symbols extend the token resolver's allow-list (mostly for
                ``conv_dict`` callables in per-source ``incorp_params``).
                See :func:`incorporator.usercode.load_user_module`.
            refresh_interval: Default sleep between refresh ticks for each
                source daemon.  Per-entry ``refresh_interval`` overrides this.
                ``None`` means "one-shot then exit".
            export_interval: Default sleep between outflow-and-export ticks.
                ``None`` means "one-shot then exit".

        Yields:
            :class:`Wave`: One per phase. The ``operation`` field
            identifies what fired:

            - ``"fjord_incorp:<ClassName>"`` — seed phase, one per source.
            - ``"fjord_refresh:<ClassName>"`` — per-source refresh tick.
            - ``"export:<ClassName>"`` — per-source export tick (when an
              entry has ``export_params``).
            - ``"outflow:<DynamicClassName>"`` — outflow-and-export tick.

        Example:
            ``coin_market.py``::

                from incorporator import Incorporator

                class Coin(Incorporator): pass
                class BinanceFutures(Incorporator): pass

                def outflow(state):
                    rows = []
                    for c in state["Coin"]:
                        f = state["BinanceFutures"].inc_dict.get(c.inc_code)
                        if f:
                            rows.append({
                                "inc_code":      c.inc_code,
                                "coin_name":     getattr(c, "name", ""),
                                "spot_price":    getattr(c, "current_price", 0.0),
                                "futures_price": getattr(f, "price", 0.0),
                                "spread":        getattr(f, "price", 0.0) - getattr(c, "current_price", 0.0),
                            })
                    return rows

            Driver — note no ``CoinMarket`` class to declare::

                from incorporator import Incorporator
                from coin_market import Coin, BinanceFutures

                async for wave in Incorporator.fjord(
                    stream_params=[
                        {"cls": Coin,
                         "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"},
                         "refresh_params": {}},
                        {"cls": BinanceFutures,
                         "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"},
                         "refresh_params": {}},
                    ],
                    outflow="coin_market.py",           # → output class auto-named "CoinMarket"
                    export_params={"file_path": "markets.ndjson"},
                    refresh_interval=60.0,
                    export_interval=300.0,
                ):
                    print(f"{wave.operation}: {wave.rows_processed} rows")
        """
        from .observability.pipeline import _run_fjord_engine

        # Optional inflow= sidecar.  Phase 10 Design A: when the sidecar
        # defines a top-level ``inflow(state)`` callable, fjord switches to
        # sequential seed and calls it before each source's ``incorp()``
        # to obtain per-source ``conv_dict`` overrides.  Without that
        # callable, the sidecar's public names still extend the token
        # resolver's allow-list (legacy behaviour) and fjord keeps the
        # parallel ``asyncio.gather`` seed.
        inflow_callable: Any = None
        if inflow is not None:
            from .usercode import load_inflow_callable

            inflow_callable = load_inflow_callable(inflow)

        # Validate stream_params shape early — fail fast with clear errors.
        if not stream_params:
            raise ValueError("[Incorporator.fjord] requires at least one stream in stream_params.")
        for idx, entry in enumerate(stream_params):
            if "cls" not in entry:
                raise ValueError(
                    f"[Incorporator.fjord] stream_params[{idx}] missing required key 'cls' (Incorporator subclass)."
                )
            if not isinstance(entry["cls"], type) or not issubclass(entry["cls"], Incorporator):
                raise TypeError(
                    f"[Incorporator.fjord] stream_params[{idx}]['cls'] must be an Incorporator subclass, "
                    f"got {type(entry['cls']).__name__!r}."
                )
            if "incorp_params" not in entry:
                raise ValueError(f"[Incorporator.fjord] stream_params[{idx}] missing required key 'incorp_params'.")

        # Derive the output class name from the outflow filename. fjord
        # builds the actual Pydantic class lazily on the first non-empty
        # outflow() tick — see _outflow_daemon in observability/pipeline.py.
        #
        # Phase 10 Design B (multi-output fjord): we need the outflow
        # MODULE too, not just the callable, so the engine can check it
        # for user-pre-declared Incorporator subclasses matching the
        # keys returned by ``outflow(state)``.  ``load_outflow_module``
        # returns both via the per-path module cache, so this costs one
        # file-read regardless of which loader is called first.
        output_class_name = pascal_case_from_stem(outflow)
        outflow_fn, outflow_module = load_outflow_module(outflow)

        async for wave in _run_fjord_engine(
            output_class_name=output_class_name,
            base_class=Incorporator,
            stream_params=stream_params,
            outflow_fn=outflow_fn,
            outflow_module=outflow_module,
            inflow_callable=inflow_callable,
            export_params=export_params,
            r_interval=refresh_interval,
            e_interval=export_interval,
        ):
            yield wave

    @classmethod
    async def test(
        cls: Type[TIncorporator],
        **kwargs: Any,
    ) -> Union[TIncorporator, "IncorporatorList[TIncorporator]", List[Any]]:
        """Explore an unknown endpoint without writing any schema first.

        Swap ``await Class.incorp(...)`` for ``await Class.test(...)`` when
        you don't yet know what shape an API returns.  ``test()`` fetches
        a single safe sample, walks the payload tree, identifies primary-
        key candidates (UUIDs, integer IDs, slugs) and human-readable
        labels, detects type-cast candidates (ISO-8601 timestamps,
        numeric strings), then prints the **exact** ``inc_code``,
        ``inc_name``, ``rec_path``, and ``conv_dict`` you'd hand-write
        for a real ``incorp()`` call.  On a fetch failure it prints
        diagnostics instead of the tree.

        Safety guards baked in:

        - Short timeout (5 s) so unresponsive endpoints fail fast.
        - When a paginator is supplied, only one page is fetched even
          if ``call_lim`` would normally allow more.
        - The returned list is sliced to at most 3 records so a giant
          endpoint doesn't flood your terminal — enough to poke at the
          shape, not enough to be a real ingest.

        Args:
            **kwargs: Same as :meth:`incorp`.  ``timeout`` and ``call_lim``
                get the safe defaults above if you don't override them.

        Returns:
            An :class:`IncorporatorList` of at most 3 records on success,
            or an empty list when the fetch raises.  Either way, the
            inspector's tree-view and suggestions have already been
            printed to stdout by the time this returns.

        Example::

            class User(Incorporator):
                pass

            sample = await User.test(inc_url="https://api.unknown.com/v1/users")
            # → tree + suggested kwargs printed to stdout
            # → `sample` is a 3-record preview for poking at structure
        """
        kwargs["__inspect"] = True

        if "inc_page" in kwargs and not kwargs.get("call_lim"):
            kwargs["call_lim"] = 1

        if "timeout" not in kwargs:
            kwargs["timeout"] = 5.0  # Fail fast!

        try:
            result = await cls.incorp(**kwargs)

        except Exception as e:
            # Defer DX logging to the Inspector module
            from .tools.inspector import analyze_error

            analyze_error(e)
            return IncorporatorList(cls, [])

        if isinstance(result, IncorporatorList):
            sliced = result[:_INSPECTION_LIMIT]
            new_list: IncorporatorList[Any] = IncorporatorList(result._model_class, sliced, result.failed_sources)
            new_list.inc_child_path = result.inc_child_path
            return new_list
        elif isinstance(result, list):
            return result[:_INSPECTION_LIMIT]

        return result
