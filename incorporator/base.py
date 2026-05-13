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
from .usercode import apply_code_transform, load_outflow_function, pascal_case_from_stem

if TYPE_CHECKING:
    from .observability.logger import AuditResult

# Type variable for strict IDE hinting on subclass generation
TIncorporator = TypeVar("TIncorporator", bound="Incorporator")
logger = logging.getLogger(__name__)

_INSPECTION_LIMIT = 3
_counter_lock = threading.Lock()


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

    ``Incorporator`` is the public surface of the entire framework.  Subclassing
    (not instantiation) is the primary user interaction: every subclass
    automatically gets its own dynamically generated Pydantic V2 model **and**
    its own ``WeakValueDictionary`` instance registry, so unrelated data sources
    never share state.

    The "Holy Trinity" API is three ``@classmethod`` factories:

    - :meth:`incorp` — Extract & Transform: fetch unknown JSON/XML/CSV/SQLite,
      coerce types, and return dot-notation Python objects.
    - :meth:`refresh` — Stateful Update: re-fetch live data into existing
      instances, deduplicated via origin URL/file.
    - :meth:`export` — Load: serialise the in-memory object graph out to CSV,
      JSON, XML, SQLite, Avro, NDJSON, etc.

    Design contract:

    - Inherits from ``pydantic.BaseModel`` with ``extra='allow'`` so unexpected
      fields from messy APIs never raise ``ValidationError``.
    - Every instance auto-registers into its subclass's ``inc_dict`` via
      :meth:`model_post_init`, plus every parent class's registry up the MRO
      (the "Bubble-Up" pattern — see that method's docstring).
    - The ``inc_dict`` is a ``weakref.WeakValueDictionary``; once the user's
      list of objects goes out of scope, the entries are garbage-collected.
      This makes 10M-row ingestion safe by default.

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
    # PUBLIC "HOLY TRINITY" API
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

        ``refresh()`` is the Stateful Update verb of the Holy Trinity.  It is
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

        if not target_file and extracted_data:
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
        code_file: Optional[Union[str, Path]] = None,  # legacy alias for outflow
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
                file extension.  See :class:`FormatType` for supported
                values (``JSON``, ``NDJSON``, ``CSV``, ``TSV``, ``PSV``,
                ``XML``, ``SQLITE``, ``AVRO``).
            compression: Optional compression to apply **after** writing
                (e.g. ``"gz"``, ``"bz2"``, ``"xz"``, ``"zip"``, ``"tar"``,
                ``"zstd"``, ``"lz4"``, ``"snappy"``, ``"brotli"``).  Runs
                in a background thread via ``asyncio.to_thread``.
            sql_table: Table name for SQLite exports. Defaults to
                ``cls.__name__.lower()``.  Sanitised against SQL injection.
            if_exists: How to handle an existing table/file:
                ``"replace"`` (default), ``"append"``, or ``"fail"``.
            code_file: Path to a Python file defining a top-level
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
            ValueError: When ``code_file`` is provided but its
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

            With code_file transform::

                # transform.py
                def transform(instances):
                    return [
                        {"id": u.id, "name": u.name.upper()}
                        for u in instances
                    ]

                await User.export(
                    instance=users,
                    file_path="upper.csv",
                    code_file="transform.py",
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

        # Resolve outflow/code_file alias.  outflow= is the canonical name;
        # code_file= remains as a deprecated alias.
        if outflow is None and code_file is not None:
            import warnings as _warnings

            _warnings.warn(
                "[Incorporator.export] code_file= is a deprecated alias for outflow=. "
                "Pass outflow= directly; code_file= will be removed in a future major.",
                DeprecationWarning,
                stacklevel=2,
            )
            outflow = code_file

        # Optional code transform — runs in a thread (user code may be CPU-bound).
        transform_source: Iterable[Any] = instances
        code_file_field_names: Optional[List[str]] = None
        if outflow is not None:
            transform_source = await asyncio.to_thread(apply_code_transform, instances, outflow)
            # Peek at the first transformed row so we can rebuild all_field_names from the
            # *actual* output schema — code_file may add, remove, or rename fields.
            import itertools as _itertools

            _transform_iter = iter(transform_source)
            _first_row = next(_transform_iter, None)
            if _first_row is not None:
                if isinstance(_first_row, dict):
                    code_file_field_names = list(_first_row.keys())
                elif hasattr(_first_row, "model_dump"):
                    code_file_field_names = list(_first_row.model_dump(by_alias=True, mode="json").keys())
                transform_source = _itertools.chain([_first_row], _transform_iter)

        active_format = format_type or infer_format(actual_path)
        kwargs.update(
            {
                "sql_table": sql_table or (cls.__name__.lower() if active_format == FormatType.SQLITE else None),
                "if_exists": if_exists,
                "pydantic_schema": {"properties": cls._schema_union},
                "all_field_names": code_file_field_names or list(cls._schema_union.keys()) or None,
            }
        )

        # Lazy serialization generator: model_dump() is called per-object only when
        # the handler requests the next row — no full-list copy in RAM.
        # Handles both Incorporator objects (model_dump) and raw dicts from code_file transforms.
        def _make_lazy_iter(source: Iterable[Any]) -> Iterable[Dict[str, Any]]:
            for obj in source:
                if isinstance(obj, Incorporator):
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
        refresh_params: Optional[Dict[str, Any]] = None,
        export_params: Optional[Dict[str, Any]] = None,
        poll_interval: Optional[float] = None,
        stateful_polling: bool = False,
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
        inflow: Optional[Union[str, Path]] = None,
        outflow: Optional[Union[str, Path]] = None,
    ) -> AsyncGenerator["AuditResult", None]:
        """Yield :class:`AuditResult` objects from a long-running pipeline.

        ``stream()`` is the autonomous pipeline verb — an async generator that
        keeps fetching, transforming, and (optionally) exporting until exhausted
        or cancelled.  It routes to one of two execution engines:

        - **Chunking engine** (``stateful_polling=False``, default): Sequential
          chunked ingestion.  Each iteration calls ``incorp(**incorp_params)``,
          then ``refresh()`` and/or ``export()`` per the configured params,
          then yields one ``AuditResult``.  Memory stays O(1) — each chunk is
          released and ``gc.collect()`` runs before the next.  Best for
          large paginated sources where you want a steady throughput trace.

        - **Stateful-polling engine** (``stateful_polling=True``): Runs
          ``incorp()`` once to seed the dataset, then spawns independent
          ``_refresh_daemon`` and ``_export_daemon`` asyncio tasks on
          decoupled schedules.  Daemons coordinate via an internal
          ``asyncio.Lock`` so refresh mutations are atomic and export
          snapshots are consistent.  Best for keeping a live in-memory
          graph synchronised at one rate while exporting at another.

        Interval cascade: ``refresh_interval`` and ``export_interval``
        each fall back to ``poll_interval`` when not specified, so a
        single ``poll_interval=60.0`` schedules both daemons identically.

        Args:
            incorp_params: kwargs forwarded to :meth:`incorp` on every
                ingestion cycle.
            refresh_params: kwargs for :meth:`refresh`.  When ``None``,
                no refresh daemon is spawned (chunking) or scheduled
                (stateful).
            export_params: kwargs for :meth:`export`.  When ``None``,
                no export daemon is spawned (chunking) or scheduled
                (stateful).  ``if_exists`` is overridden to ``"append"``
                in the chunking engine so successive chunks accumulate.
            poll_interval: Default sleep between full cycles (chunking)
                or default daemon period (stateful).  ``None`` means
                "one shot then exit".
            stateful_polling: Engine selector. ``False`` → chunking;
                ``True`` → independent daemon tasks.
            refresh_interval: Stateful-polling override for the refresh
                daemon period. Falls back to ``poll_interval``.
            export_interval: Stateful-polling override for the export
                daemon period. Falls back to ``poll_interval``.

        Yields:
            :class:`AuditResult`: One per chunk (chunking) or per daemon
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

                async for audit in User.stream(
                    incorp_params={
                        "inc_url": "https://api.example.com/users",
                        "inc_page": NextUrlPaginator("next"),
                    },
                    export_params={"file_path": "users.ndjson"},
                ):
                    print(f"Chunk {audit.chunk_index}: {audit.rows_processed} rows")

            Stateful polling — refresh every 5 min, export every 30 s::

                async for audit in User.stream(
                    incorp_params={"inc_url": "https://api.example.com/users"},
                    refresh_params={},
                    export_params={"file_path": "snapshot.json"},
                    stateful_polling=True,
                    refresh_interval=300.0,
                    export_interval=30.0,
                ):
                    handle(audit)
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

        async for audit in run_pipeline(
            cls=receiver_cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
            stateful_polling=stateful_polling,
            refresh_interval=refresh_interval,
            export_interval=export_interval,
        ):
            yield audit

    @classmethod
    async def fjord(
        cls,
        stream_params: List[Dict[str, Any]],
        outflow: Optional[Union[str, Path]] = None,
        export_params: Optional[Dict[str, Any]] = None,
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
        inflow: Optional[Union[str, Path]] = None,
        code_file: Optional[Union[str, Path]] = None,  # legacy alias for outflow
    ) -> AsyncGenerator["AuditResult", None]:
        """Multi-source stateful streaming with a dynamically-built output class.

        ``fjord()`` is the multi-source analogue of :meth:`stream` — it ingests
        from N independent Incorporator subclasses concurrently, exposes their
        live ``inc_dict`` registries to a developer-provided ``outflow(state)``
        function, and feeds whatever ``outflow()`` returns into the same
        dynamic-schema-inference + export pipeline ``incorp()`` already uses.
        **No user-defined output class is required** — the class is built
        automatically and named after the ``code_file``'s filename
        (snake_case → PascalCase; e.g. ``coin_market.py`` →
        ``CoinMarket``).

        Think of it as: N rivers (streams) → one fjord (combined body) → one
        exported output.

        Stateful polling only.  For single-source streaming or chunked
        sequential ingestion, use :meth:`stream`.

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
            outflow: Path to a Python file defining a top-level
                ``outflow(state)`` function and (optionally) the
                ``Incorporator`` subclasses referenced by ``stream_params``
                ``cls_name`` entries.  ``state`` is a dict mapping each
                source class's ``__name__`` to its current ``IncorporatorList``
                snapshot.  ``outflow(state)`` must return ``list[dict]``
                (or a single ``dict``, auto-wrapped); each row is used to
                construct one instance of the dynamic output class.
            export_params: kwargs forwarded to the dynamic output class's
                ``export()`` for the combined output.  Required — the joined
                output must have a destination.
            inflow: Optional path to a sidecar ``inflow.py`` whose public
                symbols extend the token resolver's allow-list (mostly for
                ``conv_dict`` callables in per-source ``incorp_params``).
                See :func:`incorporator.usercode.load_user_module`.
            code_file: **Deprecated alias for ``outflow``.**  Emits a
                ``DeprecationWarning`` when supplied; will be removed in a
                future major release.
            refresh_interval: Default sleep between refresh ticks for each
                source daemon.  Per-entry ``refresh_interval`` overrides this.
                ``None`` means "one-shot then exit".
            export_interval: Default sleep between outflow-and-export ticks.
                ``None`` means "one-shot then exit".

        Yields:
            :class:`AuditResult`: One per phase. The ``operation`` field
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

                async for audit in Incorporator.fjord(
                    stream_params=[
                        {"cls": Coin,
                         "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"},
                         "refresh_params": {}},
                        {"cls": BinanceFutures,
                         "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"},
                         "refresh_params": {}},
                    ],
                    code_file="coin_market.py",          # → output class auto-named "CoinMarket"
                    export_params={"file_path": "markets.ndjson"},
                    refresh_interval=60.0,
                    export_interval=300.0,
                ):
                    print(f"{audit.operation}: {audit.rows_processed} rows")
        """
        import warnings

        from .observability.pipeline import _run_fjord_engine

        # Resolve outflow/code_file alias — code_file is the legacy name.
        if outflow is None and code_file is not None:
            warnings.warn(
                "[Incorporator.fjord] code_file= is a deprecated alias for outflow=. "
                "Pass outflow= directly; code_file= will be removed in a future major.",
                DeprecationWarning,
                stacklevel=2,
            )
            outflow = code_file
        if outflow is None:
            raise ValueError("[Incorporator.fjord] outflow= (path to outflow.py) is required.")
        if export_params is None:
            raise ValueError("[Incorporator.fjord] export_params= is required.")

        # Optional inflow= sidecar — load once so its public names are in
        # sys.modules for any string-form tokens already resolved by the CLI
        # before this method ran.  Python users who pre-resolve their kwargs
        # don't need it; the load is a no-op cost via importlib caching.
        if inflow is not None:
            from .usercode import load_user_module

            load_user_module(inflow, name_hint="_inc_fjord_inflow")

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
        output_class_name = pascal_case_from_stem(outflow)
        outflow_fn = load_outflow_function(outflow)

        async for audit in _run_fjord_engine(
            output_class_name=output_class_name,
            base_class=Incorporator,
            stream_params=stream_params,
            outflow_fn=outflow_fn,
            export_params=export_params,
            r_interval=refresh_interval,
            e_interval=export_interval,
        ):
            yield audit

    @classmethod
    async def test(
        cls: Type[TIncorporator],
        **kwargs: Any,
    ) -> Union[TIncorporator, "IncorporatorList[TIncorporator]", List[Any]]:
        """JIT API Profiler — explore an unknown endpoint without writing a schema.

        ``test()`` is a Developer Experience helper that wraps :meth:`incorp`
        with ``__inspect=True`` to trigger the :mod:`incorporator.tools.inspector`
        tree analyser.  On a successful fetch it prints a deep tree-view of
        the payload structure, detects identity-shaped fields (UUIDs,
        timestamps, etc.), and emits the exact ``inc_code``, ``inc_name``,
        ``rec_path``, and ``conv_dict`` you'd plug into a real ``incorp()``
        call.  On a failed fetch it routes the exception through
        :func:`inspector.analyze_error` for actionable diagnostics.

        Differences from :meth:`incorp` for safety:

        - Default ``timeout=5.0`` (fail fast on unresponsive endpoints).
        - When a paginator is supplied, ``call_lim`` is forced to ``1`` so
          you only fetch one page during exploration.
        - The return value is sliced to at most ``_INSPECTION_LIMIT`` (3)
          records to prevent terminal flooding.

        Args:
            **kwargs: Same as :meth:`incorp`. ``timeout`` and ``call_lim``
                get safe defaults if not provided.

        Returns:
            An :class:`IncorporatorList` of at most 3 records, or an empty
            list on exception.

        Example::

            class User(Incorporator):
                pass

            # No idea what the API looks like — let test() figure it out:
            sample = await User.test(inc_url="https://api.unknown.com/v1/users")
            # Tree + suggested kwargs are printed to stdout.
            # `sample` is a 3-record preview for inspection.
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
