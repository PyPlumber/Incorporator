"""Core orchestrator and declarative factory for the Incorporator framework.

Acts purely as a coordination layer: no data parsing, network looping, or
schema compilation logic lives here.  Delegates to ``io/``, ``schema/``,
``observability/``, ``tools/``, ``usercode.py``, and ``list.py``, then
assembles the resulting dynamic Pydantic object graphs.
"""

import asyncio
import logging
import os
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
    Literal,
    Mapping,
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
from .schema import JsonSchemaProperty, router
from .schema import factory as _factory
from .usercode import apply_code_transform, apply_inflow_resolution, load_outflow_module, pascal_case_from_stem

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


class Incorporator(BaseModel):
    """Typed dot-notation against any REST / CSV / Parquet / XML source — no schema declaration.

    Subclass once, point a verb at a URL or file path, and you get a
    Pydantic V2 object graph with O(1) registry lookups, weakref-backed
    memory, and converters wired in.  The framework infers the schema
    from the payload, so the DX scanning pdoc never has to hand-write a
    dataclass before exploring an endpoint.  Pair with :meth:`test` for
    discovery, :meth:`export` for persistence, :meth:`stream` for
    overnight chunked drains, and :meth:`fjord` for live multi-source
    fusion.

    Example::

        from incorporator import Incorporator

        class Coin(Incorporator):
            pass

        coins = await Coin.incorp(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={"vs_currency": "usd", "per_page": 10},
            inc_code="id",
        )
        print(coins.inc_dict["bitcoin"].current_price)   # O(1) lookup by id

    Verbs you'll call on a subclass:

    - :meth:`test` — Shady Jimmy probe of an unknown endpoint; prints
      the kwargs you'd hand-write for :meth:`incorp`.
    - :meth:`incorp` — fetch + parse + build the object graph.
    - :meth:`refresh` — one-shot live mark-to-market re-fetch into
      existing instances.
    - :meth:`export` — serialise the graph to JSON, NDJSON, CSV,
      Parquet, SQLite, Avro, Feather, ORC, Excel, XML, …
    - :meth:`stream` — unattended overnight chunked drain of a
      paginated source, flat on RSS.
    - :meth:`fjord` — live stateful daemon that fuses N concurrent
      sources through your ``outflow(state)``.
    - :meth:`display` — REPL identity print for ad-hoc Jupyter
      spot-checks.

    **Kwarg forwarding contract.**  Every verb accepts ``**kwargs:
    Any`` so caller-supplied keys like ``params``, ``headers``,
    ``timeout``, ``rec_path``, ``concurrency_limit``,
    ``requests_per_second``, and handler-specific kwargs
    (``delimiter``, ``sql_table``, ``xml_root``, etc.) flow through
    to ``httpx`` or the format writer unchanged.  The trade-off is
    that **typos pass silently** — ``await Coin.incorp(...,
    tiemout=5)`` will not raise; ``tiemout`` is forwarded to the HTTP
    client which ignores it.  Use ``incorporator validate
    pipeline.json`` (CLI) or load via :class:`StreamConfig` /
    :class:`FjordConfig` (Pydantic) when you want kwarg-key
    validation; both surfaces reject unknown keys before the pipeline
    runs.

    Implementation note: subclassing (not instantiation) is the
    primary user interaction.  Each subclass gets its own dynamically
    generated Pydantic V2 model and its own instance registry, so
    unrelated data sources never share state.  Inherits from
    :class:`pydantic.BaseModel` with ``extra='allow'`` so messy APIs
    never raise ``ValidationError``.  Every instance auto-registers
    into its subclass's :attr:`inc_dict` via :meth:`model_post_init`,
    **and** into every parent class's registry up the MRO (the
    "Bubble-Up" pattern).  Together with ``weakref``-backed storage
    this is what keeps 10M-row ingestion O(1) in memory: entries vanish
    from the registry as soon as the holding list goes out of scope.
    Sibling subclasses keep registries isolated::

        class A(Incorporator): pass
        class B(Incorporator): pass
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
    #: in :mod:`incorporator.schema.factory`.  See
    #: :class:`incorporator.schema.JsonSchemaProperty` for the value shape
    #: — a TypedDict mirroring Pydantic V2's
    #: ``model_json_schema()["properties"]`` entries with all keys optional.
    _schema_union: ClassVar[Dict[str, JsonSchemaProperty]] = {}

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

    #: Network + format-handler call-context preserved from the first
    #: :meth:`incorp` call so :meth:`refresh` can replay the same fetch
    #: (``params``, ``headers``, ``rec_path``, ``conv_dict``, etc.) without
    #: the caller re-declaring anything.  User-supplied ``refresh_params``
    #: still win on key conflict.
    _incorp_kwargs: ClassVar[Dict[str, Any]] = {}

    #: Cross-engine strong-ref snapshot used by the Tideweaver scheduler.
    #: A :class:`~incorporator.observability.tideweaver.scheduler.Scheduler`
    #: parks ``list(cls.inc_dict.values())`` here at the end of a
    #: :class:`~incorporator.observability.tideweaver.current.Stream` tick
    #: (and at the end of a :class:`~incorporator.observability.tideweaver.current.Fjord`
    #: flush, via ``_outflow.flush``) so that downstream currents can read
    #: a stable upstream view between ticks without the
    #: :class:`weakref.WeakValueDictionary` reclaiming entries mid-flight.
    #: Downstream code reads it uniformly via
    #: ``getattr(dep.cls, "_tideweaver_snapshot", None)``.  Documenting it
    #: here as a typed ``ClassVar`` declares the cross-engine contract for
    #: mypy and IDEs; the assignment still happens at runtime on each
    #: subclass.  ``None`` means "no Tideweaver run has touched this class
    #: yet" — distinct from the empty-list case ("upstream ran, produced
    #: zero rows").
    _tideweaver_snapshot: ClassVar[Optional[List[Any]]] = None

    #: Bulk-insert gate set by :func:`~incorporator.schema.factory.build_instances`
    #: and :func:`~incorporator.observability.pipeline._outflow.flush` around their
    #: batch-validate calls so ``model_post_init`` skips per-instance ``inc_dict``
    #: writes during the loop; the caller does a single ``inc_dict.update()`` after
    #: the loop completes.  Class-level so subclass inheritance is intentional --
    #: the bulk caller sets and clears it on the concrete class only.
    _BATCH_INSERT_MODE: ClassVar[bool] = False

    #: Schema-registry cache-hit flag written by
    #: :func:`~incorporator.schema.factory.build_instances` at the
    #: ``SCHEMA_REGISTRY`` lookup.  ``True`` when the registry returned an
    #: existing compiled class; ``False`` when a new class was built.
    #: Initialized to ``True`` so refresh() paths (which supply a
    #: ``target_class`` and bypass schema inference) report a hit — correct
    #: because no registry lookup was needed.
    #:
    #: Yield-point-safe, not thread-safe: written inside ``build_instances``
    #: (no ``await`` between write and read), read at the chunk boundary in
    #: ``observability/pipeline/chunked.py`` after ``build_instances`` returns.
    _last_schema_cache_hit: ClassVar[bool] = True

    #: Byte count of the most recent HTTP response body processed by this
    #: class's fetch path.  **Reserved for future fetch-layer plumbing** —
    #: ``incorporator/observability/pipeline/chunked.py`` already reads this
    #: ClassVar at the chunk boundary into the Wave's ``bytes_processed``
    #: field, so wiring up ``cls._last_bytes_processed = len(response.content)``
    #: at the fetch site in ``io/fetch.py`` is a one-line follow-up that
    #: lights up the field for HTTP sources.  Until that lands, the
    #: corresponding Wave field stays ``None`` (file-mode sources stay
    #: ``None`` permanently — they don't go through the fetch path).
    #:
    #: Yield-point-safe, not thread-safe (same caveat as
    #: :attr:`_last_schema_cache_hit`).
    _last_bytes_processed: ClassVar[Optional[int]] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Per-class isolation of inc_dict / _schema_union / _incorp_kwargs
        # is required so sibling subclasses don't share the base
        # Incorporator's containers.  The fork is deferred to the first
        # write via ``_ensure_X()`` so a subclass that's only ever READ
        # from (or never used at all) doesn't pay the allocation cost.
        # The base's empty defaults cover the read path.
        super().__init_subclass__(**kwargs)

    @classmethod
    def _ensure_inc_dict(cls) -> None:
        """Fork a per-class ``inc_dict`` on first write — preserves sibling isolation.

        Cheap (one dict-membership check) on every call after the first;
        the first call allocates a fresh ``WeakValueDictionary`` directly
        on ``cls.__dict__`` so ``cls.inc_dict`` stops shadowing the
        inherited base default and becomes per-class authoritative.
        """
        if "inc_dict" not in cls.__dict__:
            cls.inc_dict = weakref.WeakValueDictionary()

    @classmethod
    def _ensure_schema_union(cls) -> None:
        """Fork a per-class ``_schema_union`` on first write."""
        if "_schema_union" not in cls.__dict__:
            cls._schema_union = {}

    @classmethod
    def _ensure_incorp_kwargs(cls) -> None:
        """Fork a per-class ``_incorp_kwargs`` on first write."""
        if "_incorp_kwargs" not in cls.__dict__:
            cls._incorp_kwargs = {}

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
        """REPL spot-check while poking at an ``inc_dict`` in a Jupyter cell.

        Reach for ``obj.display()`` when you're exploring a freshly
        fetched object graph and want to confirm identity fields landed
        the way you expected, without expanding a full ``model_dump()``.
        Emits a compact line containing ``class``, ``inc_code``,
        ``inc_name``, and ``last_rcd`` to stdout.  For structured
        machine-readable output use
        :meth:`pydantic.BaseModel.model_dump_json` instead.

        Returns:
            ``None``.
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

        if not cls._BATCH_INSERT_MODE:
            cls._ensure_inc_dict()
            cls.inc_dict[self.inc_code] = self

            # DSA OPTIMIZATION: Fast-path C-tuple evaluation.
            # Only iterate bases if this is a deeply nested dynamic subclass.
            if cls.__bases__ and cls.__bases__[0] is not Incorporator:
                for base in cls.__bases__:
                    if issubclass(base, Incorporator) and base is not Incorporator:
                        base._ensure_inc_dict()
                        base.inc_dict[self.inc_code] = self

    @classmethod
    async def incorp(
        cls: Type[TIncorporator],
        inc_url: Optional[Union[str, List[str]]] = None,
        inc_file: Optional[Union[str, "os.PathLike[str]", List[Union[str, "os.PathLike[str]"]]]] = None,
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
        """The entry-point verb every pipeline starts with — fetch a source into typed objects.

        Reach for ``incorp()`` once you know the URL or file path, the
        identity field (``inc_code``), and roughly what shape the
        response is in.  If any of those are still unknown, run
        :meth:`test` first to print suggested kwargs.  Builds a dynamic
        Pydantic V2 model from the raw payload, coerces every field
        through the ``conv_dict`` converter pipeline, registers each
        instance in ``cls.inc_dict`` for O(1) lookups, and returns
        either a single instance or an :class:`IncorporatorList`.

        Example::

            class Coin(Incorporator):
                pass

            coins = await Coin.incorp(
                "https://api.coingecko.com/api/v3/coins/markets",
                params={"vs_currency": "usd", "per_page": 10},
                inc_code="id",
                inc_name="name",
            )
            print(coins.inc_dict["bitcoin"].current_price)

        Headline kwargs:

        - ``inc_url`` / ``inc_file`` — HTTP source or local path; a
          ``List[str]`` triggers concurrent fan-out.
        - ``inc_code`` / ``inc_name`` — source-field names used as the
          registry key and the human-readable label.
        - ``inc_parent`` / ``inc_child`` — Parent-Child HATEOAS
          enrichment; child URLs are drilled out of parent records via
          dot-notation.
        - ``inc_page`` — :class:`AsyncPaginator` for streaming
          pagination (NextUrl, Cursor, Offset, PageNumber, LinkHeader,
          SQLite, CSV, Avro).
        - ``excl_lst`` — field names to drop before Pydantic compiles
          the schema (strip heavy keys like ``"image_data"``).
        - ``conv_dict`` — ``field_name → converter`` map applied before
          validation (:func:`inc`, :func:`calc`, :func:`link_to`,
          :func:`pluck`, :func:`split_and_get`, :func:`each`,
          :func:`join_all`, :func:`as_list`).
        - ``name_chg`` — list of ``(old_name, new_name)`` renames for
          normalising heterogeneous sources.

        Concurrency: list-typed ``inc_url`` / ``inc_file`` arguments
        fan out via an ``asyncio.gather`` sliding window (size
        ``concurrency_limit``, default 50) with built-in per-source
        rate limiting and exponential-backoff retries.  The first call
        also stashes its identity-mapping kwargs and network context
        on the class so :meth:`refresh` can replay the same fetch
        without re-passing them.

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
            conv_dict, inc_page = apply_inflow_resolution(inflow, conv_dict, inc_page)

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

        if source:
            network._inject_sqlite_query(source, kwargs.get("sql_table") or cls.__name__.lower(), kwargs)

        is_file_mode = bool(inc_file)
        source_list: List[str] = network._normalize_source_list(source, kwargs.get("payload_list"))

        is_single = not isinstance(source, list) and inc_page is None

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
        # conflict (handled in ``refresh()`` below).  ``__inspect`` and
        # ``__capture_into`` are one-shot inspector flags, intentionally
        # not replayed by ``refresh()``.
        cls._incorp_kwargs = {
            "conv_dict": conv_dict,
            "excl_lst": excl_lst,
            "name_chg": name_chg,
            **{k: v for k, v in kwargs.items() if k not in ("__inspect", "__capture_into")},
        }

        # Extract control flags before network call so they don't pollute handlers
        __inspect = kwargs.pop("__inspect", False)
        # ``__capture_into`` is the architect's sidechannel: when set to a
        # mutable list, the inspector's structured ``SourceProfile`` lands in
        # it and the print path is suppressed.  Lets ``cls.architect()``
        # share the same probe codebase as ``cls.test()`` without printing
        # N per-source reports.
        __capture_into = kwargs.pop("__capture_into", None)
        payload_list = kwargs.pop("payload_list", None)

        parsed_data, rejects = await network.fetch_concurrent_payloads(
            source_list=source_list,
            is_file_mode=is_file_mode,
            inc_page=inc_page,
            payload_list=payload_list,
            __inspect=__inspect,
            **kwargs,
        )

        if __inspect:
            from .tools.inspector import analyze_data, capture_signals

            if __capture_into is not None:
                __capture_into.append(capture_signals(parsed_data, {"rec_path": kwargs.get("rec_path")}))
            else:
                analyze_data(parsed_data, {"rec_path": kwargs.get("rec_path")})

        result = await asyncio.to_thread(
            _factory.build_instances,
            cls,
            parsed_data,
            rejects,
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
        """One-shot live mark-to-market re-fetch into the instances you already hold.

        Reach for ``refresh()`` from a REPL, a notebook, or your own
        scheduler when you want to manually pull fresh values for an
        already-loaded class — the classic Binance ticker pattern:
        ``incorp()`` once at startup, ``refresh()`` whenever a strategy
        loop wants the latest price.  References you already hold are
        mutated in-place via Pydantic field updates, so the existing
        list keeps pointing at the refreshed objects without
        reassignment.  For an unattended daemon that keeps doing this
        forever on a cadence, reach for :meth:`fjord` instead — its
        engine is built around stateful concurrent refresh + fusion.

        Example::

            coins = await Coin.incorp(COINGECKO_URL, inc_code="id")
            # ... time passes ...
            await Coin.refresh()                     # in-state — reuses stored URL + params
            print(coins.inc_dict["bitcoin"].current_price)   # latest tick

        Headline kwargs:

        - ``instance`` — resolution mode selector (see closing table).
        - ``new_url`` / ``new_file`` — typed override of the source
          when re-sourcing onto a different endpoint.
        - ``inc_code`` / ``inc_name`` — override the identity-mapping
          fields stored at incorp time.
        - ``conv_dict`` / ``excl_lst`` / ``name_chg`` — override the
          transform pipeline persisted from the original incorp.
        - ``inc_page`` — paginator for streaming refresh.

        Instance resolution has three modes, chosen by ``instance``:

        - **In-state (``instance=None``)** — refresh every object
          currently in ``cls.inc_dict``.  Origin URLs/files come from
          each instance's stored ``inc_url`` / ``inc_file``; the
          network context (params, headers, ``rec_path``, ``conv_dict``)
          persisted by :meth:`incorp` is replayed so endpoints with
          required query params (CoinGecko ``vs_currency=usd``) keep
          working.
        - **Re-source (``instance=str | Path``)** — re-fetch
          ``cls.inc_dict`` from a new URL or local file.  Strings
          starting with ``http`` are URLs; anything else is a path.
        - **Targeted (``instance=List[obj] | obj``)** — refresh only
          the listed instances; useful for partial updates.

        Deduplication: HTTP requests are deduplicated across the
        resolved instance set by origin URL/file, so 1000 instances
        sharing 20 source URLs trigger 20 fetches, not 1000.

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
            A single ``TIncorporator`` instance when the source resolves
            to exactly one record, or an ``IncorporatorList[TIncorporator]``
            for multi-record sources.  Existing Python references are
            mutated in-place via Pydantic field updates, so callers holding
            the old list will see updated values without reassigning.

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
        persisted_net = {k: v for k, v in persisted.items() if k not in ("conv_dict", "excl_lst", "name_chg")}
        kwargs = {**persisted_net, **kwargs}

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
        # ``target_url`` / ``target_file`` are typed ``str | list[str] | None``
        # to accept the multi-URL refresh shape, but the class attr is a
        # single ``Optional[str]`` — narrow on assignment.
        if target_url:
            cls.inc_url = target_url if isinstance(target_url, str) else target_url[0]
        elif target_file:
            cls.inc_file = target_file if isinstance(target_file, str) else target_file[0]

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
            conv_dict, inc_page = apply_inflow_resolution(inflow, conv_dict, inc_page)

        TargetClass = inst_list[0].__class__
        kwargs["http_method"] = kwargs.pop("method", kwargs.pop("http_method", "GET")).upper()

        child_path = inc_child or getattr(inst_list[0], "inc_child_path", None)

        extracted_data = router.extract_parent_data(inst_list, child_path) if child_path else inst_list

        if child_path and extracted_data:
            extracted_data = _deduplicate_extracted(extracted_data)

        target = target_url or target_file
        source_urls = network._normalize_source_list(target, None)

        # Declarative routing only fires when there's something declarative to
        # route — a `new_url` (URL-template / POST-token injection) or a
        # `child_path` (parent → child drill).  Pure in-state refresh (no
        # args, no inc_child) skips this and falls through to the origin-URL
        # fallback below, which reads each instance's stored `inc_url`.
        if not target_file and extracted_data and (target_url or child_path):
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

        parsed_data, rejects = await network.fetch_concurrent_payloads(
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
            rejects,
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
        """Persist an Incorporator graph to disk — backtest snapshot, warehouse stage, hand-off file.

        Reach for ``export()`` whenever you need the in-memory object
        graph on disk: capturing a backtest input set as Parquet,
        staging a CSV for an analyst, dropping NDJSON onto S3, or
        materialising a SQLite warehouse table.  Supported formats:
        JSON, NDJSON, CSV / TSV / PSV, XML, SQLite, Parquet, Feather,
        ORC, Avro, XLSX (some require opt-in extras — see
        :doc:`/docs/formats_and_compression`).  Optional compression
        (``gz``, ``bz2``, ``xz``, ``zip``, ``tar``, ``zstd``, ``lz4``,
        ``snappy``, ``brotli``) is applied in a background thread.

        Example::

            launches = await Launch.incorp(LL2_URL, rec_path="results")
            await Launch.export(instance=launches, file_path="launches.parquet")

        Headline kwargs (all keyword-only, enforced by the bare ``*``):

        - ``instance`` — the data source (or the destination path in
          in-state mode; see the closing aside below).
        - ``file_path`` — destination file path.  Omit for in-state mode.
        - ``format_type`` — override the format inferred from the file
          extension.
        - ``compression`` — post-write compression codec.
        - ``outflow`` — path to a ``transform(instances)`` sidecar that
          rewrites rows before serialisation (great for shaping warehouse
          tables).
        - ``sql_table`` / ``if_exists`` — SQLite destination controls
          (``"replace"`` / ``"append"`` / ``"fail"``).

        Streaming-first: records flow through a lazy generator and
        ``model_dump()`` runs per-row only when the handler asks for
        the next item, so 10M-row exports stay flat on RSS.

        When the first arg is a path vs. data — ``instance`` is
        polymorphic.  With ``file_path`` omitted (in-state mode),
        ``instance`` is read as the **output path** and the data
        source is ``cls.inc_dict.values()``: a tidy one-liner for
        "dump my current state to disk."  With ``file_path`` set,
        ``instance`` must be the data source (a Pydantic ``BaseModel``
        or a ``list`` of them); passing a string raises ``TypeError``.

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
        refresh_params: Optional[Dict[str, Any]] = _UNSET,
        export_params: Optional[Dict[str, Any]] = None,
        poll_interval: Optional[float] = None,
        stateful_polling: bool = False,
        refresh_interval: Optional[float] = None,
        export_interval: Optional[float] = None,
        inflow: Optional[Union[str, Path]] = None,
        outflow: Optional[Union[str, Path]] = None,
    ) -> AsyncGenerator["Wave", None]:
        """Overnight unattended drain of a paginated source — one chunk in memory at a time.

        Reach for ``stream()`` when you have a paginated source big
        enough that loading it all at once would blow RSS — a 10M-row
        warehouse dump, a multi-day archive, an exchange's full
        historical klines.  Each wave fetches the next page, hands you
        a :class:`Wave` you can log or count, and releases the chunk
        before the next page lands, so memory stays O(1) for the whole
        drain.  When the paginator runs out, the generator exits — no
        babysitting required.

        Example::

            from incorporator import NextUrlPaginator

            async for wave in Launch.stream(
                incorp_params={
                    "inc_url": "https://ll.thespacedevs.com/2.2.0/launch/upcoming/",
                    "rec_path": "results",
                    "inc_page": NextUrlPaginator("next"),
                },
                export_params={"file_path": "launches.ndjson"},
            ):
                print(f"chunk {wave.chunk_index}: {wave.rows_processed} rows")

        Headline kwargs:

        - ``incorp_params`` — kwargs forwarded to :meth:`incorp` every
          wave; usually contains ``inc_url`` plus an ``inc_page=``
          paginator.
        - ``export_params`` — kwargs for :meth:`export`.  In chunking
          mode ``if_exists`` is forced to ``"append"`` so chunks
          accumulate into a single output file.
        - ``poll_interval`` — sleep between waves.  ``None`` means
          "one-shot then exit" — useful in tests.
        - ``inflow`` / ``outflow`` — sidecar paths for user-defined
          helpers and (stateful only) a user-defined receiver class.

        Chunking is the default; ``stateful_polling=True`` delegates
        to the :meth:`fjord` engine with a synthesised identity
        outflow for single-source stateful runs.  For new live
        dashboards, reach for :meth:`fjord` directly — it's the
        user-facing daemon framing and scales to N concurrent sources.
        ``refresh_interval`` and ``export_interval`` each fall back to
        ``poll_interval`` when omitted, so a single ``poll_interval=60.0``
        drives both rhythms identically.

        Args:
            incorp_params: kwargs forwarded to :meth:`incorp` every wave.
            refresh_params: kwargs for :meth:`refresh`.  Omit to skip
                refresh entirely.
            export_params: kwargs for :meth:`export`.  Omit to skip
                export entirely.  In chunking mode, ``if_exists`` is
                forced to ``"append"`` so successive chunks accumulate
                into one output file.
            poll_interval: Default sleep between waves.  ``None`` means
                "one-shot then exit" — useful for testing.
            stateful_polling: Mode selector — see the two modes above.
            refresh_interval: Override the refresh wave period in stateful
                mode.  Falls back to ``poll_interval`` when omitted.
            export_interval: Override the export wave period in stateful
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
        from .observability.pipeline._dispatch import assert_engine_supported

        # Front-door format check — reject impossible combos at call-site
        # time.  The only failure here is chunking + paginator + monolithic
        # format, which would silently overwrite the prior chunk's output.
        _file_path_for_check = export_params.get("file_path") if export_params else None
        assert_engine_supported(
            file_path=_file_path_for_check,
            stateful_polling=stateful_polling,
            has_paginator=incorp_params.get("inc_page") is not None,
        )

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
        outflow_user_module: Optional[Any] = None
        if outflow is not None:
            from .usercode import load_user_module, pascal_case_from_stem

            outflow_user_module = load_user_module(outflow, name_hint="_inc_stream_outflow")
            preferred_name = pascal_case_from_stem(outflow)
            candidate = getattr(outflow_user_module, preferred_name, None)
            if not (isinstance(candidate, type) and issubclass(candidate, Incorporator)):
                # Fall back: first Incorporator subclass in the module.
                candidate = next(
                    (
                        getattr(outflow_user_module, n)
                        for n in dir(outflow_user_module)
                        if isinstance(getattr(outflow_user_module, n, None), type)
                        and issubclass(getattr(outflow_user_module, n), Incorporator)
                        and getattr(outflow_user_module, n) is not Incorporator
                    ),
                    None,
                )
            if candidate is None:
                raise ValueError(
                    f"[Incorporator.stream] outflow={outflow!r} must define an "
                    f"Incorporator subclass (preferred name {preferred_name!r})."
                )
            receiver_cls = candidate

        # inflow= is a CLI/JSON convenience plus, for the stateful path, an
        # optional state-aware seed hook.  ``load_inflow_callable`` loads the
        # sidecar (so its public symbols extend the token resolver's allow-list
        # via sys.modules caching) AND returns the top-level ``inflow(state)``
        # callable when one is defined.  That callable is meaningful only on
        # the stateful path — chunking mode ignores it (no state to inject).
        inflow_callable: Optional[Any] = None
        if inflow is not None:
            from .usercode import load_inflow_callable

            inflow_callable = load_inflow_callable(inflow)

        # Translate the _UNSET sentinel to "{}" (run refresh with defaults).
        # Callers that explicitly want to skip refresh pass refresh_params=None,
        # which propagates through unchanged.  This makes the common case
        # ("just refresh the same source") the default rather than requiring
        # boilerplate refresh_params={} on every stream() call.
        if refresh_params is _UNSET:
            refresh_params = {}

        if stateful_polling:
            # ----------------------------------------------------------
            # Stateful path: delegate to the fjord engine with a
            # synthesised identity outflow.  This collapses what used to
            # be a separate _run_stateful_engine into a one-source fjord
            # pipeline.  Python-object identity in ``cls.inc_dict`` is
            # preserved across waves by the IncorporatorList pass-through
            # fast path in ``_outflow.flush()``.  See
            # ``observability/pipeline/_stateful_shim.py``.
            # ----------------------------------------------------------
            from .observability.pipeline._stateful_shim import stream_stateful_via_fjord

            async for wave in stream_stateful_via_fjord(
                receiver_cls=receiver_cls,
                base_class=Incorporator,
                incorp_params=incorp_params,
                refresh_params=refresh_params,
                export_params=export_params,
                poll_interval=poll_interval,
                refresh_interval=refresh_interval,
                export_interval=export_interval,
                outflow_user_module=outflow_user_module,
                inflow_callable=inflow_callable,
            ):
                yield wave
            return

        # Stateless chunking — straight delegation, no engine branch.
        from .observability.pipeline import run_pipeline

        async for wave in run_pipeline(
            cls=receiver_cls,
            incorp_params=incorp_params,
            refresh_params=refresh_params,
            export_params=export_params,
            poll_interval=poll_interval,
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
        """The stateful live-daemon verb — concurrent source refresh fused through your ``outflow``.

        Reach for ``fjord()`` when you need an unattended daemon that
        keeps N sources live and joined: a live mark-to-market
        dashboard combining CoinGecko USD with Binance USDT,
        Sunday-afternoon fantasy NASCAR fusion of leaderboard +
        lineup state, cross-exchange arbitrage spreads on a 30-second
        cadence.  Each source refreshes on its own schedule; on every
        export tick the engine hands a snapshot of all of them to your
        ``outflow(state)`` function and writes the returned rows to a
        single combined output.  N=1 is legitimate too — call
        ``fjord()`` on one source when you want the daemon shape
        without writing a custom loop around :meth:`refresh`.

        Example — ``coin_market.py`` defines two source classes plus
        the outflow::

            from incorporator import Incorporator

            class Coin(Incorporator): pass
            class BinanceFutures(Incorporator): pass

            def outflow(state):
                return [
                    {"inc_code": c.inc_code,
                     "spot": getattr(c, "current_price", 0.0),
                     "futures": getattr(state["BinanceFutures"].inc_dict.get(c.inc_code), "price", 0.0)}
                    for c in state["Coin"]
                ]

        Driver — no ``CoinMarket`` class to declare; the engine infers it::

            async for wave in Incorporator.fjord(
                stream_params=[
                    {"cls": Coin, "incorp_params": {"inc_url": COINGECKO_URL, "inc_code": "id"},
                     "refresh_params": {}},
                    {"cls": BinanceFutures, "incorp_params": {"inc_url": BINANCE_URL, "inc_code": "symbol"},
                     "refresh_params": {}},
                ],
                outflow="coin_market.py",
                export_params={"file_path": "markets.ndjson"},
                refresh_interval=60.0,
                export_interval=300.0,
            ):
                print(f"{wave.operation}: {wave.rows_processed} rows")

        Headline kwargs:

        - ``stream_params`` — list of per-source config dicts; each
          carries ``cls`` (Incorporator subclass) + ``incorp_params``
          (seed kwargs), optionally ``refresh_params``, per-source
          ``export_params``, and per-entry ``refresh_interval`` /
          ``export_interval`` overrides.
        - ``outflow`` — path to a Python file defining
          ``outflow(state) -> list[dict]``; the engine infers the
          dynamic output class and names it after the file stem in
          PascalCase (``coin_market.py`` → ``CoinMarket``).
        - ``export_params`` — kwargs forwarded to the dynamic class's
          :meth:`export` for the combined output (required).
        - ``refresh_interval`` / ``export_interval`` — default
          cadences for the per-source refresh daemons and the
          outflow-and-export wave.  ``None`` means "one-shot then exit".
        - ``inflow`` — optional sidecar whose public symbols extend the
          token resolver's allow-list for ``conv_dict`` callables.

        The dynamic output class behaves like any other
        :class:`Incorporator` subclass — it has its own ``inc_dict``
        and its own :meth:`export`.  ``outflow(state)`` returning ``[]``
        yields a zero-row Wave and skips the export, which is useful
        for gating output on a join condition.  For chunked drains of
        a single paginated source, use :meth:`stream` instead.

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
                export wave.  ``state`` is a dict mapping each source
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
            refresh_interval: Default sleep between refresh waves for each
                source daemon.  Per-entry ``refresh_interval`` overrides this.
                ``None`` means "one-shot then exit".
            export_interval: Default sleep between outflow-and-export waves.
                ``None`` means "one-shot then exit".

        Yields:
            :class:`Wave`: One per phase. The ``operation`` field
            identifies what fired:

            - ``"fjord_incorp:<ClassName>"`` — seed phase, one per source.
            - ``"fjord_refresh:<ClassName>"`` — per-source refresh wave.
            - ``"export:<ClassName>"`` — per-source export wave (when an
              entry has ``export_params``).
            - ``"outflow:<DynamicClassName>"`` — outflow-and-export wave.

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

        # When inflow= defines a top-level ``inflow(state)`` callable, fjord
        # switches to sequential seed and calls it before each source's
        # ``incorp()`` to obtain per-source ``conv_dict`` overrides.  Without
        # that callable, the sidecar's public names still extend the token
        # resolver's allow-list and fjord keeps the parallel asyncio.gather seed.
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

        # Derive the output class name from the outflow filename. fjord builds
        # the actual Pydantic class lazily on the first non-empty outflow() tick.
        # ``load_outflow_module`` returns the callable AND the module so the
        # engine can check for user-pre-declared Incorporator subclasses matching
        # the keys returned by ``outflow(state)`` (multi-output fjord).
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
        """The 30-second Shady Jimmy probe — point it at an unknown URL and read the suggestions.

        Reach for ``test()`` when you have a URL, no docs, and no idea
        what shape the response is in.  Swap ``await Class.incorp(...)``
        for ``await Class.test(...)`` on the same kwargs you'd guess,
        and the framework fetches a single safe sample, walks the
        payload tree, identifies primary-key candidates (UUIDs, integer
        IDs, slugs) and human-readable labels, detects type-cast
        candidates (ISO-8601 timestamps, numeric strings), and prints
        the **exact** ``inc_code``, ``inc_name``, ``rec_path``, and
        ``conv_dict`` you'd hand-write for a real :meth:`incorp` call.
        Paste the suggestions into your code and you're done.

        Example::

            class User(Incorporator):
                pass

            sample = await User.test(inc_url="https://api.unknown.com/v1/users")
            # → tree + suggested kwargs printed to stdout
            # → `sample` is a 3-record preview for poking at structure

        Headline kwargs: anything :meth:`incorp` accepts is forwarded
        unchanged.  The two values ``test()`` rewrites for safety are
        ``timeout`` (defaulted to 5 s) and ``call_lim`` (capped at 1
        when a paginator is set).

        Safety guards baked in so an exploratory probe can never run
        away:

        - 5-second HTTP timeout so unresponsive endpoints fail fast.
        - When a paginator is supplied, only one page is fetched even
          if ``call_lim`` would normally allow more.
        - The returned list is sliced to at most 3 records — enough to
          poke at shape in the REPL, not enough to flood a terminal or
          rack up rate-limit hits.

        Args:
            **kwargs: Same as :meth:`incorp`.  ``timeout`` and ``call_lim``
                get the safe defaults above if you don't override them.

        Returns:
            An :class:`IncorporatorList` of at most 3 records on success,
            or an empty list when the fetch raises.  Either way, the
            inspector's tree-view and suggestions have already been
            printed to stdout by the time this returns.

        .. note::
            The returned list is a **view** into the live class
            registry, not a snapshot.  ``test()`` populates
            ``cls.inc_dict`` like :meth:`incorp` does and the returned
            ``IncorporatorList`` shares it.  A subsequent
            :meth:`refresh` call mutates the instances in this list in
            place (the same way ``refresh()`` mutates instances held
            anywhere else).  This is the framework's identity
            contract; ``test()`` doesn't deep-copy.  If you need an
            immutable snapshot of the probe sample, ``model_copy`` /
            ``model_dump`` each record before refreshing.
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

    @classmethod
    async def architect(
        cls: Type[TIncorporator],
        sources: Mapping[str, Union[str, Path, Mapping[str, Any]]],
        *,
        output: Literal["report", "python", "json", "plan"] = "report",
        shared_kwargs: Optional[Mapping[str, Any]] = None,
    ) -> Optional[Any]:
        """Probe N sources, recommend a Watershed architecture (paste-ready).

        The multi-source counterpart of :meth:`test`.  Where ``test()`` profiles
        one unknown endpoint and prints the recommended ``incorp()`` kwargs,
        ``architect()`` profiles many and emits a full Tideweaver scaffold:
        which :class:`~incorporator.observability.tideweaver.Watershed` shape
        to pick, per-source verb (Stream / Fjord / Export), and per-edge
        :class:`~incorporator.observability.tideweaver.FlowControl` with a
        host-aware Penstock when the rate registry recognises the source.

        Example::

            class Coin(Incorporator):
                pass

            await Coin.architect(
                sources={
                    "binance":  "examples/11-tideweaver/fixtures/binance_book.json",
                    "coinbase": "examples/11-tideweaver/fixtures/coinbase_ticker.json",
                    "kraken":   "examples/11-tideweaver/fixtures/kraken_ticker.json",
                },
                output="json",
            )
            # → prints + returns a complete watershed.json that loads via
            #   `incorporator tideweaver run watershed.json`.

        Args:
            sources: Mapping of ``name -> URL | Path | dict``.

                * URL string (``http://`` / ``https://``) → fetched as ``inc_url=``.
                * Path / file string → loaded as ``inc_file=``.
                * Dict → spread verbatim as ``incorp()`` kwargs.  Use this
                  form to nominate a tail Fjord (``{"verb": "fjord", ...}``).
            output: ``"report"`` prints inspector output + cross-source hints;
                ``"python"`` emits a paste-ready Python module; ``"json"``
                emits a paste-ready ``watershed.json`` body.  Default: ``"report"``.
            shared_kwargs: Common ``incorp()`` kwargs applied to every probe
                (``timeout``, ``headers``, ...).  Per-source kwargs win on
                conflict.

        Returns:
            ``None`` for ``output="report"`` (prints only).  The rendered
            scaffold string for ``"python"`` and ``"json"`` (also printed).

        See :mod:`incorporator.observability.tideweaver.architect` for the
        full implementation — this classmethod is a thin shim that delegates
        to ``architect.run()``.
        """
        from .observability.tideweaver.architect import run

        return await run(cls, sources, output=output, shared_kwargs=shared_kwargs)
