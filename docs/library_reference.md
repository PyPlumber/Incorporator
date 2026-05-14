# 📖 Library Reference

> **Why not "API Reference"?** Incorporator is a client-side framework
> that *consumes* HTTP APIs — so "API Reference" was semantically
> ambiguous. This page documents Incorporator's **own classes, methods,
> and converters** (its library surface), not the external APIs you can
> point it at.

The canonical reference for Incorporator is **auto-generated from the
Google-style docstrings** that live alongside the source code. Because
the docstrings are the single source of truth, this reference never
drifts from the implementation.

We use [**pdoc**](https://pdoc.dev) to render those docstrings as a
searchable HTML site.

---

## Build & view the reference locally

```bash
# 1. Install the docs extra (one time)
pip install "incorporator[docs]"

# 2. Live-reload dev server while editing docstrings
pdoc incorporator
#    → http://localhost:8080

# 3. Static build (writes ./site/ — git-ignored)
python scripts/build_docs.py
```

Open `site/incorporator.html` in any browser, or publish the folder to
GitHub Pages, Netlify, or S3 to host it.

---

## What's in there

Every public class, method, function, and converter is documented with
its full signature, parameter table, return value, raised exceptions,
and worked examples. The library surface, by verb:

### Core verbs on `Incorporator`
- `incorp()` — fetch + parse + build the object graph. Accepts an
  optional `inflow=` sidecar (path to a `.py` with user-defined helper
  functions referenced from `conv_dict` string tokens).
- `test()` — JIT API profiler; prints recommended `incorp()` kwargs for
  an unknown endpoint.
- `refresh()` — stateful update of an existing object graph. Same
  `inflow=` semantics as `incorp()`.
- `export()` — serialise to CSV, JSON, NDJSON, XML, SQLite, Parquet,
  Feather, ORC, Excel, or Avro. Accepts an optional `outflow=` path to
  a `.py` defining a `transform(instances)` hook.
- `stream()` — long-running single-source pipeline (chunking or
  stateful polling). Accepts `inflow=` (any mode) and `outflow=`
  (**stateful-polling only** — chunking has no persistent registry).
- `fjord()` — long-running multi-source pipeline with a user-defined
  `outflow(state)` function in `outflow.py`. Accepts `inflow=` for
  per-source converter helpers.
- `display()` — REPL identity print (debug helper).

### Submodules
- `incorporator.schema.converters` — `inc()`, `calc()`, `each()`,
  `as_list()` — declarative tokens for `conv_dict`
- `incorporator.schema.extractors` — `link_to()`, `link_to_list()`,
  `split_and_get()`, `join_all()` — relational / multi-API joining
- `incorporator.io.pagination` — `CursorPaginator`,
  `NextUrlPaginator`, `OffsetPaginator`, `PageNumberPaginator`,
  `LinkHeaderPaginator`, plus local-file `SQLitePaginator`,
  `CSVPaginator`, `AvroPaginator`
- `incorporator.cli` — the `incorporator init / validate / stream /
  fjord` Typer subcommands
- `incorporator.observability` — `LoggedIncorporator`, `Wave`,
  `LoggingMixin`, plus the pipeline engines
- `incorporator.io.formats` — `FormatType` enum + extension inference
- `incorporator.io.compression` — `CompressionType` enum + auto-extract

---

## Prefer narrative docs?

The rest of `/docs/` covers the **how** and **why** with hands-on
walkthroughs — start with the [Quick Setup tutorial](./1_quick_setup.md).
The pdoc reference covers the **what** for every parameter and return
type.
