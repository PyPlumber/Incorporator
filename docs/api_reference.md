# 📖 API Reference

The canonical API reference for Incorporator is **auto-generated from the
Google-style docstrings** that live alongside the source code. Because the
docstrings are the single source of truth, the reference never drifts from
the implementation.

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
its full signature, parameter table, return value, raised exceptions, and
worked examples. Highlights:

### The "Holy Trinity"
- `Incorporator.incorp()` — extract + transform any URL, file, or archive
- `Incorporator.refresh()` — stateful update of an existing object graph
- `Incorporator.export()` — serialize to CSV, JSON, XML, SQLite, Avro,
  Parquet, Feather, ORC, Excel, or HTML

### Pipeline Orchestration
- `Incorporator.stream()` — single-source autonomous pipeline (chunking or
  stateful polling)
- `Incorporator.fjord()` — multi-source stateful pipeline with a
  user-defined `combine()` function

### Discovery & DX
- `Incorporator.test()` — JIT API profiler that prints recommended
  `incorp()` kwargs

### Submodules
- `incorporator.methods.converters` — `inc()`, `calc()`, `link_to()`,
  `join_all()`, `each()`, `as_list()`
- `incorporator.methods.paginate` — `CursorPaginator`,
  `NextUrlPaginator`, `OffsetPaginator`, `PageNumberPaginator`,
  `LinkHeaderPaginator`, plus local `SQLitePaginator`, `CSVPaginator`,
  `AvroPaginator`
- `incorporator.cli` — the `incorporator stream` and `incorporator fjord`
  Typer subcommands
- `incorporator.observability` — `LoggedIncorporator`, `AuditResult`, the
  pipeline engine

---

## Prefer narrative docs?

The rest of `/docs/` covers the **how** and **why** with hands-on
walkthroughs — start with the [Quick Setup tutorial](./1_quick_setup.md).
The pdoc reference covers the **what** for every parameter and return
type.
