# Contributing to Incorporator

Thanks for your interest in improving Incorporator. This document covers
the workflow, conventions, and quality bar.

## TL;DR

```bash
# 1. Fork + clone
git clone https://github.com/<your-fork>/incorporator
cd incorporator

# 2. Install in editable mode with dev + speedups extras
pip install -e ".[dev,speedups]"

# 3. Before opening a PR — these must all pass
pytest --no-cov -q
mypy incorporator/
ruff check incorporator/ tests/
black --check incorporator/ tests/
```

If you're adding format support, also run the benchmark suite:

```bash
pytest -m benchmark
```

## Quality Bar

Every PR is expected to land with:

- **All 521+ tests green** (`pytest --no-cov -q`).
- **`mypy --strict` clean** on the source tree (47 files, no errors).
- **`ruff check` clean** on source + tests.
- **`black --check` clean** on source + tests (line length 120).
- **New tests** covering any new public method, handler, or CLI flag.
- **No regressions** to the benchmark floors (run `pytest -m benchmark`
  if your change touches the parse / write hot paths).

The CI bar is intentionally narrow — these four checks. We don't gate on
coverage percentages because the existing suite already exceeds 85% and
adding test count for its own sake is not the goal.

## Branch & Commit Conventions

- **Branches**: short, kebab-case, prefixed with the change type:
  `feat/`, `fix/`, `perf/`, `docs/`, `refactor/`, `test/`, `build/`.
- **Commit messages**: [Conventional Commits](https://www.conventionalcommits.org/)
  format. Look at `git log --oneline -30` for the house style — short
  imperative subject line, then a body explaining *why* (not just *what*).

Examples from recent history:

```
perf: HTTP/2, LRU schema cache, async outflow, ETL loop inversion
docs(readme): verb-forward rewrite, CLI/Docker as the production bridge
test(bench): add throughput benchmarks for CSV/TSV/PSV, XML, HTML, Avro, XLSX
fix(compression): compatibility with cramjam ≥2.x API changes
```

## Architecture Conventions

Incorporator is a **client-side framework / ETL library** — *not* a service
or REST API. Keep that lens when picking names and surfacing concepts.

- **Verb-forward.** Public methods on `Incorporator` are verbs: `incorp`,
  `refresh`, `export`, `stream`, `fjord`, `test`, `display`. Avoid adding
  new noun-based class-level entry points; extend an existing verb or add
  a converter token in `incorporator/schema/converters.py` instead.
- **Storage is `List[Dict]`.** Don't switch the runtime representation to
  Arrow / NumPy / pandas in the hot path — the "smaller than pandas"
  identity depends on dict-native storage. Use pyarrow internally for
  format I/O only.
- **Async-first.** Every public verb is `async`. Synchronous helpers live
  in handlers and may run under `asyncio.to_thread`.
- **No required system deps.** Anything heavyweight (pyarrow, openpyxl,
  prefect) lives in an optional extra. Update `pyproject.toml`, the
  `formats_and_compression.md` table, and the README install block when
  you add one.
- **Format handlers** live under `incorporator/io/handlers/` and subclass
  `BaseFormatHandler`. New handlers must implement `parse()` and `write()`,
  register in `_HANDLERS`, and ship with throughput benchmarks under
  `tests/benchmarks/`.

## Comment Style

Code comments are durable artifacts.  Write them for a future reader,
not for the chat or commit that introduced the change.

- **Avoid**:
  - "Fixed in the May 2026 refactor / commit abc123 / chat session X."
  - "Was broken when the validator skipped this branch."
  - "Recent change moved the field out of `incorp_params`."
- **Prefer**:
  - The **what** and the **why** of any non-obvious choice.
  - Time-neutral phrasing — e.g. *"Daemons coordinate via a shared
    lock so refresh mutations are atomic"*, not *"Recently added lock
    for refresh atomicity."*
  - Cross-references to docstrings or `docs/` for deeper background
    instead of restating debugging history inline.

The same rule applies to commit messages and docstrings: explain the
design, not the journey to it.

## Documentation

- **Public docstrings** use Google style and are auto-rendered by `pdoc`
  into `docs/library_reference.md`. Don't write prose duplicates of class /
  method documentation elsewhere — link to the generated reference.
- **Tutorials** (the numbered `docs/N_*.md` files) cover one feature each
  and end with a "Run it from the CLI" addendum mapping the Python code
  to a `pipeline.json`. New tutorials should follow this pattern.
- **README.md** is verb-forward and stays under ~250 lines. Long content
  goes into `docs/`.

## Adding a Format Handler

1. Add the handler class to `incorporator/io/handlers/<category>.py`
   subclassing `BaseFormatHandler`. Implement `parse()` and `write()`.
2. Add the `FormatType` enum entry in `incorporator/io/formats.py` and
   wire the extension into `infer_format()`.
3. Register the handler in `_HANDLERS` in
   `incorporator/io/handlers/__init__.py`.
4. Add an optional dep in `pyproject.toml` if it pulls in a new library.
5. Add tests in `tests/test_handlers_<format>.py`.
6. Add **both write and parse throughput benchmarks** in
   `tests/benchmarks/test_<format>_throughput.py` *and*
   `tests/benchmarks/test_parse_throughput.py`. Pick a conservative floor.
7. Update the table in `docs/formats_and_compression.md` and the install
   bullet in `docs/installation.md`.
8. Add one line to the README's "format support" sentence.

## Continuous Integration & Branch Protection

Every PR and every push to `main` triggers
[`.github/workflows/ci.yml`](./.github/workflows/ci.yml), which runs
three jobs in parallel:

- **lint** — `ruff check`, `ruff format --check`, `black --check` on `incorporator/`.
- **typecheck** — `mypy incorporator/` under strict mode.
- **test** — `pytest -m "not benchmark"` across a 3×2 matrix
  (Python 3.9 / 3.11 / 3.13 on Ubuntu + Windows).

Total wall-clock is ~2–3 minutes; the test matrix runs all six cells
concurrently. A red ❌ on any cell blocks the merge button when branch
protection is on.

### Branch protection setup (maintainer one-time task)

`main` should require the CI workflow to pass before merge. This is a
click-through GitHub setting, not a committed file:

1. **github.com/PyPlumber/incorporator → Settings → Branches**.
2. Under **Branch protection rules**, click **Add rule**.
3. **Branch name pattern**: `main`.
4. Tick **Require status checks to pass before merging** and
   **Require branches to be up to date before merging**.
5. After the first CI run lands, search the status-checks box and add:
   - `lint`
   - `typecheck`
   - `test (ubuntu-latest, 3.9)` and the other five matrix cells.
6. (Recommended) tick **Require a pull request before merging**.
7. (Optional) tick **Do not allow bypassing the above settings** to
   apply the rule to admins.
8. **Create** / **Save changes**.

The "Merge" button on every PR against `main` is now disabled until
all eight CI cells are green.

## Reporting Bugs / Asking Questions

Open an issue on GitHub. For security disclosures, see [`SECURITY.md`](./SECURITY.md)
— do **not** open a public issue for a vulnerability.

## License

By contributing, you agree that your contributions will be licensed under
the [MIT License](./LICENSE).
