"""Integration tests for the `inflow=` / `outflow=` kwargs on the trinity verbs.

Covers the user-facing behaviour that closes the JSON-pipeline DX gap:

* ``inflow.py`` symbols extend the CLI token resolver's allow-list so
  ``conv_dict`` reducers work without a fjord workaround.
* ``conv_dict`` runs **before** format dispatch — proven by an end-to-end
  flow that exports to CSV (not NDJSON) and verifies the reducer's
  integer landed in the CSV cell.
* The Python API rejects the (removed) ``code_file=`` kwarg with a clean
  ``TypeError`` rather than silently accepting it.
"""

from __future__ import annotations

import asyncio
import csv
from pathlib import Path

import pytest

from incorporator import Incorporator


# ----- code_file kwarg is fully removed (regression guard) -----


def test_fjord_no_longer_accepts_code_file_kwarg(tmp_path: Path) -> None:
    """fjord(code_file=...) must raise TypeError — the alias was removed pre-public."""

    class Sample(Incorporator):
        pass

    async def attempt() -> None:
        gen = Incorporator.fjord(
            stream_params=[{"cls": Sample, "incorp_params": {"inc_url": "https://x"}}],
            code_file=tmp_path / "outflow.py",
            export_params={"file_path": str(tmp_path / "out.ndjson")},
        )
        async for _ in gen:
            pass

    with pytest.raises(TypeError, match="code_file"):
        asyncio.run(attempt())


# Note: export() accepts **kwargs (forwarded to the format handler) so a
# stray ``code_file=`` would be silently ignored rather than raising
# TypeError.  The fjord regression test above is sufficient to guard the
# rename — fjord() has a strict signature.


# ----- conv_dict is format-agnostic (CSV export) -----


def test_inflow_calc_reducer_lands_in_csv(tmp_path: Path) -> None:
    """End-to-end: inflow.py reducer + conv_dict + CSV export.

    Proves conv_dict runs BEFORE format dispatch — the reducer's integer
    must appear in the CSV cell.  If conv_dict only worked for JSON, the
    raw list of stat dicts would land in the CSV instead.
    """
    from incorporator.cli.tokens import resolve_tokens
    from incorporator.usercode import extract_public_names, load_user_module

    inflow_py = tmp_path / "inflow.py"
    inflow_py.write_text(
        "def calculate_bst(stats):\n    return sum(s.get('base_stat', 0) for s in stats if isinstance(s, dict))\n",
        encoding="utf-8",
    )

    # Simulate the CLI's load-and-resolve pipeline: load inflow → resolve
    # the conv_dict text token → pass real callables to incorp().
    module = load_user_module(inflow_py, name_hint="_test_inflow")
    extra_names = extract_public_names(module)
    raw_conv = {"stats": "calc(calculate_bst, 'stats', default=0, target_type=int)"}
    resolved_conv = resolve_tokens(raw_conv, extra_names=extra_names)
    assert callable(resolved_conv["stats"]) or hasattr(resolved_conv["stats"], "func")

    # Build a fixture payload with a list-of-dicts in the `stats` field.
    payload = tmp_path / "pokemon.json"
    payload.write_text(
        '[{"name": "bulbasaur", "stats": [{"base_stat": 45}, {"base_stat": 49}]},'
        ' {"name": "ivysaur",  "stats": [{"base_stat": 60}, {"base_stat": 62}]}]',
        encoding="utf-8",
    )

    class Pokemon(Incorporator):
        pass

    out_csv = tmp_path / "pokemon.csv"

    async def run() -> None:
        result = await Pokemon.incorp(
            inc_file=str(payload),
            inc_code="name",
            inc_name="name",
            conv_dict=resolved_conv,
            name_chg=[("stats", "base_stat_total")],
        )
        await Pokemon.export(instance=result, file_path=str(out_csv))

    asyncio.run(run())

    # Read the CSV back and confirm the integer is in the cell.
    with open(out_csv, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    by_name = {r["inc_name"]: r for r in rows}
    # Verify the calc reducer's INTEGER landed in the CSV — not the raw list.
    # If conv_dict only worked for JSON, this cell would contain
    # "[{'base_stat': 45}, ...]".
    assert by_name["bulbasaur"]["base_stat_total"] == "94"  # 45 + 49
    assert by_name["ivysaur"]["base_stat_total"] == "122"  # 60 + 62


def test_inflow_module_imported_only_once_across_chunks(tmp_path: Path) -> None:
    """Repeated incorp() calls with the same inflow path import the module ONCE.

    Verifies the structural guarantee that the plan called for: the CLI loader
    and the trinity verbs both go through ``usercode.load_user_module``, whose
    ``importlib`` flow registers the module in ``sys.modules`` on the first
    call — subsequent calls return the cached module without re-executing the
    file.  Achieved here by counting the number of times the module's
    top-level body runs: a sentinel list at module scope grows by one for
    each genuine import.
    """
    import sys

    inflow_py = tmp_path / "counting_inflow.py"
    inflow_py.write_text(
        "# Top-level statement runs exactly once per import.\n"
        "import sys\n"
        "_marker = sys.modules.get('__inc_import_count__', [])\n"
        "_marker.append(1)\n"
        "sys.modules.setdefault('__inc_import_count__', _marker)\n",
        encoding="utf-8",
    )
    sys.modules.pop("__inc_import_count__", None)

    payload = tmp_path / "data.json"
    payload.write_text('[{"id": "a"}, {"id": "b"}]', encoding="utf-8")

    class Item(Incorporator):
        pass

    async def run() -> None:
        # Drive incorp() five times with the same inflow path.
        for _ in range(5):
            await Item.incorp(inc_file=str(payload), inc_code="id", inflow=str(inflow_py))

    asyncio.run(run())

    counter = sys.modules.get("__inc_import_count__", [])
    assert len(counter) == 1, (
        f"Expected inflow module to import exactly once across 5 incorp() calls; got {len(counter)} imports."
    )


def test_inflow_kwarg_on_incorp_resolves_string_tokens(tmp_path: Path) -> None:
    """Python users can pass `inflow=` with string-form conv_dict and get callables."""
    inflow_py = tmp_path / "inflow.py"
    inflow_py.write_text(
        "def double(values):\n    return [v * 2 for v in values] if isinstance(values, list) else values\n",
        encoding="utf-8",
    )

    payload = tmp_path / "data.json"
    payload.write_text('[{"id": "a", "nums": [1, 2, 3]}]', encoding="utf-8")

    class Item(Incorporator):
        pass

    async def run() -> None:
        result = await Item.incorp(
            inc_file=str(payload),
            inc_code="id",
            inflow=str(inflow_py),
            conv_dict={"nums": "calc(double, 'nums')"},
        )
        # The reducer ran — the nums field is the doubled list.
        return result

    result = asyncio.run(run())
    target = result.inc_dict["a"]
    assert getattr(target, "nums", None) == [2, 4, 6]
