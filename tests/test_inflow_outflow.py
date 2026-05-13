"""Integration tests for the `inflow=` / `outflow=` kwargs on the trinity verbs.

Covers the user-facing behaviour that closes the JSON-pipeline DX gap:

* ``inflow.py`` symbols extend the CLI token resolver's allow-list so
  ``conv_dict`` reducers work without a fjord workaround.
* ``conv_dict`` runs **before** format dispatch — proven by an end-to-end
  flow that exports to CSV (not NDJSON) and verifies the reducer's
  integer landed in the CSV cell.
* The deprecated ``code_file=`` alias on ``fjord()`` and ``export()``
  still works but emits ``DeprecationWarning``.
"""

from __future__ import annotations

import asyncio
import csv
import warnings
from pathlib import Path

import pytest

from incorporator import Incorporator


# ----- code_file → outflow deprecation -----


def test_fjord_code_file_alias_emits_deprecation_warning(tmp_path: Path) -> None:
    """Supplying ``code_file=`` to fjord() warns but still routes to outflow=."""
    outflow = tmp_path / "outflow.py"
    outflow.write_text(
        "from incorporator import Incorporator\n"
        "class Sample(Incorporator): pass\n"
        "def outflow(state): return []\n",
        encoding="utf-8",
    )

    class Sample(Incorporator):
        pass

    async def drain() -> None:
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            gen = Incorporator.fjord(
                stream_params=[
                    {"cls": Sample, "incorp_params": {"inc_url": "https://example.invalid/x"}}
                ],
                code_file=outflow,  # deprecated alias
                export_params={"file_path": str(tmp_path / "out.ndjson")},
            )
            # Pull the first audit so the generator actually runs the deprecation
            # branch — the seed phase will fail on the invalid URL, which is fine.
            try:
                await gen.__anext__()
            except (StopAsyncIteration, Exception):  # noqa: BLE001
                pass
            await gen.aclose()

            assert any(
                issubclass(w.category, DeprecationWarning) and "code_file=" in str(w.message)
                for w in captured
            ), f"Expected DeprecationWarning naming code_file, got {[str(w.message) for w in captured]}"

    asyncio.run(drain())


def test_export_code_file_alias_emits_deprecation_warning(tmp_path: Path) -> None:
    """Supplying ``code_file=`` to export() warns but still routes to outflow=."""

    class Item(Incorporator):
        pass

    transform_py = tmp_path / "transform.py"
    transform_py.write_text(
        "def transform(instances):\n    return instances\n",
        encoding="utf-8",
    )

    instances = [Item.model_construct(inc_code="a", inc_name="alpha")]

    async def run() -> None:
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always")
            await Item.export(
                instance=instances,
                file_path=str(tmp_path / "out.ndjson"),
                code_file=str(transform_py),  # deprecated alias
            )
            assert any(
                issubclass(w.category, DeprecationWarning) and "code_file=" in str(w.message)
                for w in captured
            )

    asyncio.run(run())


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
        "def calculate_bst(stats):\n"
        "    return sum(s.get('base_stat', 0) for s in stats if isinstance(s, dict))\n",
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
    assert by_name["bulbasaur"]["base_stat_total"] == "94"   # 45 + 49
    assert by_name["ivysaur"]["base_stat_total"] == "122"   # 60 + 62


def test_inflow_kwarg_on_incorp_resolves_string_tokens(tmp_path: Path) -> None:
    """Python users can pass `inflow=` with string-form conv_dict and get callables."""
    inflow_py = tmp_path / "inflow.py"
    inflow_py.write_text(
        "def double(values):\n"
        "    return [v * 2 for v in values] if isinstance(values, list) else values\n",
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
