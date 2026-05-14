"""
Tutorial 2 — Universal Formats: One Verb, Any File
--------------------------------------------------
Companion script for `docs/2_universal_formats.md`.

Demonstrates the framework's universal-format promise: the same
`incorp()` call shape reads JSON, CSV, XML, SQLite, and (with the
[parquet] extra) Parquet — every iteration produces the same Pydantic
object graph.

The script builds the fixture files itself inside a temporary
directory so it runs without external setup.

Run with:
    python examples/2_universal_formats.py
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path

from incorporator import Incorporator


class Trade(Incorporator):
    """Trade ledger entry — auto-keyed by ``trade_id``."""


SAMPLE_TRADES = [
    {"trade_id": "T001", "symbol": "AAPL", "qty": 100, "price": 175.50},
    {"trade_id": "T002", "symbol": "MSFT", "qty": 50, "price": 410.25},
    {"trade_id": "T003", "symbol": "GOOG", "qty": 25, "price": 162.80},
]


def write_fixtures(root: Path) -> dict[str, Path]:
    """Materialise the same three trades in four formats inside ``root``."""
    json_path = root / "trades.json"
    csv_path = root / "trades.csv"
    xml_path = root / "trades.xml"
    sqlite_path = root / "trades.sqlite"

    # JSON
    json_path.write_text(json.dumps(SAMPLE_TRADES), encoding="utf-8")

    # CSV
    headers = list(SAMPLE_TRADES[0].keys())
    rows = [",".join(str(t[h]) for h in headers) for t in SAMPLE_TRADES]
    csv_path.write_text(",".join(headers) + "\n" + "\n".join(rows), encoding="utf-8")

    # XML
    items = "".join(
        "<item>" + "".join(f"<{k}>{v}</{k}>" for k, v in t.items()) + "</item>"
        for t in SAMPLE_TRADES
    )
    xml_path.write_text(f"<root>{items}</root>", encoding="utf-8")

    # SQLite
    conn = sqlite3.connect(sqlite_path)
    try:
        conn.execute(
            "CREATE TABLE trades (trade_id TEXT, symbol TEXT, qty INTEGER, price REAL)"
        )
        conn.executemany(
            "INSERT INTO trades VALUES (?, ?, ?, ?)",
            [(t["trade_id"], t["symbol"], t["qty"], t["price"]) for t in SAMPLE_TRADES],
        )
        conn.commit()
    finally:
        conn.close()

    return {"json": json_path, "csv": csv_path, "xml": xml_path, "sqlite": sqlite_path}


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        fixtures = write_fixtures(Path(tmp))
        print(f"📁 Wrote fixtures to {tmp}\n")

        # The universal call shape — same kwargs across every format.
        # Two format-specific kwargs are needed:
        #   * SQLite needs sql_query=... because the format is execution-shaped.
        #   * XML needs rec_path="item" to drill past the <root> wrapper that
        #     xml_to_dict produces when every child shares a tag.
        for label, path in fixtures.items():
            kwargs: dict = {"inc_file": str(path), "inc_code": "trade_id"}
            if label == "sqlite":
                kwargs["sql_query"] = "SELECT * FROM trades"
            elif label == "xml":
                # xml_to_dict wraps every payload in {<root_tag>: ...}, then
                # collapses identically-named children into a list. Drill the
                # dotted path to reach the trade items.
                kwargs["rec_path"] = "root.item"

            trades = await Trade.incorp(**kwargs)
            # Some formats / shapes return a single instance instead of a list;
            # normalise so len() works regardless.
            count = len(trades) if isinstance(trades, list) else 1
            aapl = Trade.inc_dict["T001"]
            print(
                f"{label:8s} → {count} rows; "
                f"AAPL qty={aapl.qty:>3}, price={aapl.price}"
            )

        # Data-lake pivot: same data, two destination formats.
        # Re-load from JSON, then write Parquet + SQLite.
        ledger = await Trade.incorp(inc_file=str(fixtures["json"]), inc_code="trade_id")
        out_parquet = Path(tmp) / "out.parquet"
        out_sqlite = Path(tmp) / "out.sqlite"
        try:
            await Trade.export(instance=ledger, file_path=str(out_parquet))
            print(f"\n📤 Wrote Parquet: {out_parquet.name} ({out_parquet.stat().st_size} bytes)")
        except Exception as e:
            print(f"\n⚠️  Parquet export skipped ({e!s}) — install incorporator[parquet]")

        await Trade.export(
            instance=ledger,
            file_path=str(out_sqlite),
            sql_table="trades",
            if_exists="replace",
        )
        print(f"📤 Wrote SQLite: {out_sqlite.name} ({out_sqlite.stat().st_size} bytes)")


if __name__ == "__main__":
    asyncio.run(main())
