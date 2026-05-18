"""
Tutorial 2 — Universal Formats: Build a Crypto Snapshot Warehouse
-----------------------------------------------------------------
Companion script for `docs/2_universal_formats.md`.

Demonstrates the framework's universal-format promise via the real
crypto-ETL pattern: snapshot CoinGecko's top-100 markets, land each
snapshot in multiple stores, then round-trip every artifact back into
the same Python object graph.

Pipeline:
1. ``incorp()`` once from CoinGecko (top-100 by market cap).
2. Append to NDJSON + CSV (append-friendly log).
3. Append to SQLite (upsert-friendly warehouse).
4. Snapshot-write Parquet (columnar, atomic replace each call).
5. Re-``incorp()`` each artifact and verify the round-trip.

Run with:
    python examples/03-universal-formats/universal_formats.py
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from incorporator import Incorporator


class Coin(Incorporator):
    """CoinGecko market row — auto-keyed by ``id``."""


COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"


async def snapshot() -> "list[Coin]":
    """Pull one snapshot of CoinGecko top-100 markets."""
    coins = await Coin.incorp(
        inc_url=COINGECKO_MARKETS_URL,
        params={"vs_currency": "usd", "per_page": 100, "page": 1},
        inc_code="id",
        inc_name="name",
        excl_lst=["image"],  # heavy field — see Tutorial 1 inspector output
    )
    print(f"📥 Loaded {len(coins)} coins from CoinGecko.")
    return coins


async def append_log(coins, data_dir: Path) -> None:
    """Append to NDJSON + CSV — both grow row-wise."""
    await Coin.export(
        instance=coins,
        file_path=data_dir / "coins_log.ndjson",
        if_exists="append",
    )
    await Coin.export(
        instance=coins,
        file_path=data_dir / "coins_log.csv",
        if_exists="append",
    )
    print(f"📜 Appended {len(coins)} rows to NDJSON + CSV log.")


async def upsert_warehouse(coins, data_dir: Path) -> None:
    """Append into a SQLite warehouse table."""
    await Coin.export(
        instance=coins,
        file_path=data_dir / "coins_warehouse.sqlite",
        sql_table="coin_snapshots",
        if_exists="append",
    )
    print(f"🗃️  Upserted {len(coins)} rows into SQLite warehouse.")


async def snapshot_parquet(coins, data_dir: Path) -> None:
    """Atomically snapshot-replace a Parquet file each call."""
    out = data_dir / "coins_latest.parquet"
    try:
        await Coin.export(
            instance=coins,
            file_path=out,
            parquet_compression="snappy",
        )
        print(f"📊 Wrote Parquet snapshot ({out.stat().st_size} bytes).")
    except Exception as e:                                  # noqa: BLE001
        print(f"⚠️  Parquet export skipped ({e!s}) — install incorporator[parquet]")


async def verify_round_trip(data_dir: Path) -> None:
    """Re-incorp every artifact and prove the object graph round-trips."""
    print("\n🔁 Round-trip verification:")

    from_ndjson = await Coin.incorp(inc_file=data_dir / "coins_log.ndjson", inc_code="id")
    btc = from_ndjson.inc_dict["bitcoin"]
    print(f"  ndjson  → BTC ${btc.current_price:,.2f}")

    from_csv = await Coin.incorp(inc_file=data_dir / "coins_log.csv", inc_code="id")
    btc = from_csv.inc_dict["bitcoin"]
    print(f"  csv     → BTC ${btc.current_price:,.2f}")

    from_sqlite = await Coin.incorp(
        inc_file=data_dir / "coins_warehouse.sqlite",
        sql_query="SELECT * FROM coin_snapshots",
        inc_code="id",
    )
    btc = from_sqlite.inc_dict["bitcoin"]
    print(f"  sqlite  → BTC ${btc.current_price:,.2f}")

    parquet_path = data_dir / "coins_latest.parquet"
    if parquet_path.exists():
        from_parquet = await Coin.incorp(inc_file=parquet_path, inc_code="id")
        btc = from_parquet.inc_dict["bitcoin"]
        print(f"  parquet → BTC ${btc.current_price:,.2f}")


async def main() -> None:
    # Output lives next to the script (``examples/03-universal-formats/out/``)
    # so you can inspect each of the four artefacts (NDJSON / CSV / SQLite /
    # Parquet) with your own tools after the run.  ``examples/**/out/`` is
    # gitignored — nothing leaks into git.
    here = Path(__file__).resolve().parent
    data_dir = here / "out"
    data_dir.mkdir(exist_ok=True)
    print(f"📁 Warehouse root: {data_dir}\n")

    coins = await snapshot()
    await append_log(coins, data_dir)
    await upsert_warehouse(coins, data_dir)
    await snapshot_parquet(coins, data_dir)
    await verify_round_trip(data_dir)


if __name__ == "__main__":
    asyncio.run(main())
