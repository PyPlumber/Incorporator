# 🚀 Incorporator (v1.0.6)

**A schema-free data mapper that turns JSON, XML, or CSV into a unified Python object graph with dot-notation and access-at-runtime.**

<!-- PROJECT HEALTH & DISTRIBUTION -->
[![PyPI version](https://img.shields.io/pypi/v/incorporator?color=blue)](https://pypi.org/project/incorporator/)
[![Python Versions](https://img.shields.io/pypi/pyversions/incorporator.svg)](https://pypi.org/project/incorporator/)

<!-- TECH STACK & TOOLING -->
[![Pydantic v2](https://img.shields.io/badge/pydantic-v2.0+-e92063.svg)](https://pydantic.dev/)
[![HTTPX](https://img.shields.io/badge/httpx-async-blue.svg)](https://www.python-httpx.org/)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)

<!-- OPEN SOURCE -->
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

### ✨ Highlights
* **Works with unpredictable JSON APIs** without writing a single line of schema.
* **Turns API responses into Python objects instantly**—no data models required.
* **Handles changing JSON structures at runtime** without breaking your code or throwing validation errors.
* **Use Pydantic and HTTPX** under the hood without defining models, managing context managers, or writing `while` loops.

### 🎯 Use this when:
* You are working with unpredictable or evolving APIs.
* You don't want to maintain brittle data models.
* You need to explore or transform deeply nested JSON quickly.

---

## 📖 Table of Contents
- [How it Works: Zero-Schema Ingestion](#-how-it-works-zero-schema-ingestion)
- [Installation](#-installation)
- [The "Holy Trinity" API](#-the-holy-trinity-api)
- [Core Superpowers](#-core-superpowers)
  - [1. Pagination & Type Casting](#1-the-1-liner-pagination-cleaning--type-casting)
  - [2. Array Reduction & Enrichment](#2-deep-enrichment--array-reduction)
  - [3. Multi-API Graph Fusion](#3-multi-api-graph-fusion)
  - [4. Declarative Bulk POSTs](#4-xml-ingestion--declarative-bulk-posts)
- [Enterprise Resilience](#-enterprise-resilience--features)
- [Documentation & Examples](#-documentation--examples)

---

## 🛠️ How it Works: Zero-Schema Ingestion

Imagine receiving this spacecraft telemetry JSON. Notice how the nested `"st"` dictionary **changes its structure completely** for every subsystem (`pos` vs `sig` vs `bat`). Standard parsers would crash instantly. 

**The Input (`telemetry.json`):**
```json
[
  {"id":"NAV", "st":{"pos":[12,44], "ok":1}},
  {"id":"COM", "st":{"sig":78, "ok":1}},
  {"id":"PWR", "st":{"bat":92, "ok":1}},
  {"id":"THR", "st":{"lvl":63, "ok":0}}
]
```

**The Incorporator Way:**
Feed it the unpredictable JSON. Incorporator dynamically unifies the changing structures into a single object graph and gives you instant dot-notation access.

```python
import asyncio
from incorporator import Incorporator

async def main():
    # 1. Parse unpredictable JSON directly into Python objects. No models defined!
    systems = await Incorporator.incorp(
        inc_file="telemetry.json",
        inc_code="id" # Sets 'id' as the O(1) Memory Registry lookup key
    )

    # 2. Instantly access the unified Python object graph via dot-notation
    print(f"Navigation Position: {systems.inc_dict['NAV'].st.pos}")   # Output:[12, 44]
    print(f"Power Battery Level: {systems.inc_dict['PWR'].st.bat}%")  # Output: 92%
    
    # 3. Interpret and manipulate data effortlessly at runtime
    thr = systems.inc_dict["THR"]
    if not thr.st.ok:
        print(f"⚠️ THRUST FAILURE! Efficiency dropped to {thr.st.lvl}")
        
asyncio.run(main())
```

### 🤷‍♂️ Wait, what if my data isn't JSON?
It doesn't matter. Incorporator automatically infers the format from the URL or file extension. The syntax **never changes**.

If that exact same telemetry data comes from a legacy system as XML:
```xml
<root>
    <item id="NAV"><st><pos>12</pos><pos>44</pos><ok>1</ok></st></item>
</root>
```
You parse it the *exact* same way:
```python
# The syntax doesn't change for XML...
systems_xml = await Incorporator.incorp(inc_file="telemetry.xml", inc_code="id")
print(systems_xml.inc_dict["NAV"].st.pos) # Output:['12', '44']

# ...and it doesn't change for CSV!
systems_csv = await Incorporator.incorp(inc_file="telemetry.csv", inc_code="id")
```

---

## 📦 Installation

Built entirely on the standard library, Pydantic V2 metaprogramming, and HTTPX.

```bash
pip install incorporator
```
*Dependencies: `pydantic (>=2.0)`, `httpx`, `tenacity`.*

---

## ⛪ The "Holy Trinity" API

Manage your entire data lifecycle with just three `@classmethod` factories. Everything Incorporator does stems from these three commands:

1. **`incorp()`**: **Extract & Transform.** Fetch unknown data, clean it dynamically, and build the Python object graph.[*(Read the API Reference)*](./docs/incorp_reference.md)
2. **`refresh()`**: **Stateful Updates.** Pass existing objects back in to seamlessly fetch live updates and hydrate your memory registries. [*(Read the API Reference)*](./docs/refresh_reference.md)
3. **`export()`**: **Load.** Instantly serialize your deeply nested Python objects out to clean CSV, XML, or JSON files. [*(Read the API Reference)*](./docs/export_reference.md)

---

## ⚡️ Core Superpowers

### 1. The 1-Liner: Pagination, Cleaning, & Type Casting
*Example: Fetching Space Devs upcoming launches.*

You don't need a `while` loop to paginate, and you don't need to define a massive schema to drill into nested data.

```python
from datetime import datetime
from incorporator import Incorporator, NextUrlPaginator, inc

class Launch(Incorporator): pass

launches = await Launch.incorp(
    inc_url="https://ll.thespacedevs.com/2.2.0/launch/upcoming/",
    rec_path="results",                   # Drill past the useless metadata wrapper
    inc_page=NextUrlPaginator("next"),    # Auto-paginate using the 'next' JSON key
    call_lim=2,                           # Safely cap at 2 pages
    excl_lst=["image", "vid_urls"],       # Drop heavy unneeded keys instantly
    conv_dict={
        "net": inc(datetime)              # Safely cast ISO-8601 strings to datetime objects
    }
)

# Access deeply nested, strongly-typed attributes with ZERO schema definition
print(f"🚀 {launches[0].name}")
print(f"⏰ {launches[0].net.strftime('%B %d, %Y')}")
print(f"📍 {launches[0].pad.location.name}") # Dot-notation straight through nested dicts!
```

### 2. Deep Enrichment & Array Reduction
*Example: Discovering Pokémon and flattening their stats.*

When APIs return heavily nested arrays, Incorporator lets you intercept them using `calc()`, run a custom Python reduction function, and flatten them into simple native types.

```python
from incorporator.methods.converters import calc

def calculate_bst(stats_array) -> int:
    """Reduces a nested JSON array into a single integer."""
    return sum(stat.get("base_stat", 0) for stat in stats_array if isinstance(stat, dict))

# 1. Shallow Discovery (Fetches URLs)
pokemon_nav = await Nav.incorp(..., inc_child="url") 

# 2. Deep Enrichment (Spawns concurrent requests to all discovered URLs seamlessly)
enriched_pokemon = await Pokemon.incorp(
    inc_parent=pokemon_nav,  # Routes the parent list directly into the network engine!
    inc_code="id",
    conv_dict={
        # Intercepts the raw JSON array, calculates the total, and saves it as an integer!
        "stats": calc(calculate_bst, "stats", default=0, target_type=int),
    },
    name_chg=[("stats", "base_stat_total")] # Rename the key dynamically
)
```

### 3. Multi-API Graph Fusion
*Example: Fusing CoinGecko assets with Binance Live Order Books.*

Stop writing manual matching loops or dumping data into SQL just to join it. Incorporator lets you bind independent APIs together natively using `link_to`.

```python
from incorporator.methods.converters import link_to, calc

# 1. Define a clean, null-safe formatting function (No lambdas!)
def to_usdt(sym: str) -> str:
    return f"{str(sym).upper()}USDT" if sym else None

# 2. Fetch Binance Order Books (Instantly becomes an O(1) in-memory registry)
binance_books = await BinanceBook.incorp(
    inc_url="https://api.binance.us/.../bookTicker", 
    inc_code="symbol"
)

# 3. Fetch CoinGecko Assets and fuse them dynamically
assets = await CryptoAsset.incorp(
    inc_url="https://api.coingecko.com/...",
    inc_code="id",
    conv_dict={
        # We pass our named formatting function cleanly into the extractor
        "live_book": calc(link_to(binance_books, extractor=to_usdt), "symbol")
    }
)

# Traverse the unified multi-API graph natively
print(f"{assets[0].name} Live Bid: {assets[0].live_book.bidPrice}")
```

### 4. XML Ingestion & Declarative Bulk POSTs
*Example: Auditing a local XML ledger against a Federal Database.*

Need to send a batch POST request based on dynamically extracted XML data? Pass a parent object and use the magical `join_all()` token to automatically concatenate parent IDs across a Bulk POST payload.

```python
from incorporator.methods.converters import join_all

# 1. Ingest a local XML file
invoices = await Invoice.incorp(
    inc_file="jimmy_ledger.xml",
    rec_path="Dealership.AuditFile.Invoices.Invoice",
    inc_child="Vehicle.VIN" # Extract the VIN numbers from the XML
)

# 2. Declarative Bulk POST using the XML data!
govt_specs = await NHTSASpec.incorp(
    inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
    inc_parent=invoices,
    http_method="POST",
    payload_type="form",
    form_payload={
        "format": "json",
        "data": join_all(";") # Magically joins all XML VINs by a semicolon!
    },
    rec_path="Results",
    inc_code="VIN"
)
```

---

## 🛠 Enterprise Resilience & Features

### 📡 Invisible Networking & DLQs
You never have to manage `httpx.AsyncClient` contexts. Incorporator handles shared connection pools natively. It includes exponential backoff retries via Tenacity. If a URL repeatedly fails with an HTTP 429, it gracefully skips it and places it in a **Dead Letter Queue**.
```python
if launches.failed_sources:
    print(f"DLQ Alert: Programmatically retry these {len(launches.failed_sources)} URLs.")
```

### 🧠 Zero-OOM Memory Management
When fetching hundreds of thousands of records, standard Python lists of dicts cause Out-Of-Memory (OOM) crashes. 
Incorporator wraps lists in an `IncorporatorList`. Every instance automatically registers itself into its class `inc_dict`—backed by a `weakref.WeakValueDictionary`. You get lightning-fast O(1) lookups without blocking the Garbage Collector.

### 🗄️ Non-Blocking Observability
Swap your base class to `LoggedIncorporator` and set `enable_logging=True`. Incorporator spins up `QueueHandler` background threads to write auto-rotating JSON-line logs (`api.log`, `error.log`, `debug.log`) so disk I/O *never* blocks your asyncio event loop.

### 🔄 Stateful Updates & Cross-Format Exports
Fetch XML, interact with it as clean Python objects, and dump it to CSV instantly.
```python
# Update state in memory, then serialize to disk safely without boilerplate
await Incorporator.refresh(launches)
await Incorporator.export(launches, "upcoming_launches.csv", format_type="csv")
```

---

## 📚 Documentation & Examples

The best way to learn Incorporator is through our deeply documented API references and Guided Tutorials. 

### API References
* [📖 **`incorp()` API Reference & ETL Guide**](./docs/incorp_reference.md)
* [📖 **`refresh()` API Reference & Stateful Updates**](./docs/refresh_reference.md)
* [📖 **`export()` API Reference & Serialization**](./docs/export_reference.md)

### Guided Tutorials (Real-World Examples)
Check out the [`/examples`](./examples) directory for runnable code, and the links below for detailed Markdown walkthroughs of each feature:
* [🚀 **Space Devs Tutorial**](./docs/1_quick_setup.md) - Pagination, simple ETL, and type casting.
* [⚡️ **Pokédex Power Rankings**](./docs/2_advanced_etl_calc.md) - Deep enrichment (HATEOAS) and array reductions.
* [📊 **Stablecoin Dashboard**](./docs/3_graph_mapping.md) - Multi-API graph fusion and relational data binding.
* [🕵️‍♂️ **Shady Jimmy's Ledger**](./docs/4_xml_post_auditing.md) - XML ingestion, O(1) memory audits, and declarative bulk POSTs.

---

## 🤝 Philosophy & Contributing
Incorporator is built on strict OOP principles, non-blocking observability, and a forgiving metaprogramming shield. We trap standard library exceptions (`JSONDecodeError`, `httpx.HTTPStatusError`) and gracefully recast them as domain errors. Your event loop is safe with us.

Contributions are welcome!
```