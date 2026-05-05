***

## 🌌 Incorporator (v1.0.5)
**The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway.**

[![PyPI version](https://img.shields.io/pypi/v/incorporator.svg)](https://pypi.org/project/incorporator/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Pydantic V2](https://img.shields.io/badge/Pydantic-V2-e92063.svg)](https://docs.pydantic.dev/)
[![Typing: Strict](https://img.shields.io/badge/typing-strict-green.svg)](https://mypy.readthedocs.io/en/stable/)
[![Code style: Ruff](https://img.shields.io/badge/code%20style-Ruff-261230.svg)](https://github.com/astral-sh/ruff)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Stop writing boilerplate models, manual HTTP connection loops, pagination state-trackers, and fragile data-cleaning lambda functions. 

**Incorporator** is an elite Python framework that transforms raw JSON, CSV, and XML APIs into fully typed, relational Python Object Graphs in a single line of code. Trade away pages of unrelated code for an easy, prebuilt engine.  

This is a framework that handles dynamic Pydantic metaprogramming, graph relational mapping, asynchronous connection pooling, and declarative ETL in less than 30KB.

## 🚀 Installation

```bash
pip install incorporator
```

## ⚡ The "Zero-Boilerplate" Philosophy

**The Old Way:** Define a rigid `BaseModel`, write an `httpx` loop, handle 429 retries, write a custom paginator, manually link foreign keys, catch `KeyErrors`, and hope the API schema doesn't change.

**The Incorporator Way:**
```python
class Crypto(Incorporator): pass

async def crypto_coins():
    coins = await Crypto.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd",
        inc_code="id",
        inc_name="name",
    )

    # Returns a dynamically compiled Pydantic list wrapper with an O(1) memory registry!
    bitcoin = coins.inc_dict['bitcoin']
    print(bitcoin.circulating_supply)       # 20021206.0
    print(bitcoin.current_price)            # 78321

asyncio.run(crypto_coins())
```

---

## 🛠️ The Core Architectural Pillars

### 1. The Holy Trinity API & Dynamic Registries
- `incorp()`: Extracts raw data, compiles dynamic `Pydantic` schemas natively, and loads data into intelligent `IncorporatorList` wrappers.
- `refresh()`: Hydrates existing instances seamlessly with new data (perfect for live feeds).
- `export()`: Dumps stateful object graphs back into sanitized JSON, XML, or CSV files.
- **The `inc_dict`:** Every object automatically registers itself into a memory-safe `WeakValueDictionary`. Look up any object instantly: `coins.inc_dict.get('bitcoin')`.

### 2. Declarative ETL & Null-Safe Converters
Data is messy. Incorporator's built-in `conv_dict` tools intercept bad data *before* Pydantic validation, shielding you from crashes with beautiful, readable syntax.
*   **`inc(type)`**: Automatically ranks fallbacks. `inc(float)` safely converts API garbage like `"unknown"` or `"n/a"` into `0.0`. 
*   **`calc(func, *keys)`**: Multi-column row calculations. `calc(len, 'residents', default=0)`.
*   **`link_to` & `link_to_list`**: Zero-boilerplate Graph Relational Mapping.

### 3. Native Concurrency & The State Carrier
Pass parent objects into `inc_parent` and declare an `inc_child` path. Incorporator caches the path state, drills into the nested objects, automatically spins up an `asyncio.Semaphore`, and batches concurrent deep-drills across a single shared `httpx` pool.

### 4. Advanced Asynchronous Pagination
Isolated OOP strategies gracefully handle pagination without infinite loops. Includes `NextUrlPaginator`, `CursorPaginator`, `OffsetPaginator`, `PageNumberPaginator`, and natively supports POST-body cursor overrides.

---

## 📖 Real-World Showcases

### Showcase 1: Graph Relational Mapping (Star Wars API)
Turn disconnected flat APIs into deeply nested, traversable object graphs using `split_and_get` and `link_to`.

```python
from incorporator.methods.converters import split_and_get, link_to, link_to_list

class Planet(Incorporator): pass
class Film(Incorporator): pass
class Person(Incorporator): pass

async def far_far_away():
    BASE_URL = "https://swapi.dev/api"
    
    # 0. Build a reusable, highly efficient ID extractor
    get_id = split_and_get('/', -1, int)
    
    # 1. Build the foundational Graph Nodes
    planets = await Planet.incorp(
        inc_url=f"{BASE_URL}/planets/", rec_path="results",
        inc_code="id", inc_name="name", inc_page=NextUrlPaginator("next"),
        conv_dict={"url": get_id}, name_chg=[("url", "id")]
    )

    films = await Film.incorp(
        inc_url=f"{BASE_URL}/films/", rec_path="results",
        inc_code="id", inc_name="title", inc_page=NextUrlPaginator("next"),
        conv_dict={"url": get_id}, name_chg=[("url", "id")]
    )

    # 2. Fetch People and map relations natively
    people = await Person.incorp(
        inc_url=f"{BASE_URL}/people/", rec_path="results",
        inc_code="id", inc_name="name", inc_page=NextUrlPaginator("next"),
        conv_dict={
            "url": get_id,
            "homeworld": link_to(planets, extractor=get_id),
            "films": link_to_list(films, extractor=get_id)
        },
        name_chg=[("url", "id")]
    )

    # Find Boba Fett at O(1) speed with graph mapping already built natively!
    boba_fett = people.inc_dict.get(22)
    print(boba_fett.homeworld.inc_name)  # "Kamino"
    print(boba_fett.films[0].inc_name)   # "The Empire Strikes Back"

asyncio.run(far_far_away())
```

### Showcase 2: Explicit Parent-Based Enrichment (PokéAPI)
Pass shallow objects into `inc_parent` and explicitly declare `inc_child` to trigger automatic concurrent bulk scraping. No `for` loops required.

```python
class Nav(Incorporator): pass
class Pokemon(Incorporator): pass

async def inc_pokedex():
    BASE_URL = "https://pokeapi.co/api/v2"

    # 1. SHALLOW DISCOVERY: Fetch 150 navigation objects.
    # We explicitly tell the framework that the next URLs live in the "url" key.
    pokemon_nav = await Nav.incorp(
        inc_url=f"{BASE_URL}/pokemon/?limit=50&offset=0",
        rec_path="results",
        inc_name="name",
        inc_child="url",  # <--- The State Carrier saves this path!
        inc_page=NextUrlPaginator("next"),
        call_lim=3
    )

    # 2. DEEP ENRICHMENT: The framework reads the cached state from Phase 1,
    # drills into the 150 objects, extracts the URLs, and fires 150 concurrent requests!
    enriched_pokemon = await Pokemon.incorp(
        inc_parent=pokemon_nav,
        inc_code="id",
        inc_name="name",
        excl_lst=["sprites", "moves", "game_indices", "held_items"]
    )

    # Deep objects are fully built.
    for pokemon in enriched_pokemon[:3]:
        print(pokemon.inc_name, pokemon.abilities[0].ability.name)

asyncio.run(inc_pokedex())
```

### Showcase 3: XML Parsing to Live Bulk POSTs (NHTSA API)
Seamlessly bridge deep local XML data with live JSON REST APIs using Declarative POST tokens.

```python
from incorporator.methods.converters import join_all

class JimmyInvoice(Incorporator): pass
class NHTSARecord(Incorporator): pass

async def audit_jimmys():
    # 1. Extract nested data from a local XML file into the Pydantic engine.
    # We set `inc_child` to cache the dot-notation path to the VINs.
    invoices = await JimmyInvoice.incorp(
        inc_file="shady_jimmy.xml",
        rec_path="Dealership.AuditFile.Invoices.Invoice",
        inc_child="Vehicle.VIN" 
    )

    # 2. Hit a live JSON Bulk Endpoint using a Declarative POST payload.
    # The `join_all` token automatically extracts the VINs and joins them 
    # into a single, highly-optimized Batch POST Request!
    live_records = await NHTSARecord.incorp(
        inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
        inc_parent=invoices,
        method="POST",
        payload_type="form",
        form_payload={
            "format": "json", 
            "data": join_all(";") # <--- Zero-boilerplate batching!
        },
        rec_path="Results",
        inc_code="VIN"
    )

    # 3. Audit instantly via the memory-safe registry
    for inv in invoices:
        vin = inv.Vehicle.VIN
        actual_car = live_records.inc_dict.get(vin)
        if actual_car and actual_car.ModelYear != int(inv.Vehicle.Year):
            print("Fraud Detected!", inv.inc_code, inv.Vehicle.Model)

asyncio.run(audit_jimmys())
```

---

## 🕵️ Non-Blocking Observability
Need production logs without starving your async event loop?
```python
from incorporator import LoggedIncorporator

class WebAPI(LoggedIncorporator): pass

# Configures background multithreaded queue logging automatically
instance = await WebAPI.incorp(
    inc_url="https://api.example.com/data",
    enable_logging=True
)

instance.log_info("Standard trace")
instance.log_error("API Offline", exc_info=True)
instance.log_api("Web traffic trace") # Routes to isolated api.log
```

## 🤝 Contributing
1. Let's go!

*Built for data engineers who want to sleep at night.*