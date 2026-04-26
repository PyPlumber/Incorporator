## 🌌 Incorporator (v1.0.0)
**The Dynamic Class Building and Zero-Boilerplate Universal Data Gateway.**

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Pydantic V2](https://img.shields.io/badge/Pydantic-V2-e92063.svg)](https://docs.pydantic.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: Ruff](https://img.shields.io/badge/code%20style-Ruff-261230.svg)](https://github.com/astral-sh/ruff)
[![Typing: Strict](https://img.shields.io/badge/typing-strict-green.svg)](https://mypy.readthedocs.io/en/stable/)

Stop writing boilerplate models, manual HTTP connection loops, pagination state-trackers, and fragile data-cleaning lambda functions. 

**Incorporator** is an elite Python framework that transforms raw JSON, CSV, and XML APIs into fully typed, relational Python Object Graphs in a single line of code.

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
    # Fetch 150 coins, auto-paginate, generate Pydantic models on the fly, and rate-limit perfectly.
    coins = await Crypto.incorp(
        inc_url="https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd",
        inc_code="id",
        inc_name="name",
        inc_page=NextUrlPaginator("next"),
        call_lim=3
)

    # Returns Dynamically created class and dictionary
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
*   **`inc(type)`**: Automatically ranks fallbacks. `inc(datetime)` will parse ISO-8601 or 10+ standard string formats natively.
*   **`calc(func, *keys)`**: Multi-column row calculations. `calc(len, 'residents', default=0)`.
*   **`link_to` & `link_to_list`**: Zero-boilerplate Graph Relational Mapping.

### 3. Native Concurrency & Invisible Resilience
Pass a list of 500 URLs or trigger a deep-drill. Incorporator automatically spins up an `asyncio.Semaphore`, shares a single `httpx.AsyncClient` pool, and batches requests. 
*Hit a 429 Too Many Requests?* It automatically jitter-retries via `tenacity`.
*Still 429?* It gracefully skips the failed row, logs it to `results.failed_sources`, and returns the remaining objects without crashing your pipeline.

### 4. Advanced Asynchronous Pagination
Isolated OOP strategies to gracefully handle pagination without infinite loops. Includes `NextUrlPaginator`, `CursorPaginator`, `OffsetPaginator`, `PageNumberPaginator`, and `LinkHeaderPaginator`.

---

## 📖 Real-World Showcases

### Showcase 1: HATEOAS & Relational Mapping (Star Wars API)
Turn disconnected flat APIs into deeply nested, traversable object graphs using `link_to` and `link_to_list`.

```python
class Planet(Incorporator): pass

class Film(Incorporator): pass

class Person(Incorporator): pass

async def get_luke():
    BASE_URL = "https://swapi.dev/api"
    # 1. Build the foundational Graph Nodes
    planets = await Planet.incorp(
        inc_url=f"{BASE_URL}/planets/", rec_path="results",
        inc_code="id", inc_name="name",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    films = await Film.incorp(
        inc_url=f"{BASE_URL}/films/", rec_path="results",
        inc_code="id", inc_name="title",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={"url": extract_url_id(int)},
        name_chg=[("url", "id")]
    )

    # 2. Fetch People and map relations natively
    people = await Person.incorp(
        inc_url=f"{BASE_URL}/people/", rec_path="results",
        inc_code="id", inc_name="name",
        inc_page=NextUrlPaginator("next"), ignore_ssl=True,
        conv_dict={
            "url": extract_url_id(int),
            "homeworld": calc(link_to(planets, extractor=extract_url_id(int)), default=None),
            "films": calc(link_to_list(films, extractor=extract_url_id(int)), default=[])
        },
        name_chg=[("url", "id")]
    )

    # Yoda, you seek yoda with instant list access
    for person in people[17:22]:
        person.display()            #<class, inc_code (key), inc_name, lact_rcd>
    print('\n')

    # Find Boba, I'd say you have with ( O(1) speed with graph already built )
    boba_fett = people.inc_dict[22]
    print(boba_fett.homeworld.inc_name)  # "Kamino"
    print(boba_fett.films[0].inc_name)  # "The Empire Strikes Back"

asyncio.run(get_luke())
```

### Showcase 2: Parent-Based Enrichment (PokéAPI)
Pass shallow objects into `inc_parent` to trigger automatic concurrent bulk detail scraping.

```python
class Nav(Incorporator): pass

class Pokemon(Incorporator): pass

async def inc_pokedex():
    BASE_URL = "https://pokeapi.co/api/v2"

    # 1. SHALLOW DISCOVERY: Fetch 150 navigation URLs
    pokemon_nav = await Nav.incorp(
        inc_url=f"{BASE_URL}/pokemon/?limit=50&offset=0",
        rec_path="results",
        inc_name="name",
        name_chg=[('url', 'detail_url')],
        inc_page=NextUrlPaginator("next"),
        call_lim=3  # 3 pages * 50 = 150 Pokemon
    )

    def calculate_bst(stats: list) -> int:
        return sum(s.get("base_stat", 0) for s in stats if isinstance(s, dict))

    # 2. DEEP ENRICHMENT: Pass the parent objects. The framework tears out 'detail_url',
    # fires 150 concurrent requests, and builds deep objects automatically.
    enriched_pokemon = await Pokemon.incorp(
        inc_parent=pokemon_nav,
        inc_code="id",
        inc_name="name",
        excl_lst=["sprites", "moves", "game_indices", "held_items"]
    )

    for pokemon in enriched_pokemon[:3]:
        print(pokemon.inc_name, pokemon.abilities[0].ability.name)

asyncio.run(inc_pokedex())
```

### Showcase 3: Local XML to Live JSON Bulk POST (NHTSA API)
Seamlessly bridge deep local XML data with live JSON REST APIs.

```python
class JimmyInvoice(Incorporator): pass

class NHTSARecord(Incorporator): pass

async def audit_jimmys():
    # 1. Extract nested data from a local XML file
    invoices = await JimmyInvoice.incorp(
        inc_file="shady_jimmy.xml",
        rec_path="Dealership.AuditFile.Invoices.Invoice"
    )

    vin_batch_string = ";".join([getattr(inv.Vehicle, "VIN", "") for inv in invoices])

    # 2. Hit a live JSON Bulk Endpoint using a POST payload
    live_records = await NHTSARecord.incorp(
        inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
        method="POST",
        form_payload={"format": "json", "DATA": vin_batch_string},
        rec_path="Results",
        inc_code="VIN",
        conv_dict={"ModelYear": inc(int)}  # Force string years to integers
    )

    # 3. Audit instantly via the memory-safe registry
    for inv in invoices:
        vin = inv.Vehicle.VIN
        actual_car = live_records.inc_dict.get(vin)
        if actual_car.ModelYear != int(inv.Vehicle.Year):
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
