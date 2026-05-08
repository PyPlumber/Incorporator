***

# 📖 `export()` Reference & Cross-Format Serialization

The `export()` classmethod is the final pillar of Incorporator's "Holy Trinity" (`incorp`, `refresh`, `export`). 

While `incorp()` handles Extraction and Transformation, `export()` is your **Load** phase. It takes your dynamically generated Python object graphs and serializes them safely to disk as clean JSON, CSV, or XML files.

Because Incorporator operates asynchronously, all heavy disk I/O is automatically offloaded to background threads, guaranteeing that exporting massive datasets will **never block your asyncio event loop**.

---

## 🚦 The Export Mechanics

When you call `await MyClass.export(instances, "output.csv")`, the framework executes the following sequence:

1. **Serialization:** It safely traverses your in-memory objects and converts them back into standard Python dictionaries using Pydantic's `model_dump()`.
2. **Format Inference:** It automatically detects the desired format based on your file extension (e.g., `.csv`, `.xml`).
3. **Format-Specific Sanitization:** (See below) It applies specific safety measures to ensure nested graphs write cleanly to flat files like CSVs.
4. **Thread-Safe I/O:** It hands the payload to `asyncio.to_thread()`, streaming the bytes to disk in the background while your main application continues executing.

---

## 🛠️ Core Parameters

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`instance`** | `Incorporator` \| `List` | **(Required)** The object or list of objects you want to save to disk. |
| **`file_path`** | `str` | **(Required)** The destination file path (e.g., `"data/cleaned_users.xml"`). |
| **`format_type`**| `str` \| `FormatType` | Optional. Explicitly declare the format (`"json"`, `"csv"`, `"xml"`). If omitted, Incorporator infers it from the `file_path` extension. |

---

## 🎩 Format-Specific Magic (Zero-Boilerplate)

Writing to CSVs or XMLs normally requires writing extensive boilerplate to flatten dictionaries or sanitize XML tags. Incorporator handles this invisibly.

### 📊 CSV Auto-Flattening
CSVs are flat grids, but Incorporator objects are deeply nested graphs. If you export an object with nested data (e.g., `user.profile.address`), Incorporator automatically intercepts the nested dictionaries/lists and serializes them into valid JSON strings *inside* the CSV cell. 

This guarantees the CSV writer never crashes, and the resulting file can be instantly re-ingested by Incorporator later!

### 📝 XML Tag Sanitization
XML has strict naming rules (e.g., tags cannot start with a number or contain spaces). When you export to XML, Incorporator automatically sanitizes your attribute names to ensure the resulting XML document is perfectly valid.

---

## 🔄 Common Workflows

### Scenario A: The "Format Translator" (JSON API ➡️ CSV File)
The most common use case for `export()` is instantly translating a nested JSON API into a spreadsheet-ready CSV for business stakeholders, completely skipping the manual formatting loop.

```python
import asyncio
from incorporator import Incorporator

class LaunchData(Incorporator): pass

async def fetch_and_translate():
    # 1. Extract from a JSON REST API
    launches = await LaunchData.incorp("https://ll.thespacedevs.com/2.2.0/launch/upcoming/")
    
    # 2. Export instantly to CSV. 
    # Incorporator infers the format from the ".csv" extension!
    await LaunchData.export(launches, "upcoming_launches.csv")

asyncio.run(fetch_and_translate())
```

### Scenario B: Exporting Enriched Data
`export()` seamlessly serializes the custom attributes you generated during the ETL phase using `calc()` or `inc()`.

```python
# 1. Fetch data and create a new custom attribute
pokemon = await Pokemon.incorp(
    inc_url="https://pokeapi.co/api/v2/pokemon?limit=50",
    conv_dict={
        "base_stat_total": calc(calculate_bst, "stats")
    }
)

# 2. The exported JSON will now contain your newly calculated "base_stat_total"!
await Pokemon.export(pokemon, "enriched_pokemon.json")
```

### Scenario C: Explicit Format Targeting
Sometimes you need to save a file without a standard extension (e.g., streaming to a generic `.dat` or `.log` file). You can bypass the auto-inference by passing the `format_type` kwarg.

```python
# Force Incorporator to serialize the objects as XML, 
# even though the file extension is .dat
await Invoice.export(
    instance=invoices,
    file_path="secure_ledger.dat",
    format_type="xml"
)
```

---

## 🗄️ Observability with `LoggedIncorporator`

If your base class inherits from `LoggedIncorporator` (instead of the standard `Incorporator`), calling `export()` will automatically trigger class-level logging. 

It will write standard `INFO` logs to your `error.log` file when the export begins and completes. If the disk is full or a permission error occurs, it will gracefully catch the `OSError`, write a highly detailed `ERROR` log with the full stack trace, and then re-raise the exception so your app can handle the failure.