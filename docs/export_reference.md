***

# 📖 `export()` API Reference & Cross-Format Serialization

The `export()` classmethod is the final pillar of Incorporator's "Holy Trinity" API (`incorp`, `refresh`, `export`). 

While `incorp()` handles Extraction and Transformation, `export()` is your **Load** phase. It takes your dynamically generated Python object graphs and serializes them safely to disk as clean JSON, CSV, XML, SQLite, or Apache Avro files.

Because Incorporator operates asynchronously, all heavy disk I/O and compression algorithms are automatically offloaded to background threads, guaranteeing that exporting massive datasets will **never block your asyncio event loop**.

---

## 🚦 The Export Mechanics

When you call `await MyClass.export(instances, "output.csv")`, the framework executes the following sequence:

1. **Serialization:** It safely traverses your in-memory objects and converts them back into standard Python dictionaries using Pydantic's `model_dump()`.
2. **Format Inference:** It automatically detects the desired format based on your file extension (e.g., `.csv`, `.db`, `.avro`).
3. **Format-Specific Sanitization:** (See below) It applies specific safety measures to ensure nested graphs write cleanly to flat files or strictly typed binary formats.
4. **Thread-Safe I/O:** It hands the payload to `asyncio.to_thread()`, streaming the bytes to disk in the background while your main application continues executing.
5. **Background Compression:** If a compression algorithm is requested, it seamlessly archives the flat file into a `.gz`, `.zip`, `.zst`, etc., and cleans up the original uncompressed file.

---

## ✍️ Supported Calling Signatures

Incorporator uses intelligent overloading so you can export data however it fits your workflow best.

**1. The "Active Record" Dump (Recommended):**
Exports every living instance currently tracked in the class memory registry.
```python
await MyClass.export("output.csv")
```
**2. The Positional Subset:**
Exports only a specific list of instances.
```python
await MyClass.export(my_list, "output.csv")
```
**3. The Explicit Kwarg Target:**
For strict readability or legacy compatibility.
```python
await MyClass.export(instance=my_list, file_path="output.csv")
```
---

## 🛠️ Core Parameters

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`instance`** | `Incorporator` \| `List` | (Optional) A specific subset of objects. If omitted, Incorporator automatically exports every living instance currently tracked in the class's inc_dict! |
| **`file_path`** | `str` | **(Required)** The destination file path (e.g., `"data/cleaned_users.xml"`). |
| **`format_type`**| `str` \| `FormatType` | Optional. Explicitly declare the format (`"json"`, `"csv"`, `"xml"`, `"sqlite"`, `"avro"`). If omitted, Incorporator infers it from the `file_path` extension. |

### Optional Keyword Arguments (**kwargs)

| Parameter | Type | Description |
| :--- | :--- | :--- |
| `compression` | `str` \| `CompressionType` | Automatically compresses the exported file in a background thread. Supports `"gz"`, `"bz2"`, `"xz"`, `"zip"`, `"tar"`. (If `[cramjam]` is installed: `"zst"`, `"lz4"`, `"snappy"`). |
| `sql_table` | `str` | **(SQLite Only)** The name of the table to generate. Defaults to the class name (e.g., `user`). |
| `if_exists` | `str` | **(SQLite Only)** Behavior when the table exists. Options: `"replace"` (default), or `"fail"`. |

---

## 🎩 Format-Specific Magic (Zero-Boilerplate)

Writing to CSVs, strict databases, or XMLs normally requires writing extensive boilerplate to flatten dictionaries or sanitize schema tags. Incorporator handles this invisibly.

### 📊 CSV Auto-Flattening
CSVs are flat grids, but Incorporator objects are deeply nested graphs. If you export an object with nested data (e.g., `user.profile.address`), Incorporator automatically intercepts the nested dictionaries/lists and serializes them into valid JSON strings *inside* the CSV cell. 

### 🗄️ SQLite Magic Schema Generation
You do not need to write `CREATE TABLE` statements. Incorporator analyzes the Python types of your objects (mapping `int` to `INTEGER`, `str` to `TEXT`, etc.), dynamically creates the SQL table, and uses the C-level `executemany` driver for lightning-fast, injection-safe bulk inserts.

### 🐘 Avro Strict-Schema Translation
Apache Avro requires a strict schema dictionary embedded in its binary headers. Incorporator dynamically infers this schema from your Pydantic data, mapping Python types to Avro primitives (`long`, `double`, `boolean`). It elegantly declares all columns as `["null", type]` to prevent write-crashes if your API data has missing keys!

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

### Scenario B: Background Compression
Need to save disk space? Pass the `compression` kwarg. Incorporator will write the flat file, compress it using C-optimized streaming, and delete the uncompressed source instantly.

```python
# Creates "upcoming_launches.csv.gz" seamlessly!
await LaunchData.export(launches, "upcoming_launches.csv", compression="gz")
```

### Scenario C: The Local Data Warehouse (JSON ➡️ SQLite)
Instantly dump an API payload into a local database for analytics without writing an ORM.

```python
# 1. Fetch JSON data
users = await User.incorp("https://api.domain.com/v1/users")

# 2. Dump directly to a local SQLite database! 
# Because we didn't pass `sql_table`, it auto-creates a table named 'user'.
await User.export(
    users, 
    "local_warehouse.db", 
    if_exists="replace"
)
```

### Scenario D: Big Data Streaming (API ➡️ Avro)
*(Requires `pip install incorporator[avro]`)*
Move data directly from REST APIs into Hadoop/Kafka-ready binary streams.

```python
# Instantly translates the Pydantic models to an Avro schema and writes binary bytes
await TelemetryEvent.export(events, "archive.avro")
```

---

## 👁️ Observability with `LoggedIncorporator`

If your base class inherits from `LoggedIncorporator` (instead of the standard `Incorporator`), calling `export()` will automatically trigger class-level logging. 

It will write standard `INFO` logs to your `error.log` file when the export begins and completes. If the disk is full or a permission error occurs, it will gracefully catch the `OSError`, write a highly detailed `ERROR` log with the full stack trace, and then re-raise the exception so your app can handle the failure.