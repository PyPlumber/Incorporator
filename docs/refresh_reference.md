***

# 📖 `refresh()` Reference & Stateful Updates

The `refresh()` classmethod is the second pillar of Incorporator's "Holy Trinity" (`incorp`, `refresh`, `export`). 

While `incorp()` is used for initial **Discovery and Extraction**, `refresh()` is designed for **Stateful Updates**. It takes an existing Incorporator instance (or a list of instances), automatically traces their origins, fetches live remote data, and returns freshly updated objects while maintaining your dynamic Pydantic schemas and `inc_dict` registries.

---

## 🚦 The Refresh Mechanics

When you call `await MyClass.refresh(instances, ...)`, the framework performs the following sequence:

1. **Origin Resolution:** If you don't provide a `new_url`, Incorporator inspects the provided instances and automatically extracts the URLs or File paths they were originally created from.
2. **Concurrent Fetch:** It chunks and throttles the network requests concurrently via `network.py`.
3. **Re-Transformation:** It applies any provided `excl_lst`, `conv_dict`, or `name_chg` to the newly fetched data.
4. **Registry Hydration:** It compiles the data back into the *exact same dynamic subclass* as the original instances, instantly updating your `inc_dict` memory registry with the new state.

---

## 🛠️ Core Parameters

| Parameter | Type | Description |
| :--- | :--- | :--- |
| **`instance`** | `Incorporator` \| `List` | **(Required)** The existing object(s) you want to update. |
| **`new_url`** | `str` \| `List[str]` | Optional. Explicitly provide a new endpoint to fetch updates from. If omitted, it defaults to the instance's original `inc_url`. |
| **`new_file`** | `str` \| `List[str]` | Optional. Explicitly provide a new local file to read from. |
| **`inc_child`** | `str` | Used for HATEOAS routing. Drills into the existing instances to extract a specific child URL for the refresh. |

### ETL Parameters (Passed Through)
Just like `incorp()`, `refresh()` accepts the full suite of Declarative ETL parameters. This is incredibly powerful because the "Update API" endpoint often returns a different JSON schema than the initial "List API" endpoint.

*   `conv_dict`: Apply `inc()` or `calc()` to the incoming refresh data.
*   `excl_lst`: Drop new keys.
*   `name_chg`: Rename incoming keys.
*   `inc_code` / `inc_name`: Re-bind the primary keys.

---

## 🔄 Common Workflows

### Scenario A: The "In-Place" Polling Refresh (Implicit Origin)
If your API endpoints represent live data (e.g., a stock ticker, a server status, or a weather feed), you can pass the objects back into `refresh()` without specifying URLs. Incorporator remembers where they came from.

```python
import asyncio
from incorporator import Incorporator

class ServerNode(Incorporator): pass

async def monitor_servers():
    # 1. Initial Ingestion
    servers = await ServerNode.incorp(
        inc_url=["https://api.datacenter.com/node/1", "https://api.datacenter.com/node/2"],
        inc_code="node_id"
    )
    
    print(f"Node 1 CPU: {servers[0].cpu_load}%")

    await asyncio.sleep(60) # Wait a minute...

    # 2. Implicit Refresh: Fetches from the exact same URLs seamlessly!
    updated_servers = await ServerNode.refresh(servers)
    
    print(f"Node 1 CPU (Updated): {updated_servers[0].cpu_load}%")
```

### Scenario B: Target Override (Explicit Origin)
Sometimes, you fetch a list of objects from one endpoint (e.g., `/users`), but you need to refresh their specific details from a completely different endpoint (e.g., `/users/updates` or a new CSV file). 

You can explicitly pass `new_url` or `new_file`, and Incorporator will gracefully map the new data into the existing objects' dynamic classes.

```python
# We have an existing list of 'invoices' fetched yesterday.
# Today, we received a new CSV file with updated payment statuses.

updated_invoices = await Invoice.refresh(
    instance=invoices,
    new_file="daily_payment_updates.csv", # Overrides the original origin
    conv_dict={
        # We can apply converters specifically for the refresh pipeline!
        "paid_date": inc(datetime),
        "is_cleared": inc(bool, default=False)
    }
)
```

### Scenario C: Deep HATEOAS Refresh (`inc_child`)
If your initial objects contain a specific URL field for their live status (e.g., `status_url`), you can tell `refresh()` to extract that specific field to perform the update.

```python
# The objects have a dynamic nested attribute: `server.metadata.live_health_url`

live_health_data = await ServerNode.refresh(
    instance=servers,
    inc_child="metadata.live_health_url", # Extracts the URL from the instances automatically
    excl_lst=["heavy_logs"]               # Drops heavy keys from the new payload
)
```

---

## 🧠 Memory & Registry Safety

Because Incorporator utilizes a `weakref.WeakValueDictionary` for its `.inc_dict` registry, calling `refresh()` is 100% Out-Of-Memory (OOM) safe. 

When `refresh()` returns the updated list of objects, they share the exact same `inc_code` (Primary Key) as the old objects. They automatically overwrite the old objects in the `.inc_dict` registry. Once the old objects are no longer referenced by your local variables, Python's Garbage Collector instantly deletes them, preventing memory leaks during long-running polling scripts.