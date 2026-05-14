***

> 📎 **Appendix — XML ingestion + declarative bulk POST + fraud
> audit.**  XML parsing, state-carrier graph drilling, and a
> templated bulk-POST round-trip against an audit endpoint.  See
> [Tutorial 4 — Parent-Child Drilling](../4_parent_child_drilling.md)
> for the canonical parent-child intro before tackling this one.

***

# 🚨 Declarative Bulk POST Enrichment: Auditing "Shady Jimmy"

REST APIs don’t just use `GET`. When querying bulk endpoints (like Government Databases or GraphQL), you often need to use `POST` and send a **dynamic payload** mapping multiple records. 

In this tutorial, you play the role of a State Auditor. You have seized an XML ledger from **"Shady Jimmy's Used Cars"**. Your job is to extract his vehicle inventory from the local XML file and `POST` those VINs to the official **NHTSA** batch database to verify Jimmy isn't selling fraudulent cars.

## 🗄️ The Input Data (`jimmy_ledger.xml`)
Save the seized XML data into a local file called `jimmy_ledger.xml`. Notice how deeply nested the vehicle data is inside the dealership audit wrapper:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Dealership name="Shady Jimmy's Used Cars">
    <AuditFile generatedAt="2026-04-24T10:15:00Z">
        <Invoices>
            <Invoice id="INV-001">
                <!-- ... nested buyer data ... -->
                <Vehicle>
                    <VIN>1HGCM82633A004352</VIN>
                    <Make>Honda</Make>
                    <Model>Accord</Model>
                </Vehicle>
            </Invoice>
            <!-- ... more invoices ... -->
        </Invoices>
    </AuditFile>
</Dealership>
```

---

## 💻 The Audit Script

Create a file called `audit_jimmy.py`. We are going to use Incorporator's native XML parsing to load the ledger, define our extraction path using `inc_child`, and use the `join_all` declarative token to bulk-enrich the data in a single, highly-optimized network call.

```python
import asyncio
from typing import Any
from incorporator import Incorporator
from incorporator.schema.extractors import join_all

# ==========================================
# 1. DEFINE OUR OBJECTS
# ==========================================
class Invoice(Incorporator):
    pass

class NHTSASpec(Incorporator):
    pass

async def run_audit() -> None:
    print("📂 Parsing Shady Jimmy's Local XML Ledger...")

    # ==========================================
    # PHASE 1: Ingest the XML File
    # ==========================================
    invoices = await Invoice.incorp(
        inc_file="jimmy_ledger.xml",
        rec_path="Dealership.AuditFile.Invoices.Invoice",
        inc_code="id",
        
        # 🛡️ The State Carrier: We explicitly declare the child path here!
        # The IncorporatorList will cache this state for Phase 2.
        inc_child="Vehicle.VIN"
    )

    print(f"✅ Extracted {len(invoices)} Invoices. Contacting Federal Databases...")

    # ==========================================
    # PHASE 2: Declarative Bulk POST Enrichment
    # ==========================================
    # Incorporator reads the cached `inc_child_path`, extracts every VIN, 
    # and automatically joins them with semicolons into 1 Bulk Batch Request!
    govt_specs = await NHTSASpec.incorp(
        inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
        inc_parent=invoices,
        http_method="POST",
        payload_type="form",        
        form_payload={
            "format": "json",
            "data": join_all(";")  # 🛡️ Zero-boilerplate batching token!
        },
        rec_path="Results",
        inc_code="VIN"
    )

    print(f"✅ Government Data Received for {len(govt_specs)} vehicles. Initiating Fraud Audit...\n")

    # ==========================================
    # PHASE 3: The Fraud Audit (O(1) Lookups)
    # ==========================================
    print("=" * 85)
    print(f"{'INVOICE':<10} | {'VIN':<18} | {'JIMMY LISTED':<20} | {'NHTSA TRUE SPEC':<25}")
    print("=" * 85)

    fraud_count = 0

    for inv in invoices:
        inv_id = getattr(inv, "inc_code", "UNKNOWN")

        # 1. Jimmy's Claims (Auto-nested by Incorporator's XML Parser)
        jimmy_vin = getattr(inv.Vehicle, "VIN", "UNKNOWN")
        jimmy_make = getattr(inv.Vehicle, "Make", "UNKNOWN").upper()
        jimmy_model = getattr(inv.Vehicle, "Model", "UNKNOWN").upper()
        jimmy_claim = f"{jimmy_make} {jimmy_model}"

        # 2. The Government Truth (O(1) Memory Registry Lookup)
        true_spec = govt_specs.inc_dict.get(jimmy_vin)

        if true_spec:
            true_make = getattr(true_spec, "Make", "UNKNOWN").upper()
            true_model = getattr(true_spec, "Model", "UNKNOWN").upper()
            federal_claim = f"{true_make} {true_model}"
        else:
            federal_claim = "API OFFLINE / UNKNOWN"

        # 3. Discrepancy Detection
        if jimmy_make not in federal_claim and federal_claim != "API OFFLINE / UNKNOWN":
            print(f"🚨 {inv_id:<7} | {jimmy_vin:<18} | {jimmy_claim:<20} | {federal_claim:<25} <-- FRAUD!")
            fraud_count += 1
        else:
            print(f"✅ {inv_id:<7} | {jimmy_vin:<18} | {jimmy_claim:<20} | {federal_claim:<25}")

    print("=" * 85)
    if fraud_count > 0:
        print(f"🛑 AUDIT FAILED: Discovered {fraud_count} fraudulent transaction(s). Dispatching authorities.")
    else:
        print("🟢 AUDIT PASSED: Ledger matches federal records.")

if __name__ == "__main__":
    asyncio.run(run_audit())
```

---

## 🧠 Framework Highlights

### 1. Zero-Boilerplate XML Parsing
Parsing XML in standard Python usually requires messy libraries like `xml.etree.ElementTree` and writing recursive loops. Incorporator auto-detects the `.xml` extension, drills through the `rec_path`, and dynamically builds nested Python objects (like `inv.Vehicle.VIN`) implicitly.

### 2. The Explicit State Carrier (`inc_child`)
Instead of relying on implicitly mapped URLs or dummy strings, Incorporator uses the **State Carrier** pattern. By declaring `inc_child="Vehicle.VIN"` in Phase 1, the returned `invoices` list securely memorizes that path. When passed into Phase 2, Incorporator instantly knows exactly where to drill to retrieve the primary keys for the next network request.

### 3. Declarative Bulk POST Execution (`join_all`)
The NHTSA endpoint is a "Batch" processor—it expects a single string of VINs separated by semicolons. Instead of forcing you to write `for` loops, extraction lambdas, or punishing the government servers with 500 individual concurrent requests, Incorporator solves this declaratively:
```python
form_payload={
    "format": "json",
    "data": join_all(";")
}
```
By providing the `join_all` token, Incorporator automatically intercepts all 500 extracted VINs, joins them perfectly, and generates **one single, polite, highly-optimized network call**. It automatically translates it to Form-Data (`application/x-www-form-urlencoded`) via `payload_type="form"`.

### 4. $O(1)$ Graph Relational Lookups
We didn't need to write a messy dictionary merge algorithm to join Jimmy's records with the Government records. Because we set `inc_code="VIN"` when parsing the NHTSA response, the data was securely cached in memory. 

In Phase 3, we retrieved the federal specs in $O(1)$ time by simply querying the registry: `govt_specs.inc_dict.get(jimmy_vin)`.

---

## 🐳 Run it from the CLI

`join_all(";")` itself is JSON-expressible — the CLI's text-token resolver
will turn `"join_all(\";\")"` into a real callable at load time. What
still requires an `outflow.py` here is the **two-phase chain** (Phase 1's
`invoices` becomes Phase 2's `inc_parent`, and Phase 3 reconciles the two
registries). That's the natural fjord shape: each source is its own
`stream_params` entry, and the `outflow(state)` function runs the
reconciliation across both in-memory registries:

```json
{
  "outflow": "audit_jimmy.py",
  "stream_params": [
    {
      "cls_name": "Invoice",
      "incorp_params": {
        "inc_file": "jimmy_ledger.xml",
        "rec_path": "Dealership.AuditFile.Invoices.Invoice",
        "inc_code": "id",
        "inc_child": "Vehicle.VIN"
      }
    }
  ],
  "export_params": {"file_path": "data/jimmy_audit.ndjson"}
}
```

```bash
incorporator validate pipeline.json
incorporator fjord pipeline.json
```

`audit_jimmy.py` defines the `Invoice` and `NHTSASpec` classes, and the `outflow(state)` function that issues the bulk POST with `join_all(";")`, then reconciles each invoice VIN against the federal registry in O(1). See [`examples/fjord_code/outflow_example.py`](../examples/fjord_code/outflow_example.py) for the pattern and [the CLI guide](./cli_and_configuration.md) for the full schema.