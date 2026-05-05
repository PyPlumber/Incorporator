***

# 🚨 Concurrent POST Enrichment: Auditing "Shady Jimmy"

REST APIs don’t just use `GET`. When querying bulk endpoints (like Government Databases or GraphQL), you often need to use `POST` and send a **dynamic payload** for every request. 

In this tutorial, you play the role of a State Auditor. You have seized an XML ledger from **"Shady Jimmy's Used Cars"**. Your job is to extract his vehicle inventory from the local XML file and concurrently `POST` those VINs to the official **NHTSA** database to verify Jimmy isn't selling fraudulent cars.

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

Create a file called `audit_jimmy.py`. We are going to use Incorporator's native XML parsing to load the ledger, inject a static URL into the objects, define a dynamic `POST` payload closure, and bulk-enrich the data.

```python
import asyncio
from typing import Any, Dict
from incorporator import Incorporator, inc

# ==========================================
# 1. DEFINE OUR OBJECTS
# ==========================================
class Invoice(Incorporator):
    pass

class NHTSASpec(Incorporator):
    pass

# ==========================================
# 2. DYNAMIC POST PAYLOAD BUILDER
# ==========================================
def build_nhtsa_payload(invoice_obj: Any) -> Dict[str, Any]:
    """Dynamically builds the POST body for NHTSA based on each invoice."""
    vin = getattr(invoice_obj.Vehicle, "VIN", "UNKNOWN") if hasattr(invoice_obj, "Vehicle") else "UNKNOWN"

    return {
        # 1. MUST be lowercase "data" for Form-Encoded requests
        # 2. Append a semicolon so the NHTSA batch endpoint parses it correctly
        "data": f"{vin};",
        "format": "json"
    }

async def run_audit():
    print("📂 Parsing Shady Jimmy's Local XML Ledger...")

    # ==========================================
    # PHASE 1: Ingest the XML File
    # ==========================================
    invoices = await Invoice.incorp(
        inc_file="jimmy_ledger.xml",
        rec_path="Dealership.AuditFile.Invoices.Invoice",
        inc_code="id",

        # 🛡️ Declarative Static Injection:
        # "detail_url" doesn't exist in the XML. `inc` safely catches the missing
        # value and injects the NHTSA URL into every single object natively!
        conv_dict={
            "detail_url": inc(str, default="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/")
        }
    )

    print(f"✅ Extracted {len(invoices)} Invoices. Contacting Federal Databases...")

    # ==========================================
    # PHASE 2: Concurrent POST Enrichment
    # ==========================================
    # Incorporator reads the `detail_url` from the Invoices, fires concurrent POST
    # requests to NHTSA using our payload builder, and returns the deep specs.
    govt_specs = await NHTSASpec.incorp(
        inc_parent=invoices,
        http_method="POST",
        payload_type="form",                  # Tell httpx to send application/x-www-form-urlencoded
        payload_builder=build_nhtsa_payload,  # Inject our dynamic POST body closure
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

### 2. Declarative Static Injection (`inc` Defaults)
We need to tell the `invoices` objects *where* to route their HTTP traffic. Instead of hardcoding a Python `@property` inside the class, we use `conv_dict`. By mapping a missing key (`"detail_url"`) to `inc(str, default="...")`, Incorporator effortlessly injects the NHTSA URL into every single object as it is built.

### 3. Dynamic POST Concurrency (`payload_builder`)
When hitting a bulk API endpoint, you can't just send an empty `POST` request. By defining a `payload_builder` closure, Incorporator:
1. Passes the individual `Invoice` object into your function right before dispatch.
2. Injects the resulting dictionary as the body of the request.
3. Automatically translates it to Form-Data (`application/x-www-form-urlencoded`) via `payload_type="form"`.
4. Fires all requests concurrently and merges the JSON results.

### 4. $O(1)$ Graph Relational Lookups
We didn't need to write a messy dictionary merge algorithm to join Jimmy's records with the Government records. Because we set `inc_code="VIN"` when parsing the NHTSA response, the data was securely cached in memory. 

In Phase 3, we retrieved the federal specs in $O(1)$ time by simply querying the registry: `govt_specs.inc_dict.get(jimmy_vin)`.