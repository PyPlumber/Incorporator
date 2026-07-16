***

# Tutorial 4 — XML Post Audit: Federal VIN Fraud Detection

**Prerequisites:** [Tutorial 3 — Universal Formats](../03-universal-formats/README.md) (warehouse round-trip).

You're a state auditor. You've seized an XML ledger from **Shady Jimmy's Used Cars** and have 24 hours to flag every fraudulent VIN before he gets a tip-off and disappears. One `incorp()` call parses the ledger without a schema file. One batched POST hits NHTSA's federal VIN database. One O(1) join on VIN surfaces the mismatch.

Real-world compliance teams run this arc every day: banking against the SEC, healthcare against the FDA, vehicle resale against NHTSA. REST APIs aren't just `GET`; bulk endpoints (government databases, GraphQL, batch lookup services) often require `POST` with a dynamic payload mapping multiple records — and that's where this tutorial earns its slot.

## The Input Data (`jimmy_ledger.xml`)

Save the file below as `jimmy_ledger.xml`. The real ledger holds **10 invoices** (INV-001 through INV-010). The snippet shows the structure plus the fraudulent record at the end — `jimmy_ledger.xml` in this directory is the complete file.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Dealership name="Shady Jimmy's Used Cars" location="Baltimore, MD">
    <AuditFile generatedAt="2026-04-24T10:15:00Z" auditor="White Hat Auditors">
        <Invoices>

            <Invoice id="INV-001" status="completed">
                <SaleDate>2026-04-01</SaleDate>
                <Salesperson id="SP-100">Jimmy Carter</Salesperson>
                <Vehicle>
                    <VIN>1HGCM82633A004352</VIN>
                    <Make>Honda</Make>
                    <Model>Accord</Model>
                    <Year>2003</Year>
                </Vehicle>
            </Invoice>

            <Invoice id="INV-002" status="completed">
                <!-- ... INV-002 through INV-009 are legitimate records ... -->
            </Invoice>

            <!-- FRAUDULENT RECORD -->
            <Invoice id="INV-010" status="completed">
                <SaleDate>2026-04-10</SaleDate>
                <Salesperson id="SP-999">Jimmy Carter</Salesperson>
                <Vehicle>
                    <VIN>WP0AB2A94HS124053</VIN>
                    <Make>Honda</Make>
                    <Model>Civic</Model>
                    <Year>2015</Year>
                    <Mileage>12000</Mileage>
                </Vehicle>
                <Notes>Manager special discount</Notes>
            </Invoice>

        </Invoices>
    </AuditFile>
</Dealership>
```

> **The planted fraud.** VIN `WP0AB2A94HS124053` on INV-010 is listed as a Honda Civic sold for $499. NHTSA's batch decoder returns the true make — a Porsche. The join on VIN surfaces the mismatch on exactly one row. The other nine invoices check out.

---

## The Audit Script

Create `audit_jimmy.py` (the runnable version ships in this directory as `nhtsa_post_audit.py`). Incorporator auto-detects the `.xml` extension, drills through `rec_path`, and builds nested Python objects (`inv.Vehicle.VIN`) without a schema file — that's the schema-free ingestion path.

```python
import asyncio
from pathlib import Path

from incorporator import Incorporator, register_host_penstock
from incorporator.schema.converters import calc
from incorporator.schema.extractors import join_all, pluck

# Pace NHTSA vPIC at 1.5 req/sec (90/min — under the 100-200/min ceiling).
# register_host_penstock applies to all HTTP methods, including POST.
register_host_penstock("vpic.nhtsa.dot.gov", rate_per_sec=1.5)

HERE = Path(__file__).resolve().parent

# Build-time lift of the nested Vehicle.* fields Jimmy's ledger buries three
# levels deep — pluck(chain=str.upper) drills + normalizes in one pass, so the
# report loop below reads plain attributes instead of a getattr pyramid.
INVOICE_CONV_DICT = {
    "jimmy_vin": pluck("Vehicle.VIN"),
    "jimmy_make": pluck("Vehicle.Make", chain=str.upper),
    "jimmy_model": pluck("Vehicle.Model", chain=str.upper),
}

# calc() is used here because uppercasing is a FUNCTION applied to the source
# value, not a type coercion — calc(str.upper, "Make", ...) would still be the
# right call even if the output key were "Make" (same as the source key).
NHTSA_CONV_DICT = {
    "true_make": calc(str.upper, "Make", default="UNKNOWN", target_type=str),
    "true_model": calc(str.upper, "Model", default="UNKNOWN", target_type=str),
}


class Invoice(Incorporator):
    pass


class NHTSASpec(Incorporator):
    pass


async def run_audit() -> None:
    print("Parsing Shady Jimmy's Local XML Ledger...")

    invoices = await Invoice.incorp(
        inc_file=HERE / "jimmy_ledger.xml",
        rec_path="Dealership.AuditFile.Invoices.Invoice",
        inc_code="id",
        # inc_child caches the VIN path on the list for the enrichment call below.
        inc_child="Vehicle.VIN",
        conv_dict=INVOICE_CONV_DICT,
    )

    print(f"Extracted {len(invoices)} invoices. Contacting Federal Databases...")

    # Incorporator reads the cached inc_child path, extracts every VIN,
    # joins them with the delimiter, and issues one network call —
    # regardless of ledger size.
    govt_specs = await NHTSASpec.incorp(
        inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
        inc_parent=invoices,
        http_method="POST",
        payload_type="form",
        form_payload={
            "format": "json",
            "data": join_all(";"),
        },
        rec_path="Results",
        inc_code="VIN",
        conv_dict=NHTSA_CONV_DICT,
    )

    print(f"Government data received for {len(govt_specs)} vehicles. Running audit...\n")

    print("=" * 85)
    print(f"{'INVOICE':<10} | {'VIN':<18} | {'JIMMY LISTED':<20} | {'NHTSA TRUE SPEC':<25}")
    print("=" * 85)

    fraud_count = 0

    for inv in invoices:
        jimmy_claim = f"{inv.jimmy_make} {inv.jimmy_model}"

        # Honest read-time boundary: NHTSASpec doesn't exist until AFTER Invoice
        # is fully built (the POST enrichment is a second network phase keyed on
        # invoices as inc_parent) — the VIN join is inherently read-time, and
        # .inc_dict.get() IS the O(1) lookup this tutorial demonstrates.
        true_spec = govt_specs.inc_dict.get(inv.jimmy_vin)

        if true_spec:
            federal_claim = f"{true_spec.true_make} {true_spec.true_model}"
        else:
            federal_claim = "API OFFLINE / UNKNOWN"

        row = f"{inv.inc_code:<7} | {inv.jimmy_vin:<18} | {jimmy_claim:<20} | {federal_claim:<25}"
        if inv.jimmy_make not in federal_claim and federal_claim != "API OFFLINE / UNKNOWN":
            print(f"FRAUD  {row}")
            fraud_count += 1
        else:
            print(f"OK     {row}")

    print("=" * 85)
    if fraud_count > 0:
        print(f"AUDIT FAILED: {fraud_count} fraudulent transaction(s). Dispatching authorities.")
    else:
        print("AUDIT PASSED: Ledger matches federal records.")


if __name__ == "__main__":
    asyncio.run(run_audit())
```

---

## Framework Highlights

### 1. Schema-Free XML Parsing

Parsing XML in standard Python requires `xml.etree.ElementTree` and recursive loops. `incorp()` auto-detects the `.xml` extension, drills through `rec_path`, and returns nested Python objects (`inv.Vehicle.VIN`) with no class definitions or schema files.

**Security:** every XML payload runs through a pre-flight regex that blocks DTDs and external entities before any parser sees the bytes (`incorporator/io/formats.py:358` — `check_xml_security`). When lxml is installed (`pip install incorporator[speedups]`), the XMLParser uses `resolve_entities=False, no_network=True` as a second layer. The combined approach rejects XXE and billion-laughs payloads regardless of which parser is active — relevant for compliance pipelines ingesting ledgers from untrusted sources.

### 2. Shape Stability with `xml_force_list`

XML collapses a single child element into a dict, but returns multiple children as a list. A ledger with one invoice would produce `dict`; a ledger with ten produces `list[dict]`. Passing `xml_force_list=["Invoice"]` forces the tag to always be a list, preventing downstream shape drift when ledger size varies (`incorporator/io/handlers/text.py:291`). Not needed for the 10-invoice ledger shipped in this directory (already parses as a list), so the code block above omits it — add it if you swap in a ledger that might ever hold exactly one invoice.

### 3. The State Carrier (`inc_child`)

Declaring `inc_child="Vehicle.VIN"` on the first call caches that dot-notation path on the returned `invoices` list. When `invoices` is passed as `inc_parent` to the NHTSA call, Incorporator drills into every invoice, extracts VINs, and passes them to `join_all(";")` — no boilerplate loops.

### 4. Declarative Bulk POST (`join_all`)

The NHTSA vPIC batch endpoint expects a semicolon-delimited string of VINs. The `join_all(";")` token extracts all VINs, joins them with the delimiter, and issues one network call — regardless of ledger size. No per-VIN requests, no manual string assembly:

```python
form_payload={
    "format": "json",
    "data": join_all(";"),
}
```

The `payload_type="form"` flag encodes the payload as `application/x-www-form-urlencoded`.

### 5. Rate Control with `register_host_penstock`

`register_host_penstock` attaches a `SustainedPenstock` to any hostname. It applies to all HTTP verbs — GET and POST — so the NHTSA vPIC host is paced at 1.5 req/sec with one declaration at module import time. This is the same Penstock primitive used to pace edges inside a Tideweaver window, making rate-control skills from this tutorial transferable to multi-source orchestration.

### 6. O(1) Join via `inc_dict`

No dictionary merge algorithm needed. Setting `inc_code="VIN"` when parsing the NHTSA response caches each spec by VIN in memory. The join is a single dict lookup:

```python
true_spec = govt_specs.inc_dict.get(jimmy_vin)
```

---

## Run it

```bash
python examples/04-xml-post-audit/nhtsa_post_audit.py
```

The full audit — including the NHTSA batch-POST reconciliation — also runs
from the CLI via `incorporator fjord pipeline.json` (see
[`pipeline.json`](pipeline.json) + [`outflow.py`](outflow.py)) and in
Docker via the mount pattern at
[../README.md](../README.md#running-a-tutorial-in-docker) (Docker: not
run or verified). Verified live: the CLI form reproduces the same single
fraud hit as the Python entry — INV-010, Porsche 911 flagged under a
Honda Civic listing.

---

## Where to Go Next

> **Up next: [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md).** T4 enriched a flat ledger via one batched POST; T5 introduces the canonical fan-out pattern — `inc_parent` + `inc_child` against a parent list with N children per row.

| Goal | Read |
|---|---|
| Canonical parent-child fan-out intro | [Tutorial 5 — Parent-Child Drilling](../05-parent-child-drilling/README.md) |
| Apply parent-child to a single-instance drill (different vertical) | [Tutorial 6 — State Sports](../06-state-sports/README.md) |
| Fuse audit output into a multi-source pipeline | [Tutorial 10 — Multi-Source Fjord](../10-multi-source-fjord/README.md) |
| Stream a giant XML feed with chunking | [Streaming & Pagination Deep Dive](../../docs/streaming_and_pagination.md) |
| Ship the bulk-POST workflow as a daemon | [Tutorial 8 — Streaming Daemons](../08-streaming-daemon/README.md) |

---

**Have a suggestion or hitting a snag?**
[Edit this page on GitHub](https://github.com/PyPlumber/incorporator/edit/main/examples/04-xml-post-audit/README.md) ·
[Report an issue](https://github.com/PyPlumber/incorporator/issues/new/choose) ·
[Browse open issues](https://github.com/PyPlumber/incorporator/issues)
