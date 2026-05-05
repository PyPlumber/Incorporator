"""
Concurrent POST Enrichment: Auditing "Shady Jimmy"
--------------------------------------------------
This example demonstrates how to parse a deeply nested XML file and use
Incorporator's dynamic POST capabilities to concurrently query a federal
government database (NHTSA) to verify the records.

It highlights:
1. Zero-Boilerplate XML parsing.
2. Native URL injection via `inc()` defaults.
3. Concurrent POST requests with dynamically built Form-Data payloads.
4. O(1) Memory lookups using the internal `.inc_dict` registry.
"""

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

        # 1. What Jimmy claims he sold (From the XML)
        jimmy_vin = getattr(inv.Vehicle, "VIN", "UNKNOWN")
        jimmy_make = getattr(inv.Vehicle, "Make", "UNKNOWN").upper()
        jimmy_model = getattr(inv.Vehicle, "Model", "UNKNOWN").upper()
        jimmy_claim = f"{jimmy_make} {jimmy_model}"

        # 2. What the Government says it actually is (From the Memory Registry)
        # We instantly query the NHTSASpec registry using the VIN!
        true_spec = govt_specs.inc_dict.get(jimmy_vin)

        if true_spec:
            true_make = getattr(true_spec, "Make", "UNKNOWN").upper()
            true_model = getattr(true_spec, "Model", "UNKNOWN").upper()
            federal_claim = f"{true_make} {true_model}"
        else:
            federal_claim = "API OFFLINE / UNKNOWN"

        # 3. Detect Discrepancies
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