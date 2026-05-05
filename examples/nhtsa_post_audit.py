"""
Declarative Bulk POST Enrichment: Auditing "Shady Jimmy"
--------------------------------------------------------
This example demonstrates how to parse a deeply nested XML file and use
Incorporator's dynamic POST capabilities to batch-query a federal
government database (NHTSA) to verify the records.

It highlights:
1. Zero-Boilerplate XML parsing.
2. The Explicit `inc_child` State Carrier.
3. Declarative Bulk POST execution using the `join_all` token.
4. O(1) Memory lookups using the internal `.inc_dict` registry.
"""

import asyncio
from incorporator import Incorporator
from incorporator.methods.converters import join_all


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
            "data": join_all(";")
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