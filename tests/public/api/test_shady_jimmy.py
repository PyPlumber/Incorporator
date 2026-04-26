"""Integration tests for XML parsing and Bulk POST Requests (NHTSA API)."""

import json
from typing import Any
from pathlib import Path

import asyncio
import os

import httpx
import pytest

from incorporator import Incorporator
from incorporator.methods.converters import inc
from real.shady_jimmy import generate_xml_file


# --- EXPLICIT SUBCLASSING ---
class JimmyInvoice(Incorporator): pass


class NHTSARecord(Incorporator): pass


# --- MOCK NETWORK SETUP ---
async def mock_nhtsa_execute_post(url: str, *args: Any, **kwargs: Any) -> httpx.Response:
    """Mocks the NHTSA Bulk POST endpoint."""
    if "DecodeVINValuesBatch" in url:
        form_data = kwargs.get("form_payload", {})
        vins_string = form_data.get("DATA", "")
        vins = vins_string.split(";")

        results = []
        for vin in vins:
            if vin == "1HGCM82633A004352":  # Updated Honda VIN
                results.append({"VIN": vin, "Make": "HONDA", "Model": "ACCORD", "ModelYear": "2003"})
            elif vin == "2T1BR32E54C123456":  # Updated Toyota VIN
                results.append({"VIN": vin, "Make": "TOYOTA", "Model": "COROLLA", "ModelYear": "2004"})
            elif vin == "1G1RC6E45BU000003":
                # The real car is a Chevy Volt, not a Porsche!
                results.append({"VIN": vin, "Make": "CHEVROLET", "Model": "VOLT", "ModelYear": "2011"})

        payload = {"Results": results}
        req = httpx.Request("POST", url)
        return httpx.Response(200, text=json.dumps(payload), request=req)

    return httpx.Response(404, text="{}")


# --- TESTS ---
@pytest.mark.asyncio
async def test_shady_jimmy_audit(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Proves XML parsing, nested attribute extraction, and Bulk POST requests."""

    monkeypatch.setattr("incorporator.methods.network.execute_request", mock_nhtsa_execute_post)

    # 1. Create the mock XML file in an isolated temp directory
    xml_file = tmp_path / "shady_jimmy.xml"
    xml_data = """<?xml version="1.0" encoding="UTF-8"?>
    <Dealership>
        <AuditFile>
            <Invoices>
                <Invoice id="INV-001">
                    <Vehicle>
                        <VIN>1HGCM82633A004352</VIN>
                        <Make>Honda</Make>
                        <Model>Accord</Model>
                        <Year>2003</Year>
                    </Vehicle>
                    <Financial>
                        <SalePrice currency="USD">28500.00</SalePrice>
                    </Financial>
                </Invoice>
                <Invoice id="INV-002">
                    <Vehicle>
                        <VIN>2T1BR32E54C123456</VIN>
                        <Make>Toyota</Make>
                        <Model>Corolla</Model>
                        <Year>2024</Year>
                    </Vehicle>
                    <Financial>
                        <SalePrice currency="USD">24000.00</SalePrice>
                    </Financial>
                </Invoice>
                <!-- The Fraudulent Entry -->
                <Invoice id="INV-003">
                    <Vehicle>
                        <VIN>1G1RC6E45BU000003</VIN>
                        <Make>Porsche</Make>
                        <Model>911 GT3</Model>
                        <Year>2024</Year>
                    </Vehicle>
                    <Financial>
                        <SalePrice currency="USD">185000.00</SalePrice>
                    </Financial>
                </Invoice>
            </Invoices>
        </AuditFile>
    </Dealership>
    """

    xml_file.write_text(xml_data, encoding="utf-8")

    # 2. Extract XML
    invoices = await JimmyInvoice.incorp(
        inc_file=str(xml_file),
        rec_path="Dealership.AuditFile.Invoices.Invoice"
    )

    assert isinstance(invoices, list)
    assert len(invoices) == 3

    # 3. Execute Bulk POST
    vin_list = [getattr(inv.Vehicle, "VIN", "") for inv in invoices]
    vin_batch_string = ";".join(vin_list)

    live_records = await NHTSARecord.incorp(
        inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
        method="POST",
        form_payload={"format": "json", "DATA": vin_batch_string},
        rec_path="Results",
        inc_code="VIN",
        conv_dict={
            "ModelYear": inc(int)
        }
    )

    assert isinstance(live_records, list)
    assert len(live_records) == 3

    # 4. Assert the Relational Audit Logic
    fraud_count = 0
    for invoice in invoices:
        vin = getattr(invoice.Vehicle, "VIN", "")
        claimed_make = getattr(invoice.Vehicle, "Make", "Unknown").title()
        claimed_year = int(getattr(invoice.Vehicle, "Year", 0))

        real_car = live_records.inc_dict.get(vin)
        assert real_car is not None

        actual_make = getattr(real_car, "Make", "Unknown").title()
        actual_year = getattr(real_car, "ModelYear", 0)

        assert isinstance(actual_year, int)

        if claimed_make != actual_make or claimed_year != actual_year:
            fraud_count += 1

    # INV-002 (Wrong Year) and INV-003 (Fake Porsche) are fraud!
    assert fraud_count == 2  # <--- CHANGED FROM 1 TO 2

    print("🚨 INITIATING SHADY JIMMY AUDIT 🚨\n")

    xml_file = "shady_jimmy.xml"
    generate_xml_file(xml_file)

    # ==========================================
    # STEP 2: Live Bulk Enrichment (POST Request)
    # ==========================================
    print("2. Contacting US Dept of Transportation (NHTSA VPIC API Bulk Endpoint)...")

    # Extract all VINs and format them for the NHTSA Bulk API
    vin_list = [getattr(inv.Vehicle, "VIN", "") for inv in invoices]
    vin_batch_string = ";".join(vin_list)

    # Showcasing method="POST" and form_payload
    live_records = await NHTSARecord.incorp(
        inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
        method="POST",
        form_payload={"format": "json", "DATA": vin_batch_string},
        rec_path="Results",
        inc_code="VIN",
        conv_dict={
            # The API returns years as strings, we force them to integers
            "ModelYear": inc(int)
        }
    )
    print("   -> Background Checks Complete (1 Optimized Request).\n")

    # ==========================================
    # STEP 3: Relational Audit
    # ==========================================
    print("=====================================================================")
    print("                       OFFICIAL AUDIT REPORT                         ")
    print("=====================================================================")

    fraud_count = 0

    for invoice in invoices:
        # Safely navigate dynamic nested XML attributes
        inv_id = getattr(invoice, "id", "UNKNOWN")
        vin = getattr(invoice.Vehicle, "VIN", "")
        claimed_make = getattr(invoice.Vehicle, "Make", "Unknown").title()
        claimed_model = getattr(invoice.Vehicle, "Model", "Unknown").title()
        claimed_year = int(getattr(invoice.Vehicle, "Year", 0))

        # XML tags with attributes return as objects containing 'text'
        price_obj = invoice.Financial.SalePrice
        price = getattr(price_obj, "text", price_obj)

        # Relational Lookup: Match XML to Live JSON instantly via class registry
        real_car = live_records.inc_dict.get(vin)

        if not real_car:
            print(f"[ERROR] Could not retrieve NHTSA data for VIN: {vin}")
            continue

        actual_make = getattr(real_car, "Make", "Unknown").title()
        actual_model = getattr(real_car, "Model", "Unknown").title()
        actual_year = getattr(real_car, "ModelYear", 0)

        # Business Logic: Check for discrepancies
        is_fraud = (
                claimed_make != actual_make or
                claimed_year != actual_year
        )

        if is_fraud:
            fraud_count += 1
            print(f"❌ FRAUD DETECTED (Invoice: {inv_id} | VIN: {vin})")
            print(f"   Jimmy Claims : {claimed_year} {claimed_make} {claimed_model} (${price})")
            print(f"   Actual Car   : {actual_year} {actual_make} {actual_model}")
            print("-" * 69)
        else:
            print(f"✅ VERIFIED (Invoice: {inv_id}): {actual_year} {actual_make}")

    print(f"\nAUDIT COMPLETE. {fraud_count} Fraudulent invoices detected.")

    # Clean up the dummy file
    if os.path.exists(xml_file):
        os.remove(xml_file)