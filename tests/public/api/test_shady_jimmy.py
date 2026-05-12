"""Integration tests for XML parsing and Bulk POST Requests (NHTSA API)."""

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from incorporator import Incorporator
from incorporator.io import fetch
from incorporator.schema.converters import inc


# --- EXPLICIT SUBCLASSING ---
class JimmyInvoice(Incorporator):
    pass


class NHTSARecord(Incorporator):
    pass


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

    monkeypatch.setattr(fetch, "execute_request", mock_nhtsa_execute_post)

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
    invoices = await JimmyInvoice.incorp(inc_file=str(xml_file), rec_path="Dealership.AuditFile.Invoices.Invoice")

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
        conv_dict={"ModelYear": inc(int)},
    )

    assert isinstance(live_records, list)
    assert len(live_records) == 3

    # 4. Assert the Relational Audit Logic — per-invoice, per-field
    def audit(vin: str) -> tuple:
        """Return (claimed_make, claimed_year, actual_make, actual_year) for a VIN."""
        invoice = next(inv for inv in invoices if getattr(inv.Vehicle, "VIN", "") == vin)
        real_car = live_records.inc_dict.get(vin)
        assert real_car is not None, f"NHTSA returned no record for VIN {vin}"
        claimed_make = getattr(invoice.Vehicle, "Make", "").title()
        claimed_year = int(getattr(invoice.Vehicle, "Year", 0))
        actual_make = getattr(real_car, "Make", "").title()
        actual_year = getattr(real_car, "ModelYear", 0)
        assert isinstance(actual_year, int), f"ModelYear should be int after conv_dict, got {type(actual_year)}"
        return claimed_make, claimed_year, actual_make, actual_year

    # INV-001: Honda Accord 2003 — make and year both match → clean
    cm, cy, am, ay = audit("1HGCM82633A004352")
    assert cm == am, f"INV-001 make mismatch: claimed {cm!r}, actual {am!r}"
    assert cy == ay, f"INV-001 year mismatch: claimed {cy}, actual {ay}"

    # INV-002: claimed 2024 Toyota, NHTSA says 2004 → year fraud
    cm, cy, am, ay = audit("2T1BR32E54C123456")
    assert cm == am, f"INV-002 make should match: claimed {cm!r}, actual {am!r}"
    assert cy != ay, f"INV-002 year should mismatch (claimed 2024, actual 2004)"
    assert cy == 2024 and ay == 2004

    # INV-003: claimed Porsche 911 GT3, NHTSA says Chevrolet Volt → make fraud
    cm, cy, am, ay = audit("1G1RC6E45BU000003")
    assert cm != am, f"INV-003 make should mismatch (claimed {cm!r}, actual {am!r})"
    assert cm == "Porsche" and am == "Chevrolet"
