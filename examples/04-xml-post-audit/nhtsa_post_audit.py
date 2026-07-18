"""
Bulk POST Enrichment: Auditing "Shady Jimmy"
---------------------------------------------
Parses a deeply nested XML ledger, then batch-POSTs the VINs to a federal
government database (NHTSA) to verify each record against the truth.

Covers: schema-free XML parsing, the `inc_child` state carrier, a single
batched POST via `join_all`, and an O(1) `.inc_dict` join.

``Invoice``/``NHTSASpec`` and the ``to_upper`` helper are defined ONCE,
here. ``outflow.py`` (the sibling CLI sidecar for ``pipeline.json``)
re-exports them via a guarded ``sys.path.insert`` + plain import, rather
than redefining them, so both entry forms operate on the exact same
class objects and conv_dict logic (see ``outflow.py``'s docstring for
why that matters). This file never runs a Fjord/Watershed in-process --
``incorporator fjord pipeline.json`` is a separate CLI process that
imports FROM here, the same direction as
``examples/appendix/crypto-graph-mapping``.

Run with:
    python examples/04-xml-post-audit/nhtsa_post_audit.py
"""

import asyncio
from pathlib import Path

from incorporator import Incorporator, register_host_penstock
from incorporator.schema.converters import calc
from incorporator.schema.extractors import join_all, pluck

# Pace NHTSA vPIC at 1.5 req/sec (90/min — under the 100-200/min
# documented ceiling).  Method-agnostic: applies to both GET and POST.
register_host_penstock("vpic.nhtsa.dot.gov", rate_per_sec=1.5)

HERE = Path(__file__).resolve().parent


def to_upper(value: str) -> str:
    """Named wrapper for ``str.upper``. ``str.upper`` is attribute access,
    rejected by the JSON token grammar's safe-eval walker (see
    ``incorporator/cli/tokens.py``); a named module-level function resolves
    as a bare ``ast.Name`` instead. Shared by both entry forms so the same
    token name works in ``pipeline.json`` and here.
    """
    return value.upper()


class Invoice(Incorporator):
    """One dealership sale, parsed from Jimmy's local XML ledger."""


class NHTSASpec(Incorporator):
    """One federal VIN decode result from NHTSA's batch endpoint."""


async def run_audit() -> None:
    print("Parsing Shady Jimmy's Local XML Ledger...")

    # ==========================================
    # PHASE 1: Ingest the XML File
    # ==========================================
    # Build-time lift of the nested Vehicle.* fields Jimmy's ledger buries three
    # levels deep — pluck() drills, calc(to_upper) normalizes, so the report
    # loop below reads plain attributes instead of a getattr pyramid.
    invoices = await Invoice.incorp(
        inc_file=HERE / "jimmy_ledger.xml",
        rec_path="Dealership.AuditFile.Invoices.Invoice",
        inc_code="id",
        inc_child="Vehicle.VIN",
        conv_dict={
            "jimmy_vin": pluck("Vehicle.VIN"),
            "jimmy_make": calc(to_upper, "Vehicle.Make", target_type=str),
            "jimmy_model": calc(to_upper, "Vehicle.Model", target_type=str),
        },
    )

    print(f"OK: Extracted {len(invoices)} Invoices. Contacting Federal Databases...")

    # ==========================================
    # PHASE 2: Declarative Bulk POST Enrichment
    # ==========================================
    # Incorporator reads the cached `inc_child_path`, extracts every VIN,
    # and automatically joins them with semicolons into 1 Bulk Batch Request!
    # NHTSA's Results rows are already flat; calc() is required (not inc())
    # because the output key (true_make) differs from the source key (Make).
    govt_specs = await NHTSASpec.incorp(
        inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
        inc_parent=invoices,
        http_method="POST",
        payload_type="form",
        form_payload={"format": "json", "data": join_all(";")},
        rec_path="Results",
        inc_code="VIN",
        conv_dict={
            "true_make": calc(to_upper, "Make", default="UNKNOWN", target_type=str),
            "true_model": calc(to_upper, "Model", default="UNKNOWN", target_type=str),
        },
    )

    print(f"OK: Government Data Received for {len(govt_specs)} vehicles. Initiating Fraud Audit...\n")

    # ==========================================
    # PHASE 3: The Fraud Audit (O(1) Lookups)
    # ==========================================
    print("=" * 85)
    print(f"{'INVOICE':<10} | {'VIN':<18} | {'JIMMY LISTED':<20} | {'NHTSA TRUE SPEC':<25}")
    print("=" * 85)

    fraud_count = 0

    for inv in invoices:
        # 1. What Jimmy claims he sold (From the XML)
        jimmy_claim = f"{inv.jimmy_make} {inv.jimmy_model}"

        # 2. What the Government says it actually is (From the Memory Registry)
        # Honest read-time boundary: NHTSASpec doesn't exist until AFTER Invoice
        # is fully built (the POST enrichment is a second network phase keyed on
        # invoices as inc_parent) — the VIN join is inherently read-time, and
        # .inc_dict.get() IS the O(1) lookup this tutorial demonstrates.
        true_spec = govt_specs.inc_dict.get(inv.jimmy_vin)

        if true_spec:
            federal_claim = f"{true_spec.true_make} {true_spec.true_model}"
        else:
            federal_claim = "API OFFLINE / UNKNOWN"

        # 3. Detect Discrepancies
        row = f"{inv.inc_code:<7} | {inv.jimmy_vin:<18} | {jimmy_claim:<20} | {federal_claim:<25}"
        if inv.jimmy_make not in federal_claim and federal_claim != "API OFFLINE / UNKNOWN":
            print(f"FRAUD  {row}")
            fraud_count += 1
        else:
            print(f"OK     {row}")

    print("=" * 85)
    if fraud_count > 0:
        print(f"AUDIT FAILED: Discovered {fraud_count} fraudulent transaction(s). Dispatching authorities.")
    else:
        print("AUDIT PASSED: Ledger matches federal records.")


if __name__ == "__main__":
    asyncio.run(run_audit())
