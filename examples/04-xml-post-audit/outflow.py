"""Outflow sidecar for examples/04-xml-post-audit/pipeline.json.

fjord's outflow(state) has no JSON primitive for a second, dependent
incorp() call, so this sidecar performs the NHTSA batch-POST phase in
plain Python and returns the reconciled rows. outflow(state) runs off the
main event loop, so it's free to open its own loop via asyncio.run() --
the host throttle registered below still applies correctly.
"""

import asyncio
from typing import Any

from incorporator import Incorporator, register_host_penstock
from incorporator.schema.converters import calc
from incorporator.schema.extractors import join_all, pluck

# Pace NHTSA vPIC at 1.5 req/sec (90/min — under the 100-200/min ceiling).
register_host_penstock("vpic.nhtsa.dot.gov", rate_per_sec=1.5)

# Referenced from pipeline.json's Invoice entry via the "@INVOICE_CONV_DICT"
# sigil -- pluck(chain=str.upper) can't be expressed as a JSON call-grammar
# token (str.upper is attribute access, rejected by the safe-eval walker).
INVOICE_CONV_DICT = {
    "jimmy_vin": pluck("Vehicle.VIN"),
    "jimmy_make": pluck("Vehicle.Make", chain=str.upper),
    "jimmy_model": pluck("Vehicle.Model", chain=str.upper),
}

NHTSA_CONV_DICT = {
    "true_make": calc(str.upper, "Make", default="UNKNOWN", target_type=str),
    "true_model": calc(str.upper, "Model", default="UNKNOWN", target_type=str),
}


class Invoice(Incorporator):
    pass


class NHTSASpec(Incorporator):
    pass


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Bulk-POST invoices["Invoice"] to NHTSA, then reconcile VIN by VIN."""
    invoices = state["Invoice"]
    govt_specs = asyncio.run(
        NHTSASpec.incorp(
            inc_url="https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
            inc_parent=invoices,
            http_method="POST",
            payload_type="form",
            form_payload={"format": "json", "data": join_all(";")},
            rec_path="Results",
            inc_code="VIN",
            conv_dict=NHTSA_CONV_DICT,
        )
    )
    rows = []
    for inv in invoices:
        true_spec = govt_specs.inc_dict.get(inv.jimmy_vin)
        federal_make = true_spec.true_make if true_spec else None
        federal_model = true_spec.true_model if true_spec else None
        is_fraud = bool(true_spec) and inv.jimmy_make not in f"{federal_make} {federal_model}"
        rows.append(
            {
                "invoice_id": inv.inc_code,
                "vin": inv.jimmy_vin,
                "jimmy_make": inv.jimmy_make,
                "jimmy_model": inv.jimmy_model,
                "federal_make": federal_make,
                "federal_model": federal_model,
                "is_fraud": is_fraud,
            }
        )
    return rows
