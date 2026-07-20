"""Pure-store sidecar for the CLI form (``pipeline.json``).

``Invoice``/``NHTSASpec`` and the ``to_upper`` helper are defined ONCE, in
``nhtsa_post_audit.py``. This module only re-exports them (via a plain
import) plus the fjord's ``outflow(state)`` fusion hook -- there is no
JSON-declarable primitive for a second, dependent ``incorp()`` call keyed
on a first source's cached child path, so the NHTSA batch-POST phase
stays hand-written Python here, returning the reconciled rows.
``outflow(state)`` runs off the main event loop, so it's free to open its
own loop via ``asyncio.run()`` -- the host throttle registered in
``nhtsa_post_audit.py`` still applies correctly, since importing that
module runs its module-level ``register_host_penstock(...)`` call as a
side effect.

**Why the ``sys.path.insert`` below is still here.** Real framework/CLI
loads (``incorporator fjord run`` via :meth:`Incorporator.fjord`) go
through ``load_user_module``, which since ``e6ab772`` caches purely on
resolved file path, short-circuits to an already-running ``__main__``,
and auto-inserts each sidecar's own directory onto ``sys.path`` -- no
guard needed there. This file's guard survives only because
``incorporator fjord validate`` / ``incorporator tideweaver validate``
route ``pipeline.json`` through a *different*, bespoke loader
(``incorporator/cli/validate.py``'s ``_import_module``) that hand-rolls
its own ``importlib.util.spec_from_file_location`` call and never got
that fix. Until that validation path is unified onto
``load_user_module``, this sidecar needs its own sibling-dir insert to
survive ``incorporator fjord validate pipeline.json``.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

from nhtsa_post_audit import Invoice, NHTSASpec, to_upper  # noqa: E402

from incorporator.schema.converters import calc  # noqa: E402
from incorporator.schema.extractors import join_all  # noqa: E402

__all__ = ["Invoice", "NHTSASpec", "to_upper", "outflow"]


def outflow(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Bulk-POST ``state["Invoice"]`` to NHTSA, then reconcile VIN by VIN.

    ``incorporator fjord pipeline.json`` runs the fjord DAEMON path, so
    ``state["Invoice"]`` is a live ``IncorporatorList`` (has ``.inc_dict``) --
    distinct from a Tideweaver Fjord current's plain-list snapshots.
    """
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
            conv_dict={
                "true_make": calc(to_upper, "Make", default="UNKNOWN", target_type=str),
                "true_model": calc(to_upper, "Model", default="UNKNOWN", target_type=str),
            },
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
