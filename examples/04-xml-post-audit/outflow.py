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

**Identity safety, and why this arrangement is required, not cosmetic.**
This file gets ``exec_module``'d under the CLI's default ``name_hint``
(``incorporator/usercode.py``'s ``load_user_module``), invoked once for
``merge_sidecar_extra_names`` (token/``cls_name`` resolution) and again
for :meth:`Incorporator.fjord`'s own load -- both share the same cache
key here, so there's only one exec in this daemon's case. Importing
``Invoice``/``NHTSASpec`` (rather than redefining them) still matters:
Python's own ``sys.modules['nhtsa_post_audit']`` cache guarantees this
module and ``nhtsa_post_audit.py``'s own code always see the SAME
canonical class objects, with no risk of drift between the two files'
conv_dicts.

**The one gap this file works around.** ``load_user_module`` does not add
this file's own parent directory to ``sys.path`` before running it (unlike
``python <script>.py``, which auto-prepends the script's directory) -- the
``sys.path.insert`` below is required, guarded against a double insert.
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
