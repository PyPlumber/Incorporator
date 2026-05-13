"""Developer-experience tools for Incorporator users.

This subpackage hosts modules whose job is to *help the developer*
rather than do production data work:

- :mod:`.inspector` — analyse a payload or a failure and print actionable
  configuration suggestions (used by ``incorp(__inspect=True)`` and
  :meth:`~incorporator.base.Incorporator.test`).

Plausible future homes for this subpackage:

- ``profiler``    — per-source timing and rate-limit reports
- ``schema_diff`` — detect schema drift between two ``incorp()`` runs
- ``exporter``    — emit a Pydantic-class scaffold from a sample payload

Tools may import freely from the rest of the package, but nothing in
:mod:`incorporator.base`, :mod:`incorporator.schema`, or
:mod:`incorporator.io` should import from ``tools`` — keep the
dependency one-directional.
"""
