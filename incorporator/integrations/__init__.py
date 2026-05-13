"""Optional third-party orchestration integrations (Prefect, Dagster, etc.).

Modules in this subpackage import their host orchestrator behind a
``try/except ImportError`` shield, so installing Incorporator without
the orchestrator never breaks ``import incorporator``.

- :mod:`.prefect` — Prefect ``@task`` / ``@flow`` wrappers around
  :class:`~incorporator.observability.logger.LoggedIncorporator` streams.

To add a new orchestrator (Dagster, Airflow, Temporal, Argo, …) drop a
new module next to ``prefect.py`` following the same shield pattern.
"""
