"""Example outflow code for examples/pipeline_fjord.json.

The fjord engine builds the dynamic output class from this file's stem
(snake_case → PascalCase), so this file's stem ``outflow_example`` yields
the output class ``OutflowExample``.
"""

from typing import Any, Dict, List

from incorporator import Incorporator


class SpaceXLaunch(Incorporator):
    """Latest SpaceX launch (single object endpoint)."""

    pass


class SpaceXRocket(Incorporator):
    """SpaceX rocket catalogue (list endpoint, keyed by rocket id)."""

    pass


def outflow(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Join the latest launch with its matching rocket record.

    ``state["SpaceXLaunch"]`` is the latest-launch instance (single object).
    ``state["SpaceXRocket"]`` is a list with an ``inc_dict`` keyed by
    rocket id. We look the launch's rocket up and emit one combined row.
    """
    launch = state["SpaceXLaunch"]
    rockets = state["SpaceXRocket"]

    # The /launches/latest endpoint returns a single object — Incorporator
    # wraps it in a single-element IncorporatorList. Normalise to one.
    if isinstance(launch, list):
        if not launch:
            return []
        launch = launch[0]

    rocket = rockets.inc_dict.get(getattr(launch, "rocket", None))
    if rocket is None:
        return []

    return [
        {
            "launch_id": launch.inc_code,
            "launch_name": getattr(launch, "name", None),
            "rocket_id": rocket.inc_code,
            "rocket_name": getattr(rocket, "name", None),
        }
    ]
