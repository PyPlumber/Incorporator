"""
Outflow sidecar for `examples/7_multi_source_fjord.py`.

The fjord engine imports this file at runtime, registers the two source
classes (SpaceXLaunch + SpaceXRocket), and calls `outflow(state)` on each
export wave to fuse them into a single row stream. The dynamic output
class is built from this file's stem — `launch_with_rocket.py` →
`LaunchWithRocket`.
"""

from typing import Any, Dict, List

from incorporator import Incorporator


class SpaceXLaunch(Incorporator):
    """Source A — the current SpaceX launch."""


class SpaceXRocket(Incorporator):
    """Source B — the full rocket catalogue, registered by id."""


def outflow(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Join rocket specs onto the current launch row.

    `state` is a snapshot of each source by class name, taken under the
    engine's shared lock. Return List[dict]; fjord handles the export.
    """
    launches = state["SpaceXLaunch"] or []
    rockets = state["SpaceXRocket"]

    rows = []
    for launch in launches:
        rocket = rockets.inc_dict.get(launch.rocket) if rockets else None
        rows.append({
            "id": launch.id,
            "name": launch.name,
            "rocket_name": getattr(rocket, "name", None) if rocket else None,
            "rocket_height_m": getattr(rocket.height, "meters", None) if rocket else None,
            "rocket_mass_kg": getattr(rocket.mass, "kg", None) if rocket else None,
            "rocket_success_pct": getattr(rocket, "success_rate_pct", None) if rocket else None,
        })
    return rows
