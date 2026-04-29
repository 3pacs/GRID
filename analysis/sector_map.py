"""GRID Sector Map — shim loader for ``analysis/sector_map_data.yaml``.

This module historically contained a 12,327-line Python literal with the full
``SECTOR_MAP`` (20 sectors / 262 subsectors / 3,533 actors) and
``JUNCTION_POINTS`` (23 cross-sector capital flow nodes). The data has been
extracted verbatim into ``analysis/sector_map_data.yaml`` so the dataset is
editable, diffable, and reusable from non-Python callers. This module now
exists purely to:

    1. Load the YAML file at import time.
    2. Re-expose ``SECTOR_MAP`` and ``JUNCTION_POINTS`` as module-level
       constants, preserving the historical
       ``from analysis.sector_map import SECTOR_MAP`` contract.
    3. Provide the original helper functions (``get_sector_features``,
       ``get_actor_influence``, ``get_all_sectors``,
       ``get_junction_points_for_sector``, ``get_junction_point``).

The YAML document has a thin wrapper:

    SECTOR_MAP:       { ... }
    JUNCTION_POINTS:  { ... }

Callers should treat the loaded dicts as byte-identical replacements for the
old Python literals — same shape, same keys, same values.

Loader uses ``yaml.CSafeLoader`` when libyaml is available (~150 ms cold) and
falls back to the pure-Python loader otherwise. The result is cached at the
module level, so the cost is paid exactly once per process.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

try:  # LibYAML C extension — ~10x faster than the pure-Python loader.
    from yaml import CSafeLoader as _SafeLoader  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - libyaml is standard on grid-svr
    from yaml import SafeLoader as _SafeLoader  # type: ignore[assignment]

_YAML_PATH = Path(__file__).parent / "sector_map_data.yaml"


def _load_sector_map_document() -> dict[str, Any]:
    """Read the YAML document once and return the top-level mapping."""
    with open(_YAML_PATH, "r", encoding="utf-8") as fh:
        doc = yaml.load(fh, Loader=_SafeLoader)
    if not isinstance(doc, dict):
        raise ValueError(
            f"sector_map_data.yaml: top-level must be a mapping, got {type(doc).__name__}"
        )
    if "SECTOR_MAP" not in doc or "JUNCTION_POINTS" not in doc:
        raise ValueError(
            "sector_map_data.yaml: missing required top-level keys "
            "'SECTOR_MAP' and/or 'JUNCTION_POINTS'"
        )
    return doc


_DOC = _load_sector_map_document()

#: Canonical hierarchical mapping: ``sector -> subsector -> [actors]``.
#:
#: Each actor has a ``weight`` representing their relative market-moving
#: influence within that subsector. ``features`` ties the actor back to the
#: feature registry so macro/market signals can be joined against them.
SECTOR_MAP: dict[str, dict] = _DOC["SECTOR_MAP"]

#: Cross-sector capital flow nodes (Fed balance sheet, TGA, BTC ETF flows,
#: etc.). These are the "junction points" where liquidity enters or exits the
#: system.
JUNCTION_POINTS: dict[str, dict] = _DOC["JUNCTION_POINTS"]


# -----------------------------------------------------------------------------
# Helper functions (preserved verbatim from the legacy module)
# -----------------------------------------------------------------------------


def get_sector_features(sector: str) -> list[str]:
    """Return all feature names relevant to a sector."""
    s = SECTOR_MAP.get(sector, {})
    features: list[str] = []
    for sub in s.get("subsectors", {}).values():
        for actor in sub.get("actors", []):
            features.extend(actor.get("features", []))
    return [f for f in features if f]


def get_actor_influence(sector: str) -> list[dict]:
    """Return all actors in a sector grouped by subsector, sorted within each group."""
    s = SECTOR_MAP.get(sector, {})
    actors: list[dict] = []
    for sub_name, sub in s.get("subsectors", {}).items():
        sub_weight = sub.get("weight", 1.0)
        sub_actors: list[dict] = []
        for actor in sub.get("actors", []):
            sub_actors.append(
                {
                    "name": actor["name"],
                    "subsector": sub_name,
                    "type": actor["type"],
                    "ticker": actor.get("ticker"),
                    "influence": round(sub_weight * actor["weight"], 4),
                    "description": actor.get("description", ""),
                    "features": actor.get("features", []),
                }
            )
        # Sort within subsector by influence
        sub_actors.sort(key=lambda a: a["influence"], reverse=True)
        actors.extend(sub_actors)
    return actors


def get_all_sectors() -> list[str]:
    return list(SECTOR_MAP.keys())


def get_junction_points_for_sector(sector: str) -> list[str]:
    """Return junction point IDs relevant to a sector."""
    s = SECTOR_MAP.get(sector, {})
    return list(s.get("junction_points", []))


def get_junction_point(junction_id: str) -> dict | None:
    """Return a junction point config by ID."""
    return JUNCTION_POINTS.get(junction_id)


__all__ = [
    "SECTOR_MAP",
    "JUNCTION_POINTS",
    "get_sector_features",
    "get_actor_influence",
    "get_all_sectors",
    "get_junction_points_for_sector",
    "get_junction_point",
]
