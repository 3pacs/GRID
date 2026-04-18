"""Canonical YAML loader for sector network data.

Replaces 10 hardcoded Python modules (`intelligence/<sector>_network.py`,
~18k LOC total) with YAML files under this package. The
`intelligence.adapters.sector_network_adapter` reads from here, as do any
other callers that need the raw sector actor graph.

YAML schema (uniform across sectors):

    sector: <sector_label>
    source_module: <legacy .py module basename, for traceability>
    export_name: <legacy top-level dict name, for traceability>
    data: { ... }        # the original network dict (or list, for defi)

Only `sector` and `data` are load-bearing. Callers should treat `data` as
the exact equivalent of the old module-level constant (same shape, same
values).
"""

from __future__ import annotations

import os
import time
from typing import Any

import yaml

_DIR = os.path.dirname(os.path.abspath(__file__))
_CACHE_TTL_SECONDS = 300.0  # 5 min; data is static but allow hot reload in dev

# (sector_label, legacy_module_path, legacy_export_name)
# Matches what `intelligence.adapters.sector_network_adapter` used to
# dynamic-import. The third tuple element is retained so any caller that
# keyed off the old export name still has a reference.
SECTOR_MODULES: list[tuple[str, str, str]] = [
    ("defense", "intelligence.defense_contractors", "DEFENSE_CONTRACTOR_NETWORK"),
    ("pharma", "intelligence.pharma_network", "PHARMA_POWER_NETWORK"),
    ("sovereign_wealth", "intelligence.swf_network", "SWF_INTELLIGENCE"),
    ("banking", "intelligence.banking_network", "BANKING_NETWORK"),
    ("energy", "intelligence.energy_network", "ENERGY_NETWORK"),
    ("tech", "intelligence.tech_monopoly_network", "TECH_MONOPOLY_NETWORK"),
    ("real_estate", "intelligence.real_estate_network", "REAL_ESTATE_NETWORK"),
    ("commodities", "intelligence.commodities_agriculture_network", "COMMODITIES_AGRICULTURE_NETWORK"),
    ("defi", "intelligence.defi_protocols", "DEFI_PROTOCOLS"),
    ("media", "intelligence.media_network", "MEDIA_NETWORK"),
]

_cache: dict[str, tuple[float, dict[str, Any]]] = {}


def _yaml_path(sector: str) -> str:
    return os.path.join(_DIR, f"{sector}.yaml")


def list_sectors() -> list[str]:
    """Return all sector labels that have a YAML file on disk.

    Order matches `SECTOR_MODULES` for the declared sectors; any extras
    (future additions) are appended alphabetically.
    """
    declared = [s for s, _, _ in SECTOR_MODULES if os.path.exists(_yaml_path(s))]
    extras = sorted(
        fn[:-5]
        for fn in os.listdir(_DIR)
        if fn.endswith(".yaml") and fn[:-5] not in {s for s, _, _ in SECTOR_MODULES}
    )
    return declared + extras


def load_sector_network(sector: str) -> dict[str, Any]:
    """Read `{sector}.yaml` and return the full wrapped document.

    Cached for `_CACHE_TTL_SECONDS`. Raises `FileNotFoundError` if the
    sector YAML does not exist.
    """
    now = time.monotonic()
    cached = _cache.get(sector)
    if cached is not None and (now - cached[0]) < _CACHE_TTL_SECONDS:
        return cached[1]

    path = _yaml_path(sector)
    if not os.path.exists(path):
        raise FileNotFoundError(f"No sector network YAML for '{sector}': {path}")

    with open(path, "r", encoding="utf-8") as fh:
        doc = yaml.safe_load(fh) or {}

    if not isinstance(doc, dict):
        raise ValueError(f"sector_networks/{sector}.yaml: top-level must be a mapping")

    _cache[sector] = (now, doc)
    return doc


def get_sector_data(sector: str) -> Any:
    """Return just the `data` field (the original module-level dict/list).

    This is the byte-identical replacement for the legacy
    `getattr(intelligence.<sector>_network, <EXPORT_NAME>)` call.
    """
    doc = load_sector_network(sector)
    return doc.get("data", {})


def get_actors(sector: str) -> list[dict]:
    """Flatten every actor-like entry (dict with 'name' or 'ticker') under `data`.

    Convenience helper mirroring the recursive walk used by the adapter.
    """
    data = get_sector_data(sector)
    out: list[dict] = []

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if "name" in obj or "ticker" in obj:
                out.append(obj)
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(data)
    return out


def clear_cache() -> None:
    """Drop cached YAML loads (useful for tests / hot reload)."""
    _cache.clear()
