"""GRID Signal Adapter — Sector Networks. Actor density + concentration signals from 10 sector modules."""

from __future__ import annotations
import hashlib
from datetime import datetime, timedelta, timezone
from loguru import logger as log
from intelligence.signal_registry import RegisteredSignal, SignalType
from sqlalchemy.engine import Engine

_REFRESH = 24.0

def _sid(*p): return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]
def _now(): return datetime.now(timezone.utc)
def _clamp(v, lo=0.0, hi=1.0): return max(lo, min(hi, v))

# Map module -> (import_path, dict_name, sector_label)
_SECTOR_MODULES = [
    ("intelligence.defense_contractors", "DEFENSE_CONTRACTOR_NETWORK", "defense"),
    ("intelligence.pharma_network", "PHARMA_POWER_NETWORK", "pharma"),
    ("intelligence.swf_network", "SWF_INTELLIGENCE", "sovereign_wealth"),
    ("intelligence.banking_network", "BANKING_NETWORK", "banking"),
    ("intelligence.energy_network", "ENERGY_NETWORK", "energy"),
    ("intelligence.tech_monopoly_network", "TECH_MONOPOLY_NETWORK", "tech"),
    ("intelligence.real_estate_network", "REAL_ESTATE_NETWORK", "real_estate"),
    ("intelligence.commodities_agriculture_network", "COMMODITIES_AGRICULTURE_NETWORK", "commodities"),
    ("intelligence.defi_protocols", "DEFI_PROTOCOLS", "defi"),
    ("intelligence.media_network", "MEDIA_NETWORK", "media"),
]


def _count_actors(network: dict) -> int:
    """Recursively count actor entries in a nested network dict."""
    count = 0
    if isinstance(network, dict):
        for k, v in network.items():
            if isinstance(v, dict):
                if "name" in v or "ticker" in v or "influence" in v:
                    count += 1
                count += _count_actors(v)
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict) and ("name" in item or "ticker" in item):
                        count += 1
                    elif isinstance(item, dict):
                        count += _count_actors(item)
    return count


def _extract_tickers(network: dict) -> list[str]:
    """Extract all ticker symbols from a nested network dict."""
    tickers = []
    if isinstance(network, dict):
        for k, v in network.items():
            if k == "ticker" and isinstance(v, str) and v:
                tickers.append(v.upper())
            elif isinstance(v, dict):
                tickers.extend(_extract_tickers(v))
            elif isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        tickers.extend(_extract_tickers(item))
    return list(set(tickers))


class SectorNetworkAdapter:
    @property
    def source_module(self): return "sector_network"
    @property
    def refresh_interval_hours(self): return _REFRESH

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now()
        vu = now + timedelta(hours=24)
        signals = []

        for mod_path, dict_name, sector in _SECTOR_MODULES:
            try:
                mod = __import__(mod_path, fromlist=[dict_name])
                network = getattr(mod, dict_name, {})
                if not isinstance(network, dict) or not network:
                    continue

                actor_count = _count_actors(network)
                tickers = _extract_tickers(network)
                subsectors = len([k for k in network.keys() if isinstance(network[k], dict)])

                src = f"sector_network:{sector}"

                # MAGNITUDE: sector actor density
                signals.append(RegisteredSignal(
                    signal_id=_sid(src, "density", sector, str(now.date())),
                    source_module=src, signal_type=SignalType.MAGNITUDE,
                    ticker=None, direction="neutral",
                    value=float(actor_count), z_score=None,
                    confidence=_clamp(min(actor_count / 50, 1.0)),
                    valid_from=now, valid_until=vu, freshness_hours=0.0,
                    metadata={"sector": sector, "actor_count": actor_count,
                              "subsector_count": subsectors, "tickers": tickers[:20]},
                    provenance=f"sector_network:{sector}:density",
                ))
            except Exception as e:
                log.debug("sector_network_adapter: {s} failed - {e}", s=sector, e=e)

        log.info("sector_network_adapter: {n} signals from {m} sectors",
                 n=len(signals), m=len(_SECTOR_MODULES))
        return signals
