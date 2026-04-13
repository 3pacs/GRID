"""GRID Signal Adapter — Sector Networks. Actor density + concentration signals.

Reads the 10 sector actor graphs from `intelligence/sector_networks/*.yaml`
via the canonical loader. Previously these lived as giant Python dict
literals in `intelligence/<sector>_network.py`; those modules were deleted
as part of Wave 4 of the module dedupe plan. The public API of this
adapter (`SectorNetworkAdapter.extract_signals`) is unchanged — same
signal set, same IDs, same metadata.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone

from loguru import logger as log
from sqlalchemy.engine import Engine

from intelligence.sector_networks.loader import SECTOR_MODULES, get_sector_data
from intelligence.signal_registry import RegisteredSignal, SignalType

_REFRESH = 24.0


def _sid(*p: str) -> str:
    return hashlib.sha1(":".join(p).encode()).hexdigest()[:16]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, v))


def _count_actors(network: dict) -> int:
    """Recursively count actor entries in a nested network dict.

    Preserved byte-for-byte from the pre-YAML adapter so that signal
    values (and thus signal_ids) are byte-identical.
    """
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
    """Extract all ticker symbols from a nested network dict.

    Preserved byte-for-byte from the pre-YAML adapter.
    """
    tickers: list[str] = []
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
    def source_module(self) -> str:
        return "sector_network"

    @property
    def refresh_interval_hours(self) -> float:
        return _REFRESH

    def extract_signals(self, engine: Engine) -> list[RegisteredSignal]:
        now = _now()
        vu = now + timedelta(hours=24)
        signals: list[RegisteredSignal] = []

        for sector, _legacy_mod, _legacy_attr in SECTOR_MODULES:
            try:
                network = get_sector_data(sector)
                # Byte-identical to legacy behavior: non-dict or empty
                # networks were skipped entirely (this notably excluded
                # defi, whose top-level export is a list).
                if not isinstance(network, dict) or not network:
                    continue

                actor_count = _count_actors(network)
                tickers = _extract_tickers(network)
                subsectors = len([k for k in network.keys() if isinstance(network[k], dict)])

                src = f"sector_network:{sector}"

                # MAGNITUDE: sector actor density
                signals.append(
                    RegisteredSignal(
                        signal_id=_sid(src, "density", sector, str(now.date())),
                        source_module=src,
                        signal_type=SignalType.MAGNITUDE,
                        ticker=None,
                        direction="neutral",
                        value=float(actor_count),
                        z_score=None,
                        confidence=_clamp(min(actor_count / 50, 1.0)),
                        valid_from=now,
                        valid_until=vu,
                        freshness_hours=0.0,
                        metadata={
                            "sector": sector,
                            "actor_count": actor_count,
                            "subsector_count": subsectors,
                            "tickers": tickers[:20],
                        },
                        provenance=f"sector_network:{sector}:density",
                    )
                )
            except Exception as e:
                log.debug("sector_network_adapter: {s} failed - {e}", s=sector, e=e)

        log.info(
            "sector_network_adapter: {n} signals from {m} sectors",
            n=len(signals),
            m=len(SECTOR_MODULES),
        )
        return signals
