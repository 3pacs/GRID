"""GRID Signal Adapter — Sector Networks. Actor density + per-ticker concentration.

Reads the 10 sector actor graphs from `intelligence/sector_networks/*.yaml`
via the canonical loader. Previously these lived as giant Python dict
literals in `intelligence/<sector>_network.py`; those modules were deleted
as part of Wave 4 of the module dedupe plan.

Emits:
 - One `sector_density` MAGNITUDE signal per sector (ticker=None,
   byte-identical to the legacy adapter).
 - ALPHA-14: per-ticker `sector_share` MAGNITUDE signals (market-cap
   share within the sector) so oracle.SignalAggregator can filter them
   onto individual tickers during predict().
 - ALPHA-14: per-ticker `market_power` MAGNITUDE signals derived from
   the YAML `market_power.assessment` / `influence` fields.
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


_POWER_ASSESSMENT_SCORE: dict[str, float] = {
    "monopoly_gatekeeper": 1.0,
    "monopoly": 1.0,
    "duopoly": 0.85,
    "oligopoly": 0.75,
    "dominant": 0.70,
    "market_leader": 0.60,
    "significant": 0.50,
    "competitor": 0.35,
    "niche": 0.20,
}


def _score_market_power(entry: dict) -> float | None:
    """Convert YAML `market_power` / `influence` fields to a [0, 1] score.

    Returns None when no usable field is present.
    """
    mp = entry.get("market_power")
    if isinstance(mp, dict):
        assessment = mp.get("assessment")
        if isinstance(assessment, str):
            key = assessment.strip().lower()
            if key in _POWER_ASSESSMENT_SCORE:
                return _POWER_ASSESSMENT_SCORE[key]

    influence = entry.get("influence")
    if isinstance(influence, (int, float)):
        v = float(influence)
        if v > 1.0:
            v = v / 10.0
        return _clamp(v)

    return None


def _extract_ticker_entries(network: Any) -> list[tuple[str, dict]]:
    """Walk the YAML tree and return `(ticker, actor_dict)` for every entry
    that exposes a string ticker symbol.

    Deduplicates on first occurrence so the same ticker appearing in
    multiple subsectors does not double-count market-cap share.
    """
    out: list[tuple[str, dict]] = []
    seen: set[str] = set()

    def _walk(obj: Any) -> None:
        if isinstance(obj, dict):
            tkr = obj.get("ticker")
            if isinstance(tkr, str) and tkr:
                key = tkr.strip().upper()
                if key and key not in seen:
                    seen.add(key)
                    out.append((key, obj))
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(network)
    return out


def _market_cap_usd(entry: dict) -> float | None:
    v = entry.get("market_cap_usd")
    if isinstance(v, (int, float)) and v > 0:
        return float(v)
    return None


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

                # ── ALPHA-14: per-ticker concentration signals ───────
                ticker_entries = _extract_ticker_entries(network)
                caps: dict[str, float] = {}
                for tkr, entry in ticker_entries:
                    mc = _market_cap_usd(entry)
                    if mc is not None:
                        caps[tkr] = mc
                total_cap = sum(caps.values())

                for tkr, entry in ticker_entries:
                    # sector_share — market cap share within sector
                    mc = caps.get(tkr)
                    if mc is not None and total_cap > 0:
                        share = mc / total_cap
                        signals.append(
                            RegisteredSignal(
                                signal_id=_sid(src, "share", sector, tkr, str(now.date())),
                                source_module=src,
                                signal_type=SignalType.MAGNITUDE,
                                ticker=tkr,
                                direction="neutral",
                                value=float(share),
                                z_score=None,
                                confidence=_clamp(min(share * 4.0, 1.0)),
                                valid_from=now,
                                valid_until=vu,
                                freshness_hours=0.0,
                                metadata={
                                    "sector": sector,
                                    "market_cap_usd": mc,
                                    "sector_total_cap_usd": total_cap,
                                    "signal_kind": "sector_share",
                                },
                                provenance=f"sector_network:{sector}:share:{tkr}",
                            )
                        )

                    # market_power — assessment / influence derived
                    mp_score = _score_market_power(entry)
                    if mp_score is not None:
                        assessment = None
                        if isinstance(entry.get("market_power"), dict):
                            assessment = entry["market_power"].get("assessment")
                        signals.append(
                            RegisteredSignal(
                                signal_id=_sid(src, "power", sector, tkr, str(now.date())),
                                source_module=src,
                                signal_type=SignalType.MAGNITUDE,
                                ticker=tkr,
                                direction="neutral",
                                value=float(mp_score),
                                z_score=None,
                                confidence=_clamp(mp_score),
                                valid_from=now,
                                valid_until=vu,
                                freshness_hours=0.0,
                                metadata={
                                    "sector": sector,
                                    "assessment": assessment,
                                    "signal_kind": "market_power",
                                },
                                provenance=f"sector_network:{sector}:power:{tkr}",
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
