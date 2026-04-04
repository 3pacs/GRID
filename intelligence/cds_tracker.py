"""GRID — CDS (Credit Default Swap) Tracker.

Derives CDS-equivalent credit risk signals from:
  1. FRED OAS spreads (BAML series) — actual option-adjusted spreads
  2. yfinance ETF ratios (HYG/LQD vs TLT) — intraday proxy for CDX moves
  3. Spread term structure (BB/B/CCC cascade) — distress detection

No direct CDS data needed. FRED OAS vs actual CDX correlation ~0.92 R².

Five outputs:
  - CDS dashboard: IG/HY/distress spread levels + z-scores
  - Credit regime: risk_on / risk_off / transitioning / stress
  - Spread momentum: 1d/5d/20d changes
  - HY-IG compression ratio: risk appetite signal
  - Term structure slope: normalized distress gradient (CCC/BB)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Series mapping ──────────────────────────────────────────────────────

CDS_SERIES = {
    "ig": {
        "id": "BAMLC0A4CBBB",
        "label": "IG (BBB OAS)",
        "desc": "CDX NA IG proxy — ICE BofA BBB Corporate OAS",
        "normal_range": (1.0, 2.5),   # bps/100
        "stress_threshold": 3.0,
    },
    "hy": {
        "id": "BAMLH0A0HYM2",
        "label": "HY (Total OAS)",
        "desc": "CDX NA HY proxy — ICE BofA US High Yield OAS",
        "normal_range": (3.0, 5.5),
        "stress_threshold": 7.0,
    },
    "bb": {
        "id": "BAMLH0A1HYBB",
        "label": "BB OAS",
        "desc": "ICE BofA BB High Yield",
        "normal_range": (2.0, 4.0),
        "stress_threshold": 5.0,
    },
    "b": {
        "id": "BAMLH0A2HYB",
        "label": "B OAS",
        "desc": "ICE BofA B High Yield",
        "normal_range": (3.5, 6.5),
        "stress_threshold": 8.0,
    },
    "ccc": {
        "id": "BAMLH0A3HYC",
        "label": "CCC OAS",
        "desc": "ICE BofA CCC & Lower — deep distress",
        "normal_range": (8.0, 14.0),
        "stress_threshold": 16.0,
    },
    "euro_hy": {
        "id": "BAMLHE00EHYIOAS",
        "label": "Euro HY OAS",
        "desc": "ICE BofA Euro High Yield",
        "normal_range": (3.0, 5.5),
        "stress_threshold": 7.0,
    },
}

ETF_SPREAD_PAIRS = {
    "hyg_tlt": {"long": "YF:HYG:close", "short": "YF:TLT:close", "label": "HYG-TLT (HY proxy)"},
    "lqd_tlt": {"long": "YF:LQD:close", "short": "YF:TLT:close", "label": "LQD-TLT (IG proxy)"},
    "jnk_tlt": {"long": "YF:JNK:close", "short": "YF:TLT:close", "label": "JNK-TLT (Alt HY)"},
}


# ── Data classes ────────────────────────────────────────────────────────

@dataclass(frozen=True)
class SpreadSnapshot:
    """Current spread level + changes + z-score."""
    series_key: str
    label: str
    value: float | None
    change_1d: float | None
    change_5d: float | None
    change_20d: float | None
    z_score_2y: float | None
    percentile_2y: float | None
    is_stressed: bool
    confidence: str


@dataclass(frozen=True)
class CDSDashboard:
    """Complete CDS intelligence output."""
    as_of: date
    spreads: tuple[SpreadSnapshot, ...]
    regime: str                 # risk_on / risk_off / transitioning / stress
    hy_ig_compression: float | None  # HY OAS / IG OAS ratio
    term_slope: float | None         # CCC OAS / BB OAS (distress gradient)
    spread_momentum: str             # tightening / widening / stable
    etf_signals: dict[str, Any]
    narrative: str


# ── Query helpers ───────────────────────────────────────────────────────

def _get_spread_history(
    engine: Engine, series_id: str, as_of: date, lookback_days: int = 730,
) -> list[tuple[date, float]]:
    """Fetch OAS spread history from raw_series."""
    start = as_of - timedelta(days=lookback_days)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT obs_date, value FROM raw_series
            WHERE series_id = :sid
            AND obs_date BETWEEN :start AND :end
            AND pull_status = 'SUCCESS'
            ORDER BY obs_date
        """), {"sid": series_id, "start": start, "end": as_of}).fetchall()
    return [(r[0], float(r[1])) for r in rows]


def _get_etf_ratio(
    engine: Engine, long_sid: str, short_sid: str, as_of: date,
) -> tuple[float | None, float | None]:
    """Compute ETF price ratio and its 5-day change."""
    with engine.connect() as conn:
        def _latest(sid: str, d: date) -> float | None:
            row = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """), {"sid": sid, "d": d}).fetchone()
            return float(row[0]) if row else None

        long_now = _latest(long_sid, as_of)
        short_now = _latest(short_sid, as_of)
        long_5d = _latest(long_sid, as_of - timedelta(days=5))
        short_5d = _latest(short_sid, as_of - timedelta(days=5))

    if long_now and short_now and short_now != 0:
        ratio_now = long_now / short_now
    else:
        return None, None

    if long_5d and short_5d and short_5d != 0:
        ratio_5d = long_5d / short_5d
        change = round(ratio_now - ratio_5d, 6)
    else:
        change = None

    return round(ratio_now, 6), change


# ── Core engine ─────────────────────────────────────────────────────────

def build_spread_snapshot(
    engine: Engine, key: str, cfg: dict, as_of: date,
) -> SpreadSnapshot:
    """Build a SpreadSnapshot for a single OAS series."""
    history = _get_spread_history(engine, cfg["id"], as_of)

    if not history:
        return SpreadSnapshot(
            series_key=key, label=cfg["label"], value=None,
            change_1d=None, change_5d=None, change_20d=None,
            z_score_2y=None, percentile_2y=None,
            is_stressed=False, confidence="stale",
        )

    values = [v for _, v in history]
    dates = [d for d, _ in history]
    latest_val = values[-1]
    latest_date = dates[-1]

    # Changes
    def _find_val(target_days: int) -> float | None:
        target = latest_date - timedelta(days=target_days)
        best_idx, best_dist = None, 999
        for i, d in enumerate(dates):
            dist = abs((d - target).days)
            if dist < best_dist:
                best_dist = dist
                best_idx = i
        if best_idx is not None and best_dist <= max(target_days, 5):
            return values[best_idx]
        return None

    val_1d = _find_val(1)
    val_5d = _find_val(5)
    val_20d = _find_val(20)

    change_1d = round(latest_val - val_1d, 4) if val_1d is not None else None
    change_5d = round(latest_val - val_5d, 4) if val_5d is not None else None
    change_20d = round(latest_val - val_20d, 4) if val_20d is not None else None

    # Z-score
    if len(values) >= 20:
        mean = sum(values) / len(values)
        std = (sum((v - mean) ** 2 for v in values) / len(values)) ** 0.5
        z = round((latest_val - mean) / std, 4) if std > 0 else 0.0
    else:
        z = None

    # Percentile
    if len(values) >= 20:
        below = sum(1 for v in values if v <= latest_val)
        pct = round(below / len(values), 4)
    else:
        pct = None

    is_stressed = latest_val >= cfg["stress_threshold"]
    freshness = (as_of - latest_date).days
    confidence = "confirmed" if freshness <= 2 else "estimated" if freshness <= 7 else "stale"

    return SpreadSnapshot(
        series_key=key, label=cfg["label"], value=latest_val,
        change_1d=change_1d, change_5d=change_5d, change_20d=change_20d,
        z_score_2y=z, percentile_2y=pct,
        is_stressed=is_stressed, confidence=confidence,
    )


def build_cds_dashboard(
    engine: Engine, as_of: date | None = None,
) -> CDSDashboard:
    """Build the complete CDS intelligence dashboard."""
    if as_of is None:
        as_of = date.today()

    log.info("Building CDS dashboard as_of={}", as_of)

    # Build spread snapshots
    spreads = tuple(
        build_spread_snapshot(engine, key, cfg, as_of)
        for key, cfg in CDS_SERIES.items()
    )

    # HY-IG compression ratio
    hy_snap = next((s for s in spreads if s.series_key == "hy"), None)
    ig_snap = next((s for s in spreads if s.series_key == "ig"), None)
    compression = None
    if hy_snap and ig_snap and hy_snap.value and ig_snap.value and ig_snap.value > 0:
        compression = round(hy_snap.value / ig_snap.value, 4)

    # Term structure slope: CCC / BB
    ccc_snap = next((s for s in spreads if s.series_key == "ccc"), None)
    bb_snap = next((s for s in spreads if s.series_key == "bb"), None)
    term_slope = None
    if ccc_snap and bb_snap and ccc_snap.value and bb_snap.value and bb_snap.value > 0:
        term_slope = round(ccc_snap.value / bb_snap.value, 4)

    # Spread momentum
    hy_chg = hy_snap.change_5d if hy_snap else None
    if hy_chg is not None:
        if hy_chg > 0.15:
            momentum = "widening"
        elif hy_chg < -0.15:
            momentum = "tightening"
        else:
            momentum = "stable"
    else:
        momentum = "unknown"

    # ETF signals
    etf_signals = {}
    for pair_key, pair_cfg in ETF_SPREAD_PAIRS.items():
        ratio, change = _get_etf_ratio(engine, pair_cfg["long"], pair_cfg["short"], as_of)
        etf_signals[pair_key] = {
            "label": pair_cfg["label"],
            "ratio": ratio,
            "change_5d": change,
            "signal": "tightening" if change and change > 0 else "widening" if change and change < 0 else "neutral",
        }

    # Credit regime
    regime = _determine_credit_regime(spreads, compression, term_slope, momentum)

    # Narrative
    narrative = _build_narrative(spreads, regime, compression, term_slope, momentum)

    log.info(
        "CDS dashboard: regime={}, HY={}, IG={}, compression={}, slope={}, momentum={}",
        regime,
        hy_snap.value if hy_snap else None,
        ig_snap.value if ig_snap else None,
        compression,
        term_slope,
        momentum,
    )

    return CDSDashboard(
        as_of=as_of,
        spreads=spreads,
        regime=regime,
        hy_ig_compression=compression,
        term_slope=term_slope,
        spread_momentum=momentum,
        etf_signals=etf_signals,
        narrative=narrative,
    )


def _determine_credit_regime(
    spreads: tuple[SpreadSnapshot, ...],
    compression: float | None,
    term_slope: float | None,
    momentum: str,
) -> str:
    """Classify credit regime from spread levels and dynamics."""
    stressed_count = sum(1 for s in spreads if s.is_stressed)
    high_z = sum(1 for s in spreads if s.z_score_2y is not None and s.z_score_2y > 1.5)

    if stressed_count >= 3 or (stressed_count >= 2 and momentum == "widening"):
        return "stress"
    if momentum == "widening" and high_z >= 2:
        return "risk_off"
    if momentum == "tightening" and stressed_count == 0:
        return "risk_on"
    if momentum == "widening" or stressed_count >= 1:
        return "transitioning"
    return "risk_on"


def _build_narrative(
    spreads: tuple[SpreadSnapshot, ...],
    regime: str,
    compression: float | None,
    term_slope: float | None,
    momentum: str,
) -> str:
    """Generate human-readable credit narrative."""
    hy = next((s for s in spreads if s.series_key == "hy"), None)
    ig = next((s for s in spreads if s.series_key == "ig"), None)
    ccc = next((s for s in spreads if s.series_key == "ccc"), None)

    parts = []

    # Regime
    regime_map = {
        "stress": "Credit markets under stress",
        "risk_off": "Credit risk-off: spreads widening",
        "risk_on": "Credit risk-on: spreads tight",
        "transitioning": "Credit markets transitioning",
    }
    parts.append(regime_map.get(regime, "Credit regime unclear") + ".")

    # HY level
    if hy and hy.value is not None:
        parts.append(f"HY OAS at {hy.value:.2f}% (z={hy.z_score_2y:.1f})." if hy.z_score_2y else f"HY OAS at {hy.value:.2f}%.")

    # Compression
    if compression is not None:
        if compression < 2.0:
            parts.append(f"HY/IG compression at {compression:.2f}x — risk appetite elevated.")
        elif compression > 3.0:
            parts.append(f"HY/IG ratio at {compression:.2f}x — credit discrimination high.")

    # Distress
    if ccc and ccc.value is not None and ccc.is_stressed:
        parts.append(f"CCC spreads at {ccc.value:.1f}% — distress zone.")

    # Momentum
    if momentum == "widening":
        parts.append("5-day momentum: spreads widening.")
    elif momentum == "tightening":
        parts.append("5-day momentum: spreads tightening.")

    return " ".join(parts)


# ── Convenience ─────────────────────────────────────────────────────────

def cds_to_dict(dashboard: CDSDashboard) -> dict[str, Any]:
    """Serialize dashboard to JSON-safe dict."""
    return {
        "as_of": dashboard.as_of.isoformat(),
        "regime": dashboard.regime,
        "hy_ig_compression": dashboard.hy_ig_compression,
        "term_slope": dashboard.term_slope,
        "spread_momentum": dashboard.spread_momentum,
        "narrative": dashboard.narrative,
        "spreads": [
            {
                "key": s.series_key,
                "label": s.label,
                "value": s.value,
                "change_1d": s.change_1d,
                "change_5d": s.change_5d,
                "change_20d": s.change_20d,
                "z_score_2y": s.z_score_2y,
                "percentile_2y": s.percentile_2y,
                "is_stressed": s.is_stressed,
                "confidence": s.confidence,
            }
            for s in dashboard.spreads
        ],
        "etf_signals": dashboard.etf_signals,
    }
