#!/usr/bin/env python3
"""
Bottom Detector Live Monitor.

Runs daily (via cron or Hermes). Checks all conditions from the
mega-rally detector framework and fires alerts when setup scores
cross thresholds.

Alert levels:
  WATCH  (score 3-4): Conditions forming, monitor closely
  PILOT  (score 5):   Start 5% pilot position
  SCALE  (score 6-7): Scale to 10-20% position
  FIRE   (score 8+):  Maximum conviction — full position

Output: JSON + log + optional webhook/email notification.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger as log

_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from db import get_engine
from sqlalchemy import text
from alpha_research.data.split_adjuster import compute_real_drawdown


@dataclass(frozen=True)
class MacroSnapshot:
    """Immutable snapshot of current macro conditions."""
    timestamp: str
    vix: float | None
    hy_spread: float | None
    fin_stress: float | None
    yield_curve_2s10s: float | None
    skew: float | None
    fed_funds: float | None
    psi: float | None
    mars_vol: float | None
    mercury_retro: float | None
    nakshatra: float | None
    jup_sat_angle: float | None
    solar_cycle: float | None
    lunar_illum: float | None
    vix_5d_change: float | None
    hy_5d_change: float | None


@dataclass(frozen=True)
class CandidateAlert:
    """Immutable alert for a specific candidate."""
    ticker: str
    score: int
    alert_level: str  # WATCH, PILOT, SCALE, FIRE
    drawdown_pct: float
    current_price: float
    ath_price: float
    ath_date: str
    mom_5d: float
    mom_30d: float
    mom_90d: float
    signals: list[str]
    momentum_turning: bool
    price_stabilizing: bool


def _load_latest(engine, name: str) -> tuple[float | None, date | None]:
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT rs.value, rs.obs_date FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :n ORDER BY rs.obs_date DESC LIMIT 1
        """), {"n": name}).fetchone()
    return (float(row[0]), row[1]) if row else (None, None)


def _load_recent(engine, name: str, days: int = 30) -> pd.Series:
    cutoff = date.today() - timedelta(days=days)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT rs.obs_date, rs.value FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :n AND rs.obs_date >= :d ORDER BY rs.obs_date
        """), {"n": name, "d": cutoff}).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]))
    return s[~s.index.duplicated(keep="last")].sort_index()


def snapshot_macro(engine) -> MacroSnapshot:
    """Take a complete macro snapshot."""
    features = {}
    for name in ["vix_spot", "hy_oas_spread", "ofr_financial_stress",
                  "yld_curve_2s10s", "skew_index", "fed_funds_rate",
                  "planetary_stress_index", "mars_volatility_index",
                  "mercury_retrograde", "nakshatra_index",
                  "jupiter_saturn_angle", "solar_cycle_phase", "lunar_illumination"]:
        val, _ = _load_latest(engine, name)
        features[name] = val

    # Compute 5-day changes
    vix_recent = _load_recent(engine, "vix_spot", 15)
    hy_recent = _load_recent(engine, "hy_oas_spread", 15)

    vix_5d_chg = None
    if len(vix_recent) >= 6:
        vix_5d_chg = float(vix_recent.iloc[-1] - vix_recent.iloc[-6])

    hy_5d_chg = None
    if len(hy_recent) >= 6:
        hy_5d_chg = float(hy_recent.iloc[-1] - hy_recent.iloc[-6])

    return MacroSnapshot(
        timestamp=datetime.now(timezone.utc).isoformat(),
        vix=features.get("vix_spot"),
        hy_spread=features.get("hy_oas_spread"),
        fin_stress=features.get("ofr_financial_stress"),
        yield_curve_2s10s=features.get("yld_curve_2s10s"),
        skew=features.get("skew_index"),
        fed_funds=features.get("fed_funds_rate"),
        psi=features.get("planetary_stress_index"),
        mars_vol=features.get("mars_volatility_index"),
        mercury_retro=features.get("mercury_retrograde"),
        nakshatra=features.get("nakshatra_index"),
        jup_sat_angle=features.get("jupiter_saturn_angle"),
        solar_cycle=features.get("solar_cycle_phase"),
        lunar_illum=features.get("lunar_illumination"),
        vix_5d_change=vix_5d_chg,
        hy_5d_change=hy_5d_chg,
    )


def scan_candidates(engine, macro: MacroSnapshot) -> list[CandidateAlert]:
    """Scan all tickers and score them."""
    with engine.connect() as conn:
        tickers = conn.execute(text("""
            SELECT fr.name FROM feature_registry fr
            JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.name LIKE '%_full'
            GROUP BY fr.name HAVING COUNT(*) > 200
        """)).fetchall()

    alerts = []
    for (feat_name,) in tickers:
        ticker = feat_name.replace("_full", "").upper()

        with engine.connect() as conn:
            prices_raw = conn.execute(text("""
                SELECT rs.obs_date, rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name = :n ORDER BY rs.obs_date
            """), {"n": feat_name}).fetchall()

        if len(prices_raw) < 60:
            continue

        s = pd.Series([p[1] for p in prices_raw], index=pd.to_datetime([p[0] for p in prices_raw]))
        s = s[~s.index.duplicated(keep="last")].sort_index()

        # Split-adjusted metrics
        metrics = compute_real_drawdown(s)
        if "error" in metrics:
            continue

        ath = metrics["ath"]
        ath_date = metrics["ath_date"]
        current = metrics["current"]
        drawdown = metrics["drawdown_pct"]

        # Only score assets with significant drawdown
        if drawdown > -25:
            continue

        # Momentum (from split-adjusted data)
        mom_5d = metrics["mom_5d"]
        mom_30d = metrics["mom_30d"]
        mom_90d = metrics["mom_90d"]

        # === SCORING ===
        score = 0
        signals = []

        # STRUCTURAL: VIX elevated
        if macro.vix is not None:
            if macro.vix > 30:
                score += 3
                signals.append(f"VIX={macro.vix:.0f}>30 [PANIC]")
            elif macro.vix > 25:
                score += 2
                signals.append(f"VIX={macro.vix:.0f}>25 [FEAR]")
            elif macro.vix > 20:
                score += 1
                signals.append(f"VIX={macro.vix:.0f}>20")

        # STRUCTURAL: PSI in range
        if macro.psi is not None and 0.5 < macro.psi < 4.0:
            score += 1
            signals.append(f"PSI={macro.psi:.1f} [favorable]")

        # CONFIRMATION: Credit stress
        if macro.hy_spread is not None:
            if macro.hy_spread > 5.0:
                score += 2
                signals.append(f"HY={macro.hy_spread:.1f}>5 [crisis]")
            elif macro.hy_spread > 4.0:
                score += 1
                signals.append(f"HY={macro.hy_spread:.1f}>4")

        # CONFIRMATION: Financial stress
        if macro.fin_stress is not None and macro.fin_stress > 0:
            score += 1
            signals.append(f"FinStress={macro.fin_stress:.1f}>0")

        # CONFIRMATION: Yield curve inverted
        if macro.yield_curve_2s10s is not None and macro.yield_curve_2s10s < 0:
            score += 1
            signals.append(f"YC={macro.yield_curve_2s10s:.2f} [inverted]")

        # CONFIRMATION: VIX declining (fear receding)
        if macro.vix_5d_change is not None and macro.vix_5d_change < -2:
            score += 1
            signals.append(f"VIX_5d_chg={macro.vix_5d_change:+.1f} [fear receding]")

        # ASSET-SPECIFIC: Deep drawdown
        if drawdown < -70:
            score += 1
            signals.append(f"DD={drawdown:.0f}%<-70%")
        elif drawdown < -50:
            score += 0.5

        # ASSET-SPECIFIC: Momentum turning
        momentum_turning = mom_30d > 0 and mom_90d < 0
        if momentum_turning:
            score += 1
            signals.append("MomTurn [30d↑ 90d↓]")

        # ASSET-SPECIFIC: Price stabilizing (5d flat or up after 30d down)
        price_stabilizing = mom_5d > -1 and mom_30d < -5
        if price_stabilizing:
            score += 0.5
            signals.append("PriceStab [5d flat, 30d down]")

        # ASSET-SPECIFIC: Mercury retrograde ending (timing precision)
        if macro.mercury_retro is not None and macro.mercury_retro == 0:
            # Not retrograde — favorable for new positions
            pass  # neutral, don't penalize

        score = int(round(score))

        # Alert level
        if score >= 8:
            level = "FIRE"
        elif score >= 6:
            level = "SCALE"
        elif score >= 5:
            level = "PILOT"
        elif score >= 3:
            level = "WATCH"
        else:
            continue  # Skip low scores

        alerts.append(CandidateAlert(
            ticker=ticker,
            score=score,
            alert_level=level,
            drawdown_pct=round(drawdown, 1),
            current_price=round(current, 2),
            ath_price=round(ath, 2),
            ath_date=str(ath_date.date()),
            mom_5d=round(mom_5d, 1),
            mom_30d=round(mom_30d, 1),
            mom_90d=round(mom_90d, 1),
            signals=signals,
            momentum_turning=momentum_turning,
            price_stabilizing=price_stabilizing,
        ))

    return sorted(alerts, key=lambda a: (-a.score, a.drawdown_pct))


def main() -> None:
    engine = get_engine()

    log.info("=" * 70)
    log.info("BOTTOM DETECTOR MONITOR — {d}", d=date.today())
    log.info("=" * 70)

    # 1. Macro snapshot
    macro = snapshot_macro(engine)

    log.info("\n--- MACRO CONDITIONS ---")
    log.info("  VIX:          {v}  (5d chg: {c})",
             v=f"{macro.vix:.1f}" if macro.vix else "N/A",
             c=f"{macro.vix_5d_change:+.1f}" if macro.vix_5d_change else "N/A")
    log.info("  HY Spread:    {v}  (5d chg: {c})",
             v=f"{macro.hy_spread:.2f}" if macro.hy_spread else "N/A",
             c=f"{macro.hy_5d_change:+.2f}" if macro.hy_5d_change else "N/A")
    log.info("  Fin Stress:   {v}", v=f"{macro.fin_stress:.2f}" if macro.fin_stress else "N/A")
    log.info("  Yield Curve:  {v}", v=f"{macro.yield_curve_2s10s:.2f}" if macro.yield_curve_2s10s else "N/A")
    log.info("  PSI:          {v}", v=f"{macro.psi:.2f}" if macro.psi else "N/A")
    log.info("  Mercury Retro:{v}", v=f"{macro.mercury_retro:.0f}" if macro.mercury_retro is not None else "N/A")

    # Macro regime assessment
    fear_count = sum([
        (macro.vix or 0) > 25,
        (macro.hy_spread or 0) > 4.0,
        (macro.fin_stress or 0) > 0,
    ])

    if fear_count >= 3:
        log.info("\n  REGIME: *** CRISIS *** — All fear signals firing")
    elif fear_count >= 2:
        log.info("\n  REGIME: ** ELEVATED FEAR ** — {n}/3 fear signals", n=fear_count)
    elif fear_count >= 1:
        log.info("\n  REGIME: * CAUTIOUS * — {n}/3 fear signals", n=fear_count)
    else:
        log.info("\n  REGIME: CALM — No fear signals")

    # 2. Scan candidates
    alerts = scan_candidates(engine, macro)

    # 3. Display results
    fire = [a for a in alerts if a.alert_level == "FIRE"]
    scale = [a for a in alerts if a.alert_level == "SCALE"]
    pilot = [a for a in alerts if a.alert_level == "PILOT"]
    watch = [a for a in alerts if a.alert_level == "WATCH"]

    if fire:
        log.info("\n" + "!" * 70)
        log.info("!!! FIRE ALERTS — MAXIMUM CONVICTION !!!")
        log.info("!" * 70)
        for a in fire:
            log.info("  {t:8s} Score={s:2d}  DD={dd:>6.1f}%  ${px:>9.2f}  5d={m5:>6.1f}%  30d={m30:>6.1f}%  90d={m90:>6.1f}%",
                     t=a.ticker, s=a.score, dd=a.drawdown_pct, px=a.current_price,
                     m5=a.mom_5d, m30=a.mom_30d, m90=a.mom_90d)
            for sig in a.signals:
                log.info("    → {s}", s=sig)

    if scale:
        log.info("\n--- SCALE ALERTS — Add to positions ---")
        for a in scale:
            log.info("  {t:8s} Score={s:2d}  DD={dd:>6.1f}%  ${px:>9.2f}  30d={m30:>6.1f}%  {sigs}",
                     t=a.ticker, s=a.score, dd=a.drawdown_pct, px=a.current_price,
                     m30=a.mom_30d, sigs=" | ".join(a.signals))

    if pilot:
        log.info("\n--- PILOT ALERTS — Start small positions ---")
        for a in pilot:
            turn = " [TURNING]" if a.momentum_turning else ""
            log.info("  {t:8s} Score={s:2d}  DD={dd:>6.1f}%  ${px:>9.2f}  30d={m30:>6.1f}%{turn}  {sigs}",
                     t=a.ticker, s=a.score, dd=a.drawdown_pct, px=a.current_price,
                     m30=a.mom_30d, turn=turn, sigs=" | ".join(a.signals))

    if watch:
        log.info("\n--- WATCH LIST ---")
        for a in watch[:15]:
            turn = " [TURNING]" if a.momentum_turning else ""
            log.info("  {t:8s} Score={s:2d}  DD={dd:>6.1f}%  ${px:>9.2f}  30d={m30:>6.1f}%{turn}",
                     t=a.ticker, s=a.score, dd=a.drawdown_pct, px=a.current_price,
                     m30=a.mom_30d, turn=turn)

    log.info("\n--- SUMMARY ---")
    log.info("  FIRE:  {n}  SCALE: {s}  PILOT: {p}  WATCH: {w}",
             n=len(fire), s=len(scale), p=len(pilot), w=len(watch))

    # 4. Save JSON report
    report = {
        "date": str(date.today()),
        "macro": asdict(macro),
        "fear_count": fear_count,
        "alerts": {
            "fire": [asdict(a) for a in fire],
            "scale": [asdict(a) for a in scale],
            "pilot": [asdict(a) for a in pilot],
            "watch": [asdict(a) for a in watch],
        },
        "total_candidates": len(alerts),
    }

    report_path = Path("outputs/bottom_detector")
    report_path.mkdir(parents=True, exist_ok=True)
    report_file = report_path / f"scan_{date.today()}.json"
    report_file.write_text(json.dumps(report, indent=2, default=str))
    log.info("  Report saved: {p}", p=report_file)


if __name__ == "__main__":
    main()
