"""
GRID API — Derivatives / Dealer Flow Intelligence endpoints.

Exposes dealer gamma exposure (GEX), vol surface, term structure,
OI heatmaps, skew curves, and flow narrative data from the
DealerGammaEngine and options_snapshots tables.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine

router = APIRouter(
    prefix="/api/v1/derivatives",
    tags=["derivatives"],
    dependencies=[Depends(require_auth)],
)


def _get_gex_engine():
    """Lazily import and instantiate DealerGammaEngine."""
    from physics.dealer_gamma import DealerGammaEngine
    return DealerGammaEngine(get_db_engine())


# ── GET /overview ────────────────────────────────────────────────────

@router.get("/overview")
async def get_overview() -> dict[str, Any]:
    """Market-wide dealer positioning summary.

    Calls DealerGammaEngine.get_market_gex_summary() for SPY and
    returns aggregate regime, GEX, gamma flip, vanna/charm exposure,
    and top signals from options_daily_signals.
    """
    try:
        engine_gex = _get_gex_engine()
        summary = engine_gex.get_market_gex_summary()

        # Fetch latest daily signals for top tickers
        db = get_db_engine()
        with db.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT ON (ticker) ticker, signal_date, put_call_ratio, "
                "max_pain, iv_skew, iv_atm, total_oi, total_volume, spot_price "
                "FROM options_daily_signals "
                "ORDER BY ticker, signal_date DESC "
                "LIMIT 20"
            )).fetchall()

        top_signals = [
            {
                "ticker": r[0], "signal_date": str(r[1]),
                "put_call_ratio": r[2], "max_pain": r[3],
                "iv_skew": r[4], "iv_atm": r[5],
                "total_oi": r[6], "total_volume": r[7],
                "spot_price": r[8],
            }
            for r in rows
        ]

        return {
            **summary,
            "top_signals": top_signals,
        }
    except Exception as exc:
        log.warning("Derivatives overview failed: {e}", e=str(exc))
        return {"error": str(exc), "regime": "UNKNOWN"}


# ── GET /gex/{ticker} ───────────────────────────────────────────────

@router.get("/gex/{ticker}")
async def get_gex(ticker: str) -> dict[str, Any]:
    """Full GEX profile for a single ticker.

    Returns gex_aggregate, gamma_flip, gamma_wall, put_wall, call_wall,
    dealer_delta, vanna_exposure, charm_exposure, regime, profile curve,
    and per_strike breakdown.
    """
    try:
        engine_gex = _get_gex_engine()
        result = engine_gex.compute_gex_profile(ticker.upper())
        return result
    except Exception as exc:
        log.warning("GEX computation failed for {t}: {e}", t=ticker, e=str(exc))
        return {"error": str(exc), "ticker": ticker.upper()}


# ── GET /regime ──────────────────────────────────────────────────────

@router.get("/regime")
async def get_regime() -> dict[str, Any]:
    """Current dealer regime with plain-English interpretation.

    Computes SPY GEX and returns regime classification plus explanation
    of what it means for market dynamics.
    """
    try:
        engine_gex = _get_gex_engine()
        spy = engine_gex.compute_gex_profile("SPY")

        regime = spy.get("regime", "UNKNOWN")
        interpretations = {
            "LONG_GAMMA": (
                "Dealers are long gamma. They hedge by selling rallies and buying dips, "
                "dampening volatility. Expect mean-reversion and range-bound price action. "
                "Intraday moves tend to fade. Realized vol will likely undershoot implied."
            ),
            "SHORT_GAMMA": (
                "Dealers are short gamma. They hedge by buying rallies and selling dips, "
                "amplifying moves in both directions. Expect trend-following dynamics, "
                "potential breakouts, and elevated realized volatility. Directional risk is high."
            ),
            "NEUTRAL": (
                "Gamma exposure is near zero. The market is at or near the gamma flip point. "
                "Small changes in spot could shift dealers from stabilizing to amplifying flows. "
                "Watch for regime transitions — this is an inflection zone."
            ),
        }

        return {
            "regime": regime,
            "interpretation": interpretations.get(regime, "Unable to determine regime."),
            "gex_aggregate": spy.get("gex_aggregate"),
            "gex_normalized": spy.get("gex_normalized"),
            "gamma_flip": spy.get("gamma_flip"),
            "spot": spy.get("spot"),
            "snap_date": spy.get("snap_date"),
        }
    except Exception as exc:
        log.warning("Regime computation failed: {e}", e=str(exc))
        return {"regime": "UNKNOWN", "interpretation": f"Error: {str(exc)}"}


# ── GET /walls/{ticker} ─────────────────────────────────────────────

@router.get("/walls/{ticker}")
async def get_walls(ticker: str) -> dict[str, Any]:
    """Support/resistance levels derived from gamma walls.

    Returns put_wall (support), call_wall (resistance), gamma_flip,
    and current spot price.
    """
    try:
        engine_gex = _get_gex_engine()
        result = engine_gex.compute_gex_profile(ticker.upper())
        return {
            "ticker": ticker.upper(),
            "put_wall": result.get("put_wall"),
            "call_wall": result.get("call_wall"),
            "gamma_wall": result.get("gamma_wall"),
            "gamma_flip": result.get("gamma_flip"),
            "spot": result.get("spot"),
            "regime": result.get("regime"),
        }
    except Exception as exc:
        log.warning("Walls computation failed for {t}: {e}", t=ticker, e=str(exc))
        return {"error": str(exc), "ticker": ticker.upper()}


# ── GET /vanna-charm/{ticker} ───────────────────────────────────────

@router.get("/vanna-charm/{ticker}")
async def get_vanna_charm(ticker: str) -> dict[str, Any]:
    """Decomposed vanna and charm exposures with per-strike breakdown.

    Returns aggregate vanna/charm, per-strike decomposition, net dealer
    delta change from charm decay, days to next OpEx, and a plain-English
    interpretation of projected dealer hedging flows.
    """
    try:
        engine_gex = _get_gex_engine()
        result = engine_gex.compute_gex_profile(ticker.upper())

        if result.get("error"):
            return {"error": result["error"], "ticker": ticker.upper()}

        vanna = result.get("vanna_exposure", 0)
        charm = result.get("charm_exposure", 0)
        spot = result.get("spot", 0)
        per_strike = result.get("per_strike", [])

        # Build per-strike vanna/charm arrays
        vanna_by_strike = [
            {"strike": s["strike"], "vanna": s.get("vanna", 0)}
            for s in per_strike if abs(s.get("vanna", 0)) > 0.0001
        ]
        charm_by_strike = [
            {"strike": s["strike"], "charm": s.get("charm", 0)}
            for s in per_strike if abs(s.get("charm", 0)) > 0.0001
        ]

        # Find next monthly OpEx (3rd Friday of next month)
        today = date.today()

        def _next_opex(from_date: date) -> date:
            """Find the next monthly options expiration (3rd Friday)."""
            # Start from from_date's month; if 3rd Friday has passed, go next month
            for month_offset in range(0, 3):
                y = from_date.year + (from_date.month + month_offset - 1) // 12
                m = (from_date.month + month_offset - 1) % 12 + 1
                # Find 3rd Friday: first day of month, find first Friday, add 14 days
                first = date(y, m, 1)
                # weekday(): Monday=0 ... Friday=4
                days_until_friday = (4 - first.weekday()) % 7
                third_friday = first + timedelta(days=days_until_friday + 14)
                if third_friday > from_date:
                    return third_friday
            return from_date + timedelta(days=30)  # fallback

        opex = _next_opex(today)
        days_to_opex = (opex - today).days

        # Net dealer delta change: charm accumulates daily until OpEx
        # charm_exposure is daily delta decay; project forward
        net_delta_change = round(charm * days_to_opex, 0)

        # Build interpretation
        action = "sell" if net_delta_change < 0 else "buy"
        abs_delta_m = abs(net_delta_change) / 1e6
        interpretation = (
            f"Dealers will need to {action} ~${abs_delta_m:.1f}M delta by "
            f"{opex.strftime('%b %d')} OpEx due to charm decay"
        )

        return {
            "ticker": ticker.upper(),
            "spot": spot,
            "vanna_exposure": vanna,
            "charm_exposure": charm,
            "vanna_by_strike": vanna_by_strike,
            "charm_by_strike": charm_by_strike,
            "net_dealer_delta_change": net_delta_change,
            "interpretation": interpretation,
            "days_to_opex": days_to_opex,
            "opex_date": str(opex),
            "regime": result.get("regime"),
        }
    except Exception as exc:
        log.warning("Vanna-charm failed for {t}: {e}", t=ticker, e=str(exc))
        return {"error": str(exc), "ticker": ticker.upper()}


# ── GET /vol-surface/{ticker} ───────────────────────────────────────

@router.get("/vol-surface/{ticker}")
async def get_vol_surface(ticker: str) -> dict[str, Any]:
    """Volatility surface data: IV grid by strike x expiry.

    Returns array of {strike, expiry, dte, iv, oi, volume, type} from
    the options_snapshots table.
    """
    try:
        db = get_db_engine()
        with db.connect() as conn:
            # Get latest snap_date for ticker
            latest = conn.execute(text(
                "SELECT MAX(snap_date) FROM options_snapshots WHERE ticker = :t"
            ), {"t": ticker.upper()}).fetchone()

            if not latest or not latest[0]:
                return {"ticker": ticker.upper(), "surface": [], "error": "No data"}

            snap_date = latest[0]

            rows = conn.execute(text(
                "SELECT strike, expiry, (expiry - :sd) AS dte, "
                "implied_volatility, open_interest, volume, opt_type "
                "FROM options_snapshots "
                "WHERE ticker = :t AND snap_date = :sd "
                "AND implied_volatility > 0 AND open_interest > 0 "
                "AND expiry > :sd "
                "ORDER BY expiry, strike"
            ), {"t": ticker.upper(), "sd": snap_date}).fetchall()

        surface = [
            {
                "strike": float(r[0]),
                "expiry": str(r[1]),
                "dte": r[2].days if hasattr(r[2], 'days') else int(r[2]),
                "iv": float(r[3]),
                "oi": int(r[4]),
                "volume": int(r[5]) if r[5] else 0,
                "type": r[6],
            }
            for r in rows
        ]

        return {
            "ticker": ticker.upper(),
            "snap_date": str(snap_date),
            "surface": surface,
            "count": len(surface),
        }
    except Exception as exc:
        log.warning("Vol surface failed for {t}: {e}", t=ticker, e=str(exc))
        return {"error": str(exc), "ticker": ticker.upper(), "surface": []}


# ── GET /skew/{ticker} ──────────────────────────────────────────────

@router.get("/skew/{ticker}")
async def get_skew(ticker: str) -> dict[str, Any]:
    """Skew curves: IV at each strike for each expiry.

    Returns array of {expiry, dte, strikes: [{strike, call_iv, put_iv}]}.
    """
    try:
        db = get_db_engine()
        with db.connect() as conn:
            latest = conn.execute(text(
                "SELECT MAX(snap_date) FROM options_snapshots WHERE ticker = :t"
            ), {"t": ticker.upper()}).fetchone()

            if not latest or not latest[0]:
                return {"ticker": ticker.upper(), "skew": [], "error": "No data"}

            snap_date = latest[0]

            rows = conn.execute(text(
                "SELECT strike, expiry, (expiry - :sd) AS dte, "
                "implied_volatility, opt_type "
                "FROM options_snapshots "
                "WHERE ticker = :t AND snap_date = :sd "
                "AND implied_volatility > 0 AND expiry > :sd "
                "ORDER BY expiry, strike"
            ), {"t": ticker.upper(), "sd": snap_date}).fetchall()

        # Group by expiry
        from collections import defaultdict
        expiry_map: dict[str, dict] = {}

        for r in rows:
            strike = float(r[0])
            expiry = str(r[1])
            dte = r[2].days if hasattr(r[2], 'days') else int(r[2])
            iv = float(r[3])
            opt_type = r[4]

            if expiry not in expiry_map:
                expiry_map[expiry] = {"expiry": expiry, "dte": dte, "strikes_map": {}}

            if strike not in expiry_map[expiry]["strikes_map"]:
                expiry_map[expiry]["strikes_map"][strike] = {"strike": strike, "call_iv": None, "put_iv": None}

            if opt_type == "call":
                expiry_map[expiry]["strikes_map"][strike]["call_iv"] = iv
            else:
                expiry_map[expiry]["strikes_map"][strike]["put_iv"] = iv

        skew = [
            {
                "expiry": v["expiry"],
                "dte": v["dte"],
                "strikes": sorted(v["strikes_map"].values(), key=lambda s: s["strike"]),
            }
            for v in sorted(expiry_map.values(), key=lambda x: x["dte"])
        ]

        return {"ticker": ticker.upper(), "snap_date": str(snap_date), "skew": skew}
    except Exception as exc:
        log.warning("Skew failed for {t}: {e}", t=ticker, e=str(exc))
        return {"error": str(exc), "ticker": ticker.upper(), "skew": []}


# ── GET /term-structure/{ticker} ─────────────────────────────────────

@router.get("/term-structure/{ticker}")
async def get_term_structure(ticker: str) -> dict[str, Any]:
    """ATM IV term structure across expiries.

    Returns [{expiry, dte, iv_atm, iv_25d_put, iv_25d_call}].
    ATM is approximated as the strike closest to spot.
    """
    try:
        db = get_db_engine()
        with db.connect() as conn:
            latest = conn.execute(text(
                "SELECT MAX(snap_date) FROM options_snapshots WHERE ticker = :t"
            ), {"t": ticker.upper()}).fetchone()

            if not latest or not latest[0]:
                return {"ticker": ticker.upper(), "term_structure": [], "error": "No data"}

            snap_date = latest[0]

            # Get spot price approximation (highest OI call strike)
            spot_row = conn.execute(text(
                "SELECT strike FROM options_snapshots "
                "WHERE ticker = :t AND snap_date = :sd AND opt_type = 'call' "
                "ORDER BY open_interest DESC LIMIT 1"
            ), {"t": ticker.upper(), "sd": snap_date}).fetchone()

            spot = float(spot_row[0]) if spot_row else 0

            rows = conn.execute(text(
                "SELECT strike, expiry, (expiry - :sd) AS dte, "
                "implied_volatility, opt_type, open_interest "
                "FROM options_snapshots "
                "WHERE ticker = :t AND snap_date = :sd "
                "AND implied_volatility > 0 AND expiry > :sd "
                "ORDER BY expiry, strike"
            ), {"t": ticker.upper(), "sd": snap_date}).fetchall()

        # Group by expiry, find ATM and 25-delta approximations
        from collections import defaultdict
        expiry_data: dict[str, list] = defaultdict(list)
        for r in rows:
            expiry_data[str(r[1])].append({
                "strike": float(r[0]),
                "dte": r[2].days if hasattr(r[2], 'days') else int(r[2]),
                "iv": float(r[3]),
                "type": r[4],
                "oi": int(r[5]),
            })

        term_structure = []
        for expiry, strikes_list in sorted(expiry_data.items()):
            if not strikes_list:
                continue

            dte = strikes_list[0]["dte"]

            # ATM: strike closest to spot
            calls = [s for s in strikes_list if s["type"] == "call"]
            puts = [s for s in strikes_list if s["type"] == "put"]

            atm_iv = None
            if calls and spot > 0:
                atm_call = min(calls, key=lambda s: abs(s["strike"] - spot))
                atm_iv = atm_call["iv"]

            # 25-delta approximations (roughly 5-8% OTM for typical vols)
            otm_pct = 0.05
            iv_25d_put = None
            iv_25d_call = None

            if puts and spot > 0:
                target_put = spot * (1 - otm_pct)
                nearest_put = min(puts, key=lambda s: abs(s["strike"] - target_put))
                iv_25d_put = nearest_put["iv"]

            if calls and spot > 0:
                target_call = spot * (1 + otm_pct)
                nearest_call = min(calls, key=lambda s: abs(s["strike"] - target_call))
                iv_25d_call = nearest_call["iv"]

            term_structure.append({
                "expiry": expiry,
                "dte": dte,
                "iv_atm": round(atm_iv, 4) if atm_iv else None,
                "iv_25d_put": round(iv_25d_put, 4) if iv_25d_put else None,
                "iv_25d_call": round(iv_25d_call, 4) if iv_25d_call else None,
            })

        term_structure.sort(key=lambda x: x["dte"])

        return {
            "ticker": ticker.upper(),
            "snap_date": str(snap_date),
            "spot": spot,
            "term_structure": term_structure,
        }
    except Exception as exc:
        log.warning("Term structure failed for {t}: {e}", t=ticker, e=str(exc))
        return {"error": str(exc), "ticker": ticker.upper(), "term_structure": []}


# ── GET /oi-heatmap/{ticker} ────────────────────────────────────────

@router.get("/oi-heatmap/{ticker}")
async def get_oi_heatmap(ticker: str) -> dict[str, Any]:
    """Open interest heatmap: OI by strike x expiry.

    Returns grid of [{strike, expiry, call_oi, put_oi, call_vol, put_vol}].
    """
    try:
        db = get_db_engine()
        with db.connect() as conn:
            latest = conn.execute(text(
                "SELECT MAX(snap_date) FROM options_snapshots WHERE ticker = :t"
            ), {"t": ticker.upper()}).fetchone()

            if not latest or not latest[0]:
                return {"ticker": ticker.upper(), "heatmap": [], "error": "No data"}

            snap_date = latest[0]

            rows = conn.execute(text(
                "SELECT strike, expiry, opt_type, open_interest, "
                "COALESCE(volume, 0) AS volume "
                "FROM options_snapshots "
                "WHERE ticker = :t AND snap_date = :sd "
                "AND open_interest > 0 AND expiry > :sd "
                "ORDER BY expiry, strike"
            ), {"t": ticker.upper(), "sd": snap_date}).fetchall()

        # Aggregate into strike x expiry cells
        from collections import defaultdict
        cell_map: dict[tuple, dict] = {}

        for r in rows:
            strike = float(r[0])
            expiry = str(r[1])
            opt_type = r[2]
            oi = int(r[3])
            vol = int(r[4])

            key = (strike, expiry)
            if key not in cell_map:
                cell_map[key] = {
                    "strike": strike, "expiry": expiry,
                    "call_oi": 0, "put_oi": 0, "call_vol": 0, "put_vol": 0,
                }

            if opt_type == "call":
                cell_map[key]["call_oi"] += oi
                cell_map[key]["call_vol"] += vol
            else:
                cell_map[key]["put_oi"] += oi
                cell_map[key]["put_vol"] += vol

        heatmap = sorted(cell_map.values(), key=lambda c: (c["expiry"], c["strike"]))

        return {
            "ticker": ticker.upper(),
            "snap_date": str(snap_date),
            "heatmap": heatmap,
            "count": len(heatmap),
        }
    except Exception as exc:
        log.warning("OI heatmap failed for {t}: {e}", t=ticker, e=str(exc))
        return {"error": str(exc), "ticker": ticker.upper(), "heatmap": []}


# ── GET /flow-narrative ──────────────────────────────────────────────

@router.get("/flow-narrative")
async def get_flow_narrative() -> dict[str, Any]:
    """Latest dealer flow narrative briefing (LLM-powered).

    Returns the most recent Cem-Karsan-style briefing covering gamma
    regime, key levels, vanna/charm dynamics, OpEx outlook, and
    mechanical flow synthesis. Generated daily at 15:00 UTC.

    Falls back to a quick inline summary if no LLM briefing exists yet.
    """
    db = get_db_engine()

    # Try the LLM-powered briefing first
    try:
        from ollama.dealer_flow_briefing import get_latest_flow_briefing
        result = get_latest_flow_briefing(db)
        if result.get("content"):
            return result
    except Exception as exc:
        log.debug("LLM flow briefing unavailable: {e}", e=str(exc))

    # Fallback: quick inline narrative from live GEX
    try:
        engine_gex = _get_gex_engine()
        spy = engine_gex.compute_gex_profile("SPY")

        regime = spy.get("regime", "UNKNOWN")
        spot = spy.get("spot", 0)
        gex = spy.get("gex_aggregate", 0)
        flip = spy.get("gamma_flip")
        put_wall = spy.get("put_wall")
        call_wall = spy.get("call_wall")
        vanna = spy.get("vanna_exposure", 0)
        charm = spy.get("charm_exposure", 0)

        parts = [f"SPY is trading at ${spot:.2f}."]

        if regime == "LONG_GAMMA":
            parts.append(
                "Dealers are currently LONG GAMMA, meaning hedging flows will dampen "
                "price moves. Expect range-bound, mean-reverting action."
            )
        elif regime == "SHORT_GAMMA":
            parts.append(
                "Dealers are currently SHORT GAMMA. Hedging flows amplify directional "
                "moves. Risk of gap moves and sustained trends is elevated."
            )
        else:
            parts.append(
                "Dealer gamma is near NEUTRAL. The market sits close to the gamma "
                "flip point, making regime transitions likely on small moves."
            )

        if flip:
            position = "above" if spot > flip else "below"
            parts.append(f"Gamma flip is at ${flip:.0f} (spot is {position}).")

        if put_wall and call_wall:
            parts.append(
                f"Gamma walls: put wall (support) at ${put_wall:.0f}, "
                f"call wall (resistance) at ${call_wall:.0f}."
            )

        parts.append(
            f"Aggregate GEX: ${gex:,.0f}. "
            f"Vanna exposure: ${vanna:,.0f}. Charm exposure: ${charm:,.0f}."
        )

        narrative = " ".join(parts)

        return {
            "content": narrative,
            "positioning_data": {
                "gex": {"SPY": {
                    "gex_aggregate": gex, "regime": regime,
                    "gamma_flip": flip, "put_wall": put_wall,
                    "call_wall": call_wall, "spot": spot,
                    "vanna_exposure": vanna, "charm_exposure": charm,
                }},
            },
            "briefing_date": spy.get("snap_date"),
            "created_at": None,
            "stale": True,
            "note": "Inline fallback — LLM briefing not yet generated. "
                    "Trigger via POST /api/v1/derivatives/flow-narrative/generate",
        }
    except Exception as exc:
        log.warning("Flow narrative failed: {e}", e=str(exc))
        return {
            "content": None,
            "positioning_data": None,
            "briefing_date": None,
            "stale": True,
            "error": str(exc),
        }


@router.post("/flow-narrative/generate")
async def generate_flow_narrative() -> dict[str, Any]:
    """Trigger on-demand generation of a dealer flow narrative briefing.

    Computes GEX for SPY/QQQ/IWM, gathers market context, and calls
    the LLM to produce a Cem-Karsan-style mechanical flow analysis.
    """
    db = get_db_engine()
    try:
        from ollama.dealer_flow_briefing import generate_dealer_flow_briefing
        return generate_dealer_flow_briefing(db)
    except Exception as exc:
        log.warning("Dealer flow briefing generation failed: {e}", e=str(exc))
        return {"content": None, "error": str(exc)}


# ── GET /signals ─────────────────────────────────────────────────────

@router.get("/signals")
async def get_signals(
    limit: int = Query(50, ge=1, le=500),
) -> dict[str, Any]:
    """Latest options daily signals for all tickers."""
    try:
        db = get_db_engine()
        with db.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT ON (ticker) ticker, signal_date, put_call_ratio, "
                "max_pain, iv_skew, total_oi, total_volume, near_expiry, "
                "spot_price, iv_atm, term_structure_slope, oi_concentration "
                "FROM options_daily_signals "
                "ORDER BY ticker, signal_date DESC"
            )).fetchall()

        signals = [
            {
                "ticker": r[0], "signal_date": str(r[1]),
                "put_call_ratio": r[2], "max_pain": r[3],
                "iv_skew": r[4], "total_oi": r[5],
                "total_volume": r[6],
                "near_expiry": str(r[7]) if r[7] else None,
                "spot_price": r[8], "iv_atm": r[9],
                "term_structure_slope": r[10],
                "oi_concentration": r[11],
            }
            for r in rows[:limit]
        ]

        return {"signals": signals, "count": len(signals)}
    except Exception as exc:
        log.warning("Derivatives signals failed: {e}", e=str(exc))
        return {"signals": [], "count": 0, "error": str(exc)}


# ── GET /scan ────────────────────────────────────────────────────────

@router.get("/scan")
async def get_scan(
    min_score: float = Query(5.0, ge=0, le=10),
) -> dict[str, Any]:
    """Run options scanner with dealer context."""
    try:
        from discovery.options_scanner import OptionsScanner

        db = get_db_engine()
        scanner = OptionsScanner(db)
        opps = scanner.scan_all(min_score=min_score)

        results = [
            {
                "ticker": o.ticker,
                "scan_date": str(o.scan_date),
                "score": o.score,
                "estimated_payoff_multiple": o.estimated_payoff_multiple,
                "direction": o.direction,
                "thesis": o.thesis,
                "strikes": o.strikes,
                "expiry": o.expiry,
                "spot_price": o.spot_price,
                "iv_atm": o.iv_atm,
                "confidence": o.confidence,
                "is_100x": o.is_100x,
            }
            for o in opps
        ]

        return {
            "opportunities": results,
            "count": len(results),
            "count_100x": sum(1 for o in opps if o.is_100x),
        }
    except Exception as exc:
        log.warning("Derivatives scan failed: {e}", e=str(exc))
        return {"opportunities": [], "count": 0, "count_100x": 0, "error": str(exc)}


# ── GET /flow-timeline/{ticker} ─────────────────────────────────────

def _generate_opex_calendar(start_date: date, end_date: date) -> list[dict]:
    """Generate OpEx calendar programmatically.

    - Every Friday = weekly
    - 3rd Friday of month = monthly
    - 3rd Friday of March/June/Sept/Dec = quarterly "quad witch"
    """
    import calendar
    from datetime import timedelta

    events = []
    current = start_date

    while current <= end_date:
        if current.weekday() == 4:  # Friday
            first_day_wday, _ = calendar.monthrange(current.year, current.month)
            first_friday = 1 + (4 - first_day_wday) % 7
            third_friday = first_friday + 14

            is_third_friday = current.day == third_friday
            is_quarterly = is_third_friday and current.month in (3, 6, 9, 12)

            if is_quarterly:
                quarter_names = {3: "Q1", 6: "Q2", 9: "Q3", 12: "Q4"}
                events.append({
                    "date": str(current),
                    "type": "quarterly",
                    "label": f"{quarter_names[current.month]} Quad Witch",
                })
            elif is_third_friday:
                events.append({
                    "date": str(current),
                    "type": "monthly",
                    "label": f"{current.strftime('%B')} OpEx",
                })
            else:
                events.append({
                    "date": str(current),
                    "type": "weekly",
                    "label": "Weekly",
                })

        current += timedelta(days=1)

    return events


def _generate_catalysts(start_date: date, end_date: date, ticker: str) -> list[dict]:
    """Generate known macro catalysts for the date range.

    Hardcodes recurring FOMC and CPI dates. Attempts yfinance for earnings.
    """
    catalysts = []

    # FOMC 2026 scheduled dates (2-day meetings ending on these dates)
    fomc_dates = [
        date(2026, 1, 28), date(2026, 3, 18), date(2026, 5, 6),
        date(2026, 6, 17), date(2026, 7, 29), date(2026, 9, 16),
        date(2026, 10, 28), date(2026, 12, 16),
    ]
    for d in fomc_dates:
        if start_date <= d <= end_date:
            catalysts.append({
                "date": str(d),
                "type": "fomc",
                "label": "FOMC Decision",
            })

    # CPI release dates 2026 (typically ~10th-15th of each month)
    cpi_dates = [
        date(2026, 1, 14), date(2026, 2, 11), date(2026, 3, 11),
        date(2026, 4, 10), date(2026, 5, 13), date(2026, 6, 10),
        date(2026, 7, 15), date(2026, 8, 12), date(2026, 9, 16),
        date(2026, 10, 14), date(2026, 11, 12), date(2026, 12, 9),
    ]
    for d in cpi_dates:
        if start_date <= d <= end_date:
            catalysts.append({
                "date": str(d),
                "type": "cpi",
                "label": "CPI Release",
            })

    # Try yfinance for earnings date
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        cal = tk.calendar
        if cal is not None:
            earnings_dates = None
            if isinstance(cal, dict):
                earnings_dates = cal.get("Earnings Date", [])
            elif hasattr(cal, "columns"):
                if "Earnings Date" in cal.index:
                    earnings_dates = cal.loc["Earnings Date"].tolist()
            if earnings_dates:
                for ed in earnings_dates:
                    try:
                        ed_date = ed.date() if hasattr(ed, 'date') else date.fromisoformat(str(ed)[:10])
                        if start_date <= ed_date <= end_date:
                            catalysts.append({
                                "date": str(ed_date),
                                "type": "earnings",
                                "label": f"{ticker} Earnings",
                            })
                    except Exception as e:
                        log.debug("Derivatives: earnings date parse failed: {e}", e=str(e))
    except Exception as e:
        log.warning("Derivatives: catalyst aggregation failed: {e}", e=str(e))

    catalysts.sort(key=lambda c: c["date"])
    return catalysts


@router.get("/flow-timeline/{ticker}")
async def get_flow_timeline(
    ticker: str,
    days: int = Query(90, ge=7, le=365),
) -> dict[str, Any]:
    """Historical GEX timeline with OpEx calendar and catalysts.

    Builds a time-series of net GEX, spot price, and regime,
    overlaid with OpEx expiration dates and macro catalysts.
    """
    from datetime import timedelta

    ticker = ticker.upper()
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    db = get_db_engine()
    history: list[dict] = []
    gamma_flip_crossings: list[dict] = []

    # ── Try options_daily_signals first for stored data ──
    try:
        with db.connect() as conn:
            rows = conn.execute(text(
                "SELECT signal_date, spot_price, iv_atm, put_call_ratio, total_oi "
                "FROM options_daily_signals "
                "WHERE ticker = :t "
                "AND signal_date >= :start AND signal_date <= :end "
                "ORDER BY signal_date ASC"
            ), {"t": ticker, "start": start_date, "end": end_date}).fetchall()

        if rows:
            engine_gex = _get_gex_engine()
            prev_gex = None

            for r in rows:
                sig_date = r[0]
                spot = float(r[1]) if r[1] else 0
                try:
                    gex_result = engine_gex.compute_gex_profile(ticker, snap_date=sig_date)
                    net_gex = gex_result.get("gex_aggregate", 0)
                    regime_raw = (gex_result.get("regime") or "NEUTRAL").lower()
                    spot = gex_result.get("spot", spot)
                except Exception:
                    net_gex = 0
                    regime_raw = "neutral"

                history.append({
                    "date": str(sig_date),
                    "net_gex": round(net_gex),
                    "regime": "short_gamma" if regime_raw == "short_gamma" else
                              "long_gamma" if regime_raw == "long_gamma" else "neutral",
                    "spot": round(spot, 2),
                })

                if prev_gex is not None and prev_gex * net_gex < 0:
                    gamma_flip_crossings.append({
                        "date": str(sig_date),
                        "direction": "below" if net_gex < 0 else "above",
                        "spot_at_crossing": round(spot, 2),
                    })
                prev_gex = net_gex

    except Exception as exc:
        log.debug("Flow timeline daily signals query failed: {e}", e=str(exc))

    # ── Fallback: compute from latest snapshot ──
    if not history:
        try:
            engine_gex = _get_gex_engine()
            result = engine_gex.compute_gex_profile(ticker)
            if not result.get("error"):
                history.append({
                    "date": result.get("snap_date", str(end_date)),
                    "net_gex": round(result.get("gex_aggregate", 0)),
                    "regime": (result.get("regime") or "NEUTRAL").lower(),
                    "spot": result.get("spot", 0),
                })
        except Exception as exc:
            log.debug("Flow timeline GEX fallback failed: {e}", e=str(exc))

    # ── Generate OpEx calendar (past + 90 days forward) ──
    opex_calendar = _generate_opex_calendar(start_date, end_date + timedelta(days=90))

    # ── Generate catalysts (past + 90 days forward) ──
    catalysts = _generate_catalysts(start_date, end_date + timedelta(days=90), ticker)

    return {
        "ticker": ticker,
        "days": days,
        "history": history,
        "opex_calendar": opex_calendar,
        "catalysts": catalysts,
        "gamma_flip_crossings": gamma_flip_crossings,
    }


# ── GET /history/{ticker} ───────────────────────────────────────────

@router.get("/history/{ticker}")
async def get_history(
    ticker: str,
    days: int = Query(30, ge=1, le=365),
) -> dict[str, Any]:
    """Historical GEX-related data from options_daily_signals time series."""
    try:
        db = get_db_engine()
        with db.connect() as conn:
            rows = conn.execute(text(
                "SELECT signal_date, put_call_ratio, max_pain, iv_skew, "
                "total_oi, total_volume, spot_price, iv_atm, "
                "term_structure_slope, oi_concentration "
                "FROM options_daily_signals "
                "WHERE ticker = :t "
                "AND signal_date >= CURRENT_DATE - make_interval(days => :days) "
                "ORDER BY signal_date ASC"
            ), {"t": ticker.upper(), "days": days}).fetchall()

        history = [
            {
                "date": str(r[0]),
                "put_call_ratio": r[1], "max_pain": r[2],
                "iv_skew": r[3], "total_oi": r[4],
                "total_volume": r[5], "spot_price": r[6],
                "iv_atm": r[7], "term_structure_slope": r[8],
                "oi_concentration": r[9],
            }
            for r in rows
        ]

        return {
            "ticker": ticker.upper(),
            "days": days,
            "history": history,
            "count": len(history),
        }
    except Exception as exc:
        log.warning("Derivatives history failed for {t}: {e}", t=ticker, e=str(exc))
        return {"error": str(exc), "ticker": ticker.upper(), "history": []}
