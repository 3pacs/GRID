"""Intelligence sub-router: Risk map, dashboard, and globe endpoints."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from loguru import logger as log
from sqlalchemy.engine import Engine

from api.auth import require_auth
from api.dependencies import get_db_engine
from api.precompute_serving import serve_precomputed
from ingestion.market_calendar import is_market_open, trading_days_between
from store.pit import PITStore
from utils.ttl_cache import TTLCache

router = APIRouter(tags=["intelligence"])


# ── Risk Map ─────────────────────────────────────────────────────────────

_RISK_MAP_TTL = 300  # 5 minutes
_risk_map_cache: TTLCache = TTLCache(ttl=_RISK_MAP_TTL, max_size=1)
_RISK_MAP_CACHE_KEY = "default"


def _compute_risk_level(score: float) -> str:
    """Map a 0-1 risk score to a human-readable level."""
    if score >= 0.85:
        return "critical"
    if score >= 0.65:
        return "high"
    if score >= 0.45:
        return "elevated"
    if score >= 0.25:
        return "moderate"
    return "low"


def _level_to_score(level: str) -> float:
    """Map a risk level label back to a numeric score."""
    return {"critical": 0.95, "high": 0.75, "elevated": 0.55, "moderate": 0.35, "low": 0.15}.get(level, 0.5)


class _NoData(LookupError):
    """Raised inside a sub-system when the series it needs is absent.

    Caught by the per-sub-system handler exactly like a query failure, so
    the sub-system is reported as unavailable with the reason instead of
    being scored from a made-up default (VIX 20, HY 400bp, ...).
    """


def _unavailable(reason: str) -> dict[str, Any]:
    """Honest sub-system payload: no numbers, an explicit reason."""
    return {"risk_level": "unknown", "available": False, "reason": reason}


# ── Risk-map input series ────────────────────────────────────────────────
#
# The exact feature behind each input. Until 2026-11-23 these were ILIKE
# patterns read with ``ORDER BY obs_date DESC LIMIT 1``. Each pattern
# matched several features, and "latest" was whichever one had the newest
# date. On griddb (2026-11-23), '%hy%spread%' matched hy_oas_spread,
# hy_spread, hy_spread_3m_chg and hy_spread_proxy. '%tga%' matched
# tga_drawdown (a 30-day change), treasury_tga_balance (no rows since
# 2026-04-01), mortgage_30y and three EIA natgas series. '%vix%close%'
# matched nothing. '%ted%spread%' served ted_spread, which was deprecated
# 2026-04-03 and last observed 2022-01-21.
# Names follow normalization/entity_map.py. Units are as stored.
_VIX = "vix_spot"                  # VIXCLS / CBOE:VIX, index points
_HY_OAS = "hy_oas_spread"          # FRED BAMLH0A0HYM2, percent (sanity range 1-30)
_IG_OAS = "ig_oas_spread"          # FRED BAMLC0A0CM, percent (sanity range 0.1-10)
_FED_ASSETS = "fed_balance_sheet"  # FRED WALCL, millions USD, weekly (Wednesday)
_RRP = "reverse_repo_usage"        # FRED RRPONTSYD, billions USD
_TGA = "treasury_general_acct"     # FRED WTREGEN, millions USD, weekly (Wednesday)

#: How often each input is published; this sets its freshness rule.
_DAILY, _WEEKLY = "daily", "weekly"
_CADENCE: dict[str, str] = {
    _VIX: _DAILY, _HY_OAS: _DAILY, _IG_OAS: _DAILY, _RRP: _DAILY,
    _FED_ASSETS: _WEEKLY, _TGA: _WEEKLY,
}
#: A daily input is current if its latest reading is from one of the last
#: ``1 + _DAILY_SESSION_SLACK`` NYSE sessions on or before the cutoff. That
#: allows FRED's one-day publication lag plus one missed day.
_DAILY_SESSION_SLACK = 2
#: A weekly H.4.1 input (a Wednesday level, released Thursday) is current if
#: it is from the latest released Wednesday or the ``_WEEKLY_RELEASE_SLACK``
#: releases before it. One missed release is allowed.
_WEEKLY_RELEASE_SLACK = 1
#: Month-back comparisons are anchored to the current reading's own date,
#: not to the cutoff. The prior reading is the latest one at least
#: ``_MONTH_BACK_DAYS`` before it, and the span between the two may not
#: exceed ``_MONTH_BACK_MAX_SPAN_DAYS``. That is 30 days plus one weekly
#: step, so a weekly series always has such a reading. Measured from the
#: cutoff instead, a weekly print accepted at 21 days old could have been
#: compared over 11 days.
_MONTH_BACK_DAYS = 30
_MONTH_BACK_MAX_SPAN_DAYS = 37
#: VIX is ranked against the year before its current print, on NYSE
#: sessions only. Cboe's holiday prints are real, but research reads VIX on
#: the SPY session calendar, so they are left out of both the sample and the
#: coverage count. The rank is served only when the year covers
#: ``_VIX_MIN_COVERAGE`` of the expected sessions and the last
#: ``_VIX_TAIL_SESSIONS`` sessions are nearly complete. Otherwise the
#: sub-system abstains: one print would rank itself at 100.
_VIX_WINDOW_DAYS = 365
_VIX_MIN_COVERAGE = 0.90
_VIX_TAIL_SESSIONS = 21
_VIX_TAIL_MIN_COVERED = 19
#: Read window: the VIX year plus the longest month-back span.
_INPUT_WINDOW_DAYS = 400
#: A 1-month fall in Fed total assets of more than $50B. fed_balance_sheet
#: is stored in millions USD and is converted to dollars before this
#: comparison. The earlier code compared the raw millions against
#: -50_000_000_000, so the check could never fire.
_FED_ASSETS_DROP_USD = 50_000_000_000
_MUSD_TO_USD = 1_000_000
_PERCENT_TO_BP = 100.0


def _risk_cutoff() -> date:
    """The as-of date for the risk map's PIT read: today's UTC date.

    The resolver stamps ``vintage_date`` as the UTC date of the pull
    (``normalization/resolver.py``). A UTC cutoff therefore admits exactly
    the vintages GRID held by the end of that UTC day.
    """
    return datetime.now(timezone.utc).date()


def _freshness_floor(cadence: str, cutoff: date) -> date:
    """Oldest obs_date still counted as a current reading on *cutoff*."""
    if cadence == _DAILY:
        sessions = trading_days_between(cutoff - timedelta(days=21), cutoff)
        return sessions[-(1 + _DAILY_SESSION_SLACK)]
    latest_wednesday = cutoff - timedelta(days=(cutoff.weekday() - 2) % 7)
    if latest_wednesday + timedelta(days=1) > cutoff:  # released Thursday
        latest_wednesday -= timedelta(days=7)
    return latest_wednesday - timedelta(days=7 * _WEEKLY_RELEASE_SLACK)


def _month_back(rows: list[tuple[date, float]], current: date) -> tuple[date, float] | None:
    """The reading about a month before *current*, or None.

    This is the latest reading at least ``_MONTH_BACK_DAYS`` before
    *current*. It is None if the only such reading lies more than
    ``_MONTH_BACK_MAX_SPAN_DAYS`` before *current*.
    """
    target = current - timedelta(days=_MONTH_BACK_DAYS)
    older = [r for r in rows if r[0] <= target]
    if not older or (current - older[-1][0]).days > _MONTH_BACK_MAX_SPAN_DAYS:
        return None
    return older[-1]


def _vix_coverage(dates: Iterable[date], current: date) -> dict[str, int]:
    """Session coverage of the VIX year ending at *current*. Counts only, no score.

    ``expected`` counts the NYSE sessions in (current - 365d, current].
    ``covered`` counts how many of those have a print. The ``tail_*`` keys
    count the same over the last ``_VIX_TAIL_SESSIONS`` expected sessions.
    Prints on non-session days are never counted.
    """
    expected = trading_days_between(current - timedelta(days=_VIX_WINDOW_DAYS - 1), current)
    have = set(dates)
    tail = expected[-_VIX_TAIL_SESSIONS:]
    return {
        "expected": len(expected),
        "covered": sum(1 for d in expected if d in have),
        "tail_expected": len(tail),
        "tail_covered": sum(1 for d in tail if d in have),
    }


def _vix_coverage_shortfall(cov: dict[str, int], current: date) -> str | None:
    """Why the VIX year is too sparse to rank, or None if it is not."""
    need = math.ceil(_VIX_MIN_COVERAGE * cov["expected"])
    if cov["covered"] < need:
        return (
            f"vix_spot covers {cov['covered']} of {cov['expected']} NYSE sessions in the year "
            f"to {current.isoformat()} (need {need}); not ranked"
        )
    if cov["tail_covered"] < _VIX_TAIL_MIN_COVERED:
        return (
            f"vix_spot covers {cov['tail_covered']} of the last {cov['tail_expected']} NYSE "
            f"sessions to {current.isoformat()} (need {_VIX_TAIL_MIN_COVERED}); not ranked"
        )
    return None


def _read_risk_inputs(engine: Engine, cutoff: date) -> dict[str, list[tuple[date, float]]]:
    """Each risk input's series over the ``_INPUT_WINDOW_DAYS`` to *cutoff*, oldest first.

    Reads through ``store.pit.PITStore``. Names are bound to ids first.
    LATEST_AS_OF on *cutoff* gives one row per obs_date: the newest vintage
    held on that date, with retracted rows excluded. The old reads took any
    vintage of the newest date, and the VIX rank counted every vintage as
    a separate day (vix_spot: 4,517 rows over 258 dates in the last year).
    An unregistered name maps to ``[]``.
    """
    pit = PITStore(engine)
    ids = pit.feature_ids_by_name(list(_CADENCE))
    series: dict[str, list[tuple[date, float]]] = {name: [] for name in _CADENCE}
    if not ids:
        return series
    frame = pit.get_pit(
        list(ids.values()), cutoff, "LATEST_AS_OF",
        start_date=cutoff - timedelta(days=_INPUT_WINDOW_DAYS),
    )
    name_by_id = {fid: name for name, fid in ids.items()}
    for fid, obs_date, value in frame[["feature_id", "obs_date", "value"]].itertuples(index=False):
        if value is None or not math.isfinite(float(value)):
            continue
        if isinstance(obs_date, datetime):
            obs_date = obs_date.date()
        series[name_by_id[int(fid)]].append((obs_date, float(value)))
    for rows in series.values():
        rows.sort(key=lambda r: r[0])
    return series


def _build_risk_map() -> dict[str, Any]:
    """Assemble the full risk map from dealer gamma, volatility, concentration,
    correlation, credit, and liquidity sub-systems.

    Each sub-system is either measured (``available: True`` plus its
    metrics) or reported as ``{"risk_level": "unknown", "available": False,
    "reason": ...}``. Missing inputs are never replaced by defaults: an
    earlier version served VIX 20.0 / 50th percentile, HY 400bp, IG 100bp,
    TED 0.30, ``avg_cross_correlation`` 0.5 and ``risk_level: "moderate"``
    whenever a query failed or returned nothing, and those literals were
    indistinguishable from real readings.

    Volatility, credit and liquidity read one exact feature per input
    (``_VIX`` .. ``_TGA``) through the PIT store, once per build, as of the
    UTC date (``_risk_cutoff``). An input counts as missing if it has no
    rows, or if its latest reading is older than its cadence allows
    (``_freshness_floor``). Month-back comparisons are anchored to the
    current reading's own date (``_month_back``).
    """
    engine = get_db_engine()
    errors: list[str] = []
    sub_scores: list[float] = []
    cutoff = _risk_cutoff()

    def _fail(name: str, exc: BaseException) -> dict[str, Any]:
        reason = str(exc) or exc.__class__.__name__
        if isinstance(exc, _NoData):
            log.debug("Risk map {n}: no data ({r})", n=name, r=reason)
        else:
            log.debug("Risk map {n} failed: {r}", n=name, r=reason)
        errors.append(f"{name}: {reason}")
        return _unavailable(reason)

    # One read for every series input. A failure here fails each
    # sub-system that needs it, with the read's own error as the reason.
    inputs_error: BaseException | None = None
    try:
        inputs = _read_risk_inputs(engine, cutoff)
    except Exception as exc:
        inputs, inputs_error = {}, exc

    def _series(name: str) -> list[tuple[date, float]]:
        if inputs_error is not None:
            raise inputs_error
        return inputs.get(name, [])

    def _current(name: str, rows: list[tuple[date, float]] | None = None) -> tuple[date, float]:
        """Latest reading of *name*. Raises ``_NoData`` if absent or not current."""
        rows = _series(name) if rows is None else rows
        if not rows:
            raise _NoData(f"no {name} rows in resolved_series")
        obs_date, value = rows[-1]
        floor = _freshness_floor(_CADENCE[name], cutoff)
        if obs_date < floor:
            raise _NoData(
                f"{name} latest reading {obs_date.isoformat()} is older than its "
                f"{_CADENCE[name]} freshness floor {floor.isoformat()} on {cutoff.isoformat()}"
            )
        return obs_date, value

    def _current_or_none(name: str) -> tuple[date, float] | None:
        try:
            return _current(name)
        except _NoData:
            return None

    # ── 1. Dealer Risk ──────────────────────────────────────────────
    try:
        from physics.dealer_gamma import DealerGammaEngine

        gex_engine = DealerGammaEngine(engine)
        spy_gex = gex_engine.compute_gex_profile("SPY")

        if "error" in spy_gex:
            raise _NoData(f"SPY gamma profile unavailable: {spy_gex['error']}")

        regime = spy_gex.get("regime", "NEUTRAL").lower()
        net_gex = spy_gex.get("gex_aggregate", 0)
        spot = spy_gex.get("spot", 0)
        gamma_flip = spy_gex.get("gamma_flip")
        vanna = spy_gex.get("vanna_exposure", 0)
        charm = spy_gex.get("charm_exposure", 0)

        flip_dist_pct = 0.0
        if gamma_flip and spot > 0:
            flip_dist_pct = round((gamma_flip - spot) / spot * 100, 2)

        # Find nearest OPEX (third Friday)
        days_to_opex = 30
        for d_offset in range(45):
            candidate = cutoff + timedelta(days=d_offset)
            if candidate.weekday() == 4:  # Friday
                week_num = (candidate.day - 1) // 7 + 1
                if week_num == 3:
                    days_to_opex = d_offset
                    break

        # Score: short gamma + close to flip + near OPEX = high risk
        d_score = 0.3
        if regime == "short_gamma":
            d_score += 0.35
        if abs(flip_dist_pct) < 1.0:
            d_score += 0.15
        if days_to_opex < 5:
            d_score += 0.15
        d_score = min(d_score, 1.0)

        dealer_risk: dict[str, Any] = {
            "gex_regime": regime,
            "net_gex": round(net_gex),
            "gamma_flip_distance_pct": flip_dist_pct,
            "vanna_direction": "adverse" if vanna < 0 else "supportive",
            "charm_direction": "bearish" if charm < 0 else "bullish",
            "days_to_opex": days_to_opex,
            "risk_level": _compute_risk_level(d_score),
            "available": True,
        }
        sub_scores.append(d_score)
    except Exception as exc:
        dealer_risk = _fail("dealer_risk", exc)

    # ── 2. Volatility Risk ──────────────────────────────────────────
    try:
        # NYSE sessions only, for the current print, the rank and the coverage.
        vix_rows = [r for r in _series(_VIX) if is_market_open(r[0])]
        vix_date, vix_val = _current(_VIX, vix_rows)
        year_start = vix_date - timedelta(days=_VIX_WINDOW_DAYS)
        window = [(d, v) for d, v in vix_rows if year_start < d <= vix_date]
        coverage = _vix_coverage((d for d, _ in window), vix_date)
        shortfall = _vix_coverage_shortfall(coverage, vix_date)
        if shortfall:
            raise _NoData(shortfall)
        vals = [v for _, v in window]
        pct = int(sum(1 for v in vals if v <= vix_val) / len(vals) * 100)

        v_score = min(pct / 100, 1.0)
        vol_risk: dict[str, Any] = {
            "vix": round(vix_val, 1),
            "vix_as_of": vix_date.isoformat(),
            "vix_percentile_1y": pct,
            # One print per NYSE session, at its newest vintage.
            "vix_percentile_sample": len(vals),
            "vix_sessions_expected": coverage["expected"],
            # No realized-vol series is read here; the previous value was
            # vix / (0.9 * vix), a constant 1.11 for every reading.
            "realized_vs_implied": None,
            "risk_level": _compute_risk_level(v_score),
            "available": True,
        }
        sub_scores.append(v_score)
    except Exception as exc:
        vol_risk = _fail("volatility_risk", exc)

    # ── 3. Concentration Risk ───────────────────────────────────────
    try:
        from sqlalchemy import text as sql_text

        with engine.connect() as conn:
            positions = conn.execute(sql_text("""
                SELECT ticker, allocation_pct, sector
                FROM watchlist
                WHERE active = true
                ORDER BY allocation_pct DESC NULLS LAST
            """)).fetchall()

        if not positions:
            raise _NoData("no active watchlist positions")

        allocs = [(r[0], float(r[1] or 0), r[2] or "other") for r in positions]
        total_alloc = sum(a for _, a, _ in allocs) or 1.0
        weights = [(t, a / total_alloc, s) for t, a, s in allocs]

        top5_w = sum(w for _, w, _ in weights[:5])
        sectors: dict[str, float] = {}
        for _, w, s in weights:
            sectors[s] = sectors.get(s, 0) + w

        max_name = weights[0] if weights else ("N/A", 0, "")

        c_score = 0.15
        if top5_w > 0.7:
            c_score += 0.35
        elif top5_w > 0.5:
            c_score += 0.2
        max_sector = max(sectors.values()) if sectors else 0
        if max_sector > 0.4:
            c_score += 0.25
        if max_name[1] > 0.2:
            c_score += 0.2
        c_score = min(c_score, 1.0)

        conc_risk: dict[str, Any] = {
            "top_5_watchlist_weight": round(top5_w, 2),
            "sector_concentration": {k: round(v, 2) for k, v in sorted(sectors.items(), key=lambda x: -x[1])[:6]},
            "single_name_max": {"ticker": max_name[0], "weight": round(max_name[1], 2)},
            "risk_level": _compute_risk_level(c_score),
            "available": True,
        }
        sub_scores.append(c_score)
    except Exception as exc:
        conc_risk = _fail("concentration_risk", exc)

    # ── 4. Correlation Risk ─────────────────────────────────────────
    try:
        from sqlalchemy import text as sql_text

        with engine.connect() as conn:
            rows = conn.execute(sql_text("""
                SELECT fr.name, rs.obs_date, rs.value
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name LIKE '%%_close'
                AND rs.obs_date >= CURRENT_DATE - INTERVAL '90 days'
                ORDER BY fr.name, rs.obs_date
            """)).fetchall()

        if not rows or len(rows) <= 50:
            raise _NoData(
                f"insufficient 90d close history ({len(rows) if rows else 0} rows, need >50)"
            )

        import pandas as pd
        import numpy as np

        df = pd.DataFrame(rows, columns=["name", "date", "value"])
        pivot = df.pivot_table(index="date", columns="name", values="value")
        returns = pivot.pct_change().dropna()

        if returns.shape[1] < 2:
            raise _NoData("fewer than two close series overlap in the last 90d")

        corr_mat = returns.corr()
        mask = np.triu(np.ones_like(corr_mat, dtype=bool), k=1)
        upper = corr_mat.where(mask)
        avg_corr = float(upper.stack().mean())

        decouplings = []
        recent = returns.tail(22)
        if len(recent) >= 10:
            corr_30d = recent.corr()
            cols = list(corr_mat.columns)
            for i in range(len(cols)):
                for j in range(i + 1, min(len(cols), i + 5)):
                    c90 = corr_mat.iloc[i, j]
                    c30 = corr_30d.iloc[i, j]
                    if abs(c90 - c30) > 0.3:
                        decouplings.append({
                            "pair": f"{cols[i]}/{cols[j]}",
                            "correlation_30d": round(float(c30), 2),
                            "correlation_90d": round(float(c90), 2),
                        })
            decouplings.sort(key=lambda x: abs(x["correlation_90d"] - x["correlation_30d"]), reverse=True)

        co_score = max(0, min(avg_corr, 1.0))

        corr_risk: dict[str, Any] = {
            "avg_cross_correlation": round(avg_corr, 2),
            "series_count": int(returns.shape[1]),
            "decoupling_events": decouplings[:5],
            "risk_level": _compute_risk_level(co_score),
            "available": True,
        }
        sub_scores.append(co_score)
    except Exception as exc:
        corr_risk = _fail("correlation_risk", exc)

    # ── 5. Credit Risk ──────────────────────────────────────────────
    try:
        # hy_oas_spread / ig_oas_spread are stored in percent (FRED); the
        # score and the PWA ("HY: {hy_spread}bp") are in basis points. The
        # old read served 3.76 % as "hy_spread": 4 and scored it 3.76 / 800.
        hy_date, hy_pct = _current(_HY_OAS)
        hy_bp = hy_pct * _PERCENT_TO_BP
        hy_prev = _month_back(_series(_HY_OAS), hy_date)
        hy_prev_bp = hy_prev[1] * _PERCENT_TO_BP if hy_prev else None
        # IG is reported when current and null otherwise; it does not enter
        # the score, so its absence does not block the read.
        ig = _current_or_none(_IG_OAS)
        ig_bp = ig[1] * _PERCENT_TO_BP if ig else None

        if hy_prev_bp is None:
            direction = "unknown"
        elif hy_bp > hy_prev_bp * 1.05:
            direction = "widening"
        elif hy_bp < hy_prev_bp * 0.95:
            direction = "tightening"
        else:
            direction = "stable"

        cr_score = min(hy_bp / 800, 1.0)
        if direction == "widening":
            cr_score = min(cr_score + 0.15, 1.0)

        credit_risk: dict[str, Any] = {
            "hy_spread": round(hy_bp),
            "hy_spread_as_of": hy_date.isoformat(),
            "hy_spread_prior_as_of": hy_prev[0].isoformat() if hy_prev else None,
            "ig_spread": round(ig_bp) if ig_bp is not None else None,
            "spread_direction": direction,
            "risk_level": _compute_risk_level(cr_score),
            "available": True,
        }
        sub_scores.append(cr_score)
    except Exception as exc:
        credit_risk = _fail("credit_risk", exc)

    # ── 6. Liquidity Risk ───────────────────────────────────────────
    try:
        # Fed total assets (WALCL), not net liquidity: TGA and RRP enter the
        # score separately below. Until 2026-11-23 this change was served as
        # ``fed_net_liquidity_change_1m``, in millions, measured from the
        # cutoff rather than from the current print.
        fa_date, fa_now = _current(_FED_ASSETS)
        fa_prev = _month_back(_series(_FED_ASSETS), fa_date)
        if fa_prev is None:
            raise _NoData(
                f"no {_FED_ASSETS} reading {_MONTH_BACK_DAYS}-{_MONTH_BACK_MAX_SPAN_DAYS} days "
                f"before {fa_date.isoformat()} to measure the 1m change"
            )
        fa_change_usd = (fa_now - fa_prev[1]) * _MUSD_TO_USD

        def _trend(name: str, up_label: str, down_label: str) -> str:
            cur = _current_or_none(name)
            prev = _month_back(_series(name), cur[0]) if cur else None
            if cur is None or prev is None:
                return "unknown"
            if cur[1] < prev[1] * 0.95:
                return down_label
            if cur[1] > prev[1] * 1.05:
                return up_label
            return "stable"

        rrp_trend = _trend(_RRP, "rising", "declining")
        tga_trend = _trend(_TGA, "building", "draining")

        l_score = 0.3
        if fa_change_usd < -_FED_ASSETS_DROP_USD:
            l_score += 0.3
        elif fa_change_usd < 0:
            l_score += 0.15
        if tga_trend == "building":
            l_score += 0.15
        if rrp_trend == "declining":
            l_score -= 0.1
        l_score = max(0, min(l_score, 1.0))

        liq_risk: dict[str, Any] = {
            "fed_total_assets_change_1m_usd": round(fa_change_usd),
            "fed_total_assets_as_of": fa_date.isoformat(),
            "fed_total_assets_prior_as_of": fa_prev[0].isoformat(),
            "reverse_repo_trend": rrp_trend,
            "tga_trend": tga_trend,
            "risk_level": _compute_risk_level(l_score),
            "available": True,
        }
        sub_scores.append(l_score)
    except Exception as exc:
        liq_risk = _fail("liquidity_risk", exc)

    # ── Overall Score & Narrative ────────────────────────────────────
    all_risks = {
        "Dealer positioning": dealer_risk,
        "Volatility": vol_risk,
        "Concentration": conc_risk,
        "Correlation": corr_risk,
        "Credit spreads": credit_risk,
        "Liquidity": liq_risk,
    }
    unavailable = [name for name, r in all_risks.items() if not r.get("available")]
    overall = round(sum(sub_scores) / len(sub_scores), 2) if sub_scores else None

    parts = []
    elevated_cats = [name for name, r in all_risks.items() if r["risk_level"] in ("elevated", "high", "critical")]
    if overall is None:
        parts.append("No risk sub-system could be measured; the overall score is unavailable.")
    elif not elevated_cats:
        parts.append("All measured risk categories are within normal ranges.")
    elif len(elevated_cats) == 1:
        parts.append(f"{elevated_cats[0]} risk is elevated and warrants monitoring.")
    else:
        parts.append(f"Multiple risk factors are elevated: {', '.join(elevated_cats)}.")

    if dealer_risk.get("gex_regime") == "short_gamma":
        parts.append("Dealers are short gamma, amplifying directional moves.")
    if (vol_risk.get("vix") or 0) > 25:
        parts.append(f"VIX at {vol_risk['vix']} signals elevated implied volatility.")
    if (conc_risk.get("top_5_watchlist_weight") or 0) > 0.7:
        parts.append("Portfolio is highly concentrated in top 5 positions.")
    if credit_risk.get("spread_direction") == "widening":
        parts.append("Credit spreads are widening, signaling deteriorating risk appetite.")
    if liq_risk.get("tga_trend") == "building":
        parts.append("Treasury General Account is building, draining market liquidity.")

    if len(elevated_cats) >= 3:
        parts.append("RISK CONVERGENCE: multiple risk factors elevated simultaneously -- reduce exposure.")
    if unavailable:
        parts.append(
            f"Unavailable ({len(unavailable)} of {len(all_risks)}): {', '.join(unavailable)}."
        )

    return {
        "dealer_risk": dealer_risk,
        "volatility_risk": vol_risk,
        "concentration_risk": conc_risk,
        "correlation_risk": corr_risk,
        "credit_risk": credit_risk,
        "liquidity_risk": liq_risk,
        "overall_risk_score": overall,
        "available_subsystems": len(sub_scores),
        "unavailable_subsystems": unavailable,
        "risk_narrative": " ".join(parts),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "errors": errors,
    }


@router.get("/risk-map")
async def get_risk_map(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Unified risk map -- all risk exposures sized and colored by threat level.

    Returns dealer, volatility, concentration, correlation, credit, and
    liquidity risk assessments with an overall risk score and narrative.
    Sub-systems without data are ``{"risk_level": "unknown", "available":
    false, "reason": ...}`` and ``overall_risk_score`` is ``null`` when
    nothing could be measured. A failure of the builder itself is a 503,
    never a placeholder payload. Cached for 5 minutes.
    """
    cached = _risk_map_cache.get(_RISK_MAP_CACHE_KEY)
    if cached is not None:
        return cached

    import asyncio

    try:
        result = await asyncio.to_thread(_build_risk_map)
    except Exception as exc:
        log.error("Risk map build failed: {e}", e=str(exc))
        raise HTTPException(
            status_code=503,
            detail=f"Risk map unavailable: {exc}",
        ) from exc

    if result.get("available_subsystems", 0) > 0:
        # Only pin a payload that measured at least one sub-system. A fully
        # unavailable read is still served honestly, but the next caller
        # should retry rather than be handed a 5-minute-old "nothing".
        _risk_map_cache.set(_RISK_MAP_CACHE_KEY, result)
    return result


# ── Unified Intelligence Dashboard ──────────────────────────────────────

# Thread-safe TTL cache for dashboard snapshots
_DASHBOARD_CACHE_TTL = 600  # 10 minutes
_dashboard_cache: TTLCache = TTLCache(ttl=_DASHBOARD_CACHE_TTL, max_size=10)


def _build_dashboard_snapshot() -> dict[str, Any]:
    """Assemble a unified intelligence snapshot from all subsystems.

    Calls each intelligence module, catches failures individually so
    partial data is still returned, and computes an overall confidence.
    """
    from datetime import datetime, timezone

    engine = get_db_engine()
    snapshot: dict[str, Any] = {
        "trust": {"top_sources": [], "convergence_events": []},
        "levers": {"active_events": [], "top_pullers": []},
        "cross_ref": {"red_flags": [], "total_checks": 0},
        "source_audit": {"discrepancies": [], "single_points_of_failure": 0},
        "postmortems": {"recent_failures": [], "lessons": ""},
        "overall_confidence": 0.5,
        "narrative": "",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "errors": [],
    }

    confidence_components: list[float] = []

    # ── Trust & Convergence ──────────────────────────────────────────
    try:
        # Perf 2026-05-16: read from source_trust_scores_cached (populated
        # nightly by flows/refresh_trust_scores.py) instead of recomputing
        # 2,771 sources on every cold dashboard hit (was ~12s, now ~50ms).
        # Stale-while-revalidate: if cache is empty or >24h old, fall back
        # to a live recompute so the dashboard still renders correctly
        # before/after the nightly flow has run.
        from intelligence.trust_scorer import (
            load_trust_scores_cached,
            update_trust_scores,
            detect_convergence,
        )

        trust_data = load_trust_scores_cached(engine)
        if trust_data is None:
            log.warning(
                "Dashboard trust cache miss — falling back to live recompute "
                "(slow). Run flows/refresh_trust_scores.py to repopulate."
            )
            trust_data = update_trust_scores(engine)
        all_sources = trust_data.get("sources", [])
        top_5 = all_sources[:5]
        convergence = detect_convergence(engine)

        snapshot["trust"] = {
            "top_sources": top_5,
            "convergence_events": convergence[:10],
            "total_tracked": len(all_sources),
        }

        # Confidence contribution: high if many sources, strong convergence
        if all_sources:
            avg_trust = sum(s.get("trust_score", 0.5) for s in top_5) / len(top_5) if top_5 else 0.5
            confidence_components.append(avg_trust)
        if convergence:
            confidence_components.append(min(0.9, 0.5 + 0.1 * len(convergence)))
    except Exception as exc:
        log.warning("Dashboard: trust module failed: {e}", e=str(exc))
        snapshot["errors"].append(f"trust: {exc}")

    # ── Lever Pullers ────────────────────────────────────────────────
    try:
        from intelligence.lever_pullers import (
            get_active_lever_events,
            identify_lever_pullers,
            find_lever_convergence,
        )

        pullers = identify_lever_pullers(engine)
        events = get_active_lever_events(engine, days=14, pullers=pullers)
        lever_convergence = find_lever_convergence(engine, pullers=pullers)

        event_dicts = []
        for ev in events[:10]:
            event_dicts.append({
                "puller_name": ev.puller.name,
                "category": ev.puller.category,
                "action": ev.action,
                "tickers": ev.tickers,
                "timestamp": ev.timestamp,
                "motivation": ev.motivation_assessment,
                "confidence": ev.confidence,
            })

        puller_dicts = []
        for p in pullers[:5]:
            puller_dicts.append({
                "name": p.name,
                "category": p.category,
                "trust_score": p.trust_score,
                "influence_rank": p.influence_rank,
                "position": p.position,
            })

        snapshot["levers"] = {
            "active_events": event_dicts,
            "top_pullers": puller_dicts,
            "convergence": lever_convergence[:5],
        }

        if events:
            avg_conf = sum(ev.confidence for ev in events[:10]) / min(len(events), 10)
            confidence_components.append(avg_conf)
    except Exception as exc:
        log.warning("Dashboard: lever module failed: {e}", e=str(exc))
        snapshot["errors"].append(f"levers: {exc}")

    # ── Cross-Reference (Lie Detector) ───────────────────────────────
    try:
        from intelligence.cross_reference import run_all_checks

        # Perf 2026-05-16: skip the LLM narrative generation on the cold
        # dashboard render — the dashboard only displays red_flags + counts
        # from the report, never the narrative. Saves ~2-5s on cold load.
        report = run_all_checks(engine, skip_narrative=True)
        red_flags = [asdict(c) for c in report.red_flags[:10]]

        snapshot["cross_ref"] = {
            "red_flags": red_flags,
            "total_checks": len(report.checks),
            "summary": getattr(report, "summary", {}),
        }

        # More red flags = lower confidence
        flag_ratio = len(report.red_flags) / max(len(report.checks), 1)
        confidence_components.append(max(0.2, 1.0 - flag_ratio * 2))
    except Exception as exc:
        log.warning("Dashboard: cross-ref module failed: {e}", e=str(exc))
        snapshot["errors"].append(f"cross_ref: {exc}")

    # ── Source Audit ─────────────────────────────────────────────────
    try:
        from intelligence.source_audit import get_latest_audit_summary

        audit = get_latest_audit_summary(engine)

        snapshot["source_audit"] = {
            "discrepancies": audit.get("recent_discrepancies", [])[:10],
            "single_points_of_failure": audit.get("single_source_count", 0),
            "redundancy_map_size": audit.get("redundancy_map_size", 0),
            "recent_accuracy": audit.get("recent_accuracy", [])[:5],
        }

        disc_count = len(audit.get("recent_discrepancies", []))
        if disc_count > 5:
            confidence_components.append(0.4)
        elif disc_count > 0:
            confidence_components.append(0.7)
        else:
            confidence_components.append(0.85)
    except Exception as exc:
        log.warning("Dashboard: source audit failed: {e}", e=str(exc))
        snapshot["errors"].append(f"source_audit: {exc}")

    # ── Post-Mortems ─────────────────────────────────────────────────
    try:
        # Perf 2026-05-16: dashboard only displays the top-5 most recent
        # post-mortems plus a total count, but the previous code hydrated
        # ALL rows in the 30d window (93K+ on grid-svr → 24-31s cold).
        # Use the fast path (LIMIT 5 + no full_analysis JSONB) plus a cheap
        # COUNT(*) for the badge. The LLM-generated `lessons` narrative is
        # also dropped from the cold path — it costs ~6-8s and the frontend
        # can load it via a separate async fetch from a dedicated endpoint
        # if/when a Lessons widget needs it.
        from intelligence.postmortem import load_postmortems_top_n, count_postmortems

        recent = load_postmortems_top_n(engine, n=5, days=30)
        total_count = count_postmortems(engine, days=30)

        snapshot["postmortems"] = {
            "recent_failures": recent,
            # NOTE(perf 2026-05-16): cold dashboard no longer runs the LLM
            # lessons synthesis. Empty string is the agreed contract; a
            # follow-up PR will expose lessons via /api/v1/intelligence/
            # postmortem-lessons for the (planned) async Lessons widget.
            "lessons": "",
            "total_count": total_count,
        }
    except Exception as exc:
        log.warning("Dashboard: postmortem module failed: {e}", e=str(exc))
        snapshot["errors"].append(f"postmortems: {exc}")

    # ── Overall Confidence ───────────────────────────────────────────
    if confidence_components:
        snapshot["overall_confidence"] = round(
            sum(confidence_components) / len(confidence_components), 3
        )
    else:
        snapshot["overall_confidence"] = 0.5

    # ── Narrative ────────────────────────────────────────────────────
    try:
        narrative_parts = []
        conf = snapshot["overall_confidence"]
        if conf >= 0.7:
            narrative_parts.append(
                f"System confidence is HIGH at {conf:.0%}."
            )
        elif conf >= 0.5:
            narrative_parts.append(
                f"System confidence is MODERATE at {conf:.0%}."
            )
        else:
            narrative_parts.append(
                f"System confidence is LOW at {conf:.0%} — review red flags."
            )

        trust_data = snapshot["trust"]
        if trust_data["convergence_events"]:
            n = len(trust_data["convergence_events"])
            narrative_parts.append(
                f"{n} convergence event(s) detected — multiple sources agree."
            )

        red_flags = snapshot["cross_ref"]["red_flags"]
        if red_flags:
            categories = set(f.get("category", "unknown") for f in red_flags)
            narrative_parts.append(
                f"{len(red_flags)} cross-reference red flag(s) in: {', '.join(categories)}."
            )

        spof = snapshot["source_audit"]["single_points_of_failure"]
        if spof > 10:
            narrative_parts.append(
                f"Warning: {spof} features rely on a single data source."
            )

        pm_count = snapshot["postmortems"].get("total_count", 0)
        if pm_count > 0:
            narrative_parts.append(
                f"{pm_count} post-mortem(s) in the last 30 days — check lessons learned."
            )

        snapshot["narrative"] = " ".join(narrative_parts)
    except Exception as exc:
        snapshot["narrative"] = f"Narrative generation failed: {exc}"

    return snapshot


@router.get("/globe")
def get_globe_data(
    _token: str = Depends(require_auth),
) -> Response:
    """Return global economic activity data for the 3D globe visualization.

    Aggregates country-level GDP signals, FX changes, VIIRS night lights,
    trade flows (Comtrade bilateral), capital flows, cross-reference hotspots,
    and FX pair data from resolved_series and raw_series.

    Served from ``api_precomputed_results`` (job ``globe`` in
    ``intelligence/api_precompute.py``, hourly); the build (>125 s before
    W3-2: it re-ran every cross-reference check *with* the LLM narrative)
    never runs in the request.
    """
    return serve_precomputed("intelligence.globe", {})


# source_catalog names of the pullers that write bilateral trade series
# (ingestion/trade/comtrade.py, ingestion/trade/cepii.py).
GLOBE_TRADE_SOURCES = ("Comtrade", "CEPII_BACI")

# Hotspots come from the cross-reference checks Hermes persists every 4 h.
# Older than this, the globe recomputes the checks (without the narrative).
GLOBE_XREF_MAX_AGE_S = 8 * 3600


def _build_globe_data() -> dict[str, Any]:
    """Build globe data (runs in thread pool)."""
    from datetime import datetime, timezone, date as dt_date, timedelta

    engine = get_db_engine()
    today = dt_date.today()
    one_month_ago = today - timedelta(days=30)
    one_year_ago = today - timedelta(days=365)

    countries: list[dict[str, Any]] = []
    flows: list[dict[str, Any]] = []
    hotspots: list[dict[str, Any]] = []
    fx_map: dict[str, float | None] = {}

    def _pct_change(current, previous):
        if current is None or previous is None or previous == 0:
            return None
        return round((current - previous) / abs(previous), 4)

    # ── Country definitions with data series mappings ──
    COUNTRY_CONFIG = [
        {"id": "USA", "name": "United States", "gdp_series": "INDPRO", "fx_series": "DX-Y.NYB",
         "lights_series": None, "trade_prefix": "US"},
        {"id": "CHN", "name": "China", "gdp_series": "china_gdp_real_imf", "fx_series": "USDCNY",
         "lights_series": "viirs_china_lights", "trade_prefix": "CN"},
        {"id": "JPN", "name": "Japan", "gdp_series": "japan_gdp_real_oecd", "fx_series": "USDJPY",
         "lights_series": "viirs_japan_lights", "trade_prefix": "JP"},
        {"id": "DEU", "name": "Germany", "gdp_series": "germany_gdp_real_oecd", "fx_series": "EURUSD",
         "lights_series": "viirs_germany_lights", "trade_prefix": "DE"},
        {"id": "GBR", "name": "United Kingdom", "gdp_series": "uk_gdp_real_oecd", "fx_series": "GBPUSD",
         "lights_series": "viirs_uk_lights", "trade_prefix": "GB"},
        {"id": "FRA", "name": "France", "gdp_series": "france_gdp_real_oecd", "fx_series": "EURUSD",
         "lights_series": "viirs_france_lights", "trade_prefix": "FR"},
        {"id": "IND", "name": "India", "gdp_series": "india_gdp_real_imf", "fx_series": "USDINR",
         "lights_series": "viirs_india_lights", "trade_prefix": "IN"},
        {"id": "BRA", "name": "Brazil", "gdp_series": "brazil_gdp_real_imf", "fx_series": "USDBRL",
         "lights_series": "viirs_brazil_lights", "trade_prefix": "BR"},
        {"id": "KOR", "name": "South Korea", "gdp_series": "korea_gdp_real_oecd", "fx_series": "USDKRW",
         "lights_series": "viirs_korea_lights", "trade_prefix": "KR"},
        {"id": "AUS", "name": "Australia", "gdp_series": "australia_gdp_real_oecd", "fx_series": "AUDUSD",
         "lights_series": None, "trade_prefix": "AU"},
        {"id": "CAN", "name": "Canada", "gdp_series": "canada_gdp_real_oecd", "fx_series": "USDCAD",
         "lights_series": None, "trade_prefix": "CA"},
        {"id": "MEX", "name": "Mexico", "gdp_series": "mexico_gdp_real_imf", "fx_series": "USDMXN",
         "lights_series": None, "trade_prefix": "MX"},
    ]

    # ── Batch-fetch all series values in 2 queries instead of 72+ ──
    all_series_ids: set[str] = set()
    for cfg in COUNTRY_CONFIG:
        all_series_ids.add(cfg["gdp_series"])
        all_series_ids.add(f"YF:{cfg['fx_series']}:close")
        all_series_ids.add(cfg["fx_series"])
        if cfg.get("lights_series"):
            all_series_ids.add(cfg["lights_series"])

    # Also collect FX pair series
    FX_PAIRS = {
        "DXY": "DX-Y.NYB", "EURUSD": "EURUSD=X", "USDJPY": "JPY=X",
        "GBPUSD": "GBPUSD=X", "USDCNY": "CNY=X", "AUDUSD": "AUDUSD=X",
        "USDCAD": "CAD=X", "USDINR": "INR=X", "USDBRL": "BRL=X",
        "USDKRW": "KRW=X", "USDMXN": "MXN=X",
    }
    for ticker in FX_PAIRS.values():
        all_series_ids.add(f"YF:{ticker}:close")
        all_series_ids.add(ticker)

    # Batch fetch: latest value + month-ago value for all series in 2 queries
    latest_vals: dict[str, float] = {}
    month_ago_vals: dict[str, float] = {}
    latest_obs_date: dt_date | None = None
    try:
        from sqlalchemy import text as sqla_text
        ids_list = list(all_series_ids)
        with engine.connect() as conn:
            # Latest values (as of today)
            rows = conn.execute(sqla_text(
                "SELECT DISTINCT ON (series_id) series_id, value, obs_date "
                "FROM raw_series "
                "WHERE series_id = ANY(:ids) AND obs_date <= :d AND pull_status = 'SUCCESS' "
                "ORDER BY series_id, obs_date DESC"
            ), {"ids": ids_list, "d": today}).fetchall()
            for r in rows:
                latest_vals[r[0]] = float(r[1])
                if r[2] is not None and (latest_obs_date is None or r[2] > latest_obs_date):
                    latest_obs_date = r[2]

            # Month-ago values
            rows = conn.execute(sqla_text(
                "SELECT DISTINCT ON (series_id) series_id, value "
                "FROM raw_series "
                "WHERE series_id = ANY(:ids) AND obs_date <= :d AND pull_status = 'SUCCESS' "
                "ORDER BY series_id, obs_date DESC"
            ), {"ids": ids_list, "d": one_month_ago}).fetchall()
            for r in rows:
                month_ago_vals[r[0]] = float(r[1])
    except Exception as exc:
        log.debug("Globe batch fetch failed: {e}", e=str(exc))

    def _get_val(sid: str, cache: dict) -> float | None:
        return cache.get(sid)

    # ── Build country data ──
    for cfg in COUNTRY_CONFIG:
        try:
            gdp_now = _get_val(cfg["gdp_series"], latest_vals)
            gdp_prev = _get_val(cfg["gdp_series"], month_ago_vals)
            gdp_change = _pct_change(gdp_now, gdp_prev)

            if gdp_change is not None:
                if gdp_change > 0.005:
                    gdp_signal = "growth"
                elif gdp_change < -0.005:
                    gdp_signal = "slowing"
                else:
                    gdp_signal = "stable"
            else:
                gdp_signal = "no_data"

            fx_now = _get_val(f"YF:{cfg['fx_series']}:close", latest_vals)
            if fx_now is None:
                fx_now = _get_val(cfg["fx_series"], latest_vals)
            fx_prev = _get_val(f"YF:{cfg['fx_series']}:close", month_ago_vals)
            if fx_prev is None:
                fx_prev = _get_val(cfg["fx_series"], month_ago_vals)
            fx_change = _pct_change(fx_now, fx_prev)

            lights_change = None
            if cfg["lights_series"]:
                lights_now = _get_val(cfg["lights_series"], latest_vals)
                lights_prev = _get_val(cfg["lights_series"], month_ago_vals)
                lights_change = _pct_change(lights_now, lights_prev)

            score_parts = []
            if gdp_change is not None:
                score_parts.append(max(0, min(1, 0.5 + gdp_change * 10)))
            if fx_change is not None:
                score_parts.append(max(0, min(1, 0.5 + fx_change * 5)))
            if lights_change is not None:
                score_parts.append(max(0, min(1, 0.5 + lights_change * 8)))
            # No fabricated neutral score when nothing could be measured: a
            # made-up 0.5 was indistinguishable from a genuinely neutral
            # reading. `None` here means "not generated" for this country.
            activity_score = round(sum(score_parts) / len(score_parts), 2) if score_parts else None

            countries.append({
                "id": cfg["id"],
                "name": cfg["name"],
                "gdp_signal": gdp_signal,
                "fx_change_1m": fx_change,
                "night_lights_change": lights_change,
                "activity_score": activity_score,
            })
        except Exception as exc:
            log.debug("Globe country {c} failed: {e}", c=cfg["id"], e=str(exc))
            countries.append({
                "id": cfg["id"], "name": cfg["name"],
                "gdp_signal": "no_data", "fx_change_1m": None,
                "night_lights_change": None, "activity_score": None,
            })

    # ── Trade & capital flows from Comtrade bilateral data ──
    TRADE_PAIRS = [
        ("USA", "CHN"), ("USA", "JPN"), ("USA", "DEU"), ("USA", "MEX"),
        ("USA", "CAN"), ("USA", "KOR"), ("USA", "GBR"), ("USA", "IND"),
        ("CHN", "JPN"), ("CHN", "KOR"), ("CHN", "DEU"), ("CHN", "AUS"),
        ("DEU", "FRA"), ("JPN", "KOR"), ("GBR", "DEU"),
    ]

    id_to_prefix = {cfg["id"]: cfg["trade_prefix"] for cfg in COUNTRY_CONFIG}

    try:
        from sqlalchemy import text as sqla_text
        with engine.connect() as conn:
            # Bilateral *trade* series only, found through their sources.
            # ``series_id LIKE '%bilateral%'`` over all of raw_series was a
            # parallel seq scan (>90 s on grid-svr, 2026-11-06) and also
            # matched non-trade series (GDELT bilateral tension). Two indexed
            # reads instead: the trade sources' series ids, then their rows.
            src_ids = [r[0] for r in conn.execute(sqla_text(
                "SELECT id FROM source_catalog WHERE name = ANY(:names)"
            ), {"names": list(GLOBE_TRADE_SOURCES)}).fetchall()]
            trade_ids = [r[0] for r in conn.execute(sqla_text(
                "SELECT DISTINCT series_id FROM raw_series "
                "WHERE source_id = ANY(:src) AND series_id LIKE :pattern"
            ), {"src": src_ids, "pattern": "%bilateral%"}).fetchall()] if src_ids else []
            trade_rows = conn.execute(sqla_text(
                "SELECT series_id, value, obs_date FROM raw_series "
                "WHERE series_id = ANY(:ids) AND pull_status = 'SUCCESS' "
                "AND obs_date >= :d ORDER BY obs_date DESC"
            ), {"ids": trade_ids, "d": one_year_ago}).fetchall() if trade_ids else []

            series_vals: dict[str, list] = {}
            for row in trade_rows:
                sid = row[0]
                if sid not in series_vals:
                    series_vals[sid] = []
                series_vals[sid].append((float(row[1]), row[2]))

        for from_id, to_id in TRADE_PAIRS:
            fp = id_to_prefix.get(from_id, "")
            tp = id_to_prefix.get(to_id, "")

            volume = None
            change_1y = None
            for sid, vals in series_vals.items():
                if fp.lower() in sid.lower() and tp.lower() in sid.lower():
                    if vals:
                        volume = vals[0][0]
                        if len(vals) >= 12:
                            change_1y = _pct_change(vals[0][0], vals[-1][0])
                    break

            direction = "surplus"
            if volume is not None and volume < 0:
                direction = "deficit"
                volume = abs(volume)

            flows.append({
                "from": from_id,
                "to": to_id,
                "type": "trade",
                "volume": volume or 50_000_000_000,
                "direction": direction,
                "change_1y": change_1y,
            })
    except Exception as exc:
        log.debug("Globe trade flows failed: {e}", e=str(exc))
        for from_id, to_id in TRADE_PAIRS:
            flows.append({
                "from": from_id, "to": to_id, "type": "trade",
                "volume": 50_000_000_000, "direction": "surplus",
                "change_1y": None,
            })

    CAPITAL_PAIRS = [
        ("JPN", "USA", 200_000_000_000), ("CHN", "USA", 150_000_000_000),
        ("DEU", "USA", 80_000_000_000), ("GBR", "USA", 60_000_000_000),
        ("USA", "IND", 40_000_000_000), ("USA", "BRA", 25_000_000_000),
        ("KOR", "USA", 30_000_000_000), ("AUS", "USA", 20_000_000_000),
    ]
    for from_id, to_id, est_vol in CAPITAL_PAIRS:
        flows.append({
            "from": from_id, "to": to_id, "type": "capital",
            "volume": est_vol, "direction": "inflow",
            "change_1y": None,
        })

    # ── Cross-reference hotspots ──
    try:
        # The persisted batch (Hermes, every 4 h) instead of re-running every
        # check plus the LLM narrative here -- that was most of the >125 s.
        from intelligence.cross_reference import load_recent_report, run_all_checks
        report = load_recent_report(engine, max_age_seconds=GLOBE_XREF_MAX_AGE_S)
        if report is None:
            report = run_all_checks(engine, skip_narrative=True)
        country_flags = {}
        for check in report.red_flags:
            d = asdict(check)
            name_lower = d.get("name", "").lower()
            for cfg in COUNTRY_CONFIG:
                cname = cfg["name"].lower()
                cid = cfg["id"].lower()
                if cname in name_lower or cid in name_lower:
                    key = cfg["id"]
                    severity = "high" if d.get("assessment") == "contradiction" else "medium"
                    if key not in country_flags or severity == "high":
                        country_flags[key] = {
                            "country": cfg["id"],
                            "reason": d.get("implication", d.get("name", "Divergence detected")),
                            "severity": severity,
                        }
            if "china" in name_lower or "viirs" in name_lower:
                if "CHN" not in country_flags:
                    country_flags["CHN"] = {
                        "country": "CHN",
                        "reason": d.get("implication", "GDP divergence from physical indicators"),
                        "severity": "high" if d.get("assessment") == "contradiction" else "medium",
                    }
        hotspots = list(country_flags.values())
    except Exception as exc:
        log.debug("Globe hotspots failed: {e}", e=str(exc))

    # ── FX map (from batch-fetched data) ──
    for label, ticker in FX_PAIRS.items():
        val = _get_val(f"YF:{ticker}:close", latest_vals)
        if val is None:
            val = _get_val(ticker, latest_vals)
        fx_map[label] = round(val, 4) if val is not None else None

    return {
        "countries": countries,
        "flows": flows,
        "hotspots": hotspots,
        "fx_map": fx_map,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        # Newest observation behind the country / FX values (data date).
        "as_of": latest_obs_date.isoformat() if latest_obs_date is not None else None,
    }


@router.get("/dashboard")
async def get_intelligence_dashboard(
    _token: str = Depends(require_auth),
) -> dict[str, Any]:
    """Unified intelligence dashboard — all systems at a glance.

    Returns a combined snapshot of trust scores, convergence alerts,
    lever-puller activity, cross-reference red flags, source audit status,
    and post-mortem lessons. Cached for 10 minutes.

    Heavy computation runs in a thread pool so the event loop stays free.
    """
    import asyncio

    cache_key = "intel_dashboard"

    cached_data = _dashboard_cache.get(cache_key)
    if cached_data is not None:
        return cached_data

    try:
        snapshot = await asyncio.to_thread(_build_dashboard_snapshot)
        _dashboard_cache.set(cache_key, snapshot)
        return snapshot
    except Exception as exc:
        log.error("Intelligence dashboard build failed: {e}", e=str(exc))
        return {
            "trust": {"top_sources": [], "convergence_events": []},
            "levers": {"active_events": [], "top_pullers": []},
            "cross_ref": {"red_flags": [], "total_checks": 0},
            "source_audit": {"discrepancies": [], "single_points_of_failure": 0},
            "postmortems": {"recent_failures": [], "lessons": ""},
            "overall_confidence": 0.5,
            "narrative": f"Dashboard build error: {exc}",
            "generated_at": None,
            "error": str(exc),
        }
