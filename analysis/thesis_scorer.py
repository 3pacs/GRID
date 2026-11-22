"""
GRID — Granular Thesis Scoring Engine.

Replaces the old weight-accumulation system in flow_thesis.py with
auditable, decomposable scoring.  Every claim traces to a data point,
a threshold, and a reason.

What it scores: SPY direction over the next 5 trading days.
How it scores:  Each model produces a score (-100 to +100) and a
                confidence (0-100%).  The final score is the
                confidence-weighted average.  Every number adds up.

Output contract:
  {
    "score": -23.4,           # weighted average, -100..+100
    "direction": "BEARISH",   # derived from score sign
    "conviction": 23,         # abs(score), 0..100
    "bull_pct": 38.3,         # % of weighted votes bullish
    "bear_pct": 61.7,         # % of weighted votes bearish
    "models": [ ... ],        # per-model breakdown (see ModelVerdict)
    "evaluation_window": "5d",
    "generated_at": "...",
  }

Each model in the breakdown:
  {
    "key": "fed_liquidity",
    "name": "Fed Net Liquidity",
    "score": -40,             # this model's call: -100..+100
    "confidence": 72,         # 0..100, weights this model's vote
    "direction": "bearish",
    "data_point": "-$187B 30d change",
    "threshold": "bearish < -$50B, bullish > +$50B",
    "reasoning": "Net liquidity fell $187B in 30 days ...",
    "weight_in_final": 0.18,  # how much this model contributed
    "historical_accuracy": 0.62,  # win rate from past snapshots
    "data_age_hours": 4.2,    # how fresh the underlying data is
    "status": "active",       # active | stale | broken | no_data
  }
"""

from __future__ import annotations

import datetime as _dt
import json
import time as _time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from analysis.sec_valuation import trailing_pe
from ingestion.altdata.fed_liquidity import RRPONTSYD_TO_MILLIONS, format_liquidity_usd
from ingestion.market_calendar import trading_days_between
from store.options_provenance import VISIBLE_DAILY_SIGNALS, visible_sql
from store.journal_qualification import DECISION_JOURNAL, non_provisional_journal_select


# ── Constants ───────────────────────────────────────────────────────────

EVALUATION_WINDOW_DAYS = 5
STALE_HOURS = 72  # data older than this gets downweighted
DEFAULT_ACCURACY = 0.50  # start at coin-flip until we have history

# Regime changepoints (_score_regime_changepoints). The detector
# (discovery/changepoint_detector.py) stamps its version in
# signal_registry.provenance and writes a break only while it is fresh; these
# two values are its CHANGEPOINT_DETECTOR_VERSION and FRESH_BREAK_DAYS (a test
# pins them together). Rows from the earlier detector (provenance NULL; about
# 1.08M rows, and 59% of all registry rows written 2026-11-08..22) are ignored
# by version, not deleted.
CHANGEPOINT_DETECTOR_VERSION = "changepoint_detector/2"
CHANGEPOINT_FRESH_DAYS = 5
# Each feature with a fresh break moves the score by this times the average
# confidence; the tally is capped so it can never dominate the thesis.
CHANGEPOINT_POINTS_PER_FEATURE = 10.0
CHANGEPOINT_SCORE_CAP = 30.0
CHANGEPOINT_CONFIDENCE_CAP = 60.0

# Fed Net Liquidity 30-day change bands (millions USD; WALCL/WTREGEN are millions,
# RRPONTSYD is converted from billions via RRPONTSYD_TO_MILLIONS).
FED_LIQ_MODERATE_MILLIONS = 50_000.0   # $50B
FED_LIQ_STRONG_MILLIONS = 100_000.0    # $100B
FED_LIQ_BAND_STEP_MILLIONS = 50_000.0  # $50B linear ramp width

# Options-derived models (dealer_gamma, vanna_charm) freshness gate and proxy labels.
# Model keys stay "dealer_gamma" and "vanna_charm" for downstream accuracy table stability;
# names and measurement tags distinguish these proxies from directly observed dealer Greeks.
OPTIONS_MAX_AGE_TRADING_DAYS = 2
_DEALER_GAMMA_NAME = "Options Sentiment (PCR Proxy)"
_DEALER_GAMMA_MEASUREMENT = "proxy:put_call_ratio"
_VANNA_CHARM_NAME = "Vanna/Charm (Max-Pain Proxy)"
_VANNA_CHARM_MEASUREMENT = "proxy:max_pain_distance"
_UNSET = object()

# News sentiment scoring thresholds
NEWS_MIN_DIRECTIONAL_ARTICLES = 5
NEWS_DEFAULT_ARTICLE_CONF = 0.5
NEWS_SCORE_SCALE = 100.0

# Social sentiment contrarian scoring thresholds
SOCIAL_SENTIMENT_MAX_SCORE = 80.0
SOCIAL_SENTIMENT_EXTREME_RATIO = 2.0

# CFTC COT positioning thresholds (net speculative contracts) and asset-class weights
CFTC_EXTREME_NET_CONTRACTS = 100_000.0
CFTC_MODERATE_NET_CONTRACTS = 50_000.0
CFTC_VIX_EXTREME_CONTRACTS = 50_000.0
CFTC_VIX_MODERATE_CONTRACTS = 20_000.0
CFTC_EQUITY_WEIGHT = 2.0
CFTC_SAFE_HAVEN_WEIGHT = 1.0
CFTC_VIX_WEIGHT = 1.0

# Valuation compression thresholds
VALUATION_MAX_PE = 500.0
VALUATION_MIN_TICKERS = 3
VALUATION_PE_CHANGE_MODERATE_PCT = 5.0
VALUATION_PE_CHANGE_STRONG_PCT = 10.0
VALUATION_MIN_EPS_GROWTH_PCT = 0.0  # EPS growth must be > 0% for earnings-driven compression

# Snapshot summary limits
TOP_RISK_FACTORS_LIMIT = 3


def _trading_days_age(sig_date: date, as_of: date) -> int:
    """Count US equity trading days elapsed strictly after ``sig_date`` through ``as_of``.

    Raises ``ValueError`` when ``sig_date > as_of`` so a future-dated row can
    never be misread as age 0.
    """
    if sig_date > as_of:
        raise ValueError(
            f"signal_date {sig_date.isoformat()} is after decision date {as_of.isoformat()}"
        )
    if sig_date == as_of:
        return 0
    return len(trading_days_between(sig_date + timedelta(days=1), as_of))


def _coerce_utc_timestamp(value: Any) -> datetime | None:
    """Normalize a timestamp value to timezone-aware UTC, or None if invalid."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = _dt.datetime.fromisoformat(value)
        except ValueError:
            return None
    if not isinstance(value, _dt.datetime):
        return None
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _coerce_signal_date(value: Any) -> date | None:
    """Normalize a DB date value to ``date``, or None if missing/invalid."""
    if value is None:
        return None
    if isinstance(value, _dt.datetime):
        return value.date()
    if isinstance(value, _dt.date):
        return value
    if isinstance(value, str):
        try:
            return _dt.date.fromisoformat(value)
        except ValueError:
            try:
                return _dt.datetime.fromisoformat(value).date()
            except ValueError:
                return None
    return None


def _validate_options_clock(
    *,
    model_key: str,
    model_name: str,
    measurement: str,
    metric_summary: str,
    sig_date_raw: Any,
    created_at_raw: Any,
    as_of_ts: datetime,
    accuracy: float,
) -> tuple[float | None, dict[str, Any] | None]:
    """Defend options-derived scores against future dates, late captures, and staleness.

    Returns ``(age_hours, None)`` when eligible and fresh, or ``(None, verdict)``
    with a fail-closed ``no_data`` / ``stale`` verdict otherwise.
    """
    as_of_date = as_of_ts.date()
    sig_date = _coerce_signal_date(sig_date_raw)
    if sig_date is None:
        log.warning(
            "{m}: missing signal_date — marking stale and excluding from score",
            m=model_name,
        )
        return None, _verdict(
            model_key, model_name,
            0, 0, f"stale {metric_summary} (unknown date)",
            f"max age {OPTIONS_MAX_AGE_TRADING_DAYS} trading days",
            "SPY options signal_date is missing; excluded from score.",
            data_age_hours=None, historical_accuracy=accuracy, status="stale",
            measurement=measurement,
        )

    date_age_hours = (as_of_date - sig_date).days * 24.0
    if sig_date > as_of_date or date_age_hours < 0:
        log.warning(
            "{m}: lookahead guard rejected signal_date={sd} > as_of={as_of} "
            "(age_hours={ah:.1f}) — failing closed",
            m=model_name, sd=sig_date, as_of=as_of_date, ah=date_age_hours,
        )
        return None, _verdict(
            model_key, model_name,
            0, 0, "no eligible data",
            f"signal_date <= {as_of_date.isoformat()}",
            f"SPY options signal_date {sig_date} is after decision date {as_of_date}; "
            "excluded from score (lookahead guard).",
            data_age_hours=None, historical_accuracy=accuracy, status="no_data",
            measurement=measurement,
        )

    age_hours = date_age_hours
    if created_at_raw is not _UNSET:
        created_ts = _coerce_utc_timestamp(created_at_raw)
        if created_ts is None:
            log.warning(
                "{m}: missing/invalid created_at for signal_date={sd} — failing closed",
                m=model_name, sd=sig_date,
            )
            return None, _verdict(
                model_key, model_name,
                0, 0, "no eligible data",
                f"created_at <= {as_of_ts.isoformat()}",
                "SPY options created_at timestamp is missing or unverifiable; excluded from score.",
                data_age_hours=None, historical_accuracy=accuracy, status="no_data",
                measurement=measurement,
            )
        capture_age_hours = (as_of_ts - created_ts).total_seconds() / 3600.0
        if capture_age_hours < 0:
            log.warning(
                "{m}: lookahead guard rejected created_at={ca} > as_of={as_of} "
                "(age_hours={ah:.2f}) — failing closed",
                m=model_name, ca=created_ts.isoformat(), as_of=as_of_ts.isoformat(),
                ah=capture_age_hours,
            )
            return None, _verdict(
                model_key, model_name,
                0, 0, "no eligible data",
                f"created_at <= {as_of_ts.isoformat()}",
                f"SPY options row captured at {created_ts.isoformat()} after decision clock "
                f"{as_of_ts.isoformat()}; excluded from score (lookahead guard).",
                data_age_hours=None, historical_accuracy=accuracy, status="no_data",
                measurement=measurement,
            )
        age_hours = max(date_age_hours, capture_age_hours)

    trading_age = _trading_days_age(sig_date, as_of_date)
    if trading_age > OPTIONS_MAX_AGE_TRADING_DAYS:
        log.warning(
            "{m} stale: signal_date={sd}, trading_days_old={td} > {lim}",
            m=model_name, sd=sig_date, td=trading_age, lim=OPTIONS_MAX_AGE_TRADING_DAYS,
        )
        return None, _verdict(
            model_key, model_name,
            0, 0, f"stale {metric_summary} (as of {sig_date})",
            f"max age {OPTIONS_MAX_AGE_TRADING_DAYS} trading days",
            f"SPY options data is {trading_age} trading days old (last signal {sig_date}, "
            f"max {OPTIONS_MAX_AGE_TRADING_DAYS}); excluded from score.",
            data_age_hours=age_hours, historical_accuracy=accuracy, status="stale",
            measurement=measurement,
        )

    return age_hours, None


# ══════════════════════════════════════════════════════════════════════════
# MODEL VERDICT (immutable output per model)
# ══════════════════════════════════════════════════════════════════════════

def _verdict(
    key: str,
    name: str,
    score: float,
    confidence: float,
    data_point: str,
    threshold: str,
    reasoning: str,
    data_age_hours: float | None = None,
    historical_accuracy: float = DEFAULT_ACCURACY,
    status: str = "active",
    measurement: str | None = None,
) -> dict[str, Any]:
    """Build an immutable model verdict dict.

    score: -100 to +100.  confidence: 0 to 100.
    """
    clamped_score = max(-100, min(100, score))
    clamped_conf = max(0, min(100, confidence))

    if clamped_score > 5:
        direction = "bullish"
    elif clamped_score < -5:
        direction = "bearish"
    else:
        direction = "neutral"

    verdict: dict[str, Any] = {
        "key": key,
        "name": name,
        "score": round(clamped_score, 1),
        "confidence": round(clamped_conf, 1),
        "direction": direction,
        "data_point": data_point,
        "threshold": threshold,
        "reasoning": reasoning,
        "data_age_hours": round(data_age_hours, 1) if data_age_hours is not None else None,
        "historical_accuracy": round(historical_accuracy, 3),
        "status": status,
    }
    if measurement is not None:
        verdict["measurement"] = measurement
    return verdict


# ══════════════════════════════════════════════════════════════════════════
# INDIVIDUAL MODEL SCORERS
# ══════════════════════════════════════════════════════════════════════════

def _score_fed_liquidity(engine: Engine, accuracy: float) -> dict:
    """Fed net liquidity: balance sheet minus TGA minus reverse repo.

    Logic: 30-day change in net liquidity (all inputs converted to millions USD:
    WALCL and WTREGEN are FRED millions; RRPONTSYD is FRED billions scaled by
    RRPONTSYD_TO_MILLIONS). Thresholds are ±$50B (±50,000M) moderate and
    ±$100B (±100,000M) strong. Fails closed if any current or 30-day baseline
    component is missing.

    Score scales linearly within bands.  Confidence from data freshness
    and historical accuracy.
    """
    try:
        with engine.connect() as conn:
            bs = conn.execute(text(
                "SELECT value, pull_timestamp FROM raw_series "
                "WHERE series_id = 'WALCL' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            rr = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'RRPONTSYD' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            tga = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'WTREGEN' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            missing_cur = [
                name for name, row in (("WALCL", bs), ("RRPONTSYD", rr), ("WTREGEN", tga))
                if not row or row[0] is None
            ]
            if missing_cur:
                log.warning(
                    "Fed liquidity: missing current component(s) {m} — failing closed",
                    m=", ".join(missing_cur),
                )
                return _verdict("fed_liquidity", "Fed Net Liquidity",
                    0, 0, "no data", "",
                    f"Missing current Fed liquidity component(s): {', '.join(missing_cur)}.",
                    status="no_data", historical_accuracy=accuracy)

            # RRPONTSYD is billions; WALCL/WTREGEN are millions
            net_liq = float(bs[0]) - float(rr[0]) * RRPONTSYD_TO_MILLIONS - float(tga[0])
            pull_ts = bs[1] if bs[1] else datetime.now(timezone.utc)
            if hasattr(pull_ts, "replace"):
                ts_utc = pull_ts.astimezone(timezone.utc) if pull_ts.tzinfo else pull_ts.replace(tzinfo=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - ts_utc).total_seconds() / 3600
            else:
                age_hours = None

            # 30-day prior
            bs_30 = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'WALCL' AND pull_status = 'SUCCESS' "
                "AND obs_date <= CURRENT_DATE - 30 "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            rr_30 = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'RRPONTSYD' AND pull_status = 'SUCCESS' "
                "AND obs_date <= CURRENT_DATE - 30 "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            tga_30 = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'WTREGEN' AND pull_status = 'SUCCESS' "
                "AND obs_date <= CURRENT_DATE - 30 "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            missing_30 = [
                name for name, row in (("WALCL", bs_30), ("RRPONTSYD", rr_30), ("WTREGEN", tga_30))
                if not row or row[0] is None
            ]
            if missing_30:
                log.warning(
                    "Fed liquidity: missing 30d baseline component(s) {m} — failing closed",
                    m=", ".join(missing_30),
                )
                return _verdict("fed_liquidity", "Fed Net Liquidity",
                    0, 0, f"{format_liquidity_usd(net_liq, unit='millions_usd')} current",
                    "need 30d history",
                    f"Current net liquidity known but missing 30-day baseline ({', '.join(missing_30)}).",
                    data_age_hours=age_hours, historical_accuracy=accuracy, status="stale")

            # RRPONTSYD is billions; WALCL/WTREGEN are millions
            net_liq_30 = float(bs_30[0]) - float(rr_30[0]) * RRPONTSYD_TO_MILLIONS - float(tga_30[0])
            change = net_liq - net_liq_30

            # Score: odd-symmetric linear scale within ±$50B / ±$100B bands (millions USD)
            if change >= FED_LIQ_STRONG_MILLIONS:
                score = 60 + min(
                    20, (change - FED_LIQ_STRONG_MILLIONS) / FED_LIQ_BAND_STEP_MILLIONS * 20
                )
            elif change >= FED_LIQ_MODERATE_MILLIONS:
                score = 30 + (change - FED_LIQ_MODERATE_MILLIONS) / FED_LIQ_BAND_STEP_MILLIONS * 30
            elif change > -FED_LIQ_MODERATE_MILLIONS:
                score = change / FED_LIQ_MODERATE_MILLIONS * 25
            elif change > -FED_LIQ_STRONG_MILLIONS:
                score = -30 - (abs(change) - FED_LIQ_MODERATE_MILLIONS) / FED_LIQ_BAND_STEP_MILLIONS * 30
            else:
                score = -60 - min(
                    20, (abs(change) - FED_LIQ_STRONG_MILLIONS) / FED_LIQ_BAND_STEP_MILLIONS * 20
                )

            # Confidence: base 60%, +20% if data fresh, scale by accuracy
            conf_base = 60
            conf_fresh = 20 if (age_hours is not None and age_hours < 24) else 0
            conf_accuracy = accuracy * 20  # 0-20% from track record
            confidence = min(95, conf_base + conf_fresh + conf_accuracy)

            return _verdict(
                "fed_liquidity", "Fed Net Liquidity",
                score, confidence,
                data_point=(f"{format_liquidity_usd(change, unit='millions_usd', signed=True)} "
                            f"30d change (current {format_liquidity_usd(net_liq, unit='millions_usd')})"),
                threshold="bearish < -$50B (-$50,000M), bullish > +$50B (+$50,000M), strong at ±$100B (±$100,000M)",
                reasoning=(
                    f"Net liquidity {'rose' if change > 0 else 'fell' if change < 0 else 'was unchanged by'} "
                    f"{format_liquidity_usd(abs(change), unit='millions_usd')} over 30 days. "
                    f"{'Expanding liquidity supports risk assets.' if change >= FED_LIQ_MODERATE_MILLIONS else 'Contracting liquidity pressures risk assets.' if change <= -FED_LIQ_MODERATE_MILLIONS else 'Liquidity roughly flat — no strong directional signal.'}"
                ),
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Fed liquidity scorer error: {e}", e=str(exc))
        return _verdict("fed_liquidity", "Fed Net Liquidity",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_dealer_gamma(engine: Engine, accuracy: float) -> dict:
    """Put/call ratio proxy for dealer gamma (not directly observed dealer gamma).

    Model key remains ``dealer_gamma`` for downstream accuracy table stability.

    Logic: PCR is the ratio of put volume to call volume.
    - PCR < 0.6  → extreme complacency, contrarian bearish (-30)
    - PCR 0.6-0.8 → healthy bullish sentiment (+20 to +40)
    - PCR 0.8-1.0 → neutral
    - PCR 1.0-1.3 → fear building, contrarian bullish (+20 to +40)
    - PCR > 1.3  → extreme fear, strong contrarian bullish (+50 to +70)

    Note: PCR is CONTRARIAN.  High put buying = bullish setup.
    """
    try:
        as_of_ts = datetime.now(timezone.utc)
        if as_of_ts.tzinfo is None:
            as_of_ts = as_of_ts.replace(tzinfo=timezone.utc)
        as_of_date = as_of_ts.date()

        with engine.connect() as conn:
            row = conn.execute(
                visible_sql(
                    "SELECT put_call_ratio, spot_price, signal_date, created_at "
                    "FROM " + VISIBLE_DAILY_SIGNALS + " ods "
                    "WHERE ticker = 'SPY' AND put_call_ratio IS NOT NULL "
                    "AND signal_date <= :as_of_date "
                    "AND created_at <= :as_of_ts "
                    "ORDER BY signal_date DESC, created_at DESC LIMIT 1"
                ),
                {"as_of_date": as_of_date, "as_of_ts": as_of_ts},
            ).fetchone()

            if not row or not row[0]:
                return _verdict(
                    "dealer_gamma", _DEALER_GAMMA_NAME,
                    0, 0, "no data", "", "No SPY options data available.",
                    status="no_data", historical_accuracy=accuracy,
                    measurement=_DEALER_GAMMA_MEASUREMENT,
                )

            pcr = float(row[0])
            spot = float(row[1]) if row[1] else None
            sig_date_raw = row[2]
            created_at_raw = row[3] if len(row) > 3 else _UNSET
            age_hours, rejected = _validate_options_clock(
                model_key="dealer_gamma",
                model_name=_DEALER_GAMMA_NAME,
                measurement=_DEALER_GAMMA_MEASUREMENT,
                metric_summary=f"SPY PCR = {pcr:.2f}",
                sig_date_raw=sig_date_raw,
                created_at_raw=created_at_raw,
                as_of_ts=as_of_ts,
                accuracy=accuracy,
            )
            if rejected is not None:
                return rejected

            # Contrarian scoring
            if pcr < 0.6:
                score = -30  # extreme complacency → bearish
                label = "extreme complacency — contrarian bearish"
            elif pcr < 0.8:
                score = 20 + (0.8 - pcr) / 0.2 * 20
                label = "healthy bullish sentiment"
            elif pcr < 1.0:
                score = (pcr - 0.8) / 0.2 * 10 - 5  # slight range around 0
                label = "neutral sentiment"
            elif pcr < 1.3:
                score = 20 + (pcr - 1.0) / 0.3 * 20
                label = "elevated fear — contrarian bullish"
            else:
                score = 50 + min(20, (pcr - 1.3) / 0.3 * 20)
                label = "extreme fear — strong contrarian bullish"

            confidence = 55 + (accuracy * 20)
            if age_hours and age_hours > 48:
                confidence *= 0.7

            return _verdict(
                "dealer_gamma", _DEALER_GAMMA_NAME,
                score, confidence,
                data_point=f"SPY PCR = {pcr:.2f}" + (f", spot ${spot:.0f}" if spot else ""),
                threshold="<0.6 contrarian bear, 0.6-0.8 bull, 1.0-1.3 contrarian bull, >1.3 strong contrarian bull",
                reasoning=(
                    f"Put/call ratio proxy (not directly observed dealer gamma): "
                    f"SPY put/call ratio is {pcr:.2f} ({label}). "
                    f"{'High put buying historically precedes reversals upward.' if pcr > 1.0 else 'Low put buying signals complacency risk.' if pcr < 0.7 else 'Sentiment in neutral range.'}"
                ),
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
                measurement=_DEALER_GAMMA_MEASUREMENT,
            )
    except Exception as exc:
        log.debug("Dealer gamma scorer error: {e}", e=str(exc))
        return _verdict(
            "dealer_gamma", _DEALER_GAMMA_NAME,
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
            measurement=_DEALER_GAMMA_MEASUREMENT,
        )


def _score_vanna_charm(engine: Engine, accuracy: float) -> dict:
    """Max-pain distance proxy for vanna/charm (not directly observed dealer vanna/charm).

    Model key remains ``vanna_charm`` for downstream accuracy table stability.

    Logic: Uses spot distance from max pain as a pin-pressure proxy.
    - Spot > max pain by 2%+ → pull DOWN (bearish -30 to -50)
    - Spot > max pain by 1-2% → mild bearish (-10 to -30)
    - Spot within 1% of max pain → pinned/neutral
    - Spot < max pain by 1-2% → mild bullish (+10 to +30)
    - Spot < max pain by 2%+ → pull UP (bullish +30 to +50)

    Confidence increases closer to OpEx (Friday).
    """
    try:
        as_of_ts = datetime.now(timezone.utc)
        if as_of_ts.tzinfo is None:
            as_of_ts = as_of_ts.replace(tzinfo=timezone.utc)
        as_of_date = as_of_ts.date()

        with engine.connect() as conn:
            row = conn.execute(
                visible_sql(
                    "SELECT max_pain, spot_price, signal_date, created_at "
                    "FROM " + VISIBLE_DAILY_SIGNALS + " ods "
                    "WHERE ticker = 'SPY' AND max_pain IS NOT NULL AND spot_price > 0 "
                    "AND signal_date <= :as_of_date "
                    "AND created_at <= :as_of_ts "
                    "ORDER BY signal_date DESC, created_at DESC LIMIT 1"
                ),
                {"as_of_date": as_of_date, "as_of_ts": as_of_ts},
            ).fetchone()

            if not row:
                return _verdict(
                    "vanna_charm", _VANNA_CHARM_NAME,
                    0, 0, "no data", "", "No max pain data available.",
                    status="no_data", historical_accuracy=accuracy,
                    measurement=_VANNA_CHARM_MEASUREMENT,
                )

            mp, spot, sig_date_raw = float(row[0]), float(row[1]), row[2]
            created_at_raw = row[3] if len(row) > 3 else _UNSET
            age_hours, rejected = _validate_options_clock(
                model_key="vanna_charm",
                model_name=_VANNA_CHARM_NAME,
                measurement=_VANNA_CHARM_MEASUREMENT,
                metric_summary=f"max pain ${mp:.0f}",
                sig_date_raw=sig_date_raw,
                created_at_raw=created_at_raw,
                as_of_ts=as_of_ts,
                accuracy=accuracy,
            )
            if rejected is not None:
                return rejected

            gap_pct = (spot - mp) / spot * 100

            # Score: linear in gap
            if gap_pct > 2:
                score = -30 - min(20, (gap_pct - 2) * 10)
            elif gap_pct > 1:
                score = -10 - (gap_pct - 1) * 20
            elif gap_pct > -1:
                score = gap_pct * 8  # gentle pull
            elif gap_pct > -2:
                score = 10 + (abs(gap_pct) - 1) * 20
            else:
                score = 30 + min(20, (abs(gap_pct) - 2) * 10)

            # Confidence: higher closer to Friday (OpEx)
            days_to_friday = (4 - as_of_date.weekday()) % 7
            opex_boost = max(0, 15 - days_to_friday * 3)
            confidence = 40 + opex_boost + (accuracy * 15)
            if age_hours and age_hours > 48:
                confidence *= 0.6

            return _verdict(
                "vanna_charm", _VANNA_CHARM_NAME,
                score, confidence,
                data_point=f"Spot ${spot:.0f} vs max pain ${mp:.0f} (gap {gap_pct:+.1f}%)",
                threshold="bearish if spot >1% above max pain, bullish if >1% below",
                reasoning=(
                    f"Max-pain distance proxy (not directly observed vanna/charm): "
                    f"SPY spot is {abs(gap_pct):.1f}% {'above' if gap_pct > 0 else 'below'} "
                    f"max pain (${mp:.0f}). "
                    f"{'Distance above max pain implies downward pin pressure toward max pain.' if gap_pct > 1 else 'Distance below max pain implies upward pin pressure toward max pain.' if gap_pct < -1 else 'Spot near max pain — limited implied pin pressure.'}"
                    + (f" OpEx in {days_to_friday} day{'s' if days_to_friday != 1 else ''} — gravitational pull strengthening." if days_to_friday <= 3 else "")
                ),
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
                measurement=_VANNA_CHARM_MEASUREMENT,
            )
    except Exception as exc:
        log.debug("Vanna/charm scorer error: {e}", e=str(exc))
        return _verdict(
            "vanna_charm", _VANNA_CHARM_NAME,
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
            measurement=_VANNA_CHARM_MEASUREMENT,
        )


def _score_congressional(engine: Engine, accuracy: float) -> dict:
    """Congressional trading signal.

    Logic: Net buy/sell ratio of congressional trades in last 45 days.
    Score scales with the imbalance.  Confidence is lower because of
    disclosure lag (trades reported 30-45 days after execution).
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT signal_type, COUNT(*) as cnt "
                "FROM signal_sources "
                "WHERE source_type = 'congressional' "
                "AND signal_date >= CURRENT_DATE - 45 "
                "GROUP BY signal_type"
            )).fetchall()

            if not rows:
                return _verdict("congressional", "Congressional Trading",
                    0, 0, "no data", "", "No congressional trades in last 45 days.",
                    status="no_data", historical_accuracy=accuracy)

            buys = sum(r[1] for r in rows if r[0] and r[0].upper() in ("BUY", "PURCHASE"))
            sells = sum(r[1] for r in rows if r[0] and r[0].upper() in ("SELL", "SALE", "SALE_FULL", "SALE_PARTIAL"))
            total = buys + sells

            if total == 0:
                return _verdict("congressional", "Congressional Trading",
                    0, 10, "0 trades", "", "No buy/sell trades found.",
                    status="no_data", historical_accuracy=accuracy)

            buy_ratio = buys / total
            # Score: centered at 50/50, scales with imbalance
            # 70% buys → +40, 80% buys → +60, 90%+ → +80
            # 30% buys → -40, 20% buys → -60, 10%- → -80
            imbalance = (buy_ratio - 0.5) * 2  # -1 to +1
            score = imbalance * 80

            # Confidence: low base (lagging data), boost with volume
            conf_base = 30  # inherently lagging
            conf_volume = min(20, total / 5)  # more trades = more signal
            conf_accuracy = accuracy * 15
            confidence = conf_base + conf_volume + conf_accuracy

            return _verdict(
                "congressional", "Congressional Trading",
                score, confidence,
                data_point=f"{buys} buys vs {sells} sells (45d, {total} total)",
                threshold="bearish if sells >60%, bullish if buys >60%",
                reasoning=(
                    f"Congress members made {buys} buys and {sells} sells in 45 days "
                    f"({buy_ratio*100:.0f}% buy rate). "
                    f"{'Net buying suggests insiders see upside.' if buy_ratio > 0.6 else 'Net selling suggests insiders see risk.' if buy_ratio < 0.4 else 'Roughly balanced — no strong insider signal.'} "
                    f"Note: 30-45 day disclosure lag means these trades already happened."
                ),
                data_age_hours=45 * 24,  # inherent lag
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Congressional scorer error: {e}", e=str(exc))
        return _verdict("congressional", "Congressional Trading",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_insider_cluster(engine: Engine, accuracy: float) -> dict:
    """Insider cluster buy/sell detection.

    Logic: 3+ insiders buying the same stock in 14 days = cluster.
    Net cluster direction across all active clusters determines signal.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT ticker, signal_type, COUNT(*) as cnt "
                "FROM signal_sources "
                "WHERE source_type = 'insider' "
                "AND signal_date >= CURRENT_DATE - 14 "
                "GROUP BY ticker, signal_type "
                "HAVING COUNT(*) >= 3 "
                "ORDER BY COUNT(*) DESC"
            )).fetchall()

            if not rows:
                return _verdict("insider_cluster", "Insider Clusters",
                    0, 15, "no active clusters", "",
                    "No insider cluster events (3+ insiders, same stock, 14 days) detected.",
                    status="active", historical_accuracy=accuracy)

            buy_clusters = [r for r in rows if r[1] and r[1].upper() in ("BUY", "PURCHASE")]
            sell_clusters = [r for r in rows if r[1] and r[1].upper() in ("SELL", "SALE", "SALE_FULL")]
            buy_count = len(buy_clusters)
            sell_count = len(sell_clusters)
            total = buy_count + sell_count

            if total == 0:
                return _verdict("insider_cluster", "Insider Clusters",
                    0, 10, "no buy/sell clusters", "",
                    "Clusters detected but no clear buy/sell direction.",
                    status="active", historical_accuracy=accuracy)

            # Score: each net cluster ~ 15 points
            net = buy_count - sell_count
            score = net * 15
            score = max(-80, min(80, score))

            # Build detail string
            buy_tickers = [f"{r[0]}({r[2]})" for r in buy_clusters[:3]]
            sell_tickers = [f"{r[0]}({r[2]})" for r in sell_clusters[:3]]

            confidence = 35 + min(25, total * 5) + (accuracy * 15)

            return _verdict(
                "insider_cluster", "Insider Clusters",
                score, confidence,
                data_point=f"{buy_count} buy clusters, {sell_count} sell clusters",
                threshold="each cluster ≈ ±15 points; 3+ insiders in 14 days = cluster",
                reasoning=(
                    f"{buy_count} buy cluster{'s' if buy_count != 1 else ''}"
                    + (f" ({', '.join(buy_tickers)})" if buy_tickers else "")
                    + f" vs {sell_count} sell cluster{'s' if sell_count != 1 else ''}"
                    + (f" ({', '.join(sell_tickers)})" if sell_tickers else "")
                    + ". "
                    + ("Net insider buying — corporate insiders see value." if net > 0
                       else "Net insider selling — insiders reducing exposure." if net < 0
                       else "Balanced insider activity — no clear signal.")
                ),
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Insider cluster scorer error: {e}", e=str(exc))
        return _verdict("insider_cluster", "Insider Clusters",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_supply_chain(engine: Engine, accuracy: float) -> dict:
    """Supply chain leading indicators (FRED manufacturing/trade data).

    Logic: composite of durable goods orders, industrial production,
    trade balance changes over 3 months. These are SLOW signals
    (90-120 day lead) so confidence is moderate and score is capped.
    """
    # Key FRED series for supply chain health
    _SC_SERIES = [
        "supply_chain.durable_goods_orders",
        "supply_chain.mfg_new_orders",
        "supply_chain.industrial_production",
        "supply_chain.mfg_shipments",
        "supply_chain.capex_orders",
        "supply_chain.trade_balance",
    ]
    try:
        with engine.connect() as conn:
            # Get latest values for each supply chain series from raw_series
            changes: list[float] = []
            latest_date = None
            details: list[str] = []

            for sid in _SC_SERIES:
                recent = conn.execute(text(
                    "SELECT value, obs_date FROM raw_series "
                    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"sid": sid}).fetchone()

                if not recent:
                    continue

                prior = conn.execute(text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                    "AND obs_date <= CURRENT_DATE - 90 "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"sid": sid}).fetchone()

                if not prior or float(prior[0]) == 0:
                    continue

                pct = (float(recent[0]) - float(prior[0])) / abs(float(prior[0])) * 100
                changes.append(pct)
                short_name = sid.split(".")[-1]
                details.append(f"{short_name} {pct:+.1f}%")

                if latest_date is None or recent[1] > latest_date:
                    latest_date = recent[1]

            if not changes:
                return _verdict("supply_chain", "Supply Chain",
                    0, 0, "no data", "", "No supply chain data available.",
                    status="no_data", historical_accuracy=accuracy)

            avg_change = sum(changes) / len(changes)
            age_days = (date.today() - latest_date).days if latest_date else None

            # Score: capped at ±50 (slow signal)
            score = max(-50, min(50, avg_change * 3))

            confidence = 35 + (accuracy * 15) + min(20, len(changes) * 3)
            if age_days and age_days > 30:
                confidence *= 0.7

            direction = "expanding" if avg_change > 2 else "contracting" if avg_change < -2 else "flat"

            return _verdict(
                "supply_chain", "Supply Chain",
                score, confidence,
                data_point=f"{len(changes)} indicators, avg 3m change {avg_change:+.1f}%",
                threshold="bearish if avg <-2%, bullish if >+2%",
                reasoning=(
                    f"Supply chain composite ({len(changes)} FRED series) is {direction} "
                    f"with avg 3-month change of {avg_change:+.1f}%. "
                    f"Components: {', '.join(details)}. "
                    f"Manufacturing/trade data leads GDP by 3-6 months."
                ),
                data_age_hours=(age_days or 0) * 24,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Supply chain scorer error: {e}", e=str(exc))
        return _verdict("supply_chain", "Supply Chain",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_capital_flows(engine: Engine, accuracy: float) -> dict:
    """Net capital flows across all sectors (from dollar_flows).

    Logic: Are institutions putting money IN or TAKING money OUT?
    Uses the flow_aggregator to get net flows by actor tier.
    """
    try:
        from analysis.flow_aggregator import get_full_aggregation
        agg = get_full_aggregation(engine, days=14)
        tiers = agg.get("by_actor_tier", {})

        inst = tiers.get("institutional", {})
        inst_net = inst.get("net_flow", 0)
        inst_dir = inst.get("direction", "neutral")

        indiv = tiers.get("individual", {})
        indiv_net = indiv.get("net_flow", 0)

        total_net = inst_net + indiv_net

        if total_net == 0:
            return _verdict("capital_flows", "Capital Flows (14d)",
                0, 10, "no flow data", "", "No dollar flow data in last 14 days.",
                status="no_data", historical_accuracy=accuracy)

        # Score: institutional flows matter more (3x weight)
        # Normalize by dividing by $1B increments
        inst_signal = inst_net / 1e9 * 3  # $1B inst inflow ≈ +3 points
        indiv_signal = indiv_net / 1e9 * 0.5  # $1B individual ≈ +0.5 points
        score = max(-70, min(70, inst_signal + indiv_signal))

        # Sector breakdown for detail
        sectors = agg.get("by_sector", {})
        top_inflows = sorted(
            [(k, v.get("net_flow", 0)) for k, v in sectors.items() if v.get("net_flow", 0) > 0],
            key=lambda x: -x[1]
        )[:3]
        top_outflows = sorted(
            [(k, v.get("net_flow", 0)) for k, v in sectors.items() if v.get("net_flow", 0) < 0],
            key=lambda x: x[1]
        )[:3]

        inflow_str = ", ".join(f"{s} +${f/1e6:,.0f}M" for s, f in top_inflows) if top_inflows else "none"
        outflow_str = ", ".join(f"{s} -${abs(f)/1e6:,.0f}M" for s, f in top_outflows) if top_outflows else "none"

        confidence = 45 + (accuracy * 15)

        return _verdict(
            "capital_flows", "Capital Flows (14d)",
            score, confidence,
            data_point=f"Institutional: ${inst_net/1e9:+,.1f}B, Individual: ${indiv_net/1e6:+,.0f}M",
            threshold="$1B institutional inflow ≈ +3 points; outflow ≈ -3 points",
            reasoning=(
                f"14-day institutional net flow: ${inst_net/1e9:+,.1f}B ({inst_dir}). "
                f"Individual net: ${indiv_net/1e6:+,.0f}M. "
                f"Top inflows: {inflow_str}. Top outflows: {outflow_str}."
            ),
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("Capital flows scorer error: {e}", e=str(exc))
        return _verdict("capital_flows", "Capital Flows (14d)",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_timesfm_consensus(engine: Engine, accuracy: float) -> dict:
    """TimesFM forward-looking signal consensus.

    Logic: TimesFM forecasts all resolved signals 30 steps ahead.
    If >60% of signals forecast UP, that's bullish.
    If >60% forecast DOWN, that's bearish.
    The expected move magnitude and confidence band width determine conviction.

    This is the ONLY forward-looking model in the scorer — all others are
    backward-looking evidence.  TimesFM converts historical patterns into
    probabilistic directional calls.
    """
    try:
        from inference.timesfm_service import get_forecast_summary
        summary = get_forecast_summary(engine)

        total = summary.get("total_forecasted", 0)
        if total == 0:
            return _verdict("timesfm_consensus", "TimesFM Signal Consensus",
                0, 0, "no forecasts", "", "No TimesFM forecasts available. Run forecast cycle first.",
                status="no_data", historical_accuracy=accuracy)

        consensus = summary.get("consensus", "MIXED")
        up_pct = summary.get("up_pct", 50)
        down_pct = summary.get("down_pct", 50)
        families = summary.get("families", {})

        # Score from direction imbalance
        # 60% UP → +20, 70% → +40, 80% → +60, 90%+ → +80
        imbalance = (up_pct - down_pct) / 100  # -1 to +1
        score = imbalance * 80

        # Boost/penalize based on key families
        equity_fam = families.get("equity", {})
        families.get("macro", {})
        vol_fam = families.get("vol", {})

        # If equity signals strongly directional, boost
        equity_net = equity_fam.get("net_move", 0)
        if abs(equity_net) > 2:
            score += equity_net * 3  # each 1% equity move → 3 points

        # If vol signals UP (rising VIX), dampen bullish / boost bearish
        vol_up = vol_fam.get("UP", 0)
        vol_down = vol_fam.get("DOWN", 0)
        if vol_up > vol_down:
            score -= 10  # rising vol → headwind

        score = max(-90, min(90, score))

        # Confidence: higher with more signals and narrower bands
        dir_counts = summary.get("direction_counts", {})
        avg_band = max(
            dir_counts.get("UP", {}).get("avg_band", 50),
            dir_counts.get("DOWN", {}).get("avg_band", 50),
        )
        # Narrow bands (<10%) → high confidence, wide bands (>50%) → low
        band_conf = max(10, 70 - avg_band)
        volume_conf = min(20, total / 5)
        confidence = band_conf + volume_conf + (accuracy * 15)

        return _verdict(
            "timesfm_consensus", "TimesFM Signal Consensus",
            score, confidence,
            data_point=f"{total} signals: {up_pct:.0f}% UP, {down_pct:.0f}% DOWN",
            threshold=">60% UP = bullish, >60% DOWN = bearish, else mixed",
            reasoning=(
                f"TimesFM forecasts {total} signals 30 days ahead. "
                f"{up_pct:.0f}% forecast UP, {down_pct:.0f}% DOWN. "
                f"Consensus: {consensus}. "
                f"Equity signals avg {equity_net:+.1f}% expected move. "
                + ("Rising volatility signals detected — adds uncertainty. " if vol_up > vol_down else "")
                + "This is the only FORWARD-LOOKING model in the scorer."
            ),
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("TimesFM consensus scorer error: {e}", e=str(exc))
        return _verdict("timesfm_consensus", "TimesFM Signal Consensus",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_trust_convergence(engine: Engine, accuracy: float) -> dict:
    """Multi-source convergence: when 3+ independent sources agree on a ticker.

    This is the highest-conviction signal — multiple actors seeing the same thing.
    """
    try:
        from intelligence.trust_scorer import detect_convergence
        events = detect_convergence(engine)

        if not events:
            return _verdict("trust_convergence", "Signal Convergence",
                0, 15, "no convergence events", "",
                "No multi-source convergence events detected (need 3+ independent sources on same ticker).",
                status="active", historical_accuracy=accuracy)

        buy_events = [e for e in events if e.get("direction", "").upper() == "BUY"]
        sell_events = [e for e in events if e.get("direction", "").upper() == "SELL"]

        net = len(buy_events) - len(sell_events)
        score = net * 20  # each convergence event ≈ 20 points
        score = max(-80, min(80, score))

        # Confidence: convergence events are high-quality by definition
        confidence = 60 + min(20, len(events) * 5) + (accuracy * 10)

        buy_detail = [f"{e.get('ticker', '?')}({e.get('source_count', '?')}src)" for e in buy_events[:3]]
        sell_detail = [f"{e.get('ticker', '?')}({e.get('source_count', '?')}src)" for e in sell_events[:3]]

        return _verdict(
            "trust_convergence", "Signal Convergence",
            score, confidence,
            data_point=f"{len(buy_events)} buy convergences, {len(sell_events)} sell convergences",
            threshold="each convergence ≈ ±20 points; 3+ independent sources required",
            reasoning=(
                f"{len(events)} convergence event{'s' if len(events) != 1 else ''}: "
                + (f"BUY signals on {', '.join(buy_detail)}" if buy_detail else "no buy convergences")
                + "; "
                + (f"SELL signals on {', '.join(sell_detail)}" if sell_detail else "no sell convergences")
                + ". Multi-source agreement is the strongest conviction signal."
            ),
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("Trust convergence scorer error: {e}", e=str(exc))
        return _verdict("trust_convergence", "Signal Convergence",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_regime_changepoints(engine: Engine, accuracy: float, as_of: datetime | None = None) -> dict:
    """Regime changepoints: which declared regime features broke recently, as of ``as_of``.

    Logic: discovery/changepoint_detector.py writes one signal_registry row
    per point-in-time drift break on a declared regime feature (VIX, credit
    spreads, curve, 10y, S&P 500, net liquidity, dollar, claims, breakevens),
    stamped with its version in ``provenance``. ``direction`` is already the
    risk reading: bullish = toward risk-on, bearish = toward risk-off.

    ``as_of`` is an explicit instant (default: now, UTC; a naive value is
    UTC). A row is visible at ``as_of`` only if both fences hold, each on the
    row itself, in SQL and before any reconciliation:

    * ``valid_from <= as_of``: its availability;
    * ``metadata.published_at <= as_of``: the scan that actually wrote it.
      A break's first row written by a late scan has valid_from = its
      effective known-at, earlier than the scan; without this fence a replay
      between the two would see a break GRID did not hold yet, and a
      withdrawal or revision published after ``as_of`` could change what was
      live then.

    A row with no ``published_at`` (none is written by detector version 2)
    is not visible and is counted as ``unpublished``.

    Counts *features*, not visible rows, in three steps:

    1. Each break (``signal_id``) is reduced to its latest availability row
       (``valid_from``). A break re-evaluated on a later vintage has a later
       row of the same signal_id; one no longer detected has a row with
       metadata status ``withdrawn``.
    2. Breaks whose latest row is a withdrawal are dropped, and so are breaks
       whose effective known-at is older than CHANGEPOINT_FRESH_DAYS
       before ``as_of``.
    3. Each feature counts once, by its surviving break with the latest
       break date, so withdrawing one break never hides another on the same
       feature.

    Two times are kept apart. ``metadata.effective_known_at`` is when the
    break became knowable from its inputs (release, vintage and retraction
    dates); the freshness window is measured on it. ``metadata.
    first_published_at`` is when the detector first wrote the break; it is
    informational and never extends freshness. ``valid_from`` is the row's
    availability: the effective known-at for a break's first row, and never
    before the scan that saw a revision or withdrawal.

    Only rows of CHANGEPOINT_DETECTOR_VERSION count; the earlier detector's
    rows (about 1.08M, one per feature per index per 12 h scan) are ignored.
    Score = net (risk-on minus risk-off features) x
    CHANGEPOINT_POINTS_PER_FEATURE x average confidence, capped at
    +/-CHANGEPOINT_SCORE_CAP; confidence is capped at
    CHANGEPOINT_CONFIDENCE_CAP.
    """
    try:
        as_of = as_of or datetime.now(timezone.utc)
        if as_of.tzinfo is None:
            as_of = as_of.replace(tzinfo=timezone.utc)
        with engine.connect() as conn:
            # Guard: table might not exist yet
            try:
                conn.execute(text("SELECT 1 FROM signal_registry LIMIT 0"))
            except Exception:
                return _verdict(
                    "regime_changepoints", "Regime Changepoints",
                    0, 0, "no table", "",
                    "signal_registry table does not exist.",
                    status="no_data", historical_accuracy=accuracy,
                )

            # Both fences on every row, before reconciliation. valid_from >=
            # effective known-at on every row, so the window holds every row
            # whose break can still be fresh at as_of. Rows without
            # published_at come back flagged and are dropped below.
            rows = conn.execute(text(
                "SELECT signal_id, ticker, direction, confidence, valid_from, "
                "metadata->>'status', metadata->>'break_date', "
                "COALESCE(CAST(metadata->>'effective_known_at' AS timestamptz), valid_from), "
                "metadata->>'published_at' IS NULL "
                "FROM signal_registry "
                "WHERE source_module = :src "
                "AND signal_type = :stype "
                "AND provenance = :version "
                "AND valid_from <= :as_of "
                "AND valid_from > :window_start "
                "AND (metadata->>'published_at' IS NULL "
                "OR CAST(metadata->>'published_at' AS timestamptz) <= :as_of) "
                "ORDER BY valid_from DESC "
                "LIMIT :row_limit"
            ).bindparams(
                src="discovery.changepoint_detector",
                stype="regime_change",
                version=CHANGEPOINT_DETECTOR_VERSION,
                as_of=as_of,
                window_start=as_of - timedelta(days=CHANGEPOINT_FRESH_DAYS),
                row_limit=1000,
            )).fetchall()

        window = f"{CHANGEPOINT_FRESH_DAYS}d"
        unpublished_count = sum(1 for r in rows if r[8])
        rows = [r for r in rows if not r[8]]
        if not rows:
            return _verdict(
                "regime_changepoints", "Regime Changepoints",
                0, 0, "no fresh breaks", "",
                f"No declared regime feature has a break known in the last {window} "
                f"as of {as_of.isoformat()} (detector {CHANGEPOINT_DETECTOR_VERSION}"
                + (f"; {unpublished_count} row{'s' if unpublished_count != 1 else ''} without "
                   "published_at not visible" if unpublished_count else "")
                + ").",
                status="no_data", historical_accuracy=accuracy,
            )

        # rows: (signal_id, ticker, direction, confidence, valid_from, status,
        # break_date, effective_known_at, unpublished), newest valid_from first.
        def _utc(ts: Any) -> Any:
            return ts if ts is None or ts.tzinfo else ts.replace(tzinfo=timezone.utc)

        # 1. Latest availability per break (by valid_from, whatever the row order).
        latest_by_break: dict[Any, Any] = {}
        for i, r in enumerate(rows):
            key = r[0] if r[0] is not None else ("row", i)
            current = latest_by_break.get(key)
            if current is None or _utc(r[4]) > _utc(current[4]):
                latest_by_break[key] = r
        # 2. Drop withdrawn breaks, then breaks no longer fresh by effective known-at.
        live = [r for r in latest_by_break.values() if r[5] != "withdrawn"]
        withdrawn_count = len(latest_by_break) - len(live)
        fresh_after = as_of - timedelta(days=CHANGEPOINT_FRESH_DAYS)
        fresh = [r for r in live if _utc(r[7]) is not None and _utc(r[7]) > fresh_after]
        aged_count = len(live) - len(fresh)
        # 3. One break per feature: the latest break date, then the latest availability.
        by_feature: dict[Any, Any] = {}
        for r in fresh:
            current = by_feature.get(r[1])
            if current is None or (str(r[6] or ""), _utc(r[4])) > (str(current[6] or ""), _utc(current[4])):
                by_feature[r[1]] = r
        features = [(r[1], r[2], r[3], r[7]) for r in by_feature.values()]
        if not features:
            return _verdict(
                "regime_changepoints", "Regime Changepoints",
                0, 0, "no fresh breaks", "",
                f"No live regime break known in the last {window}: {withdrawn_count} "
                f"withdrawn on later data, {aged_count} older than the window "
                f"(as of {as_of.isoformat()}, detector {CHANGEPOINT_DETECTOR_VERSION}).",
                status="no_data", historical_accuracy=accuracy,
            )
        total = len(features)
        bullish = sorted(str(r[0]) for r in features if r[1] == "bullish")
        bearish = sorted(str(r[0]) for r in features if r[1] == "bearish")
        bull_count = len(bullish)
        bear_count = len(bearish)
        neutral_count = sum(r[1] == "neutral" for r in features)
        other_count = total - bull_count - bear_count - neutral_count

        # Average confidence over every counted feature, unknown ones included
        avg_conf = sum(float(r[2] or 0) for r in features) / total

        net = bull_count - bear_count
        score = net * CHANGEPOINT_POINTS_PER_FEATURE * avg_conf
        score = max(-CHANGEPOINT_SCORE_CAP, min(CHANGEPOINT_SCORE_CAP, score))

        conf_base = 20
        conf_volume = min(20, total * 4)
        conf_quality = avg_conf * 20  # 0-20 from break confidence
        conf_accuracy = accuracy * 10
        confidence = min(CHANGEPOINT_CONFIDENCE_CAP,
                         conf_base + conf_volume + conf_quality + conf_accuracy)

        # Data age from the most recently knowable counted break
        most_recent = max(_utc(r[3]) for r in features)
        age_hours = None
        if most_recent:
            try:
                ts = most_recent if most_recent.tzinfo else most_recent.replace(tzinfo=timezone.utc)
                age_hours = (as_of - ts).total_seconds() / 3600
            except Exception as exc:
                log.warning("Failed to compute regime changepoints age: {e}", e=exc)

        def _named(names: list[str]) -> str:
            return f" ({', '.join(names)})" if names else ""

        result = _verdict(
            "regime_changepoints", "Regime Changepoints",
            score, confidence,
            data_point=(
                f"{total} feature{'s' if total != 1 else ''} with a fresh break: "
                f"{bull_count} bullish, {bear_count} bearish, "
                f"{neutral_count} neutral, {other_count} other/unknown "
                f"({window}, avg conf {avg_conf:.0%})"
            ),
            threshold=(
                f"each feature ~ +/-{CHANGEPOINT_POINTS_PER_FEATURE:g} pts scaled by confidence, "
                f"capped at +/-{CHANGEPOINT_SCORE_CAP:g}; latest break per feature in {window}"
            ),
            reasoning=(
                f"{total} declared regime feature{'s' if total != 1 else ''} broke in the last {window}: "
                f"{bull_count} toward risk-on{_named(bullish)}, "
                f"{bear_count} toward risk-off{_named(bearish)}. "
                f"{neutral_count} neutral; {other_count} other/unknown. "
                + (f"{withdrawn_count} break{'s' if withdrawn_count != 1 else ''} withdrawn on later data, "
                   "not counted. " if withdrawn_count else "")
                + f"Average confidence {avg_conf:.0%}. "
                + ("Net risk-on regime shifts suggest improving conditions. "
                   if net > 0
                   else "Net risk-off regime shifts suggest deteriorating conditions. "
                   if net < 0
                   else "Balanced regime shifts — no clear directional signal. ")
                + f"Point-in-time drift breaks as of {as_of.isoformat()}, detector {CHANGEPOINT_DETECTOR_VERSION}."
            ),
            data_age_hours=age_hours,
            historical_accuracy=accuracy,
        )
        result["category_counts"] = {
            "unit": "features", "rows_read": len(rows), "breaks_read": len(latest_by_break),
            "total": total, "bullish": bull_count, "bearish": bear_count,
            "neutral": neutral_count, "other_unknown": other_count,
            "withdrawn": withdrawn_count, "aged_out": aged_count, "unpublished": unpublished_count,
            "known_categories_exhaustive": other_count == 0,
        }
        return result
    except Exception as exc:
        log.debug("Regime changepoints scorer error: {e}", e=str(exc))
        return _verdict(
            "regime_changepoints", "Regime Changepoints",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


# ══════════════════════════════════════════════════════════════════════════
# HISTORICAL ACCURACY LOADER
# ══════════════════════════════════════════════════════════════════════════

_accuracy_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_ACCURACY_CACHE_TTL = 300.0  # seconds (5 min)


def _load_model_accuracies(engine: Engine) -> dict[str, float]:
    """Load per-model win rates from thesis_snapshots.

    Returns {model_key: accuracy} where accuracy is between 0 and 1.
    Falls back to DEFAULT_ACCURACY (0.50) for models with no history.
    """
    now = _time.time()
    if (
        _accuracy_cache["data"] is not None
        and (now - _accuracy_cache["ts"]) < _ACCURACY_CACHE_TTL
    ):
        return _accuracy_cache["data"]

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT model_states, outcome FROM thesis_snapshots "
                "WHERE outcome IS NOT NULL AND model_states IS NOT NULL "
                "ORDER BY timestamp DESC LIMIT 200"
            )).fetchall()

        if not rows:
            return {}

        # Count hits and misses per model
        hits: dict[str, int] = {}
        total: dict[str, int] = {}

        for row in rows:
            states = row[0] if isinstance(row[0], dict) else json.loads(row[0]) if row[0] else {}
            outcome = row[1]  # correct / wrong / partial

            for model_key, state in states.items():
                model_dir = state.get("direction", "neutral")
                if model_dir == "neutral":
                    continue

                total[model_key] = total.get(model_key, 0) + 1

                if outcome == "correct":
                    # This model agreed with the majority and the majority was right
                    hits[model_key] = hits.get(model_key, 0) + 1
                elif outcome == "partial":
                    hits[model_key] = hits.get(model_key, 0) + 0.5

        accuracies = {}
        for key in total:
            if total[key] >= 5:  # need at least 5 scored snapshots
                accuracies[key] = hits.get(key, 0) / total[key]

        _accuracy_cache["data"] = accuracies
        _accuracy_cache["ts"] = _time.time()
        return accuracies

    except Exception as exc:
        log.debug("Failed to load model accuracies: {e}", e=str(exc))
        return {}


# ══════════════════════════════════════════════════════════════════════════
# MAIN SCORING FUNCTION
# ══════════════════════════════════════════════════════════════════════════

# All model scorers in execution order
def _score_news_sentiment(engine: Engine, accuracy: float) -> dict:
    """News sentiment: aggregate mood from recent headlines.

    Queries news_articles for last 6 hours.  Counts bullish/bearish/neutral
    sentiment labels and surfaces the top headlines so the thesis reflects
    breaking events (wars, crashes, policy shifts).

    Score: +100 = all bullish, -100 = all bearish.  Weighted by recency.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT title, source, sentiment, published_at,
                       confidence, llm_summary
                FROM news_articles
                WHERE published_at >= NOW() - INTERVAL '6 hours'
                ORDER BY published_at DESC
            """)).fetchall()

        if not rows:
            log.debug("News sentiment: no articles in last 6h — excluding from score")
            return _verdict(
                "news_sentiment", "News Sentiment",
                0, 0, "no recent articles", "",
                "No news articles in the last 6 hours.",
                status="stale", historical_accuracy=accuracy,
            )

        total = len(rows)
        bull = sum(1 for r in rows if r[2] and r[2].upper() == "BULLISH")
        bear = sum(1 for r in rows if r[2] and r[2].upper() == "BEARISH")
        neutral = total - bull - bear

        # Weighted score: high-confidence articles count more, including neutrals in denominator
        weighted_bull = 0.0
        weighted_bear = 0.0
        weighted_neutral = 0.0
        for r in rows:
            sent = (r[2] or "").upper()
            conf = float(r[4]) if r[4] is not None else NEWS_DEFAULT_ARTICLE_CONF
            if sent == "BULLISH":
                weighted_bull += conf
            elif sent == "BEARISH":
                weighted_bear += conf
            else:
                weighted_neutral += conf

        # Score: weighted net sentiment over ALL articles (including neutrals), scaled to [-100, +100].
        # Require at least NEWS_MIN_DIRECTIONAL_ARTICLES directional headlines for a nonzero score.
        directional_count = bull + bear
        weight_total = weighted_bull + weighted_bear + weighted_neutral
        if directional_count >= NEWS_MIN_DIRECTIONAL_ARTICLES and weight_total > 0:
            net_pct = (weighted_bull - weighted_bear) / weight_total
            score = net_pct * NEWS_SCORE_SCALE
            conf_skew = min(30, abs(bull - bear) * 2)
        else:
            score = 0.0
            conf_skew = 0.0

        # Confidence: volume + skew + quality
        conf_volume = min(40, total * 0.5)
        conf_accuracy = accuracy * 20
        confidence = min(90, 20 + conf_volume + conf_skew + conf_accuracy)

        # Top stories with LLM impact reasoning (prefer non-neutral, high-conf)
        scored = [
            (r, float(r[4]) if r[4] is not None else 0.5)
            for r in rows if r[2] and r[2].upper() != "NEUTRAL"
        ]
        scored.sort(key=lambda x: -x[1])
        top = [s[0] for s in scored[:5]]
        if not top:
            top = rows[:5]

        headline_parts = []
        for r in top:
            impact = r[5] or r[0][:80]  # llm_summary or title fallback
            headline_parts.append(
                f"[{(r[2] or 'NEUTRAL').upper()}] {r[1]}: {impact[:120]}"
            )
        headlines_text = "\n  ".join(headline_parts)

        # Data age
        most_recent = rows[0][3]
        age_hours = None
        if most_recent:
            try:
                ts = most_recent if most_recent.tzinfo else most_recent.replace(
                    tzinfo=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            except Exception:
                pass

        if directional_count < NEWS_MIN_DIRECTIONAL_ARTICLES:
            mood = (
                f"insufficient directional articles ({directional_count} < "
                f"{NEWS_MIN_DIRECTIONAL_ARTICLES}) — neutral"
            )
        elif bear > bull * 2:
            mood = "strongly negative — risk-off environment"
        elif bear > bull:
            mood = "leaning negative"
        elif bull > bear * 2:
            mood = "strongly positive — risk-on environment"
        elif bull > bear:
            mood = "leaning positive"
        else:
            mood = "mixed — no dominant narrative"

        reasoning = (
            f"{total} articles in last 6h: {bull} bullish, {bear} bearish, "
            f"{neutral} neutral. Overall mood: {mood}.\n"
            f"  Top stories:\n  {headlines_text}"
        )

        return _verdict(
            "news_sentiment", "News Sentiment",
            score, confidence,
            data_point=f"{total} articles: {bull} bullish, {bear} bearish, {neutral} neutral",
            threshold=(
                f"weighted (bull - bear) / (bull + bear + neutral) scaled to [-100, +100]; "
                f"requires >={NEWS_MIN_DIRECTIONAL_ARTICLES} directional articles"
            ),
            reasoning=reasoning,
            data_age_hours=age_hours,
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("News sentiment scorer error: {e}", e=str(exc))
        return _verdict(
            "news_sentiment", "News Sentiment",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_geopolitical_risk(engine: Engine, accuracy: float) -> dict:
    """Geopolitical risk from news + GDELT + Crucix OSINT signals.

    Scans news_articles for war, sanctions, military, tariff keywords
    in the last 12 hours. High concentration of geopolitical headlines
    signals risk-off environment. Also checks Crucix bridge data and
    GDELT event counts if available.
    """
    try:
        with engine.connect() as conn:
            # News-based geopolitical signal (most reliable)
            geo_keywords = [
                '%war%', '%bomb%', '%strike%', '%attack%', '%military%',
                '%missile%', '%invasion%', '%escalation%', '%sanctions%',
                '%tariff%', '%embargo%', '%nuclear%', '%threat%',
                '%ceasefire%', '%peace deal%', '%treaty%',
            ]
            conditions = " OR ".join(
                f"title ILIKE :k{i}" for i in range(len(geo_keywords))
            )
            params = {f"k{i}": kw for i, kw in enumerate(geo_keywords)}

            geo_count = conn.execute(text(
                f"SELECT COUNT(*) FROM news_articles "
                f"WHERE published_at >= NOW() - INTERVAL '12 hours' "
                f"AND ({conditions})"
            ), params).fetchone()[0]

            total_news = conn.execute(text(
                "SELECT COUNT(*) FROM news_articles "
                "WHERE published_at >= NOW() - INTERVAL '12 hours'"
            )).fetchone()[0]

            # Peace vs conflict ratio
            peace_kw = ['%ceasefire%', '%peace deal%', '%treaty%',
                        '%de-escalation%', '%truce%']
            peace_cond = " OR ".join(
                f"title ILIKE :p{i}" for i in range(len(peace_kw))
            )
            peace_params = {f"p{i}": kw for i, kw in enumerate(peace_kw)}
            peace_count = conn.execute(text(
                f"SELECT COUNT(*) FROM news_articles "
                f"WHERE published_at >= NOW() - INTERVAL '12 hours' "
                f"AND ({peace_cond})"
            ), peace_params).fetchone()[0]

            conflict_count = geo_count - peace_count

            # Crucix OSINT signals (if available)
            crucix_alerts = 0
            try:
                r = conn.execute(text(
                    "SELECT COUNT(DISTINCT series_id) FROM raw_series "
                    "WHERE series_id LIKE 'crucix.%' "
                    "AND pull_status = 'SUCCESS' "
                    "AND obs_date >= CURRENT_DATE - 1 "
                    "AND value IS NOT NULL AND value != 0"
                )).fetchone()
                crucix_alerts = r[0] if r else 0
            except Exception:
                pass

            # GDELT tension signals (if available)
            gdelt_events = 0
            try:
                r = conn.execute(text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE series_id LIKE 'gdelt_tension_%' "
                    "AND pull_status = 'SUCCESS' "
                    "AND obs_date >= CURRENT_DATE - 1"
                )).fetchone()
                gdelt_events = r[0] if r else 0
            except Exception:
                pass

        if total_news == 0:
            return _verdict(
                "geopolitical_risk", "Geopolitical Risk",
                0, 10, "no recent news", "",
                "No news in the last 12 hours to assess geopolitical risk.",
                status="stale", historical_accuracy=accuracy,
            )

        geo_pct = geo_count / total_news * 100 if total_news > 0 else 0

        # Score: more geopolitical news = more bearish
        # >30% of news is geopolitical = high risk
        # Peace headlines offset conflict
        net_conflict = conflict_count - peace_count * 2
        if geo_pct > 30 and net_conflict > 5:
            score = -80
        elif geo_pct > 20 and net_conflict > 3:
            score = -60
        elif geo_pct > 10 and net_conflict > 0:
            score = -40
        elif peace_count > conflict_count:
            score = 20  # de-escalation is bullish
        elif geo_pct > 5:
            score = -20
        else:
            score = 0

        confidence = min(85, 30 + geo_pct * 1.5 + accuracy * 15)

        reasoning = (
            f"{geo_count}/{total_news} articles ({geo_pct:.0f}%) are geopolitical in last 12h. "
            f"{conflict_count} conflict, {peace_count} peace/de-escalation. "
        )
        if crucix_alerts > 0:
            reasoning += f"Crucix flagged {crucix_alerts} OSINT alerts. "
        if gdelt_events > 0:
            reasoning += f"GDELT logged {gdelt_events} events. "

        if score < -40:
            reasoning += "Elevated geopolitical risk — markets in risk-off mode."
        elif score > 0:
            reasoning += "De-escalation signals — risk appetite recovering."
        else:
            reasoning += "Background geopolitical noise, not yet market-moving."

        return _verdict(
            "geopolitical_risk", "Geopolitical Risk",
            score, confidence,
            data_point=f"{geo_count} geo articles / {total_news} total ({geo_pct:.0f}%)",
            threshold=">30% = high risk (-80), >20% = elevated (-60), >10% = moderate (-40)",
            reasoning=reasoning,
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("Geopolitical risk scorer error: {e}", e=str(exc))
        return _verdict(
            "geopolitical_risk", "Geopolitical Risk",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_social_sentiment(engine: Engine, accuracy: float) -> dict:
    """Social media sentiment from signal_feed (Reddit, StockTwits, Google Trends).

    Reads the signal_feed table for social-origin anomalies and
    raw_series for Google Trends / StockTwits data.
    """
    try:
        with engine.connect() as conn:
            # Signal feed social signals
            social = conn.execute(text("""
                SELECT COUNT(*),
                       SUM(CASE WHEN z_score > 1.5 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN z_score < -1.5 THEN 1 ELSE 0 END)
                FROM signal_feed
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                AND (family ILIKE '%social%' OR family ILIKE '%sentiment%'
                     OR signal_type ILIKE '%reddit%' OR signal_type ILIKE '%social%'
                     OR signal_type ILIKE '%trend%' OR signal_type ILIKE '%stocktwits%')
            """)).fetchone()

            total = social[0] or 0
            bull = social[1] or 0  # positive z-score = bullish anomaly
            bear = social[2] or 0  # negative z-score = bearish anomaly

            if total == 0:
                # Fallback: check signal_data for social/sentiment signals
                alt = conn.execute(text("""
                    SELECT COUNT(*) FROM signal_data
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                    AND (signal_type ILIKE '%social%' OR signal_type ILIKE '%reddit%'
                         OR signal_type ILIKE '%sentiment%' OR signal_type ILIKE '%google%')
                """)).fetchone()
                if alt and alt[0] > 0:
                    total = alt[0]
                    # Can't distinguish bull/bear from signal_data easily
                    return _verdict(
                        "social_sentiment", "Social Sentiment",
                        0, 25,
                        data_point=f"{total} social signals (direction unknown)",
                        threshold="net social sentiment scaled to [-100, +100]",
                        reasoning=f"{total} social media signals detected but direction not resolved. "
                                  "Treat as neutral until signal_feed captures direction.",
                        historical_accuracy=accuracy,
                    )
                log.debug("Social sentiment: no signals in last 24h — excluding from score")
                return _verdict(
                    "social_sentiment", "Social Sentiment",
                    0, 0, "no social signals", "",
                    "No social media signals in the last 24 hours.",
                    status="stale", historical_accuracy=accuracy,
                )

            net_pct = (bull - bear) / total if total > 0 else 0
            # Contrarian: retail euphoria (bull > bear) scores bearish; panic scores bullish.
            score = -net_pct * SOCIAL_SENTIMENT_MAX_SCORE

            confidence = min(70, 15 + total * 0.5 + accuracy * 15)

            if bull > bear * SOCIAL_SENTIMENT_EXTREME_RATIO:
                mood = "retail euphoria (contrarian bearish)"
                implication = "Extreme retail euphoria often precedes corrections — contrarian bearish signal."
            elif bull > bear:
                mood = "leaning positive (mildly contrarian bearish)"
                implication = "Skewed bullish retail chatter leans contrarian bearish."
            elif bear > bull * SOCIAL_SENTIMENT_EXTREME_RATIO:
                mood = "retail panic (contrarian bullish)"
                implication = "Extreme retail panic often marks capitulation — contrarian bullish signal."
            elif bear > bull:
                mood = "leaning negative (mildly contrarian bullish)"
                implication = "Skewed bearish retail chatter leans contrarian bullish."
            else:
                mood = "mixed chatter (neutral)"
                implication = "Balanced retail chatter — no contrarian directional edge."

            reasoning = (
                f"{total} social signals (24h): {bull} bullish, {bear} bearish. "
                f"Retail mood: {mood}. "
                f"Social sentiment is a lagging/contrarian indicator: {implication}"
            )

            return _verdict(
                "social_sentiment", "Social Sentiment",
                score, confidence,
                data_point=f"{total} signals: {bull} bullish, {bear} bearish",
                threshold=(
                    f"contrarian -(bull - bear)/total scaled to "
                    f"[-{int(SOCIAL_SENTIMENT_MAX_SCORE)}, +{int(SOCIAL_SENTIMENT_MAX_SCORE)}] "
                    "(euphoria = bearish, panic = bullish)"
                ),
                reasoning=reasoning,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Social sentiment scorer error: {e}", e=str(exc))
        return _verdict(
            "social_sentiment", "Social Sentiment",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_crypto_risk(engine: Engine, accuracy: float) -> dict:
    """Crypto market risk signal from CoinGecko/DeFi Llama/Binance data.

    Crypto often leads equity risk-off moves. Bitcoin below 200-day MA
    or stablecoin depegs signal broad risk aversion.
    """
    try:
        with engine.connect() as conn:
            # Bitcoin price from raw_series
            btc = conn.execute(text("""
                SELECT value, obs_date FROM raw_series
                WHERE series_id IN ('coingecko:bitcoin:usd', 'YF:BTC-USD:close',
                                    'binance:BTCUSDT:close', 'CG:bitcoin:usd')
                AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            btc_30d = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id IN ('coingecko:bitcoin:usd', 'YF:BTC-USD:close',
                                    'binance:BTCUSDT:close', 'CG:bitcoin:usd')
                AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 30
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            if not btc:
                return _verdict(
                    "crypto_risk", "Crypto Risk Barometer",
                    0, 10, "no BTC data", "",
                    "No Bitcoin price data available.",
                    status="stale", historical_accuracy=accuracy,
                )

            btc_price = float(btc[0])
            btc_date = btc[1]

            # 30-day change
            change_30d = 0.0
            if btc_30d:
                prior = float(btc_30d[0])
                if prior > 0:
                    change_30d = (btc_price - prior) / prior * 100

            # DeFi TVL from raw_series (if available)
            defi_tvl = None
            try:
                r = conn.execute(text("""
                    SELECT value FROM raw_series
                    WHERE series_id LIKE 'defillama%tvl%'
                    AND pull_status = 'SUCCESS'
                    ORDER BY obs_date DESC LIMIT 1
                """)).fetchone()
                if r:
                    defi_tvl = float(r[0])
            except Exception:
                pass

            # Score: BTC down >15% in 30d = bearish risk signal
            if change_30d < -20:
                score = -70
            elif change_30d < -10:
                score = -45
            elif change_30d < -5:
                score = -20
            elif change_30d > 20:
                score = 50
            elif change_30d > 10:
                score = 30
            elif change_30d > 5:
                score = 15
            else:
                score = 0

            confidence = min(75, 30 + abs(change_30d) * 1.5 + accuracy * 15)

            reasoning = (
                f"Bitcoin at ${btc_price:,.0f} ({change_30d:+.1f}% over 30 days). "
            )
            if change_30d < -10:
                reasoning += "Significant crypto drawdown — risk appetite shrinking across all speculative assets. "
            elif change_30d > 15:
                reasoning += "Strong crypto rally — risk-on environment, speculative appetite high. "
            else:
                reasoning += "Crypto range-bound — no strong risk signal. "

            if defi_tvl:
                reasoning += f"DeFi TVL: ${defi_tvl/1e9:.1f}B. "

            # Age
            age_hours = None
            if btc_date:
                from datetime import date as dt_date
                if isinstance(btc_date, dt_date):
                    days = (date.today() - btc_date).days
                    age_hours = days * 24.0

            return _verdict(
                "crypto_risk", "Crypto Risk Barometer",
                score, confidence,
                data_point=f"BTC ${btc_price:,.0f} ({change_30d:+.1f}% 30d)",
                threshold=">-20% = bearish (-70), >+20% = bullish (+50)",
                reasoning=reasoning,
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Crypto risk scorer error: {e}", e=str(exc))
        return _verdict(
            "crypto_risk", "Crypto Risk Barometer",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_cftc_positioning(engine: Engine, accuracy: float) -> dict:
    """CFTC Commitments of Traders — net speculative positioning for equity direction.

    Tracks smart-money futures positioning with asset-class-appropriate signs:
    - ES (E-mini S&P, equity index, weight 2x): net long + rising → bullish (+60),
      net short → bearish (-20 to -50).
    - GC (Gold, safe haven, weight 1x, INVERSE): heavy net long Gold reflects
      flight-to-safety / risk-off positioning (bearish for equities, -10 to -30);
      net short Gold reflects risk-on (+10 to +20).
    - VX (VIX, equity vol, weight 1x, CONTRARIAN): extreme net long VIX → fear
      hedging / contrarian bullish (+15 to +30); extreme net short → complacency (-20).
    - CL (WTI crude, commodity): reported for context only and excluded from
      the equity-direction score so commodity longs cannot flip equity positioning.

    Data is weekly (Tuesday snapshot, Friday release).

    Reads only the code-keyed ids (``cftc.<cftc_contract_market_code>.*``);
    the legacy ``cftc.SP500.*`` etc. mixed several markets under one id.
    With no code-keyed rows the verdict is ``no_data``.
    """
    from ingestion.altdata.cftc_markets import series_id_for_root

    # root -> display name; identity is the market code behind the root
    KEY_CONTRACTS = {"ES": "E-mini S&P", "GC": "Gold", "CL": "WTI", "VX": "VIX"}
    try:
        with engine.connect() as conn:
            weighted_scores: list[tuple[float, float]] = []
            details: list[str] = []
            context_details: list[str] = []
            oldest_date = None

            for root, contract in KEY_CONTRACTS.items():
                sid = series_id_for_root(root, "net_speculative")
                cur = conn.execute(text(
                    "SELECT value, obs_date FROM raw_series "
                    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"sid": sid}).fetchone()

                prior = conn.execute(text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                    "AND obs_date <= CURRENT_DATE - 28 "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"sid": sid}).fetchone()

                if not cur:
                    continue

                val = float(cur[0])
                obs = cur[1]
                # No prior observation -> momentum unknown (never "val - 0").
                momentum = val - float(prior[0]) if prior else None
                direction = "long" if val > 0 else "short" if val < 0 else "flat"

                if root == "CL":
                    # Non-equity commodity positioning: context only, excluded from equity score.
                    context_details.append(f"{contract}: {val:+,.0f} net spec ({direction}, context only)")
                    continue

                if oldest_date is None or (obs and obs < oldest_date):
                    oldest_date = obs

                if root == "ES":
                    # Equity index: pro-equity direction (primary weight).
                    if val > CFTC_EXTREME_NET_CONTRACTS and momentum is not None and momentum > 0:
                        s = 60.0
                    elif val > CFTC_MODERATE_NET_CONTRACTS:
                        s = 30.0
                    elif val > 0:
                        s = 10.0
                    elif val == 0:
                        s = 0.0
                    elif val > -CFTC_MODERATE_NET_CONTRACTS:
                        s = -20.0
                    else:
                        s = -50.0
                    weighted_scores.append((s, CFTC_EQUITY_WEIGHT))
                    details.append(f"{contract}: {val:+,.0f} net spec ({direction})")
                elif root == "GC":
                    # Safe haven: net long Gold = risk-off (bearish for equities).
                    if val > CFTC_EXTREME_NET_CONTRACTS and momentum is not None and momentum > 0:
                        s = -30.0
                    elif val > CFTC_MODERATE_NET_CONTRACTS:
                        s = -20.0
                    elif val > 0:
                        s = -10.0
                    elif val == 0:
                        s = 0.0
                    elif val > -CFTC_MODERATE_NET_CONTRACTS:
                        s = 10.0
                    else:
                        s = 20.0
                    weighted_scores.append((s, CFTC_SAFE_HAVEN_WEIGHT))
                    tag = "long, risk-off" if val > 0 else "short, risk-on" if val < 0 else "flat"
                    details.append(f"{contract}: {val:+,.0f} net spec ({tag})")
                elif root == "VX":
                    # VIX is contrarian: net long VIX = fear = bullish for equities.
                    if val > CFTC_VIX_EXTREME_CONTRACTS:
                        s = 30.0  # extreme fear → contrarian bullish
                    elif val > CFTC_VIX_MODERATE_CONTRACTS:
                        s = 15.0
                    elif val < -CFTC_VIX_EXTREME_CONTRACTS:
                        s = -20.0  # complacency
                    else:
                        s = 0.0
                    weighted_scores.append((s, CFTC_VIX_WEIGHT))
                    details.append(f"{contract}: {val:+,.0f} net spec ({direction})")

            if not weighted_scores:
                log.debug("CFTC positioning: no scored equity/safe-haven/VIX COT series found")
                return _verdict(
                    "cftc_positioning", "Futures Positioning (COT)",
                    0, 0, "no CFTC data", "",
                    "No CFTC Commitments of Traders data found.",
                    status="no_data", historical_accuracy=accuracy,
                )

            total_weight = sum(w for _, w in weighted_scores)
            score = sum(s * w for s, w in weighted_scores) / total_weight
            all_details = details + context_details

            age_hours = None
            if oldest_date:
                days = (date.today() - oldest_date).days
                age_hours = days * 24.0

            # Weekly data → lower confidence, but high-signal
            confidence = min(80, 45 + len(weighted_scores) * 5 + accuracy * 15)
            if age_hours and age_hours > 168:  # older than 1 week
                confidence *= 0.7

            reasoning = "Futures positioning from CFTC COT report (equity-weighted; Gold longs scored risk-off). "
            if score > 30:
                reasoning += "Smart money heavily long equities — institutions riding the trend. "
            elif score > 0:
                reasoning += "Modest risk-on / net long equity positioning — cautiously bullish. "
            elif score > -20:
                reasoning += "Near-neutral equity positioning — no strong conviction. "
            else:
                reasoning += "Risk-off / net short equity positioning — institutions hedging or betting on decline. "
            reasoning += "; ".join(all_details[:4]) + "."

            return _verdict(
                "cftc_positioning", "Futures Positioning (COT)",
                score, confidence,
                data_point="; ".join(all_details[:2]),
                threshold=(
                    "ES equity (2x): long >100K rising (+60), short <-50K (-50); "
                    "GC safe-haven (1x, risk-off inverse): long >100K rising (-30); "
                    "VX contrarian (1x); CL context only"
                ),
                reasoning=reasoning,
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("CFTC positioning scorer error: {e}", e=str(exc))
        return _verdict(
            "cftc_positioning", "Futures Positioning (COT)",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_fed_hawkishness(engine: Engine, accuracy: float) -> dict:
    """Fed tone + FOMC proximity — are rate moves coming?

    Combines NLP hawkish/dovish scoring from Fed speeches with
    FOMC meeting proximity to gauge near-term policy risk.

    - Hawkish tone > +0.4 AND <14 days to FOMC → strong bearish (-65)
    - Dovish tone < -0.3 → bullish (+50)
    - Very recent meeting (<3 days) dampens signal (already priced)
    - Speech frequency burst = higher conviction
    """
    try:
        with engine.connect() as conn:
            # Latest hawkish score (7-day average)
            tone = conn.execute(text(
                "SELECT value, obs_date FROM raw_series "
                "WHERE series_id = 'fed_tone_7d_avg' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            # Days to next FOMC
            days_to = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'fomc_days_to_meeting' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            # Days since last FOMC
            days_since = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'fomc_days_since_meeting' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            # Speech frequency (activity indicator)
            freq = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'fed_speech_frequency' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            if not tone:
                return _verdict(
                    "fed_hawkishness", "Fed Tone & FOMC",
                    0, 0, "no Fed speech data", "",
                    "No Fed hawkish/dovish scoring data available.",
                    status="no_data", historical_accuracy=accuracy,
                )

            tone_val = float(tone[0])  # -1 (dovish) to +1 (hawkish)
            tone_date = tone[1]
            days_to_val = float(days_to[0]) if days_to else 30
            days_since_val = float(days_since[0]) if days_since else 15
            freq_val = float(freq[0]) if freq else 3

            age_hours = None
            if tone_date:
                days = (date.today() - tone_date).days
                age_hours = days * 24.0

            # Score: hawkish = bearish for equities
            if tone_val > 0.5:
                base_score = -65
            elif tone_val > 0.3:
                base_score = -40
            elif tone_val > 0.1:
                base_score = -15
            elif tone_val > -0.1:
                base_score = 0
            elif tone_val > -0.3:
                base_score = 20
            else:
                base_score = 50

            # FOMC proximity amplifier: closer meeting = louder signal
            if days_to_val <= 7:
                proximity_mult = 1.3
            elif days_to_val <= 14:
                proximity_mult = 1.15
            elif days_to_val <= 30:
                proximity_mult = 1.0
            else:
                proximity_mult = 0.8  # far from meeting, less urgent

            # Recent meeting dampener: just happened = already priced in
            if days_since_val <= 3:
                proximity_mult *= 0.5

            score = max(-80, min(80, base_score * proximity_mult))

            # Confidence: speech frequency boosts conviction
            confidence = min(85, 50 + freq_val * 3 + accuracy * 15)
            if age_hours and age_hours > 72:
                confidence *= 0.8

            tone_label = "hawkish" if tone_val > 0.1 else "dovish" if tone_val < -0.1 else "neutral"
            reasoning = (
                f"Fed tone is {tone_label} ({tone_val:+.2f} on -1 to +1 scale). "
                f"{int(days_to_val)} days to next FOMC meeting. "
            )
            if tone_val > 0.3 and days_to_val < 14:
                reasoning += "Hawkish speeches near an FOMC meeting — rate hike or tough stance likely. Markets hate that. "
            elif tone_val < -0.3:
                reasoning += "Dovish Fed speak — easing or patience ahead. Good for stocks. "
            else:
                reasoning += "Fed is keeping its cards close. No strong policy signal. "

            if freq_val > 5:
                reasoning += f"Unusually high speech activity ({int(freq_val)} in 7 days) — Fed trying to signal something."

            return _verdict(
                "fed_hawkishness", "Fed Tone & FOMC",
                score, confidence,
                data_point=f"tone {tone_val:+.2f} ({tone_label}), {int(days_to_val)}d to FOMC",
                threshold="hawkish >+0.3 = bearish, dovish <-0.3 = bullish, FOMC <14d amplifies",
                reasoning=reasoning,
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Fed hawkishness scorer error: {e}", e=str(exc))
        return _verdict(
            "fed_hawkishness", "Fed Tone & FOMC",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_valuation_compression(engine: Engine, accuracy: float) -> dict:
    """Trailing PE momentum — is the market getting cheap or expensive?

    P/E is price over trailing diluted EPS from filed SEC reports
    (analysis.sec_valuation; Tiingo fundamentals was retired 2026-11-08).

    Tracks aggregate PE ratio changes across major DOW stocks.
    PE expansion = investors paying more per dollar of earnings (risk of overvaluation).
    PE compression = earnings growing faster than price (potential value).

    - Aggregate PE > 30 AND rising → expensive, bearish (-40)
    - Aggregate PE 20-30 AND falling → compressing, bullish (+30)
    - Aggregate PE < 15 → historically cheap, strong bullish (+60)
    """
    TICKERS = ["AAPL", "MSFT", "AMZN", "NVDA", "JPM", "V", "UNH", "HD", "DIS", "MCD"]
    try:
        with engine.connect() as conn:
            pe_current = []
            paired_cur_pe = []
            paired_prior_pe = []
            eps_changes = []

            today = date.today()
            for ticker in TICKERS:
                cur = trailing_pe(conn, ticker, today)
                prior = trailing_pe(conn, ticker, today - timedelta(days=30))
                # Same 0-500 band as the old Tiingo reads: a near-zero EPS
                # makes a P/E in the thousands that would swamp the average.
                cur_ok = cur is not None and cur.pe < VALUATION_MAX_PE
                prior_ok = prior is not None and prior.pe < VALUATION_MAX_PE
                if cur_ok:
                    pe_current.append(cur.pe)
                if cur_ok and prior_ok:
                    paired_cur_pe.append(cur.pe)
                    paired_prior_pe.append(prior.pe)
                    cur_eps = getattr(cur, "eps_ttm", None)
                    prior_eps = getattr(prior, "eps_ttm", None)
                    if cur_eps is not None and prior_eps is not None and prior_eps > 0:
                        eps_changes.append((cur_eps - prior_eps) / prior_eps * 100.0)

            if len(pe_current) < VALUATION_MIN_TICKERS:
                return _verdict(
                    "valuation_compression", "Valuation Momentum",
                    0, 0, f"only {len(pe_current)} tickers with PE data", "",
                    "Not enough filed SEC earnings and recent prices to assess valuations.",
                    status="no_data", historical_accuracy=accuracy,
                )

            avg_pe = sum(pe_current) / len(pe_current)
            if paired_cur_pe and paired_prior_pe:
                avg_pe_paired = sum(paired_cur_pe) / len(paired_cur_pe)
                avg_pe_30d = sum(paired_prior_pe) / len(paired_prior_pe)
                pe_change_pct = (
                    ((avg_pe_paired - avg_pe_30d) / avg_pe_30d * 100.0)
                    if avg_pe_30d > 0
                    else 0.0
                )
            else:
                pe_change_pct = 0.0

            avg_eps_change_pct = (
                sum(eps_changes) / len(eps_changes) if eps_changes else None
            )
            earnings_growing = (
                avg_eps_change_pct is not None
                and avg_eps_change_pct > VALUATION_MIN_EPS_GROWTH_PCT
            )
            price_driven_compression = (
                pe_change_pct < -VALUATION_PE_CHANGE_MODERATE_PCT
                and not earnings_growing
            )

            # Score based on level AND direction
            if avg_pe > 35:
                level_score = -50
            elif avg_pe > 30:
                level_score = -30
            elif avg_pe > 25:
                level_score = -10
            elif avg_pe > 20:
                level_score = 10
            elif avg_pe > 15:
                level_score = 30
            else:
                level_score = 60

            # Momentum adjustment: expansion = bearish; compression is bullish ONLY
            # when trailing EPS actually grew (> 0%) over the 30-day window.
            if pe_change_pct > VALUATION_PE_CHANGE_STRONG_PCT:
                momentum_adj = -20  # rapid expansion
            elif pe_change_pct > VALUATION_PE_CHANGE_MODERATE_PCT:
                momentum_adj = -10
            elif pe_change_pct < -VALUATION_PE_CHANGE_STRONG_PCT and earnings_growing:
                momentum_adj = 20  # rapid earnings-driven compression (value appearing)
            elif pe_change_pct < -VALUATION_PE_CHANGE_MODERATE_PCT and earnings_growing:
                momentum_adj = 10
            else:
                momentum_adj = 0

            score = max(-80, min(80, level_score + momentum_adj))
            if price_driven_compression:
                # Price-driven P/E compression (prices fell without EPS growth): never score bullish.
                score = min(0, score)

            confidence = min(75, 35 + len(pe_current) * 3 + accuracy * 15)

            if avg_pe > 30:
                valuation_label = "expensive"
            elif avg_pe > 20:
                valuation_label = "fairly valued"
            else:
                valuation_label = "cheap"

            direction = "expanding" if pe_change_pct > 0 else "compressing"
            reasoning = (
                f"Market looks {valuation_label} at {avg_pe:.1f}x earnings "
                f"(avg across {len(pe_current)} major stocks). "
                f"PE ratios {direction} {abs(pe_change_pct):.1f}% over 30 days. "
            )
            if avg_pe > 30 and pe_change_pct > VALUATION_PE_CHANGE_MODERATE_PCT:
                reasoning += "Investors paying more and more for each dollar of earnings — classic late-cycle stretch. Risky. "
            elif price_driven_compression:
                eps_note = (
                    f"avg trailing EPS {avg_eps_change_pct:+.1f}%"
                    if avg_eps_change_pct is not None
                    else "trailing EPS growth unavailable"
                )
                reasoning += (
                    f"Price-driven compression ({eps_note} over 30d) — P/E fell because prices dropped "
                    "without positive earnings growth; not scored bullish. "
                )
            elif pe_change_pct < -VALUATION_PE_CHANGE_MODERATE_PCT and earnings_growing:
                reasoning += (
                    f"Earnings growing faster than prices (avg trailing EPS {avg_eps_change_pct:+.1f}%) "
                    "— value is appearing. "
                )
            elif avg_pe < 20:
                reasoning += "Stocks are historically cheap relative to earnings — potential bargains. "

            return _verdict(
                "valuation_compression", "Valuation Momentum",
                score, confidence,
                data_point=f"avg PE {avg_pe:.1f}x ({pe_change_pct:+.1f}% 30d), {len(pe_current)} stocks",
                threshold="PE >30 expensive, <20 cheap; expansion = bearish, compression = bullish only when 30d EPS growth > 0%",
                reasoning=reasoning,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Valuation compression scorer error: {e}", e=str(exc))
        return _verdict(
            "valuation_compression", "Valuation Momentum",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_fear_greed(engine: Engine, accuracy: float) -> dict:
    """CNN Fear & Greed Index — contrarian sentiment indicator.

    0 = Extreme Fear (contrarian bullish)
    25 = Fear
    50 = Neutral
    75 = Greed
    100 = Extreme Greed (contrarian bearish)

    This is CONTRARIAN: extreme fear = buy signal, extreme greed = sell signal.
    """
    try:
        with engine.connect() as conn:
            cur = conn.execute(text(
                "SELECT value, obs_date FROM raw_series "
                "WHERE series_id = 'feargreed.cnn_value' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            prior = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'feargreed.cnn_value' AND pull_status = 'SUCCESS' "
                "AND obs_date <= CURRENT_DATE - 7 "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            if not cur:
                return _verdict(
                    "fear_greed", "Fear & Greed Index",
                    0, 0, "no data", "", "No CNN Fear & Greed data.",
                    status="no_data", historical_accuracy=accuracy,
                )

            fg_val = float(cur[0])
            fg_date = cur[1]
            prior_val = float(prior[0]) if prior else 50.0

            age_hours = (date.today() - fg_date).days * 24.0 if fg_date else None

            # Contrarian scoring: extreme fear = bullish, extreme greed = bearish
            if fg_val <= 10:
                score = 80  # extreme fear → strong buy
            elif fg_val <= 25:
                score = 50  # fear → buy
            elif fg_val <= 40:
                score = 20
            elif fg_val <= 60:
                score = 0   # neutral
            elif fg_val <= 75:
                score = -20
            elif fg_val <= 90:
                score = -50  # greed → sell
            else:
                score = -80  # extreme greed → strong sell

            # Momentum: rapid fear increase = stronger contrarian buy
            momentum = prior_val - fg_val  # positive = fear increasing
            if abs(momentum) > 15:
                score += int(momentum * 0.3)
                score = max(-80, min(80, score))

            confidence = min(85, 55 + abs(fg_val - 50) * 0.5 + accuracy * 15)

            label = ("Extreme Fear" if fg_val <= 25 else "Fear" if fg_val <= 40
                     else "Neutral" if fg_val <= 60 else "Greed" if fg_val <= 75
                     else "Extreme Greed")

            reasoning = f"Fear & Greed at {fg_val:.0f} ({label}). "
            if fg_val <= 25:
                reasoning += "Markets are terrified — historically a buying opportunity. "
            elif fg_val >= 75:
                reasoning += "Markets are euphoric — historically a warning sign. "
            else:
                reasoning += "Sentiment is balanced — no strong contrarian signal. "

            if abs(momentum) > 10:
                direction = "toward fear" if momentum > 0 else "toward greed"
                reasoning += f"Shifted {abs(momentum):.0f} points {direction} in a week."

            return _verdict(
                "fear_greed", "Fear & Greed Index",
                score, confidence,
                data_point=f"F&G: {fg_val:.0f} ({label})",
                threshold="<25 = contrarian bullish, >75 = contrarian bearish",
                reasoning=reasoning,
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Fear & Greed scorer error: {e}", e=str(exc))
        return _verdict(
            "fear_greed", "Fear & Greed Index",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_retail_sentiment(engine: Engine, accuracy: float) -> dict:
    """AAII Investor Sentiment Survey — retail investor mood.

    Tracks bullish/bearish/neutral percentages from the weekly AAII survey.
    Contrarian: extreme bullish retail = bearish signal, extreme bearish = bullish.
    Bull-bear spread is the key metric.
    """
    try:
        with engine.connect() as conn:
            spread = conn.execute(text(
                "SELECT value, obs_date FROM raw_series "
                "WHERE series_id = 'aaii.bull_bear_spread' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            bullish = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'aaii.bullish_pct' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            bearish = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'aaii.bearish_pct' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            if not spread:
                return _verdict(
                    "retail_sentiment", "Retail Investor Sentiment",
                    0, 0, "no AAII data", "", "No AAII sentiment survey data.",
                    status="no_data", historical_accuracy=accuracy,
                )

            spread_val = float(spread[0])  # bullish% - bearish%
            spread_date = spread[1]
            bull_pct = float(bullish[0]) if bullish else 50.0
            bear_pct = float(bearish[0]) if bearish else 50.0

            age_hours = (date.today() - spread_date).days * 24.0 if spread_date else None

            # Contrarian: retail euphoria = sell, retail panic = buy
            # Historical mean spread is ~+6%. >+20 is extreme bullish, <-20 is extreme bearish
            if spread_val < -30:
                score = 70  # extreme retail fear → strong contrarian buy
            elif spread_val < -15:
                score = 40
            elif spread_val < 0:
                score = 15
            elif spread_val < 15:
                score = 0   # normal range
            elif spread_val < 30:
                score = -25
            else:
                score = -60  # extreme retail euphoria → contrarian sell

            # Weekly data → moderate confidence
            confidence = min(75, 40 + abs(spread_val) * 0.5 + accuracy * 15)
            if age_hours and age_hours > 168:  # older than 1 week
                confidence *= 0.7

            mood = "extremely bearish" if spread_val < -20 else "bearish" if spread_val < 0 else "neutral" if spread_val < 15 else "bullish" if spread_val < 30 else "extremely bullish"

            reasoning = (
                f"Retail investors are {mood} (bull-bear spread: {spread_val:+.1f}%). "
                f"Bullish: {bull_pct:.0f}%, Bearish: {bear_pct:.0f}%. "
            )
            if spread_val < -20:
                reasoning += "Retail panic is historically a buying opportunity. Smart money buys what retail sells."
            elif spread_val > 25:
                reasoning += "Retail euphoria is historically a warning. When everyone's bullish, who's left to buy?"
            else:
                reasoning += "Sentiment in normal range — no strong contrarian signal."

            return _verdict(
                "retail_sentiment", "Retail Investor Sentiment",
                score, confidence,
                data_point=f"AAII spread {spread_val:+.1f}% (bull {bull_pct:.0f}% / bear {bear_pct:.0f}%)",
                threshold="spread <-20 = contrarian bullish, >+25 = contrarian bearish",
                reasoning=reasoning,
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Retail sentiment scorer error: {e}", e=str(exc))
        return _verdict(
            "retail_sentiment", "Retail Investor Sentiment",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_gdp_nowcast(engine: Engine, accuracy: float) -> dict:
    """GDP Nowcast — real-time growth estimate from Atlanta Fed.

    Positive GDP = economy expanding = bullish backdrop.
    Negative GDP = contraction = bearish.
    """
    try:
        with engine.connect() as conn:
            cur = conn.execute(text(
                "SELECT value, obs_date FROM raw_series "
                "WHERE series_id = 'nowcast.gdpnow' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            if not cur:
                return _verdict(
                    "gdp_nowcast", "GDP Nowcast",
                    0, 0, "no data", "", "No GDPNow estimate available.",
                    status="no_data", historical_accuracy=accuracy,
                )

            gdp = float(cur[0])
            gdp_date = cur[1]
            age_hours = (date.today() - gdp_date).days * 24.0 if gdp_date else None

            # Score based on GDP growth rate
            if gdp > 4.0:
                score = 60   # strong growth
            elif gdp > 2.5:
                score = 35   # healthy growth
            elif gdp > 1.0:
                score = 10   # modest growth
            elif gdp > 0:
                score = -15  # stalling
            elif gdp > -1.0:
                score = -40  # contraction
            else:
                score = -70  # deep contraction

            confidence = min(80, 50 + abs(gdp) * 5 + accuracy * 15)

            if gdp > 2.5:
                label = "strong growth"
            elif gdp > 0:
                label = "modest growth"
            elif gdp > -1:
                label = "stalling"
            else:
                label = "contraction"

            reasoning = (
                f"Atlanta Fed GDPNow estimates {gdp:+.2f}% annualized GDP ({label}). "
            )
            if gdp > 3:
                reasoning += "Economy running hot — supports corporate earnings and risk assets."
            elif gdp > 1:
                reasoning += "Growth is positive but modest — okay backdrop for stocks."
            elif gdp > 0:
                reasoning += "Growth is barely positive — economy on the edge."
            else:
                reasoning += "GDP is contracting — recession risk is real. Defensive positioning warranted."

            return _verdict(
                "gdp_nowcast", "GDP Nowcast",
                score, confidence,
                data_point=f"GDPNow: {gdp:+.2f}% ({label})",
                threshold=">2.5% bullish, 0-1% caution, <0% bearish",
                reasoning=reasoning,
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("GDP Nowcast scorer error: {e}", e=str(exc))
        return _verdict(
            "gdp_nowcast", "GDP Nowcast",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_crucix_osint(engine: Engine, accuracy: float) -> dict:
    """Crucix OSINT composite: supply chain, sanctions, conflict, nuclear.

    Reads actual Crucix series from raw_series (crucix.* prefix) and
    builds a composite risk score from:
      - GSCPI (Global Supply Chain Pressure Index)
      - Sanctions entity counts (OpenSanctions)
      - ACLED conflict events / fatalities
      - Nuclear monitoring anomalies (Safecast)
      - Military aircraft in hotspots (ADS-B / OpenSky)
      - Weather disruptions (NOAA severe alerts)
      - Treasury debt movements

    Score interpretation:
      Positive = risk-off signal (bearish for equities)
      Negative = calm / de-escalation (bullish)
    """
    try:
        with engine.connect() as conn:
            def _latest(sid: str) -> float | None:
                r = conn.execute(text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                    "AND obs_date >= CURRENT_DATE - 3 "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"sid": sid}).fetchone()
                return float(r[0]) if r and r[0] is not None else None

            # Supply chain pressure (GSCPI: 0 = normal, >1 = stressed)
            gscpi = _latest("crucix.gscpi.value")
            gscpi_mom = _latest("crucix.gscpi.mom_change")

            # Sanctions pressure
            sanctions_total = _latest("crucix.opensanctions.total_entities")
            russia_sanctions = _latest("crucix.opensanctions.russia_results")
            iran_sanctions = _latest("crucix.opensanctions.iran_results")

            # Conflict intensity
            acled_events = _latest("crucix.acled.event_count")
            acled_fatalities = _latest("crucix.acled.fatalities")

            # Nuclear monitoring
            nuclear_anomalies = sum(
                1 for site in (
                    "crucix.safecast.zaporizhzhia_anomaly",
                    "crucix.safecast.yongbyon_anomaly",
                    "crucix.safecast.bushehr_anomaly",
                    "crucix.safecast.dimona_anomaly",
                    "crucix.safecast.fukushima_anomaly",
                    "crucix.safecast.chernobyl_anomaly",
                )
                if (_latest(site) or 0) > 0
            )

            # Military aircraft in hotspots
            hotspot_aircraft = _latest("crucix.opensky.total_hotspot_aircraft")
            taiwan_aircraft = _latest("crucix.opensky.taiwan_aircraft")

            # Severe weather
            severe_weather = _latest("crucix.noaa.severe_alerts_total")

            # Treasury debt change (big daily swings = fiscal stress)
            debt_change = _latest("crucix.treasury.debt_daily_change_bn")

            # Defense spending surges
            defense_contracts = _latest("crucix.usaspending.defense_contract_count")

            # Telegram urgency
            telegram_urgent = _latest("crucix.telegram.urgent_post_count")

        # Count how many signals we have data for
        signals_present = sum(1 for v in (
            gscpi, acled_events, sanctions_total, hotspot_aircraft,
            severe_weather, debt_change, nuclear_anomalies,
        ) if v is not None)

        if signals_present < 2:
            return _verdict(
                "crucix_osint", "Crucix OSINT Composite",
                0, 0, "insufficient data", "",
                "Fewer than 2 Crucix signals available — cannot score.",
                status="no_data", historical_accuracy=accuracy,
            )

        # Build composite score
        score = 0.0
        reasons = []

        # GSCPI: >0.5 stressed, >1.0 crisis, <0 easing
        if gscpi is not None:
            if gscpi > 1.0:
                score -= 25
                reasons.append(f"GSCPI {gscpi:.2f} (crisis-level supply pressure)")
            elif gscpi > 0.5:
                score -= 15
                reasons.append(f"GSCPI {gscpi:.2f} (elevated supply pressure)")
            elif gscpi < -0.3:
                score += 10
                reasons.append(f"GSCPI {gscpi:.2f} (supply chains easing)")

        # GSCPI momentum
        if gscpi_mom is not None and abs(gscpi_mom) > 0.1:
            mom_impact = -10 if gscpi_mom > 0 else 8
            score += mom_impact
            reasons.append(f"GSCPI MoM {gscpi_mom:+.2f} ({'worsening' if gscpi_mom > 0 else 'improving'})")

        # Sanctions pressure (more entities = more geopolitical tension)
        if sanctions_total is not None and sanctions_total > 0:
            if russia_sanctions and russia_sanctions > 500:
                score -= 10
                reasons.append(f"Russia sanctions elevated ({russia_sanctions:.0f} entities)")
            if iran_sanctions and iran_sanctions > 200:
                score -= 8
                reasons.append(f"Iran sanctions elevated ({iran_sanctions:.0f} entities)")

        # Conflict events
        if acled_events is not None and acled_events > 50:
            score -= min(20, acled_events / 10)
            reasons.append(f"ACLED: {acled_events:.0f} conflict events")
        if acled_fatalities is not None and acled_fatalities > 100:
            score -= min(15, acled_fatalities / 50)
            reasons.append(f"ACLED: {acled_fatalities:.0f} fatalities")

        # Nuclear anomalies (binary — any anomaly is significant)
        if nuclear_anomalies > 0:
            score -= 15 * nuclear_anomalies
            reasons.append(f"{nuclear_anomalies} nuclear monitoring anomalies")

        # Military aircraft concentration
        if taiwan_aircraft is not None and taiwan_aircraft > 20:
            score -= 15
            reasons.append(f"Taiwan strait: {taiwan_aircraft:.0f} military aircraft")
        if hotspot_aircraft is not None and hotspot_aircraft > 100:
            score -= 10
            reasons.append(f"{hotspot_aircraft:.0f} aircraft across hotspots")

        # Weather disruptions
        if severe_weather is not None and severe_weather > 10:
            score -= min(10, severe_weather / 5)
            reasons.append(f"{severe_weather:.0f} NOAA severe alerts")

        # Treasury debt shock
        if debt_change is not None and abs(debt_change) > 50:
            score -= 8 if debt_change > 0 else -5
            reasons.append(f"Treasury debt daily Δ ${debt_change:+.0f}B")

        # Defense spending surge
        if defense_contracts is not None and defense_contracts > 50:
            score -= 8
            reasons.append(f"{defense_contracts:.0f} defense contracts (elevated)")

        # Telegram urgency
        if telegram_urgent is not None and telegram_urgent > 5:
            score -= min(10, telegram_urgent * 2)
            reasons.append(f"{telegram_urgent:.0f} urgent Telegram posts")

        score = max(-100, min(100, score))
        confidence = min(85, 25 + signals_present * 8 + accuracy * 15)
        reasoning = "; ".join(reasons) if reasons else "Crucix signals within normal ranges."

        return _verdict(
            "crucix_osint", "Crucix OSINT Composite",
            score, confidence,
            data_point=f"{signals_present} Crucix signals active" + (f", GSCPI={gscpi:.2f}" if gscpi else ""),
            threshold="GSCPI>1.0=-25, nuclear anomaly=-15, conflict>50=-10, Taiwan aircraft>20=-15",
            reasoning=reasoning,
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("Crucix OSINT scorer error: {e}", e=str(exc))
        return _verdict(
            "crucix_osint", "Crucix OSINT Composite",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_gdelt_geopolitical(engine: Engine, accuracy: float) -> dict:
    """GDELT geopolitical tension scoring from country-pair and actor tone data.

    Reads GDELT tension scores (gdelt_tension_*) and actor tone data
    (gdelt_actor_*_tone) from raw_series. These are real numeric values
    from the GDELT 2.0 API, not just event counts.

    Tension scoring:
      - Country-pair tension > 3.0 = severe (bearish)
      - Country-pair tension > 1.5 = elevated (mildly bearish)
      - Actor tone < -4.0 = very negative rhetoric (bearish)

    Key pairs tracked: US-China, US-Russia, US-Iran, China-Taiwan,
    Russia-Ukraine, Israel-Iran, India-China.
    """
    try:
        tension_pairs = {
            "us_china": {"series": "gdelt_tension_us_china", "weight": 1.5, "sector": "SMH,FXI"},
            "us_russia": {"series": "gdelt_tension_us_russia", "weight": 1.2, "sector": "XLE,RSX"},
            "us_iran": {"series": "gdelt_tension_us_iran", "weight": 1.0, "sector": "XLE,USO"},
            "china_taiwan": {"series": "gdelt_tension_china_taiwan", "weight": 1.4, "sector": "SMH,TSM"},
            "russia_ukraine": {"series": "gdelt_tension_russia_ukraine", "weight": 1.1, "sector": "WEAT,XLE"},
            "israel_iran": {"series": "gdelt_tension_israel_iran", "weight": 1.0, "sector": "XLE,USO"},
            "india_china": {"series": "gdelt_tension_india_china", "weight": 0.8, "sector": "INDA"},
        }

        actor_series = {
            "powell": "gdelt_actor_powell_tone",
            "lagarde": "gdelt_actor_lagarde_tone",
            "xi": "gdelt_actor_xi_tone",
            "putin": "gdelt_actor_putin_tone",
            "mbs": "gdelt_actor_mbs_tone",
            "yellen": "gdelt_actor_yellen_tone",
            "ueda": "gdelt_actor_ueda_tone",
        }

        with engine.connect() as conn:
            def _avg_recent(sid: str, days: int = 3) -> float | None:
                """Average of recent values (GDELT can have multiple per day)."""
                r = conn.execute(text(
                    "SELECT AVG(value) FROM raw_series "
                    "WHERE series_id = :sid AND pull_status = 'SUCCESS' "
                    "AND obs_date >= CURRENT_DATE - :d"
                ), {"sid": sid, "d": days}).fetchone()
                return float(r[0]) if r and r[0] is not None else None

            # Collect tension readings
            tension_data: dict[str, float] = {}
            for pair_name, pair_cfg in tension_pairs.items():
                val = _avg_recent(pair_cfg["series"])
                if val is not None:
                    tension_data[pair_name] = val

            # Collect actor tone readings
            actor_data: dict[str, float] = {}
            for actor_name, series_id in actor_series.items():
                val = _avg_recent(series_id)
                if val is not None:
                    actor_data[actor_name] = val

        total_signals = len(tension_data) + len(actor_data)
        if total_signals < 2:
            return _verdict(
                "gdelt_geopolitical", "GDELT Geopolitical Tension",
                0, 0, "insufficient data", "",
                "Fewer than 2 GDELT signals available.",
                status="no_data", historical_accuracy=accuracy,
            )

        score = 0.0
        reasons = []

        # Score tension pairs
        for pair_name, tension_val in tension_data.items():
            pair_cfg = tension_pairs[pair_name]
            weight = pair_cfg["weight"]
            label = pair_name.replace("_", "-").upper()

            if tension_val > 3.0:
                impact = -20 * weight
                score += impact
                reasons.append(f"{label} tension {tension_val:.1f} (SEVERE)")
            elif tension_val > 1.5:
                impact = -10 * weight
                score += impact
                reasons.append(f"{label} tension {tension_val:.1f} (elevated)")
            elif tension_val < -1.0:
                impact = 5 * weight
                score += impact
                reasons.append(f"{label} tension {tension_val:.1f} (de-escalating)")

        # Score actor tone (negative tone from major leaders = risk-off)
        negative_leaders = []
        positive_leaders = []
        for actor_name, tone_val in actor_data.items():
            if tone_val < -4.0:
                score -= 8
                negative_leaders.append(f"{actor_name.title()} ({tone_val:.1f})")
            elif tone_val < -2.0:
                score -= 4
                negative_leaders.append(f"{actor_name.title()} ({tone_val:.1f})")
            elif tone_val > 2.0:
                score += 3
                positive_leaders.append(f"{actor_name.title()} ({tone_val:.1f})")

        if negative_leaders:
            reasons.append(f"Negative tone: {', '.join(negative_leaders)}")
        if positive_leaders:
            reasons.append(f"Positive tone: {', '.join(positive_leaders)}")

        score = max(-100, min(100, score))
        confidence = min(85, 20 + total_signals * 6 + accuracy * 15)
        reasoning = "; ".join(reasons) if reasons else "GDELT geopolitical signals within normal ranges."

        # Build data point summary
        top_tension = max(tension_data.items(), key=lambda x: abs(x[1])) if tension_data else None
        dp = f"{len(tension_data)} pairs, {len(actor_data)} actors tracked"
        if top_tension:
            dp += f", hottest: {top_tension[0].replace('_','-').upper()}={top_tension[1]:.1f}"

        return _verdict(
            "gdelt_geopolitical", "GDELT Geopolitical Tension",
            score, confidence,
            data_point=dp,
            threshold="pair>3.0=severe(-20×w), pair>1.5=elevated(-10×w), actor_tone<-4=-8",
            reasoning=reasoning,
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("GDELT geopolitical scorer error: {e}", e=str(exc))
        return _verdict(
            "gdelt_geopolitical", "GDELT Geopolitical Tension",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


_MODEL_SCORERS = [
    _score_fed_liquidity,
    _score_dealer_gamma,
    _score_vanna_charm,
    _score_congressional,
    _score_insider_cluster,
    _score_supply_chain,
    _score_capital_flows,
    _score_timesfm_consensus,
    _score_trust_convergence,
    _score_regime_changepoints,
    _score_news_sentiment,
    _score_geopolitical_risk,
    _score_social_sentiment,
    _score_crypto_risk,
    _score_cftc_positioning,
    _score_fed_hawkishness,
    _score_valuation_compression,
    _score_fear_greed,
    _score_retail_sentiment,
    _score_gdp_nowcast,
    _score_crucix_osint,
    _score_gdelt_geopolitical,
]


def _get_regime_context(engine: Engine) -> dict[str, Any]:
    """Load current regime state from decision_journal.

    Returns regime name, confidence, and model weight adjustments
    based on which signals matter more in each regime.
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(
                non_provisional_journal_select(
                    "inferred_state", "state_confidence", "counterfactual",
                )
                .order_by(DECISION_JOURNAL.c.decision_timestamp.desc())
                .limit(1)
            ).fetchone()

        if not row or not row[0]:
            return {"regime": "UNKNOWN", "confidence": 0, "adjustments": {}}

        regime = str(row[0]).upper()
        conf = float(row[1]) if row[1] else 0

        # Parse stress index
        stress = None
        cf = row[2] or ""
        if "S=" in cf:
            try:
                stress = float(cf.split("S=")[1].split(",")[0])
            except (ValueError, IndexError):
                pass

        # Regime-specific weight adjustments (multiply confidence)
        # In volatile regimes: options/gamma signals matter more
        # In trending regimes: flows/momentum matter more
        # In crisis: everything defensive matters more
        adjustments: dict[str, float] = {}
        if regime in ("CRISIS", "STRESS", "RISK_OFF"):
            adjustments = {
                "dealer_gamma": 1.4, "vanna_charm": 1.4,
                "capital_flows": 1.2, "trust_convergence": 1.3,
                "regime_changepoints": 1.3,
                "supply_chain": 0.7,
                "crucix_osint": 1.5, "gdelt_geopolitical": 1.4,
            }
        elif regime == "FRAGILE":
            # scripts/auto_regime.py's elevated-stress state (stress index
            # above 0.6 or rising fast; posture DEFENSIVE): the CRISIS tilts
            # at half strength.
            adjustments = {
                "dealer_gamma": 1.2, "vanna_charm": 1.2,
                "capital_flows": 1.1, "trust_convergence": 1.15,
                "regime_changepoints": 1.15,
                "supply_chain": 0.85,
                "crucix_osint": 1.25, "gdelt_geopolitical": 1.2,
            }
        elif regime in ("EXPANSION", "RISK_ON", "GROWTH"):
            adjustments = {
                "capital_flows": 1.3, "insider_cluster": 1.2,
                "fed_liquidity": 1.2, "supply_chain": 1.1,
                "regime_changepoints": 0.8,
                "vanna_charm": 0.8,
                "crucix_osint": 0.8, "gdelt_geopolitical": 0.8,
            }
        elif regime in ("MEAN_REVERSION", "CONSOLIDATION", "NEUTRAL"):
            adjustments = {
                "vanna_charm": 1.3, "insider_cluster": 1.2,
                "dealer_gamma": 1.1,
            }

        return {
            "regime": regime,
            "confidence": conf,
            "stress_index": stress,
            "adjustments": adjustments,
        }

    except Exception as exc:
        log.debug("Regime context load failed: {e}", e=str(exc))
        return {"regime": "UNKNOWN", "confidence": 0, "adjustments": {}}


_thesis_result_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_THESIS_RESULT_CACHE_TTL = 300.0  # 5 minutes — thesis doesn't change faster than this


def score_thesis(engine: Engine, force_refresh: bool = False) -> dict[str, Any]:
    """Run all models and produce a unified, decomposable thesis score.

    Returns a dict that any human can read and verify:
    - Every model shows its score, confidence, data point, and reasoning
    - The final score is a confidence-weighted average (the math adds up)
    - Bull/bear percentages show exactly how much goes each way
    - Current regime context influences model weights

    Results are cached for 5 minutes to avoid hammering the DB.
    """
    now = _time.time()
    if (
        not force_refresh
        and _thesis_result_cache["data"] is not None
        and (now - _thesis_result_cache["ts"]) < _THESIS_RESULT_CACHE_TTL
    ):
        return _thesis_result_cache["data"]

    accuracies = _load_model_accuracies(engine)
    regime_ctx = _get_regime_context(engine)

    verdicts: list[dict] = []
    for scorer_fn in _MODEL_SCORERS:
        key = scorer_fn.__name__.replace("_score_", "")
        acc = accuracies.get(key, DEFAULT_ACCURACY)
        verdict = scorer_fn(engine, acc)
        verdicts.append(verdict)

    # ── Apply regime adjustments ───────────────────────────────────────
    adjustments = regime_ctx.get("adjustments", {})
    for v in verdicts:
        adj = adjustments.get(v["key"], 1.0)
        if adj != 1.0 and v["confidence"] > 0:
            v["confidence"] = round(min(95, v["confidence"] * adj), 1)
            v["regime_adjusted"] = True
        else:
            v["regime_adjusted"] = False

    # ── Weighted average ────────────────────────────────────────────────
    # Each model votes with: score × confidence.
    # Final score = sum(score_i × conf_i) / sum(conf_i)
    weighted_sum = 0.0
    conf_sum = 0.0
    bull_weight = 0.0
    bear_weight = 0.0

    for v in verdicts:
        conf = v["confidence"]
        if conf <= 0 or v["status"] in ("broken", "no_data", "stale"):
            continue
        weighted_sum += v["score"] * conf
        conf_sum += conf
        if v["score"] > 5:
            bull_weight += conf
        elif v["score"] < -5:
            bear_weight += conf

    if conf_sum > 0:
        final_score = weighted_sum / conf_sum
        bull_pct = round(bull_weight / conf_sum * 100, 1)
        bear_pct = round(bear_weight / conf_sum * 100, 1)
    else:
        final_score = 0
        bull_pct = 0
        bear_pct = 0

    # Assign weight_in_final to each verdict
    for v in verdicts:
        if conf_sum > 0 and v["confidence"] > 0 and v["status"] not in ("broken", "no_data", "stale"):
            v["weight_in_final"] = round(v["confidence"] / conf_sum, 3)
        else:
            v["weight_in_final"] = 0.0

    # Direction from score
    if final_score > 10:
        direction = "BULLISH"
    elif final_score < -10:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    conviction = round(min(100, abs(final_score)))
    active_models = sum(1 for v in verdicts if v["status"] == "active")
    broken_models = sum(1 for v in verdicts if v["status"] in ("broken", "no_data"))

    result = {
        "score": round(final_score, 1),
        "direction": direction,
        "conviction": conviction,
        "bull_pct": bull_pct,
        "bear_pct": bear_pct,
        "neutral_pct": round(100 - bull_pct - bear_pct, 1),
        "active_models": active_models,
        "broken_models": broken_models,
        "total_models": len(verdicts),
        "regime": regime_ctx.get("regime", "UNKNOWN"),
        "regime_confidence": regime_ctx.get("confidence", 0),
        "stress_index": regime_ctx.get("stress_index"),
        "evaluation_window": f"{EVALUATION_WINDOW_DAYS}d",
        "scoring_method": "confidence-weighted average of model scores (-100 to +100)",
        "models": verdicts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    # Cache result
    _thesis_result_cache["data"] = result
    _thesis_result_cache["ts"] = _time.time()

    return result


# ══════════════════════════════════════════════════════════════════════════
# SNAPSHOT (persist for accuracy tracking)
# ══════════════════════════════════════════════════════════════════════════

def snapshot_thesis(engine: Engine, thesis: dict[str, Any]) -> int | None:
    """Persist a thesis score to thesis_snapshots for future accuracy evaluation.

    Returns the snapshot ID, or None on failure.
    """
    try:
        from intelligence.thesis_tracker import _ensure_tables, _snapshot_conviction_fraction
        # This scorer's declared output is percent; normalize before any effect.
        if thesis.get("conviction_unit", "percent") != "percent":
            raise ValueError("scored thesis conviction_unit must be percent")
        conviction = _snapshot_conviction_fraction({
            "conviction": thesis["conviction"], "conviction_unit": "percent"})
        if conviction is None:
            return None
        schema = _ensure_tables(engine)
        model_states = {}
        for m in thesis.get("models", []):
            model_states[m["key"]] = {
                "direction": m["direction"],
                "score": m["score"],
                "confidence": m["confidence"],
                "data_point": m["data_point"],
                "reasoning": m["reasoning"],
                "status": m["status"],
            }

        top_risk_limit = globals().get("TOP_RISK_FACTORS_LIMIT", 3)
        active_models = [m for m in thesis.get("models", []) if m["status"] == "active"]
        key_drivers = [
            {"key": m["key"], "name": m["name"], "detail": m["reasoning"]}
            for m in sorted(active_models, key=lambda x: -abs(x["score"]))[:top_risk_limit]
        ]
        thesis_dir = thesis["direction"]
        if thesis_dir == "BULLISH":
            risk_candidates = [m for m in active_models if m["direction"] == "bearish"]
        elif thesis_dir == "BEARISH":
            risk_candidates = [m for m in active_models if m["direction"] == "bullish"]
        else:
            risk_candidates = [m for m in active_models if m["direction"] != "neutral"]
        risk_factors = [
            {"key": m["key"], "name": m["name"], "detail": m["reasoning"]}
            for m in sorted(
                risk_candidates, key=lambda x: (-abs(x["score"]), x["score"])
            )[:top_risk_limit]
        ]

        with engine.begin() as conn:
            from intelligence.thesis_schema import require_thesis_connection
            require_thesis_connection(conn, expected_schema=schema)
            row = conn.execute(text(
                "INSERT INTO thesis_snapshots "
                "(overall_direction, conviction, conviction_unit, key_drivers, risk_factors, "
                "model_states, narrative) "
                "VALUES (:dir, :conv, 'fraction', :kd, :rf, :ms, :narr) "
                "RETURNING id"
            ), {
                "dir": thesis["direction"].lower(),
                "conv": conviction,
                "kd": json.dumps(key_drivers),
                "rf": json.dumps(risk_factors),
                "ms": json.dumps(model_states),
                "narr": _build_narrative(thesis),
            }).fetchone()

        snap_id = row[0] if row else None
        log.info("Thesis snapshot saved: id={id}, dir={d}, conv={c}",
                 id=snap_id, d=thesis["direction"], c=thesis["conviction"])
        return snap_id

    except Exception as exc:
        log.warning("Failed to snapshot thesis: {e}", e=str(exc))
        return None


def _build_narrative(thesis: dict) -> str:
    """Build a plain-English narrative from the scored thesis.

    Output must be understandable by a non-expert.  Lead with a simple
    verdict, explain *why* in conversational language, then append raw
    numbers in a compact detail block.
    """
    direction = thesis["direction"]
    conviction = thesis["conviction"]
    score = thesis["score"]
    bull = thesis["bull_pct"]
    bear = thesis["bear_pct"]

    # ── Plain-English verdict ──────────────────────────────────────────
    if direction == "BULLISH" and conviction >= 40:
        verdict = "Markets look positive — most signals point up."
    elif direction == "BULLISH":
        verdict = "Slight lean toward up, but not much agreement between models."
    elif direction == "BEARISH" and conviction >= 40:
        verdict = "Warning signs are flashing — most signals lean negative."
    elif direction == "BEARISH":
        verdict = "Slight lean toward down, but the picture is mixed."
    else:
        if conviction <= 10:
            verdict = "Markets look flat — nobody's confident either way."
        else:
            verdict = "Mixed signals — no clear direction right now."

    parts = [verdict]

    # ── Why (top 3 drivers in plain language) ──────────────────────────
    active = [m for m in thesis.get("models", []) if m["status"] == "active"]
    top = sorted(active, key=lambda x: -abs(x["score"]))[:3]
    if top:
        parts.append("Here's why:")
        for m in top:
            plain = _simplify_driver(m["name"], m["reasoning"], m["score"])
            parts.append(f"  • {plain}")

    # ── Compact raw data (for power users / LLM context) ───────────────
    parts.append(
        f"\n[Details: {direction}, {conviction}% conviction, score {score:+.1f}. "
        f"Bull {bull}% / Bear {bear}%. "
        f"Window: {thesis['evaluation_window']}. "
        f"{thesis['active_models']}/{thesis['total_models']} models active.]"
    )

    return "\n".join(parts)


# Maps jargon-heavy model names to plain descriptions
_MODEL_PLAIN_NAMES: dict[str, str] = {
    "timesfm": "AI Price Forecasts",
    "timesfm_consensus": "AI Price Forecasts",
    "fed_net_liquidity": "Fed Money Supply",
    "fed_liquidity": "Fed Money Supply",
    "insider_clusters": "Corporate Insider Trading",
    "insider_cluster": "Corporate Insider Trading",
    "options_flow": "Options Market Bets",
    "dealer_gamma": _DEALER_GAMMA_NAME,
    "vanna_charm": _VANNA_CHARM_NAME,
    "gex": "Dealer Positioning",
    "news_sentiment": "News Mood",
    "social_sentiment": "Social Media Mood",
    "macro_regime": "Economic Conditions",
    "cross_reference": "Data Cross-Check",
    "trust_scorer": "Source Reliability",
    "trust_convergence": "Source Agreement",
    "regime_changepoints": "Regime Shifts",
    "supply_chain": "Supply Chain Health",
    "capital_flows": "Money Flows",
    "congressional": "Congressional Trading",
    "geopolitical_risk": "Geopolitical Risk",
    "crypto_risk": "Crypto Risk Barometer",
    "cftc_positioning": "Futures Positioning (COT)",
    "fed_hawkishness": "Fed Tone & FOMC",
    "valuation_compression": "Valuation Momentum",
    "fear_greed": "Fear & Greed Index",
    "retail_sentiment": "Retail Investor Mood",
    "gdp_nowcast": "GDP Growth Estimate",
}


def _simplify_driver(name: str, reasoning: str, score: float) -> str:
    """Turn a model driver into plain English."""
    plain_name = _MODEL_PLAIN_NAMES.get(
        name.lower().replace(" ", "_").replace("-", "_"),
        name.replace("_", " ").title(),
    )
    direction = "positive" if score > 0 else "negative"

    # Simplify common jargon patterns in reasoning text
    simple = reasoning
    for old, new in [
        ("Net liquidity rose", "The Fed pumped more money into the system —"),
        ("Net liquidity fell", "The Fed pulled money out of the system —"),
        ("Expanding liquidity supports risk assets", "that usually helps stocks go up"),
        ("Contracting liquidity pressures risk assets", "that usually pushes stocks down"),
        ("buy clusters", "groups of insider buying"),
        ("sell clusters", "groups of insider selling"),
        ("Net insider selling — insiders reducing exposure",
         "insiders are selling more than buying — they're cautious"),
        ("Net insider buying", "insiders are buying — they see opportunity"),
        ("Consensus: MIXED", "no clear agreement"),
        ("Consensus: BULLISH", "leaning positive"),
        ("Consensus: BEARISH", "leaning negative"),
        ("expected move", "predicted change"),
        ("forecast UP", "predict prices will rise"),
        ("forecast DOWN", "predict prices will fall"),
        ("This is the only FORWARD-LOOKING model in the scorer.", ""),
        ("Equity signals avg", "Average stock signal:"),
    ]:
        simple = simple.replace(old, new)

    return f"{plain_name} ({direction}): {simple}"
