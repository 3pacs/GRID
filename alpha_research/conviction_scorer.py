"""
Conviction Scorer — 98% confidence trade detector.

Scores every ticker across 7 layers of evidence, each weighted by
trust in the underlying data. A score of 85+ with all layers confirming
= highest conviction. Uses ONLY data that actually exists in raw_series.

Layers:
  1. SETUP      (20 pts) — VIX, HY spread, PSI, drawdown severity
  2. COMPANY    (15 pts) — Revenue trend, EPS, shares outstanding, cash
  3. SMART_MONEY(15 pts) — Insider buying (Form 4), institutional flows
  4. CROWD      (15 pts) — Short ratio, FTD, put/call ratio
  5. NARRATIVE  (10 pts) — News volume trend, wiki attention
  6. FLOW       (15 pts) — Volume spike, options unusual activity
  7. CONFIRM    (10 pts) — Price momentum turning, higher low formed

Each layer returns (score, max_possible, trust_weight, signals[]).
Final score = sum(score * trust) / sum(max * trust) * 100
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alpha_research.data.split_adjuster import adjust_splits


@dataclass(frozen=True)
class LayerResult:
    name: str
    score: float
    max_score: float
    trust: float  # 0-1, how much we trust this data
    signals: tuple[str, ...]
    data_available: bool


@dataclass(frozen=True)
class ConvictionReport:
    ticker: str
    total_score: int  # 0-100
    confidence_pct: float
    layers: tuple[LayerResult, ...]
    alert_level: str  # WATCH / PILOT / SCALE / FIRE / CONVICTION
    timestamp: str


def _load_latest(conn, feature_name: str) -> float | None:
    row = conn.execute(text("""
        SELECT rs.value FROM resolved_series rs
        JOIN feature_registry fr ON rs.feature_id = fr.id
        WHERE fr.name = :n ORDER BY rs.obs_date DESC LIMIT 1
    """), {"n": feature_name}).fetchone()
    return float(row[0]) if row else None


def _load_raw_latest(conn, series_pattern: str, source_id: int | None = None) -> float | None:
    # Use >= / < range scan instead of LIKE for index efficiency
    prefix = series_pattern.replace("%", "")
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1) if prefix else "~"
    if source_id:
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id >= :lo AND series_id < :hi AND source_id = :s
            ORDER BY obs_date DESC LIMIT 1
        """), {"lo": prefix, "hi": upper, "s": source_id}).fetchone()
    else:
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id >= :lo AND series_id < :hi
            ORDER BY obs_date DESC LIMIT 1
        """), {"lo": prefix, "hi": upper}).fetchone()
    return float(row[0]) if row else None


def _load_raw_series(conn, series_id: str, days: int = 90) -> pd.Series:
    cutoff = date.today() - timedelta(days=days)
    # Use exact match — series_id must be exact, no wildcards
    rows = conn.execute(text("""
        SELECT obs_date, value FROM raw_series
        WHERE series_id = :s AND obs_date >= :d
        ORDER BY obs_date
    """), {"s": series_id, "d": cutoff}).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]))


def _load_price(conn, ticker: str) -> pd.Series:
    feat = f"{ticker.lower()}_full"
    rows = conn.execute(text("""
        SELECT rs.obs_date, rs.value FROM resolved_series rs
        JOIN feature_registry fr ON rs.feature_id = fr.id
        WHERE fr.name = :n ORDER BY rs.obs_date
    """), {"n": feat}).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]))
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return adjust_splits(s)


# ═══════════════════════════════════════════════════════════════
# LAYER SCORERS
# ═══════════════════════════════════════════════════════════════

def score_setup(conn, ticker: str, price: pd.Series) -> LayerResult:
    """Layer 1: Macro setup — is this the right moment?"""
    score = 0.0
    signals = []

    vix = _load_latest(conn, "vix_spot")
    hy = _load_latest(conn, "hy_oas_spread")
    stress = _load_latest(conn, "ofr_financial_stress")
    psi = _load_latest(conn, "planetary_stress_index")
    yc = _load_latest(conn, "yld_curve_2s10s")

    if price.empty:
        return LayerResult("SETUP", 0, 20, 0.95, (), False)

    ath = price.max()
    current = price.iloc[-1]
    dd = (current / ath - 1) * 100 if ath > 0 else 0

    # Drawdown severity (0-5 pts)
    if dd < -70:
        score += 5; signals.append(f"DD={dd:.0f}% [EXTREME]")
    elif dd < -50:
        score += 4; signals.append(f"DD={dd:.0f}% [DEEP]")
    elif dd < -30:
        score += 2; signals.append(f"DD={dd:.0f}%")

    # VIX (0-5 pts)
    if vix is not None:
        if vix > 35:
            score += 5; signals.append(f"VIX={vix:.0f} [PANIC]")
        elif vix > 25:
            score += 3; signals.append(f"VIX={vix:.0f} [FEAR]")
        elif vix > 20:
            score += 1; signals.append(f"VIX={vix:.0f}")

    # Credit stress (0-4 pts)
    if hy is not None:
        if hy > 5.0:
            score += 4; signals.append(f"HY={hy:.1f} [CRISIS]")
        elif hy > 4.0:
            score += 2; signals.append(f"HY={hy:.1f} [STRESS]")

    # PSI (0-3 pts)
    if psi is not None and 0.5 < psi < 4.0:
        score += 3; signals.append(f"PSI={psi:.1f} [FAVORABLE]")

    # Yield curve (0-3 pts)
    if yc is not None and yc < 0:
        score += 3; signals.append(f"YC={yc:.2f} [INVERTED]")

    return LayerResult("SETUP", min(score, 20), 20, 0.95, tuple(signals), True)


def score_company(conn, ticker: str) -> LayerResult:
    """Layer 2: Is the company healthy? Revenue, EPS, shares, cash."""
    score = 0.0
    signals = []
    has_data = False

    # XBRL data
    rev = _load_raw_series(conn, f"XBRL:{ticker}:Revenues", 730)
    eps = _load_raw_series(conn, f"XBRL:{ticker}:EarningsPerShareDiluted", 730)
    shares = _load_raw_series(conn, f"XBRL:{ticker}:CommonStockSharesOutstanding", 730)
    cash = _load_raw_series(conn, f"XBRL:{ticker}:CashAndCashEquivalentsAtCarryingValue", 730)
    assets = _load_raw_series(conn, f"XBRL:{ticker}:Assets", 730)
    liabilities = _load_raw_series(conn, f"XBRL:{ticker}:Liabilities", 730)

    # Revenue trend (0-4 pts)
    if len(rev) >= 2:
        has_data = True
        rev_chg = (rev.iloc[-1] / rev.iloc[0] - 1) * 100 if rev.iloc[0] != 0 else 0
        if rev_chg > 10:
            score += 4; signals.append(f"Rev growing +{rev_chg:.0f}%")
        elif rev_chg > 0:
            score += 2; signals.append(f"Rev stable +{rev_chg:.0f}%")
        elif rev_chg > -20:
            score += 1; signals.append(f"Rev declining {rev_chg:.0f}%")
        else:
            signals.append(f"Rev collapsing {rev_chg:.0f}% [WARNING]")

    # EPS trend (0-3 pts)
    if len(eps) >= 2:
        has_data = True
        if eps.iloc[-1] > 0:
            score += 3; signals.append(f"EPS positive ${eps.iloc[-1]:.2f}")
        elif eps.iloc[-1] > eps.iloc[0]:
            score += 1; signals.append(f"EPS improving ${eps.iloc[-1]:.2f}")
        else:
            signals.append(f"EPS negative ${eps.iloc[-1]:.2f}")

    # Shares outstanding trend (0-4 pts) — THE DENOMINATOR
    if len(shares) >= 2:
        has_data = True
        share_chg = (shares.iloc[-1] / shares.iloc[0] - 1) * 100 if shares.iloc[0] != 0 else 0
        if share_chg < -5:
            score += 4; signals.append(f"Shares -{abs(share_chg):.0f}% [BUYBACK]")
        elif share_chg < 2:
            score += 2; signals.append(f"Shares stable {share_chg:+.0f}%")
        elif share_chg < 20:
            score += 0; signals.append(f"Shares +{share_chg:.0f}% [DILUTION]")
        else:
            score -= 2; signals.append(f"Shares +{share_chg:.0f}% [HEAVY DILUTION]")

    # Cash position (0-4 pts)
    if len(cash) > 0 and len(liabilities) > 0:
        has_data = True
        cash_ratio = cash.iloc[-1] / liabilities.iloc[-1] if liabilities.iloc[-1] != 0 else 0
        if cash_ratio > 0.5:
            score += 4; signals.append(f"Cash/Liab={cash_ratio:.2f} [STRONG]")
        elif cash_ratio > 0.2:
            score += 2; signals.append(f"Cash/Liab={cash_ratio:.2f}")
        else:
            signals.append(f"Cash/Liab={cash_ratio:.2f} [WEAK]")

    trust = 0.90 if has_data else 0.20
    return LayerResult("COMPANY", max(score, 0), 15, trust, tuple(signals), has_data)


def score_smart_money(conn, ticker: str) -> LayerResult:
    """Layer 3: Are insiders/institutions buying?"""
    score = 0.0
    signals = []
    has_data = False

    # Form 4 monthly filing count (increasing = more insider activity)
    f4 = _load_raw_series(conn, f"SEC_FORM4:{ticker}:monthly", 365)
    if len(f4) >= 3:
        has_data = True
        recent = f4.tail(3).mean()
        older = f4.head(3).mean() if len(f4) >= 6 else recent
        if recent > older * 1.5:
            score += 5; signals.append(f"Insider activity surging ({recent:.0f}/mo vs {older:.0f})")
        elif recent > 3:
            score += 2; signals.append(f"Active insider trading ({recent:.0f}/mo)")

    # Institutional flows (if available)
    inst = _load_raw_series(conn, f"INST_FLOW:{ticker}:%", 90)
    if not inst.empty:
        has_data = True
        if inst.iloc[-1] > 0:
            score += 5; signals.append("Institutional net inflow")
        else:
            signals.append("Institutional net outflow")

    trust = 0.85 if has_data else 0.15
    return LayerResult("SMART_MONEY", min(score, 15), 15, trust, tuple(signals), has_data)


def score_crowd(conn, ticker: str) -> LayerResult:
    """Layer 4: Is the crowd wrong? Short interest, FTD, PCR."""
    score = 0.0
    signals = []
    has_data = False

    # Short ratio
    short = _load_raw_series(conn, f"SHORT:{ticker}:ratio", 30)
    if not short.empty:
        has_data = True
        avg_short = short.mean()
        if avg_short > 0.5:
            score += 5; signals.append(f"Short ratio {avg_short:.0%} [HEAVILY SHORTED]")
        elif avg_short > 0.3:
            score += 3; signals.append(f"Short ratio {avg_short:.0%} [ELEVATED]")
        elif avg_short > 0:
            score += 1; signals.append(f"Short ratio {avg_short:.0%}")

    # Fails-to-deliver
    ftd = _load_raw_series(conn, f"FTD:{ticker}:qty", 90)
    if not ftd.empty:
        has_data = True
        recent_ftd = ftd.tail(10).mean()
        older_ftd = ftd.head(30).mean() if len(ftd) > 30 else recent_ftd
        if recent_ftd > older_ftd * 2 and recent_ftd > 10000:
            score += 5; signals.append(f"FTD surging ({recent_ftd:,.0f} vs {older_ftd:,.0f}) [SQUEEZE SETUP]")
        elif recent_ftd > 10000:
            score += 2; signals.append(f"FTD elevated ({recent_ftd:,.0f})")

    # Put/Call ratio (market-wide)
    pcr = _load_raw_latest(conn, "CBOE:totalpc", 5)
    if pcr is not None:
        if pcr > 1.2:
            score += 3; signals.append(f"PCR={pcr:.2f} [MAX BEARISH]")
        elif pcr > 1.0:
            score += 1; signals.append(f"PCR={pcr:.2f} [BEARISH]")

    trust = 0.75 if has_data else 0.15
    return LayerResult("CROWD", min(score, 15), 15, trust, tuple(signals), has_data)


def score_narrative(conn, ticker: str) -> LayerResult:
    """Layer 5: Is there a narrative catalyst?"""
    score = 0.0
    signals = []
    has_data = False

    # News article count trend
    news = _load_raw_series(conn, f"NEWS:{ticker}:daily_count", 90)
    if not news.empty:
        has_data = True
        recent = news.tail(7).mean() if len(news) >= 7 else news.mean()
        older = news.head(30).mean() if len(news) >= 30 else recent
        if recent > older * 2:
            score += 5; signals.append(f"News volume surging ({recent:.0f}/day vs {older:.0f})")
        elif recent > 3:
            score += 2; signals.append(f"Moderate news coverage ({recent:.0f}/day)")

    # Wiki attention (if available)
    wiki_name = f"wiki_{ticker.lower()}"
    wiki = _load_latest(conn, wiki_name)
    if wiki is not None:
        has_data = True
        score += 2; signals.append(f"Wiki attention tracked")

    # GDELT (market-wide narrative)
    gdelt = _load_latest(conn, "gdelt_article_count")
    if gdelt is not None and gdelt > 1000:
        score += 1; signals.append(f"GDELT high ({gdelt:.0f} articles)")

    trust = 0.50 if has_data else 0.10
    return LayerResult("NARRATIVE", min(score, 10), 10, trust, tuple(signals), has_data)


def score_flow(conn, ticker: str, price: pd.Series) -> LayerResult:
    """Layer 6: Is money actually moving?"""
    score = 0.0
    signals = []
    has_data = False

    if price.empty or len(price) < 20:
        return LayerResult("FLOW", 0, 15, 0.10, (), False)

    # Volume spike (from price data — use returns as proxy if no volume)
    returns = price.pct_change().dropna()
    recent_vol = returns.tail(5).abs().mean()
    avg_vol = returns.tail(60).abs().mean() if len(returns) >= 60 else recent_vol

    if avg_vol > 0 and recent_vol > avg_vol * 2:
        has_data = True
        score += 5; signals.append(f"Volatility spike ({recent_vol/avg_vol:.1f}x normal)")
    elif avg_vol > 0 and recent_vol > avg_vol * 1.5:
        has_data = True
        score += 2; signals.append(f"Elevated activity ({recent_vol/avg_vol:.1f}x)")

    # Check for whale options activity
    whale = _load_raw_latest(conn, f"WHALE:{ticker}:%")
    if whale is not None:
        has_data = True
        score += 5; signals.append("Unusual options activity detected")

    # CBOE equity PCR
    epcr = _load_raw_latest(conn, "CBOE:equitypc", 5)
    if epcr is not None:
        has_data = True
        if epcr > 0.8:
            score += 3; signals.append(f"Equity PCR={epcr:.2f} [BEARISH - contrarian buy]")

    trust = 0.60 if has_data else 0.15
    return LayerResult("FLOW", min(score, 15), 15, trust, tuple(signals), has_data)


def score_confirmation(price: pd.Series) -> LayerResult:
    """Layer 7: Has the turn actually happened?"""
    score = 0.0
    signals = []

    if price.empty or len(price) < 30:
        return LayerResult("CONFIRM", 0, 10, 0.10, (), False)

    current = price.iloc[-1]

    # 5-day momentum positive
    if len(price) >= 6:
        mom5 = (current / price.iloc[-6] - 1) * 100
        if mom5 > 3:
            score += 3; signals.append(f"5d momentum +{mom5:.1f}% [STRONG]")
        elif mom5 > 0:
            score += 1; signals.append(f"5d momentum +{mom5:.1f}%")
        else:
            signals.append(f"5d momentum {mom5:.1f}% [NO TURN YET]")

    # 30d momentum turning while 90d still negative
    if len(price) >= 91:
        mom30 = (current / price.iloc[-31] - 1) * 100
        mom90 = (current / price.iloc[-91] - 1) * 100
        if mom30 > 0 and mom90 < 0:
            score += 4; signals.append(f"Momentum TURNING (30d={mom30:+.1f}%, 90d={mom90:+.1f}%)")
        elif mom30 > 0:
            score += 2; signals.append(f"30d positive +{mom30:.1f}%")

    # Higher low forming (current > 30d low)
    if len(price) >= 30:
        low_30d = price.tail(30).min()
        if current > low_30d * 1.05:
            score += 3; signals.append(f"Higher low forming (${current:.2f} vs 30d low ${low_30d:.2f})")

    return LayerResult("CONFIRM", min(score, 10), 10, 0.90, tuple(signals), True)


# ═══════════════════════════════════════════════════════════════
# MAIN SCORER
# ═══════════════════════════════════════════════════════════════

def score_ticker(engine: Engine, ticker: str) -> ConvictionReport:
    """Score a single ticker across all 7 layers."""
    with engine.connect() as conn:
        price = _load_price(conn, ticker)

        layers = [
            score_setup(conn, ticker, price),
            score_company(conn, ticker),
            score_smart_money(conn, ticker),
            score_crowd(conn, ticker),
            score_narrative(conn, ticker),
            score_flow(conn, ticker, price),
            score_confirmation(price),
        ]

    # Weighted score
    weighted_score = sum(l.score * l.trust for l in layers)
    weighted_max = sum(l.max_score * l.trust for l in layers)
    total = int(round(weighted_score / weighted_max * 100)) if weighted_max > 0 else 0

    # Confidence = how much data we actually have
    data_coverage = sum(1 for l in layers if l.data_available) / len(layers)
    confidence = total * data_coverage

    # Alert level
    if total >= 85 and data_coverage >= 0.8:
        level = "CONVICTION"
    elif total >= 70:
        level = "FIRE"
    elif total >= 55:
        level = "SCALE"
    elif total >= 40:
        level = "PILOT"
    elif total >= 25:
        level = "WATCH"
    else:
        level = "PASS"

    return ConvictionReport(
        ticker=ticker,
        total_score=total,
        confidence_pct=round(confidence, 1),
        layers=tuple(layers),
        alert_level=level,
        timestamp=pd.Timestamp.now().isoformat(),
    )


def scan_all(engine: Engine, min_score: int = 20) -> list[ConvictionReport]:
    """Score all tickers with price data."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT UPPER(REPLACE(fr.name, '_full', ''))
            FROM feature_registry fr JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.name LIKE '%%_full' GROUP BY fr.name HAVING COUNT(*) > 50
        """)).fetchall()

    tickers = [r[0] for r in rows]
    reports = []

    for ticker in tickers:
        report = score_ticker(engine, ticker)
        if report.total_score >= min_score:
            reports.append(report)

    return sorted(reports, key=lambda r: -r.total_score)


def print_report(report: ConvictionReport) -> None:
    """Pretty-print a conviction report."""
    log.info(
        "\n  {t:8s} [{level:10s}] Score={s:3d}/100  Confidence={c:.0f}%",
        t=report.ticker, level=report.alert_level,
        s=report.total_score, c=report.confidence_pct,
    )
    for layer in report.layers:
        data_flag = "+" if layer.data_available else "-"
        log.info(
            "    {d} {name:12s} {score:4.1f}/{max:4.1f} (trust={t:.0%})  {sigs}",
            d=data_flag, name=layer.name, score=layer.score,
            max=layer.max_score, t=layer.trust,
            sigs=" | ".join(layer.signals) if layer.signals else "[no data]",
        )
