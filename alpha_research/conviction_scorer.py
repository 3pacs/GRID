"""
Conviction Scorer — 98% confidence trade detector.

Scores every ticker across 7 layers of evidence, each weighted by
trust in the underlying data. A score of 85+ with all layers confirming
= highest conviction. Uses ONLY data that actually exists in raw_series.

Layers:
  1. SETUP      (17 pts) — VIX, HY spread, curve, drawdown severity
  2. COMPANY    (15 pts) — Revenue trend, EPS, shares outstanding, cash
  3. SMART_MONEY(15 pts) — Insider reported P/S (Form 4)
  4. CROWD      (15 pts) — Short ratio, FTD, put/call ratio
  5. NARRATIVE  (10 pts) — News volume trend, wiki attention
  6. FLOW       (15 pts) — Volume spike, options unusual activity
  7. CONFIRM    (10 pts) — Price momentum turning, higher low formed

Each layer returns (score, max_possible, trust_weight, signals[]).
Final score = sum(score * trust) / sum(max * full_trust) * 100, where
full_trust is the layer's trust with its data in hand (LAYER_FULL_TRUST):
missing or thinner data lowers a layer's numerator weight only, never the
denominator, so less evidence can never raise the total.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from types import MappingProxyType

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from alpha_research.data.split_adjuster import adjust_splits
from store.observations import read_window_known_at
from store.pit import retraction_cutoff


@dataclass(frozen=True)
class LayerResult:
    name: str
    score: float  # may be negative (e.g., SMART_MONEY Form 4 net sell, capped at -2)
    max_score: float
    trust: float  # 0-1, how much we trust this data (weights the layer's score)
    signals: tuple[str, ...]
    data_available: bool
    # Weight of max_score in the composite denominator; None means ``trust``.
    # Every layer scorer sets it to the layer's full-data trust
    # (LAYER_FULL_TRUST), so missing or thinner data can only shrink a
    # layer's contribution toward zero and never raises total_score by
    # shrinking the denominator (Codex HOLD on #1353 57514cc5 for
    # SMART_MONEY; the same hole in CROWD / NARRATIVE / FLOW / CONFIRM /
    # SETUP closed 2026-11-23).
    baseline_trust: float | None = None

    @property
    def denominator_trust(self) -> float:
        return self.trust if self.baseline_trust is None else max(self.trust, self.baseline_trust)


@dataclass(frozen=True)
class ConvictionReport:
    ticker: str
    total_score: int  # 0-100
    confidence_pct: float
    layers: tuple[LayerResult, ...]
    alert_level: str  # WATCH / PILOT / SCALE / FIRE / CONVICTION
    timestamp: str
    unavailable_layers: tuple[str, ...] = ()


def _resolve_as_of_date(as_of_date: date | None) -> date:
    return as_of_date or date.today()


def _load_latest(
    conn,
    feature_name: str,
    as_of_date: date | None = None,
) -> float | None:
    as_of = _resolve_as_of_date(as_of_date)
    row = conn.execute(text("""
        SELECT rs.value FROM resolved_series rs
        JOIN feature_registry fr ON rs.feature_id = fr.id
        WHERE fr.name = :n
          AND rs.obs_date <= :as_of
          AND rs.release_date <= :as_of
          AND rs.vintage_date <= :as_of
          AND NOT EXISTS (
              SELECT 1 FROM resolved_series_retractions rr
              WHERE rr.feature_id = rs.feature_id
                AND rr.obs_date = rs.obs_date
                AND rr.vintage_date = rs.vintage_date
                AND rr.retracted_at <= :retraction_cutoff
          )
        ORDER BY rs.obs_date DESC, rs.release_date DESC, rs.vintage_date DESC
        LIMIT 1
    """), {"n": feature_name, "as_of": as_of,
           "retraction_cutoff": retraction_cutoff(as_of)}).fetchone()
    return float(row[0]) if row else None


def _load_raw_latest(
    conn,
    series_pattern: str,
    source_id: int | None = None,
    as_of_date: date | None = None,
) -> float | None:
    as_of = _resolve_as_of_date(as_of_date)
    # Use >= / < range scan instead of LIKE for index efficiency
    prefix = series_pattern.replace("%", "")
    upper = prefix[:-1] + chr(ord(prefix[-1]) + 1) if prefix else "~"
    # raw_series does not carry release_date in live griddb; use pull_timestamp
    # as the conservative availability proxy for PIT reads.
    if source_id is not None:
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id >= :lo AND series_id < :hi
              AND source_id = :s
              AND pull_status = 'SUCCESS'
              AND obs_date <= :as_of
              AND pull_timestamp::date <= :as_of
            ORDER BY obs_date DESC, pull_timestamp DESC
            LIMIT 1
        """), {"lo": prefix, "hi": upper, "s": source_id, "as_of": as_of}).fetchone()
    else:
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id >= :lo AND series_id < :hi
              AND pull_status = 'SUCCESS'
              AND obs_date <= :as_of
              AND pull_timestamp::date <= :as_of
            ORDER BY obs_date DESC, pull_timestamp DESC
            LIMIT 1
        """), {"lo": prefix, "hi": upper, "as_of": as_of}).fetchone()
    return float(row[0]) if row else None


def _load_raw_series(
    conn,
    series_id: str,
    days: int = 90,
    as_of_date: date | None = None,
) -> pd.Series:
    as_of = _resolve_as_of_date(as_of_date)
    cutoff = as_of - timedelta(days=days)
    # Use exact match — series_id must be exact, no wildcards
    rows = conn.execute(text("""
        SELECT obs_date, value FROM raw_series
        WHERE series_id = :s
          AND pull_status = 'SUCCESS'
          AND obs_date >= :d
          AND obs_date <= :as_of
          AND pull_timestamp::date <= :as_of
        ORDER BY obs_date, pull_timestamp
    """), {"s": series_id, "d": cutoff, "as_of": as_of}).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    return pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]))


def _load_price(
    conn,
    ticker: str,
    as_of_date: date | None = None,
) -> pd.Series:
    as_of = _resolve_as_of_date(as_of_date)
    feat = f"{ticker.lower()}_full"
    rows = conn.execute(text("""
        SELECT rs.obs_date, rs.value FROM resolved_series rs
        JOIN feature_registry fr ON rs.feature_id = fr.id
        WHERE fr.name = :n
          AND rs.obs_date <= :as_of
          AND rs.release_date <= :as_of
          AND rs.vintage_date <= :as_of
          AND NOT EXISTS (
              SELECT 1 FROM resolved_series_retractions rr
              WHERE rr.feature_id = rs.feature_id
                AND rr.obs_date = rs.obs_date
                AND rr.vintage_date = rs.vintage_date
                AND rr.retracted_at <= :retraction_cutoff
          )
        ORDER BY rs.obs_date, rs.release_date, rs.vintage_date
    """), {"n": feat, "as_of": as_of,
           "retraction_cutoff": retraction_cutoff(as_of)}).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]))
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return adjust_splits(s)


# ═══════════════════════════════════════════════════════════════
# LAYER SCORERS
# ═══════════════════════════════════════════════════════════════

# Max attainable SETUP points: drawdown 5 + VIX 5 + credit 4 + curve 3. The
# 3-point planetary_stress_index component was removed 2026-09-10 (astrology
# is not a market-scoring input), so the layer's weight is 17, not 20.
SETUP_MAX_SCORE = 17

# Each layer's fixed weight in score_ticker's denominator: its max points and
# its trust with its data in hand. A layer scorer returns its own trust
# (lower when its data is missing or thin), which weights the numerator only;
# the denominator weight is the same for every ticker and date, so a ticker
# with a missing layer never scores above one whose same layer read neutral.
LAYER_MAX_SCORE: Mapping[str, float] = MappingProxyType({
    "SETUP": float(SETUP_MAX_SCORE),
    "COMPANY": 15.0,
    "SMART_MONEY": 15.0,
    "CROWD": 15.0,
    "NARRATIVE": 10.0,
    "FLOW": 15.0,
    "CONFIRM": 10.0,
})
LAYER_FULL_TRUST: Mapping[str, float] = MappingProxyType({
    "SETUP": 0.95,
    "COMPANY": 0.90,
    "SMART_MONEY": 0.85,
    "CROWD": 0.75,
    "NARRATIVE": 0.50,
    "FLOW": 0.60,
    "CONFIRM": 0.90,
})


def _layer_result(
    name: str,
    score: float,
    trust: float,
    signals: Sequence[str],
    data_available: bool,
) -> LayerResult:
    """A layer's result with its fixed composite weight (LAYER_MAX_SCORE, LAYER_FULL_TRUST)."""
    return LayerResult(
        name,
        score,
        LAYER_MAX_SCORE[name],
        trust,
        tuple(signals),
        data_available,
        baseline_trust=LAYER_FULL_TRUST[name],
    )


def score_setup(
    conn,
    ticker: str,
    price: pd.Series,
    as_of_date: date | None = None,
) -> LayerResult:
    """Layer 1: Macro setup — is this the right moment?"""
    score = 0.0
    signals = []

    vix = _load_latest(conn, "vix_spot", as_of_date=as_of_date)
    hy = _load_latest(conn, "hy_oas_spread", as_of_date=as_of_date)
    _load_latest(conn, "ofr_financial_stress", as_of_date=as_of_date)
    # planetary_stress_index was removed from this layer 2026-09-10: a celestial
    # feature must not be an input to a market-scoring endpoint
    # (LEVER-PACKAGE.md §4.1 / §7 T0.5). It stays available to AstroGrid.
    yc = _load_latest(conn, "yld_curve_2s10s", as_of_date=as_of_date)

    if price.empty:
        return _layer_result("SETUP", 0, LAYER_FULL_TRUST["SETUP"], (), False)

    ath = price.max()
    current = price.iloc[-1]
    dd = (current / ath - 1) * 100 if ath > 0 else 0

    # Drawdown severity (0-5 pts)
    if dd < -70:
        score += 5
        signals.append(f"DD={dd:.0f}% [EXTREME]")
    elif dd < -50:
        score += 4
        signals.append(f"DD={dd:.0f}% [DEEP]")
    elif dd < -30:
        score += 2
        signals.append(f"DD={dd:.0f}%")

    # VIX (0-5 pts)
    if vix is not None:
        if vix > 35:
            score += 5
            signals.append(f"VIX={vix:.0f} [PANIC]")
        elif vix > 25:
            score += 3
            signals.append(f"VIX={vix:.0f} [FEAR]")
        elif vix > 20:
            score += 1
            signals.append(f"VIX={vix:.0f}")

    # Credit stress (0-4 pts)
    if hy is not None:
        if hy > 5.0:
            score += 4
            signals.append(f"HY={hy:.1f} [CRISIS]")
        elif hy > 4.0:
            score += 2
            signals.append(f"HY={hy:.1f} [STRESS]")

    # Yield curve (0-3 pts)
    if yc is not None and yc < 0:
        score += 3
        signals.append(f"YC={yc:.2f} [INVERTED]")

    # Max is 17 since the 3-point celestial component was removed.
    return _layer_result("SETUP", min(score, SETUP_MAX_SCORE), LAYER_FULL_TRUST["SETUP"], signals, True)


def score_company(
    conn,
    ticker: str,
    as_of_date: date | None = None,
) -> LayerResult | None:
    """Layer 2: Is the company healthy? Revenue, EPS, shares, cash."""
    # no XBRL:{ticker}:* writer exists in the repo (sec_xbrl_financials writes capital_flows); re-enable when a writer exists
    log.debug(
        "COMPANY layer unavailable for {t} (as_of={a}): no XBRL:* writer exists in raw_series",
        t=ticker,
        a=_resolve_as_of_date(as_of_date),
    )
    return None

    score = 0.0
    signals = []
    has_data = False

    # XBRL data
    rev = _load_raw_series(conn, f"XBRL:{ticker}:Revenues", 730, as_of_date=as_of_date)
    eps = _load_raw_series(
        conn,
        f"XBRL:{ticker}:EarningsPerShareDiluted",
        730,
        as_of_date=as_of_date,
    )
    shares = _load_raw_series(
        conn,
        f"XBRL:{ticker}:CommonStockSharesOutstanding",
        730,
        as_of_date=as_of_date,
    )
    cash = _load_raw_series(
        conn,
        f"XBRL:{ticker}:CashAndCashEquivalentsAtCarryingValue",
        730,
        as_of_date=as_of_date,
    )
    _load_raw_series(conn, f"XBRL:{ticker}:Assets", 730, as_of_date=as_of_date)
    liabilities = _load_raw_series(
        conn,
        f"XBRL:{ticker}:Liabilities",
        730,
        as_of_date=as_of_date,
    )

    # Revenue trend (0-4 pts)
    if len(rev) >= 2:
        has_data = True
        rev_chg = (rev.iloc[-1] / rev.iloc[0] - 1) * 100 if rev.iloc[0] != 0 else 0
        if rev_chg > 10:
            score += 4
            signals.append(f"Rev growing +{rev_chg:.0f}%")
        elif rev_chg > 0:
            score += 2
            signals.append(f"Rev stable +{rev_chg:.0f}%")
        elif rev_chg > -20:
            score += 1
            signals.append(f"Rev declining {rev_chg:.0f}%")
        else:
            signals.append(f"Rev collapsing {rev_chg:.0f}% [WARNING]")

    # EPS trend (0-3 pts)
    if len(eps) >= 2:
        has_data = True
        if eps.iloc[-1] > 0:
            score += 3
            signals.append(f"EPS positive ${eps.iloc[-1]:.2f}")
        elif eps.iloc[-1] > eps.iloc[0]:
            score += 1
            signals.append(f"EPS improving ${eps.iloc[-1]:.2f}")
        else:
            signals.append(f"EPS negative ${eps.iloc[-1]:.2f}")

    # Shares outstanding trend (0-4 pts) — THE DENOMINATOR
    if len(shares) >= 2:
        has_data = True
        share_chg = (shares.iloc[-1] / shares.iloc[0] - 1) * 100 if shares.iloc[0] != 0 else 0
        if share_chg < -5:
            score += 4
            signals.append(f"Shares -{abs(share_chg):.0f}% [BUYBACK]")
        elif share_chg < 2:
            score += 2
            signals.append(f"Shares stable {share_chg:+.0f}%")
        elif share_chg < 20:
            score += 0
            signals.append(f"Shares +{share_chg:.0f}% [DILUTION]")
        else:
            score -= 2
            signals.append(f"Shares +{share_chg:.0f}% [HEAVY DILUTION]")

    # Cash position (0-4 pts)
    if len(cash) > 0 and len(liabilities) > 0:
        has_data = True
        cash_ratio = cash.iloc[-1] / liabilities.iloc[-1] if liabilities.iloc[-1] != 0 else 0
        if cash_ratio > 0.5:
            score += 4
            signals.append(f"Cash/Liab={cash_ratio:.2f} [STRONG]")
        elif cash_ratio > 0.2:
            score += 2
            signals.append(f"Cash/Liab={cash_ratio:.2f}")
        else:
            signals.append(f"Cash/Liab={cash_ratio:.2f} [WEAK]")

    trust = LAYER_FULL_TRUST["COMPANY"] if has_data else 0.20
    return _layer_result("COMPANY", max(score, 0), trust, signals, has_data)


# SMART_MONEY Form 4 input: reported non-derivative P/S transactions only
# (ingestion/edgar.py's FORM4:{ticker}:PS_* family plus its coverage series).
#
#   * P/S is "open market or private" (SEC Form 4 codes); the venue is not
#     reported, so nothing here is labelled open-market (Codex HOLD on #1353
#     67ce6bdc). Never the all-code NET_SHARES / NET_VALUE: they count awards,
#     exercises and imputed $0 prices as trades (Codex HOLD on 5c4c7b32).
#   * The writer writes every date of a pull's verified coverage interval,
#     dates without filings included, so a missing date is "not observed",
#     never "no trades" (Codex HOLD on 4f55dab9). A date is verified when all
#     of _FORM4_PS_FIELDS are present from one vintage (one pull_timestamp),
#     with every P/S row share-valid and no unread filing.
#   * Scored: the contiguous run of verified dates ending at the as-of date
#     (or the day before, which a routine pull covers once that ET date has
#     closed), at most _FORM4_WINDOW_DAYS long. A gap or an unverified date
#     ends the run. Fewer than _FORM4_MIN_VERIFIED_DAYS verified: not scored.
#   * Full 90-day run, every P/S row priced: dollar rule
#       >= +$500k -> +5,  > $0 -> +2,  == $0 -> 0 (fully priced offset),  < $0 -> -2
#     Any unpriced row: share direction only (the dollar total is unknown):
#       > 0 sh -> +2,  == 0 -> 0,  < 0 -> -2
#   * Shorter run (7..89 days), a deliberately weaker policy: direction only,
#     +2 / 0 / -2 at most, trust 0.5, and the signal names the verified span.
#   * Zero P/S rows in the run scores 0: no P/S in the observed Form 4 data
#     (it does not prove the other codes were awards or exercises).
#   * Legacy rows (OPEN_MKT_*, all-code only) carry no validity or coverage:
#     unavailable.
#   Selling is capped at -2 because insider net selling is mostly routine
#   liquidity / 10b5-1 selling (not strong bearish conviction), so SMART_MONEY
#   layer scores can be negative.
#   Reads go through store.observations.read_window_known_at with the explicit
#   writer source (SEC_EDGAR): per series and date, the latest vintage pulled
#   by the end of the as-of date (UTC); another source's rows never mix in.
#   A later recovery pull therefore never changes an earlier as-of read.
FORM4_SOURCE = "SEC_EDGAR"
_FORM4_PS_FIELDS: tuple[str, ...] = (
    "PS_NET_SHARES", "PS_NET_VALUE", "PS_TXN_COUNT",
    "PS_SHARES_VALID_COUNT", "PS_PRICED_COUNT", "FILINGS_LISTED", "FILINGS_UNREAD",
)
_FORM4_WINDOW_DAYS = 90
_FORM4_MIN_VERIFIED_DAYS = 7
_FORM4_PS_LABEL = "Insider reported P/S (non-derivative, venue unknown)"
_FORM4_VALUE_STRONG_USD = 500_000.0
_FORM4_SELL_CAP = -2.0
# SMART_MONEY's trust on a full verified 90-day window is
# LAYER_FULL_TRUST["SMART_MONEY"], which is also its fixed weight in
# score_ticker's denominator whatever the coverage (LayerResult.baseline_trust).


@dataclass(frozen=True)
class Form4PSWindow:
    """Reported non-derivative P/S Form 4 evidence over the verified run."""

    start: date | None = None  # first verified date of the run
    end: date | None = None    # last verified date of the run
    txns: float = 0.0
    shares_valid: float = 0.0
    priced: float = 0.0
    net_shares: float = 0.0
    net_value: float = 0.0  # priced rows only; meaningful iff priced == txns
    unverified_dates: int = 0  # observed dates in the read window that failed verification

    @property
    def verified_days(self) -> int:
        if self.start is None or self.end is None:
            return 0
        return (self.end - self.start).days + 1

    @property
    def dollars_verified(self) -> bool:
        return self.txns > 0 and self.priced == self.txns


def _is_count(value: float) -> bool:
    return math.isfinite(value) and value >= 0 and value == int(value)


def _form4_day(obs: list) -> tuple[float, ...] | None:
    """The day's (shares, value, txns, valid, priced) if it verifies, else None."""
    if any(o is None for o in obs) or len({o.pull_timestamp for o in obs}) != 1:
        return None  # a series missing, or the series from different vintages
    d_shares, d_value, d_txns, d_valid, d_priced, d_listed, d_unread = (o.value for o in obs)
    if not (
        math.isfinite(d_shares) and math.isfinite(d_value)
        and all(_is_count(v) for v in (d_txns, d_valid, d_priced, d_listed, d_unread))
        and d_unread == 0 and d_valid == d_txns and d_priced <= d_valid
        and (d_txns == 0 or d_listed > 0)
    ):
        return None  # unread filings, invalid share rows, or inconsistent counts
    return d_shares, d_value, d_txns, d_valid, d_priced


def _form4_ps_window(conn, ticker: str, as_of_date: date | None) -> Form4PSWindow:
    """Verify the PS_* series per date and sum the contiguous verified run."""
    as_of = _resolve_as_of_date(as_of_date)
    by_field = {
        field: {
            o.obs_date: o
            for o in read_window_known_at(
                conn,
                f"FORM4:{ticker}:{field}",
                as_of=as_of,
                lag=None,
                source=FORM4_SOURCE,
                start=as_of - timedelta(days=_FORM4_WINDOW_DAYS),
            )
        }
        for field in _FORM4_PS_FIELDS
    }
    observed = {d for rows in by_field.values() for d in rows}
    days = {d: _form4_day([by_field[f].get(d) for f in _FORM4_PS_FIELDS]) for d in observed}
    unverified = sum(1 for v in days.values() if v is None)

    end = as_of if days.get(as_of) is not None else as_of - timedelta(days=1)
    if days.get(end) is None:
        return Form4PSWindow(unverified_dates=unverified)
    start = end
    while (
        (end - start).days + 1 < _FORM4_WINDOW_DAYS
        and days.get(start - timedelta(days=1)) is not None
    ):
        start -= timedelta(days=1)
    run = [days[start + timedelta(days=i)] for i in range((end - start).days + 1)]
    return Form4PSWindow(
        start=start,
        end=end,
        net_shares=sum(r[0] for r in run),
        net_value=sum(r[1] for r in run),
        txns=sum(r[2] for r in run),
        shares_valid=sum(r[3] for r in run),
        priced=sum(r[4] for r in run),
        unverified_dates=unverified,
    )


def _score_form4_ps(window: Form4PSWindow) -> tuple[float, str, bool, bool]:
    """(points, signal, has_data, full_window) for SMART_MONEY's Form 4 part."""
    label = _FORM4_PS_LABEL
    days = window.verified_days
    if days == 0:
        if window.unverified_dates:
            return (0.0, (
                "Insider Form 4 P/S coverage not verified through the as-of date "
                f"({window.unverified_dates} observed date(s) incomplete: missing series, "
                "mixed vintages, invalid share rows or unread filings); not scored"
            ), False, False)
        return (0.0, (
            "Insider Form 4 P/S series unavailable (legacy OPEN_MKT_* and all-code rows "
            "carry no price, share or coverage evidence; not scored)"
        ), False, False)
    span = f"verified {window.start:%Y-%m-%d}..{window.end:%Y-%m-%d}, {days}d of {_FORM4_WINDOW_DAYS}d"
    if days < _FORM4_MIN_VERIFIED_DAYS:
        return (0.0, (
            f"Insider Form 4 P/S coverage too short ({span}; needs "
            f"{_FORM4_MIN_VERIFIED_DAYS}d); not scored"
        ), False, False)
    full = days >= _FORM4_WINDOW_DAYS
    if window.txns == 0:
        return 0.0, f"No reported non-derivative P/S insider transactions in the observed Form 4 data ({span})", True, full
    if not full:
        # Shorter verified run: direction only, never the +5 tier.
        if window.dollars_verified:
            direction, basis = window.net_value, f"net ${window.net_value:,.0f}"
        else:
            direction, basis = window.net_shares, f"net {window.net_shares:+,.0f} sh, dollars unverified"
        if direction > 0:
            return 2.0, f"{label} net buy ({basis}; {span}; short window, direction only)", True, False
        if direction < 0:
            return _FORM4_SELL_CAP, f"{label} net sell ({basis}; {span}; short window, direction only)", True, False
        return 0.0, f"{label} buys and sells offset ({basis}; {span}; short window)", True, False
    if window.dollars_verified:
        value = window.net_value
        if value >= _FORM4_VALUE_STRONG_USD:
            return 5.0, f"{label} net buy ${value:,.0f} ({span})", True, True
        if value > 0:
            return 2.0, f"{label} net buy ${value:,.0f} ({span})", True, True
        if value < 0:
            return _FORM4_SELL_CAP, f"{label} net sell ${abs(value):,.0f} ({span})", True, True
        return 0.0, f"{label} buys and sells offset (fully priced, net $0; {span})", True, True
    unpriced = (
        f"dollars unverified: {window.txns - window.priced:,.0f} of {window.txns:,.0f} "
        "P/S rows unpriced"
    )
    shares = window.net_shares
    if shares > 0:
        return 2.0, f"{label} net share buy +{shares:,.0f} sh ({unpriced}; {span})", True, True
    if shares < 0:
        return _FORM4_SELL_CAP, f"{label} net share sell {shares:,.0f} sh ({unpriced}; {span})", True, True
    return 0.0, f"{label} share buys and sells offset (net 0 sh; {unpriced}; {span})", True, True


def score_smart_money(
    conn,
    ticker: str,
    as_of_date: date | None = None,
) -> LayerResult:
    """Layer 3: Are insiders buying? (Form 4 P/S only; no institutional input.)"""
    points, form4_signal, has_data, full_window = _score_form4_ps(
        _form4_ps_window(conn, ticker, as_of_date)
    )
    signals = [form4_signal]

    # Institutional flow: no code writes INST_FLOW:*, and institutional (13F)
    # holdings changes are non-directional (owner decision 2026-11-23), so the
    # layer reads no institutional series instead of whichever pooled row
    # sorted last (Codex HOLD on #1353 5c4c7b32).
    signals.append("Institutional flow unavailable (no INST_FLOW writer; 13F is non-directional)")

    trust = (LAYER_FULL_TRUST["SMART_MONEY"] if full_window else 0.5) if has_data else 0.15
    return _layer_result(
        "SMART_MONEY",
        max(_FORM4_SELL_CAP, min(points, 15.0)),
        trust,
        signals,
        has_data,
    )


def score_crowd(
    conn,
    ticker: str,
    as_of_date: date | None = None,
) -> LayerResult:
    """Layer 4: Is the crowd wrong? Short interest, FTD, PCR."""
    score = 0.0
    signals = []
    has_data = False

    # Short ratio
    short = _load_raw_series(conn, f"SHORT:{ticker}:ratio", 30, as_of_date=as_of_date)
    if not short.empty:
        has_data = True
        avg_short = short.mean()
        if avg_short > 0.5:
            score += 5
            signals.append(f"Short ratio {avg_short:.0%} [HEAVILY SHORTED]")
        elif avg_short > 0.3:
            score += 3
            signals.append(f"Short ratio {avg_short:.0%} [ELEVATED]")
        elif avg_short > 0:
            score += 1
            signals.append(f"Short ratio {avg_short:.0%}")

    # Fails-to-deliver
    ftd = _load_raw_series(conn, f"FTD:{ticker}:qty", 90, as_of_date=as_of_date)
    if not ftd.empty:
        has_data = True
        recent_ftd = ftd.tail(10).mean()
        older_ftd = ftd.head(30).mean() if len(ftd) > 30 else recent_ftd
        if recent_ftd > older_ftd * 2 and recent_ftd > 10000:
            score += 5
            signals.append(
                f"FTD surging ({recent_ftd:,.0f} vs {older_ftd:,.0f}) [SQUEEZE SETUP]"
            )
        elif recent_ftd > 10000:
            score += 2
            signals.append(f"FTD elevated ({recent_ftd:,.0f})")

    # Put/Call ratio (market-wide)
    pcr = _load_raw_latest(conn, "CBOE:totalpc", 5, as_of_date=as_of_date)
    if pcr is not None:
        if pcr > 1.2:
            score += 3
            signals.append(f"PCR={pcr:.2f} [MAX BEARISH]")
        elif pcr > 1.0:
            score += 1
            signals.append(f"PCR={pcr:.2f} [BEARISH]")

    trust = LAYER_FULL_TRUST["CROWD"] if has_data else 0.15
    return _layer_result("CROWD", min(score, 15), trust, signals, has_data)


def score_narrative(
    conn,
    ticker: str,
    as_of_date: date | None = None,
) -> LayerResult:
    """Layer 5: Is there a narrative catalyst?"""
    score = 0.0
    signals = []
    has_data = False

    # News article count trend
    news = _load_raw_series(conn, f"NEWS:{ticker}:daily_count", 90, as_of_date=as_of_date)
    if not news.empty:
        has_data = True
        recent = news.tail(7).mean() if len(news) >= 7 else news.mean()
        older = news.head(30).mean() if len(news) >= 30 else recent
        if recent > older * 2:
            score += 5
            signals.append(f"News volume surging ({recent:.0f}/day vs {older:.0f})")
        elif recent > 3:
            score += 2
            signals.append(f"Moderate news coverage ({recent:.0f}/day)")

    # Wiki attention (if available)
    wiki_name = f"wiki_{ticker.lower()}"
    wiki = _load_latest(conn, wiki_name, as_of_date=as_of_date)
    if wiki is not None:
        has_data = True
        score += 2
        signals.append("Wiki attention tracked")

    # GDELT (market-wide narrative)
    gdelt = _load_latest(conn, "gdelt_article_count", as_of_date=as_of_date)
    if gdelt is not None and gdelt > 1000:
        score += 1
        signals.append(f"GDELT high ({gdelt:.0f} articles)")

    trust = LAYER_FULL_TRUST["NARRATIVE"] if has_data else 0.10
    return _layer_result("NARRATIVE", min(score, 10), trust, signals, has_data)


def score_flow(
    conn,
    ticker: str,
    price: pd.Series,
    as_of_date: date | None = None,
) -> LayerResult:
    """Layer 6: Is money actually moving?"""
    score = 0.0
    signals = []
    has_data = False

    if price.empty or len(price) < 20:
        return _layer_result("FLOW", 0, 0.10, (), False)

    # Volume spike (from price data — use returns as proxy if no volume)
    returns = price.pct_change().dropna()
    recent_vol = returns.tail(5).abs().mean()
    avg_vol = returns.tail(60).abs().mean() if len(returns) >= 60 else recent_vol

    if avg_vol > 0 and recent_vol > avg_vol * 2:
        has_data = True
        score += 5
        signals.append(f"Volatility spike ({recent_vol/avg_vol:.1f}x normal)")
    elif avg_vol > 0 and recent_vol > avg_vol * 1.5:
        has_data = True
        score += 2
        signals.append(f"Elevated activity ({recent_vol/avg_vol:.1f}x)")

    # Check for whale options activity
    whale = _load_raw_latest(conn, f"WHALE:{ticker}:%", as_of_date=as_of_date)
    if whale is not None:
        has_data = True
        score += 5
        signals.append("Unusual options activity detected")

    # CBOE equity PCR
    epcr = _load_raw_latest(conn, "CBOE:equitypc", 5, as_of_date=as_of_date)
    if epcr is not None:
        has_data = True
        if epcr > 0.8:
            score += 3
            signals.append(f"Equity PCR={epcr:.2f} [BEARISH - contrarian buy]")

    trust = LAYER_FULL_TRUST["FLOW"] if has_data else 0.15
    return _layer_result("FLOW", min(score, 15), trust, signals, has_data)


def score_confirmation(price: pd.Series) -> LayerResult:
    """Layer 7: Has the turn actually happened?"""
    score = 0.0
    signals = []

    if price.empty or len(price) < 30:
        return _layer_result("CONFIRM", 0, 0.10, (), False)

    current = price.iloc[-1]

    # 5-day momentum positive
    if len(price) >= 6:
        mom5 = (current / price.iloc[-6] - 1) * 100
        if mom5 > 3:
            score += 3
            signals.append(f"5d momentum +{mom5:.1f}% [STRONG]")
        elif mom5 > 0:
            score += 1
            signals.append(f"5d momentum +{mom5:.1f}%")
        else:
            signals.append(f"5d momentum {mom5:.1f}% [NO TURN YET]")

    # 30d momentum turning while 90d still negative
    if len(price) >= 91:
        mom30 = (current / price.iloc[-31] - 1) * 100
        mom90 = (current / price.iloc[-91] - 1) * 100
        if mom30 > 0 and mom90 < 0:
            score += 4
            signals.append(f"Momentum TURNING (30d={mom30:+.1f}%, 90d={mom90:+.1f}%)")
        elif mom30 > 0:
            score += 2
            signals.append(f"30d positive +{mom30:.1f}%")

    # Higher low forming (current > 30d low)
    if len(price) >= 30:
        low_30d = price.tail(30).min()
        if current > low_30d * 1.05:
            score += 3
            signals.append(f"Higher low forming (${current:.2f} vs 30d low ${low_30d:.2f})")

    return _layer_result("CONFIRM", min(score, 10), LAYER_FULL_TRUST["CONFIRM"], signals, True)


# ═══════════════════════════════════════════════════════════════
# MAIN SCORER
# ═══════════════════════════════════════════════════════════════

MIN_COVERAGE_FOR_ALERT = 0.5


def score_ticker(
    engine: Engine,
    ticker: str,
    as_of_date: date | None = None,
) -> ConvictionReport:
    """Score a single ticker across all 7 layers."""
    as_of = _resolve_as_of_date(as_of_date)
    with engine.connect() as conn:
        price = _load_price(conn, ticker, as_of_date=as_of)

        named_layers: list[tuple[str, LayerResult | None]] = [
            ("SETUP", score_setup(conn, ticker, price, as_of_date=as_of)),
            ("COMPANY", score_company(conn, ticker, as_of_date=as_of)),
            ("SMART_MONEY", score_smart_money(conn, ticker, as_of_date=as_of)),
            ("CROWD", score_crowd(conn, ticker, as_of_date=as_of)),
            ("NARRATIVE", score_narrative(conn, ticker, as_of_date=as_of)),
            ("FLOW", score_flow(conn, ticker, price, as_of_date=as_of)),
            ("CONFIRM", score_confirmation(price)),
        ]

    present_layers = tuple(layer for _, layer in named_layers if layer is not None)
    available_layers = [layer for layer in present_layers if layer.data_available]
    unavailable_layers = tuple(
        name for name, layer in named_layers if layer is None or not layer.data_available
    )

    # Exclude never-written None layers (COMPANY) from the weighted total, while
    # keeping data_available=False layers in it (counted as 0) so thin data
    # cannot inflate total_score.
    # A layer's denominator weight is its baseline_trust (its full-data trust,
    # LAYER_FULL_TRUST): missing data, a shorter SMART_MONEY window or a
    # too-short price history lowers the numerator weight only, so the same
    # evidence with less certainty, or a missing layer, can never promote the
    # total.
    weighted_score = sum(layer.score * layer.trust for layer in present_layers)
    weighted_max = sum(layer.max_score * layer.denominator_trust for layer in present_layers)
    total = (
        max(0, min(100, int(round(weighted_score / weighted_max * 100))))
        if weighted_max > 0
        else 0
    )

    # Confidence = how much data we actually have across the 7 designed layers
    data_coverage = len(available_layers) / len(named_layers)
    confidence = total * data_coverage

    # Alert level (coverage floor: thin data < 50% coverage is capped at WATCH)
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

    if data_coverage < MIN_COVERAGE_FOR_ALERT and level in {
        "CONVICTION",
        "FIRE",
        "SCALE",
        "PILOT",
    }:
        level = "WATCH"

    return ConvictionReport(
        ticker=ticker,
        total_score=total,
        confidence_pct=round(confidence, 1),
        layers=present_layers,
        alert_level=level,
        timestamp=pd.Timestamp.now().isoformat(),
        unavailable_layers=unavailable_layers,
    )


def scan_all(
    engine: Engine,
    min_score: int = 20,
    as_of_date: date | None = None,
) -> list[ConvictionReport]:
    """Score all tickers with price data."""
    as_of = _resolve_as_of_date(as_of_date)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT UPPER(REPLACE(fr.name, '_full', ''))
            FROM feature_registry fr JOIN resolved_series rs ON fr.id = rs.feature_id
            WHERE fr.name LIKE '%%_full'
              AND rs.obs_date <= :as_of
              AND rs.release_date <= :as_of
              AND rs.vintage_date <= :as_of
            GROUP BY fr.name HAVING COUNT(*) > 50
        """), {"as_of": as_of}).fetchall()

    tickers = [r[0] for r in rows]
    reports = []

    for ticker in tickers:
        report = score_ticker(engine, ticker, as_of_date=as_of)
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
    present_names = {layer.name for layer in report.layers}
    for name in report.unavailable_layers:
        if name not in present_names:
            log.info("    - {name:12s} [unavailable]", name=name)
