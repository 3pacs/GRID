"""
Conviction Scorer — 98% confidence trade detector.

Scores every ticker across 7 layers of evidence, each weighted by
trust in the underlying data. A score of 85+ with all layers confirming
= highest conviction. Uses ONLY data that a live writer puts in raw_series
or resolved_series; an input with no such writer is reported unavailable
(a signal saying why) and scores nothing.

Layers (weight; inputs read today, then inputs reported unavailable):
  1. SETUP      (17 pts) — VIX, HY spread, curve, drawdown severity
  2. COMPANY    (15 pts) — unavailable: no XBRL:* writer
  3. SMART_MONEY(15 pts) — Insider reported P/S (Form 4); institutional flow unavailable
  4. CROWD      (15 pts) — SEC fails-to-deliver balance; total put/call shown, not scored; short interest unavailable
  5. NARRATIVE  (10 pts) — nothing scored: Tiingo news count shown, not scored; wiki and GDELT unavailable
  6. FLOW       (15 pts) — Volatility spike; equity put/call shown, not scored; unusual options not scored
  7. CONFIRM    (10 pts) — Price momentum turning, higher low formed

CROWD, NARRATIVE and FLOW's equity put/call read raw_series through
store.observations.read_window_known_at with each writer's own source: a row
counts only if it was pulled by the end of the as-of date (UTC), and only the
latest vintage pulled by then. An FTD balance scores only when every row of
its CUSIP series in the read window was filed under the ticker, including the
selected vintages (see _score_ftd). See "CROWD / NARRATIVE / FLOW inputs"
below for the per-input measurements (griddb, 2026-11-23).

Each layer returns (score, max_possible, trust_weight, signals[]).
Final score = sum(score * trust) / sum(max * full_trust) * 100, where
full_trust is the layer's trust with its data in hand (LAYER_FULL_TRUST):
missing or thinner data lowers a layer's numerator weight only, never the
denominator, so a missing layer can never raise the total above that layer
read neutral. (It can still sit above that layer read adverse, which is why
the alert does not come from the total; see score_low below.)

total_score treats a missing layer as contributing nothing (neutral). The
alert level instead comes from score_low, the total with every unknown at
its worst attainable value (LAYER_MIN_SCORE): a missing layer at its minimum
over its full weight, and the untrusted share of a partly trusted layer
(full_trust - trust) at its minimum. Lost evidence therefore never promotes
an alert: a layer that goes missing can only lower score_low, since its
minimum is at or below anything it could have measured. score_high is the
same with every unknown at its best attainable value (LAYER_ATTAINABLE_SCORE).

CONVICTION (85) and FIRE (70) are dormant: with the current scorers the best
attainable total is 57 (36.15 of 63.15), so the ladder tops out at SCALE. It
was 81 (50.95) while CROWD, NARRATIVE and FLOW still counted points from inputs
that no live writer feeds (short interest, wiki, GDELT, equity put/call), that
fire for every covered ticker (unusual options), whose counts cannot tell an
empty fetch from a missing one (Tiingo news), or whose pull clock is not the
acquisition time (Cboe total put/call). In production those inputs
had added only wiki's +2, to GOLD and SP500, from series last updated
2026-04-03. The equity put/call ratio the CBOE puller stores since #1397
(equity_put_call_ratio) is shown in FLOW on the same terms as the total
ratio and scores nothing. Each report says so (max_attainable_score,
dormant_alert_levels); raising it needs a separately approved recalibration,
not a quiet threshold change.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from types import MappingProxyType

import pandas as pd
from loguru import logger as log
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from alpha_research.data.split_adjuster import adjust_splits
from normalization.entity_map import pattern_collision
from store.observations import Observation, read_window_known_at
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
    alert_level: str  # PASS / WATCH / PILOT / SCALE / FIRE / CONVICTION, from score_low
    timestamp: str
    unavailable_layers: tuple[str, ...] = ()
    # Total with every unknown at its worst / best attainable value (see the
    # module docstring); score_ticker sets the alert level from score_low.
    score_low: int | None = None
    score_high: int | None = None
    # Best total the current scorers can produce over the present layers,
    # and the alert levels it cannot reach (CONVICTION while it is 81).
    max_attainable_score: int | None = None
    dormant_alert_levels: tuple[str, ...] = ()


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


# _load_raw_latest (a ">= prefix / < prefix-plus-one" range scan) was removed
# 2026-11-23. Under griddb's en_US.UTF-8 collation punctuation sorts at a lower
# level, so 'WHALE:SPY:600' < 'WHALE:SPY;' is false and the scan returned 0 of
# SPY's 88,256 WHALE rows. It also had no freshness bound, and CROWD passed it a
# positional 5 as source_id (CBOE's id happens to be 5). Its three callers now
# read named series through _read_known, or report the input unavailable.


def _read_known(
    conn,
    series_id: str,
    source: str,
    as_of: date,
    days: int,
) -> list[Observation]:
    """``series_id``'s rows from ``source`` dated in the ``days`` to ``as_of``, oldest first.

    Point in time: a row counts only if it was pulled by the end of ``as_of``
    (UTC), and a date's value is its latest vintage pulled by then
    (store.observations.read_window_known_at with no modeled lag).
    """
    return read_window_known_at(
        conn,
        series_id,
        as_of=as_of,
        lag=None,
        source=source,
        start=as_of - timedelta(days=days),
    )


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

# Each layer's lowest and highest points its current scorer can produce. Only
# SMART_MONEY goes below zero (the Form 4 sell cap, -2); the others top out
# where their scored components sum: SETUP 5+5+4+3, COMPANY 4+3+4+4,
# SMART_MONEY the +5 strong-buy tier, CROWD 5 (FTD; put/call is
# descriptive), NARRATIVE 0 (news is descriptive), FLOW 5 (volatility;
# equity put/call is descriptive),
# CONFIRM 3+4+3. The inputs reported unavailable or not scored score nothing,
# so they add nothing here (CROWD was 13 with short interest and put/call,
# NARRATIVE 8 with news, wiki and GDELT, FLOW 13 with unusual options and
# equity put/call). They bound what an unknown could have been (score_low /
# score_high) and which alert levels are reachable at all; max_score stays
# the weight.
LAYER_MIN_SCORE: Mapping[str, float] = MappingProxyType({
    "SETUP": 0.0,
    "COMPANY": 0.0,
    "SMART_MONEY": -2.0,
    "CROWD": 0.0,
    "NARRATIVE": 0.0,
    "FLOW": 0.0,
    "CONFIRM": 0.0,
})
LAYER_ATTAINABLE_SCORE: Mapping[str, float] = MappingProxyType({
    "SETUP": 17.0,
    "COMPANY": 15.0,
    "SMART_MONEY": 5.0,
    "CROWD": 5.0,
    "NARRATIVE": 0.0,
    "FLOW": 5.0,
    "CONFIRM": 10.0,
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


# ── CROWD / NARRATIVE / FLOW inputs ──────────────────────────────────────
#
# Measured on griddb 2026-11-23 (read-only; 139 tickers in the scan universe).
# Before this, CROWD read no data for 139 of 139 tickers, NARRATIVE for 137
# and FLOW for 94:
#
#   * SHORT:{t}:ratio and FTD:{t}:qty were one-off loads pulled 2026-04-01
#     (obs to 2026-03-31 / 2026-03-13); nothing has written them since.
#   * The SEC FTD writer (ingestion/altdata/sec_ftd.py, 24 h) keys balances by
#     CUSIP, sec:ftd_balance:<CUSIP>, with the file's SYMBOL in raw_payload.
#   * CBOE:totalpc stopped with Cboe's totalpc.csv; the CBOE puller (24 h)
#     stores the total exchange ratio as put_call_ratio since 2026-11-04.
#     CBOE:equitypc never had a writer; since #1397 the same daily page's
#     equity ratio is stored as equity_put_call_ratio (shown, not scored).
#   * News is written as TIINGO_NEWS:{t}:daily_count (6 h), not
#     NEWS:{t}:daily_count. wiki_{t} was a one-off topic load ending
#     2026-04-03, and gdelt_article_count was deleted by the 2026-11-16 GDELT
#     rebuild.
#   * WHALE:{t}:* exists for 25 universe tickers, but the old prefix range
#     scan read none of it (collation, see _read_known).

# Reported unavailable: no live writer, or the live series does not measure
# what the rule scores. Like COMPANY and INST_FLOW, nothing is read for them.
SHORT_INTEREST_UNAVAILABLE = (
    "Short interest unavailable (no SHORT:* writer since a one-off 2026-04-01 load; "
    "FINRA daily short-sale volume is trade volume, not short interest)"
)
WIKI_UNAVAILABLE = (
    "Wiki attention unavailable (no per-ticker writer; wiki_* was a one-off topic load "
    "ending 2026-04-03)"
)
GDELT_UNAVAILABLE = (
    "GDELT article count unavailable (gdelt_article_count was retired by the "
    "2026-11-16 GDELT rebuild)"
)
WHALE_NOT_SCORED = (
    "Unusual options activity not scored (the WHALE:* detector flags every covered "
    "ticker every day, so a flag is not unusual)"
)
# Read and shown, never scored (Codex HOLD on #1395 f821c50c): the writer
# stores a count only when a fetch returns articles, so an empty fetch and a
# failed or skipped one look alike; it counts every article returned, repeats
# included; and each day's count covers an overlapping trailing 7 days.
NEWS_NOT_SCORED = (
    "News count not scored (Tiingo stores no row for an empty fetch, counts repeated "
    "articles, and each count overlaps the last 7 days)"
)
# Read and shown, never scored (Codex HOLD on #1395 a994e347): the CBOE
# puller opened one transaction before its HTTP fetches and leaves
# pull_timestamp to DEFAULT NOW(), the transaction start, so a session
# fetched late could carry an earlier pull time and pass an as-of cutoff it
# did not meet. Since #1397 it reads every page before the insert
# transaction opens, so a new row's stamp is at or after the read, but it is
# still a proxy, and rows stored before that keep the early stamp. Scoring
# it waits for a qualified clock and an approved policy.
PCR_NOT_SCORED = (
    "Total put/call not scored (the Cboe puller stamps rows with its transaction start, "
    "not the fetch time)"
)
# The equity ratio comes from the same page and writer, so it carries the
# same proxy clock (Codex HOLD on #1397 cd0b24b1 / 4e30ed90). Shown, adds no
# points, never makes FLOW available; scoring it needs a qualified clock, a
# same-tape ablation against the total ratio and an approved policy.
EQUITY_PCR_NOT_SCORED = (
    "Equity put/call not scored (same Cboe page and pull-time proxy as the total ratio)"
)

# CROWD: SEC fails-to-deliver balances (ingestion/altdata/sec_ftd.py).
# Each row is the outstanding balance on a settlement date, published about
# two weeks after each half-month closes. The file lists only securities with
# fails, so a missing date is not a zero and is left out (the rule averages
# the balances on file, as it did on the legacy FTD:{t}:qty rows).
FTD_SOURCE = "SEC_FTD"
FTD_SERIES_PREFIX = "sec:ftd_balance:"
FTD_WINDOW_DAYS = 90
# Newest balance must settle within this many days of the as-of date: the
# normal publication lag is up to ~31 days, plus pull cadence.
FTD_FRESH_DAYS = 45
# Both queries use the bounds _read_known applies to the balances it selects:
# SUCCESS rows of the writer's source, settled in [as_of - FTD_WINDOW_DAYS,
# as_of], pulled before the end of as_of's UTC day (pull_timestamp is
# TIMESTAMPTZ, so this is read_window_known_at's pulled-by rule).
# Symbol -> CUSIP series: one batch per scan.
_FTD_IDS_SQL = text("""
    SELECT r.raw_payload ->> 'symbol' AS symbol, r.series_id
    FROM raw_series r
    JOIN source_catalog sc ON sc.id = r.source_id
    WHERE LOWER(sc.name) = LOWER(:source)
      AND r.series_id LIKE :pattern ESCAPE '\\'
      AND r.pull_status = 'SUCCESS'
      AND r.obs_date >= :start
      AND r.obs_date <= :as_of
      AND r.pull_timestamp < :known_by
      AND r.raw_payload ->> 'symbol' IN :symbols
    GROUP BY 1, 2
""").bindparams(bindparam("symbols", expanding=True))
# CUSIP series -> the symbol on each of its rows, run after the balances are
# read so every selected vintage is already on file (~10 ms per ticker).
_FTD_FILED_AS_SQL = text("""
    SELECT r.obs_date, r.pull_timestamp, r.raw_payload ->> 'symbol' AS symbol
    FROM raw_series r
    JOIN source_catalog sc ON sc.id = r.source_id
    WHERE r.series_id = :sid
      AND LOWER(sc.name) = LOWER(:source)
      AND r.pull_status = 'SUCCESS'
      AND r.obs_date >= :start
      AND r.obs_date <= :as_of
      AND r.pull_timestamp < :known_by
""")

# CROWD: Cboe total exchange put/call ratio (ingestion/altdata/cboe_indices.py),
# one value per trading day, market-wide, shown, not scored (PCR_NOT_SCORED).
# The newest within PCR_FRESH_DAYS (covers a long weekend) is shown.
PCR_SERIES = "put_call_ratio"
PCR_SOURCE = "CBOE"
PCR_FRESH_DAYS = 5

# FLOW: Cboe equity put/call ratio, same page and writer
# (ingestion/altdata/cboe_indices.py), one value per trading day,
# market-wide, shown, not scored (EQUITY_PCR_NOT_SCORED). The newest within
# EQUITY_PCR_FRESH_DAYS is shown.
EQUITY_PCR_SERIES = "equity_put_call_ratio"
EQUITY_PCR_SOURCE = "CBOE"
EQUITY_PCR_FRESH_DAYS = 5

# NARRATIVE: Tiingo news (ingestion/tiingo_news_pull.py), shown, not scored
# (NEWS_NOT_SCORED). Each day's value is the number of articles a trailing
# NEWS_TRAILING_DAYS query returned, capped at the pull's limit
# (NEWS_FETCH_CAP), written once per day by the first pull that found any. A
# value at the cap is a lower bound. The newest count within
# NEWS_TRAILING_DAYS is shown with its date.
NEWS_SOURCE = "tiingo_news"
NEWS_SERIES = "TIINGO_NEWS:{ticker}:daily_count"
NEWS_TRAILING_DAYS = 7
NEWS_FETCH_CAP = 50

# FLOW: a volatility spike needs a 60-day baseline of daily returns.
FLOW_BASELINE_RETURNS = 60


def _panel_instrument_conflict(ticker: str) -> str | None:
    """The price feed showing ``{ticker}_full`` is not the listed security ``ticker``.

    gold_full is gold futures (YF:GC=F) and btc_full Binance BTCUSDT, while SEC
    and Tiingo file a listed security under the same symbol (Barrick Gold, a
    bitcoin trust). Per-ticker inputs keyed by symbol are not read for such a
    ticker (normalization.entity_map.pattern_collision).
    """
    return pattern_collision(f"YF:{ticker.upper()}:adj_close", f"{ticker.lower()}_full")


def _ftd_series_ids(conn, tickers: Sequence[str], as_of: date) -> dict[str, tuple[str, ...]]:
    """The SEC FTD balance series each ticker's symbol was filed under, known at ``as_of``.

    One query for all tickers (scan_all passes the result down; one lookup
    costs ~0.6 s on griddb, the batch for 139 tickers 0.8 s). Only SUCCESS
    rows of the writer's source that settled in the read window and were
    pulled by the end of ``as_of`` (UTC) count. A ticker whose price panel is
    another instrument is not looked up.
    """
    symbols = sorted({t.upper() for t in tickers if _panel_instrument_conflict(t) is None})
    if not symbols:
        return {}
    rows = conn.execute(_FTD_IDS_SQL, {
        "source": FTD_SOURCE,
        "pattern": FTD_SERIES_PREFIX.replace("_", "\\_") + "%",
        "start": as_of - timedelta(days=FTD_WINDOW_DAYS),
        "as_of": as_of,
        "known_by": datetime.combine(as_of + timedelta(days=1), time(0), tzinfo=timezone.utc),
        "symbols": symbols,
    }).fetchall()
    found: dict[str, set[str]] = {}
    for symbol, series_id in rows:
        if symbol is not None and str(series_id).startswith(FTD_SERIES_PREFIX):
            found.setdefault(str(symbol).upper(), set()).add(str(series_id))
    return {symbol: tuple(sorted(ids)) for symbol, ids in found.items()}


def _utc(ts: object) -> datetime | None:
    """A raw_series pull_timestamp as an aware UTC instant (naive is UTC, as griddb runs Etc/UTC)."""
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)
    if not isinstance(ts, datetime):
        return None
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def _ftd_identity_break(
    conn,
    series_id: str,
    symbol: str,
    as_of: date,
    selected: Sequence[Observation],
) -> str | None:
    """Why ``selected`` (read from ``series_id``) is not ``symbol``'s balance, or None.

    The symbol -> CUSIP lookup finds a CUSIP from any one row filed under the
    symbol; the balances read from that CUSIP series may be filed under
    another. So every row of the series in the read bounds, all vintages,
    must be filed under ``symbol``. One that is not means a symbol change
    (old A rows and newer B rows on one CUSIP) or a corrected filing in the
    window, and the input abstains. Each selected vintage must also be one of
    those rows. This query runs after the read, so a row pulled in between
    (an as-of of today) is checked, never scored unchecked.
    """
    rows = conn.execute(_FTD_FILED_AS_SQL, {
        "sid": series_id,
        "source": FTD_SOURCE,
        "start": as_of - timedelta(days=FTD_WINDOW_DAYS),
        "as_of": as_of,
        "known_by": datetime.combine(as_of + timedelta(days=1), time(0), tzinfo=timezone.utc),
    }).fetchall()
    others = sorted({"no symbol" if s is None else str(s) for _, _, s in rows if s != symbol})
    if others:
        return f"{series_id} was also filed as {', '.join(others)} in the window"
    on_file = {(str(d)[:10], _utc(ts)) for d, ts, _ in rows}
    for o in selected:
        if (o.obs_date.isoformat(), _utc(o.pull_timestamp)) not in on_file:
            return f"the {o.obs_date} balance read from {series_id} is not on file under {symbol}"
    return None


def _score_ftd(
    conn,
    ticker: str,
    as_of: date,
    series_ids: Mapping[str, tuple[str, ...]] | None,
) -> tuple[float, str, bool]:
    """(points, signal, has_data) for CROWD's fails-to-deliver part."""
    conflict = _panel_instrument_conflict(ticker)
    if conflict is not None:
        return 0.0, f"FTD not read ({ticker}'s price panel is {conflict}, not the listed {ticker})", False
    if series_ids is None:
        series_ids = _ftd_series_ids(conn, [ticker], as_of)
    ids = series_ids.get(ticker.upper(), ())
    if not ids:
        return 0.0, f"No SEC FTD balance filed under {ticker} in the {FTD_WINDOW_DAYS} days to {as_of}", False
    if len(ids) > 1:
        return 0.0, f"FTD not scored ({ticker} is filed under {len(ids)} CUSIPs in the window)", False
    obs = _read_known(conn, ids[0], FTD_SOURCE, as_of, FTD_WINDOW_DAYS)
    if not obs:
        return 0.0, f"No SEC FTD balance known for {ticker} at {as_of}", False
    unbound = _ftd_identity_break(conn, ids[0], ticker.upper(), as_of, obs)
    if unbound is not None:
        return 0.0, f"FTD not scored ({unbound})", False
    latest = obs[-1].obs_date
    if latest < as_of - timedelta(days=FTD_FRESH_DAYS):
        return 0.0, f"SEC FTD balance stale for {ticker} (newest settlement {latest})", False
    ftd = pd.Series([o.value for o in obs], dtype=float)
    recent_ftd = ftd.tail(10).mean()
    older_ftd = ftd.head(30).mean() if len(ftd) > 30 else recent_ftd
    settled = f"settled to {latest}"
    if recent_ftd > older_ftd * 2 and recent_ftd > 10000:
        return 5.0, (
            f"FTD balance surging ({recent_ftd:,.0f} vs {older_ftd:,.0f} sh; {settled}) [SQUEEZE SETUP]"
        ), True
    if recent_ftd > 10000:
        return 2.0, f"FTD balance elevated ({recent_ftd:,.0f} sh; {settled})", True
    return 0.0, f"FTD balance {recent_ftd:,.0f} sh ({settled})", True


def score_crowd(
    conn,
    ticker: str,
    as_of_date: date | None = None,
    ftd_series_ids: Mapping[str, tuple[str, ...]] | None = None,
) -> LayerResult:
    """Layer 4: Is the crowd wrong? FTD balance scored; total put/call shown, not scored.

    Short interest is unavailable.

    ``ftd_series_ids`` is _ftd_series_ids for the scan (looked up for this
    ticker when None).
    """
    as_of = _resolve_as_of_date(as_of_date)
    signals = [SHORT_INTEREST_UNAVAILABLE]

    score, ftd_signal, has_data = _score_ftd(conn, ticker, as_of, ftd_series_ids)
    signals.append(ftd_signal)

    # Total put/call (market-wide): shown, adds no points, never makes the
    # layer available (PCR_NOT_SCORED).
    signals.append(PCR_NOT_SCORED)
    pcr_obs = _read_known(conn, PCR_SERIES, PCR_SOURCE, as_of, PCR_FRESH_DAYS)
    if pcr_obs:
        signals.append(f"Total put/call, not scored: {pcr_obs[-1].value:.2f} ({pcr_obs[-1].obs_date})")
    else:
        signals.append(f"No total put/call in the {PCR_FRESH_DAYS} days to {as_of}")

    trust = LAYER_FULL_TRUST["CROWD"] if has_data else 0.15
    return _layer_result("CROWD", min(score, 15), trust, signals, has_data)


def _describe_news(conn, ticker: str, as_of: date) -> str:
    """NARRATIVE's Tiingo news count as a signal; it never scores (NEWS_NOT_SCORED)."""
    conflict = _panel_instrument_conflict(ticker)
    if conflict is not None:
        return f"News not read ({ticker}'s price panel is {conflict}, not the listed {ticker})"
    obs = _read_known(conn, NEWS_SERIES.format(ticker=ticker), NEWS_SOURCE, as_of, NEWS_TRAILING_DAYS)
    if not obs:
        return f"No Tiingo news count for {ticker} in the {NEWS_TRAILING_DAYS} days to {as_of}"
    latest = obs[-1]
    at_least = ">=" if latest.value >= NEWS_FETCH_CAP else ""  # at Tiingo's fetch cap
    return (
        f"Tiingo news, not scored: {at_least}{latest.value:.0f} articles returned for the "
        f"{NEWS_TRAILING_DAYS} days to {latest.obs_date}"
    )


def score_narrative(
    conn,
    ticker: str,
    as_of_date: date | None = None,
) -> LayerResult:
    """Layer 5: Is there a narrative catalyst? Nothing scored today.

    The Tiingo news count is shown, not scored (NEWS_NOT_SCORED); wiki
    attention and GDELT have no writer. The layer has no data and stays in the
    denominator at its full weight.
    """
    as_of = _resolve_as_of_date(as_of_date)
    signals = [NEWS_NOT_SCORED, _describe_news(conn, ticker, as_of), WIKI_UNAVAILABLE, GDELT_UNAVAILABLE]
    return _layer_result("NARRATIVE", 0.0, 0.10, signals, False)


def _describe_equity_pcr(conn, as_of: date) -> str:
    """FLOW's equity put/call as a signal; it never scores (EQUITY_PCR_NOT_SCORED)."""
    obs = _read_known(conn, EQUITY_PCR_SERIES, EQUITY_PCR_SOURCE, as_of, EQUITY_PCR_FRESH_DAYS)
    if not obs:
        return f"No equity put/call in the {EQUITY_PCR_FRESH_DAYS} days to {as_of}"
    return f"Equity put/call, not scored: {obs[-1].value:.2f} ({obs[-1].obs_date})"


def score_flow(
    conn,
    ticker: str,
    price: pd.Series,
    as_of_date: date | None = None,
) -> LayerResult:
    """Layer 6: Is money actually moving? Volatility spike; options inputs not scored.

    The equity put/call ratio (market-wide) is shown with its date, adds no
    points and never makes FLOW available (EQUITY_PCR_NOT_SCORED).
    """
    as_of = _resolve_as_of_date(as_of_date)
    score = 0.0
    signals = [WHALE_NOT_SCORED, EQUITY_PCR_NOT_SCORED, _describe_equity_pcr(conn, as_of)]
    has_data = False

    if price.empty or len(price) < 20:
        return _layer_result("FLOW", 0, 0.10, signals, False)

    # Volume spike (from price data — use returns as proxy if no volume)
    returns = price.pct_change().dropna()
    recent_vol = returns.tail(5).abs().mean()
    baseline = len(returns) >= FLOW_BASELINE_RETURNS
    avg_vol = returns.tail(FLOW_BASELINE_RETURNS).abs().mean() if baseline else recent_vol
    # With a 60-day baseline the comparison is a measurement either way; a
    # quiet tape scores 0 with data, not "no data".
    has_data = bool(baseline and avg_vol > 0)

    if avg_vol > 0 and recent_vol > avg_vol * 2:
        has_data = True
        score += 5
        signals.append(f"Volatility spike ({recent_vol/avg_vol:.1f}x normal)")
    elif avg_vol > 0 and recent_vol > avg_vol * 1.5:
        has_data = True
        score += 2
        signals.append(f"Elevated activity ({recent_vol/avg_vol:.1f}x)")
    elif has_data:
        signals.append(f"No volatility spike ({recent_vol/avg_vol:.1f}x the 60d mean)")

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
CONVICTION_MIN_COVERAGE = 0.8
# Alert ladder, highest first: (level, minimum score). Below the last is PASS.
ALERT_THRESHOLDS: tuple[tuple[str, int], ...] = (
    ("CONVICTION", 85),
    ("FIRE", 70),
    ("SCALE", 55),
    ("PILOT", 40),
    ("WATCH", 25),
)


def _pct(weighted: float, weighted_max: float) -> int:
    return max(0, min(100, int(round(weighted / weighted_max * 100)))) if weighted_max > 0 else 0


def _layer_bound(layer: LayerResult, extreme: Mapping[str, float]) -> float:
    """The layer's weighted contribution with its unknown part at ``extreme``.

    A missing layer is all unknown: its full weight at the extreme (any
    market-wide points it scored at low trust are not ticker evidence). An
    available layer keeps its measured score * trust and puts the untrusted
    share of its weight (full trust - trust) at the extreme.
    """
    full = layer.denominator_trust
    if not layer.data_available:
        return extreme[layer.name] * full
    return layer.score * layer.trust + extreme[layer.name] * (full - layer.trust)


def _alert_level(score: int, coverage: float) -> str:
    """Alert for ``score``; thin coverage (< 50 %) caps the alert at WATCH."""
    level = "PASS"
    for name, floor in ALERT_THRESHOLDS:
        if score >= floor and (name != "CONVICTION" or coverage >= CONVICTION_MIN_COVERAGE):
            level = name
            break
    if coverage < MIN_COVERAGE_FOR_ALERT and level in {"CONVICTION", "FIRE", "SCALE", "PILOT"}:
        level = "WATCH"
    return level


def score_ticker(
    engine: Engine,
    ticker: str,
    as_of_date: date | None = None,
    ftd_series_ids: Mapping[str, tuple[str, ...]] | None = None,
) -> ConvictionReport:
    """Score a single ticker across all 7 layers.

    ``ftd_series_ids``: scan_all's one-pass _ftd_series_ids for the universe;
    None looks up this ticker alone.
    """
    as_of = _resolve_as_of_date(as_of_date)
    with engine.connect() as conn:
        price = _load_price(conn, ticker, as_of_date=as_of)
        if ftd_series_ids is None:
            ftd_series_ids = _ftd_series_ids(conn, [ticker], as_of)

        named_layers: list[tuple[str, LayerResult | None]] = [
            ("SETUP", score_setup(conn, ticker, price, as_of_date=as_of)),
            ("COMPANY", score_company(conn, ticker, as_of_date=as_of)),
            ("SMART_MONEY", score_smart_money(conn, ticker, as_of_date=as_of)),
            ("CROWD", score_crowd(conn, ticker, as_of_date=as_of, ftd_series_ids=ftd_series_ids)),
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
    # too-short price history lowers the numerator weight only, so a missing
    # layer never lifts total_score above that layer read neutral. Against an
    # adverse reading it still can (SMART_MONEY -2 lost), so the alert comes
    # from score_low, where every unknown sits at its worst attainable value
    # (Codex HOLD on #1394 7da0c760).
    weighted_max = sum(layer.max_score * layer.denominator_trust for layer in present_layers)
    total = _pct(sum(layer.score * layer.trust for layer in present_layers), weighted_max)
    # Unknowns at their worst / best attainable value: a missing layer over
    # its full weight, a partly trusted layer over its untrusted share.
    score_low = _pct(sum(_layer_bound(layer, LAYER_MIN_SCORE) for layer in present_layers), weighted_max)
    score_high = _pct(sum(_layer_bound(layer, LAYER_ATTAINABLE_SCORE) for layer in present_layers), weighted_max)
    max_attainable = _pct(
        sum(LAYER_ATTAINABLE_SCORE[layer.name] * LAYER_FULL_TRUST[layer.name] for layer in present_layers),
        sum(LAYER_MAX_SCORE[layer.name] * LAYER_FULL_TRUST[layer.name] for layer in present_layers),
    )

    # Confidence = how much data we actually have across the 7 designed layers
    data_coverage = len(available_layers) / len(named_layers)
    confidence = total * data_coverage

    return ConvictionReport(
        ticker=ticker,
        total_score=total,
        confidence_pct=round(confidence, 1),
        layers=present_layers,
        # From score_low, so lost evidence never promotes the alert.
        alert_level=_alert_level(score_low, data_coverage),
        timestamp=pd.Timestamp.now().isoformat(),
        unavailable_layers=unavailable_layers,
        score_low=score_low,
        score_high=score_high,
        max_attainable_score=max_attainable,
        dormant_alert_levels=tuple(
            name for name, floor in ALERT_THRESHOLDS if floor > max_attainable
        ),
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
        # One FTD symbol lookup for the whole universe instead of one per ticker.
        ftd_series_ids = _ftd_series_ids(conn, tickers, as_of)

    reports = []

    for ticker in tickers:
        report = score_ticker(engine, ticker, as_of_date=as_of, ftd_series_ids=ftd_series_ids)
        if report.total_score >= min_score:
            reports.append(report)

    return sorted(reports, key=lambda r: -r.total_score)


def print_report(report: ConvictionReport) -> None:
    """Pretty-print a conviction report."""
    log.info(
        "\n  {t:8s} [{level:10s}] Score={s:3d}/100 (unknowns {lo}..{hi}; alert from {lo})  Confidence={c:.0f}%",
        t=report.ticker, level=report.alert_level,
        s=report.total_score, lo=report.score_low, hi=report.score_high, c=report.confidence_pct,
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
