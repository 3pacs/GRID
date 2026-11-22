"""Synthetic MOVE proxy: Treasury-ETF implied volatility from TLT & IEF option chains.

GRID has no ICE BofA MOVE feed. This module derives a constant-maturity 30-day
ATM implied-volatility proxy from GRID's own option captures for ``TLT``
(20Y+ Treasuries) and ``IEF`` (7-10Y Treasuries):

  - ``options_rates_vol:tlt_iv_30d``        (constant-maturity 30d ATM IV, decimal)
  - ``options_rates_vol:ief_iv_30d``        (constant-maturity 30d ATM IV, decimal)
  - ``options_rates_vol:tlt_iv_term_slope`` (30d IV minus 90d IV; >0 = inverted, stress)

Existing coverage: ``ingestion/options.py`` already writes a near-expiry
``{ticker}_iv_atm`` and ``{ticker}_term_slope`` for every ticker it captures.
What this adds is total-variance interpolation to a fixed 30/90-day maturity,
so the level does not jump when the front expiry rolls.

Which rows it reads. The chain comes from ``store/options_provenance.py``'s
``VISIBLE_SNAPSHOTS`` (the ``options_snapshots`` view: the latest complete
capture batch per ticker and day) with Schwab days excluded
(``include_schwab=False``). The values land in ``raw_series`` and from there
in ``resolved_series``, which has no per-viewer filter, so a Schwab-captured
chain must never feed them (the same rule ``ingestion/options.py`` follows
with ``provider_is_schwab``). Reading the view also keeps one batch per day:
``options_snapshots_all`` holds every intraday batch.

Point in time. ``obs_date`` is the capture day; the row's ``pull_timestamp``
(when this job ran, never before the capture) is its known-at, and the batch
id and completion time are kept in ``raw_payload``. A day in the future is
refused.

Fail-closed quote hygiene (:func:`quote_rejection`, applied before any
comparison, interpolation or write; each refused quote is counted by reason):
  * strike, bid, ask and IV must be finite numbers (``math.isfinite`` first, so
    a NaN never slips past a comparison; booleans are refused);
  * ``strike > 0``; ``bid > 0`` and ``ask > bid`` (a two-sided market: a zero
    bid or a locked/crossed quote gives no usable mid); the mid must be finite
    and positive and the relative spread ``(ask - bid) / mid <= 0.25``;
  * IV within the declared plausible range ``[MIN_IV, MAX_IV] = [0.02, 2.0]``
    (decimal; a percent-scaled feed such as ``16.0`` fails closed);
  * the option type is exactly call/put (``call``/``put``/``C``/``P``);
  * two clean quotes for one expiry, side and strike that disagree are a
    conflict: that strike is dropped for that side, never picked by order;
  * an expiry counts only ``1 <= min_dte <= DTE <= max_dte`` days after the
    capture day (default 7..180), so time to expiry is always positive.
Then per expiry it picks the ATM strike where ``|C_mid - P_mid|`` is smallest
(forward ATM by put-call parity; ties go to the lower strike), requiring a
clean call and put there; interpolates total variance (``sigma^2 * T``)
linearly between the expiries bracketing the target; and only uses a
one-sided value when the nearest expiry is within 12 days of the target. The
constant-maturity result must itself be finite and inside ``[MIN_IV, MAX_IV]``,
and the term slope finite, or nothing is emitted. A database row that cannot
be converted (bad expiry, non-numeric field) is skipped and counted instead of
aborting the day.

Not scheduled anywhere yet: nothing calls :meth:`OptionsRatesVolPuller.pull_for_date`
except tests.
"""

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller
from store.options_provenance import VISIBLE_SNAPSHOTS, visible_sql

MAX_REL_SPREAD = 0.25
#: Declared plausible range for a Treasury-ETF implied volatility (decimal).
MIN_IV = 0.02
MAX_IV = 2.00
MAX_ONE_SIDED_GAP_DAYS = 12
_SIDES: dict[str, str] = {"call": "C", "c": "C", "put": "P", "p": "P"}
RATES_VOL_TICKERS: tuple[str, ...] = ("TLT", "IEF")

#: One ticker-day chain, Schwab days excluded (see the module docstring).
CHAIN_SQL: str = (
    "SELECT os.expiry, os.opt_type, os.strike, os.bid, os.ask, os.implied_vol, "
    "os.capture_batch_id, os.capture_completed_at "
    "FROM " + VISIBLE_SNAPSHOTS + " os "
    "WHERE os.ticker = :tk AND os.snap_date = :sd "
    "AND os.bid IS NOT NULL AND os.ask IS NOT NULL AND os.implied_vol IS NOT NULL"
)


@dataclass(frozen=True)
class OptionQuoteRow:
    expiry: date
    opt_type: str   # "call"/"put" (schema) or "C"/"P"
    strike: float
    bid: float
    ask: float
    iv: float


def _finite(value: Any) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value))


def _side(opt_type: Any) -> str | None:
    return _SIDES.get(str(opt_type).strip().lower()) if isinstance(opt_type, str) else None


def quote_rejection(q: OptionQuoteRow) -> str | None:
    """Why ``q`` is unusable, or None when it passes every bound (see the module docstring)."""
    if not isinstance(q.expiry, date) or isinstance(q.expiry, datetime):
        return "bad_expiry"
    if _side(q.opt_type) is None:
        return "unknown_option_type"
    for name in ("strike", "bid", "ask", "iv"):
        if not _finite(getattr(q, name)):
            return f"non_finite_{name}"
    if q.strike <= 0:
        return "non_positive_strike"
    if q.bid <= 0:
        return "non_positive_bid"
    if q.ask <= q.bid:
        return "locked_or_crossed_quote"
    mid = 0.5 * (q.bid + q.ask)
    if not (math.isfinite(mid) and mid > 0):
        return "non_positive_mid"
    spread = (q.ask - q.bid) / mid
    if not (math.isfinite(spread) and spread <= MAX_REL_SPREAD):
        return "wide_spread"
    if not (MIN_IV <= q.iv <= MAX_IV):
        return "iv_out_of_range"
    return None


def screen_quotes(quotes: Iterable[OptionQuoteRow]) -> tuple[list[OptionQuoteRow], dict[str, int]]:
    """Clean quotes (one per expiry/side/strike) and the count of each rejection reason.

    Order-independent: a conflicting duplicate drops the strike for that side.
    """
    reasons: Counter[str] = Counter()
    groups: dict[tuple[date, str, float], set[OptionQuoteRow]] = {}
    for q in quotes:
        why = quote_rejection(q)
        if why is not None:
            reasons[why] += 1
            continue
        key = (q.expiry, _side(q.opt_type) or "", float(q.strike))
        groups.setdefault(key, set()).add(
            OptionQuoteRow(q.expiry, key[1], key[2], float(q.bid), float(q.ask), float(q.iv))
        )
    clean: list[OptionQuoteRow] = []
    for key in sorted(groups):
        same = groups[key]
        if len(same) > 1:
            reasons["conflicting_duplicate_quote"] += len(same)
            continue
        clean.extend(same)
    return clean, dict(reasons)


def compute_expiry_atm_iv(quotes: list[OptionQuoteRow]) -> float | None:
    """ATM IV for one expiry, at the strike where call and put mids are closest.

    Pure function; quotes pass :func:`screen_quotes` first. Returns ``None``
    when no clean call/put pair exists. Raises ``ValueError`` when the quotes
    span more than one expiry.
    """
    clean, _ = screen_quotes(quotes)
    if len({q.expiry for q in clean}) > 1:
        raise ValueError("compute_expiry_atm_iv takes the quotes of one expiry")
    calls = {q.strike: q for q in clean if q.opt_type == "C"}
    puts = {q.strike: q for q in clean if q.opt_type == "P"}
    common = sorted(set(calls) & set(puts))
    if not common:
        return None
    k_atm = min(
        common,
        key=lambda k: abs(0.5 * (calls[k].bid + calls[k].ask) - 0.5 * (puts[k].bid + puts[k].ask)),
    )
    iv = 0.5 * (calls[k_atm].iv + puts[k_atm].iv)
    return iv if math.isfinite(iv) else None


def _plausible_iv(iv: float | None) -> float | None:
    return iv if iv is not None and math.isfinite(iv) and MIN_IV <= iv <= MAX_IV else None


def interpolate_constant_maturity_iv(
    snap_date: date,
    quotes: list[OptionQuoteRow],
    *,
    target_dte: int = 30,
    min_dte: int = 7,
    max_dte: int = 180,
) -> float | None:
    """Total-variance-interpolated ATM IV at ``target_dte`` calendar days.

    Requires ``1 <= min_dte <= target_dte <= max_dte`` (``ValueError``
    otherwise). Returns None unless the result is finite and in
    ``[MIN_IV, MAX_IV]``.
    """
    if not isinstance(snap_date, date) or isinstance(snap_date, datetime):
        raise ValueError("snap_date must be a date")
    if not (1 <= min_dte <= target_dte <= max_dte):
        raise ValueError(f"need 1 <= min_dte <= target_dte <= max_dte, got {min_dte}, {target_dte}, {max_dte}")
    clean, _ = screen_quotes(quotes)
    by_exp: dict[date, list[OptionQuoteRow]] = {}
    for q in clean:
        dte = (q.expiry - snap_date).days
        if min_dte <= dte <= max_dte:
            by_exp.setdefault(q.expiry, []).append(q)

    points: list[tuple[int, float]] = []
    for exp_d, exp_quotes in sorted(by_exp.items()):
        atm_iv = compute_expiry_atm_iv(exp_quotes)
        if atm_iv is not None:
            points.append(((exp_d - snap_date).days, atm_iv))

    if not points:
        return None

    for dte, iv in points:
        if dte == target_dte:
            return _plausible_iv(iv)

    below = [p for p in points if p[0] < target_dte]
    above = [p for p in points if p[0] > target_dte]

    if below and above:
        d1, iv1 = below[-1]
        d2, iv2 = above[0]
        w = (target_dte - d1) / (d2 - d1)
        var_interp = (1.0 - w) * (iv1 * iv1 * d1) + w * (iv2 * iv2 * d2)
        if not (math.isfinite(var_interp) and var_interp > 0):
            return None
        return _plausible_iv(math.sqrt(var_interp / target_dte))

    # One-sided value only when the nearest expiry is close to the target.
    nearest_dte, nearest_iv = min(points, key=lambda p: (abs(p[0] - target_dte), p[0]))
    if abs(nearest_dte - target_dte) <= MAX_ONE_SIDED_GAP_DAYS:
        return _plausible_iv(nearest_iv)
    return None


_ISO_DATE_PREFIX = re.compile(r"\d{4}-\d{2}-\d{2}(?:$|[T ])")


def _as_date(value: Any) -> date:
    """A database expiry as a ``date``; ``ValueError`` for numbers and non-ISO text.

    Only a ``date``/``datetime`` or a string starting ``YYYY-MM-DD`` is read: a
    number such as ``20261210`` (which ``date.fromisoformat`` would accept once
    turned into text) or an epoch count is refused, never converted.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and _ISO_DATE_PREFIX.match(value.strip()):
        return date.fromisoformat(value.strip()[:10])
    raise ValueError(f"expiry {value!r} is not a date or an ISO YYYY-MM-DD string")


class OptionsRatesVolPuller(BasePuller):
    """Derives synthetic MOVE rates-vol series from TLT/IEF option captures."""

    SOURCE_NAME: str = "options_rates_vol"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "internal://options_snapshots",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 25,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)

    def _load_quotes_for_day(
        self, conn: Any, ticker: str, snap_d: date,
    ) -> tuple[list[OptionQuoteRow], dict[str, Any]]:
        """The ticker-day chain (latest batch, no Schwab day), its provenance and row rejections.

        A row that cannot be converted is skipped and counted under
        ``provenance["unconvertible_rows"]``; it never aborts the day.
        """
        rows = conn.execute(
            visible_sql(CHAIN_SQL, include_schwab=False),
            {"tk": ticker, "sd": snap_d},
        ).fetchall()
        out: list[OptionQuoteRow] = []
        batches: set[str] = set()
        completed: list[str] = []
        unconvertible: Counter[str] = Counter()
        for exp_d, otype, strike, bid, ask, iv, batch_id, completed_at in rows:
            if batch_id is not None:
                batches.add(str(batch_id))
            if completed_at is not None:
                completed.append(
                    completed_at.isoformat() if hasattr(completed_at, "isoformat") else str(completed_at)
                )
            if exp_d is None or otype is None:
                unconvertible["missing_expiry_or_type"] += 1
                continue
            try:
                expiry = _as_date(exp_d)
            except (TypeError, ValueError):
                unconvertible["bad_expiry"] += 1
                continue
            try:
                numbers = [float(v) for v in (strike, bid, ask, iv)]
            except (TypeError, ValueError, OverflowError):
                unconvertible["non_numeric_field"] += 1
                continue
            out.append(OptionQuoteRow(expiry, str(otype), *numbers))
        provenance = {
            "capture_batch_ids": sorted(batches),
            "capture_completed_at": max(completed) if completed else None,
            "unconvertible_rows": dict(unconvertible),
        }
        return out, provenance

    def pull_for_date(self, snap_d: date) -> dict[str, Any]:
        """Compute and store the rates-vol series for one capture day.

        Writes a value only when it is finite (and an IV only inside
        ``[MIN_IV, MAX_IV]``); every quote rejection is counted in the result.
        """
        if not isinstance(snap_d, date) or isinstance(snap_d, datetime):
            raise ValueError("snap_d must be a date")
        if snap_d > date.today():
            return {"status": "FAILED", "snap_date": snap_d.isoformat(), "rows_inserted": 0,
                    "error": "snap_date is in the future"}
        inserted = 0
        metrics: dict[str, float | None] = {}
        rejected: dict[str, dict[str, int]] = {}
        with self.engine.begin() as conn:
            for ticker in RATES_VOL_TICKERS:
                raw_quotes, provenance = self._load_quotes_for_day(conn, ticker, snap_d)
                quotes, reasons = screen_quotes(raw_quotes)
                rejected[ticker] = {**provenance["unconvertible_rows"], **reasons}
                provenance = {**provenance, "rejected_quotes": reasons}
                iv30 = _plausible_iv(interpolate_constant_maturity_iv(snap_d, quotes, target_dte=30))
                sid30 = f"options_rates_vol:{ticker.lower()}_iv_30d"
                metrics[sid30] = iv30
                if iv30 is not None:
                    existing = self._get_existing_dates(sid30, conn, start_date=snap_d, end_date=snap_d)
                    if snap_d not in existing:
                        self._insert_raw(
                            conn=conn,
                            series_id=sid30,
                            obs_date=snap_d,
                            value=round(iv30, 6),
                            raw_payload={"ticker": ticker, "target_dte": 30,
                                         "basis": "pcp_atm_total_variance_interp", **provenance},
                        )
                        inserted += 1

                if ticker == "TLT":
                    iv90 = _plausible_iv(interpolate_constant_maturity_iv(
                        snap_d, quotes, target_dte=90, min_dte=30, max_dte=240))
                    if iv30 is not None and iv90 is not None and math.isfinite(iv30 - iv90):
                        slope = iv30 - iv90
                        sid_sl = "options_rates_vol:tlt_iv_term_slope"
                        metrics[sid_sl] = slope
                        existing_sl = self._get_existing_dates(sid_sl, conn, start_date=snap_d, end_date=snap_d)
                        if snap_d not in existing_sl:
                            self._insert_raw(
                                conn=conn,
                                series_id=sid_sl,
                                obs_date=snap_d,
                                value=round(slope, 6),
                                raw_payload={"ticker": "TLT", "iv30": round(iv30, 6),
                                             "iv90": round(iv90, 6), **provenance},
                            )
                            inserted += 1
        log.info("options_rates_vol({d}): inserted={n} metrics={m} rejected={r}",
                 d=snap_d, n=inserted, m=metrics, r=rejected)
        status = "SUCCESS" if any(v is not None for v in metrics.values()) else "NO_DATA"
        return {"status": status, "snap_date": snap_d.isoformat(), "rows_inserted": inserted,
                "metrics": metrics, "rejected": rejected}
