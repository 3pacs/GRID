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

Fail-closed quote hygiene:
  * rejects quotes with ``bid <= 0``, ``ask <= bid``, relative spread
    ``(ask - bid) / mid > 0.25``, or IV outside ``[0.02, 2.0]``;
  * picks the ATM strike per expiry where ``|C_mid - P_mid|`` is smallest
    (forward ATM by put-call parity), requiring a clean call and put there;
  * interpolates total variance (``sigma^2 * T``) linearly between the
    expiries bracketing the target, and only uses a one-sided value when the
    nearest expiry is within 12 days of the target.

Not scheduled anywhere yet: nothing calls :meth:`OptionsRatesVolPuller.pull_for_date`
except tests.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller
from store.options_provenance import VISIBLE_SNAPSHOTS, visible_sql

MAX_REL_SPREAD = 0.25
MIN_IV = 0.02
MAX_IV = 2.00
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


def compute_expiry_atm_iv(quotes: list[OptionQuoteRow]) -> float | None:
    """ATM IV for one expiry, at the strike where call and put mids are closest.

    Pure function; returns ``None`` when no clean call/put pair exists.
    """
    calls: dict[float, OptionQuoteRow] = {}
    puts: dict[float, OptionQuoteRow] = {}
    for q in quotes:
        if not (q.bid > 0 and q.ask > q.bid and math.isfinite(q.iv) and MIN_IV <= q.iv <= MAX_IV):
            continue
        mid = 0.5 * (q.bid + q.ask)
        if mid <= 0 or (q.ask - q.bid) / mid > MAX_REL_SPREAD:
            continue
        cp = q.opt_type.upper()[:1]
        if cp == "C":
            calls[q.strike] = q
        elif cp == "P":
            puts[q.strike] = q

    common = sorted(set(calls) & set(puts))
    if not common:
        return None

    k_atm = min(
        common,
        key=lambda k: abs(
            0.5 * (calls[k].bid + calls[k].ask) - 0.5 * (puts[k].bid + puts[k].ask)
        ),
    )
    return 0.5 * (calls[k_atm].iv + puts[k_atm].iv)


def interpolate_constant_maturity_iv(
    snap_date: date,
    quotes: list[OptionQuoteRow],
    *,
    target_dte: int = 30,
    min_dte: int = 7,
    max_dte: int = 180,
) -> float | None:
    """Total-variance-interpolated ATM IV at ``target_dte`` calendar days."""
    by_exp: dict[date, list[OptionQuoteRow]] = {}
    for q in quotes:
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
            return iv

    below = [p for p in points if p[0] < target_dte]
    above = [p for p in points if p[0] > target_dte]

    if below and above:
        d1, iv1 = below[-1]
        d2, iv2 = above[0]
        w = (target_dte - d1) / (d2 - d1)
        var_interp = (1.0 - w) * (iv1 * iv1 * d1) + w * (iv2 * iv2 * d2)
        if var_interp <= 0:
            return None
        return math.sqrt(var_interp / target_dte)

    # One-sided value only when the nearest expiry is within 12 days of target.
    nearest_dte, nearest_iv = min(points, key=lambda p: abs(p[0] - target_dte))
    if abs(nearest_dte - target_dte) <= 12:
        return nearest_iv
    return None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


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
        """The ticker-day chain (latest batch, no Schwab day) and its batch provenance."""
        rows = conn.execute(
            visible_sql(CHAIN_SQL, include_schwab=False),
            {"tk": ticker, "sd": snap_d},
        ).fetchall()
        out: list[OptionQuoteRow] = []
        batches: set[str] = set()
        completed: list[str] = []
        for exp_d, otype, strike, bid, ask, iv, batch_id, completed_at in rows:
            if exp_d is None or otype is None:
                continue
            out.append(
                OptionQuoteRow(
                    expiry=_as_date(exp_d),
                    opt_type=str(otype),
                    strike=float(strike),
                    bid=float(bid),
                    ask=float(ask),
                    iv=float(iv),
                )
            )
            if batch_id is not None:
                batches.add(str(batch_id))
            if completed_at is not None:
                completed.append(
                    completed_at.isoformat() if hasattr(completed_at, "isoformat") else str(completed_at)
                )
        provenance = {
            "capture_batch_ids": sorted(batches),
            "capture_completed_at": max(completed) if completed else None,
        }
        return out, provenance

    def pull_for_date(self, snap_d: date) -> dict[str, Any]:
        """Compute and store the rates-vol series for one capture day."""
        if snap_d > date.today():
            return {"status": "FAILED", "snap_date": snap_d.isoformat(), "rows_inserted": 0,
                    "error": "snap_date is in the future"}
        inserted = 0
        metrics: dict[str, float | None] = {}
        with self.engine.begin() as conn:
            for ticker in RATES_VOL_TICKERS:
                quotes, provenance = self._load_quotes_for_day(conn, ticker, snap_d)
                iv30 = interpolate_constant_maturity_iv(snap_d, quotes, target_dte=30)
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
                    iv90 = interpolate_constant_maturity_iv(snap_d, quotes, target_dte=90, min_dte=30, max_dte=240)
                    if iv30 is not None and iv90 is not None:
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
        log.info("options_rates_vol({d}): inserted={n} metrics={m}", d=snap_d, n=inserted, m=metrics)
        status = "SUCCESS" if any(v is not None for v in metrics.values()) else "NO_DATA"
        return {"status": status, "snap_date": snap_d.isoformat(), "rows_inserted": inserted, "metrics": metrics}
