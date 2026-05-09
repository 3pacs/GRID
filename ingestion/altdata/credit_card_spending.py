"""
GRID credit card spending + delinquency FRED puller (CAT-75).

Why this matters for the oracle
================================
Credit card outstanding + delinquency data is the most lagged-but-direct
read on the US consumer. It feeds two critical downstream intelligence
modules:

1. **Consumer spending momentum (upside lever)**
   Rising credit card loans outstanding — especially the weekly H.8 series
   ``CCLACBW027SBOG`` — is a coincident indicator of consumer demand. When
   balances expand faster than trend, retail earnings (XRT, DIS, AMZN) and
   the broader SPY revenue line follow within 30-60 days. This is the
   dominant signal for the retail sector component of GRID's SPY thesis.

2. **Credit cycle rollover (downside lever)**
   When delinquency rates (``DRCCLACBS``) and charge-offs (``CORCCACBS``)
   start rising *while* outstanding keeps climbing, the consumer is
   levering up into stress. This is the textbook setup for small-cap
   and regional-bank drawdowns. The ``DRCCLACBN`` series (banks not in
   top 100) leads the all-bank series by ~1 quarter because smaller banks
   have weaker underwriting and hit stress first.

Downstream consumers
--------------------
- ``intelligence/credit_cycle_phase.py`` (CAT-126): classifies the US
  credit cycle into {expansion, late_cycle, stress, contraction} using
  the delta between outstanding growth and delinquency delta. Needs all
  four series pulled here.
- ``features/fci.py`` (CAT-124, Financial Conditions Index): uses the
  ``TERMCBCCALLNS`` interest rate as the consumer-credit-cost component
  alongside the Aaa/Baa corporate spread.
- ``oracle/engine.py``: the credit_cycle_phase output is one of the five
  regime gates that adjust sector-level prediction weights.

Series pulled
-------------
- ``CCLACBW027SBOG`` — consumer credit card loans, all commercial banks (weekly level, $B)
- ``CCLACBM027NBOG`` — consumer credit card loans, small banks (monthly, $B)
- ``DRCCLACBS``      — delinquency rate on credit card loans, all commercial banks (quarterly %)
- ``DRCCLACBN``      — delinquency rate on credit card loans, banks NOT in top 100 (quarterly %)
- ``CORCCACBS``      — charge-off rate on credit card loans (quarterly %)
- ``TERMCBCCALLNS``  — commercial bank interest rate on credit card plans (quarterly %)

Graceful degradation
--------------------
If ``FRED_API_KEY`` is missing (local/dev environments), the puller logs
a warning and returns a zero-row result rather than crashing. All series
are pulled independently — a single series failure does not block the
others (partial success returned in the result dict).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# ──────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────

FRED_API_BASE: str = "https://api.stlouisfed.org/fred/series/observations"

# Map FRED series ID → (internal label, human description, units)
CREDIT_CARD_SERIES: dict[str, dict[str, str]] = {
    "CCLACBW027SBOG": {
        "label": "outstanding_all_banks_weekly",
        "description": "Consumer credit card loans, all commercial banks (weekly, $B)",
        "frequency": "weekly",
    },
    "CCLACBM027NBOG": {
        "label": "outstanding_small_banks_monthly",
        "description": "Consumer credit card loans, small banks (monthly, $B)",
        "frequency": "monthly",
    },
    "DRCCLACBS": {
        "label": "delinq_rate_all_banks",
        "description": "Delinquency rate on credit card loans, all commercial banks (quarterly %)",
        "frequency": "quarterly",
    },
    "DRCCLACBN": {
        "label": "delinq_rate_smaller_banks",
        "description": "Delinquency rate on credit card loans, banks NOT in top 100 (quarterly %)",
        "frequency": "quarterly",
    },
    "CORCCACBS": {
        "label": "charge_off_rate_all_banks",
        "description": "Charge-off rate on credit card loans, all commercial banks (quarterly %)",
        "frequency": "quarterly",
    },
    "TERMCBCCALLNS": {
        "label": "interest_rate_all_accounts",
        "description": "Commercial bank interest rate on credit card plans, all accounts (quarterly %)",
        "frequency": "quarterly",
    },
}

_REQUEST_TIMEOUT: int = 30
_RATE_LIMIT_DELAY: float = 0.3


# ──────────────────────────────────────────────────────────────────────────
# Dataclass
# ──────────────────────────────────────────────────────────────────────────


@dataclass
class CreditCardSnapshot:
    """Single-date aggregated snapshot of US credit card health.

    Not every field is populated on every observation date — the FRED
    series run at different frequencies (weekly / monthly / quarterly),
    so only the fields whose source series reports on the given date
    will be non-None.

    Attributes:
        date: Observation date.
        outstanding_usd: Total credit card outstanding in $B (all banks).
        delinq_pct: Delinquency rate (%) across all commercial banks.
        charge_off_pct: Charge-off rate (%) on credit card loans.
        interest_rate_pct: Commercial bank interest rate on credit cards (%).
    """

    date: date
    outstanding_usd: float | None = None
    delinq_pct: float | None = None
    charge_off_pct: float | None = None
    interest_rate_pct: float | None = None


# ──────────────────────────────────────────────────────────────────────────
# Puller
# ──────────────────────────────────────────────────────────────────────────


class CreditCardSpendingPuller(BasePuller):
    """Pulls credit card outstanding + delinquency data from FRED (CAT-75).

    Uses the FRED REST API via ``requests`` (no fedfred dependency) and
    stores each observation in ``raw_series`` with a ``credit_card:<label>``
    prefix so downstream consumers can distinguish these from the raw
    FRED pulls keyed by ticker.
    """

    SOURCE_NAME: str = "FRED"  # Reuses existing FRED source entry
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.stlouisfed.org/fred",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "SOMETIMES",
        "trust_score": "HIGH",
        "priority_rank": 10,
    }

    def __init__(self, api_key: str, db_engine: Engine) -> None:
        """Initialise the credit card spending puller.

        Parameters:
            api_key: FRED API key (may be empty — puller degrades gracefully).
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self._api_key = api_key or ""
        super().__init__(db_engine)
        if not self._api_key:
            log.warning(
                "CreditCardSpendingPuller: FRED_API_KEY not set — "
                "pull() will return zero rows (graceful degradation)."
            )
        log.info(
            "CreditCardSpendingPuller initialised — source_id={sid}, "
            "series_count={n}",
            sid=getattr(self, "source_id", None),
            n=len(CREDIT_CARD_SERIES),
        )

    # ── Low-level fetch ──────────────────────────────────────────────

    def _fetch_series(
        self,
        fred_series_id: str,
        start_date: date | None = None,
    ) -> list[dict[str, Any]]:
        """Fetch a single FRED series via the observations REST endpoint.

        Parameters:
            fred_series_id: FRED series identifier (e.g. ``CCLACBW027SBOG``).
            start_date: Optional lower bound for observations.

        Returns:
            List of observation dicts with keys ``date``, ``value``.
            Rows with FRED's "." (missing) sentinel are dropped.

        Raises:
            requests.RequestException: on network errors (caller handles).
            ValueError: on malformed response body.
        """
        params: dict[str, str] = {
            "series_id": fred_series_id,
            "api_key": self._api_key,
            "file_type": "json",
        }
        if start_date is not None:
            params["observation_start"] = start_date.isoformat()

        resp = requests.get(
            FRED_API_BASE, params=params, timeout=_REQUEST_TIMEOUT
        )
        resp.raise_for_status()

        body = resp.json()
        obs: list[dict[str, Any]] = body.get("observations", [])
        parsed: list[dict[str, Any]] = []
        for row in obs:
            raw_val = row.get("value")
            raw_dt = row.get("date")
            if raw_val is None or raw_val == "." or raw_dt is None:
                continue
            try:
                parsed.append(
                    {
                        "date": datetime.strptime(raw_dt, "%Y-%m-%d").date(),
                        "value": float(raw_val),
                    }
                )
            except (ValueError, TypeError) as exc:
                log.warning(
                    "FRED {sid}: skipping malformed row {r}: {e}",
                    sid=fred_series_id,
                    r=row,
                    e=str(exc),
                )
                continue
        return parsed

    # ── Public API ───────────────────────────────────────────────────

    def pull(
        self,
        start_date: date | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Fetch all configured credit card series from FRED.

        Parameters:
            start_date: Optional lower bound for observations. Defaults to
                2008-01-01 (start of modern credit-card reporting).

        Returns:
            Dict mapping internal label → list of observation dicts.
            Failed series return an empty list (partial success is fine).
        """
        if not self._api_key:
            log.warning(
                "CreditCardSpendingPuller.pull() called with empty "
                "FRED_API_KEY — returning empty payload."
            )
            return {cfg["label"]: [] for cfg in CREDIT_CARD_SERIES.values()}

        if start_date is None:
            start_date = date(2008, 1, 1)

        results: dict[str, list[dict[str, Any]]] = {}
        for fred_id, cfg in CREDIT_CARD_SERIES.items():
            label = cfg["label"]
            try:
                obs = self._fetch_series(fred_id, start_date=start_date)
                log.info(
                    "CreditCard {label} ({sid}): fetched {n} observations",
                    label=label,
                    sid=fred_id,
                    n=len(obs),
                )
                results[label] = obs
            except Exception as exc:
                # FRED upstream periodically returns HTTP 5xx — transient
                # and not actionable in our codebase. Downgrade those to
                # WARNING so errors.jsonl stays signal-rich; everything
                # else (auth/permission, parse errors, network bugs) keeps
                # ERROR severity for the operator.
                from ingestion._http_severity import is_warning_worthy

                resp = getattr(exc, "response", None)
                status = getattr(resp, "status_code", None)
                if is_warning_worthy(status):
                    log.warning(
                        "CreditCard {sid} fetch transient HTTP {s}: {e}",
                        sid=fred_id, s=status, e=str(exc),
                    )
                else:
                    log.error(
                        "CreditCard {sid} fetch failed: {e}",
                        sid=fred_id,
                        e=str(exc),
                    )
                results[label] = []
            time.sleep(_RATE_LIMIT_DELAY)
        return results

    def save_to_db(
        self,
        fetched: dict[str, list[dict[str, Any]]],
    ) -> dict[str, int]:
        """Upsert fetched observations into raw_series.

        Series IDs are stored as ``credit_card:<label>`` so they can be
        cleanly separated from the FRED-ticker-keyed rows already in
        raw_series via ``ingestion/fred.py``.

        Parameters:
            fetched: Output of ``pull()`` — dict of label → observation list.

        Returns:
            Dict of label → rows_inserted count.
        """
        per_series: dict[str, int] = {}
        with self.engine.begin() as conn:
            for label, rows in fetched.items():
                series_id = f"credit_card:{label}"
                inserted = 0
                if not rows:
                    per_series[label] = 0
                    continue

                existing = self._get_existing_dates(series_id, conn)
                for row in rows:
                    obs_date = row["date"]
                    value = row["value"]
                    if obs_date in existing:
                        continue
                    self._insert_raw(
                        conn=conn,
                        series_id=series_id,
                        obs_date=obs_date,
                        value=float(value),
                        raw_payload={
                            "fred_label": label,
                            "origin": "credit_card_spending_puller",
                        },
                    )
                    inserted += 1
                per_series[label] = inserted
        return per_series


# ──────────────────────────────────────────────────────────────────────────
# Module-level entrypoint (matches run_wage_tracker_puller signature)
# ──────────────────────────────────────────────────────────────────────────


def run_credit_card_puller(engine: Engine) -> dict[str, Any]:
    """Run the CAT-75 credit card spending puller end-to-end.

    Reads ``FRED_API_KEY`` from ``config.settings``, fetches every
    configured series, upserts into raw_series, and returns a summary
    dict in the same shape as ``run_wage_tracker_puller``.

    Parameters:
        engine: SQLAlchemy engine connected to the GRID database.

    Returns:
        Dict with keys:
          - ``fetched``: total rows fetched across all series
          - ``inserted``: total rows inserted into raw_series
          - ``series``: per-label dict with ``fetched`` + ``inserted`` counts
    """
    try:
        from config import settings

        api_key = getattr(settings, "FRED_API_KEY", "") or getattr(
            settings, "fred_api_key", ""
        )
    except Exception as exc:
        log.warning(
            "run_credit_card_puller: failed to read config.settings ({e}) — "
            "treating as missing API key",
            e=str(exc),
        )
        api_key = ""

    puller = CreditCardSpendingPuller(api_key=api_key, db_engine=engine)

    if not api_key:
        return {
            "fetched": 0,
            "inserted": 0,
            "series": {
                cfg["label"]: {"fetched": 0, "inserted": 0}
                for cfg in CREDIT_CARD_SERIES.values()
            },
            "status": "SKIPPED_NO_API_KEY",
        }

    try:
        fetched = puller.pull()
    except Exception as exc:
        log.error("CreditCardSpendingPuller.pull() failed: {e}", e=str(exc))
        return {
            "fetched": 0,
            "inserted": 0,
            "series": {},
            "status": "FAILED",
            "error": str(exc),
        }

    try:
        inserted = puller.save_to_db(fetched)
    except Exception as exc:
        log.error("CreditCardSpendingPuller.save_to_db() failed: {e}", e=str(exc))
        return {
            "fetched": sum(len(v) for v in fetched.values()),
            "inserted": 0,
            "series": {
                label: {"fetched": len(rows), "inserted": 0}
                for label, rows in fetched.items()
            },
            "status": "FAILED",
            "error": str(exc),
        }

    total_fetched = sum(len(v) for v in fetched.values())
    total_inserted = sum(inserted.values())
    per_series: dict[str, dict[str, int]] = {}
    for label, rows in fetched.items():
        per_series[label] = {
            "fetched": len(rows),
            "inserted": inserted.get(label, 0),
        }

    log.info(
        "CreditCard puller complete — {f} fetched, {i} inserted across {n} series",
        f=total_fetched,
        i=total_inserted,
        n=len(CREDIT_CARD_SERIES),
    )
    return {
        "fetched": total_fetched,
        "inserted": total_inserted,
        "series": per_series,
        "status": "SUCCESS",
    }


if __name__ == "__main__":  # pragma: no cover
    from db import get_engine

    result = run_credit_card_puller(get_engine())
    print(result)
