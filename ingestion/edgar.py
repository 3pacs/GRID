"""
GRID SEC EDGAR data ingestion module.

Pulls institutional holdings (13F), insider transactions (Form 4), and
corporate event filings (8-K) from SEC EDGAR using the ``edgartools`` library.
Stores results in ``raw_series`` with source 'SEC_EDGAR'.
"""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from typing import Any

import pandas as pd
from edgar import Company, get_filings, set_identity
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from ingestion.altdata.sec_13f_live import FILERS
from ingestion.base import BasePuller

# 13F filer CIK numbers for institutional-holdings tracking.
#
# GD0 §1.3 / §6 item 4 (owner decision, adopted 2026-09-28): this module used
# to carry its own hardcoded 50-fund CIK list that disagreed with both
# ``ingestion/altdata/institutional_flows.py`` and
# ``ingestion/altdata/sec_13f_live.py`` on the same CIK for different funds
# (e.g. CIK 1167483 was "Eton Park Capital" here, "Elliott Management" in
# institutional_flows.py, and "Tiger Global Management" — the
# verified-correct entry — in sec_13f_live.py). The owner decision retires
# this independent list in favor of ``sec_13f_live.FILERS``, the 13F
# writer's own map, verified correct: this is now a derived, zero-padded
# view over it, not a second copy that can drift out of sync. The resulting
# universe is smaller/different from the old list by design — it is the
# single verified set, not a superset of every previously (mis)tracked CIK.
TOP_HEDGE_FUND_CIKS: list[str] = [f.cik.zfill(10) for f in FILERS]

# Rate limit between EDGAR requests (be polite to SEC servers)
_RATE_LIMIT_DELAY: float = 0.12  # SEC asks for <=10 req/sec


class EDGARPuller(BasePuller):
    """Pulls SEC filing data from EDGAR into ``raw_series``.

    Supports three filing types:
    - 13F-HR: Quarterly institutional holdings
    - Form 4: Daily insider transactions
    - 8-K: Corporate event filings (used for sector velocity)

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for SEC_EDGAR.
    """

    SOURCE_NAME = "SEC_EDGAR"
    SOURCE_CONFIG = {"base_url": "https://www.sec.gov/cgi-bin/browse-edgar", "cost_tier": "FREE", "latency_class": "EOD", "pit_available": True, "revision_behavior": "RARE", "trust_score": "HIGH", "priority_rank": 12}

    def __init__(
        self,
        db_engine: Engine,
        identity: str = "GRID Trading System grid@localhost",
    ) -> None:
        """Initialise the EDGAR puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
            identity: User-agent identity string for SEC EDGAR compliance.
        """
        set_identity(identity)
        super().__init__(db_engine)
        log.info("EDGARPuller initialised — source_id={sid}", sid=self.source_id)

    # ------------------------------------------------------------------
    # 13F Institutional Holdings
    # ------------------------------------------------------------------

    def pull_13f_holdings(
        self,
        cik_list: list[str] | None = None,
        max_filings_per_fund: int = 4,
    ) -> dict[str, Any]:
        """Pull 13F-HR institutional holdings for top hedge funds.

        Stores aggregated portfolio metrics as series:
        - 13F:{CIK}:TOTAL_VALUE — total portfolio market value
        - 13F:{CIK}:NUM_POSITIONS — count of distinct holdings
        - 13F:{CIK}:TOP5_CONCENTRATION — top 5 holdings as % of total

        Parameters:
            cik_list: CIK numbers to pull. Defaults to TOP_HEDGE_FUND_CIKS.
            max_filings_per_fund: How many recent 13F filings to process per fund.

        Returns:
            dict: Result summary with rows_inserted and any errors.
        """
        if cik_list is None:
            cik_list = TOP_HEDGE_FUND_CIKS

        log.info("Pulling 13F holdings for {n} funds", n=len(cik_list))
        total_inserted = 0
        errors: list[str] = []

        for cik in cik_list:
            try:
                filings = get_filings(
                    form="13F-HR",
                    cik=cik,
                )
                if filings is None:
                    continue

                processed = 0
                for filing in filings:
                    if processed >= max_filings_per_fund:
                        break

                    try:
                        thirteenf = filing.obj()
                        holdings = thirteenf.infotable

                        if holdings is None or holdings.empty:
                            processed += 1
                            continue

                        filing_date = (
                            filing.filing_date
                            if isinstance(filing.filing_date, date)
                            else pd.Timestamp(filing.filing_date).date()
                        )

                        # Compute portfolio metrics
                        total_value = float(holdings["value"].sum()) if "value" in holdings.columns else 0.0
                        num_positions = len(holdings)

                        # Top 5 concentration
                        if "value" in holdings.columns and total_value > 0:
                            top5_value = float(
                                holdings.nlargest(5, "value")["value"].sum()
                            )
                            top5_pct = top5_value / total_value
                        else:
                            top5_pct = 0.0

                        metrics = {
                            f"13F:{cik}:TOTAL_VALUE": total_value,
                            f"13F:{cik}:NUM_POSITIONS": float(num_positions),
                            f"13F:{cik}:TOP5_CONCENTRATION": round(top5_pct, 6),
                        }

                        with self.engine.begin() as conn:
                            for sid, val in metrics.items():
                                if not self._row_exists(sid, filing_date, conn):
                                    conn.execute(
                                        text(
                                            "INSERT INTO raw_series "
                                            "(series_id, source_id, obs_date, value, "
                                            "raw_payload, pull_status) "
                                            "VALUES (:sid, :src, :od, :val, :payload, 'SUCCESS')"
                                        ),
                                        {
                                            "sid": sid,
                                            "src": self.source_id,
                                            "od": filing_date,
                                            "val": val,
                                            "payload": json.dumps({
                                                "cik": cik,
                                                "filing_date": str(filing_date),
                                                "num_positions": num_positions,
                                            }),
                                        },
                                    )
                                    total_inserted += 1

                        processed += 1
                    except Exception as exc:
                        log.debug(
                            "Could not parse 13F filing for CIK {cik}: {e}",
                            cik=cik, e=str(exc),
                        )
                        processed += 1

                    time.sleep(_RATE_LIMIT_DELAY)

            except Exception as exc:
                msg = f"13F pull failed for CIK {cik}: {exc}"
                log.warning(msg)
                errors.append(msg)

            time.sleep(_RATE_LIMIT_DELAY)

        log.info("13F pull complete — {n} rows inserted", n=total_inserted)
        return {
            "type": "13F",
            "rows_inserted": total_inserted,
            "funds_attempted": len(cik_list),
            "errors": errors,
            "status": "SUCCESS" if not errors else "PARTIAL",
        }

    # ------------------------------------------------------------------
    # Form 4 Insider Transactions
    # ------------------------------------------------------------------

    def pull_form4_transactions(
        self,
        tickers: list[str] | None = None,
        days_back: int = 7,
    ) -> dict[str, Any]:
        """Pull Form 4 insider transactions for tracked companies.

        Stores daily aggregated insider activity:
        - FORM4:{ticker}:NET_SHARES — net shares bought/sold
        - FORM4:{ticker}:NET_VALUE — net dollar value of transactions
        - FORM4:{ticker}:TXN_COUNT — number of transactions

        Parameters:
            tickers: Company tickers to track. Defaults to major indices components.
            days_back: How many days of recent filings to process.

        Returns:
            dict: Result summary.
        """
        if tickers is None:
            tickers = [
                "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
                "BRK-B", "JPM", "V", "JNJ", "UNH", "HD", "PG", "MA",
                "XOM", "BAC", "ABBV", "KO", "PFE", "COST", "MRK", "TMO",
                "AVGO", "PEP",
            ]

        log.info("Pulling Form 4 for {n} tickers", n=len(tickers))
        total_inserted = 0
        errors: list[str] = []

        for ticker in tickers:
            try:
                company = Company(ticker)
                form4_filings = company.get_filings(form="4")

                if form4_filings is None:
                    continue

                # Aggregate transactions by date
                daily_agg: dict[date, dict[str, float]] = {}

                for filing in form4_filings[:20]:  # Limit to recent filings
                    try:
                        filing_date = (
                            filing.filing_date
                            if isinstance(filing.filing_date, date)
                            else pd.Timestamp(filing.filing_date).date()
                        )

                        cutoff = date.today() - timedelta(days=days_back)
                        if filing_date < cutoff:
                            break

                        f4 = filing.obj()
                        if not hasattr(f4, "transactions") or f4.transactions is None:
                            continue

                        txns = f4.transactions
                        if hasattr(txns, "empty") and txns.empty:
                            continue

                        if filing_date not in daily_agg:
                            daily_agg[filing_date] = {
                                "net_shares": 0.0,
                                "net_value": 0.0,
                                "txn_count": 0.0,
                            }

                        for _, txn in txns.iterrows():
                            shares = float(txn.get("shares", txn.get("transactionShares", 0)) or 0)
                            price = float(txn.get("pricePerShare", txn.get("price", 0)) or 0)
                            acq_disp = str(txn.get("acquiredDisposedCode", txn.get("acquired_disposed", "A")))

                            if acq_disp == "D":
                                shares = -shares

                            daily_agg[filing_date]["net_shares"] += shares
                            daily_agg[filing_date]["net_value"] += shares * price
                            daily_agg[filing_date]["txn_count"] += 1

                    except Exception as exc:
                        log.debug(
                            "Could not parse Form 4 for {t}: {e}",
                            t=ticker, e=str(exc),
                        )

                    time.sleep(_RATE_LIMIT_DELAY)

                # Insert aggregated daily data
                with self.engine.begin() as conn:
                    for fdate, agg in daily_agg.items():
                        metrics = {
                            f"FORM4:{ticker}:NET_SHARES": agg["net_shares"],
                            f"FORM4:{ticker}:NET_VALUE": agg["net_value"],
                            f"FORM4:{ticker}:TXN_COUNT": agg["txn_count"],
                        }
                        for sid, val in metrics.items():
                            if not self._row_exists(sid, fdate, conn):
                                conn.execute(
                                    text(
                                        "INSERT INTO raw_series "
                                        "(series_id, source_id, obs_date, value, "
                                        "pull_status) "
                                        "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                                    ),
                                    {
                                        "sid": sid,
                                        "src": self.source_id,
                                        "od": fdate,
                                        "val": val,
                                    },
                                )
                                total_inserted += 1

            except Exception as exc:
                msg = f"Form 4 pull failed for {ticker}: {exc}"
                log.warning(msg)
                errors.append(msg)

        log.info("Form 4 pull complete — {n} rows inserted", n=total_inserted)
        return {
            "type": "FORM4",
            "rows_inserted": total_inserted,
            "tickers_attempted": len(tickers),
            "errors": errors,
            "status": "SUCCESS" if not errors else "PARTIAL",
        }

    # ------------------------------------------------------------------
    # 8-K Filing Counts by Sector
    # ------------------------------------------------------------------

    # Process-scoped breaker for the daily 8-K full-index endpoint. Once
    # SEC's WAF starts returning 403 for our identity (typically because
    # the daily index isn't yet generated for the current quarter), every
    # subsequent call within the cycle hits the same wall. Tripping this
    # once spares us 2 ERROR rows/day until the index appears.
    _PULL_8K_BREAKER: bool = False
    _PULL_8K_BREAKER_TS: float = 0.0
    _PULL_8K_BREAKER_TTL_S: int = 6 * 3600  # 6 hours

    def pull_8k_counts(
        self,
        days_back: int = 7,
    ) -> dict[str, Any]:
        """Pull 8-K filing counts as a velocity metric.

        Counts total 8-K filings in the recent window and stores as
        series 8K:TOTAL:COUNT. Sector-level breakdown is handled by
        sec_velocity.py for deeper historical analysis.

        Parameters:
            days_back: How many days of recent filings to count.

        Returns:
            dict: Result summary.
        """
        # Honour the 403-cooldown breaker — return SKIPPED with no log noise.
        import time as _t
        now = _t.time()
        if (
            EDGARPuller._PULL_8K_BREAKER
            and now - EDGARPuller._PULL_8K_BREAKER_TS
            < EDGARPuller._PULL_8K_BREAKER_TTL_S
        ):
            return {
                "type": "8K",
                "rows_inserted": 0,
                "errors": [],
                "status": "SKIPPED",
            }
        # Cooldown expired — clear the breaker and try again.
        EDGARPuller._PULL_8K_BREAKER = False

        log.info("Pulling 8-K filing counts (last {d} days)", d=days_back)
        total_inserted = 0
        errors: list[str] = []

        try:
            filings = get_filings(form="8-K")
            if filings is None:
                return {
                    "type": "8K",
                    "rows_inserted": 0,
                    "errors": ["No filings returned"],
                    "status": "PARTIAL",
                }

            # Count filings by date
            daily_counts: dict[date, int] = {}
            cutoff = date.today() - timedelta(days=days_back)

            for filing in filings:
                try:
                    filing_date = (
                        filing.filing_date
                        if isinstance(filing.filing_date, date)
                        else pd.Timestamp(filing.filing_date).date()
                    )

                    if filing_date < cutoff:
                        break

                    daily_counts[filing_date] = daily_counts.get(filing_date, 0) + 1
                except Exception as exc:
                    log.warning("EDGAR filing parse failed: {e}", e=exc)
                    continue

            with self.engine.begin() as conn:
                for fdate, count in daily_counts.items():
                    sid = "8K:TOTAL:COUNT"
                    if not self._row_exists(sid, fdate, conn):
                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, "
                                "pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {
                                "sid": sid,
                                "src": self.source_id,
                                "od": fdate,
                                "val": float(count),
                            },
                        )
                        total_inserted += 1

        except Exception as exc:
            err_str = str(exc)
            # SEC full-index 403 (e.g. .../full-index/2026/QTR1/form.gz):
            # quarter index isn't generated yet OR our UA was WAF-blocked.
            # Trip the breaker, log ONE warning, retry after the cooldown.
            if "403" in err_str and "full-index" in err_str:
                EDGARPuller._PULL_8K_BREAKER = True
                EDGARPuller._PULL_8K_BREAKER_TS = _t.time()
                log.warning(
                    "EDGAR full-index returned 403 — index likely not yet "
                    "generated for current quarter. Skipping 8-K count "
                    "pull for {h}h. Endpoint: {e}",
                    h=EDGARPuller._PULL_8K_BREAKER_TTL_S // 3600,
                    e=err_str[:200],
                )
                return {
                    "type": "8K",
                    "rows_inserted": 0,
                    "errors": [err_str],
                    "status": "SKIPPED",
                }
            msg = f"8-K count pull failed: {exc}"
            log.error(msg)
            errors.append(msg)

        log.info("8-K count pull complete — {n} rows inserted", n=total_inserted)
        return {
            "type": "8K",
            "rows_inserted": total_inserted,
            "errors": errors,
            "status": "SUCCESS" if not errors else "PARTIAL",
        }

    def pull_all(self) -> list[dict[str, Any]]:
        """Run all EDGAR pulls sequentially.

        Returns:
            list[dict]: Results from 13F, Form 4, and 8-K pulls.
        """
        log.info("Starting full EDGAR pull")
        results = []

        results.append(self.pull_13f_holdings())
        results.append(self.pull_form4_transactions())
        results.append(self.pull_8k_counts())

        total_rows = sum(r["rows_inserted"] for r in results)
        log.info("Full EDGAR pull complete — {n} total rows", n=total_rows)
        return results


if __name__ == "__main__":
    from db import get_engine

    puller = EDGARPuller(db_engine=get_engine())

    # Quick test: just pull 8-K counts
    result = puller.pull_8k_counts(days_back=3)
    print(f"8-K counts: {result}")
