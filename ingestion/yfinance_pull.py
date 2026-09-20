"""
GRID yfinance data ingestion module.

Downloads OHLCV market data from Yahoo Finance via the ``yfinance`` library
and stores each field as a separate entry in ``raw_series``.
"""

from __future__ import annotations

import inspect
import logging
import re
from datetime import date
from typing import Any, Callable

import pandas as pd
import yfinance as yf
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

# yfinance logs missing/delisted symbols at ERROR level internally. The puller
# already downgrades those outcomes to PARTIAL/SKIPPED, so keep the third-party
# logger from polluting production error scans.
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# fable-hermes-repair-bound follow-up (2026-09-19), review Check 2a: one
# yf.download() call cannot be interrupted by should_continue — that is only
# polled between tickers (see pull_all below). If the installed yfinance
# version's yf.download() accepts a `timeout` kwarg, pull_ticker passes a
# bounded one (_YF_DOWNLOAD_TIMEOUT_SECONDS) so a single hung HTTP call can't
# run indefinitely. Checked via inspect.signature (not a version-string
# comparison) so this stays correct across yfinance upgrades. requirements.txt
# pins yfinance>=1.5.1; the environment this was verified against has 1.7.0,
# whose yf.download() already accepts and defaults `timeout=10` — this module
# passes an explicit, slightly larger bound instead of relying on that
# upstream default, so the behavior doesn't silently change if yfinance drops
# or alters its own default in a future version.
try:
    _YF_DOWNLOAD_ACCEPTS_TIMEOUT = "timeout" in inspect.signature(yf.download).parameters
except (TypeError, ValueError):
    _YF_DOWNLOAD_ACCEPTS_TIMEOUT = False
_YF_DOWNLOAD_TIMEOUT_SECONDS = 30

# Default tickers to pull
YF_TICKER_LIST: list[str] = [
    # US Equity Indices (ETF proxies — Tiingo compatible)
    "SPY", "DIA", "QQQ", "IWM",
    # Also keep originals for yfinance fallback
    "^GSPC", "^DJI", "^IXIC", "^RUT", "^VIX",
    # Sector ETFs
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLB", "XLC",
    # Thematic/Subsector ETFs (sector_map proxies)
    "SMH", "KRE", "ICLN", "LIT", "XBI", "ITA",
    # Sector-map companies
    "TSM",
    # Bond ETFs
    "TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "EMB", "MUB",
    # Commodity ETFs
    "GLD", "SLV", "USO", "DBA", "PDBC",
    # Currency
    "UUP", "FXE", "FXY", "EEM", "DX-Y.NYB",
    # VIX Term Structure
    "^VIX9D", "^VIX3M", "^VIX6M",
    # Futures
    "HG=F", "GC=F", "SI=F", "CL=F",
    # FX Pairs
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCHF=X", "USDCAD=X", "NZDUSD=X",
    # Crypto (queried by layer_crypto.py)
    "BTC-USD", "ETH-USD",
    # International (sovereign/cross-border flow proxies)
    "FXI", "EWJ", "EWZ", "EFA",
    # Real yields / TIPS
    "TIP",
    # Copper / industrial metals (bellwether)
    "COPX",
    # High-yield / distress credit
    "SJNK", "BKLN", "ANGL",
]

# OHLCV fields to store individually
_FIELDS: list[str] = ["Open", "High", "Low", "Close", "Volume", "Adj Close"]
_FIELD_MAP: dict[str, str] = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Volume": "volume",
    "Adj Close": "adj_close",
}

_INVALID_TICKER_VALUES = {"", "N/A", "NA", "NONE", "NULL", "-"}
_CLASS_SHARE_RE = re.compile(r"^[A-Z]{1,5}\.[A-Z]$")


def _normalize_yahoo_ticker(ticker: str) -> str | None:
    """Return a yfinance-compatible ticker or None for junk symbols."""
    clean = str(ticker or "").strip().strip("\"'")
    if clean.upper() in _INVALID_TICKER_VALUES:
        return None
    if clean.startswith("(") and clean.endswith(")"):
        clean = clean[1:-1].strip()
    if not clean or clean.upper() in _INVALID_TICKER_VALUES:
        return None
    if ":" in clean or "/" in clean:
        return None
    if _CLASS_SHARE_RE.match(clean):
        clean = clean.replace(".", "-")
    return clean


class YFinancePuller(BasePuller):
    """Pulls OHLCV data from Yahoo Finance into ``raw_series``.

    Attributes:
        engine: SQLAlchemy engine for database writes.
        source_id: The ``source_catalog.id`` for the yfinance source.
    """

    SOURCE_NAME: str = "yfinance"

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the yfinance puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info("YFinancePuller initialised — source_id={sid}", sid=self.source_id)

    def pull_ticker(
        self,
        ticker: str,
        start_date: str | date,
        end_date: str | date | None = None,
        interval: str = "1d",
    ) -> dict[str, Any]:
        """Download OHLCV data for a single ticker and insert into raw_series.

        Each OHLCV field is stored as a separate series with the naming
        convention ``YF:{ticker}:{field}`` (e.g. ``YF:^GSPC:close``).

        Parameters:
            ticker: Yahoo Finance ticker symbol.
            start_date: Earliest date to download.
            end_date: Latest date (default: today).
            interval: Data frequency ('1d', '1wk', '1mo').

        Returns:
            dict: Result with keys ``ticker``, ``rows_inserted``, ``status``,
            ``errors``, and ``outcome``. ``outcome`` is the per-ticker
            classification consumed by the repair-summary logging in
            scripts/hermes_fixers.py::_retry_source — one of ``"inserted"``
            (rows_inserted > 0), ``"duplicate_only"`` (a non-empty download
            that inserted 0 rows — every date it returned was already
            present), ``"no_data"`` (the provider returned nothing for the
            requested window), or ``"error"`` (invalid ticker or an
            exception). A "checked" outcome (any of these four) is NOT a
            claim that this ticker's data is current through today — see
            pull_all's docstring.
        """
        yf_ticker = _normalize_yahoo_ticker(ticker)
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
            "outcome": "inserted",
        }
        if yf_ticker is None:
            result["status"] = "SKIPPED"
            result["outcome"] = "error"
            result["errors"].append("Invalid Yahoo ticker")
            log.warning("yfinance {t}: invalid ticker; skipping", t=ticker)
            return result

        log.info("Pulling yfinance ticker {t} from {sd}", t=yf_ticker, sd=start_date)

        try:
            download_kwargs: dict[str, Any] = {
                "start": str(start_date),
                "end": str(end_date) if end_date else None,
                "interval": interval,
                "progress": False,
            }
            if _YF_DOWNLOAD_ACCEPTS_TIMEOUT:
                download_kwargs["timeout"] = _YF_DOWNLOAD_TIMEOUT_SECONDS
            # auto_adjust is passed literally at the call site (not via the
            # kwargs dict) so tests/test_yfinance_auto_adjust_explicit.py can
            # verify the basis statically.
            df: pd.DataFrame = yf.download(
                yf_ticker, auto_adjust=False, **download_kwargs
            )

            # yfinance >=0.2.31 returns MultiIndex columns (field, ticker)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # Drop any rows where the index is not a valid datetime
            # (yfinance MultiIndex flattening can leak column names as rows)
            valid_idx = pd.to_datetime(df.index, errors="coerce").notna()
            if not valid_idx.all():
                log.warning(
                    "yfinance {t}: dropped {n} non-datetime index entries",
                    t=ticker,
                    n=int((~valid_idx).sum()),
                )
                df = df[valid_idx]

            if df is None or df.empty:
                log.warning("yfinance returned no data for {t}", t=yf_ticker)
                result["status"] = "PARTIAL"
                result["outcome"] = "no_data"
                result["errors"].append("No data returned")
                return result

            inserted = 0

            # yfinance's own `end` is exclusive (start="2026-09-10",
            # end="2026-09-11" returns only 09-10 — confirmed against the
            # live API, not assumed), so the last date col_data can actually
            # contain is one day before end_date, not end_date itself.
            # _get_existing_dates's own end_date parameter is a normal
            # inclusive bound; this converts once, before the per-field
            # loop, rather than passing yfinance's exclusive convention
            # into a shared method other callers would reasonably expect
            # to be inclusive on both ends.
            existing_end_bound = (
                (pd.Timestamp(end_date) - pd.Timedelta(days=1)).date()
                if end_date
                else None
            )

            with self.engine.begin() as conn:
                for col_name, field_key in _FIELD_MAP.items():
                    if col_name not in df.columns:
                        continue

                    series_id = f"YF:{yf_ticker}:{field_key}"
                    selected = df[col_name]
                    # Duplicate-column collapses (e.g. MultiIndex flattening
                    # producing repeated headers) yield a DataFrame instead of
                    # a Series, whose .items() iterates column names rather
                    # than the DatetimeIndex — which leaks strings like "Open"
                    # into obs_date and poisons the insert.
                    if isinstance(selected, pd.DataFrame):
                        log.warning(
                            "yfinance {t}: column {c} resolved to DataFrame "
                            "({n} duplicates); taking first column",
                            t=yf_ticker, c=col_name, n=selected.shape[1],
                        )
                        selected = selected.iloc[:, 0]
                    col_data = selected.dropna()
                    # Bounded to the same window this call already requested
                    # from yfinance: col_data can only contain dates inside
                    # [start_date, end_date), since yf.download() itself
                    # bounds the response — so checking existing dates
                    # outside that window can never affect the skip check
                    # below. end_date stays open when unset ("through
                    # today"): we can't have already inserted a future date.
                    existing_dates = self._get_existing_dates(
                        series_id,
                        conn,
                        start_date=pd.Timestamp(start_date).date(),
                        end_date=existing_end_bound,
                    )

                    for dt_idx, value in col_data.items():
                        # Defensive: reject any index entry that isn't a real
                        # date. yfinance occasionally leaks header strings
                        # ("Open", "Ticker") into the row index.
                        dt_parsed = pd.to_datetime(dt_idx, errors="coerce")
                        if pd.isna(dt_parsed):
                            log.warning(
                                "yfinance {t}:{f}: dropped non-date index "
                                "{v!r}",
                                t=yf_ticker, f=field_key, v=dt_idx,
                            )
                            continue
                        try:
                            float_val = float(value)
                        except (TypeError, ValueError):
                            log.warning(
                                "yfinance {t}:{f}: non-numeric value {v!r} "
                                "at {d}",
                                t=yf_ticker, f=field_key, v=value, d=dt_idx,
                            )
                            continue

                        # Sanity guard: skip obviously corrupt prices
                        if field_key == "adj_close" and (
                            float_val > 100_000 or float_val < 0.001
                        ):
                            log.warning(
                                "Suspicious adj_close for {t}: {v} on {d} — skipping",
                                t=ticker,
                                v=float_val,
                                d=dt_idx,
                            )
                            continue

                        obs_date_val = dt_parsed.date()
                        if obs_date_val in existing_dates:
                            continue

                        conn.execute(
                            text(
                                "INSERT INTO raw_series "
                                "(series_id, source_id, obs_date, value, pull_status) "
                                "VALUES (:sid, :src, :od, :val, 'SUCCESS')"
                            ),
                            {
                                "sid": series_id,
                                "src": self.source_id,
                                "od": obs_date_val,
                                "val": float_val,
                            },
                        )
                        existing_dates.add(obs_date_val)
                        inserted += 1

            result["rows_inserted"] = inserted
            # "duplicate-only" is established per logged ticker here: a
            # non-empty download whose every date was already present
            # (inserted == 0) is a successful CHECK of that ticker, not
            # evidence its data is stale — but it is also not evidence any
            # OTHER ticker is current. Do not generalise this per-ticker
            # result into a source- or asset-class-wide freshness claim.
            result["outcome"] = "inserted" if inserted > 0 else "duplicate_only"
            log.info(
                "yfinance {t}: checked — inserted {n} rows ({o})",
                t=yf_ticker, n=inserted, o=result["outcome"],
            )

        except Exception as exc:
            log.warning(
                "yfinance {t}: could not pull cleanly: {err}",
                t=yf_ticker,
                err=str(exc),
            )
            result["status"] = "SKIPPED"
            result["outcome"] = "error"
            result["errors"].append(str(exc))

        return result

    def pull_all(
        self,
        ticker_list: list[str] | None = None,
        start_date: str | date = "1990-01-01",
        should_continue: Callable[[], bool] | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Pull multiple tickers sequentially.

        Never stops on a single-ticker failure — logs and continues.

        IMPORTANT — freshness semantics: a "checked" ticker (any of the four
        outcomes below) means this call attempted the pull and got an
        answer from the provider. It does NOT mean the ticker's data is
        current through today, and it does NOT mean any OTHER ticker in
        ``ticker_list`` is current — each ticker's outcome only describes
        that ticker. Callers that advance ``source_catalog.last_pull_at``
        (scripts/hermes_fixers.py::_retry_source, ingestion/scheduler.py)
        are recording "this source was checked at this time", never "every
        symbol of this source is current" — see those callers' own
        docstrings/comments.

        Parameters:
            ticker_list: List of Yahoo Finance ticker symbols.
                         Defaults to YF_TICKER_LIST.
            start_date: Earliest observation date. Callers doing a bounded
                        repair (scripts/hermes_fixers.py::_retry_source)
                        always pass a recent date here — this method's own
                        default ("1990-01-01") is for deliberate, explicitly
                        authorised full-history backfills only (see
                        REPAIR_LOOKBACK_DAYS in hermes_fixers.py).
            should_continue: Optional cooperative-budget check, polled
                        between tickers. When it returns False, the pull
                        stops before the next ticker and this method
                        returns a dict (not the usual list) describing a
                        partial run — see Returns below. Ordinary callers
                        that never pass this keep getting the plain
                        list[dict] they always got. Note this is only
                        checked BETWEEN tickers — one in-flight provider download
                        call cannot itself be interrupted this way; see
                        pull_ticker's own timeout handling and
                        _run_with_timeout in scripts/hermes_operator.py for
                        the outer bound on that case.

        Returns:
            - should_continue is None (default, all existing callers):
              list[dict], one result dict per ticker, unchanged (each dict
              now also carries an "outcome" key — see pull_ticker).
            - should_continue is given and the budget ran out before every
              ticker was attempted: a dict — {"status": "PARTIAL",
              "stopped_by_budget": True, "results": [...per-ticker results
              attempted so far...], "tickers_not_attempted": [...],
              "counts": {"inserted": int, "duplicate_only": int,
              "no_data": int, "error": int, "unattempted": int}}.
              "unattempted" counts tickers never reached because the budget
              ran out — those are NOT checked, and must not be treated as
              "ok" by anything reading this result.
            - should_continue is given and every ticker was attempted: the
              same dict shape with "status": "SUCCESS",
              "stopped_by_budget": False, "tickers_not_attempted": [],
              "counts": {..., "unattempted": 0}.
        """
        if ticker_list is None:
            ticker_list = YF_TICKER_LIST

        log.info(
            "Starting yfinance bulk pull — checking {n} tickers from {sd}",
            n=len(ticker_list),
            sd=start_date,
        )
        results: list[dict[str, Any]] = []
        stopped_by_budget = False
        for idx, ticker in enumerate(ticker_list):
            if should_continue is not None and not should_continue():
                log.warning(
                    "yfinance bulk pull: budget exhausted after {n}/{total} "
                    "tickers — stopping",
                    n=idx, total=len(ticker_list),
                )
                stopped_by_budget = True
                break
            res = self.pull_ticker(ticker, start_date)
            results.append(res)

        log.info(
            "yfinance bulk pull complete — {ok}/{total} checked successfully",
            ok=sum(1 for r in results if r["status"] == "SUCCESS"),
            total=len(results),
        )

        if should_continue is None:
            return results

        not_attempted = ticker_list[len(results):]
        counts = {"inserted": 0, "duplicate_only": 0, "no_data": 0, "error": 0, "unattempted": 0}
        for r in results:
            outcome = r.get("outcome")
            if outcome in counts:
                counts[outcome] += 1
            else:
                counts["error"] += 1
        counts["unattempted"] = len(not_attempted)
        return {
            "status": "PARTIAL" if stopped_by_budget else "SUCCESS",
            "stopped_by_budget": stopped_by_budget,
            "results": results,
            "tickers_not_attempted": not_attempted,
            "counts": counts,
        }


if __name__ == "__main__":
    from db import get_engine

    puller = YFinancePuller(db_engine=get_engine())
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(f"  {r['ticker']}: {r['status']} ({r['rows_inserted']} rows)")
