"""
GRID yfinance data ingestion module.

Downloads OHLCV market data from Yahoo Finance via the ``yfinance`` library
and stores each field as a separate entry in ``raw_series``.
"""

from __future__ import annotations

import inspect
import json
import logging
import math
import re
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

import pandas as pd
import yfinance as yf
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller
from price_close_contract import (
    SPY_CAPTURE_MAX_LOOKBACK_DAYS,
    SPY_CLOSE_SERIES,
    capture_payload,
    observation_end_utc,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

# GRID-YF-CLOSE-REPAIR-20260926 root-cause fix: the smart scheduler
# (ingestion/smart_scheduler.py) runs YFinancePuller.pull_all in a daemon
# thread bounded by a thread-join timeout. If that budget is exceeded the
# thread is *orphaned* (left running, never killed — see
# SmartScheduler._run_puller's docstring) while the NEXT scheduler tick can
# start a fresh pull_all() call in the same process. Two concurrent
# pull_all() runs against an old, thread-unsafe yfinance version (fixed in
# 1.7.0, see the module-level timeout note above) was the confirmed
# mechanism by which one ticker's frame got written under another ticker's
# series_id. This lock makes pull_all single-flight at the PROCESS level —
# a non-blocking acquire — so a second concurrent call (whether from an
# orphaned thread or an ordinary double-invocation) skips its run entirely
# rather than racing the first one. It is a plain (non-reentrant)
# threading.Lock at module scope, not an instance attribute, because a new
# YFinancePuller instance is constructed for every scheduled call
# (SmartScheduler._build_puller_instance) — the lock must survive across
# instances and threads for the life of the process.
_PULL_ALL_LOCK = threading.Lock()

# Wrong-instrument sanity guard (GRID-YF-CLOSE-REPAIR-20260926, fix #4):
# refuse to write a series' freshly downloaded values if they disagree
# wildly with that exact series' own existing recent SUCCESS values on the
# dates where both exist. A different instrument's price series will almost
# always fail this cheaply, even when everything else about the response
# (columns, dtypes, dates) looks well-formed.
_WILD_RATIO_LOW = 0.5
_WILD_RATIO_HIGH = 2.0
# Below this many overlapping dates, a ratio is too noisy to judge (a single
# stale or off-by-one-day existing row could otherwise trip the guard).
_WILD_RATIO_MIN_OVERLAP = 3

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

    def _median_ratio_vs_existing(
        self,
        series_id: str,
        values_by_date: dict[date, float],
        conn: Any,
    ) -> tuple[float | None, int]:
        """Compare freshly downloaded values to this series' own history.

        Looks up existing SUCCESS rows for ``series_id`` on the same dates
        (bounded to the min/max of ``values_by_date``, not a full scan) and
        returns the median of new/old ratios plus how many dates overlapped.
        Zero or near-zero existing values are excluded (division is
        meaningless there). Returns ``(None, overlap_count)`` when there
        isn't enough overlap to judge — callers must treat ``None`` as "no
        opinion", never as "safe".

        Parameters:
            series_id: The raw_series series identifier being written.
            values_by_date: Freshly downloaded {obs_date: value} pairs.
            conn: Active database connection (used inside the same
                transaction as the pending inserts).
        """
        if not values_by_date:
            return None, 0

        rows = conn.execute(
            text(
                "SELECT obs_date, value FROM raw_series "
                "WHERE series_id = :sid AND source_id = :src "
                "AND pull_status = 'SUCCESS' "
                "AND obs_date BETWEEN :start_date AND :end_date"
            ),
            {
                "sid": series_id,
                "src": self.source_id,
                "start_date": min(values_by_date),
                "end_date": max(values_by_date),
            },
        ).fetchall()
        existing: dict[date, float] = {}
        for row in rows:
            try:
                existing[row[0]] = float(row[1])
            except (TypeError, ValueError):
                continue

        ratios: list[float] = []
        for obs_date_val, new_val in values_by_date.items():
            old_val = existing.get(obs_date_val)
            if old_val is None or old_val == 0 or new_val == 0:
                continue
            ratios.append(new_val / old_val)

        if len(ratios) < _WILD_RATIO_MIN_OVERLAP:
            return None, len(ratios)

        ratios.sort()
        mid = len(ratios) // 2
        median = (
            ratios[mid]
            if len(ratios) % 2 == 1
            else (ratios[mid - 1] + ratios[mid]) / 2
        )
        return median, len(ratios)

    def pull_ticker(
        self,
        ticker: str,
        start_date: str | date,
        end_date: str | date | None = None,
        interval: str = "1d",
        *,
        only_fields: frozenset[str] | None = None,
    ) -> dict[str, Any]:
        """Download OHLCV data for a single ticker and insert into raw_series.

        Each OHLCV field is stored as a separate series with the naming
        convention ``YF:{ticker}:{field}`` (e.g. ``YF:^GSPC:close``).

        Parameters:
            ticker: Yahoo Finance ticker symbol.
            start_date: Earliest date to download.
            end_date: Latest date (default: today).
            interval: Data frequency ('1d', '1wk', '1mo').
            only_fields: Optional field keys to persist from the bounded
                download. The scheduled completed-SPY-close request uses
                ``frozenset({'close'})`` so it cannot write other OHLCV fields.

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

            # yfinance >=0.2.31 returns MultiIndex columns (field, ticker).
            # Wrong-instrument guard (GRID-YF-CLOSE-REPAIR-20260926, fix #3):
            # verify every ticker-level entry matches what we asked for
            # BEFORE dropping that level. A mismatch means the frame yfinance
            # returned belongs (wholly or partly) to a different instrument
            # — the confirmed mechanism behind the April 2026 SPY/QQQ/BTC/ETH
            # contamination (donor tickers like CL=F, GBPUSD=X, another
            # crypto). Fail closed for the whole ticker rather than silently
            # writing data that isn't this ticker's.
            if isinstance(df.columns, pd.MultiIndex):
                ticker_level = df.columns.get_level_values(-1)
                unexpected = sorted(
                    {str(t) for t in ticker_level if str(t) != yf_ticker}
                )
                if unexpected:
                    log.warning(
                        "yfinance {t}: downloaded frame contains other "
                        "ticker column(s) {u} — refusing to write "
                        "(wrong-instrument guard)",
                        t=yf_ticker, u=unexpected,
                    )
                    result["status"] = "SKIPPED"
                    result["outcome"] = "error"
                    result["errors"].append(
                        f"Wrong-instrument frame: unexpected ticker column(s) {unexpected}"
                    )
                    return result
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
            requested_start_bound = pd.Timestamp(start_date).date()

            with self.engine.begin() as conn:
                for col_name, field_key in _FIELD_MAP.items():
                    if only_fields is not None and field_key not in only_fields:
                        continue
                    if col_name not in df.columns:
                        continue

                    series_id = f"YF:{yf_ticker}:{field_key}"
                    selected = df[col_name]
                    # Duplicate-column collapses (e.g. MultiIndex flattening
                    # producing repeated headers) yield a DataFrame instead of
                    # a Series, whose .items() iterates column names rather
                    # than the DatetimeIndex — which leaks strings like "Open"
                    # into obs_date and poisons the insert. Fix #3
                    # (GRID-YF-CLOSE-REPAIR-20260926): we can't tell which
                    # duplicate column is correct, so fail closed for this
                    # field instead of silently guessing via the first one.
                    if isinstance(selected, pd.DataFrame):
                        log.warning(
                            "yfinance {t}: column {c} resolved to DataFrame "
                            "({n} duplicate columns) — refusing to guess; "
                            "skipping this field",
                            t=yf_ticker, c=col_name, n=selected.shape[1],
                        )
                        result["errors"].append(
                            f"{field_key}: ambiguous duplicate columns — skipped"
                        )
                        continue
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
                        start_date=requested_start_bound,
                        end_date=existing_end_bound,
                    )

                    # Wrong-instrument sanity guard (fix #4,
                    # GRID-YF-CLOSE-REPAIR-20260926): before writing anything
                    # for this series, compare the freshly downloaded values
                    # to this exact series' own existing recent SUCCESS
                    # values on the dates where both exist. A median ratio
                    # far from 1 — outside [0.5, 2] — means this download
                    # disagrees wildly with the series' own history, which is
                    # cheap, strong evidence of a wrong-instrument frame
                    # (donor tickers off by 0.4x-30x+ per the incident
                    # writeup) rather than a normal revision. Refuse the
                    # whole field's writes rather than trying to salvage
                    # individual dates.
                    guard_values_by_date: dict[date, float] = {}
                    for dt_idx, raw_value in col_data.items():
                        parsed = pd.to_datetime(dt_idx, errors="coerce")
                        if pd.isna(parsed):
                            continue
                        try:
                            guard_values_by_date[parsed.date()] = float(raw_value)
                        except (TypeError, ValueError):
                            continue
                    median_ratio, overlap_n = self._median_ratio_vs_existing(
                        series_id, guard_values_by_date, conn,
                    )
                    if median_ratio is not None and not (
                        _WILD_RATIO_LOW <= median_ratio <= _WILD_RATIO_HIGH
                    ):
                        log.warning(
                            "yfinance {t}:{f}: refusing write — median ratio "
                            "{r:.3f} vs {n} overlapping existing SUCCESS "
                            "dates is outside [{lo},{hi}] (wrong-instrument "
                            "guard)",
                            t=yf_ticker, f=field_key, r=median_ratio,
                            n=overlap_n, lo=_WILD_RATIO_LOW, hi=_WILD_RATIO_HIGH,
                        )
                        result["errors"].append(
                            f"{field_key}: wild median ratio {median_ratio:.3f} "
                            f"vs {overlap_n} existing dates — refused"
                        )
                        continue

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
                        # The close-only scheduled request is for exactly one
                        # completed session. Do not trust a provider response
                        # to obey the requested one-day bounds.
                        if only_fields is not None and (
                            obs_date_val < requested_start_bound
                            or (existing_end_bound is not None
                                and obs_date_val > existing_end_bound)
                        ):
                            continue
                        if series_id == SPY_CLOSE_SERIES and interval == "1d":
                            if not math.isfinite(float_val) or float_val <= 0:
                                continue
                            captured_at = _utc_now()
                            # No provisional SPY close and no implicit history
                            # backfill. A later pull may append one marked row
                            # even if an older unmarked row exists.
                            if obs_date_val < captured_at.date() - timedelta(
                                days=SPY_CAPTURE_MAX_LOOKBACK_DAYS
                            ):
                                continue
                            marker = capture_payload(obs_date_val, captured_at)
                            if marker is None:
                                continue
                            already_marked = conn.execute(
                                text(
                                    "SELECT 1 FROM raw_series "
                                    "WHERE series_id = :sid AND source_id = :src "
                                    "AND obs_date = :od AND pull_status = 'SUCCESS' "
                                    "AND raw_payload @> CAST(:payload AS jsonb) "
                                    "AND pull_timestamp >= :period_end "
                                    "LIMIT 1"
                                ),
                                {"sid": series_id, "src": self.source_id,
                                 "od": obs_date_val, "payload": json.dumps(marker),
                                 "period_end": observation_end_utc(obs_date_val)},
                            ).fetchone()
                            if already_marked:
                                continue
                            conn.execute(
                                text(
                                    "INSERT INTO raw_series "
                                    "(series_id, source_id, obs_date, pull_timestamp, "
                                    "value, raw_payload, pull_status) "
                                    "VALUES (:sid, :src, :od, :pulled_at, :val, "
                                    "CAST(:payload AS jsonb), 'SUCCESS')"
                                ),
                                {"sid": series_id, "src": self.source_id,
                                 "od": obs_date_val, "pulled_at": captured_at,
                                 "val": float_val, "payload": json.dumps(marker)},
                            )
                            inserted += 1
                            continue
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
            - a previous pull_all() call is still active in this process
              (single-flight guard, GRID-YF-CLOSE-REPAIR-20260926 fix #1):
              this call makes NO attempt at all — not even the first
              ticker — and returns immediately. should_continue is None:
              an empty list ``[]`` (zero tickers checked; distinguishable
              from a real run only by being empty, since a real run over an
              empty ticker_list is degenerate and not a supported input).
              should_continue given: the usual dict shape with
              "status": "SKIPPED", "stopped_by_budget": True (so callers
              like scripts/hermes_fixers.py::_retry_source that gate
              "mark this source freshly checked" on `stopped_by_budget`
              correctly do NOT advance last_pull_at), "tickers_not_attempted"
              equal to the full requested ticker_list, "counts" all-zero
              except "unattempted", and "skipped_reason" naming why.
        """
        if ticker_list is None:
            ticker_list = YF_TICKER_LIST

        # Single-flight guard (fix #1): non-blocking acquire so a second
        # concurrent pull_all() — most commonly the scheduler's NEXT tick
        # finding an earlier orphaned/still-running call, but any
        # accidental double-invocation is equally dangerous — skips its
        # entire run rather than racing the in-flight one. See the module
        # docstring on _PULL_ALL_LOCK for why this must be a module-level
        # lock rather than an instance attribute.
        if not _PULL_ALL_LOCK.acquire(blocking=False):
            log.warning(
                "yfinance pull_all: a previous run is still active in this "
                "process — skipping this run entirely (single-flight guard, "
                "no tickers attempted)",
            )
            if should_continue is None:
                return []
            counts = {"inserted": 0, "duplicate_only": 0, "no_data": 0,
                      "error": 0, "unattempted": len(ticker_list)}
            return {
                "status": "SKIPPED",
                "stopped_by_budget": True,
                "results": [],
                "tickers_not_attempted": list(ticker_list),
                "counts": counts,
                "skipped_reason": "pull_all already running in this process",
            }

        try:
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
        finally:
            _PULL_ALL_LOCK.release()


if __name__ == "__main__":
    from db import get_engine

    puller = YFinancePuller(db_engine=get_engine())
    results = puller.pull_all(start_date="2020-01-01")
    for r in results:
        print(f"  {r['ticker']}: {r['status']} ({r['rows_inserted']} rows)")
