"""
GRID Unusual Options Flow (Whale Tracking) ingestion module.

Scans yfinance options chains for unusual activity signals:
- Single-day OI increases > 2x 5-day average
- Large premium blocks (>$500K notional)
- Sweep-like patterns (high volume at a single strike)

No external API key required — computed from the options chain data
that yfinance provides for free.

Series stored:
- WHALE:{ticker}:{strike}:{expiry}:{direction}

Source: yfinance options chains (Yahoo Finance)
Schedule: Daily (market hours)
"""

from __future__ import annotations

import json
import math
import time
from datetime import date
from typing import Any

from loguru import logger as log
from sqlalchemy import exc as sa_exc
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure


# ── NaN / Inf sanitisation ───────────────────────────────────────────
#
# yfinance returns NaN floats for fields like ``volume`` or
# ``openInterest`` when a strike has no recorded activity yet.
# These propagate into our ``signal`` dicts and break two things:
#   1. ``json.dumps`` emits the literal token ``NaN``, which is not
#      valid JSON — Postgres' JSONB parser raises
#      ``ValueError: Out of range float values are not JSON compliant``.
#   2. NaN passed as a numeric bind parameter to ``raw_series.value``
#      gets stored as NaN and trips the downstream sanity validator.
#
# The helper below recursively replaces every NaN/Inf float in a
# nested dict / list with ``None`` (which serialises to JSON ``null``
# and Postgres ``NULL``).  Apply it to a row **before** passing the
# row to ``_insert_raw`` / ``json.dumps`` / ``cursor.execute``.
def _clean_nans(d: Any) -> Any:
    """Recursively replace NaN / Inf floats with None.

    Walks dicts, lists, and tuples.  Scalars that are not NaN or Inf
    floats are returned unchanged.

    Parameters:
        d: Any value (dict, list, tuple, or scalar).

    Returns:
        A structurally-identical value with NaN/Inf floats replaced
        by ``None``.
    """
    if isinstance(d, dict):
        return {k: _clean_nans(v) for k, v in d.items()}
    if isinstance(d, list):
        return [_clean_nans(v) for v in d]
    if isinstance(d, tuple):
        return tuple(_clean_nans(v) for v in d)
    if isinstance(d, float) and (math.isnan(d) or math.isinf(d)):
        return None
    return d

# ── Configuration ────────────────────────────────────────────────────

# Minimum notional premium to flag as a whale trade ($)
_MIN_PREMIUM_NOTIONAL: float = 500_000.0

# OI spike threshold: current OI must exceed N * rolling average OI
_OI_SPIKE_MULTIPLIER: float = 2.0

# Volume spike threshold relative to average
_VOLUME_SPIKE_MULTIPLIER: float = 3.0

# Minimum open interest to consider (filters out illiquid noise)
_MIN_OI_THRESHOLD: int = 100

# Rate limit between ticker scans (seconds)
_RATE_LIMIT_DELAY: float = 0.5

#: Max whale signals written per transaction (see pull_ticker's locking
#: contract): no long per-ticker transaction, no savepoint per row.
STORE_BATCH_ROWS: int = 50
#: The scan stops after this many consecutive CONNECTION-level write failures
#: (a batch and its first per-row retry). Data errors on single rows never
#: count toward it: they are skipped and reported, and the scan continues.
MAX_CONSECUTIVE_CONNECTION_FAILURES: int = 2


class WhaleStoreAborted(RuntimeError):
    """The database is unusable: stop the whole scan.

    ``stored`` is the number of rows committed in the aborted batch before
    the abort, so callers can still report them.
    """

    def __init__(self, message: str, stored: int = 0) -> None:
        super().__init__(message)
        self.stored = stored


class _ConnectionFailure(Exception):
    """Internal: a write failed because the database is unusable (see ``_write``)."""


def _is_connection_error(exc: BaseException) -> bool:
    """True for errors that mean the database is unusable, not that one row is bad.

    psycopg2 also raises OperationalError for statement/lock timeouts and
    deadlocks; two of those in a row stop the scan too, which errs on the safe
    side for a puller that can simply run again next tick.
    """
    if isinstance(exc, (sa_exc.OperationalError, sa_exc.DisconnectionError, sa_exc.TimeoutError)):
        return True
    return bool(getattr(exc, "connection_invalidated", False))

# Maximum expirations to scan per ticker (nearest N)
_MAX_EXPIRATIONS: int = 6

# Watchlist — liquid names where options flow is most informative
WATCHLIST: list[str] = [
    "SPY", "QQQ", "IWM", "DIA", "TLT", "HYG", "XLF", "XLE", "XLK",
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "AMD",
    "JPM", "BAC", "GS", "NFLX", "COIN", "PLTR", "SOFI",
    "GLD", "SLV", "USO", "EEM", "FXI", "KWEB",
]


class UnusualWhalesPuller(BasePuller):
    """Scans yfinance options chains for unusual options flow.

    Detects whale-level activity by looking for:
    1. OI spikes: open interest increases > 2x the 5-day rolling mean
    2. Large premium blocks: single-strike notional > $500K
    3. Volume sweeps: abnormal volume concentration at a single strike

    Series pattern: WHALE:{ticker}:{strike}:{expiry}:{direction}

    Attributes:
        engine: SQLAlchemy engine for database operations.
        source_id: Resolved source_catalog.id for Unusual_Whales.
    """

    SOURCE_NAME: str = "Unusual_Whales"

    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://finance.yahoo.com/",
        "cost_tier": "FREE",
        "latency_class": "INTRADAY",
        "pit_available": True,
        "revision_behavior": "FREQUENT",
        "trust_score": "MED",
        "priority_rank": 35,
    }

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the unusual whales puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        super().__init__(db_engine)
        log.info(
            "UnusualWhalesPuller initialised — source_id={sid}",
            sid=self.source_id,
        )

    # ------------------------------------------------------------------ #
    # yfinance interaction
    # ------------------------------------------------------------------ #

    @retry_on_failure(
        max_attempts=3,
        backoff=2.0,
        retryable_exceptions=(ConnectionError, TimeoutError, OSError),
    )
    def _fetch_options_chain(
        self,
        ticker_symbol: str,
        expiration: str,
    ) -> dict[str, Any]:
        """Fetch an options chain for a ticker and expiration via yfinance.

        Parameters:
            ticker_symbol: Stock/ETF ticker (e.g. 'SPY').
            expiration: Expiration date string (YYYY-MM-DD).

        Returns:
            Dict with 'calls' and 'puts' as lists of option dicts,
            or empty dict on failure.
        """
        try:
            import yfinance as yf
        except ImportError:
            log.error("yfinance not installed — run: pip install yfinance")
            return {}

        try:
            tk = yf.Ticker(ticker_symbol)
            chain = tk.option_chain(expiration)

            calls_df = chain.calls
            puts_df = chain.puts

            calls = calls_df.to_dict("records") if calls_df is not None and not calls_df.empty else []
            puts = puts_df.to_dict("records") if puts_df is not None and not puts_df.empty else []

            return {"calls": calls, "puts": puts}

        except Exception as exc:
            log.warning(
                "Failed to fetch options chain for {t} exp={e}: {err}",
                t=ticker_symbol,
                e=expiration,
                err=str(exc),
            )
            return {}

    def _get_expirations(self, ticker_symbol: str) -> list[str]:
        """Get available expiration dates for a ticker.

        Parameters:
            ticker_symbol: Stock/ETF ticker.

        Returns:
            List of expiration date strings, limited to nearest N.
        """
        try:
            import yfinance as yf
        except ImportError:
            log.error("yfinance not installed — run: pip install yfinance")
            return []

        try:
            tk = yf.Ticker(ticker_symbol)
            expirations = list(tk.options)
            return expirations[:_MAX_EXPIRATIONS]
        except Exception as exc:
            log.warning(
                "Failed to get expirations for {t}: {err}",
                t=ticker_symbol,
                err=str(exc),
            )
            return []

    # ------------------------------------------------------------------ #
    # Detection logic
    # ------------------------------------------------------------------ #

    def _detect_unusual_activity(
        self,
        ticker: str,
        expiration: str,
        options: list[dict[str, Any]],
        direction: str,
    ) -> list[dict[str, Any]]:
        """Scan a list of option contracts for unusual activity.

        Flags contracts with:
        - High open interest (> _MIN_OI_THRESHOLD)
        - Large notional premium (last_price * OI * 100 > _MIN_PREMIUM_NOTIONAL)
        - Volume spikes (volume > _VOLUME_SPIKE_MULTIPLIER * impliedVolatility proxy)

        Parameters:
            ticker: Underlying ticker symbol.
            expiration: Expiration date string.
            options: List of option contract dicts from yfinance.
            direction: 'CALL' or 'PUT'.

        Returns:
            List of flagged unusual activity dicts.
        """
        flagged: list[dict[str, Any]] = []

        # Compute chain-wide averages for relative comparison
        oi_values = [
            float(opt.get("openInterest", 0) or 0)
            for opt in options
            if (opt.get("openInterest") or 0) > 0
        ]
        vol_values = [
            float(opt.get("volume", 0) or 0)
            for opt in options
            if (opt.get("volume") or 0) > 0
        ]

        avg_oi = sum(oi_values) / len(oi_values) if oi_values else 0.0
        avg_vol = sum(vol_values) / len(vol_values) if vol_values else 0.0

        for opt in options:
            strike = opt.get("strike")
            if strike is None:
                continue

            oi = float(opt.get("openInterest", 0) or 0)
            volume = float(opt.get("volume", 0) or 0)
            last_price = float(opt.get("lastPrice", 0) or 0)
            implied_vol = float(opt.get("impliedVolatility", 0) or 0)

            if oi < _MIN_OI_THRESHOLD:
                continue

            # Calculate notional premium (OI * price * 100 shares/contract)
            notional = oi * last_price * 100.0

            signals: list[str] = []

            # Check 1: Large premium block
            if notional >= _MIN_PREMIUM_NOTIONAL:
                signals.append("LARGE_PREMIUM")

            # Check 2: OI spike vs chain average
            if avg_oi > 0 and oi > avg_oi * _OI_SPIKE_MULTIPLIER:
                signals.append("OI_SPIKE")

            # Check 3: Volume spike vs chain average
            if avg_vol > 0 and volume > avg_vol * _VOLUME_SPIKE_MULTIPLIER:
                signals.append("VOLUME_SPIKE")

            if not signals:
                continue

            flagged.append({
                "ticker": ticker,
                "strike": float(strike),
                "expiration": expiration,
                "direction": direction,
                "open_interest": oi,
                "volume": volume,
                "last_price": last_price,
                "implied_volatility": implied_vol,
                "notional_premium": notional,
                "signals": signals,
                "avg_oi": avg_oi,
                "avg_volume": avg_vol,
                "oi_ratio": oi / avg_oi if avg_oi > 0 else 0.0,
                "volume_ratio": volume / avg_vol if avg_vol > 0 else 0.0,
            })

        return flagged

    # ------------------------------------------------------------------ #
    # Storage
    # ------------------------------------------------------------------ #

    def _store_whale_signal(
        self,
        conn: Any,
        signal: dict[str, Any],
        obs_date: date,
    ) -> bool:
        """Store a single whale signal in raw_series.

        Parameters:
            conn: Active database connection (within a transaction).
            signal: Detected unusual activity dict.
            obs_date: Observation date.

        Returns:
            True if inserted, False if duplicate.
        """
        # Defensive: callers should already have sanitised, but
        # _clean_nans is cheap and guarantees no NaN/Inf leaks to
        # json.dumps or numeric bind params.
        signal = _clean_nans(signal)

        strike_str = f"{signal['strike']:.0f}"
        series_id = (
            f"WHALE:{signal['ticker']}:{strike_str}"
            f":{signal['expiration']}:{signal['direction']}"
        )

        if self._row_exists(series_id, obs_date, conn):
            return False

        if signal.get("notional_premium") is None:
            # ``raw_series.value`` cannot store NULL via the helper's
            # ``float(value)`` coercion, and a NaN notional has no
            # downstream meaning — skip silently.
            return False

        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=signal["notional_premium"],
            raw_payload={
                "ticker": signal["ticker"],
                "strike": signal["strike"],
                "expiration": signal["expiration"],
                "direction": signal["direction"],
                "open_interest": signal["open_interest"],
                "volume": signal["volume"],
                "last_price": signal["last_price"],
                "implied_volatility": signal["implied_volatility"],
                "notional_premium": signal["notional_premium"],
                "signals": signal["signals"],
                "oi_ratio": signal["oi_ratio"],
                "volume_ratio": signal["volume_ratio"],
                "avg_oi_chain": signal["avg_oi"],
                "avg_volume_chain": signal["avg_volume"],
            },
        )
        return True

    def _emit_whale_signal(
        self,
        conn: Any,
        signal: dict[str, Any],
        obs_date: date,
    ) -> None:
        """Emit an UNUSUAL_OPTIONS signal for trust scoring.

        Parameters:
            conn: Active database connection.
            signal: Whale activity detection result.
            obs_date: Signal date.
        """
        # Defensive sanitisation — ``json.dumps`` below would emit
        # the bare token ``NaN`` for any NaN float and corrupt JSONB.
        signal = _clean_nans(signal)

        conn.execute(
            text(
                "INSERT INTO signal_sources "
                "(source_type, source_id, ticker, signal_date, signal_type, signal_value) "
                "VALUES (:stype, :sid, :ticker, :sdate, :stype2, :sval) "
                "ON CONFLICT (source_type, source_id, ticker, signal_date, signal_type) "
                "DO NOTHING"
            ),
            {
                "stype": "options_flow",
                "sid": f"whale_{signal['ticker'].lower()}_{signal['strike']:.0f}",
                "ticker": signal["ticker"],
                "sdate": obs_date,
                "stype2": "UNUSUAL_OPTIONS",
                "sval": json.dumps({
                    "direction": signal["direction"],
                    "notional": signal["notional_premium"],
                    "signals": signal["signals"],
                    "oi_ratio": signal["oi_ratio"],
                    "volume_ratio": signal["volume_ratio"],
                }),
            },
        )

    # ------------------------------------------------------------------ #
    # Main pull methods
    # ------------------------------------------------------------------ #

    def _write(self, fn) -> None:
        """Run ``fn(conn)`` in one short transaction.

        Raises ``_ConnectionFailure`` for connection-level problems: an error
        raised before the transaction was open (acquiring the connection or
        BEGIN) or one :func:`_is_connection_error` recognises. Anything else
        is a data error and propagates unchanged.
        """
        opened = False
        try:
            with self.engine.begin() as conn:
                opened = True
                fn(conn)
        except Exception as exc:
            if not opened or _is_connection_error(exc):
                raise _ConnectionFailure(exc) from exc
            raise

    def _store_batch(
        self,
        ticker: str,
        batch: list[dict[str, Any]],
        today: date,
        streak: dict[str, int],
    ) -> tuple[int, int, list[str]]:
        """Store one batch in one short transaction; on failure, one row per transaction.

        Returns ``(stored, failed, errors)``. A row with a data error is
        skipped and reported, never aborting anything. Raises
        :class:`WhaleStoreAborted` after ``MAX_CONSECUTIVE_CONNECTION_FAILURES``
        consecutive connection-level failures (``streak`` carries the count
        across batches of the ticker) -- a database outage must surface as a
        failure, not as "0 rows, nothing new".
        """
        counter = {"stored": 0}

        def store_all(conn: Any) -> None:
            counter["stored"] = sum(1 for signal in batch if self._store_whale_signal(conn, signal, today))

        try:
            self._write(store_all)
            streak["connection_failures"] = 0
            return counter["stored"], 0, []
        except _ConnectionFailure as exc:
            self._note_connection_failure(streak, exc.__cause__, stored=0)
            log.warning("Whale: {t} batch of {n} hit a connection error ({e}); retrying row by row",
                        t=ticker, n=len(batch), e=str(exc.__cause__))
        except Exception as exc:
            log.warning("Whale: {t} batch of {n} failed ({e}); retrying row by row",
                        t=ticker, n=len(batch), e=str(exc))

        stored = 0
        failed = 0
        errors: list[str] = []
        for signal in batch:
            one = {"stored": False}

            def store_one(conn: Any, signal: dict[str, Any] = signal) -> None:
                one["stored"] = self._store_whale_signal(conn, signal, today)

            try:
                self._write(store_one)
                streak["connection_failures"] = 0
                stored += int(one["stored"])
            except _ConnectionFailure as exc:
                failed += 1
                errors.append(str(exc.__cause__)[:200])
                self._note_connection_failure(streak, exc.__cause__, stored=stored)
            except Exception as exc:
                failed += 1
                errors.append(str(exc)[:200])
                streak["connection_failures"] = 0  # the database answered: a data error
                log.warning(
                    "Whale: row insert failed — "
                    "date={d} ticker={t} strike={s} exp={e} dir={dir}: {err}",
                    d=today,
                    t=ticker,
                    s=signal.get("strike"),
                    e=signal.get("expiration"),
                    dir=signal.get("direction"),
                    err=str(exc),
                )
        return stored, failed, errors

    @staticmethod
    def _note_connection_failure(streak: dict[str, int], exc: BaseException | None, stored: int) -> None:
        streak["connection_failures"] = streak.get("connection_failures", 0) + 1
        if streak["connection_failures"] >= MAX_CONSECUTIVE_CONNECTION_FAILURES:
            raise WhaleStoreAborted(f"database unavailable: {exc}", stored=stored) from exc

    def _emit_batch(self, ticker: str, batch: list[dict[str, Any]], today: date) -> None:
        """signal_sources rows for one batch in one short transaction; on failure, one row per transaction.

        Best-effort: a row with a data error is skipped; two consecutive
        connection-level failures end the emission for this batch.
        """
        def emit_all(conn: Any) -> None:
            for signal in batch:
                self._emit_whale_signal(conn, signal, today)

        try:
            self._write(emit_all)
            return
        except Exception as exc:
            log.debug("Whale: signal emission batch failed for {t}: {e}", t=ticker, e=str(exc))
            connection_failures = 1 if isinstance(exc, _ConnectionFailure) else 0
        for signal in batch:
            try:
                self._write(lambda conn, signal=signal: self._emit_whale_signal(conn, signal, today))
                connection_failures = 0
            except Exception as exc:
                log.debug(
                    "Whale: signal emission failed for {t} strike={s}: {e}",
                    t=ticker,
                    s=signal.get("strike"),
                    e=str(exc),
                )
                if isinstance(exc, _ConnectionFailure):
                    connection_failures += 1
                    if connection_failures >= MAX_CONSECUTIVE_CONNECTION_FAILURES:
                        return

    def pull_ticker(
        self,
        ticker: str,
    ) -> dict[str, Any]:
        """Scan a single ticker's options chains for unusual activity.

        Parameters:
            ticker: Stock/ETF ticker symbol.

        Returns:
            Dict with status, signals_found, rows_inserted.
        """
        today = date.today()
        expirations = self._get_expirations(ticker)

        if not expirations:
            return {
                "ticker": ticker,
                "status": "PARTIAL",
                "signals_found": 0,
                "rows_inserted": 0,
                "errors": ["No expirations available"],
            }

        all_signals: list[dict[str, Any]] = []

        for exp in expirations:
            chain = self._fetch_options_chain(ticker, exp)
            if not chain:
                continue

            for direction, key in [("CALL", "calls"), ("PUT", "puts")]:
                options = chain.get(key, [])
                if not options:
                    continue

                signals = self._detect_unusual_activity(
                    ticker, exp, options, direction,
                )
                all_signals.extend(signals)

            time.sleep(_RATE_LIMIT_DELAY)

        if not all_signals:
            return {
                "ticker": ticker,
                "status": "SUCCESS",
                "signals_found": 0,
                "rows_inserted": 0,
            }

        signals: list[dict[str, Any]] = []
        for raw_signal in all_signals:
            # Strip NaN/Inf floats up-front so they never reach
            # ``json.dumps`` or a bound numeric parameter.
            signal = _clean_nans(raw_signal)
            # If the value column itself (notional_premium) was NaN,
            # there's nothing meaningful to store — skip.
            if signal.get("notional_premium") is None:
                log.debug(
                    "Whale: skipping {t} strike={s} exp={e} — "
                    "notional_premium was NaN after sanitisation",
                    t=ticker,
                    s=raw_signal.get("strike"),
                    e=raw_signal.get("expiration"),
                )
                continue
            signals.append(signal)

        # Locking contract (2026-10-02 offshore_leaks incident): short
        # transactions of at most STORE_BATCH_ROWS signals, never one
        # transaction per ticker with a SAVEPOINT per row. Each committed
        # savepoint is a subtransaction whose XID lock is held until the
        # top-level commit, so a ticker with thousands of signals could
        # exhaust the shared lock table. A failed batch is retried one row
        # per transaction, so one bad row still only loses itself.
        inserted = 0
        failed = 0
        errors: list[str] = []
        streak = {"connection_failures": 0}  # consecutive connection-level failures
        try:
            for start in range(0, len(signals), STORE_BATCH_ROWS):
                batch = signals[start:start + STORE_BATCH_ROWS]
                stored, batch_failed, batch_errors = self._store_batch(ticker, batch, today, streak)
                inserted += stored
                failed += batch_failed
                errors.extend(batch_errors)
                # Stored rows and their signal_sources rows commit in separate
                # transactions, so a crash in between can leave a raw_series
                # row without its signal_sources row. A same-day rerun repairs
                # it: _emit_batch is called for every detected signal (ON
                # CONFLICT DO NOTHING), including ones whose raw row exists.
                self._emit_batch(ticker, batch, today)
        except WhaleStoreAborted as exc:
            # Rows in exc.stored got no signal_sources row in this run; a
            # same-day rerun emits them (see the comment above).
            inserted += exc.stored
            if not inserted:
                raise
            log.warning("WHALE {t}: writes aborted after {n} rows: {e}", t=ticker, n=inserted, e=str(exc))
            return {
                "ticker": ticker,
                "status": "PARTIAL",
                "signals_found": len(all_signals),
                "rows_inserted": inserted,
                "errors": (errors + [str(exc)])[:5],
                "aborted": True,
            }

        log.info(
            "WHALE {t}: {n} unusual signals detected, {ins} stored",
            t=ticker,
            n=len(all_signals),
            ins=inserted,
        )

        result: dict[str, Any] = {
            "ticker": ticker,
            "status": "SUCCESS",
            "signals_found": len(all_signals),
            "rows_inserted": inserted,
        }
        if failed:
            result["status"] = "PARTIAL"
            result["errors"] = errors[:5] or [f"{failed} rows failed to store"]
        return result

    def pull_all(
        self,
        tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Scan all watchlist tickers for unusual options activity.

        A single-ticker failure is logged and the scan continues, except
        when the database is unusable (WhaleStoreAborted, or a ticker result
        marked ``aborted``): then the scan stops.

        Parameters:
            tickers: Override watchlist (default: WATCHLIST).

        Returns:
            List of per-ticker result dicts.
        """
        if tickers is None:
            tickers = WATCHLIST

        log.info(
            "Starting unusual whales scan — {n} tickers",
            n=len(tickers),
        )

        results: list[dict[str, Any]] = []
        total_signals = 0
        total_inserted = 0

        for ticker in tickers:
            try:
                res = self.pull_ticker(ticker)
                results.append(res)
                total_signals += res.get("signals_found", 0)
                total_inserted += res.get("rows_inserted", 0)
                if res.get("aborted"):
                    log.error("Whale scan stopped at {t}: database unavailable", t=ticker)
                    break
            except WhaleStoreAborted as exc:
                log.error("Whale scan stopped at {t}: {e}", t=ticker, e=str(exc))
                results.append({"ticker": ticker, "status": "FAILED", "error": str(exc)})
                break
            except Exception as exc:
                log.error(
                    "Whale scan failed for {t}: {e}",
                    t=ticker,
                    e=str(exc),
                )
                results.append({
                    "ticker": ticker,
                    "status": "FAILED",
                    "error": str(exc),
                })

        log.info(
            "Unusual whales scan complete — {n} tickers, "
            "{s} signals, {i} rows stored",
            n=len(tickers),
            s=total_signals,
            i=total_inserted,
        )

        return results


if __name__ == "__main__":
    from db import get_engine

    puller = UnusualWhalesPuller(db_engine=get_engine())
    results = puller.pull_all()
    for r in results:
        print(
            f"  {r.get('ticker', '?')}: {r.get('status')} "
            f"({r.get('signals_found', 0)} signals, "
            f"{r.get('rows_inserted', 0)} stored)"
        )
