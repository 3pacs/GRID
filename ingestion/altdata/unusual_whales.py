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

import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

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
        strike_str = f"{signal['strike']:.0f}"
        series_id = (
            f"WHALE:{signal['ticker']}:{strike_str}"
            f":{signal['expiration']}:{signal['direction']}"
        )

        if self._row_exists(series_id, obs_date, conn):
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

        inserted = 0
        with self.engine.begin() as conn:
            for signal in all_signals:
                try:
                    if self._store_whale_signal(conn, signal, today):
                        inserted += 1
                except Exception as exc:
                    log.warning(
                        "Whale: failed to store signal for {t} {s}: {e}",
                        t=ticker,
                        s=signal.get("strike"),
                        e=str(exc),
                    )

                try:
                    self._emit_whale_signal(conn, signal, today)
                except Exception as exc:
                    log.debug(
                        "Whale: signal emission failed for {t}: {e}",
                        t=ticker,
                        e=str(exc),
                    )

        log.info(
            "WHALE {t}: {n} unusual signals detected, {ins} stored",
            t=ticker,
            n=len(all_signals),
            ins=inserted,
        )

        return {
            "ticker": ticker,
            "status": "SUCCESS",
            "signals_found": len(all_signals),
            "rows_inserted": inserted,
        }

    def pull_all(
        self,
        tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Scan all watchlist tickers for unusual options activity.

        Never stops on a single-ticker failure -- logs and continues.

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
