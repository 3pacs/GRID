"""GRID — Options chain ingestion via Yahoo Finance API.

Pulls options chains for equity tickers, computes daily signals
(put/call ratio, max pain, IV skew, total OI, vol surface metrics),
and pushes to resolved_series for PIT-correct access.

Uses the Yahoo Finance crumb+cookie auth method for reliable access.
Falls back to yfinance if the direct API fails.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


# Tickers with listed equity options
EQUITY_TICKERS: list[str] = [
    "SPY", "QQQ", "IWM",           # Index ETFs
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL", "AVGO",  # Mega-cap tech
    "JPM", "BAC", "GS", "BLK", "UNH", "BRK-B",  # Financials/Insurance
    "JNJ", "PFE", "MRK", "ABBV", "LLY", "TMO",   # Healthcare
    "EOG", "DVN", "XOM",           # Energy
    "CMCSA", "CI", "PYPL", "CRM", "V", "MA",      # Software/Payments
    "RTX", "GD", "HD", "COST", "PG", "KO", "PEP", # Industrials/Staples
    "INTC", "AMD",                  # Semis
]

# Maximum expirations to pull per ticker
MAX_EXPIRATIONS = 6


# ── Yahoo Finance direct API client ────────────────────────────

class YahooOptionsClient:
    """Fetch options chains directly from Yahoo Finance API with crumb auth."""

    _USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self._USER_AGENT})
        self.crumb: str | None = None
        self._init_session()

    def _init_session(self) -> None:
        """Get cookies and crumb from Yahoo Finance.

        Uses fc.yahoo.com to obtain the A3 cookie (returns 404 but sets cookie),
        then fetches crumb from query2.finance.yahoo.com.
        """
        try:
            # Step 1: Get A3 cookie via fc.yahoo.com (404 is expected)
            self.session.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)

            # Step 2: Get crumb using query2 endpoint
            resp = self.session.get(
                "https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10,
            )
            if resp.status_code == 200 and resp.text:
                self.crumb = resp.text
                log.info("Yahoo options client initialised — crumb obtained")
            else:
                log.warning("Yahoo crumb fetch failed: {s}", s=resp.status_code)
        except Exception as exc:
            log.warning("Yahoo session init failed: {e}", e=str(exc))

    @property
    def is_available(self) -> bool:
        return self.crumb is not None

    def get_options(
        self, ticker: str, expiry_ts: int | None = None,
    ) -> dict[str, Any] | None:
        """Fetch options chain for a ticker and optional expiry timestamp.

        Returns dict with keys: expirations, strikes, calls, puts, quote.
        """
        if not self.crumb:
            return None

        url = f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}"
        params: dict[str, Any] = {"crumb": self.crumb}
        if expiry_ts is not None:
            params["date"] = expiry_ts

        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                log.warning("Yahoo options {t}: HTTP {s}", t=ticker, s=resp.status_code)
                return None
            data = resp.json()
            result = data["optionChain"]["result"][0]
            quote = result.get("quote", {})
            options = result.get("options", [{}])[0]
            return {
                "expirations": result.get("expirationDates", []),
                "strikes": result.get("strikes", []),
                "calls": options.get("calls", []),
                "puts": options.get("puts", []),
                "quote": quote,
            }
        except Exception as exc:
            log.warning("Yahoo options {t} failed: {e}", t=ticker, e=str(exc))
            return None


class OptionsPuller(BasePuller):
    """Pull options chains and compute daily signals for watchlist tickers.

    Attributes:
        SOURCE_NAME: Fixed to 'YFINANCE_OPTIONS' in source_catalog.
    """

    SOURCE_NAME = "YFINANCE_OPTIONS"

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create options tables if they don't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS options_snapshots (
                    id           BIGSERIAL PRIMARY KEY,
                    ticker       TEXT NOT NULL,
                    snap_date    DATE NOT NULL,
                    expiry       DATE NOT NULL,
                    opt_type     TEXT NOT NULL CHECK (opt_type IN ('call', 'put')),
                    strike       DOUBLE PRECISION NOT NULL,
                    last_price   DOUBLE PRECISION,
                    bid          DOUBLE PRECISION,
                    ask          DOUBLE PRECISION,
                    volume       INTEGER,
                    open_interest INTEGER,
                    implied_vol  DOUBLE PRECISION,
                    in_the_money BOOLEAN,
                    created_at   TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (ticker, snap_date, expiry, opt_type, strike)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_opts_snap_ticker_date
                ON options_snapshots (ticker, snap_date)
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS options_daily_signals (
                    id              BIGSERIAL PRIMARY KEY,
                    ticker          TEXT NOT NULL,
                    signal_date     DATE NOT NULL,
                    put_call_ratio  DOUBLE PRECISION,
                    max_pain        DOUBLE PRECISION,
                    iv_skew         DOUBLE PRECISION,
                    total_oi        BIGINT,
                    total_volume    BIGINT,
                    near_expiry     DATE,
                    spot_price      DOUBLE PRECISION,
                    iv_atm          DOUBLE PRECISION,
                    iv_25d_put      DOUBLE PRECISION,
                    iv_25d_call     DOUBLE PRECISION,
                    term_structure_slope DOUBLE PRECISION,
                    oi_concentration     DOUBLE PRECISION,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (ticker, signal_date)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_opts_sig_ticker
                ON options_daily_signals (ticker, signal_date)
            """))

    def pull_all(self, tickers: list[str] | None = None) -> list[dict[str, Any]]:
        """Pull options chains for all tickers and compute signals.

        Parameters:
            tickers: Override ticker list (default: EQUITY_TICKERS).

        Returns:
            list[dict]: Per-ticker results with status and row counts.
        """
        self._yahoo = YahooOptionsClient()
        if not self._yahoo.is_available:
            log.error("Yahoo options client unavailable — cannot pull options")
            return [{"ticker": "N/A", "status": "FAILED", "error": "Yahoo auth failed"}]

        tickers = tickers or EQUITY_TICKERS
        today_str = date.today().isoformat()
        results: list[dict[str, Any]] = []

        for ticker in tickers:
            result = self._pull_ticker(ticker, today_str)
            results.append(result)
            time.sleep(0.3)  # rate limit

        total_snaps = sum(r.get("snapshots", 0) for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "Options pull complete — {ok}/{total} tickers, {snaps} snapshots",
            ok=succeeded, total=len(tickers), snaps=total_snaps,
        )
        return results

    def _pull_ticker(self, ticker: str, today_str: str) -> dict[str, Any]:
        """Pull options chain for a single ticker and compute signals."""
        try:
            # Fetch first page to get expirations and spot price
            first = self._yahoo.get_options(ticker)
            if not first:
                return {"ticker": ticker, "status": "FAILED", "error": "no data from Yahoo"}

            quote = first.get("quote", {})
            spot_price = quote.get("regularMarketPrice") or quote.get("regularMarketPreviousClose")
            if not spot_price:
                log.warning("{t}: no spot price available", t=ticker)
                return {"ticker": ticker, "status": "SKIPPED", "reason": "no spot price"}

            expirations = first.get("expirations", [])
            if not expirations:
                log.warning("{t}: no options expirations", t=ticker)
                return {"ticker": ticker, "status": "SKIPPED", "reason": "no expirations"}

            total_call_oi = 0
            total_put_oi = 0
            total_call_vol = 0
            total_put_vol = 0
            snap_count = 0

            # Per-expiry IV data for term structure
            expiry_ivs: list[tuple[str, float]] = []

            # Collect calls/puts DataFrames for signal computation
            all_calls_dfs: list[pd.DataFrame] = []
            all_puts_dfs: list[pd.DataFrame] = []
            near_calls_df = pd.DataFrame()
            near_puts_df = pd.DataFrame()

            with self.engine.begin() as conn:
                for i, exp_ts in enumerate(expirations[:MAX_EXPIRATIONS]):
                    # Use data from first request for first expiry, fetch rest
                    if i == 0:
                        chain_data = first
                    else:
                        chain_data = self._yahoo.get_options(ticker, exp_ts)
                        time.sleep(0.2)
                    if not chain_data:
                        continue

                    exp_date = datetime.utcfromtimestamp(exp_ts).strftime("%Y-%m-%d")

                    for opt_type, raw_list in [("call", chain_data.get("calls", [])),
                                               ("put", chain_data.get("puts", []))]:
                        if not raw_list:
                            continue

                        rows_for_df = []
                        for opt in raw_list:
                            strike = opt.get("strike")
                            if strike is None:
                                continue
                            vol = opt.get("volume", 0) or 0
                            oi = opt.get("openInterest", 0) or 0
                            iv = opt.get("impliedVolatility")
                            last_price = opt.get("lastPrice")
                            bid = opt.get("bid")
                            ask = opt.get("ask")
                            itm = opt.get("inTheMoney", False)

                            conn.execute(
                                text(
                                    "INSERT INTO options_snapshots "
                                    "(ticker, snap_date, expiry, opt_type, strike, "
                                    "last_price, bid, ask, volume, open_interest, "
                                    "implied_vol, in_the_money) "
                                    "VALUES (:ticker, :snap_date, :expiry, :opt_type, :strike, "
                                    ":last_price, :bid, :ask, :volume, :oi, :iv, :itm) "
                                    "ON CONFLICT DO NOTHING"
                                ),
                                {
                                    "ticker": ticker, "snap_date": today_str,
                                    "expiry": exp_date, "opt_type": opt_type,
                                    "strike": strike, "last_price": last_price,
                                    "bid": bid, "ask": ask, "volume": vol,
                                    "oi": oi, "iv": iv, "itm": itm,
                                },
                            )
                            snap_count += 1
                            rows_for_df.append({
                                "strike": strike, "volume": vol,
                                "openInterest": oi, "impliedVolatility": iv,
                                "lastPrice": last_price, "bid": bid, "ask": ask,
                                "inTheMoney": itm,
                            })

                        df = pd.DataFrame(rows_for_df) if rows_for_df else pd.DataFrame()
                        if opt_type == "call":
                            total_call_oi += df["openInterest"].fillna(0).sum() if not df.empty else 0
                            total_call_vol += df["volume"].fillna(0).sum() if not df.empty else 0
                            all_calls_dfs.append(df)
                            if i == 0:
                                near_calls_df = df
                        else:
                            total_put_oi += df["openInterest"].fillna(0).sum() if not df.empty else 0
                            total_put_vol += df["volume"].fillna(0).sum() if not df.empty else 0
                            all_puts_dfs.append(df)
                            if i == 0:
                                near_puts_df = df

                    # ATM IV for this expiry
                    all_opts = chain_data.get("calls", []) + chain_data.get("puts", [])
                    atm_ivs = [
                        o["impliedVolatility"] for o in all_opts
                        if o.get("impliedVolatility") and o.get("strike")
                        and spot_price * 0.97 <= o["strike"] <= spot_price * 1.03
                    ]
                    if atm_ivs:
                        expiry_ivs.append((exp_date, float(np.mean(atm_ivs))))

                # Compute signals from nearest LIQUID expiration
                # Skip expiries within 2 days (near-worthless, garbage data)
                today_ts = datetime.now(timezone.utc).timestamp()
                min_dte_seconds = 2 * 86400  # 2 days
                liquid_expirations = [e for e in expirations if e - today_ts >= min_dte_seconds]
                if not liquid_expirations:
                    liquid_expirations = expirations  # Fallback
                near_expiry = datetime.utcfromtimestamp(liquid_expirations[0]).strftime("%Y-%m-%d")

                # If the nearest expiry was skipped, rebuild near_calls/puts from the correct expiry
                if liquid_expirations[0] != expirations[0]:
                    # Find which index in our pulled chains matches the liquid expiry
                    liquid_idx = None
                    for ci, e in enumerate(expirations[:MAX_EXPIRATIONS]):
                        if e == liquid_expirations[0]:
                            liquid_idx = ci
                            break
                    if liquid_idx is not None and liquid_idx < len(all_calls_dfs) and liquid_idx < len(all_puts_dfs):
                        near_calls_df = all_calls_dfs[liquid_idx]
                        near_puts_df = all_puts_dfs[liquid_idx]
                        log.info("{t}: skipped near-expiry, using DTE {d}d chain",
                                 t=ticker, d=int((liquid_expirations[0] - today_ts) / 86400))

                put_call_ratio = float(total_put_oi / total_call_oi) if total_call_oi > 0 else None
                max_pain = compute_max_pain(near_calls_df, near_puts_df, spot_price)
                iv_skew = compute_iv_skew(near_puts_df, spot_price)
                total_oi = int(total_call_oi + total_put_oi)
                total_volume = int(total_call_vol + total_put_vol)

                # ATM IV
                iv_atm = _compute_atm_iv(near_calls_df, near_puts_df, spot_price)

                # 25-delta wings
                iv_25d_put = _compute_wing_iv(near_puts_df, spot_price, delta_strike_pct=0.90)
                iv_25d_call = _compute_wing_iv(near_calls_df, spot_price, delta_strike_pct=1.10)

                # Term structure slope (IV of far expiry - near expiry)
                term_slope = None
                if len(expiry_ivs) >= 2:
                    term_slope = expiry_ivs[-1][1] - expiry_ivs[0][1]

                # OI concentration (max single strike OI / total OI)
                oi_conc = _compute_oi_concentration(near_calls_df, near_puts_df, total_oi)

                # Insert daily signals
                conn.execute(
                    text(
                        "INSERT INTO options_daily_signals "
                        "(ticker, signal_date, put_call_ratio, max_pain, iv_skew, "
                        "total_oi, total_volume, near_expiry, spot_price, iv_atm, "
                        "iv_25d_put, iv_25d_call, term_structure_slope, oi_concentration) "
                        "VALUES (:ticker, :sd, :pcr, :mp, :ivs, :oi, :vol, :ne, :spot, "
                        ":iv_atm, :iv_25d_put, :iv_25d_call, :ts_slope, :oi_conc) "
                        "ON CONFLICT (ticker, signal_date) DO UPDATE SET "
                        "put_call_ratio = EXCLUDED.put_call_ratio, "
                        "max_pain = EXCLUDED.max_pain, "
                        "iv_skew = EXCLUDED.iv_skew, "
                        "total_oi = EXCLUDED.total_oi, "
                        "total_volume = EXCLUDED.total_volume, "
                        "near_expiry = EXCLUDED.near_expiry, "
                        "spot_price = EXCLUDED.spot_price, "
                        "iv_atm = EXCLUDED.iv_atm, "
                        "iv_25d_put = EXCLUDED.iv_25d_put, "
                        "iv_25d_call = EXCLUDED.iv_25d_call, "
                        "term_structure_slope = EXCLUDED.term_structure_slope, "
                        "oi_concentration = EXCLUDED.oi_concentration"
                    ),
                    {
                        "ticker": ticker, "sd": today_str,
                        "pcr": put_call_ratio, "mp": max_pain, "ivs": iv_skew,
                        "oi": total_oi, "vol": total_volume,
                        "ne": near_expiry, "spot": spot_price,
                        "iv_atm": iv_atm, "iv_25d_put": iv_25d_put,
                        "iv_25d_call": iv_25d_call,
                        "ts_slope": term_slope, "oi_conc": oi_conc,
                    },
                )

                # Push to resolved_series for PIT access
                self._push_to_resolved(conn, ticker, today_str, {
                    "pcr": ("sentiment", f"{ticker} Put/Call Ratio", put_call_ratio),
                    "max_pain": ("sentiment", f"{ticker} Max Pain Strike", max_pain),
                    "iv_skew": ("vol", f"{ticker} IV Skew (OTM/ATM)", iv_skew),
                    "total_oi": ("sentiment", f"{ticker} Total Open Interest", total_oi),
                    "opt_vol": ("sentiment", f"{ticker} Total Options Volume", total_volume),
                    "iv_atm": ("vol", f"{ticker} ATM Implied Volatility", iv_atm),
                    "iv_25d_put": ("vol", f"{ticker} 25-Delta Put IV", iv_25d_put),
                    "iv_25d_call": ("vol", f"{ticker} 25-Delta Call IV", iv_25d_call),
                    "term_slope": ("vol", f"{ticker} IV Term Structure Slope", term_slope),
                    "oi_conc": ("sentiment", f"{ticker} OI Concentration", oi_conc),
                })

            log.info(
                "{t}: {n} snaps, PCR={pcr}, MaxPain={mp}, OI={oi}, IV_ATM={iv}",
                t=ticker, n=snap_count,
                pcr=f"{put_call_ratio:.2f}" if put_call_ratio else "N/A",
                mp=f"${max_pain:,.0f}" if max_pain else "N/A",
                oi=f"{total_oi:,}",
                iv=f"{iv_atm:.1%}" if iv_atm else "N/A",
            )
            return {
                "ticker": ticker, "status": "SUCCESS",
                "snapshots": snap_count, "rows_inserted": snap_count,
                "signals": {
                    "put_call_ratio": put_call_ratio,
                    "max_pain": max_pain,
                    "iv_skew": iv_skew,
                    "iv_atm": iv_atm,
                    "total_oi": total_oi,
                },
            }

        except Exception as e:
            log.error("{t}: {e}", t=ticker, e=e)
            return {"ticker": ticker, "status": "FAILED", "error": str(e), "rows_inserted": 0}

    def _push_to_resolved(
        self,
        conn: Any,
        ticker: str,
        today_str: str,
        signals: dict[str, tuple[str, str, Any]],
    ) -> None:
        """Push computed signals to feature_registry + resolved_series."""
        prefix = ticker.lower().replace("-", "_")
        for suffix, (family, desc, val) in signals.items():
            if val is None:
                continue
            feat_name = f"{prefix}_{suffix}"

            # Ensure family is valid for schema constraint
            if family not in ("rates", "credit", "equity", "vol", "fx",
                              "commodity", "sentiment", "macro", "crypto",
                              "alternative", "flows", "systemic", "trade",
                              "breadth", "earnings"):
                family = "sentiment"

            row = conn.execute(
                text(
                    "INSERT INTO feature_registry "
                    "(name, family, description, transformation, "
                    "transformation_version, lag_days, normalization, "
                    "missing_data_policy, eligible_from_date, model_eligible) "
                    "VALUES (:name, :fam, :desc, 'RAW', 1, 0, 'ZSCORE', "
                    "'FORWARD_FILL', '2024-04-01', TRUE) "
                    "ON CONFLICT (name) DO NOTHING RETURNING id"
                ),
                {"name": feat_name, "fam": family, "desc": desc},
            ).fetchone()

            if row:
                fid = row[0]
            else:
                fid_row = conn.execute(
                    text("SELECT id FROM feature_registry WHERE name = :name"),
                    {"name": feat_name},
                ).fetchone()
                if fid_row is None:
                    continue
                fid = fid_row[0]

            conn.execute(
                text(
                    "INSERT INTO resolved_series "
                    "(feature_id, obs_date, release_date, vintage_date, value, "
                    "source_priority_used) "
                    "VALUES (:fid, :od, :rd, :vd, :val, :src) "
                    "ON CONFLICT DO NOTHING"
                ),
                {
                    "fid": fid, "od": today_str, "rd": today_str,
                    "vd": today_str, "val": float(val), "src": self.source_id,
                },
            )


# ---------------------------------------------------------------------------
# Signal computation helpers
# ---------------------------------------------------------------------------

def compute_max_pain(
    calls_df: pd.DataFrame, puts_df: pd.DataFrame, spot_price: float
) -> float | None:
    """Compute max pain strike — where total option losses are minimized for writers."""
    if calls_df.empty or puts_df.empty:
        return None

    all_strikes = sorted(set(
        calls_df["strike"].tolist() + puts_df["strike"].tolist()
    ))
    if not all_strikes:
        return None

    min_pain = float("inf")
    max_pain_strike = spot_price

    for strike in all_strikes:
        call_pain = sum(
            max(0, strike - s) * oi
            for s, oi in zip(calls_df["strike"], calls_df["openInterest"].fillna(0))
        )
        put_pain = sum(
            max(0, s - strike) * oi
            for s, oi in zip(puts_df["strike"], puts_df["openInterest"].fillna(0))
        )
        total_pain = call_pain + put_pain
        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = strike

    return float(max_pain_strike)


def compute_iv_skew(puts_df: pd.DataFrame, spot_price: float) -> float | None:
    """IV skew: OTM put IV vs ATM put IV."""
    if puts_df.empty or "impliedVolatility" not in puts_df.columns:
        return None

    atm_puts = puts_df[
        (puts_df["strike"] >= spot_price * 0.97) &
        (puts_df["strike"] <= spot_price * 1.03)
    ]
    otm_puts = puts_df[
        (puts_df["strike"] >= spot_price * 0.85) &
        (puts_df["strike"] <= spot_price * 0.92)
    ]

    if atm_puts.empty or otm_puts.empty:
        return None

    atm_iv = atm_puts["impliedVolatility"].mean()
    otm_iv = otm_puts["impliedVolatility"].mean()

    if atm_iv and atm_iv > 0:
        return float(otm_iv / atm_iv)
    return None


def _compute_atm_iv(
    calls_df: pd.DataFrame, puts_df: pd.DataFrame, spot_price: float
) -> float | None:
    """ATM implied volatility — average of calls and puts near the money."""
    all_opts = pd.concat([calls_df, puts_df])
    if all_opts.empty or "impliedVolatility" not in all_opts.columns:
        return None

    atm = all_opts[
        (all_opts["strike"] >= spot_price * 0.98) &
        (all_opts["strike"] <= spot_price * 1.02)
    ]
    if atm.empty:
        return None

    iv = atm["impliedVolatility"].mean()
    return float(iv) if pd.notna(iv) else None


def _compute_wing_iv(
    df: pd.DataFrame, spot_price: float, delta_strike_pct: float
) -> float | None:
    """Compute average IV around a specific strike level (e.g. 90% or 110% of spot)."""
    if df.empty or "impliedVolatility" not in df.columns:
        return None

    target = spot_price * delta_strike_pct
    band = spot_price * 0.03
    wing = df[
        (df["strike"] >= target - band) &
        (df["strike"] <= target + band)
    ]
    if wing.empty:
        return None

    iv = wing["impliedVolatility"].mean()
    return float(iv) if pd.notna(iv) else None


def _compute_oi_concentration(
    calls_df: pd.DataFrame, puts_df: pd.DataFrame, total_oi: int
) -> float | None:
    """Max single-strike OI as fraction of total OI."""
    if total_oi == 0:
        return None

    all_opts = pd.concat([calls_df, puts_df])
    if all_opts.empty:
        return None

    max_oi = all_opts["openInterest"].fillna(0).max()
    return float(max_oi / total_oi)


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    puller = OptionsPuller(db_engine=engine)
    results = puller.pull_all()
    for r in results:
        print(f"  {r['ticker']:6s}  {r['status']}")
