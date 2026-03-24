"""GRID — Options chain ingestion via yfinance.

Pulls options chains for equity tickers, computes daily signals
(put/call ratio, max pain, IV skew, total OI, vol surface metrics),
and pushes to resolved_series for PIT-correct access.

Uses BasePuller for source_catalog resolution and deduplication.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


# Tickers with listed equity options
EQUITY_TICKERS: list[str] = [
    "SPY", "QQQ", "IWM",           # Index ETFs
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META",  # Mega-cap
    "EOG", "DVN",                   # Energy
    "CMCSA", "CI", "PYPL",         # Value plays
    "RTX", "GD",                    # Defense
    "INTC",                         # Watchlist
]

# Maximum expirations to pull per ticker
MAX_EXPIRATIONS = 6


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
        try:
            import yfinance as yf  # noqa: F401
        except ImportError:
            log.error("yfinance not installed: pip install yfinance")
            return [{"ticker": "N/A", "status": "FAILED", "error": "yfinance not installed"}]

        tickers = tickers or EQUITY_TICKERS
        today_str = date.today().isoformat()
        results: list[dict[str, Any]] = []

        for ticker in tickers:
            result = self._pull_ticker(ticker, today_str)
            results.append(result)

        total_snaps = sum(r.get("snapshots", 0) for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "Options pull complete — {ok}/{total} tickers, {snaps} snapshots",
            ok=succeeded, total=len(tickers), snaps=total_snaps,
        )
        return results

    def _pull_ticker(self, ticker: str, today_str: str) -> dict[str, Any]:
        """Pull options chain for a single ticker and compute signals."""
        import yfinance as yf

        try:
            stock = yf.Ticker(ticker)
            info = stock.info or {}
            spot_price = info.get("regularMarketPrice") or info.get("previousClose")
            if not spot_price:
                log.warning("{t}: no spot price available", t=ticker)
                return {"ticker": ticker, "status": "SKIPPED", "reason": "no spot price"}

            expirations = stock.options
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

            with self.engine.begin() as conn:
                for exp_date in expirations[:MAX_EXPIRATIONS]:
                    chain = stock.option_chain(exp_date)

                    for opt_type, df in [("call", chain.calls), ("put", chain.puts)]:
                        if df.empty:
                            continue
                        for _, row in df.iterrows():
                            vol = int(row["volume"]) if pd.notna(row.get("volume")) else 0
                            oi = int(row["openInterest"]) if pd.notna(row.get("openInterest")) else 0
                            iv = float(row["impliedVolatility"]) if pd.notna(row.get("impliedVolatility")) else None

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
                                    "ticker": ticker,
                                    "snap_date": today_str,
                                    "expiry": exp_date,
                                    "opt_type": opt_type,
                                    "strike": row.get("strike"),
                                    "last_price": row.get("lastPrice"),
                                    "bid": row.get("bid"),
                                    "ask": row.get("ask"),
                                    "volume": vol,
                                    "oi": oi,
                                    "iv": iv,
                                    "itm": bool(row.get("inTheMoney")),
                                },
                            )
                            snap_count += 1

                        oi_sum = df["openInterest"].fillna(0).sum()
                        vol_sum = df["volume"].fillna(0).sum()
                        if opt_type == "call":
                            total_call_oi += oi_sum
                            total_call_vol += vol_sum
                        else:
                            total_put_oi += oi_sum
                            total_put_vol += vol_sum

                    # ATM IV for this expiry
                    all_ivs = pd.concat([chain.calls, chain.puts])
                    atm_mask = (all_ivs["strike"] >= spot_price * 0.97) & (all_ivs["strike"] <= spot_price * 1.03)
                    atm_iv = all_ivs.loc[atm_mask, "impliedVolatility"].mean()
                    if pd.notna(atm_iv):
                        expiry_ivs.append((exp_date, float(atm_iv)))

                # Compute signals from nearest expiration
                near_expiry = expirations[0]
                chain = stock.option_chain(near_expiry)

                put_call_ratio = float(total_put_oi / total_call_oi) if total_call_oi > 0 else None
                max_pain = compute_max_pain(chain.calls, chain.puts, spot_price)
                iv_skew = compute_iv_skew(chain.puts, spot_price)
                total_oi = int(total_call_oi + total_put_oi)
                total_volume = int(total_call_vol + total_put_vol)

                # ATM IV
                iv_atm = _compute_atm_iv(chain.calls, chain.puts, spot_price)

                # 25-delta wings
                iv_25d_put = _compute_wing_iv(chain.puts, spot_price, delta_strike_pct=0.90)
                iv_25d_call = _compute_wing_iv(chain.calls, spot_price, delta_strike_pct=1.10)

                # Term structure slope (IV of far expiry - near expiry)
                term_slope = None
                if len(expiry_ivs) >= 2:
                    term_slope = expiry_ivs[-1][1] - expiry_ivs[0][1]

                # OI concentration (max single strike OI / total OI)
                oi_conc = _compute_oi_concentration(chain.calls, chain.puts, total_oi)

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
