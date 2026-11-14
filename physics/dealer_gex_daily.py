"""Writer for ``dealer_gex_daily`` (migrations/versions/god_view_market_tables_20260918.py): one SPY row per session.

The table had a reader (``godview/read_model.py``) and no writer. Each row is the dealer-gamma profile of one session's
latest options capture *completed by the end of that UTC day*, computed by ``physics.dealer_gamma.DealerGammaEngine``
(assumed dealer position: long calls, short puts; Black-Scholes from each contract's IV). The engine's own registered-
batch replay is used with that cutoff, so a later capture can never change a past row, and a capture from another day
is never used. Days before registered capture batches exist (2026-09-26 for SPY) have no row: the engine fails closed
on chains without batch provenance.

Columns:
  net/call/put_gex_usd_m   dealer gamma in $M per 1 % move in SPY (engine units x spot x 0.01 / 1e6)
  gamma_flip_strike        the GEX(spot) zero crossing nearest spot; a session without one is not written
  spot_to_flip_pct         (spot - flip) / spot x 100: positive = spot above the flip
  gex_regime               the engine's regime at spot
  max_pain_strike          for the nearest expiry at least one day out
  put_call_oi_ratio        put OI / call OI, every listed expiry in the capture
  atm_iv                   mean IV of contracts 7-45 days out within 1 % of spot
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

TICKER = "SPY"

_INSERT_SQL = text(
    "INSERT INTO dealer_gex_daily (obs_date, ticker, spot_price, net_gex_usd_m, call_gex_usd_m, put_gex_usd_m, "
    "gamma_flip_strike, spot_to_flip_pct, gex_regime, max_pain_strike, put_call_oi_ratio, atm_iv) "
    "VALUES (:d, :t, :spot, :net, :call, :put, :flip, :dist, :regime, :pain, :pcr, :iv) "
    "ON CONFLICT (obs_date, ticker) DO NOTHING"
)


def day_cutoff(d: date) -> datetime:
    """End of UTC day ``d``: the latest a capture may complete to count for that day."""
    return datetime.combine(d + timedelta(days=1), time(0), tzinfo=timezone.utc)


def max_pain(chain: pd.DataFrame) -> float | None:
    """Strike minimising the total intrinsic value paid at settlement, for one expiry's rows."""
    if chain.empty:
        return None
    ks = sorted(chain["strike"].astype(float).unique())
    calls = chain[chain["opt_type"].str.lower().str.startswith("c")]
    puts = chain[chain["opt_type"].str.lower().str.startswith("p")]

    def pay(k: float) -> float:
        return float(((k - calls["strike"]).clip(lower=0) * calls["open_interest"]).sum()
                     + ((puts["strike"] - k).clip(lower=0) * puts["open_interest"]).sum())

    return float(min(ks, key=pay))      # ks holds numpy scalars; the driver can't bind np.float64 under numpy 2


def chain_extras(chain: pd.DataFrame, spot: float) -> dict[str, float | None]:
    """max pain (nearest expiry >= 1 day out), put/call OI ratio, ATM IV (7-45 days, within 1 % of spot)."""
    c = chain.copy()
    c["dte"] = pd.to_numeric(c["dte"].apply(lambda x: getattr(x, "days", x)), errors="coerce")
    c["open_interest"] = pd.to_numeric(c["open_interest"], errors="coerce").fillna(0.0)
    c["strike"] = pd.to_numeric(c["strike"], errors="coerce")
    is_call = c["opt_type"].str.lower().str.startswith("c")
    call_oi, put_oi = float(c.loc[is_call, "open_interest"].sum()), float(c.loc[~is_call, "open_interest"].sum())
    out_exp = c[c["dte"] >= 1]
    pain = None
    if not out_exp.empty:
        nearest = out_exp["dte"].min()
        pain = max_pain(out_exp[out_exp["dte"] == nearest])
    iv = pd.to_numeric(c["implied_volatility"], errors="coerce")
    atm = c[(c["dte"] >= 7) & (c["dte"] <= 45) & ((c["strike"] / spot - 1).abs() <= 0.01) & (iv > 0)]
    atm_iv = float(pd.to_numeric(atm["implied_volatility"]).mean()) if not atm.empty else None
    return {"max_pain_strike": pain, "put_call_oi_ratio": put_oi / call_oi if call_oi > 0 else None, "atm_iv": atm_iv}


def compute_row(engine: Engine, d: date, ticker: str = TICKER) -> dict[str, Any]:
    """The ``dealer_gex_daily`` row for session ``d``, or ``{"skip": reason}``."""
    from physics.dealer_gamma import DealerGammaEngine

    eng = DealerGammaEngine(engine)
    cutoff = day_cutoff(d)
    sel = eng.latest_completed_capture_batch(ticker, d, cutoff)
    if sel is None or sel[0] != d:
        return {"skip": "no registered capture of this session completed by the end of its UTC day"}
    batch = sel[1]
    prof = eng.compute_gex_profile(ticker, d, capture_batch_id=batch, cutoff=cutoff)
    if prof.get("error") or prof.get("available") is False:
        return {"skip": f"engine: {prof.get('error') or prof.get('reason')}"}
    spot, flip = float(prof["spot"]), prof.get("gamma_flip")
    if flip is None:
        return {"skip": "no GEX zero crossing within the engine's search range"}
    chain = eng._load_chain(ticker, d, capture_batch_id=batch, cutoff=cutoff)
    per = eng._compute_per_strike(chain, spot)
    to_m = spot * 0.01 / 1e6
    call = sum(s["call_gex"] for s in per) * to_m
    put = sum(s["put_gex"] for s in per) * to_m
    ex = chain_extras(chain, spot)
    if any(ex[k] is None or (isinstance(ex[k], float) and math.isnan(ex[k])) for k in ex):
        return {"skip": f"chain extras incomplete: {ex}"}
    return {"obs_date": d, "ticker": ticker, "spot_price": spot, "net_gex_usd_m": float(prof["gex_aggregate"]) * to_m,
            "call_gex_usd_m": call, "put_gex_usd_m": put, "gamma_flip_strike": float(flip),
            "spot_to_flip_pct": (spot - float(flip)) / spot * 100.0, "gex_regime": str(prof.get("regime")),
            "capture_batch_id": batch, "capture_completed_at": prof.get("chain_capture_completed_at"), **ex}


_NUMERIC = {"spot": "spot_price", "net": "net_gex_usd_m", "call": "call_gex_usd_m", "put": "put_gex_usd_m",
            "flip": "gamma_flip_strike", "dist": "spot_to_flip_pct", "pain": "max_pain_strike",
            "pcr": "put_call_oi_ratio", "iv": "atm_iv"}


def insert_params(row: dict[str, Any]) -> dict[str, Any]:
    """Bind parameters for ``_INSERT_SQL``: numeric columns as plain Python floats.

    Under numpy 2, psycopg2 renders an ``np.float64`` by its repr (``np.float64(765.0)``), which Postgres reads as a
    schema-qualified name and rejects (first live run, 2026-11-13).
    """
    out: dict[str, Any] = {"d": row["obs_date"], "t": row["ticker"], "regime": row["gex_regime"]}
    for key, col in _NUMERIC.items():
        v = row[col]
        out[key] = None if v is None else float(v)
    return out


def store_row(engine: Engine, row: dict[str, Any]) -> bool:
    with engine.begin() as conn:
        res = conn.execute(_INSERT_SQL, insert_params(row))
    return bool(res.rowcount)


def run(engine: Engine, days: list[date], *, dry_run: bool = False) -> dict[str, Any]:
    written, present, skipped = 0, 0, {}
    for d in days:
        row = compute_row(engine, d)
        if "skip" in row:
            skipped[d.isoformat()] = row["skip"]
            continue
        if dry_run:
            log.info("dealer_gex_daily {d}: net {n:.0f}M flip {f} regime {r}", d=d, n=row["net_gex_usd_m"],
                     f=row["gamma_flip_strike"], r=row["gex_regime"])
            written += 1
            continue
        if store_row(engine, row):
            written += 1
        else:
            present += 1
    return {"job": "dealer_gex_daily", "sessions": len(days), "written": written, "already_present": present,
            "skipped": skipped, "dry_run": dry_run}
