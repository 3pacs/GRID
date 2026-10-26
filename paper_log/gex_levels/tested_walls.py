"""Amendment 1: tested walls, computed independently of the engine's own
(untested) put_wall/call_wall.

Pre-registration (as amended): "The engine's own put wall and call wall
are recorded as `engine_put_wall` / `engine_call_wall` but are not tested.
The tested walls come from the engine's per-strike output: Put wall: among
strikes at or below 0.995 * P0, the strike with the largest put gamma
exposure by magnitude (|put_gex|). Call wall: among strikes at or above
1.005 * P0, the strike with the largest call gamma exposure (call_gex).
Ties go to the strike closer to P0. If no strike qualifies on a side,
that wall is missing for the session (recorded, not an exclusion)."

Why this isn't just reading ``DealerGammaEngine.compute_gex_profile()``'s
own ``per_strike`` field: that field is capped at the first 30 strikes by
ascending strike order (``physics/dealer_gamma.py``,
``"per_strike": [... for s in per_strike[:30]]``). SPY's chain routinely
has well over 30 distinct strikes with open interest across its listed
expiries, so the truncated list is biased toward the lowest strikes and
can be missing the entire above-P0 side needed for the call wall search —
exactly the kind of silent gap Amendment 1 exists to close.

Rather than re-derive Black-Scholes gamma independently (which could drift
from the engine's own math over time), this module recomputes per-strike
GEX using the SAME public primitives the engine itself uses —
``physics.dealer_gamma.bs_gamma`` and the ``DEALER_CALL_SIGN`` /
``DEALER_PUT_SIGN`` module constants — over the FULL chain, loaded with a
query that mirrors ``DealerGammaEngine._load_chain``'s filter exactly
(ticker, snap_date, open_interest > 0, implied_vol > 0, expiry > snap_date)
but, unlike that private method, never falls back to a different
``snap_date`` — the caller has already resolved a PIT-safe `snap_date` via
``chain.select_chain_snapshot``, and a silent fallback here would
reopen exactly the leak that guards against.

Amendment 1 item 5 (07ef4a3c) confirms and makes explicit a property this
module already had: gamma is computed *contract by contract* —
:func:`aggregate_per_strike_gex` derives ``T`` from each row's own
``expiry`` and uses each row's own ``implied_volatility``, never a
strike-grouped first-row value applied to the strike's whole open
interest. That grouped shortcut is the separate bug item 5 describes in
``DealerGammaEngine._compute_per_strike`` itself (one expiry's
time-to-expiry applied to all OI at a strike, on top of the 30-row cap) —
a fix for the pinned engine, tracked on the `fix/dealer-gamma-sign-spot-
20260924` lane, not something this module needed to change.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from sqlalchemy import Date, text
from sqlalchemy.engine import Engine

from physics.dealer_gamma import DEALER_CALL_SIGN, DEALER_PUT_SIGN, bs_gamma

# Matches DealerGammaEngine's own default (physics/dealer_gamma.py).
DEFAULT_RISK_FREE_RATE = 0.05

# `expiry` is selected plain (not `expiry - :snap_date` as
# DealerGammaEngine._load_chain does) and dte is computed in Python
# instead — SQL date subtraction is not portable across dialects (SQLite's
# `-` on DATE columns does not return a day-count the way Postgres's does;
# confirmed directly rather than assumed while writing this module's
# tests), and computing it in Python needs no dialect-specific SQL at all.
# `.columns(expiry=Date())` makes that subtraction well-typed regardless
# of dialect, the same technique `chain.py` uses for its own query.
_CHAIN_QUERY = text(
    """
    SELECT strike, opt_type, open_interest, implied_vol AS implied_volatility, expiry
    FROM options_snapshots
    WHERE ticker = :ticker AND snap_date = :snap_date
    AND open_interest > 0 AND implied_vol > 0
    AND expiry > :snap_date
    """
).columns(expiry=Date())


@dataclass(frozen=True)
class WallSelection:
    put_wall: float | None
    call_wall: float | None


def _load_full_chain_rows(db_engine: Engine, ticker: str, snap_date: date) -> list:
    with db_engine.connect() as conn:
        return conn.execute(_CHAIN_QUERY, {"ticker": ticker, "snap_date": snap_date}).fetchall()


def aggregate_per_strike_gex(
    rows,
    spot: float,
    snap_date: date,
    *,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
) -> dict[float, dict[str, float]]:
    """{strike: {"call_gex": ..., "put_gex": ...}}, aggregated across
    expiries at that strike — the same aggregation unit as
    ``DealerGammaEngine._compute_per_strike``'s ``chain.groupby("strike")``.

    ``rows`` are row-like objects (SQLAlchemy Row or any object) with
    ``.strike``, ``.opt_type`` ("call"/"put"), ``.open_interest``,
    ``.implied_volatility``, ``.expiry`` (a ``date``) attributes — this
    function is pure and takes no database dependency, so it's directly
    unit-testable with synthetic rows.
    """
    agg: dict[float, dict[str, float]] = {}
    for row in rows:
        dte = (row.expiry - snap_date).days
        if dte <= 0:
            continue
        T = dte / 365.0
        K = float(row.strike)
        iv = float(row.implied_volatility) if row.implied_volatility and row.implied_volatility > 0 else 0.25
        oi = float(row.open_interest)

        gamma_dollar = bs_gamma(spot, K, T, risk_free_rate, iv) * oi * 100.0 * spot

        bucket = agg.setdefault(K, {"call_gex": 0.0, "put_gex": 0.0})
        if row.opt_type == "call":
            bucket["call_gex"] += DEALER_CALL_SIGN * gamma_dollar
        else:
            bucket["put_gex"] += DEALER_PUT_SIGN * gamma_dollar

    return agg


def select_tested_walls(
    per_strike: dict[float, dict[str, float]],
    p0: float,
    *,
    min_distance_pct: float,
) -> WallSelection:
    """Amendment 1's tested-wall selection rule, given a
    ``{strike: {"call_gex", "put_gex"}}`` map from any source — pure and
    independently testable with synthetic per-strike data."""
    put_threshold = (1.0 - min_distance_pct) * p0
    call_threshold = (1.0 + min_distance_pct) * p0

    put_candidates = [
        (strike, abs(vals["put_gex"]))
        for strike, vals in per_strike.items()
        if strike <= put_threshold
    ]
    call_candidates = [
        (strike, vals["call_gex"])
        for strike, vals in per_strike.items()
        if strike >= call_threshold
    ]

    return WallSelection(
        put_wall=_pick_wall(put_candidates, p0),
        call_wall=_pick_wall(call_candidates, p0),
    )


def _pick_wall(candidates: list[tuple[float, float]], p0: float) -> float | None:
    """Largest exposure wins; ties go to the strike closer to P0."""
    if not candidates:
        return None
    best_exposure = max(exposure for _, exposure in candidates)
    tied = [strike for strike, exposure in candidates if exposure == best_exposure]
    return min(tied, key=lambda strike: abs(strike - p0))


def compute_tested_walls_from_db(
    db_engine: Engine,
    ticker: str,
    snap_date: date,
    spot: float,
    p0: float,
    *,
    min_distance_pct: float,
    risk_free_rate: float = DEFAULT_RISK_FREE_RATE,
) -> WallSelection:
    """End-to-end: load the full chain for `snap_date`, aggregate per-strike
    GEX at `spot` (the engine's own spot — so these numbers agree with what
    the engine's own per-strike output would show, uncapped), then select
    tested walls using `p0` per Amendment 1's rule."""
    rows = _load_full_chain_rows(db_engine, ticker, snap_date)
    per_strike = aggregate_per_strike_gex(rows, spot, snap_date, risk_free_rate=risk_free_rate)
    return select_tested_walls(per_strike, p0, min_distance_pct=min_distance_pct)
