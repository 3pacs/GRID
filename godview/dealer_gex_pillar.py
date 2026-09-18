"""godview.dealer_gex_pillar — dealer gamma exposure (GEX) God View pillar.

A from-scratch engine over ``options_snapshots`` (columns per ``schema.sql``:
``ticker, snap_date, expiry, opt_type, strike, last_price, bid, ask, volume,
open_interest, implied_vol, in_the_money``). The untracked
``derivatives/dealer_gex_engine.py`` in the sibling ``GRID`` checkout was
never read -- it is explicit incident evidence, out of bounds for this
lane, per the original task's read-only-cross-branch rules.

**Sign convention (stated explicitly, since options_snapshots carries no
dealer-vs-customer position split -- this is a MODELED convention, not
derived from real positioning data):** customers are assumed to buy both
calls and puts from dealers, so dealers are modeled as net short the
customer side of both. Following the standard public gamma-exposure
methodology (SqueezeMetrics-style, used by the great majority of public
"GEX" dashboards), CALL open interest contributes POSITIVELY and PUT open
interest contributes NEGATIVELY to net dealer gamma exposure at each
strike -- this encodes dealer call-side hedging flow as gamma-stabilizing
and put-side hedging flow as gamma-destabilizing. It is an explicit,
disclosed assumption (``provenance='modeled'`` for every gamma figure),
never presented as measured.

**Units:** dollar gamma per 1% move in the underlying --
``Gamma * Spot^2 * 0.01 * CONTRACT_MULTIPLIER * open_interest`` per
contract, summed with the sign convention above. ``CONTRACT_MULTIPLIER
= 100``, per FINRA's own investor page
(https://www.finra.org/investors/investing/investment-products/options,
quoted 2026-09-18): "A standard-size options contract is equal to 100
shares of the underlying security."

**Black-Scholes gamma** (``black_scholes_gamma``): risk-free rate and
dividend yield are both assumed 0% -- a standard simplification for gamma
(unlike delta/price, gamma is only weakly sensitive to r/q at typical
tenors), disclosed here rather than silently assumed. ``implied_vol`` is
read DIRECTLY from ``options_snapshots`` (already provided per contract --
this engine never solves for or assumes one). A contract with a missing/
non-positive ``implied_vol``, or with ``expiry <= snap_date`` (already
expired, so time-to-expiry T is not positive), is SKIPPED and counted
against ``coverage_fraction`` -- never defaulted to 0.25 or any other
literal.

**Spot price**: resolved via the SAME candidate-name rule
``api/routers/watchlist_helpers.py::_resolve_feature_names`` uses (that
module is tracked and imported directly here, not copied), then read
through ``store/pit.py::PITStore.get_pit`` (``LATEST_AS_OF``) -- mirroring
``evaluation/prices.py``'s own resolver on
``origin/fable/signal-eval-20260918`` (not merged into this branch, so
its logic is REPLICATED here rather than imported: candidate-name match
against ``feature_registry``, ``AmbiguousInstrumentError``-equivalent
refusal on >1 match, PIT-safe read, never a fresh guess). No spot,
ambiguous spot, or zero/negative spot -> the WHOLE ticker/date is
``unavailable`` -- this pillar never substitutes a median strike or any
other proxy for a real spot price.

**Gamma flip**: cumulative net gamma across strikes in ascending order;
the flip is the (linearly interpolated) strike where that cumulative
sum's SIGN changes between two adjacent strikes. No sign change anywhere
in the chain -> ``gamma_flip: unavailable`` -- never an endpoint strike.

**Coverage**: ``contracts_used / contracts_present`` -- contracts_present
is every ``options_snapshots`` row for that ticker/date; contracts_used
is how many had a usable IV and positive T (i.e. actually entered the
gamma sum).

Everything above the "DB wrappers" marker is pure Python.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Sequence

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from godview.generations import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    latest_attempt as _latest_attempt,
    latest_complete_generation as _latest_complete_generation,
    new_generation_id,
    record_generation,
)
from store.availability import measured_or_none
from store.pit import PITStore

PILLAR_NAME = "dealer_gex"
UNIT_USD_PER_1PCT_MOVE = "usd_per_1pct_move"

CONTRACT_MULTIPLIER = 100  # https://www.finra.org/investors/investing/investment-products/options
RISK_FREE_RATE = 0.0
DIVIDEND_YIELD = 0.0

SIGN_CONVENTION_NOTE = (
    "dealers modeled net short the customer side of both calls and puts; "
    "call OI contributes +gamma, put OI contributes -gamma to net dealer exposure "
    "(standard public GEX methodology, a stated modeling assumption -- "
    "options_snapshots carries no real dealer/customer position split)"
)
GAMMA_ASSUMPTIONS_NOTE = (
    f"Black-Scholes gamma, r={RISK_FREE_RATE}, q={DIVIDEND_YIELD} (both assumed 0, "
    "a standard simplification); implied_vol read directly from options_snapshots, "
    "never solved for or defaulted"
)

MISSING_INPUT = (
    "no real captured options chain fixture exists to validate this engine against a "
    "known-correct GEX figure -- the three synthetic cases in "
    "tests/godview/test_dealer_gex_pillar_pure.py exercise the MECHANICS (sign "
    "convention, gamma-flip interpolation, coverage under missing IV) but cannot "
    "confirm the sign convention or the modeled dealer-positioning assumption against "
    "a real chain. This prevents: any claim that a computed net_gex/gamma_flip figure "
    "matches a real dealer's actual book, and any promotion of this pillar's output "
    "from provenance='modeled' to anything stronger."
)

_SQRT_2PI = math.sqrt(2 * math.pi)


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / _SQRT_2PI


def black_scholes_gamma(spot: float, strike: float, t_years: float, sigma: float) -> float | None:
    """Black-Scholes Gamma (same formula for calls and puts). None if inputs are invalid.

    r and q are module-level constants (both 0), documented in
    GAMMA_ASSUMPTIONS_NOTE, not parameters -- this pillar never silently
    varies them per call.
    """
    if spot <= 0 or strike <= 0 or t_years <= 0 or sigma <= 0:
        return None
    try:
        d1 = (math.log(spot / strike) + (RISK_FREE_RATE - DIVIDEND_YIELD + 0.5 * sigma * sigma) * t_years) / (
            sigma * math.sqrt(t_years)
        )
    except (ValueError, ZeroDivisionError):
        return None
    denom = spot * sigma * math.sqrt(t_years)
    if denom <= 0:
        return None
    return _norm_pdf(d1) / denom


def contract_dollar_gamma(spot: float, gamma: float, open_interest: float, multiplier: int = CONTRACT_MULTIPLIER) -> float:
    """Dollar gamma per 1% move for one contract's full open interest."""
    return gamma * spot * spot * 0.01 * multiplier * open_interest


def signed_gamma_contribution(opt_type: str, dollar_gamma: float) -> float:
    """+dollar_gamma for calls, -dollar_gamma for puts. See SIGN_CONVENTION_NOTE."""
    return dollar_gamma if opt_type == "call" else -dollar_gamma


def compute_cumulative_by_strike(strike_net_gamma: dict[float, float]) -> list[tuple[float, float]]:
    """Ascending-strike list of (strike, cumulative net gamma up to and including that strike)."""
    running = 0.0
    out: list[tuple[float, float]] = []
    for strike in sorted(strike_net_gamma):
        running += strike_net_gamma[strike]
        out.append((strike, running))
    return out


def find_gamma_flip(cumulative_by_strike: Sequence[tuple[float, float]]) -> float | None:
    """Linearly interpolated strike where the cumulative sum's sign changes.

    None if there is no sign change anywhere in the chain -- never an
    endpoint strike, never a guess.
    """
    for (s1, c1), (s2, c2) in zip(cumulative_by_strike, cumulative_by_strike[1:]):
        if c1 == 0:
            return s1
        if (c1 < 0) != (c2 < 0):
            # Linear interpolation between (s1, c1) and (s2, c2) for the zero-crossing.
            if c2 == c1:
                continue
            frac = -c1 / (c2 - c1)
            return s1 + frac * (s2 - s1)
    return None


def classify_gex_regime(net_gex: float | None, *, neutral_band: float = 1e-6) -> str:
    """Always returns a value (gex_regime column is NOT NULL)."""
    if net_gex is None:
        return "insufficient_data"
    if net_gex > neutral_band:
        return "long_gamma"
    if net_gex < -neutral_band:
        return "short_gamma"
    return "neutral"


def compute_put_call_oi_ratio(put_oi_total: float, call_oi_total: float) -> float | None:
    if call_oi_total <= 0:
        return None
    return put_oi_total / call_oi_total


def resolve_atm_iv(rows: Sequence[dict[str, Any]], spot: float) -> float | None:
    """implied_vol of whichever row's strike is closest to spot. None if no usable row."""
    usable = [r for r in rows if r.get("implied_vol") is not None]
    if not usable:
        return None
    closest = min(usable, key=lambda r: abs(r["strike"] - spot))
    return closest["implied_vol"]


def compute_max_pain(rows: Sequence[dict[str, Any]]) -> float | None:
    """Strike minimizing total option-holder payout at that strike (a real, derivable stat).

    payout(K) = sum over calls of OI*max(K-strike,0) + sum over puts of OI*max(strike-K,0)
    -- evaluated at each strike present in the chain; the minimizing strike wins.
    """
    strikes = sorted({r["strike"] for r in rows})
    if not strikes:
        return None
    best_strike, best_payout = None, None
    for candidate_k in strikes:
        payout = 0.0
        for r in rows:
            oi = r.get("open_interest") or 0
            if r["opt_type"] == "call":
                payout += oi * max(candidate_k - r["strike"], 0)
            else:
                payout += oi * max(r["strike"] - candidate_k, 0)
        if best_payout is None or payout < best_payout:
            best_payout, best_strike = payout, candidate_k
    return best_strike


def coverage_fraction(contracts_used: int, contracts_present: int) -> float | None:
    if contracts_present <= 0:
        return None
    return contracts_used / contracts_present


@dataclass(frozen=True)
class MaterializationResult:
    status: str
    generation_id: str
    rows_written: int = 0
    tickers_with_data: int = 0
    tickers_discovered: int = 0
    message: str = ""


# ---------------------------------------------------------------------------
# DB wrappers
# ---------------------------------------------------------------------------


class SpotUnavailable(Exception):
    """No spot price -- never a fallback (median strike, etc.)."""


def resolve_spot_price(conn: Connection, engine: Engine, ticker: str, as_of: date) -> float:
    """Mirrors evaluation/prices.py's resolver (not merged into this branch --
    replicated here): candidate-name match against feature_registry, refuse
    on 0 or >1 matches, PIT-safe (LATEST_AS_OF) read. Raises SpotUnavailable
    rather than ever returning a guessed/proxy value.
    """
    from api.routers.watchlist_helpers import _resolve_feature_names

    candidates = _resolve_feature_names(ticker)
    rows = conn.execute(
        text("SELECT id, name FROM feature_registry WHERE name = ANY(:names)"),
        {"names": candidates},
    ).fetchall()
    if len(rows) == 0:
        raise SpotUnavailable(f"no feature_registry row matches any candidate name for {ticker}")
    if len(rows) > 1:
        raise SpotUnavailable(f"{len(rows)} feature_registry rows matched candidate names for {ticker}; refusing to pick one")

    feature_id = rows[0][0]
    pit = PITStore(engine)
    pit_df = pit.get_pit([feature_id], as_of, vintage_policy="LATEST_AS_OF")
    if pit_df.empty:
        raise SpotUnavailable(f"no PIT-available price for {ticker} as of {as_of}")

    latest = pit_df.sort_values("obs_date").iloc[-1]
    value = measured_or_none(latest["value"])
    if value is None or value <= 0:
        raise SpotUnavailable(f"resolved price for {ticker} is missing or non-positive")
    return float(value)


def _discover_tickers(conn: Connection, as_of: date) -> list[str]:
    rows = conn.execute(
        text("SELECT DISTINCT ticker FROM options_snapshots WHERE snap_date <= :as_of"),
        {"as_of": as_of},
    ).fetchall()
    return sorted(r[0] for r in rows)


def _read_snapshot_rows(conn: Connection, ticker: str, snap_date: date) -> list[dict[str, Any]]:
    rows = conn.execute(
        text(
            "SELECT ticker, snap_date, expiry, opt_type, strike, open_interest, implied_vol "
            "FROM options_snapshots WHERE ticker = :t AND snap_date = :d"
        ),
        {"t": ticker, "d": snap_date},
    ).mappings().all()
    return [dict(r) for r in rows]


def _latest_snap_date(conn: Connection, ticker: str, as_of: date) -> date | None:
    row = conn.execute(
        text("SELECT MAX(snap_date) FROM options_snapshots WHERE ticker = :t AND snap_date <= :as_of"),
        {"t": ticker, "as_of": as_of},
    ).fetchone()
    return row[0] if row else None


def _existing_obs_dates(conn: Connection, ticker: str) -> set[date]:
    rows = conn.execute(
        text("SELECT obs_date FROM dealer_gex_daily WHERE ticker = :t"), {"t": ticker}
    ).fetchall()
    return {r[0] for r in rows}


def _compute_gex_for_chain(
    rows: list[dict[str, Any]], spot: float, snap_date: date
) -> dict[str, Any] | None:
    """Pure-ish core (no DB): given a chain's rows + spot, compute every derived field.

    Returns None only if there are zero usable contracts at all.
    """
    strike_net_gamma: dict[float, float] = {}
    call_gex_total = 0.0
    put_gex_total = 0.0
    call_oi_total = 0.0
    put_oi_total = 0.0
    used = 0

    for r in rows:
        implied_vol = measured_or_none(r.get("implied_vol"))
        oi = measured_or_none(r.get("open_interest")) or 0.0
        t_years = (r["expiry"] - snap_date).days / 365.25
        if implied_vol is None or implied_vol <= 0 or t_years <= 0:
            continue
        gamma = black_scholes_gamma(spot, r["strike"], t_years, implied_vol)
        if gamma is None:
            continue
        dollar_gamma = contract_dollar_gamma(spot, gamma, oi)
        signed = signed_gamma_contribution(r["opt_type"], dollar_gamma)
        strike_net_gamma[r["strike"]] = strike_net_gamma.get(r["strike"], 0.0) + signed
        if r["opt_type"] == "call":
            call_gex_total += dollar_gamma
            call_oi_total += oi
        else:
            put_gex_total += dollar_gamma
            put_oi_total += oi
        used += 1

    if used == 0:
        return None

    cumulative = compute_cumulative_by_strike(strike_net_gamma)
    net_gex = cumulative[-1][1] if cumulative else 0.0
    gamma_flip = find_gamma_flip(cumulative)

    return {
        "net_gex": net_gex,
        "call_gex": call_gex_total,
        "put_gex": -put_gex_total,  # stored as the SIGNED (negative) contribution, consistent with net_gex
        "gamma_flip_strike": gamma_flip,
        "spot_to_flip_pct": ((gamma_flip - spot) / spot * 100.0) if gamma_flip is not None else None,
        "gex_regime": classify_gex_regime(net_gex),
        "max_pain_strike": compute_max_pain(rows),
        "put_call_oi_ratio": compute_put_call_oi_ratio(put_oi_total, call_oi_total),
        "atm_iv": resolve_atm_iv(rows, spot),
        "contracts_used": used,
        "contracts_present": len(rows),
    }


def materialize_dealer_gex_pillar(engine: Engine, *, as_of: date | None = None) -> MaterializationResult:
    """Materialize new dealer-GEX rows as one atomic generation."""
    as_of = as_of or date.today()
    generation_id = new_generation_id()

    try:
        with engine.begin() as conn:
            tickers = _discover_tickers(conn, as_of)
            if not tickers:
                raise _EmptyUpstream()

            rows_to_insert: list[dict[str, Any]] = []
            tickers_with_data: set[str] = set()

            for ticker in tickers:
                snap_date = _latest_snap_date(conn, ticker, as_of)
                if snap_date is None:
                    continue
                if snap_date in _existing_obs_dates(conn, ticker):
                    continue

                chain_rows = _read_snapshot_rows(conn, ticker, snap_date)
                if not chain_rows:
                    continue

                try:
                    spot = resolve_spot_price(conn, engine, ticker, as_of)
                except SpotUnavailable as exc:
                    # No fallback -- this ticker/date stays unmaterialized.
                    _ = exc
                    continue

                computed = _compute_gex_for_chain(chain_rows, spot, snap_date)
                if computed is None:
                    continue

                cov = coverage_fraction(computed["contracts_used"], computed["contracts_present"])
                source_ref = f"{SIGN_CONVENTION_NOTE}; {GAMMA_ASSUMPTIONS_NOTE}"

                rows_to_insert.append(
                    {
                        "obs_date": snap_date,
                        "ticker": ticker,
                        "spot_price": spot,
                        "net_gex_usd_m": computed["net_gex"] / 1_000_000.0,
                        "call_gex_usd_m": computed["call_gex"] / 1_000_000.0,
                        "put_gex_usd_m": computed["put_gex"] / 1_000_000.0,
                        "gamma_flip_strike": computed["gamma_flip_strike"],
                        "spot_to_flip_pct": computed["spot_to_flip_pct"],
                        "gex_regime": computed["gex_regime"],
                        "max_pain_strike": computed["max_pain_strike"],
                        "put_call_oi_ratio": computed["put_call_oi_ratio"],
                        "atm_iv": computed["atm_iv"],
                        "release_date": None,
                        "available_at": None,
                        "provenance": "modeled",
                        "availability_basis": "unknown",
                        "generation_id": generation_id,
                        "coverage_fraction": cov,
                        "contracts_used": computed["contracts_used"],
                        "contracts_present": computed["contracts_present"],
                        "source_ref": source_ref,
                    }
                )
                tickers_with_data.add(ticker)

            for row in rows_to_insert:
                conn.execute(
                    text(
                        """
                        INSERT INTO dealer_gex_daily (
                            obs_date, ticker, spot_price, net_gex_usd_m, call_gex_usd_m,
                            put_gex_usd_m, gamma_flip_strike, spot_to_flip_pct, gex_regime,
                            max_pain_strike, put_call_oi_ratio, atm_iv, release_date,
                            available_at, provenance, availability_basis, generation_id,
                            coverage_fraction, contracts_used, contracts_present, source_ref
                        ) VALUES (
                            :obs_date, :ticker, :spot_price, :net_gex_usd_m, :call_gex_usd_m,
                            :put_gex_usd_m, :gamma_flip_strike, :spot_to_flip_pct, :gex_regime,
                            :max_pain_strike, :put_call_oi_ratio, :atm_iv, :release_date,
                            :available_at, :provenance, :availability_basis, :generation_id,
                            :coverage_fraction, :contracts_used, :contracts_present, :source_ref
                        )
                        ON CONFLICT (obs_date, ticker) DO NOTHING
                        """
                    ),
                    row,
                )

            gen_coverage = len(tickers_with_data) / len(tickers) if tickers else None
            record_generation(
                conn, pillar=PILLAR_NAME, generation_id=generation_id,
                status=STATUS_COMPLETE, row_count=len(rows_to_insert),
                coverage_fraction=gen_coverage,
            )

        status = "SUCCESS" if rows_to_insert else "SUCCESS_NOOP"
        return MaterializationResult(
            status=status, generation_id=generation_id, rows_written=len(rows_to_insert),
            tickers_with_data=len(tickers_with_data), tickers_discovered=len(tickers),
            message=f"{len(rows_to_insert)} new row(s) across {len(tickers_with_data)} ticker(s)",
        )
    except _EmptyUpstream:
        _record_failure(engine, generation_id, "empty_upstream")
        return MaterializationResult(status="EMPTY", generation_id=generation_id, message="no options_snapshots rows")
    except Exception as exc:  # noqa: BLE001
        _record_failure(engine, generation_id, str(exc))
        return MaterializationResult(status="FAILED", generation_id=generation_id, message=str(exc))


class _EmptyUpstream(Exception):
    pass


def _record_failure(engine: Engine, generation_id: str, reason: str) -> None:
    try:
        with engine.begin() as conn:
            record_generation(conn, pillar=PILLAR_NAME, generation_id=generation_id, status=STATUS_FAILED, failure_reason=reason)
    except Exception:  # noqa: BLE001
        pass


@dataclass(frozen=True)
class PillarReadResult:
    state: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    tickers_with_data: int = 0
    generation_id: str | None = None
    generation_published_at: Any = None


def read_dealer_gex_pillar(conn: Connection, as_of: date) -> PillarReadResult:
    """Latest row per ticker with obs_date <= as_of."""
    generation = _latest_complete_generation(conn, PILLAR_NAME)
    attempt = _latest_attempt(conn, PILLAR_NAME)

    if generation is None:
        if attempt is not None and attempt["status"] == STATUS_FAILED:
            return PillarReadResult(state="materializer_failed")
        return PillarReadResult(state="never_configured")

    rows = conn.execute(
        text(
            "SELECT DISTINCT ON (ticker) "
            "ticker, obs_date, spot_price, net_gex_usd_m, call_gex_usd_m, put_gex_usd_m, "
            "gamma_flip_strike, spot_to_flip_pct, gex_regime, max_pain_strike, "
            "put_call_oi_ratio, atm_iv, provenance, availability_basis, generation_id, "
            "coverage_fraction, contracts_used, contracts_present, source_ref "
            "FROM dealer_gex_daily WHERE obs_date <= :as_of ORDER BY ticker, obs_date DESC"
        ),
        {"as_of": as_of},
    ).mappings().all()

    rows_out = [dict(r) for r in rows]
    return PillarReadResult(
        state="ok", rows=rows_out, tickers_with_data=len(rows_out),
        generation_id=generation["generation_id"], generation_published_at=generation["published_at"],
    )
