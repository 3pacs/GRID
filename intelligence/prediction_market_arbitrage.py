"""
GRID Prediction Market Arbitrage Detector (CAT-183 / #285).

Cross-market edge detector that flags when the GRID oracle disagrees with
liquid prediction markets (Polymarket / Kalshi / Manifold). When the oracle
says "BTC >= 110K by month-end at p=0.72" but Polymarket is trading the same
event at p=0.45, either:

    (a) the oracle is wrong,
    (b) the prediction market is wrong, or
    (c) there is real edge.

This module scores the magnitude of the disagreement and returns a conviction
multiplier that *rewards calibrated disagreement* (oracle has history of
beating the market on similar ticker/direction/horizon triples) and
*punishes uncalibrated disagreement* (oracle is extrapolating into a regime
with no track record).

## Public API

    ArbitrageReport                  — frozen dataclass report
    build_arbitrage_report(...)      — full report builder
    arbitrage_conviction_multiplier  — live-path entry point (never raises)

## Data sources

- `prediction_odds` table (Polymarket / Kalshi snapshots) — schema:
    ticker, question_direction, yes_price, resolves_on, created_at, ...
  This module defensively tolerates the table not existing: missing table
  is treated the same as "no coverage" and returns a neutral multiplier.

- `oracle_predictions` table (GRID oracle history) — used to compute the
  oracle's calibration edge vs the prediction market on the same
  ticker/direction/horizon bucket.

## Lookahead guard

Every SELECT applies `created_at <= :as_of`. Nothing created after the
decision timestamp is ever used to score that decision.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError

# ── Magnitude thresholds (hardcoded module constants) ─────────────────

_MAG_NOISE: float = 0.05       # < 5 points → signal_strength 0.0 (noise)
_MAG_SMALL: float = 0.10       # < 10 points → signal_strength 0.5
_MAG_MEDIUM: float = 0.20      # < 20 points → signal_strength 0.8
# >= 20 points → signal_strength 1.0 (extreme)

_SIGNAL_SMALL: float = 0.5
_SIGNAL_MEDIUM: float = 0.8
_SIGNAL_EXTREME: float = 1.0

# ── Multiplier envelope ───────────────────────────────────────────────

_ALIGNED_MAX_BOOST: float = 0.10   # [1.00, 1.10] when oracle + market agree
_MISALIGNED_MAX_HAIRCUT: float = 0.05  # [0.95, 1.00] when oracle is contra-trade

# ── Calibration gate ──────────────────────────────────────────────────

_CALIBRATION_MIN_SAMPLES: int = 20
_CALIBRATION_MIN_EDGE: float = 0.05
_UNCALIBRATED_HAIRCUT: float = 0.5     # unvalidated signal gets halved

# ── Resolution window fuzzy-match ─────────────────────────────────────

_WINDOW_LOWER: float = 0.80   # target = horizon_days * 0.80
_WINDOW_UPPER: float = 1.20   # target = horizon_days * 1.20

# ── Valid trade directions (anything else → neutral) ──────────────────

_BULLISH: frozenset[str] = frozenset({"bullish", "long", "up", "call"})
_BEARISH: frozenset[str] = frozenset({"bearish", "short", "down", "put"})


# ══════════════════════════════════════════════════════════════════════
# Report dataclass
# ══════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class ArbitrageReport:
    """Immutable report from a single arbitrage check.

    Attributes:
        ticker: Ticker under evaluation.
        as_of: ISO-formatted decision date.
        oracle_confidence: Oracle's probability (0..1).
        market_implied_prob: Prediction market's probability, or None if
            no coverage exists for this ticker/direction/horizon.
        disagreement: oracle_confidence - market_implied_prob, or 0 when
            there is no market.
        signal_strength: Bucketed magnitude in [0, 1].
        oracle_calibrated_vs_market: True if the oracle has historical
            edge over the market on this bucket.
        n_head_to_head: Number of prior prediction/market pairs used
            to compute calibration.
        conviction_multiplier: Final multiplier, clamped to [0.95, 1.10].
        advisory: Human-readable short description of the state.
    """

    ticker: str
    as_of: str
    oracle_confidence: float
    market_implied_prob: float | None
    disagreement: float
    signal_strength: float
    oracle_calibrated_vs_market: bool
    n_head_to_head: int
    conviction_multiplier: float
    advisory: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dict representation."""
        return asdict(self)


def _neutral_report(
    *,
    ticker: str,
    as_of: date,
    oracle_confidence: float,
    advisory: str,
) -> ArbitrageReport:
    """Construct a neutral (multiplier=1.00) report for failure paths."""
    return ArbitrageReport(
        ticker=ticker,
        as_of=as_of.isoformat(),
        oracle_confidence=float(oracle_confidence),
        market_implied_prob=None,
        disagreement=0.0,
        signal_strength=0.0,
        oracle_calibrated_vs_market=False,
        n_head_to_head=0,
        conviction_multiplier=1.00,
        advisory=advisory,
    )


# ══════════════════════════════════════════════════════════════════════
# Data access — every call is defensive
# ══════════════════════════════════════════════════════════════════════


def get_market_implied_prob(
    engine: Engine,
    *,
    ticker: str,
    direction: str,
    horizon_days: int,
    as_of: date,
) -> float | None:
    """Fetch the most recent prediction-market implied probability.

    Applies a fuzzy resolution window match: questions resolving between
    ``horizon_days * 0.80`` and ``horizon_days * 1.20`` days out from
    ``as_of`` are eligible. The most recently created matching row wins.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol, e.g. "BTC", "SPY".
        direction: Question direction ("bullish" / "bearish" / raw text).
        horizon_days: Target horizon in days.
        as_of: Decision cutoff; nothing newer is used.

    Returns:
        Implied probability in [0, 1], or None if no row matches OR the
        table does not exist.
    """
    if horizon_days <= 0:
        return None

    lower_resolve = as_of + timedelta(days=int(round(horizon_days * _WINDOW_LOWER)))
    upper_resolve = as_of + timedelta(days=int(round(horizon_days * _WINDOW_UPPER)))
    direction_norm = _canon_direction(direction)

    sql = text(
        """
        SELECT yes_price, resolves_on, created_at
        FROM prediction_odds
        WHERE ticker = :ticker
          AND question_direction = :direction
          AND resolves_on >= :lower_resolve
          AND resolves_on <= :upper_resolve
          AND created_at <= :as_of
        ORDER BY created_at DESC
        LIMIT 1
        """
    )

    try:
        with engine.connect() as conn:
            row = conn.execute(
                sql,
                {
                    "ticker": ticker,
                    "direction": direction_norm,
                    "lower_resolve": lower_resolve,
                    "upper_resolve": upper_resolve,
                    "as_of": as_of,
                },
            ).fetchone()
    except (ProgrammingError, OperationalError) as exc:
        log.debug(
            "prediction_odds table missing or unreadable: {e}",
            e=str(exc),
        )
        return None
    except Exception as exc:  # pragma: no cover — defensive safety net
        log.warning("get_market_implied_prob failed: {e}", e=str(exc))
        return None

    if row is None:
        return None

    try:
        yes_price = float(row[0])
    except (TypeError, ValueError, IndexError):
        return None

    if yes_price != yes_price:  # NaN guard
        return None
    if yes_price < 0.0 or yes_price > 1.0:
        return None

    return yes_price


def get_oracle_vs_market_calibration(
    engine: Engine,
    *,
    ticker: str,
    direction: str,
    horizon_days: int,
    as_of: date,
) -> tuple[bool, int]:
    """Query oracle_predictions for prior head-to-head samples.

    Compares the oracle's hit rate against a pure-market-follower
    baseline on the same ticker/direction/horizon bucket. A bucket is
    considered "calibrated" when the oracle has at least
    ``_CALIBRATION_MIN_SAMPLES`` prior scored outcomes AND its edge over
    the market-follower is at least ``_CALIBRATION_MIN_EDGE``.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.
        direction: Trade direction under evaluation.
        horizon_days: Target horizon in days.
        as_of: Decision cutoff (lookahead guard).

    Returns:
        Tuple of ``(is_calibrated, n_head_to_head)``. Returns
        ``(False, 0)`` on any error.
    """
    direction_norm = _canon_direction(direction)

    sql = text(
        """
        SELECT oracle_hit, market_hit
        FROM oracle_predictions
        WHERE ticker = :ticker
          AND direction = :direction
          AND horizon_days = :horizon_days
          AND created_at <= :as_of
          AND verdict IN ('hit', 'miss', 'partial')
          AND oracle_hit IS NOT NULL
          AND market_hit IS NOT NULL
        """
    )

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sql,
                {
                    "ticker": ticker,
                    "direction": direction_norm,
                    "horizon_days": int(horizon_days),
                    "as_of": as_of,
                },
            ).fetchall()
    except (ProgrammingError, OperationalError) as exc:
        log.debug(
            "oracle_predictions calibration query failed "
            "(columns or table missing): {e}",
            e=str(exc),
        )
        return (False, 0)
    except Exception as exc:  # pragma: no cover — defensive safety net
        log.warning("get_oracle_vs_market_calibration failed: {e}", e=str(exc))
        return (False, 0)

    n = len(rows)
    if n < _CALIBRATION_MIN_SAMPLES:
        return (False, n)

    oracle_hits = 0
    market_hits = 0
    for row in rows:
        try:
            oracle_hits += int(bool(row[0]))
            market_hits += int(bool(row[1]))
        except (TypeError, ValueError, IndexError):
            continue

    edge = (oracle_hits - market_hits) / float(n)
    is_calibrated = edge >= _CALIBRATION_MIN_EDGE
    return (is_calibrated, n)


# ══════════════════════════════════════════════════════════════════════
# Pure scoring helpers
# ══════════════════════════════════════════════════════════════════════


def _canon_direction(direction: str) -> str:
    """Normalise a direction label to 'bullish' / 'bearish' / raw."""
    if not isinstance(direction, str):
        return ""
    lowered = direction.strip().lower()
    if lowered in _BULLISH:
        return "bullish"
    if lowered in _BEARISH:
        return "bearish"
    return lowered


def _signal_strength(magnitude: float) -> float:
    """Map absolute disagreement to a bucketed signal strength in [0,1]."""
    if magnitude < _MAG_NOISE:
        return 0.0
    if magnitude < _MAG_SMALL:
        return _SIGNAL_SMALL
    if magnitude < _MAG_MEDIUM:
        return _SIGNAL_MEDIUM
    return _SIGNAL_EXTREME


def _direction_aware_multiplier(
    *,
    disagreement: float,
    trade_direction: str,
    trusted_signal: float,
) -> float:
    """Compute the direction-aware conviction multiplier.

    +disagreement = oracle more bullish than market.
    -disagreement = oracle more bearish than market.

    Aligned (oracle and trade on the same side) → reward up to +0.10.
    Misaligned (oracle fights the trade)        → haircut up to -0.05.
    """
    canon = _canon_direction(trade_direction)
    if canon not in ("bullish", "bearish"):
        return 1.00
    if trusted_signal <= 0.0:
        return 1.00

    bullish_edge = disagreement > 0 and canon == "bullish"
    bearish_edge = disagreement < 0 and canon == "bearish"
    if bullish_edge or bearish_edge:
        return 1.00 + _ALIGNED_MAX_BOOST * trusted_signal

    oracle_fights_long = disagreement < 0 and canon == "bullish"
    oracle_fights_short = disagreement > 0 and canon == "bearish"
    if oracle_fights_long or oracle_fights_short:
        return 1.00 - _MISALIGNED_MAX_HAIRCUT * trusted_signal

    return 1.00


# ══════════════════════════════════════════════════════════════════════
# Public API — report + live-path multiplier
# ══════════════════════════════════════════════════════════════════════


def build_arbitrage_report(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
    direction: str,
    horizon_days: int,
    oracle_confidence: float,
) -> ArbitrageReport:
    """Build a full arbitrage report for a single decision.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.
        as_of: Decision date (used as the lookahead cutoff).
        direction: Trade direction (bullish / bearish / long / short / ...).
        horizon_days: Target holding horizon.
        oracle_confidence: Oracle's probability for the event (0..1).

    Returns:
        A fully populated ``ArbitrageReport``. The multiplier is always
        within [0.95, 1.10]. On any DB failure the multiplier is 1.00
        and the advisory explains why.
    """
    try:
        safe_confidence = float(oracle_confidence)
    except (TypeError, ValueError):
        safe_confidence = 0.0

    if safe_confidence != safe_confidence:  # NaN
        safe_confidence = 0.0
    safe_confidence = max(0.0, min(1.0, safe_confidence))

    market_prob = get_market_implied_prob(
        engine,
        ticker=ticker,
        direction=direction,
        horizon_days=horizon_days,
        as_of=as_of,
    )

    if market_prob is None:
        return _neutral_report(
            ticker=ticker,
            as_of=as_of,
            oracle_confidence=safe_confidence,
            advisory="no prediction market coverage",
        )

    disagreement = safe_confidence - market_prob
    magnitude = abs(disagreement)
    signal_strength = _signal_strength(magnitude)

    calibrated, n_head_to_head = get_oracle_vs_market_calibration(
        engine,
        ticker=ticker,
        direction=direction,
        horizon_days=horizon_days,
        as_of=as_of,
    )

    trusted_signal = signal_strength if calibrated else signal_strength * _UNCALIBRATED_HAIRCUT

    multiplier = _direction_aware_multiplier(
        disagreement=disagreement,
        trade_direction=direction,
        trusted_signal=trusted_signal,
    )

    advisory = _advisory(
        magnitude=magnitude,
        disagreement=disagreement,
        direction=direction,
        calibrated=calibrated,
    )

    return ArbitrageReport(
        ticker=ticker,
        as_of=as_of.isoformat(),
        oracle_confidence=safe_confidence,
        market_implied_prob=market_prob,
        disagreement=disagreement,
        signal_strength=signal_strength,
        oracle_calibrated_vs_market=calibrated,
        n_head_to_head=n_head_to_head,
        conviction_multiplier=multiplier,
        advisory=advisory,
    )


def arbitrage_conviction_multiplier(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
    direction: str,
    horizon_days: int,
    oracle_confidence: float,
) -> float:
    """Return the conviction multiplier only. Never raises.

    This is the live-path entry point the trading stack calls. On any
    exception it returns 1.0 and logs the failure.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.
        as_of: Decision date.
        direction: Trade direction.
        horizon_days: Target horizon.
        oracle_confidence: Oracle's probability (0..1).

    Returns:
        Float multiplier, typically in [0.95, 1.10]. Always returns 1.0
        on any unexpected failure.
    """
    try:
        report = build_arbitrage_report(
            engine,
            ticker=ticker,
            as_of=as_of,
            direction=direction,
            horizon_days=horizon_days,
            oracle_confidence=oracle_confidence,
        )
        return float(report.conviction_multiplier)
    except Exception as exc:  # pragma: no cover — ultimate safety net
        log.warning(
            "arbitrage_conviction_multiplier neutralised by exception: {e}",
            e=str(exc),
        )
        return 1.0


# ══════════════════════════════════════════════════════════════════════
# Private helpers
# ══════════════════════════════════════════════════════════════════════


def _advisory(
    *,
    magnitude: float,
    disagreement: float,
    direction: str,
    calibrated: bool,
) -> str:
    """Build a short human-readable advisory for the report."""
    if magnitude < _MAG_NOISE:
        return "oracle and market agree within noise"

    canon = _canon_direction(direction)
    side = "bullish" if disagreement > 0 else "bearish"
    cal = "calibrated" if calibrated else "uncalibrated"
    magnitude_pct = int(round(magnitude * 100))

    if canon not in ("bullish", "bearish"):
        return (
            f"{cal} disagreement — oracle {side} by {magnitude_pct}pts vs market"
            " (trade direction unknown)"
        )

    aligned = (
        (disagreement > 0 and canon == "bullish")
        or (disagreement < 0 and canon == "bearish")
    )
    relation = "aligned with" if aligned else "fighting"
    return (
        f"{cal} disagreement — oracle {side} by {magnitude_pct}pts, "
        f"{relation} {canon} trade"
    )


__all__ = [
    "ArbitrageReport",
    "arbitrage_conviction_multiplier",
    "build_arbitrage_report",
    "get_market_implied_prob",
    "get_oracle_vs_market_calibration",
]
