"""EDGE-signal multipliers derived from the backtest edge_table.

The 2026-05-11 walk-forward backtest identified 124 ``(source_type,
ticker, signal_direction)`` triples with statistically significant
information coefficient (IC) and small p-values — these are the
empirical "edges" hidden inside the signal universe. Examples:

  * insider SELL on AMZN: hit rate when present 80.3% vs absent 17.4%
    → IC +0.62 (signal genuinely predicts).
  * quiverquant:offexchange on META: hit rate when present 19.3% vs
    absent 78.0% → IC -0.53 (signal predicts the OPPOSITE direction;
    take contrarian).

This module loads ``outputs/backtest/edge_table.csv`` once, exposes a
lookup keyed by ``(source_type, ticker, signal_direction)``, and turns
each row into a conviction multiplier that downstream consumers
(``intelligence.hypothesis_engine``, ``intelligence.signal_provenance``)
can fold into their existing boost chains.

Multiplier semantics
--------------------

For a positive-IC edge (signal aligned with truth):

    multiplier = 1 + EDGE_BOOST_GAIN * IC

For a negative-IC edge (contrarian — signal fires when actuals move
opposite):

    multiplier = 1 - EDGE_PENALTY_GAIN * |IC|

So +0.62 IC with default ``EDGE_BOOST_GAIN = 0.8`` becomes a 1.5×
boost; -0.53 IC with default ``EDGE_PENALTY_GAIN = 0.8`` becomes a
0.58× penalty. Both are bounded to ``(EDGE_MULTIPLIER_MIN,
EDGE_MULTIPLIER_MAX)`` so a single edge can never dominate aggregate
conviction.

When the (source_type, ticker, signal_direction) triple is not in the
edge table, the lookup returns ``None`` and the caller should fall
through to its existing default (multiplier = 1.0).
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Any

from loguru import logger as log


# Default location of the edge_table.csv produced by walk_forward_validate.
# Overridable via env so the tests + ops can pin a specific snapshot.
_DEFAULT_EDGE_TABLE_PATH = (
    Path(__file__).resolve().parent.parent
    / "outputs" / "backtest" / "edge_table.csv"
)
_EDGE_TABLE_PATH_ENV = "GRID_EDGE_TABLE_PATH"


# ── Tuning knobs (all readable via env so the operator can adjust
# without code changes) ──────────────────────────────────────────────────


def _env_float(name: str, default: float) -> float:
    """Read a float env var or return default. Tolerant of empty / junk."""
    raw = os.environ.get(name, "")
    if not raw.strip():
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        log.warning(
            "edge_signals: env {n}={v!r} is not a float; using default {d}",
            n=name, v=raw, d=default,
        )
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "")
    if not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "enabled")


# Master kill-switch. Defaults to FALSE so production behaviour is
# unchanged until the operator opts in. Flip to true (env or set_enabled)
# once you're confident the multiplier semantics fit your conviction
# stack.
EDGE_SIGNALS_ENABLED: bool = _env_bool("GRID_EDGE_SIGNALS_ENABLED", False)

# How much to scale the IC into the multiplier. Defaults: +0.62 IC at
# ~1.50× and -0.53 IC at ~0.58×.
EDGE_BOOST_GAIN: float = _env_float("GRID_EDGE_BOOST_GAIN", 0.8)
EDGE_PENALTY_GAIN: float = _env_float("GRID_EDGE_PENALTY_GAIN", 0.8)

# Hard bounds so no single edge can blow up the aggregate.
EDGE_MULTIPLIER_MIN: float = _env_float("GRID_EDGE_MULT_MIN", 0.4)
EDGE_MULTIPLIER_MAX: float = _env_float("GRID_EDGE_MULT_MAX", 1.8)


def set_enabled(value: bool) -> None:
    """Toggle the master switch at runtime (mostly for tests + REPL)."""
    global EDGE_SIGNALS_ENABLED
    EDGE_SIGNALS_ENABLED = bool(value)

# Only honour rows the backtest itself flagged as ``EDGE``. Other
# verdicts (e.g. NOISE, INCONCLUSIVE) stay out of the multiplier path
# regardless of their nominal IC.
_EDGE_VERDICT_TOKEN: str = "EDGE"


@dataclass(frozen=True)
class EdgeRecord:
    """One entry from the edge_table.csv."""

    source_type: str
    ticker: str
    signal_direction: str
    information_coefficient: float
    p_value: float
    n_events: int
    hit_rate_present: float
    hit_rate_absent: float


# ── In-process cache (loaded once per process) ────────────────────────────


_CACHE: dict[tuple[str, str, str], EdgeRecord] | None = None
_CACHE_LOCK = Lock()


def _normalize_key(source_type: Any, ticker: Any, signal_direction: Any) -> tuple[str, str, str]:
    return (
        str(source_type or "").strip(),
        str(ticker or "").strip().upper(),
        str(signal_direction or "").strip(),
    )


def _resolve_path() -> Path:
    override = os.environ.get(_EDGE_TABLE_PATH_ENV)
    return Path(override) if override else _DEFAULT_EDGE_TABLE_PATH


def _load_table(path: Path) -> dict[tuple[str, str, str], EdgeRecord]:
    """Parse the CSV and return a ``{(source, ticker, dir): EdgeRecord}`` map.

    Returns an empty dict if the file is missing or unreadable — callers
    fall through to multiplier=1.0 cleanly.
    """
    out: dict[tuple[str, str, str], EdgeRecord] = {}
    if not path.exists():
        log.info("edge_signals: edge table not found at {p}; lookups disabled", p=path)
        return out

    try:
        with path.open(newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                verdict = (row.get("verdict") or "").strip().upper()
                if verdict != _EDGE_VERDICT_TOKEN:
                    continue
                try:
                    ic = float(row["information_coefficient"])
                    p = float(row.get("p_value", "1") or "1")
                    n = int(row.get("n_events", "0") or "0")
                    hr_p = float(row.get("hit_rate_present", "0") or "0")
                    hr_a = float(row.get("hit_rate_absent", "0") or "0")
                except (KeyError, TypeError, ValueError):
                    continue
                key = _normalize_key(
                    row.get("source_type"),
                    row.get("ticker"),
                    row.get("signal_direction"),
                )
                if not all(key):
                    continue
                out[key] = EdgeRecord(
                    source_type=key[0],
                    ticker=key[1],
                    signal_direction=key[2],
                    information_coefficient=ic,
                    p_value=p,
                    n_events=n,
                    hit_rate_present=hr_p,
                    hit_rate_absent=hr_a,
                )
    except Exception as exc:  # noqa: BLE001
        log.warning("edge_signals: failed to load {p}: {e}", p=path, e=str(exc))
        return {}

    log.info("edge_signals: loaded {n} EDGE rows from {p}", n=len(out), p=path)
    return out


def _ensure_loaded() -> dict[tuple[str, str, str], EdgeRecord]:
    global _CACHE
    with _CACHE_LOCK:
        if _CACHE is None:
            _CACHE = _load_table(_resolve_path())
        return _CACHE


def reload() -> int:
    """Force a re-read of the CSV. Returns the number of EDGE rows loaded.

    Useful after a fresh backtest writes a new edge_table.csv on disk.
    """
    global _CACHE
    new_cache = _load_table(_resolve_path())
    with _CACHE_LOCK:
        _CACHE = new_cache
    return len(new_cache)


def lookup_edge(
    source_type: str,
    ticker: str,
    signal_direction: str,
) -> EdgeRecord | None:
    """Return the EdgeRecord matching ``(source_type, ticker, signal_direction)``
    or ``None`` if no edge has been identified for that triple.

    Case-insensitive on ticker; exact match on source_type and direction.
    """
    cache = _ensure_loaded()
    return cache.get(_normalize_key(source_type, ticker, signal_direction))


def _ic_to_multiplier(ic: float) -> float:
    if ic >= 0:
        mult = 1.0 + EDGE_BOOST_GAIN * ic
    else:
        mult = 1.0 - EDGE_PENALTY_GAIN * abs(ic)
    return max(EDGE_MULTIPLIER_MIN, min(EDGE_MULTIPLIER_MAX, mult))


# Signal_type → directional implication. Mirrors
# ``intelligence.trust_scorer._infer_signal_direction`` but inlined
# here so edge_signals doesn't depend on trust_scorer's import chain.
# Inferring direction is what lets us flip the IC sign when the
# oracle's prediction direction opposes the signal's natural direction
# — see ``_directional_ic`` below.
_SIGNAL_DIRECTION_BULLISH: frozenset[str] = frozenset({
    "BUY", "CLUSTER_BUY", "insider_buy", "wsb_bullish", "trade_idea_long",
    "gov_contracts", "CONTRACT_AWARD",
})
_SIGNAL_DIRECTION_BEARISH: frozenset[str] = frozenset({
    "SELL", "UNUSUAL_SELL", "insider_sell", "wsb_bearish", "trade_idea_short",
})


def _infer_signal_dir(signal_type: str) -> str:
    """Return 'bullish' / 'bearish' / 'unknown' for a signal_type."""
    if signal_type in _SIGNAL_DIRECTION_BULLISH:
        return "bullish"
    if signal_type in _SIGNAL_DIRECTION_BEARISH:
        return "bearish"
    lower = signal_type.lower() if signal_type else ""
    if any(tok in lower for tok in ("bull", "_buy", "long_", "_long")):
        return "bullish"
    if any(tok in lower for tok in ("bear", "_sell", "short_", "_short")):
        return "bearish"
    return "unknown"


def _normalize_prediction_direction(direction: str) -> str:
    """Map CALL/PUT/bullish/bearish/long/short to {bullish, bearish, unknown}."""
    d = (direction or "").strip().lower()
    if d in ("call", "bullish", "long", "up", "buy"):
        return "bullish"
    if d in ("put", "bearish", "short", "down", "sell"):
        return "bearish"
    return "unknown"


def _directional_ic(
    ic: float,
    signal_direction: str,
    prediction_direction: str,
) -> float:
    """Flip IC sign when the signal's natural direction opposes the
    oracle's prediction direction.

    Semantics:
      * Aligned (signal direction matches prediction direction):
        +IC → boost (signal confirms our call AND predictions hit
        more often when this signal fires).
        -IC → penalty (signal fires when predictions miss; we're going
        the same way, so we share the failure mode).
      * Opposed (signal points the other way from our prediction):
        +IC → strong penalty. The signal's positive IC was earned by
        predictions that went the OPPOSITE way of us; our prediction
        is statistically the minority case here.
        -IC → boost. The signal historically misfires when present;
        if it points opposite to us, we're in the "doing the right
        thing" position relative to the signal's failure mode.
      * Unknown direction on either side: leave IC as-is — fall back
        to direction-agnostic boost/penalty (the pre-2026-05-13 path).
    """
    if signal_direction == "unknown" or prediction_direction == "unknown":
        return ic
    if signal_direction == prediction_direction:
        return ic
    # Opposed → flip the sign on the multiplier
    return -ic


def edge_multiplier(
    source_type: str,
    ticker: str,
    signal_direction: str,
) -> float:
    """Return the conviction multiplier to apply for this signal triple.

    Returns ``1.0`` when:
      * the master switch ``EDGE_SIGNALS_ENABLED`` is False, or
      * no edge has been identified for the (source, ticker, direction)
        triple in the loaded edge_table.

    Otherwise scales by IC with sign-aware gain and clips to
    ``[EDGE_MULTIPLIER_MIN, EDGE_MULTIPLIER_MAX]``.
    """
    if not EDGE_SIGNALS_ENABLED:
        return 1.0
    edge = lookup_edge(source_type, ticker, signal_direction)
    if edge is None:
        return 1.0
    return _ic_to_multiplier(edge.information_coefficient)


def multiplier_for_source_ticker(source_type: str, ticker: str) -> float:
    """Variant that only knows ``(source_type, ticker)``.

    When direction isn't available at the call site (e.g. consumers
    walking ``SignalEvidence`` which carries only ``signal_source``),
    we still want the conviction calibration. Strategy: among all EDGE
    rows for this ``(source_type, ticker)`` pair, pick the one with the
    strongest ``|IC|`` and return its multiplier. Degenerates to 1.0
    when no edges match or the master switch is off.
    """
    if not EDGE_SIGNALS_ENABLED:
        return 1.0
    cache = _ensure_loaded()
    if not cache:
        return 1.0
    src_norm = str(source_type or "").strip()
    tkr_norm = str(ticker or "").strip().upper()
    if not src_norm or not tkr_norm:
        return 1.0
    candidates = [
        rec for (s, t, _d), rec in cache.items()
        if s == src_norm and t == tkr_norm
    ]
    if not candidates:
        return 1.0
    strongest = max(candidates, key=lambda r: abs(r.information_coefficient))
    return _ic_to_multiplier(strongest.information_coefficient)


def compute_aggregate_edge_multiplier(
    signal_evidence: list[Any],
    ticker: str,
) -> float:
    """Roll up the per-signal edge multipliers into one scalar.

    Iterates the provided ``signal_evidence`` (each item is expected to
    have a ``signal_source`` attribute — duck-typed against the canonical
    ``intelligence.signal_provenance.SignalEvidence``), looks each one up
    by ``(signal_source, ticker)`` via :func:`multiplier_for_source_ticker`,
    and returns the **geometric mean** of the resulting per-signal
    multipliers — so 10 signals each with ~1.1× boost don't compound to
    2.6×; instead the aggregate stays near 1.1×.

    Returns 1.0 when the master switch is off or no evidence is
    provided. The geometric mean is also clipped to
    ``[EDGE_MULTIPLIER_MIN, EDGE_MULTIPLIER_MAX]`` so the aggregate is
    bounded the same way as a single edge.
    """
    if not EDGE_SIGNALS_ENABLED or not signal_evidence or not ticker:
        return 1.0
    multipliers: list[float] = []
    for ev in signal_evidence:
        source = getattr(ev, "signal_source", None)
        if not source:
            continue
        m = multiplier_for_source_ticker(source, ticker)
        if m != 1.0:
            multipliers.append(m)
    if not multipliers:
        return 1.0
    # Geometric mean: nth root of the product.
    log_sum = 0.0
    for m in multipliers:
        # log only defined for positive — multipliers always > 0 by
        # construction (min bound > 0).
        from math import log as _log
        log_sum += _log(m)
    from math import exp as _exp
    geomean = _exp(log_sum / len(multipliers))
    return max(EDGE_MULTIPLIER_MIN, min(EDGE_MULTIPLIER_MAX, geomean))


def apply_multiplier(
    base_conviction: float,
    source_type: str,
    ticker: str,
    signal_direction: str,
) -> float:
    """Apply the edge multiplier to ``base_conviction``.

    Thin sugar so callers can write
    ``boost = apply_multiplier(boost, ...)`` without remembering the
    multiplier shape. Pure — no side effects.
    """
    return float(base_conviction) * edge_multiplier(source_type, ticker, signal_direction)


# Per-source-type "information half-life" in days. Encodes how long
# a signal of each type stays directionally informative. Pulled from
# trust_scorer.EVALUATION_WINDOWS conceptually but inlined so this
# module has no upstream import.
# Operator guidance: a social HEAT_SPIKE from 14 days ago shouldn't
# meaningfully influence today's 3-day prediction. A gov_contracts
# announcement from 30 days ago still informs the next 30 days.
_SIGNAL_HALF_LIFE_DAYS: dict[str, int] = {
    "social": 3,
    "smart_money": 3,
    "options_flow": 5,
    "quiverquant:wsb": 3,
    "quiverquant:offexchange": 5,
    "scanner": 7,
    "insider": 14,
    "quiverquant:insider": 14,
    "quiverquant:house": 30,
    "quiverquant:senate": 30,
    "congressional": 30,
    "legislative": 30,
    "quiverquant:lobbying": 60,
    "quiverquant:gov_contracts": 90,
    "lobbying": 60,
    "foreign_lobbying": 60,
}
_DEFAULT_HALF_LIFE_DAYS: int = 14
_MAX_LOOKBACK_DAYS: int = 90   # hard upper bound; cuts off ancient signals entirely


def _signal_recency_weight(source_type: str, signal_age_days: float) -> float:
    """Exponential-decay weight for a signal of age ``signal_age_days``.

    weight = 0.5 ** (age / half_life)

    A signal at exactly its half-life is weighted 0.5. A signal at 3×
    half-life is weighted 0.125. Returns 0.0 for signals older than
    ``_MAX_LOOKBACK_DAYS`` regardless of half-life (avoids long-tail
    drag from ancient rows).
    """
    if signal_age_days < 0:
        return 0.0
    if signal_age_days > _MAX_LOOKBACK_DAYS:
        return 0.0
    half_life = _SIGNAL_HALF_LIFE_DAYS.get(source_type, _DEFAULT_HALF_LIFE_DAYS)
    return 0.5 ** (signal_age_days / half_life)


def edge_multiplier_for_prediction(
    engine: Any,
    ticker: str,
    signal_date: Any,
    *,
    lookback_days: int = _MAX_LOOKBACK_DAYS,
    prediction_direction: str = "",
) -> float:
    """Aggregate EDGE multiplier for a prediction at a given (ticker, date).

    The walk_forward / decision_gateway callsites that work in terms of
    Shapley feature names cannot match the edge_table directly — the
    edge_table is keyed by signal-source ``source_type`` (e.g. ``insider``,
    ``quiverquant:offexchange``, ``smart_money``), which is the schema of
    the raw ``signal_sources`` table, not of the Shapley contribution
    breakdown. This helper bridges that gap:

      1. Pull all ``signal_sources`` rows for ``ticker`` whose
         ``signal_date`` lies in ``[signal_date - lookback_days, signal_date]``.
      2. For each (source_type, signal_type), look up the matching EDGE.
      3. Return the geomean of per-row multipliers, clipped to bounds.

    Returns 1.0 when the master switch is off, no engine is provided, the
    query fails, or no signal_sources rows fall in the window.
    """
    if not EDGE_SIGNALS_ENABLED or engine is None or not ticker or signal_date is None:
        return 1.0

    # Lazy SQLAlchemy import — keeps the module loadable in test harnesses
    # that don't have a DB.
    try:
        from sqlalchemy import text  # noqa: WPS433
    except Exception:  # noqa: BLE001
        return 1.0

    try:
        sd = signal_date.date() if hasattr(signal_date, "date") else signal_date
    except Exception:  # noqa: BLE001
        return 1.0

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT source_type, signal_type, signal_date
                    FROM signal_sources
                    WHERE ticker = :t
                      AND signal_date BETWEEN :start AND :end
                    """
                ),
                {
                    "t": str(ticker).upper().strip(),
                    "start": sd - __import__("datetime").timedelta(days=int(lookback_days)),
                    "end": sd,
                },
            ).fetchall()
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "edge_signals.edge_multiplier_for_prediction: query failed "
            "({t}, {d}): {e}", t=ticker, d=sd, e=str(exc),
        )
        return 1.0

    pred_dir = _normalize_prediction_direction(prediction_direction)

    # Recency-weighted aggregation. Each signal_sources row produces a
    # multiplier ``m`` and a recency weight ``w``; we combine into the
    # geomean as ``Π m_i^(w_i / Σw)``.  Old signals (beyond their
    # source-type's half-life) automatically shrink toward neutral via
    # the exponential decay; ancient signals (>_MAX_LOOKBACK_DAYS) drop
    # out entirely (weight=0).
    log_weighted_sum = 0.0
    total_weight = 0.0
    from math import exp as _exp, log as _log

    for r in rows or []:
        try:
            src_type = str(r[0] or "").strip()
            sig_type = str(r[1] or "").strip()
            row_date = r[2]
        except (TypeError, IndexError):
            continue
        if not src_type or not sig_type or row_date is None:
            continue
        edge = lookup_edge(src_type, ticker, sig_type)
        if edge is None:
            continue
        signal_dir = _infer_signal_dir(sig_type)
        ic = _directional_ic(
            edge.information_coefficient,
            signal_dir,
            pred_dir,
        )
        m = _ic_to_multiplier(ic)
        if m == 1.0:
            continue
        # Recency: how many days back is this signal from the prediction
        try:
            row_d = row_date.date() if hasattr(row_date, "date") else row_date
            age_days = float((sd - row_d).days)
        except Exception:  # noqa: BLE001
            continue
        w = _signal_recency_weight(src_type, age_days)
        if w <= 0:
            continue
        log_weighted_sum += w * _log(m)
        total_weight += w

    if total_weight <= 0:
        return 1.0

    geomean = _exp(log_weighted_sum / total_weight)
    return max(EDGE_MULTIPLIER_MIN, min(EDGE_MULTIPLIER_MAX, geomean))
