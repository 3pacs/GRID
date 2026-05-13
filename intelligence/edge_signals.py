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
