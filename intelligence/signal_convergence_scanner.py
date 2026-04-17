"""
GRID Intelligence — Signal Convergence Scanner (Dot-Connector).

The dot-connector: scan every orthogonal alt-data stream (congressional
trades, insider filings, dark pool prints, unusual options flow, smart
money flow, 13F institutional delta, social heat, prediction market odds)
and measure how many independently agree on a ticker+direction in a tight
window. A single stream means nothing; 3+ orthogonal streams lined up in
the same direction is a fingerprint of something big moving.

Per user SOP (feedback_connect_dots.md):
    "Cross-reference everything: insider+congress+whale+events chains,
    not siloed signals."

Sibling modules:
  * intelligence/lever_pullers.py — find_lever_convergence() detects when
    registered "lever pullers" co-fire; this module is orthogonal — it
    scans live stream data regardless of whether a source is registered
    in the lever_pullers table.
  * intelligence/trust_scorer.py — per-source Bayesian trust history;
    this module uses the average trust_score per source_type as the
    weight for each stream's contribution.

Key entry points:
    scan_convergence                — full report for one ticker
    convergence_conviction_multiplier — live-path multiplier only
    rank_universe_by_convergence    — dashboard "hot convergences" view

All per-stream extractors wrap SQL in try/except so missing tables or
unknown schemas degrade gracefully (None) rather than crashing the scan.
Every query is filtered by `created_at <= :as_of` and `signal_date <=
:as_of` to enforce point-in-time discipline.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError


# ── Module-level constants ────────────────────────────────────────────────

#: Default scan window (days back from as_of) for stream co-firing.
DEFAULT_WINDOW_DAYS: int = 7

#: Maximum number of streams this scanner tracks. Used to size neutral
#: fallback reports and the missing_stream_count advisory.
N_STREAMS: int = 8

#: Direction labels this module understands.
BULLISH: str = "bullish"
BEARISH: str = "bearish"
NEUTRAL: str = "neutral"
VALID_DIRECTIONS: frozenset[str] = frozenset({BULLISH, BEARISH, NEUTRAL})

#: Default trust weight when trust_scorer lookup fails or source is
#: unknown. Matches the `signal_sources.trust_score DEFAULT 0.5` schema
#: default in schema.sql.
DEFAULT_TRUST_WEIGHT: float = 0.5

#: Floor for convergence-multiplier math when the denominator of total
#: trust weight would otherwise be zero. Prevents division artefacts.
_MIN_DENOMINATOR: float = 1.0

#: Neutral streams (e.g. dark pool without a directional tilt) are
#: counted at HALF weight toward the convergence score — they show
#: something is moving but not which way.
_NEUTRAL_DAMPENER: float = 0.5

# ── Conviction multiplier thresholds ──────────────────────────────────────
#
# Convergence is rare and orthogonal streams are expensive, so the
# multiplier schedule is deliberately top-heavy: you have to earn 1.25.
# ALL thresholds are hardcoded here as module constants so they can be
# audited and tuned in one place.

#: Five aligned streams with high conviction → maximum multiplier.
THRESHOLD_STRONG_N: int = 5
THRESHOLD_STRONG_SCORE: float = 0.70
MULTIPLIER_STRONG: float = 1.25

#: Four aligned streams, solid conviction.
THRESHOLD_SOLID_N: int = 4
THRESHOLD_SOLID_SCORE: float = 0.60
MULTIPLIER_SOLID: float = 1.18

#: Three aligned streams, moderate conviction.
THRESHOLD_MODERATE_N: int = 3
THRESHOLD_MODERATE_SCORE: float = 0.50
MULTIPLIER_MODERATE: float = 1.10

#: Two aligned streams, weak confirmation.
THRESHOLD_WEAK_N: int = 2
THRESHOLD_WEAK_SCORE: float = 0.40
MULTIPLIER_WEAK: float = 1.05

#: Single-stream fallback — no confirmation, neutral multiplier.
MULTIPLIER_NEUTRAL: float = 1.00

#: Two or more OPPOSED streams with weighted opposition greater than
#: weighted alignment → the streams disagree with the caller's direction,
#: so we punish conviction.
THRESHOLD_OPPOSED_N: int = 2
MULTIPLIER_OPPOSED: float = 0.92

#: Clamp for the final multiplier regardless of intermediate math.
MULTIPLIER_FLOOR: float = 0.92
MULTIPLIER_CEILING: float = 1.25

# ── Direction alias map (for downstream payload normalization) ────────────

_DIRECTION_ALIASES: dict[str, str] = {
    # bullish synonyms
    "bullish": BULLISH,
    "bull": BULLISH,
    "buy": BULLISH,
    "long": BULLISH,
    "call": BULLISH,
    "calls": BULLISH,
    "up": BULLISH,
    "positive": BULLISH,
    "increased": BULLISH,
    "new": BULLISH,
    # bearish synonyms
    "bearish": BEARISH,
    "bear": BEARISH,
    "sell": BEARISH,
    "short": BEARISH,
    "put": BEARISH,
    "puts": BEARISH,
    "down": BEARISH,
    "negative": BEARISH,
    "decreased": BEARISH,
    "closed": BEARISH,
    # neutral
    "neutral": NEUTRAL,
    "flat": NEUTRAL,
    "unknown": NEUTRAL,
}


def _normalize_direction(raw: Any) -> str:
    """Map an arbitrary direction label into {bullish, bearish, neutral}.

    Never raises. Unknown values fall back to NEUTRAL so a corrupt
    payload can't break the convergence scan.
    """
    if raw is None:
        return NEUTRAL
    key = str(raw).strip().lower()
    return _DIRECTION_ALIASES.get(key, NEUTRAL)


def _clamp(value: float, lo: float, hi: float) -> float:
    """Numeric clamp helper. Tolerates NaN/None by returning lo."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return lo
    if math.isnan(v) or math.isinf(v):
        return lo
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


# ── Stream source_type labels (as written by ingestion/altdata pullers) ──
#
# These are the literal `source_type` values emitted into the unified
# `signal_sources` table by the 8 puller groups we scan. Verified via
# grep of ingestion/altdata/*.py — never invent schema.

STREAM_CONGRESSIONAL: str = "congressional"
STREAM_INSIDER: str = "insider"
STREAM_DARKPOOL: str = "darkpool"
STREAM_OPTIONS_FLOW: str = "options_flow"
STREAM_SMART_MONEY: str = "smart_money"
STREAM_INSTITUTIONAL: str = "13f"
STREAM_SOCIAL: str = "social"
STREAM_PREDICTION_MARKET: str = "prediction_market"

#: Ordered tuple of the 8 canonical streams this scanner targets.
ALL_STREAM_NAMES: tuple[str, ...] = (
    STREAM_CONGRESSIONAL,
    STREAM_INSIDER,
    STREAM_DARKPOOL,
    STREAM_OPTIONS_FLOW,
    STREAM_SMART_MONEY,
    STREAM_INSTITUTIONAL,
    STREAM_SOCIAL,
    STREAM_PREDICTION_MARKET,
)


# ── Public data classes ───────────────────────────────────────────────────

@dataclass(frozen=True)
class StreamSignal:
    """One alt-data stream's contribution to a convergence scan.

    Attributes:
        stream_name: Canonical stream label (e.g. ``"congressional"``).
        intensity: Normalized magnitude in [0, 1].
        direction: One of ``"bullish"`` / ``"bearish"`` / ``"neutral"``.
        trust_weight: Per-source trust score in [0, 1], from trust_scorer.
        evidence_line: One-line human-readable evidence string with a
            leading ISO date and stream tag, suitable for LLM /
            dashboard narration.
        raw_payload: Debug dict of the raw aggregates used to compute
            intensity and direction. Never part of the scoring math.
    """

    stream_name: str
    intensity: float
    direction: str
    trust_weight: float
    evidence_line: str
    raw_payload: dict[str, Any] = field(default_factory=dict)

    @property
    def source_type(self) -> str:
        """Compatibility alias for callers/tests keyed to signal_sources."""
        if self.stream_name == STREAM_INSTITUTIONAL:
            return "institutional"
        return self.stream_name

    def to_dict(self) -> dict[str, Any]:
        """Round-trip-safe serialization for JSON APIs."""
        return {
            "stream_name": self.stream_name,
            "source_type": self.source_type,
            "intensity": float(self.intensity),
            "direction": self.direction,
            "trust_weight": float(self.trust_weight),
            "evidence_line": self.evidence_line,
            "raw_payload": dict(self.raw_payload),
        }


@dataclass(frozen=True)
class ConvergenceReport:
    """Aggregated convergence result for one (ticker, target_direction).

    Every field is populated even on the empty-streams / all-missing
    path, so callers can safely rely on the dataclass shape.
    """

    ticker: str
    as_of: str
    window_days: int
    target_direction: str
    stream_signals: tuple[StreamSignal, ...]
    n_active_streams: int
    n_aligned: int
    n_opposed: int
    convergence_score: float
    conviction_multiplier: float
    evidence_chain: tuple[str, ...]
    advisory: str
    missing_stream_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "as_of": self.as_of,
            "window_days": int(self.window_days),
            "target_direction": self.target_direction,
            "stream_signals": [s.to_dict() for s in self.stream_signals],
            "n_active_streams": int(self.n_active_streams),
            "n_aligned": int(self.n_aligned),
            "n_opposed": int(self.n_opposed),
            "convergence_score": float(self.convergence_score),
            "conviction_multiplier": float(self.conviction_multiplier),
            "evidence_chain": list(self.evidence_chain),
            "advisory": self.advisory,
            "missing_stream_count": int(self.missing_stream_count),
        }


# ── Trust weighting ───────────────────────────────────────────────────────

def _lookup_trust_weight(
    engine: Engine,
    stream_name: str,
    cache: dict[str, float],
) -> float:
    """Return the trust weight for a given stream (source_type).

    Strategy:
        1. If ``intelligence.trust_scorer.get_trust_score`` exists, use
           it directly — this gives us whatever central cache or
           weighted aggregation the trust layer computes.
        2. Otherwise, average the stored ``trust_score`` values in
           ``signal_sources`` for that source_type.
        3. Fall back to ``DEFAULT_TRUST_WEIGHT`` (0.5) on any failure
           or if the source is unknown.

    Results are memoized in ``cache`` so one scan never round-trips the
    DB twice for the same stream.
    """
    if stream_name in cache:
        return cache[stream_name]

    # Path 1: delegate to trust_scorer if it exports the expected API.
    try:
        from intelligence import trust_scorer  # type: ignore
        get_fn = getattr(trust_scorer, "get_trust_score", None)
        if callable(get_fn):
            value = float(get_fn(engine, stream_name))
            clamped = _clamp(value, 0.0, 1.0)
            cache[stream_name] = clamped
            return clamped
    except Exception as exc:  # pragma: no cover — defensive
        log.debug(
            "trust_scorer.get_trust_score unavailable for {s}: {e}",
            s=stream_name, e=str(exc),
        )

    # Path 2: compute from signal_sources history.
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT AVG(trust_score) "
                    "FROM signal_sources "
                    "WHERE source_type = :stype "
                    "  AND trust_score IS NOT NULL"
                ),
                {"stype": stream_name},
            ).fetchone()
        if row and row[0] is not None:
            clamped = _clamp(float(row[0]), 0.0, 1.0)
            cache[stream_name] = clamped
            return clamped
    except (ProgrammingError, OperationalError, Exception) as exc:
        log.debug(
            "Trust lookup failed for stream={s}: {e}",
            s=stream_name, e=str(exc),
        )

    cache[stream_name] = DEFAULT_TRUST_WEIGHT
    return DEFAULT_TRUST_WEIGHT


# ── Payload extraction helpers ────────────────────────────────────────────

def _parse_signal_value(raw: Any) -> dict[str, Any]:
    """Decode a signal_sources.signal_value payload to a dict.

    The column is JSONB; SQLAlchemy will return either a ``dict`` or a
    JSON string depending on driver. Unknown shapes return ``{}``.
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except (TypeError, ValueError):
            return {}
    return {}


def _signal_type_direction(signal_type: str | None) -> str:
    """Map a signal_type column value to a direction.

    BUY / CLUSTER_BUY / UNUSUAL_BUY → bullish. SELL / UNUSUAL_SELL →
    bearish. Anything else (UNUSUAL_VOLUME, FARA_ACTIVITY, RAPID_SHIFT,
    13F_POSITION_CHANGES) stays neutral — those carry direction in the
    JSON payload, not the signal_type column.
    """
    if not signal_type:
        return NEUTRAL
    stype = signal_type.upper()
    if "BUY" in stype:
        return BULLISH
    if "SELL" in stype:
        return BEARISH
    return NEUTRAL


# ── Per-stream extractors ─────────────────────────────────────────────────
#
# Each extractor wraps its SQL in try/except (ProgrammingError,
# OperationalError, Exception) → returns ``None`` on any failure so
# missing tables don't break the scan.  Every query is parameterized;
# NEVER f-string SQL.

def _window_bounds(
    as_of: date, window_days: int
) -> tuple[date, datetime]:
    """Return (window_start, as_of_ts) for PIT filtering.

    ``as_of_ts`` is the end-of-day timestamp on ``as_of`` so rows
    written partway through the decision day are still included.
    """
    start = as_of - timedelta(days=window_days)
    as_of_ts = datetime.combine(
        as_of, datetime.max.time(), tzinfo=timezone.utc
    )
    return start, as_of_ts


def _fetch_stream_rows(
    engine: Engine,
    stream_name: str,
    ticker: str,
    as_of: date,
    window_days: int,
) -> list[Any] | None:
    """Fetch recent signal_sources rows for one stream + ticker.

    Returns the row list or ``None`` on SQL failure (missing table,
    schema drift, connection refused…). An empty list is a distinct
    valid result — it means "stream exists, no data in window".
    """
    start, as_of_ts = _window_bounds(as_of, window_days)
    def _coerce_row(row: Any) -> tuple[Any, Any, Any, Any, Any, Any]:
        if isinstance(row, dict):
            return (
                row.get("source_id"),
                row.get("signal_date"),
                row.get("signal_type"),
                row.get("signal_value"),
                row.get("trust_score"),
                row.get("created_at"),
            )
        values = tuple(row)
        if len(values) >= 6:
            return values[:6]
        if len(values) == 5:
            # Some tests and older adapters hand back compact rows as
            # (source_type, signal_type, ticker, signal_date, signal_value).
            if values[0] in {
                STREAM_CONGRESSIONAL,
                STREAM_INSIDER,
                STREAM_DARKPOOL,
                STREAM_OPTIONS_FLOW,
                STREAM_SMART_MONEY,
                STREAM_INSTITUTIONAL,
                STREAM_SOCIAL,
                STREAM_PREDICTION_MARKET,
            }:
                return (None, values[3], values[1], values[4], None, None)
            # Otherwise treat it as the selected DB row without created_at.
            return (values[0], values[1], values[2], values[3], values[4], None)
        padded = values + (None,) * (6 - len(values))
        return padded[:6]

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT source_id, signal_date, signal_type, "
                    "       signal_value, trust_score, created_at "
                    "FROM signal_sources "
                    "WHERE source_type = :stype "
                    "  AND ticker = :ticker "
                    "  AND signal_date >= :t0 "
                    "  AND signal_date <= :as_of "
                    "  AND created_at <= :as_of_ts "
                    "ORDER BY signal_date ASC"
                ),
                {
                    "stype": stream_name,
                    "ticker": ticker,
                    "t0": start,
                    "as_of": as_of,
                    "as_of_ts": as_of_ts,
                },
            )
            try:
                mapped_rows = result.mappings().all()
                if mapped_rows:
                    return [_coerce_row(dict(r)) for r in mapped_rows]
            except Exception:
                pass
            rows = result.fetchall()
        return [_coerce_row(r) for r in rows]
    except (ProgrammingError, OperationalError) as exc:
        log.debug(
            "Stream {s} table/schema missing for ticker={t}: {e}",
            s=stream_name, t=ticker, e=str(exc),
        )
        return None
    except Exception as exc:
        log.warning(
            "Stream {s} query failed for ticker={t}: {e}",
            s=stream_name, t=ticker, e=str(exc),
        )
        return None


def _scan_congressional(
    engine: Engine,
    ticker: str,
    as_of: date,
    window_days: int,
    trust_cache: dict[str, float],
) -> StreamSignal | None:
    """Congressional trades: aggregate BUY vs SELL dollar volume.

    Intensity = ``log10(1 + total_usd / 100_000)`` clamped [0, 1].
    Direction = majority transaction_type in window.
    """
    rows = _fetch_stream_rows(
        engine, STREAM_CONGRESSIONAL, ticker, as_of, window_days
    )
    if rows is None or len(rows) == 0:
        return None

    buy_usd = 0.0
    sell_usd = 0.0
    latest_date = as_of
    for r in rows:
        _sid, sdate, stype, sval, _trust, _created = r
        payload = _parse_signal_value(sval)
        amount = float(payload.get("amount_midpoint") or 0.0)
        if _signal_type_direction(stype) == BULLISH:
            buy_usd += amount
        elif _signal_type_direction(stype) == BEARISH:
            sell_usd += amount
        if isinstance(sdate, date) and sdate > latest_date - timedelta(days=window_days):
            latest_date = sdate

    total = buy_usd + sell_usd
    if total <= 0:
        return None

    direction = BULLISH if buy_usd >= sell_usd else BEARISH
    intensity = _clamp(math.log10(1.0 + total / 100_000.0) / 3.0, 0.0, 1.0)

    top_member = None
    for r in rows:
        sid = r[0]
        if sid:
            top_member = sid
            break

    evidence = (
        f"{latest_date.isoformat()}  CONGRESS   "
        f"{top_member or 'member'} {direction.upper()} ${total:,.0f}"
    )
    trust = _lookup_trust_weight(engine, STREAM_CONGRESSIONAL, trust_cache)
    return StreamSignal(
        stream_name=STREAM_CONGRESSIONAL,
        intensity=intensity,
        direction=direction,
        trust_weight=trust,
        evidence_line=evidence,
        raw_payload={
            "buy_usd": buy_usd,
            "sell_usd": sell_usd,
            "n_trades": len(rows),
        },
    )


def _scan_insider(
    engine: Engine,
    ticker: str,
    as_of: date,
    window_days: int,
    trust_cache: dict[str, float],
) -> StreamSignal | None:
    """Insider Form 4: cluster buys only (3+ insiders in window).

    Intensity from total dollar value; direction from net buy/sell.
    A single Form 4 is too noisy to qualify as a stream signal.
    """
    rows = _fetch_stream_rows(
        engine, STREAM_INSIDER, ticker, as_of, window_days
    )
    if rows is None or len(rows) == 0:
        return None

    cluster_value = 0.0
    buy_count = 0
    sell_count = 0
    insider_ids: set[str] = set()
    latest_date = as_of - timedelta(days=window_days)
    cluster_row_count = 0

    for r in rows:
        sid, sdate, stype, sval, _trust, _created = r
        payload = _parse_signal_value(sval)
        amount = float(
            payload.get("total_value")
            or payload.get("value_usd")
            or payload.get("dollar_value")
            or 0.0
        )
        insider_ids.add(str(sid or ""))
        if stype and "CLUSTER" in stype.upper():
            cluster_value += amount
            cluster_row_count += 1
        sig_dir = _signal_type_direction(stype)
        if sig_dir == BULLISH:
            buy_count += 1
        elif sig_dir == BEARISH:
            sell_count += 1
        if isinstance(sdate, date) and sdate > latest_date:
            latest_date = sdate

    # Cluster discipline: require at least 3 distinct insider ids or a
    # CLUSTER_BUY row. Drop the stream otherwise — single Form 4s are
    # noise.
    distinct_insiders = sum(1 for x in insider_ids if x)
    if distinct_insiders < 3 and cluster_row_count == 0:
        return None

    if buy_count > sell_count:
        direction = BULLISH
    elif sell_count > buy_count:
        direction = BEARISH
    else:
        direction = BULLISH if cluster_value > 0 else NEUTRAL

    total_value = max(cluster_value, 0.0)
    intensity = _clamp(
        math.log10(1.0 + total_value / 100_000.0) / 4.0, 0.0, 1.0
    ) if total_value > 0 else _clamp(distinct_insiders / 5.0, 0.0, 1.0)

    evidence = (
        f"{latest_date.isoformat()}  INSIDER    "
        f"{distinct_insiders} execs cluster-buy ${total_value:,.0f}"
    )
    trust = _lookup_trust_weight(engine, STREAM_INSIDER, trust_cache)
    return StreamSignal(
        stream_name=STREAM_INSIDER,
        intensity=intensity,
        direction=direction,
        trust_weight=trust,
        evidence_line=evidence,
        raw_payload={
            "cluster_value": cluster_value,
            "distinct_insiders": distinct_insiders,
            "buys": buy_count,
            "sells": sell_count,
        },
    )


def _scan_darkpool(
    engine: Engine,
    ticker: str,
    as_of: date,
    window_days: int,
    trust_cache: dict[str, float],
) -> StreamSignal | None:
    """Dark pool prints: intensity from volume z-score.

    Direction is neutral unless ``short_volume_ratio`` is extreme
    (>0.6 bearish, <0.3 bullish). Dark pool volume alone does not
    signal direction.
    """
    rows = _fetch_stream_rows(
        engine, STREAM_DARKPOOL, ticker, as_of, window_days
    )
    if rows is None or len(rows) == 0:
        return None

    ratios: list[float] = []
    max_spike = 0.0
    latest_date = as_of - timedelta(days=window_days)
    for r in rows:
        _sid, sdate, _stype, sval, _trust, _created = r
        payload = _parse_signal_value(sval)
        spike = float(
            payload.get("spike_ratio")
            or payload.get("volume_z")
            or 0.0
        )
        max_spike = max(max_spike, spike)
        sv_ratio = payload.get("short_volume_ratio")
        if sv_ratio is not None:
            try:
                ratios.append(float(sv_ratio))
            except (TypeError, ValueError):
                pass
        if isinstance(sdate, date) and sdate > latest_date:
            latest_date = sdate

    intensity = _clamp(max_spike / 4.0, 0.0, 1.0)
    if intensity <= 0:
        intensity = _clamp(len(rows) / 5.0, 0.0, 1.0)

    direction = NEUTRAL
    avg_ratio = sum(ratios) / len(ratios) if ratios else None
    if avg_ratio is not None:
        if avg_ratio > 0.6:
            direction = BEARISH
        elif avg_ratio < 0.3:
            direction = BULLISH

    evidence = (
        f"{latest_date.isoformat()}  DARK_POOL  "
        f"+{max_spike:.1f}σ volume"
        + (f", short% {avg_ratio:.2f}" if avg_ratio is not None else "")
    )
    trust = _lookup_trust_weight(engine, STREAM_DARKPOOL, trust_cache)
    return StreamSignal(
        stream_name=STREAM_DARKPOOL,
        intensity=intensity,
        direction=direction,
        trust_weight=trust,
        evidence_line=evidence,
        raw_payload={
            "max_spike": max_spike,
            "avg_short_ratio": avg_ratio,
            "n_rows": len(rows),
        },
    )


def _scan_options_flow(
    engine: Engine,
    ticker: str,
    as_of: date,
    window_days: int,
    trust_cache: dict[str, float],
) -> StreamSignal | None:
    """Unusual whales options flow: aggregate net premium + call/put tilt.

    Intensity from ``log10(sum(premium) / 10_000)`` clamped. Direction
    is bullish when >70% of the net premium is calls, bearish when >70%
    puts, else neutral. The ingestion payload stores ``notional`` and a
    ``direction`` string.
    """
    rows = _fetch_stream_rows(
        engine, STREAM_OPTIONS_FLOW, ticker, as_of, window_days
    )
    if rows is None or len(rows) == 0:
        return None

    call_prem = 0.0
    put_prem = 0.0
    latest_date = as_of - timedelta(days=window_days)

    for r in rows:
        _sid, sdate, _stype, sval, _trust, _created = r
        payload = _parse_signal_value(sval)
        notional = float(payload.get("notional") or payload.get("premium_usd") or 0.0)
        side = _normalize_direction(payload.get("direction"))
        if side == BULLISH:
            call_prem += notional
        elif side == BEARISH:
            put_prem += notional
        else:
            # Unknown → split evenly so we still count magnitude.
            call_prem += notional / 2.0
            put_prem += notional / 2.0
        if isinstance(sdate, date) and sdate > latest_date:
            latest_date = sdate

    total_prem = call_prem + put_prem
    if total_prem <= 0:
        return None

    call_share = call_prem / total_prem
    if call_share > 0.7:
        direction = BULLISH
    elif call_share < 0.3:
        direction = BEARISH
    else:
        direction = NEUTRAL

    intensity = _clamp(math.log10(total_prem / 10_000.0 + 1.0) / 3.0, 0.0, 1.0)

    evidence = (
        f"{latest_date.isoformat()}  WHALES     "
        f"${total_prem:,.0f} net, calls {call_share:.0%}"
    )
    trust = _lookup_trust_weight(engine, STREAM_OPTIONS_FLOW, trust_cache)
    return StreamSignal(
        stream_name=STREAM_OPTIONS_FLOW,
        intensity=intensity,
        direction=direction,
        trust_weight=trust,
        evidence_line=evidence,
        raw_payload={
            "call_premium": call_prem,
            "put_premium": put_prem,
            "call_share": call_share,
            "n_signals": len(rows),
        },
    )


def _scan_smart_money(
    engine: Engine,
    ticker: str,
    as_of: date,
    window_days: int,
    trust_cache: dict[str, float],
) -> StreamSignal | None:
    """Smart-money-flow aggregate: signed net position delta.

    Intensity from abs(delta) normalized; direction from delta sign.
    Source_type ``smart_money`` is not written by any puller in this
    branch, so most of the time this returns None gracefully.
    """
    rows = _fetch_stream_rows(
        engine, STREAM_SMART_MONEY, ticker, as_of, window_days
    )
    if rows is None or len(rows) == 0:
        return None

    net_delta = 0.0
    latest_date = as_of - timedelta(days=window_days)
    for r in rows:
        _sid, sdate, _stype, sval, _trust, _created = r
        payload = _parse_signal_value(sval)
        delta = float(
            payload.get("net_position_delta")
            or payload.get("net_delta")
            or payload.get("position_delta")
            or payload.get("delta")
            or 0.0
        )
        net_delta += delta
        if isinstance(sdate, date) and sdate > latest_date:
            latest_date = sdate

    if net_delta == 0.0:
        return None

    direction = BULLISH if net_delta > 0 else BEARISH
    intensity = _clamp(
        math.log10(1.0 + abs(net_delta) / 1_000_000.0) / 3.0, 0.0, 1.0
    )

    evidence = (
        f"{latest_date.isoformat()}  SMART_MONEY "
        f"net delta ${net_delta:+,.0f}"
    )
    trust = _lookup_trust_weight(engine, STREAM_SMART_MONEY, trust_cache)
    return StreamSignal(
        stream_name=STREAM_SMART_MONEY,
        intensity=intensity,
        direction=direction,
        trust_weight=trust,
        evidence_line=evidence,
        raw_payload={
            "net_delta": net_delta,
            "n_rows": len(rows),
        },
    )


def _scan_institutional(
    engine: Engine,
    ticker: str,
    as_of: date,
    window_days: int,
    trust_cache: dict[str, float],
) -> StreamSignal | None:
    """13F institutional delta: net NEW + INCREASED vs CLOSED + DECREASED.

    The institutional_flows puller stores position change counts in
    the JSONB payload. We compute ``(new + increased) - (closed +
    decreased)`` to get signed net action count.
    """
    rows = _fetch_stream_rows(
        engine, STREAM_INSTITUTIONAL, ticker, as_of, window_days
    )
    if rows is None or len(rows) == 0:
        return None

    net_positions = 0
    latest_date = as_of - timedelta(days=window_days)
    for r in rows:
        _sid, sdate, _stype, sval, _trust, _created = r
        payload = _parse_signal_value(sval)
        new = int(payload.get("new_positions") or 0)
        inc = int(payload.get("increased") or 0)
        closed = int(payload.get("closed_positions") or 0)
        dec = int(payload.get("decreased") or 0)
        action = str(payload.get("action") or "").upper()
        if action in {"NEW", "ADDED", "INCREASED", "INCREASE"}:
            inc += 1
        elif action in {"CLOSED", "DECREASED", "DECREASE", "REDUCED"}:
            dec += 1
        net_positions += (new + inc) - (closed + dec)
        if isinstance(sdate, date) and sdate > latest_date:
            latest_date = sdate

    if net_positions == 0:
        return None

    direction = BULLISH if net_positions > 0 else BEARISH
    intensity = _clamp(abs(net_positions) / 10.0, 0.0, 1.0)

    evidence = (
        f"{latest_date.isoformat()}  13F        "
        f"net positions {net_positions:+d}"
    )
    trust = _lookup_trust_weight(engine, STREAM_INSTITUTIONAL, trust_cache)
    return StreamSignal(
        stream_name=STREAM_INSTITUTIONAL,
        intensity=intensity,
        direction=direction,
        trust_weight=trust,
        evidence_line=evidence,
        raw_payload={
            "net_positions": net_positions,
            "n_filings": len(rows),
        },
    )


def _scan_social(
    engine: Engine,
    ticker: str,
    as_of: date,
    window_days: int,
    trust_cache: dict[str, float],
) -> StreamSignal | None:
    """Social heat: mention count + optional sentiment.

    Direction neutral unless payload sentiment > 0.3 (bullish) or
    < -0.3 (bearish).
    """
    rows = _fetch_stream_rows(
        engine, STREAM_SOCIAL, ticker, as_of, window_days
    )
    if rows is None or len(rows) == 0:
        return None

    mentions = 0
    sentiment_sum = 0.0
    sentiment_count = 0
    latest_date = as_of - timedelta(days=window_days)
    for r in rows:
        _sid, sdate, _stype, sval, _trust, _created = r
        payload = _parse_signal_value(sval)
        mentions += int(payload.get("mentions") or payload.get("count") or 1)
        sentiment = payload.get("sentiment")
        if sentiment is not None:
            try:
                sentiment_sum += float(sentiment)
                sentiment_count += 1
            except (TypeError, ValueError):
                pass
        if isinstance(sdate, date) and sdate > latest_date:
            latest_date = sdate

    intensity = _clamp(math.log10(1.0 + mentions) / 3.0, 0.0, 1.0)
    avg_sent = sentiment_sum / sentiment_count if sentiment_count > 0 else None

    direction = NEUTRAL
    if avg_sent is not None:
        if avg_sent > 0.3:
            direction = BULLISH
        elif avg_sent < -0.3:
            direction = BEARISH

    evidence = (
        f"{latest_date.isoformat()}  SOCIAL     "
        f"{mentions} mentions"
        + (f", sent {avg_sent:+.2f}" if avg_sent is not None else "")
    )
    trust = _lookup_trust_weight(engine, STREAM_SOCIAL, trust_cache)
    return StreamSignal(
        stream_name=STREAM_SOCIAL,
        intensity=intensity,
        direction=direction,
        trust_weight=trust,
        evidence_line=evidence,
        raw_payload={
            "mentions": mentions,
            "avg_sentiment": avg_sent,
        },
    )


def _scan_prediction_market(
    engine: Engine,
    ticker: str,
    as_of: date,
    window_days: int,
    trust_cache: dict[str, float],
) -> StreamSignal | None:
    """Prediction market: aggregate RAPID_SHIFT magnitude + direction.

    Each signal_sources row from the polymarket puller carries
    ``direction`` (``up`` / ``down``) and ``shift`` (probability delta).
    """
    rows = _fetch_stream_rows(
        engine, STREAM_PREDICTION_MARKET, ticker, as_of, window_days
    )
    if rows is None or len(rows) == 0:
        return None

    net_shift = 0.0
    max_abs = 0.0
    latest_date = as_of - timedelta(days=window_days)
    for r in rows:
        _sid, sdate, _stype, sval, _trust, _created = r
        payload = _parse_signal_value(sval)
        shift = float(payload.get("shift") or 0.0)
        direction_hint = _normalize_direction(payload.get("direction"))
        signed = shift if direction_hint != BEARISH else -shift
        net_shift += signed
        max_abs = max(max_abs, abs(shift))
        if isinstance(sdate, date) and sdate > latest_date:
            latest_date = sdate

    if net_shift == 0.0 and max_abs == 0.0:
        return None

    direction = BULLISH if net_shift > 0 else BEARISH if net_shift < 0 else NEUTRAL
    intensity = _clamp(max_abs * 2.0, 0.0, 1.0)

    evidence = (
        f"{latest_date.isoformat()}  PREDICTION "
        f"7d shift {net_shift:+.1%} (peak {max_abs:.1%})"
    )
    trust = _lookup_trust_weight(engine, STREAM_PREDICTION_MARKET, trust_cache)
    return StreamSignal(
        stream_name=STREAM_PREDICTION_MARKET,
        intensity=intensity,
        direction=direction,
        trust_weight=trust,
        evidence_line=evidence,
        raw_payload={
            "net_shift": net_shift,
            "max_shift": max_abs,
            "n_rows": len(rows),
        },
    )


#: Registry of extractors — declarative so new streams can be added in
#: one place and the fan-out loop in ``scan_convergence`` picks them up
#: automatically.
_STREAM_EXTRACTORS: tuple[
    tuple[str, Any], ...
] = (
    (STREAM_CONGRESSIONAL, _scan_congressional),
    (STREAM_INSIDER, _scan_insider),
    (STREAM_DARKPOOL, _scan_darkpool),
    (STREAM_OPTIONS_FLOW, _scan_options_flow),
    (STREAM_SMART_MONEY, _scan_smart_money),
    (STREAM_INSTITUTIONAL, _scan_institutional),
    (STREAM_SOCIAL, _scan_social),
    (STREAM_PREDICTION_MARKET, _scan_prediction_market),
)


# ── Scoring ───────────────────────────────────────────────────────────────

def _compute_convergence(
    signals: list[StreamSignal],
    target_direction: str,
) -> tuple[float, int, int, float, float, float]:
    """Trust-weighted convergence math. Returns a tuple of

        (convergence_score, n_aligned, n_opposed,
         weighted_alignment, weighted_opposition, weighted_neutral)

    Keeping the intermediates visible in the return tuple is
    deliberate: tests inspect the weighted-opposition branch of the
    multiplier ladder, and ``scan_convergence`` uses them to pick the
    right bracket.
    """
    weighted_alignment = 0.0
    weighted_opposition = 0.0
    weighted_neutral = 0.0
    total_trust = 0.0
    n_aligned = 0
    n_opposed = 0

    for s in signals:
        total_trust += s.trust_weight
        if s.direction == target_direction:
            weighted_alignment += s.intensity * s.trust_weight
            n_aligned += 1
        elif s.direction == NEUTRAL:
            weighted_neutral += _NEUTRAL_DAMPENER * s.intensity * s.trust_weight
        else:
            weighted_opposition += s.intensity * s.trust_weight
            n_opposed += 1

    denom = max(_MIN_DENOMINATOR, total_trust)
    raw = (weighted_alignment + weighted_neutral - weighted_opposition) / denom
    score = _clamp(raw, 0.0, 1.0)
    return (
        score,
        n_aligned,
        n_opposed,
        weighted_alignment,
        weighted_opposition,
        weighted_neutral,
    )


def _pick_multiplier(
    n_aligned: int,
    n_opposed: int,
    convergence_score: float,
    weighted_alignment: float,
    weighted_opposition: float,
) -> float:
    """Walk the conviction multiplier ladder in strict order.

    Returns the final multiplier clamped to [MULTIPLIER_FLOOR,
    MULTIPLIER_CEILING]. The opposed branch takes precedence over
    alignment when weighted_opposition dominates — that's the
    "streams disagree with the call" case the user asked for.
    """
    if (
        n_opposed >= THRESHOLD_OPPOSED_N
        and weighted_opposition > weighted_alignment
    ):
        return _clamp(MULTIPLIER_OPPOSED, MULTIPLIER_FLOOR, MULTIPLIER_CEILING)

    if n_aligned >= THRESHOLD_STRONG_N and convergence_score >= THRESHOLD_STRONG_SCORE:
        return _clamp(MULTIPLIER_STRONG, MULTIPLIER_FLOOR, MULTIPLIER_CEILING)
    if n_aligned >= THRESHOLD_SOLID_N and convergence_score >= THRESHOLD_SOLID_SCORE:
        return _clamp(MULTIPLIER_SOLID, MULTIPLIER_FLOOR, MULTIPLIER_CEILING)
    if n_aligned >= THRESHOLD_MODERATE_N and convergence_score >= THRESHOLD_MODERATE_SCORE:
        return _clamp(MULTIPLIER_MODERATE, MULTIPLIER_FLOOR, MULTIPLIER_CEILING)
    if n_aligned >= THRESHOLD_WEAK_N and convergence_score >= THRESHOLD_WEAK_SCORE:
        return _clamp(MULTIPLIER_WEAK, MULTIPLIER_FLOOR, MULTIPLIER_CEILING)

    return _clamp(MULTIPLIER_NEUTRAL, MULTIPLIER_FLOOR, MULTIPLIER_CEILING)


def _empty_report(
    ticker: str,
    as_of: date,
    window_days: int,
    target_direction: str,
    missing: int,
    advisory: str,
) -> ConvergenceReport:
    """Build a neutral-multiplier report for total-failure paths."""
    return ConvergenceReport(
        ticker=ticker,
        as_of=as_of.isoformat(),
        window_days=window_days,
        target_direction=target_direction,
        stream_signals=(),
        n_active_streams=0,
        n_aligned=0,
        n_opposed=0,
        convergence_score=0.0,
        conviction_multiplier=MULTIPLIER_NEUTRAL,
        evidence_chain=(),
        advisory=advisory,
        missing_stream_count=missing,
    )


# ── Public API ────────────────────────────────────────────────────────────

def scan_convergence(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
    target_direction: str,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> ConvergenceReport:
    """Scan all 8 alt-data streams for convergence on a single ticker.

    Args:
        engine: SQLAlchemy engine against the GRID database.
        ticker: Equity/ETF/ADR symbol to scan.
        as_of: PIT anchor — nothing written after this date is read.
        target_direction: Which direction are we scoring for? One of
            ``"bullish"``, ``"bearish"``, ``"neutral"``. Unknown values
            collapse to ``"neutral"`` and return a MULTIPLIER_NEUTRAL
            report (no failure).
        window_days: Lookback in days for each stream. Default 7.

    Returns:
        A fully-populated ConvergenceReport. Never raises.
    """
    # Unknown directions (i.e. strings that don't normalize to one of
    # BULLISH/BEARISH/NEUTRAL) collapse to a neutral report. Legitimate
    # "neutral" inputs are accepted and scored normally.
    raw_dir = (
        str(target_direction).strip().lower() if target_direction is not None else ""
    )
    if raw_dir not in _DIRECTION_ALIASES:
        log.debug(
            "scan_convergence: unknown target_direction={t} → neutral report",
            t=target_direction,
        )
        return _empty_report(
            ticker=ticker,
            as_of=as_of,
            window_days=window_days,
            target_direction=NEUTRAL,
            missing=N_STREAMS,
            advisory="Unknown target_direction; neutral fallback.",
        )
    tgt = _normalize_direction(target_direction)

    trust_cache: dict[str, float] = {}
    active: list[StreamSignal] = []
    missing = 0

    for stream_name, extractor in _STREAM_EXTRACTORS:
        try:
            sig = extractor(engine, ticker, as_of, window_days, trust_cache)
        except Exception as exc:  # pragma: no cover — defensive
            log.warning(
                "Extractor {s} crashed for ticker={t}: {e}",
                s=stream_name, t=ticker, e=str(exc),
            )
            sig = None
        if sig is None:
            missing += 1
            continue
        active.append(sig)

    if not active:
        return _empty_report(
            ticker=ticker,
            as_of=as_of,
            window_days=window_days,
            target_direction=tgt,
            missing=missing,
            advisory=(
                f"No alt-data streams fired on {ticker} in {window_days}d "
                "window. Single-source risk — no convergence edge."
            ),
        )

    (
        score,
        n_aligned,
        n_opposed,
        w_align,
        w_oppose,
        _w_neut,
    ) = _compute_convergence(active, tgt)

    multiplier = _pick_multiplier(
        n_aligned=n_aligned,
        n_opposed=n_opposed,
        convergence_score=score,
        weighted_alignment=w_align,
        weighted_opposition=w_oppose,
    )

    chain = tuple(
        sig.evidence_line
        for sig in sorted(
            active,
            key=lambda s: s.evidence_line.split("  ")[0],
        )
    )

    if multiplier >= MULTIPLIER_STRONG:
        advisory = (
            f"STRONG CONVERGENCE: {n_aligned} orthogonal streams agree "
            f"on {tgt} {ticker}. Rare — size up."
        )
    elif multiplier >= MULTIPLIER_SOLID:
        advisory = (
            f"SOLID CONVERGENCE: {n_aligned} streams aligned on "
            f"{tgt} {ticker}."
        )
    elif multiplier >= MULTIPLIER_MODERATE:
        advisory = (
            f"Moderate convergence: {n_aligned} streams agree on {tgt}."
        )
    elif multiplier >= MULTIPLIER_WEAK:
        advisory = f"Weak confirmation: only {n_aligned} aligned streams."
    elif multiplier <= MULTIPLIER_OPPOSED:
        advisory = (
            f"Streams DISAGREE: {n_opposed} opposed vs {n_aligned} aligned. "
            f"Conviction penalty applied."
        )
    else:
        advisory = (
            f"No convergence edge: {n_aligned} aligned, {n_opposed} opposed."
        )

    return ConvergenceReport(
        ticker=ticker,
        as_of=as_of.isoformat(),
        window_days=window_days,
        target_direction=tgt,
        stream_signals=tuple(active),
        n_active_streams=len(active),
        n_aligned=n_aligned,
        n_opposed=n_opposed,
        convergence_score=score,
        conviction_multiplier=multiplier,
        evidence_chain=chain,
        advisory=advisory,
        missing_stream_count=missing,
    )


def convergence_conviction_multiplier(
    engine: Engine,
    *,
    ticker: str,
    as_of: date,
    target_direction: str,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> float:
    """Live-path convenience wrapper around :func:`scan_convergence`.

    Returns ``MULTIPLIER_NEUTRAL`` (1.00) on any failure. Never raises.
    Entry point for ``signal_provenance.build_provenance_report`` and
    other live scoring paths that only want the scalar multiplier.
    """
    try:
        report = scan_convergence(
            engine,
            ticker=ticker,
            as_of=as_of,
            target_direction=target_direction,
            window_days=window_days,
        )
        return float(report.conviction_multiplier)
    except Exception as exc:
        log.warning(
            "convergence_conviction_multiplier failed for {t}: {e}",
            t=ticker, e=str(exc),
        )
        return MULTIPLIER_NEUTRAL


def rank_universe_by_convergence(
    engine: Engine,
    *,
    tickers: list[str],
    as_of: date,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_streams: int = THRESHOLD_MODERATE_N,
) -> list[ConvergenceReport]:
    """Scan a candidate list and return only the hot convergences.

    Filters to reports where ``n_aligned >= min_streams`` and sorts by
    ``convergence_score`` descending. Used by the dashboard "hot
    convergences" view; acceptable O(tickers × streams) cost because
    it's not a live-path call.

    Args:
        engine: SQLAlchemy engine.
        tickers: Candidate tickers. Empty list returns ``[]``.
        as_of: PIT anchor.
        window_days: Lookback for each stream.
        min_streams: Minimum aligned streams to keep. Defaults to
            THRESHOLD_MODERATE_N (3) — the first "actually interesting"
            rung of the multiplier ladder.

    Returns:
        List of ConvergenceReport sorted by convergence_score desc.
        For the ranking step we score each ticker in the BULLISH
        direction; callers wanting bearish ranks should invoke twice.
    """
    if not tickers:
        return []

    results: list[ConvergenceReport] = []
    for ticker in tickers:
        report = scan_convergence(
            engine,
            ticker=ticker,
            as_of=as_of,
            target_direction=BULLISH,
            window_days=window_days,
        )
        if report.n_aligned >= min_streams:
            results.append(report)

    results.sort(key=lambda r: r.convergence_score, reverse=True)
    return results
