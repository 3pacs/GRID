"""
GRID Intelligence — Trump-Proximity Score (TPS) v0.

Phase 0 of the GRID-4-product pivot (see ``docs/planning/GRID-4-PRODUCT-PIVOT.md``).

For a given ticker we compute a per-ticker proximity score (0-100) by
combining five independent evidence layers:

  1. Direct government contract $ flowing to the issuer (USAspending / gov_contracts).
  2. Lobbying $ aimed at administration priorities (Senate LDA / OpenSecrets).
  3. Congressional buy pressure over the trailing 30 days.
  4. FARA edges from foreign principals to admin-aligned lobbyists.
  5. Actor-network hops from issuer to the closest admin-aligned actor.

NULL-propagation contract (CRITICAL — see section 7 of the pivot doc):
  * Each layer returns ``Optional[float]`` in [0, 1].
  * A missing upstream MUST return ``None`` — never silently default to
    1.0 or 0. That defensive default is the root cause of "11.9% hit rate
    at HIGH confidence" in the legacy stack and we are explicitly NOT
    repeating it here.
  * The aggregate score is the simple weighted sum over the layers that
    actually returned a value. ``coverage`` reports which layers fired,
    so downstream UI can display a "low coverage" badge.
  * If zero layers report data we return ``score=None`` (NOT 0).

This module is intentionally narrow (~300 LOC). It is the Phase 0
"unblocking artifact" — weights are flat 1.0 until we have 30 days
of forward returns to tune them.

Public API:
  * ``compute_tps_for_ticker(engine, ticker, as_of) -> TPSResult``
  * ``compute_tps_batch(engine, tickers, as_of) -> list[TPSResult]``
  * ``persist_snapshot(engine, result)``
  * ``refresh_top_universe(engine, as_of, universe) -> list[TPSResult]``
"""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any, Optional

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Tunables (v0 — all weights flat per the pivot plan) ──────────────────

LAYER_WEIGHTS: dict[str, float] = {
    "direct_contracts": 1.0,
    "lobbying_admin": 1.0,
    "congressional_30d": 1.0,
    "fara_admin": 1.0,
    "actor_hops": 1.0,
}

# Normalisation reference values. Anything at or above these caps out at
# 1.0 for that layer. Calibrated from the post-2025 distribution; weights
# are revisited in Phase 1.
CONTRACT_REF_USD: float = 1_000_000_000.0      # $1B / year
LOBBYING_REF_USD: float = 5_000_000.0          # $5M / year
CONGRESSIONAL_REF_USD: float = 1_000_000.0     # $1M aggregate notional / 30d
FARA_REF_EDGES: float = 10.0                   # 10 FARA edges
HOPS_MAX: int = 4                              # >=4 hops scores 0

CONGRESSIONAL_WINDOW_DAYS: int = 30
LOBBYING_WINDOW_DAYS: int = 90
CONTRACT_WINDOW_DAYS: int = 365
FARA_WINDOW_DAYS: int = 180


# ── Result dataclasses ──────────────────────────────────────────────────


@dataclass(frozen=True)
class EvidenceItem:
    """One row of TPS evidence backing a layer's contribution."""

    layer: str
    source: str
    detail: str
    amount_usd: Optional[float]
    observed_at: Optional[str]


@dataclass(frozen=True)
class TPSResult:
    """Computed TPS for a single (ticker, as_of) pair."""

    ticker: str
    as_of: str
    score: Optional[float]              # 0-100 or None if no coverage
    layer_scores: dict[str, Optional[float]]   # per-layer normalised [0, 1] or None
    coverage: dict[str, bool]           # which layers had data
    evidence: list[EvidenceItem] = field(default_factory=list)


# ── Layer 1: direct government contracts ────────────────────────────────


def _layer_direct_contracts(
    engine: Engine, ticker: str, as_of: date
) -> tuple[Optional[float], list[EvidenceItem]]:
    """Total contract obligations awarded to the ticker over CONTRACT_WINDOW_DAYS.

    Returns (normalised_score, evidence). ``None`` if upstream is empty
    or the table is missing (NULL-propagation).
    """
    start = as_of - timedelta(days=CONTRACT_WINDOW_DAYS)
    sql = text(
        """
        SELECT
            signal_value,
            signal_date,
            source_id
        FROM signal_sources
        WHERE source_type = 'gov_contract'
          AND ticker = :ticker
          AND signal_date >= :start
          AND signal_date <= :as_of
        ORDER BY signal_date DESC
        LIMIT 200
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sql,
                {"ticker": ticker, "start": start, "as_of": as_of},
            ).fetchall()
    except Exception as exc:
        log.debug("TPS contracts layer DB error for {t}: {e}", t=ticker, e=str(exc))
        return None, []

    if not rows:
        return None, []

    total = 0.0
    evidence: list[EvidenceItem] = []
    for row in rows:
        sv = row[0] if not isinstance(row[0], str) else json.loads(row[0])
        amount = _coerce_float((sv or {}).get("amount") or (sv or {}).get("value"))
        if amount is None:
            continue
        total += amount
        evidence.append(
            EvidenceItem(
                layer="direct_contracts",
                source=str(row[2] or "usaspending"),
                detail=(sv or {}).get("description", "")[:160],
                amount_usd=amount,
                observed_at=str(row[1]),
            )
        )

    if total <= 0:
        return None, []
    score = min(total / CONTRACT_REF_USD, 1.0)
    return score, evidence[:8]


# ── Layer 2: lobbying $ on admin priorities ─────────────────────────────


def _layer_lobbying(
    engine: Engine, ticker: str, as_of: date
) -> tuple[Optional[float], list[EvidenceItem]]:
    """Aggregate lobbying $ tagged to the ticker over LOBBYING_WINDOW_DAYS.

    Sums Senate LDA + OpenSecrets disclosures emitted as ``lobbying`` rows
    in ``signal_sources``.
    """
    start = as_of - timedelta(days=LOBBYING_WINDOW_DAYS)
    sql = text(
        """
        SELECT signal_value, signal_date, source_id
        FROM signal_sources
        WHERE source_type = 'lobbying'
          AND ticker = :ticker
          AND signal_date >= :start
          AND signal_date <= :as_of
        ORDER BY signal_date DESC
        LIMIT 200
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sql,
                {"ticker": ticker, "start": start, "as_of": as_of},
            ).fetchall()
    except Exception as exc:
        log.debug("TPS lobbying layer DB error for {t}: {e}", t=ticker, e=str(exc))
        return None, []

    if not rows:
        return None, []

    total = 0.0
    evidence: list[EvidenceItem] = []
    for row in rows:
        sv = row[0] if not isinstance(row[0], str) else json.loads(row[0])
        amount = _coerce_float(
            (sv or {}).get("amount") or (sv or {}).get("total_amount")
        )
        if amount is None:
            continue
        total += amount
        evidence.append(
            EvidenceItem(
                layer="lobbying_admin",
                source=str(row[2] or "senate_lda"),
                detail=(sv or {}).get("issue") or (sv or {}).get("client", "")[:160],
                amount_usd=amount,
                observed_at=str(row[1]),
            )
        )

    if total <= 0:
        return None, []
    return min(total / LOBBYING_REF_USD, 1.0), evidence[:8]


# ── Layer 3: congressional buy pressure (30d) ───────────────────────────


def _layer_congressional(
    engine: Engine, ticker: str, as_of: date
) -> tuple[Optional[float], list[EvidenceItem]]:
    """Net buy notional from congressional_trades over the trailing 30d.

    A negative net flow (sells > buys) still surfaces as evidence but
    is clamped to 0 on the score side — TPS is unsigned.
    """
    start = as_of - timedelta(days=CONGRESSIONAL_WINDOW_DAYS)
    sql = text(
        """
        SELECT
            representative,
            party,
            transaction_type,
            amount_midpoint,
            disclosure_date
        FROM congressional_trades
        WHERE ticker = :ticker
          AND disclosure_date >= :start
          AND disclosure_date <= :as_of
        ORDER BY disclosure_date DESC
        LIMIT 50
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sql,
                {"ticker": ticker, "start": start, "as_of": as_of},
            ).fetchall()
    except Exception as exc:
        log.debug(
            "TPS congressional layer DB error for {t}: {e}", t=ticker, e=str(exc)
        )
        return None, []

    if not rows:
        return None, []

    net = 0.0
    evidence: list[EvidenceItem] = []
    for rep, party, txn, amt, when in rows:
        amount = _coerce_float(amt)
        if amount is None:
            continue
        sign = 1.0 if (txn or "").upper().startswith("BUY") else -1.0
        net += sign * amount
        evidence.append(
            EvidenceItem(
                layer="congressional_30d",
                source=f"{rep} ({party or '?'})",
                detail=f"{txn} ${amount:,.0f}",
                amount_usd=amount,
                observed_at=str(when),
            )
        )

    if not evidence:
        return None, []
    score = max(net, 0.0) / CONGRESSIONAL_REF_USD
    return min(score, 1.0), evidence[:8]


# ── Layer 4: FARA foreign-principal edges to admin-aligned actors ───────


def _layer_fara(
    engine: Engine, ticker: str, as_of: date
) -> tuple[Optional[float], list[EvidenceItem]]:
    """Count FARA activities where the registrant is admin-aligned and the
    targeted sector/ticker matches our ticker."""
    start = as_of - timedelta(days=FARA_WINDOW_DAYS)
    sql = text(
        """
        SELECT signal_value, signal_date, source_id
        FROM signal_sources
        WHERE source_type = 'foreign_lobbying'
          AND ticker = :ticker
          AND signal_date >= :start
          AND signal_date <= :as_of
        ORDER BY signal_date DESC
        LIMIT 200
        """
    )
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                sql,
                {"ticker": ticker, "start": start, "as_of": as_of},
            ).fetchall()
    except Exception as exc:
        log.debug("TPS FARA layer DB error for {t}: {e}", t=ticker, e=str(exc))
        return None, []

    if not rows:
        return None, []

    admin_edges = 0
    evidence: list[EvidenceItem] = []
    for row in rows:
        sv = row[0] if not isinstance(row[0], str) else json.loads(row[0])
        if not isinstance(sv, dict):
            continue
        registrant = str(sv.get("registrant_name", "") or row[2] or "")
        country = str(sv.get("country", ""))
        # Admin-aligned heuristic: a registrant string that contains a
        # known Trump-aligned lobbying firm. The actor-network lookup
        # below is what makes this defensible — we don't rely on the
        # heuristic alone.
        if _is_admin_aligned_registrant(registrant):
            admin_edges += 1
            evidence.append(
                EvidenceItem(
                    layer="fara_admin",
                    source=registrant,
                    detail=f"FARA edge from {country}: {sv.get('activity_type', '')}",
                    amount_usd=_coerce_float(sv.get("compensation")),
                    observed_at=str(row[1]),
                )
            )

    if admin_edges == 0:
        return None, []
    return min(admin_edges / FARA_REF_EDGES, 1.0), evidence[:8]


_ADMIN_ALIGNED_FIRM_TOKENS: tuple[str, ...] = (
    "ballard",
    "checkmate",
    "bgr group",
    "miller strategies",
    "mercury public affairs",
    "continental strategy",
)


def _is_admin_aligned_registrant(name: str) -> bool:
    """Heuristic: registrant string contains a known admin-aligned firm.

    Phase 1 will replace this with a graph lookup against the actors
    table's ``political_affiliations`` and ``metadata.political_connections``
    fields. For Phase 0 the firm-name list is the documented gate.
    """
    if not name:
        return False
    low = name.lower()
    return any(tok in low for tok in _ADMIN_ALIGNED_FIRM_TOKENS)


# ── Layer 5: actor-network hops issuer → admin actor ────────────────────


def _layer_actor_hops(
    engine: Engine, ticker: str, as_of: date  # noqa: ARG001 (kept for PIT future use)
) -> tuple[Optional[float], list[EvidenceItem]]:
    """Inverse of the shortest path (in hops) from the ticker's primary
    actor to any actor flagged as admin-aligned.

    Returns ``None`` if the actor graph has no entry for the ticker. A
    hop count >= ``HOPS_MAX`` scores 0 (NOT None — the data is present,
    it just disagrees).
    """
    try:
        with engine.connect() as conn:
            # Resolve ticker → actor id (issuer or known wrapper)
            issuer_row = conn.execute(
                text(
                    """
                    SELECT id FROM actors
                    WHERE metadata->>'ticker' = :ticker
                       OR id = :ticker_lower
                    LIMIT 1
                    """
                ),
                {"ticker": ticker, "ticker_lower": ticker.lower()},
            ).fetchone()
            if issuer_row is None:
                return None, []
            issuer_id = issuer_row[0]

            # BFS up to HOPS_MAX hops looking for admin-aligned actors.
            hops, admin_actor = _bfs_to_admin(conn, issuer_id, max_hops=HOPS_MAX)
    except Exception as exc:
        log.debug("TPS hops layer DB error for {t}: {e}", t=ticker, e=str(exc))
        return None, []

    if hops is None:
        return 0.0, []  # graph present but no path within budget
    score = max(0.0, 1.0 - (hops - 1) / max(HOPS_MAX - 1, 1))
    return score, [
        EvidenceItem(
            layer="actor_hops",
            source=admin_actor or "actor_graph",
            detail=f"{hops}-hop path from {issuer_id} to admin-aligned actor",
            amount_usd=None,
            observed_at=None,
        )
    ]


def _bfs_to_admin(conn, start: str, max_hops: int) -> tuple[Optional[int], Optional[str]]:
    """Walk the ``actors.connections`` JSONB graph BFS up to max_hops.

    Returns (hops, admin_actor_id) or (None, None) if no admin actor reached.
    """
    seen: set[str] = {start}
    frontier: list[str] = [start]
    for hop in range(1, max_hops + 1):
        if not frontier:
            return None, None
        # Batch lookup for the frontier.
        rows = conn.execute(
            text(
                """
                SELECT id, connections, political_affiliations, metadata
                FROM actors
                WHERE id = ANY(:ids)
                """
            ),
            {"ids": frontier},
        ).fetchall()
        next_frontier: list[str] = []
        for actor_id, conns, pol, meta in rows:
            if _actor_is_admin_aligned(pol, meta) and actor_id != start:
                return hop, actor_id
            for neighbor in _extract_neighbors(conns):
                if neighbor and neighbor not in seen:
                    seen.add(neighbor)
                    next_frontier.append(neighbor)
        frontier = next_frontier
    return None, None


def _extract_neighbors(conns: Any) -> list[str]:
    if conns is None:
        return []
    if isinstance(conns, str):
        try:
            conns = json.loads(conns)
        except (ValueError, TypeError):
            return []
    if not isinstance(conns, list):
        return []
    out: list[str] = []
    for c in conns:
        if isinstance(c, dict):
            actor = c.get("actor") or c.get("id")
            if isinstance(actor, str):
                out.append(actor)
        elif isinstance(c, str):
            out.append(c)
    return out


def _actor_is_admin_aligned(pol: Any, meta: Any) -> bool:
    blob_parts: list[str] = []
    for part in (pol, meta):
        if isinstance(part, str):
            blob_parts.append(part)
        elif isinstance(part, (dict, list)):
            blob_parts.append(json.dumps(part))
    blob = " ".join(blob_parts).lower()
    return "trump-aligned" in blob or "doge" in blob or "trump 47" in blob


# ── Aggregation ────────────────────────────────────────────────────────


def compute_tps_for_ticker(
    engine: Engine, ticker: str, as_of: Optional[date] = None
) -> TPSResult:
    """Compute the TPS for a single ticker as of ``as_of`` (default today).

    NULL-propagation contract: if every layer returns ``None`` the
    aggregate score is ``None``, not 0.
    """
    if as_of is None:
        as_of = date.today()

    layers: list[tuple[str, Optional[float], list[EvidenceItem]]] = [
        ("direct_contracts", *_layer_direct_contracts(engine, ticker, as_of)),
        ("lobbying_admin", *_layer_lobbying(engine, ticker, as_of)),
        ("congressional_30d", *_layer_congressional(engine, ticker, as_of)),
        ("fara_admin", *_layer_fara(engine, ticker, as_of)),
        ("actor_hops", *_layer_actor_hops(engine, ticker, as_of)),
    ]

    layer_scores: dict[str, Optional[float]] = {}
    coverage: dict[str, bool] = {}
    evidence: list[EvidenceItem] = []
    weighted_sum = 0.0
    weight_total = 0.0
    have_any = False

    for name, score, evid in layers:
        layer_scores[name] = score
        coverage[name] = score is not None
        if score is None:
            continue
        have_any = True
        w = LAYER_WEIGHTS.get(name, 1.0)
        weighted_sum += w * score
        weight_total += w
        evidence.extend(evid)

    if not have_any:
        return TPSResult(
            ticker=ticker,
            as_of=as_of.isoformat(),
            score=None,
            layer_scores=layer_scores,
            coverage=coverage,
            evidence=[],
        )

    normalised = weighted_sum / weight_total  # weight_total > 0 since have_any
    if math.isnan(normalised) or math.isinf(normalised):
        log.warning("TPS aggregate produced non-finite value for {t}", t=ticker)
        normalised = 0.0
    final_score = round(100.0 * max(0.0, min(normalised, 1.0)), 2)

    return TPSResult(
        ticker=ticker,
        as_of=as_of.isoformat(),
        score=final_score,
        layer_scores=layer_scores,
        coverage=coverage,
        evidence=evidence,
    )


def compute_tps_batch(
    engine: Engine, tickers: list[str], as_of: Optional[date] = None
) -> list[TPSResult]:
    """Run ``compute_tps_for_ticker`` over a list, preserving order."""
    if as_of is None:
        as_of = date.today()
    return [compute_tps_for_ticker(engine, t, as_of) for t in tickers]


# ── Persistence ────────────────────────────────────────────────────────


def persist_snapshot(engine: Engine, result: TPSResult) -> None:
    """Upsert a single TPSResult into ``tps_snapshots``.

    Idempotent on (ticker, as_of_date). Score and evidence are overwritten
    on a re-run because the underlying signal_sources may have backfilled.
    """
    sql = text(
        """
        INSERT INTO tps_snapshots
            (ticker, as_of_date, score, coverage, evidence, layer_scores, generated_at)
        VALUES
            (:ticker, :as_of, :score,
             CAST(:coverage AS jsonb),
             CAST(:evidence AS jsonb),
             CAST(:layer_scores AS jsonb),
             NOW())
        ON CONFLICT (ticker, as_of_date) DO UPDATE SET
            score = EXCLUDED.score,
            coverage = EXCLUDED.coverage,
            evidence = EXCLUDED.evidence,
            layer_scores = EXCLUDED.layer_scores,
            generated_at = NOW()
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "ticker": result.ticker,
                "as_of": result.as_of,
                "score": result.score,
                "coverage": json.dumps(result.coverage),
                "evidence": json.dumps(
                    [asdict(e) for e in result.evidence]
                ),
                "layer_scores": json.dumps(result.layer_scores),
            },
        )


def refresh_top_universe(
    engine: Engine,
    as_of: Optional[date] = None,
    universe: Optional[list[str]] = None,
) -> list[TPSResult]:
    """Daily 06:00 ET entry point — refresh TPS for the watchlist universe.

    The default universe is read from the ``tps_universe`` table if it
    exists, otherwise from a small hard-coded high-government-exposure
    starter set. Phase 1 will replace the hard-coded set with the full
    S&P 500.
    """
    if as_of is None:
        as_of = date.today()
    tickers = universe if universe is not None else _resolve_universe(engine)
    results = compute_tps_batch(engine, tickers, as_of=as_of)
    for r in results:
        try:
            persist_snapshot(engine, r)
        except Exception as exc:
            log.warning(
                "TPS persist failed for {t} as_of={d}: {e}",
                t=r.ticker, d=r.as_of, e=str(exc),
            )
    log.info(
        "TPS refresh complete: {n} tickers as_of={d}",
        n=len(results), d=as_of.isoformat(),
    )
    return results


_DEFAULT_UNIVERSE: tuple[str, ...] = (
    "LMT", "RTX", "NOC", "GD", "BA",         # defense primes
    "PLTR", "ANDURIL",                         # admin-favored tech
    "TSLA", "X",                               # Musk-related
    "TSM", "INTC", "AVGO", "NVDA",             # CHIPS-Act exposure
    "XOM", "CVX", "OXY",                       # energy
    "JPM", "GS", "MS",                         # banks
    "META", "GOOG", "MSFT", "AMZN", "AAPL",    # large caps with lobbying spend
)


def _resolve_universe(engine: Engine) -> list[str]:
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT ticker FROM tps_universe WHERE active = TRUE")
            ).fetchall()
            if rows:
                return [r[0] for r in rows]
    except Exception:
        pass
    return list(_DEFAULT_UNIVERSE)


# ── Helpers ────────────────────────────────────────────────────────────


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


__all__ = [
    "EvidenceItem",
    "TPSResult",
    "compute_tps_for_ticker",
    "compute_tps_batch",
    "persist_snapshot",
    "refresh_top_universe",
    "LAYER_WEIGHTS",
]
