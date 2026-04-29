"""
Deployer track-record registry.

The most durable edge in Solana memecoin trading is piggy-backing on
deployers with a repeat history. This module:

  * stores one row per wallet in ``solana_deployers``
  * pulls the wallet's launch history from any
    :class:`trading.solana.helius_client.DeployInfoProvider`
  * computes a deterministic, interpretable score based on:
      - graduation rate (% of launches that hit a minimum market cap)
      - median peak market cap
      - how long the deployer typically holds
      - recency of the last launch
      - confidence term that grows with sample size
  * persists per-launch history in ``solana_deployer_launches`` so we
    can recompute the score later without re-hitting the provider

The scoring function is **pure** (``score_deployer``) so it's trivial
to unit-test and replay against new weights.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from trading.solana.helius_client import DeployInfoProvider, DeployRecord


# ----------------------------------------------------------------------
# Scoring weights — all public so the operator can tune from config
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class DeployerScoreWeights:
    graduation_rate_weight: float = 0.50
    peak_multiplier_weight: float = 0.25
    hold_duration_weight: float = 0.10
    min_launches_for_confidence: int = 5
    confidence_cap: int = 10
    graduation_mc_threshold_usd: float = 100_000.0
    peak_multiplier_cap: float = 10.0
    recency_half_life_days: float = 60.0
    min_launches_for_score: int = 3


DEFAULT_WEIGHTS = DeployerScoreWeights()


# ----------------------------------------------------------------------
# DTOs
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class DeployerStats:
    """Aggregate stats computed over a deployer's launches."""

    wallet: str
    n_launches: int
    n_graduated: int
    median_peak_mc_usd: float
    best_peak_mc_usd: float
    avg_hold_seconds: float
    last_launch_at: datetime | None

    @property
    def graduation_rate(self) -> float:
        if self.n_launches == 0:
            return 0.0
        return self.n_graduated / self.n_launches


@dataclass(frozen=True)
class DeployerScoreResult:
    """Result of :func:`score_deployer` — the score plus its components."""

    wallet: str
    score: float
    components: dict[str, float]
    stats: DeployerStats
    reasons: tuple[str, ...]


# ----------------------------------------------------------------------
# Pure scoring function
# ----------------------------------------------------------------------
def score_deployer(
    stats: DeployerStats,
    weights: DeployerScoreWeights = DEFAULT_WEIGHTS,
    now: datetime | None = None,
) -> DeployerScoreResult:
    """Compute a deployer score in [0, 1] from aggregate stats.

    Pure function: deterministic, side-effect-free, ``now`` injectable
    for tests.
    """
    now = now or datetime.now(timezone.utc)
    components: dict[str, float] = {}
    reasons: list[str] = []

    if stats.n_launches < weights.min_launches_for_score:
        components["min_samples"] = 0.0
        reasons.append(
            f"fewer than {weights.min_launches_for_score} launches"
        )
        return DeployerScoreResult(
            wallet=stats.wallet,
            score=0.0,
            components=components,
            stats=stats,
            reasons=tuple(reasons),
        )

    # ---- Graduation rate --------------------------------------------
    graduation = stats.graduation_rate
    components["graduation_rate"] = graduation
    reasons.append(
        f"graduation_rate={graduation:.2f} "
        f"({stats.n_graduated}/{stats.n_launches})"
    )

    # ---- Peak multiplier (median peak MC / threshold) ---------------
    peak_mult_raw = (
        stats.median_peak_mc_usd / weights.graduation_mc_threshold_usd
        if weights.graduation_mc_threshold_usd > 0
        else 0.0
    )
    peak_mult = min(peak_mult_raw, weights.peak_multiplier_cap) / weights.peak_multiplier_cap
    components["peak_multiplier"] = peak_mult
    reasons.append(f"median_peak_mc=${stats.median_peak_mc_usd:,.0f}")

    # ---- Hold duration --------------------------------------------
    # Reward deployers who hold for at least a few minutes — fast dumps
    # mean the deployer is exit liquidity themselves.
    hold_score = _normalise_hold(stats.avg_hold_seconds)
    components["hold_duration"] = hold_score

    # ---- Confidence (sample size) -----------------------------------
    confidence = min(
        stats.n_launches / max(weights.confidence_cap, 1), 1.0
    )
    components["confidence"] = confidence

    # ---- Recency multiplier ----------------------------------------
    recency_multiplier = _recency_multiplier(
        stats.last_launch_at, now, weights.recency_half_life_days
    )
    components["recency_multiplier"] = recency_multiplier

    # ---- Weighted sum ----------------------------------------------
    base = (
        weights.graduation_rate_weight * graduation
        + weights.peak_multiplier_weight * peak_mult
        + weights.hold_duration_weight * hold_score
    )
    score = base * confidence * recency_multiplier
    score = max(0.0, min(score, 1.0))

    return DeployerScoreResult(
        wallet=stats.wallet,
        score=score,
        components=components,
        stats=stats,
        reasons=tuple(reasons),
    )


# ----------------------------------------------------------------------
# Registry — DB-backed store and refresh loop
# ----------------------------------------------------------------------
class DeployerRegistry:
    """Persistent deployer track record + scorer."""

    def __init__(
        self,
        engine: Engine,
        provider: DeployInfoProvider | None = None,
        weights: DeployerScoreWeights = DEFAULT_WEIGHTS,
    ) -> None:
        self.engine = engine
        self.provider = provider
        self.weights = weights
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------
    def _ensure_tables(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS solana_deployers (
                        wallet              TEXT PRIMARY KEY,
                        n_launches          INTEGER NOT NULL DEFAULT 0,
                        n_graduated         INTEGER NOT NULL DEFAULT 0,
                        median_peak_mc_usd  FLOAT NOT NULL DEFAULT 0,
                        best_peak_mc_usd    FLOAT NOT NULL DEFAULT 0,
                        avg_hold_seconds    FLOAT NOT NULL DEFAULT 0,
                        score               FLOAT NOT NULL DEFAULT 0,
                        last_launch_at      TIMESTAMPTZ,
                        components          JSONB,
                        notes               TEXT,
                        active              BOOLEAN NOT NULL DEFAULT TRUE,
                        first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS solana_deployer_launches (
                        id                   SERIAL PRIMARY KEY,
                        wallet               TEXT NOT NULL,
                        mint                 TEXT NOT NULL,
                        launch_at            TIMESTAMPTZ NOT NULL,
                        initial_liquidity_usd FLOAT,
                        peak_mc_usd          FLOAT,
                        current_mc_usd       FLOAT,
                        deployer_hold_seconds INTEGER,
                        graduated            BOOLEAN NOT NULL DEFAULT FALSE,
                        source               TEXT NOT NULL DEFAULT 'helius',
                        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        UNIQUE (wallet, mint)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_deployer_launches_wallet "
                    "ON solana_deployer_launches(wallet)"
                )
            )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------
    def get(self, wallet: str) -> DeployerScoreResult | None:
        """Load a cached scored deployer from the DB."""
        sql = text(
            "SELECT wallet, n_launches, n_graduated, median_peak_mc_usd, "
            "best_peak_mc_usd, avg_hold_seconds, score, last_launch_at, "
            "components FROM solana_deployers WHERE wallet = :w"
        )
        with self.engine.connect() as conn:
            row = conn.execute(sql, {"w": wallet}).fetchone()
        if row is None:
            return None
        stats = DeployerStats(
            wallet=row[0],
            n_launches=int(row[1] or 0),
            n_graduated=int(row[2] or 0),
            median_peak_mc_usd=float(row[3] or 0.0),
            best_peak_mc_usd=float(row[4] or 0.0),
            avg_hold_seconds=float(row[5] or 0.0),
            last_launch_at=row[7],
        )
        components_raw = row[8]
        if isinstance(components_raw, str):
            try:
                components = json.loads(components_raw)
            except json.JSONDecodeError:
                components = {}
        elif isinstance(components_raw, dict):
            components = components_raw
        else:
            components = {}
        return DeployerScoreResult(
            wallet=stats.wallet,
            score=float(row[6] or 0.0),
            components=components,
            stats=stats,
            reasons=(),
        )

    def list_active(self, min_score: float = 0.0) -> list[DeployerScoreResult]:
        """Return every active deployer above ``min_score``, best first."""
        sql = text(
            "SELECT wallet, n_launches, n_graduated, median_peak_mc_usd, "
            "best_peak_mc_usd, avg_hold_seconds, score, last_launch_at, "
            "components FROM solana_deployers "
            "WHERE active = TRUE AND score >= :s "
            "ORDER BY score DESC"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"s": min_score}).fetchall()
        out: list[DeployerScoreResult] = []
        for row in rows:
            stats = DeployerStats(
                wallet=row[0],
                n_launches=int(row[1] or 0),
                n_graduated=int(row[2] or 0),
                median_peak_mc_usd=float(row[3] or 0.0),
                best_peak_mc_usd=float(row[4] or 0.0),
                avg_hold_seconds=float(row[5] or 0.0),
                last_launch_at=row[7],
            )
            out.append(
                DeployerScoreResult(
                    wallet=stats.wallet,
                    score=float(row[6] or 0.0),
                    components={},
                    stats=stats,
                    reasons=(),
                )
            )
        return out

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def upsert_launches(
        self,
        wallet: str,
        launches: list[DeployRecord],
    ) -> int:
        """Insert any new launches and return how many rows were added."""
        if not launches:
            return 0
        added = 0
        with self.engine.begin() as conn:
            for rec in launches:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO solana_deployer_launches
                            (wallet, mint, launch_at, initial_liquidity_usd,
                             peak_mc_usd, current_mc_usd,
                             deployer_hold_seconds, graduated, source)
                        VALUES
                            (:w, :m, :la, :il, :pm, :cm, :dh, :g, :s)
                        ON CONFLICT (wallet, mint) DO UPDATE
                            SET peak_mc_usd = COALESCE(EXCLUDED.peak_mc_usd, solana_deployer_launches.peak_mc_usd),
                                current_mc_usd = COALESCE(EXCLUDED.current_mc_usd, solana_deployer_launches.current_mc_usd),
                                deployer_hold_seconds = COALESCE(EXCLUDED.deployer_hold_seconds, solana_deployer_launches.deployer_hold_seconds),
                                graduated = EXCLUDED.graduated
                        RETURNING (xmax = 0) AS inserted
                        """
                    ),
                    {
                        "w": wallet,
                        "m": rec.mint,
                        "la": rec.created_at,
                        "il": rec.initial_liquidity_usd,
                        "pm": rec.peak_market_cap_usd,
                        "cm": rec.current_market_cap_usd,
                        "dh": rec.deployer_hold_seconds,
                        "g": _is_graduated(rec, self.weights),
                        "s": rec.source,
                    },
                )
                row = result.fetchone()
                if row and row[0]:
                    added += 1
        return added

    def recompute(self, wallet: str) -> DeployerScoreResult:
        """Recompute stats and score from ``solana_deployer_launches``."""
        stats = self._aggregate_launches(wallet)
        result = score_deployer(stats, self.weights)
        self._persist_score(result)
        return result

    def refresh_wallet(
        self,
        wallet: str,
        lookback_days: int = 180,
    ) -> DeployerScoreResult:
        """Pull deploys from the provider, persist, and recompute.

        Requires a provider to be attached. Use :meth:`recompute` when
        you've already ingested launches from some other source.
        """
        if self.provider is None:
            raise ValueError("refresh_wallet requires a DeployInfoProvider")
        launches = self.provider.list_wallet_deploys(
            wallet, lookback_days=lookback_days
        )
        self.upsert_launches(wallet, launches)
        return self.recompute(wallet)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _aggregate_launches(self, wallet: str) -> DeployerStats:
        sql = text(
            "SELECT launch_at, peak_mc_usd, deployer_hold_seconds, graduated "
            "FROM solana_deployer_launches WHERE wallet = :w "
            "ORDER BY launch_at DESC"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"w": wallet}).fetchall()

        peaks: list[float] = []
        holds: list[float] = []
        n_graduated = 0
        last_launch_at: datetime | None = None
        for row in rows:
            if last_launch_at is None and row[0] is not None:
                last_launch_at = row[0]
            if row[1] is not None:
                peaks.append(float(row[1]))
            if row[2] is not None:
                holds.append(float(row[2]))
            if bool(row[3]):
                n_graduated += 1

        n_launches = len(rows)
        median_peak = _median(peaks)
        best_peak = max(peaks) if peaks else 0.0
        avg_hold = sum(holds) / len(holds) if holds else 0.0

        return DeployerStats(
            wallet=wallet,
            n_launches=n_launches,
            n_graduated=n_graduated,
            median_peak_mc_usd=median_peak,
            best_peak_mc_usd=best_peak,
            avg_hold_seconds=avg_hold,
            last_launch_at=last_launch_at,
        )

    def _persist_score(self, result: DeployerScoreResult) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO solana_deployers
                        (wallet, n_launches, n_graduated,
                         median_peak_mc_usd, best_peak_mc_usd,
                         avg_hold_seconds, score, last_launch_at,
                         components, updated_at)
                    VALUES
                        (:w, :nl, :ng, :mp, :bp, :ah, :sc, :ll,
                         CAST(:cm AS JSONB), NOW())
                    ON CONFLICT (wallet) DO UPDATE SET
                        n_launches = EXCLUDED.n_launches,
                        n_graduated = EXCLUDED.n_graduated,
                        median_peak_mc_usd = EXCLUDED.median_peak_mc_usd,
                        best_peak_mc_usd = EXCLUDED.best_peak_mc_usd,
                        avg_hold_seconds = EXCLUDED.avg_hold_seconds,
                        score = EXCLUDED.score,
                        last_launch_at = EXCLUDED.last_launch_at,
                        components = EXCLUDED.components,
                        updated_at = NOW()
                    """
                ),
                {
                    "w": result.wallet,
                    "nl": result.stats.n_launches,
                    "ng": result.stats.n_graduated,
                    "mp": result.stats.median_peak_mc_usd,
                    "bp": result.stats.best_peak_mc_usd,
                    "ah": result.stats.avg_hold_seconds,
                    "sc": result.score,
                    "ll": result.stats.last_launch_at,
                    "cm": json.dumps(result.components),
                },
            )
        log.info(
            "Deployer scored: {w} score={s:.3f} n={n}",
            w=result.wallet[:12] + "...",
            s=result.score,
            n=result.stats.n_launches,
        )


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _is_graduated(rec: DeployRecord, weights: DeployerScoreWeights) -> bool:
    if rec.peak_market_cap_usd is None:
        return False
    return rec.peak_market_cap_usd >= weights.graduation_mc_threshold_usd


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    n = len(sorted_values)
    mid = n // 2
    if n % 2 == 1:
        return sorted_values[mid]
    return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0


def _normalise_hold(seconds: float) -> float:
    """Map hold duration to [0, 1].

    < 60s  (fast dump)        → 0.0
    60s-5m (typical pump)     → linear ramp to 0.5
    5m-30m (real conviction)  → linear ramp to 1.0
    > 30m                     → 1.0
    """
    if seconds < 60:
        return 0.0
    if seconds < 300:
        return (seconds - 60) / 240 * 0.5
    if seconds < 1800:
        return 0.5 + (seconds - 300) / 1500 * 0.5
    return 1.0


def _recency_multiplier(
    last_launch_at: datetime | None,
    now: datetime,
    half_life_days: float,
) -> float:
    """Exponential decay on days-since-last-launch.

    A wallet that hasn't launched in half_life_days gets a multiplier
    of 0.5. Returns 1.0 for a wallet that launched today, 0.0 if the
    timestamp is unknown.
    """
    if last_launch_at is None or half_life_days <= 0:
        return 0.0
    if last_launch_at.tzinfo is None:
        last_launch_at = last_launch_at.replace(tzinfo=timezone.utc)
    delta_days = max(0.0, (now - last_launch_at).total_seconds() / 86_400.0)
    return math.pow(0.5, delta_days / half_life_days)
