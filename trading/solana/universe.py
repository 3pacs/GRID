"""
Read-only view over ``solana_token_universe``.

The top-volume ingestor populates that table every 4 hours. The trading
side needs a way to ask "is this mint already getting organic volume?"
without knowing anything about how the snapshot is written, so this
module wraps the table in a minimal query API that the cross-referencer
can depend on without circular imports with ``ingestion/solana``.

Nothing in this module writes — the ingestor owns the schema.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class UniverseRank:
    mint: str
    rank: int
    volume_24h_usd: float
    snapshot_at: datetime


class UniverseRankSource(Protocol):
    """Protocol used by :class:`trading.solana.cross_ref.CrossReferencer`.

    Implementations return a ``UniverseRank`` if the mint is present in
    the most recent snapshot, or ``None`` otherwise. A protocol lets the
    cross-referencer stay decoupled from the concrete SQL and lets tests
    inject a trivial dict-backed mock.
    """

    def get_latest_rank(self, mint: str) -> UniverseRank | None: ...


class UniverseRegistry:
    """Read-only query helper over ``solana_token_universe``.

    The table is populated by
    :class:`ingestion.solana.top_volume.TopVolumeIngestor` every 4 hours,
    so the "latest" rank is always at most ~4h stale.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    # ------------------------------------------------------------------
    # Latest rank lookups
    # ------------------------------------------------------------------
    def get_latest_rank(self, mint: str) -> UniverseRank | None:
        if not mint:
            return None
        sql = text(
            """
            SELECT mint, rank, volume_24h_usd, snapshot_at
            FROM solana_token_universe
            WHERE mint = :m
            ORDER BY snapshot_at DESC
            LIMIT 1
            """
        )
        try:
            with self.engine.connect() as conn:
                row = conn.execute(sql, {"m": mint}).fetchone()
        except Exception as exc:  # noqa: BLE001
            log.warning("UniverseRegistry.get_latest_rank failed: {e}", e=str(exc))
            return None
        if row is None:
            return None
        return UniverseRank(
            mint=row[0],
            rank=int(row[1]),
            volume_24h_usd=float(row[2] or 0.0),
            snapshot_at=row[3],
        )

    def get_latest_snapshot(self, limit: int = 250) -> list[UniverseRank]:
        """Return the latest snapshot rows, best-ranked first."""
        sql = text(
            """
            WITH latest AS (
                SELECT MAX(snapshot_at) AS ts FROM solana_token_universe
            )
            SELECT mint, rank, volume_24h_usd, snapshot_at
            FROM solana_token_universe
            WHERE snapshot_at = (SELECT ts FROM latest)
            ORDER BY rank ASC
            LIMIT :lim
            """
        )
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(sql, {"lim": limit}).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.warning("UniverseRegistry.get_latest_snapshot failed: {e}", e=str(exc))
            return []
        return [
            UniverseRank(
                mint=r[0],
                rank=int(r[1]),
                volume_24h_usd=float(r[2] or 0.0),
                snapshot_at=r[3],
            )
            for r in rows
        ]


# ----------------------------------------------------------------------
# Score function — pure, exposed so tests can exercise the curve
# ----------------------------------------------------------------------
def rank_to_score(rank: int | None, limit: int = 250) -> float:
    """Map a universe rank to a cross-ref contribution in ``[0, 1]``.

    Curve: logarithmic so the top of the book is worth a lot more than
    the tail. ``rank=1`` returns 1.0, ``rank=limit`` returns ~0.0, and
    a missing rank returns 0.0.

    Implementation uses ``1 - log(rank) / log(limit)`` so rank=10 with
    limit=250 gives ~0.58, rank=50 gives ~0.29, rank=250 gives 0.0.
    """
    if rank is None or rank < 1 or limit <= 1:
        return 0.0
    if rank >= limit:
        return 0.0
    return max(0.0, 1.0 - math.log(rank) / math.log(limit))
