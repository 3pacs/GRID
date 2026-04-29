"""
Curated smart-money wallet registry.

Hand-seeded list of wallets whose purchases carry signal — early buyers
of multiple past winners, known deployer accumulation accounts, anon
devs with on-chain track records. The registry is intentionally small
(dozens, not thousands) and operator-maintained.

It answers one question for the cross-referencer:
  *Of the wallets that bought this mint in the first N seconds, how
  many are on our smart-money list — and what's their combined trust?*
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ----------------------------------------------------------------------
# DTOs
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class SmartMoneyWallet:
    wallet: str
    label: str
    source: str  # "nansen" | "birdeye_top" | "operator" | ...
    trust: float  # [0, 1]
    notes: str | None = None
    active: bool = True


@dataclass(frozen=True)
class SmartMoneyMatch:
    wallet: str
    label: str
    trust: float
    source: str


@dataclass(frozen=True)
class SmartMoneyMatchSet:
    """Result of matching a set of candidate wallets against the registry."""

    matches: tuple[SmartMoneyMatch, ...]

    @property
    def count(self) -> int:
        return len(self.matches)

    @property
    def combined_trust(self) -> float:
        """Diminishing returns across multiple hits.

        Three trustworthy hits is meaningfully better than one, but the
        fourth matters less than the second. We use ``1 - Π(1 - t_i)``
        which is the classic independent-source trust aggregation.
        """
        if not self.matches:
            return 0.0
        product = 1.0
        for m in self.matches:
            product *= 1.0 - max(0.0, min(1.0, m.trust))
        return 1.0 - product


# ----------------------------------------------------------------------
# Registry
# ----------------------------------------------------------------------
class SmartMoneyRegistry:
    """CRUD + matching over a curated smart-money wallet list."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS solana_smart_money (
                        wallet        TEXT PRIMARY KEY,
                        label         TEXT NOT NULL,
                        source        TEXT NOT NULL DEFAULT 'operator',
                        trust         FLOAT NOT NULL DEFAULT 0.5
                                      CHECK (trust >= 0 AND trust <= 1),
                        notes         TEXT,
                        active        BOOLEAN NOT NULL DEFAULT TRUE,
                        added_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        last_hit_at   TIMESTAMPTZ,
                        hit_count     INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
            )

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------
    def upsert(self, wallet: SmartMoneyWallet) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO solana_smart_money
                        (wallet, label, source, trust, notes, active, updated_at)
                    VALUES
                        (:w, :l, :s, :t, :n, :a, NOW())
                    ON CONFLICT (wallet) DO UPDATE SET
                        label = EXCLUDED.label,
                        source = EXCLUDED.source,
                        trust = EXCLUDED.trust,
                        notes = EXCLUDED.notes,
                        active = EXCLUDED.active,
                        updated_at = NOW()
                    """
                ),
                {
                    "w": wallet.wallet,
                    "l": wallet.label,
                    "s": wallet.source,
                    "t": wallet.trust,
                    "n": wallet.notes,
                    "a": wallet.active,
                },
            )

    def ensure_seed(self, wallets: Iterable[SmartMoneyWallet]) -> int:
        """Insert seed wallets, skipping any that already exist."""
        n = 0
        with self.engine.begin() as conn:
            for w in wallets:
                result = conn.execute(
                    text(
                        """
                        INSERT INTO solana_smart_money
                            (wallet, label, source, trust, notes, active)
                        VALUES
                            (:w, :l, :s, :t, :n, :a)
                        ON CONFLICT (wallet) DO NOTHING
                        RETURNING wallet
                        """
                    ),
                    {
                        "w": w.wallet,
                        "l": w.label,
                        "s": w.source,
                        "t": w.trust,
                        "n": w.notes,
                        "a": w.active,
                    },
                )
                if result.fetchone():
                    n += 1
        log.info("SmartMoneyRegistry: seeded {n} wallets", n=n)
        return n

    def deactivate(self, wallet: str) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE solana_smart_money SET active = FALSE, "
                    "updated_at = NOW() WHERE wallet = :w"
                ),
                {"w": wallet},
            )

    def record_hit(self, wallet: str, now: datetime) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE solana_smart_money SET "
                    "hit_count = hit_count + 1, last_hit_at = :now "
                    "WHERE wallet = :w"
                ),
                {"w": wallet, "now": now},
            )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------
    def get(self, wallet: str) -> SmartMoneyWallet | None:
        sql = text(
            "SELECT wallet, label, source, trust, notes, active "
            "FROM solana_smart_money WHERE wallet = :w"
        )
        with self.engine.connect() as conn:
            row = conn.execute(sql, {"w": wallet}).fetchone()
        if row is None:
            return None
        return SmartMoneyWallet(
            wallet=row[0],
            label=row[1],
            source=row[2],
            trust=float(row[3] or 0.0),
            notes=row[4],
            active=bool(row[5]),
        )

    def list_active(self) -> list[SmartMoneyWallet]:
        sql = text(
            "SELECT wallet, label, source, trust, notes, active "
            "FROM solana_smart_money WHERE active = TRUE "
            "ORDER BY trust DESC, wallet"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql).fetchall()
        return [
            SmartMoneyWallet(
                wallet=r[0],
                label=r[1],
                source=r[2],
                trust=float(r[3] or 0.0),
                notes=r[4],
                active=bool(r[5]),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Matching
    # ------------------------------------------------------------------
    def match_early_buyers(
        self,
        early_buyers: Iterable[str],
    ) -> SmartMoneyMatchSet:
        """Return the subset of ``early_buyers`` that are on the active list.

        Accepts any iterable of wallet addresses. Order of the match
        list mirrors the order of ``early_buyers`` so callers can trace
        which buy triggered which match.
        """
        buyers = [b for b in early_buyers if b]
        if not buyers:
            return SmartMoneyMatchSet(matches=())

        # Single query with ANY() to avoid N round-trips.
        sql = text(
            "SELECT wallet, label, source, trust FROM solana_smart_money "
            "WHERE active = TRUE AND wallet = ANY(:ws)"
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"ws": buyers}).fetchall()

        by_wallet = {
            r[0]: SmartMoneyMatch(
                wallet=r[0],
                label=r[1],
                trust=float(r[3] or 0.0),
                source=r[2],
            )
            for r in rows
        }
        matches = tuple(by_wallet[b] for b in buyers if b in by_wallet)
        return SmartMoneyMatchSet(matches=matches)
