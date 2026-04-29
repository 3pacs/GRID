"""
Cross-referencer — composite confidence scoring for new Solana launches.

Takes a :class:`LaunchEvent` (or any ``launch``-shaped dataclass) plus
whatever signal sources the operator has wired up, and returns a
:class:`CrossRefReport` with a numeric composite score in ``[0, 1]``
plus a human-readable list of contributing reasons.

Signal sources consumed:
  * ``DeployerRegistry`` — who launched this? What's their track record?
  * ``SmartMoneyRegistry`` — did any curated smart-money wallet buy
    within the early-buy window?
  * ``NarrativeRegistry`` — does the token's name/symbol match any
    operator-curated narrative term currently in play?
  * ``TrustConvergence`` — GRID's existing ``trust_scorer.detect_convergence``
    for cross-signal aggregation.

Any source can be ``None`` — the composite degrades gracefully, only
summing over the weights whose sources were actually consulted, so the
score stays in ``[0, 1]``.

**The composite is deterministic.** No RNG, no LLM — everything here
runs on the hot path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from loguru import logger as log

from trading.solana.deployer_registry import DeployerRegistry, DeployerScoreResult
from trading.solana.smart_money import SmartMoneyMatchSet, SmartMoneyRegistry
from trading.solana.universe import UniverseRank, UniverseRankSource, rank_to_score


# ----------------------------------------------------------------------
# Weights
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class CrossRefWeights:
    deployer: float = 0.40
    smart_money: float = 0.25
    narrative: float = 0.15
    convergence: float = 0.10
    universe: float = 0.10

    def sum_active(
        self,
        *,
        deployer: bool,
        smart_money: bool,
        narrative: bool,
        convergence: bool,
        universe: bool = False,
    ) -> float:
        total = 0.0
        if deployer:
            total += self.deployer
        if smart_money:
            total += self.smart_money
        if narrative:
            total += self.narrative
        if convergence:
            total += self.convergence
        if universe:
            total += self.universe
        return total


DEFAULT_CROSS_REF_WEIGHTS = CrossRefWeights()


# ----------------------------------------------------------------------
# Input DTO — deliberately minimal so launch_monitor / fast_entry can
# share it without circular imports
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class LaunchEvent:
    """A single new-token event to be scored.

    Fields beyond ``mint`` are all optional — the cross-referencer
    handles missing data by just not running that check.
    """

    mint: str
    deployer: str | None = None
    symbol: str | None = None
    name: str | None = None
    early_buyers: tuple[str, ...] = ()
    initial_liquidity_usd: float | None = None
    observed_at: Any = None  # datetime; Any to avoid imports
    source: str = "unknown"
    pool_address: str | None = None
    raw: dict[str, Any] = field(default_factory=dict)


# ----------------------------------------------------------------------
# Narrative registry — operator-curated memecoin themes
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class NarrativeHit:
    term: str
    weight: float
    matched_field: str  # 'symbol' | 'name'


class NarrativeRegistry:
    """In-memory registry of operator-suggested narrative terms.

    The intent is for humans to inject terms they believe will break
    through the noise — e.g. a meme that's trending on Twitter, a
    topical theme ("election", "ai16z"), or a specific token family.
    Terms can be added and removed at runtime; the cross-referencer
    picks the latest state on every call.

    Matching rules:
      * case-insensitive substring match
      * either the token's symbol or its name must contain the term
      * weight is accumulated across all hits, clipped to [0, 1]
    """

    def __init__(self) -> None:
        self._terms: dict[str, float] = {}

    def add(self, term: str, weight: float = 1.0) -> None:
        term_clean = term.strip().lower()
        if not term_clean:
            return
        self._terms[term_clean] = max(0.0, min(1.0, weight))
        log.info("NarrativeRegistry +{t} (w={w:.2f})", t=term_clean, w=weight)

    def remove(self, term: str) -> None:
        self._terms.pop(term.strip().lower(), None)

    def clear(self) -> None:
        self._terms.clear()

    def list_terms(self) -> list[tuple[str, float]]:
        return sorted(self._terms.items(), key=lambda kv: -kv[1])

    def match(self, symbol: str | None, name: str | None) -> list[NarrativeHit]:
        hits: list[NarrativeHit] = []
        if not self._terms:
            return hits
        symbol_l = (symbol or "").lower()
        name_l = (name or "").lower()
        for term, weight in self._terms.items():
            if symbol_l and term in symbol_l:
                hits.append(
                    NarrativeHit(term=term, weight=weight, matched_field="symbol")
                )
            elif name_l and term in name_l:
                hits.append(
                    NarrativeHit(term=term, weight=weight, matched_field="name")
                )
        return hits


# ----------------------------------------------------------------------
# Trust-scorer adapter — lets us depend only on a protocol so tests
# don't need the full GRID intelligence layer
# ----------------------------------------------------------------------
class ConvergenceProvider(Protocol):
    def detect(self, mint: str) -> float: ...


# ----------------------------------------------------------------------
# Output DTO
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class CrossRefReport:
    mint: str
    composite_score: float
    deployer_score: float
    smart_money_hits: int
    smart_money_trust: float
    narrative_weight: float
    convergence_score: float
    deployer_result: DeployerScoreResult | None
    smart_money_matches: SmartMoneyMatchSet
    narrative_hits: tuple[NarrativeHit, ...]
    reasons: tuple[str, ...]
    weights: CrossRefWeights
    universe_score: float = 0.0
    universe_rank: UniverseRank | None = None

    @property
    def actionable(self) -> bool:
        """True when at least one source contributed AND score > 0."""
        return self.composite_score > 0.0


# ----------------------------------------------------------------------
# Cross-referencer
# ----------------------------------------------------------------------
class CrossReferencer:
    """Composite scorer — the only place launch decisions are made."""

    def __init__(
        self,
        deployer_registry: DeployerRegistry | None = None,
        smart_money: SmartMoneyRegistry | None = None,
        narratives: NarrativeRegistry | None = None,
        convergence: ConvergenceProvider | None = None,
        universe: UniverseRankSource | None = None,
        weights: CrossRefWeights = DEFAULT_CROSS_REF_WEIGHTS,
        universe_limit: int = 250,
    ) -> None:
        self.deployer_registry = deployer_registry
        self.smart_money = smart_money
        self.narratives = narratives
        self.convergence = convergence
        self.universe = universe
        self.weights = weights
        self.universe_limit = universe_limit

    def evaluate(self, launch: LaunchEvent) -> CrossRefReport:
        reasons: list[str] = []

        # ---- Deployer score ------------------------------------------
        deployer_result: DeployerScoreResult | None = None
        deployer_score = 0.0
        deployer_active = False
        if self.deployer_registry is not None and launch.deployer:
            try:
                deployer_result = self.deployer_registry.get(launch.deployer)
            except Exception as exc:  # noqa: BLE001 — hot-path guard
                log.warning(
                    "Deployer lookup failed for {w}: {e}",
                    w=launch.deployer, e=str(exc),
                )
            if deployer_result is not None:
                deployer_score = deployer_result.score
                deployer_active = True
                reasons.append(
                    f"deployer {launch.deployer[:8]}: "
                    f"score={deployer_score:.2f} n={deployer_result.stats.n_launches}"
                )

        # ---- Smart money ---------------------------------------------
        smart_money_matches = SmartMoneyMatchSet(matches=())
        smart_money_norm = 0.0
        smart_money_active = False
        if self.smart_money is not None and launch.early_buyers:
            try:
                smart_money_matches = self.smart_money.match_early_buyers(
                    launch.early_buyers
                )
            except Exception as exc:  # noqa: BLE001 — hot-path guard
                log.warning("Smart money match failed: {e}", e=str(exc))
            if smart_money_matches.count > 0:
                smart_money_norm = smart_money_matches.combined_trust
                smart_money_active = True
                reasons.append(
                    f"{smart_money_matches.count} smart-money wallet(s) "
                    f"bought early (trust={smart_money_norm:.2f})"
                )
            elif launch.early_buyers:
                # Ran the check, got nothing → still counts as active so
                # the composite denominator includes smart money.
                smart_money_active = True

        # ---- Narrative -----------------------------------------------
        narrative_hits: tuple[NarrativeHit, ...] = ()
        narrative_weight = 0.0
        narrative_active = False
        if self.narratives is not None:
            hits = self.narratives.match(launch.symbol, launch.name)
            narrative_hits = tuple(hits)
            narrative_active = True
            if hits:
                narrative_weight = min(1.0, sum(h.weight for h in hits))
                top = ", ".join(h.term for h in hits[:3])
                reasons.append(
                    f"narrative match: {top} (w={narrative_weight:.2f})"
                )

        # ---- Convergence ---------------------------------------------
        convergence_score = 0.0
        convergence_active = False
        if self.convergence is not None:
            try:
                convergence_score = max(0.0, min(1.0, self.convergence.detect(launch.mint)))
                convergence_active = True
                if convergence_score > 0:
                    reasons.append(
                        f"trust_scorer convergence={convergence_score:.2f}"
                    )
            except Exception as exc:  # noqa: BLE001 — hot-path guard
                log.warning("Convergence provider failed: {e}", e=str(exc))

        # ---- Universe (top-N volume membership) ----------------------
        universe_score = 0.0
        universe_rank: UniverseRank | None = None
        universe_active = False
        if self.universe is not None:
            try:
                universe_rank = self.universe.get_latest_rank(launch.mint)
                universe_active = True
                if universe_rank is not None:
                    universe_score = rank_to_score(
                        universe_rank.rank, limit=self.universe_limit
                    )
                    reasons.append(
                        f"universe rank #{universe_rank.rank} "
                        f"(score={universe_score:.2f})"
                    )
            except Exception as exc:  # noqa: BLE001 — hot-path guard
                log.warning("Universe lookup failed: {e}", e=str(exc))

        # ---- Composite -----------------------------------------------
        w = self.weights
        raw = (
            (w.deployer * deployer_score if deployer_active else 0.0)
            + (w.smart_money * smart_money_norm if smart_money_active else 0.0)
            + (w.narrative * narrative_weight if narrative_active else 0.0)
            + (w.convergence * convergence_score if convergence_active else 0.0)
            + (w.universe * universe_score if universe_active else 0.0)
        )
        denom = w.sum_active(
            deployer=deployer_active,
            smart_money=smart_money_active,
            narrative=narrative_active,
            convergence=convergence_active,
            universe=universe_active,
        )
        composite = raw / denom if denom > 0 else 0.0
        composite = max(0.0, min(1.0, composite))

        if not reasons:
            reasons.append("no matching sources")

        return CrossRefReport(
            mint=launch.mint,
            composite_score=composite,
            deployer_score=deployer_score,
            smart_money_hits=smart_money_matches.count,
            smart_money_trust=smart_money_norm,
            narrative_weight=narrative_weight,
            convergence_score=convergence_score,
            universe_score=universe_score,
            universe_rank=universe_rank,
            deployer_result=deployer_result,
            smart_money_matches=smart_money_matches,
            narrative_hits=narrative_hits,
            reasons=tuple(reasons),
            weights=w,
        )
