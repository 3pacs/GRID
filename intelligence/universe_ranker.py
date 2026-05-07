"""
Cross-ticker conviction ranker.

Runs the full decision gateway over a ticker universe (SP500,
NASDAQ100, custom list), ranks by composite score, surfaces top-K
calls + distribution stats. The distribution IS the calibration — if
50 of 500 names are HIGH, the market is trending and conviction is
correlated; if 2 of 500 are HIGH, those 2 are the real trades.

The morning tool
----------------

``should_i_trade(engine, ticker, ...)`` is the single-ticker capstone.
An operator hunting the day's best setup has to call it 500 times by
hand. ``rank_universe`` fans that call across a ticker universe
(defensively — one ticker blowing up never kills the sweep),
aggregates into a ``UniverseRankingReport``, and emits:

  1. Top-K names by composite score (conviction × robustness-weighted)
  2. Distribution stats (high / medium / low / no_trade counts per sector)
  3. Concentration alerts — if > 30 % of HIGH verdicts are in a single
     sector, the regime is driving the calls, not ticker-specific edge
  4. Regime signature — 'trending' (>20 % HIGH), 'divergent' (<5 %),
     'mixed' otherwise
  5. A composed narrative string that names the top-3 tickers and the
     regime signature

Design rules
------------

* ``should_i_trade`` is IMPORTED from ``intelligence.decision_gateway``
  and NEVER reimplemented.  Sector lookups reuse
  ``intelligence.sector_networks.loader``.
* Every engine-touching function is try/except-wrapped.
  ``rank_universe`` never raises — a full failure returns an empty
  report with the failure reason in the narrative.
* Imports of downstream modules are LAZY (inside each function) so a
  missing optional dependency cannot break the module at import time.
* Pure helpers (composite_score, classify_regime_signature,
  detect_sector_concentration, rank_tickers, build_narrative) take no
  engine and have no I/O so they're trivially unit-testable.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────


DEFAULT_TOP_K: int = 20

# Regime signature cutoffs (pct of succeeded tickers with verdict='high')
TRENDING_HIGH_FRACTION: float = 0.20   # > 20% → trending
DIVERGENT_HIGH_FRACTION: float = 0.05  # < 5%  → divergent

# Single sector holding > threshold fraction of all HIGH verdicts → alert
SECTOR_CONCENTRATION_ALERT_THRESHOLD: float = 0.30

# Composite score bounds — matches provenance.aggregate_conviction clamp
COMPOSITE_SCORE_MIN: float = 0.0
COMPOSITE_SCORE_MAX: float = 1.5

# Parallel fan-out — I/O bound decision_gateway calls
MAX_PARALLEL_WORKERS: int = 8


# ── Verdict ordering ─────────────────────────────────────────────────────


_VERDICT_ORDER: tuple[str, ...] = ("no_trade", "low", "medium", "high")
_RANKABLE_VERDICTS: frozenset[str] = frozenset({"high", "medium"})


# ── Universe constants ────────────────────────────────────────────────────

# V1 cold-start: the top ~100 SP500 names by market cap. Full 500 is
# deferred until we have a live constituent feed — the comment below
# documents the gap so no one forgets. Tickers are a frozen tuple so
# they can't be mutated at runtime.
#
# Sourced from the top SP500 weights as of early 2026; ordering roughly
# reflects index weight. Duplicates removed, alphabetical within tiers
# omitted for readability — the ranker only cares about set membership.
UNIVERSE_SP500: tuple[str, ...] = (
    # Mega-cap tech
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA",
    "AVGO", "ORCL", "CRM", "ADBE", "NFLX", "AMD", "INTC", "CSCO",
    "QCOM", "TXN", "IBM", "NOW", "INTU", "AMAT", "MU", "ADI",
    "LRCX", "KLAC", "PANW", "CRWD", "SNPS", "CDNS",
    # Consumer
    "WMT", "COST", "HD", "MCD", "NKE", "SBUX", "TGT", "LOW",
    "TJX", "BKNG", "DIS", "CMCSA",
    # Financials
    "BRK.B", "JPM", "V", "MA", "BAC", "WFC", "GS", "MS",
    "C", "AXP", "SCHW", "BLK", "SPGI", "MMC", "CME", "ICE",
    "PGR", "AFL", "USB", "PNC",
    # Healthcare
    "UNH", "LLY", "JNJ", "ABBV", "MRK", "PFE", "TMO", "ABT",
    "DHR", "BMY", "AMGN", "CVS", "GILD", "ELV", "ISRG", "REGN",
    "VRTX", "MDT", "SYK",
    # Industrial
    "GE", "CAT", "RTX", "BA", "HON", "UNP", "LMT", "DE",
    "UPS", "ADP", "ETN", "MMM",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG", "PSX", "MPC", "OXY",
    # Staples & other
    "PG", "KO", "PEP", "PM", "MO", "MDLZ", "CL",
    # Communication & misc
    "T", "VZ", "TMUS", "CHTR",
    # Utilities
    "NEE", "SO", "DUK", "CEG",
)
# Gap: full SP500 is ~503 tickers. UNIVERSE_SP500 is a 100-name cold
# start. Extend when a live constituent feed is available (see
# ``scripts/update_sp500_universe.py`` — TODO).


UNIVERSE_NASDAQ100: tuple[str, ...] = (
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA",
    "AVGO", "COST", "NFLX", "ADBE", "PEP", "CSCO", "AMD", "TMUS",
    "INTC", "INTU", "QCOM", "TXN", "AMAT", "AMGN", "CMCSA", "HON",
    "ISRG", "BKNG", "VRTX", "ADP", "GILD", "SBUX", "REGN", "MDLZ",
    "LRCX", "PANW", "ADI", "KLAC", "SNPS", "CDNS", "MU", "MELI",
    "ASML", "CRWD", "CTAS", "ORLY", "MAR", "ABNB", "MNST", "FTNT",
    "PYPL", "CHTR", "MRVL", "ADSK", "PDD", "NXPI", "WDAY", "PCAR",
    "ROST", "LULU", "DXCM", "KDP", "CEG", "EA", "KHC", "ODFL",
    "FAST", "IDXX", "GEHC", "CSGP", "CPRT", "EXC", "AEP", "VRSK",
    "BKR", "CTSH", "XEL", "DDOG", "ON", "MCHP", "AZN", "FANG",
    "CDW", "TEAM", "TTD", "ZS", "ILMN", "ANSS", "BIIB", "MDB",
    "DLTR", "ZM", "SIRI", "LCID", "WBA", "ALGN", "ENPH", "JD",
    "SGEN", "MRNA", "SPLK", "VRSN",
)


_NAMED_UNIVERSES: dict[str, tuple[str, ...]] = {
    "SP500": UNIVERSE_SP500,
    "NASDAQ100": UNIVERSE_NASDAQ100,
}


# ── Dataclasses ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TickerRanking:
    """One row in the ranking. Always populated even on failure —
    failed runs land with verdict='no_trade' and a populated error.
    """

    ticker: str
    sector: str | None
    verdict: str
    aggregate_conviction: float
    robustness_score: float
    robustness_label: str | None
    composite_score: float
    has_ticket: bool
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "sector": self.sector,
            "verdict": self.verdict,
            "aggregate_conviction": round(self.aggregate_conviction, 4),
            "robustness_score": round(self.robustness_score, 4),
            "robustness_label": self.robustness_label,
            "composite_score": round(self.composite_score, 4),
            "has_ticket": self.has_ticket,
            "error": self.error,
        }


@dataclass(frozen=True)
class SectorDistribution:
    """Per-sector verdict tallies. ``concentrated`` flips when the
    sector holds more than ``SECTOR_CONCENTRATION_ALERT_THRESHOLD`` of
    all HIGH verdicts across the universe.
    """

    sector: str
    high_count: int
    medium_count: int
    low_count: int
    no_trade_count: int
    high_pct: float
    concentrated: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "sector": self.sector,
            "high_count": self.high_count,
            "medium_count": self.medium_count,
            "low_count": self.low_count,
            "no_trade_count": self.no_trade_count,
            "high_pct": round(self.high_pct, 4),
            "concentrated": self.concentrated,
        }


@dataclass(frozen=True)
class UniverseRankingReport:
    """The full ranking report handed back to the operator."""

    universe_name: str
    tickers_attempted: int
    tickers_succeeded: int
    top_k: list[TickerRanking]
    all_rankings: list[TickerRanking]
    sector_distributions: list[SectorDistribution]
    concentration_alerts: list[str]
    regime_signature: str
    narrative: str
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "universe_name": self.universe_name,
            "tickers_attempted": self.tickers_attempted,
            "tickers_succeeded": self.tickers_succeeded,
            "top_k": [r.to_dict() for r in self.top_k],
            "all_rankings": [r.to_dict() for r in self.all_rankings],
            "sector_distributions": [
                d.to_dict() for d in self.sector_distributions
            ],
            "concentration_alerts": list(self.concentration_alerts),
            "regime_signature": self.regime_signature,
            "narrative": self.narrative,
            "generated_at": self.generated_at,
        }


# ── Pure helpers ─────────────────────────────────────────────────────────


def composite_score(
    aggregate_conviction: float,
    robustness_score: float,
) -> float:
    """Robustness-weighted conviction.

    ``score = aggregate_conviction × (0.5 + 0.5 × robustness_score)``

    A stress-robust conviction of 1.2 (1.2 × 1.0 = 1.2) beats a fragile
    1.4 (1.4 × 0.5 = 0.7). Robustness of 0 halves the conviction,
    robustness of 1 preserves it. Result is clamped to
    [COMPOSITE_SCORE_MIN, COMPOSITE_SCORE_MAX] so downstream Kelly
    sizing stays bounded.
    """
    try:
        conviction = float(aggregate_conviction)
    except (TypeError, ValueError):
        conviction = 0.0
    try:
        robustness = float(robustness_score)
    except (TypeError, ValueError):
        robustness = 0.0

    # Clamp robustness to [0, 1] before weighting
    robustness = max(0.0, min(1.0, robustness))
    weighted = conviction * (0.5 + 0.5 * robustness)
    return max(COMPOSITE_SCORE_MIN, min(COMPOSITE_SCORE_MAX, weighted))


def classify_regime_signature(report_stats: dict[str, int | float]) -> str:
    """Turn aggregate high/total counts into a regime label.

    >20% HIGH → 'trending' (many names agree → correlated overconfidence risk)
    <5% HIGH  → 'divergent' (few names agree → any HIGH is real edge)
    else      → 'mixed'

    ``report_stats`` must expose ``high_count`` and ``total_count``.
    Missing keys or total==0 → 'mixed' (safe default).
    """
    total = int(report_stats.get("total_count", 0) or 0)
    high = int(report_stats.get("high_count", 0) or 0)
    if total <= 0:
        return "mixed"
    fraction = high / total
    if fraction > TRENDING_HIGH_FRACTION:
        return "trending"
    if fraction < DIVERGENT_HIGH_FRACTION:
        return "divergent"
    return "mixed"


def detect_sector_concentration(
    sector_distributions: Sequence[SectorDistribution],
    total_high: int,
) -> list[str]:
    """Return a list of human-readable alert strings for any sector
    holding more than ``SECTOR_CONCENTRATION_ALERT_THRESHOLD`` of the
    total HIGH verdicts across the universe.

    Concentration means the regime is driving the call, not
    ticker-specific edge — the operator should treat the crowded sector
    as a correlated bet, not N independent trades.
    """
    alerts: list[str] = []
    if total_high <= 0:
        return alerts
    for dist in sector_distributions:
        if dist.high_count <= 0:
            continue
        share = dist.high_count / total_high
        if share > SECTOR_CONCENTRATION_ALERT_THRESHOLD:
            alerts.append(
                f"sector '{dist.sector}' holds {share:.0%} of HIGH "
                f"verdicts ({dist.high_count}/{total_high}) — regime-driven, "
                f"treat as correlated exposure"
            )
    return alerts


def rank_tickers(
    rankings: Sequence[TickerRanking],
    k: int = DEFAULT_TOP_K,
) -> list[TickerRanking]:
    """Filter to actionable verdicts, sort desc by composite score, cap at k.

    - Drops verdict='no_trade' and verdict='low' (operator only wants
      tradeable setups in the top-K).
    - Stable sort by ``(-composite_score, -aggregate_conviction)`` so
      ties break on raw conviction.
    - ``k <= 0`` returns an empty list.
    """
    if k <= 0:
        return []
    filtered = [r for r in rankings if r.verdict in _RANKABLE_VERDICTS]
    filtered.sort(
        key=lambda r: (-r.composite_score, -r.aggregate_conviction, r.ticker),
    )
    return filtered[:k]


def build_narrative(report: "UniverseRankingReport") -> str:
    """Compose a one-paragraph summary string.

    Names the regime signature, the top-3 tickers with their verdicts,
    and mentions any concentration alerts. Pure formatting, no I/O.
    """
    if report.tickers_attempted == 0:
        return (
            f"Universe '{report.universe_name}': no tickers supplied — "
            "nothing to rank."
        )
    if not report.top_k:
        return (
            f"Universe '{report.universe_name}': "
            f"{report.tickers_succeeded}/{report.tickers_attempted} "
            f"tickers scored, regime={report.regime_signature}, "
            "no high/medium verdicts found — stand down."
        )

    top_mentions = ", ".join(
        f"{r.ticker}({r.verdict}:{r.composite_score:.2f})"
        for r in report.top_k[:3]
    )
    alert_tail = ""
    if report.concentration_alerts:
        alert_tail = (
            f" Concentration alerts: {len(report.concentration_alerts)}."
        )

    return (
        f"Universe '{report.universe_name}': "
        f"{report.tickers_succeeded}/{report.tickers_attempted} scored, "
        f"regime={report.regime_signature}, "
        f"top: {top_mentions}.{alert_tail}"
    )


# ── Sector lookup (reuses sector_networks loader) ────────────────────────


def _get_ticker_sector(ticker: str) -> str | None:
    """Resolve a ticker → sector label using the canonical YAML loader.

    Reuses ``intelligence.sector_networks.loader`` — does NOT
    reimplement sector mapping. Returns ``None`` when the ticker is
    unknown, the loader is unavailable, or the lookup fails.

    Import is lazy so the universe_ranker module can be imported in
    environments where the loader's YAML files are missing (tests).
    """
    if not ticker:
        return None
    try:
        from intelligence.sector_networks.loader import (
            get_actors,
            list_sectors,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("universe_ranker: sector loader import failed: {e}", e=exc)
        return None

    needle = ticker.upper().strip()
    try:
        for sector in list_sectors():
            try:
                actors = get_actors(sector)
            except Exception:  # noqa: BLE001
                continue
            for actor in actors:
                if not isinstance(actor, dict):
                    continue
                actor_ticker = (actor.get("ticker") or "").upper().strip()
                if actor_ticker and actor_ticker == needle:
                    return sector
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "universe_ranker: sector lookup failed for {t}: {e}",
            t=ticker, e=exc,
        )
        return None
    return None


# ── Per-ticker runner (wraps should_i_trade) ─────────────────────────────


def _extract_float(obj: Any, attr: str, default: float = 0.0) -> float:
    val = getattr(obj, attr, None)
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _run_one_ticker(
    engine: Engine,
    ticker: str,
    *,
    account_size_usd: float,
) -> TickerRanking:
    """Call ``should_i_trade`` for a single ticker defensively.

    Any exception becomes a ``TickerRanking`` with verdict='no_trade',
    composite_score=0, and ``error`` populated. Never raises.

    The import of ``should_i_trade`` is lazy — this function reuses
    the capstone from ``intelligence.decision_gateway`` and NEVER
    reimplements the decision stack.
    """
    sector = _get_ticker_sector(ticker)
    try:
        from intelligence.decision_gateway import should_i_trade
    except Exception as exc:  # noqa: BLE001
        return TickerRanking(
            ticker=ticker.upper(),
            sector=sector,
            verdict="no_trade",
            aggregate_conviction=0.0,
            robustness_score=0.0,
            robustness_label=None,
            composite_score=0.0,
            has_ticket=False,
            error=f"decision_gateway import failed: {exc}",
        )

    try:
        response = should_i_trade(
            engine,
            ticker,
            account_size_usd=account_size_usd,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug(
            "universe_ranker: should_i_trade({t}) failed: {e}",
            t=ticker, e=exc,
        )
        return TickerRanking(
            ticker=ticker.upper(),
            sector=sector,
            verdict="no_trade",
            aggregate_conviction=0.0,
            robustness_score=0.0,
            robustness_label=None,
            composite_score=0.0,
            has_ticket=False,
            error=f"should_i_trade raised: {exc}",
        )

    verdict = getattr(response, "unified_verdict", "no_trade") or "no_trade"
    if verdict not in _VERDICT_ORDER:
        verdict = "no_trade"

    prov = getattr(response, "provenance_report", None)
    stress = getattr(response, "stress_report", None)
    ticket = getattr(response, "trade_ticket", None)

    aggregate_conviction = _extract_float(prov, "aggregate_conviction", 0.0)
    robustness_score = _extract_float(stress, "robustness_score", 0.0)
    robustness_label = getattr(stress, "robustness_label", None)

    score = composite_score(aggregate_conviction, robustness_score)

    return TickerRanking(
        ticker=ticker.upper(),
        sector=sector,
        verdict=verdict,
        aggregate_conviction=aggregate_conviction,
        robustness_score=robustness_score,
        robustness_label=robustness_label,
        composite_score=score,
        has_ticket=ticket is not None,
        error=None,
    )


# ── Distribution aggregation ─────────────────────────────────────────────


def _build_sector_distributions(
    rankings: Sequence[TickerRanking],
) -> tuple[list[SectorDistribution], int]:
    """Bucket rankings by sector and tally verdict counts.

    Returns (distributions, total_high_across_all_sectors).
    Unknown sectors bucket under '__unknown__'.
    """
    buckets: dict[str, dict[str, int]] = {}
    total_high = 0
    for r in rankings:
        sector = r.sector or "__unknown__"
        b = buckets.setdefault(
            sector,
            {"high": 0, "medium": 0, "low": 0, "no_trade": 0},
        )
        if r.verdict == "high":
            b["high"] += 1
            total_high += 1
        elif r.verdict == "medium":
            b["medium"] += 1
        elif r.verdict == "low":
            b["low"] += 1
        else:
            b["no_trade"] += 1

    dists: list[SectorDistribution] = []
    for sector, tally in sorted(buckets.items()):
        sector_total = (
            tally["high"] + tally["medium"] + tally["low"] + tally["no_trade"]
        )
        high_pct = (tally["high"] / sector_total) if sector_total > 0 else 0.0
        concentrated = False
        if total_high > 0:
            concentrated = (
                tally["high"] / total_high
            ) > SECTOR_CONCENTRATION_ALERT_THRESHOLD
        dists.append(
            SectorDistribution(
                sector=sector,
                high_count=tally["high"],
                medium_count=tally["medium"],
                low_count=tally["low"],
                no_trade_count=tally["no_trade"],
                high_pct=high_pct,
                concentrated=concentrated,
            )
        )
    return dists, total_high


# ── Main entry point ─────────────────────────────────────────────────────


def _resolve_universe(universe: Sequence[str] | str) -> tuple[str, list[str]]:
    """Turn a universe spec into (name, ticker_list).

    String names resolve via ``_NAMED_UNIVERSES``; unknown names fall
    back to an empty list with the given name preserved. Lists pass
    through with name='custom'.
    """
    if isinstance(universe, str):
        key = universe.strip().upper()
        tickers = _NAMED_UNIVERSES.get(key, ())
        return key, list(tickers)
    return "custom", list(universe)


def rank_universe(
    engine: Engine,
    universe: Sequence[str] | str,
    *,
    account_size_usd: float = 100_000.0,
    top_k: int = DEFAULT_TOP_K,
    parallel: bool = False,
) -> UniverseRankingReport:
    """Run ``should_i_trade`` across a ticker universe and rank by
    composite score.

    Sequential by default; ``parallel=True`` fans out over a
    ``ThreadPoolExecutor`` with at most ``MAX_PARALLEL_WORKERS`` workers
    (decision_gateway stages are I/O bound so threads help). One ticker
    failing NEVER kills the sweep — failures land as ``no_trade`` rows
    with ``error`` populated, the report still returns.

    This function never raises. Catastrophic failure returns an empty
    report with the error captured in ``narrative``.
    """
    name, tickers = _resolve_universe(universe)
    attempted = len(tickers)

    if attempted == 0:
        empty_report = UniverseRankingReport(
            universe_name=name,
            tickers_attempted=0,
            tickers_succeeded=0,
            top_k=[],
            all_rankings=[],
            sector_distributions=[],
            concentration_alerts=[],
            regime_signature="mixed",
            narrative="",
        )
        return _with_narrative(empty_report)

    rankings: list[TickerRanking] = []

    try:
        if parallel and attempted > 1:
            max_workers = min(MAX_PARALLEL_WORKERS, attempted)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        _run_one_ticker,
                        engine,
                        t,
                        account_size_usd=account_size_usd,
                    ): t
                    for t in tickers
                }
                for fut in as_completed(futures):
                    try:
                        rankings.append(fut.result())
                    except Exception as exc:  # noqa: BLE001
                        ticker = futures[fut]
                        rankings.append(
                            TickerRanking(
                                ticker=ticker.upper(),
                                sector=_get_ticker_sector(ticker),
                                verdict="no_trade",
                                aggregate_conviction=0.0,
                                robustness_score=0.0,
                                robustness_label=None,
                                composite_score=0.0,
                                has_ticket=False,
                                error=f"future raised: {exc}",
                            )
                        )
        else:
            for t in tickers:
                rankings.append(
                    _run_one_ticker(
                        engine, t, account_size_usd=account_size_usd
                    )
                )
    except Exception as exc:  # noqa: BLE001
        log.debug("universe_ranker: sweep failed: {e}", e=exc)
        empty_report = UniverseRankingReport(
            universe_name=name,
            tickers_attempted=attempted,
            tickers_succeeded=0,
            top_k=[],
            all_rankings=rankings,
            sector_distributions=[],
            concentration_alerts=[],
            regime_signature="mixed",
            narrative=f"Sweep failed catastrophically: {exc}",
        )
        return empty_report

    succeeded = sum(1 for r in rankings if r.error is None)
    top = rank_tickers(rankings, k=top_k)

    sector_dists, total_high = _build_sector_distributions(rankings)
    alerts = detect_sector_concentration(sector_dists, total_high)

    regime = classify_regime_signature(
        {
            "total_count": succeeded if succeeded > 0 else attempted,
            "high_count": total_high,
        }
    )

    # Rankings sorted for readability: actionable first, then by score
    all_sorted = sorted(
        rankings,
        key=lambda r: (
            0 if r.verdict in _RANKABLE_VERDICTS else 1,
            -r.composite_score,
            r.ticker,
        ),
    )

    report = UniverseRankingReport(
        universe_name=name,
        tickers_attempted=attempted,
        tickers_succeeded=succeeded,
        top_k=top,
        all_rankings=all_sorted,
        sector_distributions=sector_dists,
        concentration_alerts=alerts,
        regime_signature=regime,
        narrative="",
    )
    return _with_narrative(report)


def _with_narrative(report: UniverseRankingReport) -> UniverseRankingReport:
    """Return a copy of ``report`` with ``narrative`` populated.

    The dataclass is frozen, so we rebuild it. Keeps build_narrative
    pure (it takes a report, returns a string).
    """
    text_ = build_narrative(report)
    return UniverseRankingReport(
        universe_name=report.universe_name,
        tickers_attempted=report.tickers_attempted,
        tickers_succeeded=report.tickers_succeeded,
        top_k=report.top_k,
        all_rankings=report.all_rankings,
        sector_distributions=report.sector_distributions,
        concentration_alerts=report.concentration_alerts,
        regime_signature=report.regime_signature,
        narrative=text_,
        generated_at=report.generated_at,
    )


# ── Persistence ──────────────────────────────────────────────────────────


def ensure_ranking_table(engine: Engine) -> None:
    """Create ``universe_ranking_history`` if missing. Idempotent.

    Any DB error is logged and swallowed — persistence must never
    break the sweep.
    """
    ddl = text(
        """
        CREATE TABLE IF NOT EXISTS universe_ranking_history (
            id               BIGSERIAL PRIMARY KEY,
            generated_at     TIMESTAMPTZ NOT NULL,
            universe_name    TEXT NOT NULL,
            tickers_attempted INTEGER NOT NULL,
            tickers_succeeded INTEGER NOT NULL,
            regime_signature TEXT NOT NULL,
            top_k            JSONB NOT NULL,
            sector_distributions JSONB NOT NULL,
            concentration_alerts JSONB NOT NULL,
            narrative        TEXT NOT NULL
        )
        """
    )
    try:
        with engine.begin() as conn:
            conn.execute(ddl)
    except Exception as exc:  # noqa: BLE001
        log.debug("universe_ranker: ensure_ranking_table failed: {e}", e=exc)


def persist_ranking(engine: Engine, report: UniverseRankingReport) -> int:
    """Insert a ranking report row. Returns inserted row id, or -1
    on any failure. Never raises.
    """
    import json

    try:
        ensure_ranking_table(engine)
    except Exception:  # noqa: BLE001
        pass

    insert_sql = text(
        """
        INSERT INTO universe_ranking_history (
            generated_at, universe_name,
            tickers_attempted, tickers_succeeded,
            regime_signature, top_k,
            sector_distributions, concentration_alerts, narrative
        ) VALUES (
            :generated_at, :universe_name,
            :tickers_attempted, :tickers_succeeded,
            :regime_signature, :top_k,
            :sector_distributions, :concentration_alerts, :narrative
        )
        RETURNING id
        """
    )

    try:
        with engine.begin() as conn:
            row = conn.execute(
                insert_sql,
                {
                    "generated_at": report.generated_at,
                    "universe_name": report.universe_name,
                    "tickers_attempted": report.tickers_attempted,
                    "tickers_succeeded": report.tickers_succeeded,
                    "regime_signature": report.regime_signature,
                    "top_k": json.dumps(
                        [r.to_dict() for r in report.top_k]
                    ),
                    "sector_distributions": json.dumps(
                        [d.to_dict() for d in report.sector_distributions]
                    ),
                    "concentration_alerts": json.dumps(
                        list(report.concentration_alerts)
                    ),
                    "narrative": report.narrative,
                },
            ).first()
        if row is None:
            return -1
        return int(row[0])
    except Exception as exc:  # noqa: BLE001
        log.debug("universe_ranker: persist_ranking failed: {e}", e=exc)
        return -1


# ── CLI ──────────────────────────────────────────────────────────────────


def main(argv: list[str] | None = None) -> int:
    """Tiny CLI: ``python -m intelligence.universe_ranker --universe SP500``.

    Never raises — catches and prints failures.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="Rank a ticker universe by decision_gateway conviction.",
    )
    parser.add_argument(
        "--universe",
        default="SP500",
        help="Named universe (SP500, NASDAQ100) or comma-separated tickers.",
    )
    parser.add_argument(
        "--top-k", type=int, default=DEFAULT_TOP_K,
        help="Top-K names to surface.",
    )
    parser.add_argument(
        "--account-size", type=float, default=100_000.0,
        help="Account size in USD.",
    )
    parser.add_argument(
        "--parallel", action="store_true",
        help="Fan out with a thread pool.",
    )
    args = parser.parse_args(argv)

    try:
        from db import get_engine
        engine = get_engine()
    except Exception as exc:  # noqa: BLE001
        print(f"universe_ranker: engine bootstrap failed: {exc}")
        return 1

    if args.universe.upper() in _NAMED_UNIVERSES:
        universe: Sequence[str] | str = args.universe.upper()
    else:
        universe = [t.strip().upper() for t in args.universe.split(",") if t.strip()]

    report = rank_universe(
        engine,
        universe,
        account_size_usd=args.account_size,
        top_k=args.top_k,
        parallel=args.parallel,
    )
    print(report.narrative)
    for i, r in enumerate(report.top_k, start=1):
        print(
            f"  {i:2d}. {r.ticker:8s} verdict={r.verdict:6s} "
            f"conviction={r.aggregate_conviction:.2f} "
            f"robust={r.robustness_score:.2f} "
            f"score={r.composite_score:.2f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
