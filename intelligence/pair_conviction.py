"""
Pair trade conviction detector.

Finds relative-value trades with structurally higher Sharpe than outright
directional calls by running the full decision gateway on both legs of a
pair and requiring: (a) both legs pass their own stress test, (b)
directional consistency (long-leg bullish / short-leg bearish OR long-leg
conviction > short-leg conviction when both bullish), (c) same sector or
documented spread relationship, (d) no correlated-risk trap. Output:
``PairTradeTicket`` with leg sizing, invalidation, spread thesis.

Why pair trades
---------------

A directional call carries the full market beta: you are long the
company AND long the market. A pair trade (long X / short Y) hedges out
the shared beta and isolates the *delta* between the two names — the
exact thing the decision stack has independent opinions on. Historical
pair Sharpe is typically 2-3x outright Sharpe for correlated pairs
(same sector, same regime exposure), which is why the user memory rule
"every value on screen must be defensible" is satisfied structurally:
BOTH legs must pass the entire should_i_trade decision stack before a
pair ticket is ever generated.

Design
------

This module is pure orchestration on top of
``intelligence.decision_gateway.should_i_trade``. It does NOT
reimplement any scoring — it imports the single-leg gateway and loops
it over the two legs, then assembles a ``PairTradeTicket`` from the two
``DecisionResponse`` objects. The pair-level logic lives entirely in
pure helpers (``compute_pair_conviction_score``, ``compute_spread_sharpness``,
``is_correlated_risk_trap``, ``size_pair_legs``) so every decision is
trivially unit-testable.

All engine-touching paths are wrapped in try/except — ``generate_pair_ticket``
MUST NEVER raise, even if the decision gateway itself is broken. A
disqualifying leg or exception results in ``None`` being returned.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log


# ── Constants ─────────────────────────────────────────────────────────────

# Both legs must clear this conviction bar before we'll even consider
# the pair. Raw-gateway aggregate conviction is in [0, 1].
MIN_LEG_CONVICTION: float = 0.7

# The minimum "gap" the pair must have between the two legs — below this
# the pair is noise, not a spread. Same-direction pairs use abs(gap);
# opposite-direction pairs use the average conviction instead (already a
# non-trivial value when both legs clear MIN_LEG_CONVICTION).
MIN_SPREAD_SHARPNESS: float = 0.3

# Pair-level Kelly cap. Slightly higher than the single-leg 5% cap
# because a true pair hedges beta — gross exposure is 2x but net risk
# is much smaller. This is the cap APPLIED PER LEG; total gross
# exposure = 2 x PAIR_KELLY_CAP.
PAIR_KELLY_CAP: float = 0.08

# Default reward-to-risk ratio used to price target_price from
# entry/stop on each leg when the single-leg ticket did not carry one.
SPREAD_REWARD_RISK_RATIO: float = 1.5

# Verdict thresholds applied to the combined pair_conviction_score.
_PAIR_VERDICT_HIGH: float = 0.8
_PAIR_VERDICT_MEDIUM: float = 0.55
_PAIR_VERDICT_LOW: float = 0.35

# Direction tokens (kept lowercase, mirroring trade_ticket_generator).
_LONG: str = "long"
_SHORT: str = "short"
_BULLISH: str = "bullish"
_BEARISH: str = "bearish"


# ── Data classes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PairLeg:
    """A single leg of a pair trade with all sizing + exit fields pre-computed."""

    ticker: str
    direction: str                       # 'long' / 'short'
    kelly_size_pct: float                # fraction of account equity, <= PAIR_KELLY_CAP
    kelly_size_dollars: float
    entry_price: float
    stop_price: float
    target_price: float
    conviction: float                    # aggregate conviction in [0, 1]
    robustness_label: str                # 'robust' / 'moderate' / 'fragile' / 'unknown'
    robustness_score: float              # in [0, 1]; 0.0 when unknown
    signal_summary: str                  # 3-line summary of the single-leg provenance
    sector: str | None = None            # best-effort sector tag from sector_networks

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PairTradeTicket:
    """A self-contained relative-value trade ticket.

    A human reading this dataclass MUST be able to enter BOTH legs
    without referencing any other file, exactly like the single-leg
    ``TradeTicket`` from ``trading.trade_ticket_generator``.
    """

    pair_name: str                       # e.g. "LONG TSM / SHORT NVDA"
    long_leg: PairLeg
    short_leg: PairLeg
    pair_conviction_score: float         # in [0, 1]; the single headline number
    spread_sharpness: float              # in [0, 1]; gap/avg between legs
    net_exposure_usd: float              # long_$ - short_$; ~0 when dollar-neutral
    gross_exposure_usd: float            # long_$ + short_$
    thesis: str                          # lever → flow → actor for each leg
    invalidation: str                    # exit rule (both stops + spread reversal)
    causation_chain: str                 # one-line "why this pair"
    generated_at: str
    verdict: str                         # 'high' / 'medium' / 'low' / 'no_trade'

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair_name": self.pair_name,
            "long_leg": self.long_leg.to_dict(),
            "short_leg": self.short_leg.to_dict(),
            "pair_conviction_score": self.pair_conviction_score,
            "spread_sharpness": self.spread_sharpness,
            "net_exposure_usd": self.net_exposure_usd,
            "gross_exposure_usd": self.gross_exposure_usd,
            "thesis": self.thesis,
            "invalidation": self.invalidation,
            "causation_chain": self.causation_chain,
            "generated_at": self.generated_at,
            "verdict": self.verdict,
        }


@dataclass(frozen=True)
class PairCandidate:
    """Operator-supplied candidate for pair scanning.

    ``expected_relationship`` is a free-text tag (e.g.
    ``'fundamental_vs_retail'``, ``'iron_ore_vs_copper'``) that names
    the known macro reason the pair exists. It's surfaced on the output
    ticket so the operator can cross-check against their watchlist.
    """

    long_ticker: str
    short_ticker: str
    expected_relationship: str
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# Curated default pair candidates. These are well-known relative-value
# setups — the operator is expected to add / drop / override at will.
DEFAULT_PAIR_CANDIDATES: list[PairCandidate] = [
    PairCandidate(
        long_ticker="TSM",
        short_ticker="NVDA",
        expected_relationship="fundamental_vs_retail",
        rationale="Semi cycle fundamentals (TSM capex cycle) vs retail momentum (NVDA AI narrative).",
    ),
    PairCandidate(
        long_ticker="BHP",
        short_ticker="FCX",
        expected_relationship="iron_ore_vs_copper",
        rationale="Iron ore demand cycle vs copper supply-constrained cycle rotation.",
    ),
    PairCandidate(
        long_ticker="JPM",
        short_ticker="BAC",
        expected_relationship="regional_risk",
        rationale="Money-center diversification (JPM) vs regional deposit exposure (BAC).",
    ),
    PairCandidate(
        long_ticker="XOM",
        short_ticker="CVX",
        expected_relationship="upstream_vs_integrated",
        rationale="Upstream volumes leadership (XOM) vs integrated margin drag (CVX).",
    ),
    PairCandidate(
        long_ticker="GLD",
        short_ticker="SLV",
        expected_relationship="flight_to_safety",
        rationale="Gold-silver ratio shift — flight to safety favors gold.",
    ),
    PairCandidate(
        long_ticker="COST",
        short_ticker="WMT",
        expected_relationship="premium_vs_value_retail",
        rationale="Premium membership model vs mass-market volume retailer.",
    ),
    PairCandidate(
        long_ticker="LMT",
        short_ticker="BA",
        expected_relationship="defense_vs_commercial_aero",
        rationale="Defense backlog strength vs commercial aerospace execution risk.",
    ),
]


# ── Pure helpers (trivially testable) ─────────────────────────────────────


def compute_spread_sharpness(
    long_conviction: float,
    short_conviction: float,
    long_direction: str,
    short_direction: str,
) -> float:
    """Return the spread sharpness score in [0, 1].

    Two regimes:

    * Opposite directions (long=bullish, short=bearish) — the classic
      relative-value trade. Sharpness = average of the two convictions.
      Both legs pushing AWAY from each other IS the spread, so the more
      confident either leg is, the sharper the spread.

    * Same direction (both bullish, long conviction > short conviction)
      — weaker form of pair where the long leg simply has stronger
      evidence than the short. Sharpness = abs(gap) between the two
      convictions. Must exceed ``MIN_SPREAD_SHARPNESS`` to be a real spread.

    Pure function. Zero dependencies.
    """
    lc = max(0.0, min(1.0, float(long_conviction)))
    sc = max(0.0, min(1.0, float(short_conviction)))

    ld = (long_direction or "").lower()
    sd = (short_direction or "").lower()

    opposite = (ld == _BULLISH and sd == _BEARISH) or (ld == _BEARISH and sd == _BULLISH)
    if opposite:
        return round((lc + sc) / 2.0, 6)

    # Same direction — only the GAP is the spread signal
    return round(abs(lc - sc), 6)


def compute_pair_conviction_score(
    long_conviction: float,
    short_conviction: float,
    spread_sharpness: float,
) -> float:
    """Return the headline pair conviction score in [0, 1].

    Formula:

        pair_conviction = min(long_conviction, short_conviction) * spread_sharpness

    The ``min`` enforces "chain is as strong as its weakest link" — a
    pair with a 0.9 long and a 0.3 short is NOT a 0.6 pair, it's a
    0.3 pair with a dragging short. The ``spread_sharpness`` multiplier
    then discounts pairs where both legs are strong but too similar
    (noise).

    Pure function.
    """
    lc = max(0.0, min(1.0, float(long_conviction)))
    sc = max(0.0, min(1.0, float(short_conviction)))
    sharp = max(0.0, min(1.0, float(spread_sharpness)))
    return round(min(lc, sc) * sharp, 6)


def is_correlated_risk_trap(
    long_sector: str | None,
    short_sector: str | None,
    long_dir: str,
    short_dir: str,
) -> bool:
    """True when the "pair" is actually a concentrated long (or short).

    Two bullish tech megacaps are NOT a pair — they're a 2x long tech
    with extra steps. The operator-memory rule "first principles — every
    association must make logical sense" applies: if both legs are in
    the same sector AND both legs are directionally the same, this is
    not a spread trade.

    When either sector is ``None`` we cannot prove it's a trap, so we
    return ``False`` and let the downstream check pass it through.
    Opposite-direction same-sector pairs (long JPM / short BAC, both
    financials) ARE valid pairs.
    """
    if not long_sector or not short_sector:
        return False
    if long_sector.strip().lower() != short_sector.strip().lower():
        return False
    return (long_dir or "").lower() == (short_dir or "").lower()


def _one_line(text: str, limit: int = 120) -> str:
    """Collapse whitespace + truncate for single-line display fields."""
    if not text:
        return ""
    clean = " ".join(str(text).split())
    return clean if len(clean) <= limit else clean[: limit - 1] + "…"


def compose_pair_thesis(
    long_ticker: str,
    short_ticker: str,
    long_leg: PairLeg,
    short_leg: PairLeg,
) -> str:
    """Compose the spread thesis naming both legs + both lever chains.

    The operator-memory SOP requires lever → flow → actor for every
    prediction. The pair thesis simply concatenates the two legs'
    summaries with an explicit "spread" framing.
    """
    long_tag = (long_ticker or "").upper()
    short_tag = (short_ticker or "").upper()
    long_why = _one_line(long_leg.signal_summary) or "long-leg lever pending"
    short_why = _one_line(short_leg.signal_summary) or "short-leg lever pending"
    return (
        f"SPREAD: LONG {long_tag} / SHORT {short_tag}. "
        f"LONG LEG ({long_tag}): {long_why}. "
        f"SHORT LEG ({short_tag}): {short_why}. "
        f"Pair isolates the {long_tag}-{short_tag} delta and hedges shared beta."
    )


def compute_pair_invalidation(long_leg: PairLeg, short_leg: PairLeg) -> str:
    """Derive the exit rule referencing BOTH leg stops + spread reversal.

    Matches the single-leg ``TradeTicket.invalidation`` format so the
    operator can paste this directly into their exit rules.
    """
    long_stop = long_leg.stop_price
    short_stop = short_leg.stop_price
    return (
        f"EXIT pair when ANY of: "
        f"(1) {long_leg.ticker.upper()} long stopped at ${long_stop:.2f}, "
        f"(2) {short_leg.ticker.upper()} short stopped at ${short_stop:.2f}, "
        f"(3) spread (LONG - SHORT) reverses vs entry by > 2x initial R."
    )


def verdict_from_pair_conviction(
    pair_conviction: float,
    worst_leg_robustness: float,
) -> str:
    """Map the numeric pair_conviction_score to a coarse verdict.

    A very fragile worst-leg (robustness < 0.5) downgrades by one
    level — the pair is only as robust as its worst leg.
    """
    pc = max(0.0, min(1.0, float(pair_conviction)))
    wr = max(0.0, min(1.0, float(worst_leg_robustness)))

    if pc < MIN_LEG_CONVICTION * MIN_SPREAD_SHARPNESS:
        return "no_trade"

    if pc >= _PAIR_VERDICT_HIGH:
        verdict = "high"
    elif pc >= _PAIR_VERDICT_MEDIUM:
        verdict = "medium"
    elif pc >= _PAIR_VERDICT_LOW:
        verdict = "low"
    else:
        return "no_trade"

    if wr < 0.5:
        ladder = ("no_trade", "low", "medium", "high")
        idx = ladder.index(verdict)
        verdict = ladder[max(0, idx - 1)]
    return verdict


def size_pair_legs(
    account_size_usd: float,
    pair_conviction: float,
    long_price: float,
    short_price: float,
) -> tuple[float, float]:
    """Return ``(long_dollars, short_dollars)`` dollar-neutral-ish sizing.

    Both legs are capped at ``PAIR_KELLY_CAP`` of account equity. Because
    a pair trade hedges shared beta, the dollar-neutral convention is to
    match the two legs in *notional* — i.e. both legs get the same
    dollars, not the same share count. We allow a slight long-leg bias
    when pair_conviction clears the 0.8 "high" bar (the short leg simply
    hedges; the long is carrying the alpha).

    Pure function. Ignores engine / provenance.
    """
    if account_size_usd <= 0 or long_price <= 0 or short_price <= 0:
        return 0.0, 0.0

    pc = max(0.0, min(1.0, float(pair_conviction)))
    base_pct = min(PAIR_KELLY_CAP, PAIR_KELLY_CAP * pc / _PAIR_VERDICT_HIGH)
    base_dollars = round(account_size_usd * base_pct, 2)

    # High-conviction pairs get a ~5% LONG bias within the cap (long leg
    # carries the alpha; short leg only hedges). Medium/low stays neutral.
    long_bias = 1.05 if pc >= _PAIR_VERDICT_HIGH else 1.0
    long_dollars = round(min(base_dollars * long_bias, account_size_usd * PAIR_KELLY_CAP), 2)
    short_dollars = round(base_dollars, 2)

    return long_dollars, short_dollars


# ── Sector lookup (best-effort) ───────────────────────────────────────────


_TICKER_TO_SECTOR_CACHE: dict[str, str] = {}
_SECTOR_CACHE_LOADED: bool = False


def _load_ticker_sector_map() -> dict[str, str]:
    """Build a ticker → sector map from ``intelligence.sector_networks``.

    Cached after first load. Swallows any loader error — the entire
    module must remain importable even if sector YAML is broken.
    """
    global _SECTOR_CACHE_LOADED
    if _SECTOR_CACHE_LOADED:
        return _TICKER_TO_SECTOR_CACHE

    try:
        from intelligence.sector_networks.loader import get_actors, list_sectors

        for sector in list_sectors():
            try:
                for actor in get_actors(sector):
                    ticker = actor.get("ticker") if isinstance(actor, dict) else None
                    if ticker:
                        _TICKER_TO_SECTOR_CACHE.setdefault(str(ticker).upper(), sector)
            except Exception as inner:  # noqa: BLE001
                log.debug("pair_conviction: sector {s} load failed: {e}", s=sector, e=inner)
    except Exception as exc:  # noqa: BLE001
        log.debug("pair_conviction: sector_networks loader unavailable: {e}", e=exc)

    _SECTOR_CACHE_LOADED = True
    return _TICKER_TO_SECTOR_CACHE


def _sector_for_ticker(ticker: str) -> str | None:
    """Best-effort ticker → sector lookup. Returns ``None`` on miss."""
    if not ticker:
        return None
    return _load_ticker_sector_map().get(ticker.upper())


# ── Engine-touching functions (all try/except-wrapped) ───────────────────


def _extract_leg_fields(response: Any, direction: str) -> dict[str, Any]:
    """Pull the fields we need off a ``DecisionResponse``.

    Defensive: every attribute access is a getattr with a default. The
    decision gateway's partial-response contract means any sub-report
    can be ``None``.
    """
    prediction = getattr(response, "prediction", None)
    provenance = getattr(response, "provenance_report", None)
    stress = getattr(response, "stress_report", None)
    ticket = getattr(response, "trade_ticket", None)

    conviction = float(getattr(provenance, "aggregate_conviction", 0.0) or 0.0)

    # Prefer the provenance direction (validated causation chain);
    # fall back to prediction direction; otherwise neutral.
    direction_source = (
        getattr(provenance, "direction", None)
        or getattr(prediction, "direction", None)
        or "neutral"
    )

    robustness_label = getattr(stress, "robustness_label", None) or "unknown"
    robustness_score = float(getattr(stress, "robustness_score", 0.0) or 0.0)

    # Price fields — only present when trade_ticket was generated
    entry = float(getattr(ticket, "entry_price", 0.0) or 0.0)
    stop = float(getattr(ticket, "stop_price", 0.0) or 0.0)
    target = float(getattr(ticket, "target_price", 0.0) or 0.0)
    kelly_pct = float(getattr(ticket, "kelly_size_pct", 0.0) or 0.0)
    kelly_dollars = float(getattr(ticket, "kelly_size_dollars", 0.0) or 0.0)

    thesis_text = getattr(ticket, "thesis", None) or ""
    lever_text = getattr(ticket, "lever", None) or ""
    summary = _compose_signal_summary(
        ticker=getattr(response, "ticker", ""),
        direction=direction_source,
        thesis=thesis_text,
        lever=lever_text,
        conviction=conviction,
    )

    return {
        "ticker": getattr(response, "ticker", "") or "",
        "direction": direction,  # 'long' or 'short' — the pair role
        "signal_direction": str(direction_source).lower(),
        "conviction": conviction,
        "robustness_label": robustness_label,
        "robustness_score": robustness_score,
        "entry_price": entry,
        "stop_price": stop,
        "target_price": target,
        "kelly_size_pct": kelly_pct,
        "kelly_size_dollars": kelly_dollars,
        "signal_summary": summary,
        "unified_verdict": getattr(response, "unified_verdict", "no_trade") or "no_trade",
    }


def _compose_signal_summary(
    ticker: str,
    direction: str,
    thesis: str,
    lever: str,
    conviction: float,
) -> str:
    """3-line summary used for the pair thesis composition."""
    t = (ticker or "").upper() or "?"
    dr = (direction or "neutral").lower()
    thesis_line = _one_line(thesis, limit=140) or "(no thesis on single-leg ticket)"
    lever_line = _one_line(lever, limit=80) or "(lever unknown)"
    return f"{t} {dr} conv={conviction:.2f} | {lever_line} | {thesis_line}"


def _run_leg(
    engine: Any,
    ticker: str,
    direction: str,
    *,
    account_size_usd: float,
) -> dict[str, Any]:
    """Run ``should_i_trade`` on one leg. NEVER raises.

    Returns a plain dict so the caller can introspect even when the
    gateway itself is unavailable. The ``error`` key is populated when
    anything went wrong; the remaining fields are best-effort defaults.
    """
    base: dict[str, Any] = {
        "ticker": (ticker or "").upper(),
        "direction": direction,
        "signal_direction": "neutral",
        "conviction": 0.0,
        "robustness_label": "unknown",
        "robustness_score": 0.0,
        "entry_price": 0.0,
        "stop_price": 0.0,
        "target_price": 0.0,
        "kelly_size_pct": 0.0,
        "kelly_size_dollars": 0.0,
        "signal_summary": "",
        "unified_verdict": "no_trade",
        "response": None,
        "error": None,
    }
    try:
        from intelligence.decision_gateway import should_i_trade  # noqa: WPS433
    except Exception as exc:  # noqa: BLE001
        base["error"] = f"decision_gateway import failed: {exc}"
        log.debug("pair_conviction: {e}", e=base["error"])
        return base

    try:
        response = should_i_trade(
            engine,
            ticker,
            account_size_usd=float(account_size_usd),
        )
    except Exception as exc:  # noqa: BLE001
        base["error"] = f"should_i_trade raised: {exc}"
        log.debug("pair_conviction: {e}", e=base["error"])
        return base

    try:
        extracted = _extract_leg_fields(response, direction)
    except Exception as exc:  # noqa: BLE001
        base["error"] = f"leg field extraction failed: {exc}"
        log.debug("pair_conviction: {e}", e=base["error"])
        base["response"] = response
        return base

    extracted["response"] = response
    extracted["error"] = None
    return extracted


def _build_pair_leg(
    raw: dict[str, Any],
    direction: str,
    sized_dollars: float,
    account_size_usd: float,
    fallback_price: float,
) -> PairLeg:
    """Translate the raw dict from ``_run_leg`` into a frozen ``PairLeg``.

    When the single-leg ticket was not produced (ticket=None path), we
    still emit a PairLeg with defensive price fallbacks so downstream
    sizing/invalidation math stays finite.
    """
    entry = raw.get("entry_price") or fallback_price or 0.0
    stop = raw.get("stop_price") or (entry * (0.95 if direction == _LONG else 1.05) if entry else 0.0)
    target = raw.get("target_price") or (
        entry + (entry - stop) * SPREAD_REWARD_RISK_RATIO if entry and stop else 0.0
    )
    kelly_pct = (
        sized_dollars / account_size_usd if account_size_usd > 0 else 0.0
    )
    return PairLeg(
        ticker=(raw.get("ticker") or "").upper(),
        direction=direction,
        kelly_size_pct=round(kelly_pct, 6),
        kelly_size_dollars=round(sized_dollars, 2),
        entry_price=round(float(entry), 4),
        stop_price=round(float(stop), 4),
        target_price=round(float(target), 4),
        conviction=round(float(raw.get("conviction", 0.0)), 6),
        robustness_label=str(raw.get("robustness_label") or "unknown"),
        robustness_score=round(float(raw.get("robustness_score", 0.0)), 6),
        signal_summary=str(raw.get("signal_summary") or ""),
        sector=_sector_for_ticker(raw.get("ticker", "")),
    )


def generate_pair_ticket(
    engine: Any,
    long_ticker: str,
    short_ticker: str,
    *,
    account_size_usd: float = 100_000.0,
) -> PairTradeTicket | None:
    """Run both legs through ``should_i_trade`` and emit a pair ticket.

    Returns ``None`` whenever any disqualifying condition fires:

    * either leg's ``_run_leg`` returned an error
    * either leg's aggregate conviction < ``MIN_LEG_CONVICTION``
    * either leg's stress robustness_label == 'fragile' with score < 0.5
    * the two legs produce a correlated-risk trap (same sector, same dir)
    * spread sharpness < ``MIN_SPREAD_SHARPNESS``
    * final pair_conviction maps to verdict == 'no_trade'

    NEVER raises. All engine paths try/except-wrapped.
    """
    try:
        long_raw = _run_leg(engine, long_ticker, _LONG, account_size_usd=account_size_usd)
        short_raw = _run_leg(engine, short_ticker, _SHORT, account_size_usd=account_size_usd)

        if long_raw.get("error"):
            log.debug("pair_conviction: long leg error: {e}", e=long_raw["error"])
            return None
        if short_raw.get("error"):
            log.debug("pair_conviction: short leg error: {e}", e=short_raw["error"])
            return None

        long_conv = float(long_raw.get("conviction", 0.0))
        short_conv = float(short_raw.get("conviction", 0.0))
        if long_conv < MIN_LEG_CONVICTION or short_conv < MIN_LEG_CONVICTION:
            log.debug(
                "pair_conviction: leg conviction below floor "
                "(long={lc} short={sc} floor={f})",
                lc=long_conv,
                sc=short_conv,
                f=MIN_LEG_CONVICTION,
            )
            return None

        # Stress fragility gate — pair cannot be robust if either leg is fragile
        for label, raw in (("long", long_raw), ("short", short_raw)):
            if (
                str(raw.get("robustness_label", "")).lower() == "fragile"
                and float(raw.get("robustness_score", 0.0) or 0.0) < 0.5
            ):
                log.debug("pair_conviction: {l} leg fragile", l=label)
                return None

        long_sig_dir = str(long_raw.get("signal_direction") or "").lower()
        short_sig_dir = str(short_raw.get("signal_direction") or "").lower()

        sharpness = compute_spread_sharpness(
            long_conviction=long_conv,
            short_conviction=short_conv,
            long_direction=long_sig_dir,
            short_direction=short_sig_dir,
        )
        if sharpness < MIN_SPREAD_SHARPNESS:
            log.debug(
                "pair_conviction: sharpness {s} below floor {f}",
                s=sharpness,
                f=MIN_SPREAD_SHARPNESS,
            )
            return None

        # Correlated-risk trap check: same sector + same direction = concentrated long
        long_sector = _sector_for_ticker(str(long_raw.get("ticker", "")))
        short_sector = _sector_for_ticker(str(short_raw.get("ticker", "")))
        if is_correlated_risk_trap(long_sector, short_sector, long_sig_dir, short_sig_dir):
            log.debug(
                "pair_conviction: correlated-risk trap "
                "({ls}/{ss} both {ld})",
                ls=long_sector,
                ss=short_sector,
                ld=long_sig_dir,
            )
            return None

        pair_conv = compute_pair_conviction_score(long_conv, short_conv, sharpness)

        worst_robust = min(
            float(long_raw.get("robustness_score", 0.0) or 0.0),
            float(short_raw.get("robustness_score", 0.0) or 0.0),
        )
        verdict = verdict_from_pair_conviction(pair_conv, worst_robust)
        if verdict == "no_trade":
            log.debug("pair_conviction: verdict=no_trade (pair_conv={p})", p=pair_conv)
            return None

        long_dollars, short_dollars = size_pair_legs(
            account_size_usd=account_size_usd,
            pair_conviction=pair_conv,
            long_price=float(long_raw.get("entry_price") or 1.0),
            short_price=float(short_raw.get("entry_price") or 1.0),
        )

        long_leg = _build_pair_leg(
            long_raw,
            _LONG,
            sized_dollars=long_dollars,
            account_size_usd=account_size_usd,
            fallback_price=float(long_raw.get("entry_price") or 0.0),
        )
        short_leg = _build_pair_leg(
            short_raw,
            _SHORT,
            sized_dollars=short_dollars,
            account_size_usd=account_size_usd,
            fallback_price=float(short_raw.get("entry_price") or 0.0),
        )

        thesis = compose_pair_thesis(long_leg.ticker, short_leg.ticker, long_leg, short_leg)
        invalidation = compute_pair_invalidation(long_leg, short_leg)
        causation = (
            f"LONG {long_leg.ticker} ({long_sig_dir}, conv={long_conv:.2f}) vs "
            f"SHORT {short_leg.ticker} ({short_sig_dir}, conv={short_conv:.2f}); "
            f"sharpness={sharpness:.2f}; pair_conviction={pair_conv:.2f}"
        )

        return PairTradeTicket(
            pair_name=f"LONG {long_leg.ticker} / SHORT {short_leg.ticker}",
            long_leg=long_leg,
            short_leg=short_leg,
            pair_conviction_score=pair_conv,
            spread_sharpness=sharpness,
            net_exposure_usd=round(long_dollars - short_dollars, 2),
            gross_exposure_usd=round(long_dollars + short_dollars, 2),
            thesis=thesis,
            invalidation=invalidation,
            causation_chain=causation,
            generated_at=datetime.now(timezone.utc).isoformat(),
            verdict=verdict,
        )
    except Exception as exc:  # noqa: BLE001
        log.debug("pair_conviction: generate_pair_ticket unexpected: {e}", e=exc)
        return None


def scan_candidate_pairs(
    engine: Any,
    candidates: list[PairCandidate],
    *,
    account_size_usd: float = 100_000.0,
) -> list[PairTradeTicket]:
    """Run ``generate_pair_ticket`` over every candidate in ``candidates``.

    Invalid candidates (returned ``None``) are silently dropped. The
    surviving tickets are sorted by ``pair_conviction_score`` descending
    so the operator sees the strongest spreads first.

    NEVER raises.
    """
    tickets: list[PairTradeTicket] = []
    if not candidates:
        return tickets

    for cand in candidates:
        try:
            ticket = generate_pair_ticket(
                engine,
                cand.long_ticker,
                cand.short_ticker,
                account_size_usd=account_size_usd,
            )
        except Exception as exc:  # noqa: BLE001
            log.debug("pair_conviction: scan candidate {c} raised: {e}", c=cand, e=exc)
            continue
        if ticket is not None:
            tickets.append(ticket)

    tickets.sort(key=lambda t: t.pair_conviction_score, reverse=True)
    return tickets


__all__ = [
    "MIN_LEG_CONVICTION",
    "MIN_SPREAD_SHARPNESS",
    "PAIR_KELLY_CAP",
    "SPREAD_REWARD_RISK_RATIO",
    "PairLeg",
    "PairTradeTicket",
    "PairCandidate",
    "DEFAULT_PAIR_CANDIDATES",
    "compute_spread_sharpness",
    "compute_pair_conviction_score",
    "is_correlated_risk_trap",
    "compose_pair_thesis",
    "compute_pair_invalidation",
    "verdict_from_pair_conviction",
    "size_pair_legs",
    "generate_pair_ticket",
    "scan_candidate_pairs",
]
