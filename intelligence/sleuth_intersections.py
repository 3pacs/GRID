"""Sleuth intersection leads: independent sources agreeing on one ticker.

The 2026-11-07 audit of ``investigation_leads`` found the old generators made
noise: a z-score on levels called a flat Selic rate's 0.0551 -> 0.0543 step
"43 trillion sigma", chat questions were stored as leads, and every finding
ended in "further investigation needed". This module is the scheduled path
that replaces them. The old generators stay reachable from the API only.

A lead is an *intersection*: at least ``MIN_FAMILIES`` independent evidence
families point the same way on one ticker inside ``WINDOW_DAYS``. One source
alone is never a lead. Families (``classify_signal``):

* ``options_flow``      unusual options activity, CALL = up, PUT = down
* ``insider``           Form 4 buys / sells (``insider``, ``quiverquant:insider``)
* ``congress``          congressional trades
* ``gov_contract``      contract awards (up)
* ``retail_sentiment``  WSB / reddit positioning
* ``price_move``        a robust excess-return anomaly vs SPY (``robust_z``)

Every observation carries its citation (table and row key) and was recorded
by ``as_of`` (``created_at <= as_of``): rows dated in the future or written
later are not evidence yet.

Every investigation ends in a declared outcome (``investigate_intersection_lead``):

* ``hypothesis_declared``  a ``discovered_hypotheses`` row with fixed,
  pre-registered test criteria (ticker, the lead's direction, ``HYPOTHESIS_WINDOW_DAYS``,
  ``HYPOTHESIS_MIN_MOVE_PCT``), scored once by the v2 trial scorer
  (``intelligence.hypothesis_trials``, #1008) on data after it was declared
* ``dead_end``             with its reason

The LLM may veto a lead (dead end, with a reason) or write its narrative. It
never chooses the ticker, direction, window or threshold of the test.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from statistics import median
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

# ── Constants (pre-registered; change only in a reviewed commit) ────────────

WINDOW_DAYS = 7               # observations must fall inside the 7 days to as_of
MIN_FAMILIES = 2              # independent families agreeing on a direction
MAX_NEW_LEADS = 20            # per run; the daily budget for LLM investigations
LEAD_COOLDOWN_DAYS = 7        # one open intersection lead per (ticker, direction)
MAX_PRICE_CANDIDATES = 40     # tickers whose closes are read per run
FAMILY_MAJORITY = 2 / 3       # a family votes only when this share of its rows agree
MAX_CITATIONS_PER_FAMILY = 10  # evidence rows kept per family (counts keep the rest)

PRICE_HISTORY_DAYS = 400      # calendar days of closes behind the window
PRICE_MIN_RETURNS = 120       # non-zero daily excess returns needed for robust_z
PRICE_Z_THRESHOLD = 4.0       # |robust z| of one day's excess log return
ROBUST_Z_CAP = 50.0           # reported magnitude cap; no more "43 trillion sigma"
MAX_PLAUSIBLE_DAILY_MOVE = 0.5  # |log return| above this is a data error, not a move

HYPOTHESIS_WINDOW_DAYS = 14   # the v2 ticker test's window (DEFAULT_WINDOW_DAYS)
HYPOTHESIS_MIN_MOVE_PCT = 2.0  # the v2 ticker test's raw-move floor (DEFAULT_MIN_MOVE_PCT)
HYPOTHESIS_PATTERN = "sleuth_intersection"

ORIGIN = "intersection"
OUTCOME_DECLARED = "hypothesis_declared"
OUTCOME_DEAD_END = "dead_end"

MARKET_TICKER = "SPY"
_US_TICKER = re.compile(r"^[A-Z][A-Z0-9]{0,5}(-[A-Z]{1,3})?$")  # evaluation.prices
_CRYPTO_TICKER = re.compile(r"^[A-Z0-9]{2,10}-USD$", re.IGNORECASE)  # evaluation.prices


def is_testable_ticker(ticker: str) -> bool:
    """US-listed and not crypto: what the v2 ticker test can score."""
    return bool(_US_TICKER.match(ticker)) and not _CRYPTO_TICKER.match(ticker)

_UP = {"BUY", "UNUSUAL_BUY", "CLUSTER_BUY", "INSIDER_BUY", "CONTRACT_AWARD", "WSB_BULLISH"}
_DOWN = {"SELL", "UNUSUAL_SELL", "INSIDER_SELL", "WSB_BEARISH"}
_FAMILY_BY_SOURCE = {
    "options_flow": "options_flow",
    "insider": "insider",
    "quiverquant:insider": "insider",
    "congressional": "congress",
    "gov_contract": "gov_contract",
    "quiverquant:gov_contracts": "gov_contract",
    "quiverquant:wsb": "retail_sentiment",
    "smart_money": "retail_sentiment",
}


# ── Pure parts ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Observation:
    family: str
    ticker: str
    direction: str            # "up" | "down"
    obs_date: date
    citation: dict[str, Any]
    magnitude: float | None = None


@dataclass(frozen=True)
class FamilyVote:
    """One family's net direction on a ticker: it votes only when at least
    ``FAMILY_MAJORITY`` of its rows agree. Two-sided flow casts no vote."""
    family: str
    direction: str | None
    up: int
    down: int
    rows: tuple[Observation, ...]


def family_votes(observations: list[Observation]) -> list[FamilyVote]:
    by_family: dict[str, list[Observation]] = {}
    for o in observations:
        by_family.setdefault(o.family, []).append(o)
    votes: list[FamilyVote] = []
    for family, rows in sorted(by_family.items()):
        up = sum(o.direction == "up" for o in rows)
        down = len(rows) - up
        direction = ("up" if up >= FAMILY_MAJORITY * len(rows)
                     else "down" if down >= FAMILY_MAJORITY * len(rows) else None)
        votes.append(FamilyVote(family, direction, up, down, tuple(rows)))
    return votes


@dataclass
class IntersectionLead:
    ticker: str
    direction: str
    window_end: date
    agree: list[FamilyVote]
    dissent: list[FamilyVote] = field(default_factory=list)
    split: list[FamilyVote] = field(default_factory=list)

    @property
    def families(self) -> list[str]:
        return [v.family for v in self.agree]

    @property
    def lead_id(self) -> str:
        key = f"{self.ticker}|{self.direction}|{self.window_end.isoformat()}"
        return f"LEAD-INTX-{hashlib.sha256(key.encode()).hexdigest()[:12]}"

    @property
    def priority(self) -> float:
        return round(min(1.0, 0.25 + 0.2 * len(self.agree) - 0.1 * len(self.dissent)
                         - 0.05 * len(self.split)), 3)

    def question(self) -> str:
        fams = ", ".join(self.families)
        return (f"{len(self.agree)} independent sources ({fams}) point {self.direction} "
                f"on {self.ticker} in the {WINDOW_DAYS} days to {self.window_end.isoformat()}. "
                f"Is there a common cause, and does it predict the next "
                f"{HYPOTHESIS_WINDOW_DAYS} days?")

    def evidence(self) -> list[dict[str, Any]]:
        """One summary row per family (its up/down counts), then up to
        ``MAX_CITATIONS_PER_FAMILY`` cited rows from each agreeing family."""
        out: list[dict[str, Any]] = []
        for role, votes in (("agrees", self.agree), ("dissents", self.dissent),
                            ("split", self.split)):
            for v in votes:
                out.append({"role": role, "kind": "family_vote", "family": v.family,
                            "ticker": self.ticker,
                            "direction": self.direction if role == "agrees" else v.direction,
                            "up": v.up, "down": v.down})
        for v in self.agree:
            rows = sorted((o for o in v.rows if o.direction == self.direction),
                          key=lambda o: (o.obs_date, -abs(o.magnitude or 0)), reverse=True)
            for o in rows[:MAX_CITATIONS_PER_FAMILY]:
                out.append({"role": "cites", "family": o.family, "ticker": o.ticker,
                            "direction": o.direction, "obs_date": o.obs_date.isoformat(),
                            "magnitude": o.magnitude, **o.citation})
        return out


def classify_signal(source_type: str | None, signal_type: str | None,
                    signal_value: Any) -> tuple[str, str] | None:
    """(family, direction) of a ``signal_sources`` row, or None when it has no
    family or no direction (lobbying, off-exchange volume, legislation...)."""
    family = _FAMILY_BY_SOURCE.get((source_type or "").strip().lower())
    if family is None:
        return None
    value = signal_value
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = None
    stype = (signal_type or "").strip().upper()
    if family == "options_flow":
        raw = str((value or {}).get("direction", "")).upper() if isinstance(value, dict) else ""
        direction = {"CALL": "up", "PUT": "down"}.get(raw)
    elif family == "retail_sentiment" and stype not in _UP | _DOWN:
        raw = str((value or {}).get("direction", "")).upper() if isinstance(value, dict) else ""
        direction = {"BULLISH": "up", "BEARISH": "down"}.get(raw)
    elif stype in _UP:
        direction = "up"
    elif stype in _DOWN:
        direction = "down"
    else:
        direction = None
    return (family, direction) if direction else None


def robust_z(history: list[float], value: float) -> float | None:
    """Robust z of ``value`` against ``history``: median/MAD of the history.

    None when the history is too short, has too few non-zero values (a step
    function or a stale feed), or has no spread. Capped at ``ROBUST_Z_CAP``.
    """
    nonzero = [x for x in history if x != 0 and math.isfinite(x)]
    if len(nonzero) < PRICE_MIN_RETURNS:
        return None
    med = median(history)
    mad = median(abs(x - med) for x in history)
    if mad <= 0:
        return None
    z = (value - med) / (1.4826 * mad)
    return max(-ROBUST_Z_CAP, min(ROBUST_Z_CAP, z))


def excess_log_returns(closes: dict[date, float],
                       market: dict[date, float]) -> list[tuple[date, float]]:
    """Daily log return minus SPY's, on dates both have consecutive closes.
    Drops implausible jumps (``MAX_PLAUSIBLE_DAILY_MOVE``): data errors."""
    days = sorted(d for d in closes if d in market)
    out: list[tuple[date, float]] = []
    for prev, cur in pairwise(days):
        try:
            r = math.log(closes[cur] / closes[prev])
            m = math.log(market[cur] / market[prev])
        except (ValueError, ZeroDivisionError):
            continue
        if abs(r) > MAX_PLAUSIBLE_DAILY_MOVE or abs(m) > MAX_PLAUSIBLE_DAILY_MOVE:
            continue
        out.append((cur, r - m))
    return out


def price_observation(ticker: str, returns: list[tuple[date, float]],
                      window_start: date, window_end: date) -> Observation | None:
    """The largest robust excess-return anomaly inside the window, if any day
    reaches ``PRICE_Z_THRESHOLD``. History is the returns before the window."""
    history = [r for d, r in returns if d < window_start]
    best: tuple[float, date, float] | None = None
    for d, r in returns:
        if not window_start <= d <= window_end:
            continue
        z = robust_z(history, r)
        if z is None or abs(z) < PRICE_Z_THRESHOLD:
            continue
        if best is None or abs(z) > abs(best[0]):
            best = (z, d, r)
    if best is None:
        return None
    z, d, r = best
    return Observation(
        family="price_move", ticker=ticker, direction="up" if z > 0 else "down",
        obs_date=d, magnitude=round(z, 2),
        citation={"table": "raw_series", "series_id": f"YF:{ticker}:close",
                  "reader": "store.observations.read_completed_closes",
                  "excess_log_return": round(r, 6), "vs": MARKET_TICKER},
    )


def find_intersections(observations: list[Observation], window_end: date,
                       min_families: int = MIN_FAMILIES) -> list[IntersectionLead]:
    """Tickers where ``min_families`` family votes agree on a direction and
    outnumber the families voting the other way."""
    by_ticker: dict[str, list[Observation]] = {}
    for o in observations:
        by_ticker.setdefault(o.ticker, []).append(o)
    leads: list[IntersectionLead] = []
    for ticker, obs in sorted(by_ticker.items()):
        votes = family_votes(obs)
        split = [v for v in votes if v.direction is None]
        for direction in ("up", "down"):
            agree = [v for v in votes if v.direction == direction]
            dissent = [v for v in votes if v.direction not in (direction, None)]
            if len(agree) < min_families or len(dissent) >= len(agree):
                continue
            leads.append(IntersectionLead(ticker=ticker, direction=direction,
                                          window_end=window_end, agree=agree,
                                          dissent=dissent, split=split))
    leads.sort(key=lambda lead: (-lead.priority, lead.ticker))
    return leads


# ── Data access (read-only) ─────────────────────────────────────────────────

_SIGNALS_SQL = text("""
    SELECT id, source_type, source_id, ticker, signal_date, signal_type, signal_value
    FROM signal_sources
    WHERE signal_date >= :start
      AND signal_date <= :end
      AND created_at <= :as_of_ts
      AND ticker IS NOT NULL
      AND source_type = ANY(:sources)
""")


def collect_signal_observations(conn: Any, window_start: date, window_end: date,
                                as_of_ts: datetime) -> list[Observation]:
    """Directional ``signal_sources`` rows known by ``as_of_ts``. One
    observation per (family, ticker, direction) is enough for the
    intersection; every row is kept as a citation."""
    rows = conn.execute(_SIGNALS_SQL, {
        "start": window_start, "end": window_end, "as_of_ts": as_of_ts,
        "sources": sorted(_FAMILY_BY_SOURCE),
    }).fetchall()
    out: list[Observation] = []
    for rid, source_type, source_id, ticker, sdate, stype, svalue in rows:
        cls = classify_signal(source_type, stype, svalue)
        tick = (ticker or "").strip().upper()
        if cls is None or not tick:
            continue
        family, direction = cls
        obs_date = sdate.date() if isinstance(sdate, datetime) else sdate
        out.append(Observation(
            family=family, ticker=tick, direction=direction, obs_date=obs_date,
            citation={"table": "signal_sources", "id": int(rid), "source_type": source_type,
                      "source_id": str(source_id) if source_id is not None else None,
                      "signal_type": stype},
        ))
    return out


def collect_price_observations(conn: Any, tickers: list[str], window_start: date,
                               window_end: date, as_of_ts: datetime) -> list[Observation]:
    """Robust excess-return anomalies for ``tickers`` (US-listed only)."""
    from evaluation.prices import UnsupportedInstrumentError
    from intelligence.hypothesis_trials import load_completed_closes

    start = window_start - timedelta(days=PRICE_HISTORY_DAYS)
    try:
        market = load_completed_closes(conn, MARKET_TICKER, start, window_end, as_of_ts)
    except UnsupportedInstrumentError:
        return []
    out: list[Observation] = []
    for ticker in tickers:
        if ticker == MARKET_TICKER or not is_testable_ticker(ticker):
            continue
        try:
            closes = load_completed_closes(conn, ticker, start, window_end, as_of_ts)
        except UnsupportedInstrumentError:
            continue
        obs = price_observation(ticker, excess_log_returns(closes, market),
                                window_start, window_end)
        if obs is not None:
            out.append(obs)
    return out


def _recent_lead_keys(conn: Any, since: datetime) -> set[tuple[str, str]]:
    rows = conn.execute(text("""
        SELECT evidence FROM investigation_leads
        WHERE origin = :origin AND created_at >= :since
    """), {"origin": ORIGIN, "since": since}).fetchall()
    keys: set[tuple[str, str]] = set()
    for (evidence,) in rows:
        items = evidence if isinstance(evidence, list) else json.loads(evidence or "[]")
        for item in items:
            if isinstance(item, dict) and item.get("role") == "agrees":
                keys.add((str(item.get("ticker")), str(item.get("direction"))))
    return keys


def price_candidates(signals: list[Observation], limit: int = MAX_PRICE_CANDIDATES) -> list[str]:
    """Tickers worth a closes read: those with at least one family vote,
    most votes first (a price anomaly can complete or strengthen them)."""
    by_ticker: dict[str, list[Observation]] = {}
    for o in signals:
        by_ticker.setdefault(o.ticker, []).append(o)
    voted = {t: sum(v.direction is not None for v in family_votes(rows))
             for t, rows in by_ticker.items()}
    ranked = sorted((t for t, n in voted.items() if n >= 1 and is_testable_ticker(t)),
                    key=lambda t: (-voted[t], t))
    return ranked[:limit]


# ── Generation (the scheduled path) ─────────────────────────────────────────


def generate_intersection_leads(engine: Engine, as_of: datetime | None = None,
                                max_new: int = MAX_NEW_LEADS) -> list[str]:
    """Find intersection leads as of ``as_of`` and save up to ``max_new`` new
    ones (``origin='intersection'``). Returns the saved lead ids."""
    from intelligence.sleuth import Lead, Sleuth

    as_of_ts = (as_of or datetime.now(timezone.utc)).astimezone(timezone.utc)
    window_end = as_of_ts.date()
    window_start = window_end - timedelta(days=WINDOW_DAYS)
    sleuth = Sleuth(engine)
    with engine.connect() as conn:
        signals = collect_signal_observations(conn, window_start, window_end, as_of_ts)
        candidates = price_candidates(signals)
        prices = collect_price_observations(conn, candidates, window_start, window_end, as_of_ts)
        seen = _recent_lead_keys(conn, as_of_ts - timedelta(days=LEAD_COOLDOWN_DAYS))

    leads = find_intersections(signals + prices, window_end)
    saved: list[str] = []
    for lead in leads:
        if len(saved) >= max_new:
            break
        if (lead.ticker, lead.direction) in seen:
            continue
        sleuth._save_lead(Lead(
            id=lead.lead_id, question=lead.question(), category="connection_found",
            priority=lead.priority, evidence=lead.evidence(), origin=ORIGIN,
        ))
        seen.add((lead.ticker, lead.direction))
        saved.append(lead.lead_id)
    log.info("Sleuth intersections: {n} candidates, {s} new leads (as_of {d})",
             n=len(leads), s=len(saved), d=window_end)
    return saved


# ── Investigation: every lead ends in a declared outcome ────────────────────

_VERDICT_PROMPT = (
    "You review an intersection lead: independent data sources agree on one "
    "ticker's direction. Decide whether it deserves a forward test. Answer with "
    "ONE JSON object and nothing else:\n"
    '{"verdict": "declare" | "dead_end", "reason": "<one sentence>", '
    '"common_cause": "<the most likely shared driver, or none>", '
    '"invalidation": "<what in the next 14 days would show it was nothing>"}\n'
    "Choose dead_end when the sources are not independent (one event reported "
    "twice), when the move is an already-public scheduled event, or when the "
    "evidence is too thin. You do not choose the ticker, direction, window or "
    "threshold of the test; they are fixed."
)


def _parse_verdict(response: str | None) -> dict[str, str] | None:
    if not response:
        return None
    match = re.search(r"\{.*\}", response, re.DOTALL)
    if not match:
        return None
    try:
        parsed = json.loads(match.group(0))
    except (TypeError, ValueError):
        return None
    verdict = str(parsed.get("verdict", "")).strip().lower()
    if verdict not in ("declare", "dead_end"):
        return None
    return {k: str(parsed.get(k) or "").strip()[:500]
            for k in ("reason", "common_cause", "invalidation")} | {"verdict": verdict}


def _lead_direction(lead: Any) -> tuple[str, str] | None:
    """(ticker, direction) the lead's agreeing evidence names, or None."""
    pairs = {(str(e.get("ticker")), str(e.get("direction")))
             for e in lead.evidence if isinstance(e, dict) and e.get("role") == "agrees"}
    return next(iter(pairs)) if len(pairs) == 1 else None


def declare_hypothesis(conn: Any, lead: Any, ticker: str, direction: str,
                       verdict: dict[str, str]) -> str:
    """Insert the lead's forward test into ``discovered_hypotheses``. Its
    ``created_at`` is now, so the v2 scorer's holdout is data after this."""
    hyp_id = f"hyp_sleuth_{lead.id.removeprefix('LEAD-INTX-')}"
    criteria = {"ticker": ticker, "window_days": HYPOTHESIS_WINDOW_DAYS,
                "min_move_pct": HYPOTHESIS_MIN_MOVE_PCT, "expected_direction": direction,
                "source": "sleuth", "lead_id": lead.id}
    thesis = (f"{ticker} moves {direction} at least {HYPOTHESIS_MIN_MOVE_PCT:g}% over "
              f"{HYPOTHESIS_WINDOW_DAYS} days after independent sources agreed "
              f"({lead.question[:160]})")
    conn.execute(text("""
        INSERT INTO discovered_hypotheses
            (id, thesis, pattern_type, evidence, test_criteria,
             invalidation, confidence, status, role, pair_id)
        VALUES
            (:id, :thesis, :ptype, CAST(:evidence AS JSONB), CAST(:criteria AS JSONB),
             :inv, 0.5, 'active', 'thesis', :id)
        ON CONFLICT (id) DO NOTHING
    """), {"id": hyp_id, "thesis": thesis, "ptype": HYPOTHESIS_PATTERN,
           "evidence": json.dumps(lead.evidence, default=str),
           "criteria": json.dumps(criteria),
           "inv": verdict.get("invalidation") or "No move in the declared direction."})
    return hyp_id


def investigate_intersection_lead(engine: Engine, lead_id: str, llm: Any = None) -> dict[str, Any]:
    """Review one intersection lead and record its outcome.

    Raises ``RuntimeError`` when the LLM is unavailable or answers outside the
    format, so the goal is retried and the lead stays ``new``. A lead that is
    not an open intersection lead is left alone.
    """
    from intelligence.sleuth import Sleuth, _get_llm

    sleuth = Sleuth(engine)
    lead = sleuth._load_lead(lead_id)
    if lead is None or lead.origin != ORIGIN or lead.status != "new":
        return {"lead_id": lead_id, "skipped": True,
                "status": getattr(lead, "status", None)}

    pair = _lead_direction(lead)
    now = datetime.now(timezone.utc).isoformat()
    if pair is None or not is_testable_ticker(pair[0]):
        lead.status, lead.outcome = "dead_end", OUTCOME_DEAD_END
        lead.outcome_reason = "UNSUPPORTED_INSTRUMENT" if pair else "AMBIGUOUS_EVIDENCE"
        lead.resolved_at = now
        sleuth._save_lead(lead)
        return {"lead_id": lead_id, "outcome": lead.outcome, "reason": lead.outcome_reason}

    client = llm if llm is not None else _get_llm()
    if client is None:
        raise RuntimeError("Sleuth: LLM unavailable; lead stays new")
    response = client.chat(
        [{"role": "system", "content": _VERDICT_PROMPT},
         {"role": "user", "content": f"LEAD: {lead.question}\n\nEVIDENCE:\n"
                                     f"{json.dumps(lead.evidence, indent=1, default=str)[:6000]}"}],
        temperature=0.2, num_predict=400,
    )
    verdict = _parse_verdict(response)
    if verdict is None:
        raise RuntimeError("Sleuth: LLM answer outside the verdict format; lead stays new")

    ticker, direction = pair
    lead.findings = verdict["common_cause"] or None
    lead.resolved_at = now
    if verdict["verdict"] == "declare":
        with engine.begin() as conn:
            hyp_id = declare_hypothesis(conn, lead, ticker, direction, verdict)
        lead.status, lead.outcome = "resolved", OUTCOME_DECLARED
        lead.outcome_reason = verdict["reason"] or None
        lead.declared_hypothesis_id = hyp_id
        lead.hypotheses = [{"id": hyp_id, "ticker": ticker, "direction": direction,
                            "window_days": HYPOTHESIS_WINDOW_DAYS,
                            "min_move_pct": HYPOTHESIS_MIN_MOVE_PCT}]
    else:
        lead.status, lead.outcome = "dead_end", OUTCOME_DEAD_END
        lead.outcome_reason = verdict["reason"] or "LLM review: dead end"
    sleuth._save_lead(lead)
    return {"lead_id": lead_id, "outcome": lead.outcome, "reason": lead.outcome_reason,
            "hypothesis_id": lead.declared_hypothesis_id}
