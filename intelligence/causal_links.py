"""Point-in-time causal-link edges for the Timeline / Causal Map / Why views.

Slice N2 (2026-09-27). This replaces the old ``find_causes`` scan, which was
not honest enough to persist:

* it scored events up to 14 days AFTER a trade as the trade's "probable
  cause" (and gave the post-trade ones the highest scores);
* it stamped congressional trades at the transaction date, although the
  public learns of them weeks later (``disclosure_date`` in ``congressional``
  equals the transaction date on every checked row, so it is not a real
  disclosure date);
* it read USASpending's award *Start Date* (which can be in the future) as
  the date a contract became known;
* it labelled any two same-direction trades by other actors as
  ``insider_knowledge``, matched legislation through committee-to-sector
  ticker baskets, and its macro branch matched no series at all;
* it re-inserted every row on every run (no key, no provenance), and counted
  the same Form 4 trade once per channel.

What an edge claims now — and nothing more:

    "Public event E on ticker T was knowable before actor A traded T."

That is a time-ordered co-occurrence, not proof that E caused the trade.
Every edge carries the evidence for its timing:

* ``action_known_at`` — the earliest defensible time the trade was public
  (Form 4 ``fileDate``; else the time GRID first saw it; for congressional
  trades ``max(transaction + 45 days, first seen)``, the statutory bound);
* ``event_known_at`` — when the event was public (earnings: end of the
  release day; contract awards: when GRID first saw the award, and only for
  awards that were fresh then — backfilled old awards are not events);
* ``known_at`` — ``max(action_known_at, event_known_at)``, the first moment
  the pair could have been asserted. Views never show an edge before it.

An event is only linked when ``event_known_at <= start of the trade day``,
so no edge can be "caused" by something disclosed after the trade.

Channels and de-duplication follow GRID-GRANULAR-DISCOVERY-PLAN-20260927
§2.1: one Form 4 act seen through ``insider`` (EDGAR) and
``quiverquant:insider`` is one action (key: ticker, normalized owner name,
transaction date, direction). ``quiverquant:house``/``senate`` mirror
``congressional`` row for row and are not read. ``CLUSTER_BUY`` rows are
aggregates of other rows and are skipped.

The score stored in ``probability`` (the legacy column name other readers
use as an edge strength) is a heuristic recency score, ``score_method =
recency_linear_v1``. It is not a probability; the APIs say so.

The granular people-events / density / flywheel work (plan GD2/GD5/GD9) is
the intended successor of this construct.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

EDGE_SCHEMA_VERSION = "antecedent_v1"
SCORE_METHOD = "recency_linear_v1"

EARNINGS_WINDOW_DAYS = 30
CONTRACT_WINDOW_DAYS = 60
# A contract counts as an event only if GRID first saw it at most this many
# days after its Start Date (or up to CONTRACT_MAX_LEAD_DAYS before it).
CONTRACT_MAX_STALENESS_DAYS = 30
CONTRACT_MAX_LEAD_DAYS = 365
CONGRESS_STATUTORY_LAG_DAYS = 45

_FORM4_SOURCES = ("insider", "quiverquant:insider")
_CONGRESS_SOURCES = ("congressional",)
ACTION_SOURCE_TYPES: tuple[str, ...] = _FORM4_SOURCES + _CONGRESS_SOURCES

_BUY_TYPES = {"BUY", "UNUSUAL_BUY", "INSIDER_BUY"}
_SELL_TYPES = {"SELL", "UNUSUAL_SELL", "INSIDER_SELL"}

_REQUIRED_LINK_COLUMNS = (
    "edge_key", "known_at", "action_known_at", "event_known_at", "event_kind",
    "event_key", "event_date", "run_id", "first_run_id", "code_sha",
    "score_method", "computed_at",
)

try:  # SEC fileDate values are US/Eastern wall-clock times without an offset.
    from zoneinfo import ZoneInfo

    _EASTERN: Any = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - tzdata missing
    _EASTERN = None


class CausalLinksSchemaMissing(RuntimeError):
    """The provenance columns / run table are absent: run ``alembic upgrade head``."""


# ── Data classes ─────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Action:
    """One real-world trade, merged across the channels that reported it."""

    channel: str                  # 'form4' | 'congress'
    ticker: str
    actor: str
    actor_key: str
    direction: str                # 'BUY' | 'SELL'
    action_date: date
    known_at: datetime
    known_at_basis: str           # 'filing' | 'first_seen' | 'statutory_bound'
    source_refs: tuple[int, ...]
    source_types: tuple[str, ...]


@dataclass(frozen=True)
class AntecedentEvent:
    """A public event on a ticker with the time it became knowable."""

    kind: str                     # 'earnings' | 'contract'
    ticker: str
    key: str
    event_date: date
    known_at: datetime
    known_at_basis: str           # 'release_date' | 'first_seen'
    window_days: int
    description: str
    evidence: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Edge:
    edge_key: str
    action: Action
    event: AntecedentEvent
    known_at: datetime
    lead_time_days: float
    score: float

    def to_row(self) -> dict[str, Any]:
        a, e = self.action, self.event
        evidence = [{
            "type": e.kind,
            "event_key": e.key,
            "event_date": e.event_date.isoformat(),
            "event_known_at": e.known_at.isoformat(),
            "event_known_at_basis": e.known_at_basis,
            "action_known_at": a.known_at.isoformat(),
            "action_known_at_basis": a.known_at_basis,
            "action_source_types": list(a.source_types),
            "action_source_refs": list(a.source_refs),
            "claim": "public event preceded the trade; not proof of cause",
            **e.evidence,
        }]
        return {
            "edge_key": self.edge_key,
            "signal_id": a.source_refs[0] if a.source_refs else None,
            "actor": a.actor,
            "ticker": a.ticker,
            "action": a.direction,
            "action_channel": a.channel,
            "action_date": a.action_date,
            "action_known_at": a.known_at,
            "action_known_at_basis": a.known_at_basis,
            "cause_type": e.kind,
            "probable_cause": e.description,
            "event_kind": e.kind,
            "event_key": e.key,
            "event_date": e.event_date,
            "event_known_at": e.known_at,
            "event_known_at_basis": e.known_at_basis,
            "known_at": self.known_at,
            "lead_time_days": self.lead_time_days,
            "probability": self.score,
            "score_method": SCORE_METHOD,
            "evidence": json.dumps(evidence, default=str),
        }


@dataclass
class RunSummary:
    run_id: str
    as_of: datetime
    code_sha: str
    days: int
    tickers_processed: int = 0
    actions_processed: int = 0
    edges_found: int = 0
    edges_written: int = 0
    status: str = "running"
    dry_run: bool = False
    edges: list[Edge] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "as_of": self.as_of.isoformat(),
            "code_sha": self.code_sha,
            "days": self.days,
            "tickers_processed": self.tickers_processed,
            "actions_processed": self.actions_processed,
            "edges_found": self.edges_found,
            "edges_written": self.edges_written,
            "status": self.status,
            "dry_run": self.dry_run,
        }


# ── Pure helpers ─────────────────────────────────────────────────────────


def resolve_code_sha(repo_root: Path | None = None) -> str:
    """Commit of the running code: GRID_CODE_SHA, a VERSION file, git, else 'unknown'."""
    env = os.environ.get("GRID_CODE_SHA", "").strip()
    if env:
        return env
    root = repo_root or Path(__file__).resolve().parent.parent
    version = root / "VERSION"
    try:
        if version.exists():
            sha = version.read_text(encoding="utf-8").strip()
            if sha:
                return sha
    except OSError:
        pass
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return "unknown"


def _utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def day_start(d: date) -> datetime:
    return datetime.combine(d, time(0, 0), tzinfo=timezone.utc)


def end_of_day(d: date) -> datetime:
    """A date-only public time, rounded up to the end of that day (UTC)."""
    return day_start(d + timedelta(days=1))


def _as_date(val: Any) -> date | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    try:
        return date.fromisoformat(str(val)[:10])
    except (TypeError, ValueError):
        return None


def _as_datetime(val: Any) -> datetime | None:
    if val is None:
        return None
    if isinstance(val, datetime):
        return _utc(val)
    try:
        return _utc(datetime.fromisoformat(str(val).replace("Z", "+00:00")))
    except (TypeError, ValueError):
        return None


def parse_sec_filetime(val: Any) -> datetime | None:
    """Parse a QuiverQuant/SEC ``fileDate`` (Eastern wall clock) to UTC.

    A date-only value is rounded up to the end of that Eastern day. Without
    tz data the value is shifted by +5h (EST), which is never earlier than
    the true UTC time.
    """
    if not val:
        return None
    raw = str(val).strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return _utc(parsed)
    if len(raw) <= 10:
        parsed = parsed + timedelta(days=1)
    if _EASTERN is not None:
        return parsed.replace(tzinfo=_EASTERN).astimezone(timezone.utc)
    return (parsed + timedelta(hours=5)).replace(tzinfo=timezone.utc)  # pragma: no cover


def _parse_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            out = json.loads(raw)
            return out if isinstance(out, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def normalize_actor(name: str | None) -> str:
    """Order-insensitive owner key: 'Cutt Timothy J.' == 'Timothy Cutt'."""
    tokens = re.sub(r"[^a-z0-9 ]+", " ", (name or "").lower()).split()
    tokens = [t for t in tokens if len(t) > 1 and t not in {"jr", "sr", "ii", "iii", "iv"}]
    return " ".join(sorted(tokens))


def direction_of(signal_type: str | None) -> str | None:
    st = (signal_type or "").strip().upper()
    if st in _BUY_TYPES:
        return "BUY"
    if st in _SELL_TYPES:
        return "SELL"
    return None


def edge_key_for(action: Action, event: AntecedentEvent) -> str:
    raw = "|".join([
        EDGE_SCHEMA_VERSION, action.channel, action.ticker, action.actor_key,
        action.action_date.isoformat(), action.direction, event.kind, event.key,
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def recency_score(lead_days: float, window_days: int) -> float:
    """Heuristic (NOT a probability): 0.7 for a same-day event, 0.3 at the window edge."""
    frac = max(0.0, min(1.0, lead_days / float(window_days)))
    return round(0.7 - 0.4 * frac, 3)


# ── Canonical actions ────────────────────────────────────────────────────


def _row_action_fields(row: dict[str, Any]) -> tuple[str, str, datetime, str] | None:
    """Return (channel, actor, known_at, basis) for one signal_sources row."""
    stype = row.get("source_type")
    value = _parse_json(row.get("signal_value"))
    first_seen = _as_datetime(row.get("created_at"))
    trade_day = _as_date(row.get("signal_date"))
    if first_seen is None or trade_day is None:
        return None
    if stype == "quiverquant:insider":
        actor = str(value.get("Name") or "").strip()
        filed = parse_sec_filetime(value.get("fileDate"))
        if filed is not None:
            return "form4", actor, filed, "filing"
        return "form4", actor, first_seen, "first_seen"
    if stype == "insider":
        return "form4", str(row.get("source_id") or "").strip(), first_seen, "first_seen"
    if stype == "congressional":
        # disclosure_date in this channel equals the trade date (not real);
        # use the plan's statutory bound instead.
        bound = end_of_day(trade_day + timedelta(days=CONGRESS_STATUTORY_LAG_DAYS))
        return "congress", str(row.get("source_id") or "").strip(), max(bound, first_seen), "statutory_bound"
    return None


def canonical_actions(rows: Iterable[dict[str, Any]], as_of: datetime) -> list[Action]:
    """Merge signal_sources rows into one Action per real-world trade.

    Rows first seen after ``as_of``, trades dated after ``as_of`` and trades
    not yet public at ``as_of`` are dropped.
    """
    as_of = _utc(as_of)
    groups: dict[tuple, list[tuple[dict[str, Any], str, datetime, str]]] = {}
    for row in rows:
        ticker = (row.get("ticker") or "").strip().upper()
        direction = direction_of(row.get("signal_type"))
        trade_day = _as_date(row.get("signal_date"))
        first_seen = _as_datetime(row.get("created_at"))
        if not ticker or direction is None or trade_day is None or first_seen is None:
            continue
        if first_seen > as_of or day_start(trade_day) > as_of:
            continue
        fields_ = _row_action_fields(row)
        if fields_ is None:
            continue
        channel, actor, known_at, basis = fields_
        actor_key = normalize_actor(actor)
        if not actor_key:
            continue
        key = (channel, ticker, actor_key, trade_day, direction)
        groups.setdefault(key, []).append((row, actor, known_at, basis))

    actions: list[Action] = []
    for (channel, ticker, actor_key, trade_day, direction), members in groups.items():
        _, _, known_at, basis = min(members, key=lambda m: m[2])
        if known_at > as_of:
            continue
        display = next((m[1] for m in members if m[0].get("source_type") == "quiverquant:insider" and m[1]), members[0][1])
        actions.append(Action(
            channel=channel,
            ticker=ticker,
            actor=display,
            actor_key=actor_key,
            direction=direction,
            action_date=trade_day,
            known_at=known_at,
            known_at_basis=basis,
            source_refs=tuple(sorted(int(m[0]["id"]) for m in members if m[0].get("id") is not None)),
            source_types=tuple(sorted({str(m[0].get("source_type")) for m in members})),
        ))
    actions.sort(key=lambda a: (a.ticker, a.action_date, a.actor_key, a.direction, a.channel))
    return actions


# ── Antecedent events ────────────────────────────────────────────────────


def earnings_events(rows: Iterable[dict[str, Any]]) -> list[AntecedentEvent]:
    """Reported earnings releases. Unreported calendar rows are estimates, not events."""
    out: dict[tuple[str, date], AntecedentEvent] = {}
    for row in rows:
        ticker = (row.get("ticker") or "").strip().upper()
        e_day = _as_date(row.get("earnings_date"))
        eps_act = row.get("eps_actual")
        if not ticker or e_day is None:
            continue
        if not (row.get("reported") is True or eps_act is not None):
            continue
        eps_est = row.get("eps_estimate")
        quarter = row.get("fiscal_quarter")
        if eps_act is not None and eps_est is not None:
            a, e = float(eps_act), float(eps_est)
            verdict = "beat" if a > e else "miss" if a < e else "inline"
            desc = f"Earnings {verdict} released {e_day} ({a:.2f} vs {e:.2f} est)"
        else:
            desc = f"Earnings released {e_day}"
        if quarter:
            desc += f" [{quarter}]"
        out[(ticker, e_day)] = AntecedentEvent(
            kind="earnings",
            ticker=ticker,
            key=f"earnings:{ticker}:{e_day.isoformat()}",
            event_date=e_day,
            known_at=end_of_day(e_day),
            known_at_basis="release_date",
            window_days=EARNINGS_WINDOW_DAYS,
            description=desc,
            evidence={
                "eps_estimate": None if eps_est is None else float(eps_est),
                "eps_actual": None if eps_act is None else float(eps_act),
                "surprise_pct": None if row.get("eps_surprise_pct") is None else float(row["eps_surprise_pct"]),
                "fiscal_quarter": quarter,
            },
        )
    return list(out.values())


def contract_events(rows: Iterable[dict[str, Any]]) -> list[AntecedentEvent]:
    """USASpending awards keyed by award_id, timed by when GRID first saw them.

    ``signal_date`` in this channel is the award Start Date (sometimes in the
    future), not a publication date, so it is kept as evidence only. Awards
    first seen more than CONTRACT_MAX_STALENESS_DAYS after their Start Date
    are backfill, not events, and are skipped.
    """
    out: dict[tuple[str, str], AntecedentEvent] = {}
    for row in rows:
        ticker = (row.get("ticker") or "").strip().upper()
        first_seen = _as_datetime(row.get("created_at"))
        start = _as_date(row.get("signal_date"))
        if not ticker or first_seen is None or start is None:
            continue
        # Only awards that were fresh when GRID first saw them. Most rows are
        # backfilled awards first seen months or years after they started
        # (2026-09-27: 628 of 986 rows >800 days); for those first_seen is an
        # ingestion date, not an event time, so they are not events here.
        staleness = (first_seen.date() - start).days
        if staleness > CONTRACT_MAX_STALENESS_DAYS or staleness < -CONTRACT_MAX_LEAD_DAYS:
            continue
        value = _parse_json(row.get("signal_value"))
        award_id = str(value.get("award_id") or "").strip() or f"row{row.get('id')}"
        key = f"contract:{ticker}:{award_id}"
        prev = out.get((ticker, award_id))
        if prev is not None and prev.known_at <= first_seen:
            continue
        amount = value.get("amount")
        agency = row.get("source_id") or value.get("awarding_agency") or "unknown agency"
        try:
            amount_txt = f"${float(amount):,.0f} " if amount else ""
        except (TypeError, ValueError):
            amount_txt = ""
        out[(ticker, award_id)] = AntecedentEvent(
            kind="contract",
            ticker=ticker,
            key=key,
            event_date=first_seen.date(),
            known_at=first_seen,
            known_at_basis="first_seen",
            window_days=CONTRACT_WINDOW_DAYS,
            description=f"{amount_txt}contract award {award_id} from {agency} (first seen {first_seen.date()})",
            evidence={
                "award_id": award_id,
                "amount": amount,
                "agency": agency,
                "award_start_date": start.isoformat(),
                "award_start_date_note": "USASpending Start Date; not when the award became public",
                "first_seen_minus_start_days": staleness,
                "description": str(value.get("description") or "")[:200],
            },
        )
    return list(out.values())


def build_edges(
    actions: Sequence[Action], events: Sequence[AntecedentEvent],
) -> list[Edge]:
    """Link each action to events on its ticker that were public before the trade day."""
    by_ticker: dict[str, list[AntecedentEvent]] = {}
    for ev in events:
        by_ticker.setdefault(ev.ticker, []).append(ev)
    edges: dict[str, Edge] = {}
    for action in actions:
        trade_start = day_start(action.action_date)
        for ev in by_ticker.get(action.ticker, ()):
            if ev.known_at > trade_start:
                continue  # disclosed on/after the trade day: cannot be its cause
            lead = (trade_start - ev.known_at).total_seconds() / 86400.0
            if lead > ev.window_days:
                continue
            key = edge_key_for(action, ev)
            edges[key] = Edge(
                edge_key=key,
                action=action,
                event=ev,
                known_at=max(action.known_at, ev.known_at),
                lead_time_days=round(lead, 3),
                score=recency_score(lead, ev.window_days),
            )
    return sorted(edges.values(), key=lambda e: (e.action.ticker, e.action.action_date, e.edge_key))


# ── Database I/O ─────────────────────────────────────────────────────────


def _mappings(conn: Connection, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    return [dict(r) for r in conn.execute(text(sql), params).mappings().fetchall()]


def select_tickers(
    conn: Connection, since: date, as_of: datetime, max_tickers: int,
    tickers: Sequence[str] | None = None,
) -> list[str]:
    params: dict[str, Any] = {
        "types": list(ACTION_SOURCE_TYPES), "since": since,
        "until": as_of.date(), "as_of": as_of, "lim": int(max_tickers),
    }
    sql = (
        "SELECT UPPER(ticker) AS ticker FROM signal_sources "
        "WHERE source_type = ANY(:types) AND ticker IS NOT NULL AND ticker <> '' "
        "AND signal_date BETWEEN :since AND :until AND created_at <= :as_of "
    )
    if tickers:
        sql += "AND UPPER(ticker) = ANY(:tickers) "
        params["tickers"] = [t.strip().upper() for t in tickers if t.strip()]
    # Most recently active tickers first, so a max_tickers cap drops the stalest.
    sql += "GROUP BY 1 ORDER BY MAX(created_at) DESC, 1 LIMIT :lim"
    return [r["ticker"] for r in _mappings(conn, sql, params)]


def load_action_rows(
    conn: Connection, tickers: Sequence[str], since: date, as_of: datetime,
) -> list[dict[str, Any]]:
    return _mappings(conn, (
        "SELECT id, source_type, source_id, UPPER(ticker) AS ticker, signal_date, "
        "       signal_type, signal_value, created_at "
        "FROM signal_sources "
        "WHERE source_type = ANY(:types) AND UPPER(ticker) = ANY(:tickers) "
        "AND signal_date BETWEEN :since AND :until AND created_at <= :as_of "
        "ORDER BY id"
    ), {
        "types": list(ACTION_SOURCE_TYPES), "tickers": list(tickers),
        "since": since, "until": as_of.date(), "as_of": as_of,
    })


def load_event_rows(
    conn: Connection, tickers: Sequence[str], since: date, as_of: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    earn = _mappings(conn, (
        "SELECT UPPER(ticker) AS ticker, earnings_date, fiscal_quarter, eps_estimate, "
        "       eps_actual, eps_surprise_pct, reported "
        "FROM earnings_calendar "
        "WHERE UPPER(ticker) = ANY(:tickers) AND earnings_date BETWEEN :start AND :until"
    ), {
        "tickers": list(tickers),
        "start": since - timedelta(days=EARNINGS_WINDOW_DAYS + 1),
        "until": as_of.date(),
    })
    contracts = _mappings(conn, (
        "SELECT id, source_id, UPPER(ticker) AS ticker, signal_date, signal_value, created_at "
        "FROM signal_sources "
        "WHERE source_type = 'gov_contract' AND UPPER(ticker) = ANY(:tickers) "
        "AND created_at >= :start AND created_at <= :as_of"
    ), {
        "tickers": list(tickers),
        "start": day_start(since - timedelta(days=CONTRACT_WINDOW_DAYS + 1)),
        "as_of": as_of,
    })
    return earn, contracts


def schema_ready(conn: Connection) -> bool:
    """True when causal_links has the provenance columns and causal_link_runs exists."""
    tables = conn.execute(text(
        "SELECT to_regclass('causal_links') IS NOT NULL, to_regclass('causal_link_runs') IS NOT NULL"
    )).fetchone()
    if not tables or not tables[0] or not tables[1]:
        return False
    cols = {r[0] for r in conn.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = current_schema() AND table_name = 'causal_links'"
    )).fetchall()}
    return all(c in cols for c in _REQUIRED_LINK_COLUMNS)


_UPSERT_SQL = text("""
    INSERT INTO causal_links (
        edge_key, signal_id, actor, ticker, action, action_channel, action_date,
        action_known_at, action_known_at_basis, cause_type, probable_cause,
        event_kind, event_key, event_date, event_known_at, event_known_at_basis,
        known_at, lead_time_days, probability, score_method, evidence,
        run_id, first_run_id, code_sha, computed_at
    ) VALUES (
        :edge_key, :signal_id, :actor, :ticker, :action, :action_channel, :action_date,
        :action_known_at, :action_known_at_basis, :cause_type, :probable_cause,
        :event_kind, :event_key, :event_date, :event_known_at, :event_known_at_basis,
        :known_at, :lead_time_days, :probability, :score_method, CAST(:evidence AS JSONB),
        :run_id, :run_id, :code_sha, :computed_at
    )
    ON CONFLICT (edge_key) DO UPDATE SET
        signal_id = EXCLUDED.signal_id,
        actor = EXCLUDED.actor,
        action_known_at = EXCLUDED.action_known_at,
        action_known_at_basis = EXCLUDED.action_known_at_basis,
        probable_cause = EXCLUDED.probable_cause,
        event_date = EXCLUDED.event_date,
        event_known_at = EXCLUDED.event_known_at,
        event_known_at_basis = EXCLUDED.event_known_at_basis,
        known_at = EXCLUDED.known_at,
        lead_time_days = EXCLUDED.lead_time_days,
        probability = EXCLUDED.probability,
        score_method = EXCLUDED.score_method,
        evidence = EXCLUDED.evidence,
        run_id = EXCLUDED.run_id,
        code_sha = EXCLUDED.code_sha,
        computed_at = EXCLUDED.computed_at
""")


def upsert_edges(
    conn: Connection, edges: Sequence[Edge], run_id: str, code_sha: str, computed_at: datetime,
) -> int:
    """Idempotent write keyed on ``edge_key``; ``first_run_id``/``created_at`` are kept."""
    if not edges:
        return 0
    rows = []
    for e in edges:
        row = e.to_row()
        row.update(run_id=run_id, code_sha=code_sha, computed_at=computed_at)
        rows.append(row)
    conn.execute(_UPSERT_SQL, rows)
    return len(rows)


def compute_batch(
    conn: Connection, tickers: Sequence[str], since: date, as_of: datetime,
) -> tuple[list[Action], list[Edge]]:
    """Read-only: canonical actions and their antecedent edges for a ticker batch."""
    actions = canonical_actions(load_action_rows(conn, tickers, since, as_of), as_of)
    earn_rows, contract_rows = load_event_rows(conn, tickers, since, as_of)
    events = [
        ev for ev in earnings_events(earn_rows) + contract_events(contract_rows)
        if ev.known_at <= as_of
    ]
    return actions, build_edges(actions, events)


def _batches(items: Sequence[str], size: int) -> Iterable[list[str]]:
    size = max(1, int(size))
    for i in range(0, len(items), size):
        yield list(items[i:i + size])


def run_causal_links(
    engine: Engine,
    *,
    days: int = 30,
    as_of: datetime | None = None,
    tickers: Sequence[str] | None = None,
    batch_size: int = 25,
    max_tickers: int = 500,
    code_sha: str = "unknown",
    dry_run: bool = False,
    keep_edges: bool = False,
) -> RunSummary:
    """Compute and (unless ``dry_run``) persist edges in bounded ticker batches.

    Each batch commits in its own transaction, so a failure keeps earlier
    batches and the run row records ``failed``. Re-running is idempotent.
    """
    as_of = _utc(as_of or datetime.now(timezone.utc))
    since = as_of.date() - timedelta(days=int(days))
    summary = RunSummary(
        run_id=uuid.uuid4().hex, as_of=as_of, code_sha=code_sha, days=int(days), dry_run=dry_run,
    )
    params = {
        "days": int(days), "tickers": list(tickers or []), "batch_size": int(batch_size),
        "max_tickers": int(max_tickers), "edge_schema": EDGE_SCHEMA_VERSION,
        "score_method": SCORE_METHOD,
    }

    with engine.connect() as conn:
        if not dry_run and not schema_ready(conn):
            raise CausalLinksSchemaMissing(
                "causal_links provenance columns / causal_link_runs missing — "
                "run `alembic upgrade head` (causal_links_provenance_20260927)"
            )
        universe = select_tickers(conn, since, as_of, max_tickers, tickers)

    if not dry_run:
        with engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO causal_link_runs (run_id, started_at, as_of, code_sha, params, status) "
                "VALUES (:rid, NOW(), :as_of, :sha, CAST(:params AS JSONB), 'running')"
            ), {"rid": summary.run_id, "as_of": as_of, "sha": code_sha, "params": json.dumps(params)})

    try:
        for batch in _batches(universe, batch_size):
            ctx = engine.connect() if dry_run else engine.begin()
            with ctx as conn:
                actions, edges = compute_batch(conn, batch, since, as_of)
                if not dry_run:
                    summary.edges_written += upsert_edges(
                        conn, edges, summary.run_id, code_sha, datetime.now(timezone.utc),
                    )
            summary.tickers_processed += len(batch)
            summary.actions_processed += len(actions)
            summary.edges_found += len(edges)
            if keep_edges:
                summary.edges.extend(edges)
            log.info(
                "causal_links batch {b}: {a} actions, {e} edges",
                b=",".join(batch[:3]) + ("…" if len(batch) > 3 else ""), a=len(actions), e=len(edges),
            )
        summary.status = "succeeded"
    except Exception as exc:
        summary.status = "failed"
        if not dry_run:
            _finish_run(engine, summary, error=str(exc)[:1000])
        raise
    if not dry_run:
        _finish_run(engine, summary)
    return summary


def _finish_run(engine: Engine, summary: RunSummary, error: str | None = None) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE causal_link_runs SET finished_at = NOW(), status = :status, "
            "tickers_processed = :t, actions_processed = :a, edges_found = :f, "
            "edges_written = :w, error = :err WHERE run_id = :rid"
        ), {
            "status": summary.status, "t": summary.tickers_processed,
            "a": summary.actions_processed, "f": summary.edges_found,
            "w": summary.edges_written, "err": error, "rid": summary.run_id,
        })


# ── Read side (SELECT only; used by GET routes) ──────────────────────────


def latest_run(conn: Connection) -> dict[str, Any] | None:
    row = conn.execute(text(
        "SELECT run_id, as_of, finished_at, code_sha, edges_written, tickers_processed "
        "FROM causal_link_runs WHERE status = 'succeeded' "
        "ORDER BY finished_at DESC NULLS LAST LIMIT 1"
    )).mappings().fetchone()
    if not row:
        return None
    return {
        "run_id": row["run_id"],
        "as_of": row["as_of"].isoformat() if row["as_of"] else None,
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
        "code_sha": row["code_sha"],
        "edges_written": row["edges_written"],
        "tickers_processed": row["tickers_processed"],
    }


def read_links(
    conn: Connection, *, ticker: str | None, since: date, limit: int = 200,
) -> list[dict[str, Any]]:
    """Persisted provenance-bearing edges (legacy rows without edge_key are excluded)."""
    params: dict[str, Any] = {"since": since, "lim": int(limit)}
    sql = (
        "SELECT id, edge_key, signal_id, actor, ticker, action, action_channel, action_date, "
        "       action_known_at, action_known_at_basis, cause_type, probable_cause, event_kind, "
        "       event_key, event_date, event_known_at, event_known_at_basis, known_at, "
        "       lead_time_days, probability, score_method, evidence, run_id, first_run_id, "
        "       code_sha, computed_at "
        "FROM causal_links "
        "WHERE edge_key IS NOT NULL AND known_at <= NOW() AND action_date >= :since "
    )
    if ticker:
        sql += "AND ticker = :ticker "
        params["ticker"] = ticker.strip().upper()
    sql += "ORDER BY action_date DESC, probability DESC NULLS LAST, id LIMIT :lim"
    return _mappings(conn, sql, params)


def _iso(v: Any) -> str | None:
    if v is None:
        return None
    return v.isoformat() if hasattr(v, "isoformat") else str(v)


def serialize_link(row: dict[str, Any]) -> dict[str, Any]:
    """One persisted edge in the shape both views read."""
    evidence = row.get("evidence")
    if isinstance(evidence, str):
        try:
            evidence = json.loads(evidence)
        except ValueError:
            evidence = []
    score = None if row.get("probability") is None else float(row["probability"])
    lead = None if row.get("lead_time_days") is None else float(row["lead_time_days"])
    action_txt = f"{row.get('actor') or 'Unknown'} {row.get('action') or ''} {row.get('ticker') or ''}".strip()
    return {
        "id": str(row.get("id")),
        "edge_key": row.get("edge_key"),
        "ticker": row.get("ticker"),
        "actor": row.get("actor"),
        "action": row.get("action"),
        "action_channel": row.get("action_channel"),
        "action_date": _iso(row.get("action_date")),
        "action_known_at": _iso(row.get("action_known_at")),
        "action_known_at_basis": row.get("action_known_at_basis"),
        "cause_type": row.get("cause_type"),
        "probable_cause": row.get("probable_cause"),
        "event_kind": row.get("event_kind"),
        "event_key": row.get("event_key"),
        "event_date": _iso(row.get("event_date")),
        "event_known_at": _iso(row.get("event_known_at")),
        "event_known_at_basis": row.get("event_known_at_basis"),
        "known_at": _iso(row.get("known_at")),
        "lead_time_days": lead,
        "score": score,
        "probability": score,  # legacy field name; heuristic, see score_method
        "score_method": row.get("score_method"),
        "score_is_probability": False,
        "evidence": evidence if isinstance(evidence, list) else [],
        "run_id": row.get("run_id"),
        "first_run_id": row.get("first_run_id"),
        "code_sha": row.get("code_sha"),
        "computed_at": _iso(row.get("computed_at")),
        # Timeline arrow: event (earlier) -> the trade (later).
        "cause_signal_id": None if row.get("signal_id") is None else str(row["signal_id"]),
        "cause_date": _iso(row.get("event_date")),
        "cause_description": row.get("probable_cause"),
        "effect_date": _iso(row.get("action_date")),
        "effect_description": action_txt,
        "lever_actor": row.get("actor") or "Unknown",
        "effect_ticker": row.get("ticker"),
    }


NOT_GENERATED_REASON = (
    "No causal-link run has completed: scripts/run_causal_links.py is not scheduled "
    "(timer template deploy/systemd/grid-causal-links.timer.template is not installed)."
)


def read_links_payload(
    engine: Engine, *, ticker: str | None, days: int, limit: int = 200,
) -> dict[str, Any]:
    """SELECT-only payload with as-of labels; never creates tables."""
    since = datetime.now(timezone.utc).date() - timedelta(days=int(days))
    base: dict[str, Any] = {
        "claim": "public event preceded the trade; not proof of cause",
        "score_method": SCORE_METHOD,
        "score_is_probability": False,
    }
    with engine.connect() as conn:
        if not schema_ready(conn):
            return {**base, "links": [], "generated": False, "as_of": None,
                    "last_run": None, "reason": NOT_GENERATED_REASON}
        run = latest_run(conn)
        rows = read_links(conn, ticker=ticker, since=since, limit=limit)
    if run is None:
        return {**base, "links": [], "generated": False, "as_of": None,
                "last_run": None, "reason": NOT_GENERATED_REASON}
    return {**base, "links": [serialize_link(r) for r in rows], "generated": True,
            "as_of": run["as_of"], "last_run": run, "reason": None}
