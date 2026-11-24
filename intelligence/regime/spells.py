"""Spells and transitions of the market-state record (rule market-state-spells/1, #1309).

The record gives each variable a label per session (normal, above, below, high, low; ``baseline.py``) but no memory:
no change of label, and no time in the current state. This module derives both from the labels already stored in
``market_state_daily``, so a spell always agrees with the stored labels. It adds no reading of its own.

* **Sessions** are the pinned XNYS sessions (``market_state.xnys_sessions``), never the approximate scheduler helpers.
  A stored row on a day that is not a session (2025-01-09, the national day of mourning, was backfilled) is ignored,
  and a session with no row is ``missing_row``.
* **Eligible readings.** A label spell counts only sessions whose reading is ``ok``. A ``stale``, ``unavailable`` or
  ``no_baseline`` reading, or a missing row, neither starts nor ends a spell. Inside a spell such sessions are counted
  as ``ineligible_sessions``; the ones after the last eligible session are ``trailing_ineligible_sessions``, and a
  change of label across them is logged with that ``gap_sessions``.
* **Transitions.** A transition is logged at the first eligible session whose label differs from the previous eligible
  label: variable, from, to, date, observation date, z, status, and the length of the spell it ended.
* **Observations vs sessions.** A weekly or monthly reading repeats one observation across many sessions, so each
  spell reports both its ``spell_sessions`` and its distinct ``observations``.
* **Left censoring.** A spell that starts at a variable's first eligible reading began at least that early;
  ``left_censored`` says so and its length is a lower bound.
* **Named spells** follow a condition on a value rather than a label: VIX above VIX3M, SPY below its gamma flip, and
  HY credit above normal. A value condition counts ``ok`` and ``no_baseline`` readings (the value is fresh, it only
  lacks a baseline); ``stale`` and ``unavailable`` never count.
* **Origin.** A row the decision writer stored in daily mode with an explicit ``dry_run: false``
  (``writer_provenance``) is ``daily_write``. That is the writer's mode, not proof that the timer ran it: the record's
  ``execution_origin`` stays ``unverified``, and an operator running the default command by hand writes the same row.
  Every other row is ``reconstruction``: the 2026-11-13 backfill without provenance, ranges, explicit sessions,
  replacements, and any row whose provenance lacks ``dry_run`` (per the 2026-11-13 note on #1191). Each transition
  names the origin of the row it was read in, and each spell counts its sessions by origin.
* **Stored vs recomputed.** A block has a ``source``. ``writer`` is the block the decision writer stored in the
  record: the rows of ``market_state_daily`` as that run read them, then the records the run wrote (or, in a dry
  run, would write) for earlier targets, then the record itself. ``recomputed`` is any later derivation, such as
  ``scripts/market_state_spells.py``. It selects the current stored rows by session date, not by when each row was
  written: a row backfilled or replaced after a session's own record is folded in. So a recomputed block is a
  reconstruction from today's store, not what was known that evening, and its ``lineage`` counts those rows.

The decision writer (``scripts/run_market_state.py``) stores the block in each new record, with that session's
transitions, so each run appends to the log; ``scripts/market_state_spells.py`` reads the stored log back
(read-only), recomputes it from the current store (covering rows stored before spells existed), and checks stored
blocks against a recomputation. A provisional record (#1312) carries no spells: at a session's close the previous
session's decision record is not written yet, and a display-only row never advances a spell.

No hysteresis here: a spell changes exactly when the stored label does. Confirmation rules for regimes are #1307.
"""

from __future__ import annotations

import json
import math
from bisect import bisect_left, bisect_right
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, NamedTuple

from loguru import logger as log
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from intelligence.regime.market_state import xnys_sessions

SPELL_RULE = "market-state-spells/1"
LABEL = "label"
NAMED = "named"
DAILY_WRITE = "daily_write"          # stored by the decision writer in daily mode, dry_run explicitly false
RECONSTRUCTION = "reconstruction"
WRITER = "writer"                    # block source: stored by the decision writer with its record
RECOMPUTED = "recomputed"            # block source: derived later from the current store
_SOURCE_DETAIL = {
    WRITER: "the rows of market_state_daily as this run read them before its first target, then the records this "
            "run stored (in a dry run, would store) for earlier targets, then this record",
    RECOMPUTED: "the rows of market_state_daily as stored when they were read, selected by session date "
                "(as_of_date <= as_of), not by when each row was written: a row backfilled or replaced after this "
                "session's own record is folded in, so this is a reconstruction from the current store, not what "
                "was known that evening",
}
ELIGIBLE_LABEL_STATUS = frozenset({"ok"})
ELIGIBLE_VALUE_STATUS = frozenset({"ok", "no_baseline"})
_KEEP = ("status", "label", "value", "robust_z", "obs_date")   # the only fields a spell reads


@dataclass(frozen=True)
class NamedSpell:
    """A spell on one variable's value (``on``/``off``) or on its label set."""

    name: str
    variable: str
    description: str
    above: float | None = None          # on when value > above
    below: float | None = None          # on when value < below
    labels: frozenset[str] | None = None  # on when the label is one of these (label basis)

    def reading(self, entry: dict[str, Any] | None) -> tuple[str | None, str]:
        """(``on``/``off``, "") for an eligible reading, else (None, why not)."""
        if not isinstance(entry, dict):
            return None, "missing"
        status = entry.get("status")
        if self.labels is not None:
            if status not in ELIGIBLE_LABEL_STATUS:
                return None, status or "missing"
            label = entry.get("label")
            if label is None:
                return None, "no_label"
            return ("on" if label in self.labels else "off"), ""
        if status not in ELIGIBLE_VALUE_STATUS:
            return None, status or "missing"
        value = entry.get("value")
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
            return None, "no_value"
        on = (self.above is not None and value > self.above) or (self.below is not None and value < self.below)
        return ("on" if on else "off"), ""


NAMED_SPELLS: tuple[NamedSpell, ...] = (
    NamedSpell("vix_above_vix3m", "vix_term", "VIX above VIX3M (VIX / VIX3M > 1): near-term fear priced above "
               "three-month", above=1.0),
    NamedSpell("spot_below_gamma_flip", "spot_vs_gamma_flip", "SPY below its dealer gamma flip "
               "(spot_vs_gamma_flip < 0)", below=0.0),
    NamedSpell("hy_spread_above_normal", "hy_spread", "HY credit spread labelled above or high",
               labels=frozenset({"above", "high"})),
)
_NAMED = {s.name: s for s in NAMED_SPELLS}


def label_reading(entry: dict[str, Any] | None) -> tuple[str | None, str]:
    """(label, "") for an eligible reading, else (None, why not)."""
    if not isinstance(entry, dict):
        return None, "missing"
    status = entry.get("status")
    if status not in ELIGIBLE_LABEL_STATUS:
        return None, status or "missing"
    label = entry.get("label")
    if label is None:
        return None, "no_label"
    return label, ""


def row_origin(record: dict[str, Any]) -> str:
    """``daily_write`` for a daily-mode row whose provenance says ``dry_run: false``; ``reconstruction`` otherwise.

    A missing or non-boolean ``dry_run`` is reconstruction. ``daily_write`` names the writer's mode only; whether the
    timer ran it is not recorded (``execution_origin`` is ``unverified``).
    """
    prov = record.get("writer_provenance")
    if not isinstance(prov, dict):
        return RECONSTRUCTION
    return DAILY_WRITE if prov.get("mode") == "daily" and prov.get("dry_run") is False else RECONSTRUCTION


@dataclass(frozen=True)
class SpellRow:
    """The parts of one stored record a spell reads."""

    as_of: date
    rule: str
    origin: str
    variables: dict[str, dict[str, Any]]
    computed_at: datetime | None = None   # when the stored row was last written; None for a record not stored yet

    @classmethod
    def from_record(cls, as_of: date, rule: str, record: dict[str, Any],
                    computed_at: datetime | None = None) -> SpellRow:
        variables = {name: {k: entry.get(k) for k in _KEEP}
                     for name, entry in (record.get("variables") or {}).items() if isinstance(entry, dict)}
        return cls(as_of=as_of, rule=rule, origin=row_origin(record), variables=variables, computed_at=computed_at)


@dataclass
class _Spell:
    state: str | None = None
    since: date | None = None
    last: date | None = None
    sessions: int = 0
    observations: set[str] = field(default_factory=set)
    ineligible: int = 0
    pending: int = 0                 # ineligible sessions since the last eligible one
    left_censored: bool = True
    basis: Counter = field(default_factory=Counter)
    previous: dict[str, Any] | None = None
    at_end: str = "missing"          # the last folded session's reading: "eligible" or why not

    def ended(self) -> dict[str, Any]:
        return {"state": self.state, "since": self.since.isoformat(), "last_eligible": self.last.isoformat(),
                "spell_sessions": self.sessions, "observations": len(self.observations),
                "ineligible_sessions": self.ineligible, "left_censored": self.left_censored}

    def summary(self) -> dict[str, Any]:
        if self.state is None:
            return {"state": None, "as_of_reading": self.at_end, "reason": "no eligible reading yet"}
        out = {**self.ended(), "trailing_ineligible_sessions": self.pending, "as_of_reading": self.at_end,
               "basis": {DAILY_WRITE: self.basis[DAILY_WRITE], RECONSTRUCTION: self.basis[RECONSTRUCTION]}}
        if self.previous is not None:
            out["previous"] = self.previous
        return out


@dataclass
class Fold:
    """Every spell after the last session folded, and every transition on the way."""

    first: date | None = None
    last: date | None = None
    spells: dict[tuple[str, str], _Spell] = field(default_factory=dict)
    transitions: list[dict[str, Any]] = field(default_factory=list)
    ignored_rows: list[date] = field(default_factory=list)
    missing_sessions: list[date] = field(default_factory=list)


def _start(sp: _Spell, state: str, day: date, obs: Any, origin: str, *, left_censored: bool) -> None:
    sp.state, sp.since, sp.last, sp.sessions = state, day, day, 1
    sp.observations, sp.ineligible, sp.pending = {str(obs)}, 0, 0
    sp.left_censored, sp.basis = left_censored, Counter({origin: 1})


def fold(rows: Iterable[SpellRow], sessions: tuple[date, ...]) -> Fold:
    """Fold stored rows over the pinned sessions from the first row to the last.

    ``sessions`` is the full ordered session list (``xnys_sessions()[2]``). Two rows for one day raise ValueError.
    """
    by_day: dict[date, SpellRow] = {}
    out = Fold()
    for row in rows:
        i = bisect_left(sessions, row.as_of)
        if i == len(sessions) or sessions[i] != row.as_of:
            out.ignored_rows.append(row.as_of)
            continue
        if row.as_of in by_day:
            raise ValueError(f"two market-state rows for {row.as_of}")
        by_day[row.as_of] = row
    out.ignored_rows.sort()
    if not by_day:
        return out
    out.first, out.last = min(by_day), max(by_day)
    names = sorted({name for row in by_day.values() for name in row.variables})
    out.spells = {**{(LABEL, n): _Spell() for n in names}, **{(NAMED, s.name): _Spell() for s in NAMED_SPELLS}}
    for day in sessions[bisect_left(sessions, out.first):bisect_right(sessions, out.last)]:
        row = by_day.get(day)
        if row is None:
            out.missing_sessions.append(day)
        for (kind, name), sp in out.spells.items():
            variable = name if kind == LABEL else _NAMED[name].variable
            entry = None if row is None else row.variables.get(variable)
            if row is None:
                state, why = None, "missing_row"
            elif kind == LABEL:
                state, why = label_reading(entry)
            else:
                state, why = _NAMED[name].reading(entry)
            if state is None:
                sp.at_end = why
                if sp.state is not None:
                    sp.pending += 1
                continue
            sp.at_end = "eligible"
            obs = entry.get("obs_date")
            if sp.state is None:
                _start(sp, state, day, obs, row.origin, left_censored=True)
            elif state == sp.state:
                sp.sessions += 1
                sp.ineligible += sp.pending
                sp.pending = 0
                sp.observations.add(str(obs))
                sp.basis[row.origin] += 1
                sp.last = day
            else:
                ended = sp.ended()
                out.transitions.append({
                    "kind": kind, "name": name, "variable": variable, "as_of": day.isoformat(),
                    "from": sp.state, "to": state, "obs_date": obs, "value": entry.get("value"),
                    "robust_z": entry.get("robust_z"), "status": entry.get("status"), "gap_sessions": sp.pending,
                    "ended_spell": ended, "origin": row.origin, "record_rule": row.rule,
                })
                sp.previous = ended
                _start(sp, state, day, obs, row.origin, left_censored=False)
    return out


def spells_block(rows: Iterable[SpellRow], as_of: date, *, source: str = RECOMPUTED) -> dict[str, Any]:
    """The ``spells`` block of the record for ``as_of``: every spell at that session and its transitions.

    ``rows`` must end with the record for ``as_of`` itself; rows after it are ignored. ``source`` says where the rows
    came from: only the decision writer passes ``writer``; anything else is ``recomputed``.
    """
    if source not in _SOURCE_DETAIL:
        raise ValueError(f"unknown spells source {source!r}")
    start, end, sessions, version = xnys_sessions()
    base = {"rule": SPELL_RULE, "as_of": as_of.isoformat(), "calendar": version, "source": source,
            "source_detail": _SOURCE_DETAIL[source]}
    i = bisect_left(sessions, as_of)
    if i == len(sessions) or sessions[i] != as_of:
        return {**base, "status": "unavailable",
                "reason": f"{as_of} is not a session in the pinned XNYS calendar ({start}..{end})"}
    kept = [r for r in rows if r.as_of <= as_of]
    if not any(r.as_of == as_of for r in kept):
        return {**base, "status": "unavailable", "reason": f"no record for {as_of} among the rows"}
    f = fold(kept, sessions)
    day = as_of.isoformat()
    return {
        **base, "status": "ok",
        "basis": "stored labels of market_state_daily on the pinned XNYS sessions; ok readings only for label "
                 "spells, ok or no_baseline for value spells; stale, unavailable or missing sessions neither start "
                 "nor end a spell",
        "first_session": f.first.isoformat(),
        "missing_sessions": len(f.missing_sessions),
        "ignored_non_session_rows": [d.isoformat() for d in f.ignored_rows],
        "variables": {name: sp.summary() for (kind, name), sp in f.spells.items() if kind == LABEL},
        "named": {name: {"variable": _NAMED[name].variable, "description": _NAMED[name].description, **sp.summary()}
                  for (kind, name), sp in f.spells.items() if kind == NAMED},
        "transitions": [t for t in f.transitions if t["as_of"] == day],
    }


# ── Reading stored rows (never creates or writes a table) ─────────────────

_ROWS_BEFORE_SQL = text("SELECT as_of_date, rule, record, computed_at FROM market_state_daily "
                        "WHERE as_of_date < :d ORDER BY as_of_date")
_ROWS_THROUGH_SQL = text("SELECT as_of_date, rule, record, computed_at FROM market_state_daily "
                         "WHERE as_of_date <= :d ORDER BY as_of_date")


class StoredRecord(NamedTuple):
    """One stored decision row. ``computed_at`` is when its current version was written (a replace resets it)."""

    as_of: date
    rule: str
    record: dict[str, Any]
    computed_at: datetime | None

    def spell_row(self) -> SpellRow:
        return SpellRow.from_record(self.as_of, self.rule, self.record, self.computed_at)


def _day(raw: Any) -> date:
    return raw if isinstance(raw, date) else date.fromisoformat(str(raw)[:10])


def _ts(raw: Any) -> datetime | None:
    if raw is None or isinstance(raw, datetime):
        return raw
    return datetime.fromisoformat(str(raw))


def load_records(engine: Engine, *, before: date | None = None,
                 through: date | None = None) -> list[StoredRecord]:
    """Stored decision rows, oldest first, read-only. Exactly one bound is required."""
    if (before is None) == (through is None):
        raise ValueError("pass exactly one of before= or through=")
    if not inspect(engine).has_table("market_state_daily"):
        return []
    sql, bound = (_ROWS_BEFORE_SQL, before) if before is not None else (_ROWS_THROUGH_SQL, through)
    with engine.connect() as conn:
        if conn.dialect.name == "postgresql":
            conn.execute(text("SET TRANSACTION READ ONLY"))
        rows = conn.execute(sql, {"d": bound}).fetchall()
    return [StoredRecord(_day(d), rule, rec if isinstance(rec, dict) else json.loads(rec), _ts(ca))
            for d, rule, rec, ca in rows]


def load_rows(engine: Engine, *, before: date | None = None, through: date | None = None) -> list[SpellRow]:
    return [r.spell_row() for r in load_records(engine, before=before, through=through)]


def recompute_lineage(rows: Iterable[SpellRow], as_of: date) -> dict[str, Any]:
    """How far a block recomputed at ``as_of`` from stored rows is from what that session's own record could see.

    Counts the earlier rows whose current version was written after the session's own record, by a later backfill
    or a replace. Zero does not make the recomputation point-in-time: rows backfilled together (2026-11-13) were all
    written long after their sessions, and that shows in ``own_record_computed_at`` and ``own_record_origin``.
    """
    folded = [r for r in rows if r.as_of <= as_of]
    own = next((r for r in folded if r.as_of == as_of), None)
    stamps = [r.computed_at for r in folded if r.computed_at is not None]
    after = None
    if own is not None and own.computed_at is not None:
        after = sum(1 for r in folded
                    if r.as_of < as_of and r.computed_at is not None and r.computed_at > own.computed_at)
    return {
        "rows_folded": len(folded),
        "own_record_computed_at": own.computed_at.isoformat() if own is not None and own.computed_at else None,
        "own_record_origin": own.origin if own is not None else None,
        "earlier_rows_written_after_own_record": after,
        "latest_computed_at": max(stamps).isoformat() if stamps else None,
    }


def spells_unavailable(as_of: date, reason: str, *, source: str = WRITER) -> dict[str, Any]:
    return {"rule": SPELL_RULE, "as_of": as_of.isoformat(), "source": source, "status": "unavailable",
            "reason": reason}


def record_spells(prior: Iterable[SpellRow], as_of: date, rule: str, record: dict[str, Any]) -> dict[str, Any]:
    """The block for a record about to be stored: the ``prior`` rows before ``as_of``, then the record itself.

    The writer passes the stored rows, updated with each record it computes in the same run, so a range or a dry run
    sees its own earlier targets. Never raises: an error is reported in the block, and the record is stored anyway.
    """
    try:
        own = SpellRow.from_record(as_of, rule, record)
        return spells_block([*(r for r in prior if r.as_of < as_of), own], as_of, source=WRITER)
    except Exception as exc:  # spells never sink the record
        log.warning("market_state spells failed at {d}: {e}", d=as_of, e=str(exc))
        return spells_unavailable(as_of, f"error: {type(exc).__name__}")
