"""The daily desk brief (3pacs/GRID#1164): one pre-open and one post-close brief per XNYS session.

Deterministic. The brief is assembled from records the desk already keeps (``analysis/desk_brief_sources.py``
reads them); this module only fixes the clock, labels what it was given and renders it. It computes no metric,
promotes nothing and places no order: every figure in a brief is a count, a registered counter or a value copied
from a record, with the record's own clock.

Clock (point in time). Each brief has one as-of instant, stated at its top:

* ``pre_open``: 30 minutes before the session's open (09:00 ET on a regular day);
* ``post_close``: 22:20 America/New_York on the session's day (after the 21:30-21:40 UTC shadow graders and the
  evening text in winter; out of the 02:00-02:15 UTC Hermes window in both seasons).

A caller may pass another instant (``--as-of``, for a run by hand); the brief then says so. A source whose newest
clock is later than the as-of is not used: its section reads ``after_clock`` and its figures are left out. A source
with no clock reads ``unknown``, never ``ok``. Nothing here turns a missing source into "nothing happened".

Labels. A forward trial's line goes through ``analysis.desk_evidence.evidence_card``: "unproven, n of N" until its
registered look, whatever n is. The brief never shows an interim outcome (hit rate, mean, P&L) for a trial: it
shows counts toward the look and, for the session, what the trial decided or why it withheld. Graded results in R
come only from the paper record (D4, 3pacs/GRID#14), whose rows carry their own disclosure rule.

Wording. ``narrate`` may ask the local GPU model (llama.cpp on gridz4, the host ``selector-shadow`` uses) for a
short plain-English summary of the deterministic text. It never goes through ``llm.router`` (whose chains can
reach paid providers when ``GRID_ALLOW_PAID_LLM`` is set), refuses any host outside ``LOCAL_LLM_HOSTS``, and drops
the wording when any number in it is not in the deterministic text (``ollama.number_grounding``) or when it uses a
word that would state a finding. The deterministic brief is complete without it.
"""

from __future__ import annotations

import json
import re
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from analysis.desk_evidence import DeskEvidence, EvidenceKind, evidence_card

SCHEMA = "desk-brief/1"
NY = ZoneInfo("America/New_York")
KINDS = ("pre_open", "post_close")
PRE_OPEN_LEAD = timedelta(minutes=30)
POST_CLOSE_AT = time(22, 20)
# A live endpoint (the Records page, the GEX site state) can only be read when the brief runs. A read up to
# LIVE_GRACE after the brief's clock is used and the section says how late it was; a later read (a brief made by
# hand long after its clock) is after_clock like any other source.
LIVE_GRACE = timedelta(minutes=10)
STATUSES = ("ok", "empty", "unknown", "after_clock", "stale", "not_supplied")
LOCAL_LLM_HOSTS = frozenset({"gridz4:8080", "127.0.0.1:18081"})
# Words the wording may not use: each would state a result the brief does not contain.
FINDING_WORDS = re.compile(
    r"\b(edge|alpha|proven|profitable|significant|validated|confirmed|outperform\w*|guarantee\w*|"
    r"recommend\w*|should (?:buy|sell)|buy now|sell now)\b",
    re.IGNORECASE,
)


def _aware(value: datetime, what: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError(f"{what} must be a timezone-aware datetime")
    return value.astimezone(UTC)


def parse_clock(value: Any) -> datetime | None:
    """An ISO clock as aware UTC; a naive clock is refused (None), never assumed."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.utcoffset() is not None else None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    try:
        s = str(value).strip().replace("Z", "+00:00")
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return d.astimezone(UTC) if d.utcoffset() is not None else None


def iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z") if value else None


@dataclass(frozen=True)
class SessionTimes:
    day: date
    opens: datetime
    closes: datetime


def default_as_of(kind: str, session: SessionTimes) -> datetime:
    """The brief's registered clock for its kind (see the module docstring)."""
    if kind == "pre_open":
        return session.opens - PRE_OPEN_LEAD
    if kind == "post_close":
        return datetime.combine(session.day, POST_CLOSE_AT, tzinfo=NY).astimezone(UTC)
    raise ValueError(f"brief kind must be one of {KINDS}")


@dataclass(frozen=True)
class Section:
    """One source's contribution. ``lines`` are the deterministic text; ``facts`` the same, machine-readable."""

    key: str
    title: str
    status: str
    source: str
    source_clock: datetime | None = None
    lines: tuple[str, ...] = ()
    facts: dict = field(default_factory=dict)
    problems: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in STATUSES:
            raise ValueError(f"unknown section status {self.status!r}")
        if self.source_clock is not None:
            _aware(self.source_clock, "source_clock")

    def to_dict(self) -> dict:
        return {"key": self.key, "title": self.title, "status": self.status, "source": self.source,
                "source_clock": iso(self.source_clock), "lines": list(self.lines), "facts": self.facts,
                "problems": list(self.problems)}


@dataclass(frozen=True)
class Trial:
    """A forward record: what it decided for the session, and its counts toward its one look."""

    trial_id: str
    name: str
    source: str
    registration: str | None
    have: int | None
    need: int | None
    unit: str
    today: str | None = None           # what it decided for the session ("traded", a choice), None when no row
    withheld: str | None = None        # why it did not decide or does not count, from its own record
    status: str | None = None          # the source's own look status (INTERIM, LOOK DUE, SPENT, ...)
    clock: datetime | None = None      # newest clock of the rows used
    decides: str | None = None         # when it decides (pre-open: not yet decided)
    max_age_days: int = 5
    live: bool = False                 # read from a live endpoint at run time (see LIVE_GRACE)
    daily: bool = True                 # False: the source keeps counts only, no per-session row
    counts_in: str | None = None       # its look counts are kept under another record (named here)

    def card(self, as_of: datetime) -> dict:
        """The desk_evidence label at the brief's clock. No counts or no clock: labelled, never a finding."""
        if self.status == "SPENT":
            return {"label": "look spent; its result is on the Records page, not repeated here", "reasons": []}
        if self.counts_in:
            return {"label": f"counts kept under {self.counts_in}", "reasons": []}
        if self.have is None or self.need is None or self.need <= 0:
            return {"label": "unproven; registered count not supplied", "reasons": ["count_not_supplied"]}
        item = DeskEvidence(item_id=self.trial_id[:512], kind=EvidenceKind.FORWARD, receipt_id=self.source[:512],
                            observed_at=self.clock, available_at=self.clock, samples=max(0, int(self.have)),
                            required_samples=int(self.need))
        card = evidence_card(item, as_of=as_of, max_age=timedelta(days=self.max_age_days))
        return {"label": card["label"], "reasons": list(card.get("reasons") or card.get("withheld_reasons") or [])}

    def to_dict(self, as_of: datetime) -> dict:
        return {"id": self.trial_id, "name": self.name, "source": self.source, "registration": self.registration,
                "have": self.have, "need": self.need, "unit": self.unit, "today": self.today,
                "withheld": self.withheld, "status": self.status, "clock": iso(self.clock),
                "decides": self.decides, "daily": self.daily, "evidence": self.card(as_of)}


def clocked(section: Section, as_of: datetime, live_grace: timedelta = LIVE_GRACE) -> Section:
    """Apply the brief's clock to a section: later than the as-of is ``after_clock`` and loses its figures."""
    if section.status in ("unknown", "not_supplied", "after_clock", "empty"):
        return section
    if section.source_clock is None:
        return Section(section.key, section.title, "unknown", section.source, None, (),
                       {}, section.problems + ("the source carries no clock; not used",))
    late = section.source_clock - as_of
    if section.facts.get("live") and timedelta(0) < late <= live_grace:
        return Section(section.key, section.title, section.status, section.source, section.source_clock,
                       section.lines + (f"read live {int(late.total_seconds())} s after the brief's clock",),
                       section.facts, section.problems)
    if section.source_clock > as_of:
        return Section(section.key, section.title, "after_clock", section.source, section.source_clock, (),
                       {}, section.problems + (f"written at {iso(section.source_clock)}, after the brief's clock; not used",))
    return section


def trial_clocked(trial: Trial, as_of: datetime, live_grace: timedelta = LIVE_GRACE) -> Trial:
    """A trial row read after the brief's clock is not used (its counts and decision are dropped)."""
    if trial.live and trial.clock is not None and trial.clock - as_of <= live_grace:
        return trial
    if trial.clock is not None and trial.clock > as_of:
        return Trial(trial.trial_id, trial.name, trial.source, trial.registration, None, trial.need, trial.unit,
                     None, f"read at {iso(trial.clock)}, after the brief's clock; not used", trial.status, None,
                     trial.decides, trial.max_age_days, trial.live, trial.daily, trial.counts_in)
    return trial


def build_brief(kind: str, session: SessionTimes, as_of: datetime, sections: Iterable[Section],
                trials: Iterable[Trial], *, as_of_registered: bool, generated_at: datetime,
                code_version: str | None = None) -> dict:
    """Assemble one brief. Pure: the caller read the sources; this applies the clock and labels."""
    if kind not in KINDS:
        raise ValueError(f"brief kind must be one of {KINDS}")
    as_of = _aware(as_of, "as_of")
    generated_at = _aware(generated_at, "generated_at")
    if generated_at < as_of:
        raise ValueError("a brief cannot be generated before its own clock")
    secs = [clocked(s, as_of) for s in sections]
    trs = sorted((trial_clocked(t, as_of) for t in trials), key=lambda t: (t.source, t.trial_id))
    problems: list[dict] = []
    for s in secs:
        if s.status != "ok":
            problems.append({"source": s.key, "status": s.status, "detail": "; ".join(s.problems) or s.status})
        else:
            problems.extend({"source": s.key, "status": "reported", "detail": p} for p in s.problems)
    for t in trs:
        reasons = t.card(as_of)["reasons"]
        bad = [r for r in reasons if r in ("stale_observation", "missing_clock", "future_evidence")
               and not (r == "missing_clock" and t.have == 0)]   # nothing recorded yet is not a clock fault
        if bad:
            problems.append({"source": t.trial_id, "status": "trial", "detail": ", ".join(bad)})
    return {
        "schema": SCHEMA,
        "kind": kind,
        "session": session.day.isoformat(),
        "session_opens": iso(session.opens),
        "session_closes": iso(session.closes),
        "as_of": iso(as_of),
        "as_of_rule": "registered" if as_of_registered else "given by hand",
        "generated_at": iso(generated_at),
        "code_version": code_version,
        "rule": ("Counts and registered statistics only. Every figure is copied from a record dated at or before "
                 "as_of. 'Unproven, n of N' is a count toward a trial's one look, not a result. Paper only: "
                 "nothing here is an order."),
        "sections": [s.to_dict() for s in secs],
        "trials": [t.to_dict(as_of) for t in trs],
        "data_problems": problems,
        "narrative": None,
    }


# ── rendering ────────────────────────────────────────────────────────────────

def _ny(value: str | None) -> str:
    d = parse_clock(value)
    return d.astimezone(NY).strftime("%a %d %b %H:%M ET") if d else "unknown"


def render_markdown(brief: dict) -> str:
    """The brief as plain Markdown, deterministic for a given brief dict."""
    title = "Pre-open" if brief["kind"] == "pre_open" else "Post-close"
    out = [f"# Desk brief: {title}, session {brief['session']}", ""]
    out.append(f"As of **{brief['as_of']}** ({_ny(brief['as_of'])}; clock {brief['as_of_rule']}). "
               f"Session {_ny(brief['session_opens'])} to {_ny(brief['session_closes'])}. "
               f"Generated {brief['generated_at']}.")
    out += ["", f"_{brief['rule']}_", ""]
    if brief.get("narrative") and brief["narrative"].get("text"):
        n = brief["narrative"]
        out += ["## Summary (local model wording, checked against the figures below)", "", n["text"], ""]
    for s in brief["sections"]:
        out.append(f"## {s['title']}")
        out.append("")
        clock = f", as of {s['source_clock']}" if s["source_clock"] else ""
        out.append(f"Source: `{s['source']}` ({s['status']}{clock})")
        out.append("")
        out += [f"- {line}" for line in s["lines"]] or ["- nothing to report from this source"]
        out += [f"- problem: {p}" for p in s["problems"]]
        out.append("")
    out += ["## Trials, modules and slots", "",
            "| Trial | Session | Counts toward its look | Label | Look status | Source |",
            "|---|---|---|---|---|---|"]
    for t in brief["trials"]:
        if not t.get("daily", True):
            day = "counts only (no per-session row)"
        elif t["today"]:
            day = t["today"]
        elif t["withheld"]:
            day = f"withheld: {t['withheld']}"
        elif t["decides"]:
            day = f"decides {t['decides']}"
        else:
            day = "no row for the session"
        if t["today"] and t["withheld"]:
            day += f" ({t['withheld']})"
        have = "?" if t["have"] is None else t["have"]
        need = "?" if t["need"] is None else t["need"]
        counts = f"{have} of {need} {t['unit']}".strip()
        if t["have"] is None and t["need"] is None and t["evidence"]["label"].startswith("counts kept under"):
            counts = "see the record named in Label"
        out.append(f"| {_cell(t['name'])} | {_cell(day)} | {_cell(counts)} | {_cell(t['evidence']['label'])} | "
                   f"{_cell(t['status'] or '')} | `{_cell(t['source'])}` |")
    out += ["", "## Known data problems", ""]
    out += [f"- {p['source']} ({p['status']}): {p['detail']}" for p in brief["data_problems"]] or ["- none found"]
    out.append("")
    return "\n".join(out)


def _cell(value: Any) -> str:
    return str(value).replace("|", "/").replace("\n", " ")


# ── optional wording (local GPU model only) ──────────────────────────────────

def _host(url: str) -> str:
    p = urllib.parse.urlsplit(url)
    return f"{p.hostname}:{p.port}" if p.hostname and p.port else ""


def narrate(brief: dict, *, url: str, model: str, timeout: float = 120.0,
            post: Callable[[str, bytes, float], bytes] | None = None) -> dict:
    """Ask the local model for 3-5 plain sentences. Returns the narrative block; never raises on a model failure.

    The wording is kept only when every number in it appears in the deterministic text and it uses no word that
    states a finding. Otherwise the block records why it was withheld and the brief stands without it.
    """
    host = _host(url)
    if host not in LOCAL_LLM_HOSTS:
        return {"text": None, "withheld": f"host {host or url!r} is not a local GPU model host", "model": model}
    basis = render_markdown({**brief, "narrative": None})
    prompt = ("Summarise this trading-desk brief in 3 to 5 plain sentences for its owner. Use only facts and numbers "
              "that appear in it, and copy every number exactly as written, sign included (write -2.2%, not 'fell 2.2%'). "
              "Do not predict, advise, rank or judge any trial; a count toward a look is not a "
              "result. Say plainly what is missing or withheld.\n\n" + basis)
    body = json.dumps({"model": model, "temperature": 0, "max_tokens": 400,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    send = post or _post
    try:
        raw = send(url.rstrip("/") + "/v1/chat/completions", body, timeout)
        text = json.loads(raw)["choices"][0]["message"]["content"]
    except Exception as exc:  # noqa: BLE001 - the deterministic brief must stand without the model
        return {"text": None, "withheld": f"model call failed ({type(exc).__name__})", "model": model, "host": host}
    text = re.sub(r"<think>.*?</think>", "", text or "", flags=re.DOTALL).strip()
    if not text:
        return {"text": None, "withheld": "model returned no text", "model": model, "host": host}
    word = FINDING_WORDS.search(text)
    if word:
        return {"text": None, "withheld": f"wording used {word.group(0)!r}, which states a finding", "model": model,
                "host": host}
    from ollama.number_grounding import check_numbers_grounded

    g = check_numbers_grounded(text, basis)
    if g.ungrounded:
        return {"text": None, "withheld": f"{len(g.ungrounded)} number(s) not in the brief", "model": model,
                "host": host}
    return {"text": text, "withheld": None, "model": model, "host": host, "numbers_checked": g.total_numbers}


def _post(url: str, body: bytes, timeout: float) -> bytes:
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()
