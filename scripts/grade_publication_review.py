#!/usr/bin/env python3
"""Grade GRID's own publications into review.jsonl — the in-repo daily review grader.

Replaces the interactive daily-review agent as the writer of
``<review_root>/<D>/review.jsonl`` (schema ``publication-review-verdicts-v1``, the
contract ``scripts/ingest_publication_review.py`` validates). Scheduled on grid-svr by
``deploy/systemd/grid-publication-review-grader.{service,timer}.template`` (11:35Z), it
reads the public screened packet tree written by ``scripts/daily_publication_packet.py``,
grades every publication, writes the review file and hands it straight to the ingest.

What it grades (v1):

* ``thesis_snapshots``: one ``direction`` claim per snapshot — the stored
  ``overall_direction`` against SPY's split-adjusted close from the last completed close
  before the snapshot to the close of the first trading day on/after ``stated_at`` +
  ``THESIS_HORIZON_DAYS`` calendar days (``intelligence/thesis_tracker.py``'s own rule and
  ±0.5 % band). Confirmed / refuted / partially_confirmed once that close exists, else
  ``unresolved`` with the horizon stated.
* ``market_briefings`` and ``market_diary``: a record whose text says the AI narrative was
  withheld or unavailable carries no forecast; it gets one ``unscorable`` ``data_quality``
  row saying so (that count is the machine-readable measure of how often the generators
  publish nothing gradeable). A record with a narrative goes through claim extraction on
  the LLM router (tier ``GRID_PUBLICATION_GRADER_TIER``, default REASON); extracted
  ``direction`` and ``level`` claims on instruments in ``INSTRUMENT_SERIES`` are graded
  deterministically from closes, ``event``/``causal`` claims stay ``unresolved`` (a causal
  claim is never confirmed from co-movement), and claims the grader cannot price are
  ``unscorable`` with the reason. Extractions are cached per publication sha under
  ``<review_root>/<D>/extractions/`` so re-grades never call the model again.

Every verdict row satisfies the ingest's rules by construction (verbatim span 8-300 chars,
sha of span, stated_at = published_at, horizon/evidence rules per verdict). Re-grading is
the normal path: a day is re-graded for ``DEFAULT_LOOKBACK_DAYS`` days so horizons that
fall after the first run resolve. The review file is rewritten only when the verdict rows
change (the header's ``graded_at`` alone never forces a rewrite); each rewrite is a new
``publication_review_runs`` row superseding the previous one. Nothing here ever writes to
``thesis_postmortems``.

Exit codes: 0 graded (or unchanged), 1 any day REFUSED/FAILED, 3 graded but the ingest
reported rejected rows (visible in its receipt).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

JOB = "grade_publication_review"
REVIEW_SCHEMA = "publication-review-verdicts-v1"
RECEIPT_SCHEMA = "publication-review-grader-receipt-v1"
GRADER_VERSION = "grid/grade_publication_review-v1"
DEFAULT_PACKET_ROOT = Path(os.environ.get(
    "GRID_PUBLICATION_PACKET_PUBLIC_ROOT",
    os.environ.get("GRID_PUBLICATION_PACKET_ROOT", "~/research/publication-packets"))).expanduser()
DEFAULT_REVIEW_ROOT = Path(os.environ.get("GRID_PUBLICATION_REVIEW_ROOT", "~/research/publication-reviews")).expanduser()
#: World-readable copy of each day's review and receipts, next to the public packet tree, so any
#: fleet agent can read it (the review root lives under the grid user's 0700 home).
DEFAULT_PUBLIC_ROOT = Path(os.environ.get("GRID_PUBLICATION_REVIEW_PUBLIC_ROOT",
                                          "~/research/publication-reviews-public")).expanduser()
PUBLIC_REVIEW_FILES = ("review.jsonl", "grader-receipt.json", "ingest-receipt.json")
DEFAULT_LOOKBACK_DAYS = 10         # longest horizon is "week" (+7 calendar days, rolled to a session: <= D+9)
DEFAULT_TIER = os.environ.get("GRID_PUBLICATION_GRADER_TIER", "reason")
DEFAULT_PROVIDER = os.environ.get("GRID_PUBLICATION_GRADER_PROVIDER") or None
MAX_FILE_BYTES = 64_000_000        # packet.json (7-24 MB measured)
MAX_REVIEW_BYTES = 8_000_000       # the ingest refuses a larger review.jsonl
MIN_SPAN_CHARS, MAX_SPAN_CHARS = 8, 300
MAX_EXTRACTED_CLAIMS = 20
EXTRACT_MAX_CHARS = 12_000
DIRECTION_BAND_PCT = 0.5           # intelligence/thesis_tracker.py: |move| <= 0.5 % is "neutral"
THESIS_HORIZON_DAYS = 3            # intelligence/thesis_tracker.py: SPY 3 days after the snapshot
PRICE_SOURCE = "TIINGO"            # raw_series writer whose :adj_close is split-adjusted (see store/observations.py)
#: Phrases the generators (ollama/market_briefing.py, intelligence/market_diary.py) put in a
#: record that carries no narrative, only a data summary.
WITHHELD_MARKERS = ("AI narrative withheld", "AI narrative unavailable", "Model response unavailable",
                    "Source evidence unavailable", "Data summary only", "no AI narrative is published")
#: Where a briefing's data tables start (ollama/market_briefing.py); the narrative sections
#: ("## What's Happening Now", "## Action", "## Tomorrow", ...) come before these.
DATA_SECTION_MARKERS = ("\n## Market Data Snapshot", "\n## Equities\n", "\n## Equity Volume", "\n## Rates\n",
                        "\n## Credit\n", "\n## Volatility\n", "\n## Commodities\n", "\n## Fx\n",
                        "\n## Equity ETF Proxies", "\n## Macro\n", "\n## Retained Feature Evidence",
                        "\n## Latest Regime", "\n### Feature evidence declarations", "\n**Unverified figures:**")
EXTRACTION_VERSION = "extract-v3"   # bump when EXTRACTION_SYSTEM or the claim schema changes: invalidates the cache
EXTRACTION_NUM_PREDICT = 1500
#: Reply shapes that are a property of (model, prompt, text) at temperature 0: calling again
#: gives the same answer, so they are cached like a result. Provider/transport faults are not.
DETERMINISTIC_EXTRACTION_ERRORS = frozenset({"empty reply", "reply is not a JSON array", "reply is not valid JSON"})
NY = ZoneInfo("America/New_York")
HEX64 = re.compile(r"[0-9a-f]{64}")
PUBLICATION_ID = re.compile(r"(market_briefings|thesis_snapshots|market_diary):([0-9]{1,9})")
CLAIM_TYPES = frozenset({"direction", "level", "event", "causal", "data_quality"})
DIRECTIONS = frozenset({"bullish", "bearish", "neutral"})
COMPARATORS = {">": lambda a, b: a > b, ">=": lambda a, b: a >= b, "<": lambda a, b: a < b, "<=": lambda a, b: a <= b}

#: Instruments the deterministic grader can price: ticker -> raw_series id of the
#: split-adjusted close. Anything else is reported ``unscorable`` with the reason.
INSTRUMENT_SERIES: dict[str, str] = {
    t: f"YF:{t}:adj_close" for t in (
        "SPY", "QQQ", "IWM", "DIA", "TLT", "IEF", "HYG", "LQD", "GLD", "SLV", "USO", "UNG",
        "XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLU", "XLB", "XLRE", "XLC",
        "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA",
    )
}
INSTRUMENT_ALIASES: dict[str, str] = {
    "S&P 500": "SPY", "S&P": "SPY", "SPX": "SPY", "^GSPC": "SPY", "ES": "SPY", "US EQUITIES": "SPY", "STOCKS": "SPY",
    "NASDAQ": "QQQ", "NASDAQ 100": "QQQ", "NDX": "QQQ", "^IXIC": "QQQ", "^NDX": "QQQ",
    "RUSSELL 2000": "IWM", "^RUT": "IWM", "DOW": "DIA", "DOW JONES": "DIA", "^DJI": "DIA",
    "TREASURIES": "TLT", "LONG BONDS": "TLT", "GOLD": "GLD", "SILVER": "SLV", "OIL": "USO", "CRUDE": "USO",
    "WTI": "USO", "NATURAL GAS": "UNG", "HIGH YIELD": "HYG", "CREDIT": "HYG",
}


#: Current-state figures the briefing generator states, mapped to the raw_series ids it reads
#: them from (ollama/market_briefing.py ``key_series``). ``pct`` marks a rate quoted with "%".
#: A narrative briefing is mostly sentences like "The VIX stands at 28.18": no forecast, but a
#: checkable statement of what GRID's data said at the publication instant.
@dataclass(frozen=True)
class Figure:
    series_id: str
    label: str
    names: str          # regex alternation, case-insensitive, matched on word boundaries
    pct: bool = False


FIGURES: tuple[Figure, ...] = (
    Figure("YF:^VIX9D:close", "VIX 9-day", r"VIX9D|9-day VIX|VIX 9-day(?: tenor)?|9-day tenor"),
    Figure("YF:^VIX3M:close", "VIX 3-month", r"VIX3M|3-month VIX|VIX 3-month(?: tenor)?"),
    Figure("YF:^VIX:close", "VIX", r"VIX(?![ -]?(?:9|3M|9-day|3-month))"),
    Figure("YF:^GSPC:close", "S&P 500", r"S&P 500|S&P|SPX|\^GSPC"),
    Figure("YF:^IXIC:close", "Nasdaq Composite", r"Nasdaq(?: Composite)?|\^IXIC"),
    Figure("YF:^DJI:close", "Dow Jones", r"Dow Jones|Dow|\^DJI"),
    Figure("YF:^RUT:close", "Russell 2000", r"Russell 2000|\^RUT"),
    Figure("YF:HYG:close", "HYG", r"HYG"),
    Figure("YF:LQD:close", "LQD", r"LQD"),
    Figure("YF:JNK:close", "JNK", r"JNK"),
    Figure("YF:EMB:close", "EMB", r"EMB"),
    Figure("YF:GLD:close", "GLD", r"GLD"),
    Figure("YF:UUP:close", "UUP", r"UUP"),
    Figure("T10Y2Y", "10Y-2Y spread", r"10Y-2Y(?: yield curve)?(?: spread)?|2s10s|10-year minus 2-year", pct=True),
    Figure("T10Y3M", "10Y-3M spread", r"10Y-3M(?: spread)?|10-year minus 3-month", pct=True),
    Figure("DFF", "fed funds rate", r"(?:effective )?fed funds(?: rate)?", pct=True),
    Figure("DGS2", "2-year Treasury yield", r"2-year (?:Treasury )?yield", pct=True),
    Figure("DGS5", "5-year Treasury yield", r"5-year (?:Treasury )?yield", pct=True),
)
#: Between the name and the number: a short run of words with no digit, ending in a level verb.
#: Anything naming a derived quantity (z-score, momentum, ratio, change ...) is not a level claim.
_FIGURE_GAP = r"(?P<gap>[^0-9\n.;()\[\]]{0,48}?)\b(?:stands at|stood at|is at|was at|closed at|at|of|is|was|=)\s*"
_FIGURE_NUMBER = r"(?P<num>\$?[0-9][0-9,]*(?:\.[0-9]+)?)(?P<pct>\s?%)?"
_DERIVED = re.compile(r"z-score|\bz\b|momentum|ratio|change|volume|percentile|score|return|breadth|ma\b|average|"
                      r"put/call|spread change|week|day move|bps", re.IGNORECASE)
_FIGURE_RES = tuple((fig, re.compile(rf"(?<![A-Za-z0-9^])(?P<name>{fig.names})(?![A-Za-z0-9]){_FIGURE_GAP}{_FIGURE_NUMBER}",
                                     re.IGNORECASE)) for fig in FIGURES)
FIGURE_HORIZON = timedelta(minutes=1)   # a current-state figure is checkable at once; the ingest wants horizon > stated


# ── small helpers (same conventions as the producer and the ingest) ────────────────────

def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def text_sha(value: str) -> str:
    """sha256 of text as UTF-8; a lone surrogate is passed through, never a crash (the ingest does the same)."""
    return sha256(value.encode("utf-8", "surrogatepass"))


def canonical(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, OverflowError, TypeError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def session_close_utc(session: date) -> datetime:
    """16:00 America/New_York of ``session`` as an aware UTC instant."""
    return datetime(session.year, session.month, session.day, 16, 0, tzinfo=NY).astimezone(timezone.utc)


def ny_date(ts: datetime) -> date:
    return ts.astimezone(NY).date()


def _is_market_open(d: date) -> bool:
    from ingestion.market_calendar import is_market_open
    return is_market_open(d)


def next_session_on_or_after(d: date) -> date:
    while not _is_market_open(d):
        d += timedelta(days=1)
    return d


def read_regular_file(path: Path, limit: int) -> bytes:
    """Read a regular file without following a symlink at the leaf; refuse anything else."""
    import stat
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        raise Refused(f"{path.name} unreadable: {type(exc).__name__}") from exc
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise Refused(f"{path.name} is not a regular file")
        if info.st_size > limit:
            raise Refused(f"{path.name} is larger than {limit} bytes")
        chunks = []
        while True:
            chunk = os.read(fd, 1 << 20)
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
    finally:
        os.close(fd)


class Refused(Exception):
    """The day cannot be graded; the reason is the public receipt text."""


# ── packet ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Publication:
    id: str
    channel: str
    published_at: datetime
    text: str
    sha256: str
    source_record: dict[str, Any]


@dataclass
class Packet:
    sha256: str
    publications: list[Publication]
    unusable: list[dict[str, Any]] = field(default_factory=list)


def load_packet(packet_root: Path, day: date) -> Packet:
    folder = packet_root / day.isoformat()
    receipt_path, packet_path = folder / "receipt.json", folder / "packet.json"
    if not receipt_path.exists() or not packet_path.exists():
        raise Refused(f"packet for {day} not present under {folder}")
    try:
        receipt = json.loads(read_regular_file(receipt_path, MAX_FILE_BYTES).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Refused(f"receipt.json unreadable: {type(exc).__name__}") from exc
    expected = receipt.get("packet_sha256") if isinstance(receipt, dict) else None
    if not isinstance(expected, str) or not HEX64.fullmatch(expected):
        raise Refused("receipt.json has no packet_sha256")
    raw = read_regular_file(packet_path, MAX_FILE_BYTES)
    if sha256(raw) != expected:
        raise Refused("packet.json bytes do not match receipt.packet_sha256")
    try:
        packet = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Refused(f"packet.json unreadable: {type(exc).__name__}") from exc
    if not isinstance(packet, dict) or packet.get("publication_day") != day.isoformat():
        raise Refused("packet.json is not this day's packet")
    records = packet.get("publications")
    if not isinstance(records, list):
        raise Refused("packet.json has no publications list")
    publications: list[Publication] = []
    unusable: list[dict[str, Any]] = []
    for record in records:
        reason = None
        pid = record.get("id") if isinstance(record, dict) else None
        if not isinstance(record, dict) or not isinstance(pid, str) or not PUBLICATION_ID.fullmatch(pid):
            reason = "malformed id"
        else:
            text, digest = record.get("text"), record.get("sha256")
            published = parse_ts(record.get("published_at"))
            if not isinstance(text, str) or not text:
                reason = "no text"
            elif not isinstance(digest, str) or text_sha(text) != digest:
                reason = "text sha256 mismatch"
            elif published is None:
                reason = "published_at missing or naive"
            elif record.get("source") != pid.split(":")[0]:
                reason = "source != id channel"
        if reason:
            unusable.append({"publication_id": pid if isinstance(pid, str) else None, "reason": reason})
            continue
        source_record = record.get("source_record")
        publications.append(Publication(id=pid, channel=pid.split(":")[0], published_at=published, text=text,
                                        sha256=digest, source_record=source_record if isinstance(source_record, dict) else {}))
    return Packet(sha256=expected, publications=publications, unusable=unusable)


# ── spans ──────────────────────────────────────────────────────────────────────────────

_SENTENCE_END = re.compile(r"(?<=[.!?])\s")


def first_sentence_span(text: str) -> str | None:
    """A verbatim opening span of 8-300 chars: the first sentence, or the first line, cut on a word."""
    head = text.lstrip()
    if len(head) < MIN_SPAN_CHARS:
        return None
    first_line = head.split("\n", 1)[0]
    candidate = _SENTENCE_END.split(first_line, 1)[0].rstrip()
    if len(candidate) < MIN_SPAN_CHARS:
        candidate = first_line.rstrip()
    if len(candidate) > MAX_SPAN_CHARS:
        cut = candidate[:MAX_SPAN_CHARS]
        if " " in cut:
            cut = cut[:cut.rfind(" ")]
        candidate = cut.rstrip()
    if len(candidate) < MIN_SPAN_CHARS or candidate not in text:
        return None
    return candidate


def marker_span(text: str) -> str | None:
    """The sentence that carries the withheld/unavailable marker, verbatim (markdown emphasis stripped)."""
    for marker in WITHHELD_MARKERS:
        at = text.find(marker)
        if at < 0:
            continue
        line_start = text.rfind("\n", 0, at) + 1
        line_end = text.find("\n", at)
        line = text[line_start: len(text) if line_end < 0 else line_end]
        sentence = next((s for s in _SENTENCE_END.split(line) if marker in s), line)
        sentence = sentence.strip().strip("*_").strip()
        if len(sentence) > MAX_SPAN_CHARS:
            sentence = sentence[:MAX_SPAN_CHARS].rsplit(" ", 1)[0].rstrip()
        if len(sentence) >= MIN_SPAN_CHARS and sentence in text:
            return sentence
        return first_sentence_span(text)
    return None


def narrative_window(text: str) -> str:
    """The forecast-bearing part of a record: everything before the generator's first data table.

    Real briefings put the narrative under its own ``##`` headings ("## What's Happening Now",
    "## Action", "## Tomorrow"), so the cut is at the known data-section headings only, never at
    any ``##``. A record without data sections is passed whole, up to ``EXTRACT_MAX_CHARS``.
    """
    cut = len(text)
    for marker in DATA_SECTION_MARKERS:
        at = text.find(marker)
        if 0 <= at < cut:
            cut = at
    return text[:min(cut, EXTRACT_MAX_CHARS)]


# ── prices ─────────────────────────────────────────────────────────────────────────────

def closes(conn: Any, ticker: str, start: date, end: date) -> list[Any]:
    """Split-adjusted closes of ``ticker`` in ``[start, end]`` from the accepted-only reader."""
    from store.observations import read_window
    return read_window(conn, INSTRUMENT_SERIES[ticker], source=PRICE_SOURCE, start=start, as_of=end)


def baseline_session(stated_at: datetime) -> date:
    """The last session whose close was complete when the claim was stated."""
    d = ny_date(stated_at)
    if not _is_market_open(d) or stated_at < session_close_utc(d):
        d -= timedelta(days=1)
        while not _is_market_open(d):
            d -= timedelta(days=1)
    return d


def _close_on(obs: list[Any], d: date) -> Any | None:
    for o in obs:
        if o.obs_date == d:
            return o
    return None


def resolve_instrument(name: Any, *, allow_alias: bool = True) -> str | None:
    """Ticker the grader can price. Aliases (index and commodity names -> an ETF proxy) are fine
    for a direction call but never for a level: "SPX above 6800" is not a statement about SPY."""
    if not isinstance(name, str) or not name.strip():
        return None
    key = name.strip().upper().strip("$")
    if key in INSTRUMENT_SERIES:
        return key
    return INSTRUMENT_ALIASES.get(key) if allow_alias else None


def _usable_price(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value > 0


def actual_direction_of(pct: float) -> str:
    if pct > DIRECTION_BAND_PCT:
        return "bullish"
    if pct < -DIRECTION_BAND_PCT:
        return "bearish"
    return "neutral"


def direction_verdict(claimed: str, actual: str) -> str:
    """intelligence/thesis_tracker.py's matrix: same side = correct, opposite sides = wrong,
    anything involving a flat leg (a call that went nowhere, a flat call that moved) = partial."""
    if claimed == actual:
        return "confirmed"
    if claimed == "neutral" or actual == "neutral":
        return "partially_confirmed"
    return "refuted"


# ── rows ───────────────────────────────────────────────────────────────────────────────

def _row(pub: Publication, span: str, claim_type: str, **fields: Any) -> dict[str, Any]:
    row = {"kind": "claim_verdict", "publication_id": pub.id, "publication_sha256": pub.sha256,
           "stated_at": iso(pub.published_at), "claim_span": span, "claim_sha256": text_sha(span),
           "claim_type": claim_type, "instrument": None, "horizon_end": None, "verdict": "unresolved",
           "evidence": [], "knowable_at_stated_at": None, "thesis_direction": None, "actual_direction": None,
           "root_cause": None, "lesson": None, "grader_confidence": None}
    row.update(fields)
    return row


def unscorable(pub: Publication, span: str, claim_type: str, reason: str, **fields: Any) -> dict[str, Any]:
    return _row(pub, span, claim_type, verdict="unscorable", evidence=[], root_cause=reason[:2000],
                grader_confidence=1.0, **fields)


def grade_direction(conn: Any, pub: Publication, span: str, ticker: str, claimed: str, horizon_session: date,
                    graded_at: datetime) -> dict[str, Any]:
    """A direction call on ``ticker`` from the last completed close before stated_at to ``horizon_session``'s close."""
    base_session = baseline_session(pub.published_at)
    horizon_end = session_close_utc(horizon_session)
    direction_fields = {"thesis_direction": claimed}  # the stored direction for a thesis; the extracted call otherwise
    obs = closes(conn, ticker, base_session, horizon_session)
    base = _close_on(obs, base_session)   # exact: an older close would credit moves the claim never saw
    if base is None or not _usable_price(base.value):
        return unscorable(pub, span, "direction", f"no usable {PRICE_SOURCE} close for {ticker} on {base_session}",
                          instrument=ticker, horizon_end=iso(horizon_end), **direction_fields)
    evidence = [{"source": f"{PRICE_SOURCE} {INSTRUMENT_SERIES[ticker]}", "observed": f"{base.value:.4f} close {base.obs_date}",
                 "as_of": iso(session_close_utc(base.obs_date)), "url": None}]
    out = _close_on(obs, horizon_session)
    if horizon_end > graded_at or out is None:
        return _row(pub, span, "direction", instrument=ticker, horizon_end=iso(horizon_end), verdict="unresolved",
                    evidence=evidence, grader_confidence=0.5, **direction_fields)
    if not _usable_price(out.value):
        return unscorable(pub, span, "direction", f"unusable {PRICE_SOURCE} close for {ticker} on {horizon_session}",
                          instrument=ticker, horizon_end=iso(horizon_end), **direction_fields)
    pct = (out.value - base.value) / base.value * 100.0
    actual = actual_direction_of(pct)
    verdict = direction_verdict(claimed, actual)
    evidence.append({"source": f"{PRICE_SOURCE} {INSTRUMENT_SERIES[ticker]}", "observed": f"{out.value:.4f} close {out.obs_date}",
                     "as_of": iso(horizon_end), "url": None})
    evidence.append({"source": "grader", "observed": f"{pct:+.3f}% {base.obs_date} -> {out.obs_date}; band ±{DIRECTION_BAND_PCT}%",
                     "as_of": iso(horizon_end), "url": None})
    root_cause = None
    if verdict != "confirmed":
        root_cause = (f"{ticker} moved {pct:+.2f}% from the {base.obs_date} close to the {out.obs_date} close "
                      f"({actual}) against a {claimed} call")
    return _row(pub, span, "direction", instrument=ticker, horizon_end=iso(horizon_end), verdict=verdict,
                evidence=evidence, actual_direction=actual, root_cause=root_cause, grader_confidence=0.95,
                **direction_fields)


def grade_level(conn: Any, pub: Publication, span: str, ticker: str, comparator: str, level: float,
                horizon_session: date, graded_at: datetime) -> dict[str, Any]:
    horizon_end = session_close_utc(horizon_session)
    obs = closes(conn, ticker, horizon_session, horizon_session)
    out = _close_on(obs, horizon_session)
    if horizon_end > graded_at or out is None:
        return _row(pub, span, "level", instrument=ticker, horizon_end=iso(horizon_end), verdict="unresolved",
                    grader_confidence=0.5)
    if not _usable_price(out.value):
        return unscorable(pub, span, "level", f"unusable {PRICE_SOURCE} close for {ticker} on {horizon_session}",
                          instrument=ticker, horizon_end=iso(horizon_end))
    holds = COMPARATORS[comparator](out.value, level)
    evidence = [{"source": f"{PRICE_SOURCE} {INSTRUMENT_SERIES[ticker]}", "observed": f"{out.value:.4f} close {out.obs_date}",
                 "as_of": iso(horizon_end), "url": None},
                {"source": "grader", "observed": f"close {comparator} {level:g} is {str(holds).lower()}",
                 "as_of": iso(horizon_end), "url": None}]
    root_cause = None if holds else f"{ticker} closed {out.value:.2f} on {out.obs_date}; the claim needed {comparator} {level:g}"
    return _row(pub, span, "level", instrument=ticker, horizon_end=iso(horizon_end),
                verdict="confirmed" if holds else "refuted", evidence=evidence, root_cause=root_cause,
                grader_confidence=0.95)


def grade_thesis(conn: Any, pub: Publication, graded_at: datetime) -> list[dict[str, Any]]:
    span = first_sentence_span(pub.text)
    if span is None:
        return []
    stored = pub.source_record.get("overall_direction")
    claimed = stored.strip().lower() if isinstance(stored, str) else None
    if claimed not in DIRECTIONS:
        return [unscorable(pub, span, "direction", "snapshot has no bullish/bearish/neutral overall_direction",
                           instrument="SPY")]
    horizon_session = next_session_on_or_after(ny_date(pub.published_at) + timedelta(days=THESIS_HORIZON_DAYS))
    return [grade_direction(conn, pub, span, "SPY", claimed, horizon_session, graded_at)]


# ── claim extraction (LLM) ─────────────────────────────────────────────────────────────

EXTRACTION_SYSTEM = (
    "You extract checkable claims from a market briefing. Return ONLY a JSON array. Each element: "
    '{"span": <exact verbatim substring of the text, 8-300 chars, copied character for character>, '
    '"claim_type": "direction"|"level"|"event"|"causal", '
    '"instrument": <ticker or common name or null>, '
    '"direction": "bullish"|"bearish"|"neutral"|null, '
    '"comparator": ">"|">="|"<"|"<="|null, "level": <number or null>, '
    '"horizon": "close"|"next_close"|"week"|null}. '
    "A direction claim says where an instrument goes. A level claim says an instrument closes above/below a number. "
    "Skip descriptions of what already happened. Never paraphrase the span. Return [] if there is nothing."
)


@dataclass
class Extraction:
    claims: list[dict[str, Any]]
    model: str
    error: str | None = None


def _thinking_off(client: Any) -> dict[str, Any]:
    """``enable_thinking=False`` for clients whose chat() names it (llama.cpp); nothing for the rest."""
    try:
        from llm.router import supported_chat_kwargs
        return supported_chat_kwargs(client, enable_thinking=False)
    except Exception:
        return {}


def _client_label(client: Any) -> str:
    return f"{type(client).__name__}/{getattr(client, 'model', None) or 'unknown'}"


def extract_claims(client: Any, window: str) -> Extraction:
    if client is None or not getattr(client, "is_available", False):
        return Extraction([], "none", "no LLM provider available")
    label = _client_label(client)
    try:
        # A reasoning model otherwise spends the whole budget thinking and returns no content
        # (grid-svr oracle, 2026-11-08: finish=length after 21,800 reasoning chars, 3 min, nothing
        # to parse). Structured extraction wants the answer, not the deliberation.
        reply = client.chat([{"role": "system", "content": EXTRACTION_SYSTEM},
                             {"role": "user", "content": window}], temperature=0.0,
                            num_predict=EXTRACTION_NUM_PREDICT, **_thinking_off(client))
    except Exception as exc:  # a provider fault is a grading reason, not a crash
        return Extraction([], label, f"provider error: {type(exc).__name__}")
    if not isinstance(reply, str) or not reply.strip():
        return Extraction([], label, "empty reply")
    start, end = reply.find("["), reply.rfind("]")
    if start < 0 or end <= start:
        return Extraction([], label, "reply is not a JSON array")
    try:
        parsed = json.loads(reply[start:end + 1])
    except json.JSONDecodeError:
        return Extraction([], label, "reply is not valid JSON")
    if not isinstance(parsed, list):
        return Extraction([], label, "reply is not a JSON array")
    return Extraction(validate_items(parsed, window), label)


def _str_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def validate_items(parsed: Any, window: str) -> list[dict[str, Any]]:
    """Model (or cache) output -> well-typed claims. Anything odd in an item drops that item, never the day."""
    if not isinstance(parsed, list):
        return []
    claims: list[dict[str, Any]] = []
    for item in parsed[:MAX_EXTRACTED_CLAIMS]:
        try:
            if not isinstance(item, dict):
                continue
            span = _str_or_none(item.get("span"))
            if span is None:
                continue
            span = span.strip()
            if not MIN_SPAN_CHARS <= len(span) <= MAX_SPAN_CHARS or span not in window:
                continue
            claim_type = _str_or_none(item.get("claim_type"))
            if claim_type not in CLAIM_TYPES or claim_type == "data_quality":
                continue
            direction = _str_or_none(item.get("direction"))
            comparator = _str_or_none(item.get("comparator"))
            horizon = _str_or_none(item.get("horizon"))
            level = item.get("level")
            level_ok = isinstance(level, (int, float)) and not isinstance(level, bool) and math.isfinite(level) \
                and abs(level) < 1e12
            claims.append({"span": span, "claim_type": claim_type, "instrument": _str_or_none(item.get("instrument")),
                           "direction": direction if direction in DIRECTIONS else None,
                           "comparator": comparator if comparator in COMPARATORS else None,
                           "level": float(level) if level_ok else None,
                           "horizon": horizon if horizon in {"close", "next_close", "week"} else None})
        except (TypeError, ValueError, AttributeError):
            continue
    return claims


def extraction_cache_key(pub: Publication) -> str:
    """The cache is per publication bytes AND per extraction setup, so a better model or a new
    prompt re-extracts instead of inheriting a weaker model's empty answer."""
    return sha256(f"{pub.sha256}|{EXTRACTION_VERSION}|{DEFAULT_TIER}|{DEFAULT_PROVIDER or ''}".encode("utf-8"))


def cached_extraction(folder: Path, pub: Publication, client_factory: Any, *, persist: bool = True) -> Extraction:
    """Extract once per publication sha and extraction setup; later re-grades read the cache.

    Cached claims are re-validated against the text on every read (a tampered or stale cache
    yields at most dropped claims). Cache I/O is best effort: it never fails the day.
    """
    window = narrative_window(pub.text)
    key = extraction_cache_key(pub)
    cache = folder / "extractions" / f"{pub.id.replace(':', '-')}.json"
    try:
        if cache.exists():
            stored = json.loads(read_regular_file(cache, 4_000_000).decode("utf-8"))
            if isinstance(stored, dict) and stored.get("cache_key") == key and isinstance(stored.get("model"), str) \
                    and (not stored.get("error") or stored.get("error") in DETERMINISTIC_EXTRACTION_ERRORS):
                return Extraction(validate_items(stored.get("claims"), window), stored["model"], stored.get("error") or None)
    except (OSError, Refused, UnicodeDecodeError, json.JSONDecodeError, RecursionError):
        pass
    extraction = extract_claims(client_factory(), window)
    if not persist or (extraction.error and extraction.error not in DETERMINISTIC_EXTRACTION_ERRORS):
        return extraction   # transient faults (no provider, transport) are retried next run, never cached
    try:
        cache.parent.mkdir(parents=True, exist_ok=True)
        write_file(cache, json.dumps({"publication_id": pub.id, "publication_sha256": pub.sha256, "cache_key": key,
                                      "extraction_version": EXTRACTION_VERSION, "model": extraction.model,
                                      "error": extraction.error, "claims": extraction.claims},
                                     indent=1, sort_keys=True).encode("utf-8"))
    except OSError:
        pass
    return extraction


def horizon_session_for(pub: Publication, horizon: str | None) -> date:
    d = ny_date(pub.published_at)
    if horizon == "week":
        return next_session_on_or_after(d + timedelta(days=7))
    if horizon == "next_close" or not _is_market_open(d) or pub.published_at >= session_close_utc(d):
        return next_session_on_or_after(d + timedelta(days=1))
    return d


@dataclass(frozen=True)
class StatedFigure:
    figure: Figure
    span: str
    stated: float
    decimals: int


def extract_figures(window: str) -> list[StatedFigure]:
    """Deterministic: "<instrument name> ... at <number>" level statements in a narrative window."""
    found: list[StatedFigure] = []
    taken: list[tuple[int, int]] = []
    for fig, pattern in _FIGURE_RES:
        for m in pattern.finditer(window):
            start, end = m.start("name"), m.end("num") + (len(m.group("pct")) if m.group("pct") else 0)
            if any(a < end and start < b for a, b in taken):
                continue                        # a more specific figure already claimed this text
            if _DERIVED.search(m.group("gap") or ""):
                continue
            if bool(m.group("pct")) != fig.pct:
                continue                        # "S&P 500 at -2.68%" is a move, "spread at 0.38" is not a rate
            raw = m.group("num").lstrip("$").replace(",", "")
            try:
                stated = float(raw)
            except ValueError:
                continue
            if not math.isfinite(stated):
                continue
            span = window[start:end].strip()
            if not MIN_SPAN_CHARS <= len(span) <= MAX_SPAN_CHARS or span not in window:
                continue
            decimals = len(raw.split(".", 1)[1]) if "." in raw else 0
            taken.append((start, end))
            found.append(StatedFigure(fig, span, stated, decimals))
    found.sort(key=lambda f: window.find(f.span))
    return found


# ── truth of a stated level at the instant it was stated ───────────────────────────────
#
# A briefing that says "the VIX stands at 26.9" at 20:00Z is making a claim about the market
# at 20:00Z, not about GRID's last completed close. So the grader looks at every capture GRID
# holds for that instrument around the statement (intraday pulls, the completed close, the
# session's high and low, later revisions) and asks whether the number was real:
#
#   during a session : between GRID's captures bracketing the instant -> confirmed;
#                      equal to the previous completed close           -> confirmed;
#                      only inside the session's high/low              -> partially_confirmed;
#   after a close    : equal to the newest completed close at that time -> confirmed (whether or
#                      not GRID had pulled it yet: the market had printed it);
#                      equal to an older close while a newer one was known -> partially_confirmed
#                      (stale); inside the session's range or equal to an intraday capture
#                      -> partially_confirmed;
#   FRED rates       : the same newest-vs-stale rule over observation dates;
#   otherwise        : refuted, with the nearest captures and the range as evidence.

SESSION_OPEN = (9, 30)
BRACKET_WINDOW = timedelta(minutes=90)   # a capture further from the statement does not pin its level


def session_open_utc(session: date) -> datetime:
    return datetime(session.year, session.month, session.day, *SESSION_OPEN, tzinfo=NY).astimezone(timezone.utc)


def previous_session(d: date) -> date:
    d -= timedelta(days=1)
    while not _is_market_open(d):
        d -= timedelta(days=1)
    return d


def _vintages(conn: Any, series_id: str, start: date, end: date) -> list[Any]:
    from store.observations import read_vintages
    return [o for o in read_vintages(conn, series_id, start=start, end=end)
            if o.pull_timestamp is not None and math.isfinite(o.value)]


@dataclass
class Facts:
    """What GRID holds for one instrument around one instant."""

    series_id: str
    intraday: date | None = None               # the session in progress at the instant, if any
    sessions: list[date] = field(default_factory=list)   # completed sessions considered, newest first
    by_date: dict[date, list[Any]] = field(default_factory=dict)   # close vintages per label, oldest pull first
    low: dict[date, float] = field(default_factory=dict)
    high: dict[date, float] = field(default_factory=dict)


def gather_facts(conn: Any, fig: Figure, at: datetime) -> Facts:
    facts = Facts(series_id=fig.series_id)
    if fig.series_id.startswith("YF:"):
        today = ny_date(at)
        if _is_market_open(today) and session_open_utc(today) <= at < session_close_utc(today):
            facts.intraday = today
        s1 = today if _is_market_open(today) and at >= session_close_utc(today) else previous_session(today)
        facts.sessions = [s1, previous_session(s1)]
        start, end = facts.sessions[-1], facts.intraday or s1
        rows = _vintages(conn, fig.series_id, start, end)
        stem = fig.series_id.rsplit(":", 1)[0]
        for name, target in ((f"{stem}:low", facts.low), (f"{stem}:high", facts.high)):
            for o in _vintages(conn, name, start, end):
                target[o.obs_date] = o.value          # oldest pull first: the last write is the newest
    else:
        rows = _vintages(conn, fig.series_id, at.date() - timedelta(days=14), at.date())
        facts.sessions = sorted({o.obs_date for o in rows}, reverse=True)
    for o in rows:
        facts.by_date.setdefault(o.obs_date, []).append(o)
    return facts


def _completed_close(facts: Facts, session: date) -> Any | None:
    """The newest capture of ``session`` taken after its close (Yahoo); any capture (FRED)."""
    rows = facts.by_date.get(session, [])
    if not facts.series_id.startswith("YF:"):
        return rows[-1] if rows else None
    after = [o for o in rows if o.pull_timestamp >= session_close_utc(session)]
    return after[-1] if after else None


def figure_matches(stated: float, decimals: int, observed: float) -> bool:
    """Stated equals observed at the precision the briefing printed (half a unit of its last digit,
    plus 0.05 % for a value the briefing rounded before printing)."""
    tolerance = 0.5 * 10 ** (-decimals) + 0.0005 * abs(observed)
    return abs(stated - observed) <= tolerance


def _within(stated: float, decimals: int, lo: float, hi: float) -> bool:
    tolerance = 0.5 * 10 ** (-decimals) + 0.0005 * max(abs(lo), abs(hi))
    return lo - tolerance <= stated <= hi + tolerance


@dataclass
class Judgment:
    verdict: str
    evidence: list[dict[str, Any]]
    root_cause: str | None
    knowable: bool | None
    confidence: float


def _ev(source: str, observed: str, as_of: datetime) -> dict[str, Any]:
    return {"source": source, "observed": observed, "as_of": iso(as_of), "url": None}


def judge_level(stated: StatedFigure, at: datetime, facts: Facts) -> Judgment | None:
    """None when GRID holds nothing for the instrument around ``at`` (unscorable)."""
    fig, value, dec = stated.figure, stated.stated, stated.decimals
    unit = "%" if fig.pct else ""
    src = f"raw_series {fig.series_id}"
    if not any(facts.by_date.values()):
        return None
    # 1. during a session: GRID's captures that bracket the instant
    if facts.intraday is not None:
        # only captures taken during the session and near the statement pin a level to the instant
        close = session_close_utc(facts.intraday)
        rows = [o for o in facts.by_date.get(facts.intraday, [])
                if o.pull_timestamp < close and abs(o.pull_timestamp - at) <= BRACKET_WINDOW]
        before = [o for o in rows if o.pull_timestamp <= at]
        after = [o for o in rows if o.pull_timestamp > at]
        bracket = before[-1:] + after[:1]
        if bracket and _within(value, dec, min(o.value for o in bracket), max(o.value for o in bracket)):
            ev = [_ev(f"{o.source} {fig.series_id}", f"{o.value:g}{unit} captured {iso(o.pull_timestamp)}",
                      o.pull_timestamp) for o in bracket]
            known = bool(before) and figure_matches(value, dec, before[-1].value)
            return Judgment("confirmed", ev, None, known, 0.9)
    # 2. a completed close
    closes = [(s, _completed_close(facts, s)) for s in facts.sessions]
    closes = [(s, o) for s, o in closes if o is not None]
    known_at = [(s, o) for s, o in closes if o.pull_timestamp <= at]
    newest_known = known_at[0][0] if known_at else None
    for s, o in closes:
        if not figure_matches(value, dec, o.value):
            continue
        ev = [_ev(f"{o.source} {fig.series_id}", f"{o.value:g}{unit} close {s} (captured {iso(o.pull_timestamp)})",
                  o.pull_timestamp)]
        if newest_known is not None and s < newest_known:
            newer = dict(known_at)[newest_known]
            ev.append(_ev(f"{newer.source} {fig.series_id}", f"{newer.value:g}{unit} close {newest_known} "
                          f"(captured {iso(newer.pull_timestamp)}, before the statement)", newer.pull_timestamp))
            return Judgment("partially_confirmed", ev,
                            f"stale: {stated.span!r} is the {s} close; the {newest_known} close "
                            f"({newer.value:g}{unit}) was already known", True, 0.9)
        if s == facts.sessions[0] or newest_known is None or s >= newest_known:
            return Judgment("confirmed", ev, None, o.pull_timestamp <= at, 0.9)
    # 3. inside a session's traded range, or equal to an intraday capture
    for s in ([facts.intraday] if facts.intraday else []) + facts.sessions[:1]:
        lo, hi = facts.low.get(s), facts.high.get(s)
        if lo is not None and hi is not None and _within(value, dec, lo, hi):
            return Judgment("partially_confirmed",
                            [_ev(f"raw_series {fig.series_id.rsplit(':', 1)[0]}:low/high",
                                 f"session {s} range {lo:g}-{hi:g}{unit}", at)],
                            f"{stated.span!r} lies inside the {s} range {lo:g}-{hi:g}{unit}, but no capture "
                            f"pins it to the statement time", None, 0.6)
        hit = next((o for o in facts.by_date.get(s, []) if figure_matches(value, dec, o.value)), None)
        if hit is not None:
            return Judgment("partially_confirmed",
                            [_ev(f"{hit.source} {fig.series_id}", f"{hit.value:g}{unit} intraday capture "
                                 f"{iso(hit.pull_timestamp)}", hit.pull_timestamp)],
                            f"{stated.span!r} equals an intraday capture of {s}, not a close", None, 0.6)
    # 4. nothing supports it
    ev: list[dict[str, Any]] = []
    for s in ([facts.intraday] if facts.intraday else []) + facts.sessions[:2]:
        rows = facts.by_date.get(s, [])
        if rows:
            near = min(rows, key=lambda o: abs((o.pull_timestamp - at).total_seconds()))
            ev.append(_ev(f"{near.source} {fig.series_id}", f"{near.value:g}{unit} captured "
                          f"{iso(near.pull_timestamp)} (label {s})", near.pull_timestamp))
        if s in facts.low and s in facts.high:
            ev.append(_ev(src + " low/high", f"session {s} range {facts.low[s]:g}-{facts.high[s]:g}{unit}", at))
    known_vals = ", ".join(e["observed"].split(" ")[0] for e in ev if "captured" in e["observed"])
    return Judgment("refuted", ev[:6],
                    f"stated {fig.label} {value:g}{unit}; GRID's captures around the statement were {known_vals or 'other values'}"
                    + (" and the value is outside the session range" if any("range" in e["observed"] for e in ev) else ""),
                    True, 0.85)


def grade_figure(conn: Any, pub: Publication, stated: StatedFigure) -> dict[str, Any]:
    fig = stated.figure
    horizon = iso(pub.published_at + FIGURE_HORIZON)
    try:
        facts = gather_facts(conn, fig, pub.published_at)
        judgment = judge_level(stated, pub.published_at, facts)
    except Exception as exc:  # a reader fault is this claim's reason, not the day's
        return unscorable(pub, stated.span, "data_quality",
                          f"{fig.label}: {fig.series_id} unreadable ({type(exc).__name__})", horizon_end=horizon)
    if judgment is None:
        return unscorable(pub, stated.span, "data_quality",
                          f"{fig.label}: GRID holds no {fig.series_id} capture around the statement", horizon_end=horizon)
    return _row(pub, stated.span, "data_quality", instrument=_instrument_for(fig), horizon_end=horizon,
                verdict=judgment.verdict, evidence=judgment.evidence, knowable_at_stated_at=judgment.knowable,
                root_cause=judgment.root_cause, grader_confidence=judgment.confidence)


def _instrument_for(fig: Figure) -> str:
    sid = fig.series_id
    return (sid.split(":")[1] if sid.startswith("YF:") else sid)[:32]


def grade_briefing(conn: Any, pub: Publication, graded_at: datetime, folder: Path, client_factory: Any,
                   *, persist: bool = True) -> tuple[list[dict[str, Any]], str | None]:
    """Rows for a briefing/diary record plus the extraction model used (None when no model was needed)."""
    withheld = marker_span(pub.text)
    if withheld is not None:
        return [unscorable(pub, withheld, "data_quality",
                           "no narrative to grade: the record says its AI narrative was withheld or unavailable "
                           "(data summary only)")], None
    opening = first_sentence_span(pub.text)
    figure_rows = [grade_figure(conn, pub, f) for f in extract_figures(narrative_window(pub.text))]
    extraction = cached_extraction(folder, pub, client_factory, persist=persist)
    if extraction.error:
        if figure_rows:
            return figure_rows, extraction.model
        rows = [] if opening is None else [unscorable(pub, opening, "data_quality",
                                                      f"claim extraction unavailable: {extraction.error}")]
        return rows, extraction.model
    rows: list[dict[str, Any]] = list(figure_rows)
    for claim in extraction.claims:
        span, kind = claim["span"], claim["claim_type"]
        ticker = resolve_instrument(claim["instrument"])
        session = horizon_session_for(pub, claim["horizon"])
        if kind == "level" and ticker is not None and resolve_instrument(claim["instrument"], allow_alias=False) is None:
            rows.append(unscorable(pub, span, kind, f"level claim on {claim['instrument']!r}: only an ETF proxy "
                                                     f"({ticker}) is priced and its scale differs"))
            continue
        if kind == "direction":
            if ticker is None:
                rows.append(unscorable(pub, span, kind, f"no price series for instrument {claim['instrument']!r}"))
            elif claim["direction"] is None:
                rows.append(unscorable(pub, span, kind, "direction claim without a bullish/bearish/neutral direction",
                                       instrument=ticker))
            else:
                rows.append(grade_direction(conn, pub, span, ticker, claim["direction"], session, graded_at))
        elif kind == "level":
            if ticker is None or claim["comparator"] is None or claim["level"] is None:
                rows.append(unscorable(pub, span, kind, "level claim without a priced instrument, comparator and number"))
            else:
                rows.append(grade_level(conn, pub, span, ticker, claim["comparator"], claim["level"], session, graded_at))
        else:  # event, causal: v1 cannot judge; a causal claim is never confirmed from co-movement
            rows.append(_row(pub, span, kind, instrument=ticker, verdict="unresolved", grader_confidence=0.3))
    if not rows and opening is not None:
        rows.append(unscorable(pub, opening, "data_quality", "narrative present but no checkable claim was extracted"))
    return rows, extraction.model


# ── the review file ────────────────────────────────────────────────────────────────────

def dedupe(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    out = []
    for row in rows:
        key = (row["publication_id"], row["claim_sha256"])
        if key not in seen:
            seen.add(key)
            out.append(row)
    return out


def build_header(day: date, packet_sha: str, graded_at: datetime, rows: list[dict[str, Any]], models: set[str]) -> dict[str, Any]:
    unscorable_n = sum(1 for r in rows if r["verdict"] == "unscorable")
    return {"kind": "review_header", "schema": REVIEW_SCHEMA, "publication_day": day.isoformat(),
            "packet_sha256": packet_sha,
            "grader": {"model": GRADER_VERSION, "role": "grader", "job": JOB, "extraction_models": sorted(models)},
            "adjudicator": None, "graded_at": iso(graded_at), "claims_total": len(rows),
            "claims_graded": len(rows) - unscorable_n, "claims_unscorable": unscorable_n}


def render(header: dict[str, Any], rows: list[dict[str, Any]]) -> bytes:
    return b"".join(canonical(obj) + b"\n" for obj in [header, *rows])


def existing_review(path: Path) -> tuple[bytes, dict[str, Any] | None, list[bytes]] | None:
    """(bytes, header, row lines) of the review file already on disk, or None."""
    if not path.exists():
        return None
    try:
        raw = read_regular_file(path, MAX_FILE_BYTES)
    except (OSError, Refused):
        return None
    lines = raw.split(b"\n")
    try:
        header = json.loads(lines[0]) if lines and lines[0] else None
    except (UnicodeDecodeError, json.JSONDecodeError):
        header = None
    return raw, header if isinstance(header, dict) else None, [line for line in lines[1:] if line]


def _header_sans_clock(header: dict[str, Any] | None) -> dict[str, Any] | None:
    return None if header is None else {k: v for k, v in header.items() if k != "graded_at"}


def previous_receipt(folder: Path) -> dict[str, Any]:
    try:
        stored = json.loads(read_regular_file(folder / "grader-receipt.json", 4_000_000).decode("utf-8"))
        return stored if isinstance(stored, dict) else {}
    except (OSError, Refused, UnicodeDecodeError, json.JSONDecodeError):
        return {}


def write_file(path: Path, data: bytes, mode: int = 0o644) -> None:
    if path.is_symlink():
        raise OSError(f"{path} is a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def verdict_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        counts[row["verdict"]] = counts.get(row["verdict"], 0) + 1
    return dict(sorted(counts.items()))


def grade_day(engine: Any, day: date, *, packet_root: Path = DEFAULT_PACKET_ROOT, review_root: Path = DEFAULT_REVIEW_ROOT,
              dry_run: bool = False, now: datetime | None = None, client_factory: Any = None,
              ingest: bool = True, force: bool = False, public_root: Path | None = None) -> dict[str, Any]:
    """Grade one publication day; returns the grader receipt (also written to ``<review_root>/<D>/grader-receipt.json``)."""
    graded_at = now or datetime.now(timezone.utc)
    folder = review_root / day.isoformat()
    receipt: dict[str, Any] = {"schema": RECEIPT_SCHEMA, "job": JOB, "publication_day": day.isoformat(),
                               "graded_at": iso(graded_at), "grader": GRADER_VERSION, "dry_run": dry_run}
    try:
        packet = load_packet(packet_root, day)
        receipt["packet_sha256"] = packet.sha256
        receipt["packet_records_unusable"] = packet.unusable
        factory = client_factory or default_client_factory
        rows: list[dict[str, Any]] = []
        models: set[str] = set()
        if not packet.publications:
            receipt.update(state="EMPTY", rows=0)   # a day with nothing published is not a failure
            return receipt
        with engine.connect() as conn:
            for pub in packet.publications:
                if pub.channel == "thesis_snapshots":
                    rows.extend(grade_thesis(conn, pub, graded_at))
                else:
                    briefing_rows, model = grade_briefing(conn, pub, graded_at, folder, factory, persist=not dry_run)
                    rows.extend(briefing_rows)
                    if model:
                        models.add(model)
        rows = dedupe(rows)
        if not rows:
            raise Refused("no publication yielded a gradeable row")
        header = build_header(day, packet.sha256, graded_at, rows, models)
        payload = render(header, rows)
        if len(payload) > MAX_REVIEW_BYTES:
            raise Refused(f"review.jsonl would be {len(payload)} bytes, more than the ingest accepts")
        receipt.update(rows=len(rows), verdicts=verdict_counts(rows), review_sha256=sha256(payload),
                       channels={ch: sum(1 for r in rows if r["publication_id"].startswith(ch + ":"))
                                 for ch in sorted({r["publication_id"].split(":")[0] for r in rows})})
        path = folder / "review.jsonl"
        before = existing_review(path)
        unchanged = (before is not None and not force
                     and before[2] == [canonical(r) for r in rows]
                     and _header_sans_clock(before[1]) == _header_sans_clock(header))
        if dry_run:
            receipt["state"] = "DRY_RUN"
            receipt["would_write"] = not unchanged
            return receipt
        if unchanged:
            receipt["state"] = "UNCHANGED"
            receipt["review_sha256"] = sha256(before[0])
            # The file on disk is current; make sure the database agrees. A hand-off that failed
            # on an earlier run (database down, module error) is retried here, not forgotten.
            if ingest and previous_receipt(folder).get("ingested_review_sha256") != receipt["review_sha256"]:
                receipt["ingest"] = run_ingest(engine, day, review_root, packet_root)
        else:
            write_file(path, payload)
            receipt["state"] = "WRITTEN"
            receipt["superseded_previous"] = before is not None
            if ingest:
                receipt["ingest"] = run_ingest(engine, day, review_root, packet_root)
        if receipt.get("ingest", {}).get("state") in {"INGESTED", "PRESENT"}:
            receipt["ingested_review_sha256"] = receipt["review_sha256"]
        elif "ingest" not in receipt and receipt["state"] == "UNCHANGED":
            receipt["ingested_review_sha256"] = receipt["review_sha256"]   # confirmed by the previous receipt
    except Refused as exc:
        receipt.update(state="REFUSED", reason=str(exc))
    except Exception as exc:  # the receipt must exist; the type is the public reason
        traceback.print_exc(file=sys.stderr)
        receipt.update(state="FAILED", reason=type(exc).__name__)
    if not dry_run:
        try:
            write_file(folder / "grader-receipt.json", json.dumps(receipt, indent=2, sort_keys=True, default=str).encode("utf-8"))
        except OSError as exc:
            receipt["receipt_write_error"] = type(exc).__name__
        if public_root is not None and receipt.get("state") in {"WRITTEN", "UNCHANGED"}:
            receipt["published"] = publish_review(folder, public_root / day.isoformat())
    return receipt


def run_ingest(engine: Any, day: date, review_root: Path, packet_root: Path) -> dict[str, Any]:
    """Hand the review file to the ingest in-process; its receipt lands next to the file.

    Never raises: the file is already written, so a hand-off fault is recorded in the grader
    receipt (and retried on the next run) instead of turning a graded day into FAILED.
    """
    try:
        result = _ingest_module().ingest(engine, day, review_root=review_root, packet_root=packet_root)
    except Exception as exc:  # the ingest's own ingest() already catches its faults; this is for the hand-off itself
        traceback.print_exc(file=sys.stderr)
        return {"state": "FAILED", "reason": f"hand-off: {type(exc).__name__}"}
    return {key: result.get(key) for key in ("state", "reason", "run_id", "accepted_rows", "rejected_rows",
                                              "declared_mismatch", "receipt_file")}


def publish_review(folder: Path, public_dir: Path) -> dict[str, Any]:
    """Copy the day's review and receipts (allow-list, bytes unchanged) into the public tree.

    Dirs 0755, files 0644, never through a symlink at either end; only changed files are
    rewritten. Best effort: a failure is reported, never raised (the review is already written).
    """
    out: dict[str, Any] = {"public_dir": str(public_dir), "files": []}
    try:
        if public_dir.is_symlink() or public_dir.parent.is_symlink():
            raise OSError("public review dir is a symlink")
        public_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(public_dir.parent, 0o755)
        os.chmod(public_dir, 0o755)
        for name in PUBLIC_REVIEW_FILES:
            src = folder / name
            if not src.is_file() or src.is_symlink():
                continue
            data = read_regular_file(src, MAX_FILE_BYTES)
            dst = public_dir / name
            if dst.is_file() and not dst.is_symlink() and read_regular_file(dst, MAX_FILE_BYTES) == data:
                out["files"].append(name)
                continue
            write_file(dst, data, 0o644)
            out["files"].append(name)
    except (OSError, Refused) as exc:
        out["error"] = type(exc).__name__
        print(f"{JOB}: could not publish review to {public_dir}: {type(exc).__name__}", file=sys.stderr)
    return out


def _ingest_module() -> Any:
    """scripts/ is not a package: load the sibling ingest module by path."""
    import importlib.util
    path = Path(__file__).with_name("ingest_publication_review.py")
    spec = importlib.util.spec_from_file_location("ingest_publication_review", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(module)
    return module


def default_client_factory() -> Any:
    from llm.router import Tier, get_llm
    try:
        tier = Tier(DEFAULT_TIER.lower())
    except ValueError:
        tier = Tier.REASON
    return get_llm(tier, DEFAULT_PROVIDER)


def exit_code(receipts: list[dict[str, Any]]) -> int:
    states = {r.get("state") for r in receipts}
    if states & {"REFUSED", "FAILED"}:
        return 1
    for r in receipts:
        ing = r.get("ingest") or {}
        if ing.get("state") in {"REFUSED", "FAILED"} or ing.get("rejected_rows") or ing.get("declared_mismatch"):
            return 3
    return 0


def lookback_days(now: datetime, lookback: int) -> list[date]:
    yesterday = (now - timedelta(days=1)).date()
    return [yesterday - timedelta(days=offset) for offset in range(lookback - 1, -1, -1)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--day", help="publication day YYYY-MM-DD (default: the lookback window ending yesterday)")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help=f"days ending yesterday to (re)grade (default {DEFAULT_LOOKBACK_DAYS})")
    parser.add_argument("--packet-root", default=str(DEFAULT_PACKET_ROOT))
    parser.add_argument("--review-root", default=str(DEFAULT_REVIEW_ROOT))
    parser.add_argument("--dry-run", action="store_true", help="grade and report; write nothing, ingest nothing")
    parser.add_argument("--no-ingest", action="store_true", help="write review.jsonl but leave the ingest to its timer")
    parser.add_argument("--force", action="store_true", help="rewrite review.jsonl even when the verdict rows are unchanged")
    parser.add_argument("--public-root", default=str(DEFAULT_PUBLIC_ROOT),
                        help="world-readable copy of review.jsonl + receipts per day (default from "
                             "GRID_PUBLICATION_REVIEW_PUBLIC_ROOT)")
    parser.add_argument("--no-publish", action="store_true", help="do not copy the review into the public tree")
    parser.add_argument("--json", action="store_true", help="print the receipts as JSON")
    args = parser.parse_args(argv)
    now = datetime.now(timezone.utc)
    if args.day:
        try:
            days = [date.fromisoformat(args.day)]
        except ValueError:
            parser.error("--day must be YYYY-MM-DD")
    else:
        days = lookback_days(now, max(1, args.lookback))
    from db import get_engine
    engine = get_engine()
    receipts = []
    for day in days:
        receipt = grade_day(engine, day, packet_root=Path(args.packet_root).expanduser(),
                            review_root=Path(args.review_root).expanduser(), dry_run=args.dry_run, now=now,
                            ingest=not args.no_ingest, force=args.force,
                            public_root=None if args.no_publish else Path(args.public_root).expanduser())
        if receipt.get("state") == "REFUSED" and not args.day and "not present" in str(receipt.get("reason")):
            continue  # a lookback day without a packet is not news
        receipts.append(receipt)
        if not args.json:
            ing = receipt.get("ingest") or {}
            print(f"{JOB} {day}: {receipt.get('state')} rows={receipt.get('rows', 0)} verdicts={receipt.get('verdicts', {})}"
                  f"{' ingest=' + str(ing.get('state')) if ing else ''}"
                  f"{' reason=' + str(receipt.get('reason')) if receipt.get('reason') else ''}")
    if args.json:
        print(json.dumps(receipts, indent=2, default=str))
    return exit_code(receipts)


if __name__ == "__main__":
    sys.exit(main())
