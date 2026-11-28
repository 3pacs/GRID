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
GRADER_VERSION = "grid/grade_publication_review-v4-causes-moves"
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
    #: Further raw_series ids holding the same quantity, read in order when ``series_id`` has
    #: nothing in the lookback (DGS10: GRID's bare FRED pull stopped 2026-04-02, see
    #: analysis/factory_macro.py; ingestion/altdata/yield_curve_full.py stores it as yc_10y).
    alt_series: tuple[str, ...] = ()
    lookback_days: int = 14          # non-YF series: label window before the statement (monthly: longer)
    #: A level that is never near zero: a stated value outside SCALE_BAND x what GRID holds is
    #: a ratio, return or score that happened to follow the name ("VIX/VIX3M=0.90"), not the
    #: level. Spreads cross zero, so they are exempt.
    scale_guard: bool = True


FIGURES: tuple[Figure, ...] = (
    Figure("YF:^VIX9D:close", "VIX 9-day", r"VIX9D|9-day VIX|VIX 9-day(?: tenor)?|9-day tenor"),
    Figure("YF:^VIX3M:close", "VIX 3-month", r"VIX3M|3-month VIX|VIX 3-month(?: tenor)?|3-month tenor"),
    Figure("YF:^VIX:close", "VIX", r"VIX(?![ -]?(?:9|3M|9-day|3-month))"),
    Figure("YF:^GSPC:close", "S&P 500", r"S&P 500|S&P|SPX|\^GSPC"),
    Figure("YF:^IXIC:close", "Nasdaq Composite", r"Nasdaq(?: Composite)?|\^IXIC"),
    Figure("YF:^DJI:close", "Dow Jones", r"Dow Jones|Dow|\^DJI"),
    Figure("YF:^RUT:close", "Russell 2000", r"Russell 2000|\^RUT"),
    Figure("YF:HYG:close", "HYG", r"HYG"),
    Figure("YF:LQD:close", "LQD", r"LQD"),
    Figure("YF:JNK:close", "JNK", r"JNK"),
    Figure("YF:EMB:close", "EMB", r"EMB"),
    Figure("YF:EEM:close", "EEM", r"EEM"),
    Figure("YF:FXE:close", "FXE", r"FXE"),
    Figure("YF:FXY:close", "FXY", r"FXY"),
    Figure("YF:GLD:close", "GLD", r"GLD"),
    Figure("YF:UUP:close", "UUP", r"UUP"),
    Figure("T10Y2Y", "10Y-2Y spread", r"10Y-2Y(?: yield curve)?(?: spread)?|2s10s|10-year minus 2-year|"
                                       r"10-year(?:/| to )2-year(?: Treasury)?(?: yield)?(?: curve)?(?: spread)?",
           pct=True, scale_guard=False),
    Figure("T10Y3M", "10Y-3M spread", r"10Y-3M(?: spread)?|10-year minus 3-month", pct=True, scale_guard=False),
    Figure("DFF", "fed funds rate", r"(?:effective )?fed funds(?: rate)?", pct=True),
    Figure("DGS2", "2-year Treasury yield", r"2-year (?:Treasury )?yield", pct=True),
    Figure("DGS5", "5-year Treasury yield", r"5-year (?:Treasury )?yield", pct=True),
    Figure("DGS10", "10-year Treasury yield", r"10-year (?:Treasury )?yield|DGS10", pct=True, alt_series=("yc_10y",)),
    Figure("T5YIE", "5-year breakeven inflation", r"T5YIE|5-year (?:inflation expectations?|breakeven(?: inflation)?(?: rate)?)",
           pct=True),
    Figure("UNRATE", "unemployment rate", r"unemployment rate|UNRATE", pct=True, lookback_days=100),   # monthly, ~5 weeks lag
)
SCALE_BAND = (0.2, 5.0)
#: Between the name and the number: a short run of words with no digit, ending in a level verb.
#: An optional ")" (the name was a parenthesised ticker: "volatility (VIX) stands at") and an
#: optional "(TICKER)" after a long name ("fed funds rate (DFF) is") are allowed; a parenthesised
#: number ("9-day VIX (29.26)") is the level itself.
#: Anything naming a derived quantity (z-score, momentum, ratio, change ...) is not a level claim.
_FIGURE_GAP = (r"(?P<gap>\)?(?:\s*\([A-Za-z^][A-Za-z0-9^=]{0,7}\))?[^0-9\n.;()\[\]]{0,48}?)"
               r"(?:\b(?:stands at|stood at|is at|was at|closed at|at|of|is|was|=)\s*|(?P<paren>\(\s*)"
               r"|(?P<bare>\x20(?=[+\-\u2212$]?[0-9])))")
#: A bare "VIX 19.46" is a level only right after the name, printed with decimals, and right before
#: a list or sentence boundary ("VIX 19.46 vs. VIX3M 20.64", "HYG 76.63, LQD 102.74"); "VIX 30 days
#: ago" and "the Nasdaq 100," are not.
_BARE_END = re.compile(r"\s*(?:[,;)\]]|vs\b|versus\b|and\b|\.(?:\s|$)|$)")
#: The number must end there: "(9-day" is a tenor's name, not the level 9.
_FIGURE_NUMBER = r"(?P<sign>[+\-\u2212])?(?P<num>\$?[0-9][0-9,]*(?:\.[0-9]+)?)(?![0-9A-Za-z\-])(?P<pct>\s?%)?"
_DERIVED = re.compile(r"z-score|\bz\b|momentum|ratio|change|volume|percentile|score|return|breadth|ma\b|average|"
                      r"put/call|spread change|week|day move|bps", re.IGNORECASE)
#: "/" on either side is a ratio or a pair ("VIX/VIX3M", "HYG/LQD"), never one level.
_FIGURE_RES = tuple((fig, re.compile(rf"(?<![A-Za-z0-9^/])(?P<name>{fig.names})(?![A-Za-z0-9/]){_FIGURE_GAP}{_FIGURE_NUMBER}",
                                     re.IGNORECASE)) for fig in FIGURES)
#: A signed number after words of movement is a change ("spread moved by +0.04%"), not a level.
_MOVEMENT = re.compile(r"\b(?:move[sd]?|moving|widen\w*|narrow\w*|steepen\w*|flatten\w*|ros[ei]|rising|fell|fall\w*|"
                       r"up|down|increas\w*|decreas\w*|jump\w*|drop\w*|gain\w*|loss\w*|lost|shift\w*|by)\b",
                       re.IGNORECASE)
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


#: Why a row is unscorable, as a machine-readable key (counted into ``unscorable_by_cause`` in
#: the receipt and header.grader). Carried on the row as CAUSE_KEY and stripped before writing.
CAUSE_KEY = "_cause"
WITHHELD_REASON = ("no narrative to grade: the record says its AI narrative was withheld or unavailable "
                   "(data summary only)")
NOTHING_EXTRACTED_REASON = "narrative present but no checkable claim was extracted"
NOT_A_CLOSE_LEVEL_REASON = "describes a return/score, not a close level"
INTERNAL_SIGNAL_REASON = "GRID-internal signal; no external truth"
INTERNAL_STATISTIC_REASON = "GRID-internal derived statistic"


def unscorable(pub: Publication, span: str, claim_type: str, reason: str, *, cause: str = "other",
               **fields: Any) -> dict[str, Any]:
    row = _row(pub, span, claim_type, verdict="unscorable", evidence=[], root_cause=reason[:2000],
               grader_confidence=1.0, **fields)
    row[CAUSE_KEY] = cause
    return row


KIND_KEY = "_kind"      # which grader path wrote the row (counted into the receipt's rows_by_kind)


def _kind(row: dict[str, Any], kind: str) -> dict[str, Any]:
    row.setdefault(KIND_KEY, kind)
    return row


def public_row(row: dict[str, Any]) -> dict[str, Any]:
    """The row as the review file carries it (the ingest's contract): no private keys."""
    return {k: v for k, v in row.items() if not k.startswith("_")}


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
                          cause="no_usable_close", instrument=ticker, horizon_end=iso(horizon_end), **direction_fields)
    evidence = [{"source": f"{PRICE_SOURCE} {INSTRUMENT_SERIES[ticker]}", "observed": f"{base.value:.4f} close {base.obs_date}",
                 "as_of": iso(session_close_utc(base.obs_date)), "url": None}]
    out = _close_on(obs, horizon_session)
    if horizon_end > graded_at or out is None:
        return _row(pub, span, "direction", instrument=ticker, horizon_end=iso(horizon_end), verdict="unresolved",
                    evidence=evidence, grader_confidence=0.5, **direction_fields)
    if not _usable_price(out.value):
        return unscorable(pub, span, "direction", f"unusable {PRICE_SOURCE} close for {ticker} on {horizon_session}",
                          cause="no_usable_close", instrument=ticker, horizon_end=iso(horizon_end), **direction_fields)
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


#: A level claim is a forecast only when its own words say so: a forecast verb or a horizon.
#: "SPY 5-day return is -0.63%" (market_briefings:5116) is a description the model read as "SPY < 0".
_FORECAST = re.compile(
    r"\b(?:will|should|would|could|may|might|expect(?:s|ed|ing)?|forecasts?|project(?:s|ed|ing)?|targets?|"
    r"likely|poised|set to|on track|seen|head(?:s|ed|ing)? (?:to|toward|towards|for|into)|finish(?:es|ing)?|"
    r"close (?:above|below|over|under|higher|lower)|closes (?:above|below|over|under)|"
    r"(?:stay|stays|hold|holds|remain|remains|break|breaks) (?:above|below|over|under)|reach(?:es)?|"
    r"by (?:the )?(?:close|end of|friday|monday|tuesday|wednesday|thursday|next)|into (?:the )?close|"
    r"tomorrow|next (?:week|session|close|day)|this week|end of (?:the )?(?:day|week|session)|eod)\b",
    re.IGNORECASE)
LEVEL_SCALE_BAND = (0.2, 5.0)        # a stated close level within 0.2x-5x of the last close


def level_claim_problem(conn: Any, pub: Publication, span: str, ticker: str, level: float) -> str | None:
    """Why an extracted level claim is not a forecast close level of ``ticker``, or None."""
    if not _FORECAST.search(span):
        return f"{NOT_A_CLOSE_LEVEL_REASON}: no forecast verb or horizon in the claim"
    base_session = baseline_session(pub.published_at)
    obs = [o for o in closes(conn, ticker, base_session - timedelta(days=10), base_session) if _usable_price(o.value)]
    if not obs:
        return f"{NOT_A_CLOSE_LEVEL_REASON}: no {PRICE_SOURCE} close for {ticker} on or before {base_session} to check its scale"
    last = obs[-1]
    ratio = level / last.value
    if not LEVEL_SCALE_BAND[0] <= ratio <= LEVEL_SCALE_BAND[1]:
        return (f"{NOT_A_CLOSE_LEVEL_REASON}: {level:g} is {ratio:.2f}x {ticker}'s {last.obs_date} close "
                f"{last.value:.2f}, outside {LEVEL_SCALE_BAND[0]:g}x-{LEVEL_SCALE_BAND[1]:g}x")
    return None


def grade_level(conn: Any, pub: Publication, span: str, ticker: str, comparator: str, level: float,
                horizon_session: date, graded_at: datetime) -> dict[str, Any]:
    horizon_end = session_close_utc(horizon_session)
    problem = level_claim_problem(conn, pub, span, ticker, level)
    if problem is not None:
        return unscorable(pub, span, "level", problem, cause="not_a_close_level", instrument=ticker)
    obs = closes(conn, ticker, horizon_session, horizon_session)
    out = _close_on(obs, horizon_session)
    if horizon_end > graded_at or out is None:
        return _row(pub, span, "level", instrument=ticker, horizon_end=iso(horizon_end), verdict="unresolved",
                    grader_confidence=0.5)
    if not _usable_price(out.value):
        return unscorable(pub, span, "level", f"unusable {PRICE_SOURCE} close for {ticker} on {horizon_session}",
                          cause="no_usable_close", instrument=ticker, horizon_end=iso(horizon_end))
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
                           cause="no_thesis_direction", instrument="SPY")]
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
    start: int = -1          # offset of the span in the window the figure was found in


def _overlaps(start: int, end: int, taken: list[tuple[int, int]]) -> bool:
    return any(a < end and start < b for a, b in taken)


def extract_figures(window: str, exclude: list[tuple[int, int]] | None = None) -> list[StatedFigure]:
    """Deterministic: "<instrument name> ... at <number>" level statements in a narrative window.

    ``exclude`` holds offsets another deterministic claim already covers (a realized move that
    states the close too): a figure inside one is not graded twice.
    """
    found: list[StatedFigure] = []
    taken: list[tuple[int, int]] = list(exclude or [])
    for fig, pattern in _FIGURE_RES:
        for m in pattern.finditer(window):
            start, end = m.start("name"), m.end("num") + (len(m.group("pct")) if m.group("pct") else 0)
            if m.group("paren") and window[end:end + 1] == ")":
                end += 1                        # "9-day VIX (29.26)": keep the closing parenthesis
            if start > 0 and window[start - 1] == "(" and (m.group("gap") or "").startswith(")"):
                start -= 1                      # "(VIX) stands at 28.44"
            if m.group("bare") and (m.group("gap") or "." not in m.group("num") or not _BARE_END.match(window, end)):
                continue
            if _overlaps(start, end, taken):
                continue                        # a more specific figure already claimed this text
            if _DERIVED.search(m.group("gap") or ""):
                continue
            if bool(m.group("pct")) != fig.pct:
                continue                        # "S&P 500 at -2.68%" is a move, "spread at 0.38" is not a rate
            if m.group("sign") and (not fig.pct or _MOVEMENT.search(m.group("gap") or "")):
                continue                        # a signed price, or a signed number after "moved", is a move
            raw = m.group("num").lstrip("$").replace(",", "")
            try:
                stated = float(raw)
            except ValueError:
                continue
            if m.group("sign") in ("-", "\u2212"):
                stated = -stated
            if not math.isfinite(stated):
                continue
            span = window[start:end].strip()
            if not MIN_SPAN_CHARS <= len(span) <= MAX_SPAN_CHARS or span not in window:
                continue
            decimals = len(raw.split(".", 1)[1]) if "." in raw else 0
            taken.append((start, end))
            found.append(StatedFigure(fig, span, stated, decimals, start))
    found.sort(key=lambda f: f.start)
    return found


# ── truth of a stated level at the instant it was stated ───────────────────────────────
#
# A briefing that says "the VIX stands at 26.9" at 20:00Z is making a claim about the market
# at 20:00Z, not about GRID's last completed close. So the grader looks at every capture GRID
# holds for that instrument around the statement (intraday pulls, the completed close, the
# session's high and low, later revisions) and asks whether the number was real:
#
# Intraday pulls are receipt-time evidence, not timestamped market prints. Nearby
# values/ranges can corroborate a claim but cannot confirm its exact-time value.
# Only a complete same-session range can refute an intraday value outside it.
# Close/FRED semantics below are unchanged; grader confidence is not calibrated.

SESSION_OPEN = (9, 30)
BRACKET_WINDOW = timedelta(minutes=90)   # a capture further from the statement does not pin its level

# A session low and high are one range only when one pull wrote both (see pair_range).
# ingestion/yfinance_pull.py::pull_ticker writes every field of one ticker from one
# yf.download frame inside one transaction, High loop before Low loop, stamping each row
# with its own _utc_now(). Rows of one call are therefore milliseconds apart: between
# High(D) and Low(D) sit at most ~5 dates x <=4 local round trips. Two calls for the same
# ticker are a whole pull_all (dozens of provider downloads) or a schedule slot apart.
# Rows stamped with a pull_run_id pair on it; legacy rows without one pair only inside
# RANGE_PAIR_TOLERANCE. A stall longer than that (advisory-lock wait, lazy split lookup)
# makes a legacy pair unscorable, never confirmed.
RANGE_PAIR_TOLERANCE = timedelta(seconds=5)
RANGE_RUN_MAX_SPAN = timedelta(minutes=10)   # one run id across a longer span is not one pull


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
    range_rows: dict[tuple[date, str], list[Any]] = field(default_factory=dict)   # every vintage, oldest first


@dataclass
class RangePair:
    """One session low and high that a single pull wrote."""

    low: Any
    high: Any
    basis: str          # "pull_run_id" or "retrieval_tolerance"


def _run_id(o: Any) -> str | None:
    rid = getattr(o, "pull_run_id", None)
    return rid if isinstance(rid, str) and rid else None


def same_pull(lo: Any, hi: Any) -> str | None:
    """How ``lo`` and ``hi`` are known to come from one pull, or None when they are not.

    Fail closed: a differing source never pairs; two run ids pair only when equal (and not
    stretched over RANGE_RUN_MAX_SPAN); a run id on one row only means two writer versions,
    hence two pulls; rows without run ids pair only within RANGE_PAIR_TOLERANCE.
    """
    if not lo.source or lo.source != hi.source or lo.pull_timestamp is None or hi.pull_timestamp is None:
        return None
    gap = abs(lo.pull_timestamp - hi.pull_timestamp)
    lo_run, hi_run = _run_id(lo), _run_id(hi)
    if lo_run is not None and hi_run is not None:
        return "pull_run_id" if lo_run == hi_run and gap <= RANGE_RUN_MAX_SPAN else None
    if lo_run is not None or hi_run is not None:
        return None
    return "retrieval_tolerance" if gap <= RANGE_PAIR_TOLERANCE else None


def pair_range(facts: "Facts", session: date) -> RangePair | None:
    """The newest low/high of ``session`` written by one pull, or None."""
    best: RangePair | None = None
    for lo in facts.range_rows.get((session, "low"), []):
        for hi in facts.range_rows.get((session, "high"), []):
            basis = same_pull(lo, hi)
            if basis is None or lo.value > hi.value:
                continue
            newest = max(lo.pull_timestamp, hi.pull_timestamp)
            if best is None or newest > max(best.low.pull_timestamp, best.high.pull_timestamp):
                best = RangePair(lo, hi, basis)
    return best


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
                facts.range_rows.setdefault((o.obs_date, name.rsplit(":", 1)[1]), []).append(o)
    else:
        rows = []
        for series_id in (fig.series_id, *fig.alt_series):
            rows = _vintages(conn, series_id, at.date() - timedelta(days=fig.lookback_days), at.date())
            if rows:
                facts.series_id = series_id   # the first id that holds the quantity in the window
                break
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
    src = f"raw_series {facts.series_id}"
    if not any(facts.by_date.values()):
        return None
    # Intraday current-level claims require market event times for confirmation.
    # Observation currently has only pull_timestamp: never relabel it as event time.
    if facts.intraday is not None:
        session = facts.intraday
        close = session_close_utc(session)
        rows = [o for o in facts.by_date.get(session, [])
                if session_open_utc(session) <= o.pull_timestamp < close
                and abs(o.pull_timestamp - at) <= BRACKET_WINDOW]
        before = [o for o in rows if o.pull_timestamp <= at]
        after = [o for o in rows if o.pull_timestamp > at]
        bracket = before[-1:] + after[:1]
        if bracket and _within(value, dec, min(o.value for o in bracket), max(o.value for o in bracket)):
            ev = [_ev(f"{o.source} {facts.series_id}",
                      f"{o.value:g}{unit} retrieved {iso(o.pull_timestamp)}; market event time unknown",
                      o.pull_timestamp) for o in bracket]
            return Judgment("partially_confirmed", ev,
                            "nearby captures corroborate plausibility, not the value at the statement instant; "
                            "retrieval time is not market event time", None, 0.6)
        # The newest low and high that one pull wrote (pair_range); a low from one pull
        # and a high from another are not one range.
        pair = pair_range(facts, session)
        if pair is not None:
            lo, hi = pair.low.value, pair.high.value
            low_at, high_at = pair.low.pull_timestamp, pair.high.pull_timestamp
            paired = ("same pull run" if pair.basis == "pull_run_id"
                      else f"retrieved within {int(RANGE_PAIR_TOLERANCE.total_seconds())} s")
            ev = [_ev(f"{o.source} {facts.series_id.rsplit(':', 1)[0]}:{name}",
                      f"session {session} {name} {o.value:g}{unit}; retrieved {iso(o.pull_timestamp)}; "
                      f"paired: {paired}", o.pull_timestamp)
                  for name, o in (("low", pair.low), ("high", pair.high))]
            if _within(value, dec, lo, hi):
                return Judgment("partially_confirmed", ev,
                                "inside the session range; no market-time observation pins the stated instant",
                                None, 0.6)
            # A newer single-leg capture (another pull) can only widen what is known to have traded.
            hull_lo = min([lo] + [o.value for o in facts.range_rows.get((session, "low"), [])
                                  if o.pull_timestamp >= close])
            hull_hi = max([hi] + [o.value for o in facts.range_rows.get((session, "high"), [])
                                  if o.pull_timestamp >= close])
            if low_at >= close and high_at >= close and not _within(value, dec, hull_lo, hull_hi):
                return Judgment("refuted", ev,
                                f"stated {fig.label} {value:g}{unit} is outside the complete {session} "
                                f"session range {lo:g}-{hi:g}{unit}", False, 0.85)
        # Sparse samples cannot disprove a transient price. A prior close is not
        # evidence of today's current price either. Keep the claim unscorable.
        return Judgment("unscorable", [],
                        "no market-time observation or complete same-session range establishes or contradicts "
                        "the stated intraday level; sparse captures are not refutation", None, 0.0)
    # 2. a completed close
    closes = [(s, _completed_close(facts, s)) for s in facts.sessions]
    closes = [(s, o) for s, o in closes if o is not None]
    known_at = [(s, o) for s, o in closes if o.pull_timestamp <= at]
    newest_known = known_at[0][0] if known_at else None
    for s, o in closes:
        if not figure_matches(value, dec, o.value):
            continue
        ev = [_ev(f"{o.source} {facts.series_id}", f"{o.value:g}{unit} close {s} (captured {iso(o.pull_timestamp)})",
                  o.pull_timestamp)]
        if newest_known is not None and s < newest_known:
            newer = dict(known_at)[newest_known]
            ev.append(_ev(f"{newer.source} {facts.series_id}", f"{newer.value:g}{unit} close {newest_known} "
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
                            [_ev(f"raw_series {facts.series_id.rsplit(':', 1)[0]}:low/high",
                                 f"session {s} range {lo:g}-{hi:g}{unit}", at)],
                            f"{stated.span!r} lies inside the {s} range {lo:g}-{hi:g}{unit}, but no capture "
                            f"pins it to the statement time", None, 0.6)
        hit = next((o for o in facts.by_date.get(s, []) if figure_matches(value, dec, o.value)), None)
        if hit is not None:
            return Judgment("partially_confirmed",
                            [_ev(f"{hit.source} {facts.series_id}", f"{hit.value:g}{unit} intraday capture "
                                 f"{iso(hit.pull_timestamp)}", hit.pull_timestamp)],
                            f"{stated.span!r} equals an intraday capture of {s}, not a close", None, 0.6)
    # 4. nothing supports it
    ev: list[dict[str, Any]] = []
    for s in ([facts.intraday] if facts.intraday else []) + facts.sessions[:2]:
        rows = facts.by_date.get(s, [])
        if rows:
            near = min(rows, key=lambda o: abs((o.pull_timestamp - at).total_seconds()))
            ev.append(_ev(f"{near.source} {facts.series_id}", f"{near.value:g}{unit} captured "
                          f"{iso(near.pull_timestamp)} (label {s})", near.pull_timestamp))
        if s in facts.low and s in facts.high:
            ev.append(_ev(src + " low/high", f"session {s} range {facts.low[s]:g}-{facts.high[s]:g}{unit}", at))
    known_vals = ", ".join(e["observed"].split(" ")[0] for e in ev if "captured" in e["observed"])
    return Judgment("refuted", ev[:6],
                    f"stated {fig.label} {value:g}{unit}; GRID's captures around the statement were {known_vals or 'other values'}"
                    + (" and the value is outside the session range" if any("range" in e["observed"] for e in ev) else ""),
                    True, 0.85)


def _off_scale(stated: StatedFigure, facts: Facts) -> str | None:
    """A stated level far off every value GRID holds for the series is not the level."""
    fig = stated.figure
    values = [o.value for rows in facts.by_date.values() for o in rows if o.value > 0]
    if not fig.scale_guard or not values:
        return None
    ref = sorted(values)[len(values) // 2]
    ratio = stated.stated / ref
    if SCALE_BAND[0] <= ratio <= SCALE_BAND[1]:
        return None
    unit = "%" if fig.pct else ""
    return (f"{fig.label}: stated {stated.stated:g}{unit} is {ratio:.2f}x the {facts.series_id} level GRID holds "
            f"({ref:g}{unit}): a ratio, return or score, not the level")


def grade_figure(conn: Any, pub: Publication, stated: StatedFigure) -> dict[str, Any]:
    fig = stated.figure
    horizon = iso(pub.published_at + FIGURE_HORIZON)
    try:
        facts = gather_facts(conn, fig, pub.published_at)
        off_scale = _off_scale(stated, facts)
        judgment = None if off_scale else judge_level(stated, pub.published_at, facts)
    except Exception as exc:  # a reader fault is this claim's reason, not the day's
        return unscorable(pub, stated.span, "data_quality",
                          f"{fig.label}: {fig.series_id} unreadable ({type(exc).__name__})", cause="reader_fault",
                          horizon_end=horizon)
    if off_scale:
        return unscorable(pub, stated.span, "data_quality", off_scale, cause="off_scale",
                          instrument=_instrument_for(fig), horizon_end=horizon)
    if judgment is None:
        series = "/".join((fig.series_id, *fig.alt_series))
        return unscorable(pub, stated.span, "data_quality",
                          f"{fig.label}: GRID holds no {series} capture around the statement", cause="no_capture",
                          horizon_end=horizon)
    row = _row(pub, stated.span, "data_quality", instrument=_instrument_for(fig), horizon_end=horizon,
               verdict=judgment.verdict, evidence=judgment.evidence, knowable_at_stated_at=judgment.knowable,
               root_cause=judgment.root_cause, grader_confidence=judgment.confidence)
    if judgment.verdict == "unscorable":
        row[CAUSE_KEY] = "intraday_unpinned"
    return row


def _instrument_for(fig: Figure) -> str:
    sid = fig.series_id
    return (sid.split(":")[1] if sid.startswith("YF:") else sid)[:32]


# ── realized moves ("the S&P 500 fell 1.92% to 7416.59") ──────────────────────────────
#
# The diary's WHAT HAPPENED section (intelligence/market_diary.py) and a briefing's
# "## What Changed" restate one session's close-to-close move. That is checkable at once on
# the diary's own basis: yfinance raw closes YF:<ticker>:close (market_diary PRICE_BASIS
# "raw_close", _PRICE_SOURCE "yfinance"), close(session) / close(previous session) - 1 at the
# printed precision. Confirmed only when GRID had captured both completed closes at or before
# the statement; a match on closes captured later is partially_confirmed, knowable=false.

MOVE_PRICE_SOURCE = "yfinance"      # intelligence/market_diary.py _PRICE_SOURCE


@dataclass(frozen=True)
class MoveInstrument:
    series_id: str
    label: str
    names: str
    diary_only: bool = False         # "Gold" is GC=F in the diary; in a briefing it may mean GLD


MOVE_INSTRUMENTS: tuple[MoveInstrument, ...] = (
    MoveInstrument("YF:^GSPC:close", "S&P 500", r"S&P 500|S&P|SPX"),
    MoveInstrument("YF:^DJI:close", "Dow Jones", r"Dow Jones(?: Industrial Average)?|Dow"),
    MoveInstrument("YF:^IXIC:close", "Nasdaq Composite", r"Nasdaq(?: Composite)?"),
    MoveInstrument("YF:^RUT:close", "Russell 2000", r"Russell 2000"),
    MoveInstrument("YF:^VIX:close", "VIX", r"VIX(?![ -]?(?:9|3M|9-day|3-month))"),
    MoveInstrument("YF:SPY:close", "SPY", r"SPY"),
    *(MoveInstrument(f"YF:{t}:close", t, t)
      for t in ("XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLC", "XLB")),
    MoveInstrument("YF:UUP:close", "UUP", r"UUP"),
    MoveInstrument("YF:TLT:close", "TLT", r"TLT"),
    MoveInstrument("YF:GC=F:close", "Gold", r"Gold", diary_only=True),
    MoveInstrument("YF:CL=F:close", "Crude Oil", r"Crude Oil|Crude|WTI", diary_only=True),
)
_MOVE_UP = (r"up|rose|rises|rising|gained|gains|gaining|added|adds|adding|climbed|climbs|climbing|advanced|advances|"
            r"advancing|surged|surges|surging|jumped|jumps|jumping|rallied|rallies|rallying|increased|increases|"
            r"increasing|soared|soaring|rebounded|rebounding|higher")
_MOVE_DOWN = (r"down|fell|falls|falling|dropped|drops|dropping|lost|loses|losing|declined|declines|declining|slipped|"
              r"slips|slipping|slid|slides|sliding|retreated|retreats|retreating|shed|sheds|shedding|sank|sinks|sinking|"
              r"tumbled|tumbles|tumbling|plunged|plunges|plunging|decreased|decreases|decreasing|eased|eases|easing|lower")
_MOVE_NOUN_UP, _MOVE_NOUN_DOWN = r"gain|rise|increase|jump|advance|rally|surge", r"decline|drop|fall|loss|slide|retreat"
_MOVE_NUM = r"[0-9][0-9,]*(?:\.[0-9]+)?"
_MOVE_NAMES = "|".join(f"(?:{i.names})" for i in MOVE_INSTRUMENTS)
_MOVE_NAME_AT = rf"(?<![A-Za-z0-9^/])(?:{_MOVE_NAMES})(?![A-Za-z0-9/])"
_MOVE_RE = re.compile(
    rf"(?<![A-Za-z0-9^/])(?P<name>{_MOVE_NAMES})(?![A-Za-z0-9/])"
    rf"(?P<gap>\)?(?:\s+closed\s+at\s+\$?(?P<lvl1>{_MOVE_NUM}),?)?(?:(?!{_MOVE_NAME_AT})[^0-9\n.;:()\[\]%$]){{0,60}}?)"
    rf"(?:\b(?P<verb>{_MOVE_UP}|{_MOVE_DOWN})\s+(?P<s1>[+\-−])?(?P<p1>{_MOVE_NUM})%"
    rf"|\b(?:at|of)\s+(?P<s2>[+\-−])(?P<p2>{_MOVE_NUM})%"
    rf"|\(\s*(?P<s3>[+\-−])(?P<p3>{_MOVE_NUM})%\s*\)"
    rf"|\ba\s+(?P<s4>[+\-−])?(?P<p4>{_MOVE_NUM})%\s+(?P<noun>{_MOVE_NOUN_UP}|{_MOVE_NOUN_DOWN})\b)"
    rf"(?:\s+(?:to|at)\s+\$?(?P<lvl2>{_MOVE_NUM})(?![0-9]))?"
    rf"(?=(?P<tail>[^\n]{{0,48}}))",
    re.IGNORECASE)
#: Words that make the number something other than this session's close-to-close move.
_MOVE_GAP_REJECT = re.compile(r"\b(?:week|month|year|since|ytd|to-date|intraday|pre-?market|after-?hours|futures|"
                              r"overnight|five-day|weekly|monthly|both|each|combined|together|average|respectively)",
                              re.IGNORECASE)
_MOVE_TAIL_REJECT = re.compile(
    r"^\W{0,3}(?:(?:over|in|during|for|since|from|across|this|last|past|on|so far)\b[^.;\n]{0,30}?"
    r"\b(?:days?|weeks?|months?|years?|sessions?|ytd|highs?|lows?|peak)\b"
    r"|year-to-date|ytd|month-to-date|week-to-date|respectively|week-over-week|month-over-month|year-over-year)",
    re.IGNORECASE)
_DIARY_HEAD = re.compile(r"(?mi)^[ \t]*(?:#{1,4}[ \t]*)?(?:\*\*)?WHAT HAPPENED(?:\*\*)?[ \t]*$")
_BRIEFING_HEAD = re.compile(r"(?m)^##[ \t]+What Changed[ \t]*$")
_SECTION_END = re.compile(r"(?m)^[ \t]*(?:#{1,4}[ \t]+\S|\*\*[A-Z][A-Za-z ]+\*\*[ \t]*$|---[ \t]*$)")


@dataclass(frozen=True)
class StatedMove:
    instrument: MoveInstrument
    span: str
    start: int
    end: int
    pct: float              # signed, percent
    pct_decimals: int
    level: float | None     # the close the sentence states too ("to 7416.59", "closed at 7561.67")
    level_decimals: int


def _num(raw: str) -> tuple[float, int]:
    clean = raw.replace(",", "")
    return float(clean), (len(clean.split(".", 1)[1]) if "." in clean else 0)


def move_section(pub: Publication, window: str) -> tuple[int, int] | None:
    """Offsets of the section that restates the session: the diary's WHAT HAPPENED, a briefing's What Changed."""
    head = (_DIARY_HEAD if pub.channel == "market_diary" else _BRIEFING_HEAD).search(window)
    if head is None:
        return None
    end = _SECTION_END.search(window, head.end())
    return head.end(), end.start() if end else len(window)


def extract_moves(pub: Publication, window: str) -> list[StatedMove]:
    """Deterministic: "<name> (closed at X,)? <up/down verb> N.NN% (to X)?" in the restating section."""
    section = move_section(pub, window)
    if section is None:
        return []
    lo, hi = section
    by_name = [(re.compile(rf"(?:{i.names})\Z", re.IGNORECASE), i) for i in MOVE_INSTRUMENTS]
    found: list[StatedMove] = []
    pos = lo
    while True:
        m = _MOVE_RE.search(window, pos, hi)
        if m is None:
            return found
        pos = m.start() + 1
        inst = next((i for rx, i in by_name if rx.match(m.group("name"))), None)
        if inst is None or (inst.diary_only and pub.channel != "market_diary"):
            continue
        if _MOVE_GAP_REJECT.search(m.group("gap") or "") or _MOVE_TAIL_REJECT.search(m.group("tail") or ""):
            continue
        sign = next((m.group(g) for g in ("s1", "s2", "s3", "s4") if m.group(g)), None)
        raw = next(m.group(g) for g in ("p1", "p2", "p3", "p4") if m.group(g))
        word = (m.group("verb") or m.group("noun") or "").lower()
        if word:
            up = re.fullmatch(_MOVE_UP + "|" + _MOVE_NOUN_UP, word, re.IGNORECASE) is not None
            if sign and (sign == "+") != up:
                continue                       # "rose -0.4%": the sentence contradicts itself
            negative = not up
        else:
            negative = sign in ("-", "−")
        pct, pct_dec = _num(raw)
        if negative:
            pct = -pct
        level_raw = m.group("lvl1") or m.group("lvl2")
        level, level_dec = _num(level_raw) if level_raw else (None, 0)
        start, end = m.start("name"), m.end()
        if start > 0 and window[start - 1] == "(" and (m.group("gap") or "").startswith(")"):
            start -= 1
        span = window[start:end].strip()
        if not MIN_SPAN_CHARS <= len(span) <= MAX_SPAN_CHARS or not math.isfinite(pct):
            continue
        found.append(StatedMove(inst, span, start, end, pct, pct_dec, level, level_dec))
        pos = end


def move_session(pub: Publication) -> tuple[date | None, str | None]:
    """The session whose close-to-close move the record restates, or (None, why not)."""
    at = pub.published_at
    if pub.channel == "market_diary":
        raw = pub.source_record.get("date")
        try:
            d = date.fromisoformat(raw[:10]) if isinstance(raw, str) else None
        except ValueError:
            d = None
        if d is None or not _is_market_open(d) or session_close_utc(d) > at:
            return None, f"diary date {raw!r} is not a session completed before the statement"
        return d, None
    today = ny_date(at)
    if _is_market_open(today) and session_open_utc(today) <= at < session_close_utc(today):
        return None, "stated during the session: an intraday move, not a close-to-close one"
    return baseline_session(at), None


def _closeness(stated: float, decimals: int, actual: float, slack: float, extra: float = 0.0) -> str:
    """'match' at the printed precision (plus ``extra``), 'miss' beyond that plus ``slack``, 'near' between."""
    tolerance = 0.5 * 10 ** (-decimals) + extra + 1e-9
    diff = abs(stated - actual)
    if diff <= tolerance:
        return "match"
    return "miss" if diff > tolerance + slack else "near"


def grade_move(conn: Any, pub: Publication, mv: StatedMove) -> dict[str, Any]:
    inst, at = mv.instrument, pub.published_at
    horizon = iso(at + FIGURE_HORIZON)
    instrument = inst.series_id.split(":")[1][:32]
    session, why = move_session(pub)
    if session is None:
        return unscorable(pub, mv.span, "data_quality", f"{inst.label} move: {why}", cause="no_session",
                          instrument=instrument, horizon_end=horizon)
    prev = previous_session(session)
    try:
        rows = [o for o in _vintages(conn, inst.series_id, prev, session) if o.source == MOVE_PRICE_SOURCE]
    except Exception as exc:  # a reader fault is this claim's reason, not the day's
        return unscorable(pub, mv.span, "data_quality", f"{inst.label}: {inst.series_id} unreadable ({type(exc).__name__})",
                          cause="reader_fault", instrument=instrument, horizon_end=horizon)

    def completed(d: date) -> list[Any]:
        return [o for o in rows if o.obs_date == d and o.pull_timestamp >= session_close_utc(d) and o.value > 0]

    s_all, p_all = completed(session), completed(prev)
    if not s_all or not p_all:
        missing = " and ".join(str(d) for d, have in ((prev, p_all), (session, s_all)) if not have)
        return unscorable(pub, mv.span, "data_quality",
                          f"{inst.label}: GRID holds no completed {MOVE_PRICE_SOURCE} {inst.series_id} close for {missing}",
                          cause="no_capture", instrument=instrument, horizon_end=horizon)
    s_known = [o for o in s_all if o.pull_timestamp <= at]
    p_known = [o for o in p_all if o.pull_timestamp <= at]
    knowable = bool(s_known and p_known)
    close_s, close_p = (s_known or s_all)[-1], (p_known or p_all)[-1]
    actual = (close_s.value / close_p.value - 1.0) * 100.0
    checks = [("move", _closeness(mv.pct, mv.pct_decimals, actual, max(0.02, 0.005 * abs(actual))))]
    if mv.level is not None:       # figure_matches' tolerance: half a printed unit + 0.05 % (rounded before printing)
        checks.append(("close", _closeness(mv.level, mv.level_decimals, close_s.value, 0.002 * abs(close_s.value),
                                           extra=0.0005 * abs(close_s.value))))
    src = f"{MOVE_PRICE_SOURCE} {inst.series_id}"
    ev = [_ev(src, f"{o.value:g} close {d} (captured {iso(o.pull_timestamp)})", o.pull_timestamp)
          for d, o in ((prev, close_p), (session, close_s))]
    ev.append(_ev("grader", f"{inst.label} {actual:+.4f}% {prev} -> {session}; stated {mv.pct:+.{mv.pct_decimals}f}%"
                  + (f", close {mv.level:g}" if mv.level is not None else ""),
                  max(close_s.pull_timestamp, close_p.pull_timestamp)))
    outcome = {name: result for name, result in checks}
    said = f"{mv.pct:+.{mv.pct_decimals}f}%" + (f" to {mv.level:g}" if mv.level is not None else "")
    did = f"{actual:+.2f}% to {close_s.value:g}"
    if not knowable:
        if all(r == "match" for r in outcome.values()):
            return _row(pub, mv.span, "data_quality", instrument=instrument, horizon_end=horizon,
                        verdict="partially_confirmed", evidence=ev, knowable_at_stated_at=False,
                        root_cause=f"{inst.label} {prev} -> {session} matches, but GRID captured "
                                   f"{'both closes' if not s_known and not p_known else 'a close'} only after the statement",
                        grader_confidence=0.6)
        return unscorable(pub, mv.span, "data_quality",
                          f"{inst.label}: GRID's closes for {prev} -> {session} were captured after the statement and "
                          f"show {did} against {said}; not judged", cause="move_unknowable",
                          instrument=instrument, horizon_end=horizon)
    if all(r == "match" for r in outcome.values()):
        return _row(pub, mv.span, "data_quality", instrument=instrument, horizon_end=horizon, verdict="confirmed",
                    evidence=ev, knowable_at_stated_at=True, grader_confidence=0.9)
    if "miss" in outcome.values():
        wrong = " and ".join(name for name, r in checks if r == "miss")
        return _row(pub, mv.span, "data_quality", instrument=instrument, horizon_end=horizon, verdict="refuted",
                    evidence=ev, knowable_at_stated_at=True, grader_confidence=0.85,
                    root_cause=f"stated {inst.label} {said}; GRID's closes known at the statement show {did} "
                               f"({prev} -> {session}): the {wrong} {'differ' if ' and ' in wrong else 'differs'}")
    return _row(pub, mv.span, "data_quality", instrument=instrument, horizon_end=horizon,
                verdict="partially_confirmed", evidence=ev, knowable_at_stated_at=True, grader_confidence=0.6,
                root_cause=f"stated {inst.label} {said}; GRID shows {did} ({prev} -> {session}): within rounding "
                           f"distance, not at the printed precision")


# ── a briefing or diary record ─────────────────────────────────────────────────────────

#: A claim on a quantity only GRID computes (its own scores, ratios, counts). There is no
#: outside price to grade it against; the wording only picks the unscorable reason.
_INTERNAL_SIGNAL = re.compile(r"\bscor(?:e|ed|es|ing)\b|insider|congress|\bbuys?\b.{0,20}\bsells?\b|ratio|sentiment|"
                              r"breadth|mentions|\bsignal\b|risk-(?:on|off)", re.IGNORECASE)
_INTERNAL_STATISTIC = re.compile(r"breadth|sector ETFs (?:are )?(?:positive|negative|up|down)|social mentions|mentions|"
                                 r"bull ratio|sentiment|\bscore\b", re.IGNORECASE)


def grade_briefing(conn: Any, pub: Publication, graded_at: datetime, folder: Path, client_factory: Any,
                   *, persist: bool = True) -> tuple[list[dict[str, Any]], str | None]:
    """Rows for a briefing/diary record plus the extraction model used (None when no model was needed)."""
    withheld = marker_span(pub.text)
    if withheld is not None:
        row = unscorable(pub, withheld, "data_quality", WITHHELD_REASON, cause="withheld_narrative")
        row[KIND_KEY] = "withheld_placeholder"
        return [row], None
    opening = first_sentence_span(pub.text)
    window = narrative_window(pub.text)
    moves = extract_moves(pub, window)
    figures = extract_figures(window, exclude=[(mv.start, mv.end) for mv in moves])
    deterministic = [_kind(grade_move(conn, pub, mv), "realized_move") for mv in moves] + \
                    [_kind(grade_figure(conn, pub, f), "figure") for f in figures]
    graded_spans = [mv.span for mv in moves] + [f.span for f in figures]
    extraction = cached_extraction(folder, pub, client_factory, persist=persist)
    if extraction.error:
        if deterministic:
            return deterministic, extraction.model
        rows = [] if opening is None else [_kind(unscorable(pub, opening, "data_quality",
                                                            f"claim extraction unavailable: {extraction.error}",
                                                            cause="extraction_unavailable"), "placeholder")]
        return rows, extraction.model
    rows: list[dict[str, Any]] = list(deterministic)
    for claim in extraction.claims:
        span, kind = claim["span"], claim["claim_type"]
        if any(g in span for g in graded_spans):
            continue      # the sentence's figure or move is already graded deterministically
        ticker = resolve_instrument(claim["instrument"])
        session = horizon_session_for(pub, claim["horizon"])
        internal = _INTERNAL_SIGNAL.search(span) or _INTERNAL_SIGNAL.search(claim["instrument"] or "")
        if kind == "level" and ticker is not None and resolve_instrument(claim["instrument"], allow_alias=False) is None:
            rows.append(_kind(unscorable(pub, span, kind, f"level claim on {claim['instrument']!r}: only an ETF proxy "
                                                          f"({ticker}) is priced and its scale differs",
                                         cause="alias_level"), "llm"))
            continue
        if kind == "direction":
            if ticker is None:
                row = (unscorable(pub, span, kind, INTERNAL_SIGNAL_REASON, cause="internal_signal") if internal else
                       unscorable(pub, span, kind, f"no price series for instrument {claim['instrument']!r}",
                                  cause="no_price_series"))
            elif claim["direction"] is None:
                row = unscorable(pub, span, kind, "direction claim without a bullish/bearish/neutral direction",
                                 cause="no_direction", instrument=ticker)
            else:
                row = grade_direction(conn, pub, span, ticker, claim["direction"], session, graded_at)
        elif kind == "level":
            if ticker is None or claim["comparator"] is None or claim["level"] is None:
                row = (unscorable(pub, span, kind, INTERNAL_STATISTIC_REASON, cause="internal_statistic")
                       if _INTERNAL_STATISTIC.search(span) else
                       unscorable(pub, span, kind, "level claim without a priced instrument, comparator and number",
                                  cause="incomplete_level"))
            else:
                row = grade_level(conn, pub, span, ticker, claim["comparator"], claim["level"], session, graded_at)
        else:  # event, causal: v1 cannot judge; a causal claim is never confirmed from co-movement
            row = _row(pub, span, kind, instrument=ticker, verdict="unresolved", grader_confidence=0.3)
        rows.append(_kind(row, "llm"))
    if not rows and opening is not None:
        rows.append(_kind(unscorable(pub, opening, "data_quality", NOTHING_EXTRACTED_REASON, cause="nothing_extracted"),
                          "placeholder"))
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


def unscorable_by_cause(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Unscorable rows per machine-readable cause (CAUSE_KEY); the counts sum to claims_unscorable."""
    counts: dict[str, int] = {}
    for row in rows:
        if row["verdict"] == "unscorable":
            cause = row.get(CAUSE_KEY) or "other"
            counts[cause] = counts.get(cause, 0) + 1
    return dict(sorted(counts.items()))


def withheld_records(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Records per channel whose AI narrative was withheld (one placeholder row each)."""
    counts: dict[str, int] = {}
    for row in rows:
        if row.get(CAUSE_KEY) == "withheld_narrative":
            channel = row["publication_id"].split(":")[0]
            counts[channel] = counts.get(channel, 0) + 1
    return dict(sorted(counts.items()))


def build_header(day: date, packet_sha: str, graded_at: datetime, rows: list[dict[str, Any]], models: set[str],
                 by_cause: dict[str, int] | None = None, records_withheld: int = 0) -> dict[str, Any]:
    """``by_cause`` and ``records_withheld`` go into the free-form ``grader`` object (the ingest stores it
    as grader_json): a withheld narrative stays one unscorable row, so claims_unscorable still equals
    the unscorable rows, and the split says how many of them are placeholders rather than claims."""
    unscorable_n = sum(1 for r in rows if r["verdict"] == "unscorable")
    return {"kind": "review_header", "schema": REVIEW_SCHEMA, "publication_day": day.isoformat(),
            "packet_sha256": packet_sha,
            "grader": {"model": GRADER_VERSION, "role": "grader", "job": JOB, "extraction_models": sorted(models),
                       "unscorable_by_cause": dict(by_cause or {}), "records_withheld": records_withheld},
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


MAX_REPORTED_CHANGES = 500


def verdict_changes(before_lines: list[bytes], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Rows whose verdict differs from the review file on disk (new, changed or gone), for a dry run:
    what a grader change would do to a day, row by row, before it writes anything."""
    def key(row: dict[str, Any]) -> tuple[Any, Any]:
        return row.get("publication_id"), row.get("claim_sha256")

    old: dict[tuple[Any, Any], dict[str, Any]] = {}
    for line in before_lines:
        try:
            row = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(row, dict) and row.get("kind") == "claim_verdict":
            old[key(row)] = row
    new = {key(r): r for r in rows}
    out: list[dict[str, Any]] = []
    for k in [*new, *(k for k in old if k not in new)]:
        a, b = old.get(k), new.get(k)
        if a is not None and b is not None and a.get("verdict") == b.get("verdict"):
            continue
        r = b or a
        out.append({"publication_id": r.get("publication_id"), "claim_span": str(r.get("claim_span"))[:120],
                    "before": a.get("verdict") if a else None, "after": b.get("verdict") if b else None,
                    "root_cause": str(r.get("root_cause") or "")[:200] or None})
    return out[:MAX_REPORTED_CHANGES]


def confirmed_ingest(receipt: dict[str, Any], review_sha: str, packet_sha: str) -> dict[str, Any] | None:
    """Only v2 evidence copied from the ingest's actual byte identity can skip a hand-off.

    Legacy ingested_review_sha256 alone was fabricated by --no-ingest in v1. It is
    not evidence. The next normal run reconciles it through the idempotent ingest.
    """
    confirmation = receipt.get("ingest_confirmation")
    if (isinstance(confirmation, dict)
            and confirmation.get("schema") == "publication-review-ingest-confirmation-v1"
            and confirmation.get("review_sha256") == review_sha
            and confirmation.get("packet_sha256") == packet_sha
            and type(confirmation.get("run_id")) is int and confirmation["run_id"] > 0):
        return dict(confirmation)
    return None


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
                    rows.extend(_kind(r, "thesis") for r in grade_thesis(conn, pub, graded_at))
                else:
                    briefing_rows, model = grade_briefing(conn, pub, graded_at, folder, factory, persist=not dry_run)
                    rows.extend(briefing_rows)
                    if model:
                        models.add(model)
        rows = dedupe(rows)
        if not rows:
            raise Refused("no publication yielded a gradeable row")
        by_cause, withheld = unscorable_by_cause(rows), withheld_records(rows)
        kinds: dict[str, int] = {}
        for row in rows:
            kinds[row.get(KIND_KEY) or "other"] = kinds.get(row.get(KIND_KEY) or "other", 0) + 1
        rows = [public_row(r) for r in rows]
        header = build_header(day, packet.sha256, graded_at, rows, models, by_cause, sum(withheld.values()))
        payload = render(header, rows)
        if len(payload) > MAX_REVIEW_BYTES:
            raise Refused(f"review.jsonl would be {len(payload)} bytes, more than the ingest accepts")
        receipt.update(unscorable_by_cause=by_cause, records_withheld=sum(withheld.values()),
                       records_withheld_by_channel=withheld, rows_by_kind=dict(sorted(kinds.items())))
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
            receipt["verdict_changes"] = verdict_changes(before[2] if before else [], rows)
            return receipt
        if unchanged:
            receipt["state"] = "UNCHANGED"
            receipt["review_sha256"] = sha256(before[0])
            # The file on disk is current; make sure the database agrees. A hand-off that failed
            # on an earlier run (database down, module error) is retried here, not forgotten.
            prior_confirmation = confirmed_ingest(previous_receipt(folder), receipt["review_sha256"], packet.sha256)
            if ingest and prior_confirmation is None:
                receipt["ingest"] = run_ingest(engine, day, review_root, packet_root)
        else:
            write_file(path, payload)
            receipt["state"] = "WRITTEN"
            receipt["superseded_previous"] = before is not None
            if ingest:
                receipt["ingest"] = run_ingest(engine, day, review_root, packet_root)
        result = receipt.get("ingest", {})
        if result.get("state") in {"INGESTED", "PRESENT"}:
            # The ingest reopens the file. Another invocation could have replaced it
            # after our write/read: certify its returned bytes, never just its status.
            if (result.get("review_sha256") != receipt["review_sha256"]
                    or result.get("packet_sha256") != packet.sha256
                    or type(result.get("run_id")) is not int or result["run_id"] <= 0):
                receipt["ingest_confirmation_error"] = "ingest result does not bind the expected review, packet and run"
            elif result.get("rejected_rows") != 0 or result.get("declared_mismatch") != []:
                receipt["ingest_confirmation_error"] = "ingest result is incomplete or has rejected rows"
            else:
                receipt["ingest_confirmation"] = {
                    "schema": "publication-review-ingest-confirmation-v1",
                    "review_sha256": result["review_sha256"], "packet_sha256": result["packet_sha256"],
                    "run_id": result["run_id"],
                }
        elif "ingest" not in receipt and unchanged and prior_confirmation is not None:
            receipt["ingest_confirmation"] = prior_confirmation
        if "ingest_confirmation" in receipt:
            receipt["ingested_review_sha256"] = receipt["ingest_confirmation"]["review_sha256"]
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
                                              "declared_mismatch", "receipt_file", "review_sha256", "packet_sha256")}


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
        if (r.get("ingest_confirmation_error") or ing.get("state") in {"REFUSED", "FAILED"}
                or ing.get("rejected_rows") or ing.get("declared_mismatch")):
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
    parser.add_argument("--dry-run", action="store_true",
                        help="grade and report (with --json: every row whose verdict would change); write nothing, "
                             "ingest nothing")
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
