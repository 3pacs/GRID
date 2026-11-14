"""Offline tests for scripts/grade_publication_review.py (the in-repo daily review grader).

No database, no LLM: closes come from a stub, the LLM client is a fake, and every produced
review.jsonl is pushed through the real ingest validators (scripts/ingest_publication_review.py)
so the two scripts can never drift apart silently.
"""
from __future__ import annotations

import contextlib
import hashlib
import importlib.util
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import ClassVar

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


grader = _load("grade_publication_review")
ingest = _load("ingest_publication_review")

from store.observations import Observation  # noqa: E402

DAY = date(2026, 11, 7)                       # UTC Saturday; T_SAT is still Friday in New York
T_FRI_PRE_CLOSE = datetime(2026, 11, 6, 18, 0, tzinfo=timezone.utc)   # Fri 13:00 ET, session open
T_SAT = datetime(2026, 11, 7, 2, 17, tzinfo=timezone.utc)             # Fri 21:17 ET (after close) = Nov 7 UTC
GRADED_LATE = datetime(2026, 11, 12, 11, 35, tzinfo=timezone.utc)
GRADED_EARLY = datetime(2026, 11, 8, 11, 35, tzinfo=timezone.utc)

THESIS_TEXT = ("Markets look flat — nobody's confident either way.\nHere's why:\n  • Fed Money Supply (negative): "
               "The Fed pulled money out of the system.\n")
WITHHELD_TEXT = ("**AI narrative withheld: unsupported source interpretation. Data summary only.**\n\n"
                 "# GRID Market Briefing — 2026-11-07T00:17:36 (assembly time)\n\n## Equities\n- equities:^GSPC: raw value=7353.73\n")
NARRATIVE_TEXT = ("SPY should close higher today as dealers are short gamma into the open. "
                  "We expect QQQ to finish above 600 by the close. "
                  "The move is caused by the Fed's balance sheet runoff.\n\n## Equities\n- data\n")
CLOSES = {date(2026, 11, 4): 7260.0, date(2026, 11, 5): 7300.0, date(2026, 11, 6): 7320.0,
          date(2026, 11, 9): 7400.0, date(2026, 11, 10): 7380.0, date(2026, 11, 11): 7390.0}


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def pub(pid: str, text: str, published_at: datetime, **record) -> dict:
    return {"id": pid, "source": pid.split(":")[0], "published_at": grader.iso(published_at), "text": text,
            "sha256": sha(text), "claims_complete": False, "source_record": record}


def write_packet(root: Path, day: date, pubs: list[dict]) -> str:
    folder = root / day.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    raw = json.dumps({"publication_day": day.isoformat(), "version": "publication-review-v1",
                      "publications": pubs}, sort_keys=True).encode("utf-8")
    (folder / "packet.json").write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    (folder / "receipt.json").write_text(json.dumps({"publication_day": day.isoformat(), "packet_sha256": digest}))
    return digest


class FakeEngine:
    @contextlib.contextmanager
    def connect(self):
        yield object()


class FakeClient:
    """An LLM client that answers with a canned JSON array and counts calls."""

    is_available = True
    model = "fake-27b"

    def __init__(self, reply):
        self.reply = reply
        self.calls = 0

    def chat(self, messages, **kwargs):
        self.calls += 1
        return self.reply


@pytest.fixture
def closes(monkeypatch):
    table = dict(CLOSES)

    def fake(conn, ticker, start, end):
        return [Observation(f"YF:{ticker}:adj_close", d, v, datetime(2026, 11, 12, tzinfo=timezone.utc), "TIINGO")
                for d, v in sorted(table.items()) if start <= d <= end]

    monkeypatch.setattr(grader, "closes", fake)
    return table


def validate_with_ingest(review_root: Path, packet_root: Path, day: date, now: datetime):
    packet = ingest.load_packet(packet_root, day)
    _, lines = ingest.read_lines(review_root / day.isoformat() / "review.jsonl")
    header = ingest.validate_header(lines[0], day, packet.sha256, now)
    validation = ingest.validate_rows(lines, day, packet, header)
    assert validation.rejected == [], validation.rejected
    assert ingest.declared_mismatch(validation) == []
    return validation


# ── spans and calendar ──────────────────────────────────────────────────────────────────

def test_first_sentence_span_is_verbatim_and_bounded():
    span = grader.first_sentence_span(THESIS_TEXT)
    assert span == "Markets look flat — nobody's confident either way."
    assert span in THESIS_TEXT
    long = "word " * 100
    cut = grader.first_sentence_span(long)
    assert 8 <= len(cut) <= 300 and cut in long and not cut.endswith(" ")
    assert grader.first_sentence_span("short") is None


def test_marker_span_picks_the_withheld_sentence_without_markdown():
    span = grader.marker_span(WITHHELD_TEXT)
    assert span == "AI narrative withheld: unsupported source interpretation."
    assert span in WITHHELD_TEXT
    assert grader.marker_span(NARRATIVE_TEXT) is None
    assert grader.marker_span("*AI narrative unavailable — data summary below.*\n## x") == \
        "AI narrative unavailable — data summary below."


REAL_BRIEFING = ("# GRID Market Briefing — 2026-11-07T14:17 (assembly time)\n\n"
                 "## What's Happening Now\nDealers are short gamma into the open and SPY should close higher today.\n\n"
                 "## Action\nStay long QQQ above 600 into the close.\n\n## Tomorrow\nWatch the 10y auction.\n\n"
                 "## Market Data Snapshot — 2026-11-07T14:17 (assembly time, not an evidence clock)\n"
                 "## Equities\n- equities:^GSPC: raw value=7353.73 | asof=2026-11-05\n## Rates\n- x\n"
                 "## Retained Feature Evidence (first 20; freshness unknown)\n- y\n---\n**Unverified figures:** 1, 2\n"
                 "### Feature evidence declarations\n" + "z" * 5000)


def test_narrative_window_keeps_the_narrative_headings_and_stops_at_the_data_tables():
    window = grader.narrative_window(REAL_BRIEFING)
    assert "## What's Happening Now" in window and "## Action" in window and "## Tomorrow" in window
    assert "SPY should close higher today" in window and "Stay long QQQ above 600" in window
    assert "## Market Data Snapshot" not in window and "raw value=" not in window and "zzz" not in window
    window = grader.narrative_window(NARRATIVE_TEXT)
    assert "## Equities" not in window and "SPY should close higher" in window
    assert len(grader.narrative_window("a" * 50_000)) == grader.EXTRACT_MAX_CHARS
    assert grader.narrative_window("## Macro Signals\nInflation is sticky.\n") == "## Macro Signals\nInflation is sticky.\n"


def test_all_generator_withheld_phrasings_are_detected():
    for text in ("**AI narrative withheld: unsupported source interpretation. Data summary only.**\n# x",
                 "# GRID Market Briefing\n\nModel response unavailable. Data summary only.\n## Equities",
                 "# GRID Market Briefing\n\nSource evidence unavailable or unknown. Data summary only.\n",
                 "# GRID Market Diary\n\n*AI narrative unavailable — data summary below.*\n## What Happened",
                 "# x\n*Data summary only; no AI narrative is published.*\n"):
        assert grader.marker_span(text) is not None, text
    assert grader.marker_span(REAL_BRIEFING) is None


def test_session_close_and_baseline_follow_new_york_time():
    # 2026-11-06 is after the DST change: 16:00 ET = 21:00Z
    assert grader.session_close_utc(date(2026, 11, 6)) == datetime(2026, 11, 6, 21, 0, tzinfo=timezone.utc)
    assert grader.baseline_session(T_FRI_PRE_CLOSE) == date(2026, 11, 5)   # Friday still open -> Thursday close
    assert grader.baseline_session(T_SAT) == date(2026, 11, 6)             # Friday after close -> Friday close
    assert grader.next_session_on_or_after(date(2026, 11, 7)) == date(2026, 11, 9)
    assert grader.horizon_session_for(grader.Publication("a:1", "a", T_FRI_PRE_CLOSE, "", "", {}), "close") == date(2026, 11, 6)
    assert grader.horizon_session_for(grader.Publication("a:1", "a", T_SAT, "", "", {}), "close") == date(2026, 11, 9)
    assert grader.horizon_session_for(grader.Publication("a:1", "a", T_SAT, "", "", {}), "week") == date(2026, 11, 13)  # Fri evening ET + 7d


def test_direction_band_and_verdict_matrix():
    assert grader.actual_direction_of(0.51) == "bullish"
    assert grader.actual_direction_of(-0.51) == "bearish"
    assert grader.actual_direction_of(0.5) == "neutral"
    assert grader.direction_verdict("bullish", "bullish") == "confirmed"
    assert grader.direction_verdict("bullish", "bearish") == "refuted"
    assert grader.direction_verdict("bullish", "neutral") == "partially_confirmed"
    assert grader.direction_verdict("neutral", "bullish") == "partially_confirmed"   # thesis_tracker: "partial"
    assert grader.direction_verdict("neutral", "neutral") == "confirmed"


def test_resolve_instrument_aliases():
    assert grader.resolve_instrument("S&P 500") == "SPY"
    assert grader.resolve_instrument("$qqq") == "QQQ"
    assert grader.resolve_instrument("Nasdaq") == "QQQ"
    assert grader.resolve_instrument("Nasdaq", allow_alias=False) is None and grader.resolve_instrument("QQQ", allow_alias=False) == "QQQ"
    assert grader.resolve_instrument("Bitcoin") is None
    assert grader.resolve_instrument(None) is None


# ── packet loading ──────────────────────────────────────────────────────────────────────

def test_load_packet_verifies_bytes_and_drops_unusable_records(tmp_path):
    bad = pub("thesis_snapshots:2", THESIS_TEXT, T_SAT, overall_direction="neutral")
    bad["sha256"] = "0" * 64
    write_packet(tmp_path, DAY, [pub("thesis_snapshots:1", THESIS_TEXT, T_SAT, overall_direction="neutral"), bad,
                                 {"id": "nope", "text": "x"}])
    packet = grader.load_packet(tmp_path, DAY)
    assert [p.id for p in packet.publications] == ["thesis_snapshots:1"]
    assert [u["reason"] for u in packet.unusable] == ["text sha256 mismatch", "malformed id"]
    (tmp_path / DAY.isoformat() / "packet.json").write_bytes(b"{}")
    with pytest.raises(grader.Refused, match="do not match"):
        grader.load_packet(tmp_path, DAY)
    with pytest.raises(grader.Refused, match="not present"):
        grader.load_packet(tmp_path, DAY + timedelta(days=1))


# ── thesis grading ──────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("direction, expected, actual", [
    ("neutral", "partially_confirmed", "bullish"),   # +1.09 % Fri 11-06 close -> Mon 11-09 close; tracker says "partial"
    ("bullish", "confirmed", "bullish"),
    ("bearish", "refuted", "bullish"),
])
def test_grade_thesis_resolves_against_the_three_day_close(closes, direction, expected, actual):
    p = grader.Publication("thesis_snapshots:9", "thesis_snapshots", T_SAT, THESIS_TEXT, sha(THESIS_TEXT),
                           {"overall_direction": direction})
    [row] = grader.grade_thesis(object(), p, GRADED_LATE)
    assert row["verdict"] == expected and row["thesis_direction"] == direction and row["actual_direction"] == actual
    assert row["instrument"] == "SPY" and row["claim_type"] == "direction"
    assert row["horizon_end"] == grader.iso(grader.session_close_utc(date(2026, 11, 9)))
    assert row["evidence"][0]["observed"].startswith("7320.0000 close 2026-11-06")
    assert row["evidence"][1]["observed"].startswith("7400.0000 close 2026-11-09")
    assert (row["root_cause"] is None) == (expected == "confirmed")
    assert row["claim_sha256"] == sha(row["claim_span"]) and row["claim_span"] in THESIS_TEXT


def test_grade_thesis_partially_confirmed_inside_the_band(closes):
    closes[date(2026, 11, 9)] = 7330.0   # +0.14 %
    p = grader.Publication("thesis_snapshots:9", "thesis_snapshots", T_SAT, THESIS_TEXT, sha(THESIS_TEXT),
                           {"overall_direction": "bullish"})
    [row] = grader.grade_thesis(object(), p, GRADED_LATE)
    assert row["verdict"] == "partially_confirmed" and row["actual_direction"] == "neutral"


def test_grade_thesis_is_unresolved_before_the_horizon_and_when_the_close_is_missing(closes):
    p = grader.Publication("thesis_snapshots:9", "thesis_snapshots", T_SAT, THESIS_TEXT, sha(THESIS_TEXT),
                           {"overall_direction": "neutral"})
    [early] = grader.grade_thesis(object(), p, GRADED_EARLY)
    assert early["verdict"] == "unresolved" and early["actual_direction"] is None
    assert len(early["evidence"]) == 1 and early["horizon_end"] == grader.iso(grader.session_close_utc(date(2026, 11, 9)))
    del closes[date(2026, 11, 9)]
    [missing] = grader.grade_thesis(object(), p, GRADED_LATE)
    assert missing["verdict"] == "unresolved"


def test_stale_baseline_zero_and_nan_closes_are_unscorable_not_wrong(closes):
    p = grader.Publication("thesis_snapshots:9", "thesis_snapshots", T_SAT, THESIS_TEXT, sha(THESIS_TEXT),
                           {"overall_direction": "bullish"})
    del closes[date(2026, 11, 6)]                     # baseline session missing, older closes present
    [row] = grader.grade_thesis(object(), p, GRADED_LATE)
    assert row["verdict"] == "unscorable" and "2026-11-06" in row["root_cause"]
    closes[date(2026, 11, 6)] = 0.0
    [row] = grader.grade_thesis(object(), p, GRADED_LATE)
    assert row["verdict"] == "unscorable"            # no ZeroDivisionError, no FAILED day
    closes[date(2026, 11, 6)] = 7320.0
    closes[date(2026, 11, 9)] = float("nan")
    [row] = grader.grade_thesis(object(), p, GRADED_LATE)
    assert row["verdict"] == "unscorable" and "unusable" in row["root_cause"]


def test_grade_thesis_without_baseline_or_direction_is_unscorable(closes):
    closes.clear()
    p = grader.Publication("thesis_snapshots:9", "thesis_snapshots", T_SAT, THESIS_TEXT, sha(THESIS_TEXT),
                           {"overall_direction": "neutral"})
    [row] = grader.grade_thesis(object(), p, GRADED_LATE)
    assert row["verdict"] == "unscorable" and row["evidence"] == [] and "no usable TIINGO close" in row["root_cause"]
    p2 = grader.Publication("thesis_snapshots:9", "thesis_snapshots", T_SAT, THESIS_TEXT, sha(THESIS_TEXT), {})
    [row2] = grader.grade_thesis(object(), p2, GRADED_LATE)
    assert row2["verdict"] == "unscorable" and "overall_direction" in row2["root_cause"]


# ── briefings ───────────────────────────────────────────────────────────────────────────

def test_withheld_briefing_is_one_unscorable_data_quality_row_without_a_model_call(tmp_path, closes):
    p = grader.Publication("market_briefings:5", "market_briefings", T_SAT, WITHHELD_TEXT, sha(WITHHELD_TEXT), {})
    client = FakeClient("[]")
    rows, model = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert model is None and client.calls == 0
    [row] = rows
    assert row["verdict"] == "unscorable" and row["claim_type"] == "data_quality"
    assert row["claim_span"] == "AI narrative withheld: unsupported source interpretation."
    assert "withheld or unavailable" in row["root_cause"]
    assert not (tmp_path / "extractions").exists()


def test_narrative_briefing_claims_are_extracted_validated_and_graded(tmp_path, closes):
    reply = json.dumps([
        {"span": "SPY should close higher today as dealers are short gamma into the open.", "claim_type": "direction",
         "instrument": "SPY", "direction": "bullish", "horizon": "close"},
        {"span": "We expect QQQ to finish above 600 by the close.", "claim_type": "level", "instrument": "QQQ",
         "comparator": ">", "level": 600, "horizon": "close"},
        {"span": "The move is caused by the Fed's balance sheet runoff.", "claim_type": "causal", "instrument": None},
        {"span": "not in the text at all, a paraphrase", "claim_type": "direction", "instrument": "SPY", "direction": "bullish"},
        {"span": "Bitcoin will rip higher into the weekend close.", "claim_type": "direction", "instrument": "BTC", "direction": "bullish"},
        "garbage",
    ])
    # the Bitcoin sentence sits inside the narrative window (before the first data heading)
    text = NARRATIVE_TEXT.replace("\n\n## Equities", " Bitcoin will rip higher into the weekend close.\n\n## Equities")
    p = grader.Publication("market_briefings:7", "market_briefings", T_FRI_PRE_CLOSE, text, sha(text), {})
    client = FakeClient("Sure, here it is:\n" + reply + "\nhope this helps")
    rows, model = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert model == "FakeClient/fake-27b" and client.calls == 1
    assert len(rows) == 4          # paraphrase (not verbatim) and "garbage" dropped
    spy = next(r for r in rows if r["instrument"] == "SPY")
    # Fri 13:00 ET bullish call, Thursday close 7300 -> Friday close 7320 = +0.27 % -> neutral -> partially_confirmed
    assert spy["verdict"] == "partially_confirmed" and spy["thesis_direction"] == "bullish" and spy["actual_direction"] == "neutral"
    assert spy["horizon_end"] == grader.iso(grader.session_close_utc(date(2026, 11, 6)))
    qqq = next(r for r in rows if r["instrument"] == "QQQ")
    assert qqq["verdict"] == "confirmed" and qqq["claim_type"] == "level"     # 7320 > 600 (stub closes are shared)
    causal = next(r for r in rows if r["claim_type"] == "causal")
    assert causal["verdict"] == "unresolved" and causal["evidence"] == []
    btc = next(r for r in rows if "Bitcoin" in r["claim_span"])
    assert btc["verdict"] == "unscorable" and "no price series" in btc["root_cause"]
    assert all("paraphrase" not in r["claim_span"] for r in rows)
    cache = tmp_path / "extractions" / "market_briefings-7.json"
    assert cache.exists() and json.loads(cache.read_text())["publication_sha256"] == sha(text)
    # second grading reuses the cache: no second model call, same rows
    rows2, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert client.calls == 1 and [r["claim_sha256"] for r in rows2] == [r["claim_sha256"] for r in rows]


def test_level_claim_on_an_index_alias_is_unscorable(tmp_path, closes):
    text = "SPX should close above 6800 today. Gold holds above 4000 by the close.\n\n## Equities\n- data\n"
    p = grader.Publication("market_briefings:8", "market_briefings", T_FRI_PRE_CLOSE, text, sha(text), {})
    reply = json.dumps([{"span": "SPX should close above 6800 today.", "claim_type": "level", "instrument": "SPX",
                         "comparator": ">", "level": 6800, "horizon": "close"},
                        {"span": "Gold holds above 4000 by the close.", "claim_type": "level", "instrument": "Gold",
                         "comparator": ">", "level": 4000, "horizon": "close"},
                        {"span": "SPX should close above 6800 today.", "claim_type": "direction", "instrument": "SPX",
                         "direction": "bullish", "horizon": "close"}])
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: FakeClient(reply))
    levels = [r for r in rows if r["claim_type"] == "level"]
    assert len(levels) == 2 and all(r["verdict"] == "unscorable" and "ETF proxy" in r["root_cause"] for r in levels)
    direction = next(r for r in rows if r["claim_type"] == "direction")
    assert direction["instrument"] == "SPY" and direction["verdict"] == "partially_confirmed"   # aliases are fine for direction


def test_extraction_cache_is_validated_and_keyed_by_setup(tmp_path, closes, monkeypatch):
    p = grader.Publication("market_briefings:7", "market_briefings", T_FRI_PRE_CLOSE, NARRATIVE_TEXT, sha(NARRATIVE_TEXT), {})
    good = json.dumps([{"span": "SPY should close higher today as dealers are short gamma into the open.",
                        "claim_type": "direction", "instrument": "SPY", "direction": "bullish", "horizon": "close"}])
    client = FakeClient(good)
    grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    cache = tmp_path / "extractions" / "market_briefings-7.json"
    stored = json.loads(cache.read_text())
    assert stored["cache_key"] == grader.extraction_cache_key(p) and stored["extraction_version"] == grader.EXTRACTION_VERSION
    # tampered cache: span not in text, claim_type a list -> dropped on read, no crash, no model call
    stored["claims"] = [{"span": "not in the text, injected", "claim_type": "direction", "instrument": "SPY", "direction": "bullish"},
                        {"span": "SPY should close higher today as dealers are short gamma into the open.", "claim_type": ["x"]}]
    cache.write_text(json.dumps(stored))
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert client.calls == 1 and rows[0]["verdict"] == "unscorable" and "no checkable claim" in rows[0]["root_cause"]
    # corrupt cache file -> re-extract
    cache.write_text("{not json")
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert client.calls == 2 and rows[0]["verdict"] == "partially_confirmed"
    # a different tier/provider/prompt version is a different cache key -> re-extract
    monkeypatch.setattr(grader, "DEFAULT_PROVIDER", "gemini")
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert client.calls == 3
    # cache dir unwritable (a file in the way) -> the day still grades
    import shutil
    shutil.rmtree(tmp_path / "extractions")
    (tmp_path / "extractions").write_text("in the way")
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert client.calls == 4 and rows[0]["verdict"] == "partially_confirmed"


def test_week_horizon_resolves_inside_the_default_lookback():
    p = grader.Publication("a:1", "a", T_SAT, "", "", {})
    horizon = grader.horizon_session_for(p, "week")
    last_regade = (grader.lookback_days(datetime(2030, 1, 1, tzinfo=timezone.utc), grader.DEFAULT_LOOKBACK_DAYS))
    assert (horizon - DAY).days < grader.DEFAULT_LOOKBACK_DAYS - 1   # graded again after the horizon closes
    assert len(last_regade) == grader.DEFAULT_LOOKBACK_DAYS


def test_level_claim_unresolved_before_its_horizon(closes):
    p = grader.Publication("market_briefings:7", "market_briefings", T_FRI_PRE_CLOSE, NARRATIVE_TEXT, sha(NARRATIVE_TEXT), {})
    row = grader.grade_level(object(), p, "We expect QQQ to finish above 600 by the close.", "QQQ", ">", 600.0,
                             date(2026, 11, 6), datetime(2026, 11, 6, 19, 0, tzinfo=timezone.utc))
    assert row["verdict"] == "unresolved" and row["horizon_end"] == grader.iso(grader.session_close_utc(date(2026, 11, 6)))
    refuted = grader.grade_level(object(), p, "We expect QQQ to finish above 600 by the close.", "QQQ", "<", 600.0,
                                 date(2026, 11, 6), GRADED_LATE)
    assert refuted["verdict"] == "refuted" and "needed < 600" in refuted["root_cause"]


@pytest.mark.parametrize("reply, reason", [
    (None, "empty reply"), ("", "empty reply"), ("no json here", "not a JSON array"),
    ("[{broken]", "not valid JSON"), ('{"a": 1}', "not a JSON array"),
])
def test_extraction_failures_become_an_unscorable_row_not_a_crash(tmp_path, closes, reply, reason):
    p = grader.Publication("market_briefings:7", "market_briefings", T_FRI_PRE_CLOSE, NARRATIVE_TEXT, sha(NARRATIVE_TEXT), {})
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: FakeClient(reply))
    [row] = rows
    assert row["verdict"] == "unscorable" and reason in row["root_cause"]
    # a reply-shaped failure is deterministic at temperature 0: cached, the next run does not pay for it again
    client = FakeClient("[]")
    rows2, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert client.calls == 0 and reason in rows2[0]["root_cause"]


def test_no_provider_and_raising_provider(tmp_path, closes):
    p = grader.Publication("market_briefings:7", "market_briefings", T_FRI_PRE_CLOSE, NARRATIVE_TEXT, sha(NARRATIVE_TEXT), {})
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: None)
    assert "no LLM provider available" in rows[0]["root_cause"]

    class Boom(FakeClient):
        def chat(self, *a, **k):
            raise TimeoutError("slow")

    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: Boom("[]"))
    assert "provider error: TimeoutError" in rows[0]["root_cause"]
    # transient faults are never cached: the next run with a working client extracts
    client = FakeClient("[]")
    grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: client)
    assert client.calls == 1


def test_extraction_asks_a_llama_client_to_skip_thinking_and_leaves_other_clients_alone():
    class Llama:
        is_available = True
        model = "qwen"
        seen: ClassVar[dict] = {}

        def chat(self, messages, model=None, temperature=0.3, num_predict=None, system_knowledge=None,
                 extra_metadata=None, enable_thinking=None):
            Llama.seen.update(num_predict=num_predict, enable_thinking=enable_thinking, temperature=temperature)
            return "[]"

    out = grader.extract_claims(Llama(), "some narrative window text here")
    assert out.error is None and Llama.seen == {"num_predict": grader.EXTRACTION_NUM_PREDICT, "enable_thinking": False,
                                                "temperature": 0.0}
    # FakeClient.chat takes **kwargs: the router forwards nothing provider-specific to it
    assert grader._thinking_off(FakeClient("[]")) == {}


def test_empty_extraction_on_a_narrative_is_reported(tmp_path, closes):
    p = grader.Publication("market_briefings:7", "market_briefings", T_FRI_PRE_CLOSE, NARRATIVE_TEXT, sha(NARRATIVE_TEXT), {})
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: FakeClient("[]"))
    [row] = rows
    assert row["verdict"] == "unscorable" and "no checkable claim was extracted" in row["root_cause"]


def test_hostile_model_json_drops_items_instead_of_crashing():
    window = "SPY should close higher today as dealers are short gamma into the open."
    hostile = [{"span": window, "claim_type": ["direction"]}, {"span": window, "claim_type": "direction", "direction": ["bullish"]},
               {"span": window, "claim_type": "direction", "instrument": {"a": 1}, "horizon": {"x": 1}},
               {"span": window, "claim_type": "level", "comparator": [">"], "level": "600"},
               {"span": [window], "claim_type": "direction"}, {"span": window, "claim_type": "direction", "level": float("inf")}, None, 7]
    out = grader.extract_claims(FakeClient(json.dumps([*hostile[:4], *hostile[6:]]) ), window)
    assert out.error is None and len(out.claims) == 3           # items 2, 3 and the inf-level item survive, typed clean
    assert all(c["direction"] in (None, "bullish") and c["comparator"] is None and c["level"] is None for c in out.claims)
    assert grader.validate_items(hostile, window)[0]["instrument"] is None
    assert grader.validate_items("not a list", window) == []


def test_extract_claims_caps_count_and_rejects_bad_levels():
    items = [{"span": f"Claim number {i:02d} says SPY goes up.", "claim_type": "direction", "instrument": "SPY",
              "direction": "bullish"} for i in range(30)]
    window = "\n".join(i["span"] for i in items)
    out = grader.extract_claims(FakeClient(json.dumps(items)), window)
    assert len(out.claims) == grader.MAX_EXTRACTED_CLAIMS and out.error is None
    weird = [{"span": "Claim number 00 says SPY goes up.", "claim_type": "level", "instrument": "SPY",
              "comparator": "~", "level": True}, {"span": "Claim number 01 says SPY goes up.", "claim_type": "data_quality"}]
    out = grader.extract_claims(FakeClient(json.dumps(weird)), window)
    assert len(out.claims) == 1 and out.claims[0]["comparator"] is None and out.claims[0]["level"] is None


# ── the whole day ───────────────────────────────────────────────────────────────────────

def _day_fixture(tmp_path: Path) -> tuple[Path, Path, str]:
    packet_root, review_root = tmp_path / "packets", tmp_path / "reviews"
    digest = write_packet(packet_root, DAY, [
        pub("thesis_snapshots:1", THESIS_TEXT, T_SAT, overall_direction="neutral"),
        pub("thesis_snapshots:2", THESIS_TEXT, T_SAT, overall_direction="bullish"),   # same text, other id: no dedupe across ids
        pub("market_briefings:5", WITHHELD_TEXT, T_SAT, briefing_type="hourly"),
        pub("market_diary:3", "*AI narrative unavailable — data summary below.*\n## What Happened\n- SPY up", T_SAT),
    ])
    return packet_root, review_root, digest


def test_grade_day_writes_a_file_the_ingest_accepts_and_hands_it_over(tmp_path, closes, monkeypatch):
    packet_root, review_root, digest = _day_fixture(tmp_path)
    handed = []
    monkeypatch.setattr(grader, "run_ingest", lambda engine, day, rr, pr: handed.append((day, rr, pr)) or
                        {"state": "INGESTED", "run_id": 7, "accepted_rows": 4, "rejected_rows": 0, "declared_mismatch": []})
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None)
    assert receipt["state"] == "WRITTEN" and receipt["rows"] == 4 and receipt["superseded_previous"] is False
    assert receipt["verdicts"] == {"confirmed": 1, "partially_confirmed": 1, "unscorable": 2}
    assert receipt["channels"] == {"market_briefings": 1, "market_diary": 1, "thesis_snapshots": 2}
    assert handed == [(DAY, review_root, packet_root)] and receipt["ingest"]["state"] == "INGESTED"
    assert receipt["ingested_review_sha256"] == receipt["review_sha256"]
    path = review_root / DAY.isoformat() / "review.jsonl"
    assert path.stat().st_mode & 0o777 == 0o644
    assert receipt["review_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    validation = validate_with_ingest(review_root, packet_root, DAY, GRADED_LATE)
    assert len(validation.accepted) == 4
    header = json.loads(path.read_bytes().split(b"\n")[0])
    assert header["packet_sha256"] == digest and header["grader"]["model"] == grader.GRADER_VERSION
    assert header["claims_total"] == 4 and header["claims_graded"] == 2 and header["claims_unscorable"] == 2
    saved = json.loads((review_root / DAY.isoformat() / "grader-receipt.json").read_text())
    assert saved["state"] == "WRITTEN" and saved["review_sha256"] == receipt["review_sha256"]


def test_grade_day_is_unchanged_when_verdicts_do_not_move_and_rewrites_when_they_do(tmp_path, closes, monkeypatch):
    packet_root, review_root, _ = _day_fixture(tmp_path)
    calls = []
    monkeypatch.setattr(grader, "run_ingest", lambda *a: calls.append(a) or {"state": "INGESTED"})
    first = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_EARLY,
                             client_factory=lambda: None)
    assert first["state"] == "WRITTEN" and first["verdicts"] == {"unresolved": 2, "unscorable": 2}
    again = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root,
                             now=GRADED_EARLY + timedelta(hours=1), client_factory=lambda: None)
    assert again["state"] == "UNCHANGED" and len(calls) == 1
    assert "ingest" not in again
    later = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                             client_factory=lambda: None)
    assert later["state"] == "WRITTEN" and later["superseded_previous"] is True and len(calls) == 2
    assert later["verdicts"] == {"confirmed": 1, "partially_confirmed": 1, "unscorable": 2}
    forced = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                              client_factory=lambda: None, force=True)
    assert forced["state"] == "WRITTEN" and len(calls) == 3
    # header-only change (the producer re-captured the day: new packet sha, same rows) is a rewrite
    (packet_root / DAY.isoformat() / "packet.json").write_bytes(
        (packet_root / DAY.isoformat() / "packet.json").read_bytes().replace(b'"version": "publication-review-v1"',
                                                                               b'"version": "publication-review-v1b"'))
    import hashlib as _h
    new_sha = _h.sha256((packet_root / DAY.isoformat() / "packet.json").read_bytes()).hexdigest()
    (packet_root / DAY.isoformat() / "receipt.json").write_text(json.dumps({"publication_day": DAY.isoformat(), "packet_sha256": new_sha}))
    recaptured = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                                  client_factory=lambda: None)
    assert recaptured["state"] == "WRITTEN" and len(calls) == 4
    assert json.loads((review_root / DAY.isoformat() / "review.jsonl").read_bytes().split(b"\n")[0])["packet_sha256"] == new_sha


def test_failed_hand_off_is_retried_on_the_next_unchanged_run(tmp_path, closes, monkeypatch):
    packet_root, review_root, _ = _day_fixture(tmp_path)
    outcomes = iter([{"state": "FAILED", "reason": "OperationalError"}, {"state": "INGESTED", "run_id": 9},
                     pytest.fail])
    calls = []

    def fake_ingest(*a):
        calls.append(a)
        nxt = next(outcomes)
        return nxt() if callable(nxt) else nxt

    monkeypatch.setattr(grader, "run_ingest", fake_ingest)
    first = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                             client_factory=lambda: None)
    assert first["state"] == "WRITTEN" and first["ingest"]["state"] == "FAILED" and "ingested_review_sha256" not in first
    assert grader.exit_code([first]) == 3
    second = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root,
                              now=GRADED_LATE + timedelta(days=1), client_factory=lambda: None)
    assert second["state"] == "UNCHANGED" and second["ingest"]["state"] == "INGESTED" and len(calls) == 2
    assert second["ingested_review_sha256"] == second["review_sha256"]
    third = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root,
                             now=GRADED_LATE + timedelta(days=2), client_factory=lambda: None)
    assert third["state"] == "UNCHANGED" and "ingest" not in third and len(calls) == 2   # confirmed: no DB round trip


def test_hand_off_exception_is_recorded_not_raised(tmp_path, closes, monkeypatch):
    packet_root, review_root, _ = _day_fixture(tmp_path)

    def boom():
        raise ImportError("sibling missing")

    monkeypatch.setattr(grader, "_ingest_module", boom)
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None)
    assert receipt["state"] == "WRITTEN" and receipt["ingest"] == {"state": "FAILED", "reason": "hand-off: ImportError"}
    assert (review_root / DAY.isoformat() / "review.jsonl").exists()


def test_empty_publication_day_is_empty_not_failed(tmp_path, closes):
    packet_root, review_root = tmp_path / "packets", tmp_path / "reviews"
    write_packet(packet_root, DAY, [])
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None, ingest=False)
    assert receipt["state"] == "EMPTY" and receipt["rows"] == 0 and grader.exit_code([receipt]) == 0
    assert not (review_root / DAY.isoformat() / "review.jsonl").exists()


def test_grade_day_dry_run_writes_nothing(tmp_path, closes, monkeypatch):
    packet_root, review_root, _ = _day_fixture(tmp_path)
    monkeypatch.setattr(grader, "run_ingest", lambda *a: pytest.fail("dry run must not ingest"))
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None, dry_run=True)
    assert receipt["state"] == "DRY_RUN" and receipt["would_write"] is True and receipt["rows"] == 4
    assert not review_root.exists()


def test_grade_day_refused_and_failed_leave_receipts(tmp_path, closes, monkeypatch):
    packet_root, review_root = tmp_path / "packets", tmp_path / "reviews"
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None)
    assert receipt["state"] == "REFUSED" and "not present" in receipt["reason"]
    assert json.loads((review_root / DAY.isoformat() / "grader-receipt.json").read_text())["state"] == "REFUSED"
    _day_fixture(tmp_path)

    def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(grader, "closes", boom)
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None)
    assert receipt["state"] == "FAILED" and receipt["reason"] == "RuntimeError"
    assert not (review_root / DAY.isoformat() / "review.jsonl").exists()


def test_grade_day_with_no_gradeable_row_is_refused(tmp_path, closes):
    packet_root, review_root = tmp_path / "packets", tmp_path / "reviews"
    write_packet(packet_root, DAY, [pub("thesis_snapshots:1", "short", T_SAT, overall_direction="neutral")])
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None, ingest=False)
    assert receipt["state"] == "REFUSED" and "no publication yielded" in receipt["reason"]


def test_review_file_never_follows_a_symlink(tmp_path, closes):
    packet_root, review_root, _ = _day_fixture(tmp_path)
    folder = review_root / DAY.isoformat()
    folder.mkdir(parents=True)
    victim = tmp_path / "victim"
    victim.write_text("keep")
    (folder / "review.jsonl").symlink_to(victim)
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None, ingest=False)
    assert receipt["state"] == "FAILED" and receipt["reason"] == "OSError"
    assert victim.read_text() == "keep"


def test_run_ingest_loads_the_sibling_module_and_passes_roots_through(tmp_path, closes, monkeypatch):
    packet_root, review_root, _ = _day_fixture(tmp_path)
    grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                     client_factory=lambda: None, ingest=False)
    seen = {}

    class Module:
        @staticmethod
        def ingest(engine, day, *, review_root, packet_root):
            seen.update(day=day, review_root=review_root, packet_root=packet_root)
            return {"state": "INGESTED", "run_id": 3, "accepted_rows": 4, "rejected_rows": 0, "declared_mismatch": [],
                    "receipt_file": "ingest-receipt.json", "extra": "dropped"}

    monkeypatch.setattr(grader, "_ingest_module", lambda: Module)
    out = grader.run_ingest(FakeEngine(), DAY, review_root, packet_root)
    assert seen == {"day": DAY, "review_root": review_root, "packet_root": packet_root}
    assert out == {"state": "INGESTED", "reason": None, "run_id": 3, "accepted_rows": 4, "rejected_rows": 0,
                   "declared_mismatch": [], "receipt_file": "ingest-receipt.json"}


def test_real_ingest_module_is_loadable_by_path():
    module = grader._ingest_module()
    assert callable(module.ingest) and module.REVIEW_SCHEMA == grader.REVIEW_SCHEMA


# ── CLI and exit codes ──────────────────────────────────────────────────────────────────

def test_exit_codes():
    assert grader.exit_code([{"state": "WRITTEN", "ingest": {"state": "INGESTED", "rejected_rows": 0}}]) == 0
    assert grader.exit_code([{"state": "UNCHANGED"}]) == 0
    assert grader.exit_code([{"state": "WRITTEN", "ingest": {"state": "INGESTED", "rejected_rows": 2}}]) == 3
    assert grader.exit_code([{"state": "WRITTEN", "ingest": {"state": "REFUSED"}}]) == 3
    assert grader.exit_code([{"state": "WRITTEN"}, {"state": "FAILED"}]) == 1
    assert grader.exit_code([{"state": "REFUSED"}]) == 1


def test_lookback_days_end_yesterday():
    days = grader.lookback_days(datetime(2026, 11, 12, 11, 35, tzinfo=timezone.utc), 3)
    assert days == [date(2026, 11, 9), date(2026, 11, 10), date(2026, 11, 11)]


def test_main_day_and_lookback(tmp_path, closes, monkeypatch, capsys):
    packet_root, review_root, _ = _day_fixture(tmp_path)
    monkeypatch.setattr(grader, "run_ingest", lambda *a: {"state": "INGESTED", "rejected_rows": 0})
    monkeypatch.setattr(grader, "default_client_factory", lambda: None)
    import types
    monkeypatch.setitem(sys.modules, "db", types.SimpleNamespace(get_engine=lambda: FakeEngine()))
    # Both runs read the wall clock (graded_at, and the lookback window ends yesterday): pin it
    # 6 days after DAY, so the window is DAY .. DAY+5 (unpinned, DAY fell out of the window on
    # 2026-11-14 and every run went red).
    class _SixDaysOn(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(DAY.year, DAY.month, DAY.day, 11, 35, tzinfo=timezone.utc) + timedelta(days=6)

    monkeypatch.setattr(grader, "datetime", _SixDaysOn)
    code = grader.main(["--day", DAY.isoformat(), "--packet-root", str(packet_root), "--review-root", str(review_root)])
    out = capsys.readouterr().out
    assert code == 0 and f"{DAY}: WRITTEN rows=4" in out and "ingest=INGESTED" in out
    # lookback: days without a packet are skipped silently, the graded day is UNCHANGED
    code = grader.main(["--lookback", "6", "--packet-root", str(packet_root), "--review-root", str(review_root), "--json"])
    receipts = json.loads(capsys.readouterr().out)
    assert code == 0 and [r["state"] for r in receipts] == ["UNCHANGED"]
    with pytest.raises(SystemExit):
        grader.main(["--day", "yesterday"])


# ── stated current-state figures (data_quality, graded at the publication instant) ─────

FIGURE_TEXT = ("## What's Happening Now\n"
               "The market is in a **FRAGILE** regime (confidence 0.6024). The VIX stands at 28.18, with the "
               "9-day tenor at 32.66. Equity indices (S&P 500 at 7,353.73, Nasdaq at 24315.0) show negative "
               "5-day momentum (-2.68%). The S&P 500 z-score is reported as 0.6 and S&P 500 z=0.7. "
               "The 10Y-2Y yield curve spread is positive at 0.38%. HYG at 77.28.\n\n## Equities\n- data\n")


def test_extract_figures_finds_levels_and_skips_derived_quantities():
    figs = grader.extract_figures(grader.narrative_window(FIGURE_TEXT))
    got = [(f.figure.series_id, f.span, f.stated, f.decimals) for f in figs]
    assert got == [
        ("YF:^VIX:close", "VIX stands at 28.18", 28.18, 2),
        ("YF:^VIX9D:close", "9-day tenor at 32.66", 32.66, 2),
        ("YF:^GSPC:close", "S&P 500 at 7,353.73", 7353.73, 2),
        ("YF:^IXIC:close", "Nasdaq at 24315.0", 24315.0, 1),
        ("T10Y2Y", "10Y-2Y yield curve spread is positive at 0.38%", 0.38, 2),
        ("YF:HYG:close", "HYG at 77.28", 77.28, 2),
    ]
    assert all(f.span in FIGURE_TEXT for f in figs)
    # a price quoted as a percent is a move, a rate without one is not a rate
    assert grader.extract_figures("The S&P 500 is at -2.68% on the week, the 10Y-2Y spread at 0.38 today.") == []


def test_figure_matches_at_printed_precision():
    assert grader.figure_matches(28.18, 2, 28.1849)
    assert grader.figure_matches(7353.73, 2, 7353.7301)
    assert grader.figure_matches(24315.0, 1, 24315.04)
    assert grader.figure_matches(26.9, 1, 26.91)          # rounded to one decimal before printing
    assert not grader.figure_matches(28.18, 2, 25.64)
    assert not grader.figure_matches(7353.73, 2, 7376.86)


def _o(sid: str, d: date, value: float, pulled: datetime, source: str = "yfinance") -> Observation:
    return Observation(sid, d, value, pulled, source)


FRI = date(2026, 11, 6)
THU = date(2026, 11, 5)
WED = date(2026, 11, 4)


def _z(day: date, hh: int, mm: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hh, mm, tzinfo=timezone.utc)


@pytest.fixture
def vintages(monkeypatch):
    """series_id -> list of Observation; the grader reads them through _vintages."""
    table: dict[str, list[Observation]] = {}

    def fake(conn, series_id, start, end):
        return sorted((o for o in table.get(series_id, []) if start <= o.obs_date <= end),
                      key=lambda o: (o.obs_date, o.pull_timestamp))

    monkeypatch.setattr(grader, "_vintages", fake)
    return table


def _vix(table, *rows):
    table.setdefault("YF:^VIX:close", []).extend(_o("YF:^VIX:close", d, v, t) for d, v, t in rows)


def _range(table, d, lo, hi, pulled):
    table.setdefault("YF:^VIX:low", []).append(_o("YF:^VIX:low", d, lo, pulled))
    table.setdefault("YF:^VIX:high", []).append(_o("YF:^VIX:high", d, hi, pulled))


def _judge(text: str, at: datetime):
    [stated] = grader.extract_figures(text)
    p = grader.Publication("market_briefings:1", "market_briefings", at, text, sha(text), {})
    return grader.grade_figure(object(), p, stated)


def test_intraday_spike_is_confirmed_by_the_captures_around_the_statement(vintages):
    # the old reader compared 26.9 with the last completed close (16.61) and refuted a true statement
    _vix(vintages, (THU, 16.61, _z(FRI, 4)), (FRI, 21.0, _z(FRI, 15)), (FRI, 26.5, _z(FRI, 18, 30)),
         (FRI, 27.4, _z(FRI, 19, 30)))
    row = _judge("The VIX of 26.9 shows the spike.", _z(FRI, 19))          # 14:00 ET, session open
    assert row["verdict"] == "confirmed" and row["root_cause"] is None
    assert [e["observed"].split(" ")[0] for e in row["evidence"]] == ["26.5", "27.4"]


def test_evening_statement_matches_the_days_close_before_grid_calls_it_final(vintages):
    _vix(vintages, (THU, 16.61, _z(FRI, 4)), (FRI, 26.9, _z(FRI, 21, 20)))
    row = _judge("The VIX at 26.9 into the close.", _z(FRI, 23))
    assert row["verdict"] == "confirmed" and row["knowable_at_stated_at"] is True


def test_a_true_close_grid_had_not_pulled_yet_is_confirmed_not_knowable(vintages):
    _vix(vintages, (THU, 16.61, _z(FRI, 4)), (FRI, 26.9, _z(FRI, 21, 30)))
    row = _judge("The VIX at 26.9 into the close.", _z(FRI, 21, 5))
    assert row["verdict"] == "confirmed" and row["knowable_at_stated_at"] is False


def test_quoting_yesterdays_close_after_todays_was_known_is_stale(vintages):
    _vix(vintages, (THU, 16.61, _z(FRI, 4)), (FRI, 26.9, _z(FRI, 21, 20)))
    row = _judge("The VIX at 16.61 is calm.", _z(FRI, 23))
    assert row["verdict"] == "partially_confirmed" and "stale" in row["root_cause"]
    assert "2026-11-05 close" in row["root_cause"] and "26.9" in row["root_cause"]


def test_morning_statement_quoting_the_previous_close_is_confirmed(vintages):
    _vix(vintages, (WED, 18.0, _z(THU, 4)), (THU, 16.61, _z(THU, 21, 20)))
    row = _judge("The VIX at 16.61 before the open.", _z(FRI, 13))        # 08:00 ET, before the open
    assert row["verdict"] == "confirmed"


def test_inside_the_range_without_a_pinning_capture_is_partial(vintages):
    _vix(vintages, (THU, 16.61, _z(FRI, 4)), (FRI, 18.0, _z(FRI, 15)), (FRI, 19.0, _z(FRI, 16)),
         (FRI, 29.0, _z(FRI, 21, 20)))
    _range(vintages, FRI, 16.0, 30.0, _z(FRI, 21, 20))
    row = _judge("The VIX of 26.9 shows the spike.", _z(FRI, 19))
    assert row["verdict"] == "partially_confirmed" and "inside the 2026-11-06 range 16-30" in row["root_cause"]


def test_a_value_the_market_never_printed_is_refuted_with_evidence(vintages):
    _vix(vintages, (THU, 16.61, _z(FRI, 4)), (FRI, 26.0, _z(FRI, 18)), (FRI, 27.0, _z(FRI, 20)))
    _range(vintages, FRI, 16.0, 30.0, _z(FRI, 21, 20))
    row = _judge("The VIX of 40.1 is a panic.", _z(FRI, 19))
    assert row["verdict"] == "refuted" and "outside the session range" in row["root_cause"]
    assert any("range 16-30" in e["observed"] for e in row["evidence"])
    assert all(e["as_of"] <= grader.iso(GRADED_LATE) for e in row["evidence"])


def test_fred_rate_latest_versus_stale_by_release_time(vintages):
    vintages["T10Y2Y"] = [_o("T10Y2Y", THU, 0.38, _z(FRI, 12), "FRED"), _o("T10Y2Y", FRI, 0.34, _z(date(2026, 11, 7), 12), "FRED")]
    early = _judge("The 10Y-2Y spread is at 0.38% today.", _z(date(2026, 11, 7), 1))   # Friday's value not out yet
    assert early["verdict"] == "confirmed"
    late = _judge("The 10Y-2Y spread is at 0.38% today.", _z(date(2026, 11, 7), 18))   # Friday's 0.34 was out
    assert late["verdict"] == "partially_confirmed" and "stale" in late["root_cause"]
    wrong = _judge("The 10Y-2Y spread is at 0.52% today.", _z(date(2026, 11, 7), 18))
    assert wrong["verdict"] == "refuted"


def test_no_capture_at_all_is_unscorable_and_reader_faults_stay_local(vintages, monkeypatch):
    row = _judge("HYG at 77.28 in the tape.", _z(FRI, 19))
    assert row["verdict"] == "unscorable" and "no YF:HYG:close capture" in row["root_cause"]

    def boom(*a, **k):
        raise RuntimeError("db")

    monkeypatch.setattr(grader, "_vintages", boom)
    row = _judge("HYG at 77.28 in the tape.", _z(FRI, 19))
    assert row["verdict"] == "unscorable" and "unreadable (RuntimeError)" in row["root_cause"]


def test_figure_rows_join_extracted_claims_and_survive_a_failed_extraction(tmp_path, closes, vintages):
    _vix(vintages, (THU, 28.18, _z(FRI, 4)))
    p = grader.Publication("market_briefings:5018", "market_briefings", T_FRI_PRE_CLOSE, FIGURE_TEXT, sha(FIGURE_TEXT), {})
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path, lambda: FakeClient("[]"))
    assert len(rows) == 6 and not any("no checkable claim" in (r["root_cause"] or "") for r in rows)
    rows, _ = grader.grade_briefing(object(), p, GRADED_LATE, tmp_path / "b", lambda: None)
    assert len(rows) == 6 and not any("extraction unavailable" in (r["root_cause"] or "") for r in rows)


def test_figure_rows_pass_the_real_ingest_validators(tmp_path, closes, vintages):
    _vix(vintages, (THU, 16.61, _z(FRI, 4)), (FRI, 26.9, _z(FRI, 21, 20)))
    _range(vintages, FRI, 16.0, 30.0, _z(FRI, 21, 20))
    vintages["YF:^GSPC:close"] = [_o("YF:^GSPC:close", FRI, 7353.73, _z(FRI, 21, 20))]
    text = ("## What's Happening Now\nThe VIX at 26.9 into the close, VIX 9-day at 40.0 and the S&P 500 at "
            "7,353.73. HYG at 77.28.\n\n## Equities\n- data\n")
    packet_root, review_root = tmp_path / "packets", tmp_path / "reviews"
    write_packet(packet_root, DAY, [pub("market_briefings:5018", text, T_SAT, briefing_type="hourly"),
                                    pub("thesis_snapshots:1", THESIS_TEXT, T_SAT, overall_direction="neutral")])
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: FakeClient("[]"), ingest=False)
    assert receipt["state"] == "WRITTEN"
    assert receipt["verdicts"] == {"confirmed": 2, "partially_confirmed": 1, "unscorable": 2}
    validation = validate_with_ingest(review_root, packet_root, DAY, GRADED_LATE)
    assert len(validation.accepted) == 5


def test_read_vintages_returns_every_accepted_vintage_bounded():
    import store.observations as obs_mod

    class Conn:
        def __init__(self):
            self.seen = None

        def execute(self, clause, params):
            self.seen = (str(clause), params)

            class R:
                @staticmethod
                def fetchall():
                    return [(FRI, 21.0, _z(FRI, 15), "yfinance"), (FRI, None, _z(FRI, 16), "yfinance"),
                            # A second source's vintage. Not TIINGO: Tiingo cannot serve
                            # an index, so tiingo_symbol_contract drops its ^VIX rows.
                            (FRI, 26.9, _z(FRI, 21, 20), "cboe")]
            return R()

    conn = Conn()
    out = obs_mod.read_vintages(conn, "YF:^VIX:close", start=THU, end=FRI)
    sql, params = conn.seen
    assert "pull_status = :ok" in sql and "obs_date >= :start" in sql and "LIMIT :limit" in sql
    assert params == {"sid": "YF:^VIX:close", "ok": "SUCCESS", "start": THU, "end": FRI, "limit": obs_mod.MAX_VINTAGES}
    assert [(o.value, o.source) for o in out] == [(21.0, "yfinance"), (26.9, "cboe")]


# ── public copy of the review ───────────────────────────────────────────────────────────

def test_grade_day_publishes_review_and_receipts_but_not_the_cache(tmp_path, closes, monkeypatch):
    packet_root, review_root, _ = _day_fixture(tmp_path)
    public = tmp_path / "public"
    monkeypatch.setattr(grader, "run_ingest", lambda *a: {"state": "INGESTED", "run_id": 1})
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None, public_root=public)
    folder = review_root / DAY.isoformat()
    (folder / "ingest-receipt.json").write_text('{"state": "INGESTED"}')
    (folder / "extractions").mkdir(exist_ok=True)
    out = public / DAY.isoformat()
    assert receipt["published"]["files"] == ["review.jsonl", "grader-receipt.json"]
    assert (out / "review.jsonl").read_bytes() == (folder / "review.jsonl").read_bytes()
    assert (out / "review.jsonl").stat().st_mode & 0o777 == 0o644 and out.stat().st_mode & 0o777 == 0o755
    assert public.stat().st_mode & 0o777 == 0o755
    again = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root,
                             now=GRADED_LATE + timedelta(hours=1), client_factory=lambda: None, public_root=public)
    assert again["state"] == "UNCHANGED"
    assert again["published"]["files"] == ["review.jsonl", "grader-receipt.json", "ingest-receipt.json"]
    assert sorted(p.name for p in out.iterdir()) == ["grader-receipt.json", "ingest-receipt.json", "review.jsonl"]
    assert json.loads((out / "grader-receipt.json").read_text())["state"] == "UNCHANGED"


def test_publish_is_skipped_on_dry_run_and_refusal_and_never_follows_a_symlink(tmp_path, closes):
    packet_root, review_root, _ = _day_fixture(tmp_path)
    public = tmp_path / "public"
    dry = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                           client_factory=lambda: None, dry_run=True, public_root=public)
    assert "published" not in dry and not public.exists()
    refused = grader.grade_day(FakeEngine(), DAY + timedelta(days=1), packet_root=packet_root, review_root=review_root,
                               now=GRADED_LATE, client_factory=lambda: None, public_root=public)
    assert refused["state"] == "REFUSED" and "published" not in refused
    target = tmp_path / "elsewhere"
    target.mkdir()
    public.mkdir()
    (public / DAY.isoformat()).symlink_to(target)
    receipt = grader.grade_day(FakeEngine(), DAY, packet_root=packet_root, review_root=review_root, now=GRADED_LATE,
                               client_factory=lambda: None, ingest=False, public_root=public)
    assert receipt["state"] == "WRITTEN" and receipt["published"]["error"] == "OSError"
    assert list(target.iterdir()) == []
