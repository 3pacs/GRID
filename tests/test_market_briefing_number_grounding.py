"""Integration tests for the number-grounding gate in ollama/market_briefing.py.

No live database, no network, no real LLM: a fake connection stands in for
the DB (dispatching by SQL substring, following the pattern in
tests/test_briefing_sentiment_readonly.py) and a fake Ollama client stands
in for the model. Covers:

- ``_fetch_spy_put_call`` (resolved_series first, options_daily_signals
  fallback, honest None when unavailable).
- ``_build_data_context`` rendering the put/call section with its
  definition and as-of date.
- The end-to-end ``generate_briefing`` path: a fabricated number gets
  tagged + footnoted, a real one is left alone, and the grounding stats
  are written both into the result and as a sidecar JSON file.
"""
from __future__ import annotations

import json
import os
from datetime import date

os.environ.setdefault("DB_PASSWORD", "test-password")

from ollama.market_briefing import MarketBriefingEngine


# ── Fakes ─────────────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, one=None, many=None):
        self._one = one
        self._many = many if many is not None else []

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _FakeConnection:
    """Dispatches by SQL substring, like tests/test_briefing_sentiment_readonly.py's
    ``_Connection`` but branching per statement shape since
    ``_gather_market_snapshot`` issues many distinct queries on one connection.
    """

    def __init__(self, *, row_by_sid=None, pcr_row=None, options_row=None, insert_id=1):
        self.row_by_sid = row_by_sid or {}
        self.pcr_row = pcr_row
        self.options_row = options_row
        self.insert_id = insert_id
        self.executed: list[str] = []

    def execute(self, stmt, params=None):
        sql = str(stmt)
        self.executed.append(sql)
        params = params or {}

        if "FROM raw_series" in sql:
            return _FakeResult(one=self.row_by_sid.get(params.get("sid")))
        if "FROM resolved_series" in sql and "feature_registry" in sql:
            if params.get("name") == "spy_pcr":
                return _FakeResult(one=self.pcr_row)
            return _FakeResult(many=[])  # the generic "top features" fetch
        if "FROM options_daily_signals" in sql:
            return _FakeResult(one=self.options_row)
        if "FROM decision_journal" in sql:
            return _FakeResult(one=None)
        if "FROM signal_sources" in sql:
            return _FakeResult(many=[])
        if "INSERT INTO market_briefings" in sql:
            return _FakeResult(one=(self.insert_id,))
        if "CREATE TABLE" in sql:
            return _FakeResult()
        return _FakeResult()

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeEngine:
    """A single reusable fake connection behind ``.connect()``."""

    def __init__(self, conn: _FakeConnection):
        self._conn = conn

    def connect(self):
        return self._conn


class _FakeOllamaClient:
    def __init__(self, response: str | None):
        self._response = response
        self.is_available = response is not None
        self.calls: list[dict] = []

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        return self._response


def _no_network(*_args, **_kwargs):
    """Stand-in for WikiHistoryPuller/SocialSentimentPuller constructors.

    Raises immediately so market_briefing.py's existing broad
    ``except Exception: pass`` around each optional-context block takes
    over -- deterministic and fast, per this repo's testing rule to never
    hit live endpoints in tests.
    """
    raise RuntimeError("network disabled in tests")


def _make_engine(ollama_client, db_engine, tmp_path):
    engine = MarketBriefingEngine(ollama_client=ollama_client, db_engine=db_engine)
    engine.output_dir = tmp_path  # never touch the real outputs/market_briefings/
    return engine


def _empty_snapshot() -> dict:
    return {
        "timestamp": "2026-09-24T00:00:00",
        "date": "2026-09-24",
        "equities": {}, "equity_volume": {}, "rates": {}, "credit": {},
        "volatility": {}, "commodities": {}, "fx": {}, "macro": {},
    }


# ── _fetch_spy_put_call ────────────────────────────────────────────────────


def test_fetch_spy_put_call_prefers_resolved_series(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    conn = _FakeConnection(
        pcr_row=(2.68, date(2026, 9, 24)),
        options_row=(9.99, date(2026, 9, 24)),  # should be ignored
    )
    result = engine._fetch_spy_put_call(conn)
    assert result == {"value": 2.68, "date": "2026-09-24", "source": "spy_pcr"}


def test_fetch_spy_put_call_falls_back_to_options_daily_signals(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    conn = _FakeConnection(pcr_row=None, options_row=(1.19, date(2026, 9, 24)))
    result = engine._fetch_spy_put_call(conn)
    assert result == {"value": 1.19, "date": "2026-09-24", "source": "options_daily_signals"}


def test_fetch_spy_put_call_returns_none_when_unavailable(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    conn = _FakeConnection(pcr_row=None, options_row=None)
    assert engine._fetch_spy_put_call(conn) is None


def test_fetch_spy_put_call_never_raises_on_bad_connection(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)

    class _Blows:
        def execute(self, *a, **k):
            raise RuntimeError("db exploded")

    assert engine._fetch_spy_put_call(_Blows()) is None


# ── _build_data_context put/call rendering ────────────────────────────────


def test_build_data_context_renders_put_call_with_definition_and_date(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    snapshot = _empty_snapshot()
    snapshot["options"] = {
        "spy_put_call": {"value": 2.68, "date": "2026-09-24", "source": "spy_pcr"}
    }
    context = engine._build_data_context(snapshot)
    assert "### OPTIONS — SPY PUT/CALL RATIO" in context
    assert "Definition: SPY put/call ratio, open interest, all" in context
    assert "2.68" in context
    assert "as of 2026-09-24" in context


def test_build_data_context_put_call_unavailable_renders_honestly(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    snapshot = _empty_snapshot()
    snapshot["options"] = {"spy_put_call": None}
    context = engine._build_data_context(snapshot)
    assert "unavailable" in context


def test_build_data_context_missing_options_key_renders_honestly(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    context = engine._build_data_context(_empty_snapshot())  # no "options" key at all
    assert "unavailable" in context


# ── End-to-end: generate_briefing with a fake LLM client ─────────────────


def test_generate_briefing_flags_fabricated_number_leaves_real_one_alone(tmp_path, monkeypatch):
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)

    fake_llm = _FakeOllamaClient(
        "## What's Happening Now\n"
        "SPY is trading near 5789.12 today. The P/C ratio of 8.96 signals "
        "maximum hedging demand.\n\n"
        "## Action\nWatch the tape.\n"
    )
    conn = _FakeConnection(
        row_by_sid={"YF:^GSPC:close": (5789.12, date(2026, 9, 24))},
        pcr_row=(2.68, date(2026, 9, 24)),
    )
    engine = _make_engine(fake_llm, _FakeEngine(conn), tmp_path)

    result = engine.generate_briefing(briefing_type="hourly", save=True)

    assert "5789.12" in result["content"]
    assert "5789.12 [unverified]" not in result["content"]
    assert "8.96 [unverified]" in result["content"]
    assert "Unverified figures:" in result["content"]
    assert result["grounding"]["ungrounded_count"] == 1
    assert result["grounding"]["ungrounded_values"] == ["8.96"]
    assert result["snapshot"]["grounding"] == result["grounding"]

    md_files = list(tmp_path.glob("hourly_*.md"))
    assert len(md_files) == 1
    sidecar = md_files[0].with_name(f"{md_files[0].stem}.grounding.json")
    assert sidecar.exists()
    saved_stats = json.loads(sidecar.read_text(encoding="utf-8"))
    assert saved_stats["ungrounded_count"] == 1
    assert saved_stats["ungrounded_values"] == ["8.96"]


def test_generate_briefing_all_grounded_produces_no_banner_or_tags(tmp_path, monkeypatch):
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)

    fake_llm = _FakeOllamaClient(
        "## What's Happening Now\nSPY is trading near 5789.12 today.\n\n"
        "## Action\nWatch the tape.\n"
    )
    conn = _FakeConnection(
        row_by_sid={"YF:^GSPC:close": (5789.12, date(2026, 9, 24))},
        pcr_row=(2.68, date(2026, 9, 24)),
    )
    engine = _make_engine(fake_llm, _FakeEngine(conn), tmp_path)

    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert result["grounding"]["ungrounded_count"] == 0
    assert "[unverified]" not in result["content"]
    assert "GROUNDING WARNING" not in result["content"]


def test_generate_briefing_fallback_path_still_produces_grounding_stats(tmp_path, monkeypatch):
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)

    fake_llm = _FakeOllamaClient(None)  # LLM "unavailable" -> fallback briefing path
    conn = _FakeConnection(row_by_sid={"YF:^GSPC:close": (5789.12, date(2026, 9, 24))})
    engine = _make_engine(fake_llm, _FakeEngine(conn), tmp_path)

    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert "AI analysis unavailable" in result["content"]
    assert "grounding" in result
    # The fallback text is built straight from the snapshot, so its own
    # numbers are trivially grounded -- still worth asserting the gate ran.
    assert result["grounding"]["total_numbers"] >= 1
    assert result["grounding"]["ungrounded_count"] == 0
