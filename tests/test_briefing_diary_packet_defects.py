"""Regression tests for verified defects 1-6 across market briefing, market diary,
cross-reference yield-curve text, and daily publication packet guards.

All tests are offline (small synthetic inputs, in-memory SQLite or fakes, mocked
LLM — no live database, no network).
"""
from __future__ import annotations

import copy
import hashlib
import importlib.util
import json
import os
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

os.environ.setdefault("DB_PASSWORD", "test-password")

from ollama.market_briefing import MarketBriefingEngine
from tests.test_market_briefing_number_grounding import (
    _FakeConnection,
    _FakeEngine,
    _FakeOllamaClient,
    _empty_snapshot,
    _make_engine,
    _no_network,
)

ROOT = Path(__file__).resolve().parents[1]


def _load_packet_module():
    spec = importlib.util.spec_from_file_location(
        "daily_publication_packet_defects_under_test",
        ROOT / "scripts" / "daily_publication_packet.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


# ── Defect 1: DGS10 in rates snapshot + (instrument, value) grounding ────────


def test_defect1_snapshot_rates_queries_and_stores_dgs10(tmp_path):
    """_gather_market_snapshot must query the live 10-year (raw yc_10y) and populate snapshot['rates']['DGS10']
    when present, and omit it (fail closed) when absent."""

    class _RatesConn(_FakeConnection):
        def __init__(self, row_by_sid):
            super().__init__(row_by_sid=row_by_sid)
            self.queried_sids: list[str] = []

        def execute(self, stmt, params=None):
            sql = str(stmt)
            params = params or {}
            if "FROM raw_series" in sql and "sid" in params:
                self.queried_sids.append(params["sid"])
            return super().execute(stmt, params)

    conn_with = _RatesConn({
        "DGS2": (4.80, date(2026, 11, 18)),
        "yc_10y": (4.35, date(2026, 11, 18)),
    })
    snap_with = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn_with), tmp_path)._gather_market_snapshot()
    assert "yc_10y" in conn_with.queried_sids
    assert snap_with["rates"]["DGS10"] == {"value": 4.35, "date": "2026-11-18"}
    assert snap_with["rates"]["DGS2"] == {"value": 4.8, "date": "2026-11-18"}

    conn_without = _RatesConn({"DGS2": (4.80, date(2026, 11, 18))})
    snap_without = _make_engine(_FakeOllamaClient(None), _FakeEngine(conn_without), tmp_path)._gather_market_snapshot()
    assert "yc_10y" in conn_without.queried_sids
    assert "DGS10" not in snap_without["rates"]


def test_defect1_grounding_rejects_dgs2_value_claimed_as_10y_yield(tmp_path, monkeypatch):
    """Exact audit case: DGS2=4.80 in context, no DGS10; LLM claims
    'the 10-year Treasury yield at 4.80%' -> must NOT pass with ungrounded_count=0."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    fake_llm = _FakeOllamaClient(
        "## What's Happening Now\n"
        "The 10-year Treasury yield stands at 4.80% today.\n\n"
        "## Action\nMonitor Treasury observation dates.\n"
    )
    conn = _FakeConnection(row_by_sid={"DGS2": (4.80, date(2026, 11, 18))})
    engine = _make_engine(fake_llm, _FakeEngine(conn), tmp_path)

    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert result["grounding"]["ungrounded_count"] == 1
    assert result["grounding"]["ungrounded_values"] == ["4.80%"]
    assert "4.80% [unverified]" in result["content"]


def test_defect1_grounding_checks_instrument_value_pairs_not_bare_numbers(tmp_path, monkeypatch):
    """Matching (instrument, value) pairs pass; swapped values across instruments fail."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    rows = {
        "DGS2": (4.80, date(2026, 11, 18)),
        "yc_10y": (4.35, date(2026, 11, 18)),
        "YF:^VIX:close": (17.52, date(2026, 11, 18)),
        "YF:^VIX3M:close": (19.57, date(2026, 11, 18)),
    }

    good_llm = _FakeOllamaClient(
        "## What's Happening Now\n"
        "The 10-year Treasury yield is 4.35% and the 2-year Treasury yield is 4.80%. "
        "VIX is 17.52 and VIX3M is 19.57.\n"
    )
    good = _make_engine(good_llm, _FakeEngine(_FakeConnection(row_by_sid=rows)), tmp_path).generate_briefing(save=False)
    assert good["grounding"]["ungrounded_count"] == 0
    assert "[unverified]" not in good["content"]

    swapped_llm = _FakeOllamaClient(
        "## What's Happening Now\n"
        "The 10-year Treasury yield is 4.80% while VIX is 19.57.\n"
    )
    swapped = _make_engine(swapped_llm, _FakeEngine(_FakeConnection(row_by_sid=rows)), tmp_path).generate_briefing(save=False)
    assert swapped["grounding"]["ungrounded_count"] == 2
    assert swapped["grounding"]["ungrounded_values"] == ["4.80%", "19.57"]
    assert "4.80% [unverified]" in swapped["content"]
    assert "19.57 [unverified]" in swapped["content"]


@pytest.mark.parametrize("claim_text", [
    "The 10-year Treasury yield is 4.35%, above the 2-year at 4.80%.",
    "The 10-year yield (4.35%) versus the 2-year (4.80%).",
    "The 2-year yield at 4.80% and the 10-year at 4.35%, leaving a curve spread of -0.45%.",
    "The 2-year yield at 4.80% and the 10-year at 4.35%, leaving a curve spread of \u22120.45%.",
    "Fed funds at 4.33% while 10-year yields at 4.35%.",
    "10-year yield and 2-year yield spread is 0.45 percentage points.",
    "The 10Y at 4.35% versus the 2Y at 4.80%.",
])
def test_defect1_grounding_accepts_true_multi_tenor_and_derived_phrasings(
    tmp_path, monkeypatch, claim_text,
):
    """B1 regression: true multi-tenor, bare-tenor, plural-yield, and derived-spread
    phrasings must NOT produce false [unverified] flags."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    rows = {
        "yc_10y": (4.35, date(2026, 11, 18)),
        "DGS2": (4.80, date(2026, 11, 18)),
        "T10Y2Y": (-0.45, date(2026, 11, 18)),
        "DFF": (4.33, date(2026, 11, 18)),
    }
    fake_llm = _FakeOllamaClient(f"## What's Happening Now\n{claim_text}\n")
    result = _make_engine(fake_llm, _FakeEngine(_FakeConnection(row_by_sid=rows)), tmp_path).generate_briefing(save=False)

    assert result["grounding"]["ungrounded_count"] == 0, (
        f"False positive on {claim_text!r}: {result['grounding']}"
    )
    assert "[unverified]" not in result["content"]


def test_defect1_grounding_still_flags_fabricated_spread_value(tmp_path, monkeypatch):
    """Derived spread check must not accept fabricated numbers."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    rows = {
        "yc_10y": (4.35, date(2026, 11, 18)),
        "DGS2": (4.80, date(2026, 11, 18)),
        "T10Y2Y": (-0.45, date(2026, 11, 18)),
        "DFF": (4.33, date(2026, 11, 18)),
    }
    fake_llm = _FakeOllamaClient(
        "## What's Happening Now\n10-year yield and 2-year yield spread is 1.25 percentage points.\n"
    )
    result = _make_engine(fake_llm, _FakeEngine(_FakeConnection(row_by_sid=rows)), tmp_path).generate_briefing(save=False)

    assert result["grounding"]["ungrounded_count"] == 1
    assert result["grounding"]["ungrounded_values"] == ["1.25"]


# ── Defect 2: Fallback renders same selected features as data_context ────────


def test_defect2_fallback_renders_same_features_as_data_context_with_zero_unverified(tmp_path, monkeypatch):
    """When fallback is emitted (model unavailable or claim-guard withheld), it must
    render the exact same 20 selected features as data_context so grounding does not
    brand the fallback's own DB values [unverified]."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    snapshot = _empty_snapshot()
    # Alphabetically first 20 features have small values (0.11..0.30);
    # 5 later alphabetical features have large values (101.1..105.5) that
    # select_prompt_features picks over aapl_01..aapl_05.
    features = {f"aapl_{i:02d}": {"value": round(0.10 + i * 0.01, 4), "date": "2026-11-18"} for i in range(1, 21)}
    for i in range(1, 6):
        features[f"z_macro_{i:02d}"] = {"value": round(100.0 + i * 1.1, 4), "date": "2026-11-18"}
    snapshot["features"] = features

    for llm_response in (None, "Institutions are buying puts to hedge."):
        engine = _make_engine(_FakeOllamaClient(llm_response), None, tmp_path)
        monkeypatch.setattr(engine, "_gather_market_snapshot", lambda s=snapshot: copy.deepcopy(s))

        result = engine.generate_briefing(briefing_type="hourly", save=False)

        assert result["publication_status"]["content_kind"] == "data_summary"
        assert "features:z_macro_05" in result["content"]
        assert "features:aapl_01" not in result["content"]
        assert result["grounding"]["ungrounded_count"] == 0
        assert "[unverified]" not in result["content"]
        assert "GROUNDING WARNING" not in result["content"]


# ── Defect 3: Truncation detection — never publish truncated text ────────────


def test_defect3_missing_terminal_punctuation_is_withheld_never_published(tmp_path, monkeypatch):
    """Truncated output like '...may provide early' must never be published."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    truncated = (
        "## What's Happening Now\n"
        "The 2-year Treasury yield stands at 4.80% and credit prices may provide early"
    )
    conn = _FakeConnection(row_by_sid={"DGS2": (4.80, date(2026, 11, 18))})
    engine = _make_engine(_FakeOllamaClient(truncated), _FakeEngine(conn), tmp_path)

    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert "may provide early" not in result["content"]
    assert result["publication_status"]["content_kind"] == "data_summary"
    assert result["publication_status"]["reason"] == "truncated_response"
    assert "AI narrative withheld" in result["content"]


def test_defect3_finish_reason_length_is_detected_even_with_period(tmp_path, monkeypatch):
    """If the client reports finish_reason='length', treat as truncated even if the
    cut happened right after a sentence boundary."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    class _LengthTruncatedClient(_FakeOllamaClient):
        last_finish_reason = "length"

    client = _LengthTruncatedClient("The 2-year Treasury yield stands at 4.80%.")
    conn = _FakeConnection(row_by_sid={"DGS2": (4.80, date(2026, 11, 18))})
    engine = _make_engine(client, _FakeEngine(conn), tmp_path)

    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert result["publication_status"]["content_kind"] == "data_summary"
    assert result["publication_status"]["reason"] == "truncated_response"
    assert "AI narrative withheld" in result["content"]


def test_defect3_regenerates_with_higher_budget_and_publishes_complete_retry(tmp_path, monkeypatch):
    """When first call is truncated at num_predict=800 and retry succeeds cleanly
    with a larger budget, publish the complete retry (never the truncated draft)."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    class _RetryClient:
        is_available = True

        def __init__(self):
            self.calls: list[dict] = []
            self.last_finish_reason: str | None = None

        def chat(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) == 1:
                self.last_finish_reason = "length"
                return "The 2-year Treasury yield at 4.80% may provide early"
            self.last_finish_reason = "stop"
            return (
                "## What's Happening Now\n"
                "The 2-year Treasury yield stands at 4.80%. "
                "Actor actions and liquidity valves are unavailable or unknown.\n"
            )

    client = _RetryClient()
    conn = _FakeConnection(row_by_sid={"DGS2": (4.80, date(2026, 11, 18))})
    engine = _make_engine(client, _FakeEngine(conn), tmp_path)

    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert len(client.calls) == 2
    assert client.calls[0]["num_predict"] == 800
    assert client.calls[1]["num_predict"] > 800
    assert result["publication_status"]["content_kind"] == "narrative"
    assert "may provide early" not in result["content"]
    assert "The 2-year Treasury yield stands at 4.80%." in result["content"]


@pytest.mark.parametrize("ending_text", [
    "## What's Happening Now\nThe 2-year Treasury yield stands at 4.80%",
    "## What's Happening Now\nVIX currently stands at 17.52",
    "## Action\n- Monitor Treasury observation dates",
    "## Action\n1. Watch next CPI release",
    "## Summary\nRates remain elevated.\n*Data summary only*",
    "## Summary\nRates remain elevated.\n_Snapshot as of close_",
    "## Action\nMonitor Treasury yields (see risk section)",
    "## Summary\nSignals aligned.\n---",
    "## Summary\nRates remain elevated.\n**Not investment advice**",
    "## What's Happening Now\nRates remain elevated.\n## Action",
])
def test_defect3_legitimate_markdown_endings_not_flagged_by_fallback_heuristic(ending_text):
    """B2 regression: fallback heuristic (when no client stop reason is available)
    must allow numbers/%, list items, italic/bold footers, parentheticals, ---, headings."""
    assert MarketBriefingEngine._detect_truncation(ending_text, client=None) is None


@pytest.mark.parametrize("truncated_text", [
    "## What's Happening Now\nThe 2-year Treasury yield stands at 4.80% and credit prices may provide early",
    "## What's Happening Now\nThe 2-year Treasury yield stands at 4.80%,",
    "## What's Happening Now\nKey rate drivers include:",
])
def test_defect3_mid_sentence_endings_flagged_by_fallback_heuristic(truncated_text):
    """Fallback heuristic flags a last line ending mid-sentence (lowercase word or comma/connector)."""
    result = MarketBriefingEngine._detect_truncation(truncated_text, client=None)
    assert result is not None


def test_defect3_stop_reason_is_primary_and_overrides_text_heuristic():
    """When client exposes a non-length stop reason ('stop'), fallback text heuristic
    does not fire; when it exposes 'length', it flags regardless of text."""
    stop_client = types.SimpleNamespace(last_finish_reason="stop")
    assert MarketBriefingEngine._detect_truncation(
        "The 2-year Treasury yield may provide early", client=stop_client,
    ) is None

    done_stop_client = types.SimpleNamespace(last_done_reason="stop")
    assert MarketBriefingEngine._detect_truncation(
        "The 2-year Treasury yield may provide early", client=done_stop_client,
    ) is None

    length_ollama = types.SimpleNamespace(last_done_reason="length")
    res_ollama = MarketBriefingEngine._detect_truncation(
        "Complete sentence ending with a period.", client=length_ollama,
    )
    assert res_ollama is not None and res_ollama["finish_reason"] == "length"

    length_llama = types.SimpleNamespace(last_finish_reason="length")
    res_llama = MarketBriefingEngine._detect_truncation(
        "Complete sentence ending with a period.", client=length_llama,
    )
    assert res_llama is not None and res_llama["finish_reason"] == "length"


def test_defect3_ollama_client_captures_done_reason(monkeypatch):
    """ollama/client.py must capture Ollama's done_reason on chat() and generate()."""
    from ollama import client as oc

    class _Resp:
        status_code = 200

        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    monkeypatch.setattr(oc.requests, "get", lambda *a, **k: _Resp({"models": []}))
    cli = oc.OllamaClient(base_url="http://localhost:11434", model="test-model")
    assert cli.last_done_reason is None

    monkeypatch.setattr(
        oc.requests, "post",
        lambda *a, **k: _Resp({"message": {"content": "Complete."}, "done_reason": "length"}),
    )
    assert cli.chat([{"role": "user", "content": "hi"}]) == "Complete."
    assert cli.last_done_reason == "length"

    monkeypatch.setattr(
        oc.requests, "post",
        lambda *a, **k: _Resp({"message": {"content": "Done."}, "done_reason": "stop"}),
    )
    assert cli.chat([{"role": "user", "content": "hi"}]) == "Done."
    assert cli.last_done_reason == "stop"

    monkeypatch.setattr(
        oc.requests, "post",
        lambda *a, **k: _Resp({"response": "Cut off", "done_reason": "length"}),
    )
    assert cli.generate("hi") == "Cut off"
    assert cli.last_done_reason == "length"


def test_defect3_llamacpp_client_captures_finish_reason(monkeypatch):
    """llamacpp/client.py must capture OpenAI-style finish_reason on chat()."""
    import llamacpp.client as lc

    class _Resp:
        def __init__(self, status=200, payload=None):
            self.status_code = status
            self._payload = payload or {}
            self.text = json.dumps(self._payload)
            self.headers = {"content-type": "application/json"}

        def json(self):
            return self._payload

    lc._ENDPOINT_BACKOFF_UNTIL.clear()
    lc._PROBE_FAILED_UNTIL.clear()
    lc._CTX_SIZE_BY_URL.clear()
    monkeypatch.setattr(
        lc.requests, "get",
        lambda url, **k: _Resp(200, {"default_generation_settings": {"n_ctx": 8192}} if url.endswith("/props") else {"status": "ok"}),
    )
    cli = lc.LlamaCppClient(base_url="http://localhost:18099")
    assert cli.last_finish_reason is None

    monkeypatch.setattr(
        lc.requests, "post",
        lambda *a, **k: _Resp(200, {
            "choices": [{"message": {"content": "Complete."}, "finish_reason": "length"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 10},
        }),
    )
    assert cli.chat([{"role": "user", "content": "hi"}]) == "Complete."
    assert cli.last_finish_reason == "length"

    monkeypatch.setattr(
        lc.requests, "post",
        lambda *a, **k: _Resp(200, {
            "choices": [{"message": {"content": "Done."}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 5, "completion_tokens": 3},
        }),
    )
    assert cli.chat([{"role": "user", "content": "hi"}]) == "Done."
    assert cli.last_finish_reason == "stop"


def test_defect3_truncation_runs_on_raw_model_output_before_context_annotation(tmp_path, monkeypatch):
    """Truncation detection must run on the raw model output BEFORE annotate_publication_context
    can replace a trailing clause with '[Claim omitted: ... .]' (which ends with ']')."""
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", _no_network)
    monkeypatch.setattr("intelligence.context_provider.build_full_context", lambda *_a, **_k: "")

    # The trailing clause triggers annotate_publication_context ('markets will rise' ->
    # '[Claim omitted: unsupported_directional_forecast.]') AND ends mid-sentence ('early').
    raw_truncated = (
        "## What's Happening Now\n"
        "The 2-year Treasury yield stands at 4.80%. "
        "Because rates shifted, markets will rise early"
    )
    conn = _FakeConnection(row_by_sid={"DGS2": (4.80, date(2026, 11, 18))})
    engine = _make_engine(_FakeOllamaClient(raw_truncated), _FakeEngine(conn), tmp_path)

    result = engine.generate_briefing(briefing_type="hourly", save=False)

    assert result["publication_status"]["content_kind"] == "data_summary"
    assert result["publication_status"]["reason"] == "truncated_response"
    assert "AI narrative withheld" in result["content"]


# ── Defect 4: Deduplicate cross_reference_checks + T10Y2Y < 0 inversion guard ─


@pytest.fixture()
def diary_sqlite_engine():
    engine = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String, nullable=False))
    Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", Text, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    Table(
        "thesis_snapshots", md,
        Column("id", Integer, primary_key=True),
        Column("timestamp", DateTime, nullable=False),
        Column("overall_direction", String, nullable=False),
        Column("conviction", Float),
    )
    Table(
        "cross_reference_checks", md,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("category", String),
        Column("assessment", String),
        Column("implication", Text),
        Column("checked_at", DateTime, nullable=False),
    )
    md.create_all(engine)
    with engine.begin() as conn:
        conn.execute(md.tables["source_catalog"].insert(), {"id": 1, "name": "yfinance"})
    yield engine
    engine.dispose()


def test_defect4_market_diary_deduplicates_cross_reference_checks_by_name(diary_sqlite_engine):
    """5 repeated rows for the same check name on one day must collapse to 1 row,
    making room for distinct checks rather than emitting 5 identical lines."""
    from intelligence import market_diary

    target = date(2026, 11, 18)
    md = MetaData()
    md.reflect(diary_sqlite_engine, only=["cross_reference_checks"])
    tbl = md.tables["cross_reference_checks"]
    with diary_sqlite_engine.begin() as conn:
        for i in range(5):
            conn.execute(tbl.insert(), {
                "id": i + 1,
                "name": "Yield Curve vs Fed Rhetoric",
                "category": "liquidity",
                "assessment": "major_divergence",
                "implication": f"run {i + 1} implication",
                "checked_at": datetime(2026, 11, 18, 10 + i, 0, 0),
            })
        conn.execute(tbl.insert(), {
            "id": 6,
            "name": "HY Spread vs Fed Funds Rate",
            "category": "credit",
            "assessment": "contradiction",
            "implication": "credit spread diverging",
            "checked_at": datetime(2026, 11, 18, 9, 0, 0),
        })

    result = market_diary._gather_thesis_accuracy(diary_sqlite_engine, target)
    assert result["anomalies_detected"] == 2
    assert len(result["details"]) == 2
    flags = [d["flag"] for d in result["details"]]
    assert sum("Yield Curve vs Fed Rhetoric" in f for f in flags) == 1
    assert any("run 5 implication" in f for f in flags)
    assert any("HY Spread vs Fed Funds Rate" in f for f in flags)


@pytest.mark.parametrize("t10y2y_val,expect_inversion", [
    (0.28, False),
    (0.0, False),
    (None, False),
    (-0.35, True),
])
def test_defect4_cross_reference_yield_curve_inversion_text_requires_negative_t10y2y(
    t10y2y_val, expect_inversion,
):
    """'Persistent inversion' text in check_liquidity_reality must only print when T10Y2Y < 0."""
    from intelligence.cross_reference import check_liquidity_reality

    def fake_latest(_engine, sid):
        if sid == "T10Y2Y":
            return (t10y2y_val, date(2026, 11, 18) if t10y2y_val is not None else None)
        return (4.50, date(2026, 11, 18))

    with (
        patch("intelligence.cross_reference._get_series_history", return_value=pd.Series(dtype=float)),
        patch("intelligence.cross_reference._get_latest_value", side_effect=fake_latest),
    ):
        checks = check_liquidity_reality(MagicMock())

    yc = next(c for c in checks if c.name == "Yield Curve vs Fed Rhetoric")
    if expect_inversion:
        assert "Persistent inversion" in yc.implication
    else:
        assert "inversion" not in yc.implication.lower()


# ── Defect 5: NEUTRAL thesis with |SPX| >= 1.5% graded 'missed' ─────────────


@pytest.mark.parametrize("morning,prior_px,today_px,expected_verdict", [
    ("NEUTRAL", 5000.0, 5075.0, "missed"),    # +1.50% exact boundary -> missed
    ("NEUTRAL", 5000.0, 4895.0, "missed"),    # -2.10% large drop -> missed
    ("NEUTRAL", 5000.0, 5074.0, "partial"),   # +1.48% below 1.5% -> partial
    ("NEUTRAL", 5000.0, 5002.0, "correct"),   # +0.04% flat -> correct
    ("BULLISH", 5000.0, 4900.0, "wrong"),     # opposite direction -> wrong
    ("BULLISH", 5000.0, 5002.0, "partial"),   # directional thesis, flat day -> partial
])
def test_defect5_diary_grading_neutral_on_large_spx_move_is_missed(
    diary_sqlite_engine, monkeypatch, morning, prior_px, today_px, expected_verdict,
):
    from intelligence import market_diary
    from price_close_contract import daily_capture_payload

    monkeypatch.setattr(market_diary, "PRICES_ENABLED", True)
    target = date(2026, 11, 18)
    prior = date(2026, 11, 17)

    md = MetaData()
    md.reflect(diary_sqlite_engine, only=["thesis_snapshots", "raw_series"])
    with diary_sqlite_engine.begin() as conn:
        conn.execute(md.tables["thesis_snapshots"].insert(), {
            "id": 1,
            "timestamp": datetime(2026, 11, 18, 12, 0, 0),
            "overall_direction": morning,
            "conviction": 0.5,
        })
        for obs_d, val in ((prior, prior_px), (target, today_px)):
            captured = datetime(obs_d.year, obs_d.month, obs_d.day, 6, tzinfo=timezone.utc) + timedelta(days=2)
            payload = daily_capture_payload("^GSPC", "close", obs_d, captured)
            conn.execute(md.tables["raw_series"].insert(), {
                "series_id": "YF:^GSPC:close",
                "source_id": 1,
                "obs_date": obs_d,
                "pull_timestamp": captured.isoformat(sep=" "),
                "value": val,
                "raw_payload": json.dumps(payload),
                "pull_status": "SUCCESS",
            })

    acc = market_diary._gather_thesis_accuracy(diary_sqlite_engine, target)
    assert acc["verdict"] == expected_verdict
    if expected_verdict == "missed":
        assert acc["reason"] is not None and "1.5%" in acc["reason"]


# ── Defect 6: Daily publication packet guards (regime inputs + staleness) ───


def _packet_bundle(records):
    day = "2026-11-18"
    return {
        "version": "publication-capture-v1",
        "producer": "scripts/daily_publication_packet.py",
        "publication_day": day,
        "session_start": f"{day}T00:00:00Z",
        "session_end": "2026-11-19T00:00:00Z",
        "captured_at": "2026-11-19T06:00:00Z",
        "database_receipt": {"transaction_read_only": "on"},
        "channels": {"market_briefings": {"count": len(records), "truncated": False, "complete": True, "state": "ROWS_RETURNED"}},
        "failures": [],
        "existing_diary_context": [],
        "records": records,
    }


@pytest.mark.parametrize("regime_meta", [
    {"state": "FRAGILE", "bound_inputs": 5, "timestamp": "2026-11-18T10:00:00Z"},
    {"state": "GROWTH", "features_used": {f"f{i}": f"src{i}" for i in range(6)}, "timestamp": "2026-11-18T10:00:00Z"},
    # G1: n_features (registry size) must NEVER mask len(features_used) < 8.
    {"state": "CRISIS", "n_features": 12, "features_used": {f"f{i}": f"src{i}" for i in range(6)}, "timestamp": "2026-11-18T10:00:00Z"},
])
def test_defect6_packet_refuses_regime_label_under_8_bound_inputs(regime_meta):
    job = _load_packet_module()
    bundle = _packet_bundle([{
        "channel": "market_briefings",
        "record": {
            "id": 7001,
            "briefing_type": "hourly",
            "briefing_date": "2026-11-18",
            "content": "Market briefing body.",
            "created_at": "2026-11-18T12:00:00Z",
            "snapshot_bytes": 500,
            "snapshot_data": {"latest_regime": regime_meta},
        },
    }])
    before = copy.deepcopy(bundle)

    packet, exclusions = job.build_packet(bundle)

    assert bundle == before  # read-only on inputs
    assert packet["publications"] == []
    assert any("market_briefings:7001" in b and "bound inputs" in b for b in packet["blockers"])
    assert len(exclusions) == 1
    assert exclusions[0]["disposition"] == "REFUSED_INSUFFICIENT_REGIME_INPUTS"
    assert "market_briefings:7001" in packet["capture_receipt"]["quarantined_publication_ids"]


def test_defect6_packet_allows_regime_with_8_inputs_and_notices_unavailable_count():
    job = _load_packet_module()
    bundle = _packet_bundle([
        {
            "channel": "market_briefings",
            "record": {
                "id": 7002,
                "briefing_type": "hourly",
                "briefing_date": "2026-11-18",
                "content": "Eight bound inputs briefing.",
                "created_at": "2026-11-18T12:00:00Z",
                "snapshot_bytes": 500,
                "snapshot_data": {"latest_regime": {"state": "FRAGILE", "bound_inputs": 8, "timestamp": "2026-11-18T10:00:00Z"}},
            },
        },
        {
            "channel": "market_briefings",
            "record": {
                "id": 7003,
                "briefing_type": "hourly",
                "briefing_date": "2026-11-18",
                "content": "No count present briefing.",
                "created_at": "2026-11-18T13:00:00Z",
                "snapshot_bytes": 500,
                "snapshot_data": {"latest_regime": {"state": "NEUTRAL", "timestamp": "2026-11-18T10:00:00Z"}},
            },
        },
        {
            "channel": "market_briefings",
            "record": {
                "id": 7005,
                "briefing_type": "hourly",
                "briefing_date": "2026-11-18",
                "content": "Only registry n_features present briefing.",
                "created_at": "2026-11-18T14:00:00Z",
                "snapshot_bytes": 500,
                "snapshot_data": {"latest_regime": {"state": "CRISIS", "n_features": 5, "feature_count": 5, "n_features_used": 5, "timestamp": "2026-11-18T10:00:00Z"}},
            },
        },
    ])

    packet, exclusions = job.build_packet(bundle)

    assert [p["id"] for p in packet["publications"]] == [
        "market_briefings:7002", "market_briefings:7003", "market_briefings:7005",
    ]
    assert exclusions == []
    assert not any("bound inputs" in b or "regime bound-input" in b for b in packet["blockers"])
    pub_by_id = {p["id"]: p for p in packet["publications"]}
    assert "notices" not in pub_by_id["market_briefings:7002"]
    assert "regime bound-input count unavailable" in pub_by_id["market_briefings:7003"]["notices"]
    assert "regime bound-input count unavailable" in pub_by_id["market_briefings:7005"]["notices"]
    assert any("market_briefings:7003" in n and "regime bound-input count unavailable" in n for n in packet["notices"])
    assert any("market_briefings:7005" in n and "regime bound-input count unavailable" in n for n in packet["notices"])
    assert not any("market_briefings:7002" in n for n in packet["notices"])


def test_defect6_market_briefing_carries_bound_inputs_into_latest_regime_snapshot(tmp_path):
    """G1: MarketBriefingEngine._gather_market_snapshot carries len(features_used) into
    snapshot['latest_regime']['bound_inputs'] when the regime record has features_used,
    making the packet guard live instead of inert."""
    from tests.test_market_briefing_number_grounding import _FakeResult

    class _RegimeConn(_FakeConnection):
        def __init__(self, regime_row, snap_payload):
            super().__init__()
            self._regime_row = regime_row
            self._snap_payload = snap_payload

        def execute(self, stmt, params=None):
            sql = str(stmt)
            if "FROM decision_journal" in sql:
                return _FakeResult(one=self._regime_row)
            if "FROM analytical_snapshots" in sql:
                return _FakeResult(one=(self._snap_payload,) if self._snap_payload is not None else None)
            return super().execute(stmt, params)

    regime_row = ("FRAGILE", 0.72, 0.15, {}, "DEFENSIVE", "2026-11-18T10:00:00Z")
    low_payload = {"regime": "FRAGILE", "n_features": 12, "features_used": {f"f{i}": f"s{i}" for i in range(5)}}
    snap_low = _make_engine(
        _FakeOllamaClient(None), _FakeEngine(_RegimeConn(regime_row, low_payload)), tmp_path,
    )._gather_market_snapshot()
    assert snap_low["latest_regime"]["bound_inputs"] == 5

    # End-to-end: packet refuses the snapshot carrying bound_inputs=5 < 8
    job = _load_packet_module()
    packet_low, exclusions_low = job.build_packet(_packet_bundle([{
        "channel": "market_briefings",
        "record": {
            "id": 7010,
            "briefing_type": "hourly",
            "briefing_date": "2026-11-18",
            "content": "Briefing with 5 bound regime inputs.",
            "created_at": "2026-11-18T12:00:00Z",
            "snapshot_bytes": 500,
            "snapshot_data": snap_low,
        },
    }]))
    assert packet_low["publications"] == []
    assert exclusions_low[0]["disposition"] == "REFUSED_INSUFFICIENT_REGIME_INPUTS"

    # When no features_used is available on the regime record, omit count (do not fabricate)
    snap_none = _make_engine(
        _FakeOllamaClient(None), _FakeEngine(_RegimeConn(regime_row, {"regime": "FRAGILE", "n_features": 12})), tmp_path,
    )._gather_market_snapshot()
    assert "bound_inputs" not in snap_none["latest_regime"]
    packet_none, exclusions_none = job.build_packet(_packet_bundle([{
        "channel": "market_briefings",
        "record": {
            "id": 7011,
            "briefing_type": "hourly",
            "briefing_date": "2026-11-18",
            "content": "Briefing with unavailable regime bound-input count.",
            "created_at": "2026-11-18T12:00:00Z",
            "snapshot_bytes": 500,
            "snapshot_data": snap_none,
        },
    }]))
    assert [p["id"] for p in packet_none["publications"]] == ["market_briefings:7011"]
    assert exclusions_none == []
    assert any("regime bound-input count unavailable" in n for n in packet_none["notices"])


def test_defect6_packet_marks_stale_inputs_and_models_without_mutating_inputs():
    job = _load_packet_module()
    bundle = _packet_bundle([{
        "channel": "market_briefings",
        "record": {
            "id": 7004,
            "briefing_type": "hourly",
            "briefing_date": "2026-11-18",
            "content": "Briefing carrying stale inputs and models.",
            "created_at": "2026-11-18T14:00:00Z",
            "snapshot_bytes": 900,
            "snapshot_data": {
                "rates": {
                    "DGS2": {"value": 4.80, "date": "2026-11-01"},  # 17d > 5d default max age -> STALE
                    "DGS10": {"value": 4.35, "date": "2026-11-17"}, # 1d <= 5d -> FRESH
                },
                "macro": {
                    "UNRATE": {"value": 4.3, "date": "2026-10-01"}, # 48d <= 70d monthly -> FRESH
                },
                "models": {
                    "regime_hmm": {"trained_at": "2026-10-01T00:00:00Z", "max_age_days": 30},  # 48d > 30d -> STALE
                },
            },
        },
    }])
    before = copy.deepcopy(bundle)
    expected_row_sha = hashlib.sha256(job.canonical(bundle["records"][0]["record"])).hexdigest()

    packet, exclusions = job.build_packet(bundle)

    assert bundle == before  # strictly read-only on inputs
    assert exclusions == []
    assert len(packet["publications"]) == 1
    pub = packet["publications"][0]
    # source_record remains unmodified (M0 canonical hash invariant)
    assert hashlib.sha256(job.canonical(pub["source_record"])).hexdigest() == expected_row_sha
    assert pub["stale"] is True
    stale_paths = {item["path"] for item in pub["stale_items"]}
    assert "snapshot_data.rates.DGS2" in stale_paths
    assert "snapshot_data.models.regime_hmm" in stale_paths
    assert "snapshot_data.rates.DGS10" not in stale_paths
    assert "snapshot_data.macro.UNRATE" not in stale_paths
    assert all(item["status"] == "stale" for item in pub["stale_items"])
    assert len(packet["stale_items"]) == 2
