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

import pytest

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


def test_fetch_spy_put_call_treats_stored_zero_as_unavailable_not_a_reading(tmp_path):
    # spy_pcr has repeated 0 rows in production (2026-09-17 and others) --
    # a put/call ratio of 0 is a bad write, never a real SPY reading.
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    conn = _FakeConnection(pcr_row=(0, date(2026, 9, 17)), options_row=None)
    assert engine._fetch_spy_put_call(conn) is None


def test_fetch_spy_put_call_falls_through_a_stored_zero_to_the_next_source(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    conn = _FakeConnection(pcr_row=(0, date(2026, 9, 17)), options_row=(2.51, date(2026, 9, 23)))
    result = engine._fetch_spy_put_call(conn)
    assert result == {"value": 2.51, "date": "2026-09-23", "source": "options_daily_signals"}


def test_fetch_spy_put_call_rejects_a_zero_options_daily_signals_fallback_too(tmp_path):
    engine = _make_engine(_FakeOllamaClient(""), None, tmp_path)
    conn = _FakeConnection(pcr_row=None, options_row=(0, date(2026, 9, 17)))
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
        "SPY is trading near 5789.12 today. The P/C ratio is 8.96.\n\n"
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


@pytest.mark.parametrize("briefing_type", ["hourly", "daily", "weekly"])
def test_generate_briefing_withholds_hedging_claim_before_persistence(
    tmp_path, monkeypatch, briefing_type,
):
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)

    class RecordingConnection(_FakeConnection):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.publications = []

        def execute(self, stmt, params=None):
            if "INSERT INTO market_briefings" in str(stmt):
                self.publications.append(dict(params))
            return super().execute(stmt, params)

    # Keep the original mixed numeric/semantic incident intact here. The
    # numeric-only test above retains every existing grounding assertion.
    candidate = (
        "## What's Happening Now\n"
        "SPY is trading near 5789.12 today. The P/C ratio of 8.96 signals "
        "maximum hedging demand.\n\n"
        "## Action\nWatch the tape.\n"
    )
    fake_llm = _FakeOllamaClient(candidate)
    conn = RecordingConnection(
        row_by_sid={"YF:^GSPC:close": (5789.12, date(2026, 9, 24))},
        pcr_row=(2.68, date(2026, 9, 24)),
    )
    engine = _make_engine(fake_llm, _FakeEngine(conn), tmp_path)
    result = engine.generate_briefing(briefing_type=briefing_type, save=True)

    assert len(fake_llm.calls) == 1
    guard = result["publication_claim_guard"]
    assert guard["passed"] is False
    assert guard["reasons"] == ["unsupported_options_intent"]
    assert result["snapshot"]["publication_claim_guard"] == guard
    assert "AI narrative withheld" in result["content"]
    assert "5789.12" in result["content"]
    for unsupported in [candidate, "8.96", "hedging demand"]:
        assert unsupported not in result["content"]

    md_files = list(tmp_path.glob(f"{briefing_type}_*.md"))
    assert len(md_files) == 1
    assert result["content"] in md_files[0].read_text(encoding="utf-8")
    assert "hedging demand" not in md_files[0].read_text(encoding="utf-8")
    assert len(conn.publications) == 1
    assert conn.publications[0]["content"] == result["content"]
    persisted_snapshot = json.loads(conn.publications[0]["snap"])
    assert persisted_snapshot["publication_claim_guard"] == guard


@pytest.mark.parametrize("response", [None, "PCR proves maximum hedging demand."])
def test_snapshot_recommendation_cannot_reintroduce_withheld_claim(
    tmp_path, monkeypatch, response,
):
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    engine = _make_engine(_FakeOllamaClient(response), _FakeEngine(_FakeConnection()), tmp_path)
    snapshot = _empty_snapshot()
    snapshot["latest_regime"] = {
        "state": "FRAGILE", "confidence": 0.8, "transition_prob": 0.2,
        "timestamp": "2026-09-24T00:00:00+00:00",
        "recommendation": "SPY put/call ratio proves maximum hedging demand.",
    }
    monkeypatch.setattr(engine, "_gather_market_snapshot", lambda: snapshot)
    result = engine.generate_briefing(briefing_type="hourly", save=True)
    assert "hedging demand" not in result["content"]
    assert "hedging demand" not in next(tmp_path.glob("hourly_*.md")).read_text()
    assert result["snapshot"]["latest_regime"]["recommendation"] == snapshot["latest_regime"]["recommendation"]
    assert "calibration unverified" in result["content"]


def test_unsafe_data_summary_is_withheld_before_publication_writes(tmp_path, monkeypatch):
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    client = _FakeOllamaClient("PCR proves maximum hedging demand.")
    conn = _FakeConnection()
    engine = _make_engine(client, _FakeEngine(conn), tmp_path)
    snapshot = _empty_snapshot()
    snapshot["latest_regime"] = {
        "state": "maximum hedging demand", "confidence": 0.8,
        "transition_prob": 0.2, "timestamp": "2026-09-24T00:00:00+00:00",
        "recommendation": "SPY put/call ratio proves maximum hedging demand.",
    }
    monkeypatch.setattr(engine, "_gather_market_snapshot", lambda: snapshot)
    with pytest.raises(ValueError, match="Data summary withheld"):
        engine.generate_briefing(briefing_type="hourly", save=True)
    assert len(client.calls) == 1
    assert not list(tmp_path.glob("*.md"))
    assert not any("INSERT INTO market_briefings" in query for query in conn.executed)


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

    assert "Model response unavailable" in result["content"]
    assert result["publication_status"]["reason"] == "model_unavailable"
    assert result["publication_status"]["generation_outcome"] == "model_unavailable"
    assert "grounding" in result
    # The fallback text is built straight from the snapshot, so its own
    # numbers are trivially grounded -- still worth asserting the gate ran.
    assert result["grounding"]["total_numbers"] >= 1
    assert result["grounding"]["ungrounded_count"] == 0


class TestPublicationEvidenceOutcome:
    """Synthetic model/optional-context callbacks; no live enrichment or scorer."""

    def generate(self, response, *, save=True, model_error=None, snapshot=None):
        import types
        from unittest.mock import Mock, patch
        from ollama import number_grounding

        engine = MarketBriefingEngine.__new__(MarketBriefingEngine)
        engine.engine = object()
        engine.ollama = types.SimpleNamespace(chat=Mock(return_value=response, side_effect=model_error))
        engine._gather_market_snapshot = Mock(return_value=snapshot if snapshot is not None else _empty_snapshot())
        engine._build_data_context = Mock(return_value='Synthetic data with unknown source freshness.')
        engine._save_briefing = Mock()
        engine._persist_to_db = Mock()
        self.last_engine = engine
        optional = types.ModuleType('disabled_optional_context')
        with patch.dict(__import__('sys').modules, {
                'ollama.number_grounding': number_grounding,
                'ingestion.wiki_history': optional,
                'ingestion.social_sentiment': optional,
                'intelligence.sentiment_scorer': optional}):
            result = engine.generate_briefing(save=save)
        return engine, result

    def test_model_unavailable_is_not_guard_withheld(self):
        engine, result = self.generate(None)
        assert result['publication_status']['reason'] == 'model_unavailable'
        assert result['publication_status']['generation_outcome'] == 'model_unavailable'
        assert result['publication_claim_guard']['passed']
        assert 'Model response unavailable' in result['content']
        assert 'AI narrative withheld' not in result['content']
        assert result['snapshot']['publication_status'] == result['publication_status']
        engine.ollama.chat.assert_called_once()
        engine._persist_to_db.assert_called_once_with(result)

    def test_actor_withholding_is_not_model_unavailable(self):
        engine, result = self.generate('institutional distribution is occurring beneath a retail-driven uptrend')
        assert result['publication_status']['reason'] == 'claim_guard_withheld'
        assert result['publication_status']['generation_outcome'] == 'model_response'
        assert 'unsupported_actor_interpretation' in result['publication_claim_guard']['reasons']
        assert 'AI narrative withheld' in result['content']
        assert 'AI analysis unavailable' not in result['content']
        assert 'Model response unavailable' not in result['content']
        assert 'retail-driven' not in result['content']
        engine.ollama.chat.assert_called_once()
        engine._save_briefing.assert_called_once_with(result)
        engine._persist_to_db.assert_called_once_with(result)

    def test_clean_and_unknown_evidence_stay_distinct(self):
        engine, result = self.generate('Retail positioning is unknown.', save=False)
        assert result['publication_status']['content_kind'] == 'narrative'
        assert result['publication_status']['reason'] is None
        assert result['publication_status']['source_evidence_status'] == 'unknown'
        assert result['publication_claim_guard']['passed']
        engine._save_briefing.assert_not_called()
        engine._persist_to_db.assert_not_called()

    def test_model_error_is_not_relabelled_as_unavailability(self):
        try:
            self.generate(None, model_error=RuntimeError('synthetic model callback error'))
        except RuntimeError as error:
            assert str(error) == 'synthetic model callback error'
        else:
            raise AssertionError('model error must propagate')
        self.last_engine.ollama.chat.assert_called_once()
        self.last_engine._save_briefing.assert_not_called()
        self.last_engine._persist_to_db.assert_not_called()

    def test_source_attribution_survives_without_causal_permission(self):
        snapshot = _empty_snapshot()
        snapshot['actor_evidence'] = [{'source_type': 'ownership_filing', 'source_id': 'filing-A',
            'receipt_id': 'filing-row-1', 'actor': 'institutional', 'instrument': 'SPY',
            'measure': 'holdings', 'direction': 'decreased',
            'interval_start': '2026-10-01T00:00:00+00:00',
            'interval_end': '2026-10-02T00:00:00+00:00'}]
        text = 'filing-A source-reported institutional holdings decreased for SPY over the reported interval.'
        _, result = self.generate(text, snapshot=snapshot)
        assert result['publication_claim_guard']['passed']
        assert result['content'].startswith('filing-A source-reported')
        _, result = self.generate(text + ' This proves institutional distribution.', snapshot=snapshot)
        assert not result['publication_claim_guard']['passed']
        assert result['publication_status']['reason'] == 'claim_guard_withheld'


class TestPublicationReceiptGrounding:
    """Final digest follows the existing numeric annotation, without raw retention."""

    def test_emitted_digest_after_annotation(self):
        import hashlib
        if __package__:
            from .test_publication_semantics import PublicationReceiptConsumerTests
        else:
            from test_publication_semantics import PublicationReceiptConsumerTests

        fixture = PublicationReceiptConsumerTests()
        fixture.setUp()
        try:
            text = 'Spot price is 7777.77. VIX reflects implied volatility.'
            result, receipt, _, transport, _ = fixture.run_receipted(text, save=False)
            assert result['grounding']['ungrounded_count'] == 1
            assert '[unverified]' in result['content']
            assert receipt.ungrounded_count == 1
            assert receipt.candidate_digest.sha256 == hashlib.sha256(text.encode('utf-8')).hexdigest()
            assert receipt.grounding_input_digest == receipt.candidate_digest
            assert receipt.emitted_digest.sha256 == hashlib.sha256(result['content'].encode('utf-8')).hexdigest()
            assert receipt.emitted_digest.sha256 != receipt.candidate_digest.sha256
            assert receipt.candidate_guard.passed
            assert receipt.publication_outcome == 'not_requested'
            transport.post.assert_called_once()
        finally:
            fixture.doCleanups()

    def test_summary_digest_distinct_from_rejected_candidate_and_annotation_input(self):
        if __package__:
            from .test_publication_semantics import PublicationReceiptConsumerTests
        else:
            from test_publication_semantics import PublicationReceiptConsumerTests

        fixture = PublicationReceiptConsumerTests()
        fixture.setUp()
        try:
            text = 'Traders purchased protective puts.'
            result, receipt, _, _, _ = fixture.run_receipted(text)
            assert not receipt.candidate_guard.passed
            assert receipt.fallback_guards[0].passed
            assert receipt.fallback_guards[0].input_digest != receipt.candidate_digest
            assert receipt.fallback_guards[0].input_digest != receipt.grounding_input_digest
            assert receipt.emitted_digest.sha256 == fixture.api.sha(result['content'])
            assert receipt.publication_outcome == 'emitted_data_summary_acknowledged'
        finally:
            fixture.doCleanups()


def _sentiment_with_proxies():
    from intelligence.sentiment_scorer import SentimentComponent, SentimentResult

    return SentimentResult(
        score=0.42, label="BULLISH", context="test", timestamp="2026-10-14T00:00:00",
        components=[
            SentimentComponent("flows", 0.9, 1.0, 0.2, "$29.2B in, $0.7B out, net ratio=+0.95"),
            SentimentComponent("dark_pool", 0.1, 0.1, 0.0, "Dark pool: $120M in, $80M out, net=+0.20"),
            SentimentComponent("etf_flows", 0.2, 0.2, 0.0, "ETF flows: $300M in, $100M out"),
            SentimentComponent("put_call_ratio", 0.8, -0.3, 0.1, "P/C 0.80"),
        ],
    )


def _run_with_sentiment(tmp_path, monkeypatch, response):
    monkeypatch.setattr("ingestion.wiki_history.WikiHistoryPuller", _no_network)
    monkeypatch.setattr("ingestion.social_sentiment.SocialSentimentPuller", _no_network)
    monkeypatch.setattr("intelligence.sentiment_scorer.compute_sentiment", lambda _e: _sentiment_with_proxies())
    monkeypatch.setattr("intelligence.sentiment_scorer.log_prediction", lambda *_a, **_k: None)
    fake_llm = _FakeOllamaClient(response)
    conn = _FakeConnection(row_by_sid={"YF:^GSPC:close": (5789.12, date(2026, 9, 24))})
    engine = _make_engine(fake_llm, _FakeEngine(conn), tmp_path)
    return fake_llm, engine.generate_briefing(briefing_type="hourly", save=False)


def test_proxy_components_reach_the_prompt_without_flow_amounts(tmp_path, monkeypatch):
    fake_llm, _ = _run_with_sentiment(tmp_path, monkeypatch, "## Action\nWatch the tape.\n")
    user = next(m["content"] for m in fake_llm.calls[0]["messages"] if m["role"] == "user")
    assert "positioning_proxy: score=+1.00" in user
    assert "dark_pool_proxy" in user and "etf_activity_proxy" in user
    for leaked in ("$29.2B in", "$0.7B out", "$120M in", "ETF flows:", "- flows:"):
        assert leaked not in user
    assert "put_call_ratio: score=-0.30, weight=10% — P/C 0.80" in user  # non-proxies unchanged


def test_system_prompt_no_longer_asks_for_flow_direction(tmp_path, monkeypatch):
    fake_llm, _ = _run_with_sentiment(tmp_path, monkeypatch, "## Action\nWatch the tape.\n")
    system = next(m["content"] for m in fake_llm.calls[0]["messages"] if m["role"] == "system")
    assert "flow direction" not in system
    assert "Never use these words or phrases" in system


def test_rejected_candidate_is_kept_internally_never_published(tmp_path, monkeypatch):
    candidate = "## What's Happening Now\nDirectional flows are bullish.\n"
    _, result = _run_with_sentiment(tmp_path, monkeypatch, candidate)
    guard = result["publication_claim_guard"]
    assert guard["passed"] is False and "unsupported_money_transfer" in guard["reasons"]
    assert guard["rejected_candidate"] == candidate
    assert "Directional flows" not in result["content"]
