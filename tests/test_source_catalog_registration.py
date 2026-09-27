"""Regression tests for source_catalog auto-registration payloads.

Production hit two DB constraint violations from `_resolve_source_id()`
auto-create (or its copy-pasted equivalent) for four pullers:

  - reddit_options_pulse / taiwan_strait_osint: CheckViolation on
    `source_catalog_latency_class_check` (SOURCE_CONFIG used the invalid
    value "DAILY" instead of an allowed latency_class).
  - social_sentiment / wiki_history: NotNullViolation on
    `revision_behavior` (their inline INSERT omitted several NOT NULL
    columns entirely).

These tests pin the *actual* allowed enum values from schema.sql and
verify each puller's registration payload against them, so a future
edit can't silently reintroduce an invalid value.
"""
from __future__ import annotations

from unittest.mock import MagicMock

# Allowed values per schema.sql's source_catalog CHECK constraints.
VALID_COST_TIERS = {"FREE", "LOW", "PAID"}
VALID_LATENCY_CLASSES = {"REALTIME", "EOD", "WEEKLY", "MONTHLY"}
VALID_REVISION_BEHAVIORS = {"NEVER", "RARE", "FREQUENT"}
VALID_TRUST_SCORES = {"HIGH", "MED", "LOW"}


def _assert_valid_registration_payload(payload: dict) -> None:
    cost_tier = payload.get("cost", payload.get("cost_tier"))
    latency_class = payload.get("latency", payload.get("latency_class"))
    revision_behavior = payload.get("rev", payload.get("revision_behavior"))
    trust_score = payload.get("trust", payload.get("trust_score"))
    priority_rank = payload.get("rank", payload.get("priority_rank"))

    assert cost_tier in VALID_COST_TIERS, f"invalid cost_tier: {cost_tier!r}"
    assert latency_class in VALID_LATENCY_CLASSES, (
        f"invalid latency_class: {latency_class!r}"
    )
    assert revision_behavior is not None, "revision_behavior must not be NULL"
    assert revision_behavior in VALID_REVISION_BEHAVIORS, (
        f"invalid revision_behavior: {revision_behavior!r}"
    )
    assert trust_score is not None, "trust_score must not be NULL"
    assert trust_score in VALID_TRUST_SCORES, f"invalid trust_score: {trust_score!r}"
    assert isinstance(priority_rank, int), "priority_rank must not be NULL"


# ── BasePuller-backed pullers: SOURCE_CONFIG is bound directly into the
#    auto-create INSERT by ingestion/base.py's _resolve_source_id(). ─────────


class TestRedditOptionsPulseSourceConfig:
    def test_source_config_is_valid_registration_payload(self):
        from ingestion.altdata.reddit_options_pulse import RedditOptionsPulsePuller

        cfg = RedditOptionsPulsePuller.SOURCE_CONFIG
        _assert_valid_registration_payload(cfg)


class TestTaiwanStraitOsintSourceConfig:
    def test_source_config_is_valid_registration_payload(self):
        from ingestion.altdata.taiwan_strait_osint import TaiwanStraitPuller

        cfg = TaiwanStraitPuller.SOURCE_CONFIG
        _assert_valid_registration_payload(cfg)


# ── Standalone pullers: social_sentiment / wiki_history build their own
#    inline INSERT (ATTENTION.md #11's copy-pasted _resolve_source_id
#    pattern), so we exercise save_to_db() against a mock engine and
#    capture the actual bound parameters sent to Postgres. ──────────────────


def _mock_engine_capturing_source_catalog_insert():
    engine = MagicMock()
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value = ctx

    captured: dict = {}

    def execute(stmt, params=None):
        sql = str(stmt)
        result = MagicMock()
        if "INSERT INTO source_catalog" in sql:
            captured["params"] = params or {}
        elif "SELECT id FROM source_catalog" in sql:
            result.fetchone.return_value = (1,)
        else:
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute
    return engine, captured


class TestSocialSentimentSourceCatalogInsert:
    def test_save_to_db_registers_valid_source(self):
        from ingestion.social_sentiment import SocialSentimentPuller

        engine, captured = _mock_engine_capturing_source_catalog_insert()
        puller = SocialSentimentPuller(db_engine=engine)

        ok = puller.save_to_db({
            "date": "2026-09-26",
            "ticker_sentiment": {"AAPL": {"mentions": 3}},
        })

        assert ok is True
        assert captured["params"], "INSERT INTO source_catalog was not called with bound params"
        _assert_valid_registration_payload(captured["params"])


class TestWikiHistorySourceCatalogInsert:
    def test_save_to_db_registers_valid_source(self):
        from ingestion.wiki_history import WikiHistoryPuller

        engine, captured = _mock_engine_capturing_source_catalog_insert()
        puller = WikiHistoryPuller(db_engine=engine)

        ok = puller.save_to_db({
            "date": "2026-09-26",
            "wiki_events": [{"year": "1990", "text": "example"}],
        })

        assert ok is True
        assert captured["params"], "INSERT INTO source_catalog was not called with bound params"
        _assert_valid_registration_payload(captured["params"])
