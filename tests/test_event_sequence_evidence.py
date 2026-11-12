"""D0 G6/G7 synthetic regressions. No historical data or outcome study."""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy import create_engine, text

from intelligence import event_sequence as es

NOW = datetime(2026, 11, 12, tzinfo=timezone.utc)
PULLS = [
    "_pull_signal_source_events", "_pull_news_events", "_pull_options_events",
    "_pull_decision_journal_events", "_pull_crossref_events", "_pull_earnings_events",
    "_pull_price_move_events",
]


def event(source="news_articles", **kw):
    fields = dict(timestamp=NOW.isoformat(), event_type="news", actor=None,
                  ticker="AAA", direction="bullish", amount_usd=None,
                  description="synthetic", source=source, confidence="estimated",
                  lead_time_to_next_move=None)
    return es.Event(**(fields | kw))


def fail(*args, **kwargs):
    raise AssertionError("Held source must not be read")


@pytest.fixture
def source_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        # Deliberately different physical ordering plus forbidden post-event fields.
        conn.execute(text("""CREATE TABLE signal_sources (
            outcome TEXT, trust_score REAL, signal_value TEXT, signal_type TEXT,
            ticker TEXT, source_type TEXT, source_id TEXT, signal_date TEXT, id INTEGER
        )"""))
        conn.execute(text("""INSERT INTO signal_sources VALUES
            ('FUTURE_WIN', 0.99, :payload, 'BUY', 'AAA', 'insider', 'Person', :dt, 10),
            ('FUTURE_LOSS', 0.01, '42', 'SELL', 'AAA', 'congressional', 'Member', :dt, 11),
            ('FUTURE_WIN', 1.0, '[1, 2]', 'BUY', 'AAA', 'whale', 'Scanner', :dt, 12)
        """), {"payload": '{"amount_usd": 0, "estimated_value": 999, "description": "public record"}',
               "dt": NOW.isoformat()})
    try:
        yield engine
    finally:
        engine.dispose()


def test_named_mapping_restores_real_rows_without_outcome_or_trust(source_engine):
    from sqlalchemy import event as sa_event
    queries = []
    sa_event.listen(source_engine, "before_cursor_execute",
                    lambda conn, cursor, statement, params, context, many: queries.append(statement))
    rows = es._pull_signal_source_events(source_engine, "AAA", NOW - timedelta(days=1))
    assert len(rows) == 3
    mapped = {e.source_record_id: e for e in rows}
    assert mapped["10"].amount_usd == 0
    assert mapped["11"].amount_usd == 42
    assert mapped["12"].amount_usd is None
    assert mapped["10"].actor == "Person"
    assert "public record" in mapped["10"].description
    assert {e.confidence for e in rows} == {"unverified"}
    assert all(e.timestamp_basis == "source_event_date" and es.timing_is_held(e) for e in rows)
    assert all(e.lead_time_to_next_move is None for e in rows)
    assert all("outcome" not in q.lower() and "trust_score" not in q.lower() for q in queries)
    assert "FUTURE" not in str(es.events_to_dicts(rows))


@pytest.mark.parametrize("raw", [None, "null", "[]", "42", "true", "broken", 9, [1, 2]])
def test_scalar_or_malformed_metadata_is_not_an_object(raw):
    assert es._parse_json_metadata(raw) == {}


def test_one_malformed_row_does_not_drop_following_rows():
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value.all.return_value = [
        None, {"id": 2, "source_type": "insider", "source_id": "Person", "signal_date": NOW,
               "signal_value": '{"amount_usd": "NaN"}', "signal_type": "BUY"},
    ]
    rows = es._pull_signal_source_events(engine, "AAA", NOW)
    assert len(rows) == 1 and rows[0].source_record_id == "2"
    assert rows[0].amount_usd is None


def test_calendar_and_filing_rollups_are_held_without_any_io():
    engine = Mock()
    engine.connect.side_effect = fail
    assert es._pull_earnings_events(engine, "AAA", NOW) == []
    engine.connect.assert_not_called()


def test_analytical_default_excludes_held_sources_and_display_optin_is_explicit(monkeypatch):
    for pull in PULLS:
        monkeypatch.setattr(es, pull, lambda *a, **k: [])
    held = event("signal_sources:insider")
    legacy = event()
    signal_pull = Mock(return_value=[held])
    monkeypatch.setattr(es, "_pull_signal_source_events", signal_pull)
    # Defense in depth if a future pull returns held data through another source.
    monkeypatch.setattr(es, "_pull_news_events", lambda *a: [legacy, event("earnings_events")])
    assert es.build_sequence(object(), "AAA") == [legacy]
    signal_pull.assert_not_called()
    display = es.build_sequence(object(), "AAA", include_unqualified=True)
    assert held in display
    assert any(es.timing_is_held(e) for e in display)


@pytest.mark.parametrize("include", [False, True])
def test_sector_and_leadtime_wrapper_propagate_display_flag(monkeypatch, include):
    calls = []
    monkeypatch.setattr(es, "_get_sector_tickers", lambda *a: ["AAA", "BBB"])
    monkeypatch.setattr(es, "_get_price_series", lambda *a, **k: [])
    def build(*args, **kwargs):
        calls.append(kwargs)
        return [event("signal_sources:insider", timing_hold_reason="held")]
    monkeypatch.setattr(es, "build_sequence", build)
    es.build_sector_sequence(object(), "sector", include_unqualified=include)
    es.build_sequence_with_lead_times(object(), "AAA", include_unqualified=include)
    assert len(calls) == 3
    assert all(c["include_unqualified"] is include for c in calls)


@pytest.mark.parametrize("source", ["signal_sources:insider", "earnings_calendar", "earnings_events", "custom"])
@pytest.mark.parametrize("prices", [[], [(NOW, 100), (NOW + timedelta(hours=1), 102)]])
def test_held_event_never_retains_or_gets_lead_time(source, prices):
    held = event(source, lead_time_to_next_move=999, timing_hold_reason="hold" if source == "custom" else None)
    legacy = event()
    es.compute_lead_times([held, legacy], prices)
    assert held.lead_time_to_next_move is None
    if prices:
        assert legacy.lead_time_to_next_move == 1


@pytest.mark.parametrize("mode", ["ticker", "sector", "lead"])
def test_display_endpoint_opts_in_and_labels_partial_coverage(monkeypatch, mode):
    from api.routers import intelligence_news as news
    monkeypatch.setattr(news, "get_db_engine", lambda: object())
    build = Mock(return_value=[])
    for name in ["build_sequence", "build_sector_sequence", "build_sequence_with_lead_times"]:
        monkeypatch.setattr(es, name, build)
    result = news.get_event_sequence(ticker="AAA" if mode != "sector" else None,
                                    sector="sector" if mode == "sector" else None,
                                    days=30, with_lead_times=mode == "lead", _token="test")
    assert build.call_args.kwargs["include_unqualified"] is True
    assert result["coverage"] == "partial"
    assert result["timing_policy"]["earnings"]["status"] == "withheld"
    assert result["timing_policy"]["other_sources"] == "legacy_unreviewed"
    assert result["events"] == []


def test_null_amount_falls_back_but_zero_does_not(source_engine):
    with source_engine.begin() as conn:
        conn.execute(text("UPDATE signal_sources SET signal_value = :v WHERE id = 10"),
                     {"v": '{"amount_usd": null, "estimated_value": 17}'})
    rows = es._pull_signal_source_events(source_engine, "AAA", NOW - timedelta(days=1))
    assert next(e for e in rows if e.source_record_id == "10").amount_usd == 17


def test_recurring_pattern_endpoint_labels_held_inputs(monkeypatch):
    from api.routers import intelligence_news as news
    monkeypatch.setattr(news, "get_db_engine", lambda: object())
    monkeypatch.setattr(es, "find_recurring_patterns", lambda *a, **k: [])
    result = news.get_recurring_patterns(min_occurrences=3, _token="test")
    assert result["coverage"] == "partial"
    assert result["timing_policy"]["signal_sources"]["status"] == "withheld"
    assert result["timing_policy"]["earnings"]["status"] == "withheld"
