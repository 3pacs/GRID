"""Bounded read-side/producer hold tests, using synthetic rows and fake SQL calls."""
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pytest

from intelligence import causal_links as cl

NOW = datetime(2026, 11, 12, tzinfo=timezone.utc)


@pytest.mark.parametrize("eps", [None, 0, -1, 1.25, float("nan"), "malformed"])
@pytest.mark.parametrize("reported", [True, False, None])
def test_mutable_calendar_fields_cannot_unlock_timing(eps, reported):
    row = dict(ticker="AAA", earnings_date=date(2026, 9, 30), reported=reported,
               eps_actual=eps, eps_estimate=1, eps_surprise_pct="bad",
               source="verified_release", classification="reported",
               filing_date=date(2026, 11, 10), pull_timestamp=NOW,
               known_at=NOW, receipt_id="unverified-label")
    assert cl.earnings_events([row]) == []
    row.update(eps_actual=9, summary="later revision", created_at=date(2020, 1, 1))
    assert cl.earnings_events([row]) == []


def test_held_converter_does_not_even_iterate_unqualified_rows():
    def rows():
        raise AssertionError("unqualified payload iteration")
        yield
    assert cl.earnings_events(rows()) == []


def test_event_loader_only_reads_existing_contract_channel(monkeypatch):
    calls = []
    contracts = [{"source_id": "synthetic"}]
    def mappings(conn, sql, params):
        calls.append((sql, params))
        assert "earnings" not in sql.lower()
        return contracts
    monkeypatch.setattr(cl, "_mappings", mappings)
    earn, awards = cl.load_event_rows(object(), ["AAA"], date(2026, 9, 1), NOW)
    assert earn == [] and awards is contracts
    assert len(calls) == 1 and "source_type = 'gov_contract'" in calls[0][0]


def test_read_guard_precedes_limit_and_only_selects(monkeypatch):
    calls = []
    monkeypatch.setattr(cl, "_mappings", lambda conn, sql, params: calls.append((sql, params)) or [])
    assert cl.read_links(object(), ticker="aaa", since=date(2026, 9, 1), limit=1) == []
    sql, params = calls[0]
    assert sql.startswith("SELECT ")
    assert cl.EARNINGS_EDGE_READ_GUARD_SQL in sql
    assert sql.index(cl.EARNINGS_EDGE_READ_GUARD_SQL) < sql.index("LIMIT")
    assert params["ticker"] == "AAA" and params["lim"] == 1
    assert not any(verb in sql for verb in ["DELETE", "UPDATE", "INSERT"])


def test_guard_checks_all_identities_and_tolerates_optional_legacy_columns():
    # SQL execution semantics are separately pinned by PostgreSQL integration tests.
    guard = cl.EARNINGS_EDGE_READ_GUARD_SQL
    for field in ["cause_type", "event_kind", "event_key"]:
        assert field in guard
    assert "to_jsonb(causal_links)->>'event_kind'" in guard
    assert "to_jsonb(causal_links)->>'event_key'" in guard
    assert guard.count("LOWER(BTRIM(COALESCE(") == 3


@pytest.mark.parametrize("ready,run", [(False, None), (True, None), (True, {"as_of": "now"})])
def test_empty_payload_still_reports_earnings_withheld(monkeypatch, ready, run):
    engine = MagicMock()
    monkeypatch.setattr(cl, "schema_ready", lambda conn: ready)
    monkeypatch.setattr(cl, "latest_run", lambda conn: run)
    monkeypatch.setattr(cl, "read_links", lambda *a, **k: [])
    result = cl.read_links_payload(engine, ticker="AAA", days=90)
    assert result["links"] == []
    assert result["coverage"] == "partial"
    assert result["earnings_timing"] == {
        "status": "withheld", "reason": "release_version_knowledge_unproven",
    }
    assert result["generated"] is (ready and run is not None)
    assert "excluded_count" not in result


def test_run_summary_cannot_imply_full_earnings_coverage():
    summary = cl.RunSummary("synthetic", NOW, "synthetic", 90).to_dict()
    assert summary["coverage"] == "partial"
    assert summary["earnings_timing"]["status"] == "withheld"


def test_graph_expand_uses_shared_guard_before_group_and_limit(monkeypatch):
    from api.routers import intelligence_actors as actors
    # Existing fixture's fake engine is deliberately not used to claim SQL semantics.
    import inspect
    source = inspect.getsource(actors._intel_expand_graph)
    query = source[source.index("SELECT cause_type"):source.index("for r in cause_rows:")]
    assert "EARNINGS_EDGE_READ_GUARD_SQL" in query
    assert query.index("EARNINGS_EDGE_READ_GUARD_SQL") < query.index("GROUP BY") < query.index("LIMIT")


def test_hidden_influence_query_is_guarded_before_limit(monkeypatch):
    from intelligence import deep_graph as dg
    engine = MagicMock()
    queries = []
    def execute(stmt, *args, **kwargs):
        queries.append(str(stmt))
        result = MagicMock()
        result.fetchall.return_value = []
        return result
    engine.connect.return_value.__enter__.return_value.execute.side_effect = execute
    monkeypatch.setattr(dg, "ensure_table", lambda *a: None)
    monkeypatch.setattr(dg, "_get_watchlist_tickers", lambda *a: ["AAA"])
    monkeypatch.setattr(dg, "find_all_overlaps", lambda *a: [])
    assert dg.discover_hidden_influence(engine) == []
    assert len(queries) == 1
    assert queries[0].index(cl.EARNINGS_EDGE_READ_GUARD_SQL) < queries[0].index("LIMIT")


def test_hidden_influence_envelope_labels_partial_causal_links(monkeypatch):
    from api.routers import intelligence_companies as companies
    from intelligence import deep_graph
    monkeypatch.setattr(companies, "get_db_engine", lambda: object())
    monkeypatch.setattr(deep_graph, "discover_hidden_influence", lambda *a: [])
    result = companies.get_hidden_influence(_token="test")
    assert result["count"] == 0
    assert result["causal_links_coverage"] == "partial"
    assert result["earnings_timing"]["status"] == "withheld"
