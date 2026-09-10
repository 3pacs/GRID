"""Lever map fixes (2026-09-10): every signal feed reaches the lever pullers,
communities are computed on the curated market-actor subgraph, and each
puller carries a motivation model.

No DB (fake engines), no network, no python-louvain (the networkx fallback
is exercised directly).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from intelligence import lever_pullers as lp

# The graph tests need networkx (a runtime dependency of scripts/graph_analytics);
# the lever-puller tests do not, so only those tests skip when it is missing.
try:
    import networkx as nx
except ImportError:  # pragma: no cover - exercised only on hosts without networkx
    nx = None

needs_networkx = pytest.mark.skipif(nx is None, reason="networkx not installed")

ROOT = Path(__file__).resolve().parents[1]


# ── 1. feeds → categories, identities ─────────────────────────────────────


@pytest.mark.parametrize(
    "source_type,category",
    [
        ("options_flow", "options_flow"),
        ("congressional", "congress"),
        ("quiverquant:house", "congress"),
        ("quiverquant:senate", "congress"),
        ("quiverquant:insider", "insider"),
        ("insider", "insider"),
        ("gov_contract", "government"),
        ("export_control", "regulator"),
        ("quiverquant:lobbying", "lobbyist"),
        ("crucix_idea", "analyst"),
        ("social", "social"),          # Reddit handles are no longer "institutional"
        ("darkpool", "dealer"),
        ("quiverquant:offexchange", "unknown"),  # aggregate feed, not an actor
        ("legislative", "unknown"),
        ("", "unknown"),
    ],
)
def test_source_type_categories(source_type: str, category: str) -> None:
    assert lp._category_from_source_type(source_type) == category


def test_every_category_has_an_influence_weight_and_a_position_label() -> None:
    for st, cat in lp.SOURCE_TYPE_CATEGORIES.items():
        assert cat in lp.INFLUENCE_WEIGHTS, cat
        assert lp._position_label(st, "x", {}) != "Unknown", (st, cat)
    assert lp.AGGREGATE_SOURCE_TYPES.isdisjoint(lp.SOURCE_TYPE_CATEGORIES)


def test_puller_identity_collapses_strikes_and_reads_quiverquant_names() -> None:
    assert lp.puller_identity("options_flow", "whale_aapl_100") == "whale_aapl"
    assert lp.puller_identity("options_flow", "whale_spy_823.5") == "whale_spy"
    assert lp.puller_identity("quiverquant:house", "qq_house_trading", {"Representative": "Jonathan Jackson"}) == "Jonathan Jackson"
    assert lp.puller_identity("quiverquant:senate", "qq_senate_trading", json.dumps({"Senator": "Sheldon Whitehouse"})) == "Sheldon Whitehouse"
    assert lp.puller_identity("quiverquant:insider", "qq_insider_trading", {"Name": "Offer Or"}) == "Offer Or"
    assert lp.puller_identity("quiverquant:lobbying", "qq_lobbying", {"Client": "CORMEDIX INC."}) == "CORMEDIX INC."
    # No identity field present -> the feed id is kept (never crashes on junk)
    assert lp.puller_identity("quiverquant:house", "qq_house_trading", "not json") == "qq_house_trading"
    assert lp.puller_identity("insider", "Bendza Gary Mark", {"insider_title": "CFO"}) == "Bendza Gary Mark"
    assert lp.puller_id_for("options_flow", "whale_tsm_180", None) == "options_flow:whale_tsm"


def test_identity_sql_mirrors_python_cases() -> None:
    sql = lp._IDENTITY_SQL
    for st in lp.IDENTITY_FIELDS:
        assert f"source_type = '{st}'" in sql
    for fields in lp.IDENTITY_FIELDS.values():
        for f in fields:
            assert f"signal_value->>'{f}'" in sql
    assert "regexp_replace(source_id, '_[0-9.]+$', '')" in sql
    assert "%" not in sql.replace("->>", "")  # no interpolation anywhere in the expression


def test_options_and_insider_influence_boosts() -> None:
    assert lp._influence_for_source("options_flow", "whale_spy_500", {}) == 0.8
    assert lp._influence_for_source("options_flow", "whale_ocul_10", {}) == 0.65
    assert lp._influence_for_source("quiverquant:insider", "Offer Or", {"officerTitle": "Chief Executive Officer"}) == 0.7
    assert lp._influence_for_source("insider", "x", {"insider_title": "10% owner"}) == 0.6
    assert lp._position_label("options_flow", "whale_tsm_180", {}) == "Options tape — TSM"
    assert lp._position_label("social", "reddit:abc", {}) == "Retail sentiment — reddit"


# ── identify_lever_pullers: quota, real counts, motivation ────────────────


class _Result:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def fetchall(self) -> list[Any]:
        return self._rows

    def fetchone(self) -> Any:
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, engine: "_Engine") -> None:
        self.engine = engine

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, statement: Any, params: Any = None) -> _Result:
        sql = str(statement)
        self.engine.calls.append((sql, params or {}))
        if "GROUP BY source_type" in sql:
            return _Result(self.engine.aggregate_rows)
        if "INSERT INTO lever_pullers" in sql:
            self.engine.persisted.append(params)
            return _Result([])
        if "SELECT signal_value FROM signal_sources" in sql:
            return _Result([(self.engine.meta.get(params["si"], {}),)])
        if "outcome = :oc" in sql:
            return _Result([])
        if "SELECT ticker, signal_date, signal_type, signal_value" in sql:
            return _Result(self.engine.recent.get(params["si"], []))
        return _Result([])


class _Engine:
    def __init__(self, aggregate_rows: list[tuple], recent: dict | None = None, meta: dict | None = None) -> None:
        self.aggregate_rows = aggregate_rows
        self.recent = recent or {}
        self.meta = meta or {}
        self.calls: list[tuple[str, dict]] = []
        self.persisted: list[dict] = []

    def connect(self) -> _Conn:
        return _Conn(self)

    def begin(self) -> _Conn:
        return _Conn(self)


def _agg(source_type: str, identity: str, correct: int, wrong: int) -> tuple:
    # (source_type, identity, total_scored, correct, wrong, last_signal_date, avg_lead_hours, avg_trust)
    return (source_type, identity, correct + wrong, correct, wrong, None, 48.0, 0.5)


def test_identify_applies_per_category_quota_and_persists_real_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lp, "_ensure_lever_table", lambda engine: None)
    monkeypatch.setattr(lp, "MAX_PER_CATEGORY", 2)
    monkeypatch.setattr(lp, "MAX_LEVER_PULLERS", 5)
    rows = [_agg("insider", f"Insider {i}", 9, 1) for i in range(6)]          # would swamp everything
    rows += [_agg("options_flow", "whale_tsm", 6, 4), _agg("congressional", "Angus King", 3, 2),
             _agg("quiverquant:offexchange", "qq_off_exchange", 100, 0)]      # excluded in SQL; harmless here
    engine = _Engine(rows, recent={
        "whale_tsm": [("TSM", "2026-09-07", "UNUSUAL_OPTIONS", {"direction": "CALL", "oi_ratio": 4.1, "notional": 5000})],
        "Insider 0": [("BCDA", "2026-08-17", "BUY", {"insider_title": "CEO"}), ("BCDA", "2026-08-13", "BUY", {})],
    })

    pullers = lp.identify_lever_pullers(engine)

    cats = [p.category for p in pullers]
    assert cats.count("insider") == 2 and "options_flow" in cats and "congress" in cats
    assert len(pullers) <= 5
    agg_sql, agg_params = next((s, p) for s, p in engine.calls if "GROUP BY source_type" in s)
    assert "NOT (source_type = ANY(:excluded))" in agg_sql and set(agg_params["excluded"]) == set(lp.AGGREGATE_SOURCE_TYPES)
    assert agg_params["min_scored"] == lp.MIN_SCORED_SIGNALS
    assert "HAVING COUNT(*) >= :min_scored" in agg_sql

    tsm = next(p for p in pullers if p.id == "options_flow:whale_tsm")
    assert tsm.total_signals == 10 and tsm.correct_signals == 6 and tsm.trust_score == round(7 / 12, 4)
    assert tsm.motivation_model == "likely_informed" and tsm.motivation_mix == {"likely_informed": 1}
    persisted_tsm = next(p for p in engine.persisted if p["sid"] == "options_flow:whale_tsm")
    assert persisted_tsm["total"] == 10 and persisted_tsm["correct"] == 6 and persisted_tsm["mot"] == "likely_informed"
    assert json.loads(persisted_tsm["meta"])["motivation_mix"] == {"likely_informed": 1}

    ins = next(p for p in pullers if p.id == "insider:Insider 0")
    assert ins.motivation_model == "likely_informed" and ins.motivation_mix == {"likely_informed": 2}
    quiet = next(p for p in pullers if p.category == "congress")
    assert quiet.motivation_model == "unknown" and quiet.motivation_mix == {}


def test_identify_returns_empty_when_nothing_is_scored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lp, "_ensure_lever_table", lambda engine: None)
    assert lp.identify_lever_pullers(_Engine([])) == []


# ── 3. motivation models ──────────────────────────────────────────────────


def _puller(category: str, recent: list[dict] | None = None) -> lp.LeverPuller:
    return lp.LeverPuller(
        id=f"{category}:x", name="x", category=category, influence_rank=0.6, trust_score=0.7,
        position="p", motivation_model="unknown", recent_actions=recent or [],
    )


@pytest.mark.parametrize(
    "category,action,expected",
    [
        ("options_flow", {"signal_type": "UNUSUAL_OPTIONS", "details": {"direction": "PUT"}}, "hedging"),
        ("options_flow", {"signal_type": "UNUSUAL_OPTIONS", "details": {"direction": "CALL", "oi_ratio": 4.0}}, "likely_informed"),
        ("options_flow", {"signal_type": "UNUSUAL_OPTIONS", "details": {"direction": "CALL", "notional": 2_500_000}}, "likely_informed"),
        ("options_flow", {"signal_type": "UNUSUAL_OPTIONS", "details": {"direction": "CALL", "oi_ratio": 1.2, "notional": 5000}}, "routine"),
        ("options_flow", {"signal_type": "UNUSUAL_OPTIONS", "details": {}}, "unknown"),
        ("government", {"signal_type": "CONTRACT_AWARD", "details": {"amount": 1.4e7}}, "institutional_mandate"),
        ("regulator", {"signal_type": "NEW_RULE", "details": {}}, "institutional_mandate"),
        ("lobbyist", {"signal_type": "lobbying", "details": {"Amount": "220000.0"}}, "likely_informed"),
        ("lobbyist", {"signal_type": "lobbying", "details": {"Amount": "20000"}}, "routine"),
        ("insider", {"signal_type": "SELL", "details": {"is_unusual_size": False}}, "routine"),
        ("insider", {"signal_type": "CLUSTER_BUY", "details": {}}, "unknown"),  # unchanged legacy rule
    ],
)
def test_assess_motivation_new_categories(category: str, action: dict, expected: str) -> None:
    assert lp.assess_motivation(_puller(category), action, engine=object()) == expected


def test_social_uses_pattern_deviation() -> None:
    p = _puller("social", recent=[{"signal_type": "BUY"}, {"signal_type": "BUY"}, {"signal_type": "SELL"}])
    assert lp.assess_motivation(p, {"signal_type": "SELL", "details": {}}, engine=object()) == "contrarian"
    assert lp.assess_motivation(p, {"signal_type": "BUY", "details": {}}, engine=object()) == "routine"


def test_derive_motivation_model_majority_priority_and_unknown() -> None:
    p = _puller("insider", recent=[
        {"signal_type": "SELL", "details": {}}, {"signal_type": "SELL", "details": {}},
        {"signal_type": "BUY", "details": {}},
    ])
    assert lp.derive_motivation_model(p, engine=object()) == "routine"
    assert p.motivation_model == "routine" and p.motivation_mix == {"routine": 2, "likely_informed": 1}

    # Tie -> priority order puts likely_informed ahead of routine
    p = _puller("insider", recent=[{"signal_type": "SELL", "details": {}}, {"signal_type": "BUY", "details": {}}])
    assert lp.derive_motivation_model(p, engine=object()) == "likely_informed"

    # "unknown" only wins when nothing else was observed
    p = _puller("insider", recent=[{"signal_type": "CLUSTER_BUY", "details": {}}, {"signal_type": "BUY", "details": {}}])
    assert lp.derive_motivation_model(p, engine=object()) == "likely_informed"
    p = _puller("analyst", recent=[{"signal_type": "trade_idea_long", "details": {}}])
    assert lp.derive_motivation_model(p, engine=object()) == "unknown" and p.motivation_mix == {"unknown": 1}
    assert lp.derive_motivation_model(_puller("insider"), engine=object()) == "unknown"


def test_events_and_convergence_key_on_normalised_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lp, "_ensure_lever_table", lambda engine: None)
    tsm = _puller("options_flow")
    tsm.id, tsm.name = "options_flow:whale_tsm", "whale_tsm"
    rep = _puller("congress")
    rep.id, rep.name = "quiverquant:house:Jonathan Jackson", "Jonathan Jackson"
    rows = [
        ("options_flow", "whale_tsm_180", "TSM", "2026-09-07", "UNUSUAL_OPTIONS", {"direction": "CALL", "oi_ratio": 4.0}, 0.9),
        ("options_flow", "whale_tsm_190", "TSM", "2026-09-06", "UNUSUAL_OPTIONS", {"direction": "PUT"}, 0.8),
        ("quiverquant:house", "qq_house_trading", "VSAT", "2026-08-28", "house_trading", {"Representative": "Jonathan Jackson"}, 0.6),
        ("quiverquant:house", "qq_house_trading", "LRCX", "2026-08-20", "house_trading", {"Representative": "Someone Else"}, 0.6),
    ]

    class E:
        def connect(self):
            return _FakeRows(rows)

    events = lp.get_active_lever_events(E(), days=30, pullers=[tsm, rep])
    assert sorted(e.puller.id for e in events) == [
        "options_flow:whale_tsm", "options_flow:whale_tsm", "quiverquant:house:Jonathan Jackson",
    ]
    informed = [e for e in events if "Likely informed" in e.motivation_assessment]
    assert len(informed) == 1 and informed[0].tickers == ["TSM"]


class _FakeRows:
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows

    def __enter__(self) -> "_FakeRows":
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False

    def execute(self, statement: Any, params: Any = None) -> _Result:
        return _Result(self.rows)


# ── 2. curated graph analytics ────────────────────────────────────────────


@needs_networkx
def test_graph_scope_whitelist_and_curated_edge_query() -> None:
    from scripts import graph_analytics as ga

    assert ga.table_for_scope("full") == "actor_analytics"
    assert ga.table_for_scope("curated") == "actor_analytics_curated"
    with pytest.raises(ValueError):
        ga.table_for_scope("drop table")

    with patch("scripts.graph_analytics.execute_sql") as ex:
        ex.side_effect = [
            [{"actor_a": "a", "actor_b": "b", "relationship": "controls", "strength": 0.9}],
            [{"id": "a", "name": "A", "category": "central_bank", "influence_score": 0.9},
             {"id": "b", "name": "B", "category": "fund", "influence_score": 0.4}],
        ]
        G = ga.load_actor_graph(scope="curated")
    sql, params = ex.call_args_list[0].args
    assert "JOIN actors a ON a.id = c.actor_a" in sql and "NOT LIKE %s" in sql and "= ANY(%s)" in sql
    assert params[0] == "icij%" and set(params[2]) == set(ga.DUMP_CATEGORIES) and "pep" in params[2]
    assert G.number_of_edges() == 1 and G.nodes["a"]["category"] == "central_bank"

    with patch("scripts.graph_analytics.execute_sql", return_value=[]) as ex:
        ga.load_actor_graph(scope="full")
    assert "JOIN actors" not in ex.call_args.args[0]


@needs_networkx
def test_louvain_falls_back_to_networkx_when_python_louvain_is_missing() -> None:
    from scripts import graph_analytics as ga

    G = nx.DiGraph()
    for a, b in [("a", "b"), ("b", "c"), ("c", "a"), ("x", "y"), ("y", "z"), ("z", "x")]:
        G.add_edge(a, b, weight=1.0)
    G.add_edge("a", "x", weight=0.05)
    import builtins

    real_import = builtins.__import__

    def no_louvain(name, *a, **k):
        if name == "community":
            raise ImportError("no python-louvain")
        return real_import(name, *a, **k)

    with patch("builtins.__import__", side_effect=no_louvain):
        partition = ga.compute_communities(G)
    assert set(partition) == set(G.nodes())
    assert partition["a"] == partition["b"] == partition["c"]
    assert partition["x"] == partition["y"] == partition["z"]
    assert partition["a"] != partition["x"]


@needs_networkx
def test_store_results_writes_to_the_scope_table() -> None:
    from scripts import graph_analytics as ga

    G = nx.DiGraph()
    G.add_edge("a", "b", weight=1.0)
    conn = MagicMock()
    cur = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur
    with patch("scripts.graph_analytics.get_connection", return_value=conn):
        stored = ga.store_results(G, {"a": 0.6, "b": 0.4}, {"a": 0, "b": 0}, {}, {}, {}, {}, {}, scope="curated")
    assert stored == 2
    sqls = [c.args[0] for c in cur.execute.call_args_list]
    assert any("CREATE TABLE IF NOT EXISTS actor_analytics_curated" in s for s in sqls)
    assert any(s.startswith("INSERT INTO actor_analytics_curated") for s in sqls)
    assert not any("INSERT INTO actor_analytics " in s for s in sqls)


def test_store_graph_scope_selects_table_and_single_query_community_list() -> None:
    from store import graph as sg

    assert sg.analytics_table("curated") == "actor_analytics_curated"
    with pytest.raises(ValueError):
        sg.analytics_table("actors; --")

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [(7, 42, 0.02, "Mubadala", "swf"), (3, 9, 0.01, None, None)]
    out = sg.get_community_list(engine=engine, scope="curated", limit=5)
    sql, params = conn.execute.call_args.args
    s = str(sql)
    assert "FROM actor_analytics_curated" in s and "DISTINCT ON (aa.community_id)" in s and "LIMIT :lim" in s
    assert params == {"lim": 5} and conn.execute.call_count == 1  # no per-community N+1
    assert out[0] == {"community_id": 7, "member_count": 42, "max_pagerank": 0.02, "top_member": "Mubadala", "top_category": "swf"}
    assert out[1]["top_member"] is None

    conn.execute.return_value.fetchall.return_value = [("central_bank", 3), ("fund", 2)]
    mix = sg.get_community_category_mix(7, limit=2, engine=engine, scope="curated")
    assert mix == [{"category": "central_bank", "count": 3}, {"category": "fund", "count": 2}]
    assert "FROM actor_analytics_curated aa" in str(conn.execute.call_args.args[0])

    conn.execute.return_value.fetchall.return_value = []
    sg.get_community_members(7, engine=engine, scope="full")
    assert "FROM actor_analytics aa" in str(conn.execute.call_args.args[0])
    sg.get_top_actors(metric="betweenness", engine=engine, scope="curated")
    assert "FROM actor_analytics_curated aa" in str(conn.execute.call_args.args[0])


def test_lever_hierarchy_carries_curated_communities_and_degrades(monkeypatch: pytest.MonkeyPatch) -> None:
    from intelligence import global_levers as gl

    monkeypatch.setattr(gl, "_fetch_live_lever_data", lambda engine: {})
    monkeypatch.setattr(gl, "_inject_dynamic_actors", lambda engine, hierarchy: None)
    # Unusable engine -> empty list, never an exception
    assert gl.get_lever_hierarchy(object())["actor_communities"] == []
    assert gl.get_lever_hierarchy(None)["actor_communities"] == []

    fake = {
        "get_community_list": lambda engine, scope, limit: [{"community_id": 7, "member_count": 42, "top_category": "swf"}],
        "get_community_members": lambda cid, limit, engine, scope: [
            {"actor_id": "mub", "name": "Mubadala", "category": "swf", "pagerank": 0.02}],
        "get_community_category_mix": lambda cid, limit, engine, scope: [{"category": "swf", "count": 5}],
    }
    import store.graph as sg

    for name, fn in fake.items():
        monkeypatch.setattr(sg, name, fn)
    comms = gl.get_lever_hierarchy(object())["actor_communities"]
    assert comms == [{
        "community_id": 7, "member_count": 42, "label": "swf",
        "leaders": [{"actor_id": "mub", "name": "Mubadala", "category": "swf", "pagerank": 0.02}],
        "category_mix": [{"category": "swf", "count": 5}],
    }]


def test_source_types_map_to_lever_domains() -> None:
    from intelligence.global_levers import _CATEGORY_TO_DOMAIN

    for st in ("options_flow", "quiverquant:house", "quiverquant:senate", "gov_contract",
               "export_control", "quiverquant:lobbying", "crucix_idea", "social"):
        assert st in _CATEGORY_TO_DOMAIN, st


def test_curated_graph_analytics_is_scheduled_weekly() -> None:
    src = (ROOT / "intelligence" / "scheduler.py").read_text(encoding="utf-8")
    assert '_sched.every().sunday.at("04:30").do(_curated_graph_analytics_weekly)' in src
    assert 'run_graph_analytics(scope="curated")' in src


# ── qualified-actor index: convergence and events see every actor, not the top 50 ──


def test_aggregate_query_has_no_trust_ordered_limit() -> None:
    import inspect

    src = inspect.getsource(lp._aggregate_scored_sources)
    assert "LIMIT" not in src.split("ORDER BY")[-1]  # a LIMIT here dropped every options tape before the quota
    assert "HAVING COUNT(*) >= :min_scored" in src and "NOT (source_type = ANY(:excluded))" in src


def test_build_puller_index_covers_every_qualified_actor_without_lookups() -> None:
    rows = [_agg("insider", f"Insider {i}", 9, 1) for i in range(30)]
    rows += [_agg("options_flow", "whale_spy", 20, 180), _agg("quiverquant:offexchange", "qq_off_exchange", 5, 0)]
    engine = _Engine(rows)
    index = lp.build_puller_index(engine)
    assert len(index) == 31  # aggregate feed mapped to "unknown" is dropped, nothing else is capped
    spy = next(p for p in index if p.id == "options_flow:whale_spy")
    assert spy.category == "options_flow" and spy.influence_rank == 0.8 and spy.trust_score == round(21 / 202, 4)
    assert spy.total_signals == 200 and spy.correct_signals == 20 and spy.position == "Options tape — SPY"
    # one aggregate query, no per-source enrichment
    assert len(engine.calls) == 1 and "GROUP BY source_type" in engine.calls[0][0]


def test_events_and_convergence_default_to_the_index_not_the_display_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lp, "_ensure_lever_table", lambda engine: None)
    monkeypatch.setattr(lp, "identify_lever_pullers",
                        lambda engine: (_ for _ in ()).throw(AssertionError("display list must not be used")))
    tsm_insiders = []
    for i in range(3):
        p = _puller("insider")
        p.id, p.name = f"insider:TSM Insider {i}", f"TSM Insider {i}"
        tsm_insiders.append(p)
    monkeypatch.setattr(lp, "build_puller_index", lambda engine: tsm_insiders)
    rows = [("TSM", "insider", f"TSM Insider {i}", "BUY", __import__("datetime").date(2026, 9, 7), 0.9, {}) for i in range(3)]

    class E:
        def connect(self):
            return _FakeRows(rows)

    conv = lp.find_lever_convergence(E())
    assert len(conv) == 1 and conv[0]["ticker"] == "TSM" and conv[0]["puller_count"] == 3

    ev_rows = [("insider", f"TSM Insider {i}", "TSM", "2026-09-07", "BUY", {}, 0.9) for i in range(3)]

    class E2:
        def connect(self):
            return _FakeRows(ev_rows)

    assert len(lp.get_active_lever_events(E2(), days=30)) == 3


@pytest.mark.parametrize(
    "action,expected",
    [
        ({"signal_type": "BUY"}, "BUY"),
        ({"signal_type": "UNUSUAL_SELL"}, "SELL"),
        ({"signal_type": "HEAT_SPIKE", "details": {"direction": "BEARISH"}}, "SELL"),
        ({"signal_type": "wsb_bullish", "details": {}}, "BUY"),
        ({"signal_type": "UNUSUAL_OPTIONS", "details": {"direction": "PUT"}}, "SELL"),
        ({"signal_type": "NET_POSITION_DELTA", "details": {}}, None),
    ],
)
def test_direction_of_normalises_feed_vocabularies(action: dict, expected: str | None) -> None:
    assert lp._direction_of(action) == expected


def test_social_motivation_uses_reddit_direction() -> None:
    p = _puller("social", recent=[
        {"signal_type": "HEAT_SPIKE", "details": {"direction": "BULLISH"}},
        {"signal_type": "HEAT_SPIKE", "details": {"direction": "BULLISH"}},
        {"signal_type": "HEAT_SPIKE", "details": {"direction": "BEARISH"}},
    ])
    assert lp.assess_motivation(p, {"signal_type": "HEAT_SPIKE", "details": {"direction": "BEARISH"}}, engine=object()) == "contrarian"
    assert lp.assess_motivation(p, {"signal_type": "HEAT_SPIKE", "details": {"direction": "BULLISH"}}, engine=object()) == "routine"
    assert lp.derive_motivation_model(p, engine=object()) == "routine"


@needs_networkx
def test_curated_edges_exclude_feed_artefact_nodes() -> None:
    from scripts import graph_analytics as ga

    with patch("scripts.graph_analytics.execute_sql", return_value=[]) as ex:
        ga.load_actor_graph(scope="curated")
    sql, params = ex.call_args_list[0].args
    assert "a.id NOT LIKE %s AND b.id NOT LIKE %s" in sql and "a.name NOT LIKE %s AND b.name NOT LIKE %s" in sql
    assert params[4:] == ("qq_%", "qq_%", "qq_%", "qq_%")


# ── stale-row purge, cross-feed aliases, event trust floor ────────────────


@needs_networkx
def test_store_results_purges_rows_older_than_the_run() -> None:
    from scripts import graph_analytics as ga

    G = nx.DiGraph()
    G.add_edge("a", "b", weight=1.0)
    conn = MagicMock()
    cur = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.rowcount = 7
    conn.cursor.return_value = cur
    with patch("scripts.graph_analytics.get_connection", return_value=conn):
        ga.store_results(G, {"a": 0.6, "b": 0.4}, {"a": 0, "b": 0}, {}, {}, {}, {}, {}, scope="curated")
    calls = [c.args for c in cur.execute.call_args_list]
    deletes = [c for c in calls if c[0].startswith("DELETE FROM actor_analytics_curated WHERE computed_at < %s")]
    assert len(deletes) == 1 and deletes[0][1][0].tzinfo is not None
    # the purge comes after the upserts
    assert calls.index(deletes[0]) > max(i for i, c in enumerate(calls) if c[0].startswith("INSERT INTO"))


def test_cross_feed_duplicates_merge_into_one_puller_with_aliases() -> None:
    a = _puller("congress"); a.id, a.name, a.trust_score, a.total_signals, a.correct_signals = "congressional:John Fetterman", "John Fetterman", 0.9, 8, 8
    b = _puller("congress"); b.id, b.name, b.trust_score, b.total_signals, b.correct_signals = "quiverquant:senate:John Fetterman", "John Fetterman", 0.875, 6, 6
    c = _puller("insider"); c.id, c.name = "insider:John Fetterman", "John Fetterman"  # different category: not the same lever
    d = _puller("congress"); d.id, d.name = "congressional:Angus King", "Angus King"
    merged = lp.merge_cross_feed_duplicates([b, a, c, d])
    ids = [p.id for p in merged]
    assert set(ids) == {"congressional:John Fetterman", "insider:John Fetterman", "congressional:Angus King"}
    assert ids[0] == "congressional:John Fetterman"  # highest trust first; the 0.875 feed row is folded in
    jf = merged[0]
    assert jf.aliases == ["quiverquant:senate:John Fetterman"] and jf.total_signals == 14 and jf.correct_signals == 14
    pm = lp.puller_map_of(merged)
    assert pm["quiverquant:senate:John Fetterman"] is jf and pm["congressional:John Fetterman"] is jf and len(pm) == 4


def test_convergence_counts_an_aliased_actor_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lp, "_ensure_lever_table", lambda engine: None)
    jf = _puller("congress"); jf.id, jf.name, jf.aliases = "congressional:John Fetterman", "John Fetterman", ["quiverquant:senate:John Fetterman"]
    other = _puller("congress"); other.id, other.name = "congressional:Angus King", "Angus King"
    day = __import__("datetime").date(2026, 9, 7)
    rows = [
        ("LRCX", "congressional", "John Fetterman", "BUY", day, 0.9, {}),
        ("LRCX", "quiverquant:senate", "qq_senate_trading", "senate_trading BUY", day, 0.9, {"Senator": "John Fetterman"}),
    ]

    class E:
        def connect(self):
            return _FakeRows(rows)

    assert lp.find_lever_convergence(E(), pullers=[jf, other]) == []  # one person, two feeds: no convergence
    rows.append(("LRCX", "congressional", "Angus King", "BUY", day, 0.8, {}))
    conv = lp.find_lever_convergence(E(), pullers=[jf, other])
    assert len(conv) == 1 and conv[0]["puller_count"] == 2


def test_events_skip_pullers_below_the_trust_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lp, "_ensure_lever_table", lambda engine: None)
    tape = _puller("options_flow"); tape.id, tape.name, tape.trust_score = "options_flow:whale_hyg", "whale_hyg", 0.001
    ins = _puller("insider"); ins.id, ins.name = "insider:Altman Peter", "Altman Peter"
    rows = [
        ("options_flow", "whale_hyg_80", "HYG", "2026-09-07", "UNUSUAL_OPTIONS", {"direction": "PUT"}, 0.5),
        ("insider", "Altman Peter", "BCDA", "2026-09-07", "BUY", {}, 0.84),
    ]

    class E:
        def connect(self):
            return _FakeRows(rows)

    events = lp.get_active_lever_events(E(), days=30, pullers=[tape, ins])
    assert [e.puller.id for e in events] == ["insider:Altman Peter"]
    assert lp.MIN_EVENT_TRUST == 0.02
