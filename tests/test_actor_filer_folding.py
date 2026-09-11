"""SEC-filer actor nodes fold into the ticker / insider actors they duplicate.

Covers the 2026-09-10 curated-graph finding: three Louvain communities (255 /
245 / 197 nodes) made only of SEC EDGAR display names wired to each other,
while the CVE Corp / BLK Corp ticker actors sat in the market clusters.

No DB (fake cursors), no network (the SEC ticker map is seeded from a literal
payload through the resolver's own test seam).
"""
from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from intelligence import actor_identity as ai

try:
    import networkx as nx
except ImportError:  # pragma: no cover - exercised only on hosts without networkx
    nx = None

needs_networkx = pytest.mark.skipif(nx is None, reason="networkx not installed")


# Real display names as SEC EDGAR full-text search emits them (two spaces
# before each parenthetical group — that spacing is what reached the graph).
CENOVUS = "CENOVUS ENERGY INC.  (CVE, CVE-PB)  (CIK 0001071297)"
BLACKROCK = "BlackRock Inc.  (BLK)  (CIK 0001364742)"
ISHARES = "iSHARES TRUST  (CIK 0001100663)"
SCRIVNER = "SCRIVNER DOUGLAS G  (CIK 0001234567)"


# ── 1. display-name parser ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw,tickers,cik,is_person,base",
    [
        (CENOVUS, ("CVE", "CVE-PB"), "0001071297", False, "CENOVUS ENERGY INC."),
        (BLACKROCK, ("BLK",), "0001364742", False, "BlackRock Inc."),
        (ISHARES, (), "0001100663", False, "iSHARES TRUST"),
        (SCRIVNER, (), "0001234567", True, "SCRIVNER DOUGLAS G"),
        # Un-padded CIK, and a share-class ticker with a dot
        ("BERKSHIRE HATHAWAY INC  (BRK.A, BRK.B)  (CIK 1067983)",
         ("BRK.A", "BRK.B"), "0001067983", False, "BERKSHIRE HATHAWAY INC"),
        # No parentheses at all
        ("Some Random Fund", (), None, False, "Some Random Fund"),
        # Person with a suffix
        ("SMITH JOHN A JR  (CIK 0000999999)", (), "0000999999", True, "SMITH JOHN A JR"),
        # A parenthetical that is words, not tickers, is neither
        ("ACME (the issuer)  (CIK 0000111111)", (), "0000111111", False, "ACME"),
        # EDGAR conformed names carry "(THE)" — ticker-shaped, but not the ticker
        ("COCA COLA CO (THE)  (KO)  (CIK 0000021344)",
         ("KO",), "0000021344", False, "COCA COLA CO"),
        ("BOEING CO (THE)  (CIK 0000012927)", (), "0000012927", False, "BOEING CO"),
        (None, (), None, False, ""),
        ("", (), None, False, ""),
    ],
)
def test_parse_filer_display_name(
    raw: str | None, tickers: tuple, cik: str | None, is_person: bool, base: str
) -> None:
    identity = ai.parse_filer_display_name(raw)
    assert identity.tickers == tickers
    assert identity.cik == cik
    assert identity.is_person is is_person
    assert identity.base_name == base


def test_ticker_actor_shape_matches_the_existing_corp_nodes() -> None:
    # scripts/enrich_connections.py creates corp_<TICKER> named "<TICKER> Corp"
    assert ai.ticker_actor_id("cve") == "corp_CVE"
    assert ai.ticker_actor_name("blk") == "BLK Corp"


def test_person_name_key_is_order_and_initial_independent() -> None:
    assert ai.person_name_key("SCRIVNER DOUGLAS G") == "douglas scrivner"
    assert ai.person_name_key("Douglas G. Scrivner") == "douglas scrivner"
    assert ai.person_name_key("Scrivner, Douglas") == "douglas scrivner"
    assert ai.person_name_key("SMITH JOHN A JR") == "john smith"
    assert ai.person_name_key("") == ""


def test_name_with_ticker_proposes_the_ticker_actor() -> None:
    proposal = ai.propose_canonical(CENOVUS)
    assert proposal is not None
    assert proposal.rule == ai.RULE_TICKER_IN_NAME
    assert proposal.actor_id == "corp_CVE"      # primary listing, not CVE-PB
    assert proposal.person_key is None

    assert ai.propose_canonical(BLACKROCK).actor_id == "corp_BLK"


def test_person_filer_proposes_a_name_key_not_an_id() -> None:
    proposal = ai.propose_canonical(SCRIVNER)
    assert proposal is not None
    assert proposal.rule == ai.RULE_PERSON_NAME
    assert proposal.actor_id is None
    assert proposal.person_key == "douglas scrivner"


# ── 2. CIK -> ticker lookup ───────────────────────────────────────────────


@pytest.fixture()
def sec_map(monkeypatch: pytest.MonkeyPatch):
    """Seed the shared SEC company_tickers cache without touching the network."""
    from grid.signals import sponsor_resolver as sr

    monkeypatch.setattr(sr, "_SEC_LOADED", True)
    monkeypatch.setattr(sr, "_SEC_TICKERS", set())
    monkeypatch.setattr(sr, "_SEC_TICKER_TO_CIK", {})
    monkeypatch.setattr(sr, "_SEC_CIK_TO_TICKER", {})
    monkeypatch.setattr(sr, "_SEC_CIK_TITLES", {})
    monkeypatch.setattr(sr, "_SEC_NAME_TO_TICKER", {})
    monkeypatch.setattr(sr, "_SEC_NORM_TO_TICKER", {})
    sr._ingest_sec_payload({
        "0": {"cik_str": 1364742, "ticker": "BLK", "title": "BlackRock Inc."},
        "1": {"cik_str": 1071297, "ticker": "CVE", "title": "CENOVUS ENERGY INC"},
        # Two share classes on one CIK: the first listed wins.
        "2": {"cik_str": 1652044, "ticker": "GOOGL", "title": "Alphabet Inc."},
        "3": {"cik_str": 1652044, "ticker": "GOOG", "title": "Alphabet Inc."},
        # A fund complex: one CIK, many unrelated names.
        "4": {"cik_str": 1100663, "ticker": "IVV", "title": "iShares Core S&P 500 ETF"},
        "5": {"cik_str": 1100663, "ticker": "IJH", "title": "iShares Core S&P Mid-Cap ETF"},
    })
    return sr


def test_sec_ticker_for_cik_round_trips_and_prefers_the_primary_class(sec_map) -> None:
    assert sec_map.sec_ticker_for_cik("0001364742") == "BLK"
    assert sec_map.sec_ticker_for_cik("1364742") == "BLK"       # un-padded
    assert sec_map.sec_ticker_for_cik(1364742) == "BLK"         # int, as SEC emits it
    assert sec_map.sec_ticker_for_cik("0001652044") == "GOOGL"  # not GOOG
    assert sec_map.sec_cik_for_ticker("BLK") == "0001364742"    # inverse still works
    # A fund complex — one CIK, many unrelated names — has no single ticker,
    # so it must not resolve to an arbitrary member (IVV or IJH).
    assert sec_map.sec_ticker_for_cik("0001100663") is None
    assert sec_map.sec_cik_for_ticker("IVV") == "0001100663"     # forward map unaffected
    # A CIK absent from the file entirely
    assert sec_map.sec_ticker_for_cik("0009999999") is None
    assert sec_map.sec_ticker_for_cik(None) is None
    assert sec_map.sec_ticker_for_cik("not-a-cik") is None


def test_cik_rule_resolves_a_tickerless_name(sec_map) -> None:
    # No ticker in the name, but the CIK is a listed company.
    proposal = ai.propose_canonical("BLACKROCK INC.  (CIK 0001364742)")
    assert proposal.rule == ai.RULE_CIK_TO_TICKER and proposal.actor_id == "corp_BLK"

    # A 13F filer node carries its CIK in metadata, not in the name.
    proposal = ai.propose_canonical("BlackRock", cik_hint="0001364742")
    assert proposal.rule == ai.RULE_CIK_TO_TICKER and proposal.actor_id == "corp_BLK"

    # iSHARES TRUST has a CIK but no listed equity -> left alone.
    assert ai.propose_canonical(ISHARES) is None


def test_resolve_only_returns_ids_that_exist(sec_map) -> None:
    assert ai.resolve_canonical_actor_id(CENOVUS, exists=lambda _: True) == (
        "corp_CVE", ai.RULE_TICKER_IN_NAME,
    )
    # The ticker actor is not in the graph -> the filer keeps its own node.
    assert ai.resolve_canonical_actor_id(CENOVUS, exists=lambda _: False) is None

    # Person rule needs a lookup; without one it is skipped.
    assert ai.resolve_canonical_actor_id(SCRIVNER, exists=lambda _: True) is None
    assert ai.resolve_canonical_actor_id(
        SCRIVNER,
        exists=lambda i: i == "insider_douglas_scrivner",
        person_lookup=lambda k: "insider_douglas_scrivner" if k == "douglas scrivner" else None,
    ) == ("insider_douglas_scrivner", ai.RULE_PERSON_NAME)


# ── 3. fold planning and SQL shape ────────────────────────────────────────


def test_plan_folds_skips_unresolvable_missing_self_and_chained(sec_map) -> None:
    from scripts import fold_actor_aliases as fa

    candidates = [
        ("corporation_cenovus_energy_inc_cve_cve_pb_cik_0001071297", CENOVUS, None),
        ("corporation_blackrock_inc_blk_cik_0001364742", BLACKROCK, None),
        ("corporation_ishares_trust_cik_00011006", ISHARES, None),       # no rule fires
        ("corporation_scrivner_douglas_g", SCRIVNER, None),
        ("inst_13f_1364742", "BlackRock", "0001364742"),                  # CIK rule
        ("corp_BLK", BLACKROCK, None),                                    # resolves to itself
    ]
    person_index = {"douglas scrivner": "insider_douglas_scrivner"}
    existing = {"corp_CVE", "corp_BLK", "insider_douglas_scrivner"}

    folds = fa.plan_folds(candidates, existing, person_index)
    by_alias = {f.alias_id: (f.canonical_id, f.rule) for f in folds}

    assert by_alias["corporation_cenovus_energy_inc_cve_cve_pb_cik_0001071297"] == (
        "corp_CVE", ai.RULE_TICKER_IN_NAME,
    )
    assert by_alias["corporation_blackrock_inc_blk_cik_0001364742"] == (
        "corp_BLK", ai.RULE_TICKER_IN_NAME,
    )
    assert by_alias["inst_13f_1364742"] == ("corp_BLK", ai.RULE_CIK_TO_TICKER)
    assert by_alias["corporation_scrivner_douglas_g"] == (
        "insider_douglas_scrivner", ai.RULE_PERSON_NAME,
    )
    assert "corporation_ishares_trust_cik_00011006" not in by_alias  # unresolved: kept
    assert "corp_BLK" not in by_alias                                 # never folds into itself


def test_plan_folds_refuses_a_chain(sec_map) -> None:
    from scripts import fold_actor_aliases as fa

    # corp_BLK is itself folding onward -> folding into it would strand edges
    # one hop short of the real actor.
    candidates = [
        ("corporation_blackrock_inc_blk_cik_0001364742", BLACKROCK, None),  # -> corp_BLK
        ("corp_BLK", CENOVUS, None),                                        # -> corp_CVE
    ]
    folds = fa.plan_folds(candidates, {"corp_BLK", "corp_CVE"}, {})
    assert [(f.alias_id, f.canonical_id) for f in folds] == [("corp_BLK", "corp_CVE")]


def test_build_person_index_drops_ambiguous_keys() -> None:
    from scripts import fold_actor_aliases as fa

    index = fa.build_person_index([
        ("insider_douglas_scrivner", "Douglas Scrivner"),
        ("insider_jane_doe", "Jane Doe"),
        ("ins_jane_doe", "DOE JANE"),        # same key, different actor -> ambiguous
        ("insider_blank", ""),
    ])
    assert index == {"douglas scrivner": "insider_douglas_scrivner"}


def test_fold_sql_is_parameterised_and_dedupes_on_the_unique_key() -> None:
    from scripts import fold_actor_aliases as fa

    sql = fa._FOLD_EDGES_SQL
    # Re-point both endpoints, keep the strongest edge, drop self-loops.
    assert "LEFT JOIN m ma ON ma.alias_id = c.actor_a" in sql
    assert "LEFT JOIN m mb ON mb.alias_id = c.actor_b" in sql
    assert "MAX(strength) AS strength" in sql
    assert "GROUP BY new_a, new_b, relationship" in sql
    assert "WHERE new_a <> new_b" in sql
    assert "ON CONFLICT (actor_a, actor_b, relationship) DO UPDATE" in sql
    assert "GREATEST(actor_connections.strength, EXCLUDED.strength)" in sql
    assert "DELETE FROM actor_connections WHERE id IN (SELECT id FROM touched)" in sql
    # Aliases arrive as bound arrays, never interpolated.
    assert sql.count("%s") == 2
    for statement in (fa._MARK_MERGED_SQL, fa._RECORD_ALIASES_SQL, fa._CANDIDATE_SQL,
                      fa._EXISTING_SQL, fa._AFFECTED_EDGES_SQL, fa._PERSON_INDEX_SQL):
        assert "'" + "%" not in statement and ".format(" not in statement
    # Never deletes an actors row.
    for statement in vars(fa).values():
        if isinstance(statement, str) and "DELETE FROM actors" in statement:
            pytest.fail("fold_actor_aliases must never delete actors rows")


class _FakeCursor:
    """Records executed (sql, params) and answers the two SELECTs the fold makes."""

    def __init__(self, affected: int = 0, fold_row: tuple = (5, 6, 1)) -> None:
        self.calls: list[tuple[str, Any]] = []
        self._affected = affected
        self._fold_row = fold_row
        self._next: tuple | None = None
        self.rowcount = 3

    def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((sql, params))
        if "COUNT(*) FROM actor_connections" in sql:
            self._next = (self._affected,)
        elif sql.startswith("WITH m(alias_id"):
            self._next = self._fold_row
        else:
            self._next = None

    def fetchone(self) -> tuple | None:
        return self._next


def test_fold_batch_binds_parallel_arrays_and_reports_counts() -> None:
    from scripts import fold_actor_aliases as fa

    batch = [
        fa.AliasFold("corporation_blackrock_inc_blk", BLACKROCK, "corp_BLK", ai.RULE_TICKER_IN_NAME),
        fa.AliasFold("inst_13f_1071297", CENOVUS, "corp_CVE", ai.RULE_CIK_TO_TICKER),
    ]
    cur = _FakeCursor(fold_row=(5, 6, 1))
    counts = fa.fold_batch(cur, batch)

    assert counts == {
        "aliases": 2, "edges_repointed": 5, "edges_removed": 6,
        "self_loops_dropped": 1, "actors_marked": 3,
    }

    sqls = [c[0] for c in cur.calls]
    assert sqls[0] is fa._FOLD_EDGES_SQL
    assert sqls[1] is fa._RECORD_ALIASES_SQL
    assert sqls[2] is fa._MARK_MERGED_SQL

    # Fold and mark bind alias ids against canonical ids, in the same order.
    assert cur.calls[0][1] == (
        ["corporation_blackrock_inc_blk", "inst_13f_1071297"], ["corp_BLK", "corp_CVE"],
    )
    assert cur.calls[2][1] == cur.calls[0][1]
    # The alias record binds canonical ids against the filer *names*.
    assert cur.calls[1][1] == (["corp_BLK", "corp_CVE"], [BLACKROCK, CENOVUS])


def test_count_affected_edges_binds_the_alias_list_twice() -> None:
    from scripts import fold_actor_aliases as fa

    cur = _FakeCursor(affected=42)
    assert fa.count_affected_edges(cur, ["a", "b"]) == 42
    assert cur.calls[0][1] == (["a", "b"], ["a", "b"])
    # No aliases -> no query at all
    cur2 = _FakeCursor()
    assert fa.count_affected_edges(cur2, []) == 0 and cur2.calls == []


def test_dry_run_writes_nothing(sec_map) -> None:
    from scripts import fold_actor_aliases as fa

    cur = _FakeCursor()
    cur.fetchall = MagicMock(side_effect=[
        [("corporation_blackrock_inc_blk_cik_0001364742", BLACKROCK, None)],  # candidates
        [("insider_douglas_scrivner", "Douglas Scrivner")],                   # person index
        [("corp_BLK",)],                                                     # existing
    ])
    conn = MagicMock()
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    cursor_cm = MagicMock()
    cursor_cm.__enter__ = MagicMock(return_value=cur)
    cursor_cm.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor_cm

    with patch("scripts.fold_actor_aliases.get_connection", return_value=conn), \
         patch("scripts.fold_actor_aliases.ensure_merged_into_column"):
        summary = fa.run_fold(apply=False)

    assert summary["candidates"] == 1 and summary["planned"] == 1
    assert summary["by_rule"] == {ai.RULE_TICKER_IN_NAME: 1}
    assert summary["applied"] is False
    executed = [c[0] for c in cur.calls]
    assert not any(s.startswith("UPDATE") or s.startswith("WITH m(") for s in executed)


# ── 4. curated graph excludes merged actors ───────────────────────────────


@needs_networkx
def test_curated_edges_exclude_merged_actors() -> None:
    from scripts import graph_analytics as ga

    assert "a.merged_into IS NULL AND b.merged_into IS NULL" in ga._CURATED_EDGE_SQL

    with patch("scripts.graph_analytics.execute_sql", return_value=[]) as ex:
        ga.load_actor_graph(scope="curated")
    sql = ex.call_args_list[0].args[0]
    assert "a.merged_into IS NULL AND b.merged_into IS NULL" in sql

    # The full scope is untouched: no join, no merged_into filter.
    with patch("scripts.graph_analytics.execute_sql", return_value=[]) as ex:
        ga.load_actor_graph(scope="full")
    assert "merged_into" not in ex.call_args.args[0]
    assert "JOIN actors" not in ex.call_args.args[0]


@needs_networkx
def test_curated_run_ensures_the_merged_into_column_and_full_scope_does_not() -> None:
    """The weekly curated job self-heals a tree that has not applied 0061."""
    from scripts import graph_analytics as ga

    for scope, expected in (("curated", 1), ("full", 0)):
        with patch("intelligence.actor_identity.ensure_merged_into_column") as ensure, \
             patch.object(ga, "load_actor_graph", return_value=nx.DiGraph()) as load:
            result = ga.run_graph_analytics(scope=scope)
        assert ensure.call_count == expected, scope
        assert load.call_args.kwargs == {"scope": scope}
        assert result["nodes"] == 0        # empty graph short-circuits, no writes


def test_ensure_merged_into_column_never_takes_down_its_caller() -> None:
    """ALTER TABLE needs ownership even as a no-op, and the weekly curated job
    runs as the unprivileged `grid` role — a convenience DDL must not fail it."""
    calls: list[str] = []

    def ok(sql: str, params=None):
        calls.append(sql)
        return []

    with patch.dict("sys.modules", {"db": MagicMock(execute_sql=ok)}):
        ai.ensure_merged_into_column()
    assert calls == [ai.MERGED_INTO_DDL, ai.MERGED_INTO_INDEX_DDL]

    def denied(sql: str, params=None):
        raise RuntimeError("permission denied: must be owner of table actors")

    with patch.dict("sys.modules", {"db": MagicMock(execute_sql=denied)}):
        ai.ensure_merged_into_column()      # warns, does not raise


@needs_networkx
def test_curated_run_survives_a_denied_ensure() -> None:
    """The point of the tolerance: the real curated path still runs when the
    role cannot ALTER the table (the column is already there via the migration)."""
    from scripts import graph_analytics as ga

    def denied(sql: str, params=None):
        raise RuntimeError("permission denied: must be owner of table actors")

    # The real ensure_merged_into_column runs, hits the denial, warns, returns.
    with patch.dict("sys.modules", {"db": MagicMock(execute_sql=denied)}), \
         patch.object(ga, "load_actor_graph", return_value=nx.DiGraph()) as load:
        result = ga.run_graph_analytics(scope="curated")
    assert load.call_count == 1 and result["nodes"] == 0


def test_merged_into_ddl_is_idempotent_and_matches_the_migration() -> None:
    from pathlib import Path

    assert "ADD COLUMN IF NOT EXISTS merged_into" in ai.MERGED_INTO_DDL
    assert "CREATE INDEX IF NOT EXISTS idx_actors_merged_into" in ai.MERGED_INTO_INDEX_DDL

    migration = (
        Path(__file__).resolve().parents[1] / "migrations" / "0061_actors_merged_into.sql"
    ).read_text(encoding="utf-8")
    assert "ALTER TABLE actors ADD COLUMN IF NOT EXISTS merged_into TEXT;" in migration
    assert "CREATE INDEX IF NOT EXISTS idx_actors_merged_into" in migration
    assert "GRANT ALL ON actors TO grid;" in migration          # GRANT footer
    assert "DELETE FROM actors" not in migration                # rows are marked, never deleted


# ── 5. ingestion: the spider folds before it creates a node ───────────────


class _StubResolver:
    def __init__(self) -> None:
        self.resolve_calls: list[str] = []

    def resolve(self, name: str, hint: dict) -> str | None:
        self.resolve_calls.append(name)
        return None

    def generate_id(self, name: str, category: str) -> str:
        return f"{category}_{name.lower().replace(' ', '_')}"


def _discovered(name: str):
    from intelligence.spider.models import DiscoveredConnection

    return DiscoveredConnection(
        target_name=name,
        relationship="5pct_holder",
        strength=0.85,
        confidence_tier=1,
        target_hint={"form_type": "SC 13G"},
        evidence=[{"source": "sec_edgar"}],
    )


class _StubAdapter:
    name = "stub"

    def __init__(self, names: list[str]) -> None:
        self._names = names

    def discover(self, actor_name: str, actor_hint: dict) -> list:
        return [_discovered(n) for n in self._names]


def test_spider_attaches_filer_edges_to_the_canonical_actor(sec_map) -> None:
    from intelligence.spider.discovery import DiscoveryOrchestrator
    from intelligence.spider.graph_engine import GraphEngine

    graph = GraphEngine()
    graph.add_actor("corp_CVE", {"name": "CVE Corp", "category": "corporation", "degree": 0})
    graph.add_actor("corp_BLK", {"name": "BLK Corp", "category": "corporation", "degree": 0})
    graph.add_actor("insider_douglas_scrivner", {"name": "Douglas Scrivner", "category": "insider"})
    graph.add_actor("seed", {"name": "Seed Actor", "category": "fund", "degree": 0})

    resolver = _StubResolver()
    orch = DiscoveryOrchestrator(
        graph, resolver, [_StubAdapter([CENOVUS, BLACKROCK, SCRIVNER, ISHARES])],
    )
    new_actors, new_connections = orch.expand("seed")

    targets = {target for _src, target, _meta in new_connections}
    assert {"corp_CVE", "corp_BLK", "insider_douglas_scrivner"} <= targets

    # Only the unresolvable trust became a new node — no corporation_* twins.
    assert len(new_actors) == 1
    assert new_actors[0]["name"] == ISHARES
    assert not any(a["id"].startswith("corporation_cenovus") for a in new_actors)
    assert graph.actor_count == 5

    # The filer name now resolves to the canonical actor by name alone.
    assert graph.resolve_name(CENOVUS) == "corp_CVE"
    # The fuzzy resolver was consulted only for the name no rule could place.
    assert resolver.resolve_calls == [ISHARES]


def test_spider_builds_the_person_index_at_most_once_and_only_when_needed(sec_map) -> None:
    from intelligence.spider.discovery import DiscoveryOrchestrator
    from intelligence.spider.graph_engine import GraphEngine

    graph = GraphEngine()
    graph.add_actor("corp_BLK", {"name": "BLK Corp", "category": "corporation"})
    graph.add_actor("insider_douglas_scrivner", {"name": "Douglas Scrivner", "category": "insider"})
    graph.add_actor("seed", {"name": "Seed Actor", "category": "fund", "degree": 0})

    # Two person filers and one company: one O(actors) pass, not three.
    orch = DiscoveryOrchestrator(
        graph, _StubResolver(),
        [_StubAdapter([BLACKROCK, SCRIVNER, "SMITH JOHN A  (CIK 0000999999)"])],
    )
    with patch.object(
        DiscoveryOrchestrator, "_build_person_index",
        side_effect=lambda: {"douglas scrivner": "insider_douglas_scrivner"},
        autospec=False,
    ) as build:
        orch.expand("seed")
    assert build.call_count == 1

    # A company-only expansion never touches the index at all.
    orch2 = DiscoveryOrchestrator(graph, _StubResolver(), [_StubAdapter([BLACKROCK])])
    with patch.object(DiscoveryOrchestrator, "_build_person_index", return_value={}) as build:
        orch2.expand("seed")
    assert build.call_count == 0


def test_build_person_index_only_indexes_person_shaped_ids() -> None:
    from intelligence.spider.discovery import DiscoveryOrchestrator
    from intelligence.spider.graph_engine import GraphEngine

    graph = GraphEngine()
    graph.add_actor("insider_douglas_scrivner", {"name": "Douglas Scrivner"})
    graph.add_actor("congress_angus_king", {"name": "Angus King"})
    # A company whose name would key like a person must not answer a person key.
    graph.add_actor("corporation_douglas_scrivner", {"name": "SCRIVNER DOUGLAS G"})

    index = DiscoveryOrchestrator(graph, _StubResolver(), [])._build_person_index()
    assert index == {
        "douglas scrivner": "insider_douglas_scrivner",
        "angus king": "congress_angus_king",
    }


def test_spider_still_creates_a_node_when_the_ticker_actor_is_absent(sec_map) -> None:
    from intelligence.spider.discovery import DiscoveryOrchestrator
    from intelligence.spider.graph_engine import GraphEngine

    graph = GraphEngine()
    graph.add_actor("seed", {"name": "Seed Actor", "category": "fund", "degree": 0})
    orch = DiscoveryOrchestrator(graph, _StubResolver(), [_StubAdapter([CENOVUS])])
    new_actors, _ = orch.expand("seed")

    assert len(new_actors) == 1 and new_actors[0]["name"] == CENOVUS


def test_add_alias_never_creates_or_steals_a_node() -> None:
    from intelligence.spider.graph_engine import GraphEngine

    graph = GraphEngine()
    graph.add_actor("corp_BLK", {"name": "BLK Corp", "category": "corporation"})
    graph.add_alias("corp_BLK", BLACKROCK)
    assert graph.resolve_name(BLACKROCK) == "corp_BLK" and graph.actor_count == 1

    graph.add_alias("does_not_exist", "Whoever")          # unknown actor: no-op
    assert graph.resolve_name("Whoever") is None and graph.actor_count == 1

    graph.add_actor("corp_CVE", {"name": "CVE Corp", "category": "corporation"})
    graph.add_alias("corp_CVE", "BLK Corp")               # name already taken: kept
    assert graph.resolve_name("BLK Corp") == "corp_BLK"
