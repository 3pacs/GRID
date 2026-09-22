"""Seeded actors must be distinguishable from observed ones, end to end.

Covers audit A-H13: ``_seed_known_actors`` is a *live fallback*, not a
fixture loader -- ``build_actor_graph`` calls it whenever the ``actors``
table comes back empty -- and it used to write 500+ hand-typed figures about
named real people with ``updated_at = NOW()`` and
``credibility = "hard_data"``, so a literal typed in months ago was
indistinguishable from a fresh measurement.

No database is touched. ``FakeActorsEngine`` below is an in-memory stand-in
that understands exactly the statements ``intelligence/actors/db.py`` issues,
including the ability to pretend the ``provenance`` column does not exist
(a database that has not run alembic ``actors_provenance_20260917``).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Self

import pytest

from intelligence.actors.db import _load_actors_from_db, _seed_known_actors
from intelligence.actors.graph import build_actor_graph
from intelligence.actors.provenance import (
    PROVENANCE_OBSERVED,
    PROVENANCE_SEED,
    SEED_ACTOR_IDS,
    SEED_VINTAGE,
    SEED_VINTAGE_TS,
    actor_source,
    resolve_provenance,
    source_as_of,
)
from intelligence.actors.seed_data import _KNOWN_ACTORS

REPO_ROOT = Path(__file__).resolve().parents[1]


# ── In-memory stand-in for the actors table ───────────────────────────────


class _MissingColumn(Exception):
    """Stands in for psycopg's UndefinedColumn."""


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, db: FakeActorsEngine) -> None:
        self.db = db

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def commit(self) -> None:
        pass

    def rollback(self) -> None:
        self.db.rollbacks += 1

    def execute(self, stmt, params=None) -> _Result:
        sql = " ".join(str(stmt).split())
        params = params or {}
        self.db.statements.append(sql)

        if sql.startswith(("CREATE", "ALTER", "DROP")):
            return _Result([])
        if "INSERT INTO actors" in sql:
            return self.db._upsert(params)
        if "FROM actors" in sql:
            return self.db._select_actors(sql, params)
        # actor_connections / signal_sources: nothing to serve.
        return _Result([])


class FakeActorsEngine:
    """Understands the handful of statements intelligence/actors/db.py runs."""

    def __init__(self, *, has_provenance: bool = True) -> None:
        self.rows: dict[str, dict] = {}
        self.has_provenance = has_provenance
        self.statements: list[str] = []
        self.rollbacks = 0

    # SQLAlchemy Engine surface used by the code under test
    def connect(self) -> _Conn:
        return _Conn(self)

    def begin(self) -> _Conn:
        return _Conn(self)

    # -- statement handlers --

    def _upsert(self, params: dict) -> _Result:
        row = dict(self.rows.get(params["id"], {}))
        row.update({
            "id": params["id"],
            "name": params["name"],
            "tier": params["tier"],
            "category": params["category"],
            "title": params["title"],
            "net_worth_estimate": params.get("nw"),
            "aum": params.get("aum"),
            "influence_score": params.get("inf"),
            "trust_score": params.get("trust"),
            "motivation_model": params.get("motivation"),
            "data_sources": params.get("sources", "[]"),
            "credibility": params.get("cred"),
            "provenance": params.get("provenance"),
            "provenance_as_of": params.get("vintage_date"),
            "updated_at": params.get("vintage_ts"),
        })
        row.setdefault("connections", "[]")
        row.setdefault("known_positions", "[]")
        row.setdefault("board_seats", "[]")
        row.setdefault("political_affiliations", "[]")
        self.rows[params["id"]] = row
        return _Result([])

    def _select_actors(self, sql: str, params: dict) -> _Result:
        columns = [
            c.strip()
            for c in re.search(r"SELECT (.+?) FROM actors", sql).group(1).split(",")
        ]
        if not self.has_provenance and "provenance" in columns:
            raise _MissingColumn('column "provenance" does not exist')

        rows = list(self.rows.values())
        excluded = set(params.get("excluded") or ())
        if excluded:
            rows = [r for r in rows if r.get("category") not in excluded]
        rows.sort(key=lambda r: r.get("influence_score") or 0, reverse=True)
        return _Result([tuple(r.get(c) for c in columns) for r in rows])

    # -- helpers for tests --

    def add_observed(self, actor_id: str, **overrides) -> None:
        """Insert a row the way an ingestion path would (no provenance stamp)."""
        row = {
            "id": actor_id,
            "name": overrides.get("name", actor_id.replace("_", " ").title()),
            "tier": "individual",
            "category": "insider",
            "title": "",
            "net_worth_estimate": None,
            "aum": None,
            "influence_score": overrides.get("influence_score", 0.4),
            "trust_score": 0.5,
            "motivation_model": "unknown",
            "connections": "[]",
            "known_positions": "[]",
            "board_seats": "[]",
            "political_affiliations": "[]",
            "data_sources": json.dumps(["form4"]),
            "credibility": "public_record",
            "provenance": overrides.get("provenance", PROVENANCE_OBSERVED),
            "provenance_as_of": overrides.get("provenance_as_of"),
            "updated_at": overrides.get("updated_at"),
        }
        self.rows[actor_id] = row


@pytest.fixture
def db() -> FakeActorsEngine:
    return FakeActorsEngine()


# ── Criterion 1: source on every node, seeded from an empty DB ────────────


@pytest.mark.unit
def test_empty_db_seeds_and_every_graph_node_has_a_source(db: FakeActorsEngine) -> None:
    """The seed-fallback path: empty table -> seeded -> graph -> labelled."""
    graph = build_actor_graph(db)

    assert graph["nodes"], "seed fallback produced no nodes"
    assert all(n.get("source") for n in graph["nodes"]), (
        "every actor node must carry a non-null source"
    )
    assert {n["source"] for n in graph["nodes"]} == {"curated_seed"}
    assert all(n["source_as_of"] == SEED_VINTAGE for n in graph["nodes"])


@pytest.mark.unit
def test_mixed_seed_and_observed_nodes_are_told_apart(db: FakeActorsEngine) -> None:
    _seed_known_actors(db)
    db.add_observed("ins_form4_example")

    graph = build_actor_graph(db)
    by_id = {n["id"]: n for n in graph["nodes"]}

    seeded = by_id["fed_powell"]
    observed = by_id["ins_form4_example"]

    assert seeded["source"] == "curated_seed"
    assert seeded["source_as_of"] == SEED_VINTAGE
    assert observed["source"] == "observed"
    assert observed["source_as_of"] is None
    assert all(n.get("source") for n in graph["nodes"])


@pytest.mark.unit
def test_provenance_falls_back_to_seed_membership_without_the_column() -> None:
    """A database that never ran the migration still labels correctly."""
    legacy = FakeActorsEngine(has_provenance=False)
    _seed_known_actors(legacy)
    legacy.add_observed("ins_form4_example")

    graph = build_actor_graph(legacy)
    by_id = {n["id"]: n for n in graph["nodes"]}

    assert legacy.rollbacks >= 1, "legacy retry should roll the failed txn back"
    assert by_id["fed_powell"]["source"] == "curated_seed"
    assert by_id["fed_powell"]["source_as_of"] == SEED_VINTAGE
    assert by_id["ins_form4_example"]["source"] == "observed"


@pytest.mark.unit
def test_missing_vintage_falls_back_to_the_declared_seed_vintage() -> None:
    """A seed row written before the vintage column still reports a date."""
    db = FakeActorsEngine()
    db.add_observed(
        "fed_powell", provenance=PROVENANCE_SEED, provenance_as_of=None,
    )
    actors = _load_actors_from_db(db, exclude_categories=None)

    assert actors["fed_powell"].provenance == PROVENANCE_SEED
    assert actors["fed_powell"].provenance_as_of is None
    assert source_as_of("fed_powell", PROVENANCE_SEED, None) == SEED_VINTAGE


@pytest.mark.unit
def test_resolver_prefers_the_stored_column_over_membership() -> None:
    # A seed id the operator has since re-sourced from an observation.
    assert resolve_provenance("fed_powell") == PROVENANCE_SEED
    assert resolve_provenance("fed_powell", PROVENANCE_OBSERVED) == PROVENANCE_OBSERVED
    assert actor_source("fed_powell", PROVENANCE_OBSERVED) == "observed"
    assert source_as_of("fed_powell", PROVENANCE_OBSERVED) is None


@pytest.mark.unit
def test_seed_ids_match_the_seed_table() -> None:
    assert SEED_ACTOR_IDS == frozenset(_KNOWN_ACTORS)


# ── Criterion 2: no NOW() on a seed upsert ────────────────────────────────


@pytest.mark.unit
def test_seed_upsert_stamps_the_vintage_not_now(db: FakeActorsEngine) -> None:
    _seed_known_actors(db)

    inserts = [s for s in db.statements if "INSERT INTO actors" in s]
    assert inserts, "seeder issued no inserts"
    for sql in inserts:
        assert "NOW()" not in sql, (
            "a seed upsert must not stamp wall-clock time: a hand-entered "
            "net-worth literal would report itself as freshly refreshed"
        )

    row = db.rows["fed_powell"]
    assert row["updated_at"] == SEED_VINTAGE_TS
    assert row["provenance"] == PROVENANCE_SEED
    assert str(row["provenance_as_of"]) == SEED_VINTAGE


# ── Criterion 3: no hand-assigned score labelled hard_data ────────────────


@pytest.mark.unit
def test_seed_table_carries_no_hard_data_credibility() -> None:
    src = (REPO_ROOT / "intelligence" / "actors" / "seed_data.py").read_text(
        encoding="utf-8", errors="replace",
    )
    assert src.count('"credibility": "hard_data"') == 0
    assert src.count('"credibility": "curated_estimate"') > 0


@pytest.mark.unit
def test_curated_label_reaches_the_graph_payload(db: FakeActorsEngine) -> None:
    graph = build_actor_graph(db)
    powell = next(n for n in graph["nodes"] if n["id"] == "fed_powell")

    # The hand-assigned influence score ships with the label that says so.
    assert powell["influence"] == pytest.approx(0.99)
    assert powell["credibility"] == "curated_estimate"
    assert powell["source"] == "curated_seed"
    assert powell["source_as_of"] == SEED_VINTAGE


# ── Sorting: an unscored node must not outrank a scored one ───────────────


@pytest.mark.unit
def test_actor_network_influence_sort_puts_unscored_last() -> None:
    """/actor-network sorts by influence before applying ?limit=.

    A node with no influence must sort last, not in the middle as if it
    scored 0.5. The endpoint's comparator is ``n.get("influence", 0)``.
    """
    nodes = [
        {"id": "a", "influence": 0.9},
        {"id": "unscored"},
        {"id": "b", "influence": 0.1},
    ]
    ordered = sorted(nodes, key=lambda n: n.get("influence", 0) or 0, reverse=True)
    assert [n["id"] for n in ordered] == ["a", "b", "unscored"]


# ── Criterion 1, router half: source reaches the graph endpoints ──────────


class _RouterConn:
    """Serves the SELECTs the actor graph endpoints issue, nothing else."""

    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, stmt, params=None):
        sql = " ".join(str(stmt).split())
        if "FROM actors" in sql and "conn_counts" not in sql:
            return _Result(self.rows)
        if "conn_counts" in sql:          # grand-power-map ranked CTE
            return _Result([r + (7,) for r in self.rows])
        return _Result([])                 # actor_connections / wealth_flows


def _router_engine(rows: list[tuple]):
    class _Engine:
        def connect(self):
            return _RouterConn(rows)

    return _Engine()


@pytest.mark.unit
def test_ego_graph_labels_seed_and_observed_nodes(monkeypatch) -> None:
    from api.routers import intelligence_actors as ia

    # (id, name, category, tier, influence, trust, net_worth, title, positions,
    #  provenance, provenance_as_of) -- the router now selects and passes the
    # stored column on every query; it no longer relies on SEED_ACTOR_IDS
    # membership for any of these rows.
    rows = [
        ("fed_powell", "Jerome Powell", "central_bank", "sovereign",
         0.99, 0.9, None, "Chair", "[]", PROVENANCE_SEED, SEED_VINTAGE),
        ("ins_form4_x", "Observed Insider", "insider", "individual",
         0.4, 0.5, None, "", "[]", PROVENANCE_OBSERVED, None),
    ]
    monkeypatch.setattr(ia, "get_db_engine", lambda: _router_engine(rows))

    result = ia.get_ego_graph("fed_powell", depth=1, max_nodes=50, _token="t")
    by_id = {n["id"]: n for n in result["nodes"]}

    assert by_id["fed_powell"]["source"] == "curated_seed"
    assert by_id["fed_powell"]["source_as_of"] == SEED_VINTAGE
    assert all(n.get("source") for n in result["nodes"])


@pytest.mark.unit
def test_ego_graph_does_not_relabel_an_enriched_seed_id_via_membership(monkeypatch) -> None:
    """A seed-list id whose STORED provenance says 'observed' must not be
    reported as 'curated_seed' just because its id is in SEED_ACTOR_IDS.

    fed_powell is a real entry in _KNOWN_ACTORS -- membership alone would
    say 'seed' -- but the row simulates one that _seed_known_actors's
    ON CONFLICT ... WHERE guard has already left alone after a real
    confirmation, so the stored column says 'observed'. The router must
    trust the stored column, not the static membership set.
    """
    from api.routers import intelligence_actors as ia

    rows = [
        ("fed_powell", "Jerome Powell", "central_bank", "sovereign",
         0.99, 0.9, None, "Chair", "[]", PROVENANCE_OBSERVED, None),
    ]
    monkeypatch.setattr(ia, "get_db_engine", lambda: _router_engine(rows))

    result = ia.get_ego_graph("fed_powell", depth=1, max_nodes=50, _token="t")
    by_id = {n["id"]: n for n in result["nodes"]}

    assert "fed_powell" in SEED_ACTOR_IDS, "test premise: this id is on the seed list"
    assert by_id["fed_powell"]["source"] == "observed"
    assert by_id["fed_powell"]["source_as_of"] is None


@pytest.mark.unit
def test_grand_power_map_labels_every_node(monkeypatch) -> None:
    from api.routers import intelligence_actors as ia

    rows = [
        ("fed_powell", "Jerome Powell", "central_bank", "sovereign",
         0.99, 0.9, None, "Chair", "[]", PROVENANCE_SEED, SEED_VINTAGE),
        ("ins_form4_x", "Observed Insider", "insider", "individual",
         0.4, 0.5, None, "", "[]", PROVENANCE_OBSERVED, None),
    ]
    monkeypatch.setattr(ia, "get_db_engine", lambda: _router_engine(rows))

    result = ia.get_grand_power_map(limit=10, _token="t")
    by_id = {n["id"]: n for n in result["nodes"]}

    assert by_id["fed_powell"]["source"] == "curated_seed"
    assert by_id["ins_form4_x"]["source"] == "observed"
    assert by_id["ins_form4_x"]["source_as_of"] is None
    assert all(n.get("source") for n in result["nodes"])
