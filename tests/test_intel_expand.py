"""Offline tests for the cross-domain intel-expand endpoint shaping.

Exercises ``intelligence_actors._intel_expand_graph`` and the pure
``_committees_for_ticker`` helper with a fully mocked DB engine so no live
PostgreSQL is required. Verifies the typed chain:

    TICKER → supplier → causation → committee (jurisdiction) → member_trade

is assembled with the right discrete ``relationship`` types and colours, and
that missing tables degrade gracefully rather than raising.
"""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

# Mirror test_canvas_api.py: prefer real api.auth/dependencies, stub only if
# heavy transitive deps are missing in a lightweight environment.
try:
    import api.auth  # noqa: F401
except Exception:
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub
try:
    import api.dependencies  # noqa: F401
except Exception:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub

from api.routers import intelligence_actors as ia  # noqa: E402


# ── Fake DB engine ─────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    """Routes queries to canned rows based on substrings in the SQL text."""

    def __init__(self, *, tables, supplier_rows, supplier_labels,
                 cause_rows, trade_rows):
        self._tables = set(tables)
        self._supplier_rows = supplier_rows
        self._supplier_labels = supplier_labels
        self._cause_rows = cause_rows
        self._trade_rows = trade_rows

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, stmt, params=None):
        sql = str(getattr(stmt, "text", stmt))
        # Bound params can arrive via .bindparams() (compiled into the
        # statement) or as the params dict — merge both.
        merged = dict(params or {})
        try:
            merged.update(stmt.compile().params or {})
        except Exception:
            pass
        if "to_regclass" in sql:
            name = merged.get("n")
            return _FakeResult([(name,)] if name in self._tables else [(None,)])
        if "FROM supply_chain_edges" in sql:
            return _FakeResult(self._supplier_rows)
        if "FROM supply_chain_nodes" in sql:
            return _FakeResult(self._supplier_labels)
        if "FROM causal_links" in sql:
            return _FakeResult(self._cause_rows)
        if "FROM congressional_trades" in sql:
            return _FakeResult(self._trade_rows)
        return _FakeResult([])


class _FakeEngine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


def _full_engine():
    """Engine with all four hops populated for ticker JPM."""
    conn = _FakeConn(
        tables={
            "supply_chain_edges", "supply_chain_nodes",
            "causal_links", "congressional_trades",
        },
        # (upstream_id, relationship, annual_usd, pct_downstream_cogs,
        #  chokepoint_score, confidence)
        supplier_rows=[
            ("fiserv", "payments_processor", 1.2e9, 0.05, 0.7, 0.8),
            ("visa_inc", "network", 8.0e8, 0.03, 0.4, 0.9),
        ],
        supplier_labels=[("fiserv", "Fiserv Inc"), ("visa_inc", "Visa Inc")],
        # (cause_type, probable_cause, prob)
        cause_rows=[
            ("regulatory", "Basel III capital rules tighten lending", 0.72),
        ],
        # (representative, party, state, transaction_type, amount_midpoint,
        #  committee, disclosure_date)
        trade_rows=[
            ("Smith", "R", "TX", "BUY", 50000.0,
             "House Financial Services Committee", "2026-03-01"),
        ],
    )
    return _FakeEngine(conn)


# ── _committees_for_ticker (pure) ───────────────────────────────────────────


def test_committees_for_ticker_maps_financials():
    committees = ia._committees_for_ticker("JPM")
    # JPM appears under banking / finance / financial services lists.
    assert "banking" in committees
    assert "financial services" in committees


def test_committees_for_ticker_empty_for_unknown():
    assert ia._committees_for_ticker("ZZZZ") == []
    assert ia._committees_for_ticker("") == []


def test_committees_for_ticker_is_case_insensitive():
    assert ia._committees_for_ticker("jpm") == ia._committees_for_ticker("JPM")


# ── _intel_expand_graph shaping ─────────────────────────────────────────────


def test_intel_expand_full_chain_shape():
    out = ia._intel_expand_graph(_full_engine(), "JPM")

    assert out["center"] == "t:JPM"
    assert out["ticker"] == "JPM"

    node_ids = {n["id"] for n in out["nodes"]}
    assert "t:JPM" in node_ids                 # center ticker
    assert "sc:fiserv" in node_ids             # supplier node
    assert any(i.startswith("cause:JPM:") for i in node_ids)   # causal lever
    assert any(i.startswith("committee:") for i in node_ids)   # committee
    assert "member:Smith" in node_ids          # committee member

    rels = {e["relationship"] for e in out["edges"]}
    assert {"supplier", "causation", "jurisdiction", "member_trade"} <= rels
    # member's disclosed trade ties back to the ticker
    assert "congressional_trade" in rels


def test_intel_expand_edges_are_typed_and_colored():
    out = ia._intel_expand_graph(_full_engine(), "JPM")
    for e in out["edges"]:
        # discrete type present and mirrored into `type` for the frontend
        assert e["relationship"] == e["type"]
        assert e["color"] == ia._RELATIONSHIP_COLORS.get(e["relationship"], "#64748B")
    # supplier edge points INTO the ticker (upstream → downstream)
    sup = next(e for e in out["edges"] if e["relationship"] == "supplier")
    assert sup["target"] == "t:JPM"
    assert sup["color"] == "#3B82F6"  # blue


def test_intel_expand_jurisdiction_links_cause_to_committee():
    out = ia._intel_expand_graph(_full_engine(), "JPM")
    cause_ids = {n["id"] for n in out["nodes"] if n["id"].startswith("cause:")}
    juris = [e for e in out["edges"] if e["relationship"] == "jurisdiction"]
    assert juris, "expected jurisdiction edges"
    # When a causal lever exists, jurisdiction edges anchor on it (policy →
    # committee), not directly on the ticker.
    assert all(e["target"] in cause_ids for e in juris)


def test_intel_expand_member_trade_attaches_to_matching_committee():
    out = ia._intel_expand_graph(_full_engine(), "JPM")
    member_edges = [e for e in out["edges"] if e["relationship"] == "member_trade"]
    assert member_edges, "expected member_trade edge"
    committee_ids = {n["id"] for n in out["nodes"] if n["id"].startswith("committee:")}
    # member_trade edge: committee → member
    assert member_edges[0]["source"] in committee_ids
    assert member_edges[0]["target"] == "member:Smith"


# ── Graceful degradation ────────────────────────────────────────────────────


def test_intel_expand_missing_tables_degrades_to_center_only():
    conn = _FakeConn(
        tables=set(),  # no tables exist
        supplier_rows=[], supplier_labels=[], cause_rows=[], trade_rows=[],
    )
    # ZZZZ has no committee jurisdiction either, so with every table absent the
    # graph collapses to just the center ticker — no edges, no exception.
    out = ia._intel_expand_graph(_FakeEngine(conn), "ZZZZ")
    assert [n["id"] for n in out["nodes"]] == ["t:ZZZZ"]
    assert out["edges"] == []


def test_intel_expand_no_committees_still_returns_supply_and_cause():
    # ZZZZ has no committee jurisdiction, but supply/cause hops still populate.
    conn = _FakeConn(
        tables={"supply_chain_edges", "supply_chain_nodes", "causal_links",
                "congressional_trades"},
        supplier_rows=[("acme", "parts", 1.0e8, 0.1, 0.3, 0.7)],
        supplier_labels=[("acme", "Acme Corp")],
        cause_rows=[("macro", "rates up", 0.6)],
        trade_rows=[],
    )
    out = ia._intel_expand_graph(_FakeEngine(conn), "ZZZZ")
    rels = {e["relationship"] for e in out["edges"]}
    assert "supplier" in rels
    assert "causation" in rels
    assert "jurisdiction" not in rels  # no committee for ZZZZ
    assert "member_trade" not in rels


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
